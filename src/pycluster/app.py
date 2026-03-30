from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import fnmatch
import json
import logging
from pathlib import Path
import re
import signal

from .config import AppConfig
from .maidenhead import extract_locator
from .models import Spot, is_valid_call, normalize_call
from .node_link import NodeLinkEngine
from .protocol import Pc10Message, Pc11Message, Pc12Message, Pc18Message, Pc24Message, Pc28Message, Pc29Message, Pc30Message, Pc31Message, Pc32Message, Pc33Message, Pc50Message, Pc51Message, Pc61Message, Pc93Message, WirePcFrame
from .store import SpotStore
from .telnet_server import TelnetClusterServer
from .transports import DxSpiderInboundConnection, dxspider_compat_pc18
from .public_web import PublicWebServer
from .web_admin import WebAdminServer

LOG = logging.getLogger(__name__)
_PC93_PREFIX_RE = re.compile(r"^\[(ANNOUNCE|WCY|WWV|WX)/(LOCAL|FULL|SYSOP)\]\s*(.*)$", re.IGNORECASE)
_DXSPIDER_PC19_VERSION = "5457"
_PEER_PREF_PREFIX = "peer.outbound."
_RECONNECT_BASE_SECS = 5
_RECONNECT_MAX_SECS = 300
_PEER_HEARTBEAT_SECS = 60
_PROTO_FLAP_KEYS = {
    "pc18.software",
    "pc18.proto",
    "pc18.family",
    "pc18.summary",
}


class ClusterApp:
    def __init__(self, config: AppConfig, config_path: str | None = None) -> None:
        self.config = config
        self.started_at = datetime.now(timezone.utc)
        self.store = SpotStore(config.store.sqlite_path)
        strings_path = str(Path(config_path).with_name("strings.toml")) if config_path else None
        self.node_link = NodeLinkEngine()
        self.node_link.set_trace_hook(self._trace_protocol_line)
        self._legacy_dxspider_peers: set[str] = set()
        self._mail_stream_seq = 0
        self._outbound_mail: dict[tuple[str, str], dict[str, object]] = {}
        self._outbound_mail_pending_header: dict[str, list[dict[str, object]]] = {}
        self._inbound_mail: dict[tuple[str, str], dict[str, object]] = {}
        self.telnet = TelnetClusterServer(
            config=config,
            store=self.store,
            started_at=self.started_at,
            link_stats_fn=self.node_link.stats,
            link_set_profile_fn=self.node_link.set_peer_profile,
            link_connect_fn=self.connect_peer,
            link_disconnect_fn=self.disconnect_peer,
            link_clear_policy_fn=self.node_link.clear_policy_drops,
            link_desired_peers_fn=self.desired_peer_status,
            component_status_fn=self.component_status,
            component_restart_fn=self.restart_component,
            on_chat_fn=self._relay_chat_to_links,
            on_bulletin_fn=self._relay_bulletin_to_links,
            on_spot_fn=self._relay_spot_to_links,
            on_message_fn=self._relay_message_to_links,
            on_sessions_changed_fn=self._sync_legacy_user_roster,
            on_node_login_fn=self.accept_inbound_node_login,
            strings_path=strings_path,
        )
        self.web = WebAdminServer(
            config=config,
            store=self.store,
            started_at=self.started_at,
            session_count_fn=lambda: self.telnet.session_count,
            active_calls_fn=self.telnet.active_calls,
            link_stats_fn=self.node_link.stats,
            link_desired_peers_fn=self.desired_peer_status,
            link_clear_policy_fn=self.node_link.clear_policy_drops,
            link_connect_fn=self.connect_peer,
            link_disconnect_fn=self.disconnect_peer,
            link_set_profile_fn=self.node_link.set_peer_profile,
            link_save_peer_fn=self.save_peer_target,
            publish_spot_fn=self.telnet.publish_spot,
            relay_spot_fn=self._relay_spot_to_links,
            publish_chat_fn=self.telnet.publish_chat,
            relay_chat_fn=self._relay_chat_to_links,
            publish_bulletin_fn=self.telnet.publish_bulletin,
            relay_bulletin_fn=self._relay_bulletin_to_links,
            telnet_rebind_fn=self.telnet.rebind_listeners,
            event_log_fn=self.telnet.record_event,
            audit_rows_fn=self.telnet.audit_rows,
            config_path=config_path,
        )
        self.public_web = PublicWebServer(
            config=config,
            store=self.store,
            started_at=self.started_at,
            link_stats_fn=self.node_link.stats,
            link_desired_peers_fn=self.desired_peer_status,
            publish_spot_fn=self.telnet.publish_spot,
            relay_spot_fn=self._relay_spot_to_links,
            publish_chat_fn=self.telnet.publish_chat,
            relay_chat_fn=self._relay_chat_to_links,
            publish_bulletin_fn=self.telnet.publish_bulletin,
            relay_bulletin_fn=self._relay_bulletin_to_links,
            event_log_fn=self.telnet.record_event,
        )
        self._node_ingest_task: asyncio.Task[None] | None = None
        self._peer_reconnect_task: asyncio.Task[None] | None = None
        self._peer_heartbeat_task: asyncio.Task[None] | None = None
        self._node_ingest_stop = asyncio.Event()
        self._proto_trace_lock = asyncio.Lock()
        self._public_web_started = False

    def _peer_pref_key(self, name: str, field: str) -> str:
        slug = re.sub(r"[^a-z0-9_.-]", "_", name.lower())
        return f"{_PEER_PREF_PREFIX}{slug}.{field}"

    async def _persist_peer_target(self, name: str, dsn: str, profile: str = "dxspider", reconnect: bool = True) -> None:
        now = int(datetime.now(timezone.utc).timestamp())
        p = profile.strip().lower() or "dxspider"
        await self.store.set_user_pref(self.config.node.node_call, self._peer_pref_key(name, "name"), name, now)
        await self.store.set_user_pref(self.config.node.node_call, self._peer_pref_key(name, "dsn"), dsn, now)
        await self.store.set_user_pref(self.config.node.node_call, self._peer_pref_key(name, "profile"), p, now)
        await self.store.set_user_pref(
            self.config.node.node_call,
            self._peer_pref_key(name, "reconnect"),
            "on" if reconnect else "off",
            now,
        )
        await self.store.delete_user_pref(self.config.node.node_call, self._peer_pref_key(name, "last_error"))
        await self.store.set_user_pref(self.config.node.node_call, self._peer_pref_key(name, "retry_count"), "0", now)
        await self.store.set_user_pref(self.config.node.node_call, self._peer_pref_key(name, "next_retry_epoch"), "0", now)

    async def save_peer_target(self, name: str, dsn: str, profile: str = "dxspider", reconnect: bool = True) -> None:
        await self._persist_peer_target(name, dsn, profile=profile, reconnect=reconnect)

    async def _forget_peer_target(self, name: str) -> None:
        keys = [
            "name",
            "dsn",
            "profile",
            "reconnect",
            "retry_count",
            "next_retry_epoch",
            "last_error",
            "last_connect_epoch",
        ]
        for field in keys:
            await self.store.delete_user_pref(self.config.node.node_call, self._peer_pref_key(name, field))

    async def _desired_peer_targets(self) -> dict[str, dict[str, str]]:
        prefs = await self.store.list_user_prefs(self.config.node.node_call)
        out: dict[str, dict[str, str]] = {}
        for key, value in prefs.items():
            if not key.startswith(_PEER_PREF_PREFIX):
                continue
            rest = key[len(_PEER_PREF_PREFIX):]
            if "." not in rest:
                continue
            slug, field = rest.split(".", 1)
            row = out.setdefault(slug, {})
            row[field] = str(value)
        desired: dict[str, dict[str, str]] = {}
        for row in out.values():
            name = row.get("name", "").strip()
            dsn = row.get("dsn", "").strip()
            if not name or not dsn:
                continue
            desired[name] = row
        return desired

    async def connect_peer(self, name: str, dsn: str, profile: str = "dxspider", persist: bool = True) -> None:
        if persist:
            await self._persist_peer_target(name, dsn, profile=profile, reconnect=True)
        wire_profile = "spider" if dsn.strip().lower().startswith("dxspider://") and profile == "dxspider" else profile
        await self.node_link.connect_dsn(name, dsn, profile=wire_profile)
        await self._reset_mail_transport_state(name, "peer session refreshed")
        now = int(datetime.now(timezone.utc).timestamp())
        await self.store.set_user_pref(self.config.node.node_call, self._peer_pref_key(name, "last_connect_epoch"), str(now), now)
        await self.store.set_user_pref(self.config.node.node_call, self._peer_pref_key(name, "retry_count"), "0", now)
        await self.store.set_user_pref(self.config.node.node_call, self._peer_pref_key(name, "next_retry_epoch"), "0", now)
        await self.store.delete_user_pref(self.config.node.node_call, self._peer_pref_key(name, "last_error"))
        if dsn.strip().lower().startswith("dxspider://"):
            try:
                await self._send_legacy_init_config(name)
            except KeyError:
                self._legacy_dxspider_peers.discard(name)
                LOG.info("legacy dxspider init skipped for disconnected peer=%s", name)
            else:
                self._legacy_dxspider_peers.add(name)
        await self._flush_pending_messages_for_peer(name)
    async def desired_peer_status(self) -> list[dict[str, object]]:
        desired = await self._desired_peer_targets()
        live = set(await self.node_link.peer_names())
        out: list[dict[str, object]] = []
        for name in sorted(desired):
            row = desired[name]
            reconnect_raw = str(row.get("reconnect", "on")).strip().lower()
            enabled = reconnect_raw in {"1", "on", "yes", "true"}
            try:
                retry_count = int(str(row.get("retry_count", "0")).strip() or "0")
            except ValueError:
                retry_count = 0
            try:
                next_retry_epoch = int(str(row.get("next_retry_epoch", "0")).strip() or "0")
            except ValueError:
                next_retry_epoch = 0
            try:
                last_connect_epoch = int(str(row.get("last_connect_epoch", "0")).strip() or "0")
            except ValueError:
                last_connect_epoch = 0
            out.append(
                {
                    "peer": name,
                    "dsn": str(row.get("dsn", "")).strip(),
                    "profile": str(row.get("profile", "dxspider")).strip().lower() or "dxspider",
                    "reconnect_enabled": enabled,
                    "retry_count": retry_count,
                    "next_retry_epoch": next_retry_epoch,
                    "last_connect_epoch": last_connect_epoch,
                    "last_error": str(row.get("last_error", "")).strip(),
                    "desired": True,
                    "connected": name in live,
                }
            )
        return out

    async def component_status(self) -> list[dict[str, object]]:
        telnet_ports = sorted(await self.telnet.active_ports())
        return [
            {
                "component": "telnet",
                "state": "up" if telnet_ports else "down",
                "detail": ",".join(str(p) for p in telnet_ports) if telnet_ports else "not listening",
            },
            {
                "component": "sysopweb",
                "state": "up" if self.web._server is not None else "down",
                "detail": f"{self.config.web.host}:{self.config.web.port}",
            },
            {
                "component": "publicweb",
                "state": "up" if self._public_web_started and self.public_web._server is not None else "down",
                "detail": f"{self.config.public_web.host}:{self.config.public_web.port}",
            },
        ]

    async def restart_component(self, component: str) -> tuple[bool, str]:
        comp = (component or "").strip().lower()
        if comp == "telnet":
            ports = await self.telnet.rebind_listeners()
            return True, f"Telnet listeners restarted on {','.join(str(p) for p in ports)}."
        if comp in {"sysopweb", "web", "adminweb"}:
            await self.web.stop()
            await self.web.start()
            return True, f"System Operator web restarted on {self.config.web.host}:{self.config.web.port}."
        if comp == "all":
            ports = await self.telnet.rebind_listeners()
            await self.web.stop()
            await self.web.start()
            return True, (
                f"Telnet listeners restarted on {','.join(str(p) for p in ports)}; "
                f"System Operator web restarted on {self.config.web.host}:{self.config.web.port}."
            )
        return False, "Usage: sysop/restart <telnet|sysopweb|all>"

    async def disconnect_peer(self, name: str, forget: bool = True) -> bool:
        if forget:
            await self._forget_peer_target(name)
        self._legacy_dxspider_peers.discard(name)
        return await self.node_link.disconnect_peer(name)

    async def accept_inbound_node_login(
        self,
        call: str,
        peer_name: str,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        initial_lines: list[str] | None = None,
    ) -> bool:
        profile = (await self.store.get_user_pref(call, "node_family") or "dxspider").strip().lower() or "dxspider"
        conn = DxSpiderInboundConnection(call, reader, writer, initial_lines=initial_lines)
        await self.node_link.accept_inbound(call, conn, profile=profile)
        await conn.send_line(dxspider_compat_pc18())
        await conn.send_line("PC20^")
        if profile == "dxspider":
            self._legacy_dxspider_peers.add(call)
            try:
                await self._send_legacy_init_config(call)
            except KeyError:
                self._legacy_dxspider_peers.discard(call)
                LOG.info("legacy inbound init skipped for disconnected peer=%s", call)
                return False
        await self._reset_mail_transport_state(call, "peer session refreshed")
        await self._flush_pending_messages_for_peer(call)
        LOG.info("accepted inbound node login call=%s peer=%s", call, peer_name)
        return True

    async def start(self, *, with_public_web: bool = True) -> None:
        self._node_ingest_stop.clear()
        await self.telnet.start()
        await self.web.start()
        self._public_web_started = False
        if with_public_web:
            await self.public_web.start()
            self._public_web_started = True
        self._node_ingest_task = asyncio.create_task(self._node_ingest_loop(), name="node-link-ingest")
        self._peer_reconnect_task = asyncio.create_task(self._peer_reconnect_loop(), name="node-link-reconnect")
        self._peer_heartbeat_task = asyncio.create_task(self._peer_heartbeat_loop(), name="node-link-heartbeat")

    async def stop(self) -> None:
        self._node_ingest_stop.set()
        if self._peer_reconnect_task:
            self._peer_reconnect_task.cancel()
            try:
                await self._peer_reconnect_task
            except asyncio.CancelledError:
                pass
            self._peer_reconnect_task = None
        if self._peer_heartbeat_task:
            self._peer_heartbeat_task.cancel()
            try:
                await self._peer_heartbeat_task
            except asyncio.CancelledError:
                pass
            self._peer_heartbeat_task = None
        if self._node_ingest_task:
            self._node_ingest_task.cancel()
            try:
                await self._node_ingest_task
            except asyncio.CancelledError:
                pass
            self._node_ingest_task = None
        if self._public_web_started:
            await self.public_web.stop()
            self._public_web_started = False
        await self.web.stop()
        await self.telnet.stop()
        await self.node_link.stop()
        await self.store.close()

    async def _peer_reconnect_loop(self) -> None:
        while not self._node_ingest_stop.is_set():
            try:
                await self.reconnect_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                LOG.exception("peer reconnect loop failed")
            try:
                await asyncio.wait_for(self._node_ingest_stop.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                pass

    async def reconnect_once(self) -> None:
        desired = await self._desired_peer_targets()
        if not desired:
            return
        live = set(await self.node_link.peer_names())
        now = int(datetime.now(timezone.utc).timestamp())
        for name, row in desired.items():
            if name in live:
                continue
            reconnect_raw = row.get("reconnect", "on").strip().lower()
            if reconnect_raw not in {"1", "on", "yes", "true"}:
                continue
            try:
                next_retry = int(str(row.get("next_retry_epoch", "0")).strip() or "0")
            except ValueError:
                next_retry = 0
            if next_retry > now:
                continue
            dsn = row.get("dsn", "").strip()
            profile = row.get("profile", "dxspider").strip().lower() or "dxspider"
            try:
                await self.connect_peer(name, dsn, profile=profile, persist=False)
            except Exception as exc:
                retry_count_raw = row.get("retry_count", "0").strip()
                try:
                    retry_count = int(retry_count_raw or "0")
                except ValueError:
                    retry_count = 0
                retry_count += 1
                delay = min(_RECONNECT_MAX_SECS, _RECONNECT_BASE_SECS * (2 ** max(0, retry_count - 1)))
                next_epoch = now + delay
                await self.store.set_user_pref(
                    self.config.node.node_call,
                    self._peer_pref_key(name, "retry_count"),
                    str(retry_count),
                    now,
                )
                await self.store.set_user_pref(
                    self.config.node.node_call,
                    self._peer_pref_key(name, "next_retry_epoch"),
                    str(next_epoch),
                    now,
                )
                await self.store.set_user_pref(
                    self.config.node.node_call,
                    self._peer_pref_key(name, "last_error"),
                    str(exc),
                    now,
                )
                LOG.warning("peer reconnect failed peer=%s next_retry=%ss err=%s", name, delay, exc)

    async def _peer_heartbeat_loop(self) -> None:
        while not self._node_ingest_stop.is_set():
            try:
                await self.heartbeat_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                LOG.exception("peer heartbeat loop failed")
            try:
                await asyncio.wait_for(self._node_ingest_stop.wait(), timeout=_PEER_HEARTBEAT_SECS)
            except asyncio.TimeoutError:
                pass

    async def heartbeat_once(self) -> int:
        sent = 0
        stats = await self.node_link.stats()
        for name, row in stats.items():
            profile = str(row.get("profile", "dxspider")).strip().lower()
            if profile != "dxspider":
                continue
            if bool(row.get("inbound")):
                continue
            try:
                await self.node_link.send(name, WirePcFrame("PC20", [""]))
            except KeyError:
                continue
            except (ConnectionError, OSError):
                LOG.info("peer heartbeat skipped for disconnected peer=%s", name)
                continue
            sent += 1
        return sent

    def _classify_pc93_bulletin(self, sender: str, text: str) -> tuple[str, str, str]:
        category = "chat"
        scope = "LOCAL"
        body = text
        m = _PC93_PREFIX_RE.match(text)
        if m:
            return m.group(1).strip().lower(), m.group(2).strip().upper(), (m.group(3) or "").strip() or text
        sender_u = normalize_call(sender)
        body_u = text.upper()
        if sender_u == "DK0WCY" or ("SPOTS=" in body_u and "EXPK=" in body_u):
            category = "wcy"
        elif sender_u == "WWV" or re.search(r"\bSFI\s*=\s*\d+\b", body_u):
            category = "wwv"
        return category, scope, body

    async def _send_legacy_init_config(self, peer_name: str) -> None:
        node_call = self.config.node.node_call.upper()
        await self.node_link.send(
            peer_name,
            WirePcFrame("PC19", ["1", node_call, "0", _DXSPIDER_PC19_VERSION, "H1", ""]),
        )
        await self._send_legacy_pc16(peer_name)
        await self.node_link.send(peer_name, WirePcFrame("PC22", [""]))

    async def _send_legacy_pc16(self, peer_name: str) -> None:
        node_call = self.config.node.node_call.upper()
        calls = []
        for call in self.telnet.active_calls():
            if await self._routepc19_enabled(call):
                calls.append(call)
        payload = [node_call]
        payload.extend(f"{call} - 1" for call in calls)
        payload.extend(["H1", ""])
        await self.node_link.send(peer_name, WirePcFrame("PC16", payload))

    async def _sync_legacy_user_roster(self) -> None:
        if not self._legacy_dxspider_peers:
            return
        live_peers = set(await self.node_link.peer_names())
        stale = self._legacy_dxspider_peers - live_peers
        if stale:
            self._legacy_dxspider_peers.difference_update(stale)
        for peer_name in sorted(self._legacy_dxspider_peers & live_peers):
            try:
                await self._send_legacy_pc16(peer_name)
            except KeyError:
                self._legacy_dxspider_peers.discard(peer_name)
                LOG.info("legacy pc16 sync skipped for disconnected peer=%s", peer_name)
            except (ConnectionError, OSError):
                self._legacy_dxspider_peers.discard(peer_name)
                LOG.info("legacy pc16 sync dropped disconnected peer=%s", peer_name)
            except Exception:
                LOG.exception("legacy pc16 sync failed peer=%s", peer_name)

    def _pc61_epoch(self, msg: Pc61Message) -> int:
        date_token = (msg.date_token or "").strip()
        time_token = (msg.time_token or "").strip().upper()
        if date_token and time_token:
            try:
                dt = datetime.strptime(f"{date_token} {time_token}", "%d-%b-%Y %H%MZ")
                return int(dt.replace(tzinfo=timezone.utc).timestamp())
            except ValueError:
                pass
        return int(datetime.now(timezone.utc).timestamp())

    async def _record_proto_state(self, peer_name: str, values: dict[str, str]) -> None:
        now = int(datetime.now(timezone.utc).timestamp())
        peer_tag = re.sub(r"[^a-z0-9_.-]", "_", peer_name.lower())
        pfx = f"proto.peer.{peer_tag}."
        prefs = await self.store.list_user_prefs(self.config.node.node_call)
        uvars = await self.store.list_user_vars(self.config.node.node_call)
        cfg = dict(prefs)
        cfg.update(uvars)

        def _to_int(v: str | None, default: int = 0) -> int:
            try:
                return int(str(v or "").strip())
            except ValueError:
                return default

        change_count = _to_int(cfg.get(pfx + "change_count"), 0)
        flap_score = _to_int(cfg.get(pfx + "flap_score"), 0)
        last_change_epoch = _to_int(cfg.get(pfx + "last_change_epoch"), 0)
        flap_window_secs = max(5, min(86400, _to_int(cfg.get("proto.threshold.flap_window_secs"), 300)))
        any_changed = False
        flap_relevant_changed = False
        changed_events: list[dict[str, object]] = []
        for key, value in values.items():
            prev = cfg.get(pfx + key)
            if prev is not None and prev != value:
                any_changed = True
                if key in _PROTO_FLAP_KEYS:
                    flap_relevant_changed = True
                kname = key.replace(".", "_")
                per_key = _to_int(cfg.get(pfx + f"change.{kname}"), 0) + 1
                await self.store.set_user_pref(
                    self.config.node.node_call,
                    pfx + f"change.{kname}",
                    str(per_key),
                    now,
                )
                changed_events.append(
                    {
                        "epoch": now,
                        "key": key,
                        "from": str(prev),
                        "to": str(value),
                    }
                )
            await self.store.set_user_pref(
                self.config.node.node_call,
                pfx + key,
                value,
                now,
            )
        if any_changed:
            change_count += 1
            await self.store.set_user_pref(self.config.node.node_call, pfx + "change_count", str(change_count), now)
            if flap_relevant_changed:
                # Consider only stable protocol-state changes as flapping.
                if last_change_epoch > 0 and now - last_change_epoch <= flap_window_secs:
                    flap_score += 1
                else:
                    flap_score = max(0, flap_score - 1)
                await self.store.set_user_pref(self.config.node.node_call, pfx + "flap_score", str(flap_score), now)
                await self.store.set_user_pref(self.config.node.node_call, pfx + "last_change_epoch", str(now), now)
            raw_hist = cfg.get(pfx + "history", "[]")
            try:
                hist_obj = json.loads(raw_hist)
                hist = hist_obj if isinstance(hist_obj, list) else []
            except Exception:
                hist = []
            for ev in changed_events:
                hist.append(ev)
            hist = hist[-40:]
            await self.store.set_user_pref(
                self.config.node.node_call,
                pfx + "history",
                json.dumps(hist, separators=(",", ":"), ensure_ascii=True),
                now,
            )
        await self.store.set_user_pref(
            self.config.node.node_call,
            pfx + "last_epoch",
            str(now),
            now,
        )

    def _peer_identity_from_pc18(self, software: str) -> tuple[str, str]:
        text = re.sub(r"\s+", " ", (software or "").strip())
        low = text.lower()
        family = ""
        summary = text
        if "pycluster" in low:
            family = "pycluster"
        elif "dxspider" in low or "dx spider" in low:
            family = "dxspider"
            m = re.search(r"version:\s*([^\s]+)\s+build:\s*([^\s]+)", text, re.IGNORECASE)
            if m:
                summary = f"DXSpider {m.group(1)} build {m.group(2)}"
        elif "ar-cluster" in low:
            family = "arcluster"
        elif re.search(r"\bclx\b", low):
            family = "clx"
        elif "dxnet" in low:
            family = "dxnet"
        return family, summary[:60]

    async def _touch_proto_activity(self, peer_name: str, pc_type: str) -> None:
        now = int(datetime.now(timezone.utc).timestamp())
        peer_tag = re.sub(r"[^a-z0-9_.-]", "_", peer_name.lower())
        pfx = f"proto.peer.{peer_tag}."
        await self.store.set_user_pref(
            self.config.node.node_call,
            pfx + "last_epoch",
            str(now),
            now,
        )
        await self.store.set_user_pref(
            self.config.node.node_call,
            pfx + "last_pc_type",
            str(pc_type or "").strip().upper(),
            now,
        )

    async def _handle_node_link_item(self, peer_name: str, frame: WirePcFrame, typed: object | None) -> None:
        if frame.pc_type in {"PC10", "PC11", "PC12", "PC16", "PC17", "PC18", "PC19", "PC21", "PC22", "PC23", "PC24", "PC28", "PC29", "PC30", "PC31", "PC32", "PC33", "PC50", "PC51", "PC61", "PC93"}:
            await self._touch_proto_activity(peer_name, frame.pc_type)

        if frame.pc_type == "PC18":
            msg = typed if isinstance(typed, Pc18Message) else Pc18Message.from_fields(frame.payload_fields)
            family, summary = self._peer_identity_from_pc18(msg.software)
            await self._record_proto_state(
                peer_name,
                {
                    "pc18.software": (msg.software or "").strip(),
                    "pc18.proto": (msg.proto_version or "").strip(),
                    "pc18.family": family,
                    "pc18.summary": summary,
                },
            )
            return

        if frame.pc_type == "PC24":
            msg = typed if isinstance(typed, Pc24Message) else Pc24Message.from_fields(frame.payload_fields)
            await self._record_proto_state(
                peer_name,
                {
                    "pc24.call": normalize_call(msg.call) if msg.call else "",
                    "pc24.flag": (msg.flag or "").strip(),
                },
            )
            return

        if frame.pc_type == "PC50":
            msg = typed if isinstance(typed, Pc50Message) else Pc50Message.from_fields(frame.payload_fields)
            await self._record_proto_state(
                peer_name,
                {
                    "pc50.call": normalize_call(msg.call) if msg.call else "",
                    "pc50.count": (msg.node_count or "").strip(),
                },
            )
            return

        if frame.pc_type == "PC51":
            msg = typed if isinstance(typed, Pc51Message) else Pc51Message.from_fields(frame.payload_fields)
            await self._record_proto_state(
                peer_name,
                {
                    "pc51.to": normalize_call(msg.to_call) if msg.to_call else "",
                    "pc51.from": normalize_call(msg.from_call) if msg.from_call else "",
                    "pc51.value": (msg.value or "").strip(),
                },
            )
            if (
                normalize_call(msg.to_call) == normalize_call(self.config.node.node_call)
                and (msg.value or "").strip() == "1"
            ):
                await self.node_link.send(
                    peer_name,
                    WirePcFrame(
                        "PC51",
                        Pc51Message(
                            to_call=normalize_call(msg.from_call),
                            from_call=normalize_call(msg.to_call),
                            value="0",
                        ).to_fields(),
                    ),
                )
            return

        if frame.pc_type == "PC11":
            if not await self._ingest_peer_enabled(peer_name, "spots"):
                await self.node_link.mark_policy_drop(peer_name, "ingest_spots_disabled")
                return
            msg = typed if isinstance(typed, Pc11Message) else Pc11Message.from_fields(frame.payload_fields)
            dx_call = normalize_call(msg.dx_call)
            spotter = normalize_call(msg.spotter)
            if not is_valid_call(dx_call) or not is_valid_call(spotter):
                await self.node_link.mark_policy_drop(peer_name, "ingest_spots_invalid_call")
                return
            try:
                freq_khz = float(msg.freq_khz)
            except ValueError:
                await self.node_link.mark_policy_drop(peer_name, "ingest_spots_invalid_freq")
                return
            source_node = normalize_call(msg.source_node) if msg.source_node else normalize_call(peer_name)
            if source_node == normalize_call(self.config.node.node_call):
                await self.node_link.mark_policy_drop(peer_name, "ingest_spots_loop")
                return
            epoch = self._pc61_epoch(
                Pc61Message(
                    freq_khz=msg.freq_khz,
                    dx_call=msg.dx_call,
                    date_token=msg.date_token,
                    time_token=msg.time_token,
                    info=msg.info,
                    spotter=msg.spotter,
                    source_node=msg.source_node,
                )
            )
            raw = "^".join(
                [
                    f"{freq_khz:.1f}",
                    dx_call,
                    str(epoch),
                    msg.info,
                    spotter,
                    "226",
                    "226",
                    source_node,
                ]
            )
            spot = Spot(
                freq_khz=freq_khz,
                dx_call=dx_call,
                epoch=epoch,
                info=msg.info,
                spotter=spotter,
                source_node=source_node,
                raw=raw,
            )
            inserted = await self.store.add_spot(spot)
            if inserted:
                await self.telnet.publish_spot(spot)
                await self._relay_spot_to_links(spot, exclude_peer=peer_name)
            return

        if frame.pc_type == "PC61":
            if not await self._ingest_peer_enabled(peer_name, "spots"):
                await self.node_link.mark_policy_drop(peer_name, "ingest_spots_disabled")
                return
            msg = typed if isinstance(typed, Pc61Message) else Pc61Message.from_fields(frame.payload_fields)
            dx_call = normalize_call(msg.dx_call)
            spotter = normalize_call(msg.spotter)
            if not is_valid_call(dx_call) or not is_valid_call(spotter):
                await self.node_link.mark_policy_drop(peer_name, "ingest_spots_invalid_call")
                return
            try:
                freq_khz = float(msg.freq_khz)
            except ValueError:
                await self.node_link.mark_policy_drop(peer_name, "ingest_spots_invalid_freq")
                return
            source_node = normalize_call(msg.source_node) if msg.source_node else normalize_call(peer_name)
            if source_node == normalize_call(self.config.node.node_call):
                await self.node_link.mark_policy_drop(peer_name, "ingest_spots_loop")
                return
            epoch = self._pc61_epoch(msg)
            raw = "^".join(
                [
                    f"{freq_khz:.1f}",
                    dx_call,
                    str(epoch),
                    msg.info,
                    spotter,
                    "226",
                    "226",
                    source_node,
                ]
            )
            spot = Spot(
                freq_khz=freq_khz,
                dx_call=dx_call,
                epoch=epoch,
                info=msg.info,
                spotter=spotter,
                source_node=source_node,
                raw=raw,
            )
            inserted = await self.store.add_spot(spot)
            if inserted:
                await self.telnet.publish_spot(spot)
                await self._relay_spot_to_links(spot, exclude_peer=peer_name)
            return

        if frame.pc_type == "PC93":
            msg = typed if isinstance(typed, Pc93Message) else Pc93Message.from_fields(frame.payload_fields)
            text = (msg.text or "").strip()
            if not text:
                await self.node_link.mark_policy_drop(peer_name, "ingest_pc93_empty")
                return
            if f"[via:{self.config.node.node_call}]" in text:
                await self.node_link.mark_policy_drop(peer_name, "ingest_pc93_loop")
                return
            sender = normalize_call(msg.origin_call) if msg.origin_call else normalize_call(peer_name)
            if not is_valid_call(sender):
                sender = normalize_call(peer_name)
            category, scope, body = self._classify_pc93_bulletin(sender, text)
            if not await self._ingest_peer_enabled(peer_name, category):
                await self.node_link.mark_policy_drop(peer_name, f"ingest_{category}_disabled")
                return
            now = int(datetime.now(timezone.utc).timestamp())
            await self.store.add_bulletin(category, sender, scope, now, body)
            if category == "chat":
                await self.telnet.publish_chat(sender, body)
            else:
                await self.telnet.publish_bulletin(category, sender, scope, body)
            return

        if frame.pc_type == "PC10":
            msg = typed if isinstance(typed, Pc10Message) else Pc10Message.from_fields(frame.payload_fields)
            body = (msg.text or "").strip()
            if not body:
                await self.node_link.mark_policy_drop(peer_name, "ingest_pc10_empty")
                return
            if f"[via:{self.config.node.node_call}]" in body:
                await self.node_link.mark_policy_drop(peer_name, "ingest_pc10_loop")
                return
            if not await self._ingest_peer_enabled(peer_name, "chat"):
                await self.node_link.mark_policy_drop(peer_name, "ingest_talk_disabled")
                return
            sender = normalize_call(msg.from_call) if msg.from_call else normalize_call(peer_name)
            if not is_valid_call(sender):
                sender = normalize_call(peer_name)
            recipient = normalize_call(msg.user2 or msg.user1)
            if not is_valid_call(recipient):
                await self.node_link.mark_policy_drop(peer_name, "ingest_pc10_invalid_recipient")
                return
            delivered = await self.telnet.publish_talk(recipient, sender, body)
            if delivered <= 0:
                await self.node_link.mark_policy_drop(peer_name, "ingest_pc10_offline")
            return

        if frame.pc_type == "PC28":
            msg = typed if isinstance(typed, Pc28Message) else Pc28Message.from_fields(frame.payload_fields)
            target_node = normalize_call(msg.to_node)
            local_node = normalize_call(self.config.node.node_call)
            if target_node and target_node != local_node:
                await self.node_link.mark_policy_drop(peer_name, "ingest_pc28_wrong_node")
                return
            stream = self._next_mail_stream()
            key = (normalize_call(msg.from_node or peer_name), stream)
            self._inbound_mail[key] = {
                "peer": peer_name,
                "from_node": normalize_call(msg.from_node or peer_name),
                "to_call": normalize_call(msg.to_call),
                "from_call": normalize_call(msg.from_call),
                "subject": (msg.subject or "").strip(),
                "origin": normalize_call(msg.origin or msg.from_node or peer_name),
                "lines": [],
                "count": 0,
                "linesreq": max(1, int((msg.line_count or "5").strip() or "5")),
            }
            await self.node_link.send(
                peer_name,
                WirePcFrame(
                    "PC30",
                    Pc30Message(
                        to_node=normalize_call(msg.from_node or peer_name),
                        from_node=local_node,
                        stream=stream,
                        trailer="",
                    ).to_fields(),
                ),
            )
            return

        if frame.pc_type == "PC29":
            msg = typed if isinstance(typed, Pc29Message) else Pc29Message.from_fields(frame.payload_fields)
            key = (normalize_call(msg.from_node or peer_name), (msg.stream or "").strip())
            state = self._inbound_mail.get(key)
            if state is None:
                await self.node_link.mark_policy_drop(peer_name, "ingest_pc29_unknown_stream")
                return
            lines = state.setdefault("lines", [])
            if isinstance(lines, list):
                lines.append((msg.text or "").replace("%5E", "^").strip())
            count = int(state.get("count", 0)) + 1
            state["count"] = count
            if count >= max(1, int(state.get("linesreq", 5))):
                await self.node_link.send(
                    peer_name,
                    WirePcFrame(
                        "PC31",
                        Pc31Message(
                            to_node=normalize_call(msg.from_node or peer_name),
                            from_node=normalize_call(self.config.node.node_call),
                            stream=(msg.stream or "").strip(),
                            trailer="",
                        ).to_fields(),
                    ),
                )
                state["count"] = 0
            return

        if frame.pc_type == "PC30":
            msg = typed if isinstance(typed, Pc30Message) else Pc30Message.from_fields(frame.payload_fields)
            await self._handle_mail_ack_subject(peer_name, msg)
            return

        if frame.pc_type == "PC31":
            msg = typed if isinstance(typed, Pc31Message) else Pc31Message.from_fields(frame.payload_fields)
            await self._handle_mail_ack_text(peer_name, msg)
            return

        if frame.pc_type == "PC32":
            msg = typed if isinstance(typed, Pc32Message) else Pc32Message.from_fields(frame.payload_fields)
            key = (normalize_call(msg.from_node or peer_name), (msg.stream or "").strip())
            state = self._inbound_mail.pop(key, None)
            if state is None:
                await self.node_link.mark_policy_drop(peer_name, "ingest_pc32_unknown_stream")
                return
            to_call = str(state.get("to_call") or "").strip().upper()
            from_call = str(state.get("from_call") or "").strip().upper()
            body_lines = [str(line) for line in state.get("lines", []) if str(line).strip()]
            subject = str(state.get("subject") or "").strip()
            body = "\n".join(body_lines) if body_lines else ""
            if subject and body:
                body = f"Subject: {subject}\n{body}"
            elif subject:
                body = f"Subject: {subject}"
            now = int(datetime.now(timezone.utc).timestamp())
            msg_id = await self.store.add_message(
                sender=from_call or normalize_call(peer_name),
                recipient=to_call,
                epoch=now,
                body=body,
                parent_id=None,
                origin_node=str(state.get("origin") or normalize_call(peer_name)),
                route_node="",
                delivery_state="delivered",
                delivered_epoch=now,
            )
            await self.telnet.publish_message(to_call, from_call or normalize_call(peer_name), body, msg_id)
            await self.node_link.send(
                peer_name,
                WirePcFrame(
                    "PC33",
                    Pc33Message(
                        to_node=normalize_call(msg.from_node or peer_name),
                        from_node=normalize_call(self.config.node.node_call),
                        stream=(msg.stream or "").strip(),
                        trailer="",
                    ).to_fields(),
                ),
            )
            return

        if frame.pc_type == "PC33":
            msg = typed if isinstance(typed, Pc33Message) else Pc33Message.from_fields(frame.payload_fields)
            await self._handle_mail_ack_complete(peer_name, msg)
            return

        if frame.pc_type == "PC12":
            msg = typed if isinstance(typed, Pc12Message) else Pc12Message.from_fields(frame.payload_fields)
            body = (msg.text or "").strip()
            if not body:
                await self.node_link.mark_policy_drop(peer_name, "ingest_pc12_empty")
                return
            if f"[via:{self.config.node.node_call}]" in body:
                await self.node_link.mark_policy_drop(peer_name, "ingest_pc12_loop")
                return
            category = "wx" if (msg.wx_flag or "").strip() == "1" else "announce"
            scope = "SYSOP" if (msg.sysop_flag or "").strip() == "*" else "FULL"
            if not await self._ingest_peer_enabled(peer_name, category):
                await self.node_link.mark_policy_drop(peer_name, f"ingest_{category}_disabled")
                return
            sender = normalize_call(msg.from_call) if msg.from_call else normalize_call(peer_name)
            if not is_valid_call(sender):
                sender = normalize_call(peer_name)
            now = int(datetime.now(timezone.utc).timestamp())
            await self.store.add_bulletin(category, sender, scope, now, body)
            await self.telnet.publish_bulletin(category, sender, scope, body)

    async def _routepc19_enabled(self, call: str) -> bool:
        v = (await self.store.get_user_pref(call, "routepc19") or "").strip().lower()
        if not v:
            return True
        return v in {"1", "on", "yes", "true"}

    async def _relay_category_enabled(self, sender: str, category: str) -> bool:
        key = f"relay.{category.lower()}"
        v = await self.store.get_user_pref(sender, key)
        if v is None:
            return True
        vv = v.strip().lower()
        return vv in {"1", "on", "yes", "true"}

    async def _ingest_peer_enabled(self, peer: str, category: str) -> bool:
        pl = peer.lower()
        key_cat = f"ingest.peer.{pl}.{category.lower()}"
        v_cat = await self.store.get_user_pref(self.config.node.node_call, key_cat)
        if v_cat is not None:
            return v_cat.strip().lower() in {"1", "on", "yes", "true"}
        key_all = f"ingest.peer.{pl}"
        v_all = await self.store.get_user_pref(self.config.node.node_call, key_all)
        if v_all is not None:
            return v_all.strip().lower() in {"1", "on", "yes", "true"}
        return True

    async def _relay_peer_enabled(self, sender: str, peer: str, category: str) -> bool:
        key_cat = f"relay.peer.{peer.lower()}.{category.lower()}"
        v_cat = await self.store.get_user_pref(sender, key_cat)
        if v_cat is not None:
            return v_cat.strip().lower() in {"1", "on", "yes", "true"}
        key_all = f"relay.peer.{peer.lower()}"
        v_all = await self.store.get_user_pref(sender, key_all)
        if v_all is not None:
            return v_all.strip().lower() in {"1", "on", "yes", "true"}
        return True

    def _route_peer_matches_expr(self, peer: str, expr: str) -> bool:
        text = (expr or "").strip().lower()
        if not text:
            return False
        p = peer.lower()
        if text.startswith("by "):
            pat = text[3:].strip()
            if not pat:
                return False
            if "*" in pat or "?" in pat:
                return fnmatch.fnmatchcase(p, pat)
            return p.startswith(pat)
        if text.startswith("peer "):
            pat = text[5:].strip()
            if not pat:
                return False
            if "*" in pat or "?" in pat:
                return fnmatch.fnmatchcase(p, pat)
            return p.startswith(pat)
        if "*" in text or "?" in text:
            return fnmatch.fnmatchcase(p, text)
        return text in p

    async def _route_filter_allows_peer(self, sender: str, peer: str) -> bool:
        rows = await self.store.list_filter_rules(sender)
        accepts: list[tuple[int, str]] = []
        rejects: list[tuple[int, str]] = []
        for r in rows:
            if str(r["family"]).strip().lower() != "route":
                continue
            act = str(r["action"]).strip().lower()
            expr = str(r["expr"] or "")
            slot = int(r["slot"])
            if act == "accept":
                accepts.append((slot, expr))
            elif act == "reject":
                rejects.append((slot, expr))
        matches: list[tuple[int, str]] = []
        for slot, expr in accepts:
            if self._route_peer_matches_expr(peer, expr):
                matches.append((slot, "accept"))
        for slot, expr in rejects:
            if self._route_peer_matches_expr(peer, expr):
                matches.append((slot, "reject"))
        if matches:
            matches.sort(key=lambda x: (x[0], 0 if x[1] == "reject" else 1))
            return matches[0][1] == "accept"
        if accepts:
            return False
        return True

    async def _broadcast_with_policy(self, sender: str, category: str, frame: WirePcFrame) -> int:
        names = await self.node_link.peer_names()
        sent = 0
        for name in names:
            if not await self._route_filter_allows_peer(sender, name):
                await self.node_link.mark_policy_drop(name, "route_filter")
                continue
            if not await self._relay_peer_enabled(sender, name, category):
                await self.node_link.mark_policy_drop(name, f"relay_peer_{category}_disabled")
                continue
            try:
                await self.node_link.send(name, frame)
                sent += 1
            except Exception:
                LOG.exception("relay send failed peer=%s category=%s", name, category)
        return sent

    async def _relay_chat_to_links(self, sender: str, text: str) -> None:
        if not await self._routepc19_enabled(sender):
            return
        if not await self._relay_category_enabled(sender, "chat"):
            return
        # Local origin marker to help downstream loop suppression.
        payload_text = f"{text} [via:{self.config.node.node_call}]"
        msg = Pc93Message(
            node_call=self.config.node.node_call,
            metric="0",
            star1="*",
            origin_call=sender,
            star2="*",
            text=payload_text,
            extra="",
            ip="127.0.0.1",
            hops_token="H1",
            trailer="",
        )
        frame = WirePcFrame("PC93", msg.to_fields())
        await self._broadcast_with_policy(sender, "chat", frame)

    async def _relay_bulletin_to_links(self, category: str, sender: str, scope: str, text: str) -> None:
        if not await self._routepc19_enabled(sender):
            return
        if not await self._relay_category_enabled(sender, category):
            return
        prefix = category.upper()
        body = f"[{prefix}/{scope.upper()}] {text} [via:{self.config.node.node_call}]"
        msg = Pc93Message(
            node_call=self.config.node.node_call,
            metric="0",
            star1="*",
            origin_call=sender,
            star2="*",
            text=body,
            extra="",
            ip="127.0.0.1",
            hops_token="H1",
            trailer="",
        )
        frame = WirePcFrame("PC93", msg.to_fields())
        await self._broadcast_with_policy(sender, category, frame)

    def _next_mail_stream(self) -> str:
        self._mail_stream_seq += 1
        return str(self._mail_stream_seq)

    async def _reset_mail_transport_state(self, peer_name: str, error_text: str = "") -> None:
        route_node = normalize_call(peer_name)
        queue = self._outbound_mail_pending_header.pop(route_node, [])
        inflight_keys = [key for key in self._outbound_mail if key[0] == route_node]
        pending_ids: set[int] = set()
        for state in queue:
            try:
                pending_ids.add(int(state["message_id"]))
            except Exception:
                pass
        for key in inflight_keys:
            state = self._outbound_mail.pop(key, None)
            if state is None:
                continue
            try:
                pending_ids.add(int(state["message_id"]))
            except Exception:
                pass
        for message_id in sorted(pending_ids):
            await self.store.set_message_delivery(
                message_id,
                "pending",
                route_node=route_node,
                error_text=error_text,
            )

    async def _start_outbound_mail(self, peer_name: str, row: object) -> None:
        route_node = normalize_call(peer_name)
        body = str(row["body"] or "")
        body_lines = [line.strip() for line in body.splitlines() if line.strip()]
        if not body_lines:
            body_lines = [body.strip() or " "]
        subject = " "
        if body_lines and body_lines[0].startswith("Subject:"):
            subject = body_lines.pop(0)[len("Subject:") :].strip() or " "
        state = {
            "message_id": int(row["id"]),
            "peer": route_node,
            "sender": normalize_call(str(row["sender"] or "")),
            "recipient": normalize_call(str(row["recipient"] or "")),
            "subject": subject,
            "lines": body_lines,
            "index": 0,
            "tranche_size": 5,
        }
        queue = self._outbound_mail_pending_header.setdefault(route_node, [])
        queue.append(state)
        if len(queue) > 1 or any(key_peer == route_node for key_peer, _ in self._outbound_mail):
            return
        now = datetime.now(timezone.utc)
        await self.node_link.send(
            route_node,
            WirePcFrame(
                "PC28",
                Pc28Message(
                    to_node=route_node,
                    from_node=normalize_call(self.config.node.node_call),
                    to_call=str(state["recipient"]),
                    from_call=str(state["sender"]),
                    date_token=now.strftime("%d-%b-%Y"),
                    time_token=now.strftime("%H%MZ"),
                    private_flag="1",
                    subject=subject,
                    placeholder1=" ",
                    line_count="5",
                    rr_flag="0",
                    placeholder2=" ",
                    origin=normalize_call(self.config.node.node_call),
                    trailer="~",
                ).to_fields(),
            ),
        )

    async def _handle_mail_ack_subject(self, peer_name: str, msg: Pc30Message) -> None:
        route_node = normalize_call(peer_name)
        queue = self._outbound_mail_pending_header.get(route_node, [])
        if not queue:
            return
        state = queue.pop(0)
        stream = (msg.stream or "").strip()
        state["stream"] = stream
        self._outbound_mail[(route_node, stream)] = state
        await self._send_mail_tranche(peer_name, state)

    async def _handle_mail_ack_text(self, peer_name: str, msg: Pc31Message) -> None:
        key = (normalize_call(peer_name), (msg.stream or "").strip())
        state = self._outbound_mail.get(key)
        if state is None:
            return
        await self._send_mail_tranche(peer_name, state)

    async def _send_mail_tranche(self, peer_name: str, state: dict[str, object]) -> None:
        lines = state["lines"] if isinstance(state.get("lines"), list) else []
        stream = str(state["stream"])
        index = int(state.get("index", 0))
        tranche_size = max(1, int(state.get("tranche_size", 5)))
        if index >= len(lines):
            await self.node_link.send(
                peer_name,
                WirePcFrame(
                    "PC32",
                    Pc32Message(
                        to_node=normalize_call(peer_name),
                        from_node=normalize_call(self.config.node.node_call),
                        stream=stream,
                        trailer="",
                    ).to_fields(),
                ),
            )
            return
        end = min(len(lines), index + tranche_size)
        for line in lines[index:end]:
            text = str(line).replace("^", "%5E")
            await self.node_link.send(
                peer_name,
                WirePcFrame(
                    "PC29",
                    Pc29Message(
                        to_node=normalize_call(peer_name),
                        from_node=normalize_call(self.config.node.node_call),
                        stream=stream,
                        text=text,
                        trailer="~",
                    ).to_fields(),
                ),
            )
        state["index"] = end
        if end >= len(lines):
            await self.node_link.send(
                peer_name,
                WirePcFrame(
                    "PC32",
                    Pc32Message(
                        to_node=normalize_call(peer_name),
                        from_node=normalize_call(self.config.node.node_call),
                        stream=stream,
                        trailer="",
                    ).to_fields(),
                ),
            )

    async def _handle_mail_ack_complete(self, peer_name: str, msg: Pc33Message) -> None:
        key = (normalize_call(peer_name), (msg.stream or "").strip())
        state = self._outbound_mail.pop(key, None)
        if state is None:
            return
        now = int(datetime.now(timezone.utc).timestamp())
        await self.store.set_message_delivery(
            int(state["message_id"]),
            "routed",
            delivered_epoch=now,
            route_node=normalize_call(peer_name),
            error_text="",
        )
        await self._flush_pending_messages_for_peer(peer_name)

    async def _relay_message_to_links(self, sender: str, recipient: str, body: str, message_id: int, parent_id: int | None) -> None:
        del sender, recipient, body, parent_id
        row = await self.store.get_message(message_id)
        if row is None:
            return
        route_node = str(row["route_node"] or "").strip().upper()
        if not route_node:
            return
        try:
            await self._start_outbound_mail(route_node, row)
        except Exception as exc:
            await self.store.set_message_delivery(message_id, "pending", route_node=route_node, error_text=str(exc))
            LOG.info("queued cluster mail id=%s route=%s error=%s", message_id, route_node, exc)

    async def _flush_pending_messages_for_peer(self, peer_name: str) -> None:
        route_node = normalize_call(peer_name)
        if self._outbound_mail_pending_header.get(route_node) or any(key_peer == route_node for key_peer, _ in self._outbound_mail):
            return
        rows = await self.store.list_pending_messages_for_route(peer_name, limit=200)
        for row in rows:
            try:
                await self._start_outbound_mail(peer_name, row)
            except Exception as exc:
                await self.store.set_message_delivery(int(row["id"]), "pending", route_node=peer_name, error_text=str(exc))
                LOG.info("pending cluster mail still queued id=%s peer=%s error=%s", int(row["id"]), peer_name, exc)
                return

    async def _relay_spot_to_links(self, spot: Spot, exclude_peer: str | None = None) -> None:
        sender = normalize_call(spot.spotter)
        if not await self._routepc19_enabled(sender):
            return
        if not await self._relay_category_enabled(sender, "spots"):
            return
        dt = datetime.fromtimestamp(spot.epoch, tz=timezone.utc)
        source_node = normalize_call(spot.source_node) if spot.source_node else normalize_call(self.config.node.node_call)
        pc61 = Pc61Message(
            freq_khz=f"{spot.freq_khz:.1f}",
            dx_call=spot.dx_call,
            date_token=dt.strftime("%-d-%b-%Y"),
            time_token=dt.strftime("%H%MZ"),
            info=spot.info,
            spotter=sender,
            source_node=source_node,
            ip="127.0.0.1",
            hops_token="H1",
            trailer="~",
        )
        pc61_frame = WirePcFrame("PC61", pc61.to_fields())
        pc11_frame = WirePcFrame(
            "PC11",
            [
                f"{spot.freq_khz:.1f}",
                spot.dx_call,
                dt.strftime("%-d-%b-%Y"),
                dt.strftime("%H%MZ"),
                spot.info or " ",
                sender,
                source_node,
                "H1",
                "~",
            ],
        )
        names = await self.node_link.peer_names()
        for name in names:
            if exclude_peer and normalize_call(name) == normalize_call(exclude_peer):
                continue
            if not await self._route_filter_allows_peer(sender, name):
                await self.node_link.mark_policy_drop(name, "route_filter")
                continue
            if not await self._relay_peer_enabled(sender, name, "spots"):
                await self.node_link.mark_policy_drop(name, "relay_peer_spots_disabled")
                continue
            frame = pc11_frame if name in self._legacy_dxspider_peers else pc61_frame
            try:
                await self.node_link.send(name, frame)
            except Exception:
                LOG.exception("relay send failed peer=%s category=spots", name)

    async def _node_ingest_loop(self) -> None:
        while not self._node_ingest_stop.is_set():
            try:
                item = await self.node_link.recv(timeout=1.0)
            except Exception:
                LOG.exception("node-link recv failed")
                continue
            if item is None:
                continue
            peer_name, frame, typed = item
            try:
                await self._handle_node_link_item(peer_name, frame, typed)
            except Exception:
                LOG.exception("node-link ingest failed peer=%s pc=%s", peer_name, frame.pc_type)

    async def _trace_protocol_line(self, peer_name: str, direction: str, text: str) -> None:
        ts = datetime.now(timezone.utc)
        line = f"{ts.isoformat()} {peer_name} {direction} {text}\n"
        base = Path(self.config.store.sqlite_path).resolve().parent.parent / "logs" / "proto" / ts.strftime("%Y")
        path = base / f"{ts.timetuple().tm_yday:03d}.log"

        async with self._proto_trace_lock:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as f:
                f.write(line)


async def serve_forever(config: AppConfig, config_path: str | None = None) -> None:
    app = ClusterApp(config, config_path=config_path)
    await app.start()
    logging.getLogger(__name__).info("pyCluster started")

    stop_event = asyncio.Event()

    def _trigger_stop() -> None:
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _trigger_stop)
        except NotImplementedError:
            pass

    try:
        await stop_event.wait()
    finally:
        await app.stop()
        current = asyncio.current_task()
        pending = [t for t in asyncio.all_tasks() if t is not current and not t.done()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)


async def serve_core_forever(config: AppConfig, config_path: str | None = None) -> None:
    app = ClusterApp(config, config_path=config_path)
    await app.start(with_public_web=False)
    logging.getLogger(__name__).info("pyCluster core started")

    stop_event = asyncio.Event()

    def _trigger_stop() -> None:
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _trigger_stop)
        except NotImplementedError:
            pass

    try:
        await stop_event.wait()
    finally:
        await app.stop()
        current = asyncio.current_task()
        pending = [t for t in asyncio.all_tasks() if t is not current and not t.done()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)


async def serve_public_forever(config: AppConfig) -> None:
    store = SpotStore(config.store.sqlite_path)
    public_web = PublicWebServer(config=config, store=store, started_at=datetime.now(timezone.utc))
    await public_web.start()
    logging.getLogger(__name__).info("pyCluster public web started")

    stop_event = asyncio.Event()

    def _trigger_stop() -> None:
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _trigger_stop)
        except NotImplementedError:
            pass

    try:
        await stop_event.wait()
    finally:
        await public_web.stop()
        await store.close()
        current = asyncio.current_task()
        pending = [t for t in asyncio.all_tasks() if t is not current and not t.done()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
