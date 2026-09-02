from __future__ import annotations

import asyncio
from collections import deque
from datetime import datetime, timezone
import fnmatch
import hashlib
import json
import logging
from pathlib import Path
import re
import signal
import socket
import time
import uuid
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from . import __version__
from .config import AppConfig
from .access_policy import CLUSTER_NODE_FAMILIES
from .ctydat import is_loaded as cty_loaded, lookup as cty_lookup
from .wpxloc import is_loaded as wpx_loaded, lookup as wpx_lookup
from .datafiles import describe_cty_file, describe_data_file, describe_wpxloc_file
from .geomag import WcyReading, WwvReading, canonicalize_wcy_text, canonicalize_wwv_text, parse_wcy_text, parse_wwv_text
from .maidenhead import extract_locator
from .live_spots import encode_rbn_spot, rbn_socket_address
from .models import Spot, is_plausible_spot_call, is_plausible_spotter_call, is_valid_call, normalize_call
from .netutil import detected_public_ip_addresses, valid_global_ip
from .node_link import NodeLinkEngine
from .pathmeta import describe_transport_dsn
from .peer_profiles import normalize_profile
from .protocol import Pc10Message, Pc11Message, Pc12Message, Pc18Message, Pc23Message, Pc24Message, Pc28Message, Pc29Message, Pc30Message, Pc31Message, Pc32Message, Pc33Message, Pc50Message, Pc51Message, Pc61Message, Pc73Message, Pc93Message, WirePcFrame, parse_wire_pc_frame, serialize_wire_protocol_frame
from .py_protocol import PY_CAPABILITIES, PY_CLOCK_TYPE, PY_DATASETS_TYPE, PY_ERROR_TYPE, PY_FRAME_CAPABILITIES, PY_HEALTH_TYPE, PY_HELLO_TYPE, PY_NODEINFO_TYPE, PY_NOTICE_TYPE, PY_POLICY_TYPE, PY_RBN_STATUS_TYPE, PY_REQUEST_TYPE, PY_TOPOLOGY_DIGEST_TYPE, PY_TOPOLOGY_RECORDS_TYPE, PyClockMessage, PyDatasetsMessage, PyErrorMessage, PyHealthMessage, PyHelloMessage, PyNodeInfoMessage, PyNoticeMessage, PyPolicyMessage, PyRbnStatusMessage, PyTopologyDigestEntry, PyTopologyDigestMessage, PyTopologyRecord, PyTopologyRecordsMessage, PyTopologyRequestMessage
from .rbn import is_rbn_spot, parse_rbn_dx_line
from .shdx import BAND_RANGES
from .spot_filters import SpotFilterEntry, evaluate_spot_entries
from .store import SpotStore
from .strings import StringCatalog
from .telnet_server import TelnetClusterServer
from .transports import DxSpiderInboundConnection, pycluster_pc18
from .public_web import PublicWebServer
from .web_admin import WebAdminServer

LOG = logging.getLogger(__name__)
_BULLETIN_DEDUPE_WINDOW_SECONDS = 900
_TALK_DEDUPE_WINDOW_SECONDS = 30
_PC93_PREFIX_RE = re.compile(r"^\[(ANNOUNCE|WCY|WWV|WX)/(LOCAL|FULL|SYSOP)\]\s*(.*)$", re.IGNORECASE)
_VIA_SUFFIX_RE = re.compile(r"\s*\[via:[^\]]+\]\s*$", re.IGNORECASE)
_TRUSTED_WCY_SOURCES = frozenset({"DK0WCY"})
_DXSPIDER_PC19_VERSION = "5457"
_PEER_PREF_PREFIX = "peer.outbound."
_RECONNECT_BASE_SECS = 5
_RECONNECT_MAX_SECS = 300
_PEER_HEARTBEAT_SECS = 60
_RBN_BATCH_SIZE = 100
_RBN_IDLE_FLUSH_SECONDS = 1.0
_RBN_IDLE_DISCONNECT_SECONDS = 300.0
_PROTO_FLAP_KEYS = {
    "pc18.software",
    "pc18.proto",
    "pc18.family",
    "pc18.summary",
}


class ClusterApp:
    @staticmethod
    def _is_trusted_wcy_source(sender: str) -> bool:
        base_call = normalize_call(sender).split("-", 1)[0]
        return base_call in _TRUSTED_WCY_SOURCES

    @staticmethod
    def _split_peer_password(dsn: str) -> tuple[str, str]:
        raw = str(dsn or "").strip()
        if not raw:
            return "", ""
        parsed = urlparse(raw)
        params = parse_qs(parsed.query, keep_blank_values=True)
        password = ""
        if "password" in params and params["password"]:
            password = str(params["password"][0] or "")
            params.pop("password", None)
        clean_query = urlencode([(k, v) for k, values in params.items() for v in values], doseq=True)
        clean = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, clean_query, parsed.fragment))
        return clean, password

    @staticmethod
    def _merge_peer_password(dsn: str, password: str) -> str:
        raw = str(dsn or "").strip()
        if not raw:
            return raw
        parsed = urlparse(raw)
        params = parse_qs(parsed.query, keep_blank_values=True)
        secret = str(password or "").strip()
        if secret:
            params["password"] = [secret]
        else:
            params.pop("password", None)
        merged_query = urlencode([(k, v) for k, values in params.items() for v in values], doseq=True)
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, merged_query, parsed.fragment))

    def __init__(self, config: AppConfig, config_path: str | None = None) -> None:
        self.config = config
        self.started_at = datetime.now(timezone.utc)
        self.store = SpotStore(config.store.sqlite_path)
        strings_path = str(Path(config_path).with_name("strings.toml")) if config_path else None
        self._strings = StringCatalog(strings_path)
        self._runtime_public_ip_address = ""
        self._runtime_public_ipv6_address = ""
        public_ips = self._runtime_public_ip_addresses()
        self.node_link = NodeLinkEngine(
            public_ip_address=public_ips["ipv4"],
            public_ipv6_address=public_ips["ipv6"],
            py_protocol_enabled=config.py_protocol.enabled,
            max_py_frame_bytes=config.py_protocol.max_frame_bytes,
            max_py_bytes_per_minute=config.py_protocol.max_bytes_per_minute,
        )
        self.node_link.set_trace_hook(self._trace_protocol_line)
        self._legacy_dxspider_peers: set[str] = set()
        self._pycluster_identified_peers: set[str] = set()
        self._py_hello_sent: set[str] = set()
        self._py_remote_capabilities: dict[str, frozenset[str]] = {}
        self._py_negotiated_capabilities: dict[str, frozenset[str]] = {}
        self._py_nodeinfo_sent: set[str] = set()
        self._py_topology_digest_sent: set[str] = set()
        self._py_topology_digest_epoch: dict[str, int] = {}
        self._py_topology_snapshots: dict[str, dict[str, object]] = {}
        self._py_metadata_epoch: dict[str, int] = {}
        self._mail_stream_seq = 0
        self._outbound_mail: dict[tuple[str, str], dict[str, object]] = {}
        self._outbound_mail_pending_header: dict[str, list[dict[str, object]]] = {}
        self._inbound_mail: dict[tuple[str, str], dict[str, object]] = {}
        self._recent_talk_ingest: dict[tuple[str, str, str], float] = {}
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
            link_save_peer_fn=self.save_peer_target,
            link_delete_peer_fn=self.delete_peer_target,
            component_status_fn=self.component_status,
            component_restart_fn=self.restart_component,
            on_chat_fn=self._relay_chat_to_links,
            on_bulletin_fn=self._relay_bulletin_to_links,
            on_talk_fn=self._relay_talk_to_links,
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
            link_delete_peer_fn=self.delete_peer_target,
            publish_spot_fn=self.telnet.publish_spot,
            relay_spot_fn=self._relay_spot_to_links,
            publish_chat_fn=self.telnet.publish_chat,
            relay_chat_fn=self._relay_chat_to_links,
            publish_bulletin_fn=self.telnet.publish_bulletin,
            relay_bulletin_fn=self._relay_bulletin_to_links,
            telnet_rebind_fn=self.telnet.rebind_listeners,
            event_log_fn=self.telnet.record_event,
            audit_rows_fn=self.telnet.audit_rows,
            rbn_status_fn=self.rbn_feed_status,
            rbn_reconfigure_fn=self.reconfigure_rbn_feed,
            config_updated_fn=self._apply_runtime_config,
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
            strings_path=strings_path,
            config_path=config_path,
        )
        self._node_ingest_task: asyncio.Task[None] | None = None
        self._peer_reconnect_task: asyncio.Task[None] | None = None
        self._peer_heartbeat_task: asyncio.Task[None] | None = None
        self._rbn_feed_tasks: dict[str, asyncio.Task[None]] = {}
        self._rbn_feed_statuses: dict[str, dict[str, object]] = {}
        self._rbn_recent_spot_epochs: deque[int] = deque(maxlen=10000)
        self._rbn_seen_order: deque[tuple[int, tuple[object, ...]]] = deque(maxlen=50000)
        self._rbn_seen: set[tuple[object, ...]] = set()
        self._rbn_web_socket: socket.socket | None = None
        self._rbn_feed_status: dict[str, object] = {
            "state": "disabled" if not config.rbn.enabled else "stopped",
            "last_connected_at": "",
            "last_line_at": "",
            "last_spot_at": "",
            "last_error": "",
            "last_error_at": "",
            "last_spot": "",
        }
        self._node_ingest_stop = asyncio.Event()
        self._proto_trace_lock = asyncio.Lock()
        self._proto_trace_level = "full"
        self._proto_trace_level_checked_monotonic = 0.0
        self._public_web_started = False

    def _apply_runtime_config(self) -> None:
        self._proto_trace_level_checked_monotonic = 0.0
        public_ips = self._runtime_public_ip_addresses()
        self.node_link.set_public_ip_address(public_ips["ipv4"], public_ips["ipv6"])
        self.node_link.set_py_protocol_policy(
            self.config.py_protocol.enabled,
            self.config.py_protocol.max_frame_bytes,
            self.config.py_protocol.max_bytes_per_minute,
        )
        if not self.config.py_protocol.enabled:
            peer_keys = set(self._pycluster_identified_peers).union(self._py_hello_sent)
            peer_keys.update(self._py_remote_capabilities)
            peer_keys.update(self._py_negotiated_capabilities)
            for peer_key in peer_keys:
                self._reset_py_peer_state(peer_key)

    def _runtime_public_ip_addresses(self) -> dict[str, str]:
        configured_v4 = valid_global_ip(self.config.node.public_ip_address, version=4)
        configured_v6 = valid_global_ip(self.config.node.public_ipv6_address, version=6)
        detected: dict[str, str] = {"ipv4": "", "ipv6": ""}
        if not configured_v4 or not configured_v6:
            detected = detected_public_ip_addresses()
        self._runtime_public_ip_address = configured_v4 or detected.get("ipv4", "")
        self._runtime_public_ipv6_address = configured_v6 or detected.get("ipv6", "")
        return {"ipv4": self._runtime_public_ip_address, "ipv6": self._runtime_public_ipv6_address}

    def _string(self, key: str, default: str) -> str:
        return self._strings.get(key, default)

    def _render_string(self, key: str, default: str, **values: object) -> str:
        return self._strings.render(key, default, **values)

    def _utc_status_time(self) -> str:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    def _set_rbn_feed_status(self, feed_key: str | None = None, **updates: object) -> None:
        self._rbn_feed_status.update(updates)
        if feed_key:
            feed_status = self._rbn_feed_statuses.setdefault(feed_key, {})
            feed_status.update(updates)

    def rbn_feed_status(self) -> dict[str, object]:
        status = dict(self._rbn_feed_status)
        status.update(
            {
                "enabled": bool(self.config.rbn.enabled),
                "running": any(not task.done() for task in self._rbn_feed_tasks.values()),
                "host": str(self.config.rbn.host or ""),
                "port": int(self.config.rbn.port),
                "ports": self._rbn_feed_ports(),
                "feeds": self._rbn_feed_status_payload(),
            }
        )
        return status

    def _rbn_feed_ports(self) -> tuple[int, ...]:
        return tuple(self.config.rbn.ports or (int(self.config.rbn.port),))

    def _rbn_feed_configs(self) -> tuple[dict[str, object], ...]:
        if self.config.rbn.feeds:
            return tuple(
                {
                    "key": f"{feed.name or feed.host}:{int(feed.port)}",
                    "name": feed.name or f"{feed.host}:{int(feed.port)}",
                    "host": feed.host,
                    "port": int(feed.port),
                }
                for feed in self.config.rbn.feeds
            )
        host = str(self.config.rbn.host or "")
        return tuple(
            {
                "key": f"{host}:{port}",
                "name": f"{host}:{port}" if host else str(port),
                "host": host,
                "port": port,
            }
            for port in self._rbn_feed_ports()
        )

    def _rbn_feed_status_payload(self) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        for feed in self._rbn_feed_configs():
            key = str(feed["key"])
            row = {"name": feed["name"], "host": feed["host"], "port": feed["port"], "state": "stopped"}
            row.update(self._rbn_feed_statuses.get(key, {}))
            rows.append(row)
        return rows

    def _start_rbn_feed_tasks(self) -> None:
        self._rbn_feed_statuses = {}
        if not self.config.rbn.enabled:
            self._set_rbn_feed_status(state="disabled", last_error="")
            return
        self._set_rbn_feed_status(state="starting", last_error="")
        self._rbn_feed_tasks = {
            str(feed["key"]): asyncio.create_task(self._rbn_feed_loop(feed), name=f"rbn-feed-{feed['name']}")
            for feed in self._rbn_feed_configs()
        }

    async def _stop_rbn_feed_tasks(self, *, state: str = "stopped") -> None:
        if self._rbn_feed_tasks:
            tasks = list(self._rbn_feed_tasks.values())
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._rbn_feed_tasks = {}
        self._set_rbn_feed_status(state=state)

    async def reconfigure_rbn_feed(self) -> None:
        await self._stop_rbn_feed_tasks(state="disabled" if not self.config.rbn.enabled else "stopped")
        self._start_rbn_feed_tasks()

    def _spot_review_reasons(self, dx_call: str, spotter: str) -> list[str]:
        reasons: list[str] = []
        cty = describe_cty_file(self.config.public_web.cty_dat_path, loaded=cty_loaded())
        wpx = describe_wpxloc_file(self.config.public_web.wpxloc_raw_path, loaded=wpx_loaded())
        geo_ready = cty.loaded or wpx.loaded
        if not geo_ready:
            return ["prefix_data_unavailable"]
        if cty.stale and not wpx.loaded:
            return ["prefix_data_stale"]
        if cty_lookup(dx_call) is None and wpx_lookup(dx_call) is None:
            reasons.append(f"dx_call:{dx_call}:unrecognized_prefix")
        if cty_lookup(spotter) is None and wpx_lookup(spotter) is None:
            reasons.append(f"spotter:{spotter}:unrecognized_prefix")
        return reasons

    async def _rbn_ingest_allowed_for_call(self, call: str) -> bool:
        if not self.config.rbn.enabled:
            return False
        target = normalize_call(call)
        base = target.split("-", 1)[0]
        candidates = (target, base) if base != target else (target,)
        for candidate in candidates:
            raw = await self.store.get_user_pref(candidate, "access.telnet.rbn")
            if raw is None or str(raw).strip() == "":
                continue
            return str(raw).strip().lower() in {"1", "on", "yes", "true"}
        return True

    @staticmethod
    def _parse_zone_spec(text: str, low: int, high: int) -> set[int]:
        zones: set[int] = set()
        for token in re.split(r"[,\s]+", str(text or "").strip()):
            if not token:
                continue
            m = re.fullmatch(r"(\d+)\s*-\s*(\d+)", token)
            if m:
                a, b = int(m.group(1)), int(m.group(2))
                if a > b:
                    a, b = b, a
                zones.update(z for z in range(a, b + 1) if low <= z <= high)
            elif token.isdigit():
                z = int(token)
                if low <= z <= high:
                    zones.add(z)
        return zones

    def _spot_matches_filter_expr(self, spot: Spot, expr: str) -> bool:
        text = str(expr or "").strip()
        if not text:
            return False
        low = text.lower()
        toks = low.split()
        if not toks:
            return False
        first = toks[0]
        rest = " ".join(toks[1:]).strip()
        dx_call = normalize_call(spot.dx_call)
        spotter = normalize_call(spot.spotter)
        info = str(spot.info or "")

        if first in {"all", "*"}:
            return True
        if first == "rbn":
            if not self._is_rbn_spot_obj(spot):
                return False
            return True if not rest else self._spot_matches_filter_expr(spot, rest)
        if first == "on" and rest:
            wanted = [tok.strip().lower() for tok in re.split(r"[,\s]+", rest) if tok.strip()]
            return any(
                (rng := BAND_RANGES.get(band)) is not None and rng[0] <= float(spot.freq_khz) <= rng[1]
                for band in wanted
            )
        if first == "by" and rest:
            pat = rest.upper()
            return fnmatch.fnmatchcase(spotter, pat) if any(ch in pat for ch in "*?") else spotter.startswith(pat)
        if first in {"dx", "call", "callsign"} and rest:
            pat = rest.upper()
            return fnmatch.fnmatchcase(dx_call, pat) if any(ch in pat for ch in "*?") else dx_call.startswith(pat)
        if first == "call_zone" and rest:
            ent = cty_lookup(dx_call) if cty_loaded() else None
            wanted = self._parse_zone_spec(rest, 1, 40)
            return bool(ent and wanted) and ent.cq_zone in wanted
        if first == "call_itu" and rest:
            ent = cty_lookup(dx_call) if cty_loaded() else None
            wanted = self._parse_zone_spec(rest, 1, 90)
            return bool(ent and wanted) and ent.itu_zone in wanted
        if first == "call_dxcc" and rest:
            ent = cty_lookup(dx_call) if cty_loaded() else None
            if not ent:
                return False
            wanted = [tok.strip().upper() for tok in re.split(r"[,\s]+", rest) if tok.strip()]
            ent_name = re.sub(r"[^A-Z0-9]+", "", ent.name.upper())
            ent_prefix = str(ent.prefix or "").strip().upper()
            return any(tok == ent_prefix or tok == ent_name or re.sub(r"[^A-Z0-9]+", "", tok) == ent_name for tok in wanted)
        if first in {"spotter_cont", "by_cont"} and rest:
            ent = cty_lookup(spotter) if cty_loaded() else None
            if ent is None and wpx_loaded():
                ent = wpx_lookup(spotter)
            wanted = {tok.strip().upper() for tok in re.split(r"[,\s]+", rest) if tok.strip()}
            return bool(ent and wanted) and ent.continent.upper() in wanted
        if first in {"spotter_zone", "by_zone"} and rest:
            ent = cty_lookup(spotter) if cty_loaded() else None
            if ent is None and wpx_loaded():
                ent = wpx_lookup(spotter)
            wanted = self._parse_zone_spec(rest, 1, 40)
            return bool(ent and wanted) and ent.cq_zone in wanted
        if first in {"spotter_itu", "by_itu"} and rest:
            ent = cty_lookup(spotter) if cty_loaded() else None
            if ent is None and wpx_loaded():
                ent = wpx_lookup(spotter)
            wanted = self._parse_zone_spec(rest, 1, 90)
            return bool(ent and wanted) and ent.itu_zone in wanted
        if first == "info" and rest:
            return rest in info.lower()
        return low in f"{spot.freq_khz:.1f} {dx_call} {spotter} {info}".lower()

    async def _spot_passes_ingest_filters(self, call: str, spot: Spot) -> bool:
        exact_call = normalize_call(call)
        base_call = exact_call.split("-", 1)[0]
        rows = await self.store.list_filter_rules(exact_call)
        if not rows and base_call != exact_call:
            rows = await self.store.list_filter_rules(base_call)

        entries = [
            SpotFilterEntry(
                family=str(row["family"] or "").strip().lower(),
                action=str(row["action"] or "").strip().lower(),
                slot=int(row["slot"] or 0),
                expr=str(row["expr"] or ""),
            )
            for row in rows
        ]
        return evaluate_spot_entries(
            entries,
            lambda expr: self._spot_matches_filter_expr(spot, expr),
            is_rbn=self._is_rbn_spot_obj(spot),
        )

    @staticmethod
    def _is_rbn_spot_obj(spot: Spot) -> bool:
        return is_rbn_spot(spot.dx_call, spot.spotter, f"{spot.info or ''} {spot.raw or ''}")

    @staticmethod
    def _is_rbn_peer_spot(dx_call: str, spotter: str, info: str, raw_fields: list[str] | None = None) -> bool:
        raw = "^".join(raw_fields or [])
        return is_rbn_spot(dx_call, spotter, f"{info or ''} {raw}")

    def _peer_pref_key(self, name: str, field: str) -> str:
        slug = re.sub(r"[^a-z0-9_.-]", "_", name.lower())
        return f"{_PEER_PREF_PREFIX}{slug}.{field}"

    def _peer_registry_profile(self, profile: str, dsn: str = "") -> str:
        p = str(profile or "").strip().lower()
        if p == "spider" or str(dsn or "").strip().lower().startswith(("dxspider://", "spidertelnet://")):
            return "dxspider"
        return normalize_profile(p)

    async def _record_outbound_peer_login(self, name: str, dsn: str, profile: str, epoch: int) -> None:
        peer_call = normalize_call(name)
        if not peer_call or not is_valid_call(peer_call):
            return
        family = self._peer_registry_profile(profile, dsn)
        transport, path_hint = describe_transport_dsn(dsn)
        path_parts = ["node-link", "outbound"]
        if transport:
            path_parts.append(transport)
        if path_hint:
            path_parts.append(path_hint)
        await self.store.upsert_user_registry(peer_call, epoch)
        await self.store.set_user_pref(peer_call, "node_family", family, epoch)
        await self.store.record_login(peer_call, epoch, " ".join(path_parts))

    async def _persist_peer_target(
        self,
        name: str,
        dsn: str,
        profile: str = "dxspider",
        reconnect: bool = True,
        password: str | None = "",
    ) -> None:
        now = int(datetime.now(timezone.utc).timestamp())
        p = profile.strip().lower() or "dxspider"
        clean_dsn, embedded_password = self._split_peer_password(dsn)
        if password is None:
            current_password = await self.store.get_user_pref(self.config.node.node_call, self._peer_pref_key(name, "password"))
            secret = str(embedded_password or current_password or "").strip()
        else:
            secret = str(password or embedded_password or "").strip()
        await self.store.set_user_pref(self.config.node.node_call, self._peer_pref_key(name, "name"), name, now)
        await self.store.set_user_pref(self.config.node.node_call, self._peer_pref_key(name, "dsn"), clean_dsn, now)
        await self.store.set_user_pref(self.config.node.node_call, self._peer_pref_key(name, "profile"), p, now)
        await self.store.set_user_pref(
            self.config.node.node_call,
            self._peer_pref_key(name, "reconnect"),
            "on" if reconnect else "off",
            now,
        )
        if secret:
            await self.store.set_user_pref(self.config.node.node_call, self._peer_pref_key(name, "password"), secret, now)
        else:
            await self.store.delete_user_pref(self.config.node.node_call, self._peer_pref_key(name, "password"))
        await self.store.delete_user_pref(self.config.node.node_call, self._peer_pref_key(name, "last_error"))
        await self.store.set_user_pref(self.config.node.node_call, self._peer_pref_key(name, "retry_count"), "0", now)
        await self.store.set_user_pref(self.config.node.node_call, self._peer_pref_key(name, "next_retry_epoch"), "0", now)
        peer_call = normalize_call(name)
        if peer_call and is_valid_call(peer_call):
            await self.store.upsert_user_registry(peer_call, now)
            await self.store.set_user_pref(peer_call, "node_family", self._peer_registry_profile(p, clean_dsn), now)

    async def save_peer_target(
        self,
        name: str,
        dsn: str,
        profile: str = "dxspider",
        reconnect: bool = True,
        password: str | None = "",
    ) -> None:
        await self._persist_peer_target(name, dsn, profile=profile, reconnect=reconnect, password=password)

    async def delete_peer_target(self, name: str) -> bool:
        desired = await self._desired_peer_targets()
        existed = name in desired
        await self._forget_peer_target(name)
        self._legacy_dxspider_peers.discard(name)
        disconnected = await self.node_link.disconnect_peer(name)
        return existed or disconnected

    async def _forget_peer_target(self, name: str) -> None:
        keys = [
            "name",
            "dsn",
            "profile",
            "password",
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
            if not name:
                continue
            desired[name] = row
        return desired

    async def connect_peer(
        self,
        name: str,
        dsn: str,
        profile: str = "dxspider",
        persist: bool = True,
        password: str = "",
    ) -> None:
        clean_dsn, embedded_password = self._split_peer_password(dsn)
        self._reset_py_peer_state(name)
        secret = str(password or embedded_password or "").strip()
        if persist:
            await self._persist_peer_target(name, clean_dsn, profile=profile, reconnect=True, password=secret)
        effective_dsn = self._merge_peer_password(clean_dsn, secret)
        wire_profile = "spider" if clean_dsn.strip().lower().startswith("dxspider://") and profile == "dxspider" else profile
        await self.node_link.connect_dsn(name, effective_dsn, profile=wire_profile)
        await self._begin_py_peer_session(name)
        await self._reset_mail_transport_state(name, "peer session refreshed")
        now = int(datetime.now(timezone.utc).timestamp())
        await self.store.set_user_pref(self.config.node.node_call, self._peer_pref_key(name, "last_connect_epoch"), str(now), now)
        await self.store.set_user_pref(self.config.node.node_call, self._peer_pref_key(name, "retry_count"), "0", now)
        await self.store.set_user_pref(self.config.node.node_call, self._peer_pref_key(name, "next_retry_epoch"), "0", now)
        await self.store.delete_user_pref(self.config.node.node_call, self._peer_pref_key(name, "last_error"))
        await self._record_outbound_peer_login(name, clean_dsn, profile, now)
        is_dxspider_dsn = clean_dsn.strip().lower().startswith("dxspider://")
        send_app_pc18 = wire_profile in {"spider", "dxspider", "pycluster"} and not is_dxspider_dsn
        if send_app_pc18:
            pc18 = parse_wire_pc_frame(pycluster_pc18())
            if pc18 is not None:
                await self.node_link.send(name, pc18)
        if wire_profile in {"spider", "dxspider", "pycluster"} and not is_dxspider_dsn:
            await self.node_link.send(name, WirePcFrame("PC20", [""]))
        if is_dxspider_dsn:
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
            clean_dsn, embedded_password = self._split_peer_password(str(row.get("dsn", "")).strip())
            secret = str(row.get("password", "") or embedded_password).strip()
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
            route_counts = await self.store.route_message_state_counts(name)
            pending_mail = int(route_counts.get("pending", 0))
            route_issues = int(route_counts.get("failed", 0)) + int(route_counts.get("undeliverable", 0))
            out.append(
                {
                    "peer": name,
                    "dsn": clean_dsn,
                    "profile": str(row.get("profile", "dxspider")).strip().lower() or "dxspider",
                    "password": secret,
                    "reconnect_enabled": enabled,
                    "retry_count": retry_count,
                    "next_retry_epoch": next_retry_epoch,
                    "last_connect_epoch": last_connect_epoch,
                    "last_error": str(row.get("last_error", "")).strip(),
                    "pending_mail": pending_mail,
                    "route_issues": route_issues,
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
        self._reset_py_peer_state(name)
        return await self.node_link.disconnect_peer(name)

    async def accept_inbound_node_login(
        self,
        call: str,
        peer_name: str,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        initial_lines: list[str] | None = None,
    ) -> bool:
        login_key = normalize_call(call) or call.upper()
        # The authenticated login is the remote peer. In DXSpider-style
        # handshakes, `client X telnet` names the node being connected to.
        peer_key = login_key
        self._reset_py_peer_state(peer_key)
        await self._begin_py_peer_session(peer_key)
        profile = (
            await self.store.get_user_pref(peer_key, "node_family")
            or await self.store.get_user_pref(login_key, "node_family")
            or "dxspider"
        ).strip().lower() or "dxspider"
        pycluster_identified = False
        for line in initial_lines or []:
            frame = parse_wire_pc_frame(line)
            if frame and frame.pc_type == "PC18":
                msg = Pc18Message.from_fields(frame.payload_fields)
                family, summary = self._peer_identity_from_pc18(msg.software)
                pycluster_identified = family == "pycluster"
                await self._record_proto_state(
                    peer_key,
                    {
                        "pc18.software": (msg.software or "").strip(),
                        "pc18.proto": (msg.proto_version or "").strip(),
                        "pc18.family": family,
                        "pc18.summary": summary,
                    },
                )
        conn = DxSpiderInboundConnection(peer_key, reader, writer, initial_lines=initial_lines)
        if pycluster_identified and profile == "pycluster":
            self._pycluster_identified_peers.add(peer_key)
        await self.node_link.accept_inbound(peer_key, conn, profile=profile)
        await conn.send_line(pycluster_pc18())
        await conn.send_line("PC20^")
        if pycluster_identified and profile == "pycluster":
            await self._send_py_hello(peer_key)
        if profile == "dxspider":
            self._legacy_dxspider_peers.add(peer_key)
            try:
                await self._send_legacy_init_config(peer_key)
            except KeyError:
                self._legacy_dxspider_peers.discard(peer_key)
                LOG.info("legacy inbound init skipped for disconnected peer=%s", peer_key)
                return False
        await self._reset_mail_transport_state(peer_key, "peer session refreshed")
        await self._flush_pending_messages_for_peer(peer_key)
        LOG.info("accepted inbound node login call=%s peer=%s key=%s", call, peer_name, peer_key)
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
        self._start_rbn_feed_tasks()

    async def stop(self) -> None:
        self._node_ingest_stop.set()
        await self._stop_rbn_feed_tasks()
        if self._rbn_web_socket:
            self._rbn_web_socket.close()
            self._rbn_web_socket = None
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

    @staticmethod
    def _strip_telnet_bytes(data: bytes) -> bytes:
        out = bytearray()
        i = 0
        while i < len(data):
            b = data[i]
            if b != 255:
                out.append(b)
                i += 1
                continue
            if i + 1 >= len(data):
                break
            cmd = data[i + 1]
            if cmd == 255:
                out.append(255)
                i += 2
                continue
            if cmd in {251, 252, 253, 254}:
                i += 3
                continue
            if cmd == 250:
                j = i + 2
                while j + 1 < len(data):
                    if data[j] == 255 and data[j + 1] == 240:
                        j += 2
                        break
                    j += 1
                i = j
                continue
            i += 2
        return bytes(out)

    async def _write_rbn_line(self, writer: asyncio.StreamWriter, line: str) -> None:
        writer.write((line.rstrip("\r\n") + "\r\n").encode("utf-8", errors="replace"))
        await writer.drain()

    async def _read_rbn_line(self, reader: asyncio.StreamReader, timeout: float) -> str | None:
        raw = await asyncio.wait_for(reader.readline(), timeout=timeout)
        if not raw:
            return None
        return self._strip_telnet_bytes(raw).decode("utf-8", errors="replace").strip()

    async def _flush_rbn_spot_batch(self, feed_key: str, spots: list[Spot]) -> int:
        if not spots:
            return 0
        pending = list(spots)
        spots.clear()
        ingest_epoch = int(datetime.now(timezone.utc).timestamp())
        forwarded = 0
        for idx, spot in enumerate(pending, start=1):
            if not self._remember_rbn_spot(spot, ingest_epoch):
                continue
            forwarded += 1
            self._rbn_recent_spot_epochs.append(ingest_epoch)
            self._set_rbn_feed_status(
                feed_key,
                last_spot_at=self._utc_status_time(),
                last_spot=f"{spot.spotter} {spot.freq_khz:.1f} {spot.dx_call} {spot.info}".strip(),
            )
            await self.telnet.publish_spot(spot)
            self._publish_rbn_to_public_web(spot)
            if idx % 25 == 0:
                await asyncio.sleep(0)
        return forwarded

    @staticmethod
    def _rbn_spot_key(spot: Spot) -> tuple[object, ...]:
        return (
            round(float(spot.freq_khz), 3),
            normalize_call(spot.dx_call),
            int(spot.epoch),
            normalize_call(spot.spotter),
            str(spot.info or ""),
        )

    def _remember_rbn_spot(self, spot: Spot, now_epoch: int | None = None) -> bool:
        now = int(now_epoch or datetime.now(timezone.utc).timestamp())
        cutoff = now - 600
        while self._rbn_seen_order and self._rbn_seen_order[0][0] < cutoff:
            _seen_epoch, stale_key = self._rbn_seen_order.popleft()
            self._rbn_seen.discard(stale_key)
        key = self._rbn_spot_key(spot)
        if key in self._rbn_seen:
            return False
        if len(self._rbn_seen_order) == self._rbn_seen_order.maxlen:
            _seen_epoch, stale_key = self._rbn_seen_order.popleft()
            self._rbn_seen.discard(stale_key)
        self._rbn_seen.add(key)
        self._rbn_seen_order.append((now, key))
        return True

    def _publish_rbn_to_public_web(self, spot: Spot) -> None:
        if self._rbn_web_socket is None:
            self._rbn_web_socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            self._rbn_web_socket.setblocking(False)
        try:
            self._rbn_web_socket.sendto(encode_rbn_spot(spot), rbn_socket_address(self.config))
        except (BlockingIOError, ConnectionRefusedError, FileNotFoundError, OSError):
            # Public web is optional and live RBN delivery is intentionally best-effort.
            return

    async def _publish_peer_rbn_spot(self, spot: Spot, *, exclude_peer: str) -> bool:
        now = int(datetime.now(timezone.utc).timestamp())
        if not self._remember_rbn_spot(spot, now):
            return False
        self._rbn_recent_spot_epochs.append(now)
        await self.telnet.publish_spot(spot)
        self._publish_rbn_to_public_web(spot)
        return True

    async def _run_rbn_feed_once(self, feed: dict[str, object]) -> None:
        cfg = self.config.rbn
        feed_key = str(feed["key"])
        host = str(feed["host"] or "").strip()
        port = int(feed["port"])
        call = normalize_call(cfg.callsign or self.config.node.node_call)
        self._set_rbn_feed_status(feed_key, state="connecting", last_error="")
        if not host or not call:
            self._set_rbn_feed_status(feed_key, state="error", last_error="RBN feed requires host and callsign", last_error_at=self._utc_status_time())
            raise RuntimeError("RBN feed requires host and callsign")
        reader, writer = await asyncio.open_connection(host, int(port))
        try:
            self._set_rbn_feed_status(feed_key, state="logging_in")
            await self._write_rbn_line(writer, call)
            sent_call = True
            logged_in = False
            deadline = asyncio.get_running_loop().time() + 20.0
            while asyncio.get_running_loop().time() < deadline and not logged_in:
                line = await self._read_rbn_line(reader, max(0.1, deadline - asyncio.get_running_loop().time()))
                if line is None:
                    raise RuntimeError("RBN feed closed before login completed")
                low = line.lower()
                if ("login:" in low or "call:" in low) and not sent_call:
                    await self._write_rbn_line(writer, call)
                    sent_call = True
                elif "password:" in low:
                    await self._write_rbn_line(writer, str(cfg.password or ""))
                elif ">" in line or "dx de " in low:
                    logged_in = True
            if not logged_in:
                raise RuntimeError("RBN feed did not complete login")
            self._set_rbn_feed_status(feed_key, state="connected", last_connected_at=self._utc_status_time(), last_error="")
            for command in cfg.startup_commands:
                await self._write_rbn_line(writer, command)
            pending_spots: list[Spot] = []
            last_line_monotonic = asyncio.get_running_loop().time()
            while not self._node_ingest_stop.is_set():
                try:
                    line = await self._read_rbn_line(reader, _RBN_IDLE_FLUSH_SECONDS)
                except asyncio.TimeoutError:
                    await self._flush_rbn_spot_batch(feed_key, pending_spots)
                    if asyncio.get_running_loop().time() - last_line_monotonic >= _RBN_IDLE_DISCONNECT_SECONDS:
                        return
                    continue
                if line is None:
                    await self._flush_rbn_spot_batch(feed_key, pending_spots)
                    return
                last_line_monotonic = asyncio.get_running_loop().time()
                self._set_rbn_feed_status(feed_key, last_line_at=self._utc_status_time())
                spot = parse_rbn_dx_line(line, source_node=cfg.source_node)
                if spot is None:
                    continue
                if not await self._rbn_ingest_allowed_for_call(call):
                    self._set_rbn_feed_status(feed_key, last_error="RBN ingest disabled for login call", last_error_at=self._utc_status_time())
                    continue
                if not await self._spot_passes_ingest_filters(call, spot):
                    self._set_rbn_feed_status(feed_key, last_error="RBN spot filtered for login call", last_error_at=self._utc_status_time())
                    continue
                pending_spots.append(spot)
                if len(pending_spots) >= _RBN_BATCH_SIZE:
                    await self._flush_rbn_spot_batch(feed_key, pending_spots)
        finally:
            if not self._node_ingest_stop.is_set():
                self._set_rbn_feed_status(feed_key, state="disconnected")
            writer.close()
            try:
                await asyncio.wait_for(writer.wait_closed(), timeout=1.0)
            except (asyncio.TimeoutError, ConnectionError, OSError):
                pass

    async def _rbn_feed_loop(self, feed: dict[str, object]) -> None:
        delay = max(5, int(self.config.rbn.reconnect_seconds or 60))
        while not self._node_ingest_stop.is_set():
            try:
                await self._run_rbn_feed_once(feed)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._set_rbn_feed_status(str(feed["key"]), state="error", last_error=str(exc), last_error_at=self._utc_status_time())
                LOG.warning("RBN feed disconnected; retrying in %ss: %s", delay, exc)
            try:
                await asyncio.wait_for(self._node_ingest_stop.wait(), timeout=delay)
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
            password = row.get("password", "").strip()
            profile = row.get("profile", "dxspider").strip().lower() or "dxspider"
            try:
                await self.connect_peer(name, dsn, profile=profile, persist=False, password=password)
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
        now = int(datetime.now(timezone.utc).timestamp())
        for name, row in stats.items():
            profile = str(row.get("profile", "dxspider")).strip().lower()
            if profile != "pycluster":
                continue
            try:
                heartbeat_sent = await self.node_link.send(name, WirePcFrame("PC20", [""]))
            except KeyError:
                continue
            except (ConnectionError, OSError):
                LOG.info("peer heartbeat skipped for disconnected peer=%s", name)
                continue
            if heartbeat_sent is not False:
                sent += 1
            peer_key = normalize_call(name) or name.upper()
            jitter = int(hashlib.sha256(peer_key.encode("ascii", errors="ignore")).hexdigest()[:4], 16) % 60
            last_digest = self._py_topology_digest_epoch.get(peer_key, 0)
            if now - last_digest >= self.config.py_protocol.refresh_seconds + jitter:
                try:
                    await self._send_py_topology_digest(name, force=True)
                except (KeyError, ConnectionError, OSError):
                    LOG.info("PY topology refresh skipped for disconnected peer=%s", name)
            try:
                await self._send_py_metadata(name)
            except (KeyError, ConnectionError, OSError):
                LOG.info("PY metadata refresh skipped for disconnected peer=%s", name)
        return sent

    def _classify_pc93_bulletin(self, sender: str, text: str) -> tuple[str, str, str]:
        category = "chat"
        scope = "LOCAL"
        body = _VIA_SUFFIX_RE.sub("", text).strip()
        m = _PC93_PREFIX_RE.match(text)
        if m:
            raw_body = (m.group(3) or "").strip() or text
            return m.group(1).strip().lower(), m.group(2).strip().upper(), (_VIA_SUFFIX_RE.sub("", raw_body).strip() or raw_body)
        sender_u = normalize_call(sender)
        body_u = text.upper()
        if sender_u == "DK0WCY" or parse_wcy_text(body) is not None or ("SPOTS=" in body_u and "EXPK=" in body_u):
            category = "wcy"
        elif sender_u == "WWV" or parse_wwv_text(body) is not None or re.search(r"\bSFI\s*=\s*\d+\b", body_u):
            category = "wwv"
        return category, scope, body

    async def _ingest_bulletin_from_peer(
        self,
        peer_name: str,
        *,
        category: str,
        scope: str,
        sender: str,
        body: str,
        duplicate_reason: str,
    ) -> None:
        if not body:
            return
        if category == "wcy" and not self._is_trusted_wcy_source(sender):
            await self.node_link.mark_policy_drop(peer_name, "ingest_wcy_untrusted_source")
            return
        if not await self._ingest_peer_enabled(peer_name, category):
            await self.node_link.mark_policy_drop(peer_name, f"ingest_{category}_disabled")
            return
        sender_norm = normalize_call(sender) if sender else normalize_call(peer_name)
        if not is_valid_call(sender_norm):
            sender_norm = normalize_call(peer_name)
        now = int(datetime.now(timezone.utc).timestamp())
        duplicate = await self.store.find_recent_bulletin_duplicate(
            category,
            sender_norm,
            scope,
            body,
            since_epoch=now - _BULLETIN_DEDUPE_WINDOW_SECONDS,
        )
        if duplicate is not None:
            await self.node_link.mark_policy_drop(peer_name, duplicate_reason)
            return
        await self.store.add_bulletin(category, sender_norm, scope, now, body)
        if category == "chat":
            await self.telnet.publish_chat(sender_norm, body)
        else:
            await self.telnet.publish_bulletin(category, sender_norm, scope, body)
            await self._relay_bulletin_to_links(
                category,
                sender_norm,
                scope,
                body,
                require_routepc19=False,
                exclude_peer=peer_name,
            )

    async def _live_peer_profiles(self) -> dict[str, str]:
        profiles: dict[str, str] = {}
        stats = await self.node_link.stats()
        for name, row in stats.items():
            profiles[name] = normalize_profile(str(row.get("profile", "dxspider")))
        return profiles

    def _build_dxspider_wwv_frame(self, sender: str, reading: WwvReading) -> WirePcFrame:
        now = datetime.now(timezone.utc)
        return WirePcFrame(
            "PC23",
            Pc23Message(
                date_token=now.strftime("%-d-%b-%Y"),
                hour_token=now.strftime("%H"),
                sfi=str(reading.sfi),
                a_index=str(reading.a_index),
                k_index=str(reading.k_index),
                forecast=reading.forecast,
                sender=normalize_call(sender),
                source_node=normalize_call(self.config.node.node_call),
                hops_token="H1",
                trailer="",
            ).to_fields(),
        )

    def _build_dxspider_wcy_frame(self, sender: str, reading: WcyReading) -> WirePcFrame:
        now = datetime.now(timezone.utc)
        return WirePcFrame(
            "PC73",
            Pc73Message(
                date_token=now.strftime("%-d-%b-%Y"),
                hour_token=now.strftime("%H"),
                sfi=str(reading.sfi),
                a_index=str(reading.a_index),
                k_index=str(reading.k_index),
                expk=str(reading.expk),
                sunspots=str(reading.sunspots),
                sun_activity=reading.sun_activity,
                geomagnetic_field=reading.geomagnetic_field,
                aurora=reading.aurora,
                sender=normalize_call(sender),
                source_node=normalize_call(self.config.node.node_call),
                hops_token="H1",
                trailer="",
            ).to_fields(),
        )

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
            if prev != value:
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
                        "from": str(prev or ""),
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
        if "dxspider" in low or "dx spider" in low:
            family = "dxspider"
            m = re.search(r"version:\s*([^\s]+)\s+build:\s*([^\s]+)", text, re.IGNORECASE)
            if m:
                summary = f"DXSpider {m.group(1)} build {m.group(2)}"
        elif "pycluster" in low:
            family = "pycluster"
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

    def _reset_py_peer_state(self, peer_name: str) -> None:
        peer_key = normalize_call(peer_name) or peer_name.upper()
        self._pycluster_identified_peers.discard(peer_key)
        self._py_hello_sent.discard(peer_key)
        self._py_remote_capabilities.pop(peer_key, None)
        self._py_negotiated_capabilities.pop(peer_key, None)
        self._py_nodeinfo_sent.discard(peer_key)
        self._py_topology_digest_sent.discard(peer_key)
        self._py_topology_digest_epoch.pop(peer_key, None)
        self._py_topology_snapshots.pop(peer_key, None)
        self._py_metadata_epoch.pop(peer_key, None)

    async def _begin_py_peer_session(self, peer_name: str) -> None:
        now = int(datetime.now(timezone.utc).timestamp())
        await self._record_proto_state(
            peer_name,
            {
                "py.session_epoch": str(now),
                "py.hello_sent_epoch": "",
                "py.hello_received_epoch": "",
                "py.nodeinfo.received_epoch": "",
                "py.handshake_error": "",
                "py.handshake_error_epoch": "",
            },
        )

    def _local_py_capabilities(self) -> tuple[str, ...]:
        capabilities = set(PY_CAPABILITIES)
        if self.config.py_protocol.share_node_info:
            capabilities.add("node-info")
        if self.config.py_protocol.share_topology:
            capabilities.update({"topology-digest", "topology-records", "request"})
        if self.config.py_protocol.share_health:
            capabilities.add("health")
        if self.config.py_protocol.share_datasets:
            capabilities.add("datasets")
        if self.config.py_protocol.share_rbn_status:
            capabilities.add("rbn-status")
        if self.config.py_protocol.share_notices:
            capabilities.add("notice")
        if self.config.py_protocol.share_policy:
            capabilities.add("policy")
        if self.config.py_protocol.share_clock:
            capabilities.add("clock")
        return tuple(sorted(capabilities))

    async def _send_py_hello(self, peer_name: str) -> bool:
        peer_key = normalize_call(peer_name) or peer_name.upper()
        if (
            not self.config.py_protocol.enabled
            or peer_key not in self._pycluster_identified_peers
            or peer_key in self._py_hello_sent
        ):
            return False
        stats = await self.node_link.stats()
        row = stats.get(peer_name) or stats.get(peer_key) or {}
        if str(row.get("profile") or "").strip().lower() != "pycluster":
            return False
        hello = PyHelloMessage(
            node_call=self.config.node.node_call,
            software_version=__version__,
            capabilities=self._local_py_capabilities(),
            epoch=int(datetime.now(timezone.utc).timestamp()),
        )
        if await self.node_link.send(peer_name, WirePcFrame(PY_HELLO_TYPE, hello.to_fields())) is False:
            return False
        self._py_hello_sent.add(peer_key)
        now = int(datetime.now(timezone.utc).timestamp())
        await self._record_proto_state(peer_name, {"py.hello_sent_epoch": str(now)})
        return True

    async def _build_py_node_info(self) -> PyNodeInfoMessage:
        now = int(datetime.now(timezone.utc).timestamp())
        owner = self.config.node.node_call
        node_id = str(await self.store.get_user_pref(owner, "py.local.node_id") or "").strip()
        try:
            node_id = str(uuid.UUID(node_id))
        except ValueError:
            node_id = str(uuid.uuid4())
            await self.store.set_user_pref(owner, "py.local.node_id", node_id, now)

        services = {"telnet", "sysop-web"}
        if self.config.public_web.enabled:
            services.add("public-web")
        if self.config.rbn.enabled:
            services.add("rbn-feed")
        capabilities = self._local_py_capabilities()
        public_web_url = (
            self.config.py_protocol.public_web_url.strip()
            if self.config.public_web.enabled and self.config.py_protocol.share_public_web_url
            else ""
        )
        descriptor = {
            "node_call": normalize_call(owner),
            "node_id": node_id,
            "software_version": __version__,
            "public_web_url": public_web_url,
            "locator": self.config.node.node_locator.strip().upper() if self.config.py_protocol.share_locator else "",
            "qth": self.config.node.qth.strip() if self.config.py_protocol.share_qth else "",
            "sysop_contact": self.config.node.support_contact.strip() if self.config.py_protocol.share_sysop_contact else "",
            "services": sorted(services),
            "capabilities": list(capabilities),
        }
        digest = hashlib.sha256(
            json.dumps(descriptor, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        ).hexdigest()
        previous_digest = str(await self.store.get_user_pref(owner, "py.local.nodeinfo.digest") or "")
        try:
            sequence = int(str(await self.store.get_user_pref(owner, "py.local.nodeinfo.sequence") or "0"))
        except ValueError:
            sequence = 0
        if digest != previous_digest or sequence <= 0:
            sequence += 1
            await self.store.set_user_pref(owner, "py.local.nodeinfo.digest", digest, now)
            await self.store.set_user_pref(owner, "py.local.nodeinfo.sequence", str(sequence), now)
        return PyNodeInfoMessage(
            node_call=descriptor["node_call"],
            node_id=node_id,
            sequence=sequence,
            software_version=__version__,
            public_web_url=public_web_url,
            locator=descriptor["locator"],
            qth=descriptor["qth"],
            sysop_contact=descriptor["sysop_contact"],
            services=tuple(descriptor["services"]),
            capabilities=capabilities,
            updated_epoch=now,
            expires_epoch=now + self.config.py_protocol.record_ttl_seconds,
        )

    async def _send_py_node_info(self, peer_name: str) -> bool:
        peer_key = normalize_call(peer_name) or peer_name.upper()
        negotiated = self._py_negotiated_capabilities.get(peer_key, frozenset())
        if (
            not self.config.py_protocol.enabled
            or not self.config.py_protocol.share_node_info
            or "node-info" not in negotiated
            or peer_key in self._py_nodeinfo_sent
        ):
            return False
        try:
            message = await self._build_py_node_info()
            fields = message.to_fields()
        except ValueError as exc:
            LOG.warning("PY01 NODEINFO not sent: invalid local metadata: %s", exc)
            return False
        if await self.node_link.send(peer_name, WirePcFrame(PY_NODEINFO_TYPE, fields)) is False:
            return False
        self._py_nodeinfo_sent.add(peer_key)
        return True

    @staticmethod
    def _py_record_from_info(
        info: PyNodeInfoMessage,
        *,
        source_node: str,
        learned_from: str,
        hop_count: int,
        confidence: str,
    ) -> dict[str, object]:
        return {
            "node_call": info.node_call,
            "node_id": info.node_id,
            "origin_node": info.node_call,
            "sequence": info.sequence,
            "software_version": info.software_version,
            "protocol_version": info.protocol_version,
            "public_web_url": info.public_web_url,
            "locator": info.locator,
            "qth": info.qth,
            "sysop_contact": info.sysop_contact,
            "services": list(info.services),
            "capabilities": list(info.capabilities),
            "source_node": source_node,
            "learned_from": learned_from,
            "hop_count": hop_count,
            "confidence": confidence,
            "updated_epoch": info.updated_epoch,
            "expires_at": info.expires_epoch,
            "raw_digest": info.content_digest(),
        }

    @staticmethod
    def _py_info_from_record(record: dict[str, object]) -> PyNodeInfoMessage:
        return PyNodeInfoMessage(
            node_call=str(record["node_call"]),
            node_id=str(record["node_id"]),
            sequence=int(record["sequence"]),
            software_version=str(record["software_version"]),
            public_web_url=str(record.get("public_web_url") or ""),
            locator=str(record.get("locator") or ""),
            qth=str(record.get("qth") or ""),
            sysop_contact=str(record.get("sysop_contact") or ""),
            services=tuple(str(item) for item in record.get("services") or []),
            capabilities=tuple(str(item) for item in record.get("capabilities") or []),
            updated_epoch=int(record["updated_epoch"]),
            expires_epoch=int(record["expires_at"]),
            protocol_version=str(record["protocol_version"]),
        )

    async def _ensure_local_py_record(self) -> PyNodeInfoMessage | None:
        try:
            info = await self._build_py_node_info()
            info.to_fields()
        except ValueError as exc:
            LOG.warning("Local PY topology record is invalid: %s", exc)
            return None
        now = int(datetime.now(timezone.utc).timestamp())
        await self.store.upsert_py_node_record(
            self._py_record_from_info(
                info,
                source_node=info.node_call,
                learned_from=info.node_call,
                hop_count=0,
                confidence="local",
            ),
            now,
        )
        return info

    def _py_frame_fits(self, frame: WirePcFrame) -> bool:
        return len(serialize_wire_protocol_frame(frame).encode("utf-8")) <= self.config.py_protocol.max_frame_bytes

    async def _send_py_topology_digest(self, peer_name: str, *, force: bool = False) -> bool:
        peer_key = normalize_call(peer_name) or peer_name.upper()
        negotiated = self._py_negotiated_capabilities.get(peer_key, frozenset())
        required = {"topology-digest", "topology-records", "request"}
        if (
            not self.config.py_protocol.enabled
            or not self.config.py_protocol.share_topology
            or not required.issubset(negotiated)
            or (peer_key in self._py_topology_digest_sent and not force)
        ):
            return False
        now = int(datetime.now(timezone.utc).timestamp())
        await self._ensure_local_py_record()
        await self.store.prune_expired_py_nodes(now)
        records = await self.store.list_py_node_records(now)
        records = [
            row for row in records
            if str(row.get("learned_from") or "").upper() != peer_key
            or str(row.get("node_call") or "").upper() == normalize_call(self.config.node.node_call)
        ]
        entries = [
            PyTopologyDigestEntry(
                node_call=str(row["node_call"]),
                node_id=str(row["node_id"]),
                sequence=int(row["sequence"]),
                digest=str(row["raw_digest"]),
                expires_epoch=int(row["expires_at"]),
            )
            for row in records
        ]
        snapshot_id = str(uuid.uuid4())
        bounded_entries: list[PyTopologyDigestEntry] = []
        for entry in entries:
            probe = PyTopologyDigestMessage((entry,), entry.node_call, False, now, snapshot_id, 1)
            if self._py_frame_fits(WirePcFrame(PY_TOPOLOGY_DIGEST_TYPE, probe.to_fields())):
                bounded_entries.append(entry)
            else:
                LOG.warning("PY02 digest entry exceeds configured frame size: %s", entry.node_call)
        entries = bounded_entries
        sent = False
        offset = 0
        page_number = 1
        while offset < len(entries) or (not entries and offset == 0):
            batch: list[PyTopologyDigestEntry] = []
            limit = min(len(entries), offset + self.config.py_protocol.max_records_per_frame)
            while offset + len(batch) < limit:
                candidate = batch + [entries[offset + len(batch)]]
                more = offset + len(candidate) < len(entries)
                message = PyTopologyDigestMessage(
                    tuple(candidate), candidate[-1].node_call, more, now, snapshot_id, page_number
                )
                if not self._py_frame_fits(WirePcFrame(PY_TOPOLOGY_DIGEST_TYPE, message.to_fields())):
                    break
                batch = candidate
            if entries and not batch:
                LOG.error("PY02 digest pagination could not fit a prevalidated entry: %s", entries[offset].node_call)
                return False
            more = offset + len(batch) < len(entries)
            cursor = batch[-1].node_call if batch else ""
            message = PyTopologyDigestMessage(tuple(batch), cursor, more, now, snapshot_id, page_number)
            if await self.node_link.send(peer_name, WirePcFrame(PY_TOPOLOGY_DIGEST_TYPE, message.to_fields())) is False:
                return False
            sent = True
            offset += len(batch)
            page_number += 1
            if not entries:
                break
        if sent:
            self._py_topology_digest_sent.add(peer_key)
            self._py_topology_digest_epoch[peer_key] = now
        return sent

    async def _send_py_topology_requests(self, peer_name: str, node_calls: list[str]) -> bool:
        if not node_calls:
            return False
        now = int(datetime.now(timezone.utc).timestamp())
        sent = False
        offset = 0
        calls = sorted(set(node_calls))
        while offset < len(calls):
            batch: list[str] = []
            limit = min(len(calls), offset + self.config.py_protocol.max_records_per_frame)
            while offset + len(batch) < limit:
                candidate = batch + [calls[offset + len(batch)]]
                message = PyTopologyRequestMessage(tuple(candidate), now)
                if not self._py_frame_fits(WirePcFrame(PY_REQUEST_TYPE, message.to_fields())):
                    break
                batch = candidate
            if not batch:
                LOG.warning("PY10 request entry exceeds configured frame size: %s", calls[offset])
                offset += 1
                continue
            message = PyTopologyRequestMessage(tuple(batch), now)
            if await self.node_link.send(peer_name, WirePcFrame(PY_REQUEST_TYPE, message.to_fields())) is False:
                return False
            sent = True
            offset += len(batch)
        return sent

    async def _send_py_topology_records(self, peer_name: str, node_calls: tuple[str, ...]) -> bool:
        peer_key = normalize_call(peer_name) or peer_name.upper()
        now = int(datetime.now(timezone.utc).timestamp())
        records: list[PyTopologyRecord] = []
        for node_call in node_calls:
            row = await self.store.get_py_node_record(node_call)
            if not row or int(row["expires_at"]) <= now:
                continue
            if (
                str(row.get("learned_from") or "").upper() == peer_key
                and str(row["node_call"]).upper() != normalize_call(self.config.node.node_call)
            ):
                continue
            info = self._py_info_from_record(row)
            records.append(
                PyTopologyRecord(info, str(row["origin_node"]), int(row["hop_count"]), str(row["raw_digest"]))
            )
        sent = False
        offset = 0
        while offset < len(records):
            batch: list[PyTopologyRecord] = []
            limit = min(len(records), offset + self.config.py_protocol.max_records_per_frame)
            while offset + len(batch) < limit:
                candidate = batch + [records[offset + len(batch)]]
                message = PyTopologyRecordsMessage(tuple(candidate), now)
                if not self._py_frame_fits(WirePcFrame(PY_TOPOLOGY_RECORDS_TYPE, message.to_fields())):
                    break
                batch = candidate
            if not batch:
                LOG.warning("PY03 record exceeds configured frame size: %s", records[offset].node_info.node_call)
                offset += 1
                continue
            message = PyTopologyRecordsMessage(tuple(batch), now)
            if await self.node_link.send(peer_name, WirePcFrame(PY_TOPOLOGY_RECORDS_TYPE, message.to_fields())) is False:
                return False
            sent = True
            offset += len(batch)
        return sent

    def _py_status_expiry(self, now: int) -> int:
        return now + min(86400, max(300, self.config.py_protocol.refresh_seconds * 2))

    async def _build_py_health(self, peer_name: str) -> PyHealthMessage:
        now = int(datetime.now(timezone.utc).timestamp())
        stats = await self.node_link.stats()
        peer_key = normalize_call(peer_name) or peer_name.upper()
        row = stats.get(peer_name) or stats.get(peer_key) or {}
        components = await self.component_status()
        services = {str(item["component"]): str(item["state"]) for item in components}
        if not self.config.public_web.enabled:
            services["publicweb"] = "disabled"
        services["rbn-feed"] = "up" if self.config.rbn.enabled and any(
            not task.done() for task in self._rbn_feed_tasks.values()
        ) else ("disabled" if not self.config.rbn.enabled else "down")
        last_rx = int(row.get("last_rx_epoch", 0) or 0)
        last_tx = int(row.get("last_tx_epoch", 0) or 0)
        receive_quiet = last_rx <= 0 or now - last_rx > 30 * 60
        transmit_active = last_tx > 0 and now - last_tx <= 30 * 60
        owner_prefs = await self.store.list_user_prefs(self.config.node.node_call)
        ptag = re.sub(r"[^a-z0-9_.-]", "_", peer_name.lower())
        try:
            flap_score = int(owner_prefs.get(f"proto.peer.{ptag}.flap_score", "0") or 0)
        except ValueError:
            flap_score = 0
        flapping = flap_score >= 3
        if flapping:
            link_state = "flapping"
        elif receive_quiet and last_rx > 0:
            link_state = "quiet"
        elif last_rx <= 0:
            link_state = "stale"
        else:
            link_state = "connected"
        policy_reasons = row.get("policy_reasons") if isinstance(row.get("policy_reasons"), dict) else {}
        last_error_category = ""
        if policy_reasons:
            last_error_category = max(policy_reasons, key=lambda key: int(policy_reasons.get(key, 0) or 0))
        state = "degraded" if any(value == "down" for value in services.values()) else "healthy"
        return PyHealthMessage(
            normalize_call(self.config.node.node_call), state, tuple(sorted(services.items())), link_state,
            last_rx, last_tx, receive_quiet, transmit_active, flapping, False,
            str(last_error_category), now, self._py_status_expiry(now),
        )

    @staticmethod
    def _datafile_modified_epoch(modified_iso: str) -> int:
        try:
            return int(datetime.fromisoformat(str(modified_iso)).timestamp())
        except (TypeError, ValueError):
            return 0

    def _build_py_datasets(self) -> PyDatasetsMessage:
        now = int(datetime.now(timezone.utc).timestamp())
        statuses = (
            describe_cty_file(self.config.public_web.cty_dat_path, loaded=cty_loaded()),
            describe_wpxloc_file(self.config.public_web.wpxloc_raw_path, loaded=wpx_loaded()),
            describe_data_file("KEPS", self.config.satellite.keps_path, loaded=False),
        )
        datasets = tuple(
            (
                status.name,
                status.version,
                status.version_date,
                self._datafile_modified_epoch(status.modified_iso),
                bool(status.stale),
                status.status,
            )
            for status in statuses
        )
        return PyDatasetsMessage(
            normalize_call(self.config.node.node_call), datasets, now, self._py_status_expiry(now)
        )

    @staticmethod
    def _status_iso_epoch(value: object) -> int:
        try:
            return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp())
        except (TypeError, ValueError):
            return 0

    def _rbn_status_modes(self) -> tuple[str, ...]:
        modes: set[str] = set()
        for feed in self._rbn_feed_configs():
            name = str(feed.get("name") or "").strip().upper()
            if name and not name.isdigit() and ":" not in name:
                modes.update(item for item in re.split(r"[/,+\s]+", name) if item)
                continue
            port = int(feed.get("port") or 0)
            if port == 7000:
                modes.update({"CW", "RTTY"})
            elif port == 7001:
                modes.add("FT8")
        return tuple(sorted(modes))

    def _build_py_rbn_status(self) -> PyRbnStatusMessage:
        now = int(datetime.now(timezone.utc).timestamp())
        while self._rbn_recent_spot_epochs and self._rbn_recent_spot_epochs[0] < now - 60:
            self._rbn_recent_spot_epochs.popleft()
        feeds = self._rbn_feed_status_payload()
        connected = sum(1 for feed in feeds if str(feed.get("state") or "").lower() == "connected")
        raw_state = str(self._rbn_feed_status.get("state") or "").lower()
        if not self.config.rbn.enabled:
            state = "disabled"
        elif connected:
            state = "connected" if connected == len(feeds) else "degraded"
        elif raw_state in {"starting", "connecting", "logging_in"}:
            state = "starting"
        elif raw_state == "error":
            state = "error"
        else:
            state = "stopped"
        return PyRbnStatusMessage(
            normalize_call(self.config.node.node_call), bool(self.config.rbn.enabled), self._rbn_status_modes(),
            len(feeds), connected, state, self._status_iso_epoch(self._rbn_feed_status.get("last_spot_at")),
            len(self._rbn_recent_spot_epochs), "normal", now, self._py_status_expiry(now),
        )

    def _build_py_policy(self) -> PyPolicyMessage:
        now = int(datetime.now(timezone.utc).timestamp())
        return PyPolicyMessage(
            normalize_call(self.config.node.node_call), bool(self.config.node.registration_required),
            bool(self.config.node.verified_email_required_for_web),
            bool(self.config.node.verified_email_required_for_telnet), bool(self.config.mfa.enabled),
            bool(self.config.mfa.enabled and self.config.mfa.require_for_users),
            bool(self.config.mfa.enabled and self.config.mfa.require_for_sysop),
            bool(self.config.public_web.enabled), bool(self.config.public_web.enabled),
            now, self._py_status_expiry(now),
        )

    async def _build_py_notice(self) -> PyNoticeMessage:
        now = int(datetime.now(timezone.utc).timestamp())
        owner = self.config.node.node_call
        configured_expiry = int(self.config.py_protocol.notice_expires_epoch or 0)
        active = bool(
            self.config.py_protocol.notice_message
            and now < configured_expiry <= now + 30 * 86400
        )
        descriptor = {
            "active": active,
            "severity": self.config.py_protocol.notice_severity if active else "normal",
            "message": self.config.py_protocol.notice_message if active else "",
            "expires_epoch": configured_expiry if active else self._py_status_expiry(now),
        }
        digest = hashlib.sha256(
            json.dumps(descriptor, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        ).hexdigest()
        previous_digest = str(await self.store.get_user_pref(owner, "py.local.notice.digest") or "")
        try:
            sequence = int(await self.store.get_user_pref(owner, "py.local.notice.sequence") or 0)
        except (TypeError, ValueError):
            sequence = 0
        notice_id = str(await self.store.get_user_pref(owner, "py.local.notice.id") or "")
        try:
            notice_id = str(uuid.UUID(notice_id))
        except ValueError:
            notice_id = ""
        try:
            created_epoch = int(await self.store.get_user_pref(owner, "py.local.notice.created_epoch") or 0)
        except (TypeError, ValueError):
            created_epoch = 0
        if digest != previous_digest or sequence <= 0 or not notice_id or created_epoch <= 0:
            sequence += 1
            notice_id = str(uuid.uuid4())
            created_epoch = now
            await self.store.set_user_pref(owner, "py.local.notice.digest", digest, now)
            await self.store.set_user_pref(owner, "py.local.notice.sequence", str(sequence), now)
            await self.store.set_user_pref(owner, "py.local.notice.id", notice_id, now)
            await self.store.set_user_pref(owner, "py.local.notice.created_epoch", str(created_epoch), now)
        return PyNoticeMessage(
            normalize_call(owner), notice_id, sequence, active, str(descriptor["severity"]),
            str(descriptor["message"]), created_epoch, now, int(descriptor["expires_epoch"]),
        )

    def _build_py_clock(self) -> PyClockMessage:
        now = int(datetime.now(timezone.utc).timestamp())
        boot_epoch = int(self.started_at.timestamp())
        return PyClockMessage(
            normalize_call(self.config.node.node_call), now, max(0, now - boot_epoch), boot_epoch,
            now, self._py_status_expiry(now),
        )

    async def _send_py_metadata(self, peer_name: str, *, force: bool = False) -> int:
        peer_key = normalize_call(peer_name) or peer_name.upper()
        now = int(datetime.now(timezone.utc).timestamp())
        interval = min(300, self.config.py_protocol.refresh_seconds)
        if not self.config.py_protocol.enabled or (
            not force and now - self._py_metadata_epoch.get(peer_key, 0) < interval
        ):
            return 0
        negotiated = self._py_negotiated_capabilities.get(peer_key, frozenset())
        enabled_capabilities = {
            capability
            for enabled, capability in (
                (self.config.py_protocol.share_health, "health"),
                (self.config.py_protocol.share_datasets, "datasets"),
                (self.config.py_protocol.share_rbn_status, "rbn-status"),
                (self.config.py_protocol.share_notices, "notice"),
                (self.config.py_protocol.share_policy, "policy"),
                (self.config.py_protocol.share_clock, "clock"),
            )
            if enabled and capability in negotiated
        }
        if not enabled_capabilities:
            return 0
        builders: list[tuple[str, str, object]] = []
        if "health" in enabled_capabilities:
            builders.append(("health", PY_HEALTH_TYPE, await self._build_py_health(peer_name)))
        if "datasets" in enabled_capabilities:
            builders.append(("datasets", PY_DATASETS_TYPE, self._build_py_datasets()))
        if "rbn-status" in enabled_capabilities:
            builders.append(("rbn-status", PY_RBN_STATUS_TYPE, self._build_py_rbn_status()))
        if "notice" in enabled_capabilities:
            builders.append(("notice", PY_NOTICE_TYPE, await self._build_py_notice()))
        if "policy" in enabled_capabilities:
            builders.append(("policy", PY_POLICY_TYPE, self._build_py_policy()))
        if "clock" in enabled_capabilities:
            builders.append(("clock", PY_CLOCK_TYPE, self._build_py_clock()))
        sent = 0
        sendable = 0
        for _capability, frame_type, message in builders:
            fields = message.to_fields()  # type: ignore[union-attr]
            frame = WirePcFrame(frame_type, fields)
            if not self._py_frame_fits(frame):
                LOG.warning("%s metadata exceeds configured PY frame size", frame_type)
                continue
            sendable += 1
            if await self.node_link.send(peer_name, frame) is not False:
                sent += 1
        if sendable == sent:
            self._py_metadata_epoch[peer_key] = now
        return sent

    async def _send_py_error(self, peer_name: str, code: str, offending_type: str, detail: str) -> bool:
        peer_key = normalize_call(peer_name) or peer_name.upper()
        negotiated = self._py_negotiated_capabilities.get(peer_key, frozenset())
        if "py99-error" not in negotiated or offending_type == PY_ERROR_TYPE:
            return False
        clean_detail = " ".join(str(detail or "").replace("^", " ").split())[:96]
        message = PyErrorMessage(
            code=code,
            offending_type=offending_type,
            detail=clean_detail,
            epoch=int(datetime.now(timezone.utc).timestamp()),
        )
        return await self.node_link.send(peer_name, WirePcFrame(PY_ERROR_TYPE, message.to_fields()))

    async def _validate_py_metadata_message(self, peer_name: str, frame_type: str, message: object) -> bool:
        peer_key = normalize_call(peer_name) or peer_name.upper()
        if str(getattr(message, "node_call", "")) != peer_key:
            await self.node_link.mark_policy_drop(peer_name, "py_identity_mismatch")
            await self._send_py_error(peer_name, "identity-mismatch", frame_type, "Metadata callsign does not match the authenticated peer")
            return False
        now = int(datetime.now(timezone.utc).timestamp())
        generated = int(getattr(message, "generated_epoch", 0))
        expires = int(getattr(message, "expires_epoch", 0))
        if generated > now + 300:
            await self.node_link.mark_policy_drop(peer_name, "future_py_metadata")
            await self._send_py_error(peer_name, "clock-skew", frame_type, "Metadata time is too far in the future")
            return False
        if expires <= now:
            await self.node_link.mark_policy_drop(peer_name, "stale_py_metadata")
            await self._send_py_error(peer_name, "stale-record", frame_type, "Metadata record has expired")
            return False
        return True

    async def _handle_py_frame(self, peer_name: str, frame: WirePcFrame) -> None:
        peer_key = normalize_call(peer_name) or peer_name.upper()
        if not self.config.py_protocol.enabled:
            await self.node_link.mark_policy_drop(peer_name, "py_disabled")
            return
        if peer_key not in self._pycluster_identified_peers:
            await self.node_link.mark_policy_drop(peer_name, "py_before_pc18_identity")
            return
        if frame.pc_type == PY_HELLO_TYPE:
            try:
                hello = PyHelloMessage.from_fields(frame.payload_fields)
            except ValueError:
                await self.node_link.mark_policy_drop(peer_name, "invalid_py_hello")
                now = int(datetime.now(timezone.utc).timestamp())
                await self._record_proto_state(
                    peer_name,
                    {
                        "py.handshake_error": "invalid_py00",
                        "py.handshake_error_epoch": str(now),
                    },
                )
                return
            if hello.node_call != peer_key:
                await self.node_link.mark_policy_drop(peer_name, "py_identity_mismatch")
                now = int(datetime.now(timezone.utc).timestamp())
                await self._record_proto_state(
                    peer_name,
                    {
                        "py.handshake_error": "identity_mismatch",
                        "py.handshake_error_epoch": str(now),
                    },
                )
                return
            remote_capabilities = frozenset(hello.capabilities)
            negotiated = frozenset(set(self._local_py_capabilities()).intersection(remote_capabilities))
            self._py_remote_capabilities[peer_key] = remote_capabilities
            self._py_negotiated_capabilities[peer_key] = negotiated
            await self._touch_proto_activity(peer_name, frame.pc_type)
            await self._record_proto_state(
                peer_name,
                {
                    "py.protocol_version": hello.protocol_version,
                    "py.node": hello.node_call,
                    "py.software_version": hello.software_version,
                    "py.capabilities": ",".join(hello.capabilities),
                    "py.negotiated_capabilities": ",".join(sorted(negotiated)),
                    "py.announced_epoch": str(hello.epoch),
                    "py.hello_received_epoch": str(int(datetime.now(timezone.utc).timestamp())),
                    "py.handshake_error": "",
                    "py.handshake_error_epoch": "",
                },
            )
            await self._send_py_hello(peer_name)
            await self._send_py_node_info(peer_name)
            await self._send_py_topology_digest(peer_name)
            await self._send_py_metadata(peer_name, force=True)
            return

        if peer_key not in self._py_remote_capabilities:
            await self.node_link.mark_policy_drop(peer_name, "py_before_capability_negotiation")
            return

        required_capability = PY_FRAME_CAPABILITIES.get(frame.pc_type)
        negotiated = self._py_negotiated_capabilities.get(peer_key, frozenset())
        if required_capability is None or required_capability not in negotiated:
            await self.node_link.mark_policy_drop(peer_name, "unsupported_py_type")
            await self._send_py_error(peer_name, "unsupported-type", frame.pc_type, "Frame capability was not negotiated")
            return

        if frame.pc_type == PY_ERROR_TYPE:
            try:
                error = PyErrorMessage.from_fields(frame.payload_fields)
            except ValueError:
                await self.node_link.mark_policy_drop(peer_name, "invalid_py_error")
                return
            await self._touch_proto_activity(peer_name, frame.pc_type)
            await self._record_proto_state(
                peer_name,
                {
                    "py.last_error.code": error.code,
                    "py.last_error.offending_type": error.offending_type,
                    "py.last_error.detail": error.detail,
                    "py.last_error.announced_epoch": str(error.epoch),
                },
            )
            return

        if frame.pc_type == PY_NODEINFO_TYPE:
            try:
                node_info = PyNodeInfoMessage.from_fields(frame.payload_fields)
            except ValueError:
                await self.node_link.mark_policy_drop(peer_name, "invalid_py_nodeinfo")
                await self._send_py_error(peer_name, "malformed", frame.pc_type, "NODEINFO payload is invalid")
                return
            if node_info.node_call != peer_key:
                await self.node_link.mark_policy_drop(peer_name, "py_identity_mismatch")
                await self._send_py_error(peer_name, "identity-mismatch", frame.pc_type, "NODEINFO callsign does not match the authenticated peer")
                return
            now = int(datetime.now(timezone.utc).timestamp())
            if node_info.updated_epoch > now + 300:
                await self.node_link.mark_policy_drop(peer_name, "future_py_nodeinfo")
                await self._send_py_error(peer_name, "clock-skew", frame.pc_type, "NODEINFO update time is too far in the future")
                return
            if node_info.expires_epoch <= now:
                await self.node_link.mark_policy_drop(peer_name, "stale_py_nodeinfo")
                await self._send_py_error(peer_name, "stale-record", frame.pc_type, "NODEINFO record has expired")
                return
            ptag = re.sub(r"[^a-z0-9_.-]", "_", peer_name.lower())
            pfx = f"proto.peer.{ptag}.py.nodeinfo."
            current = await self.store.list_user_prefs(self.config.node.node_call)
            existing_node_id = str(current.get(pfx + "node_id", ""))
            try:
                existing_sequence = int(str(current.get(pfx + "sequence", "0")))
            except ValueError:
                existing_sequence = 0
            incoming_digest = node_info.content_digest()
            existing_digest = str(current.get(pfx + "content_digest", ""))
            if existing_node_id == node_info.node_id and node_info.sequence < existing_sequence:
                await self.node_link.mark_policy_drop(peer_name, "stale_py_nodeinfo_sequence")
                await self._send_py_error(peer_name, "stale-record", frame.pc_type, "NODEINFO sequence is older than the stored record")
                return
            if (
                existing_node_id == node_info.node_id
                and node_info.sequence == existing_sequence
                and existing_digest
                and incoming_digest != existing_digest
            ):
                await self.node_link.mark_policy_drop(peer_name, "conflicting_py_nodeinfo_sequence")
                await self._send_py_error(peer_name, "sequence-conflict", frame.pc_type, "NODEINFO content changed without a new sequence")
                return
            previous_node_id = existing_node_id if existing_node_id and existing_node_id != node_info.node_id else ""
            await self._touch_proto_activity(peer_name, frame.pc_type)
            await self._record_proto_state(
                peer_name,
                {
                    "py.nodeinfo.node_id": node_info.node_id,
                    "py.nodeinfo.sequence": str(node_info.sequence),
                    "py.nodeinfo.content_digest": incoming_digest,
                    "py.nodeinfo.previous_node_id": previous_node_id,
                    "py.nodeinfo.software_version": node_info.software_version,
                    "py.nodeinfo.public_web_url": node_info.public_web_url,
                    "py.nodeinfo.locator": node_info.locator,
                    "py.nodeinfo.qth": node_info.qth,
                    "py.nodeinfo.sysop_contact": node_info.sysop_contact,
                    "py.nodeinfo.services": ",".join(node_info.services),
                    "py.nodeinfo.capabilities": ",".join(node_info.capabilities),
                    "py.nodeinfo.updated_epoch": str(node_info.updated_epoch),
                    "py.nodeinfo.received_epoch": str(now),
                    "py.nodeinfo.expires_epoch": str(node_info.expires_epoch),
                    "py.nodeinfo.learned_from": peer_key,
                    "py.nodeinfo.confidence": "direct",
                },
            )
            await self.store.upsert_py_node_record(
                self._py_record_from_info(
                    node_info,
                    source_node=peer_key,
                    learned_from=peer_key,
                    hop_count=0,
                    confidence="direct",
                ),
                now,
            )
            await self._send_py_node_info(peer_name)
            return

        if frame.pc_type == PY_HEALTH_TYPE:
            try:
                health = PyHealthMessage.from_fields(frame.payload_fields)
            except ValueError:
                await self.node_link.mark_policy_drop(peer_name, "invalid_py_health")
                await self._send_py_error(peer_name, "malformed", frame.pc_type, "HEALTH payload is invalid")
                return
            if not await self._validate_py_metadata_message(peer_name, frame.pc_type, health):
                return
            await self._touch_proto_activity(peer_name, frame.pc_type)
            await self._record_proto_state(peer_name, {
                "py.health.state": health.state,
                "py.health.services": json.dumps(dict(health.services), sort_keys=True, separators=(",", ":")),
                "py.health.link_state": health.link_state,
                "py.health.last_rx_epoch": str(health.last_rx_epoch),
                "py.health.last_tx_epoch": str(health.last_tx_epoch),
                "py.health.receive_quiet": "1" if health.receive_quiet else "0",
                "py.health.transmit_active": "1" if health.transmit_active else "0",
                "py.health.flapping": "1" if health.flapping else "0",
                "py.health.reconnecting": "1" if health.reconnecting else "0",
                "py.health.last_error_category": health.last_error_category,
                "py.health.generated_epoch": str(health.generated_epoch),
                "py.health.expires_epoch": str(health.expires_epoch),
            })
            return

        if frame.pc_type == PY_DATASETS_TYPE:
            try:
                datasets = PyDatasetsMessage.from_fields(frame.payload_fields)
            except ValueError:
                await self.node_link.mark_policy_drop(peer_name, "invalid_py_datasets")
                await self._send_py_error(peer_name, "malformed", frame.pc_type, "DATASETS payload is invalid")
                return
            if not await self._validate_py_metadata_message(peer_name, frame.pc_type, datasets):
                return
            dataset_rows = [
                {"name": name, "version": version, "version_date": version_date, "modified_epoch": modified,
                 "stale": stale, "status": status}
                for name, version, version_date, modified, stale, status in datasets.datasets
            ]
            await self._touch_proto_activity(peer_name, frame.pc_type)
            await self._record_proto_state(peer_name, {
                "py.datasets.records": json.dumps(dataset_rows, sort_keys=True, separators=(",", ":")),
                "py.datasets.generated_epoch": str(datasets.generated_epoch),
                "py.datasets.expires_epoch": str(datasets.expires_epoch),
            })
            return

        if frame.pc_type == PY_RBN_STATUS_TYPE:
            try:
                rbn_status = PyRbnStatusMessage.from_fields(frame.payload_fields)
            except ValueError:
                await self.node_link.mark_policy_drop(peer_name, "invalid_py_rbn_status")
                await self._send_py_error(peer_name, "malformed", frame.pc_type, "RBN_STATUS payload is invalid")
                return
            if not await self._validate_py_metadata_message(peer_name, frame.pc_type, rbn_status):
                return
            await self._touch_proto_activity(peer_name, frame.pc_type)
            await self._record_proto_state(peer_name, {
                "py.rbn.enabled": "1" if rbn_status.enabled else "0",
                "py.rbn.modes": ",".join(rbn_status.modes),
                "py.rbn.feed_count": str(rbn_status.feed_count),
                "py.rbn.connected_count": str(rbn_status.connected_count),
                "py.rbn.state": rbn_status.state,
                "py.rbn.last_spot_epoch": str(rbn_status.last_spot_epoch),
                "py.rbn.recent_spots_per_minute": str(rbn_status.recent_spots_per_minute),
                "py.rbn.queue_state": rbn_status.queue_state,
                "py.rbn.generated_epoch": str(rbn_status.generated_epoch),
                "py.rbn.expires_epoch": str(rbn_status.expires_epoch),
            })
            return

        if frame.pc_type == PY_NOTICE_TYPE:
            try:
                notice = PyNoticeMessage.from_fields(frame.payload_fields)
            except ValueError:
                await self.node_link.mark_policy_drop(peer_name, "invalid_py_notice")
                await self._send_py_error(peer_name, "malformed", frame.pc_type, "NOTICE payload is invalid")
                return
            if not await self._validate_py_metadata_message(peer_name, frame.pc_type, notice):
                return
            owner_prefs = await self.store.list_user_prefs(self.config.node.node_call)
            ptag = re.sub(r"[^a-z0-9_.-]", "_", peer_name.lower())
            pfx = f"proto.peer.{ptag}.py.notice."
            try:
                existing_sequence = int(owner_prefs.get(pfx + "sequence", "0") or 0)
            except (TypeError, ValueError):
                existing_sequence = 0
            existing_digest = str(owner_prefs.get(pfx + "content_digest", ""))
            content_digest = hashlib.sha256(
                json.dumps(
                    {
                        "notice_id": notice.notice_id, "active": notice.active,
                        "severity": notice.severity, "message": notice.message,
                        "created_epoch": notice.created_epoch, "expires_epoch": notice.expires_epoch,
                    },
                    sort_keys=True, separators=(",", ":"), ensure_ascii=True,
                ).encode("utf-8")
            ).hexdigest()
            if notice.sequence < existing_sequence:
                await self.node_link.mark_policy_drop(peer_name, "stale_py_notice_sequence")
                await self._send_py_error(peer_name, "stale-record", frame.pc_type, "NOTICE sequence is older than the stored record")
                return
            if notice.sequence == existing_sequence and existing_digest and content_digest != existing_digest:
                await self.node_link.mark_policy_drop(peer_name, "conflicting_py_notice_sequence")
                await self._send_py_error(peer_name, "sequence-conflict", frame.pc_type, "NOTICE content changed without a new sequence")
                return
            await self._touch_proto_activity(peer_name, frame.pc_type)
            await self._record_proto_state(peer_name, {
                "py.notice.notice_id": notice.notice_id,
                "py.notice.sequence": str(notice.sequence),
                "py.notice.active": "1" if notice.active else "0",
                "py.notice.severity": notice.severity,
                "py.notice.message": notice.message,
                "py.notice.created_epoch": str(notice.created_epoch),
                "py.notice.generated_epoch": str(notice.generated_epoch),
                "py.notice.expires_epoch": str(notice.expires_epoch),
                "py.notice.content_digest": content_digest,
            })
            return

        if frame.pc_type == PY_POLICY_TYPE:
            try:
                policy = PyPolicyMessage.from_fields(frame.payload_fields)
            except ValueError:
                await self.node_link.mark_policy_drop(peer_name, "invalid_py_policy")
                await self._send_py_error(peer_name, "malformed", frame.pc_type, "POLICY payload is invalid")
                return
            if not await self._validate_py_metadata_message(peer_name, frame.pc_type, policy):
                return
            await self._touch_proto_activity(peer_name, frame.pc_type)
            await self._record_proto_state(peer_name, {
                "py.policy.registration_required": "1" if policy.registration_required else "0",
                "py.policy.email_verification_web": "1" if policy.email_verification_web else "0",
                "py.policy.email_verification_telnet": "1" if policy.email_verification_telnet else "0",
                "py.policy.mfa_available": "1" if policy.mfa_available else "0",
                "py.policy.mfa_required_users": "1" if policy.mfa_required_users else "0",
                "py.policy.mfa_required_sysops": "1" if policy.mfa_required_sysops else "0",
                "py.policy.public_web_enabled": "1" if policy.public_web_enabled else "0",
                "py.policy.anonymous_web_enabled": "1" if policy.anonymous_web_enabled else "0",
                "py.policy.generated_epoch": str(policy.generated_epoch),
                "py.policy.expires_epoch": str(policy.expires_epoch),
            })
            return

        if frame.pc_type == PY_CLOCK_TYPE:
            try:
                clock = PyClockMessage.from_fields(frame.payload_fields)
            except ValueError:
                await self.node_link.mark_policy_drop(peer_name, "invalid_py_clock")
                await self._send_py_error(peer_name, "malformed", frame.pc_type, "CLOCK payload is invalid")
                return
            if not await self._validate_py_metadata_message(peer_name, frame.pc_type, clock):
                return
            now = int(datetime.now(timezone.utc).timestamp())
            await self._touch_proto_activity(peer_name, frame.pc_type)
            await self._record_proto_state(peer_name, {
                "py.clock.utc_epoch": str(clock.utc_epoch),
                "py.clock.offset_seconds": str(clock.utc_epoch - now),
                "py.clock.uptime_seconds": str(clock.uptime_seconds),
                "py.clock.boot_epoch": str(clock.boot_epoch),
                "py.clock.generated_epoch": str(clock.generated_epoch),
                "py.clock.expires_epoch": str(clock.expires_epoch),
            })
            return

        if frame.pc_type == PY_TOPOLOGY_DIGEST_TYPE:
            try:
                digest_message = PyTopologyDigestMessage.from_fields(frame.payload_fields)
            except ValueError:
                await self.node_link.mark_policy_drop(peer_name, "invalid_py_topology_digest")
                await self._send_py_error(peer_name, "malformed", frame.pc_type, "Topology digest is invalid")
                return
            if len(digest_message.entries) > self.config.py_protocol.max_records_per_frame:
                await self.node_link.mark_policy_drop(peer_name, "py_topology_digest_record_limit")
                await self._send_py_error(peer_name, "record-limit", frame.pc_type, "Topology digest has too many entries")
                return
            now = int(datetime.now(timezone.utc).timestamp())
            if digest_message.generated_epoch > now + 300:
                await self.node_link.mark_policy_drop(peer_name, "future_py_topology_digest")
                await self._send_py_error(peer_name, "clock-skew", frame.pc_type, "Topology digest time is too far in the future")
                return
            snapshot = self._py_topology_snapshots.get(peer_key)
            if digest_message.page_number == 1:
                snapshot = {
                    "snapshot_id": digest_message.snapshot_id,
                    "next_page": 1,
                    "requested": set(),
                    "seen": set(),
                    "generated_epoch": digest_message.generated_epoch,
                }
                self._py_topology_snapshots[peer_key] = snapshot
            if (
                snapshot is None
                or str(snapshot.get("snapshot_id")) != digest_message.snapshot_id
                or int(snapshot.get("next_page", 0)) != digest_message.page_number
                or int(snapshot.get("generated_epoch", 0)) != digest_message.generated_epoch
            ):
                self._py_topology_snapshots.pop(peer_key, None)
                await self.node_link.mark_policy_drop(peer_name, "py_topology_snapshot_order")
                await self._send_py_error(peer_name, "snapshot-order", frame.pc_type, "Topology snapshot page is missing or out of order")
                return
            requested: list[str] = []
            seen = snapshot["seen"]
            assert isinstance(seen, set)
            local_call = normalize_call(self.config.node.node_call)
            for entry in digest_message.entries:
                if entry.node_call in seen:
                    self._py_topology_snapshots.pop(peer_key, None)
                    await self.node_link.mark_policy_drop(peer_name, "py_topology_snapshot_duplicate")
                    await self._send_py_error(peer_name, "snapshot-duplicate", frame.pc_type, "Topology snapshot repeats a node")
                    return
                seen.add(entry.node_call)
                if entry.node_call == local_call or entry.expires_epoch <= now:
                    continue
                current = await self.store.get_py_node_record(entry.node_call)
                if current is None:
                    requested.append(entry.node_call)
                    continue
                same_identity = str(current["node_id"]) == entry.node_id
                local_sequence = int(current["sequence"])
                same_digest = str(current["raw_digest"]) == entry.digest
                if (
                    not same_identity
                    or entry.sequence > local_sequence
                    or (entry.sequence == local_sequence and not same_digest)
                ):
                    requested.append(entry.node_call)
                elif entry.sequence == local_sequence and same_digest:
                    await self.store.refresh_py_node_lease(
                        entry.node_call,
                        entry.node_id,
                        entry.sequence,
                        entry.digest,
                        entry.expires_epoch,
                        now,
                    )
            await self._touch_proto_activity(peer_name, frame.pc_type)
            accumulated = snapshot["requested"]
            assert isinstance(accumulated, set)
            accumulated.update(requested)
            snapshot["next_page"] = digest_message.page_number + 1
            if not digest_message.more:
                self._py_topology_snapshots.pop(peer_key, None)
                await self._send_py_topology_requests(peer_name, sorted(accumulated))
            return

        if frame.pc_type == PY_REQUEST_TYPE:
            try:
                request = PyTopologyRequestMessage.from_fields(frame.payload_fields)
            except ValueError:
                await self.node_link.mark_policy_drop(peer_name, "invalid_py_topology_request")
                await self._send_py_error(peer_name, "malformed", frame.pc_type, "Topology request is invalid")
                return
            if len(request.node_calls) > self.config.py_protocol.max_records_per_frame:
                await self.node_link.mark_policy_drop(peer_name, "py_topology_request_record_limit")
                await self._send_py_error(peer_name, "record-limit", frame.pc_type, "Topology request has too many entries")
                return
            await self._touch_proto_activity(peer_name, frame.pc_type)
            await self._send_py_topology_records(peer_name, request.node_calls)
            return

        if frame.pc_type == PY_TOPOLOGY_RECORDS_TYPE:
            try:
                record_message = PyTopologyRecordsMessage.from_fields(frame.payload_fields)
            except ValueError:
                await self.node_link.mark_policy_drop(peer_name, "invalid_py_topology_records")
                await self._send_py_error(peer_name, "malformed", frame.pc_type, "Topology records are invalid")
                return
            if len(record_message.records) > self.config.py_protocol.max_records_per_frame:
                await self.node_link.mark_policy_drop(peer_name, "py_topology_records_limit")
                await self._send_py_error(peer_name, "record-limit", frame.pc_type, "Topology frame has too many records")
                return
            now = int(datetime.now(timezone.utc).timestamp())
            local_call = normalize_call(self.config.node.node_call)
            changed = False
            for topology_record in record_message.records:
                info = topology_record.node_info
                if info.node_call == local_call:
                    await self.node_link.mark_policy_drop(peer_name, "py_topology_local_origin")
                    continue
                if info.updated_epoch > now + 300 or info.expires_epoch <= now:
                    await self.node_link.mark_policy_drop(peer_name, "stale_py_topology_record")
                    continue
                direct = topology_record.origin_node == peer_key
                hop_count = 0 if direct else topology_record.hop_count + 1
                if hop_count > self.config.py_protocol.max_hops:
                    await self.node_link.mark_policy_drop(peer_name, "py_topology_hop_limit")
                    continue
                result = await self.store.upsert_py_node_record(
                    self._py_record_from_info(
                        info,
                        source_node=peer_key,
                        learned_from=peer_key,
                        hop_count=hop_count,
                        confidence="direct" if direct else "reported",
                    ),
                    now,
                )
                changed = changed or result == "accepted"
                if result == "rejected-conflict":
                    await self.node_link.mark_policy_drop(peer_name, "conflicting_py_topology_sequence")
            await self._touch_proto_activity(peer_name, frame.pc_type)
            if changed:
                for other_peer in self._py_negotiated_capabilities:
                    if other_peer != peer_key:
                        self._py_topology_digest_sent.discard(other_peer)
                        self._py_topology_digest_epoch[other_peer] = 0
            return

        await self.node_link.mark_policy_drop(peer_name, "unimplemented_py_type")
        await self._send_py_error(peer_name, "not-implemented", frame.pc_type, "Frame is not implemented by this build")

    async def _handle_node_link_item(self, peer_name: str, frame: WirePcFrame, typed: object | None) -> None:
        if frame.pc_type.startswith("PY"):
            await self._handle_py_frame(peer_name, frame)
            return
        if frame.pc_type in {"PC10", "PC11", "PC12", "PC16", "PC17", "PC18", "PC19", "PC20", "PC21", "PC22", "PC23", "PC24", "PC28", "PC29", "PC30", "PC31", "PC32", "PC33", "PC50", "PC51", "PC61", "PC73", "PC93"}:
            await self._touch_proto_activity(peer_name, frame.pc_type)

        if frame.pc_type == "PC18":
            msg = typed if isinstance(typed, Pc18Message) else Pc18Message.from_fields(frame.payload_fields)
            family, summary = self._peer_identity_from_pc18(msg.software)
            peer_key = normalize_call(peer_name) or peer_name.upper()
            if family == "pycluster":
                self._pycluster_identified_peers.add(peer_key)
            else:
                self._reset_py_peer_state(peer_key)
            await self._record_proto_state(
                peer_name,
                {
                    "pc18.software": (msg.software or "").strip(),
                    "pc18.proto": (msg.proto_version or "").strip(),
                    "pc18.family": family,
                    "pc18.summary": summary,
                },
            )
            if family == "pycluster":
                await self._send_py_hello(peer_name)
            return

        if frame.pc_type == "PC16":
            fields = list(frame.payload_fields)
            remote_node = normalize_call(fields[0]) if fields else ""
            users = 0
            for item in fields[1:]:
                text = str(item or "").strip()
                if not text or text.upper().startswith("H"):
                    break
                users += 1
            await self._record_proto_state(
                peer_name,
                {
                    "pc16.node": remote_node,
                    "pc16.user_count": str(users),
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
            if not is_plausible_spot_call(dx_call) or not is_plausible_spotter_call(spotter):
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
            is_rbn = self._is_rbn_peer_spot(dx_call, spotter, msg.info, msg.raw_fields)
            if is_rbn and not await self._rbn_ingest_allowed_for_call(peer_name):
                await self.node_link.mark_policy_drop(peer_name, "ingest_rbn_disabled")
                return
            review_reasons = self._spot_review_reasons(dx_call, spotter)
            if review_reasons:
                LOG.info(
                    self._render_string(
                        "log.spot_call_review",
                        "spot call review: peer={peer} frame={frame} dx_call={dx_call} spotter={spotter} reasons={reasons}",
                        peer=peer_name,
                        frame="PC11",
                        dx_call=dx_call,
                        spotter=spotter,
                        reasons=",".join(review_reasons),
                    ),
                )
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
            if is_rbn:
                await self._publish_peer_rbn_spot(spot, exclude_peer=peer_name)
            elif await self.store.add_spot(spot):
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
            if not is_plausible_spot_call(dx_call) or not is_plausible_spotter_call(spotter):
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
            is_rbn = self._is_rbn_peer_spot(dx_call, spotter, msg.info, msg.raw_fields)
            if is_rbn and not await self._rbn_ingest_allowed_for_call(peer_name):
                await self.node_link.mark_policy_drop(peer_name, "ingest_rbn_disabled")
                return
            review_reasons = self._spot_review_reasons(dx_call, spotter)
            if review_reasons:
                LOG.info(
                    self._render_string(
                        "log.spot_call_review",
                        "spot call review: peer={peer} frame={frame} dx_call={dx_call} spotter={spotter} reasons={reasons}",
                        peer=peer_name,
                        frame="PC61",
                        dx_call=dx_call,
                        spotter=spotter,
                        reasons=",".join(review_reasons),
                    ),
                )
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
            if is_rbn:
                await self._publish_peer_rbn_spot(spot, exclude_peer=peer_name)
            elif await self.store.add_spot(spot):
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
            if category == "chat":
                await self._ingest_bulletin_from_peer(
                    peer_name,
                    category="chat",
                    scope=scope,
                    sender=sender,
                    body=body,
                    duplicate_reason="ingest_pc93_duplicate",
                )
            else:
                await self._ingest_bulletin_from_peer(
                    peer_name,
                    category=category,
                    scope=scope,
                    sender=sender,
                    body=body,
                    duplicate_reason="ingest_pc93_duplicate",
                )
            return

        if frame.pc_type == "PC23":
            msg = typed if isinstance(typed, Pc23Message) else Pc23Message.from_fields(frame.payload_fields)
            body = canonicalize_wwv_text(
                f"SFI={msg.sfi} A={msg.a_index} K={msg.k_index} {str(msg.forecast or '').strip()}".strip()
            )
            if not body:
                await self.node_link.mark_policy_drop(peer_name, "ingest_pc23_invalid")
                return
            await self._ingest_bulletin_from_peer(
                peer_name,
                category="wwv",
                scope="FULL",
                sender=msg.sender,
                body=body,
                duplicate_reason="ingest_pc23_duplicate",
            )
            return

        if frame.pc_type == "PC73":
            msg = typed if isinstance(typed, Pc73Message) else Pc73Message.from_fields(frame.payload_fields)
            body = canonicalize_wcy_text(
                ",".join(
                    [
                        f"k={msg.k_index}",
                        f"expk={msg.expk}",
                        f"a={msg.a_index}",
                        f"r={msg.sunspots}",
                        f"sf={msg.sfi}",
                        f"sa={msg.sun_activity}",
                        f"gmf={msg.geomagnetic_field}",
                        f"au={msg.aurora}",
                    ]
                )
            )
            if not body:
                await self.node_link.mark_policy_drop(peer_name, "ingest_pc73_invalid")
                return
            await self._ingest_bulletin_from_peer(
                peer_name,
                category="wcy",
                scope="FULL",
                sender=msg.sender,
                body=body,
                duplicate_reason="ingest_pc73_duplicate",
            )
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
            body = _VIA_SUFFIX_RE.sub("", body).strip() or body
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
            now_monotonic = time.monotonic()
            cutoff = now_monotonic - _TALK_DEDUPE_WINDOW_SECONDS
            self._recent_talk_ingest = {
                key: ts for key, ts in self._recent_talk_ingest.items() if ts >= cutoff
            }
            dedupe_key = (sender, recipient, body.casefold())
            if dedupe_key in self._recent_talk_ingest:
                await self.node_link.mark_policy_drop(peer_name, "ingest_pc10_duplicate")
                return
            self._recent_talk_ingest[dedupe_key] = now_monotonic
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
            origin_node = normalize_call(msg.origin or msg.from_node or peer_name)
            if origin_node == local_node:
                await self.node_link.mark_policy_drop(peer_name, "ingest_pc28_loop")
                return
            to_call = normalize_call(msg.to_call)
            if not is_valid_call(to_call):
                await self.node_link.mark_policy_drop(peer_name, "ingest_pc28_invalid_recipient")
                return
            stream = self._next_mail_stream()
            key = (normalize_call(msg.from_node or peer_name), stream)
            self._inbound_mail[key] = {
                "peer": peer_name,
                "from_node": normalize_call(msg.from_node or peer_name),
                "to_call": to_call,
                "from_call": normalize_call(msg.from_call),
                "subject": (msg.subject or "").strip(),
                "origin": origin_node,
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
            origin_node = str(state.get("origin") or normalize_call(peer_name))
            sender = from_call or normalize_call(peer_name)
            duplicate = await self.store.find_message_duplicate(
                sender=sender,
                recipient=to_call,
                body=body,
                origin_node=origin_node,
                now_epoch=now,
            )
            if duplicate is None:
                msg_id = await self.store.add_message(
                    sender=sender,
                    recipient=to_call,
                    epoch=now,
                    body=body,
                    parent_id=None,
                    origin_node=origin_node,
                    route_node="",
                    delivery_state="delivered",
                    delivered_epoch=now,
                )
                await self.telnet.publish_message(to_call, sender, body, msg_id)
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
            sender = msg.from_call or peer_name
            category, _classified_scope, classified_body = self._classify_pc93_bulletin(sender, body)
            if category == "chat":
                category = "wx" if (msg.wx_flag or "").strip() == "1" else "announce"
                classified_body = body
            scope = "SYSOP" if (msg.sysop_flag or "").strip() == "*" else "FULL"
            await self._ingest_bulletin_from_peer(
                peer_name,
                category=category,
                scope=scope,
                sender=sender,
                body=classified_body,
                duplicate_reason="ingest_pc12_duplicate",
            )

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
            ip=self._public_relay_ip(),
            hops_token="H1",
            trailer="",
        )
        frame = WirePcFrame("PC93", msg.to_fields())
        await self._broadcast_with_policy(sender, "chat", frame)

    async def _relay_bulletin_to_links(
        self,
        category: str,
        sender: str,
        scope: str,
        text: str,
        *,
        require_routepc19: bool = True,
        exclude_peer: str | None = None,
    ) -> None:
        if require_routepc19 and not await self._routepc19_enabled(sender):
            return
        if not await self._relay_category_enabled(sender, category):
            return
        names = await self.node_link.peer_names()
        profiles = await self._live_peer_profiles()
        sent = 0
        for name in names:
            if exclude_peer and name.lower() == exclude_peer.lower():
                continue
            if not await self._route_filter_allows_peer(sender, name):
                await self.node_link.mark_policy_drop(name, "route_filter")
                continue
            if not await self._relay_peer_enabled(sender, name, category):
                await self.node_link.mark_policy_drop(name, f"relay_peer_{category}_disabled")
                continue
            frame: WirePcFrame
            peer_profile = normalize_profile(str(profiles.get(name, "dxspider") or "dxspider"))
            if peer_profile == "dxspider" and category in {"announce", "wx"}:
                frame = WirePcFrame(
                    "PC12",
                    Pc12Message(
                        from_call=normalize_call(sender),
                        to_node="*",
                        text=f"{text} [via:{self.config.node.node_call}]",
                        sysop_flag="*" if scope.upper() == "SYSOP" else " ",
                        origin_node=normalize_call(self.config.node.node_call),
                        wx_flag="1" if category == "wx" else "0",
                        hops_token="H1",
                        trailer="~",
                    ).to_fields(),
                )
            elif peer_profile == "dxspider" and category in {"wwv", "wcy"}:
                if category == "wwv":
                    reading = parse_wwv_text(text)
                    if reading is not None:
                        frame = self._build_dxspider_wwv_frame(sender, reading)
                    else:
                        prefix = category.upper()
                        body = f"[{prefix}/{scope.upper()}] {text} [via:{self.config.node.node_call}]"
                        frame = WirePcFrame(
                            "PC93",
                            Pc93Message(
                                node_call=self.config.node.node_call,
                                metric="0",
                                star1="*",
                                origin_call=sender,
                                star2="*",
                                text=body,
                                extra="",
                                ip=self._public_relay_ip(),
                                hops_token="H1",
                                trailer="",
                            ).to_fields(),
                        )
                else:
                    reading = parse_wcy_text(text)
                    if reading is not None:
                        frame = self._build_dxspider_wcy_frame(sender, reading)
                    else:
                        prefix = category.upper()
                        body = f"[{prefix}/{scope.upper()}] {text} [via:{self.config.node.node_call}]"
                        frame = WirePcFrame(
                            "PC93",
                            Pc93Message(
                                node_call=self.config.node.node_call,
                                metric="0",
                                star1="*",
                                origin_call=sender,
                                star2="*",
                                text=body,
                                extra="",
                                ip=self._public_relay_ip(),
                                hops_token="H1",
                                trailer="",
                            ).to_fields(),
                        )
            else:
                prefix = category.upper()
                body = f"[{prefix}/{scope.upper()}] {text} [via:{self.config.node.node_call}]"
                frame = WirePcFrame(
                    "PC93",
                    Pc93Message(
                        node_call=self.config.node.node_call,
                        metric="0",
                        star1="*",
                        origin_call=sender,
                        star2="*",
                        text=body,
                        extra="",
                        ip=self._public_relay_ip(),
                        hops_token="H1",
                        trailer="",
                    ).to_fields(),
                )
            try:
                await self.node_link.send(name, frame)
            except Exception:
                LOG.exception("relay send failed peer=%s category=%s", name, category)
        return

    async def _relay_talk_to_links(self, sender: str, recipient: str, text: str, route_node: str | None = None) -> int:
        if not await self._routepc19_enabled(sender):
            return 0
        if not await self._relay_category_enabled(sender, "chat"):
            return 0
        target = normalize_call(recipient)
        if not is_valid_call(target):
            return 0
        requested_route = normalize_call(route_node or "")
        if requested_route and not is_valid_call(requested_route):
            return 0
        names = await self.node_link.peer_names()
        sent = 0
        for name in names:
            if requested_route and normalize_call(name) != requested_route:
                continue
            if not await self._route_filter_allows_peer(sender, name):
                await self.node_link.mark_policy_drop(name, "route_filter")
                continue
            if not await self._relay_peer_enabled(sender, name, "chat"):
                await self.node_link.mark_policy_drop(name, "relay_peer_chat_disabled")
                continue
            frame = WirePcFrame(
                "PC10",
                Pc10Message(
                    from_call=normalize_call(sender),
                    user1=requested_route or normalize_call(name),
                    text=f"{text} [via:{self.config.node.node_call}]",
                    star="*",
                    user2=target,
                    origin_node=normalize_call(self.config.node.node_call),
                    trailer="~",
                ).to_fields(),
            )
            try:
                await self.node_link.send(name, frame)
                sent += 1
            except Exception:
                LOG.exception("relay send failed peer=%s category=talk recipient=%s", name, target)
        return sent

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
        desired = await self._desired_peer_targets()
        live = set(await self.node_link.peer_names())
        if route_node not in desired and route_node not in live:
            reason = "No configured route to that peer."
            await self.store.set_message_delivery(
                int(row["id"]),
                "undeliverable",
                route_node=route_node,
                error_text=reason,
            )
            LOG.warning(
                self._render_string(
                    "log.cluster_mail_undeliverable",
                    "cluster mail undeliverable: id={message_id} route={route} reason={reason}",
                    message_id=message_id,
                    route=route_node,
                    reason=reason,
                )
            )
            return
        desired_row = desired.get(route_node, {})
        reconnect_enabled = str(desired_row.get("reconnect", "on")).strip().lower() in {"1", "on", "yes", "true"}
        if route_node not in live:
            reason = (
                "Peer is disconnected and reconnect is disabled."
                if desired_row and not reconnect_enabled
                else "Peer is disconnected and queued for reconnect."
            )
            await self.store.set_message_delivery(
                message_id,
                "pending",
                route_node=route_node,
                error_text=reason,
            )
            LOG.info(
                self._render_string(
                    "log.cluster_mail_pending",
                    "cluster mail pending: id={message_id} route={route} reason={reason}",
                    message_id=message_id,
                    route=route_node,
                    reason=reason,
                )
            )
            return
        try:
            await self._start_outbound_mail(route_node, row)
        except Exception as exc:
            await self.store.set_message_delivery(message_id, "pending", route_node=route_node, error_text=str(exc))
            LOG.info(
                self._render_string(
                    "log.cluster_mail_pending",
                    "cluster mail pending: id={message_id} route={route} reason={reason}",
                    message_id=message_id,
                    route=route_node,
                    reason=exc,
                )
            )

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
        if self._is_rbn_spot_obj(spot):
            return
        sender = normalize_call(spot.spotter)
        if not await self._relay_category_enabled(sender, "spots"):
            return
        dt = datetime.fromtimestamp(spot.epoch, tz=timezone.utc)
        source_node = normalize_call(spot.source_node) if spot.source_node else normalize_call(self.config.node.node_call)
        relay_ip = self._public_relay_ip()
        pc61 = Pc61Message(
            freq_khz=f"{spot.freq_khz:.1f}",
            dx_call=spot.dx_call,
            date_token=dt.strftime("%-d-%b-%Y"),
            time_token=dt.strftime("%H%MZ"),
            info=spot.info,
            spotter=sender,
            source_node=source_node,
            ip=relay_ip,
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
        stats = await self.node_link.stats()
        for name in names:
            if exclude_peer and normalize_call(name) == normalize_call(exclude_peer):
                continue
            if not await self._route_filter_allows_peer(sender, name):
                await self.node_link.mark_policy_drop(name, "route_filter")
                continue
            if not await self._relay_peer_enabled(sender, name, "spots"):
                await self.node_link.mark_policy_drop(name, "relay_peer_spots_disabled")
                continue
            profile = str(stats.get(name, {}).get("profile") or "").strip().lower()
            frame = pc11_frame if name in self._legacy_dxspider_peers or profile in {"spider", "dxspider"} else pc61_frame
            try:
                await self.node_link.send(name, frame)
            except Exception:
                LOG.exception("relay send failed peer=%s category=spots", name)

    def _public_relay_ip(self) -> str:
        for raw in (self._runtime_public_ip_address, self._runtime_public_ipv6_address):
            if valid_global_ip(raw):
                return str(raw)
        return "127.0.0.1"

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
        now = time.monotonic()
        if now - self._proto_trace_level_checked_monotonic >= 5.0:
            self._proto_trace_level_checked_monotonic = now
            try:
                configured = await self.store.get_user_pref(
                    self.config.node.node_call,
                    "retention.proto_log_level",
                )
            except Exception:
                LOG.exception("protocol trace preference lookup failed")
            else:
                level = str(configured or "full").strip().lower()
                self._proto_trace_level = level if level in {"full", "events", "off"} else "full"

        if self._proto_trace_level == "off":
            return
        if self._proto_trace_level == "events":
            if direction in {"rx", "tx"}:
                return
            if direction == "drop" and text.startswith(("profile_rx_block ", "profile_tx_block ")):
                text = text.split(" ", 1)[0]

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


async def serve_public_forever(config: AppConfig, config_path: str | None = None) -> None:
    store = SpotStore(config.store.sqlite_path)
    public_web = PublicWebServer(config=config, store=store, started_at=datetime.now(timezone.utc), config_path=config_path)
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
