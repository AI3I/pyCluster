from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import re
import secrets
import subprocess
import time
from urllib.parse import parse_qs, urlparse

from .access_policy import ACCESS_CAPABILITIES, ACCESS_CHANNELS, default_access_allowed
from .auth import hash_password, is_password_hash, verify_password
from .config import AppConfig, node_presentation_defaults, parse_telnet_ports, save_config
from .auth_logging import AUTHFAIL_LOG_PATH, log_auth_failure
from .geocode import estimate_location_from_locator, resolve_location_to_coords
from .maidenhead import coords_to_locator, extract_locator
from .models import Spot, is_valid_call, normalize_call
from .pathmeta import describe_session_path, describe_transport_dsn
from .spot_throttle import check_spot_throttle
from .store import SpotStore


LOG = logging.getLogger(__name__)
_AUTHFAIL_RE = re.compile(
    r"^(?P<when>\S+\s+\S+)\s+\w+\s+AUTHFAIL channel=(?P<channel>[a-z-]+)\s+ip=(?P<ip>\S+)\s+call=(?P<call>\S+)\s+reason=(?P<reason>[a-z_]+)$"
)
_CONFIG_AUTH_NODE_FIELDS = {
    "node_call",
    "node_alias",
    "owner_name",
    "qth",
    "node_locator",
    "branding_name",
    "welcome_title",
    "welcome_body",
    "login_tip",
    "show_status_after_login",
    "require_password",
    "support_contact",
    "website_url",
    "motd",
    "prompt_template",
    "telnet_ports",
}


def _is_valid_admin_record_call(call: str) -> bool:
    raw = str(call or "").strip().upper()
    return raw == "SYSOP" or is_valid_call(raw)


class WebAdminServer:
    def __init__(
        self,
        config: AppConfig,
        store: SpotStore,
        started_at: datetime,
        session_count_fn,
        active_calls_fn=None,
        link_stats_fn=None,
        link_desired_peers_fn=None,
        link_clear_policy_fn=None,
        link_connect_fn=None,
        link_disconnect_fn=None,
        link_set_profile_fn=None,
        link_save_peer_fn=None,
        publish_spot_fn=None,
        relay_spot_fn=None,
        publish_chat_fn=None,
        relay_chat_fn=None,
        publish_bulletin_fn=None,
        relay_bulletin_fn=None,
        telnet_rebind_fn=None,
        event_log_fn=None,
        audit_rows_fn=None,
        config_path: str | None = None,
    ) -> None:
        self.config = config
        self.store = store
        self.started_at = started_at
        self.session_count_fn = session_count_fn
        self.active_calls_fn = active_calls_fn
        self.link_stats_fn = link_stats_fn
        self.link_desired_peers_fn = link_desired_peers_fn
        self.link_clear_policy_fn = link_clear_policy_fn
        self.link_connect_fn = link_connect_fn
        self.link_disconnect_fn = link_disconnect_fn
        self.link_set_profile_fn = link_set_profile_fn
        self.link_save_peer_fn = link_save_peer_fn
        self.publish_spot_fn = publish_spot_fn
        self.relay_spot_fn = relay_spot_fn
        self.publish_chat_fn = publish_chat_fn
        self.relay_chat_fn = relay_chat_fn
        self.publish_bulletin_fn = publish_bulletin_fn
        self.relay_bulletin_fn = relay_bulletin_fn
        self.telnet_rebind_fn = telnet_rebind_fn
        self.event_log_fn = event_log_fn
        self.audit_rows_fn = audit_rows_fn
        self.config_path = str(config_path).strip() if config_path else ""
        self._web_sessions: dict[str, tuple[str, int, bool]] = {}
        self._server: asyncio.AbstractServer | None = None

    def _audit(self, category: str, text: str) -> None:
        if self.event_log_fn:
            try:
                self.event_log_fn(category, text)
            except Exception:
                LOG.exception("web admin audit log failed")

    async def _node_presentation(self) -> dict[str, str]:
        data = node_presentation_defaults(self.config.node)
        prefs = await self.store.list_user_prefs(self.config.node.node_call)
        for key in _CONFIG_AUTH_NODE_FIELDS:
            prefs.pop(key, None)
        data.update(prefs)
        return data

    def _node_presentation_json(self, data: dict[str, str]) -> dict[str, object]:
        def _to_int(v: str | object, default: int = 0) -> int:
            try:
                return int(str(v or "").strip())
            except Exception:
                return default
        return {
            "node_call": str(data.get("node_call", "")).strip(),
            "node_alias": str(data.get("node_alias", "")).strip(),
            "owner_name": str(data.get("owner_name", "")).strip(),
            "qth": str(data.get("qth", "")).strip(),
            "node_locator": str(data.get("node_locator", "")).strip().upper(),
            "branding_name": str(data.get("branding_name", "")).strip(),
            "welcome_title": str(data.get("welcome_title", "")).strip(),
            "welcome_body": str(data.get("welcome_body", "")).strip(),
            "login_tip": str(data.get("login_tip", "")).strip(),
            "show_status_after_login": str(data.get("show_status_after_login", "")).strip().lower() in {"1", "on", "yes", "true"},
            "require_password": str(data.get("require_password", "")).strip().lower() in {"1", "on", "yes", "true"},
            "support_contact": str(data.get("support_contact", "")).strip(),
            "website_url": str(data.get("website_url", "")).strip(),
            "motd": str(data.get("motd", "")).rstrip(),
            "prompt_template": str(data.get("prompt_template", "")).strip(),
            "telnet_ports": (
                str(data.get("telnet_ports", "")).strip()
                or ",".join(str(p) for p in (self.config.telnet.ports or (self.config.telnet.port,)))
            ),
            "retention_enabled": str(data.get("retention.enabled", "on")).strip().lower() in {"1", "on", "yes", "true"},
            "retention_spots_days": max(1, min(3650, _to_int(data.get("retention.spots_days"), 30))),
            "retention_messages_days": max(1, min(3650, _to_int(data.get("retention.messages_days"), 90))),
            "retention_bulletins_days": max(1, min(3650, _to_int(data.get("retention.bulletins_days"), 30))),
            "retention_last_run_epoch": _to_int(data.get("retention.last_run_epoch"), 0),
            "retention_last_result": str(data.get("retention.last_result", "")).strip(),
        }

    async def _user_registry_json(self, row) -> dict[str, object]:
        call = str(row["call"] or "").strip().upper()
        password = await self.store.get_user_pref(call, "password")
        homenode_pref = await self.store.get_user_pref(call, "homenode")
        node_family = str(await self.store.get_user_pref(call, "node_family") or "").strip().lower()
        if node_family not in {"pycluster", "dxspider", "dxnet", "arcluster", "clx"}:
            node_family = ""
        base_call = call.split("-", 1)[0]
        blocked_login = False
        blocked_reason = ""
        user_note = ""
        for candidate in (call, base_call):
            if not user_note:
                user_note = str(await self.store.get_user_pref(candidate, "note") or "").strip()
            raw_block = await self.store.get_user_pref(candidate, "blocked_login")
            if str(raw_block or "").strip().lower() in {"1", "on", "yes", "true"}:
                blocked_login = True
                blocked_reason = str(await self.store.get_user_pref(candidate, "blocked_reason") or "").strip()
                break
        privilege = str(row["privilege"] or "").strip().lower()
        if privilege == "admin":
            privilege = "sysop"
        if blocked_login:
            access_label = "Blocked"
        elif privilege == "sysop":
            access_label = "System Operator"
        elif privilege == "user":
            access_label = "Authenticated"
        else:
            access_label = "Non-Authenticated"
        telnet_online = False
        if self.active_calls_fn:
            try:
                active_calls = {str(c).strip().upper() for c in self.active_calls_fn()}
                telnet_online = any(
                    active == call or active.split("-", 1)[0] == base_call
                    for active in active_calls
                )
            except Exception:
                telnet_online = False
        self._cleanup_web_sessions()
        web_online = any(
            (sess_call == call or sess_call.split("-", 1)[0] == base_call) and bool(is_sysop) and exp > int(time.time())
            for sess_call, exp, is_sysop in self._web_sessions.values()
        )
        if telnet_online and web_online:
            online_status = "Telnet + Web"
        elif telnet_online:
            online_status = "Telnet"
        elif web_online:
            online_status = "System Operator Web"
        else:
            online_status = "Offline"
        access = await self._access_snapshot(call)
        return {
            "call": call,
            "display_name": str(row["display_name"] or ""),
            "home_node": str(homenode_pref or row["home_node"] or ""),
            "node_family": node_family,
            "address": str(row["address"] or ""),
            "qth": str(row["qth"] or ""),
            "qra": str(row["qra"] or ""),
            "email": str(row["email"] or ""),
            "privilege": privilege,
            "access_label": access_label,
            "last_login_epoch": int(row["last_login_epoch"] or 0),
            "last_login_peer": str(row["last_login_peer"] or ""),
            "registered_epoch": int(row["registered_epoch"] or 0),
            "updated_epoch": int(row["updated_epoch"] or 0),
            "has_password": bool(str(password or "").strip()),
            "blocked_login": blocked_login,
            "blocked_reason": blocked_reason or ("Blocked by local policy" if blocked_login else ""),
            "user_note": user_note or blocked_reason,
            "access": access,
            "access_login_summary": self._access_login_summary(access),
            "access_post_summary": self._access_post_summary(access),
            "telnet_online": telnet_online,
            "web_online": web_online,
            "online_status": online_status,
        }

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle,
            host=self.config.web.host,
            port=self.config.web.port,
            limit=16384,
        )
        addrs = ", ".join(str(s.getsockname()) for s in (self._server.sockets or []))
        LOG.info("Web admin listening on %s", addrs)

    async def stop(self) -> None:
        if self._server:
            self._server.close()
            try:
                await asyncio.wait_for(self._server.wait_closed(), timeout=1.0)
            except (asyncio.TimeoutError, ConnectionError, OSError):
                pass

    async def _write_response(
        self,
        writer: asyncio.StreamWriter,
        status: int,
        body: bytes,
        content_type: str = "application/json; charset=utf-8",
    ) -> None:
        reason = {
            200: "OK",
            400: "Bad Request",
            401: "Unauthorized",
            403: "Forbidden",
            404: "Not Found",
            405: "Method Not Allowed",
            429: "Too Many Requests",
            500: "Internal Server Error",
        }.get(status, "OK")
        headers = [
            f"HTTP/1.1 {status} {reason}\r\n",
            f"Content-Type: {content_type}\r\n",
            f"Content-Length: {len(body)}\r\n",
            "Cache-Control: no-store, no-cache, must-revalidate, max-age=0\r\n",
            "Pragma: no-cache\r\n",
            "Expires: 0\r\n",
            "Connection: close\r\n",
            "\r\n",
        ]
        writer.write("".join(headers).encode("ascii") + body)
        await writer.drain()

    def _json(self, obj) -> bytes:
        return json.dumps(obj, separators=(",", ":"), ensure_ascii=True).encode("utf-8")

    def _parse_limit(self, q: dict[str, list[str]], key: str, default: int, low: int, high: int) -> int:
        if key not in q or not q[key]:
            return default
        try:
            n = int(q[key][0])
        except ValueError:
            return default
        return max(low, min(high, n))

    def _is_authorized(self, headers: dict[str, str]) -> bool:
        return self._admin_call_from_headers(headers) is not None

    def _authorized_call(self, headers: dict[str, str]) -> str:
        return self._admin_call_from_headers(headers) or "SYSOP"

    def _cleanup_web_sessions(self) -> None:
        now = int(time.time())
        stale = [k for k, (_call, exp, _is_sysop) in self._web_sessions.items() if exp <= now]
        for k in stale:
            self._web_sessions.pop(k, None)

    def _issue_web_token(self, call: str, ttl_seconds: int = 8 * 3600, *, is_sysop: bool = False) -> tuple[str, int]:
        tok = secrets.token_urlsafe(24)
        exp = int(time.time()) + max(300, ttl_seconds)
        self._web_sessions[tok] = (call.upper(), exp, bool(is_sysop))
        return tok, exp

    def _revoke_web_token(self, headers: dict[str, str]) -> None:
        tok = headers.get("x-web-token", "").strip()
        auth = headers.get("authorization", "").strip()
        if not tok and auth.lower().startswith("bearer "):
            tok = auth[7:].strip()
        if tok:
            self._web_sessions.pop(tok, None)

    def _web_call_from_headers(self, headers: dict[str, str]) -> str | None:
        self._cleanup_web_sessions()
        tok = headers.get("x-web-token", "").strip()
        auth = headers.get("authorization", "").strip()
        if not tok and auth.lower().startswith("bearer "):
            tok = auth[7:].strip()
        if not tok:
            return None
        row = self._web_sessions.get(tok)
        if not row:
            return None
        call, exp, _is_sysop = row
        if exp <= int(time.time()):
            self._web_sessions.pop(tok, None)
            return None
        return call

    def _admin_call_from_headers(self, headers: dict[str, str]) -> str | None:
        self._cleanup_web_sessions()
        tok = headers.get("x-web-token", "").strip()
        auth = headers.get("authorization", "").strip()
        if not tok and auth.lower().startswith("bearer "):
            tok = auth[7:].strip()
        if not tok:
            return None
        row = self._web_sessions.get(tok)
        if not row:
            return None
        call, exp, is_sysop = row
        if exp <= int(time.time()):
            self._web_sessions.pop(tok, None)
            return None
        return call if is_sysop else None

    async def _admin_privileged_call(self, call: str) -> bool:
        row = await self.store.get_user_registry(call)
        level = ""
        if row:
            level = str(row["privilege"] or "").strip().lower()
        if not level:
            level = str(await self.store.get_user_pref(call, "privilege") or "").strip().lower()
        return level == "sysop"

    def _is_on_value(self, value: str) -> bool:
        return str(value or "").strip().lower() in {"1", "on", "yes", "true"}

    def _access_pref_key(self, channel: str, capability: str) -> str:
        return f"access.{channel}.{capability}"

    async def _access_subject(self, call: str) -> tuple[str, bool]:
        target = call.upper()
        base = target.split("-", 1)[0]
        blocked_login = False
        privilege = ""
        for candidate in (target, base):
            raw_block = await self.store.get_user_pref(candidate, "blocked_login")
            if str(raw_block or "").strip().lower() in {"1", "on", "yes", "true"}:
                blocked_login = True
            row = await self.store.get_user_registry(candidate)
            if row and not privilege:
                privilege = str(row["privilege"] or "").strip().lower()
            if not privilege:
                privilege = str(await self.store.get_user_pref(candidate, "privilege") or "").strip().lower()
        return privilege, blocked_login

    async def _access_allowed(self, call: str, channel: str, capability: str) -> bool:
        target = call.upper()
        base = target.split("-", 1)[0]
        for candidate in (target, base):
            raw = await self.store.get_user_pref(candidate, self._access_pref_key(channel, capability))
            if raw is None or str(raw).strip() == "":
                continue
            return self._is_on_value(str(raw))
        privilege, blocked_login = await self._access_subject(call)
        return default_access_allowed(privilege, blocked_login, channel, capability)

    def _access_channels(self) -> tuple[str, str]:
        return ACCESS_CHANNELS  # type: ignore[return-value]

    def _access_capabilities(self) -> tuple[str, str, str, str, str, str, str]:
        return ACCESS_CAPABILITIES  # type: ignore[return-value]

    async def _access_snapshot(self, call: str) -> dict[str, dict[str, bool]]:
        out: dict[str, dict[str, bool]] = {}
        for channel in self._access_channels():
            row: dict[str, bool] = {}
            for capability in self._access_capabilities():
                row[capability] = await self._access_allowed(call, channel, capability)
            out[channel] = row
        return out

    def _access_login_summary(self, access: dict[str, dict[str, bool]]) -> str:
        parts: list[str] = []
        for channel, short in (("telnet", "T"), ("web", "W")):
            parts.append(short if (access.get(channel, {}) or {}).get("login") else "-")
        return " ".join(parts)

    def _access_post_summary(self, access: dict[str, dict[str, bool]]) -> str:
        labels: list[str] = []
        for capability, short in (
            ("spots", "DX"),
            ("chat", "CH"),
            ("announce", "AN"),
            ("wx", "WX"),
            ("wcy", "WCY"),
            ("wwv", "WWV"),
        ):
            if any((access.get(channel, {}) or {}).get(capability) for channel in self._access_channels()):
                labels.append(short)
        return " ".join(labels) if labels else "-"

    def _read_recent_auth_failures(self, limit: int) -> list[dict[str, str]]:
        path = Path(AUTHFAIL_LOG_PATH)
        if not path.exists():
            return []
        try:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        except Exception:
            return []
        rows: list[dict[str, str]] = []
        for line in reversed(lines):
            m = _AUTHFAIL_RE.match(line.strip())
            if not m:
                continue
            rows.append(
                {
                    "when": m.group("when"),
                    "channel": m.group("channel"),
                    "ip": m.group("ip"),
                    "call": m.group("call"),
                    "reason": m.group("reason"),
                }
            )
            if len(rows) >= limit:
                break
        return rows

    def _fail2ban_ban_rows(self) -> list[dict[str, str]]:
        client = Path("/usr/bin/fail2ban-client")
        sudo = Path("/usr/bin/sudo")
        if not client.exists():
            return []
        rows: list[dict[str, str]] = []
        for jail in ("pycluster-core-auth", "pycluster-web-auth"):
            proc = None
            commands = [[str(client), "status", jail]]
            if sudo.exists():
                commands.append([str(sudo), "-n", str(client), "status", jail])
            for cmd in commands:
                try:
                    proc = subprocess.run(
                        cmd,
                        capture_output=True,
                        text=True,
                        timeout=3,
                        check=False,
                    )
                except Exception:
                    proc = None
                    continue
                if proc.returncode == 0:
                    break
            if not proc or proc.returncode != 0:
                continue
            banned: list[str] = []
            for line in proc.stdout.splitlines():
                if "Banned IP list:" not in line:
                    continue
                banned = [item for item in line.split(":", 1)[1].strip().split() if item]
                break
            for ip in banned:
                rows.append({"jail": jail, "ip": ip})
        return rows

    def _client_ip(self, headers: dict[str, str], writer: asyncio.StreamWriter) -> str:
        forwarded = str(headers.get("x-forwarded-for", "")).strip()
        if forwarded:
            return forwarded.split(",", 1)[0].strip() or "-"
        peer = writer.get_extra_info("peername") if hasattr(writer, "get_extra_info") else None
        if isinstance(peer, tuple) and peer:
            return str(peer[0] or "-")
        if peer is None:
            return "-"
        return str(peer)

    def _auth_log_call(self, call: str) -> str:
        raw = str(call or "").strip().upper()
        if not raw:
            return "-"
        return re.sub(r"[^A-Z0-9/-]+", "_", raw)

    def _log_auth_failure(self, writer: asyncio.StreamWriter, headers: dict[str, str], channel: str, call: str, reason: str) -> None:
        log_auth_failure(LOG, channel, self._client_ip(headers, writer), self._auth_log_call(call), reason)

    def _access_login_summary(self, access: dict[str, dict[str, bool]]) -> str:
        parts: list[str] = []
        for channel, short in (("telnet", "T"), ("web", "W")):
            parts.append(short if (access.get(channel, {}) or {}).get("login") else "-")
        return " ".join(parts)

    def _access_post_summary(self, access: dict[str, dict[str, bool]]) -> str:
        labels: list[str] = []
        for capability, short in (
            ("spots", "DX"),
            ("chat", "CH"),
            ("announce", "AN"),
            ("wx", "WX"),
            ("wcy", "WCY"),
            ("wwv", "WWV"),
        ):
            if any((access.get(channel, {}) or {}).get(capability) for channel in self._access_channels()):
                labels.append(short)
        return " ".join(labels) if labels else "-"

    def _parse_json_body(self, body: bytes) -> dict[str, object]:
        if not body:
            return {}
        try:
            obj = json.loads(body.decode("utf-8", errors="replace"))
        except Exception:
            return {}
        if not isinstance(obj, dict):
            return {}
        return obj

    async def _policy_drop_rows(self, peer_filter: str = "") -> list[dict[str, object]]:
        if not self.link_stats_fn:
            return []
        stats = await self.link_stats_fn()
        out: list[dict[str, object]] = []
        flt = peer_filter.lower()
        for name in sorted(stats):
            if flt and flt not in name.lower():
                continue
            st = stats[name]
            total = int(st.get("policy_dropped", 0))
            reasons_raw = st.get("policy_reasons") if isinstance(st, dict) else None
            reasons = reasons_raw if isinstance(reasons_raw, dict) else {}
            if total <= 0 and not reasons:
                continue
            out.append(
                {
                    "peer": name,
                    "total": total,
                    "loop_total": sum(
                        int(v)
                        for k, v in reasons.items()
                        if "loop" in str(k).lower()
                    ),
                    "reasons": {str(k): int(v) for k, v in reasons.items()},
                }
            )
        return out

    def _proto_thresholds(self, node_cfg: dict[str, str]) -> dict[str, int]:
        def _to_int(v: str | object, default: int) -> int:
            try:
                return int(str(v))
            except (TypeError, ValueError):
                return default

        return {
            "stale_mins": max(1, min(24 * 60, _to_int(node_cfg.get("proto.threshold.stale_mins"), 30))),
            "flap_score": max(1, min(999, _to_int(node_cfg.get("proto.threshold.flap_score"), 3))),
            "flap_window_secs": max(5, min(86400, _to_int(node_cfg.get("proto.threshold.flap_window_secs"), 300))),
        }

    def _proto_state_for_peer(self, node_cfg: dict[str, str], peer_name: str, now_epoch: int) -> dict[str, object]:
        ptag = re.sub(r"[^a-z0-9_.-]", "_", peer_name.lower())
        pfx = f"proto.peer.{ptag}."
        state = {
            "pc24_call": node_cfg.get(pfx + "pc24.call", ""),
            "pc24_flag": node_cfg.get(pfx + "pc24.flag", ""),
            "pc50_call": node_cfg.get(pfx + "pc50.call", ""),
            "pc50_count": node_cfg.get(pfx + "pc50.count", ""),
            "pc51_to": node_cfg.get(pfx + "pc51.to", ""),
            "pc51_from": node_cfg.get(pfx + "pc51.from", ""),
            "pc51_value": node_cfg.get(pfx + "pc51.value", ""),
            "last_epoch": node_cfg.get(pfx + "last_epoch", ""),
            "last_pc_type": node_cfg.get(pfx + "last_pc_type", ""),
            "change_count": node_cfg.get(pfx + "change_count", ""),
            "flap_score": node_cfg.get(pfx + "flap_score", ""),
            "last_change_epoch": node_cfg.get(pfx + "last_change_epoch", ""),
            "history": node_cfg.get(pfx + "history", "[]"),
        }

        def _to_int(v: str | object, default: int = 0) -> int:
            try:
                return int(str(v))
            except (TypeError, ValueError):
                return default

        thresholds = self._proto_thresholds(node_cfg)
        known = any(state[k] for k in ("pc24_call", "pc24_flag", "pc50_call", "pc50_count", "pc51_to", "pc51_from", "pc51_value"))
        health = "unknown"
        last_change_epoch = _to_int(state["last_change_epoch"], 0)
        flap_active = (
            last_change_epoch > 0
            and now_epoch - last_change_epoch <= thresholds["flap_window_secs"]
        )
        if known:
            health = "ok"
            if state["pc51_value"] and str(state["pc51_value"]).lower() in {"0", "off", "down", "fail"}:
                health = "degraded"
            if flap_active and _to_int(state["flap_score"], 0) >= thresholds["flap_score"]:
                health = "flapping"
        last_epoch = _to_int(state["last_epoch"], 0)
        age_min = ((now_epoch - last_epoch) // 60) if last_epoch > 0 else -1
        stale = last_epoch <= 0 or (now_epoch - last_epoch > thresholds["stale_mins"] * 60)
        if stale and known:
            health = "stale"
        hist: list[dict[str, object]] = []
        try:
            raw_hist = json.loads(str(state.get("history", "[]")))
            if isinstance(raw_hist, list):
                for item in raw_hist:
                    if isinstance(item, dict):
                        hist.append(
                            {
                                "epoch": _to_int(item.get("epoch", 0), 0),
                                "key": str(item.get("key", "")),
                                "from": str(item.get("from", "")),
                                "to": str(item.get("to", "")),
                            }
                        )
        except Exception:
            hist = []
        last_event = hist[-1] if hist else None
        return {
            "known": bool(known),
            "health": health,
            "age_min": age_min,
            "last_epoch": last_epoch,
            "last_pc_type": state["last_pc_type"],
            "change_count": _to_int(state["change_count"], 0),
            "flap_score": _to_int(state["flap_score"], 0),
            "last_change_epoch": last_change_epoch,
            "flap_active": flap_active,
            "history_count": len(hist),
            "last_event": last_event,
            "pc24": {"call": state["pc24_call"], "flag": state["pc24_flag"]},
            "pc50": {"call": state["pc50_call"], "count": state["pc50_count"]},
            "pc51": {"to": state["pc51_to"], "from": state["pc51_from"], "value": state["pc51_value"]},
        }

    def _render_index_html(self) -> str:
        return """<!doctype html>
<html lang="en" class="dark">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>pyCluster System Operator Console</title>
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='88'>📡</text></svg>">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Exo:ital,wght@0,300;0,400;0,500;0,600;0,700;1,400&display=swap">
<style>
:root{
  --bg:#090d13;
  --panel:#0d1320;
  --panel-strong:#111827;
  --panel-soft:#0f1724;
  --panel-ink:#0b1018;
  --line:#1a2d42;
  --text:#e6edf3;
  --muted:#6e7f94;
  --accent:#58a6ff;
  --accent-soft:rgba(88,166,255,.12);
  --warn:#c96b49;
  --header-text:#e6edf3;
  --header-border:#1e3a5f;
  --header-subtle:#8ba5be;
  --pill-bg:rgba(255,255,255,.04);
  --pill-border:#1e2d40;
  --pill-text:#e6edf3;
  --input-bg:rgba(255,255,255,.04);
  --input-border:#1e2d3d;
  --button-text:#08111b;
  --secondary-bg:rgba(255,255,255,.03);
  --secondary-border:#1e2d3d;
  --theme-bg:rgba(255,255,255,.05);
  --theme-border:#1e2d40;
  --status-ok-text:#c9d8e8;
  --status-bad-bg:#f4ddd5;
  --status-bad-text:#7d2e17;
  --table-border:rgba(255,255,255,.04);
  --panel-header-border:rgba(255,255,255,.05);
  --tag-bg:rgba(255,255,255,.04);
  --tag-text:#c9d8e8;
  --unknown-bg:rgba(255,255,255,.06);
  --unknown-text:#c9d8e8;
  --header-bg:linear-gradient(160deg,#0d1420 0%,#111827 60%,#0a1628 100%);
  --header-wave:linear-gradient(90deg,transparent,#1d6fa4 30%,#58a6ff 50%,#1d6fa4 70%,transparent);
  --mono:"Iosevka Fixed","DejaVu Sans Mono",Consolas,monospace;
  --sans:"Exo",system-ui,Arial,sans-serif;
}
html.light{
  --bg:#f3f4f6;
  --panel:#ffffff;
  --panel-strong:#f0f5ff;
  --panel-soft:#ffffff;
  --panel-ink:#f3f4f6;
  --line:#e5e7eb;
  --text:#111827;
  --muted:#6b7280;
  --accent:#1d6fa4;
  --accent-soft:rgba(29,111,164,.10);
  --warn:#b95732;
  --header-text:#111827;
  --header-border:#d7e4f4;
  --header-subtle:#5a7a99;
  --pill-bg:rgba(0,0,0,.03);
  --pill-border:#d1d5db;
  --pill-text:#111827;
  --input-bg:rgba(0,0,0,.03);
  --input-border:#d1d5db;
  --button-text:#ffffff;
  --secondary-bg:rgba(0,0,0,.03);
  --secondary-border:#d1d5db;
  --theme-bg:rgba(0,0,0,.04);
  --theme-border:#d1d5db;
  --status-ok-text:#174434;
  --status-bad-bg:#f4ddd5;
  --status-bad-text:#7d2e17;
  --table-border:#e5e7eb;
  --panel-header-border:#e5e7eb;
  --tag-bg:rgba(0,0,0,.06);
  --tag-text:#4b5563;
  --unknown-bg:rgba(0,0,0,.06);
  --unknown-text:#4b5563;
  --header-bg:linear-gradient(160deg,#ffffff 0%,#f0f5ff 60%,#e8f0fa 100%);
  --header-wave:linear-gradient(90deg,transparent,#5a90c8 30%,#1d6fa4 50%,#5a90c8 70%,transparent);
}
*{box-sizing:border-box}
body{
  margin:0;
  background:var(--bg);
  color:var(--text);
  font:15px/1.45 var(--sans);
  transition:background .25s,color .25s;
}
.shell{
  max-width:1400px;
  margin:0 auto;
  padding:18px 18px 34px;
}
.mast{
  display:flex;
  justify-content:space-between;
  gap:16px;
  align-items:flex-end;
  padding:18px 20px;
  background:var(--header-bg);
  color:var(--header-text);
  border-radius:18px;
  border:1px solid var(--header-border);
  box-shadow:0 14px 36px rgba(0,0,0,.28);
  position:relative;
  overflow:hidden;
}
.mast::after{
  content:'';
  position:absolute;
  left:0;
  right:0;
  bottom:0;
  height:2px;
  background:var(--header-wave);
  opacity:.75;
}
.mast h1{
  margin:0;
  font-size:30px;
  line-height:1.05;
  letter-spacing:.02em;
  background:linear-gradient(90deg,#58a6ff 20%,#c8e4ff 48%,#ffffff 52%,#c8e4ff 57%,#58a6ff 80%);
  background-size:400% auto;
  -webkit-background-clip:text;
  -webkit-text-fill-color:transparent;
  background-clip:text;
  filter:drop-shadow(0 0 10px rgba(88,166,255,.4));
}
.mast-actions{
  display:flex;
  flex-direction:column;
  gap:10px;
  align-items:flex-end;
  min-width:280px;
}
.mast p{
  margin:6px 0 0;
  color:var(--header-subtle);
}
.pillbar{
  display:flex;
  flex-wrap:wrap;
  gap:8px;
  justify-content:flex-end;
}
.pill{
  display:inline-flex;
  align-items:center;
  gap:6px;
  padding:7px 10px;
  border-radius:999px;
  background:var(--pill-bg);
  border:1px solid var(--pill-border);
  color:var(--pill-text);
  font-size:12px;
}
.stack{display:grid;gap:20px}
.workspace{
  display:grid;
  grid-template-columns:280px minmax(0,1fr);
  gap:18px;
  margin-top:18px;
  align-items:start;
}
.sidebar{
  position:sticky;
  top:18px;
  display:grid;
  gap:14px;
}
.sidebar-panel{
  background:var(--panel);
  border:1px solid var(--line);
  border-radius:16px;
  box-shadow:0 10px 22px rgba(0,0,0,.12);
  overflow:hidden;
}
html.light .sidebar-panel{ box-shadow:0 10px 24px rgba(17,24,39,.06); }
.sidebar-panel h2{
  margin:0;
  font-size:14px;
  letter-spacing:.03em;
  text-transform:uppercase;
}
.sidebar-panel header{
  padding:14px 16px 10px;
  background:linear-gradient(180deg,var(--panel-strong),var(--panel));
  border-bottom:1px solid var(--panel-header-border);
}
.sidebar-panel .body{
  padding:14px 16px 16px;
}
.sidebar-nav{
  display:grid;
  gap:8px;
}
.sidebar-nav a{
  display:flex;
  justify-content:space-between;
  align-items:center;
  gap:10px;
  padding:11px 12px;
  border-radius:12px;
  border:1px solid var(--line);
  text-decoration:none;
  color:var(--text);
  background:var(--panel-soft);
}
.sidebar-nav a:hover{
  border-color:var(--accent);
  background:var(--accent-soft);
}
.sidebar-nav a.active{
  border-color:var(--accent);
  background:var(--accent-soft);
}
.sidebar-nav span{
  font-size:11px;
  text-transform:uppercase;
  letter-spacing:.06em;
  color:var(--muted);
}
.sidebar-metrics{
  display:grid;
  grid-template-columns:repeat(2,minmax(0,1fr));
  gap:10px;
}
.sidebar-metric{
  padding:12px;
  border:1px solid var(--line);
  border-radius:12px;
  background:var(--panel-soft);
}
.sidebar-metric label{
  display:block;
  margin-bottom:6px;
  font-size:11px;
  text-transform:uppercase;
  letter-spacing:.06em;
  color:var(--muted);
}
.sidebar-metric strong{
  font-size:22px;
}
.sidebar-heading{
  display:flex;
  align-items:center;
  justify-content:space-between;
  gap:12px;
}
.node-badge{
  display:inline-block;
  font-size:16px;
  font-weight:900;
  letter-spacing:.1em;
  text-transform:uppercase;
  color:#ffde8a;
  text-shadow:0 0 10px rgba(255,212,111,.35), 0 0 18px rgba(255,212,111,.18);
}
html.light .node-badge{
  color:#8a5f00;
  text-shadow:0 0 8px rgba(166,118,0,.18);
}
.panel{
  background:var(--panel);
  border:1px solid var(--line);
  border-radius:16px;
  box-shadow:0 10px 22px rgba(0,0,0,.12);
  overflow:hidden;
}
html.light .panel{ box-shadow:0 10px 24px rgba(17,24,39,.06); }
.panel header{
  display:flex;
  justify-content:space-between;
  align-items:center;
  gap:10px;
  padding:15px 18px 11px;
  background:linear-gradient(180deg,var(--panel-strong),var(--panel));
  border-bottom:1px solid var(--panel-header-border);
}
.panel h2,.panel h3{
  margin:0;
  font-size:16px;
  letter-spacing:.01em;
}
.panel .body{
  padding:16px 18px 18px;
}
.subtle{
  color:var(--muted);
  font-size:12.5px;
}
.metric-grid{
  display:grid;
  grid-template-columns:repeat(6,minmax(0,1fr));
  gap:10px;
}
.metric{
  padding:12px;
  border:1px solid var(--line);
  border-radius:12px;
  background:var(--panel-soft);
}
.metric label{
  display:block;
  font-size:11px;
  text-transform:uppercase;
  letter-spacing:.06em;
  color:var(--muted);
  margin-bottom:6px;
}
.metric strong{
  font-size:24px;
  font-weight:600;
}
.form-grid{
  display:grid;
  grid-template-columns:repeat(2,minmax(0,1fr));
  gap:12px;
}
.form-grid.one{grid-template-columns:1fr}
.field.span2{grid-column:1 / -1}
.field{
  display:grid;
  gap:6px;
}
.checkgrid{
  display:grid;
  grid-template-columns:repeat(2,minmax(0,1fr));
  gap:12px;
}
.checkrow{
  display:flex;
  align-items:flex-start;
  gap:10px;
  padding:11px 12px;
  border:1px solid var(--input-border);
  background:var(--input-bg);
  border-radius:10px;
}
.checkrow.attention{
  border-color:rgba(251,191,36,.5);
  background:linear-gradient(180deg, rgba(251,191,36,.14), rgba(245,158,11,.08));
  box-shadow:inset 0 0 0 1px rgba(251,191,36,.1);
}
.checkrow input{
  width:auto;
  margin:2px 0 0;
  padding:0;
}
.checkrow label{
  margin:0;
  font-size:13px;
  font-weight:600;
  color:var(--text);
}
.checkrow.attention label{
  color:var(--text-primary);
}
html.light .checkrow.attention{
  border-color:rgba(217,119,6,.38);
  background:linear-gradient(180deg, rgba(251,191,36,.20), rgba(251,191,36,.10));
}
.field label{
  font-size:12.5px;
  font-weight:600;
  color:var(--muted);
}
input,textarea,select,button{
  font:inherit;
}
input,textarea,select{
  width:100%;
  padding:11px 12px;
  border-radius:10px;
  border:1px solid var(--input-border);
  background:var(--input-bg);
  color:var(--text);
}
select{
  appearance:none;
  -webkit-appearance:none;
  -moz-appearance:none;
  display:block;
  box-sizing:border-box;
  min-width:0;
  height:44px;
  min-height:44px;
  line-height:1.2;
  padding-right:38px;
  background-color:var(--input-bg);
  background-image:
    linear-gradient(45deg, transparent 50%, var(--muted) 50%),
    linear-gradient(135deg, var(--muted) 50%, transparent 50%);
  background-position:
    calc(100% - 18px) calc(50% - 2px),
    calc(100% - 12px) calc(50% - 2px);
  background-size:6px 6px, 6px 6px;
  background-repeat:no-repeat;
}
select::-ms-expand{
  display:none;
}
.select-shell{
  position:relative;
}
.select-shell::after{
  content:"";
  position:absolute;
  right:14px;
  top:50%;
  width:8px;
  height:8px;
  border-right:2px solid var(--muted);
  border-bottom:2px solid var(--muted);
  transform:translateY(-60%) rotate(45deg);
  pointer-events:none;
}
.select-shell select{
  background-image:none !important;
}
select option{
  color:#111827;
  background:#ffffff;
}
#user_privilege,
#user_node_family{
  height:44px;
  min-height:44px;
  line-height:1.2;
}
#peerpass{
  display:block;
  box-sizing:border-box;
  min-width:0;
  max-width:100%;
  height:44px;
  min-height:44px;
  line-height:1.2;
  padding-right:12px;
  appearance:none;
  -webkit-appearance:none;
  font:inherit;
  -webkit-text-security:disc;
}
#peerpass::-ms-reveal,
#peerpass::-ms-clear{
  display:none;
}
#peerpass::-webkit-credentials-auto-fill-button,
#peerpass::-webkit-strong-password-auto-fill-button{
  visibility:hidden;
  display:none !important;
  pointer-events:none;
}
textarea{
  min-height:104px;
  resize:vertical;
}
button{
  cursor:pointer;
  padding:10px 14px;
  border-radius:10px;
  border:1px solid var(--accent);
  background:var(--accent);
  color:var(--button-text);
  font-weight:600;
}
button:hover{ filter:brightness(1.04); }
button:active{
  transform:translateY(1px) scale(.99);
  filter:brightness(.96);
}
button:disabled{
  cursor:progress;
  opacity:.82;
}
button.busy{
  filter:saturate(1.15) brightness(.92);
  box-shadow:inset 0 0 0 999px rgba(255,255,255,.08);
}
button.done{
  background:#1d6d49;
  border-color:#1d6d49;
  color:#fff;
}
button.secondary.done{
  background:#234e3d;
  border-color:#234e3d;
}
button.warn.done{
  background:#1d6d49;
  border-color:#1d6d49;
}
button.failed{
  background:#8b2e2e;
  border-color:#8b2e2e;
  color:#fff;
}
button.secondary.failed{
  background:#6f3232;
  border-color:#6f3232;
}
button.secondary{
  background:var(--secondary-bg);
  color:var(--text);
  border-color:var(--secondary-border);
}
button.warn{
  background:var(--warn);
  border-color:#8c4529;
  color:#fff;
}
#newUser{
  background:#1d6d49;
  border-color:#1d6d49;
  color:#fff;
}
#saveUser{
  background:#58a6ff;
  border-color:#58a6ff;
  color:#08111b;
}
#saveUserPassword{
  background:#6f42c1;
  border-color:#6f42c1;
  color:#fff;
}
#accessAll{
  background:#1d6d49;
  border-color:#1d6d49;
  color:#fff;
}
#accessNone{
  background:#8c4529;
  border-color:#8c4529;
  color:#fff;
}
#peerSave{
  background:#1d6d49;
  border-color:#1d6d49;
  color:#fff;
}
#newPeer{
  background:#fbbf24;
  border-color:#f59e0b;
  color:#201102;
}
#reset{
  background:#6f42c1;
  border-color:#6f42c1;
  color:#fff;
}
.themebtn{
  background:var(--theme-bg);
  border-color:var(--theme-border);
  color:var(--text);
}
.actions{
  display:flex;
  flex-wrap:wrap;
  gap:10px;
}
.mast-actions .actions{
  justify-content:flex-end;
}
.statusline{
  margin-top:12px;
  padding:10px 12px;
  border-radius:10px;
  background:var(--accent-soft);
  color:var(--status-ok-text);
  font-weight:600;
}
.mast-actions .statusline{
  margin-top:0;
  width:100%;
}
.statusline.error{
  background:#f4ddd5;
  color:#7d2e17;
}
.split{
  display:grid;
  grid-template-columns:repeat(2,minmax(0,1fr));
  gap:18px;
}
.users-editor{
  display:grid;
  grid-template-columns:minmax(0,1.2fr) minmax(320px,.8fr);
  gap:18px;
}
.users-columns{
  display:grid;
  gap:18px;
  margin-top:14px;
}
.tablewrap{
  overflow:auto;
  border:1px solid var(--line);
  border-radius:12px;
}
table{
  width:100%;
  border-collapse:collapse;
  background:var(--panel);
}
th,td{
  padding:10px 11px;
  border-bottom:1px solid var(--table-border);
  text-align:left;
  vertical-align:top;
  font-size:12px;
}
th{
  background:var(--panel-ink);
  color:var(--header-subtle);
  text-transform:uppercase;
  letter-spacing:.05em;
  font-size:11px;
}
tbody tr:hover{ background:rgba(88,166,255,.06); }
html.light tbody tr:hover{ background:rgba(29,111,164,.07); }
tbody tr.clickable{ cursor:pointer; }
tbody tr.selected{ background:rgba(88,166,255,.12); }
html.light tbody tr.selected{ background:rgba(29,111,164,.14); }
.tag{
  display:inline-block;
  padding:2px 7px;
  border-radius:999px;
  background:var(--tag-bg);
  color:var(--tag-text);
  font-size:11px;
  font-weight:700;
}
.presence{
  font-size:16px;
  font-weight:700;
  line-height:1;
}
.presence.on{ color:#1d6d49; }
.presence.off{ color:#8b2e2e; }
.presence.now{ color:#d4a72c; }
.presence.idle{ color:var(--text); font-size:12px; font-weight:600; }
.proto-grid{
  display:grid;
  grid-template-columns:repeat(4,minmax(0,1fr));
  gap:10px;
}
.proto-card{
  padding:12px;
  border:1px solid var(--line);
  border-radius:12px;
  background:var(--panel-soft);
}
.proto-card label{
  display:block;
  margin-bottom:6px;
  font-size:11px;
  text-transform:uppercase;
  letter-spacing:.06em;
  color:var(--muted);
}
.proto-card strong{
  font-size:20px;
}
.mini{
  font-size:12px;
  color:var(--muted);
}
.health{
  display:inline-block;
  padding:3px 8px;
  border-radius:999px;
  font-size:11px;
  font-weight:700;
  text-transform:uppercase;
}
.health.ok{background:rgba(88,166,255,.14);color:#9ed0ff}
.health.degraded,.health.stale{background:rgba(201,107,73,.14);color:#f3bb9e}
.health.flapping{background:rgba(201,107,73,.22);color:#ffd1bb}
.health.unknown{background:rgba(255,255,255,.06);color:#c9d8e8}
.health.unknown{background:var(--unknown-bg);color:var(--unknown-text)}
html.light .health.ok{background:rgba(29,111,164,.12);color:#1d6fa4}
html.light .health.degraded,
html.light .health.stale{background:rgba(185,87,50,.12);color:#8b472a}
html.light .health.flapping{background:rgba(185,87,50,.18);color:#6e341e}
.empty{
  padding:14px;
  border:1px dashed var(--line);
  border-radius:12px;
  color:var(--muted);
  background:var(--panel-soft);
}
.gate{
  max-width:540px;
  margin:48px auto 24px;
}
.hidden{
  display:none !important;
}
.view-section{
  display:none;
}
.view-section.active{
  display:block;
}
.gate .body{
  display:grid;
  gap:14px;
}
.gate-note{
  font-size:13px;
  color:var(--muted);
  line-height:1.5;
}
@media (max-width: 1080px){
  .workspace,.split{grid-template-columns:1fr}
  .sidebar{position:static}
  .metric-grid{grid-template-columns:repeat(3,minmax(0,1fr))}
  .proto-grid{grid-template-columns:repeat(2,minmax(0,1fr))}
  .users-editor,.users-columns{grid-template-columns:1fr}
}
@media (max-width: 720px){
  .mast{padding:16px}
  .mast-actions{align-items:stretch}
  .metric-grid,.form-grid,.checkgrid,.proto-grid{grid-template-columns:1fr}
}
</style>
</head>
<body>
<div class="shell hidden" id="sysopApp">
  <section class="mast">
    <div>
      <h1>&#128225; pyCluster System Operator Console</h1>
      <p>Private system operator workspace for node settings, peer links, protocol health, and live cluster operations.</p>
    </div>
      <div class="mast-actions">
      <div class="statusline" id="who">System Operator session ready.</div>
      <div class="actions">
        <button class="secondary" id="reload" title="Reload all live admin views from the backend APIs.">Refresh Console</button>
        <button id="themeToggle" class="themebtn" title="Switch between the dark and light theme families used by the public dxcluster.ai3i.net UI.">Toggle Theme</button>
        <button class="secondary" id="logout" title="End the current System Operator web session.">Log Out</button>
      </div>
    </div>
  </section>

  <div class="workspace">
    <aside class="sidebar">
      <section class="sidebar-panel">
        <header><div class="sidebar-heading"><h2>Navigate</h2><span class="node-badge" id="navNodeBadge">-</span></div></header>
        <div class="body">
          <nav class="sidebar-nav">
            <a href="#node" data-view="node" class="active"><strong>Node Settings</strong><span>Branding</span></a>
            <a href="#users" data-view="users"><strong>Users</strong><span>Registry</span></a>
            <a href="#links" data-view="links"><strong>Peers and Links</strong><span>Connectivity</span></a>
            <a href="#protocol" data-view="protocol"><strong>Protocol Health</strong><span>State</span></a>
            <a href="#publish" data-view="publish"><strong>Operator Tools</strong><span>Posting</span></a>
            <a href="#telemetry" data-view="telemetry"><strong>Telemetry</strong><span>Runtime</span></a>
          </nav>
        </div>
      </section>
      <section class="sidebar-panel">
        <header><h2>At A Glance</h2></header>
        <div class="body">
          <div class="sidebar-metrics">
            <div class="sidebar-metric"><label>Uptime</label><strong id="navUptime">-</strong></div>
            <div class="sidebar-metric"><label>Spots</label><strong id="navSpots">-</strong></div>
            <div class="sidebar-metric"><label>Telnet</label><strong id="navTelnet">-</strong></div>
            <div class="sidebar-metric"><label>Web</label><strong id="navWeb">-</strong></div>
          </div>
        </div>
      </section>
    </aside>

    <main class="stack">
      <section class="panel view-section active" id="node">
        <header>
          <div>
            <h2>Node Settings</h2>
            <div class="subtle">Controls telnet welcome flow, MOTD, operator-facing branding, and contact metadata.</div>
          </div>
        </header>
        <div class="body">
          <div class="form-grid">
            <div class="field"><label for="node_call" title="Displayed node callsign and SSID used in the telnet prompt and welcome text.">Node Call / SSID</label><input id="node_call" placeholder="Node callsign" title="This changes the operator-facing node identity shown in the prompt and welcome flow."></div>
            <div class="field"><label for="node_alias" title="Short alias for the node, useful for UI and descriptive displays.">Node Alias</label><input id="node_alias" placeholder="Short alias" title="A shorter alias for the node; distinct from the full prompt callsign if desired."></div>
            <div class="field"><label for="owner_name" title="Name of the system operator or primary operator shown in node identity details.">Owner Name (QRA)</label><input id="owner_name" placeholder="Primary operator" title="Primary operator or system operator name for this node."></div>
            <div class="field"><label for="qth" title="Displayed node location used in the welcome text and status views.">Location (QTH)</label><input id="qth" placeholder="Location" title="Operator-facing location string for the node."></div>
            <div class="field"><label for="node_locator" title="Node Maidenhead grid square shown in public branding and node details.">Grid Square</label><input id="node_locator" placeholder="Grid square" title="Displayed in the public footer and operator-facing node identity details."></div>
            <div class="field"><label for="telnet_ports" title="Comma-separated list of telnet listener ports the cluster should bind.">Telnet Ports</label><input id="telnet_ports" placeholder="7300,7373,8000" title="Comma-separated list of listener ports. Saving applies the listener set live if the new ports can be bound."></div>
            <div class="field"><label for="branding_name" title="Short product or node brand shown in the telnet welcome experience.">Node Brand</label><input id="branding_name" placeholder="Node brand" title="Short product or node brand shown in the telnet welcome experience."></div>
            <div class="field"><label for="welcome_title" title="First line shown to a telnet user after successful login.">Welcome Title</label><input id="welcome_title" placeholder="Short welcome line" title="Keep this short and warm; it is prepended to the connecting callsign."></div>
            <div class="field"><label for="website_url" title="Optional URL shown in the telnet welcome block and useful for directing operators to documentation or a public site.">Website URL</label><input id="website_url" placeholder="https://example.org" title="Shown as a reference URL in the login welcome text if set."></div>
            <div class="field"><label for="support_contact" title="Contact string displayed to operators who need help with the node.">Support Contact</label><input id="support_contact" placeholder="support@example.org" title="Email address or other support contact shown in the telnet welcome block."></div>
            <div class="field"><label for="prompt_template" title="Prompt format shown to telnet operators. Available tokens: {timestamp}, {node}, {callsign}, {suffix}.">Prompt Template</label><input id="prompt_template" placeholder="[{timestamp}] {node}{suffix}" title="Use {timestamp}, {node}, {callsign}, and {suffix}. Example: [{timestamp}] {node}{suffix}"></div>
          </div>
          <div class="form-grid one" style="margin-top:12px">
            <div class="field"><label for="welcome_body" title="Main human-facing introduction shown after login and before the MOTD.">Welcome Body</label><textarea id="welcome_body" placeholder="Short human introduction shown after login." title="Use this for a friendly introduction, operating notes, or local node character."></textarea></div>
            <div class="field"><label for="motd" title="Message of the day shown in telnet and by show/motd.">MOTD</label><textarea id="motd" placeholder="Node notices and operating guidance." title="Operational notes, etiquette, maintenance notices, or other important daily information."></textarea></div>
            <div class="field"><label for="login_tip" title="Short tip line shown near the end of the telnet welcome flow.">Login Tip</label><input id="login_tip" placeholder="Short operator tip" title="Keep this to one concise sentence with a useful first-step hint."></div>
          </div>
          <div class="checkgrid" style="margin-top:12px">
            <div class="checkrow attention" title="When enabled, telnet users see a short node status line after the MOTD.">
              <input id="show_status_after_login" type="checkbox">
              <label for="show_status_after_login">Show node status after MOTD</label>
            </div>
            <div class="checkrow attention" title="Default behavior. Existing telnet users are prompted for their password. First-time users without one are sent through password creation instead. Node-classified callsigns skip this path.">
              <input id="require_password" type="checkbox">
              <label for="require_password">Require telnet passwords for users</label>
            </div>
          </div>
          <div class="form-grid" style="margin-top:12px">
            <div class="field">
              <label for="retention_enabled" title="When enabled, pyCluster runs age-based cleanup daily using the day counts below.">Automatic Cleanup</label>
              <div class="checkrow attention" title="Enable daily retention cleanup for spots, messages, and bulletins.">
                <input id="retention_enabled" type="checkbox" checked>
                <label for="retention_enabled">Enable age-based cleanup</label>
              </div>
            </div>
            <div class="field"><label for="retention_spots_days" title="Keep DX spots for this many days before purging old rows.">Keep Spots For (days)</label><input id="retention_spots_days" type="number" min="1" max="3650" value="30" title="Older spots are removed during the daily cleanup run."></div>
            <div class="field"><label for="retention_messages_days" title="Keep private messages for this many days before purging old rows.">Keep Messages For (days)</label><input id="retention_messages_days" type="number" min="1" max="3650" value="90" title="Older messages are removed during the daily cleanup run."></div>
            <div class="field"><label for="retention_bulletins_days" title="Keep bulletins for this many days before purging old rows.">Keep Bulletins For (days)</label><input id="retention_bulletins_days" type="number" min="1" max="3650" value="30" title="Older bulletins are removed during the daily cleanup run."></div>
          </div>
          <div class="subtle" id="retentionStatus" style="margin-top:8px">Automatic cleanup is disabled.</div>
          <div class="actions" style="margin-top:12px">
            <button id="saveNode" title="Persist these telnet presentation settings for this node.">Save Node Settings</button>
            <button class="secondary" id="runCleanup" title="Run the current age-based cleanup settings immediately.">Run Cleanup Now</button>
          </div>
        </div>
      </section>

      <section class="panel view-section" id="users">
        <header>
          <div>
            <h2>Users</h2>
            <div class="subtle">Manage local users and see which people on this node have System Operator access.</div>
          </div>
        </header>
        <div class="body">
          <section>
            <h3>System Operators</h3>
            <div class="tablewrap">
              <table>
                  <thead><tr><th>Callsign</th><th>Name</th><th>Email</th><th>Telnet</th><th>Web</th></tr></thead>
                  <tbody id="sysopRows"><tr><td colspan="5">Loading System Operators...</td></tr></tbody>
              </table>
            </div>
          </section>
          <div class="users-editor" style="margin-top:14px">
            <section>
              <h3 id="userEditorTitle">User Details</h3>
              <div class="form-grid">
                <div class="field"><label for="user_call" title="Local callsign record to create or edit on this node.">Callsign</label><input id="user_call" placeholder="N0CALL" title="Use the base callsign or an SSID variant for the exact local record you want to manage."></div>
                <div class="field"><label for="user_privilege" title="Access level on this node. Blocked prevents logins for this callsign and its SSIDs. System Operator grants access to the System Operator console and sysop commands.">Access Level</label><div class="select-shell"><select id="user_privilege" title="Choose Authenticated for ordinary local accounts, Non-Authenticated for local users without password-based authentication, Blocked to prevent logins for this callsign and its SSIDs, or System Operator for privileged operators on this node."><option value="">Non-Authenticated</option><option value="user">Authenticated</option><option value="sysop">System Operator</option><option value="blocked">Blocked</option></select></div></div>
                <div class="field"><label for="user_name" title="Operator name shown in local account details for this callsign.">Name (QRA)</label><input id="user_name" placeholder="Operator name" title="Friendly operator name stored for this local callsign record."></div>
                <div class="field"><label for="user_qth" title="Operator location for this local callsign record.">Location (QTH)</label><input id="user_qth" placeholder="Location" title="Human-readable location used for local operator details on this node."></div>
                <div class="field"><label for="user_grid" title="Grid square for this local user record.">Grid Square</label><input id="user_grid" placeholder="FN31PR" title="Maidenhead grid square for this local user record."></div>
                <div class="field"><label for="user_email" title="Optional contact email for this local callsign record.">Email</label><input id="user_email" placeholder="operator@example.org" title="Optional contact address used for local account details and future federation/contact features."></div>
                <div class="field"><label for="user_password" title="Change or set the local password for this callsign. Enter CLEAR to remove it.">Password</label><input id="user_password" type="password" placeholder="Change/Set or CLEAR to clear" title="Set or change the local password for this callsign. Enter CLEAR and then Set Password to remove it."></div>
                <div class="field"><label for="user_home_node" title="Authoritative home node for this callsign. This maps to set/homenode.">Home Node</label><input id="user_home_node" placeholder="N0CALL-1" title="The home node is the source of truth for this callsign and will be used by future federation features."></div>
                <div class="field span2"><label for="user_block_reason" title="Short operator notes for this local user. If Access Level is Blocked, this text is also shown as the block reason.">Notes / Block Reason</label><input id="user_block_reason" maxlength="80" placeholder="General notes or a block reason" title="Keep this brief. It can hold general notes for the local user, and if Access Level is Blocked it will also be shown as the block reason."></div>
              </div>
              <div class="actions" style="margin-top:12px">
                <button class="secondary" id="newUser">New User</button>
                <button class="secondary" id="saveUser">Save User</button>
                <button class="secondary" id="saveUserPassword">Set Password</button>
                <button class="warn" id="deleteUser" disabled>Remove User</button>
              </div>
              <details style="margin-top:12px">
                <summary class="subtle" style="cursor:pointer">Advanced Node Login (Only for Cluster Peers)</summary>
                <div class="form-grid one" style="margin-top:12px">
                <div class="field"><label for="user_node_family" title="Use this only for node-to-node records. It controls trusted cluster-peer login behavior and password bypass.">Cluster Node Family</label><div class="select-shell"><select id="user_node_family" title="Leave this unset for normal people. Set a cluster node family only for sysop-managed node records such as DXSpider, DxNet, AR-Cluster, CLX, or pyCluster."><option value="">Not a cluster peer</option><option value="pycluster">pyCluster</option><option value="dxspider">DXSpider</option><option value="dxnet">DxNet</option><option value="arcluster">AR-Cluster</option><option value="clx">CLX</option></select></div></div>
                </div>
              </details>
            </section>
            <section>
              <h3>Access Matrix</h3>
              <div class="subtle" style="margin-bottom:8px">Per-user channel and posting policy for the selected local callsign.</div>
              <div class="tablewrap">
                <table>
                  <thead><tr><th>Capability</th><th>TELNET</th><th>WEB</th></tr></thead>
                  <tbody>
                    <tr><td title="Whether this callsign may log in through that interface.">Login</td><td><input id="access_telnet_login" type="checkbox" title="Allow telnet login for this callsign."></td><td><input id="access_web_login" type="checkbox" title="Allow public web login for this callsign."></td></tr>
                    <tr><td title="Whether this callsign may post DX spots through that interface.">Spots</td><td><input id="access_telnet_spots" type="checkbox" title="Allow DX spot posting over telnet."></td><td><input id="access_web_spots" type="checkbox" title="Allow DX spot posting from the public web UI."></td></tr>
                    <tr><td title="Whether this callsign may send chat-style traffic through that interface.">Chat</td><td><input id="access_telnet_chat" type="checkbox" title="Allow chat posting over telnet."></td><td><input id="access_web_chat" type="checkbox" title="Allow chat posting from the public web UI."></td></tr>
                    <tr><td title="Whether this callsign may send announce traffic through that interface.">Announce</td><td><input id="access_telnet_announce" type="checkbox" title="Allow announce posting over telnet."></td><td><input id="access_web_announce" type="checkbox" title="Allow announce posting from the public web UI."></td></tr>
                    <tr><td title="Whether this callsign may send WX traffic through that interface.">WX</td><td><input id="access_telnet_wx" type="checkbox" title="Allow WX posting over telnet."></td><td><input id="access_web_wx" type="checkbox" title="Allow WX posting from the public web UI."></td></tr>
                    <tr><td title="Whether this callsign may send WCY traffic through that interface.">WCY</td><td><input id="access_telnet_wcy" type="checkbox" title="Allow WCY posting over telnet."></td><td><input id="access_web_wcy" type="checkbox" title="Allow WCY posting from the public web UI."></td></tr>
                    <tr><td title="Whether this callsign may send WWV traffic through that interface.">WWV</td><td><input id="access_telnet_wwv" type="checkbox" title="Allow WWV posting over telnet."></td><td><input id="access_web_wwv" type="checkbox" title="Allow WWV posting from the public web UI."></td></tr>
                  </tbody>
                </table>
              </div>
              <div class="actions" style="margin-top:12px">
                <button class="secondary" id="accessAll">Add All</button>
                <button class="secondary" id="accessNone">Remove All</button>
              </div>
            </section>
          </div>
          <div class="users-columns">
            <section>
              <h3>Blocked Users</h3>
              <div class="subtle" style="margin-bottom:12px">Calls blocked from login on this node, including matching SSID variants.</div>
              <div class="tablewrap">
                <table>
                  <thead><tr><th>Callsign</th><th>Home Node</th><th>Block Reason</th><th>Blocked</th></tr></thead>
                  <tbody id="blockedRows"><tr><td colspan="4">Loading blocked users...</td></tr></tbody>
                </table>
              </div>
            </section>
            <section>
              <h3>Local Users</h3>
              <div class="actions" style="margin-bottom:12px">
                <input id="user_search" placeholder="Filter users by callsign, name, home node, QTH, email" title="Search local users by callsign, name, home node, QTH, or email." style="max-width:320px">
                <button class="secondary" id="userSearch">Search</button>
                <button class="secondary" id="userPrev">Previous</button>
                <button class="secondary" id="userNext">Next</button>
                <span class="subtle" id="userPageInfo">Page 1</span>
              </div>
              <div class="tablewrap">
                <table>
                  <thead><tr><th>Callsign</th><th>Access</th><th>Home Node</th><th>Telnet</th><th>Web</th><th>Post</th><th>Last Login</th></tr></thead>
                  <tbody id="userRows"><tr><td colspan="9">Loading local users...</td></tr></tbody>
                </table>
              </div>
            </section>
          </div>
        </div>
      </section>

      <section class="panel view-section" id="links">
        <header>
          <div>
            <h2 id="peerEditorTitle">Peers and Links</h2>
            <div class="subtle">Shows which cluster peers are connected, whether reconnect is enabled, and what kind of traffic is moving across each link.</div>
          </div>
        </header>
        <div class="body">
          <div class="form-grid">
            <div class="field"><label for="peer" title="Optional filter applied to policy-drop and protocol-history views.">Peer Filter</label><input id="peer" placeholder="Peer name" title="Enter part of a peer name to narrow protocol and policy-drop views."></div>
            <div class="field"><label for="peername" title="Logical name for the peer you want to manage.">Peer Name</label><input id="peername" placeholder="Peer name" title="Used for connect, disconnect, and profile-change actions."></div>
            <div class="field"><label for="peerdsn" title="Transport address used to open the link to this peer.">Transport Address</label><input id="peerdsn" placeholder="dxspider://host:7300?login=LOCALNODE-1&client=PEERNODE-1" title="This is the connection method, not the cluster family. Use an explicit transport such as dxspider://host:7300?login=LOCALCALL&client=PEERCALL or tcp://host:7300. Bare host:port values are not accepted. Leave this blank only for an inbound peer."></div>
            <div class="field"><label for="peerprof" title="Cluster family/profile used once the link is established.">Cluster Family</label><input id="peerprof" placeholder="dxspider | dxnet | arcluster | clx" title="This is the peer behavior family, not the transport. For example: family dxspider with transport dxspider://host:7300?login=LOCALCALL&client=PEERCALL."></div>
            <div class="field"><label for="peerpass" title="Optional. Use this only when the remote peer is configured to require a node password.">Peer Password (Optional)</label><input id="peerpass" name="peer_secret" type="text" placeholder="Only if the peer requires it" title="Optional. If the remote peer is configured to require a node password, set it here. pyCluster stores it in the transport DSN as a password parameter." autocomplete="off" autocapitalize="off" autocorrect="off" data-lpignore="true" data-1p-ignore="true" spellcheck="false"><div class="subtle">Some system operators from peers may require a password; please coordinate with your peer operator.</div></div>
            <div class="field">
              <label for="peerretry" title="When enabled, pyCluster will keep trying to re-establish this saved outbound peer.">Retry Automatically</label>
              <div class="checkrow" title="Outbound peers retry with exponential backoff from 5 seconds up to 5 minutes. Inbound peers connect on their own and do not use local retry.">
                <input id="peerretry" type="checkbox" checked>
                <label for="peerretry">Reconnect this outbound peer automatically</label>
              </div>
              <div class="subtle" id="peerRetryHint" style="margin-top:6px">Outbound peers retry with backoff from 5s to 5m. Inbound peers connect to us and do not require a DSN transport address or use local retry.</div>
            </div>
          </div>
          <div class="actions" style="margin-top:12px">
            <button id="newPeer" title="Clear the editor and create a new outbound peer definition.">New Peer</button>
            <button id="peerSave" title="Save this outbound peer target without opening the link immediately.">Save Peer</button>
            <button id="pconnect" title="Create an outbound node-link connection to the selected peer DSN.">Connect</button>
            <button class="warn" id="pdisconnect" title="Disconnect the selected live peer session.">Disconnect</button>
            <button id="reset" title="Clear policy-drop counters, optionally limited by the current peer filter.">Reset Policy Drops</button>
          </div>
          <div class="tablewrap" style="margin-top:14px">
            <table>
              <thead><tr><th>Peer</th><th>Role</th><th>Status</th><th>Family</th><th>Traffic</th><th>Policy Drops</th><th>Health</th></tr></thead>
              <tbody id="peerRows"><tr><td colspan="7">Loading peers...</td></tr></tbody>
            </table>
          </div>
        </div>
      </section>

      <section class="panel view-section" id="protocol">
        <header>
          <div>
            <h2>Protocol Health</h2>
            <div class="subtle">Peer state, alerting, threshold controls, and history review.</div>
          </div>
        </header>
        <div class="body">
          <div class="form-grid">
            <div class="field"><label for="pstale" title="Minutes before a peer with known protocol state is considered stale.">Stale Minutes</label><input id="pstale" value="30" title="If no protocol updates arrive within this window, the peer health becomes stale."></div>
            <div class="field"><label for="pflap" title="Threshold at which repeated state changes mark a peer as flapping.">Flap Score</label><input id="pflap" value="3" title="Higher values make flap detection less sensitive."></div>
            <div class="field"><label for="pwindow" title="Time window used when evaluating protocol flap behavior.">Flap Window Seconds</label><input id="pwindow" value="300" title="Protocol state changes inside this window contribute to flap scoring."></div>
            <div class="field"><label for="phlim" title="Maximum number of protocol history rows to load into the history table.">History Limit</label><input id="phlim" value="20" title="Increase this when investigating a noisy or unstable peer."></div>
          </div>
          <div class="actions" style="margin-top:12px">
            <button id="protoSave" title="Persist the current protocol health thresholds.">Save Thresholds</button>
            <button class="secondary" id="phload" title="Reload protocol history using the current peer filter and history limit.">Reload History</button>
            <button class="warn" id="phreset" title="Delete stored protocol history for the current peer filter, or for all peers if no filter is set.">Reset Proto History</button>
          </div>
          <div class="proto-grid" style="margin-top:14px">
            <div class="proto-card"><label>Tracked Peers</label><strong id="protoTracked">-</strong></div>
            <div class="proto-card"><label>Healthy</label><strong id="protoHealthy">-</strong></div>
            <div class="proto-card"><label>Alerts</label><strong id="protoAlerts">-</strong></div>
            <div class="proto-card"><label>Acknowledged</label><strong id="protoAcked">-</strong></div>
          </div>
          <div class="split" style="margin-top:14px">
            <section>
              <h3>Protocol Summary</h3>
              <div class="tablewrap">
                <table>
                  <thead><tr><th>Peer</th><th>Health</th><th>Age</th><th>Changes</th><th>Flap</th><th>Last Event</th></tr></thead>
                  <tbody id="protoRows"><tr><td colspan="6">Loading protocol summary...</td></tr></tbody>
                </table>
              </div>
              <div class="tablewrap" style="margin-top:14px">
                <h3>Policy Drops</h3>
                <table>
                  <thead><tr><th>Peer</th><th>Total</th><th>Loop Drops</th><th>Reasons</th></tr></thead>
                  <tbody id="dropRows"><tr><td colspan="4">Loading policy drops...</td></tr></tbody>
                </table>
              </div>
            </section>
            <section>
              <h3>Protocol History</h3>
              <div class="tablewrap">
                <table>
                  <thead><tr><th>Peer</th><th>When</th><th>Key</th><th>From</th><th>To</th></tr></thead>
                  <tbody id="histRows"><tr><td colspan="5">Loading history...</td></tr></tbody>
                </table>
              </div>
            </section>
          </div>
        </div>
      </section>

      <section class="panel view-section" id="publish">
        <header>
          <div>
            <h2>Operator Tools</h2>
            <div class="subtle">Authenticated posting for spots, chat, bulletins, and operational notices.</div>
          </div>
        </header>
        <div class="body">
          <div class="form-grid">
            <div class="field"><label for="dx" title="DX callsign for a manually posted spot.">DX Call</label><input id="dx" placeholder="K1ABC" title="Destination or DX station being spotted."></div>
            <div class="field"><label for="freq" title="Frequency in kilohertz for a manual spot.">Frequency kHz</label><input id="freq" placeholder="14074.0" title="Use kilohertz, for example 14074.0 or 18100.0."></div>
            <div class="field"><label for="info" title="Comment, mode, or context for the manual spot. Supported modes include CW, WSPR, RTTY, FT8, FT4, FT2, JS8, JT9, JT65, Q65, MSK144, FSK441, MFSK, OLIVIA, DOMINO, THOR, HELL, ROS, VARA, PACTOR, WINMOR, ARDOP, PSK, FAX, SSTV, ATV, SSB, AM, FM, and DATA. Activity keywords include RARE, EME, SAT, WWFF, POTA, SOTA, IOTA, BOTA, and GMA.">Spot Info</label><input id="info" placeholder="FT8, split up 2, POTA" title="Supported modes include CW, WSPR, RTTY, FT8, FT4, FT2, JS8, JT9, JT65, Q65, MSK144, FSK441, MFSK, OLIVIA, DOMINO, THOR, HELL, ROS, VARA, PACTOR, WINMOR, ARDOP, PSK, FAX, SSTV, ATV, SSB, AM, FM, and DATA. Activity keywords include RARE, EME, SAT, WWFF, POTA, SOTA, IOTA, BOTA, and GMA."></div>
            <div class="field"><label for="scope" title="Distribution scope for announce messages only.">Announce Scope</label><input id="scope" placeholder="LOCAL | FULL | SYSOP" title="Used only for announce. WCY, WWV, WX, and chat are always local-category posts here."></div>
          </div>
          <div class="field" style="margin-top:12px"><label for="text" title="Text body for chat, announce, WCY, WWV, or WX posts.">Message / Bulletin Text</label><textarea id="text" placeholder="Enter chat, announce, WCY, WWV, or WX text here before posting." title="Required for Chat, Announce, WCY, WWV, and WX actions."></textarea></div>
          <div class="actions" style="margin-top:12px">
            <button id="spot" title="Post a DX spot using the current web-logged-in operator callsign.">Post Spot</button>
            <button class="secondary" id="chat" title="Post a local chat-style bulletin.">Chat</button>
            <button class="secondary" id="announce" title="Post an announce bulletin using the selected scope.">Announce</button>
            <button class="secondary" id="wcy" title="Post a WCY-style propagation bulletin.">WCY</button>
            <button class="secondary" id="wwv" title="Post a WWV-style propagation bulletin.">WWV</button>
            <button class="secondary" id="wx" title="Post a weather bulletin.">WX</button>
          </div>
          <div class="subtle" style="margin-top:12px">Posting identity: <strong id="postingCall">-</strong></div>
        </div>
      </section>

      <section class="panel view-section" id="telemetry">
        <header>
          <div>
            <h2>Telemetry</h2>
            <div class="subtle">Live stats, recent spots, and policy-drop details for troubleshooting.</div>
          </div>
        </header>
        <div class="body">
          <section>
            <h3>Runtime Stats</h3>
            <div class="metric-grid">
              <div class="metric"><label>Uptime</label><strong id="statUptime">-</strong></div>
              <div class="metric"><label>Stored Spots</label><strong id="statSpots">-</strong></div>
              <div class="metric"><label>Telnet Sessions</label><strong id="statSessions">-</strong></div>
              <div class="metric"><label>Web Sessions</label><strong id="statWebSessions">-</strong></div>
            </div>
          </section>
          <section style="margin-top:14px">
            <h3>Recent Spots</h3>
            <div class="tablewrap">
              <table>
                <thead><tr><th>Freq</th><th>DX</th><th>When</th><th>Spotter</th><th>Info</th></tr></thead>
                <tbody id="spotRows"><tr><td colspan="5">Loading spots...</td></tr></tbody>
              </table>
            </div>
          </section>
          <section style="margin-top:14px">
            <h3>Recent Audit</h3>
            <div class="actions" style="margin-bottom:12px">
              <select id="auditCategory" style="max-width:220px">
                <option value="">All Categories</option>
                <option value="sysop">System Operator</option>
                <option value="user">User</option>
                <option value="config">Config</option>
                <option value="control">Control</option>
                <option value="connect">Connect</option>
                <option value="disconnect">Disconnect</option>
              </select>
              <button class="secondary" id="auditReload">Reload</button>
            </div>
            <div class="tablewrap">
              <table>
                <thead><tr><th>When</th><th>Category</th><th>Activity</th></tr></thead>
                <tbody id="auditRows"><tr><td colspan="3">Loading audit activity...</td></tr></tbody>
              </table>
            </div>
          </section>
          <section style="margin-top:14px">
            <h3>Security</h3>
            <div class="actions" style="margin:8px 0 12px">
              <button id="securityReload" title="Reload recent login failures and current fail2ban bans.">Reload Security</button>
            </div>
            <div class="users-columns">
              <section>
                <h3>Recent Auth Failures</h3>
                <div class="tablewrap">
                  <table>
                    <thead><tr><th>When</th><th>Channel</th><th>IP</th><th>Call</th><th>Reason</th></tr></thead>
                    <tbody id="authFailRows"><tr><td colspan="5">Loading auth failures...</td></tr></tbody>
                  </table>
                </div>
              </section>
              <section>
                <h3>Current Bans</h3>
                <div class="tablewrap">
                  <table>
                    <thead><tr><th>Jail</th><th>IP</th></tr></thead>
                    <tbody id="banRows"><tr><td colspan="2">Loading bans...</td></tr></tbody>
                  </table>
                </div>
              </section>
            </div>
          </section>
        </div>
      </section>
    </main>
  </div>
</div>
<section class="panel gate" id="loginGate">
  <header>
    <div>
      <h2>System Operator Login</h2>
      <div class="subtle">System Operator access is required to use the pyCluster system operator console.</div>
    </div>
  </header>
  <div class="body">
    <div class="gate-note">Sign in with a callsign that has a configured password and System Operator access on this node.</div>
    <div class="form-grid">
      <div class="field">
        <label for="call" title="System Operator callsign used for operator-console authentication.">Callsign</label>
        <input id="call" placeholder="callsign" title="Only callsigns with System Operator access can enter the operator console.">
      </div>
      <div class="field">
        <label for="pass" title="Password tied to the System Operator callsign.">Password</label>
        <input id="pass" type="password" placeholder="password" title="Required for operator-console access.">
      </div>
    </div>
    <div class="actions">
      <button id="login" title="Authenticate and open the system operator console.">Sign In</button>
    </div>
    <div class="statusline hidden" id="loginStatus"></div>
    <div class="statusline" id="who">Awaiting System Operator login.</div>
  </div>
</section>
<script>
let webTok = '';
let webCall = '';
let webIsSysop = false;
let userOffset = 0;
let selectedUserCall = '';
let selectedPeerName = '';
const USER_PAGE_SIZE = 20;
const SYSOP_SESSION_KEY = 'pycluster-sysop-session';
const API_BASE = window.location.pathname.startsWith('/sysop')
  ? '/sysop'
  : (window.location.pathname.startsWith('/admin') ? '/admin' : '');
const byId = (id) => document.getElementById(id);
const THEME_KEY = 'pycluster-admin-theme';
function applyTheme(theme) {
  const root = document.documentElement;
  const next = theme === 'light' ? 'light' : 'dark';
  root.classList.remove('light','dark');
  root.classList.add(next);
  localStorage.setItem(THEME_KEY, next);
  byId('themeToggle').textContent = next === 'dark' ? 'Switch to Light' : 'Switch to Dark';
}
function setConsoleVisible(visible) {
  byId('sysopApp').classList.toggle('hidden', !visible);
  byId('loginGate').classList.toggle('hidden', visible);
}
function setView(view) {
  const target = String(view || 'node');
  document.querySelectorAll('.view-section').forEach((el) => {
    el.classList.toggle('active', el.id === target);
  });
  document.querySelectorAll('.sidebar-nav a').forEach((el) => {
    el.classList.toggle('active', el.dataset.view === target);
  });
  if (window.location.hash !== '#' + target) {
    history.replaceState(null, '', '#' + target);
  }
}
function apiUrl(u) {
  const txt = String(u || '');
  if (!txt.startsWith('/')) return txt;
  return API_BASE + txt;
}
function setText(id, value) {
  const el = byId(id);
  if (!el) return;
  el.textContent = value;
}
const hdr = () => {
  return webIsSysop && webTok ? {'X-Web-Token': webTok} : {};
};
const whdr = () => {
  const h = hdr();
  if (webTok) h['X-Web-Token'] = webTok;
  return h;
};
async function j(u, o = {}) {
  const r = await fetch(apiUrl(u), {...o, headers: {...(o.headers || {}), ...hdr()}});
  const data = await r.json();
  if (!r.ok) {
    const msg = data && data.error ? String(data.error) : `HTTP ${r.status}`;
    const err = new Error(msg);
    err.status = r.status;
    if (r.status === 401) {
      clearWebSession();
      say('System Operator session expired. Please sign in again.', false);
    }
    throw err;
  }
  return data;
}
async function jw(u, o = {}) {
  const r = await fetch(apiUrl(u), {...o, headers: {...(o.headers || {}), ...whdr()}});
  const data = await r.json();
  if (!r.ok) {
    const msg = data && data.error ? String(data.error) : `HTTP ${r.status}`;
    const err = new Error(msg);
    err.status = r.status;
    if (r.status === 401 && !o.skipAuthReset) {
      clearWebSession();
      say('System Operator session expired. Please sign in again.', false);
    }
    throw err;
  }
  return data;
}
function errText(err) {
  if (!err) return 'request failed';
  if (typeof err === 'string') return err;
  if (err && typeof err.message === 'string' && err.message) return err.message;
  return 'request failed';
}
function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
async function runButtonAction(id, fn) {
  const btn = byId(id);
  if (!btn) return fn();
  const originalText = btn.textContent;
  btn.disabled = true;
  btn.classList.remove('done', 'failed');
  btn.classList.add('busy');
  try {
    const result = await fn();
    btn.classList.remove('busy');
    btn.classList.add('done');
    await wait(300);
    return result;
  } catch (err) {
    btn.classList.remove('busy');
    btn.classList.add('failed');
    await wait(650);
    throw err;
  } finally {
    btn.classList.remove('busy', 'done', 'failed');
    btn.disabled = false;
    btn.textContent = originalText;
  }
}
function say(text, ok = true) {
  const el = byId('who');
  el.textContent = text;
  el.style.background = ok ? 'var(--accent-soft)' : '#f4ddd5';
  el.style.color = ok ? '#174434' : '#7d2e17';
}
function sayLogin(text, ok = true) {
  const el = byId('loginStatus');
  if (!el) return;
  if (!text) {
    el.classList.add('hidden');
    el.textContent = '';
    el.classList.remove('error');
    return;
  }
  el.classList.remove('hidden');
  el.textContent = text;
  el.classList.toggle('error', !ok);
}
function clearWebSession() {
  webTok = '';
  webCall = '';
  webIsSysop = false;
  sessionStorage.removeItem(SYSOP_SESSION_KEY);
  setConsoleVisible(false);
  const callEl = byId('call');
  const passEl = byId('pass');
  if (callEl) callEl.disabled = false;
  if (passEl) passEl.disabled = false;
  if (passEl) passEl.value = '';
}
function persistWebSession() {
  if (!webTok || !webCall || !webIsSysop) {
    sessionStorage.removeItem(SYSOP_SESSION_KEY);
    return;
  }
  sessionStorage.setItem(SYSOP_SESSION_KEY, JSON.stringify({token: webTok, call: webCall, sysop: webIsSysop}));
}
function restoreWebSession() {
  try {
    const raw = sessionStorage.getItem(SYSOP_SESSION_KEY);
    if (!raw) return false;
    const parsed = JSON.parse(raw);
    webTok = String(parsed.token || '');
    webCall = String(parsed.call || '');
    webIsSysop = !!parsed.sysop;
    if (!webTok || !webCall || !webIsSysop) {
      clearWebSession();
      return false;
    }
    setConsoleVisible(true);
    say('System Operator session restored for ' + webCall + '.');
    return true;
  } catch {
    clearWebSession();
    return false;
  }
}
async function logoutSysop() {
  try {
    if (webTok) {
      await jw('/api/auth/logout', {method:'POST', skipAuthReset:true});
    }
  } catch {}
  clearWebSession();
  sayLogin('');
  say('System Operator session ended.');
}
function fmtUptime(sec) {
  sec = Number(sec || 0);
  const d = Math.floor(sec / 86400);
  const h = Math.floor((sec % 86400) / 3600);
  const m = Math.floor((sec % 3600) / 60);
  const s = Math.floor(sec % 60);
  if (d > 0) return `${d}d ${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;
  if (h > 0) return `${h}h ${String(m).padStart(2, '0')}m`;
  if (m > 0) return `${m}m ${String(s).padStart(2, '0')}s`;
  return `${s}s`;
}
function fmtEpoch(epoch) {
  if (!epoch) return '-';
  const d = new Date(Number(epoch) * 1000);
  if (Number.isNaN(d.getTime())) return '-';
  return d.toISOString().replace('T', ' ').replace('.000Z', 'Z');
}
function esc(v) {
  return String(v ?? '').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;');
}
function healthBadge(v) {
  const txt = String(v || 'unknown').toLowerCase();
  const klass = ['ok','degraded','stale','flapping'].includes(txt) ? txt : 'unknown';
  return `<span class="health ${klass}">${esc(txt)}</span>`;
}
function summarizeTypes(map) {
  const entries = Object.entries(map || {}).filter(([, v]) => Number(v || 0) > 0);
  if (!entries.length) return '-';
  return entries
    .sort((a, b) => Number(b[1] || 0) - Number(a[1] || 0) || String(a[0]).localeCompare(String(b[0])))
    .slice(0, 4)
    .map(([k, v]) => `${k}:${v}`)
    .join(', ');
}
function splitPeerPassword(dsn) {
  const raw = String(dsn || '').trim();
  if (!raw || !raw.includes('?')) return {dsn: raw, password: ''};
  const qpos = raw.indexOf('?');
  const base = raw.slice(0, qpos);
  const query = raw.slice(qpos + 1);
  const params = new URLSearchParams(query);
  const password = params.get('password') || '';
  if (password) params.delete('password');
  const clean = params.toString();
  return {dsn: clean ? `${base}?${clean}` : base, password};
}
function mergePeerPassword(dsn, password) {
  const raw = String(dsn || '').trim();
  if (!raw) return raw;
  const qpos = raw.indexOf('?');
  const base = qpos >= 0 ? raw.slice(0, qpos) : raw;
  const query = qpos >= 0 ? raw.slice(qpos + 1) : '';
  const params = new URLSearchParams(query);
  if (String(password || '').trim()) params.set('password', String(password || '').trim());
  else params.delete('password');
  const merged = params.toString();
  return merged ? `${base}?${merged}` : base;
}
function fillPeerForm(peer) {
  const data = peer || {};
  const auth = splitPeerPassword(data.dsn || '');
  selectedPeerName = String(data.peer || '');
  byId('peername').value = data.peer || '';
  byId('peerdsn').value = auth.dsn || '';
  byId('peerprof').value = data.profile || 'dxspider';
  byId('peerpass').value = auth.password || '';
  byId('peerretry').checked = data.reconnect_enabled !== false;
  const editable = !!data.desired || !data.inbound;
  byId('peerdsn').disabled = !editable;
  byId('peerprof').disabled = !editable;
  byId('peerpass').disabled = !editable;
  byId('peerretry').disabled = !!data.inbound && !data.desired;
  const roleText = data.inbound ? 'Inbound peers connect to us and do not require a DSN transport address or use local retry.' : 'Outbound peers are initiated by this node and can retry automatically.';
  setText('peerRetryHint', roleText + ' Backoff runs from 5s up to 5m.');
  setText('peerEditorTitle', selectedPeerName ? `Editing ${selectedPeerName}` : 'Peers and Links');
}
function clearPeerForm() {
  selectedPeerName = '';
  byId('peername').value = '';
  byId('peerdsn').value = '';
  byId('peerprof').value = '';
  byId('peerpass').value = '';
  byId('peerretry').checked = true;
  byId('peerdsn').disabled = false;
  byId('peerprof').disabled = false;
  byId('peerpass').disabled = false;
  byId('peerretry').disabled = false;
  setText('peerRetryHint', 'Outbound peers retry with backoff from 5s to 5m. Inbound peers connect to us and do not require a DSN transport address or use local retry.');
  setText('peerEditorTitle', 'Peers and Links');
}
function bindSelectablePeerRows(body, rows) {
  body.querySelectorAll('tr[data-peer]').forEach((tr) => {
    tr.classList.add('clickable');
    tr.classList.toggle('selected', tr.dataset.peer === selectedPeerName);
    tr.addEventListener('click', () => {
      const row = rows.find((item) => item.peer === tr.dataset.peer);
      fillPeerForm(row || {});
      body.querySelectorAll('tr[data-peer]').forEach((other) => {
        other.classList.toggle('selected', other.dataset.peer === tr.dataset.peer);
      });
      if (row && row.inbound && !row.desired) {
        say(`Loaded ${tr.dataset.peer}. This is a live inbound peer and is read-only here.`);
      } else {
        say('Loaded peer ' + tr.dataset.peer + ' for editing.');
      }
    });
  });
}
function setPeerRows(peers) {
  const body = byId('peerRows');
  if (!Array.isArray(peers) || !peers.length) {
    body.innerHTML = '<tr><td colspan="7">No peers configured.</td></tr>';
    return;
  }
  body.innerHTML = peers.map((peer) => {
    const direction = peer.inbound ? 'Inbound' : 'Outbound';
    const frames = `${peer.parsed_frames || 0} in / ${peer.sent_frames || 0} out`;
    const rxTypes = summarizeTypes(peer.rx_by_type);
    const txTypes = summarizeTypes(peer.tx_by_type);
    const proto = peer.proto ? `${peer.proto.health || 'unknown'}${peer.proto.age_min >= 0 ? `, ${peer.proto.age_min}m` : ''}` : 'unknown';
    const status = peer.connected === false ? 'Disconnected' : 'Connected';
    const statusMeta = peer.connected === false ? (peer.inbound ? 'waiting for remote node' : 'waiting for outbound link') : `${direction} • active`;
    const pathHint = String(peer.path_hint || '').trim();
    const transport = String(peer.transport || '').trim();
    const desired = peer.desired ? '<div class="mini">configured peer</div>' : '';
    const reconnect = peer.inbound ? 'no local retry' : (peer.reconnect_enabled ? 'auto retry' : 'manual retry');
    const retry = peer.inbound ? 'n/a' : (peer.next_retry_epoch ? `next ${fmtEpoch(peer.next_retry_epoch)}` : 'ready');
    let err = '';
    if (peer.last_error) {
      let errText = String(peer.last_error);
      if (errText.startsWith('unsupported transport scheme: ')) {
        errText = `Unsupported peer address format. Use dxspider://host:7300?login=LOCALCALL&client=PEERCALL or tcp://host:7300.`;
      } else if (errText === 'dxspider dsn requires ?login=CALL') {
        errText = 'DXSpider transport requires a login callsign, for example ?login=LOCALNODE-1.';
      } else if (errText === 'dxspider dsn requires ?client=PEERCALL') {
        errText = 'DXSpider transport requires the remote node callsign, for example ?client=PEERNODE-1.';
      }
      err = `<div class="mini">${esc(errText)}</div>`;
    }
    const healthText = peer.proto ? `${peer.proto.age_min >= 0 ? `${peer.proto.age_min}m since update` : 'no age data'} • last ${peer.proto.last_pc_type || 'unknown'}` : 'no protocol data';
    return `<tr data-peer="${esc(peer.peer || '')}">
      <td><strong>${peer.peer}</strong></td>
      <td>${esc(direction)}<div class="mini">${peer.desired ? 'configured peer' : 'observed live peer'}</div>${pathHint ? `<div class="mini">${esc((transport ? transport + ' • ' : '') + pathHint)}</div>` : ''}</td>
      <td><strong>${status}</strong><div class="mini">${esc(statusMeta)}</div>${desired}</td>
      <td><span class="tag">${peer.profile || 'dxspider'}</span><div class="mini">${esc(reconnect)}</div></td>
      <td>${frames}<div class="mini">rx ${esc(rxTypes)}</div><div class="mini">tx ${esc(txTypes)}</div></td>
      <td>${peer.policy_dropped || 0}</td>
      <td>${healthBadge(peer.proto && peer.proto.health)} <div class="mini">${esc(healthText)}</div><div class="mini">${peer.inbound ? 'inbound link' : `retry ${esc(String(peer.retry_count || 0))} • ${esc(retry)}`}</div>${err}</td>
    </tr>`;
  }).join('');
  bindSelectablePeerRows(body, peers);
}
function setSpotRows(spots) {
  const body = byId('spotRows');
  if (!Array.isArray(spots) || !spots.length) {
    body.innerHTML = '<tr><td colspan="5">No spots stored yet.</td></tr>';
    return;
  }
  body.innerHTML = spots.map((spot) => `<tr>
    <td>${esc(Number(spot.freq_khz || 0).toFixed(1))}</td>
    <td><strong>${esc(spot.dx_call || '')}</strong></td>
    <td>${esc(fmtEpoch(spot.epoch))}</td>
    <td>${esc(spot.spotter || '')}</td>
    <td>${esc(spot.info || '')}</td>
  </tr>`).join('');
}
function setAuditRows(rows) {
  const body = byId('auditRows');
  if (!Array.isArray(rows) || !rows.length) {
    body.innerHTML = '<tr><td colspan="3">No recent audit activity.</td></tr>';
    return;
  }
  body.innerHTML = rows.map((row) => `<tr>
    <td>${esc(fmtEpoch(row.epoch))}</td>
    <td><span class="tag">${esc(row.category || '')}</span></td>
    <td>${esc(row.text || '')}</td>
  </tr>`).join('');
}
function prettyAuthChannel(channel) {
  const key = String(channel || '').toLowerCase();
  if (key === 'sysop-web') return 'System Operator Web';
  if (key === 'public-web') return 'Public Web';
  if (key === 'telnet') return 'Telnet';
  return channel || '-';
}
function prettyAuthReason(reason) {
  const key = String(reason || '').toLowerCase();
  if (key === 'bad_password') return 'Bad password';
  if (key === 'invalid_credentials') return 'Invalid credentials';
  if (key === 'invalid_callsign') return 'Invalid callsign';
  if (key === 'blocked_login') return 'Blocked login';
  if (key === 'web_login_not_allowed') return 'Web login not allowed';
  if (key === 'telnet_login_not_allowed') return 'Telnet login not allowed';
  return reason || '-';
}
function setAuthFailRows(rows) {
  const body = byId('authFailRows');
  if (!Array.isArray(rows) || !rows.length) {
    body.innerHTML = '<tr><td colspan="5">No recent auth failures recorded.</td></tr>';
    return;
  }
  body.innerHTML = rows.map((row) => `<tr>
    <td>${esc(row.when || '-')}</td>
    <td><span class="tag">${esc(prettyAuthChannel(row.channel || ''))}</span></td>
    <td>${esc(row.ip || '-')}</td>
    <td>${esc(row.call || '-')}</td>
    <td>${esc(prettyAuthReason(row.reason || '-'))}</td>
  </tr>`).join('');
}
function setBanRows(rows) {
  const body = byId('banRows');
  if (!Array.isArray(rows) || !rows.length) {
    body.innerHTML = '<tr><td colspan="2">No current fail2ban bans.</td></tr>';
    return;
  }
  body.innerHTML = rows.map((row) => `<tr>
    <td><span class="tag">${esc(row.jail || '-')}</span></td>
    <td>${esc(row.ip || '-')}</td>
  </tr>`).join('');
}
async function reloadAudit() {
  const category = byId('auditCategory')?.value || '';
  const path = '/api/audit?limit=20' + (category ? '&category=' + encodeURIComponent(category) : '');
  const rows = await j(path);
  setAuditRows(rows);
}
function setDropRows(rows) {
  const body = byId('dropRows');
  if (!Array.isArray(rows) || !rows.length) {
    body.innerHTML = '<tr><td colspan="4">No policy drops recorded.</td></tr>';
    return;
  }
  body.innerHTML = rows.map((row) => {
    const reasons = Object.entries(row.reasons || {}).map(([k,v]) => `${k}: ${v}`).join(', ') || '-';
    return `<tr><td><strong>${esc(row.peer || '')}</strong></td><td>${esc(row.total || 0)}</td><td>${esc(row.loop_total || 0)}</td><td>${esc(reasons)}</td></tr>`;
  }).join('');
}
function setProtoRows(rows) {
  const body = byId('protoRows');
  if (!Array.isArray(rows) || !rows.length) {
    body.innerHTML = '<tr><td colspan="6">No protocol-tracked peers yet.</td></tr>';
    return;
  }
  body.innerHTML = rows.map((row) => {
    const last = row.last_event || {};
    const lastTxt = last.key ? `${last.key}: ${last.from || ''} -> ${last.to || ''}` : '-';
    return `<tr>
      <td><strong>${esc(String(row.peer || '').toUpperCase())}</strong></td>
      <td>${healthBadge(row.health)}</td>
      <td>${row.age_min >= 0 ? esc(row.age_min + 'm') : '-'}</td>
      <td>${esc(row.change_count || 0)}</td>
      <td>${esc(row.flap_score || 0)}</td>
      <td>${esc(lastTxt)}</td>
    </tr>`;
  }).join('');
}
function setHistRows(rows) {
  const body = byId('histRows');
  if (!Array.isArray(rows) || !rows.length) {
    body.innerHTML = '<tr><td colspan="5">No protocol history for this filter.</td></tr>';
    return;
  }
  body.innerHTML = rows.map((row) => `<tr>
    <td><strong>${esc(String(row.peer || '').toUpperCase())}</strong></td>
    <td>${esc(fmtEpoch(row.epoch))}</td>
    <td>${esc(row.key || '')}</td>
    <td>${esc(String(row.from || '').toUpperCase())}</td>
    <td>${esc(String(row.to || '').toUpperCase())}</td>
  </tr>`).join('');
}
function fillUserForm(row) {
  const data = row || {};
  selectedUserCall = String(data.call || '').toUpperCase();
  byId('user_call').value = data.call || '';
  byId('user_privilege').value = data.blocked_login ? 'blocked' : String(data.privilege || '').toLowerCase();
  byId('user_name').value = data.display_name || '';
  byId('user_qth').value = data.qth || '';
  byId('user_email').value = data.email || '';
  byId('user_home_node').value = data.home_node || '';
  byId('user_grid').value = data.qra || '';
  byId('user_block_reason').value = data.user_note || data.blocked_reason || '';
  byId('user_node_family').value = data.node_family || '';
  byId('user_password').value = '';
  const access = data.access || {};
  ['telnet','web'].forEach((channel) => {
    const rowAccess = access[channel] || {};
    ['login','spots','chat','announce','wx','wcy','wwv'].forEach((capability) => {
      const el = byId(`access_${channel}_${capability}`);
      if (el) el.checked = !!rowAccess[capability];
    });
  });
  setText('userEditorTitle', selectedUserCall ? `Editing ${selectedUserCall}` : 'User Details');
  setText('saveUser', selectedUserCall ? 'Update User' : 'Save User');
  byId('deleteUser').disabled = !selectedUserCall;
}
function clearUserForm(defaultCall='') {
  selectedUserCall = '';
  byId('user_call').value = defaultCall || '';
  byId('user_privilege').value = '';
  byId('user_name').value = '';
  byId('user_qth').value = '';
  byId('user_email').value = '';
  byId('user_home_node').value = '';
  byId('user_grid').value = '';
  byId('user_block_reason').value = '';
  byId('user_node_family').value = '';
  byId('user_password').value = '';
  applyPrivilegeDefaults('');
  setText('userEditorTitle', 'User Details');
  setText('saveUser', 'Save User');
  byId('deleteUser').disabled = true;
}
function bindSelectableRows(body, rows) {
  body.querySelectorAll('tr[data-call]').forEach((tr) => {
    tr.classList.add('clickable');
    tr.classList.toggle('selected', tr.dataset.call === selectedUserCall);
    tr.addEventListener('click', () => {
      const row = rows.find((item) => item.call === tr.dataset.call);
      fillUserForm(row || {});
      body.querySelectorAll('tr[data-call]').forEach((other) => {
        other.classList.toggle('selected', other.dataset.call === tr.dataset.call);
      });
      say('Loaded user ' + tr.dataset.call + ' for editing.');
    });
  });
}
function mark(enabled, onTitle, offTitle) {
  return enabled
    ? `<span class="presence on" title="${esc(onTitle || 'Allowed')}">✓</span>`
    : `<span class="presence off" title="${esc(offTitle || 'Disabled')}">✗</span>`;
}
function fmtAgeEpoch(epoch) {
  if (!epoch) return '—';
  const now = Math.floor(Date.now() / 1000);
  const age = Math.max(0, now - Number(epoch || 0));
  if (age < 60) return 'now';
  const mins = Math.floor(age / 60);
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 48) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}
function seenText(active, lastEpoch, activeTitle, inactiveTitle) {
  return active
    ? `<span class="presence now" title="${esc(activeTitle || 'Active now')}">now</span>`
    : (lastEpoch
      ? `<span class="presence idle" title="${esc(inactiveTitle || 'Last seen')}">${esc(fmtAgeEpoch(lastEpoch))}</span>`
      : `<span class="subtle" title="${esc(inactiveTitle || 'No current session')}">—</span>`);
}
function anyPostingEnabled(access) {
  const channels = ['telnet','web'];
  const caps = ['spots','chat','announce','wx','wcy','wwv'];
  return channels.some((channel) => caps.some((cap) => !!(((access || {})[channel] || {})[cap])));
}
function setSysopRows(rows) {
  const body = byId('sysopRows');
  if (!Array.isArray(rows) || !rows.length) {
    body.innerHTML = '<tr><td colspan="5">No System Operators registered yet.</td></tr>';
    return;
  }
  body.innerHTML = rows.map((row) => `<tr data-call="${esc(row.call || '')}">
    <td><strong>${esc(row.call || '')}</strong></td>
    <td>${esc(row.display_name || '-')}</td>
    <td>${esc(row.email || '-')}</td>
    <td>${seenText(!!row.telnet_online, String(row.last_login_peer || '').toLowerCase().startsWith('sysop-web') ? 0 : row.last_login_epoch, 'Telnet session active now', `Last telnet login${row.last_login_peer ? ` via ${row.last_login_peer}` : ''}`)}</td>
    <td>${seenText(!!row.web_online, String(row.last_login_peer || '').toLowerCase().startsWith('sysop-web') ? row.last_login_epoch : 0, 'System Operator web session active now', `Last System Operator web login${row.last_login_peer ? ` via ${row.last_login_peer}` : ''}`)}</td>
  </tr>`).join('');
  bindSelectableRows(body, rows);
}
function collectAccessMatrix() {
  const access = {};
  ['telnet','web'].forEach((channel) => {
    access[channel] = {};
    ['login','spots','chat','announce','wx','wcy','wwv'].forEach((capability) => {
      const el = byId(`access_${channel}_${capability}`);
      access[channel][capability] = !!(el && el.checked);
    });
  });
  return access;
}
function setAccessMatrixAll(enabled) {
  ['telnet','web'].forEach((channel) => {
    ['login','spots','chat','announce','wx','wcy','wwv'].forEach((capability) => {
      const el = byId(`access_${channel}_${capability}`);
      if (el) el.checked = !!enabled;
    });
  });
}
function setAccessCapability(channel, capability, enabled) {
  const el = byId(`access_${channel}_${capability}`);
  if (el) el.checked = !!enabled;
}
function applyPrivilegeDefaults(privilege) {
  const level = String(privilege || '').trim().toLowerCase();
  if (level === 'blocked') {
    setAccessMatrixAll(false);
    return;
  }
  if (level === '') {
    setAccessMatrixAll(false);
    ['telnet','web'].forEach((channel) => {
      setAccessCapability(channel, 'login', true);
      setAccessCapability(channel, 'chat', true);
      setAccessCapability(channel, 'wx', true);
      setAccessCapability(channel, 'wcy', true);
      setAccessCapability(channel, 'wwv', true);
    });
    return;
  }
  if (level === 'user' || level === 'sysop') {
    setAccessMatrixAll(true);
  }
}
function setUserRows(payload) {
  setRegistryRows('userRows', 'userPageInfo', 'userPrev', 'userNext', payload, 'No local users match this filter.');
}
function setBlockedRows(payload) {
  const body = byId('blockedRows');
  const rows = Array.isArray(payload && payload.rows) ? payload.rows : [];
  if (!rows.length) {
    body.innerHTML = '<tr><td colspan="4">No blocked users match this filter.</td></tr>';
    return;
  }
  body.innerHTML = rows.map((row) => `<tr data-call="${esc(row.call || '')}">
    <td><strong>${esc(row.call || '')}</strong></td>
    <td>${esc(row.home_node || '-')}</td>
    <td>${esc(row.blocked_reason || 'Blocked by local policy')}</td>
    <td>${esc(fmtEpoch(row.updated_epoch))}</td>
  </tr>`).join('');
  bindSelectableRows(body, rows);
}
function setRegistryRows(bodyId, pageInfoId, prevId, nextId, payload, emptyText) {
  const body = byId(bodyId);
  const rows = Array.isArray(payload && payload.rows) ? payload.rows : [];
  const total = Number((payload && payload.total) || 0);
  const offset = Number((payload && payload.offset) || 0);
  const limit = Number((payload && payload.limit) || USER_PAGE_SIZE);
  const page = Math.floor(offset / Math.max(limit, 1)) + 1;
  const pages = Math.max(1, Math.ceil(total / Math.max(limit, 1)));
  if (pageInfoId) setText(pageInfoId, `Page ${page} of ${pages} • ${total} users`);
  if (prevId && byId(prevId)) byId(prevId).disabled = offset <= 0;
  if (nextId && byId(nextId)) byId(nextId).disabled = offset + limit >= total;
  if (!rows.length) {
    body.innerHTML = `<tr><td colspan="7">${esc(emptyText)}</td></tr>`;
    return;
  }
  body.innerHTML = rows.map((row) => `<tr data-call="${esc(row.call || '')}">
    <td><strong>${esc(row.call || '')}</strong></td>
    <td><span class="tag">${esc(row.access_label || row.privilege || 'None')}</span></td>
    <td>${esc(row.home_node || '-')}</td>
    <td title="${esc(row.last_login_peer || 'No recorded telnet login path')}">${mark(!!(((row.access || {}).telnet || {}).login), 'Telnet login allowed', 'Telnet login blocked')}</td>
    <td title="${esc(row.last_login_peer || 'No recorded web login path')}">${mark(!!(((row.access || {}).web || {}).login), 'Web login allowed', 'Web login blocked')}</td>
    <td>${mark(anyPostingEnabled(row.access), 'Posting allowed on one or more channels', 'Posting disabled on all channels')}</td>
    <td title="${esc(row.last_login_peer || 'No recorded inbound path')}">${esc(fmtEpoch(row.last_login_epoch))}</td>
  </tr>`).join('');
  bindSelectableRows(body, rows);
}
function fillNodeForm(data) {
  if (!data) return;
  ['node_call','node_alias','owner_name','qth','node_locator','telnet_ports','branding_name','welcome_title','welcome_body','login_tip','support_contact','website_url','motd','prompt_template'].forEach((key) => {
    if (byId(key) && data[key] !== undefined) byId(key).value = data[key];
  });
  byId('show_status_after_login').checked = !!data.show_status_after_login;
  byId('require_password').checked = !!data.require_password;
  byId('retention_enabled').checked = !!data.retention_enabled;
  if (data.retention_spots_days !== undefined) byId('retention_spots_days').value = data.retention_spots_days;
  if (data.retention_messages_days !== undefined) byId('retention_messages_days').value = data.retention_messages_days;
  if (data.retention_bulletins_days !== undefined) byId('retention_bulletins_days').value = data.retention_bulletins_days;
  const lastRun = Number(data.retention_last_run_epoch || 0);
  let status = byId('retention_enabled').checked ? 'Automatic cleanup is enabled.' : 'Automatic cleanup is disabled.';
  if (lastRun > 0) {
    status += ` Last run: ${fmtEpoch(lastRun)}.`;
  }
  if (data.retention_last_result) {
    try {
      const parsed = JSON.parse(data.retention_last_result);
      status += ` Removed ${Number(parsed.spots || 0)} spots, ${Number(parsed.messages || 0)} messages, ${Number(parsed.bulletins || 0)} bulletins.`;
    } catch {}
  }
  setText('retentionStatus', status);
}
async function load() {
  const peer = encodeURIComponent(byId('peer').value.trim());
  const lim = parseInt(byId('phlim').value.trim(), 10) || 20;
  const userSearch = encodeURIComponent(byId('user_search').value.trim());
  const results = await Promise.allSettled([
    j('/api/stats'),
    j('/api/spots?limit=20'),
    j('/api/peers'),
    j('/api/policydrop' + (peer ? '?peer=' + peer : '')),
    j('/api/proto/summary'),
    j('/api/proto/acks'),
    j('/api/proto/history' + (peer ? '?peer=' + peer + '&' : '?') + 'limit=' + encodeURIComponent(lim)),
    j('/api/proto/thresholds'),
    j('/api/node/presentation'),
    j('/api/audit?limit=20'),
    j('/api/security?limit=20'),
    j('/api/users?privilege=sysop&limit=100'),
    j('/api/users?exclude_privilege=sysop&exclude_blocked=1&limit=' + USER_PAGE_SIZE + '&offset=' + encodeURIComponent(userOffset) + (userSearch ? '&search=' + userSearch : '')),
    j('/api/users?blocked=1&limit=200' + (userSearch ? '&search=' + userSearch : '')),
  ]);

  const [
    statsRes,
    spotsRes,
    peersRes,
    dropRes,
    protoRes,
    protoAcksRes,
    histRes,
    thresholdsRes,
    nodeUiRes,
    auditRes,
    securityRes,
    sysopsRes,
    usersRes,
    blockedRes,
  ] = results;

  const failures = results.filter((r) => r.status === 'rejected');

  if (statsRes.status === 'fulfilled') {
    const stats = statsRes.value || {};
    setText('navNodeBadge', stats.node || '-');
    setText('navUptime', fmtUptime(stats.uptime_seconds || 0));
    setText('navSpots', String(stats.spots || 0));
    setText('navTelnet', String(stats.sessions || 0));
    setText('navWeb', String(stats.web_sessions || 0));
    setText('statSessions', String(stats.sessions || 0));
    setText('statWebSessions', String(stats.web_sessions || 0));
    setText('statSpots', String(stats.spots || 0));
    setText('statUptime', fmtUptime(stats.uptime_seconds || 0));
  }

  if (spotsRes.status === 'fulfilled') setSpotRows(spotsRes.value);
  if (peersRes.status === 'fulfilled') setPeerRows(peersRes.value);
  if (dropRes.status === 'fulfilled') setDropRows(dropRes.value);
  if (protoRes.status === 'fulfilled') {
    const protoSummary = protoRes.value;
    setProtoRows(protoSummary);
    setText('protoTracked', String(Array.isArray(protoSummary) ? protoSummary.length : 0));
    setText('protoHealthy', String(Array.isArray(protoSummary) ? protoSummary.filter((r) => String(r.health || '').toLowerCase() === 'ok').length : 0));
    setText('protoAlerts', String(Array.isArray(protoSummary) ? protoSummary.filter((r) => ['degraded','stale','flapping'].includes(String(r.health || '').toLowerCase())).length : 0));
  }
  if (protoAcksRes.status === 'fulfilled') {
    const protoAcks = protoAcksRes.value;
    setText('protoAcked', String(Array.isArray(protoAcks) ? protoAcks.filter((r) => r.acked).length : 0));
  }
  if (histRes.status === 'fulfilled') setHistRows(histRes.value);
  if (thresholdsRes.status === 'fulfilled') {
    const thresholds = thresholdsRes.value || {};
    if (thresholds.stale_mins !== undefined) byId('pstale').value = thresholds.stale_mins;
    if (thresholds.flap_score !== undefined) byId('pflap').value = thresholds.flap_score;
    if (thresholds.flap_window_secs !== undefined) byId('pwindow').value = thresholds.flap_window_secs;
  }
  if (nodeUiRes.status === 'fulfilled') fillNodeForm(nodeUiRes.value);
  if (auditRes.status === 'fulfilled') setAuditRows(auditRes.value);
  if (securityRes.status === 'fulfilled') {
    setAuthFailRows((securityRes.value || {}).auth_failures || []);
    setBanRows((securityRes.value || {}).bans || []);
  }
  if (sysopsRes.status === 'fulfilled') setSysopRows((sysopsRes.value || {}).rows || []);
  if (usersRes.status === 'fulfilled') setUserRows(usersRes.value || {});
  if (blockedRes.status === 'fulfilled') setBlockedRows(blockedRes.value || {});
  setText('postingCall', webCall || '-');

  if (failures.length) {
    console.error(failures);
    say('Some System Operator data failed to load.', false);
  } else {
    say('Console data refreshed.');
  }
}

byId('reload').onclick = load;
document.querySelectorAll('.sidebar-nav a').forEach((el) => {
  el.addEventListener('click', (ev) => {
    ev.preventDefault();
    setView(el.dataset.view || 'node');
  });
});
byId('auditReload').onclick = async () => {
  await reloadAudit();
  say('Audit reloaded.');
};
byId('securityReload').onclick = async () => {
  const payload = await j('/api/security?limit=20');
  setAuthFailRows((payload || {}).auth_failures || []);
  setBanRows((payload || {}).bans || []);
  say('Security data refreshed.');
};
byId('auditCategory').onchange = async () => {
  await reloadAudit();
};
byId('themeToggle').onclick = () => {
  const current = document.documentElement.classList.contains('light') ? 'light' : 'dark';
  applyTheme(current === 'light' ? 'dark' : 'light');
};
byId('logout').onclick = async () => {
  await logoutSysop();
};
byId('login').onclick = async () => {
  try {
    sayLogin('');
    const call = byId('call').value.trim();
    const password = byId('pass').value;
    const r = await jw('/api/auth/login', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({call, password}), skipAuthReset:true});
    if (r && r.ok) {
      webTok = r.token;
      webCall = r.call;
      webIsSysop = !!r.sysop;
      if (webIsSysop) {
        persistWebSession();
        setConsoleVisible(true);
        sayLogin('');
        say('System Operator login established for ' + webCall + '.');
        await load();
      } else {
        setConsoleVisible(false);
        sayLogin('That callsign does not have System Operator access.', false);
        say('That callsign does not have System Operator access.', false);
      }
    } else {
      clearWebSession();
      setConsoleVisible(false);
      sayLogin('Login failed. Check your callsign and password.', false);
      say('Login failed. Check your callsign and password.', false);
    }
  } catch (err) {
    clearWebSession();
    setConsoleVisible(false);
    const message = errText(err);
    if (message === 'login failed') {
      sayLogin('Login failed. Check your callsign and password.', false);
      say('Login failed. Check your callsign and password.', false);
    } else if (message === 'login blocked') {
      sayLogin('Login blocked for this callsign.', false);
      say('Login blocked for this callsign.', false);
    } else if (message === 'web login not allowed') {
      sayLogin('Web login is not allowed for this callsign.', false);
      say('Web login is not allowed for this callsign.', false);
    } else {
      sayLogin('Login failed: ' + message, false);
      say('Login failed: ' + message, false);
    }
  }
};
byId('saveNode').onclick = async () => {
  try {
    await runButtonAction('saveNode', async () => {
      const payload = {
        branding_name: byId('branding_name').value.trim(),
        node_call: byId('node_call').value.trim(),
        node_alias: byId('node_alias').value.trim(),
        owner_name: byId('owner_name').value.trim(),
        qth: byId('qth').value.trim(),
        node_locator: byId('node_locator').value.trim().toUpperCase(),
        telnet_ports: byId('telnet_ports').value.trim(),
        welcome_title: byId('welcome_title').value.trim(),
        welcome_body: byId('welcome_body').value,
        login_tip: byId('login_tip').value.trim(),
        show_status_after_login: byId('show_status_after_login').checked,
        require_password: byId('require_password').checked,
        retention_enabled: byId('retention_enabled').checked,
        retention_spots_days: parseInt(byId('retention_spots_days').value.trim(), 10) || 30,
        retention_messages_days: parseInt(byId('retention_messages_days').value.trim(), 10) || 90,
        retention_bulletins_days: parseInt(byId('retention_bulletins_days').value.trim(), 10) || 30,
        support_contact: byId('support_contact').value.trim(),
        website_url: byId('website_url').value.trim(),
        motd: byId('motd').value,
        prompt_template: byId('prompt_template').value.trim()
      };
      const r = await j('/api/node/presentation', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload)});
      say(r && r.ok ? 'Node settings saved.' : 'Saving node settings failed.', !!(r && r.ok));
      await load();
    });
  } catch (err) {
    say('Saving node settings failed: ' + errText(err), false);
  }
};
byId('runCleanup').onclick = async () => {
  const r = await j('/api/maintenance/cleanup', {method:'POST'});
  const removed = r && r.removed ? r.removed : {};
  say(`Cleanup removed ${Number(removed.spots || 0)} spots, ${Number(removed.messages || 0)} messages, ${Number(removed.bulletins || 0)} bulletins.`, !!(r && r.ok));
  await load();
};
byId('userSearch').onclick = async () => {
  userOffset = 0;
  await load();
};
byId('newUser').onclick = async () => {
  clearUserForm('');
  say('Ready to add a new local callsign.');
};
byId('user_privilege').onchange = () => {
  const level = byId('user_privilege').value.trim();
  applyPrivilegeDefaults(level);
  if (level === '') say('Non-Authenticated selected. Login, chat, WX, WCY, and WWV remain allowed by default; spots and announcements stay disabled.');
  else if (level === 'blocked') say('Blocked selected. Cleared telnet and web access in the current form.');
  else say('Authenticated access defaults applied in the current form.');
};
byId('userPrev').onclick = async () => {
  userOffset = Math.max(0, userOffset - USER_PAGE_SIZE);
  await load();
};
byId('userNext').onclick = async () => {
  userOffset += USER_PAGE_SIZE;
  await load();
};
byId('saveUser').onclick = async () => {
  try {
    const payload = {
      original_call: selectedUserCall || '',
      call: byId('user_call').value.trim(),
      display_name: byId('user_name').value.trim(),
      home_node: byId('user_home_node').value.trim(),
      node_family: byId('user_node_family').value.trim(),
      qth: byId('user_qth').value.trim(),
      qra: byId('user_grid').value.trim().toUpperCase(),
      email: byId('user_email').value.trim(),
      privilege: byId('user_privilege').value.trim(),
      blocked_reason: byId('user_block_reason').value.trim().slice(0, 80),
      access: collectAccessMatrix(),
    };
    const r = await j('/api/users', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload)});
    if (r && r.ok && r.user) fillUserForm(r.user);
    say(r && r.ok ? 'User record saved.' : 'Saving user failed.', !!(r && r.ok));
    await load();
  } catch (err) {
    say('Saving user failed: ' + errText(err), false);
  }
};
byId('saveUserPassword').onclick = async () => {
  const call = byId('user_call').value.trim();
  const password = byId('user_password').value;
  if (!call) {
    say('A callsign is required before setting a password.', false);
    return;
  }
  try {
    const clearRequested = password.trim().toUpperCase() === 'CLEAR';
    if (!clearRequested && !password.trim()) {
      say('A password is required. Enter CLEAR to remove it.', false);
      return;
    }
    const path = clearRequested ? '/api/users/password/clear' : '/api/users/password';
    const payload = clearRequested ? {call} : {call, password};
    const r = await j(path, {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload)});
    if (r && r.ok && r.user) fillUserForm(r.user);
    byId('user_password').value = '';
    say(r && r.ok ? (clearRequested ? 'Password cleared.' : 'Password updated.') : (clearRequested ? 'Password clear failed.' : 'Password update failed.'), !!(r && r.ok));
    await load();
  } catch (err) {
    say('Password update failed: ' + errText(err), false);
  }
};
byId('accessAll').onclick = () => {
  setAccessMatrixAll(true);
  say('Enabled all channel and posting permissions for the current user form.');
};
byId('accessNone').onclick = () => {
  setAccessMatrixAll(false);
  say('Cleared all channel and posting permissions for the current user form.');
};
byId('deleteUser').onclick = async () => {
  try {
    const call = byId('user_call').value.trim();
    const r = await j('/api/users/delete', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({call})});
    if (r && r.ok) clearUserForm(webCall || '');
    say(r && r.ok ? 'User record removed.' : 'Removing user failed.', !!(r && r.ok));
    await load();
  } catch (err) {
    say('Removing user failed: ' + errText(err), false);
  }
};
byId('pconnect').onclick = async () => {
  const peer = byId('peername').value.trim();
  const dsn = mergePeerPassword(byId('peerdsn').value.trim(), byId('peerpass').value);
  const profile = byId('peerprof').value.trim();
  const reconnect = !!byId('peerretry').checked;
  await j('/api/peer/save', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({peer, dsn, profile, reconnect})});
  const r = await j('/api/peer/connect', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({peer, dsn, profile})});
  say(r && r.ok ? 'Connected ' + peer + '.' : 'Peer connect failed.', !!(r && r.ok));
  await load();
};
byId('peerSave').onclick = async () => {
  const peer = byId('peername').value.trim();
  const dsn = mergePeerPassword(byId('peerdsn').value.trim(), byId('peerpass').value);
  const profile = byId('peerprof').value.trim();
  const reconnect = !!byId('peerretry').checked;
  const r = await j('/api/peer/save', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({peer, dsn, profile, reconnect})});
  say(r && r.ok ? 'Saved peer ' + peer + '.' : 'Saving peer failed.', !!(r && r.ok));
  await load();
};
byId('newPeer').onclick = () => {
  clearPeerForm();
  say('Ready to create a new outbound peer.');
};
byId('pdisconnect').onclick = async () => {
  const peer = byId('peername').value.trim();
  const r = await j('/api/peer/disconnect', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({peer})});
  say(r && r.ok ? 'Disconnected ' + peer + '.' : 'Peer disconnect failed.', !!(r && r.ok));
  await load();
};
byId('reset').onclick = async () => {
  const peer = encodeURIComponent(byId('peer').value.trim());
  const r = await j('/api/policydrop/reset' + (peer ? '?peer=' + peer : ''), {method:'POST'});
  say(r && r.ok ? 'Policy drop counters reset.' : 'Policy drop reset failed.', !!(r && r.ok));
  await load();
};
async function postText(path, obj, label) {
  const text = String((obj && obj.text) || '').trim();
  if (!text) {
    say(label + ' text is required.', false);
    return;
  }
  try {
    const r = await jw(path, {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(obj)});
    say(r && r.ok ? label + ' posted by ' + r.posted_by + '.' : label + ' failed.', !!(r && r.ok));
    await load();
  } catch (err) {
    say(label + ' failed: ' + errText(err), false);
  }
}
byId('spot').onclick = async () => {
  try {
    const dx_call = byId('dx').value.trim();
    const freq_khz = parseFloat(byId('freq').value.trim());
    const info = byId('info').value;
    const r = await jw('/api/spot', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({freq_khz, dx_call, info})});
    say(r && r.ok ? 'Spot posted by ' + r.posted_by + '.' : 'Spot post failed.', !!(r && r.ok));
    await load();
  } catch (err) {
    say('Spot post failed: ' + errText(err), false);
  }
};
byId('chat').onclick = async () => { await postText('/api/chat', {text: byId('text').value}, 'Chat'); };
byId('announce').onclick = async () => { await postText('/api/announce', {text: byId('text').value, scope: byId('scope').value || 'LOCAL'}, 'Announce'); };
byId('wcy').onclick = async () => { await postText('/api/wcy', {text: byId('text').value}, 'WCY'); };
byId('wwv').onclick = async () => { await postText('/api/wwv', {text: byId('text').value}, 'WWV'); };
byId('wx').onclick = async () => { await postText('/api/wx', {text: byId('text').value}, 'WX'); };
byId('protoSave').onclick = async () => {
  const stale_mins = parseInt(byId('pstale').value.trim(), 10);
  const flap_score = parseInt(byId('pflap').value.trim(), 10);
  const flap_window_secs = parseInt(byId('pwindow').value.trim(), 10);
  const r = await j('/api/proto/thresholds', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({stale_mins, flap_score, flap_window_secs})});
  say(r && r.ok ? 'Protocol thresholds saved.' : 'Threshold update failed.', !!(r && r.ok));
  await load();
};
byId('phreset').onclick = async () => {
  const peer = byId('peer').value.trim();
  const url = peer ? '/api/proto/history/reset?peer=' + encodeURIComponent(peer) : '/api/proto/history/reset?all=1';
  const r = await j(url, {method:'POST'});
  say(r && r.ok ? 'Protocol history reset.' : 'Protocol history reset failed.', !!(r && r.ok));
  await load();
};
byId('phload').onclick = load;
applyTheme(localStorage.getItem(THEME_KEY) || 'dark');
setView((window.location.hash || '#node').slice(1) || 'node');
if (restoreWebSession()) {
  load().catch(() => {
    clearWebSession();
    say('System Operator session expired. Please sign in again.', false);
  });
} else {
  clearWebSession();
}
</script>
</body>
</html>"""

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            req_line = await reader.readline()
            if not req_line:
                writer.close()
                await writer.wait_closed()
                return

            try:
                method, target, _version = req_line.decode("ascii", errors="replace").strip().split(" ", 2)
            except ValueError:
                await self._write_response(writer, 400, self._json({"error": "malformed request"}))
                return

            headers: dict[str, str] = {}
            while True:
                line = await reader.readline()
                if line in {b"\r\n", b"\n", b""}:
                    break
                text = line.decode("ascii", errors="replace").strip()
                if ":" in text:
                    k, v = text.split(":", 1)
                    headers[k.strip().lower()] = v.strip()
            body = b""
            if method == "POST":
                clen_txt = headers.get("content-length", "0").strip()
                try:
                    clen = int(clen_txt)
                except ValueError:
                    clen = 0
                clen = max(0, min(clen, 1024 * 64))
                if clen > 0:
                    body = await reader.readexactly(clen)

            if method not in {"GET", "POST"}:
                await self._write_response(writer, 405, self._json({"error": "only GET/POST are supported"}))
                return

            parsed = urlparse(target)
            path = parsed.path
            q = parse_qs(parsed.query)

            if path == "/health":
                await self._write_response(writer, 200, self._json({"ok": True}))
                return

            if path == "/api/auth/login":
                if method != "POST":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                payload = self._parse_json_body(body)
                raw_call = str(payload.get("call", "")).strip()
                special_sysop = raw_call.upper() == "SYSOP"
                call = "SYSOP" if special_sysop else normalize_call(raw_call)
                base_call = call.split("-", 1)[0]
                password = str(payload.get("password", ""))
                if not special_sysop and not is_valid_call(call):
                    self._log_auth_failure(writer, headers, "sysop-web", call, "invalid_callsign")
                    await self._write_response(writer, 400, self._json({"error": "invalid callsign"}))
                    return
                blocked = False
                for candidate in (call, base_call):
                    raw_block = await self.store.get_user_pref(candidate, "blocked_login")
                    if str(raw_block or "").strip().lower() in {"1", "on", "yes", "true"}:
                        blocked = True
                        break
                if blocked:
                    self._log_auth_failure(writer, headers, "sysop-web", call, "blocked_login")
                    await self._write_response(writer, 401, self._json({"error": "login blocked"}))
                    return
                if not await self._access_allowed(call, "web", "login"):
                    self._log_auth_failure(writer, headers, "sysop-web", call, "web_login_not_allowed")
                    await self._write_response(writer, 401, self._json({"error": "web login not allowed"}))
                    return
                expected = await self.store.get_user_pref(call, "password")
                if expected is not None and str(expected).strip() and not verify_password(password, str(expected)):
                    self._log_auth_failure(writer, headers, "sysop-web", call, "bad_password")
                    await self._write_response(writer, 401, self._json({"error": "login failed"}))
                    return
                has_real_password = expected is not None and bool(str(expected).strip())
                is_sysop = has_real_password and verify_password(password, str(expected)) and await self._admin_privileged_call(call)
                if has_real_password and not is_password_hash(str(expected)) and verify_password(password, str(expected)):
                    await self.store.set_user_pref(call, "password", hash_password(password), int(time.time()))
                token, exp = self._issue_web_token(call, is_sysop=is_sysop)
                await self.store.record_login(
                    call,
                    int(time.time()),
                    describe_session_path(
                        "sysop-web",
                        writer.get_extra_info("peername") if hasattr(writer, "get_extra_info") else None,
                        writer.get_extra_info("sockname") if hasattr(writer, "get_extra_info") else None,
                        headers.get("x-forwarded-for", ""),
                    ),
                )
                self._audit("sysop", f"{call} logged in to System Operator web")
                await self._write_response(
                    writer,
                    200,
                    self._json({"ok": True, "call": call, "token": token, "expires_epoch": exp, "sysop": is_sysop}),
                )
                return

            if path == "/api/auth/logout":
                if method != "POST":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                self._revoke_web_token(headers)
                await self._write_response(writer, 200, self._json({"ok": True}))
                return

            if path == "/api/stats":
                if method != "GET":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                if not self._is_authorized(headers):
                    await self._write_response(writer, 401, self._json({"error": "unauthorized"}))
                    return
                uptime = datetime.now(timezone.utc) - self.started_at
                self._cleanup_web_sessions()
                body = {
                    "node": self.config.node.node_call,
                    "uptime_seconds": int(uptime.total_seconds()),
                    "sessions": int(self.session_count_fn()),
                    "web_sessions": int(len(self._web_sessions)),
                    "spots": await self.store.count_spots(),
                }
                await self._write_response(writer, 200, self._json(body))
                return

            if path == "/api/audit":
                if method != "GET":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                if not self._is_authorized(headers):
                    await self._write_response(writer, 401, self._json({"error": "unauthorized"}))
                    return
                limit = self._parse_limit(q, "limit", default=20, low=1, high=200)
                category = str(q.get("category", [""])[0]).strip().lower()
                allowed = {"sysop", "user", "config", "control", "connect", "disconnect"}
                categories = {category} if category in allowed else None
                rows = self.audit_rows_fn(limit, categories) if self.audit_rows_fn else []
                await self._write_response(writer, 200, self._json(rows))
                return

            if path == "/api/security":
                if method != "GET":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                if not self._is_authorized(headers):
                    await self._write_response(writer, 401, self._json({"error": "unauthorized"}))
                    return
                limit = self._parse_limit(q, "limit", default=20, low=1, high=200)
                auth_rows = self._read_recent_auth_failures(limit)
                ban_rows = self._fail2ban_ban_rows()
                await self._write_response(writer, 200, self._json({"auth_failures": auth_rows, "bans": ban_rows}))
                return

            if path == "/api/node/presentation":
                if not self._is_authorized(headers):
                    await self._write_response(writer, 401, self._json({"error": "unauthorized"}))
                    return
                if method == "GET":
                    await self._write_response(writer, 200, self._json(self._node_presentation_json(await self._node_presentation())))
                    return
                if method == "POST":
                    payload = self._parse_json_body(body)
                    now = int(time.time())
                    fields = {
                        "node_call": "",
                        "node_alias": "",
                        "owner_name": "",
                        "qth": "",
                        "node_locator": "",
                        "telnet_ports": "",
                        "branding_name": "",
                        "welcome_title": "",
                        "welcome_body": "",
                        "login_tip": "",
                        "show_status_after_login": "off",
                        "require_password": "on",
                        "retention_enabled": "on",
                        "retention_spots_days": "30",
                        "retention_messages_days": "90",
                        "retention_bulletins_days": "30",
                        "support_contact": "",
                        "website_url": "",
                        "motd": "",
                        "prompt_template": "",
                    }
                    old_node_call = self.config.node.node_call
                    cfg_updates: dict[str, object] = {}
                    telnet_ports_value: tuple[int, ...] | None = None
                    for key in fields:
                        if key not in payload:
                            continue
                        if key in {"show_status_after_login", "require_password", "retention_enabled"}:
                            flag = bool(payload.get(key, False))
                            val = "on" if flag else "off"
                            if key in _CONFIG_AUTH_NODE_FIELDS:
                                cfg_updates[key] = flag
                        elif key in {"retention_spots_days", "retention_messages_days", "retention_bulletins_days"}:
                            try:
                                val = str(max(1, min(3650, int(payload.get(key, fields[key])))))
                            except Exception:
                                await self._write_response(writer, 400, self._json({"error": f"invalid {key}"}))
                                return
                        elif key == "telnet_ports":
                            try:
                                ports = parse_telnet_ports(payload.get(key, ""), fallback=self.config.telnet.port)
                            except Exception:
                                await self._write_response(writer, 400, self._json({"error": "invalid telnet_ports"}))
                                return
                            val = ",".join(str(p) for p in ports)
                            telnet_ports_value = tuple(ports)
                            cfg_updates[key] = tuple(ports)
                        else:
                            val = str(payload.get(key, "")).strip()
                            if key in {"node_call", "node_alias", "node_locator"}:
                                val = val.upper()
                            if key == "prompt_template" and len(val) > 256:
                                await self._write_response(writer, 400, self._json({"error": "prompt_template too long"}))
                                return
                            if key in _CONFIG_AUTH_NODE_FIELDS:
                                cfg_updates[key] = val
                        if key not in _CONFIG_AUTH_NODE_FIELDS:
                            await self.store.set_user_pref(self.config.node.node_call, key, val, now)
                    if cfg_updates:
                        if "node_call" in cfg_updates:
                            new_node_call = str(cfg_updates["node_call"]).strip().upper()
                            if not new_node_call:
                                await self._write_response(writer, 400, self._json({"error": "node_call is required"}))
                                return
                            if new_node_call != old_node_call:
                                await self.store.rename_call_namespace(old_node_call, new_node_call)
                                await self.store.rename_user_registry(old_node_call, new_node_call, now)
                                self.config.node.node_call = new_node_call
                        for key, value in cfg_updates.items():
                            if key == "telnet_ports":
                                ports = tuple(int(p) for p in value)
                                self.config.telnet.ports = ports
                                if ports:
                                    self.config.telnet.port = ports[0]
                                continue
                            if hasattr(self.config.node, key):
                                setattr(self.config.node, key, value)
                        if self.config_path:
                            try:
                                save_config(self.config_path, self.config)
                            except Exception as exc:
                                await self._write_response(writer, 500, self._json({"error": f"config save failed: {exc}"}))
                                return
                    self._audit("config", f"{self._authorized_call(headers)} updated node presentation settings")
                    if "telnet_ports" in payload and self.telnet_rebind_fn:
                        try:
                            ports = telnet_ports_value or parse_telnet_ports(payload.get("telnet_ports", ""), fallback=self.config.telnet.port)
                            await self.telnet_rebind_fn(tuple(ports))
                        except Exception as exc:
                            await self._write_response(writer, 500, self._json({"error": f"telnet rebind failed: {exc}"}))
                            return
                    await self._write_response(writer, 200, self._json({"ok": True, **self._node_presentation_json(await self._node_presentation())}))
                    return
                await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                return

            if path == "/api/maintenance/cleanup":
                if method != "POST":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                if not self._is_authorized(headers):
                    await self._write_response(writer, 401, self._json({"error": "unauthorized"}))
                    return
                node_cfg = await self.store.list_user_prefs(self.config.node.node_call)
                now = int(time.time())
                enabled = str(node_cfg.get("retention.enabled", "")).strip().lower() in {"1", "on", "yes", "true"}
                if not enabled:
                    await self._write_response(writer, 400, self._json({"error": "automatic cleanup is disabled"}))
                    return
                def _to_int(v: str | object, default: int) -> int:
                    try:
                        return max(1, min(3650, int(str(v or "").strip())))
                    except Exception:
                        return default
                removed = await self.store.apply_retention(
                    now,
                    spots_days=_to_int(node_cfg.get("retention.spots_days"), 30),
                    messages_days=_to_int(node_cfg.get("retention.messages_days"), 90),
                    bulletins_days=_to_int(node_cfg.get("retention.bulletins_days"), 30),
                )
                await self.store.set_user_pref(self.config.node.node_call, "retention.last_run_epoch", str(now), now)
                await self.store.set_user_pref(
                    self.config.node.node_call,
                    "retention.last_result",
                    json.dumps(removed, separators=(",", ":"), ensure_ascii=True),
                    now,
                )
                self._audit("control", f"{self._authorized_call(headers)} ran retention cleanup spots={removed.get('spots', 0)} messages={removed.get('messages', 0)} bulletins={removed.get('bulletins', 0)}")
                await self._write_response(writer, 200, self._json({"ok": True, "removed": removed}))
                return

            if path == "/api/users":
                if not self._is_authorized(headers):
                    await self._write_response(writer, 401, self._json({"error": "unauthorized"}))
                    return
                if method == "GET":
                    limit = self._parse_limit(q, "limit", default=25, low=1, high=200)
                    offset = self._parse_limit(q, "offset", default=0, low=0, high=100000)
                    privilege = str(q.get("privilege", [""])[0]).strip().lower()
                    exclude_privilege = str(q.get("exclude_privilege", [""])[0]).strip().lower()
                    if exclude_privilege == "admin":
                        exclude_privilege = "sysop"
                    exclude_blocked = str(q.get("exclude_blocked", [""])[0]).strip().lower() in {"1", "on", "yes", "true"}
                    search = str(q.get("search", [""])[0]).strip()
                    blocked_only = str(q.get("blocked", [""])[0]).strip().lower() in {"1", "on", "yes", "true"}
                    if blocked_only or exclude_privilege or exclude_blocked:
                        rows = await self.store.list_user_registry(limit=1000, offset=0, privilege=privilege, search=search)
                        body_all = [await self._user_registry_json(r) for r in rows]
                        if exclude_privilege:
                            body_all = [r for r in body_all if str(r.get("privilege", "")).strip().lower() != exclude_privilege]
                        if exclude_blocked:
                            body_all = [r for r in body_all if not bool(r.get("blocked_login"))]
                        if blocked_only:
                            body_all = [r for r in body_all if bool(r.get("blocked_login"))]
                        total = len(body_all)
                        body_rows = body_all[offset : offset + limit]
                    else:
                        rows = await self.store.list_user_registry(
                            limit=limit,
                            offset=offset,
                            privilege=privilege,
                            search=search,
                        )
                        total = await self.store.count_user_registry(privilege=privilege, search=search)
                        body_rows = [await self._user_registry_json(r) for r in rows]
                    await self._write_response(
                        writer,
                        200,
                        self._json(
                            {
                                "rows": body_rows,
                                "total": total,
                                "offset": offset,
                                "limit": limit,
                                "privilege": privilege,
                                "exclude_privilege": exclude_privilege,
                                "exclude_blocked": exclude_blocked,
                                "blocked": blocked_only,
                                "search": search,
                            }
                        ),
                    )
                    return
                if method == "POST":
                    payload = self._parse_json_body(body)
                    original_call = normalize_call(str(payload.get("original_call", "")).strip())
                    call = normalize_call(str(payload.get("call", "")).strip())
                    original_base = original_call.split("-", 1)[0] if original_call else ""
                    base_call = call.split("-", 1)[0]
                    if not _is_valid_admin_record_call(call):
                        await self._write_response(writer, 400, self._json({"error": "invalid callsign"}))
                        return
                    privilege = str(payload.get("privilege", "")).strip().lower()
                    if privilege == "admin":
                        privilege = "sysop"
                    blocked_login = privilege == "blocked"
                    if blocked_login:
                        privilege = "user"
                    if privilege and privilege not in {"", "user", "sysop"}:
                        await self._write_response(writer, 400, self._json({"error": "invalid privilege"}))
                        return
                    now = int(time.time())
                    qth = str(payload.get("qth", "")).strip()
                    qra = str(payload.get("qra", "")).strip().upper()
                    if qth:
                        coords = resolve_location_to_coords(qth)
                        if coords is not None:
                            qra = coords_to_locator(*coords)
                    elif qra:
                        qra = extract_locator(qra)
                        if qra:
                            qth = estimate_location_from_locator(qra).strip()
                    if original_call and original_call != call:
                        try:
                            renamed = await self.store.rename_user_registry(original_call, call, now)
                        except ValueError as exc:
                            await self._write_response(writer, 400, self._json({"error": str(exc)}))
                            return
                        if not renamed:
                            await self._write_response(writer, 400, self._json({"error": "original callsign not found"}))
                            return
                        moved_password = await self.store.get_user_pref(call, "password")
                        if moved_password is not None and str(moved_password).strip() and not is_password_hash(str(moved_password)):
                            await self.store.set_user_pref(call, "password", hash_password(str(moved_password)), now)
                    access_payload = payload.get("access")
                    await self.store.upsert_user_registry(
                        call,
                        now,
                        display_name=str(payload.get("display_name", "")).strip(),
                        home_node=str(payload.get("home_node", "")).strip().upper(),
                        address=str(payload.get("address", "")).strip(),
                        qth=qth,
                        qra=qra,
                        email=str(payload.get("email", "")).strip(),
                        privilege=privilege,
                    )
                    home_node = str(payload.get("home_node", "")).strip().upper()
                    if home_node:
                        await self.store.set_user_pref(call, "homenode", home_node, now)
                    else:
                        await self.store.delete_user_pref(call, "homenode")
                    node_family = str(payload.get("node_family", "")).strip().lower()
                    if node_family and node_family not in {"pycluster", "dxspider", "dxnet", "arcluster", "clx"}:
                        await self._write_response(writer, 400, self._json({"error": "invalid node family"}))
                        return
                    if node_family:
                        await self.store.set_user_pref(call, "node_family", node_family, now)
                    else:
                        await self.store.delete_user_pref(call, "node_family")
                    blocked_reason = str(payload.get("blocked_reason", "")).strip()[:80]
                    channels = self._access_channels()
                    capabilities = self._access_capabilities()
                    if original_base and original_base != base_call:
                        for channel in channels:
                            for capability in capabilities:
                                await self.store.delete_user_pref(original_base, self._access_pref_key(channel, capability))
                    if isinstance(access_payload, dict):
                        for channel in channels:
                            channel_payload = access_payload.get(channel, {})
                            if not isinstance(channel_payload, dict):
                                continue
                            for capability in capabilities:
                                value = channel_payload.get(capability)
                                if value is None:
                                    continue
                                await self.store.set_user_pref(
                                    base_call,
                                    self._access_pref_key(channel, capability),
                                    "on" if bool(value) else "off",
                                    now,
                                )
                    block_targets = {base_call}
                    if call:
                        block_targets.add(call)
                    if original_call:
                        block_targets.add(original_call)
                    if original_base:
                        block_targets.add(original_base)
                    for target in sorted({t for t in block_targets if t}):
                        if blocked_reason:
                            await self.store.set_user_pref(target, "note", blocked_reason, now)
                        else:
                            await self.store.delete_user_pref(target, "note")
                        if blocked_login:
                            await self.store.set_user_pref(target, "blocked_login", "on", now)
                            if blocked_reason:
                                await self.store.set_user_pref(target, "blocked_reason", blocked_reason, now)
                            else:
                                await self.store.delete_user_pref(target, "blocked_reason")
                        else:
                            await self.store.delete_user_pref(target, "blocked_login")
                            await self.store.delete_user_pref(target, "blocked_reason")
                    row = await self.store.get_user_registry(call)
                    self._audit(
                        "sysop",
                        f"{self._authorized_call(headers)} saved user {call} level={('blocked' if blocked_login else (privilege or 'none'))}",
                    )
                    await self._write_response(
                        writer,
                        200,
                        self._json({"ok": True, "user": await self._user_registry_json(row) if row else {"call": call}}),
                    )
                    return
                await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                return

            if path == "/api/users/delete":
                if method != "POST":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                if not self._is_authorized(headers):
                    await self._write_response(writer, 401, self._json({"error": "unauthorized"}))
                    return
                payload = self._parse_json_body(body)
                call = normalize_call(str(payload.get("call", "")).strip())
                if not _is_valid_admin_record_call(call):
                    await self._write_response(writer, 400, self._json({"error": "invalid callsign"}))
                    return
                removed = await self.store.delete_user_registry(call)
                self._audit("sysop", f"{self._authorized_call(headers)} removed user {call} removed={removed}")
                await self._write_response(writer, 200, self._json({"ok": removed > 0, "removed": removed, "call": call}))
                return

            if path == "/api/users/password":
                if method != "POST":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                if not self._is_authorized(headers):
                    await self._write_response(writer, 401, self._json({"error": "unauthorized"}))
                    return
                payload = self._parse_json_body(body)
                call = normalize_call(str(payload.get("call", "")).strip())
                password = str(payload.get("password", ""))
                if not _is_valid_admin_record_call(call):
                    await self._write_response(writer, 400, self._json({"error": "invalid callsign"}))
                    return
                if not password.strip():
                    await self._write_response(writer, 400, self._json({"error": "password is required"}))
                    return
                now = int(time.time())
                await self.store.set_user_pref(call, "password", hash_password(password), now)
                row = await self.store.get_user_registry(call)
                if row is None:
                    await self.store.upsert_user_registry(call, now)
                    row = await self.store.get_user_registry(call)
                self._audit("sysop", f"{self._authorized_call(headers)} set password for {call}")
                await self._write_response(
                    writer,
                    200,
                    self._json({"ok": True, "call": call, "user": await self._user_registry_json(row) if row else {"call": call, "has_password": True}}),
                )
                return

            if path == "/api/users/password/clear":
                if method != "POST":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                if not self._is_authorized(headers):
                    await self._write_response(writer, 401, self._json({"error": "unauthorized"}))
                    return
                payload = self._parse_json_body(body)
                call = normalize_call(str(payload.get("call", "")).strip())
                if not _is_valid_admin_record_call(call):
                    await self._write_response(writer, 400, self._json({"error": "invalid callsign"}))
                    return
                removed = await self.store.delete_user_pref(call, "password")
                row = await self.store.get_user_registry(call)
                self._audit("sysop", f"{self._authorized_call(headers)} cleared password for {call} removed={removed}")
                await self._write_response(
                    writer,
                    200,
                    self._json({"ok": True, "removed": removed, "call": call, "user": await self._user_registry_json(row) if row else {"call": call, "has_password": False}}),
                )
                return

            if path == "/api/spots":
                if method != "GET":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                if not self._is_authorized(headers):
                    await self._write_response(writer, 401, self._json({"error": "unauthorized"}))
                    return
                limit = self._parse_limit(q, "limit", default=20, low=1, high=200)
                rows = await self.store.latest_spots(limit=limit)
                body = [
                    {
                        "freq_khz": r["freq_khz"],
                        "dx_call": r["dx_call"],
                        "epoch": r["epoch"],
                        "info": r["info"],
                        "spotter": r["spotter"],
                        "source_node": r["source_node"],
                    }
                    for r in rows
                ]
                await self._write_response(writer, 200, self._json(body))
                return

            if path == "/api/peers":
                if method != "GET":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                if not self._is_authorized(headers):
                    await self._write_response(writer, 401, self._json({"error": "unauthorized"}))
                    return
                if not self.link_stats_fn:
                    stats = {}
                else:
                    stats = await self.link_stats_fn()
                desired_rows = []
                if self.link_desired_peers_fn:
                    desired_rows = await self.link_desired_peers_fn()
                desired_map = {str(row.get("peer", "")): row for row in desired_rows if str(row.get("peer", "")).strip()}
                node_cfg = await self.store.list_user_prefs(self.config.node.node_call)
                node_cfg.update(await self.store.list_user_vars(self.config.node.node_call))
                now_epoch = int(time.time())
                out = []
                for name in sorted(set(stats) | set(desired_map)):
                    st = stats.get(name, {})
                    desired = desired_map.get(name, {})
                    desired_transport, desired_path = describe_transport_dsn(str(desired.get("dsn", "")))
                    proto = self._proto_state_for_peer(node_cfg, name, now_epoch)
                    out.append(
                        {
                            "peer": name,
                            "profile": str(st.get("profile", desired.get("profile", "dxspider"))),
                            "inbound": bool(st.get("inbound", False)),
                            "parsed_frames": int(st.get("parsed_frames", 0)),
                            "sent_frames": int(st.get("sent_frames", 0)),
                            "policy_dropped": int(st.get("policy_dropped", 0)),
                            "last_pc_type": str(st.get("last_pc_type") or ""),
                            "rx_by_type": st.get("rx_by_type", {}),
                            "tx_by_type": st.get("tx_by_type", {}),
                            "policy_reasons": st.get("policy_reasons", {}),
                            "transport": str(st.get("transport", desired.get("transport", desired_transport))),
                            "path_hint": str(st.get("path_hint", desired_path)),
                            "proto": proto,
                            "desired": bool(desired),
                            "connected": name in stats,
                            "reconnect_enabled": bool(desired.get("reconnect_enabled", False)),
                            "retry_count": int(desired.get("retry_count", 0) or 0),
                            "next_retry_epoch": int(desired.get("next_retry_epoch", 0) or 0),
                            "last_connect_epoch": int(desired.get("last_connect_epoch", 0) or 0),
                            "last_error": str(desired.get("last_error", "")),
                            "dsn": str(desired.get("dsn", "")),
                        }
                    )
                await self._write_response(writer, 200, self._json(out))
                return

            if path == "/api/proto/thresholds":
                if not self._is_authorized(headers):
                    await self._write_response(writer, 401, self._json({"error": "unauthorized"}))
                    return
                if method == "GET":
                    node_cfg = await self.store.list_user_prefs(self.config.node.node_call)
                    node_cfg.update(await self.store.list_user_vars(self.config.node.node_call))
                    await self._write_response(writer, 200, self._json(self._proto_thresholds(node_cfg)))
                    return
                if method == "POST":
                    payload = self._parse_json_body(body)
                    node_cfg = await self.store.list_user_prefs(self.config.node.node_call)
                    node_cfg.update(await self.store.list_user_vars(self.config.node.node_call))
                    cur = self._proto_thresholds(node_cfg)
                    now = int(time.time())
                    for fld in ("stale_mins", "flap_score", "flap_window_secs"):
                        if fld in payload:
                            try:
                                cur[fld] = int(payload.get(fld, cur[fld]))
                            except Exception:
                                await self._write_response(writer, 400, self._json({"error": f"invalid {fld}"}))
                                return
                    cur = self._proto_thresholds({f"proto.threshold.{k}": str(v) for k, v in cur.items()})
                    for k, v in cur.items():
                        await self.store.set_user_pref(
                            self.config.node.node_call,
                            f"proto.threshold.{k}",
                            str(v),
                            now,
                        )
                    self._audit("config", f"{self._authorized_call(headers)} updated protocol thresholds")
                    await self._write_response(writer, 200, self._json({"ok": True, **cur}))
                    return
                await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                return

            if path == "/api/proto/history":
                if method != "GET":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                if not self._is_authorized(headers):
                    await self._write_response(writer, 401, self._json({"error": "unauthorized"}))
                    return
                peer_filter = q.get("peer", [""])[0].strip().lower()
                limit = self._parse_limit(q, "limit", default=20, low=1, high=200)
                node_cfg = await self.store.list_user_prefs(self.config.node.node_call)
                node_cfg.update(await self.store.list_user_vars(self.config.node.node_call))
                rows: list[dict[str, object]] = []
                for k, raw in sorted(node_cfg.items()):
                    if not (k.startswith("proto.peer.") and k.endswith(".history")):
                        continue
                    ptag = k[len("proto.peer.") : -len(".history")]
                    if peer_filter and peer_filter not in ptag:
                        continue
                    try:
                        arr = json.loads(str(raw))
                    except Exception:
                        arr = []
                    if not isinstance(arr, list):
                        continue
                    for item in arr[-limit:]:
                        if not isinstance(item, dict):
                            continue
                        rows.append(
                            {
                                "peer": ptag,
                                "epoch": int(item.get("epoch", 0) or 0),
                                "key": str(item.get("key", "")),
                                "from": str(item.get("from", "")),
                                "to": str(item.get("to", "")),
                            }
                        )
                rows.sort(key=lambda r: int(r.get("epoch", 0)), reverse=True)
                await self._write_response(writer, 200, self._json(rows[:limit]))
                return

            if path == "/api/proto/events":
                if method != "GET":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                if not self._is_authorized(headers):
                    await self._write_response(writer, 401, self._json({"error": "unauthorized"}))
                    return
                peer_filter = q.get("peer", [""])[0].strip().lower()
                key_filter = q.get("key", [""])[0].strip().lower()
                since_mins = 0
                try:
                    if "since_mins" in q and q["since_mins"]:
                        since_mins = max(0, int(q["since_mins"][0]))
                except ValueError:
                    since_mins = 0
                limit = self._parse_limit(q, "limit", default=50, low=1, high=400)
                node_cfg = await self.store.list_user_prefs(self.config.node.node_call)
                node_cfg.update(await self.store.list_user_vars(self.config.node.node_call))
                rows: list[dict[str, object]] = []
                for k, raw in sorted(node_cfg.items()):
                    if not (k.startswith("proto.peer.") and k.endswith(".history")):
                        continue
                    ptag = k[len("proto.peer.") : -len(".history")]
                    if peer_filter and peer_filter not in ptag:
                        continue
                    try:
                        arr = json.loads(str(raw))
                    except Exception:
                        arr = []
                    if not isinstance(arr, list):
                        continue
                    for item in arr:
                        if not isinstance(item, dict):
                            continue
                        evt = {
                            "peer": ptag,
                            "epoch": int(item.get("epoch", 0) or 0),
                            "key": str(item.get("key", "")),
                            "from": str(item.get("from", "")),
                            "to": str(item.get("to", "")),
                        }
                        rows.append(evt)
                rows.sort(key=lambda r: int(r.get("epoch", 0)), reverse=True)
                if key_filter:
                    rows = [r for r in rows if key_filter in str(r.get("key", "")).lower()]
                if since_mins > 0:
                    cutoff = int(time.time()) - since_mins * 60
                    rows = [r for r in rows if int(r.get("epoch", 0) or 0) >= cutoff]
                await self._write_response(writer, 200, self._json(rows[:limit]))
                return

            if path == "/api/proto/alerts":
                if method != "GET":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                if not self._is_authorized(headers):
                    await self._write_response(writer, 401, self._json({"error": "unauthorized"}))
                    return
                if not self.link_stats_fn:
                    await self._write_response(writer, 200, self._json([]))
                    return
                peer_filter = q.get("peer", [""])[0].strip().lower()
                stale_mins_override = 0
                include_acked = q.get("include_acked", [""])[0].strip().lower() in {"1", "on", "yes", "true"}
                try:
                    if "stale_mins" in q and q["stale_mins"]:
                        stale_mins_override = max(1, min(24 * 60, int(q["stale_mins"][0])))
                except ValueError:
                    stale_mins_override = 0
                stats = await self.link_stats_fn()
                node_cfg = await self.store.list_user_prefs(self.config.node.node_call)
                node_cfg.update(await self.store.list_user_vars(self.config.node.node_call))
                now_epoch = int(time.time())
                rows: list[dict[str, object]] = []
                for name in sorted(stats):
                    if peer_filter and peer_filter not in name.lower():
                        continue
                    proto = self._proto_state_for_peer(node_cfg, name, now_epoch)
                    if not bool(proto.get("known")):
                        continue
                    health = str(proto.get("health", "unknown")).lower()
                    raw_age_min = proto.get("age_min", -1)
                    try:
                        age_min = int(raw_age_min)
                    except (TypeError, ValueError):
                        age_min = -1
                    if stale_mins_override > 0 and (age_min < 0 or age_min > stale_mins_override):
                        health = "stale"
                    ack_epoch = 0
                    try:
                        ack_epoch = int(
                            node_cfg.get(
                                f"proto.peer.{re.sub(r'[^a-z0-9_.-]', '_', name.lower())}.alert_ack_epoch",
                                "0",
                            )
                        )
                    except ValueError:
                        ack_epoch = 0
                    last_epoch = int(proto.get("last_epoch", 0) or 0)
                    suppressed = ack_epoch > 0 and last_epoch > 0 and ack_epoch >= last_epoch
                    if suppressed and not include_acked:
                        continue
                    if suppressed:
                        health = "acked"
                    if health not in {"degraded", "flapping", "stale"}:
                        if health != "acked":
                            continue
                    rows.append(
                        {
                            "peer": name,
                            "health": health,
                            "suppressed": bool(suppressed),
                            "ack_epoch": ack_epoch,
                            "age_min": age_min,
                            "profile": str(stats[name].get("profile", "dxspider")),
                            "flap_score": int(proto.get("flap_score", 0) or 0),
                            "change_count": int(proto.get("change_count", 0) or 0),
                            "last_pc_type": str(stats[name].get("last_pc_type") or ""),
                        }
                    )
                await self._write_response(writer, 200, self._json(rows))
                return

            if path == "/api/proto/acks":
                if method != "GET":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                if not self._is_authorized(headers):
                    await self._write_response(writer, 401, self._json({"error": "unauthorized"}))
                    return
                peer_filter = q.get("peer", [""])[0].strip().lower()
                node_cfg = await self.store.list_user_prefs(self.config.node.node_call)
                node_cfg.update(await self.store.list_user_vars(self.config.node.node_call))
                now_epoch = int(time.time())
                rows: list[dict[str, object]] = []
                for key in sorted(node_cfg):
                    if not (key.startswith("proto.peer.") and key.endswith(".alert_ack_epoch")):
                        continue
                    peer = key[len("proto.peer.") : -len(".alert_ack_epoch")]
                    if peer_filter and peer_filter not in peer.lower():
                        continue
                    raw = str(node_cfg.get(key, "0"))
                    try:
                        ack_epoch = int(raw)
                    except ValueError:
                        continue
                    if ack_epoch <= 0:
                        continue
                    last_key = f"proto.peer.{peer}.last_epoch"
                    try:
                        last_epoch = int(str(node_cfg.get(last_key, "0")))
                    except ValueError:
                        last_epoch = 0
                    rows.append(
                        {
                            "peer": peer,
                            "ack_epoch": ack_epoch,
                            "age_min": (now_epoch - ack_epoch) // 60 if ack_epoch > 0 else -1,
                            "last_epoch": last_epoch,
                            "suppressed": bool(last_epoch > 0 and ack_epoch >= last_epoch),
                        }
                    )
                await self._write_response(writer, 200, self._json(rows))
                return

            if path == "/api/proto/alerts/ack":
                if method != "POST":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                if not self._is_authorized(headers):
                    await self._write_response(writer, 401, self._json({"error": "unauthorized"}))
                    return
                peer_filter = q.get("peer", [""])[0].strip().lower()
                all_peers = q.get("all", [""])[0].strip().lower() in {"1", "on", "yes", "true"}
                if not peer_filter and not all_peers:
                    await self._write_response(writer, 400, self._json({"error": "peer or all=1 is required"}))
                    return
                stats = await self.link_stats_fn() if self.link_stats_fn else {}
                node_cfg = await self.store.list_user_prefs(self.config.node.node_call)
                targets: set[str] = set()
                if all_peers:
                    targets.update(re.sub(r"[^a-z0-9_.-]", "_", p.lower()) for p in stats)
                    for k in node_cfg:
                        if k.startswith("proto.peer."):
                            p = k[len("proto.peer.") :].split(".", 1)[0]
                            if p:
                                targets.add(p)
                else:
                    pf = re.sub(r"[^a-z0-9_.-]", "_", peer_filter)
                    for p in (set(stats.keys()) | {k[len("proto.peer.") :].split(".", 1)[0] for k in node_cfg if k.startswith("proto.peer.")}):
                        pt = re.sub(r"[^a-z0-9_.-]", "_", str(p).lower())
                        if peer_filter in str(p).lower() or pf in pt:
                            targets.add(pt)
                now = int(time.time())
                changed = 0
                for p in sorted(targets):
                    await self.store.set_user_pref(self.config.node.node_call, f"proto.peer.{p}.alert_ack_epoch", str(now), now)
                    changed += 1
                self._audit("control", f"{self._authorized_call(headers)} acknowledged protocol alerts peers={changed}")
                await self._write_response(writer, 200, self._json({"ok": True, "acked_peers": changed, "all": all_peers, "peer": peer_filter}))
                return

            if path == "/api/proto/alerts/unack":
                if method != "POST":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                if not self._is_authorized(headers):
                    await self._write_response(writer, 401, self._json({"error": "unauthorized"}))
                    return
                peer_filter = q.get("peer", [""])[0].strip().lower()
                all_peers = q.get("all", [""])[0].strip().lower() in {"1", "on", "yes", "true"}
                if not peer_filter and not all_peers:
                    await self._write_response(writer, 400, self._json({"error": "peer or all=1 is required"}))
                    return
                prefs = await self.store.list_user_prefs(self.config.node.node_call)
                keys: list[str] = []
                for k in prefs:
                    if not (k.startswith("proto.peer.") and k.endswith(".alert_ack_epoch")):
                        continue
                    if all_peers:
                        keys.append(k)
                        continue
                    p = k[len("proto.peer.") : -len(".alert_ack_epoch")]
                    if peer_filter in p:
                        keys.append(k)
                removed = 0
                for k in keys:
                    removed += await self.store.delete_user_pref(self.config.node.node_call, k)
                self._audit("control", f"{self._authorized_call(headers)} unacknowledged protocol alerts removed={removed}")
                await self._write_response(writer, 200, self._json({"ok": True, "removed": removed, "all": all_peers, "peer": peer_filter}))
                return

            if path == "/api/proto/history/reset":
                if method != "POST":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                if not self._is_authorized(headers):
                    await self._write_response(writer, 401, self._json({"error": "unauthorized"}))
                    return
                peer_filter = q.get("peer", [""])[0].strip().lower()
                clear_all = q.get("all", [""])[0].strip().lower() in {"1", "on", "yes", "true"}
                if not peer_filter and not clear_all:
                    await self._write_response(writer, 400, self._json({"error": "peer or all=1 is required"}))
                    return
                prefs = await self.store.list_user_prefs(self.config.node.node_call)
                keys: list[str] = []
                peers_touched: set[str] = set()
                pfilter_s = re.sub(r"[^a-z0-9_.-]", "_", peer_filter)
                for k in prefs:
                    if not (k.startswith("proto.peer.") and (k.endswith(".history") or ".change." in k or k.endswith(".change_count") or k.endswith(".flap_score") or k.endswith(".last_change_epoch"))):
                        continue
                    if not peer_filter:
                        ptag = k[len("proto.peer.") :].split(".", 1)[0]
                        peers_touched.add(ptag)
                        keys.append(k)
                        continue
                    ptag = k[len("proto.peer.") :].split(".", 1)[0]
                    if peer_filter in ptag or pfilter_s in ptag:
                        peers_touched.add(ptag)
                        keys.append(k)
                removed = 0
                for k in keys:
                    removed += await self.store.delete_user_pref(self.config.node.node_call, k)
                self._audit("control", f"{self._authorized_call(headers)} reset protocol history removed={removed} peers={len(peers_touched)}")
                await self._write_response(
                    writer,
                    200,
                    self._json(
                        {
                            "ok": True,
                            "removed": removed,
                            "removed_peers": len(peers_touched),
                            "peer": peer_filter,
                            "all": clear_all,
                        }
                    ),
                )
                return

            if path == "/api/proto/summary":
                if method != "GET":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                if not self._is_authorized(headers):
                    await self._write_response(writer, 401, self._json({"error": "unauthorized"}))
                    return
                if not self.link_stats_fn:
                    await self._write_response(
                        writer,
                        200,
                        self._json(
                            {
                                "peers": 0,
                                "known": 0,
                                "ok": 0,
                                "degraded": 0,
                                "flapping": 0,
                                "stale": 0,
                                "unknown": 0,
                                "history_events": 0,
                                "history_peers": 0,
                                "latest_history_epoch": 0,
                            }
                        ),
                    )
                    return
                stats = await self.link_stats_fn()
                node_cfg = await self.store.list_user_prefs(self.config.node.node_call)
                node_cfg.update(await self.store.list_user_vars(self.config.node.node_call))
                now_epoch = int(time.time())
                known = 0
                ok = 0
                degraded = 0
                flapping = 0
                stale = 0
                unknown = 0
                history_events = 0
                history_peers = 0
                latest_history_epoch = 0
                for name in sorted(stats):
                    proto = self._proto_state_for_peer(node_cfg, name, now_epoch)
                    if not bool(proto.get("known")):
                        unknown += 1
                    else:
                        known += 1
                        h = str(proto.get("health", "unknown")).lower()
                        if h == "ok":
                            ok += 1
                        elif h == "degraded":
                            degraded += 1
                        elif h == "flapping":
                            flapping += 1
                        elif h == "stale":
                            stale += 1
                        else:
                            unknown += 1
                    hc = int(proto.get("history_count", 0) or 0)
                    history_events += hc
                    if hc > 0:
                        history_peers += 1
                    last_ev = proto.get("last_event")
                    if isinstance(last_ev, dict):
                        try:
                            ep = int(last_ev.get("epoch", 0) or 0)
                        except Exception:
                            ep = 0
                        if ep > latest_history_epoch:
                            latest_history_epoch = ep
                await self._write_response(
                    writer,
                    200,
                    self._json(
                        {
                            "peers": len(stats),
                            "known": known,
                            "ok": ok,
                            "degraded": degraded,
                            "flapping": flapping,
                            "stale": stale,
                            "unknown": unknown,
                            "history_events": history_events,
                            "history_peers": history_peers,
                            "latest_history_epoch": latest_history_epoch,
                        }
                    ),
                )
                return

            if path == "/api/policydrop":
                if method != "GET":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                if not self._is_authorized(headers):
                    await self._write_response(writer, 401, self._json({"error": "unauthorized"}))
                    return
                peer = q.get("peer", [""])[0].strip()
                rows = await self._policy_drop_rows(peer)
                await self._write_response(writer, 200, self._json(rows))
                return

            if path == "/api/policydrop/reset":
                if method != "POST":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                if not self._is_authorized(headers):
                    await self._write_response(writer, 401, self._json({"error": "unauthorized"}))
                    return
                if not self.link_clear_policy_fn:
                    await self._write_response(writer, 500, self._json({"error": "policy reset unavailable"}))
                    return
                peer = q.get("peer", [""])[0].strip() or None
                cleared = int(await self.link_clear_policy_fn(peer))
                self._audit("control", f"{self._authorized_call(headers)} reset policy drops peer={(peer or 'all')} cleared={cleared}")
                await self._write_response(writer, 200, self._json({"ok": True, "cleared_peers": cleared, "peer": peer or ""}))
                return

            if path == "/api/peer/connect":
                if method != "POST":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                if not self._is_authorized(headers):
                    await self._write_response(writer, 401, self._json({"error": "unauthorized"}))
                    return
                if not self.link_connect_fn:
                    await self._write_response(writer, 500, self._json({"error": "peer connect unavailable"}))
                    return
                payload = self._parse_json_body(body)
                peer = str(payload.get("peer", "")).strip()
                dsn = str(payload.get("dsn", "")).strip()
                profile = str(payload.get("profile", "")).strip() or "dxspider"
                if not peer or not dsn:
                    await self._write_response(writer, 400, self._json({"error": "peer and dsn are required"}))
                    return
                try:
                    await self.link_connect_fn(peer, dsn, profile)
                except Exception as exc:
                    await self._write_response(writer, 500, self._json({"error": f"connect failed: {exc}"}))
                    return
                self._audit("connect", f"{self._authorized_call(headers)} connected peer {peer} profile={profile}")
                await self._write_response(writer, 200, self._json({"ok": True, "peer": peer, "dsn": dsn, "profile": profile}))
                return

            if path == "/api/peer/save":
                if method != "POST":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                if not self._is_authorized(headers):
                    await self._write_response(writer, 401, self._json({"error": "unauthorized"}))
                    return
                if not self.link_save_peer_fn:
                    await self._write_response(writer, 500, self._json({"error": "peer save unavailable"}))
                    return
                payload = self._parse_json_body(body)
                peer = str(payload.get("peer", "")).strip()
                dsn = str(payload.get("dsn", "")).strip()
                profile = str(payload.get("profile", "")).strip() or "dxspider"
                reconnect = bool(payload.get("reconnect", True))
                if not peer or not dsn:
                    await self._write_response(writer, 400, self._json({"error": "peer and dsn are required"}))
                    return
                try:
                    await self.link_save_peer_fn(peer, dsn, profile, reconnect)
                except Exception as exc:
                    await self._write_response(writer, 500, self._json({"error": f"save failed: {exc}"}))
                    return
                self._audit("config", f"{self._authorized_call(headers)} saved peer {peer} profile={profile} reconnect={int(reconnect)}")
                await self._write_response(writer, 200, self._json({"ok": True, "peer": peer, "dsn": dsn, "profile": profile, "reconnect": reconnect}))
                return

            if path == "/api/peer/disconnect":
                if method != "POST":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                if not self._is_authorized(headers):
                    await self._write_response(writer, 401, self._json({"error": "unauthorized"}))
                    return
                if not self.link_disconnect_fn:
                    await self._write_response(writer, 500, self._json({"error": "peer disconnect unavailable"}))
                    return
                payload = self._parse_json_body(body)
                peer = str(payload.get("peer", "")).strip()
                if not peer:
                    await self._write_response(writer, 400, self._json({"error": "peer is required"}))
                    return
                ok = bool(await self.link_disconnect_fn(peer))
                self._audit("disconnect", f"{self._authorized_call(headers)} disconnected peer {peer} ok={int(ok)}")
                await self._write_response(writer, 200, self._json({"ok": ok, "peer": peer}))
                return

            if path == "/api/peer/profile":
                if method != "POST":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                if not self._is_authorized(headers):
                    await self._write_response(writer, 401, self._json({"error": "unauthorized"}))
                    return
                if not self.link_set_profile_fn:
                    await self._write_response(writer, 500, self._json({"error": "peer profile unavailable"}))
                    return
                payload = self._parse_json_body(body)
                peer = str(payload.get("peer", "")).strip()
                profile = str(payload.get("profile", "")).strip()
                if not peer or not profile:
                    await self._write_response(writer, 400, self._json({"error": "peer and profile are required"}))
                    return
                ok = bool(await self.link_set_profile_fn(peer, profile))
                self._audit("config", f"{self._authorized_call(headers)} changed peer {peer} profile={profile} ok={int(ok)}")
                await self._write_response(writer, 200, self._json({"ok": ok, "peer": peer, "profile": profile}))
                return

            if path == "/api/spot":
                if method != "POST":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                call = self._web_call_from_headers(headers)
                if not call:
                    await self._write_response(writer, 401, self._json({"error": "web login required"}))
                    return
                if not await self._access_allowed(call, "web", "spots"):
                    await self._write_response(writer, 403, self._json({"error": "spot posting not allowed via web"}))
                    return
                payload = self._parse_json_body(body)
                try:
                    freq_khz = float(payload.get("freq_khz", ""))
                except Exception:
                    await self._write_response(writer, 400, self._json({"error": "invalid freq_khz"}))
                    return
                dx_call = normalize_call(str(payload.get("dx_call", "")).strip())
                info = str(payload.get("info", "")).strip()
                if not is_valid_call(dx_call):
                    await self._write_response(writer, 400, self._json({"error": "invalid dx_call"}))
                    return
                epoch = int(datetime.now(timezone.utc).timestamp())
                throttle = await check_spot_throttle(self.store, self.config.node.node_call, call, epoch)
                if throttle.enabled and throttle.recent_count >= throttle.max_per_window:
                    await self._write_response(
                        writer,
                        429,
                        self._json(
                            {
                                "error": "spot rate limit exceeded",
                                "limit": {
                                    "max_per_window": throttle.max_per_window,
                                    "window_seconds": throttle.window_seconds,
                                },
                            }
                        ),
                    )
                    return
                raw = "^".join(
                    [
                        f"{freq_khz:.1f}",
                        dx_call,
                        str(epoch),
                        info,
                        call,
                        "226",
                        "226",
                        self.config.node.node_call,
                    ]
                )
                spot = Spot(
                    freq_khz=freq_khz,
                    dx_call=dx_call,
                    epoch=epoch,
                    info=info,
                    spotter=call,
                    source_node=self.config.node.node_call,
                    raw=raw,
                )
                inserted = await self.store.add_spot(spot)
                if inserted and self.publish_spot_fn:
                    await self.publish_spot_fn(spot)
                if inserted and self.relay_spot_fn:
                    await self.relay_spot_fn(spot)
                await self._write_response(
                    writer,
                    200,
                    self._json({"ok": True, "posted_by": call, "dx_call": dx_call, "freq_khz": freq_khz}),
                )
                return

            if path == "/api/chat":
                if method != "POST":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                call = self._web_call_from_headers(headers)
                if not call:
                    await self._write_response(writer, 401, self._json({"error": "web login required"}))
                    return
                if not await self._access_allowed(call, "web", "chat"):
                    await self._write_response(writer, 403, self._json({"error": "chat posting not allowed via web"}))
                    return
                payload = self._parse_json_body(body)
                text = str(payload.get("text", "")).strip()
                if not text:
                    await self._write_response(writer, 400, self._json({"error": "text is required"}))
                    return
                now = int(datetime.now(timezone.utc).timestamp())
                await self.store.add_bulletin("chat", call, "LOCAL", now, text)
                if self.publish_chat_fn:
                    await self.publish_chat_fn(call, text)
                if self.relay_chat_fn:
                    await self.relay_chat_fn(call, text)
                await self._write_response(writer, 200, self._json({"ok": True, "posted_by": call, "category": "chat"}))
                return

            if path in {"/api/announce", "/api/wcy", "/api/wwv", "/api/wx"}:
                if method != "POST":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                call = self._web_call_from_headers(headers)
                if not call:
                    await self._write_response(writer, 401, self._json({"error": "web login required"}))
                    return
                payload = self._parse_json_body(body)
                text = str(payload.get("text", "")).strip()
                if not text:
                    await self._write_response(writer, 400, self._json({"error": "text is required"}))
                    return
                category = path.split("/")[-1].lower()
                if not await self._access_allowed(call, "web", category):
                    await self._write_response(
                        writer,
                        403,
                        self._json({"error": f"{category} posting not allowed via web"}),
                    )
                    return
                scope = str(payload.get("scope", "LOCAL")).strip().upper() or "LOCAL"
                if category != "announce":
                    scope = "LOCAL"
                elif scope not in {"LOCAL", "FULL", "SYSOP"}:
                    scope = "LOCAL"
                now = int(datetime.now(timezone.utc).timestamp())
                await self.store.add_bulletin(category, call, scope, now, text)
                if self.publish_bulletin_fn:
                    await self.publish_bulletin_fn(category, call, scope, text)
                if self.relay_bulletin_fn:
                    await self.relay_bulletin_fn(category, call, scope, text)
                await self._write_response(
                    writer,
                    200,
                    self._json({"ok": True, "posted_by": call, "category": category, "scope": scope}),
                )
                return

            if path == "/":
                html = self._render_index_html()
                await self._write_response(writer, 200, html.encode("utf-8"), "text/html; charset=utf-8")
                return

            await self._write_response(writer, 404, self._json({"error": "not found"}))
        except Exception:
            LOG.exception("web request failed")
            try:
                await self._write_response(writer, 500, self._json({"error": "server error"}))
            except Exception:
                pass
        finally:
            writer.close()
            await writer.wait_closed()
