from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
import fnmatch
import json
import logging
import math
import re
import time
import textwrap
from pathlib import Path
from typing import Callable, Awaitable

from . import __version__
from .access_policy import ACCESS_CAPABILITIES, ACCESS_CHANNELS, default_access_allowed
from .auth import hash_password, is_password_hash, verify_password
from .auth_logging import log_auth_failure
from .config import AppConfig, node_presentation_defaults, parse_telnet_ports
from .ctydat import load_cty, lookup
from .geocode import estimate_location_from_locator, resolve_location_to_coords
from .models import Spot, display_call, is_valid_call, normalize_call
from .pathmeta import describe_session_path, normalize_recorded_path
from .peer_profiles import format_dx_line_for_profile, format_live_dx_line_for_profile, normalize_profile
from .qrz import QRZClient, QRZLookupError
from .shdx import BAND_RANGES, parse_sh_dx_args
from .spot_throttle import (
    SPOT_THROTTLE_EXEMPT_KEY,
    SPOT_THROTTLE_MAX_KEY,
    SPOT_THROTTLE_WINDOW_KEY,
    check_spot_throttle,
    load_spot_throttle_policy,
)
from .strings import StringCatalog
from .store import SpotStore
from .importer import import_spot_file
from .maidenhead import coords_to_locator, extract_locator, locator_to_coords
from .wm7d import WM7DClient, WM7DLookupError


LOG = logging.getLogger(__name__)
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


@dataclass(slots=True)
class Session:
    call: str
    writer: asyncio.StreamWriter
    connected_at: datetime
    echo: bool = True
    here: bool = True
    beep: bool = False
    language: str = "en"
    peer_profile: str = "dxspider"
    catchup: bool = False
    vars: dict[str, str] = field(default_factory=dict)
    async_line_open: bool = False


@dataclass(slots=True)
class FilterRule:
    slot: int
    expr: str


@dataclass(slots=True)
class EventLogEntry:
    epoch: int
    category: str
    text: str


class TelnetClusterServer:
    _PRIV_LEVELS: dict[str, int] = {
        "user": 0,
        "op": 1,
        "operator": 1,
        "sysop": 2,
        "admin": 2,
    }
    _SYSOP_ONLY_COMMANDS: set[str] = {
        "agwrestart",
        "create/user",
        "dbcreate",
        "dbdelkey",
        "dbexport",
        "dbimport",
        "dbremove",
        "dbshow",
        "dbupdate",
        "delete/user",
        "delete/usdb",
        "export",
        "export_users",
        "forward/latlong",
        "forward/opername",
        "init",
        "kill",
        "load/aliases",
        "load/badmsg",
        "load/badwords",
        "load/bands",
        "load/cmdcache",
        "load/db",
        "load/dxqsl",
        "load/forward",
        "load/hops",
        "load/keps",
        "load/messages",
        "load/prefixes",
        "load/swop",
        "load/usdb",
        "rinit",
        "send_config",
        "set/control",
        "set/maxconnect",
        "set/protoack",
        "set/protothreshold",
        "set/startup",
        "set/user",
        "set/privilege",
        "show/policydrop",
        "show/protoack",
        "show/protoacks",
        "show/prack",
        "sysop/password",
        "sysop/clearpassword",
        "sysop/user",
        "sysop/deleteuser",
        "sysop/privilege",
        "sysop/homenode",
        "sysop/blocklogin",
        "sysop/showuser",
        "sysop/users",
        "sysop/sysops",
        "sysop/access",
        "sysop/path",
        "sysop/setaccess",
        "sysop/audit",
        "sysop/services",
        "sysop/restart",
        "sysop/setprompt",
        "show/startup",
        "shutdown",
        "spoof",
        "stat/channel",
        "stat/db",
        "stat/msg",
        "stat/nodeconfig",
        "stat/pc19list",
        "stat/routenode",
        "stat/routeuser",
        "stat/user",
        "stat/userconfig",
        "unset/control",
        "unset/protoack",
        "unset/protothreshold",
        "unset/privilege",
    }

    def __init__(
        self,
        config: AppConfig,
        store: SpotStore,
        started_at: datetime,
        link_stats_fn: Callable[[], Awaitable[dict[str, dict[str, object]]]] | None = None,
        link_set_profile_fn: Callable[[str, str], Awaitable[bool]] | None = None,
        link_connect_fn: Callable[[str, str], Awaitable[None]] | None = None,
        link_disconnect_fn: Callable[[str], Awaitable[bool]] | None = None,
        link_clear_policy_fn: Callable[[str | None], Awaitable[int]] | None = None,
        link_desired_peers_fn: Callable[[], Awaitable[list[dict[str, object]]]] | None = None,
        component_status_fn: Callable[[], Awaitable[list[dict[str, object]]]] | None = None,
        component_restart_fn: Callable[[str], Awaitable[tuple[bool, str]]] | None = None,
        on_chat_fn: Callable[[str, str], Awaitable[None]] | None = None,
        on_bulletin_fn: Callable[[str, str, str, str], Awaitable[None]] | None = None,
        on_spot_fn: Callable[[Spot], Awaitable[None]] | None = None,
        on_message_fn: Callable[[str, str, str, int, int | None], Awaitable[None]] | None = None,
        on_sessions_changed_fn: Callable[[], Awaitable[None]] | None = None,
        on_node_login_fn: Callable[[str, str, asyncio.StreamReader, asyncio.StreamWriter, list[str] | None], Awaitable[bool]]
        | None = None,
        strings_path: str | None = None,
    ) -> None:
        self.config = config
        self.store = store
        self.started_at = started_at
        self._server: asyncio.AbstractServer | None = None
        self._servers: list[asyncio.AbstractServer] = []
        self._sessions: dict[int, Session] = {}
        self._session_seq = 0
        self._semaphore = asyncio.Semaphore(self.config.telnet.max_clients)
        self._startup_utc = datetime.now(timezone.utc)
        self._link_stats_fn = link_stats_fn
        self._link_set_profile_fn = link_set_profile_fn
        self._link_connect_fn = link_connect_fn
        self._link_disconnect_fn = link_disconnect_fn
        self._link_clear_policy_fn = link_clear_policy_fn
        self._link_desired_peers_fn = link_desired_peers_fn
        self._component_status_fn = component_status_fn
        self._component_restart_fn = component_restart_fn
        self._on_chat_fn = on_chat_fn
        self._on_bulletin_fn = on_bulletin_fn
        self._on_spot_fn = on_spot_fn
        self._on_message_fn = on_message_fn
        self._on_sessions_changed_fn = on_sessions_changed_fn
        self._on_node_login_fn = on_node_login_fn
        self._filters: dict[str, dict[str, dict[str, list[FilterRule]]]] = {}
        self._events: list[EventLogEntry] = []
        self._users: set[str] = set()
        self._strings = StringCatalog(strings_path)
        self._qrz = QRZClient(self.config.qrz)
        self._wm7d = WM7DClient()
        self._cty_loaded = False
        cty_path = self.config.public_web.cty_dat_path.strip()
        if cty_path:
            try:
                load_cty(cty_path)
                self._cty_loaded = True
            except Exception as exc:
                LOG.warning("telnet cty load failed from %s: %s", cty_path, exc)

    _LOGIN_CALL_SANITIZE_RE = re.compile(r"[^A-Z0-9/-]+")

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

    async def _read_telnet_byte(self, reader: asyncio.StreamReader, timeout: float) -> bytes | None:
        try:
            raw = await (asyncio.wait_for(reader.read(1), timeout=timeout) if timeout > 0 else reader.read(1))
        except asyncio.TimeoutError:
            return None
        if not raw:
            return None
        if raw == b"\xff":
            try:
                nxt = await (asyncio.wait_for(reader.read(1), timeout=timeout) if timeout > 0 else reader.read(1))
            except asyncio.TimeoutError:
                return None
            if not nxt:
                return None
            cmd = nxt[0]
            if cmd == 255:
                return b"\xff"
            if cmd in {251, 252, 253, 254}:
                try:
                    _ = await (asyncio.wait_for(reader.read(1), timeout=timeout) if timeout > 0 else reader.read(1))
                except asyncio.TimeoutError:
                    return None
                return b""
            if cmd == 250:
                while True:
                    try:
                        part = await (asyncio.wait_for(reader.read(1), timeout=timeout) if timeout > 0 else reader.read(1))
                    except asyncio.TimeoutError:
                        return None
                    if not part:
                        return None
                    if part == b"\xff":
                        try:
                            tail = await (asyncio.wait_for(reader.read(1), timeout=timeout) if timeout > 0 else reader.read(1))
                        except asyncio.TimeoutError:
                            return None
                        if not tail:
                            return None
                        if tail == b"\xf0":
                            break
                return b""
            return b""
        return raw

    async def active_ports(self) -> tuple[int, ...]:
        ports: list[int] = []
        for srv in self._servers:
            for sock in srv.sockets or []:
                try:
                    ports.append(int(sock.getsockname()[1]))
                except Exception:
                    continue
        return tuple(sorted(set(ports)))

    def _find_session(self, call: str) -> Session | None:
        for s in self._sessions.values():
            if s.call == call:
                return s
        return None

    def _session_vars(self, call: str) -> dict[str, str]:
        s = self._find_session(call)
        if not s:
            return {}
        return s.vars

    async def _persist_pref(self, call: str, key: str, value: str) -> None:
        now = int(datetime.now(timezone.utc).timestamp())
        await self.store.set_user_pref(call, key, value, now)

    async def _get_pref(self, call: str, key: str) -> str | None:
        return await self.store.get_user_pref(call, key)

    async def _load_prefs_for_call(self, call: str) -> dict[str, str]:
        return await self.store.list_user_prefs(call)

    async def _clear_pref_prefix(self, call: str, prefix: str) -> int:
        prefs = await self._load_prefs_for_call(call)
        keys = [k for k in prefs if k.startswith(prefix)]
        removed = 0
        for k in keys:
            removed += await self.store.delete_user_pref(call, k)
        return removed

    def _access_channels(self) -> tuple[str, ...]:
        return ACCESS_CHANNELS

    def _access_capabilities(self) -> tuple[str, ...]:
        return ACCESS_CAPABILITIES

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

    async def _require_access(self, call: str, channel: str, capability: str, action: str) -> str | None:
        if await self._access_allowed(call, channel, capability):
            return None
        return f"{action}: not allowed via {channel}\r\n"

    def _active_sessions_for_call(self, call: str) -> int:
        target = call.upper()
        return sum(1 for s in self._sessions.values() if s.call == target)

    async def _maxconnect_for_call(self, call: str) -> int:
        raw = await self._get_pref(call, "maxconnect")
        if not raw:
            return 0
        try:
            n = int(raw)
        except ValueError:
            return 0
        return max(0, min(n, 100))

    async def _privilege_level_for(self, call: str) -> int:
        row = await self.store.get_user_registry(call)
        p = ""
        if row:
            p = str(row["privilege"] or "").strip().lower()
        if not p:
            p = (await self._get_pref(call, "privilege") or "").strip().lower()
        if not p:
            s = self._find_session(call)
            if s:
                p = str(s.vars.get("privilege", "")).strip().lower()
        return self._PRIV_LEVELS.get(p, 0)

    async def _require_privilege(self, actor_call: str, level: int, action: str) -> str | None:
        actor_level = await self._privilege_level_for(actor_call)
        if actor_level >= level:
            return None
        need = "sysop" if level >= 2 else "op"
        return f"{action}: permission denied (requires {need})\r\n"

    async def _node_family_for_login(self, call: str) -> str:
        family = str(await self.store.get_user_pref(call.upper(), "node_family") or "").strip().lower()
        if family in {"pycluster", "dxspider", "dxnet", "arcluster", "clx"}:
            return family
        return ""

    async def _command_visible_for(self, call: str, command: str) -> bool:
        if await self._privilege_level_for(call) >= 2:
            return True
        if command.startswith("sysop/"):
            return False
        return command not in self._SYSOP_ONLY_COMMANDS

    async def _apply_prefs_to_session(self, session: Session) -> None:
        prefs = await self._load_prefs_for_call(session.call)
        if not prefs:
            return

        def _is_on(v: str) -> bool:
            return v.strip().lower() in {"1", "on", "yes", "true"}

        if "echo" in prefs:
            session.echo = _is_on(prefs["echo"])
        if "here" in prefs:
            session.here = _is_on(prefs["here"])
        if "beep" in prefs:
            session.beep = _is_on(prefs["beep"])
        if "language" in prefs and prefs["language"].strip():
            session.language = prefs["language"].strip().lower()
        if "profile" in prefs and prefs["profile"].strip():
            session.peer_profile = normalize_profile(prefs["profile"].strip())
        if "catchup" in prefs:
            session.catchup = _is_on(prefs["catchup"])

        for k, v in prefs.items():
            if k in {"echo", "here", "beep", "language", "profile", "catchup"}:
                continue
            session.vars[k] = v

    async def _page_size_for(self, call: str) -> int:
        s = self._find_session(call)
        raw = ""
        if s and s.vars.get("page_size"):
            raw = s.vars.get("page_size", "")
        else:
            raw = await self._get_pref(call, "page_size") or ""
            if not raw:
                raw = await self._get_pref(call, "page") or ""
        if not raw:
            return 0
        try:
            n = int(raw)
        except ValueError:
            return 0
        return max(0, min(n, 500))

    def _peer_host(self, peer) -> str:
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

    def _sanitize_login_call(self, raw_call: str | None) -> str:
        if not raw_call:
            return ""
        sanitized = self._LOGIN_CALL_SANITIZE_RE.sub("", raw_call.upper())
        return sanitized.strip()

    def _log_auth_failure(self, channel: str, peer, call: str, reason: str) -> None:
        log_auth_failure(LOG, channel, self._peer_host(peer), self._auth_log_call(call), reason)

    async def _apply_page_size(self, call: str, lines: list[str], explicit_limit: bool = False) -> list[str]:
        if explicit_limit:
            return lines
        page = await self._page_size_for(call)
        if page <= 0 or len(lines) <= page:
            return lines
        out = list(lines[:page])
        out.append(f"... ({len(lines) - page} more, use explicit limit)")
        return out

    async def _nowrap_for(self, call: str) -> bool:
        s = self._find_session(call)
        if s and "nowrap" in s.vars:
            return self._is_on_value(str(s.vars.get("nowrap", "")), default=False)
        raw = await self._get_pref(call, "nowrap") or ""
        return self._is_on_value(raw, default=False)

    def _wrap_console_line(self, line: str, width: int = 80) -> list[str]:
        if len(line) <= width:
            return [line]
        indent = len(line) - len(line.lstrip(" "))
        subsequent = " " * indent
        wrapped = textwrap.wrap(
            line,
            width=width,
            replace_whitespace=False,
            drop_whitespace=False,
            break_long_words=False,
            break_on_hyphens=False,
            subsequent_indent=subsequent,
        )
        return wrapped or [line]

    async def _format_console_lines(self, call: str, lines: list[str], width: int = 80) -> str:
        if await self._nowrap_for(call):
            return "\r\n".join(lines) + "\r\n"
        out: list[str] = []
        for line in lines:
            out.extend(self._wrap_console_line(line, width=width))
        return "\r\n".join(out) + "\r\n"

    def _ensure_filter_store(self, call: str, family: str, action: str) -> list[FilterRule]:
        c = call.upper()
        fam = family.lower()
        act = action.lower()
        self._filters.setdefault(c, {}).setdefault(fam, {}).setdefault(act, [])
        return self._filters[c][fam][act]

    async def _load_filters_for_call(self, call: str) -> None:
        c = call.upper()
        rows = await self.store.list_filter_rules(c)
        fams: dict[str, dict[str, list[FilterRule]]] = {}
        for r in rows:
            fam = str(r["family"]).lower()
            act = str(r["action"]).lower()
            fams.setdefault(fam, {}).setdefault(act, []).append(
                FilterRule(slot=int(r["slot"]), expr=str(r["expr"]))
            )
        self._filters[c] = fams

    def _log_event(self, category: str, text: str) -> None:
        self._events.append(EventLogEntry(epoch=int(datetime.now(timezone.utc).timestamp()), category=category, text=text))
        if len(self._events) > 2000:
            self._events = self._events[-2000:]

    def record_event(self, category: str, text: str) -> None:
        self._log_event(category, text)

    def audit_rows(self, limit: int = 50, categories: set[str] | None = None) -> list[dict[str, object]]:
        rows = self._events
        if categories is not None:
            rows = [e for e in rows if e.category in categories]
        rows = rows[-max(1, min(limit, 500)) :]
        rows = list(reversed(rows))
        return [{"epoch": e.epoch, "category": e.category, "text": e.text} for e in rows]

    def _format_event_rows(
        self,
        limit: int,
        categories: set[str] | None = None,
        title: str | None = None,
    ) -> str:
        rows = self._events
        if categories is not None:
            rows = [e for e in rows if e.category in categories]
        if not rows:
            return (title + "\r\n" if title else "") + self._string("events.empty", "No log events") + "\r\n"
        lines: list[str] = []
        if title:
            lines.append(title)
        for e in rows[-limit:]:
            ts = datetime.fromtimestamp(e.epoch, tz=timezone.utc).strftime("%-d-%b-%Y %H%MZ")
            lines.append(f"{ts} {e.category}: {e.text}")
        return "\r\n".join(lines) + "\r\n"
        if len(self._events) > 5000:
            del self._events[: len(self._events) - 5000]

    def _is_callsign_token(self, token: str) -> bool:
        return bool(re.match(r"^[A-Za-z0-9]+(?:-[0-9]{1,2})?$", token))

    def _is_all_token(self, token: str) -> bool:
        t = (token or "").strip().lower()
        return t in {"all", "a", "*", "--all"}

    def _parse_filter_target_slot_expr(self, call: str, arg: str | None) -> tuple[str, int, str] | None:
        if not arg:
            return None
        toks = [t for t in arg.split() if t]
        if not toks:
            return None

        target = call.upper()
        slot = 1
        idx = 0

        # sysop-ish syntax: <call> [input] [slot] <expr>
        if toks and self._is_callsign_token(toks[0]) and not toks[0].isdigit() and not self._is_all_token(toks[0]) and toks[0].lower() != "input":
            target = toks[0].upper()
            idx += 1

        if idx < len(toks) and toks[idx].lower() == "input":
            idx += 1

        if idx < len(toks) and toks[idx].isdigit():
            slot = max(0, min(int(toks[idx]), 9))
            idx += 1

        expr = " ".join(toks[idx:]).strip()
        if not expr:
            return None
        return target, slot, expr

    def _parse_filter_target_and_slot(self, call: str, arg: str | None) -> tuple[str, int | str] | None:
        if not arg:
            return call.upper(), "all"
        toks = [t for t in arg.split() if t]
        if not toks:
            return call.upper(), "all"
        target = call.upper()
        idx = 0
        if self._is_callsign_token(toks[0]) and not self._is_all_token(toks[0]) and toks[0].lower() != "input" and not toks[0].isdigit():
            target = toks[0].upper()
            idx += 1
        if idx < len(toks) and toks[idx].lower() == "input":
            idx += 1
        if idx >= len(toks):
            return target, "all"
        tok = toks[idx].lower()
        if self._is_all_token(tok):
            return target, "all"
        if tok.isdigit():
            return target, max(0, min(int(tok), 9))
        return target, "all"

    def _uptime_text(self) -> str:
        uptime = datetime.now(timezone.utc) - self.started_at
        d = uptime.days
        h, rem = divmod(uptime.seconds, 3600)
        m, _ = divmod(rem, 60)
        return f"{d}d {h:02d}:{m:02d}"

    def _fmt_epoch_short(self, epoch: int) -> str:
        if epoch <= 0:
            return "-"
        return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%-d-%b %H%MZ")

    @property
    def session_count(self) -> int:
        return len(self._sessions)

    def active_calls(self) -> list[str]:
        return sorted({s.call.upper() for s in self._sessions.values()})

    def _configured_ports(self) -> tuple[int, ...]:
        return parse_telnet_ports(self.config.telnet.ports or (), fallback=self.config.telnet.port)

    async def _effective_ports(self) -> tuple[int, ...]:
        return self._configured_ports()

    async def _bind_ports(self, ports: tuple[int, ...]) -> None:
        servers: list[asyncio.AbstractServer] = []
        try:
            for port in ports:
                srv = await asyncio.start_server(
                    self._handle_client,
                    host=self.config.telnet.host,
                    port=port,
                    limit=self.config.telnet.max_line_length,
                )
                servers.append(srv)
        except Exception:
            for srv in servers:
                srv.close()
                await srv.wait_closed()
            raise
        self._servers = servers
        self._server = servers[0] if servers else None
        addrs = ", ".join(str(s.getsockname()) for srv in servers for s in (srv.sockets or []))
        LOG.info("Telnet server listening on %s", addrs)

    async def rebind_listeners(self, ports: tuple[int, ...] | None = None) -> tuple[int, ...]:
        next_ports = ports or await self._effective_ports()
        await self.stop()
        self.config.telnet.ports = tuple(next_ports)
        if self.config.telnet.ports:
            self.config.telnet.port = self.config.telnet.ports[0]
        await self._bind_ports(tuple(next_ports))
        return tuple(next_ports)

    async def start(self) -> None:
        await self._bind_ports(await self._effective_ports())

    async def stop(self) -> None:
        for srv in self._servers:
            srv.close()
        for srv in self._servers:
            try:
                await asyncio.wait_for(srv.wait_closed(), timeout=1.0)
            except (asyncio.TimeoutError, ConnectionError, OSError):
                pass
        self._servers = []
        self._server = None
        session_ids = list(self._sessions.keys())
        if session_ids:
            await self._close_sessions(session_ids, "Server shutting down")

    async def _close_sessions(self, ids: list[int], reason: str = "") -> int:
        closed = 0
        for sid in ids:
            sess = self._sessions.get(sid)
            if not sess:
                continue
            try:
                if reason:
                    await self._write(sess.writer, f"\r\n{reason}\r\n")
            except Exception:
                pass
            try:
                sess.writer.close()
                try:
                    await asyncio.wait_for(sess.writer.wait_closed(), timeout=1.0)
                except (asyncio.TimeoutError, ConnectionError, OSError):
                    pass
            except Exception:
                pass
            self._sessions.pop(sid, None)
            closed += 1
        return closed

    async def _write(self, writer: asyncio.StreamWriter, text: str) -> None:
        writer.write(text.encode("utf-8", errors="replace"))
        await writer.drain()

    def _is_on_value(self, value: str | None, default: bool = False) -> bool:
        if value is None:
            return default
        return value.strip().lower() in {"1", "on", "yes", "true"}

    def _spot_matches_expr(self, freq_khz: float, dx_call: str, spotter: str, info: str, expr: str) -> bool:
        text = (expr or "").strip()
        if not text:
            return False
        low = text.lower()
        toks = low.split()
        if not toks:
            return False
        first = toks[0]
        rest = " ".join(toks[1:]).strip()

        if first == "on" and rest:
            band = rest.split()[0]
            rng = BAND_RANGES.get(band)
            if rng:
                return rng[0] <= freq_khz <= rng[1]

        if first == "by" and rest:
            pat = rest.upper()
            s = spotter.upper()
            if "*" in pat or "?" in pat:
                return fnmatch.fnmatchcase(s, pat)
            return s.startswith(pat)

        if first in {"dx", "call"} and rest:
            pat = rest.upper()
            d = dx_call.upper()
            if "*" in pat or "?" in pat:
                return fnmatch.fnmatchcase(d, pat)
            return d.startswith(pat)

        if first == "call_zone" and rest:
            ent = lookup(dx_call) if self._cty_loaded else None
            if not ent:
                return False
            wanted = {
                int(tok)
                for tok in re.split(r"[,\s]+", rest)
                if tok.strip().isdigit()
            }
            return bool(wanted) and ent.cq_zone in wanted

        if first == "call_itu" and rest:
            ent = lookup(dx_call) if self._cty_loaded else None
            if not ent:
                return False
            wanted = {
                int(tok)
                for tok in re.split(r"[,\s]+", rest)
                if tok.strip().isdigit()
            }
            return bool(wanted) and ent.itu_zone in wanted

        if first == "call_dxcc" and rest:
            ent = lookup(dx_call) if self._cty_loaded else None
            if not ent:
                return False
            wanted = [tok.strip().upper() for tok in re.split(r"[,\s]+", rest) if tok.strip()]
            if not wanted:
                return False
            ent_name = re.sub(r"[^A-Z0-9]+", "", ent.name.upper())
            ent_prefix = (ent.prefix or "").strip().upper()
            return any(
                tok == ent_prefix
                or tok == ent_name
                or re.sub(r"[^A-Z0-9]+", "", tok) == ent_name
                for tok in wanted
            )

        if first == "info" and rest:
            return rest in (info or "").lower()

        if first == "rbn":
            return self._is_rbn_spot(dx_call, spotter, info)

        hay = f"{freq_khz:.1f} {dx_call} {spotter} {info}".lower()
        return low in hay

    def _is_rbn_spot(self, dx_call: str, spotter: str, info: str) -> bool:
        text = f"{dx_call} {spotter} {info}".upper()
        if "RBN" in text or "SKIMMER" in text:
            return True
        if re.search(r"\b\d{1,3}\s*WPM\b", text):
            return True
        if re.search(r"\b(?:CQ|TEST)\b", text) and re.search(r"\b\d{1,3}\s*DB\b", text):
            return True
        return False

    async def _spot_passes_filters(self, call: str, freq_khz: float, dx_call: str, spotter: str, info: str) -> bool:
        target = call.upper()
        prefs = await self._load_prefs_for_call(target)
        if not self._is_on_value(prefs.get("rbn"), default=True) and self._is_rbn_spot(dx_call, spotter, info):
            return False
        if target not in self._filters:
            await self._load_filters_for_call(target)
        fam = self._filters.get(target, {}).get("spots", {})
        return self._eval_filter_family(
            fam,
            lambda expr: self._spot_matches_expr(freq_khz, dx_call, spotter, info, expr),
        )

    async def _dx_line_suffix_for_call(self, call: str, dx_call: str) -> str:
        if not self._cty_loaded:
            return ""
        prefs = await self._load_prefs_for_call(call.upper())
        ent = lookup(dx_call)
        if not ent:
            return ""
        parts: list[str] = []
        if self._is_on_value(prefs.get("dxcq"), default=False):
            parts.append(f"CQ{ent.cq_zone}")
        if self._is_on_value(prefs.get("dxitu"), default=False):
            parts.append(f"ITU{ent.itu_zone}")
        return (" " + " ".join(parts)) if parts else ""

    def _text_matches_expr(self, sender: str, text: str, expr: str) -> bool:
        e = (expr or "").strip()
        if not e:
            return False
        low = e.lower()
        if low.startswith("by "):
            pat = low[3:].strip().upper()
            s = sender.upper()
            if not pat:
                return False
            if "*" in pat or "?" in pat:
                return fnmatch.fnmatchcase(s, pat)
            return s.startswith(pat)
        return low in text.lower()

    def _route_peer_matches_expr(self, peer: str, expr: str) -> bool:
        e = (expr or "").strip().lower()
        if not e:
            return False
        p = peer.lower()
        if e.startswith("by "):
            pat = e[3:].strip()
            if not pat:
                return False
            if "*" in pat or "?" in pat:
                return fnmatch.fnmatchcase(p, pat)
            return p.startswith(pat)
        if e.startswith("peer "):
            pat = e[5:].strip()
            if not pat:
                return False
            if "*" in pat or "?" in pat:
                return fnmatch.fnmatchcase(p, pat)
            return p.startswith(pat)
        if "*" in e or "?" in e:
            return fnmatch.fnmatchcase(p, e)
        return e in p

    async def _text_family_passes_filters(self, call: str, family: str, sender: str, text: str) -> bool:
        target = call.upper()
        fam_name = family.lower()
        if target not in self._filters:
            await self._load_filters_for_call(target)
        fam = self._filters.get(target, {}).get(fam_name, {})
        return self._eval_filter_family(
            fam,
            lambda expr: self._text_matches_expr(sender, text, expr),
        )

    def _eval_filter_family(
        self,
        fam: dict[str, list[FilterRule]],
        matcher: Callable[[str], bool],
    ) -> bool:
        decision, _detail = self._eval_filter_family_detail(fam, matcher)
        return decision

    def _eval_filter_family_detail(
        self,
        fam: dict[str, list[FilterRule]],
        matcher: Callable[[str], bool],
    ) -> tuple[bool, str]:
        accept_rules = fam.get("accept", [])
        reject_rules = fam.get("reject", [])
        matches: list[tuple[int, str, str]] = []
        for r in accept_rules:
            if matcher(r.expr):
                matches.append((r.slot, "accept", r.expr))
        for r in reject_rules:
            if matcher(r.expr):
                matches.append((r.slot, "reject", r.expr))
        if matches:
            matches.sort(key=lambda x: (x[0], 0 if x[1] == "reject" else 1))
            slot, action, expr = matches[0]
            return action == "accept", f"matched={action} slot={slot} expr={expr}"
        if accept_rules:
            return False, "matched=none accept_rules=present default=deny"
        return True, "matched=none accept_rules=absent default=allow"

    async def publish_spot(self, spot: Spot) -> int:
        spot_when = datetime.fromtimestamp(spot.epoch, tz=timezone.utc)
        now_utc = datetime.now(timezone.utc)
        if spot_when.date() == now_utc.date():
            when = spot_when.strftime("%H%MZ")
        else:
            when = spot_when.strftime("%-d-%b-%Y %H%MZ")
        delivered = 0
        for s in self._sessions.values():
            if not self._is_on_value(s.vars.get("dx"), default=True):
                continue
            if not await self._spot_passes_filters(s.call, spot.freq_khz, spot.dx_call, spot.spotter, spot.info):
                continue
            line = format_live_dx_line_for_profile(
                profile=s.peer_profile,
                freq_khz=spot.freq_khz,
                dx_call=spot.dx_call,
                when=when,
                info=spot.info,
                spotter=display_call(spot.spotter),
                suffix=await self._dx_line_suffix_for_call(s.call, spot.dx_call),
            )
            prefix = "\r\n" if not s.async_line_open else ""
            await self._write(s.writer, f"{prefix}{line}\r\n")
            s.async_line_open = True
            delivered += 1
        return delivered

    async def publish_chat(self, sender: str, text: str) -> int:
        delivered = 0
        for s in self._sessions.values():
            if s.call == sender:
                continue
            prefix = "\r\n" if not s.async_line_open else ""
            await self._write(s.writer, f"{prefix}CHAT {sender}: {text}\r\n")
            s.async_line_open = True
            delivered += 1
        return delivered

    async def publish_talk(self, recipient: str, sender: str, text: str) -> int:
        t = self._find_session(recipient)
        if not t:
            return 0
        prefix = "\r\n" if not t.async_line_open else ""
        await self._write(t.writer, f"{prefix}TALK {sender}: {text}\r\n")
        t.async_line_open = True
        return 1

    async def publish_bulletin(self, category: str, sender: str, scope: str, text: str) -> int:
        delivered = 0
        tag = category.upper()
        scope_tag = scope.upper()
        prefix = f"{tag}/{scope_tag}" if scope_tag and scope_tag != "LOCAL" else tag
        for s in self._sessions.values():
            if s.call == sender:
                continue
            if category.lower() in {"announce", "wcy", "wwv"}:
                if not await self._text_family_passes_filters(s.call, category.lower(), sender, text):
                    continue
            lead = "\r\n" if not s.async_line_open else ""
            await self._write(s.writer, f"{lead}{prefix} {sender}: {text}\r\n")
            s.async_line_open = True
            delivered += 1
        return delivered

    async def publish_message(self, recipient: str, sender: str, text: str, msg_id: int, parent_id: int | None = None) -> int:
        t = self._find_session(recipient)
        if not t:
            return 0
        prefix = "\r\n" if not t.async_line_open else ""
        trailer = f" (reply {parent_id})" if parent_id is not None else ""
        await self._write(t.writer, f"{prefix}MSG#{msg_id} {sender}{trailer}: {text}\r\n")
        t.async_line_open = True
        return 1

    async def _readline(self, reader: asyncio.StreamReader) -> str | None:
        timeout = float(self.config.telnet.idle_timeout_seconds or 0)
        raw = bytearray()
        while True:
            b = await self._read_telnet_byte(reader, timeout)
            if b is None:
                return None if not raw else raw.decode("utf-8", errors="replace").strip()
            if b == b"":
                continue
            ch = b[0]
            if ch in (10, 13):
                if ch == 13:
                    try:
                        nxt = await (asyncio.wait_for(reader.read(1), timeout=0.05) if timeout > 0 else reader.read(1))
                    except asyncio.TimeoutError:
                        nxt = b""
                    if nxt not in {b"", b"\n"}:
                        reader.feed_data(self._strip_telnet_bytes(nxt))
                return raw.decode("utf-8", errors="replace").strip()
            raw.extend(b)
            if len(raw) > self.config.telnet.max_line_length:
                return ""

    async def _read_password(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> str | None:
        chars: list[str] = []
        timeout = float(self.config.telnet.idle_timeout_seconds or 0)
        while True:
            try:
                raw = await (asyncio.wait_for(reader.read(1), timeout=timeout) if timeout > 0 else reader.read(1))
            except asyncio.TimeoutError:
                return None
            if not raw:
                return None
            b = raw[0]
            if b in (10, 13):
                await self._write(writer, "\r\n")
                if b == 13:
                    try:
                        nxt = await asyncio.wait_for(reader.read(1), timeout=0.05)
                    except asyncio.TimeoutError:
                        nxt = b""
                    if nxt not in {b"", b"\n"}:
                        reader.feed_data(nxt)
                return "".join(chars).strip()
            if b in (8, 127):
                if chars:
                    chars.pop()
                    await self._write(writer, "\b \b")
                continue
            if len(chars) >= self.config.telnet.max_line_length:
                continue
            chars.append(bytes((b,)).decode("utf-8", errors="ignore"))
            await self._write(writer, "*")

    async def _prompt_new_password(self, call: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> bool:
        await self._write(writer, "A password is required before continuing.\r\n")
        await self._write(writer, "new password: ")
        first = await self._read_password(reader, writer)
        if first is None:
            return False
        first = first.strip()
        if not first:
            await self._write(writer, "Password setup failed.\r\n")
            return False
        await self._write(writer, "confirm password: ")
        second = await self._read_password(reader, writer)
        if second is None:
            return False
        if first != second.strip():
            await self._write(writer, "Passwords did not match.\r\n")
            return False
        now = int(datetime.now(timezone.utc).timestamp())
        await self.store.set_user_pref(call, "password", hash_password(first), now)
        self._log_event("user", f"{call} initial_password_set")
        await self._write(writer, f"Password set for {call}.\r\n")
        return True

    async def _prompt(self, call: str) -> str:
        node = await self._node_text("node_call")
        callsign = str(call or "").strip().upper()
        suffix = "# " if await self._privilege_level_for(call) >= 2 else "> "
        template = await self._prompt_template()
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        return self._render_prompt(template, node, callsign, suffix, timestamp)

    async def _prompt_template(self) -> str:
        template = str(getattr(self.config.node, "prompt_template", "") or "").strip()
        if template:
            return template
        return "[{timestamp}] {node}{suffix}"

    def _string(self, key: str, default: str) -> str:
        return self._strings.get(key, default)

    def _render_string(self, catalog_key: str, default: str, **values: object) -> str:
        return self._strings.render(catalog_key, default, **values)

    @staticmethod
    def _command_summary_key(cmd: str) -> str:
        return cmd.lower().replace("/", "_").replace("-", "_")

    def _render_prompt(self, template: str, node: str, callsign: str, suffix: str, timestamp: str) -> str:
        tokens = {
            "node": node,
            "callsign": callsign,
            "suffix": suffix,
            "timestamp": timestamp,
        }

        class _PromptTokens(dict[str, str]):
            def __missing__(_self, key: str) -> str:
                return ""

        return template.format_map(_PromptTokens(tokens))

    def _display_label(self, key: str) -> str:
        labels = {
            "agwengine": "AGW Engine",
            "agwmonitor": "AGW Monitor",
            "anntalk": "AnnTalk",
            "bbs": "BBS",
            "dxcq": "DX CQ",
            "dxgrid": "DX Grid",
            "dxitu": "DX ITU",
            "dup_ann": "Duplicate Ann",
            "dup_eph": "Duplicate Eph",
            "dup_spots": "Duplicate Spots",
            "dup_wcy": "Duplicate WCY",
            "dup_wwv": "Duplicate WWV",
            "gtk": "GTK",
            "homebbs": "Home BBS",
            "homenode": "Home Node",
            "local_node": "Local Node",
            "logininfo": "Logininfo",
            "maxconnect": "Max Connect",
            "pinginterval": "Ping Interval",
            "prack": "Protocol Ack",
            "protoack": "Protocol Ack",
            "qra": "QRA",
            "qth": "QTH",
            "rbn": "RBN",
            "routepc19": "Route PC19",
            "send_dbg": "Send Debug",
            "senddbg": "Send Debug",
            "sendpc16": "Send PC16",
            "sys_location": "System Location",
            "sys_qra": "System QRA",
            "usstate": "US State",
            "ve7cc": "VE7CC",
            "wantpc16": "Want PC16",
            "wantpc9x": "Want PC9X",
            "wcy": "WCY",
            "wwv": "WWV",
        }
        return labels.get(key, key.replace("_", " ").title())

    async def _node_presentation(self) -> dict[str, str]:
        data = node_presentation_defaults(self.config.node)
        prefs = await self.store.list_user_prefs(self.config.node.node_call)
        for key in {"node_call", "prompt_template", "telnet_ports", "require_password"}:
            prefs.pop(key, None)
        data.update(prefs)
        return data

    async def _node_text(self, key: str) -> str:
        data = await self._node_presentation()
        return str(data.get(key, "")).strip()

    async def _node_flag(self, key: str) -> bool:
        data = await self._node_presentation()
        return self._is_on_value(str(data.get(key, "") or "off"))

    async def _motd_text(self) -> str:
        motd = await self._node_text("motd")
        return motd or self.config.node.motd

    async def _motd_block(self) -> str:
        divider = self._string("welcome.motd.divider", "================================================================================")
        return (
            f"{divider}\r\n"
            f"{await self._motd_text()}\r\n"
            f"{divider}\r\n"
        )

    async def _welcome_block(self, call: str) -> str:
        ui = await self._node_presentation()
        branding = str(ui.get("branding_name", "")).strip() or "pyCluster"
        title = str(ui.get("welcome_title", "")).strip()
        body = str(ui.get("welcome_body", "")).strip()
        tip = str(ui.get("login_tip", "")).strip()
        website = str(ui.get("website_url", "")).strip()
        support = str(ui.get("support_contact", "")).strip()
        lines = []
        if title:
            lines.append(self._render_string("welcome.greeting.named", "{title}, {call}.", title=title, call=call))
        else:
            lines.append(self._render_string("welcome.greeting.default", "Welcome, {call}.", call=call))
        node_call = await self._node_text("node_call")
        qth = await self._node_text("qth")
        lines.append(
            self._render_string(
                "welcome.connected",
                "You're connected to {node_call}, a {branding} node in {qth}.",
                node_call=node_call,
                branding=branding,
                qth=qth,
            )
        )
        if body:
            lines.append("")
            lines.extend(body.splitlines())
        if website or support:
            lines.append("")
            if website:
                lines.append(self._render_string("welcome.website", "Web: {website}", website=website))
            if support:
                lines.append(self._render_string("welcome.contact", "Contact: {support}", support=support))
        lines.append("")
        lines.append((await self._motd_block()).rstrip("\r\n"))
        if self._is_on_value(str(ui.get("show_status_after_login", "on") or "on"), default=True):
            lines.append(
                self._render_string(
                    "welcome.status",
                    "Cluster status: {linked_nodes} nodes linked, {local_users} local users, uptime {uptime}",
                    linked_nodes=1,
                    local_users=self.session_count,
                    uptime=self._uptime_text(),
                )
            )
        if tip:
            lines.append(tip)
        lines.append("")
        return "\r\n".join(lines)

    async def _cmd_show_version(self) -> str:
        lines = [f"pyCluster version {__version__}"]
        if __version__ != "1.0.0":
            lines.append("Compatible with pyCluster version 1.0.0 command set")
        lines.append("Author: John D. Lewis (AI3I)")
        lines.append("Project: https://github.com/AI3I/pyCluster")
        return "\r\n".join(lines) + "\r\n"

    async def _cmd_show_dx(self, _call: str, arg: str | None) -> str:
        query = parse_sh_dx_args(arg)
        rows = await self.store.search_spots(query)
        if not rows:
            return self._string("show.dx.empty", "No spots available") + "\r\n"

        lines: list[str] = []
        for row in rows:
            if not await self._spot_passes_filters(
                _call,
                float(row["freq_khz"]),
                str(row["dx_call"]),
                str(row["spotter"]),
                str(row["info"] or ""),
            ):
                continue
            spot_when = datetime.fromtimestamp(row["epoch"], tz=timezone.utc)
            when = spot_when.strftime("%-d-%b-%Y %H%MZ")
            sess = self._find_session(_call)
            profile = sess.peer_profile if sess else "dxspider"
            line = format_dx_line_for_profile(
                profile=profile,
                freq_khz=float(row["freq_khz"]),
                dx_call=str(row["dx_call"]),
                when=when,
                info=str(row["info"] or ""),
                spotter=display_call(str(row["spotter"])),
            )
            line += await self._dx_line_suffix_for_call(_call, str(row["dx_call"]))
            lines.append(line)
        if not lines:
            return self._string("show.dx.empty", "No spots available") + "\r\n"
        return "\r\n".join(lines) + "\r\n"

    async def _cmd_show_mydx(self, call: str, arg: str | None) -> str:
        # In this compatibility model, show/dx already applies user filters.
        return await self._cmd_show_dx(call, arg)

    async def _cmd_show_dxcc(self, call: str, arg: str | None) -> str:
        pfx = (arg or "").strip().upper()
        if not pfx:
            return self._string("show.dxcc.usage", "Usage: show/dxcc <prefix>") + "\r\n"
        if self._cty_loaded:
            ent = lookup(pfx)
            if ent:
                return (
                    f"DXCC {pfx}: {ent.name}\r\n"
                    f"prefix={ent.prefix} continent={ent.continent} cq={ent.cq_zone} itu={ent.itu_zone}\r\n"
                    f"lat={ent.lat:.2f} lon={ent.lon:.2f}\r\n"
                )
        return await self._cmd_show_dx(call, pfx)

    async def _cmd_help(self, call: str) -> str:
        lines = [
            "Everyday commands:",
            "  help, bye, ping, connect, links, read, reply, msg, send, talk, announce",
            "Show:",
            "  show/version, show/dx, show/node, show/cluster, show/users",
            "  show/connect, show/filter, show/log, show/links, show/wcy, show/wwv,",
            "  show/wx, show/shortcuts, show/commands",
            "Set / Unset:",
            "  set/echo, set/here, set/beep, set/language, set/nowrap",
            "  unset/echo, unset/here, unset/beep, unset/nowrap",
            "Examples:",
            "  sh/dx          recent DX spots",
            "  apropos route  find route-related commands",
            "  show/links     direct peer link status",
            "  set/nowrap     disable 80-column wrapping",
            "Useful shortcuts:",
            "  SH/PRACK  -> show/protoack",
            "  SE/PRACK  -> set/protoack",
            "  UN/PRACK  -> unset/protoack",
            "  CL/PRHIST -> clear/protohistory",
        ]
        if await self._privilege_level_for(call) >= 2:
            lines.extend(
                [
                    "System Operator:",
                    "  sysop/users, sysop/sysops, sysop/showuser <call>",
                    "  sysop/access <call>, sysop/path <call|peer>, sysop/setaccess ...",
                    "  sysop/password <call> <newpass>",
                    "  sysop/services, sysop/restart <telnet|sysopweb|all>",
                    "  sysop/audit [category] [limit]",
                ]
            )
        return await self._format_console_lines(call, lines)

    async def _cmd_show_commands(self, call: str, arg: str | None) -> str:
        reg = self._build_registry()
        needle = (arg or "").strip().lower()
        top_cmds = sorted(set(self._top_level_canonical_tokens()))
        grouped = self._canonical_grouped_keys(reg)
        visible_top = [c for c in top_cmds if await self._command_visible_for(call, c)]
        visible_grouped = [c for c in sorted(grouped) if await self._command_visible_for(call, c)]

        lines: list[str] = []
        rows: list[tuple[str, str]] = []
        page = await self._page_size_for(call)

        group = self._resolve_group_token(needle) if needle else None
        if not needle:
            section_order = [
                ("Show commands", ["show/"]),
                ("Set commands", ["set/"]),
                ("Unset commands", ["unset/"]),
                (
                    "Operator commands",
                    [
                        "help",
                        "commands",
                        "apropos",
                        "show",
                        "set",
                        "unset",
                        "dx",
                        "announce",
                        "wx",
                        "wwv",
                        "wcy",
                        "chat",
                        "talk",
                        "msg",
                        "mail",
                        "read",
                        "reply",
                        "who",
                        "users",
                        "status",
                        "cluster",
                        "node",
                        "links",
                        "connect",
                        "disconnect",
                        "ping",
                        "motd",
                        "version",
                        "time",
                        "date",
                        "uptime",
                        "bye",
                        "quit",
                        "exit",
                    ],
                ),
                (
                    "Compatibility toggles",
                    [
                        "set/agwengine",
                        "unset/agwengine",
                        "show/agwengine",
                        "set/agwmonitor",
                        "unset/agwmonitor",
                        "show/agwmonitor",
                        "set/believe",
                        "unset/believe",
                        "show/believe",
                        "set/sendpc16",
                        "unset/sendpc16",
                        "show/sendpc16",
                        "set/wantpc16",
                        "unset/wantpc16",
                        "show/wantpc16",
                        "set/wantpc9x",
                        "unset/wantpc9x",
                        "show/wantpc9x",
                    ],
                ),
                ("Filter and policy commands", ["accept/", "reject/", "clear/"]),
                ("Data and maintenance commands", ["load/", "stat/", "create/", "delete/", "forward/", "get/"]),
                ("Sysop commands", ["sysop/"]),
            ]
            row_map = {cmd: self._command_summary(cmd) for cmd in visible_top + visible_grouped}
            lines.append(self._string("commands.show.title", "Available commands:"))
            lines.append(self._string("commands.show.usage", "  Use: show/commands <family>   or   show/commands <keyword>"))
            lines.append(self._string("commands.show.examples", "  Examples: show/commands show, show/commands set, show/commands dx, show/commands route"))
            emitted: set[str] = set()
            shown = 0
            max_rows = page if page > 0 else 10_000
            for heading, selectors in section_order:
                section_rows: list[tuple[str, str]] = []
                for selector in selectors:
                    if selector.endswith("/"):
                        matches = [cmd for cmd in visible_grouped if cmd.startswith(selector) and cmd not in emitted]
                    else:
                        matches = [selector] if selector in row_map and selector not in emitted else []
                    for cmd in matches:
                        if shown >= max_rows:
                            break
                        section_rows.append((cmd, row_map[cmd]))
                        emitted.add(cmd)
                        shown += 1
                    if shown >= max_rows:
                        break
                if section_rows:
                    width = min(24, max(len(cmd) for cmd, _desc in section_rows))
                    section_key = heading.split()[0].lower()
                    lines.append(self._string(f"commands.show.sections.{section_key}", f"{heading}:"))
                    for cmd, desc in section_rows:
                        lines.append(f"  {cmd.ljust(width)}  {desc}")
                if shown >= max_rows:
                    break
            remaining = [cmd for cmd in (visible_top + visible_grouped) if cmd not in emitted]
            if remaining and shown < max_rows:
                width = min(24, max(len(cmd) for cmd in remaining))
                lines.append(self._string("commands.show.other", "Other commands:"))
                for cmd in remaining:
                    if shown >= max_rows:
                        break
                    lines.append(f"  {cmd.ljust(width)}  {row_map[cmd]}")
                    emitted.add(cmd)
                    shown += 1
            more = max(0, len(row_map) - len(emitted))
            if more > 0:
                lines.append(self._render_string("commands.show.more", "  ... {count} more commands. Increase page length with set/page or filter with show/commands <family>.", count=more))
            return await self._format_console_lines(call, lines)
        if group:
            rows = [(cmd, self._command_summary(cmd)) for cmd in visible_grouped if cmd.startswith(group + "/")]
            title = f"{group.upper()} commands ({len(rows)}):"
        else:
            all_cmds = visible_top + visible_grouped
            if needle:
                nneedle = self._normalize_cmd_token(needle)
                rows = [
                    (cmd, self._command_summary(cmd))
                    for cmd in all_cmds
                    if needle in cmd or (nneedle and self._normalize_cmd_token(cmd).find(nneedle) >= 0)
                ]
            else:
                rows = [(cmd, self._command_summary(cmd)) for cmd in visible_top]
            title = f"Commands matching {needle} ({len(rows)}):"

        if not rows:
            return f"commands: no matches for {needle}\r\n" if needle else "commands: (none)\r\n"
        more = 0
        if page > 0 and len(rows) > page:
            more = len(rows) - page
            rows = rows[:page]
        width = min(24, max(len(cmd) for cmd, _desc in rows))
        lines.append(title)
        if not needle:
            lines.append("  Use: show/commands <family>   or   show/commands <keyword>")
            lines.append("  Examples: show/commands show, show/commands set, show/commands dx, show/commands route")
        elif group:
            lines.append(f"  Use: {group}/<name> or show/commands <keyword>")
        for cmd, desc in rows:
            lines.append(f"  {cmd.ljust(width)}  {desc}")
        if more > 0:
            lines.append(f"  ... {more} more commands. Increase page length with set/page or filter with show/commands <family>.")
        return await self._format_console_lines(call, lines)

    def _command_summary(self, cmd: str) -> str:
        key = f"commands.summary.{self._command_summary_key(cmd)}"
        top_map = {
            "help": "Show the operator help overview.",
            "commands": "List commands or a command family.",
            "apropos": "Search commands by keyword.",
            "agwrestart": "Request an AGW engine restart.",
            "announce": "Post an announcement bulletin.",
            "ap": "Show announcement preference status.",
            "blank": "Emit a blank line.",
            "cluster": "Show local cluster identity information.",
            "connect": "Connect to a linked peer or DSN.",
            "date": "Show the current UTC date.",
            "dbcreate": "Create or verify local database structures.",
            "dbdelkey": "Delete a key from the local database.",
            "dbexport": "Export the local database to a file.",
            "dbimport": "Import a saved database snapshot.",
            "dbremove": "Remove records from the local database.",
            "dbupdate": "Refresh local database content.",
            "debug": "Show or change debug mode.",
            "demonstrate": "Run a safe command demonstration.",
            "disconnect": "Disconnect from a linked peer.",
            "do": "Run a startup-safe command alias.",
            "dx": "Post a DX spot or show recent DX spots.",
            "dxqsl_export": "Export DXQSL data for the current user.",
            "dxqsl_import": "Import DXQSL data for the current user.",
            "echo": "Echo text back to the session.",
            "export": "Export local data to a file.",
            "export_users": "Export registered users to CSV.",
            "status": "Show current operator session status.",
            "uptime": "Show node uptime.",
            "who": "List connected users.",
            "chat": "Send a chat-style message.",
            "join": "Join a chat or bulletin group.",
            "leave": "Leave a chat or bulletin group.",
            "directory": "Show external cluster directory sources.",
            "dbavail": "Show local database availability.",
            "dbshow": "Inspect local database records.",
            "catchup": "Enable recent DX catch-up on login.",
            "init": "Reload preferences and filters into active sessions.",
            "kill": "Disconnect one or more active sessions.",
            "links": "Show connected peer links.",
            "mail": "List unread and recent personal messages.",
            "merge": "Run a startup-safe merge alias.",
            "motd": "Show the message of the day.",
            "msg": "Send a personal message to a user.",
            "node": "Show node or home-node information for a callsign.",
            "pc": "Show or set PC frame capability preferences.",
            "ping": "Send a simple ping response.",
            "privilege": "Show or refresh your privilege level.",
            "rcmd": "Show or set the remote command string.",
            "read": "Read message headers or a specific message.",
            "reply": "Reply to a personal message.",
            "rinit": "Reload preferences and restart telnet listeners.",
            "run": "Run a startup-safe command alias.",
            "save": "Persist the current session state.",
            "send": "Send a raw or compatibility message command.",
            "send_config": "Show or write a configuration snapshot.",
            "outbox": "Show recent sent and queued personal messages.",
            "shu": "Show the shutdown status summary.",
            "shutdown": "Shut down listeners and disconnect sessions.",
            "spoof": "Inject a test message or DX spot as sysop.",
            "talk": "Send an immediate talk message to a user.",
            "time": "Show the current UTC time.",
            "type": "Run a startup-safe type alias.",
            "users": "Show the registered user list.",
            "version": "Show program version information.",
            "uncatchup": "Disable recent DX catch-up on login.",
            "wcy": "Post a WCY propagation bulletin.",
            "wwv": "Post a WWV propagation bulletin.",
            "wx": "Post a weather bulletin.",
            "bye": "Disconnect from the cluster.",
            "quit": "Disconnect from the cluster.",
            "exit": "Disconnect from the cluster.",
        }
        if "/" not in cmd:
            default = top_map.get(cmd, "Top-level command.")
            return self._string(key, default)
        group, sub = cmd.split("/", 1)
        special = {
            "425": "Show local 425-style DX summary output.",
            "address": "Show or update the station mailing address.",
            "agwengine": "Show or control AGW engine integration.",
            "agwmonitor": "Show or control AGW monitor integration.",
            "announce": "Show announcement delivery status.",
            "anntalk": "Show or control announce-to-talk bridging.",
            "apropos": "Search the command catalog by keyword.",
            "bands": "Show the supported band plan summary.",
            "bbs": "Show or control BBS compatibility mode.",
            "beep": "Show or control audible alert mode.",
            "believe": "Show or control trust mode for incoming data.",
            "buddy": "Show or manage the buddy list.",
            "capabilities": "Show command-family capability counts.",
            "chat": "Show recent chat bulletins.",
            "cluster": "Show local cluster identity and status.",
            "cmdcache": "Show command-cache counts and shortcut totals.",
            "configuration": "Show node, telnet, web, and database settings.",
            "connect": "Show current peer connection status.",
            "contest": "Show recent contest-related bulletins.",
            "control": "Show or manage control-command availability.",
            "date": "Show the current UTC date.",
            "db0sdx": "Show DB0SDX gateway configuration status.",
            "debug": "Show the current debug setting.",
            "dupann": "Show duplicate filtering for announcements.",
            "dupeph": "Show duplicate filtering for ephemeris data.",
            "dupspots": "Show duplicate filtering for DX spots.",
            "dupwcy": "Show duplicate filtering for WCY bulletins.",
            "dupwwv": "Show duplicate filtering for WWV bulletins.",
            "dx": "Show recent DX spots.",
            "dxcc": "Show DXCC or entity information for a prefix or callsign.",
            "dxcq": "Show whether CQ zone suffixes are appended to DX spots.",
            "dxgrid": "Show whether grid data is appended to DX spots.",
            "dxitu": "Show whether ITU zone suffixes are appended to DX spots.",
            "dxqsl": "Show DXQSL import and export readiness.",
            "dxstats": "Show overall DX spot statistics.",
            "echo": "Show or control local echo mode.",
            "email": "Show or update the station email address.",
            "files": "Show local file and export paths.",
            "filter": "Show current filter rules or test filter matches.",
            "groups": "Show joined groups and group settings.",
            "gtk": "Show or control GTK compatibility mode.",
            "heading": "Show beam heading to a DXCC entity.",
            "here": "Show or control HERE announcements.",
            "hfstats": "Show HF-only DX spot statistics.",
            "home": "Show the configured home cluster or home node.",
            "homebbs": "Show the configured home BBS.",
            "qra": "Show or set grid square details.",
            "qth": "Show or set location details.",
            "name": "Show or set operator name details.",
            "homenode": "Show or set the local home-node preference.",
            "hops": "Show hop-related route controls.",
            "ik3qar": "Show IK3QAR gateway configuration status.",
            "ingestpeer": "Show per-peer ingest policy settings.",
            "isolate": "Show or control session isolation mode.",
            "language": "Show the current language preference.",
            "links": "Show connected peer links and transport state.",
            "localnode": "Show whether local-node mode is enabled.",
            "location": "Show or set the current location string.",
            "lockout": "Show or control login lockout mode.",
            "log": "Show recent command and event log entries.",
            "logininfo": "Show or control login banner information.",
            "mail": "Show recent message headers.",
            "maxconnect": "Show the maximum connection limit.",
            "motd": "Show the message of the day.",
            "msgstatus": "Show personal inbox and outbox message status.",
            "mydx": "Show DX spots posted by your callsign.",
            "node": "Show node and home-node information for a callsign.",
            "notimpl": "Show commands that are still not implemented.",
            "obscount": "Show the current obscure-count threshold.",
            "page": "Show the current page length setting.",
            "passphrase": "Show whether a gateway passphrase is configured.",
            "pinginterval": "Show the current network ping interval.",
            "privilege": "Show the current privilege level.",
            "policy": "Show current relay and ingest policy settings.",
            "policydrop": "Show policy drop totals and reasons by peer.",
            "prefix": "Show recent local activity for a callsign prefix.",
            "program": "Show pyCluster program and version information.",
            "proto": "Show protocol health and peer status.",
            "protoack": "Show protocol acknowledgement state by peer.",
            "protoacks": "Show protocol acknowledgement state by peer.",
            "protoalerts": "Show current protocol alerts and stale peers.",
            "protoconfig": "Show protocol thresholds and tuning values.",
            "protoevents": "Show recent protocol event history.",
            "protohistory": "Show recorded protocol state changes.",
            "prompt": "Show the current prompt template.",
            "qrz": "Look up a callsign using the configured QRZ XML service.",
            "rbn": "Show whether RBN spots are enabled.",
            "rcmd": "Show the configured remote command string.",
            "register": "Show whether automatic registration is enabled.",
            "registered": "Show the stored registry record for a user.",
            "relay": "Show relay policy settings by traffic type.",
            "relaypeer": "Show per-peer relay policy settings.",
            "route": "Show route transport counters and peer statistics.",
            "routepc19": "Show whether PC19 routing is enabled.",
            "satellite": "Show satellite-related bulletins or status.",
            "senddbg": "Show whether debug frame transmission is enabled.",
            "sendpc16": "Show whether PC16 transmission is enabled.",
            "startup": "Show startup-command status and saved commands.",
            "station": "Show station profile, registry, and last-spot details.",
            "stats": "Show runtime counts for users, spots, messages, and peers.",
            "sun": "Show solar status for the current location.",
            "grayline": "Show grayline timing for the current location.",
            "moon": "Show moon phase data for the current location.",
            "muf": "Show the current MUF estimate from WWV data.",
            "talk": "Show whether talk messages are enabled.",
            "time": "Show the current UTC time.",
            "uptime": "Show uptime, start time, and current time.",
            "usdb": "Show or update local USDB entries.",
            "users": "Show the registered user list.",
            "usstate": "Show or set the stored US state value.",
            "var": "Show stored user variables.",
            "ve7cc": "Show or control VE7CC compatibility mode.",
            "version": "Show program version information.",
            "vhfstats": "Show VHF-only DX spot statistics.",
            "wantpc16": "Show whether incoming PC16 frames are accepted.",
            "wantpc9x": "Show whether incoming PC9x frames are accepted.",
            "wm7d": "Look up a callsign using WM7D.",
            "wcy": "Show recent WCY propagation bulletins.",
            "wwv": "Show recent WWV propagation bulletins.",
            "wx": "Show recent weather bulletins.",
            "lastspot": "Show the last local spot summary for a callsign.",
            "commands": "List commands in a family or by keyword.",
            "shortcuts": "Show command shortcuts and abbreviations.",
            "setprompt": "Set the login prompt template.",
        }
        cross_group = {"qra", "qth", "name", "homenode", "usdb", "setprompt"}
        if group == "show" and sub in special:
            return self._string(key, special[sub])
        if sub in cross_group and sub in special:
            return self._string(key, special[sub])
        label = sub.replace("_", " ").replace("-", " ").upper() if sub in {"qra", "qth", "wcy", "wwv", "wx"} else sub.replace("_", " ").replace("-", " ")
        if group == "show":
            if sub.startswith("bad"):
                default = f"Show blocked {sub[3:]} rules."
            else:
                default = f"Show {label}."
            return self._string(key, default)
        if group == "set":
            if sub.startswith("bad"):
                default = f"Add a blocked {sub[3:]} rule."
                return self._string(key, default)
            if sub in {"arcluster", "clx", "dxnet", "dxspider", "nowrap", "syslocation", "sysqra", "user"}:
                default = f"Set {label}."
                return self._string(key, default)
            if sub in {"announce", "talk", "wcy", "wwv", "wx", "dx", "dxcq", "dxgrid", "dxitu", "rbn"}:
                default = f"Enable {label}."
                return self._string(key, default)
            if sub in {"debug", "beep", "echo", "register", "prompt", "localnode", "isolate", "lockout", "logininfo"}:
                default = f"Turn on {label}."
                return self._string(key, default)
            if sub in {"relay", "relaypeer", "ingestpeer"}:
                default = f"Set {label} policy."
                return self._string(key, default)
            if sub == "protothreshold":
                return self._string(key, "Set a protocol threshold value.")
            if sub in {"address", "email", "location", "password", "passphrase", "language", "privilege", "page", "pinginterval", "obscount", "maxconnect", "var", "uservar"}:
                default = f"Update {label}."
            else:
                default = f"Set {label}."
            return self._string(key, default)
        if group == "unset":
            if sub.startswith("bad"):
                default = f"Remove blocked {sub[3:]} rules."
                return self._string(key, default)
            if sub in {"arcluster", "clx", "dxnet", "nowrap"}:
                default = f"Unset {label}."
                return self._string(key, default)
            if sub in {"announce", "talk", "wcy", "wwv", "wx", "dx", "dxcq", "dxgrid", "dxitu", "rbn"}:
                default = f"Disable {label}."
                return self._string(key, default)
            if sub in {"debug", "beep", "echo", "register", "prompt", "localnode", "isolate", "lockout", "logininfo"}:
                default = f"Turn off {label}."
                return self._string(key, default)
            if sub in {"relay", "relaypeer", "ingestpeer"}:
                default = f"Clear {label} policy overrides."
                return self._string(key, default)
            if sub == "protothreshold":
                return self._string(key, "Clear a protocol threshold override.")
            if sub in {"password", "passphrase", "var", "uservar", "privilege"}:
                default = f"Clear {label}."
            else:
                default = f"Unset {label}."
            return self._string(key, default)
        if group == "accept":
            return self._string(key, f"Accept {label} filter entries.")
        if group == "reject":
            return self._string(key, f"Reject {label} filter entries.")
        if group == "clear":
            return self._string(key, f"Clear {label} state.")
        if group == "load":
            return self._string(key, f"Load {label} data.")
        if group == "stat":
            return self._string(key, f"Show {label} statistics.")
        if group == "create":
            return self._string(key, f"Create a {label} record.")
        if group == "delete":
            return self._string(key, f"Delete a {label} record.")
        if group == "forward":
            if sub == "latlong":
                return self._string(key, "Set forwarding latitude and longitude.")
            if sub == "opername":
                return self._string(key, "Set the forwarding operator name.")
            return self._string(key, f"Set forwarding {label} details.")
        if group == "get":
            return self._string(key, f"Request {label} data.")
        if group == "sysop":
            if sub == "setprompt":
                default = "Set the sysop login prompt template."
            else:
                default = f"Run the sysop {label} command."
            return self._string(key, default)
        return self._string(key, "Command.")

    async def _cmd_show_shortcuts(self, call: str, arg: str | None) -> str:
        reg = self._build_registry()
        needle = (arg or "").strip().lower()
        rows = list(self._build_shortcut_catalog(reg))
        if needle:
            nneedle = self._normalize_cmd_token(needle)
            rows = [
                r
                for r in rows
                if needle in r[0]
                or needle in r[1]
                or (nneedle and nneedle in self._normalize_cmd_token(r[0]))
                or (nneedle and nneedle in self._normalize_cmd_token(r[1]))
            ]
        if not rows:
            return f"shortcuts: no matches for {needle}\r\n" if needle else "shortcuts: (none)\r\n"
        page = await self._page_size_for(call)
        if page > 0:
            rows = rows[:page]
        lines = [f"shortcuts ({len(rows)}):"]
        lines.append("  Capital letters show the shorthand pyCluster guarantees.")
        for k, v in rows:
            emphasized = self._emphasize_shortcut(k, v)
            display = emphasized or k
            lines.append(f"  {display:<18} => {v}")
            if display != k:
                lines.append(f"    full: {k}")
        return await self._format_console_lines(call, lines)

    async def _cmd_show_capabilities(self, _call: str, _arg: str | None) -> str:
        reg = self._build_registry()
        fams = {"show": 0, "set": 0, "unset": 0, "accept": 0, "reject": 0, "clear": 0, "load": 0, "stat": 0}
        for k in reg:
            g = k.split("/", 1)[0]
            if g in fams:
                fams[g] += 1
        pending = sum(1 for _k, v in reg.items() if v == self._cmd_not_implemented)
        return (
            f"capabilities: commands={len(reg)} notimpl={pending}\r\n"
            f"  show={fams['show']} set={fams['set']} unset={fams['unset']}\r\n"
            f"  accept={fams['accept']} reject={fams['reject']} clear={fams['clear']}\r\n"
            f"  load={fams['load']} stat={fams['stat']}\r\n"
        )

    async def _cmd_show_node(self, _call: str, _arg: str | None) -> str:
        if _arg and _arg.strip():
            target = _arg.split()[0].upper()
            if is_valid_call(target):
                prefs = await self._load_prefs_for_call(target)
                homebbs = prefs.get("homebbs", "")
                homenode = prefs.get("homenode", "")
                pref_node = prefs.get("node", "")
                return (
                    self._render_string("show.node.user_title", "User node profile: {target}", target=target) + "\r\n"
                    + self._render_string("show.node.user_homebbs", "homebbs   : {homebbs}", homebbs=homebbs) + "\r\n"
                    + self._render_string("show.node.user_homenode", "homenode  : {homenode}", homenode=homenode) + "\r\n"
                    + self._render_string("show.node.user_node", "node      : {node}", node=pref_node) + "\r\n"
                )
        lines = [
            self._render_string("show.node.node", "Node       : {node_call}", node_call=self.config.node.node_call),
            self._render_string("show.node.alias", "Alias      : {node_alias}", node_alias=self.config.node.node_alias),
            self._render_string("show.node.owner", "Owner      : {owner_name}", owner_name=self.config.node.owner_name),
            self._render_string("show.node.qth", "QTH        : {qth}", qth=self.config.node.qth),
            self._render_string("show.node.uptime", "Uptime     : {uptime}", uptime=self._uptime_text()),
        ]
        if self._link_stats_fn:
            stats = await self._link_stats_fn()
            desired_rows = await self._link_desired_peers_fn() if self._link_desired_peers_fn else []
            desired = {str(row.get("peer", "")).strip(): row for row in desired_rows if str(row.get("peer", "")).strip()}
            names = sorted(set(stats) | set(desired))
            if names:
                lines.extend(["", self._string("show.node.topology", "Topology"), self._render_string("show.node.topology_root", "{node_call}", node_call=self.config.node.node_call)])
                for idx, name in enumerate(names):
                    branch = "`- " if idx == len(names) - 1 else "|- "
                    state = "up" if name in stats else "down"
                    row = desired.get(name, {})
                    fam = str(row.get("profile") or stats.get(name, {}).get("profile") or "unknown").strip().lower()
                    rendered = self._render_string("show.node.topology_branch", "{branch}{name} [{state} {family}]", branch=branch, name=name, state=state, family=fam)
                    lines.append(rendered[:77])
        return "\r\n".join(lines) + "\r\n"

    async def _cmd_show_cluster(self, _call: str, _arg: str | None) -> str:
        stats = await self._link_stats_fn() if self._link_stats_fn else {}
        local = self.session_count
        total = local + sum(max(0, int(st.get("parsed_frames", 0))) for st in stats.values())
        max_users = max(total, local)
        return self._render_string(
            "show.cluster.line",
            " {nodes} nodes, {local} local / {total} total users  Max users {max_users}  Uptime {uptime}",
            nodes=len(stats),
            local=local,
            total=total,
            max_users=max_users,
            uptime=self._uptime_text(),
        ) + "\r\n"

    async def _cmd_show_users(self, _call: str, _arg: str | None) -> str:
        if not self._sessions:
            return self._string("show.users.empty", "No users connected") + "\r\n"
        sess = self._find_session(_call)
        sess_login = (sess.vars.get("logininfo", "off") if sess else "off").lower() in {"1", "on", "yes", "true"}
        pref_login = (await self._get_pref(_call, "logininfo") or "off").lower() in {"1", "on", "yes", "true"}
        show_logininfo = sess_login or pref_login
        lines = [self._string("show.users.title", "Connected users:")]
        for s in sorted(self._sessions.values(), key=lambda x: x.call):
            age = datetime.now(timezone.utc) - s.connected_at
            mins = int(age.total_seconds() // 60)
            base = self._render_string(
                "show.users.line",
                "{call:<12} online {minutes:>4}m  lang={language}  echo={echo}",
                call=s.call,
                minutes=mins,
                language=s.language,
                echo="on" if s.echo else "off",
            )
            mc = await self._maxconnect_for_call(s.call)
            if mc > 0:
                base += self._render_string("show.users.maxconnect", "  maxc={maxconnect}", maxconnect=mc)
            if show_logininfo:
                row = await self.store.get_user_registry(s.call)
                if row:
                    base += self._render_string("show.users.last", "  last={last_login}", last_login=self._fmt_epoch_short(int(row["last_login_epoch"] or 0)))
            lines.append(base)
        lines = await self._apply_page_size(_call, lines, explicit_limit=False)
        return await self._format_console_lines(_call, lines)

    async def _cmd_show_connect(self, _call: str, _arg: str | None) -> str:
        if not self._link_stats_fn:
            return self._string("show.connect.unavailable", "Node-link subsystem not attached.") + "\r\n"
        stats = await self._link_stats_fn()
        if not stats:
            return self._string("show.connect.empty", "No outbound node links configured in this runtime.") + "\r\n"
        node_proto = await self._node_proto_map()
        lines = [self._string("show.connect.title", "Direct node-link sessions:")]
        for name in sorted(stats):
            st = stats[name]
            _, proto_txt, _ = self._proto_state_for_peer(node_proto, name)
            direction = "inbound" if bool(st.get("inbound", False)) else "outbound"
            lines.append(self._render_string(
                "show.connect.line",
                "{peer:<12} {direction:<8} profile={profile} rx={rx} tx={tx} dropped={dropped} policy_drop={policy_drop}{proto}",
                peer=name,
                direction=direction,
                profile=st.get("profile", "dxspider"),
                rx=int(st.get("parsed_frames", 0)),
                tx=int(st.get("sent_frames", 0)),
                dropped=int(st.get("dropped_frames", 0)),
                policy_drop=int(st.get("policy_dropped", 0)),
                proto=proto_txt,
            ))
        return await self._format_console_lines(_call, lines)

    async def _cmd_show_links(self, _call: str, _arg: str | None) -> str:
        if not self._link_stats_fn:
            return self._string("show.links.unavailable", "Node-link subsystem not attached.") + "\r\n"
        stats = await self._link_stats_fn()
        desired_rows = await self._link_desired_peers_fn() if self._link_desired_peers_fn else []
        desired = {str(row.get("peer", "")).strip(): row for row in desired_rows if str(row.get("peer", "")).strip()}
        names = sorted(set(stats) | set(desired))
        if not names:
            return self._string("show.links.empty", "No direct node links configured.") + "\r\n"
        node_proto = await self._node_proto_map()
        now = int(datetime.now(timezone.utc).timestamp())
        lines = [self._string("show.links.title", "Peer         Family   State Age  RX/TX  Loop Last Version")]
        for name in names:
            st = stats.get(name, {})
            row = desired.get(name, {})
            state = "up" if st else "down"
            family = str(node_proto.get(f"proto.peer.{self._proto_peer_tag(name)}.pc18.family", "")).strip().lower()
            if not family:
                family = str(row.get("profile") or st.get("profile") or "unknown").strip().lower()
            last_epoch_raw = st.get("last_rx_epoch") or row.get("last_connect_epoch") or 0
            try:
                last_epoch = int(last_epoch_raw or 0)
            except (TypeError, ValueError):
                last_epoch = 0
            age = "-"
            if last_epoch > 0:
                age_secs = max(0, now - last_epoch)
                age = f"{age_secs // 60}m" if age_secs < 3600 else f"{age_secs // 3600}h"
            rx = int(st.get("parsed_frames", 0)) if st else 0
            tx = int(st.get("sent_frames", 0)) if st else 0
            reasons_raw = st.get("policy_reasons") if isinstance(st, dict) else None
            reasons = reasons_raw if isinstance(reasons_raw, dict) else {}
            loop_total = 0
            for reason, count in reasons.items():
                if "loop" in str(reason).lower():
                    try:
                        loop_total += int(count)
                    except (TypeError, ValueError):
                        continue
            last_pc = str(st.get("last_pc_type", "") or "").strip() or "-"
            ident = str(node_proto.get(f"proto.peer.{self._proto_peer_tag(name)}.pc18.summary", "")).strip() or "-"
            lines.append(f"{name:<12} {family:<8} {state:<5} {age:>4} {f'{rx}/{tx}':>7} {loop_total:>5} {last_pc:<4} {ident[:28]}")
        return "\r\n".join(lines) + "\r\n"

    async def _cmd_show_filter(self, call: str, _arg: str | None) -> str:
        toks = [t for t in (_arg or "").split() if t]
        target = call.upper()
        rest_toks = toks
        if toks and is_valid_call(toks[0].upper()):
            target = toks[0].upper()
            rest_toks = toks[1:]
        if rest_toks and rest_toks[0].lower() == "test":
            return await self._cmd_show_filter_test(target, rest_toks[1:])
        await self._load_filters_for_call(target)
        s = self._find_session(call)
        echo = int(s.echo) if s else int((await self._get_pref(call, "echo") or "on").lower() in {"1", "on", "yes", "true"})
        here = int(s.here) if s else int((await self._get_pref(call, "here") or "on").lower() in {"1", "on", "yes", "true"})
        beep = int(s.beep) if s else int((await self._get_pref(call, "beep") or "off").lower() in {"1", "on", "yes", "true"})
        language = s.language if s else (await self._get_pref(call, "language") or "en")
        profile = s.peer_profile if s else normalize_profile(await self._get_pref(call, "profile") or "dxspider")
        hdr = (
            f"Echo={'on' if echo else 'off'}  Here={'on' if here else 'off'}  "
            f"Beep={'on' if beep else 'off'}  Language={language}  Profile={profile}"
        )
        fams = self._filters.get(target, {})
        lines = [
            self._render_string("set.language_set", "Language set to {language} for {call}.", language=language, call=target),
            f"Profile for {target} set to {profile}.",
            hdr,
            f"Filters for {target}",
        ]
        if not fams:
            lines.append(self._string("filters.empty", "  No filters are defined."))
            return await self._format_console_lines(call, [ln for ln in lines if ln])
        for fam in sorted(fams):
            lines.append(f"[{fam}]")
            merged: list[tuple[int, str, FilterRule]] = []
            for action in ("accept", "reject"):
                for r in fams[fam].get(action, []):
                    merged.append((r.slot, action, r))
            merged.sort(key=lambda x: (x[0], 0 if x[1] == "reject" else 1))
            for _, action, r in merged:
                lines.append(f"  {action}/{fam} {r.slot} {r.expr}")
        lines.append(self._string("filters.preview", "Preview:"))
        lines.append(self._string("filters.preview_spots", "  show/filter test spots <freq_khz> <dx_call> <spotter> [info]"))
        lines.append(self._string("filters.preview_route", "  show/filter test route <peer>"))
        lines.append(self._string("filters.preview_text", "  show/filter test <announce|wcy|wwv|wx> <sender> <text>"))
        lines.append(self._string("filters.preview_verbose", "  add --verbose after family to include winning rule"))
        lines = await self._apply_page_size(call, lines, explicit_limit=bool(rest_toks))
        return await self._format_console_lines(call, lines)

    async def _cmd_show_filter_test(self, target: str, toks: list[str]) -> str:
        if not toks:
            return self._string("filters.test_usage", "Usage: show/filter [<call>] test <spots [--verbose] <freq_khz> <dx_call> <spotter> [info] | route [--verbose] <peer> | <announce|wcy|wwv|wx> [--verbose] <sender> <text>>") + "\r\n"
        fam = toks[0].lower()
        args = toks[1:]
        verbose = False
        if args and args[0].lower() in {"--verbose", "verbose", "-v"}:
            verbose = True
            args = args[1:]
        call = target.upper()
        if fam == "spots":
            if len(args) < 3:
                return self._string("filters.test_spots_usage", "Usage: show/filter test spots <freq_khz> <dx_call> <spotter> [info]") + "\r\n"
            try:
                freq = float(args[0])
            except ValueError:
                return self._string("filters.test_spots_usage", "Usage: show/filter test spots <freq_khz> <dx_call> <spotter> [info]") + "\r\n"
            dx_call = args[1].upper()
            spotter = args[2].upper()
            info = " ".join(args[3:]) if len(args) > 3 else ""
            if call not in self._filters:
                await self._load_filters_for_call(call)
            f = self._filters.get(call, {}).get("spots", {})
            ok, detail = self._eval_filter_family_detail(
                f,
                lambda expr: self._spot_matches_expr(freq, dx_call, spotter, info, expr),
            )
            lines = [
                self._render_string("filters.test_spots_title", "Filter test for {call} (spots):", call=call),
                self._render_string("filters.decision", "  Decision: {decision}", decision="allow" if ok else "deny"),
                self._render_string("filters.frequency", "  Frequency: {freq:.1f} kHz", freq=freq),
                self._render_string("filters.dx_call", "  DX Call: {dx_call}", dx_call=dx_call),
                self._render_string("filters.spotter", "  Spotter: {spotter}", spotter=spotter),
                self._render_string("filters.info", "  Info: {info}", info=info or "(none)"),
            ]
            if verbose and detail:
                lines.append(self._render_string("filters.winning_rule", "  Winning Rule: {detail}", detail=detail))
            return await self._format_console_lines(target, lines)
        if fam == "route":
            if len(args) != 1:
                return self._string("filters.test_route_usage", "Usage: show/filter test route <peer>") + "\r\n"
            peer = args[0]
            if call not in self._filters:
                await self._load_filters_for_call(call)
            f = self._filters.get(call, {}).get("route", {})
            ok, detail = self._eval_filter_family_detail(f, lambda expr: self._route_peer_matches_expr(peer, expr))
            lines = [
                self._render_string("filters.test_route_title", "Filter test for {call} (route):", call=call),
                self._render_string("filters.decision", "  Decision: {decision}", decision="allow" if ok else "deny"),
                self._render_string("filters.peer", "  Peer: {peer}", peer=peer),
            ]
            if verbose and detail:
                lines.append(self._render_string("filters.winning_rule", "  Winning Rule: {detail}", detail=detail))
            return await self._format_console_lines(target, lines)
        if fam in {"announce", "wcy", "wwv", "wx"}:
            if len(args) < 2:
                return self._render_string("filters.test_text_usage", "Usage: show/filter test {family} <sender> <text>", family=fam) + "\r\n"
            sender = args[0].upper()
            text = " ".join(args[1:])
            if call not in self._filters:
                await self._load_filters_for_call(call)
            f = self._filters.get(call, {}).get(fam, {})
            ok, detail = self._eval_filter_family_detail(f, lambda expr: self._text_matches_expr(sender, text, expr))
            lines = [
                self._render_string("filters.test_text_title", "Filter test for {call} ({family}):", call=call, family=fam),
                self._render_string("filters.decision", "  Decision: {decision}", decision="allow" if ok else "deny"),
                self._render_string("filters.sender", "  Sender: {sender}", sender=sender),
                self._render_string("filters.text", "  Text: {text}", text=text),
            ]
            if verbose and detail:
                lines.append(self._render_string("filters.winning_rule", "  Winning Rule: {detail}", detail=detail))
            return await self._format_console_lines(target, lines)
        return self._string("filters.test_generic_usage", "Usage: show/filter test <spots|route|announce|wcy|wwv|wx> ...") + "\r\n"

    async def _cmd_show_configuration(self, _call: str, _arg: str | None) -> str:
        ports = ",".join(str(p) for p in self._configured_ports())
        lines = [
            "Node configuration:",
            f"  Node Call: {self.config.node.node_call}",
            f"  node_call={self.config.node.node_call}",
            f"  Node Alias: {self.config.node.node_alias}",
            f"  node_alias={self.config.node.node_alias}",
            f"  Location (QTH): {self.config.node.qth}",
            f"  qth={self.config.node.qth}",
            f"  Telnet Listener: {self.config.telnet.host}:{self.config.telnet.port}",
            f"  telnet={self.config.telnet.host}:{self.config.telnet.port}",
            f"  Telnet Ports: {ports}",
            f"  System Operator Web: {self.config.web.host}:{self.config.web.port}",
            f"  web={self.config.web.host}:{self.config.web.port}",
            f"  Database: {self.config.store.sqlite_path}",
        ]
        return await self._format_console_lines(_call, lines)

    async def _cmd_show_program(self, _call: str, _arg: str | None) -> str:
        lines = [
            "Program information:",
            "  Name: pyCluster",
            "  Mode: DXSpider compatibility",
            f"  Version: {__version__}",
        ]
        return await self._format_console_lines(_call, lines)

    async def _cmd_show_bands(self, _call: str, _arg: str | None) -> str:
        lines = [self._string("show.bands.title", "Band plan:")]
        for b in sorted(BAND_RANGES):
            lo, hi = BAND_RANGES[b]
            lines.append(f"  {b:<6} {lo:>8.1f}-{hi:<8.1f} kHz")
        return await self._format_console_lines(_call, lines)

    async def _cmd_show_dxstats(self, _call: str, _arg: str | None, scope: str = "all") -> str:
        total = await self.store.count_spots()
        hf_lo, hf_hi = BAND_RANGES["hf"]
        vhf_lo, vhf_hi = BAND_RANGES["vhf"]
        hf = await self.store.count_spots_in_range(hf_lo, hf_hi)
        vhf = await self.store.count_spots_in_range(vhf_lo, vhf_hi)
        rows = await self.store.latest_spots(limit=1)
        last = "-"
        if rows:
            r = rows[0]
            ts = datetime.fromtimestamp(int(r["epoch"]), tz=timezone.utc).strftime("%-d-%b-%Y %H%MZ")
            last = f"{r['dx_call']} {r['freq_khz']:.1f} {ts}"
        if scope == "hf":
            lines = [self._string("show.dxstats.hf_title", "HF DX summary:"), self._render_string("show.dxstats.total", "  Total: {count}", count=hf), self._render_string("show.dxstats.last", "  Last: {last}", last=last)]
            return await self._format_console_lines(_call, lines)
        if scope == "vhf":
            lines = [self._string("show.dxstats.vhf_title", "VHF DX summary:"), self._render_string("show.dxstats.total", "  Total: {count}", count=vhf), self._render_string("show.dxstats.last", "  Last: {last}", last=last)]
            return await self._format_console_lines(_call, lines)
        lines = [
            self._string("show.dxstats.title", "DX summary:"),
            self._render_string("show.dxstats.total", "  Total: {count}", count=total),
            self._render_string("show.dxstats.hf", "  HF: {count}", count=hf),
            self._render_string("show.dxstats.vhf", "  VHF: {count}", count=vhf),
            self._render_string("show.dxstats.last", "  Last: {last}", last=last),
        ]
        return await self._format_console_lines(_call, lines)

    async def _cmd_show_425(self, call: str, arg: str | None) -> str:
        # Conventional alias in many clusters: compact recent DX view.
        text = (arg or "").strip()
        if not text:
            text = "25"
        return await self._cmd_show_dx(call, text)

    async def _cmd_show_contest(self, call: str, arg: str | None) -> str:
        explicit = bool(arg and arg.split()[0].isdigit())
        limit = 20
        if explicit:
            limit = max(1, min(int(arg.split()[0]), 200))
        rows = await self.store.list_bulletins("announce", limit=200)
        out: list[str] = []
        for r in rows:
            body = str(r["body"] or "")
            if "contest" not in body.lower():
                continue
            ts = datetime.fromtimestamp(int(r["epoch"]), tz=timezone.utc).strftime("%-d-%b-%Y %H%MZ")
            scope = str(r["scope"] or "").strip().upper()
            prefix = f"[{scope}] " if scope and scope != "LOCAL" else ""
            out.append(f"{ts} {prefix}{r['sender']}: {body}")
            if len(out) >= limit:
                break
        if not out:
            return self._string("show.contest.empty", "No contest announcements.") + "\r\n"
        out = await self._apply_page_size(call, out, explicit_limit=explicit)
        return await self._format_console_lines(call, [self._string("show.contest.title", "Contest announcements:")] + out)

    async def _cmd_show_satellite(self, call: str, arg: str | None) -> str:
        explicit = bool(arg and arg.split()[0].isdigit())
        limit = 20
        if explicit:
            limit = max(1, min(int(arg.split()[0]), 200))
        rows = await self.store.latest_spots(limit=200)
        sat: list[str] = []
        for r in rows:
            dx = str(r["dx_call"] or "").upper()
            info = str(r["info"] or "")
            info_u = info.upper()
            if not (
                "SAT" in info_u
                or "SATELLITE" in info_u
                or "ISS" in info_u
                or dx.startswith(("AO-", "FO-", "SO-", "RS-", "XW-", "IO-"))
            ):
                continue
            when = datetime.fromtimestamp(int(r["epoch"]), tz=timezone.utc).strftime("%-d-%b-%Y %H%MZ")
            sess = self._find_session(call)
            profile = sess.peer_profile if sess else "dxspider"
            line = format_dx_line_for_profile(
                profile=profile,
                freq_khz=float(r["freq_khz"]),
                dx_call=dx,
                when=when,
                info=info,
                spotter=str(r["spotter"]),
            )
            sat.append(line)
            if len(sat) >= limit:
                break
        if not sat:
            return self._string("show.satellite.empty", "No satellite spots.") + "\r\n"
        sat = await self._apply_page_size(call, sat, explicit_limit=explicit)
        return await self._format_console_lines(call, [self._string("show.satellite.title", "Satellite spots:")] + sat)

    async def _cmd_show_prefix(self, _call: str, arg: str | None) -> str:
        if not arg:
            return self._string("show.prefix.usage", "Usage: show/prefix <prefix>") + "\r\n"
        p = arg.split()[0].upper()
        c = await self.store.count_spots_by_prefix(p)
        noun = "entry" if c == 1 else "entries"
        return self._render_string("show.prefix.result", "Prefix {prefix} has {count} local spot {noun}.", prefix=p, count=c, noun=noun) + "\r\n"

    async def _cmd_show_lastspot(self, _call: str, arg: str | None) -> str:
        if not arg:
            return self._string("show.lastspot.usage", "Usage: show/lastspot <call>") + "\r\n"
        call = arg.split()[0].upper()
        row = await self.store.latest_spot_for_call(call)
        if not row:
            return self._render_string("show.lastspot.empty", "{call}: no local spot data.", call=call) + "\r\n"
        ts = datetime.fromtimestamp(int(row["epoch"]), tz=timezone.utc).strftime("%-d-%b-%Y %H%MZ")
        lines = [
            self._render_string("show.lastspot.title", "{call} was last spotted on {timestamp}.", call=call, timestamp=ts),
            self._render_string("show.lastspot.frequency", "  Frequency: {freq:.1f} kHz", freq=float(row["freq_khz"])),
            self._render_string("show.lastspot.spotter", "  Spotter: {spotter}", spotter=row["spotter"]),
            self._render_string("show.lastspot.via", "  Via: {source}", source=row["source_node"]),
            self._render_string("show.lastspot.comment", "  Comment: {comment}", comment=row["info"] or "(none)"),
        ]
        return await self._format_console_lines(_call, lines)

    @staticmethod
    def _kv_line(label: str, value: str) -> str:
        if not label:
            return f"  {'':<9}   {value}"
        return f"  {label:>9} : {value}"

    async def _cmd_show_qrz(self, _call: str, arg: str | None) -> str:
        if not arg:
            return self._string("show.qrz.usage", "Usage: show/qrz <call>") + "\r\n"
        call = arg.split()[0].upper()
        try:
            result = await self._qrz.lookup(call)
        except QRZLookupError as exc:
            return f"{exc}\r\n"
        if result is None:
            return self._render_string("show.qrz.empty", "{call}: no QRZ data returned.", call=call) + "\r\n"
        lines = [self._render_string("show.qrz.title", "QRZ lookup for {call}:", call=result.callsign)]
        if result.fname or result.name:
            label = " ".join(part for part in (result.fname, result.name) if part).strip()
            lines.append(self._kv_line("Name", label))
        if result.addr1:
            lines.append(self._kv_line("Address", result.addr1))
        if result.addr2:
            lines.append(self._kv_line("QTH", result.addr2))
        if result.state:
            lines.append(self._kv_line("State", result.state))
        if result.country:
            lines.append(self._kv_line("Country", result.country))
        if result.grid:
            lines.append(self._kv_line("Grid", result.grid))
        if result.county:
            lines.append(self._kv_line("County", result.county))
        if result.dxcc:
            lines.append(self._kv_line("DXCC", result.dxcc))
        if result.cqzone:
            lines.append(self._kv_line("CQ Zone", result.cqzone))
        if result.ituzone:
            lines.append(self._kv_line("ITU Zone", result.ituzone))
        if result.lat and result.lon:
            lines.append(self._kv_line("Lat/Lon", f"{result.lat}, {result.lon}"))
        if result.aliases:
            lines.append(self._kv_line("Aliases", result.aliases))
        return await self._format_console_lines(_call, lines)

    async def _cmd_show_callsign_lookup(self, actor_call: str, arg: str | None, service: str, title: str | None = None) -> str:
        if not arg:
            return self._render_string("show.lookup.usage", "Usage: show/{service} <call>", service=service) + "\r\n"
        target = arg.split()[0].upper()
        heading = title or self._render_string("show.lookup.title", "{service} lookup for {target}:", service=service.upper(), target=target)
        lines = [heading]

        reg = await self.store.get_user_registry(target)
        if reg is not None:
            name = str(reg["display_name"] or "").strip()
            qth = str(reg["qth"] or "").strip()
            qra = str(reg["qra"] or "").strip().upper()
            home_node = str(reg["home_node"] or "").strip().upper()
            email = str(reg["email"] or "").strip()
            address = str(reg["address"] or "").strip()
            privilege = str(reg["privilege"] or "").strip().lower() or "user"
            if name:
                lines.append(f"  Name: {name}")
            if qth:
                lines.append(f"  QTH: {qth}")
            if qra:
                lines.append(f"  Grid: {qra}")
            if home_node:
                lines.append(f"  Home Node: {home_node}")
            if privilege:
                lines.append(self._render_string("show.lookup.privilege", "  Privilege: {value}", value=privilege))
            if email:
                lines.append(self._render_string("show.lookup.email", "  Email: {value}", value=email))
            if address:
                lines.append(self._render_string("show.lookup.address", "  Address: {value}", value=address))

        ent = lookup(target)
        if ent is not None:
            lines.append(self._render_string("show.lookup.dxcc", "  DXCC: {value}", value=ent.name))
            lines.append(self._render_string("show.lookup.cq", "  CQ Zone: {value}", value=ent.cq_zone))
            lines.append(self._render_string("show.lookup.itu", "  ITU Zone: {value}", value=ent.itu_zone))

        row = await self.store.latest_spot_for_call(target)
        if row is not None:
            ts = datetime.fromtimestamp(int(row["epoch"]), tz=timezone.utc).strftime("%-d-%b-%Y %H%MZ")
            lines.append(self._render_string("show.lookup.last_spotted", "  Last Spotted: {value}", value=ts))
            lines.append(self._render_string("show.lookup.frequency", "  Frequency: {freq:.1f} kHz", freq=float(row["freq_khz"])))
            lines.append(self._render_string("show.lookup.spotter", "  Spotter: {value}", value=row["spotter"]))
            lines.append(self._render_string("show.lookup.via", "  Via: {value}", value=row["source_node"]))
            lines.append(self._render_string("show.lookup.comment", "  Comment: {value}", value=row["info"] or "(none)"))

        if len(lines) == 1:
            return self._render_string("show.lookup.empty", "{target}: no local {service} data.", target=target, service=service) + "\r\n"
        return await self._format_console_lines(actor_call, lines)

    async def _cmd_show_qra(self, call: str, arg: str | None) -> str:
        target = call.upper()
        if arg and arg.strip():
            tok = arg.split()[0].upper()
            if is_valid_call(tok):
                target = tok
        reg = await self.store.get_user_registry(target)
        if reg and str(reg["qra"] or "").strip():
            return f"QRA for {target}: {reg['qra']}\r\nqra={reg['qra']}\r\n"
        pref = (await self._get_pref(target, "qra") or "").strip()
        if pref:
            return f"QRA for {target}: {pref}\r\nqra={pref}\r\n"
        s = self._find_session(target)
        if s:
            qra = str(s.vars.get("qra", "")).strip()
            if qra:
                return f"QRA for {target}: {qra}\r\nqra={qra}\r\n"
        return f"QRA for {target}: (none)\r\nqra=\r\n"

    async def _cmd_show_apropos(self, call: str, arg: str | None) -> str:
        term = (arg or "").strip().lower()
        if not term:
            return self._string("show.apropos.usage", "Usage: show/apropos <term>") + "\r\n"
        commands = sorted(self._build_registry().keys())
        commands = [c for c in commands if await self._command_visible_for(call, c)]
        matches = [c for c in commands if term in c]
        if not matches:
            return self._render_string("show.apropos.empty", "No commands match {term}.", term=term) + "\r\n"
        page = await self._page_size_for(call)
        if page > 0:
            matches = matches[:page]
        lines = [self._render_string("show.apropos.title", "Commands matching {term} ({count}):", term=term, count=len(matches))]
        lines.extend(f"  {c}" for c in matches)
        return await self._format_console_lines(call, lines)

    async def _cmd_show_notimpl(self, call: str, _arg: str | None) -> str:
        reg = self._build_registry()
        pending = sorted(k for k, v in reg.items() if v == self._cmd_not_implemented)
        if not pending:
            return self._string("show.notimpl.empty", "Not-implemented commands: none.") + "\r\n"
        page = await self._page_size_for(call)
        if page > 0:
            pending = pending[:page]
        lines = [self._render_string("show.notimpl.title", "Not-implemented commands ({count}):", count=len(pending))]
        lines.extend(f"  {k}" for k in pending)
        return await self._format_console_lines(call, lines)

    async def _cmd_show_time(self, _call: str, _arg: str | None) -> str:
        return f"UTC time: {datetime.now(timezone.utc).strftime('%H:%M:%SZ')}\r\n"

    async def _cmd_show_date(self, _call: str, _arg: str | None) -> str:
        return f"UTC date: {datetime.now(timezone.utc).strftime('%-d-%b-%Y')}\r\n"

    async def _cmd_show_uptime(self, _call: str, _arg: str | None) -> str:
        now = datetime.now(timezone.utc)
        lines = [
            f"uptime={self._uptime_text()} started={self.started_at.isoformat()} now={now.isoformat()}",
            "Uptime:",
            f"  Running: {self._uptime_text()}",
            f"  Started: {self.started_at.isoformat()}",
            f"  Now: {now.isoformat()}",
        ]
        return await self._format_console_lines(_call, lines)

    async def _cmd_show_motd(self, _call: str, _arg: str | None) -> str:
        return await self._motd_block()

    async def _cmd_show_startup(self, _call: str, _arg: str | None) -> str:
        target = _call.upper()
        if _arg and _arg.strip():
            tok = _arg.split()[0].upper()
            if is_valid_call(tok):
                target = tok
        if target != _call.upper():
            denied = await self._require_privilege(_call, 2, "show/startup")
            if denied:
                return denied
        prefs = await self._load_prefs_for_call(target)
        enabled = (prefs.get("startup", "off")).lower() in {"1", "on", "yes", "true"}
        rows = await self.store.list_startup_commands(target, limit=200)
        lines = [
            "Startup status:",
            f"  Startup UTC: {self._startup_utc.isoformat()}",
            "  Services: telnet, web",
            "  Node Link: available (transport adapters)",
            f"  Startup for {target}: {'on' if enabled else 'off'}",
            f"  Startup Commands: {len(rows)}",
        ]
        for r in rows:
            lines.append(f"  {int(r['id']):>4} {r['command']}")
        return await self._format_console_lines(_call, lines)

    async def _cmd_show_heading(self, _call: str, _arg: str | None) -> str:
        lines = [self._render_string("show.heading.origin", "{node_call} {qth}", node_call=self.config.node.node_call, qth=self.config.node.qth)]
        target = (_arg or "").strip().upper()
        if not target:
            return "\r\n".join(lines) + "\r\n"
        info = await self._coords_context_for(_call.upper())
        if not info:
            lines.append(self._string("show.heading.needs_reference", "Set your grid square or forward/latlong first."))
            return "\r\n".join(lines) + "\r\n"
        ent = lookup(target)
        if ent is None:
            lines.append(self._render_string("show.heading.missing", "No heading data for {target}.", target=target))
            return "\r\n".join(lines) + "\r\n"
        (lat1, lon1), source = info
        lat1r = math.radians(lat1)
        lon1r = math.radians(lon1)
        lat2r = math.radians(ent.lat)
        lon2r = math.radians(ent.lon)
        dlon = lon2r - lon1r
        y = math.sin(dlon) * math.cos(lat2r)
        x = math.cos(lat1r) * math.sin(lat2r) - math.sin(lat1r) * math.cos(lat2r) * math.cos(dlon)
        bearing = (math.degrees(math.atan2(y, x)) + 360.0) % 360.0
        lines.append(self._render_string("show.heading.result", "Heading to {entity} ({target}): {bearing:.0f} deg", entity=ent.name, target=target, bearing=bearing))
        lines.append(self._render_string("show.heading.reference", "Reference: {source}", source=source))
        return "\r\n".join(lines) + "\r\n"

    async def _cmd_show_stats(self, call: str, _arg: str | None) -> str:
        spots = await self.store.count_spots()
        total_msg, unread_msg = await self.store.message_counts(call)
        peers = 0
        policy_drop = 0
        if self._link_stats_fn:
            stats = await self._link_stats_fn()
            peers = len(stats)
            policy_drop = sum(int(st.get("policy_dropped", 0)) for st in stats.values())
        lines = [
            self._render_string("show.stats.headline", "Users={users} Spots={spots} Messages={messages} Peers={peers} PolicyDrop={policy_drop}", users=self.session_count, spots=spots, messages=total_msg, peers=peers, policy_drop=policy_drop),
            self._string("show.stats.title", "Runtime summary:"),
            self._render_string("show.stats.users", "  Users: {users}", users=self.session_count),
            self._render_string("show.stats.spots", "  Spots: {spots}", spots=spots),
            self._render_string("show.stats.messages", "  Messages: {total} total, {unread} unread", total=total_msg, unread=unread_msg),
            self._render_string("show.stats.peers", "  Peers: {peers}", peers=peers),
            self._render_string("show.stats.policy_drop", "  Policy Drops: {policy_drop}", policy_drop=policy_drop),
        ]
        return await self._format_console_lines(call, lines)

    async def _cmd_show_named_status(self, call: str, _arg: str | None, name: str) -> str:
        target = call.upper()
        if _arg and _arg.strip():
            tok = _arg.split()[0].upper()
            if is_valid_call(tok):
                target = tok
        if target != call.upper():
            denied = await self._require_privilege(call, 2, f"show/{name}")
            if denied:
                return denied
        vars_map = dict(self._session_vars(target))
        pref_map = await self._load_prefs_for_call(target)
        vars_map.update(pref_map)
        if name == "dxqsl":
            exp = vars_map.get("dxqsl_export_path", "")
            imp = vars_map.get("dxqsl_import_path", "")
            lines = [f"DXQSL status for {target}:"]
            lines.append(f"  Export Path: {exp or '(not set)'}")
            lines.append(f"  Import Path: {imp or '(not set)'}")
            lines.append(f"  Ready: {'yes' if exp and imp else 'no'}")
            return await self._format_console_lines(call, lines)
        if name == "cmd_cache":
            reg = self._build_registry()
            show_n = sum(1 for k in reg if k.startswith("show/"))
            set_n = sum(1 for k in reg if k.startswith("set/"))
            unset_n = sum(1 for k in reg if k.startswith("unset/"))
            short_n = len(self._build_shortcut_catalog(reg))
            lines = [
                f"Command cache for {target}:",
                f"  Commands: {len(reg)}",
                f"  Show: {show_n}",
                f"  Set: {set_n}",
                f"  Unset: {unset_n}",
                f"  Shortcuts: {short_n}",
                "  State: warm",
            ]
            return await self._format_console_lines(call, lines)
        if name in {"db0sdx", "ik3qar", "wm7d"}:
            host = vars_map.get(f"{name}.host", "") or vars_map.get(f"{name}_host", "")
            port = vars_map.get(f"{name}.port", "") or vars_map.get(f"{name}_port", "")
            enabled_v = (vars_map.get(name, "") or vars_map.get(f"{name}.enabled", "off")).strip().lower()
            enabled = enabled_v in {"1", "on", "yes", "true"}
            lines = [f"{name} gateway status:"]
            lines.append(f"  Enabled: {'on' if enabled else 'off'}")
            lines.append(f"  enabled={'on' if enabled else 'off'}")
            if host:
                lines.append(f"  Host: {host}")
                lines.append(f"  host={host}")
            if port:
                lines.append(f"  Port: {port}")
                lines.append(f"  port={port}")
            if not host and not port:
                lines.append("  Endpoint: (not configured)")
            return await self._format_console_lines(call, lines)
        if name in {"talk", "announce", "wcy", "wwv", "wx", "dx", "dxcq", "dxitu", "dxgrid"}:
            val = vars_map.get(name, "on")
            shown = "on" if self._is_on_value(str(val), default=True) else "off"
            return f"{self._display_label(name)} for {target}: {shown}\r\n{name.lower()}={shown}\r\n"
        if name in {"debug", "isolate", "lockout", "register", "prompt", "local_node"}:
            val = vars_map.get(name, "off")
            return f"{self._display_label(name)} for {target}: {val}\r\n{name.lower()}={val}\r\n"
        if name in {"dup_ann", "dup_eph", "dup_spots", "dup_wcy", "dup_wwv"}:
            val = vars_map.get(name, "off")
            return f"{self._display_label(name)} for {target}: {val}\r\n{name.lower()}={val}\r\n"
        if name in {"qra", "station", "name", "qth", "location"}:
            val = vars_map.get(name, "")
            return f"{self._display_label(name)} for {target}: {val or '(none)'}\r\n{name.lower()}={val}\r\n"
        if name in {"rcmd", "groups"}:
            if not vars_map:
                return f"{name}: (none)\r\n"
            lines = []
            if name in vars_map:
                lines.append(f"{name}: {vars_map[name]}")
                if name == "rcmd":
                    lines.append(f"rcmd={vars_map[name]}")
            lines.extend(f"{k}: {v}" for k, v in sorted(vars_map.items()) if k.startswith(name + "."))
            if name == "groups" and "groups.joined" in vars_map:
                lines.append(f"groups.joined={vars_map['groups.joined']}")
            if not lines:
                return f"{name}: (none)\r\n"
            lines.insert(0, f"Call: {target}")
            return await self._format_console_lines(call, lines)
        if name == "sun":
            return await self._cmd_show_sun(target)
        if name == "moon":
            return await self._cmd_show_moon(target)
        if name == "grayline":
            return await self._cmd_show_grayline(target)
        if name == "muf":
            return await self._cmd_show_muf(target)
        return f"No local {name.replace('_', ' ')} data for {target}.\r\n"

    async def _show_named_target(self, call: str, arg: str | None, cmd: str) -> str:
        target = call.upper()
        if arg and arg.strip():
            tok = arg.split()[0].upper()
            if is_valid_call(tok):
                target = tok
        if target != call.upper():
            denied = await self._require_privilege(call, 2, cmd)
            if denied:
                return denied
        return target

    async def _show_session_pref(self, call: str, arg: str | None, cmd: str, key: str, default: str) -> str:
        target = await self._show_named_target(call, arg, cmd)
        vars_map = dict(self._session_vars(target))
        pref_map = await self._load_prefs_for_call(target)
        vars_map.update(pref_map)
        val = vars_map.get(key, default)
        label = "TALK" if key == "talk" else self._display_label(key)
        return f"{label} for {target}: {val}\r\n{key.lower()}={val}\r\n"

    async def _show_key_value(
        self,
        call: str,
        arg: str | None,
        cmd: str,
        key: str,
        *,
        pref_key: str | None = None,
        registry_field: str | None = None,
        default: str = "",
        readable_label: str | None = None,
    ) -> str:
        target = await self._show_named_target(call, arg, cmd)
        value = ""
        if registry_field:
            row = await self.store.get_user_registry(target)
            if row is not None:
                value = str(row[registry_field] or "")
        if not value:
            vars_map = dict(self._session_vars(target))
            pref_map = await self._load_prefs_for_call(target)
            vars_map.update(pref_map)
            value = str(vars_map.get(pref_key or key, default) or "")
        label = readable_label or key.replace("_", " ").title()
        wire_key = key.lower()
        return f"{label} for {target}: {value or '(none)'}\r\n{wire_key}={value}\r\n"

    async def _show_gateway_status(self, call: str, arg: str | None, cmd: str, name: str) -> str:
        target = await self._show_named_target(call, arg, cmd)
        vars_map = dict(self._session_vars(target))
        pref_map = await self._load_prefs_for_call(target)
        vars_map.update(pref_map)
        host = vars_map.get(f"{name}.host", "") or vars_map.get(f"{name}_host", "")
        port = vars_map.get(f"{name}.port", "") or vars_map.get(f"{name}_port", "")
        enabled_v = (vars_map.get(name, "") or vars_map.get(f"{name}.enabled", "off")).strip().lower()
        enabled = enabled_v in {"1", "on", "yes", "true"}
        lines = [f"{name} gateway status:"]
        lines.append(f"  Enabled: {'on' if enabled else 'off'}")
        lines.append(f"  enabled={'on' if enabled else 'off'}")
        if host:
            lines.append(f"  Host: {host}")
            lines.append(f"  host={host}")
        if port:
            lines.append(f"  Port: {port}")
            lines.append(f"  port={port}")
        if not host and not port:
            lines.append("  Endpoint: not configured on this node")
        return await self._format_console_lines(call, lines)

    async def _show_named_map(self, call: str, arg: str | None, cmd: str, name: str) -> str:
        target = await self._show_named_target(call, arg, cmd)
        vars_map = dict(self._session_vars(target))
        pref_map = await self._load_prefs_for_call(target)
        vars_map.update(pref_map)
        if not vars_map:
            return f"{name}: (none)\r\n"
        lines = []
        if name in vars_map:
            lines.append(f"{name}: {vars_map[name]}")
            if name == "rcmd":
                lines.append(f"rcmd={vars_map[name]}")
        lines.extend(f"{k}: {v}" for k, v in sorted(vars_map.items()) if k.startswith(name + "."))
        if name == "groups" and "groups.joined" in vars_map:
            lines.append(f"groups.joined={vars_map['groups.joined']}")
        if not lines:
            return f"{name}: (none)\r\n"
        lines.insert(0, f"Call: {target}")
        return await self._format_console_lines(call, lines)

    async def _cmd_show_talk_direct(self, call: str, arg: str | None) -> str:
        return await self._show_session_pref(call, arg, "show/talk", "talk", "on")

    async def _cmd_show_debug_direct(self, call: str, arg: str | None) -> str:
        return await self._show_session_pref(call, arg, "show/debug", "debug", "off")

    async def _cmd_show_isolate_direct(self, call: str, arg: str | None) -> str:
        return await self._show_session_pref(call, arg, "show/isolate", "isolate", "off")

    async def _cmd_show_lockout_direct(self, call: str, arg: str | None) -> str:
        return await self._show_session_pref(call, arg, "show/lockout", "lockout", "off")

    async def _cmd_show_groups_direct(self, call: str, arg: str | None) -> str:
        return await self._show_named_map(call, arg, "show/groups", "groups")

    async def _cmd_show_rcmd_direct(self, call: str, arg: str | None) -> str:
        return await self._show_named_map(call, arg, "show/rcmd", "rcmd")

    async def _cmd_show_sun_direct(self, call: str, arg: str | None) -> str:
        target = await self._show_named_target(call, arg, "show/sun")
        return await self._cmd_show_sun(target)

    async def _cmd_show_moon_direct(self, call: str, arg: str | None) -> str:
        target = await self._show_named_target(call, arg, "show/moon")
        return await self._cmd_show_moon(target)

    async def _cmd_show_grayline_direct(self, call: str, arg: str | None) -> str:
        target = await self._show_named_target(call, arg, "show/grayline")
        return await self._cmd_show_grayline(target)

    async def _cmd_show_muf_direct(self, call: str, arg: str | None) -> str:
        target = await self._show_named_target(call, arg, "show/muf")
        return await self._cmd_show_muf(target, arg)

    async def _cmd_show_cmdcache_direct(self, _call: str, _arg: str | None) -> str:
        reg = self._build_registry()
        show_n = sum(1 for k in reg if k.startswith("show/"))
        set_n = sum(1 for k in reg if k.startswith("set/"))
        unset_n = sum(1 for k in reg if k.startswith("unset/"))
        short_n = len(self._build_shortcut_catalog(reg))
        lines = [
            "Command cache:",
            f"  Commands: {len(reg)}",
            f"  Show: {show_n}",
            f"  Set: {set_n}",
            f"  Unset: {unset_n}",
            f"  Shortcuts: {short_n}",
            "  State: warm",
            f"  cmd_cache: commands={len(reg)} show={show_n} set={set_n} unset={unset_n} shortcuts={short_n} state=warm",
        ]
        return await self._format_console_lines(_call, lines)

    async def _cmd_show_dxqsl_direct(self, call: str, arg: str | None) -> str:
        target = await self._show_named_target(call, arg, "show/dxqsl")
        vars_map = dict(self._session_vars(target))
        pref_map = await self._load_prefs_for_call(target)
        vars_map.update(pref_map)
        exp = vars_map.get("dxqsl_export_path", "")
        imp = vars_map.get("dxqsl_import_path", "")
        lines = [f"dxqsl status: call={target}"]
        lines.append(f"  export_path={exp or '(not set)'}")
        lines.append(f"  import_path={imp or '(not set)'}")
        lines.append(f"  ready={'yes' if exp and imp else 'no'}")
        return "\r\n".join(lines) + "\r\n"

    async def _cmd_show_db0sdx_direct(self, call: str, arg: str | None) -> str:
        return await self._show_gateway_status(call, arg, "show/db0sdx", "db0sdx")

    async def _cmd_show_ik3qar_direct(self, call: str, arg: str | None) -> str:
        return await self._show_gateway_status(call, arg, "show/ik3qar", "ik3qar")

    async def _cmd_show_wm7d_direct(self, call: str, arg: str | None) -> str:
        if not (arg and arg.strip()):
            return self._string("show.wm7d.usage", "Usage: show/wm7d <call>") + "\r\n"
        target = arg.split()[0].upper()
        try:
            result = await self._wm7d.lookup(target)
        except WM7DLookupError as exc:
            return f"{exc}\r\n"
        if result is None:
            return self._render_string("show.wm7d.empty", "{call}: no WM7D data returned.", call=target) + "\r\n"
        lines = [self._render_string("show.wm7d.title", "WM7D lookup for {call}:", call=result.callsign)]
        if result.license_class:
            lines.append(self._kv_line("Class", result.license_class))
        if result.name:
            lines.append(self._kv_line("Name", result.name))
        for idx, line in enumerate(result.address_lines, start=1):
            label = "Address" if idx == 1 else ""
            lines.append(self._kv_line(label, line))
        return await self._format_console_lines(call, lines)

    async def _cmd_show_dupann_direct(self, call: str, arg: str | None) -> str:
        return await self._show_session_pref(call, arg, "show/dupann", "dup_ann", "off")

    async def _cmd_show_dupeph_direct(self, call: str, arg: str | None) -> str:
        return await self._show_session_pref(call, arg, "show/dupeph", "dup_eph", "off")

    async def _cmd_show_dupspots_direct(self, call: str, arg: str | None) -> str:
        return await self._show_session_pref(call, arg, "show/dupspots", "dup_spots", "off")

    async def _cmd_show_dupwcy_direct(self, call: str, arg: str | None) -> str:
        return await self._show_session_pref(call, arg, "show/dupwcy", "dup_wcy", "off")

    async def _cmd_show_dupwwv_direct(self, call: str, arg: str | None) -> str:
        return await self._show_session_pref(call, arg, "show/dupwwv", "dup_wwv", "off")

    async def _coords_context_for(self, call: str) -> tuple[tuple[float, float], str] | None:
        prefs = await self._load_prefs_for_call(call)
        locator = (prefs.get("qra") or "").strip().upper()
        if not locator:
            reg = await self.store.get_user_registry(call)
            locator = str(reg["qra"] or "").strip().upper() if reg else ""
        locator_coords = self._locator_to_coords(locator) if locator else None
        location_text = str(prefs.get("location") or "").strip()
        location_source = str(prefs.get("location_source") or "").strip().lower()
        lat_s = (prefs.get("forward_lat") or "").strip()
        lon_s = (prefs.get("forward_lon") or "").strip()
        if lat_s and lon_s:
            try:
                lat = float(lat_s)
                lon = float(lon_s)
            except ValueError:
                lat = lon = None  # type: ignore[assignment]
            if lat is not None and lon is not None and -90 <= lat <= 90 and -180 <= lon <= 180:
                if location_text and location_source == "user":
                    return (lat, lon), f"location {location_text}"
                if locator and locator_coords is not None:
                    if abs(lat - locator_coords[0]) < 1e-3 and abs(lon - locator_coords[1]) < 1e-3:
                        return locator_coords, f"QRA {locator}"
                return (lat, lon), "forward latitude/longitude"

        if locator and locator_coords is not None:
            return locator_coords, f"QRA {locator}"

        node_prefs = node_presentation_defaults(self.config.node)
        pref_map = await self.store.list_user_prefs(self.config.node.node_call)
        for key in _CONFIG_AUTH_NODE_FIELDS:
            pref_map.pop(key, None)
        node_prefs.update(pref_map)
        node_locator = str(node_prefs.get("node_locator", self.config.node.node_locator)).strip().upper()
        if node_locator:
            coords = self._locator_to_coords(node_locator)
            if coords is not None:
                return coords, f"node grid square {node_locator}"
        return None

    async def _coords_for(self, call: str) -> tuple[float, float] | None:
        info = await self._coords_context_for(call)
        if info is None:
            return None
        return info[0]

    def _locator_to_coords(self, locator: str) -> tuple[float, float] | None:
        return locator_to_coords(locator)

    def _resolve_location_coords(self, text: str) -> tuple[float, float] | None:
        started = time.monotonic()
        try:
            coords = resolve_location_to_coords(text)
        except Exception:
            LOG.exception("location resolve failed call_text=%r", text)
            return None
        elapsed_ms = (time.monotonic() - started) * 1000.0
        if coords is None:
            LOG.info("location resolve miss text=%r elapsed_ms=%.1f", text, elapsed_ms)
            return None
        LOG.info("location resolve ok text=%r lat=%.4f lon=%.4f elapsed_ms=%.1f", text, coords[0], coords[1], elapsed_ms)
        return coords

    def _estimate_location_label(self, locator: str) -> str:
        started = time.monotonic()
        try:
            label = estimate_location_from_locator(locator)
        except Exception:
            LOG.exception("location estimate failed locator=%r", locator)
            return f"Grid {extract_locator(locator)}"
        elapsed_ms = (time.monotonic() - started) * 1000.0
        LOG.info("location estimate locator=%r label=%r elapsed_ms=%.1f", locator, label, elapsed_ms)
        return label

    async def _sync_locator_defaults(self, call: str, locator: str, *, overwrite_coords: bool = False) -> None:
        loc = extract_locator(locator)
        if not loc:
            return
        coords = locator_to_coords(loc)
        if coords is None:
            return
        now = int(datetime.now(timezone.utc).timestamp())
        await self.store.upsert_user_registry(call, now, qra=loc)
        if overwrite_coords:
            await self.store.set_user_pref(call, "forward_lat", f"{coords[0]:.4f}", now)
            await self.store.set_user_pref(call, "forward_lon", f"{coords[1]:.4f}", now)
            return
        cur_lat = await self.store.get_user_pref(call, "forward_lat")
        cur_lon = await self.store.get_user_pref(call, "forward_lon")
        if not str(cur_lat or "").strip() and not str(cur_lon or "").strip():
            await self.store.set_user_pref(call, "forward_lat", f"{coords[0]:.4f}", now)
            await self.store.set_user_pref(call, "forward_lon", f"{coords[1]:.4f}", now)

    async def _sync_location_defaults(self, call: str, text: str) -> str | None:
        coords = self._resolve_location_coords(text)
        if coords is None:
            return None
        lat, lon = coords
        loc = coords_to_locator(lat, lon)
        now = int(datetime.now(timezone.utc).timestamp())
        await self.store.set_user_pref(call, "qra", loc, now)
        await self.store.set_user_pref(call, "forward_lat", f"{lat:.4f}", now)
        await self.store.set_user_pref(call, "forward_lon", f"{lon:.4f}", now)
        await self.store.set_user_pref(call, "location_source", "user", now)
        await self.store.upsert_user_registry(call, now, qra=loc)
        s = self._find_session(call)
        if s:
            s.vars["qra"] = loc
            s.vars["forward_lat"] = f"{lat:.4f}"
            s.vars["forward_lon"] = f"{lon:.4f}"
            s.vars["location_source"] = "user"
        return loc

    async def _backfill_location_from_qra(self, call: str, locator: str) -> str | None:
        existing = await self.store.get_user_pref(call, "location")
        if str(existing or "").strip():
            return None
        label = self._estimate_location_label(locator)
        label = label.strip() or f"Grid {extract_locator(locator)}"
        now = int(datetime.now(timezone.utc).timestamp())
        await self.store.set_user_pref(call, "location", label, now)
        await self.store.set_user_pref(call, "location_source", "qra_estimate", now)
        s = self._find_session(call)
        if s:
            s.vars["location"] = label
            s.vars["location_source"] = "qra_estimate"
        return label

    def _solar_hour(self, now: datetime, lon: float) -> float:
        return (now.hour + now.minute / 60.0 + now.second / 3600.0 + lon / 15.0) % 24.0

    async def _cmd_show_sun(self, call: str) -> str:
        info = await self._coords_context_for(call)
        if not info:
            return self._string("show.sun.unavailable", "Sun: set your grid square or forward/latlong first.") + "\r\n"
        (lat, lon), source = info
        now = datetime.now(timezone.utc)
        sh = self._solar_hour(now, lon)
        phase = "day" if 6.0 <= sh < 18.0 else "night"
        lines = [
            self._string("show.sun.title", "Sun status:"),
            self._render_string("show.sun.reference", "  Reference: {source}", source=source),
            self._render_string("show.sun.latitude", "  Latitude: {lat:.4f}", lat=lat),
            self._render_string("show.sun.longitude", "  Longitude: {lon:.4f}", lon=lon),
            self._render_string("show.sun.solar_hour", "  Solar Hour: {hour:.2f}", hour=sh),
            self._render_string("show.sun.phase", "  Phase: {phase}", phase=phase),
        ]
        return await self._format_console_lines(call, lines)

    async def _cmd_show_grayline(self, call: str) -> str:
        info = await self._coords_context_for(call)
        if not info:
            return self._string("show.grayline.unavailable", "Grayline: set your grid square or forward/latlong first.") + "\r\n"
        (lat, lon), source = info
        now = datetime.now(timezone.utc)
        sh = self._solar_hour(now, lon)
        # Approximate transitions at 06:00 and 18:00 local solar time.
        to_sunrise = (6.0 - sh) % 24.0
        to_sunset = (18.0 - sh) % 24.0
        if to_sunrise <= to_sunset:
            nxt = f"sunrise in {to_sunrise:.2f}h"
        else:
            nxt = f"sunset in {to_sunset:.2f}h"
        lines = [
            self._string("show.grayline.title", "Grayline status:"),
            self._render_string("show.grayline.reference", "  Reference: {source}", source=source),
            self._render_string("show.grayline.latitude", "  Latitude: {lat:.4f}", lat=lat),
            self._render_string("show.grayline.longitude", "  Longitude: {lon:.4f}", lon=lon),
            self._render_string("show.grayline.next", "  Next Event: {event}", event=nxt),
        ]
        return await self._format_console_lines(call, lines)

    async def _cmd_show_moon(self, call: str) -> str:
        info = await self._coords_context_for(call)
        if not info:
            return self._string("show.moon.unavailable", "Moon: set your grid square or forward/latlong first.") + "\r\n"
        (lat, lon), source = info
        now_epoch = datetime.now(timezone.utc).timestamp()
        # Reference new moon near J2000: 2000-01-06 18:14 UTC.
        synodic = 29.53058867
        age = ((now_epoch - 947182440.0) / 86400.0) % synodic
        phase = age / synodic
        illum = 0.5 * (1 - math.cos(2 * math.pi * phase))
        lines = [
            self._string("show.moon.title", "Moon status:"),
            self._render_string("show.moon.reference", "  Reference: {source}", source=source),
            self._render_string("show.moon.latitude", "  Latitude: {lat:.4f}", lat=lat),
            self._render_string("show.moon.longitude", "  Longitude: {lon:.4f}", lon=lon),
            self._render_string("show.moon.age", "  Age: {age:.2f} days", age=age),
            self._render_string("show.moon.illumination", "  Illumination: {illumination:.1f}%", illumination=illum * 100),
        ]
        return await self._format_console_lines(call, lines)

    async def _cmd_show_muf(self, call: str, arg: str | None = None) -> str:
        toks = [t for t in (arg or "").split() if t]
        limit = 20
        long_form = False
        for tok in toks:
            tl = tok.lower()
            if tok.isdigit():
                limit = max(1, min(int(tok), 200))
                continue
            if tl in {"l", "long"}:
                long_form = True
                continue
        rows = await self.store.list_bulletins("wwv", limit=max(limit, 20))
        samples: list[tuple[sqlite3.Row, int, int | None, int | None, str]] = []
        for r in rows:
            text = str(r["body"] or "")
            sfi_m = re.search(r"\bSFI\s*=\s*(\d{2,3})\b", text, re.IGNORECASE)
            if not sfi_m:
                continue
            a_m = re.search(r"\bA\s*=\s*(\d{1,3})\b", text, re.IGNORECASE)
            k_m = re.search(r"\bK\s*=\s*(\d{1,3})\b", text, re.IGNORECASE)
            sfi = int(sfi_m.group(1))
            a = int(a_m.group(1)) if a_m else None
            k = int(k_m.group(1)) if k_m else None
            samples.append((r, sfi, a, k, text))
        if not samples:
            return self._string("show.muf.empty", "MUF estimate unavailable: no recent WWV SFI data has been received.") + "\r\n"
        latest = samples[0]
        latest_muf = 8.0 + 0.12 * latest[1]
        if not toks:
            lines = [
                self._string("show.muf.title", "MUF estimate:"),
                self._render_string("show.muf.sfi", "  SFI: {value}", value=latest[1]),
                self._render_string("show.muf.estimate", "  Estimated MUF3000: {value:.1f} MHz", value=latest_muf),
            ]
            return await self._format_console_lines(call, lines)

        lines: list[str] = []
        if long_form:
            lines.append(self._string("show.muf.long_header", "Date        Hour   SFI   A   K MUF3000 Forecast                               Logger"))
        else:
            lines.append(self._string("show.muf.header", "Date        Hour   SFI   A   K MUF3000"))
        for r, sfi, a, k, text in samples[:limit]:
            ts = datetime.fromtimestamp(int(r["epoch"]), tz=timezone.utc)
            base = (
                f"{ts.strftime('%-d-%b-%Y'):>11}   {ts.strftime('%H'):>2}"
                f"{sfi:>6}{(a if a is not None else 0):>4}{(k if k is not None else 0):>4}"
                f"{(8.0 + 0.12 * sfi):>8.1f}"
            )
            if long_form:
                forecast = text
                forecast = re.sub(r"^\s*SFI\s*=\s*\d+\s*", "", forecast, flags=re.IGNORECASE)
                forecast = re.sub(r"\bA\s*=\s*\d+\s*", "", forecast, flags=re.IGNORECASE)
                forecast = re.sub(r"\bK\s*=\s*\d+\s*", "", forecast, flags=re.IGNORECASE).strip()
                lines.append(f"{base} {forecast[:39]:<39} <{str(r['sender'] or '')}>")
            else:
                lines.append(base)
        return "\r\n".join(lines) + "\r\n"

    async def _cmd_show_relay(self, call: str, _arg: str | None) -> str:
        prefs = await self._load_prefs_for_call(call)
        route = prefs.get("routepc19", "off")
        cats = ("spots", "chat", "announce", "wcy", "wwv", "wx")
        lines = [
            self._render_string("show.relay.title", "Relay policy for {call}:", call=call.upper()),
            self._render_string("show.relay.route_pc19", "  Route PC19: {value}", value=route),
            self._render_string("show.relay.route_pc19_wire", "  routepc19={value}", value=route),
        ]
        for c in cats:
            key = f"relay.{c}"
            if key in prefs:
                lines.append(self._render_string("show.relay.user", "  {category}: {value} (user)", category=c.upper(), value=prefs[key]))
                lines.append(self._render_string("show.relay.user_wire", "  relay.{name}={value} (user)", name=c, value=prefs[key]))
            else:
                lines.append(self._render_string("show.relay.default", "  {category}: on (default)", category=c.upper()))
                lines.append(self._render_string("show.relay.default_wire", "  relay.{name}=on (default)", name=c))
        return await self._format_console_lines(call, lines)

    async def _cmd_show_relaypeer(self, call: str, arg: str | None) -> str:
        prefs = await self._load_prefs_for_call(call)
        toks = [t.strip() for t in (arg or "").split() if t.strip()]
        if toks:
            peer = toks[0]
            cats = ("spots", "chat", "announce", "wcy", "wwv", "wx")
            lines = [self._render_string("show.relaypeer.title", "Relay policy for peer {peer}:", peer=peer)]
            key_all = f"relay.peer.{peer}"
            if key_all in prefs:
                lines.append(self._render_string("show.relaypeer.all_user", "  ALL: {value} (user)", value=prefs[key_all]))
                lines.append(self._render_string("show.relaypeer.all_user_wire", "  all={value} (user)", value=prefs[key_all]))
            else:
                lines.append(self._string("show.relaypeer.all_default", "  ALL: on (default)"))
                lines.append(self._string("show.relaypeer.all_default_wire", "  all=on (default)"))
            for c in cats:
                key = f"relay.peer.{peer}.{c}"
                if key in prefs:
                    lines.append(self._render_string("show.relay.user", "  {category}: {value} (user)", category=c.upper(), value=prefs[key]))
                    lines.append(self._render_string("show.relaypeer.user_wire", "  {name}={value} (user)", name=c, value=prefs[key]))
                else:
                    lines.append(self._render_string("show.relay.default", "  {category}: on (default)", category=c.upper()))
                    lines.append(self._render_string("show.relaypeer.default_wire", "  {name}=on (default)", name=c))
            return await self._format_console_lines(call, lines)
        rows = sorted(k for k in prefs if k.startswith("relay.peer."))
        if not rows:
            return self._string("show.relaypeer.empty", "No per-peer relay overrides.") + "\r\n"
        lines = [self._render_string("show.relaypeer.list", "Per-peer relay overrides ({count}):", count=len(rows))]
        lines.extend(self._render_string("show.relaypeer.list_wire", "  {name}={value}", name=k, value=prefs[k]) for k in rows)
        return await self._format_console_lines(call, lines)

    async def _cmd_show_ingestpeer(self, call: str, arg: str | None) -> str:
        prefs = await self._load_prefs_for_call(call)
        toks = [t.strip() for t in (arg or "").split() if t.strip()]
        if toks:
            peer = toks[0]
            cats = ("spots", "chat", "announce", "wcy", "wwv", "wx")
            lines = [self._render_string("show.ingestpeer.title", "Ingest policy for peer {peer}:", peer=peer)]
            key_all = f"ingest.peer.{peer}"
            if key_all in prefs:
                lines.append(self._render_string("show.ingestpeer.all_user", "  ALL: {value} (user)", value=prefs[key_all]))
                lines.append(self._render_string("show.ingestpeer.all_user_wire", "  all={value} (user)", value=prefs[key_all]))
            else:
                lines.append(self._string("show.ingestpeer.all_default", "  ALL: on (default)"))
                lines.append(self._string("show.ingestpeer.all_default_wire", "  all=on (default)"))
            for c in cats:
                key = f"ingest.peer.{peer}.{c}"
                if key in prefs:
                    lines.append(self._render_string("show.relay.user", "  {category}: {value} (user)", category=c.upper(), value=prefs[key]))
                    lines.append(self._render_string("show.ingestpeer.user_wire", "  {name}={value} (user)", name=c, value=prefs[key]))
                else:
                    lines.append(self._render_string("show.relay.default", "  {category}: on (default)", category=c.upper()))
                    lines.append(self._render_string("show.ingestpeer.default_wire", "  {name}=on (default)", name=c))
            return await self._format_console_lines(call, lines)
        rows = sorted(k for k in prefs if k.startswith("ingest.peer."))
        if not rows:
            return self._string("show.ingestpeer.empty", "No per-peer ingest overrides.") + "\r\n"
        lines = [self._render_string("show.ingestpeer.list", "Per-peer ingest overrides ({count}):", count=len(rows))]
        lines.extend(self._render_string("show.ingestpeer.list_wire", "  {name}={value}", name=k, value=prefs[k]) for k in rows)
        return await self._format_console_lines(call, lines)

    async def _cmd_show_policy(self, call: str, _arg: str | None) -> str:
        prefs = await self._load_prefs_for_call(call)
        relay_cats = ("spots", "chat", "announce", "wcy", "wwv", "wx")
        ingest_cats = ("spots", "chat", "announce", "wcy", "wwv", "wx")
        lines = [self._render_string("show.policy.title", "Policy for {call}:", call=call.upper())]
        lines.append(self._render_string("show.policy.route_pc19", "  Route PC19: {value}", value=prefs.get("routepc19", "off")))
        lines.append(self._string("show.policy.relay", "  Relay:"))
        for c in relay_cats:
            k = f"relay.{c}"
            if k in prefs:
                lines.append(self._render_string("show.policy.user", "    {category}: {value} (user)", category=c.upper(), value=prefs[k]))
            else:
                lines.append(self._render_string("show.policy.default", "    {category}: on (default)", category=c.upper()))
        lines.append(self._string("show.policy.ingest", "  Ingest:"))
        for c in ingest_cats:
            lines.append(self._render_string("show.policy.peer_default", "    {category}: on (peer-policy default)", category=c.upper()))
        rpeers = sorted(k for k in prefs if k.startswith("relay.peer."))
        ipeers = sorted(k for k in prefs if k.startswith("ingest.peer."))
        lines.append(self._render_string("show.policy.relay_peer_overrides", "  Relay Peer Overrides: {count}", count=len(rpeers)))
        lines.append(self._render_string("show.policy.ingest_peer_overrides", "  Ingest Peer Overrides: {count}", count=len(ipeers)))
        for k in rpeers[:20]:
            lines.append(f"    {k}: {prefs[k]}")
        if len(rpeers) > 20:
            lines.append(self._render_string("show.policy.more", "    ... ({count} more)", count=len(rpeers) - 20))
        for k in ipeers[:20]:
            lines.append(f"    {k}: {prefs[k]}")
        if len(ipeers) > 20:
            lines.append(self._render_string("show.policy.more", "    ... ({count} more)", count=len(ipeers) - 20))
        return await self._format_console_lines(call, lines)

    async def _cmd_show_route(self, _call: str, _arg: str | None) -> str:
        if not self._link_stats_fn:
            return self._string("show.route.unavailable", "Route table unavailable (node-link not attached).") + "\r\n"
        stats = await self._link_stats_fn()
        if not stats:
            return self._string("show.route.empty", "No routes.") + "\r\n"
        node_proto = await self._node_proto_map()
        lines = [self._string("show.route.title", "Route/Peer table:")]
        for name in sorted(stats):
            st = stats[name]
            last_pc = st.get("last_pc_type") or "-"
            reasons_raw = st.get("policy_reasons") if isinstance(st, dict) else None
            reasons = reasons_raw if isinstance(reasons_raw, dict) else {}
            reason_txt = ""
            if reasons:
                top = sorted(reasons.items(), key=lambda kv: int(kv[1]), reverse=True)[:3]
                reason_txt = " reasons=" + ",".join(f"{k}:{int(v)}" for k, v in top)
            _, proto_txt, _ = self._proto_state_for_peer(node_proto, name)
            lines.append(self._render_string(
                "show.route.line",
                "{peer:<24} profile={profile:<10} in={inbound} rx={rx:>6} tx={tx:>6} last={last}{reasons}{proto}",
                peer=name,
                profile=st.get("profile", "dxspider"),
                inbound=int(bool(st.get("inbound", False))),
                rx=int(st.get("parsed_frames", 0)),
                tx=int(st.get("sent_frames", 0)),
                last=last_pc,
                reasons=reason_txt,
                proto=proto_txt,
            ))
        return await self._format_console_lines(_call, lines)

    async def _node_proto_map(self) -> dict[str, str]:
        prefs = await self.store.list_user_prefs(self.config.node.node_call)
        uvars = await self.store.list_user_vars(self.config.node.node_call)
        merged = dict(prefs)
        merged.update(uvars)
        return merged

    def _proto_threshold_spec(self) -> dict[str, tuple[int, int, int]]:
        return {
            "stale_mins": (1, 24 * 60, 30),
            "flap_score": (1, 999, 3),
            "flap_window_secs": (5, 86400, 300),
        }

    def _proto_peer_tag(self, peer_name: str) -> str:
        return re.sub(r"[^a-z0-9_.-]", "_", (peer_name or "").lower())

    def _proto_alert_ack_epoch(self, node_cfg: dict[str, str], peer_name: str) -> int:
        ptag = self._proto_peer_tag(peer_name)
        raw = node_cfg.get(f"proto.peer.{ptag}.alert_ack_epoch", "")
        try:
            return int(raw)
        except ValueError:
            return 0

    def _proto_thresholds(self, node_cfg: dict[str, str]) -> tuple[int, int]:
        spec = self._proto_threshold_spec()
        stale_mins = spec["stale_mins"][2]
        flap_score = spec["flap_score"][2]
        try:
            lo, hi, _d = spec["stale_mins"]
            stale_mins = max(lo, min(hi, int(node_cfg.get("proto.threshold.stale_mins", str(stale_mins)))))
        except ValueError:
            stale_mins = spec["stale_mins"][2]
        try:
            lo, hi, _d = spec["flap_score"]
            flap_score = max(lo, min(hi, int(node_cfg.get("proto.threshold.flap_score", str(flap_score)))))
        except ValueError:
            flap_score = spec["flap_score"][2]
        return stale_mins, flap_score

    async def _cmd_show_protoconfig(self, _call: str, _arg: str | None) -> str:
        cfg = await self._node_proto_map()
        spec = self._proto_threshold_spec()
        lines = [self._render_string("show.protoconfig.title", "Protocol thresholds for {node_call}:", node_call=self.config.node.node_call)]
        for k in ("stale_mins", "flap_score", "flap_window_secs"):
            pref_key = f"proto.threshold.{k}"
            if pref_key in cfg:
                v = cfg[pref_key]
                lines.append(self._render_string("show.protoconfig.node", "  {key}: {value} (node)", key=k, value=v))
                lines.append(self._render_string("show.protoconfig.node_wire", "  {key}={value} (node)", key=k, value=v))
            else:
                lines.append(self._render_string("show.protoconfig.default", "  {key}: {value} (default)", key=k, value=spec[k][2]))
                lines.append(self._render_string("show.protoconfig.default_wire", "  {key}={value} (default)", key=k, value=spec[k][2]))
        return await self._format_console_lines(_call, lines)

    async def _cmd_set_protothreshold(self, call: str, arg: str | None) -> str:
        denied = await self._require_privilege(call, 2, "set/protothreshold")
        if denied:
            return denied
        toks = [t for t in (arg or "").split() if t]
        if not toks:
            return self._string("set.protothreshold_usage", "Usage: set/protothreshold <stale_mins|flap_score|flap_window_secs> <value>") + "\r\n"
        key = ""
        value_txt = ""
        if "=" in toks[0]:
            key, value_txt = toks[0].split("=", 1)
            if len(toks) > 1:
                value_txt = value_txt + " " + " ".join(toks[1:])
        elif len(toks) >= 2:
            key = toks[0]
            value_txt = toks[1]
        else:
            return self._string("set.protothreshold_usage", "Usage: set/protothreshold <stale_mins|flap_score|flap_window_secs> <value>") + "\r\n"
        spec = self._proto_threshold_spec()
        key = key.strip().lower()
        if key.startswith("proto.threshold."):
            key = key[len("proto.threshold.") :]
        key = key.replace("-", "_")
        if key not in spec:
            kn = self._normalize_cmd_token(key)
            km = [k for k in spec if self._normalize_cmd_token(k) == kn]
            if len(km) == 1:
                key = km[0]
        if key not in spec:
            return self._string("set.protothreshold_usage", "Usage: set/protothreshold <stale_mins|flap_score|flap_window_secs> <value>") + "\r\n"
        try:
            n = int(value_txt.strip())
        except ValueError:
            return self._string("set.protothreshold_usage", "Usage: set/protothreshold <stale_mins|flap_score|flap_window_secs> <value>") + "\r\n"
        lo, hi, _d = spec[key]
        n = max(lo, min(hi, n))
        now = int(datetime.now(timezone.utc).timestamp())
        pref_key = f"proto.threshold.{key}"
        await self.store.set_user_pref(self.config.node.node_call, pref_key, str(n), now)
        label = key.replace("_", " ")
        return self._render_string("set.protothreshold_set", "Protocol threshold {label} set to {value}.", label=label, value=n) + "\r\n"

    async def _cmd_unset_protothreshold(self, call: str, arg: str | None) -> str:
        denied = await self._require_privilege(call, 2, "unset/protothreshold")
        if denied:
            return denied
        toks = [t for t in (arg or "").split() if t]
        spec = self._proto_threshold_spec()
        if not toks:
            return self._string("unset.protothreshold_usage", "Usage: unset/protothreshold <stale_mins|flap_score|flap_window_secs|all|a|*>") + "\r\n"
        key = toks[0].strip().lower()
        if self._is_all_token(key):
            removed = 0
            for k in spec:
                removed += await self.store.delete_user_pref(self.config.node.node_call, f"proto.threshold.{k}")
            return self._render_string("unset.protothreshold_all", "All protocol threshold overrides cleared ({removed} removed).", removed=removed) + "\r\n"
        if key.startswith("proto.threshold."):
            key = key[len("proto.threshold.") :]
        key = key.replace("-", "_")
        if key not in spec:
            kn = self._normalize_cmd_token(key)
            km = [k for k in spec if self._normalize_cmd_token(k) == kn]
            if len(km) == 1:
                key = km[0]
        if key not in spec:
            return self._string("unset.protothreshold_usage", "Usage: unset/protothreshold <stale_mins|flap_score|flap_window_secs|all|a|*>") + "\r\n"
        removed = await self.store.delete_user_pref(self.config.node.node_call, f"proto.threshold.{key}")
        if removed:
            return self._render_string("unset.protothreshold_one", "Protocol threshold {label} restored to default.", label=key.replace("_", " ")) + "\r\n"
        return self._render_string("unset.protothreshold_already", "Protocol threshold {label} was already using the default.", label=key.replace("_", " ")) + "\r\n"

    async def _cmd_set_protoack(self, call: str, arg: str | None) -> str:
        denied = await self._require_privilege(call, 2, "set/protoack")
        if denied:
            return denied
        toks = [t for t in (arg or "").split() if t]
        peer = ""
        all_peers = False
        for t in toks:
            tl = t.lower()
            if self._is_all_token(tl):
                all_peers = True
            elif not peer:
                peer = t
            else:
                return self._string("set.protoack_usage", "Usage: set/protoack <peer> | all|a|*") + "\r\n"
        if not peer and not all_peers:
            return self._string("set.protoack_requires", "set/protoack requires <peer> or all|a|*") + "\r\n"
        node_cfg = await self._node_proto_map()
        known: set[str] = set()
        for k in node_cfg:
            if k.startswith("proto.peer."):
                p = k[len("proto.peer.") :].split(".", 1)[0]
                if p:
                    known.add(p)
        if self._link_stats_fn:
            stats = await self._link_stats_fn()
            known.update(self._proto_peer_tag(p) for p in stats)
        targets = sorted(known) if all_peers else [self._proto_peer_tag(peer)]
        now = int(datetime.now(timezone.utc).timestamp())
        changed = 0
        for p in targets:
            await self.store.set_user_pref(
                self.config.node.node_call, f"proto.peer.{p}.alert_ack_epoch", str(now), now
            )
            changed += 1
        if all_peers:
            return self._render_string("set.protoack_all", "Protocol alerts acknowledged for {count} peer(s).", count=changed) + "\r\n"
        return self._render_string("set.protoack_one", "Protocol alerts acknowledged for {peer}.", peer=peer) + "\r\n"

    async def _cmd_unset_protoack(self, call: str, arg: str | None) -> str:
        denied = await self._require_privilege(call, 2, "unset/protoack")
        if denied:
            return denied
        toks = [t for t in (arg or "").split() if t]
        peer = ""
        all_peers = False
        for t in toks:
            tl = t.lower()
            if self._is_all_token(tl):
                all_peers = True
            elif not peer:
                peer = t
            else:
                return self._string("unset.protoack_usage", "Usage: unset/protoack <peer> | all|a|*") + "\r\n"
        if not peer and not all_peers:
            return self._string("unset.protoack_requires", "unset/protoack requires <peer> or all|a|*") + "\r\n"
        node_cfg = await self._node_proto_map()
        targets: set[str] = set()
        if all_peers:
            for k in node_cfg:
                if k.startswith("proto.peer.") and k.endswith(".alert_ack_epoch"):
                    p = k[len("proto.peer.") : -len(".alert_ack_epoch")]
                    if p:
                        targets.add(p)
        else:
            targets.add(self._proto_peer_tag(peer))
        removed = 0
        for p in sorted(targets):
            removed += await self.store.delete_user_pref(
                self.config.node.node_call, f"proto.peer.{p}.alert_ack_epoch"
            )
        if all_peers:
            return self._render_string("unset.protoack_all", "Cleared protocol alert acknowledgements for {count} peer(s).", count=removed) + "\r\n"
        if removed:
            return self._render_string("unset.protoack_one", "Protocol alert acknowledgement cleared for {peer}.", peer=peer) + "\r\n"
        return self._render_string("unset.protoack_missing", "No protocol alert acknowledgement was set for {peer}.", peer=peer) + "\r\n"

    def _proto_state_for_peer(self, node_cfg: dict[str, str], peer_name: str) -> tuple[dict[str, str], str, str]:
        ptag = re.sub(r"[^a-z0-9_.-]", "_", peer_name.lower())
        pfx = f"proto.peer.{ptag}."
        state = {
            "pc18_software": node_cfg.get(pfx + "pc18.software", ""),
            "pc18_proto": node_cfg.get(pfx + "pc18.proto", ""),
            "pc18_family": node_cfg.get(pfx + "pc18.family", ""),
            "pc18_summary": node_cfg.get(pfx + "pc18.summary", ""),
            "pc24_call": node_cfg.get(pfx + "pc24.call", ""),
            "pc24_flag": node_cfg.get(pfx + "pc24.flag", ""),
            "pc50_call": node_cfg.get(pfx + "pc50.call", ""),
            "pc50_count": node_cfg.get(pfx + "pc50.count", ""),
            "pc51_to": node_cfg.get(pfx + "pc51.to", ""),
            "pc51_from": node_cfg.get(pfx + "pc51.from", ""),
            "pc51_value": node_cfg.get(pfx + "pc51.value", ""),
            "last_epoch": node_cfg.get(pfx + "last_epoch", ""),
            "change_count": node_cfg.get(pfx + "change_count", ""),
            "flap_score": node_cfg.get(pfx + "flap_score", ""),
            "last_change_epoch": node_cfg.get(pfx + "last_change_epoch", ""),
            "history": node_cfg.get(pfx + "history", "[]"),
        }
        proto_bits: list[str] = []
        if state["pc18_summary"]:
            proto_bits.append(f"pc18={state['pc18_summary']}")
        if state["pc24_call"] or state["pc24_flag"]:
            proto_bits.append(f"pc24={state['pc24_call']}:{state['pc24_flag']}")
        if state["pc50_call"] or state["pc50_count"]:
            proto_bits.append(f"pc50={state['pc50_call']}:{state['pc50_count']}")
        if state["pc51_to"] or state["pc51_from"] or state["pc51_value"]:
            proto_bits.append(f"pc51={state['pc51_to']}>{state['pc51_from']}:{state['pc51_value']}")
        proto_txt = f" proto={'|'.join(proto_bits)}" if proto_bits else ""
        health = "unknown"
        if proto_bits:
            health = "ok"
            if state["pc51_value"] and state["pc51_value"] in {"0", "off", "down", "fail"}:
                health = "degraded"
            if state["pc50_count"]:
                try:
                    if int(state["pc50_count"]) <= 0:
                        health = "degraded"
                except ValueError:
                    health = "degraded"
            try:
                _stale_mins_unused, flap_threshold = self._proto_thresholds(node_cfg)
                flap_window_secs = 300
                try:
                    flap_window_secs = max(5, int(str(node_cfg.get("proto.threshold.flap_window_secs", "300"))))
                except ValueError:
                    flap_window_secs = 300
                last_change_epoch = int(state["last_change_epoch"] or "0") if str(state["last_change_epoch"]).isdigit() else 0
                flap_active = last_change_epoch > 0 and (int(time.time()) - last_change_epoch) <= flap_window_secs
                if flap_active and int(state["flap_score"] or "0") >= flap_threshold:
                    health = "flapping"
            except ValueError:
                health = "degraded"
        return state, proto_txt, health

    def _parse_proto_history(self, raw: str) -> list[dict[str, object]]:
        try:
            obj = json.loads(raw or "[]")
        except Exception:
            return []
        if not isinstance(obj, list):
            return []
        out: list[dict[str, object]] = []
        for it in obj:
            if not isinstance(it, dict):
                continue
            try:
                epoch = int(it.get("epoch", 0))
            except Exception:
                epoch = 0
            key = str(it.get("key", "")).strip()
            fr = str(it.get("from", ""))
            to = str(it.get("to", ""))
            if not key:
                continue
            out.append({"epoch": epoch, "key": key, "from": fr, "to": to})
        return out

    def _collect_proto_events(
        self, node_cfg: dict[str, str], peer_filter: str = "", limit: int = 50
    ) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        flt = peer_filter.lower().strip()
        for k, raw in node_cfg.items():
            if not (k.startswith("proto.peer.") and k.endswith(".history")):
                continue
            ptag = k[len("proto.peer.") : -len(".history")]
            if flt and flt not in ptag.lower():
                continue
            for ev in self._parse_proto_history(raw):
                rows.append(
                    {
                        "peer": ptag,
                        "epoch": int(ev.get("epoch", 0) or 0),
                        "key": str(ev.get("key", "")),
                        "from": str(ev.get("from", "")),
                        "to": str(ev.get("to", "")),
                    }
                )
        rows.sort(key=lambda r: int(r.get("epoch", 0)), reverse=True)
        return rows[: max(1, min(limit, 200))]

    async def _cmd_show_proto(self, _call: str, arg: str | None) -> str:
        if not self._link_stats_fn:
            return self._string("show.proto.unavailable", "Protocol table unavailable (node-link not attached).") + "\r\n"
        stats = await self._link_stats_fn()
        if not stats:
            return self._string("show.proto.empty", "No protocol peer data.") + "\r\n"
        node_proto = await self._node_proto_map()
        toks = [t for t in (arg or "").split() if t]
        peer_filter = ""
        stale_mins_default, _flap_threshold = self._proto_thresholds(node_proto)
        stale_secs = stale_mins_default * 60
        with_history = False
        history_limit = 5
        i = 0
        while i < len(toks):
            t = toks[i].lower()
            if t in {"--stale-mins", "--stale", "stale"}:
                if i + 1 >= len(toks):
                    return self._string("show.proto.usage", "Usage: show/proto [peer] [--stale-mins <minutes>] [--history] [--history-limit <n>]") + "\r\n"
                try:
                    mins = int(toks[i + 1])
                except ValueError:
                    return self._string("show.proto.usage", "Usage: show/proto [peer] [--stale-mins <minutes>] [--history] [--history-limit <n>]") + "\r\n"
                stale_secs = max(0, mins * 60)
                i += 2
                continue
            if t in {"--history", "history", "-h"}:
                with_history = True
                i += 1
                continue
            if t in {"--history-limit", "--hlimit", "hlimit"}:
                if i + 1 >= len(toks):
                    return self._string("show.proto.usage", "Usage: show/proto [peer] [--stale-mins <minutes>] [--history] [--history-limit <n>]") + "\r\n"
                try:
                    history_limit = max(1, min(50, int(toks[i + 1])))
                except ValueError:
                    return self._string("show.proto.usage", "Usage: show/proto [peer] [--stale-mins <minutes>] [--history] [--history-limit <n>]") + "\r\n"
                i += 2
                continue
            if not peer_filter:
                peer_filter = t
                i += 1
                continue
            return self._string("show.proto.usage", "Usage: show/proto [peer] [--stale-mins <minutes>] [--history] [--history-limit <n>]") + "\r\n"
        lines = [self._string("show.proto.title", "Protocol peer state:")]
        found = 0
        now = int(datetime.now(timezone.utc).timestamp())
        for name in sorted(stats):
            if peer_filter and peer_filter not in name.lower():
                continue
            st = stats[name]
            state, _, health = self._proto_state_for_peer(node_proto, name)
            if not any(state[k] for k in ("pc24_call", "pc24_flag", "pc50_call", "pc50_count", "pc51_to", "pc51_from", "pc51_value")):
                continue
            found += 1
            last_epoch = int(state["last_epoch"]) if state["last_epoch"].isdigit() else 0
            last_seen = self._fmt_epoch_short(last_epoch) if last_epoch > 0 else "-"
            age_mins = ((now - last_epoch) // 60) if last_epoch > 0 else -1
            stale = (last_epoch <= 0) or ((now - last_epoch) > stale_secs)
            health_txt = "stale" if stale else health
            change_count = int(state["change_count"]) if state["change_count"].isdigit() else 0
            flap_score = int(state["flap_score"]) if state["flap_score"].isdigit() else 0
            last_chg = int(state["last_change_epoch"]) if state["last_change_epoch"].isdigit() else 0
            last_chg_txt = self._fmt_epoch_short(last_chg) if last_chg > 0 else "-"
            lines.append(self._render_string(
                "show.proto.peer",
                "{peer:<24} health={health:<8} profile={profile:<10} last={last} age_min={age} changes={changes} flap={flap} last_change={last_change}",
                peer=name,
                health=health_txt,
                profile=st.get("profile", "dxspider"),
                last=last_seen,
                age=age_mins if age_mins >= 0 else "-",
                changes=change_count,
                flap=flap_score,
                last_change=last_chg_txt,
            ))
            lines.append(self._render_string("show.proto.pc24", "  PC24  Call: {call}  Flag: {flag}", call=state["pc24_call"] or "-", flag=state["pc24_flag"] or "-"))
            lines.append(self._render_string("show.proto.pc50", "  PC50  Call: {call}  Nodes: {count}", call=state["pc50_call"] or "-", count=state["pc50_count"] or "-"))
            lines.append(self._render_string("show.proto.pc51", "  PC51  To: {to}  From: {from_call}  Value: {value}", to=state["pc51_to"] or "-", from_call=state["pc51_from"] or "-", value=state["pc51_value"] or "-"))
            if with_history:
                hist = self._parse_proto_history(state.get("history", "[]"))
                if not hist:
                    lines.append(self._string("show.proto.history_none", "  history: (none)"))
                else:
                    lines.append(self._string("show.proto.history_title", "  history:"))
                    for ev in hist[-history_limit:]:
                        ep = int(ev.get("epoch", 0) or 0)
                        ttxt = self._fmt_epoch_short(ep) if ep > 0 else "-"
                        lines.append(self._render_string(
                            "show.proto.history_line",
                            "    {when} {field} {from_call} -> {to}",
                            when=ttxt,
                            field=ev.get("key", ""),
                            from_call=ev.get("from", ""),
                            to=ev.get("to", ""),
                        ))
        if found == 0:
            if peer_filter:
                return self._render_string("show.proto.filter_empty", "No protocol peer data for filter '{peer_filter}'", peer_filter=peer_filter) + "\r\n"
            return self._string("show.proto.empty", "No protocol peer data.") + "\r\n"
        return await self._format_console_lines(_call, lines)

    async def _cmd_show_protoevents(self, _call: str, arg: str | None) -> str:
        node_cfg = await self._node_proto_map()
        toks = [t for t in (arg or "").split() if t]
        peer_filter = ""
        key_filter = ""
        since_mins = 0
        limit = 20
        i = 0
        while i < len(toks):
            t = toks[i]
            tl = t.lower()
            if tl in {"--key", "key"}:
                if i + 1 >= len(toks):
                    return self._string("show.protoevents.usage", "Usage: show/protoevents [peer] [limit] [--key <field>] [--since <minutes>]") + "\r\n"
                key_filter = toks[i + 1].strip().lower()
                i += 2
                continue
            if tl in {"--since", "since"}:
                if i + 1 >= len(toks):
                    return self._string("show.protoevents.usage", "Usage: show/protoevents [peer] [limit] [--key <field>] [--since <minutes>]") + "\r\n"
                try:
                    since_mins = max(0, int(toks[i + 1]))
                except ValueError:
                    return self._string("show.protoevents.usage", "Usage: show/protoevents [peer] [limit] [--key <field>] [--since <minutes>]") + "\r\n"
                i += 2
                continue
            if t.isdigit():
                limit = max(1, min(200, int(t)))
                i += 1
                continue
            if not peer_filter:
                peer_filter = t
                i += 1
                continue
            return self._string("show.protoevents.usage", "Usage: show/protoevents [peer] [limit] [--key <field>] [--since <minutes>]") + "\r\n"
        rows = self._collect_proto_events(node_cfg, peer_filter=peer_filter, limit=limit)
        if key_filter:
            rows = [r for r in rows if key_filter in str(r.get("key", "")).lower()]
        if since_mins > 0:
            cutoff = int(datetime.now(timezone.utc).timestamp()) - since_mins * 60
            rows = [r for r in rows if int(r.get("epoch", 0) or 0) >= cutoff]
        if not rows:
            if peer_filter:
                return self._render_string("show.protoevents.filter_empty", "No proto history events for filter '{peer_filter}'", peer_filter=peer_filter) + "\r\n"
            return self._string("show.protoevents.empty", "No protocol history events.") + "\r\n"
        lines = [self._render_string("show.protoevents.title", "Protocol history events ({count}):", count=len(rows))]
        for r in rows:
            ep = int(r.get("epoch", 0) or 0)
            ttxt = self._fmt_epoch_short(ep) if ep > 0 else "-"
            lines.append(self._render_string("show.protoevents.line", "{when} {peer}  {field}  {from_call} -> {to}", when=ttxt, peer=r.get("peer", ""), field=r.get("key", ""), from_call=r.get("from", ""), to=r.get("to", "")))
        return await self._format_console_lines(_call, lines)

    async def _cmd_show_protoalerts(self, _call: str, arg: str | None) -> str:
        if not self._link_stats_fn:
            return self._string("show.protoalerts.unavailable", "No protocol alerts (node-link not attached).") + "\r\n"
        stats = await self._link_stats_fn()
        if not stats:
            return self._string("show.protoalerts.empty", "No protocol alerts.") + "\r\n"
        node_cfg = await self._node_proto_map()
        stale_mins_default, _ = self._proto_thresholds(node_cfg)
        stale_mins = stale_mins_default
        peer_filter = ""
        include_acked = False
        toks = [t for t in (arg or "").split() if t]
        i = 0
        while i < len(toks):
            tl = toks[i].lower()
            if tl in {"--all", "all", "a", "*", "--include-acked", "acked", "+a"}:
                include_acked = True
                i += 1
                continue
            if tl in {"--stale-mins", "--stale", "stale"}:
                if i + 1 >= len(toks):
                    return self._string("show.protoalerts.usage", "Usage: show/protoalerts [peer] [--stale-mins <minutes>]") + "\r\n"
                try:
                    stale_mins = max(1, min(24 * 60, int(toks[i + 1])))
                except ValueError:
                    return self._string("show.protoalerts.usage", "Usage: show/protoalerts [peer] [--stale-mins <minutes>]") + "\r\n"
                i += 2
                continue
            if not peer_filter:
                peer_filter = toks[i]
                i += 1
                continue
            return self._string("show.protoalerts.usage", "Usage: show/protoalerts [peer] [--stale-mins <minutes>]") + "\r\n"
        now = int(datetime.now(timezone.utc).timestamp())
        out: list[str] = []
        for peer in sorted(stats):
            if peer_filter and peer_filter.lower() not in peer.lower():
                continue
            st = stats[peer]
            state, _proto_txt, health = self._proto_state_for_peer(node_cfg, peer)
            has_state = any(
                state[k] for k in ("pc24_call", "pc24_flag", "pc50_call", "pc50_count", "pc51_to", "pc51_from", "pc51_value")
            )
            if not has_state:
                continue
            last_epoch = int(state["last_epoch"]) if state["last_epoch"].isdigit() else 0
            if last_epoch <= 0 or now - last_epoch > stale_mins * 60:
                health = "stale"
            ack_epoch = self._proto_alert_ack_epoch(node_cfg, peer)
            suppressed = ack_epoch > 0 and last_epoch > 0 and ack_epoch >= last_epoch
            if suppressed and not include_acked:
                continue
            if suppressed:
                health = "acked"
            if health not in {"degraded", "flapping", "stale", "acked"}:
                continue
            age = ((now - last_epoch) // 60) if last_epoch > 0 else -1
            flap = int(state["flap_score"]) if state["flap_score"].isdigit() else 0
            reasons: list[str] = []
            if health == "degraded":
                if state["pc51_value"] and state["pc51_value"] in {"0", "off", "down", "fail"}:
                    reasons.append("pc51_value")
                if state["pc50_count"]:
                    try:
                        if int(state["pc50_count"]) <= 0:
                            reasons.append("pc50_count")
                    except ValueError:
                        reasons.append("pc50_count")
            if health == "flapping":
                reasons.append(f"flap={flap}")
            if health == "stale":
                reasons.append(f"age={age if age >= 0 else '-'}m")
            if health == "acked":
                reasons.append(f"ack_epoch={ack_epoch}")
            rs = ",".join(reasons) if reasons else "-"
            out.append(
                f"{peer:<24} health={health:<8} profile={st.get('profile','dxspider'):<10} reasons={rs}"
            )
        if not out:
            if peer_filter:
                return self._render_string("show.protoalerts.filter_empty", "No proto alerts for filter '{peer_filter}'", peer_filter=peer_filter) + "\r\n"
            return self._string("show.protoalerts.empty", "No protocol alerts.") + "\r\n"
        return await self._format_console_lines(_call, [self._string("show.protoalerts.title", "Protocol alerts:")] + out)

    async def _cmd_show_protoacks(self, _call: str, arg: str | None) -> str:
        node_cfg = await self._node_proto_map()
        peer_filter = (arg or "").strip().lower()
        now = int(datetime.now(timezone.utc).timestamp())
        lines: list[str] = []
        for k, v in sorted(node_cfg.items()):
            if not (k.startswith("proto.peer.") and k.endswith(".alert_ack_epoch")):
                continue
            ptag = k[len("proto.peer.") : -len(".alert_ack_epoch")]
            if peer_filter and peer_filter not in ptag.lower():
                continue
            try:
                ack_epoch = int(v)
            except ValueError:
                continue
            if ack_epoch <= 0:
                continue
            last_raw = node_cfg.get(f"proto.peer.{ptag}.last_epoch", "0")
            try:
                last_epoch = int(last_raw)
            except ValueError:
                last_epoch = 0
            suppressed = ack_epoch >= last_epoch and last_epoch > 0
            age_min = (now - ack_epoch) // 60 if ack_epoch > 0 else -1
            lines.append(self._render_string(
                "show.protoacks.line",
                "{peer:<24} ack={ack} age_min={age_min} last={last} suppressed={suppressed}",
                peer=ptag,
                ack=self._fmt_epoch_short(ack_epoch),
                age_min=age_min if age_min >= 0 else "-",
                last=self._fmt_epoch_short(last_epoch) if last_epoch > 0 else "-",
                suppressed=1 if suppressed else 0,
            ))
        if not lines:
            if peer_filter:
                return self._render_string("show.protoacks.filter_empty", "No proto acks for filter '{peer_filter}'", peer_filter=peer_filter) + "\r\n"
            return self._string("show.protoacks.empty", "No protocol alert acknowledgements.") + "\r\n" + self._string("show.protoacks.legacy_empty", "Legacy clients: No proto acks") + "\r\n"
        return await self._format_console_lines(_call, [self._string("show.protoacks.title", "Protocol alert acknowledgements:")] + lines)

    async def _cmd_clear_protohistory(self, call: str, arg: str | None) -> str:
        denied = await self._require_privilege(call, 2, "clear/protohistory")
        if denied:
            return denied
        toks = [t for t in (arg or "").split() if t]
        allow_all = False
        peer = ""
        for t in toks:
            tl = t.lower()
            if self._is_all_token(tl):
                allow_all = True
            elif not peer:
                peer = t
            else:
                return self._string("clear.protohistory_usage", "Usage: clear/protohistory <peer> | all|a|*") + "\r\n"
        if not peer and not allow_all:
            return self._string("clear.protohistory_requires", "clear/protohistory requires <peer> or all|a|*") + "\r\n"
        prefs = await self.store.list_user_prefs(self.config.node.node_call)
        pfilter = peer.lower()
        pfilter_s = re.sub(r"[^a-z0-9_.-]", "_", pfilter)
        if peer:
            keys = []
            for k in prefs:
                if not k.startswith("proto.peer."):
                    continue
                if not (
                    k.endswith(".history")
                    or ".change." in k
                    or k.endswith(".change_count")
                    or k.endswith(".flap_score")
                    or k.endswith(".last_change_epoch")
                ):
                    continue
                ptag = k[len("proto.peer.") :].split(".", 1)[0]
                if pfilter in ptag or pfilter_s in ptag:
                    keys.append(k)
        else:
            keys = [
                k
                for k in prefs
                if k.startswith("proto.peer.")
                and (
                    k.endswith(".history")
                    or ".change." in k
                    or k.endswith(".change_count")
                    or k.endswith(".flap_score")
                    or k.endswith(".last_change_epoch")
                )
            ]
        removed = 0
        for k in keys:
            removed += await self.store.delete_user_pref(self.config.node.node_call, k)
        if peer:
            noun = "entry" if removed == 1 else "entries"
            return self._render_string("clear.protohistory_peer", "Cleared {count} protocol history {noun} for {peer}.", count=removed, noun=noun, peer=peer) + "\r\n"
        noun = "entry" if removed == 1 else "entries"
        return self._render_string("clear.protohistory_all", "Cleared {count} protocol history {noun}.", count=removed, noun=noun) + "\r\n"

    async def _cmd_show_policydrop(self, _call: str, _arg: str | None) -> str:
        if not self._link_stats_fn:
            return self._string("show.policydrop.unavailable", "Policy drop table unavailable (node-link not attached)") + "\r\n"
        toks = [t for t in (_arg or "").split() if t]
        do_reset = False
        allow_all = False
        rest: list[str] = []
        for t in toks:
            tl = t.lower()
            if tl in {"--reset", "reset"}:
                do_reset = True
            elif self._is_all_token(tl):
                allow_all = True
            else:
                rest.append(t)
        peer_filter_raw = rest[0] if rest else ""
        peer_filter = peer_filter_raw.lower()
        if do_reset:
            denied = await self._require_privilege(_call, 2, "show/policydrop --reset")
            if denied:
                return denied
            if not self._link_clear_policy_fn:
                return self._string("show.policydrop.reset_unavailable", "Policy drop reset unavailable (node-link not attached)") + "\r\n"
            if not peer_filter_raw and not allow_all:
                return self._string("show.policydrop.reset_requires", "show/policydrop --reset requires <peer> or all|a|*") + "\r\n"
            cleared = await self._link_clear_policy_fn(peer_filter_raw or None)
            if peer_filter_raw:
                return self._render_string("show.policydrop.reset_filtered", "policydrop reset peers={count} filter={peer_filter}", count=cleared, peer_filter=peer_filter_raw) + "\r\n"
            return self._render_string("show.policydrop.reset", "policydrop reset peers={count}", count=cleared) + "\r\n"
        stats = await self._link_stats_fn()
        if not stats:
            return self._string("show.policydrop.empty", "No policy drop data") + "\r\n"
        lines = [self._string("show.policydrop.title", "Policy drop reasons:")]
        found = 0
        for name in sorted(stats):
            if peer_filter and peer_filter not in name.lower():
                continue
            st = stats[name]
            total = int(st.get("policy_dropped", 0))
            reasons_raw = st.get("policy_reasons") if isinstance(st, dict) else None
            reasons = reasons_raw if isinstance(reasons_raw, dict) else {}
            if total <= 0 and not reasons:
                continue
            found += 1
            lines.append(self._render_string("show.policydrop.peer", "{peer}: total={total}", peer=name, total=total))
            if reasons:
                for k, v in sorted(reasons.items(), key=lambda kv: (-int(kv[1]), kv[0])):
                    lines.append(self._render_string("show.policydrop.reason", "  {name}={count}", name=k, count=int(v)))
            else:
                lines.append(self._string("show.policydrop.no_breakdown", "  (no reason breakdown)"))
        if found == 0:
            if peer_filter:
                return self._render_string("show.policydrop.filter_empty", "No policy drop data for peer filter '{peer_filter}'", peer_filter=peer_filter) + "\r\n"
            return self._string("show.policydrop.empty", "No policy drop data") + "\r\n"
        return "\r\n".join(lines) + "\r\n"

    async def _cmd_show_hops(self, _call: str, _arg: str | None) -> str:
        if not self._link_stats_fn:
            return self._string("show.hops.empty", "No hop data") + "\r\n"
        stats = await self._link_stats_fn()
        if not stats:
            return self._string("show.hops.empty", "No hop data") + "\r\n"
        lines = []
        for name in sorted(stats):
            parsed = int(stats[name].get("parsed_frames", 0))
            sent = int(stats[name].get("sent_frames", 0))
            policy = int(stats[name].get("policy_dropped", 0))
            hop = max(1, min(99, parsed // 100 + 1))
            lines.append(f"{name:<24} hop_metric={hop:>2} rx={parsed:>6} tx={sent:>6} policy_drop={policy:>4}")
        return "\r\n".join(lines) + "\r\n"

    async def _cmd_show_files(self, _call: str, _arg: str | None) -> str:
        return (
            f"db={self.config.store.sqlite_path}\r\n"
            "bulletins=sqlite\r\n"
            "user_prefs=sqlite\r\n"
            "runtime_logs=in-memory\r\n"
            "filters=sqlite+cache\r\n"
        )

    async def _event_report(self, call: str, category: str, limit: int) -> str:
        lines = []
        rows = sorted(await self.store.list_bulletins(category, limit=limit), key=lambda r: (int(r["epoch"]), int(r["id"])), reverse=True)
        if category == "wcy":
            table: list[str] = []
            for r in rows:
                sender = str(r["sender"] or "")
                body = str(r["body"] or "")
                if not await self._text_family_passes_filters(call, category, sender, body):
                    continue
                m = re.match(
                    r"^SFI=(?P<sfi>\d+)\s+A=(?P<a>\d+)\s+K=(?P<k>\d+)\s+spots=(?P<spots>\d+)\s+expk=(?P<expk>\d+)\s+aurora=(?P<aurora>\S+)\s+xray=(?P<xray>\S+)\s+storm=(?P<storm>\S+)$",
                    body,
                    re.IGNORECASE,
                )
                if not m:
                    continue
                ts = datetime.fromtimestamp(int(r["epoch"]), tz=timezone.utc)
                table.append(
                    f"{ts.strftime('%-d-%b-%Y'):>11}   {ts.strftime('%H'):>2}"
                    f"{int(m.group('sfi')):>6}{int(m.group('a')):>4}{int(m.group('k')):>4}{int(m.group('expk')):>6}"
                    f"{int(m.group('spots')):>4} {m.group('aurora')[:5]:<5} {m.group('xray')[:5]:<5}"
                    f"{m.group('storm')[:8]:>8} <{sender}>"
                )
            if table:
                return (
                    self._string("show.wcy.header", "Date        Hour   SFI   A   K Exp.K   R SA    GMF   Aurora   Logger")
                    + "\r\n"
                    + "\r\n".join(table)
                    + "\r\n"
                )
        if category == "wwv":
            table = []
            for r in rows:
                sender = str(r["sender"] or "")
                body = str(r["body"] or "")
                if not await self._text_family_passes_filters(call, category, sender, body):
                    continue
                m = re.match(r"^SFI=(?P<sfi>\d+)\s+A=(?P<a>\d+)\s+K=(?P<k>\d+)\s+(?P<forecast>.+)$", body, re.IGNORECASE)
                if not m:
                    continue
                ts = datetime.fromtimestamp(int(r["epoch"]), tz=timezone.utc)
                table.append(
                    f"{ts.strftime('%-d-%b-%Y'):>11}   {ts.strftime('%H'):>2}"
                    f"{int(m.group('sfi')):>6}{int(m.group('a')):>4}{int(m.group('k')):>4} "
                    f"{m.group('forecast')[:39]:<39} <{sender}>"
                )
            if table:
                return (
                    self._string("show.wwv.header", "Date        Hour   SFI   A   K Forecast                               Logger")
                    + "\r\n"
                    + "\r\n".join(table)
                    + "\r\n"
                )
        for r in rows:
            sender = str(r["sender"] or "")
            body = str(r["body"] or "")
            if category in {"announce", "wcy", "wwv"}:
                if not await self._text_family_passes_filters(call, category, sender, body):
                    continue
            ts = datetime.fromtimestamp(int(r["epoch"]), tz=timezone.utc).strftime("%-d-%b-%Y %H%MZ")
            scope = str(r["scope"] or "").strip().upper()
            prefix = f"[{scope}] " if scope and scope != "LOCAL" else ""
            lines.append(f"{ts} {prefix}{sender}: {body}")

        if not lines:
            mem_rows = [e for e in self._events if e.category == category][-limit:]
            for e in mem_rows:
                ts = datetime.fromtimestamp(e.epoch, tz=timezone.utc).strftime("%-d-%b-%Y %H%MZ")
                lines.append(f"{ts} {e.text}")

        if not lines:
            return self._render_string(f"show.{category}.empty", f"No {category.upper()} entries", category=category.upper()) + "\r\n"
        return "\r\n".join(lines) + "\r\n"

    async def _cmd_show_announce(self, _call: str, arg: str | None) -> str:
        explicit = bool(arg and arg.split()[0].isdigit())
        limit = 20
        if explicit:
            limit = max(1, min(int(arg.split()[0]), 200))
        else:
            page = await self._page_size_for(_call)
            if page > 0:
                limit = min(limit, page)
        return await self._event_report(_call, "announce", limit)

    async def _cmd_show_chat(self, _call: str, arg: str | None) -> str:
        explicit = bool(arg and arg.split()[0].isdigit())
        limit = 20
        if explicit:
            limit = max(1, min(int(arg.split()[0]), 200))
        else:
            page = await self._page_size_for(_call)
            if page > 0:
                limit = min(limit, page)
        return await self._event_report(_call, "chat", limit)

    async def _cmd_show_wcy(self, _call: str, arg: str | None) -> str:
        explicit = bool(arg and arg.split()[0].isdigit())
        limit = 20
        if explicit:
            limit = max(1, min(int(arg.split()[0]), 200))
        else:
            page = await self._page_size_for(_call)
            if page > 0:
                limit = min(limit, page)
        return await self._event_report(_call, "wcy", limit)

    async def _cmd_show_wwv(self, _call: str, arg: str | None) -> str:
        explicit = bool(arg and arg.split()[0].isdigit())
        limit = 20
        if explicit:
            limit = max(1, min(int(arg.split()[0]), 200))
        else:
            page = await self._page_size_for(_call)
            if page > 0:
                limit = min(limit, page)
        return await self._event_report(_call, "wwv", limit)

    async def _cmd_show_wx(self, _call: str, arg: str | None) -> str:
        explicit = bool(arg and arg.split()[0].isdigit())
        limit = 20
        if explicit:
            limit = max(1, min(int(arg.split()[0]), 200))
        else:
            page = await self._page_size_for(_call)
            if page > 0:
                limit = min(limit, page)
        return await self._event_report(_call, "wx", limit)

    async def _cmd_show_log(self, _call: str, arg: str | None) -> str:
        explicit = False
        limit = 20
        category: str | None = None
        if arg:
            toks = [t.strip().lower() for t in arg.split() if t.strip()]
            if toks:
                if toks[0].isdigit():
                    explicit = True
                    limit = max(1, min(int(toks[0]), 200))
                else:
                    category = toks[0]
                    if len(toks) > 1 and toks[1].isdigit():
                        explicit = True
                        limit = max(1, min(int(toks[1]), 200))
        if not explicit:
            page = await self._page_size_for(_call)
            if page > 0:
                # The page limit applies to the total visible lines, including the title.
                limit = min(limit, max(1, page - 1))
        categories = {category} if category else None
        title = "Recent log events"
        if category:
            title += f" ({category})"
        return self._format_event_rows(limit, categories=categories, title=title)

    async def _cmd_sysop_audit(self, call: str, arg: str | None) -> str:
        denied = await self._require_privilege(call, 2, "sysop/audit")
        if denied:
            return denied
        limit = 20
        category: str | None = None
        toks = [t.strip().lower() for t in (arg or "").split() if t.strip()]
        if toks:
            if toks[0].isdigit():
                limit = max(1, min(int(toks[0]), 200))
                if len(toks) > 1:
                    category = toks[1]
            else:
                category = toks[0]
                if len(toks) > 1 and toks[1].isdigit():
                    limit = max(1, min(int(toks[1]), 200))
        allowed = {"sysop", "control", "db", "config", "connect", "disconnect", "forward", "pc", "user"}
        if category and category not in allowed:
            return self._string("sysop.audit_usage", "Usage: sysop/audit [<category>] [<limit>]") + "\r\n"
        categories = {category} if category else allowed
        title = self._string("sysop.audit_title", "System Operator Audit")
        if category:
            title += self._render_string("sysop.audit_title_suffix", " ({category})", category=category)
        return self._format_event_rows(limit, categories=categories, title=title)

    async def _cmd_show_deny_list(self, _call: str, arg: str | None, kind: str) -> str:
        explicit = bool(arg and arg.split()[0].isdigit())
        limit = 200
        if explicit:
            limit = max(1, min(int(arg.split()[0]), 500))
        else:
            page = await self._page_size_for(_call)
            if page > 0:
                limit = min(limit, page)
        rows = await self.store.list_deny_rules(kind)
        if not rows:
            return f"{kind}: (none)\r\n"
        lines = [f"{kind} rules ({len(rows)}):"]
        for i, p in enumerate(rows[:limit], start=1):
            lines.append(f"{i:>3} {p}")
        return "\r\n".join(lines) + "\r\n"

    async def _cmd_show_buddy(self, call: str, arg: str | None) -> str:
        target = call.upper()
        if arg and arg.strip():
            target = arg.split()[0].upper()
        explicit = bool(arg and arg.strip())
        rows = await self.store.list_buddies(target)
        if not rows:
            return self._render_string("show.buddy.empty", "No buddy entries for {target}.", target=target) + "\r\n"
        if not explicit:
            page = await self._page_size_for(call)
            if page > 0:
                rows = rows[:page]
        lines = [self._render_string("show.buddy.title", "Buddy list for {target} ({count}):", target=target, count=len(rows))]
        for i, b in enumerate(rows, start=1):
            lines.append(f"{i:>3} {b}")
        return await self._format_console_lines(call, lines)

    async def _cmd_show_usdb(self, call: str, arg: str | None) -> str:
        target = call.upper()
        if arg and arg.strip():
            tok = arg.split()[0].upper()
            if is_valid_call(tok):
                target = tok
        rows = await self.store.list_usdb_entries(target)
        if not rows:
            return self._render_string("show.usdb.empty", "USDB has no entries stored locally for {target}.", target=target) + "\r\n"
        items = list(rows.items())
        if not arg:
            page = await self._page_size_for(call)
            if page > 0:
                items = items[:page]
        lines = [self._render_string("show.usdb.title", "USDB entries for {target} ({count}):", target=target, count=len(items))]
        for k, v in items:
            lines.append(f"  {k}: {v}")
        return await self._format_console_lines(call, lines)

    async def _cmd_show_station(self, call: str, arg: str | None) -> str:
        target = call.upper()
        if arg and arg.strip():
            tok = arg.split()[0].upper()
            if is_valid_call(tok):
                target = tok

        vars_map = dict(self._session_vars(target))
        pref_map = await self._load_prefs_for_call(target)
        vars_map.update(pref_map)
        reg = await self.store.get_user_registry(target)
        usdb = await self.store.list_usdb_entries(target)
        uvars = await self.store.list_user_vars(target)

        lines = [self._render_string("show.station.title", "Station profile for {target}:", target=target)]
        field_map = {
            "name": str(reg["display_name"]) if reg else vars_map.get("name", ""),
            "address": str(reg["address"]) if reg else vars_map.get("address", ""),
            "qth": str(reg["qth"]) if reg else vars_map.get("qth", ""),
            "qra": str(reg["qra"]) if reg else vars_map.get("qra", ""),
            "email": str(reg["email"]) if reg else vars_map.get("email", ""),
            "location": vars_map.get("location", ""),
            "usstate": vars_map.get("usstate", ""),
        }
        labels = {
            "name": "Name",
            "address": "Address",
            "qth": "Location (QTH)",
            "qra": "Grid Square (QRA)",
            "email": "Email",
            "location": "Location Detail",
            "usstate": "US State",
        }
        for key in ("name", "address", "qth", "qra", "email", "location", "usstate"):
            val = str(field_map.get(key) or "").strip()
            if val:
                lines.append(self._render_string(f"show.station.{key}", f"  {labels[key]}: {{value}}", value=val))
                lines.append(f"  {key}={val}")
        for k, v in usdb.items():
            lines.append(self._render_string("show.station.usdb", "  USDB {key}: {value}", key=k, value=v))
        for k, v in sorted(uvars.items()):
            if k.startswith("uservar."):
                lines.append(self._render_string("show.station.uservar", "  {key}: {value}", key=k, value=v))
                lines.append(f"  {k}={v}")
        if len(lines) == 1:
            lines.append(self._string("show.station.empty", "  No station profile details are stored."))
        return await self._format_console_lines(call, lines)

    async def _cmd_show_var(self, call: str, arg: str | None) -> str:
        target = call.upper()
        key: str | None = None
        toks = [t for t in (arg or "").split() if t]
        if toks:
            if len(toks) >= 2 and is_valid_call(toks[0].upper()):
                target = toks[0].upper()
                key = toks[1].lower()
            elif is_valid_call(toks[0].upper()):
                target = toks[0].upper()
            else:
                key = toks[0].lower()

        rows = await self.store.list_user_vars(target)
        if key:
            if key in rows:
                return f"Variable {key} for {target}: {rows[key]}\r\n{key}={rows[key]}\r\n"
            return self._render_string("show.var.item_empty", "Variable {key} for {target}: (none)", key=key, target=target) + "\r\n"
        if not rows:
            return self._render_string("show.var.empty", "No variables stored for {target}.", target=target) + "\r\n"
        items = list(rows.items())
        if not arg:
            page = await self._page_size_for(call)
            if page > 0:
                items = items[:page]
        lines = [self._render_string("show.var.title", "Variables for {target} ({count}):", target=target, count=len(items))]
        for k, v in items:
            lines.append(f"  {k}: {v}")
            lines.append(f"  {k}={v}")
        return await self._format_console_lines(call, lines)

    async def _cmd_show_registered(self, _call: str, arg: str | None) -> str:
        sess = self._find_session(_call)
        sess_login = (sess.vars.get("logininfo", "off") if sess else "off").lower() in {"1", "on", "yes", "true"}
        pref_login = (await self._get_pref(_call, "logininfo") or "off").lower() in {"1", "on", "yes", "true"}
        show_logininfo = sess_login or pref_login
        toks = [t for t in (arg or "").split() if t]
        if toks:
            target = toks[0].upper()
            if not is_valid_call(target):
                return self._string("show.registered.usage", "Usage: show/registered [<call>]") + "\r\n"
            row = await self.store.get_user_registry(target)
            if not row:
                return (
                    self._render_string("show.registered.item_empty", "Registered user {target}: (none)", target=target) + "\r\n"
                    + f"registered {target}\r\n"
                )
            prefs = await self._load_prefs_for_call(target)
            uvars = await self.store.list_user_vars(target)
            blocked = False
            for candidate in {target, normalize_call(target)}:
                raw_block = await self.store.get_user_pref(candidate, "blocked_login")
                if raw_block is not None and self._is_on_value(str(raw_block)):
                    blocked = True
                    break
            lines = [
                f"Registered user {row['call']}",
                f"registered {row['call']}",
                f"  Name: {row['display_name'] or '(none)'}",
                f"  Address: {row['address'] or '(none)'}",
                f"  Location (QTH): {row['qth'] or '(none)'}",
                f"  Grid Square (QRA): {row['qra'] or '(none)'}",
                f"  Email: {row['email'] or '(none)'}",
                f"  Privilege: {row['privilege'] or 'user'}",
                f"  Home BBS: {prefs.get('homebbs', '') or '(none)'}",
                f"  Home Node: {prefs.get('homenode', '') or '(none)'}",
                f"  Node Family: {prefs.get('node_family', '') or '(user login)'}",
                f"  Login Access: {'blocked' if blocked else 'allowed'}",
                f"  homebbs={prefs.get('homebbs', '') or '(none)'}",
                f"  homenode={prefs.get('homenode', '') or '(none)'}",
                f"  node={prefs.get('node', '') or '(none)'}",
                f"  name={row['display_name'] or ''}",
                f"  address={row['address'] or ''}",
                f"  qth={row['qth'] or ''}",
                f"  qra={row['qra'] or ''}",
                f"  email={row['email'] or ''}",
            ]
            for k, v in sorted(uvars.items()):
                if k.startswith("uservar."):
                    lines.append(f"  {k}: {v}")
                    lines.append(f"  {k}={v}")
            if show_logininfo:
                lines.append(f"  Last Login: {self._fmt_epoch_short(int(row['last_login_epoch'] or 0))}")
                lines.append(f"  Last Peer: {normalize_recorded_path(str(row['last_login_peer'] or '')) or '(none)'}")
            return await self._format_console_lines(_call, lines)
        rows = await self.store.list_user_registry(limit=500)
        if not rows:
            return self._string("show.registered.empty", "registered: (none)") + "\r\n"
        if not arg:
            page = await self._page_size_for(_call)
            if page > 0:
                rows = rows[:page]
        lines = [self._render_string("show.registered.title", "Registered users ({count}):", count=len(rows)), self._string("show.registered.header", "Callsign   Privilege  Home Node   Name")]
        for r in rows:
            name = str(r["display_name"] or "")
            privilege = str(r["privilege"] or "user")
            home_node = str(r["home_node"] or "")[:10]
            line = f"{r['call']:<10} {privilege:<10} {home_node:<10} {name}"
            if show_logininfo:
                line = f"{line[:61]:<61} {self._fmt_epoch_short(int(r['last_login_epoch'] or 0))}"
            lines.append(line[:80])
        return "\r\n".join(lines) + "\r\n"

    async def _sysop_access_matrix_lines(self, target: str) -> list[str]:
        channels = self._access_channels()
        caps = self._access_capabilities()
        login_marks = [
            ("T" if await self._access_allowed(target, "telnet", "login") else "-"),
            ("W" if await self._access_allowed(target, "web", "login") else "-"),
        ]
        post_labels: list[str] = []
        for capability, short in (
            ("spots", "DX"),
            ("chat", "CH"),
            ("announce", "AN"),
            ("wx", "WX"),
            ("wcy", "WCY"),
            ("wwv", "WWV"),
        ):
            enabled = False
            for ch in channels:
                if await self._access_allowed(target, ch, capability):
                    enabled = True
                    break
            if enabled:
                post_labels.append(short)
        post_summary = " ".join(post_labels) if post_labels else "-"
        lines = [
            f"Access for {target}",
            f"Login channels: {' '.join(login_marks)}",
            f"Posting: {post_summary}",
            f"{'':<10}{channels[0]:>8}{channels[1]:>6}",
        ]
        for capability in caps:
            states = ["on" if await self._access_allowed(target, ch, capability) else "off" for ch in channels]
            lines.append(f"{capability:<10}{states[0]:>8}{states[1]:>6}")
        return lines

    async def _cmd_set_flag(self, call: str, arg: str | None, flag: str, value: bool) -> str:
        s = self._find_session(call)
        if not s:
            return self._string("set.session_not_found", "Session not found") + "\r\n"
        normalized = "on" if value else "off"
        if hasattr(s, flag):
            setattr(s, flag, value)
        s.vars[flag] = normalized
        await self._persist_pref(call, flag, normalized)
        if flag == "nowrap":
            return (
                self._string("set.nowrap_on", "Line wrapping disabled for this session.") + "\r\n"
                if value
                else self._string("set.nowrap_off", "Line wrapping restored to the default width.") + "\r\n"
            )
        if flag in {"echo", "talk", "here", "beep"}:
            return self._render_string("set.wire", "{name}={value}", name=flag, value=normalized) + "\r\n"
        return self._render_string("set.display_set", "{label} set to {value} for {call}.", label=self._display_label(flag), value=normalized, call=call) + "\r\n"

    async def _cmd_set_page(self, call: str, arg: str | None) -> str:
        s = self._find_session(call)
        if not s:
            return self._string("set.session_not_found", "Session not found") + "\r\n"
        val = 20
        if arg and arg.strip():
            tok = arg.split()[0]
            if not tok.isdigit():
                return self._string("set.page_usage", "Usage: set/page [<lines 1..500>]") + "\r\n"
            val = max(1, min(int(tok), 500))
        s.vars["page_size"] = str(val)
        await self._persist_pref(call, "page_size", str(val))
        await self._persist_pref(call, "page", str(val))
        return self._render_string("set.page_set", "Page length set to {value} lines for {call}.", value=val, call=call) + "\r\n"

    async def _cmd_set_maxconnect(self, call: str, arg: str | None) -> str:
        if not arg or not arg.strip():
            return self._string("set.maxconnect_usage", "Usage: set/maxconnect [<call>] <0..100>") + "\r\n"
        toks = [t for t in arg.split() if t]
        target = call.upper()
        idx = 0
        if len(toks) >= 2 and is_valid_call(toks[0].upper()):
            target = toks[0].upper()
            idx = 1
        tok = toks[idx]
        if not tok.isdigit():
            return self._string("set.maxconnect_usage", "Usage: set/maxconnect [<call>] <0..100>") + "\r\n"
        if target != call.upper():
            denied = await self._require_privilege(call, 2, "set/maxconnect")
            if denied:
                return denied
        val = max(0, min(int(tok), 100))
        s = self._find_session(target)
        if s and target == call.upper():
            s.vars["maxconnect"] = str(val)
        await self.store.set_user_pref(target, "maxconnect", str(val), int(datetime.now(timezone.utc).timestamp()))
        return self._render_string("set.maxconnect_set", "Maximum connections for {target} set to {value}.", target=target, value=val) + "\r\n"

    async def _cmd_set_language(self, call: str, arg: str | None) -> str:
        s = self._find_session(call)
        if not s:
            return self._string("set.session_not_found", "Session not found") + "\r\n"
        lang = (arg or "").strip().lower()
        if not lang:
            return self._string("set.language_usage", "Usage: set/language <en|de|fr|es>") + "\r\n"
        s.language = lang
        await self._persist_pref(call, "language", s.language)
        return self._render_string("set.language_set", "Language set to {language} for {call}.", language=s.language, call=call) + "\r\n"

    async def _cmd_set_named_var(self, call: str, arg: str | None, name: str, default: str = "on") -> str:
        s = self._find_session(call)
        if not s:
            return self._string("set.session_not_found", "Session not found") + "\r\n"
        value = default
        if arg and arg.strip():
            value = arg.strip()
        low = value.lower()
        bool_names = {
            "announce",
            "anntalk",
            "dx",
            "dxcq",
            "dxitu",
            "dxgrid",
            "rbn",
            "talk",
            "wcy",
            "wwv",
            "wx",
            "debug",
            "isolate",
            "lockout",
            "prompt",
            "register",
            "local_node",
            "ve7cc",
            "wantpc16",
            "wantpc9x",
            "sendpc16",
            "routepc19",
            "send_dbg",
            "agwengine",
            "agwmonitor",
            "bbs",
            "believe",
            "gtk",
            "hops",
            "logininfo",
            "dup_ann",
            "dup_eph",
            "dup_spots",
            "dup_wcy",
            "dup_wwv",
        }
        if name in bool_names:
            if low in {"1", "on", "yes", "true"}:
                value = "on"
            elif low in {"0", "off", "no", "false"}:
                value = "off"
            elif not (arg and arg.strip()):
                value = "on"
        if name in {"pinginterval", "obscount"}:
            try:
                n = int(value)
            except ValueError:
                return self._render_string("set.integer_usage", "Usage: set/{name} <integer>", name=name) + "\r\n"
            if name == "pinginterval":
                n = max(5, min(3600, n))
            else:
                n = max(0, min(9999, n))
            value = str(n)
        if name in {"qra", "qth", "location", "name", "sys_location", "sys_qra"}:
            value = value[:128]
        if name in {"qra", "sys_qra"}:
            value = value.upper()
        if name == "password":
            value = hash_password(value)
        s.vars[name] = value
        await self._persist_pref(call, name, value)
        if name in {"name", "qth", "qra"}:
            now = int(datetime.now(timezone.utc).timestamp())
            if name == "name":
                await self.store.upsert_user_registry(call, now, display_name=value)
            elif name == "qth":
                await self.store.upsert_user_registry(call, now, qth=value)
            elif name == "qra":
                await self.store.upsert_user_registry(call, now, qra=value)
                await self._sync_locator_defaults(call, value, overwrite_coords=False)
                await self._backfill_location_from_qra(call, value)
        elif name == "location":
            now = int(datetime.now(timezone.utc).timestamp())
            await self.store.set_user_pref(call, "location_source", "user", now)
            if s:
                s.vars["location_source"] = "user"
            loc = await self._sync_location_defaults(call, value)
            if loc and s:
                s.vars["qra"] = loc
        self._log_event("set", f"{call} {name}={value}")
        if name == "password":
            return self._render_string("set.password_updated", "Password updated for {call}.", call=call) + "\r\n"
        if name == "qra":
            return self._render_string("set.qra_set", "QRA set to {value} for {call}.", value=value, call=call) + "\r\n"
        if name == "sys_qra":
            return self._render_string("set.sys_qra_set", "System QRA set to {value} for {call}.", value=value, call=call) + "\r\n"
        if name in {"talk", "debug", "rcmd", "beep", "here", "echo", "qth", "name", "page", "obscount", "pinginterval"}:
            return self._render_string("set.wire", "{name}={value}", name=name, value=value) + "\r\n"
        if name in bool_names:
            return self._render_string("set.display_set_wire", "{label} set to {value} for {call}.\n{name}={value}", label=self._display_label(name), value=value, call=call, name=name).replace("\n", "\r\n") + "\r\n"
        return self._render_string("set.display_set", "{label} set to {value} for {call}.", label=self._display_label(name), value=value, call=call) + "\r\n"

    async def _cmd_set_relay(self, call: str, arg: str | None) -> str:
        toks = [t.strip().lower() for t in (arg or "").split() if t.strip()]
        if not toks:
            return self._string("set.relay_usage", "Usage: set/relay <spots|chat|announce|wcy|wwv|wx|all|a|*> [on|off]") + "\r\n"
        cat = toks[0]
        val = (toks[1] if len(toks) >= 2 else "on").lower()
        if val not in {"on", "off"}:
            return self._string("set.relay_usage", "Usage: set/relay <spots|chat|announce|wcy|wwv|wx|all|a|*> [on|off]") + "\r\n"
        now = int(datetime.now(timezone.utc).timestamp())
        cats = ("spots", "chat", "announce", "wcy", "wwv", "wx")
        if self._is_all_token(cat):
            for c in cats:
                await self.store.set_user_pref(call.upper(), f"relay.{c}", val, now)
            return self._render_string("set.relay_all", "Relay policy set to {value} for all traffic.", value=val) + "\r\n"
        if cat not in cats:
            return self._string("set.relay_usage", "Usage: set/relay <spots|chat|announce|wcy|wwv|wx|all|a|*> [on|off]") + "\r\n"
        await self.store.set_user_pref(call.upper(), f"relay.{cat}", val, now)
        return self._render_string("set.relay_one", "Relay policy for {category} set to {value}.", category=cat, value=val) + "\r\n"

    async def _cmd_set_relaypeer(self, call: str, arg: str | None) -> str:
        toks = [t.strip().lower() for t in (arg or "").split() if t.strip()]
        if len(toks) < 2:
            return self._string("set.relaypeer_usage", "Usage: set/relaypeer <peer> [spots|chat|announce|wcy|wwv|wx|all|a|*] <on|off>") + "\r\n"
        peer = toks[0]
        cats = ("spots", "chat", "announce", "wcy", "wwv", "wx")
        if toks[-1] not in {"on", "off"}:
            return self._string("set.relaypeer_usage", "Usage: set/relaypeer <peer> [spots|chat|announce|wcy|wwv|wx|all|a|*] <on|off>") + "\r\n"
        val = toks[-1]
        cat = toks[1] if len(toks) >= 3 else "all"
        all_cat = self._is_all_token(cat)
        if not all_cat and cat not in cats:
            return self._string("set.relaypeer_usage", "Usage: set/relaypeer <peer> [spots|chat|announce|wcy|wwv|wx|all|a|*] <on|off>") + "\r\n"
        key = f"relay.peer.{peer}" if all_cat else f"relay.peer.{peer}.{cat}"
        now = int(datetime.now(timezone.utc).timestamp())
        await self.store.set_user_pref(call.upper(), key, val, now)
        scope = "all traffic" if all_cat else cat
        return self._render_string("set.relaypeer_set", "Relay policy for {peer} {scope} set to {value}.", peer=peer, scope=scope, value=val) + "\r\n"

    async def _cmd_set_ingestpeer(self, call: str, arg: str | None) -> str:
        toks = [t.strip().lower() for t in (arg or "").split() if t.strip()]
        if len(toks) < 2:
            return self._string("set.ingestpeer_usage", "Usage: set/ingestpeer <peer> [spots|chat|announce|wcy|wwv|wx|all|a|*] <on|off>") + "\r\n"
        peer = toks[0]
        cats = ("spots", "chat", "announce", "wcy", "wwv", "wx")
        if toks[-1] not in {"on", "off"}:
            return self._string("set.ingestpeer_usage", "Usage: set/ingestpeer <peer> [spots|chat|announce|wcy|wwv|wx|all|a|*] <on|off>") + "\r\n"
        val = toks[-1]
        cat = toks[1] if len(toks) >= 3 else "all"
        all_cat = self._is_all_token(cat)
        if not all_cat and cat not in cats:
            return self._string("set.ingestpeer_usage", "Usage: set/ingestpeer <peer> [spots|chat|announce|wcy|wwv|wx|all|a|*] <on|off>") + "\r\n"
        key = f"ingest.peer.{peer}" if all_cat else f"ingest.peer.{peer}.{cat}"
        now = int(datetime.now(timezone.utc).timestamp())
        await self.store.set_user_pref(call.upper(), key, val, now)
        scope = "all traffic" if all_cat else cat
        return self._render_string("set.ingestpeer_set", "Ingest policy for {peer} {scope} set to {value}.", peer=peer, scope=scope, value=val) + "\r\n"

    async def _cmd_unset_named_var(self, call: str, _arg: str | None, name: str, off: str = "off") -> str:
        s = self._find_session(call)
        if not s:
            return self._string("set.session_not_found", "Session not found") + "\r\n"
        s.vars[name] = off
        await self._persist_pref(call, name, off)
        self._log_event("unset", f"{call} {name}={off}")
        if name in {"talk", "debug", "rcmd", "beep", "here", "echo"}:
            return self._render_string("unset.wire", "{name}={value}", name=name, value=off) + "\r\n"
        if name in {
            "announce", "anntalk", "dx", "dxcq", "dxitu", "dxgrid", "rbn", "wcy", "wwv", "wx",
            "isolate", "lockout", "prompt", "register", "local_node", "ve7cc", "wantpc16",
            "wantpc9x", "sendpc16", "routepc19", "send_dbg", "agwengine", "agwmonitor",
            "bbs", "believe", "gtk", "hops", "logininfo", "dup_ann", "dup_eph", "dup_spots",
            "dup_wcy", "dup_wwv",
        }:
            return self._render_string("unset.display_set_wire", "{label} set to {value} for {call}.\n{name}={value}", label=self._display_label(name), value=off, call=call, name=name).replace("\n", "\r\n") + "\r\n"
        return self._render_string("unset.display_set", "{label} set to {value} for {call}.", label=self._display_label(name), value=off, call=call) + "\r\n"

    async def _cmd_unset_relay(self, call: str, arg: str | None) -> str:
        toks = [t.strip().lower() for t in (arg or "").split() if t.strip()]
        cat = toks[0] if toks else "all"
        cats = ("spots", "chat", "announce", "wcy", "wwv", "wx")
        if self._is_all_token(cat):
            removed = 0
            for c in cats:
                removed += await self.store.delete_user_pref(call, f"relay.{c}")
            return self._render_string("unset.relay_all", "Relay policy restored to defaults for all traffic ({removed} removed).", removed=removed) + "\r\n"
        if cat not in cats:
            return self._string("unset.relay_usage", "Usage: unset/relay [spots|chat|announce|wcy|wwv|wx|all|a|*]") + "\r\n"
        removed = await self.store.delete_user_pref(call, f"relay.{cat}")
        return self._render_string("unset.relay_one", "Relay policy for {category} restored to default ({removed} removed).", category=cat, removed=removed) + "\r\n"

    async def _cmd_unset_relaypeer(self, call: str, arg: str | None) -> str:
        toks = [t.strip().lower() for t in (arg or "").split() if t.strip()]
        if not toks:
            return self._string("unset.relaypeer_usage", "Usage: unset/relaypeer <peer> [spots|chat|announce|wcy|wwv|wx|all|a|*]") + "\r\n"
        peer = toks[0]
        cats = ("spots", "chat", "announce", "wcy", "wwv", "wx")
        cat = toks[1] if len(toks) >= 2 else "all"
        if not self._is_all_token(cat) and cat not in cats:
            return self._string("unset.relaypeer_usage", "Usage: unset/relaypeer <peer> [spots|chat|announce|wcy|wwv|wx|all|a|*]") + "\r\n"
        if self._is_all_token(cat):
            removed = await self._clear_pref_prefix(call, f"relay.peer.{peer}")
            return self._render_string("unset.relaypeer_all", "Relay policy for {peer} restored to defaults ({removed} removed).", peer=peer, removed=removed) + "\r\n"
        removed = await self.store.delete_user_pref(call, f"relay.peer.{peer}.{cat}")
        return self._render_string("unset.relaypeer_one", "Relay policy for {peer} {category} restored to default ({removed} removed).", peer=peer, category=cat, removed=removed) + "\r\n"

    async def _cmd_unset_ingestpeer(self, call: str, arg: str | None) -> str:
        toks = [t.strip().lower() for t in (arg or "").split() if t.strip()]
        if not toks:
            return self._string("unset.ingestpeer_usage", "Usage: unset/ingestpeer <peer> [spots|chat|announce|wcy|wwv|wx|all|a|*]") + "\r\n"
        peer = toks[0]
        cats = ("spots", "chat", "announce", "wcy", "wwv", "wx")
        cat = toks[1] if len(toks) >= 2 else "all"
        if not self._is_all_token(cat) and cat not in cats:
            return self._string("unset.ingestpeer_usage", "Usage: unset/ingestpeer <peer> [spots|chat|announce|wcy|wwv|wx|all|a|*]") + "\r\n"
        if self._is_all_token(cat):
            removed = await self._clear_pref_prefix(call, f"ingest.peer.{peer}")
            return self._render_string("unset.ingestpeer_all", "Ingest policy for {peer} restored to defaults ({removed} removed).", peer=peer, removed=removed) + "\r\n"
        removed = await self.store.delete_user_pref(call, f"ingest.peer.{peer}.{cat}")
        return self._render_string("unset.ingestpeer_one", "Ingest policy for {peer} {category} restored to default ({removed} removed).", peer=peer, category=cat, removed=removed) + "\r\n"

    async def _cmd_set_profile(self, call: str, _arg: str | None, profile: str) -> str:
        p = normalize_profile(profile)
        display = "spider" if p == "dxspider" else p
        # If arg is provided and link callback exists, treat it as peer name.
        if _arg and self._link_set_profile_fn:
            peer = _arg.strip()
            if peer:
                ok = await self._link_set_profile_fn(peer, p)
                if ok:
                    return f"Profile for peer {peer} set to {display}.\r\n"
                return f"Peer {peer} was not found.\r\n"
        s = self._find_session(call)
        if not s:
            return "Session not found\r\n"
        s.peer_profile = display
        await self._persist_pref(call, "profile", p)
        self._log_event("profile", f"{call} -> {p}")
        return f"Profile for {call} set to {display}.\r\n"

    async def _cmd_not_implemented(self, _call: str, _arg: str | None) -> str:
        return "Command recognized but not implemented yet\r\n"

    async def _cmd_compat_ok(self, _call: str, _arg: str | None, name: str) -> str:
        if name == "keplerian elements request":
            return "Keplerian elements request accepted.\r\nget/keps: Ok\r\n"
        label = name.replace("_", " ").replace("/", " ").strip()
        return f"{label.capitalize()} completed.\r\n"

    async def _cmd_compat_disabled(self, _call: str, _arg: str | None, name: str) -> str:
        return f"{name}: disabled in pyCluster for safety\r\n"

    async def _cmd_filter_add(self, call: str, arg: str | None, family: str, action: str) -> str:
        parsed = self._parse_filter_target_slot_expr(call, arg)
        if not parsed:
            return f"Usage: {action}/{family} [<call>] [input] [0-9] <pattern>\r\n"
        target, slot, expr = parsed
        now = int(datetime.now(timezone.utc).timestamp())
        await self.store.set_filter_rule(target, family, action, slot, expr, now)
        rules = self._ensure_filter_store(target, family, action)
        rules[:] = [r for r in rules if r.slot != slot]
        rules.append(FilterRule(slot=slot, expr=expr))
        self._log_event("filter", f"{action}/{family} {target} {slot} {expr}")
        family_label = family.upper() if family in {"wcy", "wwv", "wx"} else family
        return f"{action.capitalize()} filter for {family_label} saved for {target} in slot {slot}.\r\n"

    async def _cmd_filter_alias_expr(self, call: str, arg: str | None, family: str, action: str, expr: str, label: str) -> str:
        parsed = self._parse_filter_target_and_slot(call, arg)
        if not parsed:
            return f"Usage: {action}/{label} [<call>] [input] [<slot>|all]\r\n"
        target, slot = parsed
        slot_num = 1 if slot == "all" else int(slot)
        now = int(datetime.now(timezone.utc).timestamp())
        await self.store.set_filter_rule(target, family, action, slot_num, expr, now)
        rules = self._ensure_filter_store(target, family, action)
        rules[:] = [r for r in rules if r.slot != slot_num]
        rules.append(FilterRule(slot=slot_num, expr=expr))
        self._log_event("filter", f"{action}/{label} {target} {slot_num} {expr}")
        family_label = family.upper() if family in {"wcy", "wwv", "wx"} else family
        return (
            f"{action.capitalize()} filter for {family_label} saved for {target} in slot {slot_num} ({label}).\r\n"
            f"{action}/{label} {target} {slot_num}\r\n"
        )

    async def _cmd_set_bad_rule(self, call: str, arg: str | None, kind: str) -> str:
        if not arg or not arg.strip():
            return f"Usage: set/{kind} <pattern>\r\n"
        pat = arg.strip()
        now = int(datetime.now(timezone.utc).timestamp())
        await self.store.add_deny_rule(kind, pat, now)
        self._log_event("badrule", f"{call} set/{kind} {pat}")
        rule_name = kind.replace("bad", "blocked ")
        return f"{rule_name.capitalize()} rule added: {pat}\r\n"

    async def _cmd_set_buddy(self, call: str, arg: str | None) -> str:
        if not arg or not arg.strip():
            return self._string("set.buddy_usage", "Usage: set/buddy <call> [call ...]") + "\r\n"
        toks = [t.upper() for t in arg.split() if t]
        now = int(datetime.now(timezone.utc).timestamp())
        added = 0
        for b in toks:
            if not is_valid_call(b):
                continue
            await self.store.add_buddy(call, b, now)
            added += 1
        self._log_event("buddy", f"{call} set/buddy {','.join(toks)} added={added}")
        noun = "entry" if added == 1 else "entries"
        return self._render_string("set.buddy_added", "Added {count} buddy {noun} for {call}.", count=added, noun=noun, call=call) + "\r\n"

    async def _cmd_set_usdb(self, call: str, arg: str | None) -> str:
        if not arg or not arg.strip():
            return self._string("set.usdb_usage", "Usage: set/usdb [<call>] <field> <value>") + "\r\n"
        toks = [t for t in arg.split() if t]
        if len(toks) < 2:
            return self._string("set.usdb_usage", "Usage: set/usdb [<call>] <field> <value>") + "\r\n"
        target = call.upper()
        field_idx = 0
        value_idx = 1
        if len(toks) >= 3 and is_valid_call(toks[0].upper()):
            target = toks[0].upper()
            field_idx = 1
            value_idx = 2
        field = toks[field_idx].lower()
        value = " ".join(toks[value_idx:]).strip()
        if not value:
            return self._string("set.usdb_usage", "Usage: set/usdb [<call>] <field> <value>") + "\r\n"
        now = int(datetime.now(timezone.utc).timestamp())
        await self.store.set_usdb_entry(target, field, value, now)
        self._log_event("usdb", f"{call} set/usdb {target} {field}={value}")
        return self._render_string("set.usdb_updated", "USDB field {field} updated for {target}.", field=field, target=target) + "\r\n"

    async def _cmd_set_var(self, call: str, arg: str | None) -> str:
        if not arg or not arg.strip():
            return self._string("set.var_usage", "Usage: set/var [<call>] <key> <value> | <key>=<value>") + "\r\n"
        toks = [t for t in arg.split() if t]
        target = call.upper()
        rest = toks
        if len(toks) >= 2 and is_valid_call(toks[0].upper()):
            target = toks[0].upper()
            rest = toks[1:]
        if not rest:
            return self._string("set.var_usage", "Usage: set/var [<call>] <key> <value> | <key>=<value>") + "\r\n"

        key = ""
        value = ""
        if "=" in rest[0]:
            key, value = rest[0].split("=", 1)
            if len(rest) > 1:
                value = value + " " + " ".join(rest[1:])
        elif len(rest) >= 2:
            key = rest[0]
            value = " ".join(rest[1:])
        else:
            return self._string("set.var_usage", "Usage: set/var [<call>] <key> <value> | <key>=<value>") + "\r\n"

        key = key.strip().lower()
        value = value.strip()
        if not key or not value:
            return self._string("set.var_usage", "Usage: set/var [<call>] <key> <value> | <key>=<value>") + "\r\n"
        now = int(datetime.now(timezone.utc).timestamp())
        await self.store.set_user_var(target, key, value, now)
        self._log_event("var", f"{call} set/var {target} {key}={value}")
        return self._render_string("set.var_updated", "Variable {key} updated for {target}.", key=key, target=target) + "\r\n" + self._render_string("set.wire", "{name}={value}", name=key, value=value) + "\r\n"

    async def _cmd_set_uservar(self, call: str, arg: str | None) -> str:
        if not arg or not arg.strip():
            return self._string("set.uservar_usage", "Usage: set/uservar [<call>] <key> <value> | <key>=<value>") + "\r\n"
        toks = [t for t in arg.split() if t]
        target = call.upper()
        rest = toks
        if len(toks) >= 2 and is_valid_call(toks[0].upper()):
            target = toks[0].upper()
            rest = toks[1:]
        if not rest:
            return self._string("set.uservar_usage", "Usage: set/uservar [<call>] <key> <value> | <key>=<value>") + "\r\n"
        key = ""
        value = ""
        if "=" in rest[0]:
            key, value = rest[0].split("=", 1)
            if len(rest) > 1:
                value = value + " " + " ".join(rest[1:])
        elif len(rest) >= 2:
            key = rest[0]
            value = " ".join(rest[1:])
        else:
            return self._string("set.uservar_usage", "Usage: set/uservar [<call>] <key> <value> | <key>=<value>") + "\r\n"
        key = key.strip().lower()
        value = value.strip()
        if not key or not value:
            return self._string("set.uservar_usage", "Usage: set/uservar [<call>] <key> <value> | <key>=<value>") + "\r\n"
        now = int(datetime.now(timezone.utc).timestamp())
        full_key = f"uservar.{key}"
        await self.store.set_user_var(target, full_key, value, now)
        self._log_event("uservar", f"{call} set/uservar {target} {full_key}={value}")
        return self._render_string("set.uservar_updated", "User variable {key} updated for {target}.", key=key, target=target) + "\r\n"

    async def _cmd_set_home_pref(self, call: str, arg: str | None, key: str) -> str:
        if not arg or not arg.strip():
            return self._render_string("set.home_usage", "Usage: set/{name} <call>", name=key) + "\r\n"
        target = arg.split()[0].upper()
        if not is_valid_call(target):
            return self._render_string("set.home_usage", "Usage: set/{name} <call>", name=key) + "\r\n"
        s = self._find_session(call)
        if s:
            s.vars[key] = target
        await self._persist_pref(call, key, target)
        if key == "homenode":
            now = int(datetime.now(timezone.utc).timestamp())
            await self.store.upsert_user_registry(call, now, home_node=target)
        self._log_event("pref", f"{call} set/{key} {target}")
        return f"{key}={target}\r\n"

    async def _cmd_sysop_password(self, call: str, arg: str | None) -> str:
        denied = await self._require_privilege(call, 2, "sysop/password")
        if denied:
            return denied
        toks = [t for t in (arg or "").split() if t]
        if len(toks) < 2:
            return self._string("sysop.password_usage", "Usage: sysop/password <call> <newpass>") + "\r\n"
        target = toks[0].upper()
        if not is_valid_call(target):
            return self._string("sysop.password_usage", "Usage: sysop/password <call> <newpass>") + "\r\n"
        password = " ".join(toks[1:]).strip()
        if not password:
            return self._string("sysop.password_usage", "Usage: sysop/password <call> <newpass>") + "\r\n"
        now = int(datetime.now(timezone.utc).timestamp())
        await self.store.upsert_user_registry(target, now)
        await self.store.set_user_pref(target, "password", hash_password(password), now)
        self._log_event("sysop", f"{call} sysop/password {target}")
        return self._render_string("sysop.password_updated", "Password updated for {target}.", target=target) + "\r\n"

    async def _cmd_sysop_clearpassword(self, call: str, arg: str | None) -> str:
        denied = await self._require_privilege(call, 2, "sysop/clearpassword")
        if denied:
            return denied
        target = (arg or "").strip().upper()
        if not target or not is_valid_call(target):
            return self._string("sysop.clearpassword_usage", "Usage: sysop/clearpassword <call>") + "\r\n"
        removed = await self.store.delete_user_pref(target, "password")
        self._log_event("sysop", f"{call} sysop/clearpassword {target} removed={removed}")
        if removed:
            return self._render_string("sysop.password_cleared", "Password cleared for {target}.", target=target) + "\r\n"
        return self._render_string("sysop.password_missing", "No password was set for {target}.", target=target) + "\r\n"

    async def _cmd_sysop_homenode(self, call: str, arg: str | None) -> str:
        denied = await self._require_privilege(call, 2, "sysop/homenode")
        if denied:
            return denied
        toks = [t for t in (arg or "").split() if t]
        if len(toks) != 2:
            return self._string("sysop.homenode_usage", "Usage: sysop/homenode <call> <node>") + "\r\n"
        target = toks[0].upper()
        node = toks[1].upper()
        if not is_valid_call(target) or not is_valid_call(node):
            return self._string("sysop.homenode_usage", "Usage: sysop/homenode <call> <node>") + "\r\n"
        now = int(datetime.now(timezone.utc).timestamp())
        await self.store.upsert_user_registry(target, now, home_node=node)
        await self.store.set_user_pref(target, "homenode", node, now)
        self._log_event("sysop", f"{call} sysop/homenode {target} {node}")
        return self._render_string("sysop.homenode_set", "Home node for {target} set to {node}.", target=target, node=node) + "\r\n"

    async def _cmd_sysop_blocklogin(self, call: str, arg: str | None) -> str:
        denied = await self._require_privilege(call, 2, "sysop/blocklogin")
        if denied:
            return denied
        toks = [t for t in (arg or "").split() if t]
        if len(toks) != 2:
            return self._string("sysop.blocklogin_usage", "Usage: sysop/blocklogin <call> <on|off>") + "\r\n"
        target = toks[0].upper()
        state = toks[1].lower()
        if not is_valid_call(target) or state not in {"on", "off"}:
            return self._string("sysop.blocklogin_usage", "Usage: sysop/blocklogin <call> <on|off>") + "\r\n"
        now = int(datetime.now(timezone.utc).timestamp())
        if state == "on":
            await self.store.set_user_pref(target, "blocked_login", "on", now)
            self._log_event("sysop", f"{call} sysop/blocklogin {target} on")
            return self._render_string("sysop.blocklogin_on", "Login blocked for {target} and all SSIDs.", target=target) + "\r\n"
        removed = await self.store.delete_user_pref(target, "blocked_login")
        self._log_event("sysop", f"{call} sysop/blocklogin {target} off removed={removed}")
        return self._render_string("sysop.blocklogin_off", "Login block cleared for {target}.", target=target) + "\r\n"

    async def _cmd_sysop_showuser(self, call: str, arg: str | None) -> str:
        denied = await self._require_privilege(call, 2, "sysop/showuser")
        if denied:
            return denied
        target = (arg or "").strip().upper()
        if not target or not is_valid_call(target):
            return self._string("sysop.showuser_usage", "Usage: sysop/showuser <call>") + "\r\n"
        return await self._cmd_show_registered(call, target)

    async def _cmd_sysop_users(self, call: str, _arg: str | None) -> str:
        denied = await self._require_privilege(call, 2, "sysop/users")
        if denied:
            return denied
        return await self._cmd_show_registered(call, None)

    async def _cmd_sysop_sysops(self, call: str, _arg: str | None) -> str:
        denied = await self._require_privilege(call, 2, "sysop/sysops")
        if denied:
            return denied
        rows = await self.store.list_user_registry(limit=500, privilege="sysop")
        if not rows:
            return self._string("sysop.sysops_empty", "System Operators: (none)") + "\r\n"
        lines = [self._string("sysop.sysops_title", "System Operators:"), self._string("sysop.sysops_header", "Callsign   Node Family Home Node   Name")]
        for row in rows:
            name = str(row["display_name"] or "")
            home_node = str(row["home_node"] or "")
            node_family = await self._node_family_for_login(str(row["call"] or ""))
            detail = f"{row['call']:<10} {node_family or '-':<11} {home_node:<10} {name}".rstrip()
            lines.append(detail[:80])
        return "\r\n".join(lines) + "\r\n"

    async def _cmd_sysop_access(self, call: str, arg: str | None) -> str:
        denied = await self._require_privilege(call, 2, "sysop/access")
        if denied:
            return denied
        target = (arg or "").strip().upper()
        if not target or not is_valid_call(target):
            return self._string("sysop.access_usage", "Usage: sysop/access <call>") + "\r\n"
        lines = await self._sysop_access_matrix_lines(target)
        return "\r\n".join(lines) + "\r\n"

    async def _cmd_sysop_path(self, call: str, arg: str | None) -> str:
        denied = await self._require_privilege(call, 2, "sysop/path")
        if denied:
            return denied
        target = (arg or "").strip().upper()
        if not target:
            return "Usage: sysop/path <call|peer>\r\n"
        if self._link_stats_fn:
            stats = await self._link_stats_fn()
            st = stats.get(target)
            if st:
                direction = "inbound" if bool(st.get("inbound", False)) else "outbound"
                transport = str(st.get("transport", "") or "-")
                path_hint = str(st.get("path_hint", "") or "-")
                lines = [
                    f"Path for peer {target}:",
                    f"  direction={direction}",
                    f"  transport={transport}",
                    f"  path={path_hint}",
                ]
                return "\r\n".join(lines) + "\r\n"
        if not is_valid_call(target):
            return "Usage: sysop/path <call|peer>\r\n"
        row = await self.store.get_user_registry(target)
        if not row:
            return f"No local path record for {target}.\r\n"
        last_epoch = int(row["last_login_epoch"] or 0)
        last_peer = normalize_recorded_path(str(row["last_login_peer"] or "").strip()) or "(none)"
        lines = [
            f"Path for {target}:",
            f"  last_login={self._fmt_epoch_short(last_epoch) if last_epoch else '(never)'}",
            f"  path={last_peer}",
        ]
        return "\r\n".join(lines) + "\r\n"

    async def _cmd_sysop_spotlimit(self, call: str, arg: str | None) -> str:
        denied = await self._require_privilege(call, 2, "sysop/spotlimit")
        if denied:
            return denied
        toks = [t for t in (arg or "").split() if t]
        if not toks:
            return "Usage: sysop/spotlimit <default|call> [off|default|<max_per_window> [window_seconds]]\r\n"
        target = toks[0].upper()
        now = int(datetime.now(timezone.utc).timestamp())
        if target == "DEFAULT":
            if len(toks) == 1:
                default_max = await self.store.get_user_pref(self.config.node.node_call, SPOT_THROTTLE_MAX_KEY)
                default_window = await self.store.get_user_pref(self.config.node.node_call, SPOT_THROTTLE_WINDOW_KEY)
                policy = await load_spot_throttle_policy(self.store, self.config.node.node_call, call)
                lines = [
                    "Spot throttle defaults:",
                    f"  Enabled: {'on' if policy.enabled else 'off'}",
                    f"  Max Per Window: {policy.max_per_window}",
                    f"  Window Seconds: {policy.window_seconds}",
                    f"  Stored Max Override: {default_max or '(default)'}",
                    f"  Stored Window Override: {default_window or '(default)'}",
                ]
                return "\r\n".join(lines) + "\r\n"
            action = toks[1].lower()
            if action == "off":
                await self.store.set_user_pref(self.config.node.node_call, SPOT_THROTTLE_MAX_KEY, "0", now)
                await self.store.set_user_pref(self.config.node.node_call, SPOT_THROTTLE_WINDOW_KEY, "0", now)
                self._log_event("sysop", f"{call} sysop/spotlimit default off")
                return "Spot throttle defaults disabled.\r\n"
            if action == "default":
                await self.store.delete_user_pref(self.config.node.node_call, SPOT_THROTTLE_MAX_KEY)
                await self.store.delete_user_pref(self.config.node.node_call, SPOT_THROTTLE_WINDOW_KEY)
                self._log_event("sysop", f"{call} sysop/spotlimit default reset")
                return "Spot throttle defaults reset.\r\n"
            try:
                max_per_window = int(toks[1])
                window_seconds = int(toks[2]) if len(toks) > 2 else None
            except ValueError:
                return "Usage: sysop/spotlimit <default|call> [off|default|<max_per_window> [window_seconds]]\r\n"
            if max_per_window < 0 or (window_seconds is not None and window_seconds < 0):
                return "sysop/spotlimit: values must be non-negative\r\n"
            await self.store.set_user_pref(self.config.node.node_call, SPOT_THROTTLE_MAX_KEY, str(max_per_window), now)
            if window_seconds is not None:
                await self.store.set_user_pref(self.config.node.node_call, SPOT_THROTTLE_WINDOW_KEY, str(window_seconds), now)
            self._log_event(
                "sysop",
                f"{call} sysop/spotlimit default max={max_per_window}"
                + (f" window={window_seconds}" if window_seconds is not None else ""),
            )
            return (
                f"Spot throttle defaults updated: max={max_per_window}"
                + (f" window={window_seconds}s" if window_seconds is not None else "")
                + "\r\n"
            )
        if not is_valid_call(target):
            return "Usage: sysop/spotlimit <default|call> [off|default|<max_per_window> [window_seconds]]\r\n"
        if len(toks) == 1:
            policy = await check_spot_throttle(self.store, self.config.node.node_call, target, now)
            lines = [
                f"Spot throttle for {target}:",
                f"  Enabled: {'on' if policy.enabled else 'off'}",
                f"  Exempt: {'yes' if policy.exempt else 'no'}",
                f"  Privilege: {policy.privilege or 'non-authenticated'}",
                f"  Max Per Window: {policy.max_per_window}",
                f"  Window Seconds: {policy.window_seconds}",
                f"  Recent Count: {policy.recent_count}",
                f"  Override Scope: {policy.override_scope or 'default'}",
            ]
            return "\r\n".join(lines) + "\r\n"
        action = toks[1].lower()
        if action == "off":
            await self.store.set_user_pref(target, SPOT_THROTTLE_EXEMPT_KEY, "on", now)
            self._log_event("sysop", f"{call} sysop/spotlimit {target} off")
            return f"Spot throttle disabled for {target}.\r\n"
        if action == "default":
            await self.store.delete_user_pref(target, SPOT_THROTTLE_EXEMPT_KEY)
            await self.store.delete_user_pref(target, SPOT_THROTTLE_MAX_KEY)
            await self.store.delete_user_pref(target, SPOT_THROTTLE_WINDOW_KEY)
            self._log_event("sysop", f"{call} sysop/spotlimit {target} reset")
            return f"Spot throttle override cleared for {target}.\r\n"
        try:
            max_per_window = int(toks[1])
            window_seconds = int(toks[2]) if len(toks) > 2 else None
        except ValueError:
            return "Usage: sysop/spotlimit <default|call> [off|default|<max_per_window> [window_seconds]]\r\n"
        if max_per_window < 0 or (window_seconds is not None and window_seconds < 0):
            return "sysop/spotlimit: values must be non-negative\r\n"
        await self.store.delete_user_pref(target, SPOT_THROTTLE_EXEMPT_KEY)
        await self.store.set_user_pref(target, SPOT_THROTTLE_MAX_KEY, str(max_per_window), now)
        if window_seconds is not None:
            await self.store.set_user_pref(target, SPOT_THROTTLE_WINDOW_KEY, str(window_seconds), now)
        self._log_event(
            "sysop",
            f"{call} sysop/spotlimit {target} max={max_per_window}"
            + (f" window={window_seconds}" if window_seconds is not None else ""),
        )
        return (
            f"Spot throttle updated for {target}: max={max_per_window}"
            + (f" window={window_seconds}s" if window_seconds is not None else "")
            + "\r\n"
        )

    async def _cmd_sysop_setaccess(self, call: str, arg: str | None) -> str:
        denied = await self._require_privilege(call, 2, "sysop/setaccess")
        if denied:
            return denied
        toks = [t for t in (arg or "").split() if t]
        if len(toks) != 4:
            return self._string("sysop.setaccess_usage", "Usage: sysop/setaccess <call> <channel|all> <capability|all> <on|off>") + "\r\n"
        target = toks[0].upper()
        channel = toks[1].lower()
        capability = toks[2].lower()
        state = toks[3].lower()
        channels = self._access_channels()
        caps = self._access_capabilities()
        if not is_valid_call(target) or state not in {"on", "off"}:
            return self._string("sysop.setaccess_usage", "Usage: sysop/setaccess <call> <channel|all> <capability|all> <on|off>") + "\r\n"
        if channel in {"all", "a", "*"}:
            chosen_channels = channels
        elif channel in channels:
            chosen_channels = (channel,)
        else:
            return self._string("sysop.setaccess_usage", "Usage: sysop/setaccess <call> <channel|all> <capability|all> <on|off>") + "\r\n"
        if capability in {"all", "a", "*"}:
            chosen_caps = caps
        elif capability in caps:
            chosen_caps = (capability,)
        else:
            return self._string("sysop.setaccess_usage", "Usage: sysop/setaccess <call> <channel|all> <capability|all> <on|off>") + "\r\n"
        now = int(datetime.now(timezone.utc).timestamp())
        for chosen_channel in chosen_channels:
            for chosen_cap in chosen_caps:
                await self.store.set_user_pref(target, self._access_pref_key(chosen_channel, chosen_cap), state, now)
        self._log_event("sysop", f"{call} sysop/setaccess {target} {channel} {capability} {state}")
        scope = "all channels" if channel in {"all", "a", "*"} else channel
        what = "all capabilities" if capability in {"all", "a", "*"} else capability
        state_text = "enabled" if state == "on" else "disabled"
        return self._render_string("sysop.setaccess_result", "{what} {state} for {target} on {scope}.", what=what, state=state_text, target=target, scope=scope) + "\r\n"

    async def _cmd_sysop_setprompt(self, call: str, arg: str | None) -> str:
        denied = await self._require_privilege(call, 2, "sysop/setprompt")
        if denied:
            return denied
        template = (arg or "").strip()
        if not template:
            return self._string("sysop.setprompt_usage", "Usage: sysop/setprompt <prompt template>") + "\r\n"
        if len(template) > 256:
            return self._string("sysop.setprompt_too_long", "Prompt template must be 256 characters or fewer.") + "\r\n"
        now = int(datetime.now(timezone.utc).timestamp())
        await self.store.set_user_pref(self.config.node.node_call, "prompt_template", template, now)
        self._log_event("sysop", f"{call} sysop/setprompt template={template}")
        return self._string("sysop.setprompt_updated", "Prompt template updated.") + "\r\n"

    async def _cmd_sysop_services(self, call: str, _arg: str | None) -> str:
        denied = await self._require_privilege(call, 2, "sysop/services")
        if denied:
            return denied
        if not self._component_status_fn:
            return self._string("sysop.services_unavailable", "Service control is not attached in this runtime.") + "\r\n"
        rows = await self._component_status_fn()
        if not rows:
            return self._string("sysop.services_empty", "No component status available.") + "\r\n"
        lines = [self._string("sysop.services_title", "Service Status"), self._string("sysop.services_header", "Component   State   Detail")]
        for row in rows:
            component = str(row.get("component", "")).strip()
            state = str(row.get("state", "unknown")).strip()
            detail = str(row.get("detail", "")).strip()
            lines.append(f"{component:<10} {state:<7} {detail}"[:80])
        return "\r\n".join(lines) + "\r\n"

    async def _cmd_sysop_restart(self, call: str, arg: str | None) -> str:
        denied = await self._require_privilege(call, 2, "sysop/restart")
        if denied:
            return denied
        target = (arg or "").strip().lower()
        if not target:
            return self._string("sysop.restart_usage", "Usage: sysop/restart <telnet|sysopweb|all>") + "\r\n"
        if not self._component_restart_fn:
            return self._string("sysop.services_unavailable", "Service control is not attached in this runtime.") + "\r\n"
        ok, msg = await self._component_restart_fn(target)
        self._log_event("control", f"{call} sysop/restart {target} ok={int(ok)}")
        return (msg.rstrip() + "\r\n") if ok else (msg.rstrip() + "\r\n")

    def _startup_command_allowed(self, text: str) -> bool:
        low = text.strip().lower()
        if not low:
            return False
        if low.startswith(("announce", "talk", "msg", "send", "reply", "connect", "disconnect", "wcy", "wwv", "wx")):
            return False
        if low.startswith(("set/startup", "unset/startup", "create/", "delete/", "load/")):
            return False
        allowed_prefixes = (
            "show/", "sh/", "set/", "unset/", "help", "?", "version", "users", "node", "cluster",
            "time", "date", "motd", "mail", "stat/",
        )
        return low.startswith(allowed_prefixes)

    async def _run_startup_commands(self, call: str) -> list[str]:
        prefs = await self._load_prefs_for_call(call)
        enabled = (prefs.get("startup", "off")).lower() in {"1", "on", "yes", "true"}
        if not enabled:
            return []
        rows = await self.store.list_startup_commands(call, limit=100)
        outputs: list[str] = []
        for r in rows:
            text = str(r["command"]).strip()
            if not self._startup_command_allowed(text):
                outputs.append(f"[startup] skipped unsafe command: {text}\r\n")
                continue
            keep_going, out = await self._execute_command(call, text)
            if out:
                outputs.append(out if out.endswith("\r\n") else out + "\r\n")
            if not keep_going:
                outputs.append("[startup] halted by command request\r\n")
                break
        return outputs

    async def _cmd_set_startup(self, call: str, arg: str | None) -> str:
        target = call.upper()
        text = (arg or "").strip()
        if text:
            toks = [t for t in text.split() if t]
            if len(toks) >= 2 and is_valid_call(toks[0].upper()):
                target = toks[0].upper()
                text = " ".join(toks[1:]).strip()
        if target != call.upper():
            denied = await self._require_privilege(call, 2, "set/startup")
            if denied:
                return denied
        s = self._find_session(target)
        now = int(datetime.now(timezone.utc).timestamp())
        if not text:
            if s:
                s.vars["startup"] = "on"
            await self.store.set_user_pref(target, "startup", "on", now)
            return self._render_string("set.startup_enabled", "Startup commands enabled for {target}.", target=target) + "\r\n"
        low = text.lower()
        if low in {"on", "off"}:
            if s:
                s.vars["startup"] = low
            await self.store.set_user_pref(target, "startup", low, now)
            state = "enabled" if low == "on" else "disabled"
            return self._render_string("set.startup_state", "Startup commands {state} for {target}.", state=state, target=target) + "\r\n"
        if low == "clear":
            n = await self.store.clear_startup_commands(target)
            if s:
                s.vars["startup"] = "on"
            await self.store.set_user_pref(target, "startup", "on", now)
            return self._render_string("set.startup_cleared", "Cleared {count} startup command(s) for {target}.", count=n, target=target) + "\r\n"
        if low.startswith("del "):
            tok = text.split(maxsplit=1)[1].strip()
            if not tok.isdigit():
                return self._string("set.startup_usage", "Usage: set/startup del <id>") + "\r\n"
            removed = await self.store.remove_startup_command(target, int(tok))
            return f"Removed {removed} startup command(s) for {target}.\r\n"
        cmd_id = await self.store.add_startup_command(target, text, now)
        if s:
            s.vars["startup"] = "on"
        await self.store.set_user_pref(target, "startup", "on", now)
        return self._render_string("set.startup_added", "Added startup command #{command_id} for {target}.", command_id=cmd_id, target=target) + "\r\n"

    async def _cmd_set_user(self, call: str, arg: str | None) -> str:
        if not arg or not arg.strip():
            return "Usage: set/user <call> [<field> <value>]\r\n"
        toks = [t for t in arg.split() if t]
        target = toks[0].upper()
        if not is_valid_call(target):
            return "Usage: set/user <call> [<field> <value>]\r\n"
        now = int(datetime.now(timezone.utc).timestamp())
        if len(toks) == 1:
            if target != call.upper():
                denied = await self._require_privilege(call, 2, "set/user")
                if denied:
                    return denied
            await self.store.upsert_user_registry(target, now)
            self._log_event("user", f"{call} set/user {target}")
            return f"User record created or updated for {target}.\r\n"
        if len(toks) < 3:
            return "Usage: set/user <call> [<field> <value>]\r\n"
        field = toks[1].lower()
        value = " ".join(toks[2:]).strip()
        if not value:
            return "Usage: set/user <call> [<field> <value>]\r\n"
        if target != call.upper():
            denied = await self._require_privilege(call, 2, "set/user")
            if denied:
                return denied
        kwargs: dict[str, str] = {}
        if field in {"name", "display_name"}:
            kwargs["display_name"] = value
            fshow = "name"
        elif field == "address":
            kwargs["address"] = value
            fshow = "address"
        elif field == "qth":
            kwargs["qth"] = value
            fshow = "qth"
        elif field == "qra":
            kwargs["qra"] = value
            fshow = "qra"
        elif field == "email":
            kwargs["email"] = value
            fshow = "email"
        elif field == "privilege":
            denied = await self._require_privilege(call, 2, "set/user privilege")
            if denied:
                return denied
            actor_level = await self._privilege_level_for(call)
            req = self._PRIV_LEVELS.get(value.lower(), -1)
            if req > actor_level:
                return "set/user privilege: cannot grant above your own level\r\n"
            kwargs["privilege"] = value
            fshow = "privilege"
        elif field == "node_family":
            denied = await self._require_privilege(call, 2, "set/user node_family")
            if denied:
                return denied
            family = value.lower()
            if family not in {"pycluster", "dxspider", "dxnet", "arcluster", "clx"}:
                return "Usage: set/user <call> node_family <pycluster|dxspider|dxnet|arcluster|clx>\r\n"
            await self.store.upsert_user_registry(target, now)
            await self.store.set_user_pref(target, "node_family", family, now)
            self._log_event("user", f"{call} set/user {target} node_family={family}")
            return f"node_family updated for {target}.\r\n"
        else:
            return "Usage: set/user <call> [name|address|qth|qra|email|privilege|node_family] <value>\r\n"
        await self.store.upsert_user_registry(target, now, **kwargs)
        self._log_event("user", f"{call} set/user {target} {fshow}={value}")
        return f"{fshow} updated for {target}.\r\n"

    async def _cmd_set_contact_field(self, call: str, arg: str | None, field: str) -> str:
        if not arg or not arg.strip():
            return f"Usage: set/{field} [<call>] <value>\r\n"
        toks = [t for t in arg.split() if t]
        target = call.upper()
        value_toks = toks
        if len(toks) >= 2 and is_valid_call(toks[0].upper()) and any(ch.isalpha() for ch in toks[0]):
            target = toks[0].upper()
            value_toks = toks[1:]
        if target != call.upper():
            denied = await self._require_privilege(call, 2, f"set/{field}")
            if denied:
                return denied
        value = " ".join(value_toks).strip()
        if not value:
            return f"Usage: set/{field} [<call>] <value>\r\n"
        now = int(datetime.now(timezone.utc).timestamp())
        if field == "email":
            await self.store.upsert_user_registry(target, now, email=value)
        else:
            await self.store.upsert_user_registry(target, now, address=value)
        if target == call.upper():
            s = self._find_session(call)
            if s:
                s.vars[field] = value
            await self._persist_pref(call, field, value)
        self._log_event("user", f"{call} set/{field} {target} {value}")
        field_label = "Email" if field == "email" else "Address"
        return f"{field_label} updated for {target}.\r\n"

    async def _cmd_set_privilege(self, call: str, arg: str | None) -> str:
        if not arg or not arg.strip():
            return "Usage: set/privilege [<call>] <user|op|sysop>\r\n"
        toks = [t for t in arg.split() if t]
        target = call.upper()
        idx = 0
        if len(toks) >= 2 and is_valid_call(toks[0].upper()):
            target = toks[0].upper()
            idx = 1
        level_name = toks[idx].lower()
        if level_name not in self._PRIV_LEVELS:
            return "Usage: set/privilege [<call>] <user|op|sysop>\r\n"
        denied = await self._require_privilege(call, 2, "set/privilege")
        if denied:
            return denied
        actor_level = await self._privilege_level_for(call)
        req_level = self._PRIV_LEVELS[level_name]
        if req_level > actor_level:
            return "set/privilege: cannot grant above your own level\r\n"
        now = int(datetime.now(timezone.utc).timestamp())
        await self.store.upsert_user_registry(target, now, privilege=level_name)
        if target == call.upper():
            s = self._find_session(call)
            if s:
                s.vars["privilege"] = level_name
            await self._persist_pref(call, "privilege", level_name)
        self._log_event("user", f"{call} set/privilege {target} {level_name}")
        return f"Privilege for {target} set to {level_name}.\r\n"

    async def _cmd_unset_privilege(self, call: str, arg: str | None) -> str:
        toks = [t for t in (arg or "").split() if t]
        target = call.upper()
        if toks and is_valid_call(toks[0].upper()):
            target = toks[0].upper()
        if target != call.upper():
            denied = await self._require_privilege(call, 2, "unset/privilege")
            if denied:
                return denied
        now = int(datetime.now(timezone.utc).timestamp())
        await self.store.upsert_user_registry(target, now, privilege="user")
        if target == call.upper():
            s = self._find_session(call)
            if s:
                s.vars["privilege"] = "user"
            await self._persist_pref(call, "privilege", "user")
        self._log_event("user", f"{call} unset/privilege {target}")
        return f"Privilege for {target} reset to user.\r\n"

    async def _cmd_unset_bad_rule(self, call: str, arg: str | None, kind: str) -> str:
        target = (arg or "all").strip()
        removed = await self.store.remove_deny_rule(kind, target)
        self._log_event("badrule", f"{call} unset/{kind} {target} removed={removed}")
        return f"Removed {removed} {kind} entr{'y' if removed == 1 else 'ies'}.\r\nremoved={removed}\r\n"

    async def _cmd_unset_buddy(self, call: str, arg: str | None) -> str:
        target = (arg or "all").strip()
        removed = await self.store.remove_buddy(call, target)
        self._log_event("buddy", f"{call} unset/buddy {target} removed={removed}")
        return f"Removed {removed} buddy entr{'y' if removed == 1 else 'ies'} for {call}.\r\n"

    async def _cmd_delete_usdb(self, call: str, arg: str | None) -> str:
        target = call.upper()
        field: str | None = None
        toks = [t for t in (arg or "").split() if t]
        if toks:
            if is_valid_call(toks[0].upper()):
                target = toks[0].upper()
                if len(toks) >= 2:
                    field = toks[1].lower()
            else:
                field = toks[0].lower()
        removed = await self.store.delete_usdb_entries(target, field)
        what = field if field else "all"
        self._log_event("usdb", f"{call} delete/usdb {target} {what} removed={removed}")
        if field:
            if removed:
                return f"Removed USDB field {field} for {target}.\r\n"
            return f"USDB field {field} was not set for {target}.\r\n"
        return f"Removed {removed} USDB entr{'y' if removed == 1 else 'ies'} for {target}.\r\n"

    async def _cmd_unset_var(self, call: str, arg: str | None) -> str:
        target = call.upper()
        key: str | None = None
        toks = [t for t in (arg or "").split() if t]
        if toks:
            if len(toks) >= 2 and is_valid_call(toks[0].upper()):
                target = toks[0].upper()
                key = toks[1].lower()
            elif is_valid_call(toks[0].upper()):
                target = toks[0].upper()
            else:
                key = toks[0].lower()
        removed = await self.store.delete_user_vars(target, key)
        what = key if key else "all"
        self._log_event("var", f"{call} unset/var {target} {what} removed={removed}")
        if key:
            if removed:
                return f"Variable {key} cleared for {target}.\r\n"
            return f"Variable {key} was not set for {target}.\r\n"
        return f"Cleared {removed} variable entr{'y' if removed == 1 else 'ies'} for {target}.\r\n"

    async def _cmd_unset_uservar(self, call: str, arg: str | None) -> str:
        target = call.upper()
        key: str | None = None
        toks = [t for t in (arg or "").split() if t]
        if toks:
            if len(toks) >= 2 and is_valid_call(toks[0].upper()):
                target = toks[0].upper()
                key = toks[1].lower()
            elif is_valid_call(toks[0].upper()):
                target = toks[0].upper()
            else:
                key = toks[0].lower()
        if key and key != "all":
            key = f"uservar.{key}"
        removed = await self.store.delete_user_vars(target, key)
        what = key if key else "all"
        self._log_event("uservar", f"{call} unset/uservar {target} {what} removed={removed}")
        if key:
            short_key = key.removeprefix("uservar.")
            if removed:
                return f"User variable {short_key} cleared for {target}.\r\n"
            return f"User variable {short_key} was not set for {target}.\r\n"
        return f"Cleared {removed} user variable entr{'y' if removed == 1 else 'ies'} for {target}.\r\n"

    async def _cmd_unset_startup(self, call: str, arg: str | None) -> str:
        target = call.upper()
        text = (arg or "").strip()
        if text:
            toks = [t for t in text.split() if t]
            if len(toks) >= 2 and is_valid_call(toks[0].upper()):
                target = toks[0].upper()
                text = " ".join(toks[1:]).strip()
        if target != call.upper():
            denied = await self._require_privilege(call, 2, "unset/startup")
            if denied:
                return denied
        s = self._find_session(target)
        low = text.lower()
        now = int(datetime.now(timezone.utc).timestamp())
        if low in {"all", "clear"}:
            n = await self.store.clear_startup_commands(target)
            if s:
                s.vars["startup"] = "off"
            await self.store.set_user_pref(target, "startup", "off", now)
            return self._render_string("unset.startup_disabled_cleared", "Startup commands disabled for {target}; cleared {count} command(s).", target=target, count=n) + "\r\n"
        if s:
            s.vars["startup"] = "off"
        await self.store.set_user_pref(target, "startup", "off", now)
        return self._render_string("unset.startup_disabled", "Startup commands disabled for {target}.", target=target) + "\r\n"

    async def _cmd_unset_contact_field(self, call: str, arg: str | None, field: str) -> str:
        target = call.upper()
        toks = [t for t in (arg or "").split() if t]
        if toks and is_valid_call(toks[0].upper()) and any(ch.isalpha() for ch in toks[0]):
            target = toks[0].upper()
        if target != call.upper():
            denied = await self._require_privilege(call, 2, f"unset/{field}")
            if denied:
                return denied
        now = int(datetime.now(timezone.utc).timestamp())
        if field == "email":
            await self.store.upsert_user_registry(target, now, email="")
        else:
            await self.store.upsert_user_registry(target, now, address="")
        if target == call.upper():
            s = self._find_session(call)
            if s:
                s.vars[field] = ""
            await self._persist_pref(call, field, "")
        self._log_event("user", f"{call} unset/{field} {target}")
        field_label = "Email" if field == "email" else "Address"
        return f"{field_label} cleared for {target}.\r\n"

    async def _cmd_delete_user(self, call: str, arg: str | None) -> str:
        denied = await self._require_privilege(call, 2, "delete/user")
        if denied:
            return denied
        target = (arg or "").strip().upper()
        if not target or not is_valid_call(target):
            return "Usage: delete/user <call>\r\n"
        removed = await self.store.delete_user_registry(target)
        self._log_event("user", f"{call} delete/user {target} removed={removed}")
        if removed:
            return f"User {target} removed.\r\n"
        return f"No user record found for {target}.\r\n"

    async def _cmd_create_user(self, call: str, arg: str | None) -> str:
        denied = await self._require_privilege(call, 2, "create/user")
        if denied:
            return denied
        target = (arg or "").strip().upper()
        if not target or not is_valid_call(target):
            return "Usage: create/user <call>\r\n"
        now = int(datetime.now(timezone.utc).timestamp())
        await self.store.upsert_user_registry(target, now)
        self._log_event("user", f"{call} create/user {target}")
        return f"User record created for {target}.\r\n"

    async def _cmd_filter_clear(self, call: str, arg: str | None, family: str) -> str:
        parsed = self._parse_filter_target_and_slot(call, arg)
        if not parsed:
            return f"Usage: clear/{family} [<call>] [input] [<slot>|all]\r\n"
        target, slot = parsed
        await self.store.clear_filter_rules(target, family, slot)
        fam = self._filters.setdefault(target, {}).setdefault(family, {})
        for act in ("accept", "reject"):
            fam.setdefault(act, [])
            if slot == "all":
                fam[act].clear()
            else:
                fam[act] = [r for r in fam[act] if r.slot != slot]
        self._log_event("filter", f"clear/{family} {target} {slot}")
        scope = "all slots" if slot == "all" else f"slot {slot}"
        return (
            f"Cleared {family} filters for {target} ({scope}).\r\n"
            f"clear/{family} {target} {scope}\r\n"
        )

    async def _cmd_filter_clear_expr(self, call: str, arg: str | None, family: str, expr: str, label: str) -> str:
        parsed = self._parse_filter_target_and_slot(call, arg)
        if not parsed:
            return f"Usage: clear/{label} [<call>] [input] [<slot>|all]\r\n"
        target, slot = parsed
        if target not in self._filters:
            await self._load_filters_for_call(target)
        fam = self._filters.setdefault(target, {}).setdefault(family, {})
        kept: dict[str, list[FilterRule]] = {}
        removed = 0
        for act in ("accept", "reject"):
            current = fam.setdefault(act, [])
            next_rules: list[FilterRule] = []
            for r in current:
                if (slot == "all" or r.slot == slot) and r.expr.strip().lower() == expr.strip().lower():
                    removed += 1
                    continue
                next_rules.append(r)
            kept[act] = next_rules
        await self.store.clear_filter_rules(target, family, slot)
        now = int(datetime.now(timezone.utc).timestamp())
        for act in ("accept", "reject"):
            fam[act] = kept[act]
            for r in kept[act]:
                if slot == "all" or r.slot == slot:
                    await self.store.set_filter_rule(target, family, act, r.slot, r.expr, now)
        self._log_event("filter", f"clear/{label} {target} {slot} removed={removed}")
        return (
            f"Cleared {removed} {label} filter entr{'y' if removed == 1 else 'ies'} for {target}.\r\n"
            f"clear/{label} {target} {'all slots' if slot == 'all' else f'slot {slot}'}\r\n"
        )

    async def _cmd_clear_dupefile(self, call: str, _arg: str | None) -> str:
        s = self._find_session(call)
        now = int(datetime.now(timezone.utc).timestamp())
        keys = ("dup_ann", "dup_eph", "dup_spots", "dup_wcy", "dup_wwv")
        for k in keys:
            if s:
                s.vars[k] = "off"
            await self.store.set_user_pref(call.upper(), k, "off", now)
        removed = await self.store.clear_spot_dupes()
        await self.store.set_spot_dupe_enabled(False)
        self._log_event("maint", f"{call} clear/dupefile reset={','.join(keys)}")
        return f"Duplicate spot tracking reset; removed {removed} cached duplicate entr{'y' if removed == 1 else 'ies'}.\r\n"

    async def _cmd_set_dup_spots(self, call: str, arg: str | None) -> str:
        out = await self._cmd_set_named_var(call, arg, "dup_spots", "on")
        await self.store.set_spot_dupe_enabled(True)
        return out

    async def _cmd_unset_dup_spots(self, call: str, arg: str | None) -> str:
        out = await self._cmd_unset_named_var(call, arg, "dup_spots")
        await self.store.set_spot_dupe_enabled(False)
        return out

    def _load_target_call(self, call: str, arg: str | None) -> str:
        target = call.upper()
        text = (arg or "").strip()
        if text:
            tok = text.split()[0].upper()
            if is_valid_call(tok):
                target = tok
        return target

    async def _cmd_load_aliases(self, _call: str, _arg: str | None) -> str:
        reg = self._build_registry()
        grouped = sum(1 for k in reg if "/" in k)
        top_level = len(reg) - grouped
        direct_alias = len(self._direct_alias_map())
        return self._render_string("load.aliases", "Alias cache loaded: {commands} commands, {grouped} grouped, {top_level} top-level, {direct_aliases} direct aliases.", commands=len(reg), grouped=grouped, top_level=top_level, direct_aliases=direct_alias) + "\r\n"

    async def _cmd_load_badmsg(self, _call: str, _arg: str | None) -> str:
        words = await self.store.list_deny_rules("badword")
        noun = "entry" if len(words) == 1 else "entries"
        return self._render_string("load.badmsg", "Loaded {count} bad-message rule {noun}.", count=len(words), noun=noun) + "\r\n"

    async def _cmd_load_badwords(self, _call: str, _arg: str | None) -> str:
        words = await self.store.list_deny_rules("badword")
        noun = "entry" if len(words) == 1 else "entries"
        return self._render_string("load.badwords", "Loaded {count} bad-word rule {noun}.", count=len(words), noun=noun) + "\r\n"

    async def _cmd_load_bands(self, _call: str, _arg: str | None) -> str:
        total_spots = await self.store.count_spots()
        return self._render_string("load.bands", "Loaded {bands} band definitions with {spots} observed spot(s).", bands=len(BAND_RANGES), spots=total_spots) + "\r\n"

    async def _cmd_load_cmdcache(self, _call: str, _arg: str | None) -> str:
        reg = self._build_registry()
        groups = sorted({k.split("/", 1)[0] for k in reg if "/" in k})
        shortcuts = len(self._build_shortcut_catalog(reg))
        return self._render_string("load.cmdcache", "Command cache loaded: {commands} commands, {groups} groups, {shortcuts} shortcuts.", commands=len(reg), groups=len(groups), shortcuts=shortcuts) + "\r\n"

    async def _cmd_load_db(self, _call: str, _arg: str | None) -> str:
        dbp = Path(self.config.store.sqlite_path)
        size = dbp.stat().st_size if dbp.exists() else 0
        spots = await self.store.count_spots()
        registry = len(await self.store.list_user_registry(limit=2000))
        return self._render_string("load.db", "Database loaded from {path} ({size} bytes) with {spots} spots and {registry} registered user(s).", path=dbp, size=size, spots=spots, registry=registry) + "\r\n"

    async def _cmd_load_dxqsl(self, call: str, arg: str | None) -> str:
        target = self._load_target_call(call, arg)
        prefs = await self.store.list_user_prefs(target)
        exp = prefs.get("dxqsl_export_path", "")
        imp = prefs.get("dxqsl_import_path", "")
        return self._render_string("load.dxqsl", "DXQSL settings loaded for {target}: export={export}, import={import_}.", target=target, export="yes" if exp else "no", import_="yes" if imp else "no") + "\r\n"

    async def _cmd_load_forward(self, call: str, arg: str | None) -> str:
        target = self._load_target_call(call, arg)
        prefs = await self.store.list_user_prefs(target)
        loaded = sum(
            1 for k in ("forward_lat", "forward_lon", "forward_opername") if (prefs.get(k) or "").strip()
        )
        return self._render_string("load.forward", "Forwarding settings loaded for {target} ({count} field(s)).", target=target, count=loaded) + "\r\n"

    async def _cmd_load_hops(self, _call: str, _arg: str | None) -> str:
        peers = 0
        inbound = 0
        if self._link_stats_fn:
            stats = await self._link_stats_fn()
            peers = len(stats)
            inbound = sum(1 for st in stats.values() if bool(st.get("inbound", False)))
        return self._render_string("load.hops", "Hop data loaded: {peers} peer(s), {inbound} inbound, {outbound} outbound.", peers=peers, inbound=inbound, outbound=max(0, peers - inbound)) + "\r\n"

    async def _cmd_load_keps(self, call: str, arg: str | None) -> str:
        target = self._load_target_call(call, arg)
        last = await self.store.get_user_pref(target, "keps_last_request_epoch")
        return self._render_string("load.keps", "Keplerian request state loaded for {target}; last request epoch is {last}.", target=target, last=last or "0") + "\r\n"

    async def _cmd_load_messages(self, call: str, arg: str | None) -> str:
        target = self._load_target_call(call, arg)
        total, unread = await self.store.message_counts(target)
        return self._render_string("load.messages", "Message state loaded for {target}: {total} total, {unread} unread.", target=target, total=total, unread=unread) + "\r\n"

    async def _cmd_load_prefixes(self, _call: str, _arg: str | None) -> str:
        rows = await self.store.latest_spots(limit=500)
        prefixes = sorted(
            {
                str(r["dx_call"]).split("/", 1)[0].strip().upper()[:3]
                for r in rows
                if str(r["dx_call"]).strip()
            }
        )
        sample = ",".join(prefixes[:8]) if prefixes else "-"
        return self._render_string("load.prefixes", "Loaded {count} observed prefix sample(s): {sample}.", count=len(prefixes), sample=sample) + "\r\n"

    async def _cmd_load_swop(self, call: str, arg: str | None) -> str:
        target = self._load_target_call(call, arg)
        rows = await self.store.list_startup_commands(target, limit=200)
        noun = "entry" if len(rows) == 1 else "entries"
        return self._render_string("load.swop", "Startup commands loaded for {target}: {count} {noun}.", target=target, count=len(rows), noun=noun) + "\r\n"

    async def _cmd_load_usdb(self, call: str, arg: str | None) -> str:
        target = self._load_target_call(call, arg)
        entries = await self.store.list_usdb_entries(target)
        noun = "entry" if len(entries) == 1 else "entries"
        return self._render_string("load.usdb", "USDB loaded for {target}: {count} {noun}.", target=target, count=len(entries), noun=noun) + "\r\n"

    async def _cmd_stat_user_direct(self, _call: str, _arg: str | None) -> str:
        noun = "session" if self.session_count == 1 else "sessions"
        return self._render_string("stat.user", "User session summary: {count} active {noun}.", count=self.session_count, noun=noun) + "\r\n"

    async def _cmd_stat_msg_direct(self, call: str, _arg: str | None) -> str:
        total, unread = await self.store.message_counts(call)
        return (
            self._render_string("stat.msg", "Message summary: {total} total, {unread} unread.", total=total, unread=unread) + "\r\n"
            + self._render_string("stat.msg_wire", "Message summary: total={total} unread={unread}", total=total, unread=unread) + "\r\n"
        )

    async def _cmd_stat_db_direct(self, _call: str, _arg: str | None) -> str:
        spots = await self.store.count_spots()
        reg = len(await self.store.list_user_registry(limit=1000))
        return (
            self._render_string("stat.db", "Database summary: {spots} stored spot(s), {registry} registry record(s).", spots=spots, registry=reg) + "\r\n"
            + self._render_string("stat.db_wire", "Database summary: spots={spots} registry={registry}", spots=spots, registry=reg) + "\r\n"
        )

    async def _cmd_stat_channel_direct(self, _call: str, _arg: str | None) -> str:
        if not self._link_stats_fn:
            return self._string("stat.channel_empty", "Channel summary: peers=0 inbound=0 outbound=0 rx=0 tx=0 dropped=0 policy_drop=0") + "\r\n"
        stats = await self._link_stats_fn()
        rx = sum(int(st.get("parsed_frames", 0)) for st in stats.values())
        tx = sum(int(st.get("sent_frames", 0)) for st in stats.values())
        dropped = sum(int(st.get("dropped_frames", 0)) for st in stats.values())
        policy_drop = sum(int(st.get("policy_dropped", 0)) for st in stats.values())
        inbound = sum(1 for st in stats.values() if bool(st.get("inbound", False)))
        outbound = max(0, len(stats) - inbound)
        return (
            self._render_string("stat.channel", "Channel summary: {peers} peer(s), {inbound} accepted, {outbound} dial-out, RX {rx}, TX {tx}, dropped {dropped}, policy drops {policy_drop}.", peers=len(stats), inbound=inbound, outbound=outbound, rx=rx, tx=tx, dropped=dropped, policy_drop=policy_drop) + "\r\n"
            + f"Channel summary: peers={len(stats)} inbound={inbound} outbound={outbound} rx={rx} tx={tx} dropped={dropped} policy_drop={policy_drop}\r\n"
        )

    async def _cmd_stat_nodeconfig_direct(self, _call: str, _arg: str | None) -> str:
        prefs = await self.store.list_user_prefs(self.config.node.node_call)
        noun = "entry" if len(prefs) == 1 else "entries"
        return self._render_string("stat.nodeconfig", "Node configuration summary: {node} has {count} stored preference {noun}.", node=self.config.node.node_call, count=len(prefs), noun=noun) + "\r\n"

    async def _cmd_stat_pc19list_direct(self, _call: str, _arg: str | None) -> str:
        calls = sorted({s.call.upper() for s in self._sessions.values()})
        on = []
        for c in calls:
            v = (await self._get_pref(c, "routepc19") or "").strip().lower()
            if v in {"1", "on", "yes", "true"}:
                on.append(c)
        if not on:
            return self._string("stat.pc19list_empty", "PC19 routing is not enabled for any local calls.") + "\r\n"
        return self._render_string("stat.pc19list", "PC19 routing enabled for {count} calls: {calls}.", count=len(on), calls=",".join(on[:30])) + "\r\n"

    async def _cmd_stat_routenode_direct(self, _call: str, _arg: str | None) -> str:
        if not self._link_stats_fn:
            return self._string("stat.routenode_empty", "Route nodes: total=0 inbound=0 outbound=0") + "\r\n"
        stats = await self._link_stats_fn()
        inbound = sum(1 for _n, st in stats.items() if bool(st.get("inbound", False)))
        outbound = max(0, len(stats) - inbound)
        return (
            self._render_string("stat.routenode", "Route nodes: {total} total, {inbound} accepted, {outbound} dial-out.", total=len(stats), inbound=inbound, outbound=outbound) + "\r\n"
            + self._render_string("stat.routenode_wire", "Route nodes: total={total} inbound={inbound} outbound={outbound}", total=len(stats), inbound=inbound, outbound=outbound) + "\r\n"
        )

    async def _cmd_stat_routeuser_direct(self, _call: str, _arg: str | None) -> str:
        users = self.session_count
        peers = len(await self._link_stats_fn()) if self._link_stats_fn else 0
        return (
            self._render_string("stat.routeuser", "Route users: {users} active user session(s) across {peers} peer link(s).", users=users, peers=peers) + "\r\n"
            + self._render_string("stat.routeuser_wire", "Route users: users={users} peers={peers}", users=users, peers=peers) + "\r\n"
        )

    async def _cmd_stat_userconfig_direct(self, call: str, _arg: str | None) -> str:
        prefs = await self.store.list_user_prefs(call)
        noun = "entry" if len(prefs) == 1 else "entries"
        return self._render_string("stat.userconfig", "User configuration summary: {call} has {count} stored preference {noun}.", call=call.upper(), count=len(prefs), noun=noun) + "\r\n"

    async def _cmd_stat_named(self, _call: str, _arg: str | None, name: str) -> str:
        if name in {"spots", "spot"}:
            c = await self.store.count_spots()
            return f"Spot summary: {c} stored spot{'s' if c != 1 else ''}.\r\n"
        if name in {"users", "user"}:
            return f"User session summary: {self.session_count} active session{'s' if self.session_count != 1 else ''}.\r\n"
        if name in {"msg", "messages"}:
            total, unread = await self.store.message_counts(_call)
            return (
                self._render_string("stat.msg", "Message summary: {total} total, {unread} unread.", total=total, unread=unread) + "\r\n"
                + self._render_string("stat.msg_wire", "Message summary: total={total} unread={unread}", total=total, unread=unread) + "\r\n"
            )
        if name in {"announce", "chat", "wcy", "wwv", "wx"}:
            c = len(await self.store.list_bulletins(name, limit=200))
            noun = "entry" if c == 1 else "entries"
            return (
                self._render_string("stat.bulletin", "{name} summary: {count} stored {noun}.", name=name.upper(), count=c, noun=noun) + "\r\n"
                + self._render_string("stat.bulletin_wire", "stat/{name}: {count}", name=name, count=c) + "\r\n"
            )
        if name in {"route", "routes"}:
            if not self._link_stats_fn:
                return "Route summary: 0 live peer links.\r\nstat/route: 0\r\n"
            stats = await self._link_stats_fn()
            return f"Route summary: {len(stats)} live peer link{'s' if len(stats) != 1 else ''}.\r\nstat/route: {len(stats)}\r\n"
        if name in {"route_node"}:
            if not self._link_stats_fn:
                return "Route nodes: total=0 inbound=0 outbound=0\r\n"
            stats = await self._link_stats_fn()
            inbound = sum(1 for _n, st in stats.items() if bool(st.get("inbound", False)))
            outbound = max(0, len(stats) - inbound)
            return f"Route nodes: {len(stats)} total, {inbound} accepted, {outbound} dial-out.\r\n"
        if name in {"route_user"}:
            users = self.session_count
            peers = len(await self._link_stats_fn()) if self._link_stats_fn else 0
            return f"Route users: {users} active user session{'s' if users != 1 else ''} across {peers} peer link{'s' if peers != 1 else ''}.\r\n"
        if name in {"pc19list"}:
            calls = sorted({s.call.upper() for s in self._sessions.values()})
            on = []
            for c in calls:
                v = (await self._get_pref(c, "routepc19") or "").strip().lower()
                if v in {"1", "on", "yes", "true"}:
                    on.append(c)
            if not on:
                return "PC19 routing is not enabled for any local calls.\r\n"
            return f"PC19 routing enabled for {len(on)} call{'s' if len(on) != 1 else ''}: {','.join(on[:30])}\r\n"
        if name in {"queue", "channel"}:
            if not self._link_stats_fn:
                if name == "queue":
                    return "Queue summary: peers=0 queued=0 rx=0 tx=0 dropped=0 policy_drop=0\r\n"
                return "Channel summary: peers=0 inbound=0 outbound=0 rx=0 tx=0 dropped=0 policy_drop=0\r\n"
            stats = await self._link_stats_fn()
            rx = sum(int(st.get("parsed_frames", 0)) for st in stats.values())
            tx = sum(int(st.get("sent_frames", 0)) for st in stats.values())
            dropped = sum(int(st.get("dropped_frames", 0)) for st in stats.values())
            policy_drop = sum(int(st.get("policy_dropped", 0)) for st in stats.values())
            if name == "queue":
                queued = max(0, dropped + policy_drop)
                return (
                    f"Queue summary: {len(stats)} peer{'s' if len(stats) != 1 else ''}, "
                    f"{queued} queued item{'s' if queued != 1 else ''}, "
                    f"RX {rx}, TX {tx}, dropped {dropped}, policy drops {policy_drop}.\r\n"
                    f"Queue summary: peers={len(stats)} queued={queued} rx={rx} tx={tx} dropped={dropped} policy_drop={policy_drop}\r\n"
                )
            inbound = sum(1 for st in stats.values() if bool(st.get("inbound", False)))
            outbound = max(0, len(stats) - inbound)
            return (
                f"Channel summary: {len(stats)} peer{'s' if len(stats) != 1 else ''}, "
                f"{inbound} accepted, {outbound} dial-out, "
                f"RX {rx}, TX {tx}, dropped {dropped}, policy drops {policy_drop}.\r\n"
                f"Channel summary: peers={len(stats)} inbound={inbound} outbound={outbound} rx={rx} tx={tx} dropped={dropped} policy_drop={policy_drop}\r\n"
            )
        if name in {"db"}:
            spots = await self.store.count_spots()
            reg = len(await self.store.list_user_registry(limit=1000))
            return (
                self._render_string("stat.db", "Database summary: {spots} stored spot(s), {registry} registry record(s).", spots=spots, registry=reg) + "\r\n"
                + self._render_string("stat.db_wire", "Database summary: spots={spots} registry={registry}", spots=spots, registry=reg) + "\r\n"
            )
        if name in {"userconfig"}:
            prefs = await self.store.list_user_prefs(_call)
            return f"User configuration summary: {_call.upper()} has {len(prefs)} stored preference entr{'y' if len(prefs) == 1 else 'ies'}.\r\n"
        if name in {"nodeconfig"}:
            prefs = await self.store.list_user_prefs(self.config.node.node_call)
            return f"Node configuration summary: {self.config.node.node_call} has {len(prefs)} stored preference entr{'y' if len(prefs) == 1 else 'ies'}.\r\n"
        if name in {"proto", "protocol"}:
            if not self._link_stats_fn:
                return "Protocol summary: peers=0 known=0 ok=0 degraded=0 flapping=0 stale=0 unknown=0\r\n"
            stats = await self._link_stats_fn()
            node_proto = await self._node_proto_map()
            now = int(datetime.now(timezone.utc).timestamp())
            stale_mins_default, _flap_threshold = self._proto_thresholds(node_proto)
            known = 0
            ok = 0
            degraded = 0
            flapping = 0
            stale = 0
            unknown = 0
            for peer in sorted(stats):
                state, _proto_txt, health = self._proto_state_for_peer(node_proto, peer)
                has_state = any(
                    state[k] for k in ("pc24_call", "pc24_flag", "pc50_call", "pc50_count", "pc51_to", "pc51_from", "pc51_value")
                )
                if not has_state:
                    unknown += 1
                    continue
                known += 1
                last_epoch = int(state["last_epoch"]) if state["last_epoch"].isdigit() else 0
                if last_epoch <= 0 or now - last_epoch > stale_mins_default * 60:
                    stale += 1
                elif health == "flapping":
                    flapping += 1
                elif health == "degraded":
                    degraded += 1
                elif health == "ok":
                    ok += 1
                else:
                    unknown += 1
            return (
                f"Protocol summary: peers={len(stats)} known={known} ok={ok} "
                f"degraded={degraded} flapping={flapping} stale={stale} unknown={unknown}\r\n"
            )
        if name in {"protohistory", "prothist"}:
            if not self._link_stats_fn:
                return "Protocol history summary: peers=0 with_history=0 events=0 last_epoch=0\r\n"
            stats = await self._link_stats_fn()
            node_proto = await self._node_proto_map()
            pfilter = ((_arg or "").strip()).lower()
            total_events = 0
            with_history = 0
            last_epoch = 0
            for peer in sorted(stats):
                if pfilter and pfilter not in peer.lower():
                    continue
                state, _proto_txt, _health = self._proto_state_for_peer(node_proto, peer)
                hist = self._parse_proto_history(state.get("history", "[]"))
                if hist:
                    with_history += 1
                    total_events += len(hist)
                    ep = int(hist[-1].get("epoch", 0) or 0)
                    if ep > last_epoch:
                        last_epoch = ep
            peers_seen = sum(1 for p in stats if not pfilter or pfilter in p.lower())
            return f"Protocol history summary: peers={peers_seen} with_history={with_history} events={total_events} last_epoch={last_epoch}\r\n"
        if name in {"protoevents", "protev"}:
            node_proto = await self._node_proto_map()
            rows = self._collect_proto_events(node_proto, peer_filter=(_arg or "").strip(), limit=200)
            if not rows:
                return "Protocol event summary: events=0 keys=0\r\n"
            keys: dict[str, int] = {}
            for r in rows:
                k = str(r.get("key", ""))
                keys[k] = keys.get(k, 0) + 1
            top = sorted(keys.items(), key=lambda kv: (-kv[1], kv[0]))[:5]
            tops = ",".join(f"{k}:{v}" for k, v in top)
            return f"Protocol event summary: events={len(rows)} keys={len(keys)} top={tops}\r\n"
        if name in {"protoalerts", "protalerts"}:
            if not self._link_stats_fn:
                return "Protocol alert summary: total=0 degraded=0 flapping=0 stale=0 acked=0\r\n"
            stats = await self._link_stats_fn()
            node_cfg = await self._node_proto_map()
            now = int(datetime.now(timezone.utc).timestamp())
            stale_mins, _ = self._proto_thresholds(node_cfg)
            pfilter = ((_arg or "").strip()).lower()
            degraded = 0
            flapping = 0
            stale = 0
            acked = 0
            for peer in sorted(stats):
                if pfilter and pfilter not in peer.lower():
                    continue
                state, _proto_txt, health = self._proto_state_for_peer(node_cfg, peer)
                has_state = any(
                    state[k] for k in ("pc24_call", "pc24_flag", "pc50_call", "pc50_count", "pc51_to", "pc51_from", "pc51_value")
                )
                if not has_state:
                    continue
                last_epoch = int(state["last_epoch"]) if state["last_epoch"].isdigit() else 0
                ack_epoch = self._proto_alert_ack_epoch(node_cfg, peer)
                if ack_epoch > 0 and last_epoch > 0 and ack_epoch >= last_epoch:
                    acked += 1
                    continue
                if last_epoch <= 0 or now - last_epoch > stale_mins * 60:
                    stale += 1
                    continue
                if health == "flapping":
                    flapping += 1
                elif health == "degraded":
                    degraded += 1
            total = degraded + flapping + stale + acked
            return f"Protocol alert summary: total={total} degraded={degraded} flapping={flapping} stale={stale} acked={acked}\r\n"
        if name in {"protoacks", "protacks"}:
            node_cfg = await self._node_proto_map()
            pfilter = ((_arg or "").strip()).lower()
            total = 0
            suppressed = 0
            expired = 0
            for k, v in sorted(node_cfg.items()):
                if not (k.startswith("proto.peer.") and k.endswith(".alert_ack_epoch")):
                    continue
                peer = k[len("proto.peer.") : -len(".alert_ack_epoch")]
                if pfilter and pfilter not in peer.lower():
                    continue
                try:
                    ack_epoch = int(v)
                except ValueError:
                    continue
                if ack_epoch <= 0:
                    continue
                total += 1
                last_raw = node_cfg.get(f"proto.peer.{peer}.last_epoch", "0")
                try:
                    last_epoch = int(last_raw)
                except ValueError:
                    last_epoch = 0
                if last_epoch > 0 and ack_epoch >= last_epoch:
                    suppressed += 1
                else:
                    expired += 1
            return f"Protocol ack summary: total={total} suppressed={suppressed} expired={expired}\r\n"
        return f"No summary is available for stat/{name}.\r\n"

    async def _cmd_get_keps(self, call: str, _arg: str | None) -> str:
        now = int(datetime.now(timezone.utc).timestamp())
        await self.store.set_user_pref(call.upper(), "keps_last_request_epoch", str(now), now)
        self._log_event("keps", f"{call} get/keps")
        return "Keplerian elements request accepted.\r\nget/keps: Ok\r\n"

    async def _cmd_debug_top(self, call: str, arg: str | None) -> str:
        text = (arg or "").strip().lower()
        if not text:
            return await self._cmd_show_named_status(call, None, "debug")
        if text in {"1", "on", "yes", "true"}:
            return await self._cmd_set_named_var(call, None, "debug", "on")
        if text in {"0", "off", "no", "false"}:
            return await self._cmd_unset_named_var(call, None, "debug")
        return "Usage: debug [on|off]\r\n"

    async def _cmd_rcmd_top(self, call: str, arg: str | None) -> str:
        text = (arg or "").strip()
        if not text:
            return await self._cmd_show_named_status(call, None, "rcmd")
        return await self._cmd_set_named_var(call, text, "rcmd", "")

    async def _cmd_privilege_top(self, call: str, _arg: str | None) -> str:
        row = await self.store.get_user_registry(call)
        p = ""
        if row:
            p = str(row["privilege"] or "").strip().lower()
        if not p:
            p = (await self._get_pref(call, "privilege") or "").strip().lower()
        if not p:
            p = "user"
        return f"Access level for {call.upper()}: {p}\r\n"

    async def _cmd_save(self, call: str, _arg: str | None) -> str:
        now = int(datetime.now(timezone.utc).timestamp())
        writes = 0
        s = self._find_session(call)
        if s:
            desired: dict[str, str] = {
                "echo": "on" if s.echo else "off",
                "here": "on" if s.here else "off",
                "beep": "on" if s.beep else "off",
                "language": s.language,
                "profile": s.peer_profile,
                "catchup": "on" if s.catchup else "off",
            }
            desired.update({k: str(v) for k, v in s.vars.items()})
            for k, v in desired.items():
                cur = await self._get_pref(call, k)
                if cur != v:
                    await self.store.set_user_pref(call.upper(), k, v, now)
                    writes += 1
        await self.store.set_user_pref(call.upper(), "last_save_epoch", str(now), now)
        writes += 1
        self._log_event("save", f"{call} save writes={writes}")
        prefs = await self.store.list_user_prefs(call)
        spots = await self.store.count_spots()
        return f"Saved {writes} item(s): {len(prefs)} preference(s), {spots} spot(s).\r\n"

    async def _cmd_dbdelkey(self, _call: str, arg: str | None) -> str:
        denied = await self._require_privilege(_call, 2, "dbdelkey")
        if denied:
            return denied
        toks = [t for t in (arg or "").split() if t]
        if len(toks) != 2:
            return "Usage: dbdelkey <call> <key>\r\n"
        target = toks[0].upper()
        key = toks[1].lower()
        removed = await self.store.delete_user_pref(target, key)
        self._log_event("db", f"{_call} dbdelkey {target} {key} removed={removed}")
        if removed:
            return f"Database key {key} removed for {target}.\r\n"
        return f"Database key {key} was not present for {target}.\r\n"

    async def _cmd_dbimport(self, _call: str, arg: str | None) -> str:
        denied = await self._require_privilege(_call, 2, "dbimport")
        if denied:
            return denied
        file_path = (arg or "").strip()
        if not file_path:
            return "Usage: dbimport <file>\r\n"
        path = Path(file_path)
        if not path.exists():
            return f"dbimport: file not found {file_path}\r\n"
        try:
            imported, skipped = await import_spot_file(self.store, file_path)
        except Exception as exc:
            return f"dbimport: {exc}\r\n"
        self._log_event("db", f"{_call} dbimport {file_path} imported={imported} skipped={skipped}")
        return f"Database import complete: {imported} imported, {skipped} skipped.\r\n"

    async def _cmd_dbremove(self, _call: str, arg: str | None) -> str:
        denied = await self._require_privilege(_call, 2, "dbremove")
        if denied:
            return denied
        toks = [t for t in (arg or "").split() if t]
        if len(toks) != 2:
            return "Usage: dbremove <table> <call>\r\n"
        table = toks[0].lower()
        target = toks[1].upper()
        if table not in {"user", "registry", "prefs", "vars", "usdb", "buddy", "startup", "filters"}:
            return "Usage: dbremove <table=user|registry|prefs|vars|usdb|buddy|startup|filters> <call>\r\n"
        if table == "registry":
            removed = await self.store.delete_user_registry(target)
            self._log_event("db", f"{_call} dbremove registry {target} removed={removed}")
            return f"Removed {removed} registry entr{'y' if removed == 1 else 'ies'} for {target}.\r\n"
        if table in {"prefs", "vars", "usdb", "buddy", "startup", "filters"}:
            counts = await self.store.delete_user_data(target, scopes={table})
            key_map = {
                "prefs": "prefs",
                "vars": "vars",
                "usdb": "usdb",
                "buddy": "buddy",
                "startup": "startup",
                "filters": "filters",
            }
            key = key_map[table]
            removed = int(counts.get(key, 0))
            self._log_event("db", f"{_call} dbremove {table} {target} removed={removed}")
            return f"Removed {removed} {table} entr{'y' if removed == 1 else 'ies'} for {target}.\r\n"
        counts = await self.store.delete_user_data(target)
        removed = sum(counts.values())
        self._log_event(
            "db",
            f"{_call} dbremove user {target} removed={removed} "
            f"prefs={counts.get('prefs', 0)} vars={counts.get('vars', 0)} "
            f"usdb={counts.get('usdb', 0)} buddy={counts.get('buddy', 0)} "
            f"startup={counts.get('startup', 0)} filters={counts.get('filters', 0)}",
        )
        return (
            f"Removed {removed} stored item(s) for {target}: "
            f"prefs={counts.get('prefs', 0)} vars={counts.get('vars', 0)} "
            f"usdb={counts.get('usdb', 0)} buddy={counts.get('buddy', 0)} "
            f"startup={counts.get('startup', 0)} filters={counts.get('filters', 0)}\r\n"
        )

    async def _cmd_dxqsl_export(self, call: str, arg: str | None) -> str:
        path = (arg or "").strip() or f"/tmp/pycluster-dxqsl-export-{int(datetime.now(timezone.utc).timestamp())}.dat"
        p = Path(path)
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            return f"dxqsl_export: {exc}\r\n"
        now = int(datetime.now(timezone.utc).timestamp())
        await self.store.set_user_pref(call.upper(), "dxqsl_export_path", path, now)
        await self.store.set_user_pref(call.upper(), "dxqsl_export_epoch", str(now), now)
        return f"DXQSL export written to {path}.\r\n"

    async def _cmd_dxqsl_import(self, call: str, arg: str | None) -> str:
        path = (arg or "").strip()
        if not path:
            return "Usage: dxqsl_import <file>\r\n"
        if not Path(path).exists():
            return f"dxqsl_import: file not found {path}\r\n"
        now = int(datetime.now(timezone.utc).timestamp())
        await self.store.set_user_pref(call.upper(), "dxqsl_import_path", path, now)
        await self.store.set_user_pref(call.upper(), "dxqsl_import_epoch", str(now), now)
        return f"DXQSL import loaded from {path}.\r\n"

    async def _cmd_init(self, call: str, _arg: str | None) -> str:
        denied = await self._require_privilege(call, 2, "init")
        if denied:
            return denied
        ctrl_denied = await self._require_control_enabled("init")
        if ctrl_denied:
            return ctrl_denied
        for sid, s in list(self._sessions.items()):
            try:
                await self._apply_prefs_to_session(s)
                await self._load_filters_for_call(s.call)
            except Exception:
                LOG.exception("init reload failed sid=%s call=%s", sid, s.call)
        self._log_event("control", f"{call} init")
        return f"Reloaded preferences and filters for {len(self._sessions)} session(s).\r\n"

    async def _cmd_rinit(self, call: str, _arg: str | None) -> str:
        denied = await self._require_privilege(call, 2, "rinit")
        if denied:
            return denied
        ctrl_denied = await self._require_control_enabled("rinit")
        if ctrl_denied:
            return ctrl_denied
        out = await self._cmd_init(call, None)
        if not self._servers:
            self._log_event("control", f"{call} rinit no-listener")
            return out.strip() + " Listener restart skipped because telnet is not running.\r\n"
        await self.rebind_listeners()
        self._log_event("control", f"{call} rinit listener-restarted")
        return out.strip() + " Telnet listeners restarted.\r\n"

    async def _cmd_shutdown(self, call: str, _arg: str | None) -> str:
        denied = await self._require_privilege(call, 2, "shutdown")
        if denied:
            return denied
        ctrl_denied = await self._require_control_enabled("shutdown")
        if ctrl_denied:
            return ctrl_denied
        self._log_event("control", f"{call} shutdown")
        if self._servers:
            await self.stop()
        ids = list(self._sessions.keys())
        await self._close_sessions(ids, "Server shutdown requested")
        return f"Shutdown requested: listener stopped, {len(ids)} session(s) closed.\r\n"

    async def _cmd_kill(self, call: str, arg: str | None) -> tuple[bool, str]:
        denied = await self._require_privilege(call, 2, "kill")
        if denied:
            return True, denied
        ctrl_denied = await self._require_control_enabled("kill")
        if ctrl_denied:
            return True, ctrl_denied
        toks = [t for t in (arg or "").split() if t]
        if not toks:
            return True, "Usage: kill <call|all>\r\n"
        target = toks[0].upper()
        ids: list[int] = []
        if self._is_all_token(target):
            ids = [sid for sid, s in self._sessions.items() if s.call != call.upper()]
        else:
            ids = [sid for sid, s in self._sessions.items() if s.call == target]
        closed = await self._close_sessions(ids, f"Disconnected by {call} using kill")
        self._log_event("control", f"{call} kill {target} closed={closed}")
        return True, f"Disconnected {closed} session(s) for {target}.\r\n"

    async def _cmd_spoof(self, call: str, arg: str | None) -> str:
        denied = await self._require_privilege(call, 2, "spoof")
        if denied:
            return denied
        ctrl_denied = await self._require_control_enabled("spoof")
        if ctrl_denied:
            return ctrl_denied
        text = (arg or "").strip()
        if not text:
            return "Usage: spoof <call> <text> | spoof dx <spotter> <freq_khz> <dx_call> [info]\r\n"
        toks = [t for t in text.split() if t]
        now = int(datetime.now(timezone.utc).timestamp())
        # Spot injection: spoof dx <spotter> <freq> <dxcall> [info...]
        if len(toks) >= 5 and toks[0].lower() == "dx":
            spotter = toks[1].upper()
            try:
                freq_khz = float(toks[2])
            except ValueError:
                return "Usage: spoof dx <spotter> <freq_khz> <dx_call> [info]\r\n"
            dx_call = toks[3].upper()
            if not is_valid_call(spotter) or not is_valid_call(dx_call):
                return "Usage: spoof dx <spotter> <freq_khz> <dx_call> [info]\r\n"
            info = " ".join(toks[4:])
            raw = "^".join(
                [
                    f"{freq_khz:.1f}",
                    dx_call,
                    str(now),
                    info,
                    spotter,
                    "226",
                    "226",
                    self.config.node.node_call,
                ]
            )
            spot = Spot(
                freq_khz=freq_khz,
                dx_call=dx_call,
                epoch=now,
                info=info,
                spotter=spotter,
                source_node=self.config.node.node_call,
                raw=raw,
            )
            inserted = await self.store.add_spot(spot)
            if inserted:
                await self.publish_spot(spot)
            if self._on_spot_fn and inserted:
                await self._on_spot_fn(spot)
            self._log_event("control", f"{call} spoof dx {spotter} {freq_khz:.1f} {dx_call}")
            return f"Injected DX spot {freq_khz:.1f} {dx_call} from {spotter}.\r\n"
        # Message injection: spoof <from_call> <text...> (as chat bulletin)
        spoof_call = toks[0].upper()
        if not is_valid_call(spoof_call):
            return "Usage: spoof <call> <text> | spoof dx <spotter> <freq_khz> <dx_call> [info]\r\n"
        body = " ".join(toks[1:]).strip()
        if not body:
            return "Usage: spoof <call> <text> | spoof dx <spotter> <freq_khz> <dx_call> [info]\r\n"
        await self.store.add_bulletin("chat", spoof_call, "LOCAL", now, body)
        for s in self._sessions.values():
            if s.call != spoof_call:
                await self._write(s.writer, f"\r\nCHAT {spoof_call}: {body}\r\n")
        self._log_event("control", f"{call} spoof chat {spoof_call}: {body}")
        return f"Injected chat as {spoof_call}.\r\n"

    def _control_pref_call(self) -> str:
        return self.config.node.node_call.upper()

    async def _control_enabled(self) -> bool:
        raw = await self._get_pref(self._control_pref_call(), "control.enabled")
        if raw is None or not raw.strip():
            return True
        return raw.strip().lower() in {"1", "on", "yes", "true", "enable", "enabled"}

    async def _require_control_enabled(self, action: str) -> str | None:
        if await self._control_enabled():
            return None
        return f"{action}: disabled by control policy (set/control on to enable)\r\n"

    async def _cmd_show_control(self, call: str, arg: str | None) -> str:
        explicit = False
        limit = 20
        reset = False
        toks = [t for t in (arg or "").split() if t]
        i = 0
        while i < len(toks):
            tok = toks[i].strip().lower()
            if tok in {"--reset", "reset"}:
                reset = True
                i += 1
                continue
            if tok.isdigit():
                explicit = True
                limit = max(1, min(int(tok), 200))
                i += 1
                continue
            return "Usage: show/control [limit] [--reset]\r\n"
        if reset:
            denied = await self._require_privilege(call, 2, "show/control --reset")
            if denied:
                return denied
            removed = sum(1 for e in self._events if e.category == "control")
            self._events = [e for e in self._events if e.category != "control"]
            self._log_event("control", f"{call} show/control --reset removed={removed}")
        if not explicit:
            page = await self._page_size_for(call)
            if page > 0:
                limit = min(limit, page)
        enabled = await self._control_enabled()
        state = "on" if enabled else "off"
        lines = [f"System control is {state}."]
        rows = [e for e in self._events if e.category == "control"][-limit:]
        if not rows:
            lines.append("Recent control events: none")
            return await self._format_console_lines(call, lines)
        lines.append(f"Recent control events: {len(rows)}")
        for e in rows:
            ts = datetime.fromtimestamp(e.epoch, tz=timezone.utc).strftime("%-d-%b-%Y %H%MZ")
            lines.append(f"{ts} {e.text}")
        return await self._format_console_lines(call, lines)

    async def _cmd_set_control(self, call: str, arg: str | None) -> str:
        denied = await self._require_privilege(call, 2, "set/control")
        if denied:
            return denied
        toks = [t for t in (arg or "").split() if t]
        if len(toks) != 1 or toks[0].lower() not in {"on", "off"}:
            return "Usage: set/control <on|off>\r\n"
        state = toks[0].lower()
        await self._persist_pref(self._control_pref_call(), "control.enabled", state)
        self._log_event("control", f"{call} set/control {state}")
        return f"System control commands {'enabled' if state == 'on' else 'disabled'}.\r\n"

    async def _cmd_unset_control(self, call: str, _arg: str | None) -> str:
        denied = await self._require_privilege(call, 2, "unset/control")
        if denied:
            return denied
        removed = await self.store.delete_user_pref(self._control_pref_call(), "control.enabled")
        self._log_event("control", f"{call} unset/control removed={removed}")
        return f"System control policy restored to the default enabled state ({removed} override removed).\r\n"

    async def _cmd_nested_dispatch(self, call: str, arg: str | None, name: str) -> str:
        text = (arg or "").strip()
        if not text:
            return f"Usage: {name} <command>\r\n"
        low = text.lower()
        if low.startswith(("run ", "do ", "type ", "merge ", "kill", "shutdown", "rinit", "init")):
            return f"{name}: nested control commands are disabled\r\n"
        if not self._startup_command_allowed(text):
            return f"{name}: blocked unsafe command\r\n"
        keep_going, out = await self._execute_command(call, text)
        if not keep_going:
            return f"{name}: command requested disconnect\r\n"
        return out or "\r\n"

    async def _cmd_dbcreate(self, call: str, _arg: str | None) -> str:
        denied = await self._require_privilege(call, 2, "dbcreate")
        if denied:
            return denied
        counts = await self.store.optimize()
        self._log_event(
            "db",
            f"{call} dbcreate spots={counts.get('spots', 0)} bulletins={counts.get('bulletins', 0)}",
        )
        return (
            "Database structures verified: "
            f"{counts.get('spots', 0)} spots, "
            f"{counts.get('messages', 0)} messages, "
            f"{counts.get('bulletins', 0)} bulletins, "
            f"{counts.get('user_prefs', 0)} preferences.\r\n"
        )

    async def _cmd_dbupdate(self, call: str, _arg: str | None) -> str:
        denied = await self._require_privilege(call, 2, "dbupdate")
        if denied:
            return denied
        counts = await self.store.optimize()
        self._log_event(
            "db",
            f"{call} dbupdate spots={counts.get('spots', 0)} messages={counts.get('messages', 0)}",
        )
        return (
            "Database refresh complete: "
            f"{counts.get('spots', 0)} spots, "
            f"{counts.get('messages', 0)} messages, "
            f"{counts.get('bulletins', 0)} bulletins, "
            f"{counts.get('user_prefs', 0)} preferences.\r\n"
        )

    async def _cmd_dbexport(self, call: str, arg: str | None) -> str:
        denied = await self._require_privilege(call, 2, "dbexport")
        if denied:
            return denied
        now = int(datetime.now(timezone.utc).timestamp())
        path = (arg or "").strip() or f"/tmp/pycluster-export-{now}.sql"
        lines = await self.store.export_sql_dump(path)
        self._log_event("db", f"{call} dbexport {path} lines={lines}")
        return f"Database export written to {path} ({lines} line(s)).\r\n"

    async def _cmd_export_users(self, call: str, arg: str | None) -> str:
        denied = await self._require_privilege(call, 2, "export_users")
        if denied:
            return denied
        now = int(datetime.now(timezone.utc).timestamp())
        path = (arg or "").strip() or f"/tmp/pycluster-users-{now}.csv"
        rows = await self.store.export_users_csv(path)
        self._log_event("db", f"{call} export_users {path} rows={rows}")
        return f"User export written to {path} ({rows} row(s)).\r\n"

    async def _cmd_send_config(self, call: str, arg: str | None) -> str:
        text = (arg or "").strip()
        cfg = await self._cmd_show_configuration(call, None)
        if not text:
            return cfg
        denied = await self._require_privilege(call, 2, "send_config")
        if denied:
            return denied
        path = Path(text)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(cfg, encoding="utf-8")
        except Exception as exc:
            return f"send_config: {exc}\r\n"
        self._log_event("config", f"{call} send_config {path}")
        return f"Configuration snapshot written to {path}.\r\n"

    async def _cmd_pc(self, _call: str, _arg: str | None) -> str:
        supported = ("11", "24", "50", "61", "92", "93")
        text = (_arg or "").strip()
        if not text:
            prefs = await self._load_prefs_for_call(_call)
            route = prefs.get("routepc19", "off")
            lines = [
                "PC capability summary:",
                f"  Supported: {','.join(f'PC{x}' for x in supported)}",
                f"  Route PC19: {route}",
                f"  pc: supported={','.join(supported)} routepc19={route}",
            ]
            return await self._format_console_lines(_call, lines)
        toks = [t for t in text.split() if t]
        tok = toks[0].upper().removeprefix("PC")
        if tok not in supported:
            return "Usage: pc [11|24|50|61|92|93]\r\n"
        mapping = {
            "11": ("announce", "relay.announce"),
            "24": ("dx", "relay.spots"),
            "50": ("talk", "relay.chat"),
            "61": ("route", "routepc19"),
            "92": ("wcy", "relay.wcy"),
            "93": ("wwv", "relay.wwv"),
        }
        feature, pref_key = mapping[tok]
        if len(toks) >= 2:
            val = toks[1].strip().lower()
            if val not in {"on", "off"}:
                return "Usage: pc [11|24|50|61|92|93] [on|off]\r\n"
            await self._persist_pref(_call.upper(), pref_key, val)
            self._log_event("pc", f"{_call} pc{tok} {val}")
        prefs = await self._load_prefs_for_call(_call)
        state = prefs.get(pref_key, "on" if pref_key.startswith("relay.") else "off")
        source = "user" if pref_key in prefs else "default"
        return (
            f"PC{tok} support is available for {feature}; current state is {state} ({source}).\r\n"
            f"pc{tok}: supported=yes feature={feature} state={state} source={source}\r\n"
        )

    async def _cmd_agwrestart(self, call: str, _arg: str | None) -> str:
        denied = await self._require_privilege(call, 2, "agwrestart")
        if denied:
            return denied
        now = int(datetime.now(timezone.utc).timestamp())
        node_call = self._control_pref_call()
        seq_raw = await self.store.get_user_pref(node_call, "agwrestart_count")
        try:
            seq = int(seq_raw or "0")
        except ValueError:
            seq = 0
        seq += 1
        await self.store.set_user_pref(node_call, "agwrestart_epoch", str(now), now)
        await self.store.set_user_pref(node_call, "agwrestart_count", str(seq), now)
        self._log_event("control", f"{call} agwrestart count={seq}")
        return f"AGW restart requested at epoch {now} (count {seq}).\r\n"

    async def _cmd_demonstrate(self, call: str, arg: str | None) -> str:
        text = (arg or "").strip()
        if not text:
            return "Usage: demonstrate <command>\r\n"
        low = text.lower()
        if low.startswith(("demonstrate", "run ", "do ", "type ", "merge ")):
            return "demonstrate: nested dispatch is disabled\r\n"
        if not self._startup_command_allowed(text):
            return "demonstrate: blocked unsafe command\r\n"
        keep_going, out = await self._execute_command(call, text)
        if not keep_going:
            return "demonstrate: command requested disconnect\r\n"
        body = out if out else "\r\n"
        return f"demonstrate: {text}\r\n{body}"

    async def _cmd_sysop(self, call: str, arg: str | None) -> str:
        denied = await self._require_privilege(call, 2, "sysop")
        if denied:
            return denied
        text = (arg or "").strip()
        if not text:
            return await self._cmd_show_registered(call, call)
        first, _, rest = text.partition(" ")
        key = f"sysop/{first.lower()}"
        handler = self._build_registry().get(key)
        if not handler:
            return "?\r\n"
        return await handler(call, rest or None)

    async def _cmd_forward_latlong(self, call: str, arg: str | None) -> str:
        if not arg:
            return "Usage: forward/latlong <lat> <lon>\r\n"
        toks = [t for t in arg.split() if t]
        if len(toks) != 2:
            return "Usage: forward/latlong <lat> <lon>\r\n"
        try:
            lat = float(toks[0])
            lon = float(toks[1])
        except ValueError:
            return "Usage: forward/latlong <lat> <lon>\r\n"
        if lat < -90 or lat > 90 or lon < -180 or lon > 180:
            return "forward/latlong: range error (-90..90, -180..180)\r\n"
        now = int(datetime.now(timezone.utc).timestamp())
        await self.store.set_user_pref(call.upper(), "forward_lat", f"{lat:.4f}", now)
        await self.store.set_user_pref(call.upper(), "forward_lon", f"{lon:.4f}", now)
        await self.store.upsert_user_registry(call.upper(), now, qra=coords_to_locator(lat, lon))
        self._log_event("forward", f"{call} latlong {lat:.4f},{lon:.4f}")
        return f"Forward latitude/longitude set to {lat:.4f}, {lon:.4f}.\r\n"

    async def _cmd_forward_opername(self, call: str, arg: str | None) -> str:
        text = (arg or "").strip()
        if not text:
            return "Usage: forward/opername <text>\r\n"
        now = int(datetime.now(timezone.utc).timestamp())
        await self.store.set_user_pref(call.upper(), "forward_opername", text, now)
        self._log_event("forward", f"{call} opername {text}")
        return f"Forward operator name set to {text}.\r\n"

    async def _cmd_ping(self, _call: str, arg: str | None) -> str:
        tgt = (arg or "").strip() or self.config.node.node_call
        return f"PONG {tgt}\r\n"

    async def _cmd_blank(self, _call: str, _arg: str | None) -> str:
        return "\r\n"

    async def _cmd_echo(self, _call: str, arg: str | None) -> str:
        return ((arg or "") + "\r\n") if arg else "\r\n"

    async def _cmd_who(self, call: str, arg: str | None) -> str:
        lines: list[str] = []
        if self._sessions:
            lines.append("Connected users:")
            for s in sorted(self._sessions.values(), key=lambda x: x.call):
                age = datetime.now(timezone.utc) - s.connected_at
                mins = int(age.total_seconds() // 60)
                lines.append(f"  {s.call:<12} online {mins:>4}m")
        if self._link_stats_fn:
            stats = await self._link_stats_fn()
            if stats:
                lines.append("Connected peers:")
                for name in sorted(stats):
                    st = stats[name]
                    direction = "inbound" if bool(st.get("inbound", False)) else "outbound"
                    profile = str(st.get("profile", "dxspider"))
                    lines.append(f"  {name:<12} {direction:<8} profile={profile}")
        if not lines:
            return "No users or peers connected\r\n"
        return await self._format_console_lines(call, lines)

    async def _cmd_apropos(self, _call: str, arg: str | None) -> str:
        needle = (arg or "").strip().lower()
        if not needle:
            return "Usage: apropos <pattern>\r\n"
        reg = self._build_registry()
        matches = sorted(k for k in reg if needle in k)
        visible: list[str] = []
        for cmd in matches:
            if await self._command_visible_for(_call, cmd):
                visible.append(cmd)
        if not matches:
            return f"No commands match {needle}.\r\n"
        if not visible:
            return f"No visible commands match {needle}.\r\n"
        lines = [f"Commands matching {needle} ({min(len(visible), 120)}):"]
        lines.extend(f"  {cmd}" for cmd in visible[:120])
        return await self._format_console_lines(_call, lines)

    async def _cmd_directory(self, _call: str, _arg: str | None) -> str:
        lines = [
            "Directories:",
            f"  Database: {self.config.store.sqlite_path}",
            "  Log: in-memory",
            "  Filters: in-memory",
        ]
        return await self._format_console_lines(_call, lines)

    async def _cmd_dbshow(self, _call: str, _arg: str | None) -> str:
        dbp = Path(self.config.store.sqlite_path)
        table = ((_arg or "").strip().split() or ["summary"])[0].lower()
        spots = await self.store.count_spots()
        total_msg, unread_msg = await self.store.message_counts(_call)
        bulletins = {
            "announce": len(await self.store.list_bulletins("announce", limit=200)),
            "chat": len(await self.store.list_bulletins("chat", limit=200)),
            "wcy": len(await self.store.list_bulletins("wcy", limit=200)),
            "wwv": len(await self.store.list_bulletins("wwv", limit=200)),
            "wx": len(await self.store.list_bulletins("wx", limit=200)),
        }
        registry = len(await self.store.list_user_registry(limit=2000))
        prefs = len(await self.store.list_user_prefs(_call))
        filters = len(await self.store.list_filter_rules(_call))
        buddies = len(await self.store.list_buddies(_call))
        usdb = len(await self.store.list_usdb_entries(_call))
        size = dbp.stat().st_size if dbp.exists() else 0
        if table in {"spots", "spot"}:
            return f"Spot database entries: {spots}\r\n"
        if table in {"messages", "msg"}:
            return f"Messages: {total_msg} total, {unread_msg} unread.\r\n"
        if table in {"bulletins", "bulletin", "announce", "chat", "wcy", "wwv", "wx"}:
            if table in {"announce", "chat", "wcy", "wwv", "wx"}:
                return f"{table.upper()} bulletins: {bulletins[table]}\r\n"
            return (
                "Bulletins: "
                f"announce={bulletins['announce']} chat={bulletins['chat']} "
                f"wcy={bulletins['wcy']} wwv={bulletins['wwv']} wx={bulletins['wx']}\r\n"
            )
        if table in {"users", "registry", "user_registry"}:
            return f"Registered users: {registry}\r\n"
        if table in {"prefs", "user_prefs"}:
            return f"Preferences for {_call.upper()}: {prefs}\r\n"
        if table in {"filter", "filters"}:
            return f"Filters for {_call.upper()}: {filters}\r\n"
        if table in {"buddy", "buddies"}:
            return f"Buddies for {_call.upper()}: {buddies}\r\n"
        if table in {"usdb"}:
            return f"USDB entries for {_call.upper()}: {usdb}\r\n"
        return (
            f"dbshow: engine=sqlite path={dbp} size={size}\r\n"
            f"Database summary: sqlite at {dbp}, {size} bytes, "
            f"{spots} spots, {total_msg} messages ({unread_msg} unread), "
            f"{registry} registered users, {bulletins['announce']} announce, {bulletins['chat']} chat.\r\n"
        )

    async def _cmd_dbavail(self, _call: str, _arg: str | None) -> str:
        dbp = Path(self.config.store.sqlite_path)
        exists = dbp.exists()
        size = dbp.stat().st_size if exists else 0
        writable = dbp.parent.exists() and dbp.parent.is_dir()
        mode = "rw" if writable else "ro"
        return (
            f"SQLite database at {dbp}: exists={'yes' if exists else 'no'}, "
            f"mode={mode}, size={size} bytes.\r\n"
        )

    async def _cmd_catchup(self, call: str, _arg: str | None, enable: bool) -> str:
        s = self._find_session(call)
        if not s:
            return "Session not found\r\n"
        s.catchup = enable
        await self._persist_pref(call, "catchup", "on" if enable else "off")
        state = "enabled" if enable else "disabled"
        return f"Catch-up on login {state} for {call}.\r\n"

    async def _cmd_chat(self, call: str, arg: str | None) -> str:
        if not arg:
            return "Usage: chat <text>\r\n"
        text = arg.strip()
        now = int(datetime.now(timezone.utc).timestamp())
        await self.store.add_bulletin("chat", call, "LOCAL", now, text)
        self._log_event("chat", f"{call}: {text}")
        delivered = 0
        for s in self._sessions.values():
            if s.call != call:
                await self._write(s.writer, f"\r\nCHAT {call}: {text}\r\n")
                delivered += 1
        if self._on_chat_fn:
            await self._on_chat_fn(call, text)
        return f"chat delivered={delivered}\r\n"

    async def _cmd_join(self, call: str, arg: str | None) -> str:
        if not arg:
            return "Usage: join <group>\r\n"
        grp = arg.split()[0].lower()
        s = self._find_session(call)
        if not s:
            return "Session not found\r\n"
        cur = s.vars.get("groups.joined", "")
        vals = {x for x in cur.split(",") if x}
        vals.add(grp)
        joined = ",".join(sorted(vals))
        s.vars["groups.joined"] = joined
        await self._persist_pref(call, "groups.joined", joined)
        return f"Joined group {grp}.\r\n"

    async def _cmd_leave(self, call: str, arg: str | None) -> str:
        if not arg:
            return "Usage: leave <group>\r\n"
        grp = arg.split()[0].lower()
        s = self._find_session(call)
        if not s:
            return "Session not found\r\n"
        cur = s.vars.get("groups.joined", "")
        vals = {x for x in cur.split(",") if x}
        vals.discard(grp)
        joined = ",".join(sorted(vals))
        s.vars["groups.joined"] = joined
        await self._persist_pref(call, "groups.joined", joined)
        return f"Left group {grp}.\r\n"

    async def _cmd_post_bulletin(self, call: str, arg: str | None, name: str) -> str:
        if not arg:
            return f"Usage: {name} <text>\r\n"
        text = arg.strip()
        now = int(datetime.now(timezone.utc).timestamp())
        await self.store.add_bulletin(name, call, "LOCAL", now, text)
        self._log_event(name, f"{call}: {text}")
        if self._on_bulletin_fn:
            await self._on_bulletin_fn(name, call, "LOCAL", text)
        return f"{name}: accepted (local-safe)\r\n"

    async def _cmd_connect(self, _call: str, arg: str | None) -> str:
        if not self._link_connect_fn:
            return "Node-link connect unavailable\r\n"
        if not arg:
            return "Usage: connect <peer> <dsn|host port>\r\n"
        toks = [t for t in arg.split() if t]
        if len(toks) < 2:
            return "Usage: connect <peer> <dsn|host port>\r\n"
        peer = toks[0]
        if len(toks) >= 3 and "://" not in toks[1]:
            host = toks[1]
            port = toks[2]
            if not port.isdigit():
                return "Usage: connect <peer> <host> <port>\r\n"
            dsn = f"tcp://{host}:{int(port)}"
        else:
            dsn = " ".join(toks[1:])
        try:
            await self._link_connect_fn(peer, dsn)
            self._log_event("connect", f"{peer} {dsn}")
            return f"Connection attempt started for {peer} ({dsn}).\r\n"
        except Exception as exc:
            return f"Connection to {peer} failed: {exc}\r\n"

    async def _cmd_disconnect(self, _call: str, arg: str | None) -> str:
        if not self._link_disconnect_fn:
            return "Node-link disconnect unavailable\r\n"
        peer = (arg or "").strip()
        if not peer:
            return "Usage: disconnect <peer>\r\n"
        ok = await self._link_disconnect_fn(peer)
        if ok:
            self._log_event("disconnect", peer)
            return f"Disconnected {peer}.\r\n"
        return f"Peer {peer} was not found.\r\n"

    async def _cmd_links(self, call: str, arg: str | None) -> str:
        return await self._cmd_show_connect(call, arg)

    async def _cmd_announce(self, call: str, arg: str | None, scope: str = "LOCAL") -> str:
        denied = await self._require_access(call, "telnet", "announce", "announce")
        if denied:
            return denied
        text = (arg or "").strip()
        if not text:
            return "Usage: announce [full|sysop] <text>\r\n"
        now = int(datetime.now(timezone.utc).timestamp())
        await self.store.add_bulletin("announce", call, scope, now, text)
        self._log_event("announce", f"{scope} {call}: {text}")
        if self._on_bulletin_fn:
            await self._on_bulletin_fn("announce", call, scope, text)
        # Safe default: local session visibility only (no network propagation)
        return f"Announcement accepted ({scope.lower()}): {text}\r\nannounce/{scope.lower()} accepted: {text}\r\n"

    async def _cmd_post_dx_spot(self, call: str, arg: str | None) -> str:
        denied = await self._require_access(call, "telnet", "spots", "dx")
        if denied:
            return denied
        if not arg:
            return "Usage: dx <freq_khz> <dx_call> [info]\r\n"
        toks = [t for t in arg.split() if t]
        if len(toks) < 2:
            return "Usage: dx <freq_khz> <dx_call> [info]\r\n"
        try:
            freq_khz = float(toks[0])
        except ValueError:
            return "Usage: dx <freq_khz> <dx_call> [info]\r\n"
        dx_call = toks[1].upper()
        if not is_valid_call(dx_call):
            return "Usage: dx <freq_khz> <dx_call> [info]\r\n"
        info = " ".join(toks[2:]) if len(toks) > 2 else ""
        now = int(datetime.now(timezone.utc).timestamp())
        throttle = await check_spot_throttle(self.store, self.config.node.node_call, call, now)
        if throttle.enabled and throttle.recent_count >= throttle.max_per_window:
            self._log_event(
                "spot",
                f"{call} dx throttled count={throttle.recent_count} max={throttle.max_per_window} window={throttle.window_seconds}",
            )
            return f"dx: rate limited ({throttle.max_per_window} spots per {throttle.window_seconds}s)\r\n"
        source_node = self.config.node.node_call
        raw = "^".join(
            [
                f"{freq_khz:.1f}",
                dx_call,
                str(now),
                info,
                call.upper(),
                "226",
                "226",
                source_node,
            ]
        )
        spot = Spot(
            freq_khz=freq_khz,
            dx_call=dx_call,
            epoch=now,
            info=info,
            spotter=call.upper(),
            source_node=source_node,
            raw=raw,
        )
        inserted = await self.store.add_spot(spot)
        if inserted:
            await self.publish_spot(spot)
        if self._on_spot_fn and inserted:
            await self._on_spot_fn(spot)
        self._log_event("spot", f"{call} dx {freq_khz:.1f} {dx_call} {info}")
        return f"Spot posted: {freq_khz:.1f} {dx_call}\r\ndx posted {freq_khz:.1f} {dx_call}\r\n"

    async def _cmd_talk(self, call: str, arg: str | None) -> str:
        denied = await self._require_access(call, "telnet", "chat", "talk")
        if denied:
            return denied
        if not arg:
            return "Usage: talk <call|all> <text>\r\n"
        toks = arg.split()
        if len(toks) < 2:
            return "Usage: talk <call|all> <text>\r\n"
        target = toks[0].upper()
        text = " ".join(toks[1:])
        delivered = 0
        if target == "ALL":
            for s in self._sessions.values():
                if s.call != call:
                    await self._write(s.writer, f"\r\nTALK {call}: {text}\r\n")
                    delivered += 1
        else:
            t = self._find_session(target)
            if t:
                await self._write(t.writer, f"\r\nTALK {call}: {text}\r\n")
                delivered = 1
        self._log_event("talk", f"{call}->{target}: {text}")
        return f"Talk delivered to {delivered} session(s).\r\n"

    async def _cmd_msg(self, call: str, arg: str | None) -> str:
        denied = await self._require_access(call, "telnet", "chat", "msg")
        if denied:
            return denied
        if not arg:
            return self._string("messages.msg_usage", "Usage: msg <call|all> <text>") + "\r\n"
        toks = arg.split()
        if len(toks) < 2:
            return self._string("messages.msg_usage", "Usage: msg <call|all> <text>") + "\r\n"
        target = toks[0].upper()
        text = " ".join(toks[1:])
        now = int(datetime.now(timezone.utc).timestamp())
        route_node = ""
        state = "local"
        if target != "ALL":
            reg = await self.store.get_user_registry(target)
            home_node = str(reg["home_node"] or "").strip().upper() if reg else ""
            if home_node and home_node != normalize_call(self.config.node.node_call):
                route_node = home_node
                state = "pending"
        msg_id = await self.store.add_message(
            sender=call,
            recipient=target,
            epoch=now,
            body=text,
            parent_id=None,
            origin_node=normalize_call(self.config.node.node_call),
            route_node=route_node,
            delivery_state=state,
        )
        delivered = 0
        if target == "ALL":
            for s in self._sessions.values():
                if s.call != call:
                    await self._write(s.writer, f"\r\nMSG#{msg_id} {call}: {text}\r\n")
                    delivered += 1
        else:
            delivered = await self.publish_message(target, call, text, msg_id)
            if delivered:
                await self.store.set_message_delivery(msg_id, "delivered", delivered_epoch=now)
            elif route_node and self._on_message_fn:
                await self._on_message_fn(call, target, text, msg_id, None)
        self._log_event("msg", f"{call}->{target}: {text}")
        row = await self.store.get_message(msg_id)
        return self._render_string(
            "messages.msg_delivered",
            "Message #{message_id} delivered to {delivered} session(s). state={state}",
            message_id=msg_id,
            delivered=delivered,
            state=str(row["delivery_state"] or state) if row else state,
        ) + "\r\n"

    async def _cmd_send(self, call: str, arg: str | None) -> str:
        return await self._cmd_msg(call, arg)

    async def _cmd_read(self, call: str, arg: str | None) -> str:
        if not arg:
            rows = await self.store.list_messages(call, limit=20)
            if not rows:
                return self._string("messages.read_empty", "No messages.") + "\r\n"
            lines: list[str] = []
            for r in rows:
                ts = datetime.fromtimestamp(int(r["epoch"]), tz=timezone.utc).strftime("%-d-%b %H%MZ")
                flag = "N" if r["read_epoch"] is None else "R"
                status = str(r["delivery_state"] or "local")[:10]
                lines.append(f"{int(r['id']):>5} {flag} {ts} {status:<10} {r['sender']:<10} {r['body'][:38]}")
            return await self._format_console_lines(call, lines)

        tok = arg.split()[0]
        if not tok.isdigit():
            return self._string("messages.read_usage", "Usage: read [<msg_id>]") + "\r\n"
        msg_id = int(tok)
        row = await self.store.get_message_for_recipient(call, msg_id)
        if not row:
            return self._render_string("messages.read_missing", "No such message: {message_id}", message_id=msg_id) + "\r\n"
        now = int(datetime.now(timezone.utc).timestamp())
        if row["read_epoch"] is None:
            await self.store.mark_message_read(msg_id, now)
        ts = datetime.fromtimestamp(int(row["epoch"]), tz=timezone.utc).strftime("%-d-%b-%Y %H%MZ")
        lines = [
            self._render_string("messages.read_title", "Message #{message_id}", message_id=int(row["id"])),
            self._render_string("messages.read_from", "  From: {sender}", sender=row["sender"]),
            self._render_string("messages.read_to", "  To: {recipient}", recipient=row["recipient"]),
            self._render_string("messages.read_at", "  At: {timestamp}", timestamp=ts),
            self._render_string("messages.read_state", "  State: {state}", state=row["delivery_state"] or "local"),
            self._render_string("messages.read_body", "  Body: {body}", body=row["body"]),
        ]
        return await self._format_console_lines(call, lines)

    async def _cmd_reply(self, call: str, arg: str | None) -> str:
        if not arg:
            return self._string("messages.reply_usage", "Usage: reply <msg_id> <text>") + "\r\n"
        toks = arg.split()
        if len(toks) < 2 or not toks[0].isdigit():
            return self._string("messages.reply_usage", "Usage: reply <msg_id> <text>") + "\r\n"
        parent_id = int(toks[0])
        body = " ".join(toks[1:])
        parent = await self.store.get_message_for_recipient(call, parent_id)
        if not parent:
            return self._render_string("messages.read_missing", "No such message: {message_id}", message_id=parent_id) + "\r\n"
        target = str(parent["sender"]).upper()
        now = int(datetime.now(timezone.utc).timestamp())
        route_node = ""
        state = "local"
        reg = await self.store.get_user_registry(target)
        home_node = str(reg["home_node"] or "").strip().upper() if reg else ""
        if home_node and home_node != normalize_call(self.config.node.node_call):
            route_node = home_node
            state = "pending"
        new_id = await self.store.add_message(
            sender=call,
            recipient=target,
            epoch=now,
            body=body,
            parent_id=parent_id,
            origin_node=normalize_call(self.config.node.node_call),
            route_node=route_node,
            delivery_state=state,
        )
        delivered = await self.publish_message(target, call, body, new_id, parent_id=parent_id)
        if delivered:
            await self.store.set_message_delivery(new_id, "delivered", delivered_epoch=now)
        elif route_node and self._on_message_fn:
            await self._on_message_fn(call, target, body, new_id, parent_id)
        self._log_event("reply", f"{call}->{target} parent={parent_id}: {body}")
        row = await self.store.get_message(new_id)
        return self._render_string(
            "messages.reply_delivered",
            "Reply #{message_id} delivered to {delivered} session(s). state={state}",
            message_id=new_id,
            delivered=delivered,
            state=str(row["delivery_state"] or state) if row else state,
        ) + "\r\n"

    async def _cmd_show_msg_status(self, call: str, _arg: str | None) -> str:
        total, unread = await self.store.message_counts(call)
        inbox = await self.store.message_state_counts(call)
        outbox = await self.store.sent_message_state_counts(call)
        inbox_txt = ",".join(f"{k}={inbox[k]}" for k in sorted(inbox)) or "none"
        outbox_txt = ",".join(f"{k}={outbox[k]}" for k in sorted(outbox)) or "none"
        lines = [
            self._render_string(
                "messages.msg_status",
                "Messages for {call}: {total} total, {unread} unread.",
                call=call.upper(),
                total=total,
                unread=unread,
            ),
            f"  Inbox states: {inbox_txt}",
            f"  Outbox states: {outbox_txt}",
        ]
        return await self._format_console_lines(call, lines)

    async def _cmd_show_messages(self, call: str, arg: str | None) -> str:
        limit = 20
        explicit = False
        if arg and arg.split()[0].isdigit():
            explicit = True
            limit = max(1, min(int(arg.split()[0]), 200))
        rows = await self.store.list_messages(call, limit=limit)
        if not rows:
            return self._string("messages.read_empty", "No messages.") + "\r\n"
        lines = [self._string("messages.list_title", "Message list:")]
        for r in rows:
            mid = int(r["id"])
            sender = str(r["sender"])
            ts = datetime.fromtimestamp(int(r["epoch"]), tz=timezone.utc).strftime("%-d-%b %H%MZ")
            unread = "UNREAD" if r["read_epoch"] is None else "READ"
            status = str(r["delivery_state"] or "local")[:10]
            body = str(r["body"] or "")
            route = str(r["route_node"] or "").strip() or "-"
            origin = str(r["origin_node"] or "").strip() or "-"
            lines.append(f"{mid:>6} {sender:<10} {ts} {unread:<6} {status:<10} via={route:<10} from={origin:<10} {body}")
        lines = await self._apply_page_size(call, lines, explicit_limit=explicit)
        return await self._format_console_lines(call, lines)

    async def _cmd_show_outbox(self, call: str, arg: str | None) -> str:
        limit = 20
        explicit = False
        if arg and arg.split()[0].isdigit():
            explicit = True
            limit = max(1, min(int(arg.split()[0]), 200))
        rows = await self.store.list_sent_messages(call, limit=limit)
        if not rows:
            return "No sent messages.\r\n"
        lines = ["Outbox:"]
        for r in rows:
            mid = int(r["id"])
            recipient = str(r["recipient"])
            ts = datetime.fromtimestamp(int(r["epoch"]), tz=timezone.utc).strftime("%-d-%b %H%MZ")
            status = str(r["delivery_state"] or "local")[:10]
            route = str(r["route_node"] or "").strip() or "-"
            err = str(r["error_text"] or "").strip()
            body = str(r["body"] or "")
            extra = f" err={err}" if err else ""
            lines.append(f"{mid:>6} {recipient:<10} {ts} {status:<10} via={route:<10} {body}{extra}")
        lines = await self._apply_page_size(call, lines, explicit_limit=explicit)
        return await self._format_console_lines(call, lines)

    def _normalize_cmd_token(self, text: str) -> str:
        return re.sub(r"[^a-z0-9]", "", (text or "").lower())

    def _preferred_group_short(self, group: str) -> str:
        pref = {
            "show": "sh",
            "set": "se",
            "unset": "un",
            "accept": "ac",
            "reject": "re",
            "clear": "cl",
            "load": "lo",
            "stat": "st",
            "create": "cr",
            "delete": "de",
            "forward": "fo",
            "get": "ge",
        }
        return pref.get(group, group)

    def _build_shortcut_catalog(
        self, registry: dict[str, Callable[[str, str | None], Awaitable[str]]]
    ) -> list[tuple[str, str]]:
        rows: list[tuple[str, str]] = []
        canonical = self._canonical_grouped_keys(registry)
        by_group: dict[str, list[str]] = {}
        for key in canonical:
            g, _s = key.split("/", 1)
            by_group.setdefault(g, []).append(key)

        for group in sorted(by_group):
            keys = sorted(by_group[group])
            norm_by_key = {k: (self._normalize_cmd_token(k.split("/", 1)[1]) or k.split("/", 1)[1].lower()) for k in keys}
            gshort = self._preferred_group_short(group)
            used: set[str] = set()
            for key in keys:
                snorm = norm_by_key[key]
                sshort = snorm
                for i in range(1, len(snorm) + 1):
                    cand = snorm[:i]
                    if cand in used:
                        continue
                    got = self._resolve_subcommand(group, cand, registry)
                    if got and self._resolver_pick_equivalent({got, key}, registry):
                        sshort = cand
                        break
                used.add(sshort)
                rows.append((key, f"{gshort}/{sshort}"))
        # Top-level shortcut map for unique command-prefix dispatch.
        top = sorted(set(self._top_level_canonical_tokens()))
        preferred_top = {
            "bye": "b",
            "send": "s",
            "read": "r",
            "reply": "rep",
        }
        for cmd in top:
            pref = preferred_top.get(cmd)
            if pref and self._resolve_top_token(pref) == cmd:
                rows.append((cmd, pref))
                continue
            short = cmd
            for i in range(2, len(cmd) + 1):
                cand = cmd[:i]
                got = self._resolve_top_token(cand)
                if got == cmd:
                    short = cand
                    break
            rows.append((cmd, short))
        return rows

    def _emphasize_shortcut(self, full_key: str, short: str) -> str:
        if "/" not in full_key:
            need = len(self._normalize_cmd_token(short))
            out: list[str] = []
            seen = 0
            for ch in full_key:
                if ch.isalnum():
                    if seen < need:
                        out.append(ch.upper())
                        seen += 1
                    else:
                        out.append(ch.lower())
                else:
                    out.append(ch)
            return "".join(out)
        group, sub = full_key.split("/", 1)
        gshort, sshort = short.split("/", 1)
        gdisp = group[: len(gshort)].upper() + group[len(gshort) :].lower()
        need = len(self._normalize_cmd_token(sshort))
        out: list[str] = []
        seen = 0
        for ch in sub:
            if ch.isalnum():
                if seen < need:
                    out.append(ch.upper())
                    seen += 1
                else:
                    out.append(ch.lower())
            else:
                out.append(ch)
        return f"{gdisp}/" + "".join(out)

    def _canonical_grouped_keys(
        self, registry: dict[str, Callable[[str, str | None], Awaitable[str]]]
    ) -> list[str]:
        preferred = {
            "show/protoack": 0,
            "set/protoack": 0,
            "unset/protoack": 0,
            "clear/protohistory": 0,
        }
        picks: dict[tuple[object, object], str] = {}
        for key, h in registry.items():
            if "/" not in key:
                continue
            sig = (getattr(h, "__func__", h), getattr(h, "__self__", None))
            prev = picks.get(sig)
            prev_rank = preferred.get(prev, 1) if prev is not None else 99
            key_rank = preferred.get(key, 1)
            if (
                prev is None
                or key_rank < prev_rank
                or (key_rank == prev_rank and len(key) < len(prev))
                or (key_rank == prev_rank and len(key) == len(prev) and key < prev)
            ):
                picks[sig] = key
        out = sorted(picks.values())
        return out

    def _resolver_pick_equivalent(
        self,
        keys: set[str],
        registry: dict[str, Callable[[str, str | None], Awaitable[str]]],
    ) -> str | None:
        if not keys:
            return None
        if len(keys) == 1:
            return next(iter(keys))
        sigs: set[tuple[object, object]] = set()
        for k in keys:
            h = registry[k]
            sigs.add((getattr(h, "__func__", h), getattr(h, "__self__", None)))
        if len(sigs) == 1:
            return sorted(keys)[0]
        return None

    def _resolve_subcommand(self, group: str, prefix: str, registry: dict[str, Callable[[str, str | None], Awaitable[str]]]) -> str | None:
        cands = [k for k in registry if k.startswith(group + "/")]
        sub_to_key = {k.split("/", 1)[1]: k for k in cands}
        prefix = prefix.lower()
        matches = [s for s in sub_to_key if s.startswith(prefix)]
        if len(matches) == 1:
            return sub_to_key[matches[0]]
        if prefix in sub_to_key:
            return sub_to_key[prefix]
        # Separator-insensitive compatibility for variants like proto-acks/proto_acks/protoacks.
        npre = self._normalize_cmd_token(prefix)
        if not npre:
            return None
        norm_to_keys: dict[str, set[str]] = {}
        for sub, key in sub_to_key.items():
            ns = self._normalize_cmd_token(sub)
            if not ns:
                continue
            norm_to_keys.setdefault(ns, set()).add(key)
        exact = norm_to_keys.get(npre, set())
        pick = self._resolver_pick_equivalent(exact, registry)
        if pick:
            return pick
        nmatches: set[str] = set()
        for ns, keys in norm_to_keys.items():
            if ns.startswith(npre):
                nmatches.update(keys)
        pick = self._resolver_pick_equivalent(nmatches, registry)
        if pick:
            return pick
        return None

    def _resolve_group_token(self, token: str) -> str | None:
        t = token.lower()
        if t in {"sh", "show"}:
            return "show"
        groups = (
            "show",
            "set",
            "unset",
            "accept",
            "reject",
            "clear",
            "load",
            "stat",
            "create",
            "delete",
            "forward",
            "get",
            "sysop",
        )
        if t in groups:
            return t
        matches = [g for g in groups if g.startswith(t)]
        if len(matches) == 1:
            return matches[0]
        return None

    def _direct_alias_map(self) -> dict[str, str]:
        return {
            "version": "show/version",
            "dx": "show/dx",
            "users": "show/users",
            "node": "show/node",
            "cluster": "show/cluster",
            "motd": "show/motd",
            "date": "show/date",
            "time": "show/time",
            "ap": "show/apropos",
            "apropos": "show/apropos",
            "mail": "show/messages",
            "outbox": "show/outbox",
        }

    def _top_level_canonical_tokens(self) -> list[str]:
        return [
            "bye",
            "quit",
            "exit",
            "help",
            "commands",
            "status",
            "uptime",
            "blank",
            "echo",
            "who",
            "shu",
            "apropos",
            "directory",
            "dbshow",
            "dbavail",
            "catchup",
            "uncatchup",
            "chat",
            "join",
            "leave",
            "wcy",
            "wwv",
            "wx",
            "connect",
            "disconnect",
            "links",
            "run",
            "do",
            "type",
            "merge",
            "kill",
            "shutdown",
            "rinit",
            "init",
            "agwrestart",
            "dbcreate",
            "dbupdate",
            "dbexport",
            "export",
            "export_users",
            "send_config",
            "pc",
            "demonstrate",
            "debug",
            "rcmd",
            "privilege",
            "save",
            "dbdelkey",
            "dbimport",
            "dbremove",
            "dxqsl_export",
            "dxqsl_import",
            "spoof",
            "announce",
            "dx",
            "talk",
            "msg",
            "send",
            "read",
            "reply",
            "ping",
            "version",
            "users",
            "node",
            "cluster",
            "motd",
            "date",
            "time",
            "ap",
            "mail",
        ]

    def _resolve_top_token(self, token: str) -> str | None:
        t = (token or "").lower().strip()
        if not t or "/" in t:
            return None
        if t == "b":
            return "bye"
        if t in {"s", "sp"}:
            return "send"
        if t == "r":
            return "read"
        if t == "rep":
            return "reply"
        # Keep grouped families (sh/show, set, unset, ...) out of top-level resolution.
        if self._resolve_group_token(t):
            return None
        canon = self._top_level_canonical_tokens()
        if t in canon:
            return t
        if len(t) < 2:
            return None
        matches = [c for c in canon if c.startswith(t)]
        if len(matches) == 1:
            match = matches[0]
            if "_" not in match and "-" not in match:
                return match
        # Separator-insensitive compatibility for underscore/hyphen-less tokens.
        nt = self._normalize_cmd_token(t)
        if len(nt) < 2:
            return None
        norm_to_cmds: dict[str, set[str]] = {}
        for c in canon:
            nc = self._normalize_cmd_token(c)
            if not nc:
                continue
            norm_to_cmds.setdefault(nc, set()).add(c)
        exact = norm_to_cmds.get(nt, set())
        if len(exact) == 1:
            return next(iter(exact))
        return None

    async def _execute_command(self, call: str, line: str) -> tuple[bool, str]:
        started = time.monotonic()
        cmdline = line.strip()
        if not cmdline:
            return True, ""

        toks = cmdline.split()
        first = toks[0].lower()
        resolved_top = self._resolve_top_token(first)
        if resolved_top:
            first = resolved_top
        rest = " ".join(toks[1:]) if len(toks) > 1 else None

        # top-level immediate
        try:
            if first in {"bye", "quit", "exit"}:
                return False, ""
            if first in {"help", "?"}:
                return True, await self._cmd_help(call)
            if first == "commands":
                return True, await self._cmd_show_commands(call, rest)
            if first == "status":
                return True, await self._cmd_show_cluster(call, rest)
            if first == "uptime":
                return True, await self._cmd_show_uptime(call, rest)
            if first == "blank":
                return True, await self._cmd_blank(call, rest)
            if first == "echo":
                return True, await self._cmd_echo(call, rest)
            if first in {"who", "shu"}:
                return True, await self._cmd_who(call, rest)
            if first == "apropos":
                return True, await self._cmd_apropos(call, rest)
            if first == "directory":
                return True, await self._cmd_directory(call, rest)
            if first == "dbshow":
                return True, await self._cmd_dbshow(call, rest)
            if first == "dbavail":
                return True, await self._cmd_dbavail(call, rest)
            if first == "catchup":
                return True, await self._cmd_catchup(call, rest, True)
            if first == "uncatchup":
                return True, await self._cmd_catchup(call, rest, False)
            if first == "chat":
                return True, await self._cmd_chat(call, rest)
            if first == "join":
                return True, await self._cmd_join(call, rest)
            if first == "leave":
                return True, await self._cmd_leave(call, rest)
            if first == "wcy":
                return True, await self._cmd_post_bulletin(call, rest, "wcy")
            if first == "wwv":
                return True, await self._cmd_post_bulletin(call, rest, "wwv")
            if first == "wx":
                return True, await self._cmd_post_bulletin(call, rest, "wx")
            if first == "connect":
                return True, await self._cmd_connect(call, rest)
            if first == "disconnect":
                return True, await self._cmd_disconnect(call, rest)
            if first == "links":
                return True, await self._cmd_links(call, rest)
            if first in {"run", "do", "type", "merge"}:
                return True, await self._cmd_nested_dispatch(call, rest, first)
            if first == "kill":
                return await self._cmd_kill(call, rest)
            if first == "shutdown":
                return False, await self._cmd_shutdown(call, rest)
            if first == "rinit":
                return True, await self._cmd_rinit(call, rest)
            if first == "init":
                return True, await self._cmd_init(call, rest)
            if first == "agwrestart":
                return True, await self._cmd_agwrestart(call, rest)
            if first == "dbcreate":
                return True, await self._cmd_dbcreate(call, rest)
            if first == "dbupdate":
                return True, await self._cmd_dbupdate(call, rest)
            if first in {"dbexport", "export"}:
                return True, await self._cmd_dbexport(call, rest)
            if first == "export_users":
                return True, await self._cmd_export_users(call, rest)
            if first == "send_config":
                return True, await self._cmd_send_config(call, rest)
            if first == "pc":
                return True, await self._cmd_pc(call, rest)
            if first == "demonstrate":
                return True, await self._cmd_demonstrate(call, rest)
            if first == "sysop":
                return True, await self._cmd_sysop(call, rest)
            if first == "debug":
                return True, await self._cmd_debug_top(call, rest)
            if first == "rcmd":
                return True, await self._cmd_rcmd_top(call, rest)
            if first == "privilege":
                return True, await self._cmd_privilege_top(call, rest)
            if first == "save":
                return True, await self._cmd_save(call, rest)
            if first == "dbdelkey":
                return True, await self._cmd_dbdelkey(call, rest)
            if first == "dbimport":
                return True, await self._cmd_dbimport(call, rest)
            if first == "dbremove":
                return True, await self._cmd_dbremove(call, rest)
            if first == "dxqsl_export":
                return True, await self._cmd_dxqsl_export(call, rest)
            if first == "dxqsl_import":
                return True, await self._cmd_dxqsl_import(call, rest)
            if first == "spoof":
                return True, await self._cmd_spoof(call, rest)
            if first == "announce":
                scope = "LOCAL"
                text = rest or ""
                low = text.lower()
                if low.startswith("full "):
                    scope = "FULL"
                    text = text[5:]
                elif low.startswith("sysop "):
                    scope = "SYSOP"
                    text = text[6:]
                return True, await self._cmd_announce(call, text, scope)
            if first == "dx":
                rt = (rest or "").strip()
                rtoks = [t for t in rt.split() if t]
                if len(rtoks) >= 2:
                    try:
                        float(rtoks[0])
                    except ValueError:
                        pass
                    else:
                        if is_valid_call(rtoks[1].upper()):
                            return True, await self._cmd_post_dx_spot(call, rt)
                return True, await self._cmd_show_dx(call, rest)
            if first == "talk":
                return True, await self._cmd_talk(call, rest)
            if first == "msg":
                return True, await self._cmd_msg(call, rest)
            if first == "send":
                return True, await self._cmd_send(call, rest)
            if first == "read":
                return True, await self._cmd_read(call, rest)
            if first == "reply":
                return True, await self._cmd_reply(call, rest)

            # normalize grouped command forms:
            # show/dx ..., sh/dx ...
            # show dx ..., sh dx ...
            group = None
            sub = None
            arg = None
            if "/" in first:
                g, s = first.split("/", 1)
                group = self._resolve_group_token(g)
                sub = s.lower()
                arg = rest
            else:
                group = self._resolve_group_token(first)
                if group:
                    sub = toks[1].lower() if len(toks) > 1 else ""
                    arg = " ".join(toks[2:]) if len(toks) > 2 else None
            if not group:
                direct_alias = self._direct_alias_map()
                key = direct_alias.get(first)
                if key:
                    handler = self._build_registry().get(key)
                    if handler:
                        return True, await handler(call, rest)
                if first == "ping":
                    return True, await self._cmd_ping(call, rest)
                return True, "?\r\n"

            registry = self._build_registry()
            if sub is None:
                return True, "?\r\n"
            if not sub:
                return True, await self._cmd_show_commands(call, group)
            if "/" in sub:
                sub_parts = [p for p in sub.split("/") if p]
                if sub_parts:
                    sub = sub_parts[0]
                    slash_arg = " ".join(sub_parts[1:])
                    if slash_arg:
                        arg = f"{slash_arg} {arg}".strip() if arg else slash_arg

            key = self._resolve_subcommand(group, sub, registry)
            if not key:
                return True, "?\r\n"

            handler = registry[key]
            return True, await handler(call, arg)
        finally:
            elapsed_ms = (time.monotonic() - started) * 1000.0
            if elapsed_ms >= 1000.0:
                LOG.warning("slow telnet command call=%s line=%r elapsed_ms=%.1f", call, line, elapsed_ms)
            else:
                LOG.debug("telnet command call=%s line=%r elapsed_ms=%.1f", call, line, elapsed_ms)

    def _build_registry(self) -> dict[str, Callable[[str, str | None], Awaitable[str]]]:
        return {
            # show/*
            "show/version": lambda c, a: self._cmd_show_version(),
            "show/dx": self._cmd_show_dx,
            "show/node": self._cmd_show_node,
            "show/cluster": self._cmd_show_cluster,
            "show/users": self._cmd_show_users,
            "show/connect": self._cmd_show_connect,
            "show/commands": self._cmd_show_commands,
            "show/shortcuts": self._cmd_show_shortcuts,
            "show/capabilities": self._cmd_show_capabilities,
            "show/newconfiguration": self._cmd_show_configuration,
            "show/mydx": self._cmd_show_mydx,
            "show/dxcc": self._cmd_show_dxcc,
            "show/filter": self._cmd_show_filter,
            "show/configuration": self._cmd_show_configuration,
            "show/program": self._cmd_show_program,
            "show/time": self._cmd_show_time,
            "show/date": self._cmd_show_date,
            "show/uptime": self._cmd_show_uptime,
            "show/motd": self._cmd_show_motd,
            "show/startup": self._cmd_show_startup,
            "show/heading": self._cmd_show_heading,
            "show/stats": self._cmd_show_stats,
            "show/links": self._cmd_show_links,
            "show/apropos": self._cmd_show_apropos,
            "show/notimpl": self._cmd_show_notimpl,
            # set/*
            "set/echo": lambda c, a: self._cmd_set_flag(c, a, "echo", True),
            "set/here": lambda c, a: self._cmd_set_flag(c, a, "here", True),
            "set/beep": lambda c, a: self._cmd_set_flag(c, a, "beep", True),
            "set/nowrap": lambda c, a: self._cmd_set_flag(c, a, "nowrap", True),
            "set/language": self._cmd_set_language,
            "set/dxspider": lambda c, a: self._cmd_set_profile(c, a, "dxspider"),
            "set/dxnet": lambda c, a: self._cmd_set_profile(c, a, "dxnet"),
            "set/arcluster": lambda c, a: self._cmd_set_profile(c, a, "arcluster"),
            "set/clx": lambda c, a: self._cmd_set_profile(c, a, "clx"),
            "set/announce": lambda c, a: self._cmd_set_named_var(c, a, "announce", "on"),
            "set/anntalk": lambda c, a: self._cmd_set_named_var(c, a, "anntalk", "on"),
            "set/dx": lambda c, a: self._cmd_set_named_var(c, a, "dx", "on"),
            "set/dxcq": lambda c, a: self._cmd_set_named_var(c, a, "dxcq", "on"),
            "set/dxitu": lambda c, a: self._cmd_set_named_var(c, a, "dxitu", "on"),
            "set/dxgrid": lambda c, a: self._cmd_set_named_var(c, a, "dxgrid", "on"),
            "set/rbn": lambda c, a: self._cmd_set_named_var(c, a, "rbn", "on"),
            "set/talk": lambda c, a: self._cmd_set_named_var(c, a, "talk", "on"),
            "set/wcy": lambda c, a: self._cmd_set_named_var(c, a, "wcy", "on"),
            "set/wwv": lambda c, a: self._cmd_set_named_var(c, a, "wwv", "on"),
            "set/wx": lambda c, a: self._cmd_set_named_var(c, a, "wx", "on"),
            "set/debug": lambda c, a: self._cmd_set_named_var(c, a, "debug", "on"),
            "set/isolate": lambda c, a: self._cmd_set_named_var(c, a, "isolate", "on"),
            "set/lockout": lambda c, a: self._cmd_set_named_var(c, a, "lockout", "on"),
            "set/prompt": lambda c, a: self._cmd_set_named_var(c, a, "prompt", "on"),
            "set/register": lambda c, a: self._cmd_set_named_var(c, a, "register", "on"),
            "set/localnode": lambda c, a: self._cmd_set_named_var(c, a, "local_node", "on"),
            "set/qra": lambda c, a: self._cmd_set_named_var(c, a, "qra", ""),
            "set/qth": lambda c, a: self._cmd_set_named_var(c, a, "qth", ""),
            "set/location": lambda c, a: self._cmd_set_named_var(c, a, "location", ""),
            "set/name": lambda c, a: self._cmd_set_named_var(c, a, "name", ""),
            "set/startup": self._cmd_set_startup,
            "set/usdb": self._cmd_set_usdb,
            "set/wantpc16": lambda c, a: self._cmd_set_named_var(c, a, "wantpc16", "on"),
            "set/wantpc9x": lambda c, a: self._cmd_set_named_var(c, a, "wantpc9x", "on"),
            "set/sendpc16": lambda c, a: self._cmd_set_named_var(c, a, "sendpc16", "on"),
            "set/routepc19": lambda c, a: self._cmd_set_named_var(c, a, "routepc19", "on"),
            "set/senddbg": lambda c, a: self._cmd_set_named_var(c, a, "send_dbg", "on"),
            "set/address": lambda c, a: self._cmd_set_contact_field(c, a, "address"),
            "set/email": lambda c, a: self._cmd_set_contact_field(c, a, "email"),
            "set/password": lambda c, a: self._cmd_set_named_var(c, a, "password", ""),
            "set/passphrase": lambda c, a: self._cmd_set_named_var(c, a, "passphrase", ""),
            "set/pinginterval": lambda c, a: self._cmd_set_named_var(c, a, "pinginterval", ""),
            "set/privilege": self._cmd_set_privilege,
            "set/var": self._cmd_set_var,
            "set/agwengine": lambda c, a: self._cmd_set_named_var(c, a, "agwengine", "on"),
            "set/agwmonitor": lambda c, a: self._cmd_set_named_var(c, a, "agwmonitor", "on"),
            "set/baddx": lambda c, a: self._cmd_set_bad_rule(c, a, "baddx"),
            "set/badnode": lambda c, a: self._cmd_set_bad_rule(c, a, "badnode"),
            "set/badspotter": lambda c, a: self._cmd_set_bad_rule(c, a, "badspotter"),
            "set/badword": lambda c, a: self._cmd_set_bad_rule(c, a, "badword"),
            "set/bbs": lambda c, a: self._cmd_set_named_var(c, a, "bbs", "on"),
            "set/believe": lambda c, a: self._cmd_set_named_var(c, a, "believe", "on"),
            "set/buddy": self._cmd_set_buddy,
            "set/home": lambda c, a: self._cmd_set_home_pref(c, a, "homenode"),
            "set/homebbs": lambda c, a: self._cmd_set_home_pref(c, a, "homebbs"),
            "set/homenode": lambda c, a: self._cmd_set_home_pref(c, a, "homenode"),
            "set/hops": lambda c, a: self._cmd_set_named_var(c, a, "hops", "on"),
            "set/logininfo": lambda c, a: self._cmd_set_named_var(c, a, "logininfo", "on"),
            "set/maxconnect": self._cmd_set_maxconnect,
            "set/node": lambda c, a: self._cmd_set_home_pref(c, a, "node"),
            "set/obscount": lambda c, a: self._cmd_set_named_var(c, a, "obscount", ""),
            "set/page": self._cmd_set_page,
            "set/dupann": lambda c, a: self._cmd_set_named_var(c, a, "dup_ann", "on"),
            "set/dupeph": lambda c, a: self._cmd_set_named_var(c, a, "dup_eph", "on"),
            "set/dupspots": self._cmd_set_dup_spots,
            "set/dupwcy": lambda c, a: self._cmd_set_named_var(c, a, "dup_wcy", "on"),
            "set/dupwwv": lambda c, a: self._cmd_set_named_var(c, a, "dup_wwv", "on"),
            "set/syslocation": lambda c, a: self._cmd_set_named_var(c, a, "sys_location", ""),
            "set/sysqra": lambda c, a: self._cmd_set_named_var(c, a, "sys_qra", ""),
            "set/user": self._cmd_set_user,
            "set/uservar": self._cmd_set_uservar,
            "set/relay": self._cmd_set_relay,
            "set/relaypeer": self._cmd_set_relaypeer,
            "set/ingestpeer": self._cmd_set_ingestpeer,
            "set/usstate": lambda c, a: self._cmd_set_named_var(c, a, "usstate", ""),
            "set/protothreshold": self._cmd_set_protothreshold,
            "set/protothresholds": self._cmd_set_protothreshold,
            "set/protoack": self._cmd_set_protoack,
            "set/prack": self._cmd_set_protoack,
            "set/control": self._cmd_set_control,
            # sysop/*
            "sysop/password": self._cmd_sysop_password,
            "sysop/clearpassword": self._cmd_sysop_clearpassword,
            "sysop/user": self._cmd_set_user,
            "sysop/deleteuser": self._cmd_delete_user,
            "sysop/privilege": self._cmd_set_privilege,
            "sysop/homenode": self._cmd_sysop_homenode,
            "sysop/blocklogin": self._cmd_sysop_blocklogin,
            "sysop/showuser": self._cmd_sysop_showuser,
            "sysop/users": self._cmd_sysop_users,
            "sysop/sysops": self._cmd_sysop_sysops,
            "sysop/access": self._cmd_sysop_access,
            "sysop/path": self._cmd_sysop_path,
            "sysop/spotlimit": self._cmd_sysop_spotlimit,
            "sysop/setaccess": self._cmd_sysop_setaccess,
            "sysop/setprompt": self._cmd_sysop_setprompt,
            "sysop/audit": self._cmd_sysop_audit,
            "sysop/services": self._cmd_sysop_services,
            "sysop/restart": self._cmd_sysop_restart,
            # unset/*
            "unset/echo": lambda c, a: self._cmd_set_flag(c, a, "echo", False),
            "unset/here": lambda c, a: self._cmd_set_flag(c, a, "here", False),
            "unset/beep": lambda c, a: self._cmd_set_flag(c, a, "beep", False),
            "unset/nowrap": lambda c, a: self._cmd_set_flag(c, a, "nowrap", False),
            "unset/dxnet": lambda c, a: self._cmd_set_profile(c, a, "dxspider"),
            "unset/arcluster": lambda c, a: self._cmd_set_profile(c, a, "dxspider"),
            "unset/clx": lambda c, a: self._cmd_set_profile(c, a, "dxspider"),
            "unset/dx": lambda c, a: self._cmd_unset_named_var(c, a, "dx"),
            "unset/announce": lambda c, a: self._cmd_unset_named_var(c, a, "announce"),
            "unset/dxcq": lambda c, a: self._cmd_unset_named_var(c, a, "dxcq"),
            "unset/dxitu": lambda c, a: self._cmd_unset_named_var(c, a, "dxitu"),
            "unset/dxgrid": lambda c, a: self._cmd_unset_named_var(c, a, "dxgrid"),
            "unset/rbn": lambda c, a: self._cmd_unset_named_var(c, a, "rbn"),
            "unset/talk": lambda c, a: self._cmd_unset_named_var(c, a, "talk"),
            "unset/wcy": lambda c, a: self._cmd_unset_named_var(c, a, "wcy"),
            "unset/wwv": lambda c, a: self._cmd_unset_named_var(c, a, "wwv"),
            "unset/wx": lambda c, a: self._cmd_unset_named_var(c, a, "wx"),
            "unset/debug": lambda c, a: self._cmd_unset_named_var(c, a, "debug"),
            "unset/isolate": lambda c, a: self._cmd_unset_named_var(c, a, "isolate"),
            "unset/lockout": lambda c, a: self._cmd_unset_named_var(c, a, "lockout"),
            "unset/prompt": lambda c, a: self._cmd_unset_named_var(c, a, "prompt"),
            "unset/register": lambda c, a: self._cmd_unset_named_var(c, a, "register"),
            "unset/localnode": lambda c, a: self._cmd_unset_named_var(c, a, "local_node"),
            "unset/startup": self._cmd_unset_startup,
            "unset/wantpc16": lambda c, a: self._cmd_unset_named_var(c, a, "wantpc16"),
            "unset/wantpc9x": lambda c, a: self._cmd_unset_named_var(c, a, "wantpc9x"),
            "unset/sendpc16": lambda c, a: self._cmd_unset_named_var(c, a, "sendpc16"),
            "unset/routepc19": lambda c, a: self._cmd_unset_named_var(c, a, "routepc19"),
            "unset/senddbg": lambda c, a: self._cmd_unset_named_var(c, a, "send_dbg"),
            "unset/agwengine": lambda c, a: self._cmd_unset_named_var(c, a, "agwengine"),
            "unset/agwmonitor": lambda c, a: self._cmd_unset_named_var(c, a, "agwmonitor"),
            "unset/anntalk": lambda c, a: self._cmd_unset_named_var(c, a, "anntalk"),
            "unset/baddx": lambda c, a: self._cmd_unset_bad_rule(c, a, "baddx"),
            "unset/badnode": lambda c, a: self._cmd_unset_bad_rule(c, a, "badnode"),
            "unset/badspotter": lambda c, a: self._cmd_unset_bad_rule(c, a, "badspotter"),
            "unset/badword": lambda c, a: self._cmd_unset_bad_rule(c, a, "badword"),
            "unset/believe": lambda c, a: self._cmd_unset_named_var(c, a, "believe"),
            "unset/buddy": self._cmd_unset_buddy,
            "unset/email": lambda c, a: self._cmd_unset_contact_field(c, a, "email"),
            "unset/hops": lambda c, a: self._cmd_unset_named_var(c, a, "hops"),
            "unset/logininfo": lambda c, a: self._cmd_unset_named_var(c, a, "logininfo"),
            "unset/passphrase": lambda c, a: self._cmd_unset_named_var(c, a, "passphrase"),
            "unset/password": lambda c, a: self._cmd_unset_named_var(c, a, "password"),
            "unset/privilege": self._cmd_unset_privilege,
            "unset/dupann": lambda c, a: self._cmd_unset_named_var(c, a, "dup_ann"),
            "unset/dupeph": lambda c, a: self._cmd_unset_named_var(c, a, "dup_eph"),
            "unset/dupspots": self._cmd_unset_dup_spots,
            "unset/dupwcy": lambda c, a: self._cmd_unset_named_var(c, a, "dup_wcy"),
            "unset/dupwwv": lambda c, a: self._cmd_unset_named_var(c, a, "dup_wwv"),
            "unset/usstate": lambda c, a: self._cmd_unset_named_var(c, a, "usstate"),
            "unset/var": self._cmd_unset_var,
            "unset/uservar": self._cmd_unset_uservar,
            "unset/relay": self._cmd_unset_relay,
            "unset/relaypeer": self._cmd_unset_relaypeer,
            "unset/ingestpeer": self._cmd_unset_ingestpeer,
            "unset/protothreshold": self._cmd_unset_protothreshold,
            "unset/protothresholds": self._cmd_unset_protothreshold,
            "unset/protoack": self._cmd_unset_protoack,
            "unset/prack": self._cmd_unset_protoack,
            "unset/control": self._cmd_unset_control,
            # accept/* and reject/*
            "accept/spots": lambda c, a: self._cmd_filter_add(c, a, "spots", "accept"),
            "accept/rbn": lambda c, a: self._cmd_filter_alias_expr(c, a, "spots", "accept", "rbn", "rbn"),
            "accept/announce": lambda c, a: self._cmd_filter_add(c, a, "announce", "accept"),
            "accept/route": lambda c, a: self._cmd_filter_add(c, a, "route", "accept"),
            "accept/wcy": lambda c, a: self._cmd_filter_add(c, a, "wcy", "accept"),
            "accept/wwv": lambda c, a: self._cmd_filter_add(c, a, "wwv", "accept"),
            "accept/wx": lambda c, a: self._cmd_filter_add(c, a, "wx", "accept"),
            "reject/spots": lambda c, a: self._cmd_filter_add(c, a, "spots", "reject"),
            "reject/rbn": lambda c, a: self._cmd_filter_alias_expr(c, a, "spots", "reject", "rbn", "rbn"),
            "reject/announce": lambda c, a: self._cmd_filter_add(c, a, "announce", "reject"),
            "reject/route": lambda c, a: self._cmd_filter_add(c, a, "route", "reject"),
            "reject/wcy": lambda c, a: self._cmd_filter_add(c, a, "wcy", "reject"),
            "reject/wwv": lambda c, a: self._cmd_filter_add(c, a, "wwv", "reject"),
            "reject/wx": lambda c, a: self._cmd_filter_add(c, a, "wx", "reject"),
            # clear/*
            "clear/spots": lambda c, a: self._cmd_filter_clear(c, a, "spots"),
            "clear/rbn": lambda c, a: self._cmd_filter_clear_expr(c, a, "spots", "rbn", "rbn"),
            "clear/announce": lambda c, a: self._cmd_filter_clear(c, a, "announce"),
            "clear/route": lambda c, a: self._cmd_filter_clear(c, a, "route"),
            "clear/wcy": lambda c, a: self._cmd_filter_clear(c, a, "wcy"),
            "clear/wwv": lambda c, a: self._cmd_filter_clear(c, a, "wwv"),
            "clear/wx": lambda c, a: self._cmd_filter_clear(c, a, "wx"),
            "clear/dupefile": self._cmd_clear_dupefile,
            "clear/protohistory": self._cmd_clear_protohistory,
            "clear/prhist": self._cmd_clear_protohistory,
            "clear/prothist": self._cmd_clear_protohistory,
            # load/*
            "load/aliases": self._cmd_load_aliases,
            "load/badmsg": self._cmd_load_badmsg,
            "load/badwords": self._cmd_load_badwords,
            "load/bands": self._cmd_load_bands,
            "load/cmdcache": self._cmd_load_cmdcache,
            "load/db": self._cmd_load_db,
            "load/dxqsl": self._cmd_load_dxqsl,
            "load/forward": self._cmd_load_forward,
            "load/hops": self._cmd_load_hops,
            "load/keps": self._cmd_load_keps,
            "load/messages": self._cmd_load_messages,
            "load/prefixes": self._cmd_load_prefixes,
            "load/swop": self._cmd_load_swop,
            "load/usdb": self._cmd_load_usdb,
            # stat/*
            "stat/spot": lambda c, a: self._cmd_stat_named(c, a, "spots"),
            "stat/spots": lambda c, a: self._cmd_stat_named(c, a, "spots"),
            "stat/user": self._cmd_stat_user_direct,
            "stat/users": lambda c, a: self._cmd_stat_named(c, a, "users"),
            "stat/db": self._cmd_stat_db_direct,
            "stat/msg": self._cmd_stat_msg_direct,
            "stat/route": lambda c, a: self._cmd_stat_named(c, a, "route"),
            "stat/proto": lambda c, a: self._cmd_stat_named(c, a, "proto"),
            "stat/protohistory": lambda c, a: self._cmd_stat_named(c, a, "protohistory"),
            "stat/protoevents": lambda c, a: self._cmd_stat_named(c, a, "protoevents"),
            "stat/protoalerts": lambda c, a: self._cmd_stat_named(c, a, "protoalerts"),
            "stat/protoacks": lambda c, a: self._cmd_stat_named(c, a, "protoacks"),
            "stat/protoack": lambda c, a: self._cmd_stat_named(c, a, "protoacks"),
            "stat/prack": lambda c, a: self._cmd_stat_named(c, a, "protoacks"),
            "stat/wwv": lambda c, a: self._cmd_stat_named(c, a, "wwv"),
            "stat/wcy": lambda c, a: self._cmd_stat_named(c, a, "wcy"),
            "stat/queue": lambda c, a: self._cmd_stat_named(c, a, "queue"),
            "stat/channel": self._cmd_stat_channel_direct,
            "stat/nodeconfig": self._cmd_stat_nodeconfig_direct,
            "stat/pc19list": self._cmd_stat_pc19list_direct,
            "stat/routenode": self._cmd_stat_routenode_direct,
            "stat/routeuser": self._cmd_stat_routeuser_direct,
            "stat/userconfig": self._cmd_stat_userconfig_direct,
            # accept/reject/clear/load/stat (large command families - stubbed safely)
            "show/wcy": self._cmd_show_wcy,
            "show/wwv": self._cmd_show_wwv,
            "show/wx": self._cmd_show_wx,
            "show/announce": self._cmd_show_announce,
            "show/chat": self._cmd_show_chat,
            # extra recognized commands
            "show/qrz": self._cmd_show_qrz,
            "show/lastspot": self._cmd_show_lastspot,
            "show/qra": self._cmd_show_qra,
            "show/location": lambda c, a: self._show_key_value(c, a, "show/location", "location", default="", readable_label="Location"),
            "show/localnode": lambda c, a: self._show_key_value(c, a, "show/localnode", "local_node", pref_key="local_node", default="off", readable_label="Local Node"),
            "show/name": lambda c, a: self._show_key_value(c, a, "show/name", "name", registry_field="display_name", readable_label="Name"),
            "show/privilege": lambda c, a: self._show_key_value(c, a, "show/privilege", "privilege", registry_field="privilege", default="user", readable_label="Privilege"),
            "show/prompt": lambda c, a: self._show_key_value(c, a, "show/prompt", "prompt", default="off", readable_label="Prompt"),
            "show/qth": lambda c, a: self._show_key_value(c, a, "show/qth", "qth", registry_field="qth", readable_label="QTH"),
            "show/address": lambda c, a: self._show_key_value(c, a, "show/address", "address", registry_field="address", readable_label="Address"),
            "show/email": lambda c, a: self._show_key_value(c, a, "show/email", "email", registry_field="email", readable_label="Email"),
            "show/home": lambda c, a: self._show_key_value(c, a, "show/home", "homenode", pref_key="homenode", registry_field="home_node", readable_label="Home"),
            "show/homebbs": lambda c, a: self._show_key_value(c, a, "show/homebbs", "homebbs", default="", readable_label="Home BBS"),
            "show/homenode": lambda c, a: self._show_key_value(c, a, "show/homenode", "homenode", pref_key="homenode", registry_field="home_node", readable_label="Home Node"),
            "show/language": lambda c, a: self._show_key_value(c, a, "show/language", "language", default="en", readable_label="Language"),
            "show/passphrase": lambda c, a: self._show_key_value(c, a, "show/passphrase", "passphrase", default="", readable_label="Passphrase"),
            "show/page": lambda c, a: self._show_key_value(c, a, "show/page", "page", pref_key="page", default="20", readable_label="Page"),
            "show/maxconnect": lambda c, a: self._show_key_value(c, a, "show/maxconnect", "maxconnect", default="0", readable_label="MaxConnect"),
            "show/pinginterval": lambda c, a: self._show_key_value(c, a, "show/pinginterval", "pinginterval", default="", readable_label="Ping Interval"),
            "show/obscount": lambda c, a: self._show_key_value(c, a, "show/obscount", "obscount", default="", readable_label="Obscount"),
            "show/echo": lambda c, a: self._show_key_value(c, a, "show/echo", "echo", default="on", readable_label="Echo"),
            "show/here": lambda c, a: self._show_key_value(c, a, "show/here", "here", default="on", readable_label="Here"),
            "show/beep": lambda c, a: self._show_key_value(c, a, "show/beep", "beep", default="off", readable_label="Beep"),
            "show/rbn": lambda c, a: self._show_key_value(c, a, "show/rbn", "rbn", default="on", readable_label="RBN"),
            "show/dxcq": lambda c, a: self._show_key_value(c, a, "show/dxcq", "dxcq", default="on", readable_label="DX CQ"),
            "show/dxitu": lambda c, a: self._show_key_value(c, a, "show/dxitu", "dxitu", default="on", readable_label="DX ITU"),
            "show/dxgrid": lambda c, a: self._show_key_value(c, a, "show/dxgrid", "dxgrid", default="on", readable_label="DX Grid"),
            "show/register": lambda c, a: self._show_key_value(c, a, "show/register", "register", default="off", readable_label="Register"),
            "show/logininfo": lambda c, a: self._show_key_value(c, a, "show/logininfo", "logininfo", default="off", readable_label="Login Info"),
            "show/routepc19": lambda c, a: self._show_key_value(c, a, "show/routepc19", "routepc19", default="off", readable_label="Route PC19"),
            "show/senddbg": lambda c, a: self._show_key_value(c, a, "show/senddbg", "send_dbg", pref_key="send_dbg", default="off", readable_label="Send Debug"),
            "show/sendpc16": lambda c, a: self._show_key_value(c, a, "show/sendpc16", "sendpc16", default="off", readable_label="Send PC16"),
            "show/wantpc16": lambda c, a: self._show_key_value(c, a, "show/wantpc16", "wantpc16", default="off", readable_label="Want PC16"),
            "show/wantpc9x": lambda c, a: self._show_key_value(c, a, "show/wantpc9x", "wantpc9x", default="off", readable_label="Want PC9X"),
            "show/usstate": lambda c, a: self._show_key_value(c, a, "show/usstate", "usstate", default="", readable_label="US State"),
            "show/bbs": lambda c, a: self._show_key_value(c, a, "show/bbs", "bbs", default="off", readable_label="BBS"),
            "show/believe": lambda c, a: self._show_key_value(c, a, "show/believe", "believe", default="off", readable_label="Believe"),
            "show/anntalk": lambda c, a: self._show_key_value(c, a, "show/anntalk", "anntalk", default="off", readable_label="AnnTalk"),
            "show/agwengine": lambda c, a: self._show_key_value(c, a, "show/agwengine", "agwengine", default="off", readable_label="AGW Engine"),
            "show/agwmonitor": lambda c, a: self._show_key_value(c, a, "show/agwmonitor", "agwmonitor", default="off", readable_label="AGW Monitor"),
            "show/prefix": self._cmd_show_prefix,
            "show/files": self._cmd_show_files,
            "show/log": self._cmd_show_log,
            "show/control": self._cmd_show_control,
            "show/relay": self._cmd_show_relay,
            "show/relaypeer": self._cmd_show_relaypeer,
            "show/ingestpeer": self._cmd_show_ingestpeer,
            "show/policy": self._cmd_show_policy,
            "show/policydrop": self._cmd_show_policydrop,
            "show/route": self._cmd_show_route,
            "show/hops": self._cmd_show_hops,
            "show/proto": self._cmd_show_proto,
            "show/protohistory": lambda c, a: self._cmd_show_proto(c, ((a or "").strip() + " --history").strip()),
            "show/protoevents": self._cmd_show_protoevents,
            "show/protoalerts": self._cmd_show_protoalerts,
            "show/protoacks": self._cmd_show_protoacks,
            "show/protoack": self._cmd_show_protoacks,
            "show/prack": self._cmd_show_protoacks,
            "show/protoconfig": self._cmd_show_protoconfig,
            "show/protothresholds": self._cmd_show_protoconfig,
            "show/msgstatus": self._cmd_show_msg_status,
            "show/messages": self._cmd_show_messages,
            "show/mail": self._cmd_show_messages,
            "show/outbox": self._cmd_show_outbox,
            "show/bands": self._cmd_show_bands,
            "show/dxstats": lambda c, a: self._cmd_show_dxstats(c, a, "all"),
            "show/hfstats": lambda c, a: self._cmd_show_dxstats(c, a, "hf"),
            "show/vhfstats": lambda c, a: self._cmd_show_dxstats(c, a, "vhf"),
            "show/hftable": self._cmd_show_bands,
            "show/vhftable": self._cmd_show_bands,
            "show/talk": self._cmd_show_talk_direct,
            "show/debug": self._cmd_show_debug_direct,
            "show/isolate": self._cmd_show_isolate_direct,
            "show/lockout": self._cmd_show_lockout_direct,
            "show/registered": self._cmd_show_registered,
            "show/usdb": self._cmd_show_usdb,
            "show/var": self._cmd_show_var,
            "show/station": self._cmd_show_station,
            "show/groups": self._cmd_show_groups_direct,
            "show/buddy": self._cmd_show_buddy,
            "show/rcmd": self._cmd_show_rcmd_direct,
            "show/sun": self._cmd_show_sun_direct,
            "show/moon": self._cmd_show_moon_direct,
            "show/muf": self._cmd_show_muf_direct,
            "show/grayline": self._cmd_show_grayline_direct,
            "show/contest": self._cmd_show_contest,
            "show/425": self._cmd_show_425,
            "show/baddx": lambda c, a: self._cmd_show_deny_list(c, a, "baddx"),
            "show/badnode": lambda c, a: self._cmd_show_deny_list(c, a, "badnode"),
            "show/badspotter": lambda c, a: self._cmd_show_deny_list(c, a, "badspotter"),
            "show/badword": lambda c, a: self._cmd_show_deny_list(c, a, "badword"),
            "show/cmdcache": self._cmd_show_cmdcache_direct,
            "show/db0sdx": self._cmd_show_db0sdx_direct,
            "show/dupann": self._cmd_show_dupann_direct,
            "show/dupeph": self._cmd_show_dupeph_direct,
            "show/dupspots": self._cmd_show_dupspots_direct,
            "show/dupwcy": self._cmd_show_dupwcy_direct,
            "show/dupwwv": self._cmd_show_dupwwv_direct,
            "show/dxqsl": self._cmd_show_dxqsl_direct,
            "show/ik3qar": self._cmd_show_ik3qar_direct,
            "show/newconfiguration": self._cmd_show_configuration,
            "show/satellite": self._cmd_show_satellite,
            "show/wm7d": self._cmd_show_wm7d_direct,
            # create/delete/forward/get
            "create/user": self._cmd_create_user,
            "delete/user": self._cmd_delete_user,
            "delete/usdb": self._cmd_delete_usdb,
            "forward/latlong": self._cmd_forward_latlong,
            "forward/opername": self._cmd_forward_opername,
            "get/keps": self._cmd_get_keps,
        }

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        async with self._semaphore:
            peer = writer.get_extra_info("peername")
            self._session_seq += 1
            session_id = self._session_seq
            handoff = False

            try:
                await self._write(writer, "login: ")
                raw_call = await self._readline(reader)
                if raw_call is None:
                    writer.close()
                    await writer.wait_closed()
                    return

                call = self._sanitize_login_call(raw_call)
                if not call or not is_valid_call(call):
                    self._log_auth_failure("telnet", peer, call or raw_call, "invalid_callsign")
                    await self._write(writer, "Invalid callsign\r\n")
                    writer.close()
                    await writer.wait_closed()
                    return
                base_call = call.split("-", 1)[0]
                blocked = False
                for candidate in (call, base_call):
                    raw_block = await self.store.get_user_pref(candidate, "blocked_login")
                    if str(raw_block or "").strip().lower() in {"1", "on", "yes", "true"}:
                        blocked = True
                        break
                if blocked:
                    self._log_auth_failure("telnet", peer, call, "blocked_login")
                    await self._write(writer, "Login blocked\r\n")
                    writer.close()
                    await writer.wait_closed()
                    return
                if not await self._access_allowed(call, "telnet", "login"):
                    self._log_auth_failure("telnet", peer, call, "telnet_login_not_allowed")
                    await self._write(writer, "Login not allowed via telnet\r\n")
                    writer.close()
                    await writer.wait_closed()
                    return
                node_family = await self._node_family_for_login(call)
                expected_password = await self.store.get_user_pref(call, "password")
                password_set = expected_password is not None and str(expected_password).strip() != ""
                password_just_set = False
                if not password_set and not node_family:
                    ok = await self._prompt_new_password(call, reader, writer)
                    if not ok:
                        writer.close()
                        await writer.wait_closed()
                        return
                    expected_password = await self.store.get_user_pref(call, "password")
                    password_set = expected_password is not None and str(expected_password).strip() != ""
                    password_just_set = True
                require_password = (await self._node_flag("require_password") or password_set) and not node_family
                if require_password and not password_just_set:
                    await self._write(writer, "password: ")
                    supplied_password = await self._read_password(reader, writer)
                    if supplied_password is None:
                        writer.close()
                        await writer.wait_closed()
                        return
                    if expected_password is not None and str(expected_password).strip():
                        if not verify_password(supplied_password, str(expected_password)):
                            self._log_auth_failure("telnet", peer, call, "bad_password")
                            await self._write(writer, "Login failed\r\n")
                            writer.close()
                            await writer.wait_closed()
                            return
                        if not is_password_hash(str(expected_password)):
                            await self.store.set_user_pref(call, "password", hash_password(supplied_password), int(datetime.now(timezone.utc).timestamp()))
                maxconnect = await self._maxconnect_for_call(call)
                active_for_call = self._active_sessions_for_call(call)
                if maxconnect > 0 and active_for_call >= maxconnect:
                    await self._write(writer, f"Too many connections for {call} (maxconnect={maxconnect})\r\n")
                    writer.close()
                    await writer.wait_closed()
                    return
                login_epoch = int(datetime.now(timezone.utc).timestamp())
                await self.store.record_login(
                    call,
                    login_epoch,
                    describe_session_path(
                        "telnet",
                        peer,
                        writer.get_extra_info("sockname") if hasattr(writer, "get_extra_info") else None,
                    ),
                )

                if node_family:
                    bridged = await self._bridge_node_login(call, reader, writer)
                    if bridged:
                        handoff = True
                        return

                self._sessions[session_id] = Session(call=call, writer=writer, connected_at=datetime.now(timezone.utc))
                await self._apply_prefs_to_session(self._sessions[session_id])
                await self._load_filters_for_call(call)
                self._users.add(call)
                if self._on_sessions_changed_fn:
                    await self._on_sessions_changed_fn()
                LOG.info("login call=%s peer=%s", call, peer)
                await self._write(writer, await self._welcome_block(call))
                startup_outputs = await self._run_startup_commands(call)
                for out in startup_outputs:
                    await self._write(writer, out)

                while True:
                    sess = self._sessions.get(session_id)
                    if sess:
                        sess.async_line_open = False
                    await self._write(writer, await self._prompt(call))
                    line = await self._readline(reader)
                    if line is None:
                        break
                    keep_going, output = await self._execute_command(call, line)
                    if output:
                        await self._write(writer, output)
                    if not keep_going:
                        break

            except Exception:
                LOG.exception("session error peer=%s", peer)
            finally:
                self._sessions.pop(session_id, None)
                if self._on_sessions_changed_fn:
                    await self._on_sessions_changed_fn()
                if not handoff:
                    try:
                        writer.close()
                        await writer.wait_closed()
                    except Exception:
                        pass

    async def _bridge_node_login(
        self,
        call: str,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> bool:
        await self._write(writer, f"Hello {call}\r\n")
        await self._write(writer, await self._prompt(call))
        first = await self._readline(reader)
        if first is None:
            return True
        line = first.strip()
        client_match = re.fullmatch(r"client\s+(\S+)\s+telnet", line, re.IGNORECASE)
        if line.startswith("PC"):
            if self._on_node_login_fn:
                return await self._on_node_login_fn(call, call, reader, writer, [line])
            return False
        if client_match:
            peer_name = client_match.group(1).upper()
            if self._on_node_login_fn:
                return await self._on_node_login_fn(call, peer_name, reader, writer, None)
            return False
        await self._write(writer, "Node login requires a cluster client handshake.\r\n")
        return False
