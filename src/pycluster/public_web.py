from __future__ import annotations

import asyncio
import base64
from datetime import datetime, timezone, timedelta
import hashlib
import json
import logging
import mimetypes
from pathlib import Path
import re
import secrets
import time
from urllib.parse import parse_qs, unquote, urlparse
import urllib.request
import xml.etree.ElementTree as ET

from . import __version__
from .auth_logging import log_auth_failure
from .access_policy import default_access_allowed
from .auth import hash_password, is_password_hash, verify_password
from .config import AppConfig, node_presentation_defaults
from .ctydat import load_cty, lookup
from .geocode import estimate_location_from_locator, resolve_location_to_coords
from .maidenhead import coords_to_locator, extract_locator
from .models import Spot, display_call, is_valid_call, normalize_call
from .pathmeta import describe_session_path
from .store import SpotStore


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

_MODE_ORDER = ["CW", "WSPR", "RTTY", "FT8", "FT4", "FT2", "JS8", "JT9", "JT65", "SSB", "AM", "FM", "PSK"]

_MODE_RE = [
    (re.compile(r"\bFT8\b", re.I), "FT8"),
    (re.compile(r"\bFT4\b", re.I), "FT4"),
    (re.compile(r"\bFT2\b", re.I), "FT2"),
    (re.compile(r"\bQ65\b", re.I), "Q65"),
    (re.compile(r"\bMSK144\b", re.I), "MSK144"),
    (re.compile(r"\bFSK441\b", re.I), "FSK441"),
    (re.compile(r"\bJS8\b", re.I), "JS8"),
    (re.compile(r"\bJT65\b", re.I), "JT65"),
    (re.compile(r"\bJT9\b", re.I), "JT9"),
    (re.compile(r"\bWSPR\b", re.I), "WSPR"),
    (re.compile(r"\bRTTY\b", re.I), "RTTY"),
    (re.compile(r"\bMFSK\b", re.I), "MFSK"),
    (re.compile(r"\bOLIVIA\b", re.I), "OLIVIA"),
    (re.compile(r"\bDOMINO(?:EX)?\b", re.I), "DOMINO"),
    (re.compile(r"\bTHOR\b", re.I), "THOR"),
    (re.compile(r"\bHELL(?:SCHREIBER)?\b", re.I), "HELL"),
    (re.compile(r"\bROS\b", re.I), "ROS"),
    (re.compile(r"\bVARA\b", re.I), "VARA"),
    (re.compile(r"\bPACTOR\b", re.I), "PACTOR"),
    (re.compile(r"\bWINMOR\b", re.I), "WINMOR"),
    (re.compile(r"\bARDOP\b", re.I), "ARDOP"),
    (re.compile(r"\bPSK\d*\b", re.I), "PSK"),
    (re.compile(r"\bFAX\b", re.I), "FAX"),
    (re.compile(r"\bSSTV\b", re.I), "SSTV"),
    (re.compile(r"\bATV\b", re.I), "ATV"),
    (re.compile(r"\bDATA\b", re.I), "DATA"),
    (re.compile(r"\bDIGI(?:TAL)?\b", re.I), "DATA"),
    (re.compile(r"\bCW\b", re.I), "CW"),
    (re.compile(r"\b(LSB|USB|SSB)\b", re.I), "SSB"),
    (re.compile(r"\bAM\b", re.I), "AM"),
    (re.compile(r"\bFM\b", re.I), "FM"),
]
_ACT_RE = [
    (re.compile(r"\bSOTA\b", re.I), "SOTA"),
    (re.compile(r"\bPOTA\b|\bparks?\b", re.I), "POTA"),
    (re.compile(r"\bIOTA\b", re.I), "IOTA"),
    (re.compile(r"\bWWFF\b", re.I), "WWFF"),
    (re.compile(r"\bBOTA\b", re.I), "BOTA"),
]
_CW_RANGES = [
    (1.800, 1.840), (3.500, 3.600), (7.000, 7.040), (10.100, 10.150),
    (14.000, 14.070), (18.068, 18.100), (21.000, 21.070), (24.890, 24.920), (28.000, 28.070),
]
_BANDS = [
    ("LF/MF", 0.1357, 0.479), ("160m", 1.8, 2.0),
    ("80m", 3.5, 4.0), ("60m", 5.330, 5.407), ("40m", 7.0, 7.3), ("30m", 10.1, 10.15),
    ("20m", 14.0, 14.35), ("17m", 18.068, 18.168), ("15m", 21.0, 21.45), ("12m", 24.89, 24.99),
    ("10m", 28.0, 29.7), ("6m", 50.0, 54.0), ("4m", 70.0, 70.5), ("2m", 144.0, 148.0),
    ("1.25m", 222.0, 225.0),
    ("33cm", 902.0, 928.0),
    ("70cm", 430.0, 450.0), ("23cm", 1240.0, 1300.0),
]


def freq_to_band(freq_khz: float) -> str:
    mhz = freq_khz / 1000.0
    for name, lo, hi in _BANDS:
        if lo <= mhz <= hi:
            return name
    if mhz > 1300.0:
        return "SHF"
    return ""


def detect_mode(comment: str, freq_khz: float) -> str:
    for rx, mode in _MODE_RE:
        if rx.search(comment):
            return mode
    mhz = freq_khz / 1000.0
    for lo, hi in _CW_RANGES:
        if lo <= mhz <= hi:
            return "CW"
    return ""


def detect_activity(comment: str) -> str:
    for rx, act in _ACT_RE:
        if rx.search(comment):
            return act
    return ""


class PublicWebServer:
    def __init__(
        self,
        config: AppConfig,
        store: SpotStore,
        started_at: datetime,
        link_stats_fn=None,
        link_desired_peers_fn=None,
        publish_spot_fn=None,
        relay_spot_fn=None,
        publish_chat_fn=None,
        relay_chat_fn=None,
        publish_bulletin_fn=None,
        relay_bulletin_fn=None,
        event_log_fn=None,
    ) -> None:
        self.config = config
        self.store = store
        self.started_at = started_at
        self.link_stats_fn = link_stats_fn
        self.link_desired_peers_fn = link_desired_peers_fn
        self.publish_spot_fn = publish_spot_fn
        self.relay_spot_fn = relay_spot_fn
        self.publish_chat_fn = publish_chat_fn
        self.relay_chat_fn = relay_chat_fn
        self.publish_bulletin_fn = publish_bulletin_fn
        self.relay_bulletin_fn = relay_bulletin_fn
        self.event_log_fn = event_log_fn
        self._server: asyncio.AbstractServer | None = None
        self._cty_loaded = False
        self._ws_clients: set[asyncio.Task[None]] = set()
        self._ws_writers: set[asyncio.StreamWriter] = set()
        self._web_sessions: dict[str, tuple[str, int]] = {}

    def _audit(self, category: str, text: str) -> None:
        if self.event_log_fn:
            try:
                self.event_log_fn(category, text)
            except Exception:
                LOG.exception("public web audit log failed")

    async def _branding(self) -> dict[str, object]:
        data = node_presentation_defaults(self.config.node)
        prefs = await self.store.list_user_prefs(self.config.node.node_call)
        for key in _CONFIG_AUTH_NODE_FIELDS:
            prefs.pop(key, None)
        data.update(prefs)
        node_call = self.config.node.node_call
        node_alias = str(data.get("node_alias", self.config.node.node_alias)).strip() or self.config.node.node_alias
        qth = str(data.get("qth", self.config.node.qth)).strip()
        node_locator = str(data.get("node_locator", self.config.node.node_locator)).strip().upper()
        owner_name = str(data.get("owner_name", self.config.node.owner_name)).strip()
        branding_name = str(data.get("branding_name", self.config.node.branding_name)).strip() or "pyCluster"
        support_contact = str(data.get("support_contact", self.config.node.support_contact)).strip()
        website_url = str(data.get("website_url", self.config.node.website_url)).strip()
        telnet_ports = ",".join(str(p) for p in (self.config.telnet.ports or (self.config.telnet.port,)))
        title = f"{node_alias or node_call} {branding_name}".strip()
        title_suffix = f" - {qth}" if qth else ""
        footer_primary = f"Node {node_call}"
        if support_contact:
            footer_primary += f" • {support_contact}"
        footer_parts = [part for part in (qth, node_locator) if part]
        footer_secondary = " • ".join(footer_parts) if footer_parts else "All times UTC"
        software_version = f"{branding_name} {__version__}"
        return {
            "node_call": node_call,
            "node_alias": node_alias,
            "owner_name": owner_name,
            "qth": qth,
            "node_locator": node_locator,
            "branding_name": branding_name,
            "software_version": software_version,
            "support_contact": support_contact,
            "website_url": website_url,
            "page_title": f"{title}{title_suffix}",
            "header_title": title,
            "footer_primary": footer_primary,
            "footer_secondary": footer_secondary,
            "home_node": node_call,
            "telnet_ports": telnet_ports,
        }

    async def start(self) -> None:
        if not self.config.public_web.enabled:
            return
        cty_path = self.config.public_web.cty_dat_path.strip()
        if cty_path:
            try:
                load_cty(cty_path)
                self._cty_loaded = True
            except Exception as exc:
                LOG.warning("public web cty load failed from %s: %s", cty_path, exc)
        self._server = await asyncio.start_server(
            self._handle,
            host=self.config.public_web.host,
            port=self.config.public_web.port,
            limit=16384,
        )
        addrs = ", ".join(str(s.getsockname()) for s in (self._server.sockets or []))
        LOG.info("Public web listening on %s", addrs)

    async def stop(self) -> None:
        if self._server:
            self._server.close()
            try:
                await asyncio.wait_for(self._server.wait_closed(), timeout=1.0)
            except (asyncio.TimeoutError, ConnectionError, OSError):
                pass
        writers = list(self._ws_writers)
        self._ws_writers.clear()
        for writer in writers:
            try:
                writer.close()
            except Exception:
                pass
        clients = list(self._ws_clients)
        self._ws_clients.clear()
        for task in clients:
            task.cancel()
        if clients:
            try:
                await asyncio.wait_for(asyncio.gather(*clients, return_exceptions=True), timeout=2.0)
            except asyncio.TimeoutError:
                LOG.warning("timed out waiting for websocket clients to stop")

    def _cleanup_web_sessions(self) -> None:
        now = int(time.time())
        stale = [k for k, (_call, exp) in self._web_sessions.items() if exp <= now]
        for k in stale:
            self._web_sessions.pop(k, None)

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

    def _issue_web_token(self, call: str, ttl_seconds: int = 8 * 3600) -> tuple[str, int]:
        tok = secrets.token_urlsafe(24)
        exp = int(time.time()) + max(300, ttl_seconds)
        self._web_sessions[tok] = (call.upper(), exp)
        return tok, exp

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
        call, exp = row
        if exp <= int(time.time()):
            self._web_sessions.pop(tok, None)
            return None
        return call

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

    async def _access_snapshot(self, call: str, channel: str) -> dict[str, bool]:
        caps = ["login", "spots", "chat", "announce", "wx", "wcy", "wwv"]
        out: dict[str, bool] = {}
        for cap in caps:
            out[cap] = await self._access_allowed(call, channel, cap)
        return out

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

    async def _close_writer(self, writer: asyncio.StreamWriter) -> None:
        try:
            writer.close()
        except Exception:
            return
        try:
            await asyncio.wait_for(writer.wait_closed(), timeout=0.5)
        except Exception:
            pass

    def _json(self, obj) -> bytes:
        return json.dumps(obj, separators=(",", ":"), ensure_ascii=True).encode("utf-8")

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
            404: "Not Found",
            405: "Method Not Allowed",
            500: "Internal Server Error",
        }.get(status, "OK")
        headers = [
            f"HTTP/1.1 {status} {reason}\r\n",
            f"Content-Type: {content_type}\r\n",
            f"Content-Length: {len(body)}\r\n",
            "Connection: close\r\n",
            "\r\n",
        ]
        writer.write("".join(headers).encode("ascii") + body)
        await writer.drain()

    async def _write_ws_text(self, writer: asyncio.StreamWriter, text: str) -> None:
        payload = text.encode("utf-8")
        header = bytearray([0x81])
        n = len(payload)
        if n < 126:
            header.append(n)
        elif n < 65536:
            header.append(126)
            header.extend(n.to_bytes(2, "big"))
        else:
            header.append(127)
            header.extend(n.to_bytes(8, "big"))
        writer.write(bytes(header) + payload)
        await writer.drain()

    async def _handle_ws(self, headers: dict[str, str], writer: asyncio.StreamWriter) -> None:
        key = headers.get("sec-websocket-key", "").strip()
        if not key:
            await self._write_response(writer, 400, self._json({"error": "missing websocket key"}))
            return
        accept = base64.b64encode(
            hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")).digest()
        ).decode("ascii")
        response = (
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Accept: {accept}\r\n"
            "\r\n"
        )
        writer.write(response.encode("ascii"))
        await writer.drain()
        self._ws_writers.add(writer)

        async def _run() -> None:
            last_seen: tuple[int, str] = (0, "")
            try:
                while True:
                    rows = await self.store.latest_spots(limit=1)
                    if rows:
                        row = rows[0]
                        marker = (int(row["epoch"]), str(row["raw"] or ""))
                        if marker != last_seen:
                            last_seen = marker
                            await self._write_ws_text(writer, json.dumps(self._spot_payload(row), separators=(",", ":")))
                    await asyncio.sleep(2.0)
            finally:
                await self._close_writer(writer)

        task = asyncio.create_task(_run(), name="public-web-ws-client")
        self._ws_clients.add(task)
        try:
            await task
        finally:
            self._ws_clients.discard(task)
            self._ws_writers.discard(writer)

    def _parse_limit(self, q: dict[str, list[str]], key: str, default: int, low: int, high: int) -> int:
        if key not in q or not q[key]:
            return default
        try:
            n = int(q[key][0])
        except ValueError:
            return default
        return max(low, min(high, n))

    def _spot_payload(self, row) -> dict[str, object]:
        freq = float(row["freq_khz"])
        comment = str(row["info"] or "")
        dx_call = str(row["dx_call"] or "")
        spotter = display_call(str(row["spotter"] or ""))
        stamp = datetime.fromtimestamp(int(row["epoch"]), tz=timezone.utc).isoformat()
        dx_ent = lookup(dx_call) if self._cty_loaded else None
        sp_ent = lookup(spotter) if self._cty_loaded else None
        return {
            "time": stamp,
            "freq": freq,
            "dx_call": dx_call,
            "spotter": spotter,
            "comment": comment,
            "band": freq_to_band(freq),
            "mode": detect_mode(comment, freq),
            "activity": detect_activity(comment),
            "dx_entity": dx_ent.name if dx_ent else "",
            "dx_continent": dx_ent.continent if dx_ent else "",
            "dx_cqz": dx_ent.cq_zone if dx_ent else 0,
            "dx_ituz": dx_ent.itu_zone if dx_ent else 0,
            "dx_lat": dx_ent.lat if dx_ent else 0.0,
            "dx_lon": dx_ent.lon if dx_ent else 0.0,
            "spotter_entity": sp_ent.name if sp_ent else "",
            "spotter_continent": sp_ent.continent if sp_ent else "",
            "spotter_lat": sp_ent.lat if sp_ent else 0.0,
            "spotter_lon": sp_ent.lon if sp_ent else 0.0,
        }

    def _serve_static_path(self, path: str) -> tuple[bytes, str] | None:
        root_txt = self.config.public_web.static_dir.strip()
        if not root_txt:
            return None
        root = Path(root_txt)
        rel = path.lstrip("/") or "index.html"
        if rel == "":
            rel = "index.html"
        target = (root / unquote(rel)).resolve()
        try:
            target.relative_to(root.resolve())
        except ValueError:
            return None
        if target.is_dir():
            target = target / "index.html"
        if not target.exists() or not target.is_file():
            return None
        ctype, _ = mimetypes.guess_type(str(target))
        return target.read_bytes(), (ctype or "application/octet-stream")

    async def _web_profile_snapshot(self, call: str) -> dict[str, str]:
        reg = await self.store.get_user_registry(call)
        row = dict(reg) if reg is not None else {}
        return {
            "name": str(row.get("display_name") or "").strip(),
            "qth": str(row.get("qth") or "").strip(),
            "qra": str(row.get("qra") or "").strip().upper(),
            "homenode": str(await self.store.get_user_pref(call, "homenode") or "").strip().upper(),
        }

    async def _api_spots(self, q: dict[str, list[str]]) -> list[dict[str, object]]:
        limit = self._parse_limit(q, "limit", 200, 1, 500)
        band = str(q.get("band", [""])[0] or "").strip()
        mode = str(q.get("mode", [""])[0] or "").strip()
        activity = str(q.get("activity", [""])[0] or "").strip()
        search = str(q.get("search", [""])[0] or "").strip().lower()
        rows = await self.store.latest_spots(limit=max(limit, 500 if any((band, mode, activity, search)) else limit))
        payload = [self._spot_payload(r) for r in rows]
        if band and band != "ALL":
            payload = [r for r in payload if r["band"] == band]
        if mode and mode != "ALL":
            payload = [r for r in payload if r["mode"] == mode]
        if activity and activity != "ALL":
            payload = [r for r in payload if r["activity"] == activity]
        if search:
            payload = [
                r for r in payload
                if search in str(r["dx_call"]).lower()
                or search in str(r["spotter"]).lower()
                or search in str(r["comment"]).lower()
            ]
        return payload[:limit]

    async def _api_bulletins(self, q: dict[str, list[str]]) -> list[dict[str, object]]:
        limit = self._parse_limit(q, "limit", 100, 1, 200)
        category = str(q.get("category", ["all"])[0] or "all").strip().lower()
        categories = ["announce", "chat", "wx", "wcy", "wwv"] if category in {"", "all", "*"} else [category]
        rows = []
        for cat in categories:
            if cat not in {"announce", "chat", "wx", "wcy", "wwv"}:
                continue
            rows.extend(await self.store.list_bulletins(cat, limit=limit))
        rows.sort(key=lambda row: (int(row["epoch"]), int(row["id"])), reverse=True)
        out: list[dict[str, object]] = []
        for row in rows[:limit]:
            out.append(
                {
                    "id": int(row["id"]),
                    "category": str(row["category"]),
                    "sender": display_call(str(row["sender"])),
                    "scope": str(row["scope"]),
                    "epoch": int(row["epoch"]),
                    "time": datetime.fromtimestamp(int(row["epoch"]), tz=timezone.utc).isoformat(),
                    "body": str(row["body"]),
                }
            )
        return out

    async def _api_stats(self) -> dict[str, object]:
        cutoff = int((datetime.now(timezone.utc) - timedelta(hours=24)).timestamp())
        rows = await self.store.latest_spots(limit=500)
        payload = [self._spot_payload(r) for r in rows if int(r["epoch"]) >= cutoff]
        bands: dict[str, int] = {}
        modes: dict[str, int] = {}
        for row in payload:
            band = str(row["band"])
            mode = str(row["mode"])
            if band:
                bands[band] = bands.get(band, 0) + 1
            if mode:
                modes[mode] = modes.get(mode, 0) + 1
        mode_rank = {mode: idx for idx, mode in enumerate(_MODE_ORDER)}
        return {
            "total": len(payload),
            "bands": [{"band": k, "count": v} for k, v in sorted(bands.items(), key=lambda kv: (-kv[1], kv[0]))],
            "modes": [
                {"mode": k, "count": v}
                for k, v in sorted(modes.items(), key=lambda kv: (mode_rank.get(kv[0], len(_MODE_ORDER)), -kv[1], kv[0]))
            ],
        }

    async def _api_leaderboard(self, q: dict[str, list[str]]) -> dict[str, object]:
        hours = self._parse_limit(q, "hours", 24, 1, 24)
        cutoff = int((datetime.now(timezone.utc) - timedelta(hours=hours)).timestamp())
        rows = await self.store.latest_spots(limit=500)
        payload = [self._spot_payload(r) for r in rows if int(r["epoch"]) >= cutoff]
        spotters: dict[str, int] = {}
        dx: dict[str, dict[str, object]] = {}
        entities: dict[str, int] = {}
        continents: dict[str, int] = {}
        bands: dict[str, int] = {}
        by_hour: dict[int, int] = {}
        band_hour: dict[tuple[str, int], int] = {}
        for row in payload:
            spotter = display_call(str(row["spotter"]))
            dx_call = str(row["dx_call"])
            ent = str(row["dx_entity"])
            cont = str(row["dx_continent"])
            band = str(row["band"])
            hour = datetime.fromisoformat(str(row["time"])).hour
            spotters[spotter] = spotters.get(spotter, 0) + 1
            item = dx.setdefault(dx_call, {"call": dx_call, "entity": ent, "count": 0})
            item["count"] = int(item["count"]) + 1
            if ent:
                entities[ent] = entities.get(ent, 0) + 1
            if cont:
                continents[cont] = continents.get(cont, 0) + 1
            if band:
                bands[band] = bands.get(band, 0) + 1
                band_hour[(band, hour)] = band_hour.get((band, hour), 0) + 1
            by_hour[hour] = by_hour.get(hour, 0) + 1
        return {
            "spotters": [{"call": k, "count": v} for k, v in sorted(spotters.items(), key=lambda kv: (-kv[1], kv[0]))[:15]],
            "dx": sorted(dx.values(), key=lambda item: (-int(item["count"]), str(item["call"])))[:15],
            "entities": [{"entity": k, "count": v} for k, v in sorted(entities.items(), key=lambda kv: (-kv[1], kv[0]))[:25]],
            "continents": [{"cont": k, "count": v} for k, v in sorted(continents.items(), key=lambda kv: (-kv[1], kv[0]))],
            "bands": [{"band": k, "count": v} for k, v in sorted(bands.items(), key=lambda kv: (-kv[1], kv[0]))],
            "by_hour": [{"hour": k, "count": v} for k, v in sorted(by_hour.items())],
            "band_hour": [{"band": b, "hour": h, "count": c} for (b, h), c in sorted(band_hour.items(), key=lambda item: (item[0][0], item[0][1]))],
        }

    async def _api_history(self) -> list[dict[str, object]]:
        rows = await self.store.latest_spots(limit=500)
        buckets: dict[str, dict[str, object]] = {}
        for row in rows:
            dt = datetime.fromtimestamp(int(row["epoch"]), tz=timezone.utc)
            key = dt.strftime("%Y-%m-%d")
            band = freq_to_band(float(row["freq_khz"]))
            entry = buckets.setdefault(key, {"date": key, "spots": 0, "bands": {}})
            entry["spots"] = int(entry["spots"]) + 1
            if band:
                bands = entry["bands"]
                bands[band] = int(bands.get(band, 0)) + 1
        out: list[dict[str, object]] = []
        for key in sorted(buckets.keys(), reverse=True)[:14]:
            entry = buckets[key]
            bands = dict(entry["bands"])
            top_band = ""
            if bands:
                top_band = sorted(bands.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]
            out.append({"date": key, "spots": entry["spots"], "top_band": top_band, "by_band": bands})
        return out

    async def _api_nodes(self) -> list[dict[str, object]]:
        prefs = await self.store.list_user_prefs(self.config.node.node_call)
        out: list[dict[str, object]] = []
        lat_s = str(prefs.get("forward_lat", "")).strip()
        lon_s = str(prefs.get("forward_lon", "")).strip()
        lat = 0.0
        lon = 0.0
        if lat_s and lon_s:
            try:
                lat = float(lat_s)
                lon = float(lon_s)
            except ValueError:
                lat = 0.0
                lon = 0.0
        out.append(
            {
                "call": self.config.node.node_call,
                "sort": "node",
                "lat": lat,
                "lon": lon,
                "qra": "",
                "qth": self.config.node.qth,
                "name": self.config.node.owner_name,
                "lastin": int(self.started_at.timestamp()),
                "family": "pycluster",
                "version": f"pyCluster {__version__}",
                "connected": True,
                "desired": True,
            }
        )
        return out

    async def _api_network(self) -> dict[str, object]:
        nodes = await self._api_nodes()
        peer_rows: list[dict[str, object]] = []
        links: list[list[str]] = []
        desired_rows: dict[str, dict[str, object]] = {}
        seen_calls: set[str] = {self.config.node.node_call}
        if self.link_desired_peers_fn:
            try:
                desired = await self.link_desired_peers_fn()
                desired_rows = {str(row.get("peer", "")).strip(): row for row in desired if str(row.get("peer", "")).strip()}
            except Exception:
                desired_rows = {}
        node_cfg = await self.store.list_user_prefs(self.config.node.node_call)
        if not desired_rows:
            desired_names: set[str] = set()
            for key, value in node_cfg.items():
                if not (key.startswith("peer.outbound.") and key.endswith(".name")):
                    continue
                name = str(value or "").strip()
                if not name or name in desired_names:
                    continue
                desired_names.add(name)
                slug = key[len("peer.outbound.") : -len(".name")]
                desired_rows[name] = {
                    "peer": name,
                    "dsn": str(node_cfg.get(f"peer.outbound.{slug}.dsn", "")).strip(),
                    "profile": str(node_cfg.get(f"peer.outbound.{slug}.profile", "")).strip().lower() or "unknown",
                    "reconnect_enabled": str(node_cfg.get(f"peer.outbound.{slug}.reconnect", "")).strip().lower() in {"1", "on", "yes", "true"},
                }
        if self.link_stats_fn:
            try:
                stats = await self.link_stats_fn()
                for name in sorted(stats):
                    ptag = re.sub(r"[^a-z0-9_.-]", "_", name.lower())
                    pfx = f"proto.peer.{ptag}."
                    family = str(node_cfg.get(pfx + "pc18.family", "")).strip().lower()
                    version = str(node_cfg.get(pfx + "pc18.summary", "")).strip()
                    peer_rows.append(
                        {
                            "call": name,
                            "entity": "",
                            "lat": 0.0,
                            "lon": 0.0,
                            "family": family or str(stats[name].get("profile", "")).strip().lower() or "unknown",
                            "version": version,
                            "connected": True,
                            "desired": name in desired_rows,
                            "last_pc_type": str(stats[name].get("last_pc_type", "")).strip().upper(),
                            "inbound": bool(stats[name].get("inbound", False)),
                        }
                    )
                    links.append([self.config.node.node_call, name])
                    seen_calls.add(name)
            except Exception:
                peer_rows = []
                links = []
        for name, row in desired_rows.items():
            if name in seen_calls:
                continue
            ptag = re.sub(r"[^a-z0-9_.-]", "_", name.lower())
            family = str(node_cfg.get(f"proto.peer.{ptag}.pc18.family", "")).strip().lower()
            version = str(node_cfg.get(f"proto.peer.{ptag}.pc18.summary", "")).strip()
            last_pc_type = str(node_cfg.get(f"proto.peer.{ptag}.last_pc_type", "")).strip().upper()
            last_epoch = str(node_cfg.get(f"proto.peer.{ptag}.last_epoch", "")).strip()
            try:
                connected = int(last_epoch or "0") > 0 and (int(datetime.now(timezone.utc).timestamp()) - int(last_epoch or "0")) <= 600
            except ValueError:
                connected = False
            peer_rows.append(
                {
                    "call": name,
                    "entity": "",
                    "lat": 0.0,
                    "lon": 0.0,
                    "family": family or str(row.get("profile", "")).strip().lower() or "unknown",
                    "version": version,
                    "connected": connected,
                    "desired": True,
                    "last_pc_type": last_pc_type,
                    "inbound": False,
                }
            )
            if connected:
                links.append([self.config.node.node_call, name])
            seen_calls.add(name)
        now_epoch = int(datetime.now(timezone.utc).timestamp())
        try:
            registry = await self.store.list_user_registry(limit=1000)
        except Exception:
            registry = []
        for row in registry:
            call = str(row["call"] or "").strip().upper()
            if not call or call in seen_calls or call == self.config.node.node_call.upper():
                continue
            family = str(await self.store.get_user_pref(call, "node_family") or "").strip().lower()
            if family not in {"pycluster", "dxspider", "dxnet", "arcluster", "clx"}:
                continue
            ptag = re.sub(r"[^a-z0-9_.-]", "_", call.lower())
            version = str(node_cfg.get(f"proto.peer.{ptag}.pc18.summary", "")).strip()
            last_pc_type = str(node_cfg.get(f"proto.peer.{ptag}.last_pc_type", "")).strip().upper()
            try:
                last_epoch = int(str(node_cfg.get(f"proto.peer.{ptag}.last_epoch", "")).strip() or "0")
            except ValueError:
                last_epoch = 0
            try:
                last_login_epoch = int(row["last_login_epoch"] or 0)
            except Exception:
                last_login_epoch = 0
            connected = last_epoch > 0 and (now_epoch - last_epoch) <= 600
            if not connected and last_login_epoch > 0 and (now_epoch - last_login_epoch) <= 600:
                connected = True
            peer_rows.append(
                {
                    "call": call,
                    "entity": "",
                    "lat": 0.0,
                    "lon": 0.0,
                    "family": family,
                    "version": version,
                    "connected": connected,
                    "desired": False,
                    "last_pc_type": last_pc_type,
                    "inbound": True,
                }
            )
            if connected:
                links.append([self.config.node.node_call, call])
            seen_calls.add(call)
        return {"nodes": nodes + peer_rows, "links": links, "home": self.config.node.node_call}

    async def _api_solar(self) -> tuple[dict[str, object], int]:
        try:
            req = urllib.request.Request(
                "https://www.hamqsl.com/solarxml.php",
                headers={"User-Agent": f"pyCluster/{__version__} (+{self.config.node.website_url or 'https://github.com/AI3I/pyCluster'})"},
            )
            with urllib.request.urlopen(req, timeout=10) as r:
                xml_bytes = r.read()
            root = ET.fromstring(xml_bytes)
            sd = root.find("solardata")

            def g(tag: str) -> str:
                el = sd.find(tag) if sd is not None else None
                return el.text.strip() if (el is not None and el.text) else ""

            cond: dict[str, str] = {}
            if sd is not None:
                for b in sd.findall("calculatedconditions/band"):
                    cond[f"{b.get('name', '')}_{b.get('time', '')}"] = b.text.strip() if b.text else ""
            vhf: list[dict[str, str]] = []
            if sd is not None:
                for ph in sd.findall("calculatedvhfconditions/phenomenon"):
                    vhf.append(
                        {
                            "name": ph.get("name", ""),
                            "location": ph.get("location", ""),
                            "condition": ph.text.strip() if ph.text else "",
                        }
                    )
            return ({
                "sfi": g("solarflux"),
                "sn": g("sunspots"),
                "a": g("aindex"),
                "k": g("kindex"),
                "xray": g("xray"),
                "solarwind": g("solarwind"),
                "aurora": g("aurora"),
                "updated": g("updated"),
                "conditions": cond,
                "vhf": vhf,
            }, 200)
        except Exception as exc:
            return ({"error": str(exc)}, 503)

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            head = await reader.readuntil(b"\r\n\r\n")
        except Exception:
            await self._close_writer(writer)
            return
        try:
            text = head.decode("utf-8", errors="replace")
            lines = text.split("\r\n")
            req_line = lines[0]
            method, target, _ = req_line.split(" ", 2)
            method = method.upper()
            parsed = urlparse(target)
            path = parsed.path or "/"
            q = parse_qs(parsed.query, keep_blank_values=True)
            headers = {}
            for line in lines[1:]:
                if not line or ":" not in line:
                    continue
                k, v = line.split(":", 1)
                headers[k.strip().lower()] = v.strip()
            body = b""
            if method in {"POST", "PUT", "PATCH"}:
                try:
                    content_len = int(headers.get("content-length", "0") or "0")
                except ValueError:
                    content_len = 0
                if content_len > 0:
                    body = await reader.readexactly(content_len)

            if path == "/ws":
                if method != "GET":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                await self._handle_ws(headers, writer)
                return

            if path == "/health":
                if method != "GET":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                await self._write_response(writer, 200, self._json({"ok": True}))
                return
            if path == "/api/auth/login":
                if method != "POST":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                payload = self._parse_json_body(body)
                call = normalize_call(str(payload.get("call", "")).strip())
                password = str(payload.get("password", ""))
                if not is_valid_call(call):
                    self._log_auth_failure(writer, headers, "public-web", call, "invalid_callsign")
                    await self._write_response(writer, 400, self._json({"error": "invalid callsign"}))
                    return
                _privilege, blocked_login = await self._access_subject(call)
                if blocked_login:
                    self._log_auth_failure(writer, headers, "public-web", call, "blocked_login")
                    await self._write_response(writer, 403, self._json({"error": "login blocked"}))
                    return
                if not await self._access_allowed(call, "web", "login"):
                    self._log_auth_failure(writer, headers, "public-web", call, "web_login_not_allowed")
                    await self._write_response(writer, 403, self._json({"error": "web login not allowed"}))
                    return
                expected = await self.store.get_user_pref(call, "password")
                if expected is None or not str(expected).strip() or not verify_password(password, str(expected)):
                    self._log_auth_failure(writer, headers, "public-web", call, "invalid_credentials")
                    await self._write_response(writer, 401, self._json({"error": "invalid credentials"}))
                    return
                if not is_password_hash(str(expected)):
                    await self.store.set_user_pref(call, "password", hash_password(password), int(time.time()))
                await self.store.record_login(
                    call,
                    int(time.time()),
                    describe_session_path(
                        "public-web",
                        writer.get_extra_info("peername") if hasattr(writer, "get_extra_info") else None,
                        writer.get_extra_info("sockname") if hasattr(writer, "get_extra_info") else None,
                        headers.get("x-forwarded-for", ""),
                    ),
                )
                token, exp = self._issue_web_token(call)
                access = await self._access_snapshot(call, "web")
                profile = await self._web_profile_snapshot(call)
                await self._write_response(
                    writer,
                    200,
                    self._json(
                        {
                            "ok": True,
                            "call": call,
                            "token": token,
                            "expires_epoch": exp,
                            "access": access,
                            "profile": profile,
                        }
                    ),
                )
                return
            if path == "/api/auth/logout":
                if method != "POST":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                tok = headers.get("x-web-token", "").strip()
                auth = headers.get("authorization", "").strip()
                if not tok and auth.lower().startswith("bearer "):
                    tok = auth[7:].strip()
                if tok:
                    self._web_sessions.pop(tok, None)
                await self._write_response(writer, 200, self._json({"ok": True}))
                return
            if path == "/api/auth/me":
                if method != "GET":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                call = self._web_call_from_headers(headers)
                if not call:
                    await self._write_response(writer, 401, self._json({"error": "web login required"}))
                    return
                await self._write_response(
                    writer,
                    200,
                    self._json(
                        {
                            "ok": True,
                            "call": call,
                            "access": await self._access_snapshot(call, "web"),
                            "profile": await self._web_profile_snapshot(call),
                        }
                    ),
                )
                return
            if path == "/api/spots":
                if method != "GET":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                await self._write_response(writer, 200, self._json(await self._api_spots(q)))
                return
            if path == "/api/bulletins":
                if method != "GET":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                await self._write_response(writer, 200, self._json(await self._api_bulletins(q)))
                return
            if path == "/api/profile":
                if method != "POST":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                call = self._web_call_from_headers(headers)
                if not call:
                    await self._write_response(writer, 401, self._json({"error": "web login required"}))
                    return
                payload = self._parse_json_body(body)
                name = str(payload.get("name", "")).strip()[:80]
                qth = str(payload.get("qth", "")).strip()[:80]
                qra = str(payload.get("qra", "")).strip().upper()[:16]
                homenode = normalize_call(str(payload.get("homenode", "")).strip())[:16]
                now = int(time.time())
                if qth:
                    coords = resolve_location_to_coords(qth)
                    if coords is not None:
                        qra = coords_to_locator(*coords)
                elif qra:
                    qra = extract_locator(qra)
                    if qra:
                        qth = estimate_location_from_locator(qra).strip()[:80]
                await self.store.upsert_user_registry(
                    call,
                    now,
                    display_name=name,
                    qth=qth,
                    qra=qra,
                )
                if homenode:
                    await self.store.set_user_pref(call, "homenode", homenode, now)
                else:
                    await self.store.delete_user_pref(call, "homenode")
                self._audit("user", f"{call} updated public profile")
                await self._write_response(
                    writer,
                    200,
                    self._json({"ok": True, "call": call, "profile": await self._web_profile_snapshot(call)}),
                )
                return
            if path == "/api/stats":
                if method != "GET":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                await self._write_response(writer, 200, self._json(await self._api_stats()))
                return
            if path == "/api/leaderboard":
                if method != "GET":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                await self._write_response(writer, 200, self._json(await self._api_leaderboard(q)))
                return
            if path == "/api/history":
                if method != "GET":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                await self._write_response(writer, 200, self._json(await self._api_history()))
                return
            if path == "/api/nodes":
                if method != "GET":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                await self._write_response(writer, 200, self._json(await self._api_nodes()))
                return
            if path == "/api/network":
                if method != "GET":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                await self._write_response(writer, 200, self._json(await self._api_network()))
                return
            if path == "/api/solar":
                if method != "GET":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                body, code = await self._api_solar()
                await self._write_response(writer, code, self._json(body))
                return
            if path == "/api/public/branding":
                if method != "GET":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                await self._write_response(writer, 200, self._json(await self._branding()))
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
                await self._write_response(writer, 200, self._json({"ok": True, "posted_by": call, "dx_call": dx_call, "freq_khz": freq_khz}))
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
                    await self._write_response(writer, 403, self._json({"error": f"{category} posting not allowed via web"}))
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
                await self._write_response(writer, 200, self._json({"ok": True, "posted_by": call, "category": category, "scope": scope}))
                return
            static = self._serve_static_path(path)
            if static is not None:
                body, ctype = static
                await self._write_response(writer, 200, body, ctype)
                return
            await self._write_response(writer, 404, self._json({"error": "not found"}))
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as exc:
            is_closing = getattr(writer, "is_closing", None)
            try:
                closing = bool(is_closing()) if callable(is_closing) else False
            except Exception:
                closing = False
            if not closing:
                try:
                    await self._write_response(writer, 500, self._json({"error": str(exc)}))
                except Exception:
                    pass
        finally:
            is_closing = getattr(writer, "is_closing", None)
            try:
                closing = bool(is_closing()) if callable(is_closing) else False
            except Exception:
                closing = False
            if not closing:
                await self._close_writer(writer)
