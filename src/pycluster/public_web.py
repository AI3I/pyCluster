from __future__ import annotations

import asyncio
import base64
from datetime import datetime, timezone, timedelta
import fnmatch
import hashlib
import json
import logging
import mimetypes
from pathlib import Path
import re
import secrets
import time
import tomllib
from urllib.parse import parse_qs, unquote, urlparse
import urllib.request

from . import __version__
from .auth_logging import log_auth_failure
from .access_policy import CLUSTER_NODE_FAMILIES, default_access_allowed
from .auth import hash_password, is_password_hash, verify_password
from .config import AppConfig, node_presentation_defaults
from .ctydat import load_cty, lookup
from .wpxloc import is_loaded as wpx_loaded, load_wpxloc, lookup as wpx_lookup
from .datafiles import describe_cty_file, describe_wpxloc_file
from .geomag import canonicalize_wwv_text
from .maidenhead import extract_locator
from .mfa import EmailOtpManager, SMTPMailer, generate_totp_secret, totp_otpauth_uri, verify_totp
from .qr_svg import qr_svg
from .rbn import is_rbn_spot
from .models import Spot, display_call, is_valid_call, normalize_call
from .pathmeta import describe_session_path
from .propagation import latest_wwv_snapshot, merge_solar_snapshots, parse_hamqsl_solar_xml, snapshot_payload
from .registration import has_valid_email, mark_email_verified, registration_state
from .spot_throttle import check_spot_throttle
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
    "registration_required",
    "verified_email_required_for_web",
    "verified_email_required_for_telnet",
    "initial_grace_logins",
    "support_contact",
    "website_url",
    "motd",
    "prompt_template",
    "telnet_ports",
}

_DEFAULT_MODE_ORDER = ["CW", "WSPR", "RTTY", "FT8", "FT4", "FT2", "JS8", "JT9", "JT65", "Q65", "MSK144", "SSB", "AM", "FM", "PSK"]
_DEFAULT_MODE_RULES = [
    {"pattern": r"\bFT8\b", "value": "FT8", "button": "FT8"},
    {"pattern": r"\bFT4\b", "value": "FT4", "button": "FT4"},
    {"pattern": r"\bFT2\b", "value": "FT2", "button": "FT2"},
    {"pattern": r"\bQ65\b", "value": "Q65", "button": "Q65"},
    {"pattern": r"\bMSK144\b", "value": "MSK144", "button": "MSK144"},
    {"pattern": r"\bFSK441\b", "value": "FSK441"},
    {"pattern": r"\bJS8\b", "value": "JS8", "button": "JS8"},
    {"pattern": r"\bJT65\b", "value": "JT65", "button": "JT65"},
    {"pattern": r"\bJT9\b", "value": "JT9", "button": "JT9"},
    {"pattern": r"\bWSPR\b", "value": "WSPR", "button": "WSPR"},
    {"pattern": r"\bRTTY\b", "value": "RTTY", "button": "RTTY"},
    {"pattern": r"\bMFSK\b", "value": "MFSK"},
    {"pattern": r"\bOLIVIA\b", "value": "OLIVIA"},
    {"pattern": r"\bDOMINO(?:EX)?\b", "value": "DOMINO"},
    {"pattern": r"\bTHOR\b", "value": "THOR"},
    {"pattern": r"\bHELL(?:SCHREIBER)?\b", "value": "HELL"},
    {"pattern": r"\bROS\b", "value": "ROS"},
    {"pattern": r"\bVARA\b", "value": "VARA"},
    {"pattern": r"\bPACTOR\b", "value": "PACTOR"},
    {"pattern": r"\bWINMOR\b", "value": "WINMOR"},
    {"pattern": r"\bARDOP\b", "value": "ARDOP"},
    {"pattern": r"\bPSK\d*\b", "value": "PSK"},
    {"pattern": r"\bFAX\b", "value": "FAX"},
    {"pattern": r"\bSSTV\b", "value": "SSTV"},
    {"pattern": r"\bATV\b", "value": "ATV"},
    {"pattern": r"\bDATA\b", "value": "DATA"},
    {"pattern": r"\bDIGI(?:TAL)?\b", "value": "DATA"},
    {"pattern": r"\bCW\b", "value": "CW", "button": "CW"},
    {"pattern": r"\b(LSB|USB|SSB)\b", "value": "SSB", "button": "SSB"},
    {"pattern": r"\bAM\b", "value": "AM", "button": "AM"},
    {"pattern": r"\bFM\b", "value": "FM", "button": "FM"},
]
_DEFAULT_ACTIVITY_RULES = [
    {"pattern": r"\bEME\b|\bMOONBOUNCE\b", "value": "EME", "button": "EME"},
    {"pattern": r"\bSAT\b|\bSATELLITE\b", "value": "SAT", "button": "SAT"},
    {"pattern": r"\bWWFF\b", "value": "WWFF", "button": "WWFF"},
    {"pattern": r"\bGMA\b", "value": "GMA", "button": "GMA"},
    {"pattern": r"\bSOTA\b", "value": "SOTA", "button": "SOTA"},
    {"pattern": r"\bPOTA\b|\bPARKS?\b", "value": "POTA", "button": "POTA"},
    {"pattern": r"\bIOTA\b", "value": "IOTA", "button": "IOTA"},
    {"pattern": r"\bBOTA\b", "value": "BOTA", "button": "BOTA"},
    {"pattern": r"\bLOTA\b", "value": "LOTA", "button": "LOTA"},
]
_DEFAULT_COMMENT_TAGS = [
    {"pattern": r"\bcq\b|\bcqing\b|\bcalling\b|\blistening\b", "label": "CQ", "color": "#10b981"},
    {"pattern": r"\bsplit\b|\bspilt\b|\bup\b|\bqsx|\bdn\b", "label": "QSX", "color": "#f59e0b"},
    {"pattern": r"\bqsy\b", "label": "QSY", "color": "#67e8f9"},
    {"pattern": r"\bqrt\b", "label": "QRT", "color": "#94a3b8"},
    {"pattern": r"\bcw\b|\bskcc\b|\bfists\b|\bhst\b", "label": "CW", "color": "#facc15"},
    {"pattern": r"\bft8\b|\bft4\b|\bft2\b|\bq65\b|\bmsk144\b|\bfsk441\b|\brtty\b|\bjs8\b|\bjt65\b|\bjt9\b|\bwspr\b|\bmfsk\b|\bolivia\b|\bdomino(?:ex)?\b|\bthor\b|\bhell(?:schreiber)?\b|\bros\b|\bvara\b|\bpactor\b|\bwinmor\b|\bardop\b|\bpsk\b|\bfax\b|\bsstv\b|\batv\b|\bdata\b|\bdigi(?:tal)?\b", "label": "DIGITAL", "color": "#a78bfa"},
    {"pattern": r"\bssb\b|\bam\b|\bfm\b", "label": "VOICE", "color": "#34d399"},
    {"pattern": r"\busb\b", "label": "USB", "color": "#818cf8"},
    {"pattern": r"\blsb\b", "label": "LSB", "color": "#6ee7b7"},
    {"pattern": r"\bqrm\b", "label": "QRM", "color": "#f87171"},
    {"pattern": r"\bqrp\b", "label": "QRP", "color": "#a78bfa"},
    {"pattern": r"\bqro\b", "label": "QRO", "color": "#fb923c"},
    {"pattern": r"\beme\b|\bmoonbounce\b", "label": "EME", "color": "#c084fc"},
    {"pattern": r"\bsat\b|\bsatellite\b", "label": "SAT", "color": "#67e8f9"},
    {"pattern": r"\bcontest\b|\bqso\s+party\b|\bparty\b|qp\b|\bcqww\b|\barrl\b|\bwpx\b|\bfield\s*day\b|\bwfd\b|\bfd\b|\bwas\b", "label": "CONTEST", "color": "#f472b6"},
    {"pattern": r"\bpile.?up\b", "label": "PILEUP", "color": "#ff9f43"},
    {"pattern": r"\bdxped", "label": "DXPED", "color": "#e879f9"},
    {"pattern": r"\blong\s+path\b", "label": "LONG PATH", "color": "#a3e635"},
    {"pattern": r"\bportable\b", "label": "PORTABLE", "color": "#14b8a6"},
    {"pattern": r"\bmobile\b", "label": "MOBILE", "color": "#06b6d4"},
    {"pattern": r"\bmaritime\b", "label": "MARITIME", "color": "#0ea5e9"},
    {"pattern": r"\bnet\b", "label": "NET", "color": "#34d399"},
    {"pattern": r"\bpirate\b", "label": "PIRATE", "color": "#f97316"},
    {"pattern": r"\bdx\b|\bdistance\b", "label": "DX", "color": "#3b82f6"},
    {"pattern": r"\batno\b", "label": "ATNO", "color": "#f85149"},
    {"pattern": r"\bnew\s*one\b|\bnew\s+ctry\b|\bnew\s+country\b|\bnewone\b", "label": "NEW ONE", "color": "#f85149"},
    {"pattern": r"\bspecial\b|\baward\b|\byear\b|\bday\b|\bses\b", "label": "SPECIAL", "color": "#c084fc"},
    {"pattern": r"\b59\b|\brst\s*59\b|\b5\/9\b|\b5-9\b", "label": "5/9", "color": "#3fb950"},
    {"pattern": r"\blotw\b", "label": "LoTW", "color": "#58a6ff"},
    {"pattern": r"\brbn\b|\bskimmer\b|\b\d{1,3}\s*db\b|\b\d{1,3}\s*wpm\b|\bq:\d+\b", "label": "RBN", "color": "#38bdf8"},
    {"pattern": r"\bbeacon\b", "label": "BEACON", "color": "#fbbf24"},
    {"pattern": r"\btnx\b|\bthx\b|\btks\b|\bthank|\b73\b", "label": "TNX", "color": "#fb7185"},
    {"pattern": r"\bwwff\b", "label": "WWFF", "color": "#e8d44d"},
    {"pattern": r"\bgma\b", "label": "GMA", "color": "#6ee7b7"},
    {"pattern": r"\bsota\b", "label": "SOTA", "color": "#3fb950"},
    {"pattern": r"\bpota\b|\bparks?\b", "label": "POTA", "color": "#58a6ff"},
    {"pattern": r"\biota\b", "label": "IOTA", "color": "#a78bfa"},
    {"pattern": r"\bbota\b", "label": "BOTA", "color": "#f97316"},
    {"pattern": r"\blota\b", "label": "LOTA", "color": "#fde68a"},
]
_DEFAULT_RARE_ENTITIES = [
    "North Korea", "Bouvet Island", "Peter 1 Island", "Crozet Island", "Heard Island",
    "Macquarie Island", "Kerguelen Island", "Amsterdam & St. Paul Is.", "South Georgia Is.",
    "South Sandwich Islands", "South Shetland Islands", "South Orkney Islands",
    "Scarborough Reef", "Pratas Island", "Spratly Islands", "Andaman & Nicobar Is.",
    "Lakshadweep Is.", "Navassa Island", "Desecheo Island", "Baker & Howland Is.",
    "Johnston Island", "Palmyra & Jarvis Is.", "Kure Island", "Midway Island",
    "Minami Torishima", "Mount Athos", "Annobon Island", "Market Reef",
    "Willis Island", "Mellish Reef", "Chesterfield Islands", "Ducie Island",
    "Austral Islands", "Clipperton Island", "Malpelo Island", "Juan Fernandez Islands",
    "Easter Island", "Agalega & St. Brandon Is.", "Glorioso Islands", "Tromelin Island",
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
        strings_path: str | None = None,
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
        self._wpx_loaded = False
        self._cty_mtime_ns = 0
        self._wpx_mtime_ns = 0
        self._ws_clients: set[asyncio.Task[None]] = set()
        self._ws_writers: set[asyncio.StreamWriter] = set()
        self._web_sessions: dict[str, tuple[str, int]] = {}
        self._smtp = SMTPMailer(config.smtp)
        self._mfa = EmailOtpManager(config.mfa, self._smtp.send_code, store)
        if not strings_path:
            bundled = Path(__file__).resolve().parents[2] / "config" / "strings.toml"
            strings_path = str(bundled) if bundled.exists() else None
        self._strings_path = Path(strings_path) if strings_path else None
        self._taxonomy_mtime_ns: int | None = None
        self._taxonomy_key = ""
        self._mode_rules: list[tuple[re.Pattern[str], str]] = []
        self._activity_rules: list[tuple[re.Pattern[str], str]] = []
        self._mode_filters: list[str] = []
        self._activity_filters: list[str] = []
        self._comment_tags: list[dict[str, object]] = []
        self._rare_entities: set[str] = set()
        self._mode_order: list[str] = list(_DEFAULT_MODE_ORDER)

    def _refresh_taxonomy(self) -> None:
        raw = {
            "mode_order": _DEFAULT_MODE_ORDER,
            "mode_rules": _DEFAULT_MODE_RULES,
            "activity_rules": _DEFAULT_ACTIVITY_RULES,
            "comment_tags": _DEFAULT_COMMENT_TAGS,
            "rare_entities": _DEFAULT_RARE_ENTITIES,
        }
        if self._strings_path:
            try:
                stat = self._strings_path.stat()
            except OSError:
                stat = None
            if stat is not None and self._taxonomy_mtime_ns == stat.st_mtime_ns and self._taxonomy_key:
                return
            if stat is not None and self._taxonomy_mtime_ns != stat.st_mtime_ns:
                try:
                    text = self._strings_path.read_text(encoding="utf-8")
                    marker = "[public_web.taxonomy]"
                    idx = text.find(marker)
                    if idx >= 0:
                        parsed = tomllib.loads(text[idx:])
                        node = parsed.get("public_web", {}).get("taxonomy", {})
                        if isinstance(node, dict):
                            raw = {
                                "mode_order": node.get("mode_order", _DEFAULT_MODE_ORDER),
                                "mode_rules": node.get("mode_rules", _DEFAULT_MODE_RULES),
                                "activity_rules": node.get("activity_rules", _DEFAULT_ACTIVITY_RULES),
                                "comment_tags": node.get("comment_tags", _DEFAULT_COMMENT_TAGS),
                                "rare_entities": node.get("rare_entities", _DEFAULT_RARE_ENTITIES),
                            }
                except Exception:
                    LOG.warning("public web taxonomy load failed for %s", self._strings_path, exc_info=True)
                self._taxonomy_mtime_ns = stat.st_mtime_ns
        key = json.dumps(raw, sort_keys=True)
        if key == self._taxonomy_key:
            return
        self._taxonomy_key = key
        self._mode_order = [str(item).strip().upper() for item in raw["mode_order"] if str(item).strip()]
        self._mode_rules = []
        self._activity_rules = []
        self._mode_filters = []
        self._activity_filters = []
        self._comment_tags = []
        self._rare_entities = {str(item).strip() for item in raw["rare_entities"] if str(item).strip()}
        for row in raw["mode_rules"]:
            if not isinstance(row, dict):
                continue
            pattern = str(row.get("pattern", "")).strip()
            value = str(row.get("value", "")).strip().upper()
            if not pattern or not value:
                continue
            self._mode_rules.append((re.compile(pattern, re.I), value))
            button = str(row.get("button", value)).strip().upper()
            if button and button not in self._mode_filters:
                self._mode_filters.append(button)
        for row in raw["activity_rules"]:
            if not isinstance(row, dict):
                continue
            pattern = str(row.get("pattern", "")).strip()
            value = str(row.get("value", "")).strip().upper()
            if not pattern or not value:
                continue
            self._activity_rules.append((re.compile(pattern, re.I), value))
            button = str(row.get("button", value)).strip().upper()
            if button and button not in self._activity_filters:
                self._activity_filters.append(button)
        for row in raw["comment_tags"]:
            if not isinstance(row, dict):
                continue
            pattern = str(row.get("pattern", "")).strip()
            label = str(row.get("label", "")).strip()
            color = str(row.get("color", "")).strip() or "#58a6ff"
            if not pattern or not label:
                continue
            self._comment_tags.append({
                "pattern": pattern,
                "label": label,
                "color": color,
                "button": str(row.get("button", label)).strip() or label,
            })

    def _detect_mode(self, comment: str, freq_khz: float) -> str:
        self._refresh_taxonomy()
        for rx, mode in self._mode_rules:
            if rx.search(comment):
                return mode
        mhz = freq_khz / 1000.0
        for lo, hi in _CW_RANGES:
            if lo <= mhz <= hi:
                return "CW"
        return ""

    def _detect_activity(self, comment: str) -> str:
        self._refresh_taxonomy()
        for rx, act in self._activity_rules:
            if rx.search(comment):
                return act
        return ""

    def _taxonomy_payload(self) -> dict[str, object]:
        self._refresh_taxonomy()
        return {
            "mode_filters": self._mode_filters,
            "activity_filters": ["RARE", *self._activity_filters],
            "comment_tags": [
                {"label": str(row["label"]), "button": str(row["button"]), "pattern": str(row["pattern"]), "color": str(row["color"])}
                for row in self._comment_tags
            ],
            "rare_entities": sorted(self._rare_entities),
        }

    def _audit(self, category: str, text: str) -> None:
        if self.event_log_fn:
            try:
                self.event_log_fn(category, text)
            except Exception:
                LOG.exception("public web audit log failed")

    async def _email_for_call(self, call: str) -> str:
        row = await self.store.get_user_registry(call)
        return str(row["email"] or "").strip() if row else ""

    async def _sysop_notification_emails(self) -> list[str]:
        rows = await self.store.list_user_registry(limit=200, privilege="sysop")
        out: list[str] = []
        seen: set[str] = set()
        for row in rows:
            email = str(row["email"] or "").strip()
            low = email.lower()
            if has_valid_email(email) and low not in seen:
                seen.add(low)
                out.append(email)
        return out

    async def _submit_registration_request(
        self,
        *,
        call: str,
        display_name: str,
        home_node: str,
        qth: str,
        qra: str,
        email: str,
        note: str,
        source: str,
        email_verified: bool,
    ) -> None:
        now = int(time.time())
        await self.store.upsert_user_registry(
            call,
            now,
            display_name=display_name,
            home_node=home_node,
            qth=qth,
            qra=qra,
            email=email,
            privilege="",
        )
        base_call = call.split("-", 1)[0]
        if email_verified:
            await mark_email_verified(self.store, base_call, now_epoch=now)
        await self.store.upsert_registration_request(
            call,
            now,
            display_name=display_name,
            home_node=home_node,
            qth=qth,
            qra=qra,
            email=email,
            note=note,
            source=source,
            email_verified=email_verified,
            status="pending",
        )
        if not self._smtp.enabled():
            return
        sysops = await self._sysop_notification_emails()
        subject = f"pyCluster registration request for {call}"
        body = (
            f"A new pyCluster registration request is pending for {call}.\n\n"
            f"Name: {display_name or '-'}\n"
            f"Home node: {home_node or '-'}\n"
            f"QTH: {qth or '-'}\n"
            f"Grid: {qra or '-'}\n"
            f"Email: {email or '-'}\n"
            f"Source: {source}\n"
            f"Email verified: {'yes' if email_verified else 'no'}\n"
            f"Note: {note or '-'}\n"
        )
        for rcpt in sysops:
            try:
                self._smtp.send_code(rcpt, subject, body)
            except Exception:
                LOG.exception("public web registration notification failed rcpt=%s call=%s", rcpt, call)
        try:
            self._smtp.send_code(
                email,
                f"pyCluster registration request received for {call}",
                (
                    f"Your pyCluster registration request for {call} has been received.\n\n"
                    "A system operator will review it. You may not have posting or login "
                    "privileges until it is approved.\n"
                ),
            )
        except Exception:
            LOG.exception("public web registration acknowledgement failed call=%s email=%s", call, email)

    @staticmethod
    def _has_valid_email(email: str) -> bool:
        return has_valid_email(email)

    async def _mfa_required_for_call(self, call: str, *, is_sysop: bool) -> bool:
        base_call = call.split("-", 1)[0]
        if await self._totp_secret_for_call(call):
            return True
        override = ""
        for candidate in (call.upper(), base_call.upper()):
            raw = await self.store.get_user_pref(candidate, "mfa_email_otp")
            txt = str(raw or "").strip().lower()
            if txt:
                override = txt
                break
        if override == "required":
            return True
        if override == "off":
            return False
        if not self._mfa.required_for(is_sysop=is_sysop):
            return False
        return has_valid_email(await self._email_for_call(base_call.upper()))

    async def _totp_secret_for_call(self, call: str) -> str:
        base_call = call.split("-", 1)[0].upper()
        for candidate in (call.upper(), base_call):
            raw = await self.store.get_user_pref(candidate, "mfa_totp_secret")
            secret = str(raw or "").strip()
            if secret:
                return secret
        return ""

    async def _mfa_snapshot(self, call: str) -> dict[str, object]:
        base_call = call.split("-", 1)[0].upper()
        raw = str(await self.store.get_user_pref(base_call, "mfa_email_otp") or "").strip().lower()
        email_override = raw if raw in {"required", "off"} else "default"
        totp_enabled = bool(str(await self.store.get_user_pref(base_call, "mfa_totp_secret") or "").strip())
        reg = await self.store.get_user_registry(base_call)
        is_sysop = str(reg["privilege"] or "").strip().lower() in {"sysop", "admin"} if reg is not None else False
        if email_override == "required":
            email_effective = True
        elif email_override == "off":
            email_effective = False
        else:
            email_effective = self._mfa.required_for(is_sysop=is_sysop)
        methods = []
        if totp_enabled:
            methods.append("Authenticator")
        if email_effective and not totp_enabled:
            methods.append("Email OTP")
        return {
            "email_otp": email_override,
            "totp_enabled": totp_enabled,
            "enabled": bool(totp_enabled or email_effective),
            "methods": methods,
            "policy": "required override" if email_override == "required" else ("off override" if email_override == "off" else ("node policy" if email_effective else "not required by node policy")),
        }

    def _dataset_status(self) -> dict[str, dict[str, object]]:
        self._refresh_datafiles_if_changed()
        return {
            "cty": describe_cty_file(self.config.public_web.cty_dat_path, loaded=self._cty_loaded).to_json(),
            "wpxloc": describe_wpxloc_file(self.config.public_web.wpxloc_raw_path, loaded=self._wpx_loaded).to_json(),
        }

    def _refresh_datafiles_if_changed(self) -> None:
        cty_path = self.config.public_web.cty_dat_path.strip()
        if cty_path:
            try:
                mtime = Path(cty_path).stat().st_mtime_ns
                if mtime != self._cty_mtime_ns:
                    load_cty(cty_path)
                    self._cty_loaded = True
                    self._cty_mtime_ns = mtime
            except Exception as exc:
                self._cty_loaded = False
                LOG.warning("public web cty reload failed from %s: %s", cty_path, exc)
        wpx_path = self.config.public_web.wpxloc_raw_path.strip()
        if wpx_path:
            try:
                mtime = Path(wpx_path).stat().st_mtime_ns
                if mtime != self._wpx_mtime_ns:
                    load_wpxloc(wpx_path)
                    self._wpx_loaded = True
                    self._wpx_mtime_ns = mtime
            except Exception as exc:
                self._wpx_loaded = False
                LOG.warning("public web wpxloc reload failed from %s: %s", wpx_path, exc)

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
            "datasets": self._dataset_status(),
            "support_contact": support_contact,
            "website_url": website_url,
            "page_title": f"{title}{title_suffix}",
            "header_title": title,
            "footer_primary": footer_primary,
            "footer_secondary": footer_secondary,
            "home_node": node_call,
            "telnet_ports": telnet_ports,
            "ui_strings": self._public_ui_strings(),
        }

    def _public_ui_strings(self) -> dict[str, str]:
        defaults = {
            "login_mfa_email": "Enter the code sent to your email.",
            "login_mfa_authenticator": "Enter the code from your authenticator app.",
            "profile_saving": "Saving...",
            "profile_updated": "Profile updated.",
            "profile_update_failed": "Profile update failed.",
            "profile_mfa_updating": "Updating MFA...",
            "profile_mfa_scan_qr": "Scan the QR code, then enter the authenticator code to verify setup.",
            "profile_mfa_qr_unavailable": "Authenticator setup started, but QR setup is unavailable.",
            "profile_mfa_email_sent": "Email MFA code sent. Check your email, then enter the code.",
            "profile_mfa_email_switched": "MFA method switched to Email. Use Verify to send and validate an email code.",
            "profile_mfa_verified": "MFA code verified.",
            "profile_mfa_disabled": "MFA disabled.",
            "profile_mfa_updated": "MFA updated.",
            "profile_mfa_update_failed": "MFA update failed.",
            "profile_mfa_email_prompt": "Email MFA code sent. Enter the code:",
            "profile_mfa_email_cancelled": "Email MFA verification cancelled.",
            "profile_mfa_authenticator_prompt": "Enter your authenticator code:",
            "profile_mfa_enter_code": "Enter the MFA code first.",
            "presets_login_required": "Log in to save presets.",
            "presets_save_failed": "Saving presets failed:",
            "presets_load_failed": "Loading presets failed:",
        }
        if not self._strings_path:
            return defaults
        try:
            data = tomllib.loads(self._strings_path.read_text(encoding="utf-8"))
            node = data.get("public_web", {}).get("ui", {})
        except Exception:
            LOG.warning("public web ui strings load failed for %s", self._strings_path, exc_info=True)
            return defaults
        if not isinstance(node, dict):
            return defaults
        out = dict(defaults)
        for key in defaults:
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                out[key] = value
        return out

    async def start(self) -> None:
        if not self.config.public_web.enabled:
            return
        cty_path = self.config.public_web.cty_dat_path.strip()
        if cty_path:
            try:
                load_cty(cty_path)
                self._cty_loaded = True
                self._cty_mtime_ns = Path(cty_path).stat().st_mtime_ns
            except Exception as exc:
                LOG.warning("public web cty load failed from %s: %s", cty_path, exc)
        wpx_path = self.config.public_web.wpxloc_raw_path.strip()
        if wpx_path:
            try:
                load_wpxloc(wpx_path)
                self._wpx_loaded = True
                self._wpx_mtime_ns = Path(wpx_path).stat().st_mtime_ns
            except Exception as exc:
                LOG.warning("public web wpxloc load failed from %s: %s", wpx_path, exc)
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
        target_row = await self.store.get_user_registry(target)
        target_exists = target_row is not None
        for candidate in ((target,) if target_exists else (target, base)):
            node_family = str(await self.store.get_user_pref(candidate, "node_family") or "").strip().lower()
            if node_family in CLUSTER_NODE_FAMILIES:
                return "", False
        block_candidates = (target,) if target_exists else (target, base)
        for candidate in block_candidates:
            raw_block = await self.store.get_user_pref(candidate, "blocked_login")
            if str(raw_block or "").strip().lower() in {"1", "on", "yes", "true"}:
                blocked_login = True
        if target_exists:
            privilege = str(target_row["privilege"] or "").strip().lower()
            if not privilege:
                privilege = str(await self.store.get_user_pref(target, "privilege") or "").strip().lower()
            return privilege, blocked_login
        privilege = ""
        for candidate in (target, base):
            row = await self.store.get_user_registry(candidate)
            if row and not privilege:
                privilege = str(row["privilege"] or "").strip().lower()
            if not privilege:
                privilege = str(await self.store.get_user_pref(candidate, "privilege") or "").strip().lower()
        return privilege, blocked_login

    async def _access_snapshot(self, call: str, channel: str) -> dict[str, bool]:
        caps = ["login", "spots", "rbn", "chat", "announce", "wx", "wcy", "wwv"]
        out: dict[str, bool] = {}
        for cap in caps:
            out[cap] = await self._access_allowed(call, channel, cap)
        return out

    async def _access_allowed(self, call: str, channel: str, capability: str) -> bool:
        target = call.upper()
        base = target.split("-", 1)[0]
        target_exists = await self.store.get_user_registry(target) is not None
        candidates = (target,) if target_exists else (target, base)
        for candidate in candidates:
            node_family = str(await self.store.get_user_pref(candidate, "node_family") or "").strip().lower()
            if node_family in CLUSTER_NODE_FAMILIES:
                return True
        for candidate in candidates:
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
        self._refresh_datafiles_if_changed()
        freq = float(row["freq_khz"])
        comment = str(row["info"] or "")
        dx_call = str(row["dx_call"] or "")
        spotter = display_call(str(row["spotter"] or ""))
        stamp = datetime.fromtimestamp(int(row["epoch"]), tz=timezone.utc).isoformat()
        dx_ent = lookup(dx_call) if self._cty_loaded else None
        if dx_ent is None and self._wpx_loaded:
            dx_ent = wpx_lookup(dx_call)
        sp_ent = lookup(spotter) if self._cty_loaded else None
        if sp_ent is None and self._wpx_loaded:
            sp_ent = wpx_lookup(spotter)
        is_rbn = is_rbn_spot(dx_call, spotter, comment)
        return {
            "time": stamp,
            "freq": freq,
            "dx_call": dx_call,
            "spotter": spotter,
            "comment": comment,
            "band": freq_to_band(freq),
            "mode": self._detect_mode(comment, freq),
            "activity": self._detect_activity(comment),
            "dx_entity": dx_ent.name if dx_ent else "",
            "dx_continent": dx_ent.continent if dx_ent else "",
            "dx_cqz": dx_ent.cq_zone if dx_ent else 0,
            "dx_ituz": dx_ent.itu_zone if dx_ent else 0,
            "dx_lat": dx_ent.lat if dx_ent else 0.0,
            "dx_lon": dx_ent.lon if dx_ent else 0.0,
            "spotter_entity": sp_ent.name if sp_ent else "",
            "spotter_continent": sp_ent.continent if sp_ent else "",
            "spotter_cqz": sp_ent.cq_zone if sp_ent else 0,
            "spotter_ituz": sp_ent.itu_zone if sp_ent else 0,
            "spotter_lat": sp_ent.lat if sp_ent else 0.0,
            "spotter_lon": sp_ent.lon if sp_ent else 0.0,
            "is_rbn": is_rbn,
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

    def _watch_rule_from_filter_expr(self, expr: str, *, slot: int) -> dict[str, str] | None:
        text = str(expr or "").strip()
        if not text:
            return None
        toks = text.split()
        if not toks:
            return None
        first = toks[0].lower()
        rest = " ".join(toks[1:]).strip()
        source = f"accept/spots {slot}"
        if first == "on" and rest:
            return {"type": "band", "value": rest.split()[0].upper(), "source": source}
        if first == "by" and rest:
            return {"type": "spotter", "value": rest.upper(), "source": source}
        if first in {"dx", "call", "callsign"} and rest:
            return {"type": "call", "value": rest.upper(), "source": source}
        if first == "call_zone" and rest:
            rules: list[dict[str, str]] = []
            for token in re.split(r"[,\s]+", rest):
                token = token.strip()
                if token.isdigit():
                    rules.append({"type": "cqzone", "value": token, "source": source})
            return rules[0] if len(rules) == 1 else {"type": "multi", "value": json.dumps(rules), "source": source}
        if first == "call_itu" and rest:
            rules = []
            for token in re.split(r"[,\s]+", rest):
                token = token.strip()
                if token.isdigit():
                    rules.append({"type": "ituzone", "value": token, "source": source})
            return rules[0] if len(rules) == 1 else {"type": "multi", "value": json.dumps(rules), "source": source}
        if first == "call_dxcc" and rest:
            values = [token.strip().upper() for token in re.split(r"[,\s]+", rest) if token.strip()]
            return {"type": "entity", "value": values[0], "source": source} if len(values) == 1 else {
                "type": "multi",
                "value": json.dumps([{"type": "entity", "value": v, "source": source} for v in values]),
                "source": source,
            }
        if first == "info" and rest:
            return {"type": "comment", "value": rest.upper(), "source": source}
        return None

    async def _watch_seed_for_call(self, call: str) -> list[dict[str, str]]:
        seeds: list[dict[str, str]] = []
        seen: set[tuple[str, str, str]] = set()
        for buddy in await self.store.list_buddies(call):
            item = {"type": "call", "value": str(buddy or "").strip().upper(), "source": "buddy"}
            key = (item["type"], item["value"], item["source"])
            if item["value"] and key not in seen:
                seen.add(key)
                seeds.append(item)
        for row in await self.store.list_filter_rules(call):
            if str(row["family"] or "").strip().lower() != "spots":
                continue
            if str(row["action"] or "").strip().lower() != "accept":
                continue
            mapped = self._watch_rule_from_filter_expr(str(row["expr"] or ""), slot=int(row["slot"] or 0))
            if not mapped:
                continue
            items = []
            if mapped.get("type") == "multi":
                try:
                    items = [item for item in json.loads(mapped["value"]) if isinstance(item, dict)]
                except Exception:
                    items = []
            else:
                items = [mapped]
            for item in items:
                rule = {
                    "type": str(item.get("type") or "").strip().lower(),
                    "value": str(item.get("value") or "").strip().upper(),
                    "source": str(item.get("source") or f"accept/spots {int(row['slot'] or 0)}").strip(),
                }
                key = (rule["type"], rule["value"], rule["source"])
                if rule["type"] and rule["value"] and key not in seen:
                    seen.add(key)
                    seeds.append(rule)
        return seeds

    async def _web_profile_snapshot(self, call: str) -> dict[str, object]:
        reg = await self.store.get_user_registry(call)
        row = dict(reg) if reg is not None else {}
        return {
            "name": str(row.get("display_name") or "").strip(),
            "qth": str(row.get("qth") or "").strip(),
            "qra": str(row.get("qra") or "").strip().upper(),
            "email": str(row.get("email") or "").strip(),
            "homenode": str(await self.store.get_user_pref(call, "homenode") or "").strip().upper(),
            "mfa": await self._mfa_snapshot(call),
            "watch_seed": await self._watch_seed_for_call(call),
        }

    def _sanitize_named_presets(self, value: object, *, max_items: int = 40) -> list[dict[str, object]]:
        if not isinstance(value, list):
            return []
        out: list[dict[str, object]] = []
        seen: set[str] = set()
        for item in value:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()[:40]
            if not name or name.lower() in seen:
                continue
            clean = dict(item)
            clean["name"] = name
            out.append(clean)
            seen.add(name.lower())
            if len(out) >= max_items:
                break
        return out

    def _sanitize_watch_rules(self, value: object, *, max_items: int = 80) -> list[dict[str, object]]:
        if not isinstance(value, list):
            return []
        allowed = {"call", "spotter", "entity", "prefix", "mode", "band", "activity", "cqzone", "ituzone", "comment"}
        out: list[dict[str, object]] = []
        seen: set[tuple[str, str]] = set()
        for item in value:
            if not isinstance(item, dict):
                continue
            typ = str(item.get("type") or "call").strip().lower()
            val = str(item.get("value") or "").strip().upper()
            if typ not in allowed or not val:
                continue
            key = (typ, val)
            if key in seen:
                continue
            seen.add(key)
            out.append({
                "type": typ,
                "value": val[:80],
                "hits": max(0, int(item.get("hits") or 0)),
                "last": str(item.get("last") or "").strip()[:80],
                "sound": bool(item.get("sound", True)),
                "toast": bool(item.get("toast", True)),
            })
            if len(out) >= max_items:
                break
        return out

    def _sanitize_watch_matches(self, value: object, *, max_items: int = 10) -> list[dict[str, object]]:
        if not isinstance(value, list):
            return []
        out: list[dict[str, object]] = []
        for item in value:
            if not isinstance(item, dict):
                continue
            out.append({
                "dx_call": str(item.get("dx_call") or "").strip().upper()[:24],
                "rule_type": str(item.get("rule_type") or "").strip()[:32],
                "rule_value": str(item.get("rule_value") or "").strip().upper()[:80],
                "band": str(item.get("band") or "").strip()[:16],
                "mode": str(item.get("mode") or "").strip()[:16],
                "time": str(item.get("time") or "").strip()[:80],
                "spotter": str(item.get("spotter") or "").strip().upper()[:24],
            })
            if len(out) >= max_items:
                break
        return out

    async def _public_presets_snapshot(self, call: str) -> dict[str, object]:
        raw = await self.store.get_user_pref(call, "public.presets")
        data: object = {}
        if raw:
            try:
                data = json.loads(raw)
            except Exception:
                data = {}
        if not isinstance(data, dict):
            data = {}
        return {
            "watch_profiles": self._sanitize_named_presets(data.get("watch_profiles")),
            "filter_presets": self._sanitize_named_presets(data.get("filter_presets")),
            "watch_rules": self._sanitize_watch_rules(data.get("watch_rules")),
            "watch_matches": self._sanitize_watch_matches(data.get("watch_matches")),
        }

    async def _save_public_presets(self, call: str, payload: dict[str, object]) -> dict[str, object]:
        current = await self._public_presets_snapshot(call)
        if "watch_profiles" in payload:
            current["watch_profiles"] = self._sanitize_named_presets(payload.get("watch_profiles"))
        if "filter_presets" in payload:
            current["filter_presets"] = self._sanitize_named_presets(payload.get("filter_presets"))
        if "watch_rules" in payload:
            current["watch_rules"] = self._sanitize_watch_rules(payload.get("watch_rules"))
        if "watch_matches" in payload:
            current["watch_matches"] = self._sanitize_watch_matches(payload.get("watch_matches"))
        await self.store.set_user_pref(call, "public.presets", json.dumps(current, separators=(",", ":")), int(time.time()))
        return current

    def _spot_payload_matches_expr(self, spot: dict[str, object], expr: str) -> bool:
        text = str(expr or "").strip()
        if not text:
            return False
        low = text.lower()
        toks = low.split()
        if not toks:
            return False
        first = toks[0]
        rest = " ".join(toks[1:]).strip()
        dx_call = str(spot.get("dx_call") or "").upper()
        spotter = str(spot.get("spotter") or "").upper()
        comment = str(spot.get("comment") or "")

        if first == "on" and rest:
            return str(spot.get("band") or "").lower() == rest.split()[0].lower()
        if first == "by" and rest:
            pat = rest.upper()
            return fnmatch.fnmatchcase(spotter, pat) if any(ch in pat for ch in "*?") else spotter.startswith(pat)
        if first in {"dx", "call"} and rest:
            pat = rest.upper()
            return fnmatch.fnmatchcase(dx_call, pat) if any(ch in pat for ch in "*?") else dx_call.startswith(pat)
        if first == "call_zone" and rest:
            wanted = self._parse_zone_spec(rest, 1, 40)
            return bool(wanted) and int(spot.get("dx_cqz") or 0) in wanted
        if first == "call_itu" and rest:
            wanted = self._parse_zone_spec(rest, 1, 90)
            return bool(wanted) and int(spot.get("dx_ituz") or 0) in wanted
        if first in {"spotter_cont", "by_cont"} and rest:
            wanted = {tok.strip().upper() for tok in re.split(r"[,\s]+", rest) if tok.strip()}
            return bool(wanted) and str(spot.get("spotter_continent") or "").upper() in wanted
        if first in {"spotter_zone", "by_zone"} and rest:
            ent = lookup(spotter) if self._cty_loaded else None
            if ent is None and self._wpx_loaded:
                ent = wpx_lookup(spotter)
            wanted = self._parse_zone_spec(rest, 1, 40)
            return bool(ent and wanted) and ent.cq_zone in wanted
        if first in {"spotter_itu", "by_itu"} and rest:
            ent = lookup(spotter) if self._cty_loaded else None
            if ent is None and self._wpx_loaded:
                ent = wpx_lookup(spotter)
            wanted = self._parse_zone_spec(rest, 1, 90)
            return bool(ent and wanted) and ent.itu_zone in wanted
        if first == "rbn":
            if not bool(spot.get("is_rbn")):
                return False
            return True if not rest else self._spot_payload_matches_expr(spot, rest)
        if first == "info" and rest:
            return rest in comment.lower()
        hay = f"{spot.get('freq') or ''} {dx_call} {spotter} {comment}".lower()
        return low in hay

    async def _spot_passes_stored_filters(self, call: str, spot: dict[str, object]) -> bool:
        accept: list[tuple[int, str]] = []
        reject: list[tuple[int, str]] = []
        for row in await self.store.list_filter_rules(call):
            if str(row["family"] or "").strip().lower() != "spots":
                continue
            action = str(row["action"] or "").strip().lower()
            item = (int(row["slot"] or 0), str(row["expr"] or ""))
            if action == "accept":
                accept.append(item)
            elif action == "reject":
                reject.append(item)
        matches: list[tuple[int, str]] = []
        for slot, expr in accept:
            if self._spot_payload_matches_expr(spot, expr):
                matches.append((slot, "accept"))
        for slot, expr in reject:
            if self._spot_payload_matches_expr(spot, expr):
                matches.append((slot, "reject"))
        if matches:
            matches.sort(key=lambda item: (item[0], 0 if item[1] == "reject" else 1))
            return matches[0][1] == "accept"
        return not accept

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

    async def _spot_passes_public_policy(self, call: str, spot: dict[str, object]) -> bool:
        if not bool(spot.get("is_rbn")):
            return True
        if not call:
            return False
        return await self._access_allowed(call, "web", "rbn")

    async def _api_spots(self, q: dict[str, list[str]], call: str = "") -> list[dict[str, object]]:
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
        filtered_policy = []
        for spot in payload:
            if await self._spot_passes_public_policy(call, spot):
                filtered_policy.append(spot)
        payload = filtered_policy
        if call:
            filtered = []
            for spot in payload:
                if await self._spot_passes_stored_filters(call, spot):
                    filtered.append(spot)
            payload = filtered
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

    async def _api_stats(self, q: dict[str, list[str]]) -> dict[str, object]:
        hours = self._parse_limit(q, "hours", 24, 1, 24)
        cutoff = int((datetime.now(timezone.utc) - timedelta(hours=hours)).timestamp())
        rows = await self.store.spots_since_epoch(cutoff)
        payload = [self._spot_payload(r) for r in rows if int(r["epoch"]) >= cutoff]
        bands: dict[str, int] = {}
        modes: dict[str, int] = {}
        entities: set[str] = set()
        for row in payload:
            band = str(row["band"])
            mode = str(row["mode"])
            entity = str(row["dx_entity"])
            if band:
                bands[band] = bands.get(band, 0) + 1
            if mode:
                modes[mode] = modes.get(mode, 0) + 1
            if entity:
                entities.add(entity)
        self._refresh_taxonomy()
        mode_rank = {mode: idx for idx, mode in enumerate(self._mode_order)}
        band_rows = [{"band": k, "count": v} for k, v in sorted(bands.items(), key=lambda kv: (-kv[1], kv[0]))]
        mode_rows = [
            {"mode": k, "count": v}
            for k, v in sorted(modes.items(), key=lambda kv: (mode_rank.get(kv[0], len(self._mode_order)), -kv[1], kv[0]))
        ]
        return {
            "hours": hours,
            "total": len(payload),
            "bands": band_rows,
            "modes": mode_rows,
            "dxcc_entities": len(entities),
            "top_band": band_rows[0]["band"] if band_rows else "",
            "top_mode": mode_rows[0]["mode"] if mode_rows else "",
        }

    async def _api_leaderboard(self, q: dict[str, list[str]]) -> dict[str, object]:
        hours = self._parse_limit(q, "hours", 24, 1, 24)
        cutoff = int((datetime.now(timezone.utc) - timedelta(hours=hours)).timestamp())
        rows = await self.store.spots_since_epoch(cutoff)
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
        cutoff = int((datetime.now(timezone.utc) - timedelta(days=14)).timestamp())
        rows = await self.store.spots_since_epoch(cutoff)
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
        def proto_value(peer_name: str, key: str) -> str:
            ptag = re.sub(r"[^a-z0-9_.-]", "_", peer_name.lower())
            return str(node_cfg.get(f"proto.peer.{ptag}.{key}", "")).strip()

        def proto_version(peer_name: str) -> str:
            return proto_value(peer_name, "pc18.summary") or proto_value(peer_name, "pc18.software")

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
                    family = proto_value(name, "pc18.family").lower()
                    version = proto_version(name) or str(stats[name].get("version", "") or stats[name].get("pc18_summary", "") or "").strip()
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
            family = proto_value(name, "pc18.family").lower()
            version = proto_version(name) or str(row.get("version", "") or row.get("pc18_summary", "") or "").strip()
            last_pc_type = proto_value(name, "last_pc_type").upper()
            last_epoch = proto_value(name, "last_epoch")
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
            version = proto_version(call)
            last_pc_type = proto_value(call, "last_pc_type").upper()
            try:
                last_epoch = int(proto_value(call, "last_epoch") or "0")
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
        wwv_snapshot = None
        try:
            wwv_snapshot = latest_wwv_snapshot(await self.store.list_bulletins("wwv", limit=24))
        except Exception:
            LOG.exception("public solar WWV snapshot load failed")
            wwv_snapshot = None
        hamqsl_snapshot = None
        try:
            req = urllib.request.Request(
                "https://www.hamqsl.com/solarxml.php",
                headers={"User-Agent": f"pyCluster/{__version__} (+{self.config.node.website_url or 'https://github.com/AI3I/pyCluster'})"},
            )
            with urllib.request.urlopen(req, timeout=10) as r:
                xml_bytes = r.read()
            hamqsl_snapshot = parse_hamqsl_solar_xml(xml_bytes)
        except Exception as exc:
            if wwv_snapshot is None:
                return ({"error": str(exc)}, 503)
            LOG.warning("public solar HamQSL fetch failed; serving WWV snapshot only: %s", exc)
        snapshot = merge_solar_snapshots(wwv_snapshot, hamqsl_snapshot)
        if snapshot is None:
            return ({"error": "solar data unavailable"}, 503)
        payload = snapshot_payload(snapshot)
        payload["hamqsl_source"] = bool(hamqsl_snapshot is not None)
        payload["wwv_source"] = bool(wwv_snapshot is not None)
        return (payload, 200)

    async def _api_kp(self) -> tuple[dict[str, object], int]:
        try:
            req = urllib.request.Request(
                "https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json",
                headers={
                    "User-Agent": f"pyCluster/{__version__} (+{self.config.node.website_url or 'https://github.com/AI3I/pyCluster'})",
                    "Accept": "application/json",
                },
            )
            with urllib.request.urlopen(req, timeout=10) as r:
                payload = json.loads(r.read().decode("utf-8", errors="replace"))
            day_map: dict[str, float] = {}
            rows = payload[1:] if isinstance(payload, list) and payload and isinstance(payload[0], list) else payload
            if isinstance(rows, list):
                for row in rows:
                    if isinstance(row, list) and len(row) >= 2:
                        day = str(row[0] or "")[:10]
                        raw_kp = row[1]
                    elif isinstance(row, dict):
                        day = str(row.get("time_tag") or row.get("time") or row.get("date") or "")[:10]
                        raw_kp = row.get("kp") or row.get("Kp") or row.get("planetary_k_index")
                    else:
                        continue
                    try:
                        kp = float(str(raw_kp).strip())
                    except (TypeError, ValueError):
                        continue
                    if day:
                        day_map[day] = max(day_map.get(day, 0.0), kp)
            today = datetime.now(timezone.utc).date()
            days = []
            for offset in range(6, -1, -1):
                day = (today - timedelta(days=offset)).isoformat()
                value = day_map.get(day)
                days.append({"date": day, "kp": value if value is not None else None})
            return ({"days": days}, 200)
        except Exception as exc:
            return ({"error": str(exc), "days": []}, 503)

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
            if path == "/api/register/request":
                if method != "POST":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                if not self._smtp.enabled():
                    await self._write_response(writer, 503, self._json({"error": "registration requests unavailable"}))
                    return
                payload = self._parse_json_body(body)
                call = normalize_call(str(payload.get("call", "")).strip())
                display_name = str(payload.get("name", "")).strip()[:80]
                home_node = normalize_call(str(payload.get("homenode", "")).strip())[:16]
                qth = str(payload.get("qth", "")).strip()[:80]
                qra = extract_locator(str(payload.get("qra", "")).strip().upper())[:16]
                email = str(payload.get("email", "")).strip()
                note = str(payload.get("note", "")).strip()[:160]
                if not is_valid_call(call):
                    await self._write_response(writer, 400, self._json({"error": "invalid callsign"}))
                    return
                if not has_valid_email(email):
                    await self._write_response(writer, 400, self._json({"error": "valid email required"}))
                    return
                reg = await self.store.get_user_registry(call)
                if reg is not None:
                    await self._write_response(writer, 409, self._json({"error": "callsign is already registered"}))
                    return
                challenge_id = str(payload.get("challenge_id", "")).strip()
                otp = str(payload.get("otp", "")).strip()
                if not challenge_id or not otp:
                    try:
                        challenge_id, expires_epoch = await self._mfa.issue(call=call, email=email, purpose="public-register")
                    except Exception:
                        LOG.exception("public registration verification delivery failed call=%s", call)
                        await self._write_response(writer, 503, self._json({"error": "verification delivery failed"}))
                        return
                    await self._write_response(
                        writer,
                        202,
                        self._json({"ok": False, "verification_required": True, "challenge_id": challenge_id, "expires_epoch": expires_epoch}),
                    )
                    return
                ok, reason = await self._mfa.verify(challenge_id=challenge_id, call=call, purpose="public-register", otp=otp)
                if not ok:
                    await self._write_response(writer, 401, self._json({"error": reason}))
                    return
                await self._submit_registration_request(
                    call=call,
                    display_name=display_name,
                    home_node=home_node,
                    qth=qth,
                    qra=qra,
                    email=email,
                    note=note,
                    source="public-web",
                    email_verified=True,
                )
                await self._write_response(writer, 200, self._json({"ok": True, "call": call, "pending": True}))
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
                reg = await self.store.get_user_registry(call)
                if reg is None:
                    self._log_auth_failure(writer, headers, "public-web", call, "registration_required")
                    await self._write_response(writer, 403, self._json({"error": "registration required"}))
                    return
                if not self._has_valid_email(str(reg["email"] or "")):
                    self._log_auth_failure(writer, headers, "public-web", call, "valid_email_required")
                    await self._write_response(writer, 403, self._json({"error": "valid email required"}))
                    return
                expected = await self.store.get_user_pref(call, "password")
                if expected is None or not str(expected).strip():
                    self._log_auth_failure(writer, headers, "public-web", call, "password_setup_required")
                    await self._write_response(writer, 403, self._json({"error": "password setup required"}))
                    return
                if not verify_password(password, str(expected)):
                    self._log_auth_failure(writer, headers, "public-web", call, "invalid_credentials")
                    await self._write_response(writer, 401, self._json({"error": "invalid credentials"}))
                    return
                if not is_password_hash(str(expected)):
                    await self.store.set_user_pref(call, "password", hash_password(password), int(time.time()))
                is_sysop = str(reg["privilege"] or "").strip().lower() in {"sysop", "admin"} if reg is not None else False
                email_verification_required = False
                if self.config.node.verified_email_required_for_web:
                    state, verified_epoch, _remaining = await registration_state(self.store, call)
                    email_verification_required = verified_epoch <= 0 or state != "verified"
                mfa_required = await self._mfa_required_for_call(call, is_sysop=is_sysop)
                if email_verification_required and not mfa_required:
                    self._log_auth_failure(writer, headers, "public-web", call, "email_verification_required")
                    await self._write_response(writer, 403, self._json({"error": "email verification required"}))
                    return
                email_mfa_verified = False
                if mfa_required:
                    challenge_id = str(payload.get("challenge_id", "")).strip()
                    otp = str(payload.get("otp", "")).strip()
                    totp_secret = await self._totp_secret_for_call(call)
                    if totp_secret:
                        if not otp:
                            LOG.info("public web mfa required call=%s method=totp", call)
                            await self._write_response(writer, 202, self._json({"ok": False, "mfa_required": True, "mfa_method": "totp"}))
                            return
                        if not verify_totp(totp_secret, otp):
                            self._log_auth_failure(writer, headers, "public-web", call, "mfa_invalid_totp")
                            await self._write_response(writer, 401, self._json({"error": "invalid code"}))
                            return
                    else:
                        email = await self._email_for_call(call)
                        if not email:
                            self._log_auth_failure(writer, headers, "public-web", call, "mfa_email_missing")
                            await self._write_response(writer, 401, self._json({"error": "mfa email not configured"}))
                            return
                        if not self._smtp.enabled():
                            await self._write_response(writer, 503, self._json({"error": "mfa delivery not configured"}))
                            return
                        if not challenge_id or not otp:
                            try:
                                challenge_id, expires_epoch = await self._mfa.issue(call=call, email=email, purpose="public-web")
                            except Exception:
                                LOG.exception("public web mfa delivery failed call=%s", call)
                                await self._write_response(writer, 503, self._json({"error": "mfa delivery failed"}))
                                return
                            LOG.info("public web mfa required call=%s method=email", call)
                            await self._write_response(
                                writer,
                                202,
                                self._json({"ok": False, "mfa_required": True, "mfa_method": "email", "challenge_id": challenge_id, "expires_epoch": expires_epoch}),
                            )
                            return
                        ok, reason = await self._mfa.verify(challenge_id=challenge_id, call=call, purpose="public-web", otp=otp)
                        if not ok:
                            self._log_auth_failure(writer, headers, "public-web", call, "mfa_" + reason.replace(" ", "_"))
                            await self._write_response(writer, 401, self._json({"error": reason}))
                            return
                        email_mfa_verified = True
                if email_verification_required:
                    if email_mfa_verified:
                        await mark_email_verified(self.store, call, now_epoch=int(time.time()))
                    else:
                        self._log_auth_failure(writer, headers, "public-web", call, "email_verification_required")
                        await self._write_response(writer, 403, self._json({"error": "email verification required"}))
                        return
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
                await self._write_response(writer, 200, self._json(await self._api_spots(q, self._web_call_from_headers(headers) or "")))
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
                email = str(payload.get("email", "")).strip()[:120]
                if email and not has_valid_email(email):
                    await self._write_response(writer, 400, self._json({"error": "valid email required"}))
                    return
                homenode = normalize_call(str(payload.get("homenode", "")).strip())[:16]
                now = int(time.time())
                qra = extract_locator(qra)[:16] if qra else ""
                await self.store.upsert_user_registry(
                    call,
                    now,
                    display_name=name,
                    qth=qth,
                    qra=qra,
                    email=email,
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
            if path == "/api/profile/mfa":
                call = self._web_call_from_headers(headers)
                if not call:
                    await self._write_response(writer, 401, self._json({"error": "web login required"}))
                    return
                base_call = call.split("-", 1)[0].upper()
                now = int(time.time())
                if method == "GET":
                    await self._write_response(writer, 200, self._json({"ok": True, "call": call, "mfa": await self._mfa_snapshot(call)}))
                    return
                if method != "POST":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                payload = self._parse_json_body(body)
                action = str(payload.get("action", "")).strip().lower()
                if action in {"email", "required", "on"}:
                    email = await self._email_for_call(base_call)
                    if not has_valid_email(email):
                        await self._write_response(writer, 400, self._json({"error": "valid email required"}))
                        return
                    await self.store.set_user_pref(base_call, "mfa_email_otp", "required", now)
                    await self.store.delete_user_pref(base_call, "mfa_totp_secret")
                    await self.store.delete_user_pref(base_call, "mfa_totp_pending_secret")
                    await self.store.delete_user_pref(base_call, "mfa_totp_verified_epoch")
                    await self.store.delete_mfa_challenges_for_call(base_call, include_ssids=True)
                    self._audit("user", f"{call} enabled email MFA")
                    await self._write_response(writer, 200, self._json({"ok": True, "call": call, "mfa": await self._mfa_snapshot(call)}))
                    return
                if action in {"default"}:
                    await self.store.delete_user_pref(base_call, "mfa_email_otp")
                    self._audit("user", f"{call} reset email MFA to default")
                    await self._write_response(writer, 200, self._json({"ok": True, "call": call, "mfa": await self._mfa_snapshot(call)}))
                    return
                if action in {"off", "reset", "disable"}:
                    await self.store.set_user_pref(base_call, "mfa_email_otp", "off", now)
                    await self.store.delete_user_pref(base_call, "mfa_totp_secret")
                    await self.store.delete_user_pref(base_call, "mfa_totp_pending_secret")
                    await self.store.delete_user_pref(base_call, "mfa_totp_verified_epoch")
                    cleared = await self.store.delete_mfa_challenges_for_call(base_call, include_ssids=True)
                    self._audit("user", f"{call} disabled MFA challenges={cleared}")
                    await self._write_response(writer, 200, self._json({"ok": True, "call": call, "challenges_cleared": cleared, "mfa": await self._mfa_snapshot(call)}))
                    return
                if action in {"verify", "confirm"}:
                    otp = str(payload.get("otp") or payload.get("code") or "").strip()
                    pending_secret = str(await self.store.get_user_pref(base_call, "mfa_totp_pending_secret") or "").strip()
                    if not pending_secret:
                        email_required = str(await self.store.get_user_pref(base_call, "mfa_email_otp") or "").strip().lower() == "required"
                        if not email_required:
                            await self._write_response(writer, 400, self._json({"error": "authenticator setup not pending"}))
                            return
                        email = await self._email_for_call(base_call)
                        if not has_valid_email(email):
                            await self._write_response(writer, 400, self._json({"error": "valid email required"}))
                            return
                        challenge_id = str(payload.get("challenge_id") or "").strip()
                        if not challenge_id or not otp:
                            try:
                                challenge_id, expires_epoch = await self._mfa.issue(call=base_call, email=email, purpose="public-web-mfa-verify")
                            except Exception:
                                LOG.exception("public web email mfa verification delivery failed call=%s", call)
                                await self._write_response(writer, 503, self._json({"error": "mfa delivery failed"}))
                                return
                            self._audit("user", f"{call} requested email MFA verification")
                            await self._write_response(writer, 200, self._json({"ok": True, "call": call, "email_sent": True, "challenge_id": challenge_id, "expires_epoch": expires_epoch, "mfa": await self._mfa_snapshot(call)}))
                            return
                        ok, reason = await self._mfa.verify(challenge_id=challenge_id, call=base_call, purpose="public-web-mfa-verify", otp=otp)
                        if not ok:
                            await self._write_response(writer, 400, self._json({"error": reason}))
                            return
                        await mark_email_verified(self.store, base_call, now_epoch=now)
                        self._audit("user", f"{call} verified email MFA")
                        await self._write_response(writer, 200, self._json({"ok": True, "call": call, "verified": True, "mfa": await self._mfa_snapshot(call)}))
                        return
                    if not verify_totp(pending_secret, otp):
                        await self._write_response(writer, 400, self._json({"error": "invalid authenticator code"}))
                        return
                    await self.store.set_user_pref(base_call, "mfa_totp_secret", pending_secret, now)
                    await self.store.set_user_pref(base_call, "mfa_totp_verified_epoch", str(now), now)
                    await self.store.delete_user_pref(base_call, "mfa_email_otp")
                    await self.store.delete_user_pref(base_call, "mfa_totp_pending_secret")
                    cleared = await self.store.delete_mfa_challenges_for_call(base_call, include_ssids=True)
                    self._audit("user", f"{call} verified authenticator MFA challenges={cleared}")
                    await self._write_response(writer, 200, self._json({"ok": True, "call": call, "verified": True, "challenges_cleared": cleared, "mfa": await self._mfa_snapshot(call)}))
                    return
                if action in {"totp", "authenticator"}:
                    secret = generate_totp_secret()
                    await self.store.set_user_pref(base_call, "mfa_totp_pending_secret", secret, now)
                    cleared = await self.store.delete_mfa_challenges_for_call(base_call, include_ssids=True)
                    uri = totp_otpauth_uri(
                        issuer=self.config.mfa.issuer.strip() or self.config.node.node_call,
                        account=base_call,
                        secret=secret,
                    )
                    self._audit("user", f"{call} enrolled authenticator MFA challenges={cleared}")
                    try:
                        qr = qr_svg(uri)
                    except ValueError:
                        qr = ""
                    await self._write_response(
                        writer,
                        200,
                        self._json(
                            {
                                "ok": True,
                                "call": call,
                                "otpauth_uri": uri,
                                "qr_svg": qr,
                                "challenges_cleared": cleared,
                                "mfa": await self._mfa_snapshot(call),
                            }
                        ),
                    )
                    return
                await self._write_response(writer, 400, self._json({"error": "invalid mfa action"}))
                return
            if path == "/api/presets":
                call = self._web_call_from_headers(headers)
                if not call:
                    await self._write_response(writer, 401, self._json({"error": "web login required"}))
                    return
                if method == "GET":
                    await self._write_response(writer, 200, self._json({"ok": True, "call": call, **await self._public_presets_snapshot(call)}))
                    return
                if method == "POST":
                    payload = self._parse_json_body(body)
                    presets = await self._save_public_presets(call, payload)
                    self._audit("user", f"{call} updated public web presets")
                    await self._write_response(writer, 200, self._json({"ok": True, "call": call, **presets}))
                    return
                await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                return
            if path == "/api/filters/spots":
                call = self._web_call_from_headers(headers)
                if not call:
                    await self._write_response(writer, 401, self._json({"error": "web login required"}))
                    return
                if method == "GET":
                    rows = [
                        {
                            "family": str(row["family"] or ""),
                            "action": str(row["action"] or ""),
                            "slot": int(row["slot"] or 0),
                            "expr": str(row["expr"] or ""),
                        }
                        for row in await self.store.list_filter_rules(call)
                        if str(row["family"] or "").strip().lower() == "spots"
                    ]
                    await self._write_response(writer, 200, self._json({"ok": True, "call": call, "rules": rows}))
                    return
                if method == "POST":
                    payload = self._parse_json_body(body)
                    action = str(payload.get("action", "accept")).strip().lower()
                    if action not in {"accept", "reject", "clear"}:
                        await self._write_response(writer, 400, self._json({"error": "invalid filter action"}))
                        return
                    try:
                        slot = int(payload.get("slot", 8))
                    except Exception:
                        await self._write_response(writer, 400, self._json({"error": "invalid filter slot"}))
                        return
                    if slot < 0 or slot > 9:
                        await self._write_response(writer, 400, self._json({"error": "filter slot must be 0-9"}))
                        return
                    now = int(time.time())
                    if action == "clear":
                        await self.store.clear_filter_rules(call, "spots", slot)
                    else:
                        expr = str(payload.get("expr", "")).strip()[:160]
                        if not expr:
                            await self._write_response(writer, 400, self._json({"error": "filter expression is required"}))
                            return
                        await self.store.set_filter_rule(call, "spots", action, slot, expr, now)
                    rows = [
                        {
                            "family": str(row["family"] or ""),
                            "action": str(row["action"] or ""),
                            "slot": int(row["slot"] or 0),
                            "expr": str(row["expr"] or ""),
                        }
                        for row in await self.store.list_filter_rules(call)
                        if str(row["family"] or "").strip().lower() == "spots"
                    ]
                    await self._write_response(writer, 200, self._json({"ok": True, "call": call, "rules": rows}))
                    return
                await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                return
            if path == "/api/stats":
                if method != "GET":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                await self._write_response(writer, 200, self._json(await self._api_stats(q)))
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
            if path == "/api/kp":
                if method != "GET":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                body, code = await self._api_kp()
                await self._write_response(writer, code, self._json(body))
                return
            if path == "/api/public/branding":
                if method != "GET":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                await self._write_response(writer, 200, self._json(await self._branding()))
                return
            if path == "/api/public/taxonomy":
                if method != "GET":
                    await self._write_response(writer, 405, self._json({"error": "method not allowed"}))
                    return
                await self._write_response(writer, 200, self._json(self._taxonomy_payload()))
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
            if path == "/api/wcy":
                await self._write_response(writer, 403, self._json({"error": "WCY posting is not available from the public web"}))
                return
            if path in {"/api/announce", "/api/wwv", "/api/wx"}:
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
                if category == "wwv":
                    text = canonicalize_wwv_text(text) or text
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
