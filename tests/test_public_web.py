from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import json
import logging
from pathlib import Path
import re
from types import SimpleNamespace

from pycluster.auth import verify_password
from pycluster.config import AppConfig, NodeConfig, PublicWebConfig, StoreConfig, TelnetConfig, WebConfig
from pycluster.mfa import EmailOtpManager, totp_code
from pycluster import __version__
from pycluster import public_web as public_web_mod
from pycluster.models import Spot
from pycluster.public_web import PublicWebServer
from pycluster.store import SpotStore


def _mk_config(db_path: str, static_dir: str = "") -> AppConfig:
    return AppConfig(
        node=NodeConfig(node_call="AI3I-15", owner_name="John D. Lewis", qth="Western Pennsylvania"),
        telnet=TelnetConfig(host="127.0.0.1", port=0, idle_timeout_seconds=30),
        web=WebConfig(host="127.0.0.1", port=0, admin_token=""),
        public_web=PublicWebConfig(enabled=True, host="127.0.0.1", port=0, static_dir=static_dir),
        store=StoreConfig(sqlite_path=db_path),
    )




def _write_wpxloc(tmp_path: Path) -> str:
    path = tmp_path / "wpxloc.raw"
    path.write_text(
        "UA European-Russia 054 29 16 -3.0 55 45 0 N 37 37 0 E @\n"
        "& =RG65SM\n",
        encoding="ascii",
    )
    return str(path)


def test_public_web_static_uses_backend_kp_endpoint() -> None:
    text = Path("/home/jdlewis/GitHub/pyCluster/web/public_dxweb/static/index.html").read_text(encoding="utf-8")
    assert "const KP     = '/api/kp';" in text
    assert "fetch(KP)" in text
    assert "https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json" not in text


def test_public_web_kp_endpoint_normalizes_seven_day_values(tmp_path, monkeypatch) -> None:
    class _Resp:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self) -> bytes:
            today = datetime.now(timezone.utc).date()
            yesterday = today - timedelta(days=1)
            rows = [
                ["time_tag", "Kp", "a_running", "station_count"],
                [f"{yesterday.isoformat()} 00:00:00.000", "1.00", "1", "8"],
                [f"{yesterday.isoformat()} 03:00:00.000", "3.33", "3", "8"],
                [f"{today.isoformat()} 00:00:00.000", "2.67", "2", "8"],
            ]
            return json.dumps(rows).encode("utf-8")

    async def run() -> None:
        db = str(tmp_path / "public_web_kp.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = PublicWebServer(cfg, store, datetime.now(timezone.utc))
        monkeypatch.setattr(public_web_mod.urllib.request, "urlopen", lambda *_args, **_kwargs: _Resp())
        try:
            code, _, body = await _http_request(srv, "/api/kp")
            assert code == 200
            payload = json.loads(body.decode("utf-8"))
            assert len(payload["days"]) == 7
            assert payload["days"][-1]["kp"] == 2.67
            assert payload["days"][-2]["kp"] == 3.33
        finally:
            await store.close()

    asyncio.run(run())


def test_public_web_solar_endpoint_prefers_recent_wwv_for_core_indices(tmp_path, monkeypatch) -> None:
    class _Resp:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self) -> bytes:
            return (
                b"<solar><solardata>"
                b"<solarflux>101</solarflux><sunspots>11</sunspots><aindex>3</aindex><kindex>1</kindex>"
                b"<xray>B1.2</xray><solarwind>410.2</solarwind><aurora>1</aurora><updated>HamQSL</updated>"
                b"<calculatedconditions><band name=\"30m-20m\" time=\"day\">Good</band></calculatedconditions>"
                b"<calculatedvhfconditions><phenomenon name=\"E-Skip\" location=\"north_america\">Closed</phenomenon></calculatedvhfconditions>"
                b"</solardata></solar>"
            )

    async def run() -> None:
        db = str(tmp_path / "public_web_solar_wwv.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = PublicWebServer(cfg, store, datetime.now(timezone.utc))
        now = int(datetime.now(timezone.utc).timestamp())
        await store.add_bulletin("wwv", "WWV", "LOCAL", now, "SFI=150 A=6 K=2 No Storms")
        monkeypatch.setattr(public_web_mod.urllib.request, "urlopen", lambda *_args, **_kwargs: _Resp())
        try:
            code, _, body = await _http_request(srv, "/api/solar")
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["sfi"] == "150"
            assert data["a"] == "6"
            assert data["k"] == "2"
            assert data["muf3000"] == "26.0"
            assert data["source"] == "wwv:WWV"
            assert data["hamqsl_source"] is True
            assert data["wwv_source"] is True
            assert data["xray"] == "B1.2"
            assert data["conditions"]["30m-20m_day"] == "Good"
            assert data["vhf"][0]["condition"] == "Closed"
        finally:
            await store.close()

    asyncio.run(run())


def test_public_web_solar_endpoint_falls_back_to_wwv_when_hamqsl_fails(tmp_path, monkeypatch) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_web_solar_wwv_fallback.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = PublicWebServer(cfg, store, datetime.now(timezone.utc))
        now = int(datetime.now(timezone.utc).timestamp())
        await store.add_bulletin("wwv", "VE7CC", "LOCAL", now, "SFI=120 A=4 K=1 Quiet")
        monkeypatch.setattr(public_web_mod.urllib.request, "urlopen", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("offline")))
        try:
            code, _, body = await _http_request(srv, "/api/solar")
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["sfi"] == "120"
            assert data["a"] == "4"
            assert data["k"] == "1"
            assert data["source"] == "wwv:VE7CC"
            assert data["hamqsl_source"] is False
            assert data["wwv_source"] is True
        finally:
            await store.close()

    asyncio.run(run())


async def _http_request(
    srv: PublicWebServer,
    target: str,
) -> tuple[int, dict[str, str], bytes]:
    return await _http_request_ex(srv, "GET", target)


async def _http_request_ex(
    srv: PublicWebServer,
    method: str,
    target: str,
    body: bytes = b"",
    headers: dict[str, str] | None = None,
) -> tuple[int, dict[str, str], bytes]:
    class _DummyWriter:
        def __init__(self) -> None:
            self.buf = bytearray()

        def write(self, data: bytes) -> None:
            self.buf.extend(data)

        async def drain(self) -> None:
            return

        def close(self) -> None:
            return

        async def wait_closed(self) -> None:
            return

    reader = asyncio.StreamReader()
    writer = _DummyWriter()
    hdrs = {"Host": "test.local", "Connection": "close"}
    if headers:
        hdrs.update(headers)
    if body:
        hdrs["Content-Length"] = str(len(body))
    header_blob = "".join(f"{k}: {v}\r\n" for k, v in hdrs.items())
    req = f"{method} {target} HTTP/1.1\r\n{header_blob}\r\n".encode("ascii") + body
    reader.feed_data(req)
    reader.feed_eof()
    await srv._handle(reader, writer)  # type: ignore[arg-type]
    raw = bytes(writer.buf)
    head, _, body = raw.partition(b"\r\n\r\n")
    lines = head.decode("ascii", errors="replace").split("\r\n")
    code = int(lines[0].split()[1])
    headers: dict[str, str] = {}
    for ln in lines[1:]:
        if ":" in ln:
            k, v = ln.split(":", 1)
            headers[k.strip().lower()] = v.strip()
    return code, headers, body


def test_public_web_spot_payload_strips_ssid_in_display_only(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_web_spot_display.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = PublicWebServer(cfg, store, datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.add_spot(Spot(21351.0, "CY0S", now, "ssb", "W7XE-11", "WA9PIE-2", ""))
            code, _headers, body = await _http_request(srv, "/api/spots?limit=5")
            assert code == 200
            payload = json.loads(body.decode("utf-8"))
            assert payload[0]["spotter"] == "W7XE"

            code, _headers, body = await _http_request(srv, "/api/leaderboard?hours=24")
            assert code == 200
            board = json.loads(body.decode("utf-8"))
            assert board["spotters"][0]["call"] == "W7XE"
        finally:
            await store.close()

    asyncio.run(run())



def test_public_web_spot_endpoints_and_static_root(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_web.db")
        static_dir = str(tmp_path / "static")
        (tmp_path / "static").mkdir()
        (tmp_path / "static" / "index.html").write_text("<!doctype html><title>dxweb</title>", encoding="utf-8")
        cfg = _mk_config(db, static_dir=static_dir)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        srv = PublicWebServer(cfg, store, datetime.now(timezone.utc))
        try:
            await store.add_spot(Spot(14074.0, "K1ABC", now, "FT8 POTA", "N0CALL", "N2WQ-1", ""))
            await store.add_spot(Spot(7005.0, "W1AW", now - 300, "CW", "AI3I", "AI3I-15", ""))

            code, headers, body = await _http_request(srv, "/")
            assert code == 200
            assert headers["content-type"].startswith("text/html")
            assert b"dxweb" in body

            code, _, body = await _http_request(srv, "/api/spots?limit=10")
            assert code == 200
            rows = json.loads(body.decode("utf-8"))
            assert len(rows) == 2
            assert rows[0]["dx_call"] == "K1ABC"
            assert rows[0]["band"] == "20m"
            assert rows[0]["mode"] == "FT8"
            assert rows[0]["activity"] == "POTA"

            code, _, body = await _http_request(srv, "/api/spots?band=40m")
            assert code == 200
            rows = json.loads(body.decode("utf-8"))
            assert len(rows) == 1
            assert rows[0]["dx_call"] == "W1AW"

            code, _, body = await _http_request(srv, "/api/stats")
            assert code == 200
            stats = json.loads(body.decode("utf-8"))
            assert stats["total"] == 2
            assert any(row["band"] == "20m" for row in stats["bands"])
            assert any(row["mode"] == "FT8" for row in stats["modes"])

            code, _, body = await _http_request(srv, "/api/leaderboard?hours=24")
            assert code == 200
            board = json.loads(body.decode("utf-8"))
            assert board["spotters"][0]["call"] in {"N0CALL", "AI3I"}
            assert any(row["band"] == "20m" for row in board["bands"])
            assert any(row["call"] == "K1ABC" for row in board["dx"])

            code, _, body = await _http_request(srv, "/api/history")
            assert code == 200
            hist = json.loads(body.decode("utf-8"))
            assert hist[0]["date"] == datetime.now(timezone.utc).strftime("%Y-%m-%d")
            assert hist[0]["spots"] == 2
            assert hist[0]["top_band"] in {"20m", "40m"}

            await store.add_bulletin("announce", "AI3I", "FULL", now, "cluster announcement")
            await store.add_bulletin("wcy", "AI3I", "LOCAL", now - 60, "wcy bulletin")

            code, _, body = await _http_request(srv, "/api/bulletins?category=all&limit=10")
            assert code == 200
            rows = json.loads(body.decode("utf-8"))
            assert len(rows) == 2
            assert rows[0]["category"] == "announce"
            assert rows[0]["body"] == "cluster announcement"

            code, _, body = await _http_request(srv, "/api/bulletins?category=wcy&limit=10")
            assert code == 200
            rows = json.loads(body.decode("utf-8"))
            assert len(rows) == 1
            assert rows[0]["category"] == "wcy"
            assert rows[0]["sender"] == "AI3I"
        finally:
            await store.close()

    asyncio.run(run())


def test_public_web_stats_and_history_are_not_capped_by_recent_spot_limit(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_web_stats_uncapped.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        srv = PublicWebServer(cfg, store, datetime.now(timezone.utc))
        try:
            for i in range(250):
                await store.add_spot(
                    Spot(
                        14074.0 + (i % 5),
                        f"K1{i:03d}",
                        now - (i * 60),
                        "FT8",
                        "N0CALL",
                        "AI3I-15",
                        "",
                    )
                )

            code, _, body = await _http_request(srv, "/api/stats?hours=24")
            assert code == 200
            stats = json.loads(body.decode("utf-8"))
            assert stats["total"] == 250
            assert stats["top_band"] == "20m"
            assert stats["top_mode"] == "FT8"

            code, _, body = await _http_request(srv, "/api/leaderboard?hours=24")
            assert code == 200
            board = json.loads(body.decode("utf-8"))
            assert board["spotters"][0]["call"] == "N0CALL"
            assert board["spotters"][0]["count"] == 250

            code, _, body = await _http_request(srv, "/api/history")
            assert code == 200
            hist = json.loads(body.decode("utf-8"))
            assert sum(int(day["spots"]) for day in hist) == 250
        finally:
            await store.close()

    asyncio.run(run())


def test_public_web_nodes_and_network_use_local_state(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_nodes.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        await store.set_user_pref(cfg.node.node_call, "forward_lat", "40.4406", now)
        await store.set_user_pref(cfg.node.node_call, "forward_lon", "-79.9959", now)
        await store.upsert_user_registry("AI3I-16", now, display_name="DXSpider peer")
        await store.set_user_pref("AI3I-16", "node_family", "dxspider", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.ai3i-16.pc18.software", "DXSpider version: 1.57 build: 533", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.ai3i-16.last_epoch", str(now), now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.ai3i-16.last_pc_type", "PC18", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.w3lpl-2.pc18.summary", "pyCluster 1.0.9", now)

        async def _stats():
            return {"W3LPL-2": {"rx_ok": 1}}

        srv = PublicWebServer(cfg, store, datetime.now(timezone.utc), link_stats_fn=_stats)
        try:
            code, _, body = await _http_request(srv, "/api/nodes")
            assert code == 200
            nodes = json.loads(body.decode("utf-8"))
            assert nodes[0]["call"] == "AI3I-15"

            code, _, body = await _http_request(srv, "/api/network")
            assert code == 200
            net = json.loads(body.decode("utf-8"))
            assert net["home"] == "AI3I-15"
            calls = [node["call"] for node in net["nodes"]]
            assert calls.index("AI3I-15") < calls.index("W3LPL-2") < calls.index("AI3I-16")
            assert any(
                node["call"] == "W3LPL-2"
                and node["family"] == "pycluster"
                and node["version"] == "pyCluster 1.0.9"
                for node in net["nodes"]
            )
            assert any(
                node["call"] == "AI3I-16"
                and node["inbound"] is True
                and node["family"] == "dxspider"
                and node["version"] == "DXSpider version: 1.57 build: 533"
                for node in net["nodes"]
            )
        finally:
            await store.close()

    asyncio.run(run())


def test_public_web_branding_uses_node_settings(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_branding.db")
        cfg = _mk_config(db)
        cfg.node.node_alias = "AI3I"
        cfg.node.branding_name = "pyCluster"
        cfg.node.qth = "Western Pennsylvania"
        cfg.node.node_locator = "FN00FS"
        cfg.node.support_contact = "dxcluster@ai3i.net"
        cfg.node.website_url = "https://github.com/AI3I/pyCluster"
        cfg.telnet.ports = (7300, 7373, 8000)
        store = SpotStore(db)
        srv = PublicWebServer(cfg, store, datetime.now(timezone.utc))
        try:
            code, _, body = await _http_request(srv, "/api/public/branding")
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["node_call"] == "AI3I-15"
            assert data["node_alias"] == "AI3I"
            assert data["node_locator"] == "FN00FS"
            assert data["telnet_ports"] == "7300,7373,8000"
            assert data["support_contact"] == "dxcluster@ai3i.net"
            assert data["footer_secondary"].startswith("Western Pennsylvania • FN00FS")
            assert data["software_version"] == f"pyCluster {__version__}"
            assert "Western Pennsylvania" in data["page_title"]
            assert data["ui_strings"]["profile_mfa_email_sent"] == "Email MFA code sent. Check your email, then enter the code."
            assert data["ui_strings"]["register_required_fields"] == "Callsign, email, and password are required."
            assert data["ui_strings"]["register_password_mismatch"] == "Passwords do not match."
        finally:
            await store.close()

    asyncio.run(run())


def test_public_web_detects_ft2_and_park_activity(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_modes.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime(2026, 3, 11, 12, 0, tzinfo=timezone.utc).timestamp())
        srv = PublicWebServer(cfg, store, datetime.now(timezone.utc))
        try:
            await store.add_spot(Spot(14080.0, "K1ABC", now, "FT2 park activation tks", "N0CALL", "N2WQ-1", ""))

            code, _, body = await _http_request(srv, "/api/spots?limit=10")
            assert code == 200
            rows = json.loads(body.decode("utf-8"))
            assert rows[0]["mode"] == "FT2"
            assert rows[0]["activity"] == "POTA"
        finally:
            await store.close()

    asyncio.run(run())


def test_public_dxweb_frequency_formatter_preserves_100hz_resolution() -> None:
    text = Path("/home/jdlewis/GitHub/pyCluster/web/public_dxweb/static/index.html").read_text(encoding="utf-8")
    assert "function fmtFreq(khz)" in text
    assert "Math.floor(khz * 10 + Number.EPSILON) / 10" in text
    assert "return truncated.toFixed(1);" in text


def test_public_dxweb_static_includes_spotter_continent_filter_controls() -> None:
    text = Path("/home/jdlewis/GitHub/pyCluster/web/public_dxweb/static/index.html").read_text(encoding="utf-8")
    assert "Spotter Continent" in text
    assert 'data-ftype="spotterCont"' in text
    assert text.index('<span class="filter-label">CQ Zone</span>') < text.index('<span class="filter-label">Spotter Continent</span>')
    assert 'id="cqz-input" class="cqz-input" type="text"' in text
    assert "ITU Zone" not in text
    assert "ituzone" not in text
    assert "ituFilter" not in text
    assert "dxZones.has(Number(s.dx_cqz || 0))" in text
    assert "const FILTER_SPOTS = '/api/filters/spots';" in text
    assert "spotter_cont " in text
    assert "buildServerSpotFilterExpression" in text
    assert "normalizeBandSelection" in text
    assert "selectedBands().forEach(band => addToWatchlist(band))" in text
    assert "parts.push('on ' + bands.join(','))" in text
    assert "await webJson(API+'?limit=500')" in text


def test_public_dxweb_static_includes_footer_register_modal() -> None:
    text = Path("/home/jdlewis/GitHub/pyCluster/web/public_dxweb/static/index.html").read_text(encoding="utf-8")
    assert 'id="register-modal-bg"' in text
    assert 'id="footer-register"' in text
    assert 'id="register-password"' in text
    assert 'id="register-password-confirm"' in text
    assert "uiText('register_required_fields')" in text
    assert "uiText('register_password_mismatch')" in text
    assert "Callsign, email, and password are required." not in text
    assert "Passwords do not match." not in text
    assert "const REGISTER_REQUEST = '/api/register/request';" in text
    assert "@media (max-width:1100px)" in text
    assert "@media (max-width:760px)" in text
    assert "#content { flex-direction:column; overflow:auto; -webkit-overflow-scrolling:touch; }" in text
    assert "#main { flex:0 0 auto; }" in text
    assert "#sidebar {" in text and "flex:0 0 auto;" in text
    assert "#toast-wrap { left:12px; right:12px; bottom:calc(env(safe-area-inset-bottom, 0px) + 148px);" in text
    assert ".toast { max-width:none; }" in text
    assert ".footer-controls { display:grid; grid-template-columns:repeat(2,minmax(0,1fr));" in text
    assert '<span class="footer-control-label">Popups</span>' in text
    assert '<span class="footer-control-label">Sidebar</span>' in text
    assert "Hide Popups" not in text and "Show Popups" not in text
    assert "Hide Sidebar" not in text and "Show Sidebar" not in text
    assert "btn.className = sidebarHidden ? 'off' : 'on';" in text
    assert "@media (max-width:420px)" in text
    assert ".profile-modal-actions { display:grid; grid-template-columns:1fr; }" in text
    assert ".profile-modal-actions { flex-wrap:wrap; }" in text
    assert "class=\"profile-save-icon\" id=\"profile-save\"" in text
    assert 'id="profile-cancel"' not in text
    assert 'id="profile-email"' in text
    assert "cdn.jsdelivr.net/npm/qrcode" not in text
    assert 'id="profile-mfa-qr"' in text
    assert ">Use TOTP</button>" in text
    assert ">Disable</button>" in text
    assert "uiText('profile_mfa_email_switched'" in text
    assert "uiText('profile_mfa_email_sent'" in text
    assert "MFA method switched to Email. Use Verify to send and validate an email code." not in text
    assert "Email MFA code sent. Check your email, then enter the code." not in text
    assert 'id="profile-mfa-authenticator" type="button" data-action="authenticator">Use TOTP</button>' in text
    assert 'id="profile-mfa-off"' not in text
    assert "method.textContent = usingTotp ? 'Use Email' : 'Use TOTP';" in text
    assert "method.dataset.action = usingTotp ? 'email' : 'authenticator';" in text
    assert "document.getElementById('profile-mfa-email').addEventListener('click', () => updateProfileMfa('off'));" in text
    assert ">Verify</button>" in text
    assert "Enable/Setup MFA" not in text
    assert ">Enable/Setup</button>" not in text
    assert "MFA Settings" in text
    assert 'class="primary" type="button">Enable/Setup MFA' not in text
    assert 'id="profile-mfa-code"' in text
    assert 'id="profile-mfa-test"' in text
    assert "async function verifyProfileMfa()" in text
    assert "updateProfileMfa('verify', {challenge_id: sent.challenge_id, otp: String(otp).trim()})" in text
    assert 'id="profile-mfa-default"' not in text
    assert "window.QRCode.toCanvas" not in text
    assert "body.qr_svg" in text
    assert "Authenticator setup key:" not in text
    assert "Capabilities</div>" in text
    assert "Greyed-out actions are disabled by local node policy" not in text
    assert "['RBN', rbnAllowed]" in text
    assert "Capabilities: ${allowed.join(', ')}" not in text
    assert "Posting tools ready." in text
    assert "Logged in as ${webCall}${allowed.length" not in text
    assert ".operate-cap.on" in text and "rgba(34,197,94,.12)" in text
    assert ".operate-cap {" in text and "rgba(248,81,73,.10)" in text
    assert "#footer-edit-profile" in text and "rgba(34,197,94,.12)" in text
    assert "#footer-logout" in text and "rgba(248,81,73,.10)" in text
    assert "html.light .watch-type option" in text
    assert 'let nodeTableSortCol = \'cluster\';' in text
    assert '<th data-sort="cluster" class="sortable">Family <span class="si"></span></th>' in text
    assert "const familyOrder = n =>" in text
    assert "fam === 'pycluster'" in text
    assert "const PROFILE_PRESETS = '/api/presets';" in text
    assert "async function loadAccountPresets()" in text
    assert "const authOptional = !!opts.authOptional;" in text
    assert "res.status === 401 && webToken && !authOptional" in text
    assert "webJson(PROFILE_PRESETS, {authOptional:true})" in text
    assert 'id="login-otp-row"' in text
    assert "pendingWebLogin" in text
    assert "body && body.mfa_required" in text
    assert "uiText('login_mfa_authenticator')" in text
    assert "uiText('login_mfa_email')" in text
    assert "uiText('login_mfa_enter_code')" in text
    assert "Enter the code from your authenticator app." not in text
    assert "Enter the code sent to your email." not in text
    assert "Email Code" not in text
    assert "Authenticator Code" not in text
    assert "watch_profiles: watchProfiles" in text
    assert "filter_presets: filterPresets" in text
    assert "watch_rules: watchlist" in text
    assert "watch_matches: watchMatches.slice(0,10)" in text
    assert "const serverWatchRules = Array.isArray(data.watch_rules)" in text
    assert "const serverWatchMatches = Array.isArray(data.watch_matches)" in text
    assert "if (webToken && webCall) persistAccountPresets();" in text
    assert "opSetWarn(uiText('presets_login_required'))" in text
    assert "uiText('presets_save_failed')" in text
    assert "uiText('presets_load_failed')" in text
    assert 'id="spotter-cqz-input"' in text
    assert "function parseZoneSpec(text, low, high)" in text
    assert "spotterZones.has(Number(s.spotter_cqz || 0))" in text
    assert "spotterCqzFilter = preset.spotterCqzFilter || ''" in text
    assert '<button class="preset-del">✕</button>' in text
    assert "deleteFilterPreset(p.name)" in text
    assert '<button class="stats-rng-btn" data-hrs="3">3h</button>' in text
    assert '<button class="stats-rng-btn" data-hrs="18">18h</button>' in text
    assert '<button class="stats-rng-btn" data-hrs="48">48h</button>' not in text
    assert '<button class="stats-rng-btn" data-hrs="72">72h</button>' not in text
    assert '<button class="sb-time" data-hrs="3">3h</button>' in text
    assert '<button class="sb-time" data-hrs="18">18h</button>' in text
    assert "<h3>Band Conditions</h3>" in text
    assert 'id="prop-muf"' in text
    assert 'id="prop-source"' in text
    assert "d.wwv_source ? (d.hamqsl_source ? 'WWV + HamQSL' : 'WWV') : 'hamqsl.com'" in text
    assert "HF Day" in text
    assert "HF Night" in text
    assert "VHF Band Conditions" not in text
    assert "const lbl = `${statsHrs}h`;" in text
    assert "const legacyPeerList = legacyNodes" in text
    assert "${esc(n.call || '—')}" in text
    assert "NA:'North America'" in text
    assert "SA:'South America'" in text
    assert 'class="hbar-chart continent-bars" id="st-cont-chart"' in text
    assert ".hbar-chart.continent-bars .hbar-lbl" in text
    assert "flex:0 0 124px" in text
    assert "Sporadic E · North America" in text
    assert "Sporadic E · South America" in text
    assert "Tropospheric · North America" in text
    assert "Meteor Scatter · Northern Hemisphere" in text
    assert "Meteor Scatter · Southern Hemisphere" in text
    assert "North Hem." not in text
    assert "South Hem." not in text
    assert "E-Skip ·" not in text
    assert "Tropo ·" not in text
    assert "N. America" not in text
    assert "S. America" not in text
    assert "'Crete':'GR'" in text
    assert "'Montserrat':'MS'" in text
    assert "'Guantanamo Bay':'US'" in text
    assert "'Reunion Island':'RE'" in text
    assert "'Swains Island':'AS'" in text
    assert "'Peter 1 Island':'AQ'" in text


def test_public_dxweb_auth_locked_sidebar_tabs_stay_visible() -> None:
    text = Path("/home/jdlewis/GitHub/pyCluster/web/public_dxweb/static/index.html").read_text(encoding="utf-8")

    watch = re.search(r'<button[^>]+id="tab-watch"[^>]*>', text)
    operate = re.search(r'<button[^>]+id="tab-operate"[^>]*>', text)
    assert watch and operate
    assert "hidden" not in watch.group(0)
    assert "hidden" not in operate.group(0)
    assert "auth-locked" in watch.group(0)
    assert "auth-locked" in operate.group(0)
    assert "disabled" in watch.group(0)
    assert "disabled" in operate.group(0)
    assert 'aria-disabled="true"' in watch.group(0)
    assert 'aria-disabled="true"' in operate.group(0)
    assert "tab.disabled = !loggedIn;" in text
    assert "tab.classList.toggle('auth-locked', !loggedIn);" in text
    assert "operateTab.disabled" in text
    assert "watchTab.disabled" in text


def test_public_dxweb_operate_panel_does_not_offer_wcy_posting() -> None:
    text = Path("/home/jdlewis/GitHub/pyCluster/web/public_dxweb/static/index.html").read_text(encoding="utf-8")
    assert 'id="post-wcy-btn"' not in text
    assert "const POST_WCY" not in text
    assert "submitPublicAction('wcy')" not in text
    assert "wcy: POST_WCY" not in text
    assert "Enter chat, announce, WX, WCY, or WWV text here" not in text
    assert "Use this box for chat, announce, WX, WCY, and WWV posts." not in text


def test_public_web_login_failure_logs_structured_authfail(tmp_path, caplog) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_authfail.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        srv = PublicWebServer(cfg, store, datetime.now(timezone.utc))
        try:
            await store.upsert_user_registry("AI3I", now, privilege="user", email="ai3i@example.test")
            await store.set_user_pref("AI3I", "password", "correct", now)
            await store.set_user_pref("AI3I", "email_verified_epoch", str(now), now)
            with caplog.at_level(logging.WARNING, logger="pycluster.public_web"):
                code, _, body = await _http_request_ex(
                    srv,
                    "POST",
                    "/api/auth/login",
                    headers={
                        "Content-Type": "application/json",
                        "X-Forwarded-For": "203.0.113.77",
                    },
                    body=json.dumps({"call": "AI3I", "password": "wrong"}).encode("utf-8"),
                )
            assert code == 401
            assert json.loads(body.decode("utf-8"))["error"] == "invalid credentials"
            assert "AUTHFAIL channel=public-web ip=203.0.113.77 call=AI3I reason=invalid_credentials" in caplog.text
        finally:
            await store.close()

    asyncio.run(run())


def test_public_web_login_can_require_email_otp(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_web_mfa.db")
        cfg = _mk_config(db)
        cfg.smtp.host = "smtp.example.test"
        cfg.smtp.from_addr = "cluster@example.test"
        cfg.mfa.enabled = True
        cfg.mfa.require_for_users = True
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        sent: list[tuple[str, str, str]] = []
        srv = PublicWebServer(cfg, store, datetime.now(timezone.utc))
        srv._mfa._sender = lambda rcpt, subject, body: sent.append((rcpt, subject, body))  # type: ignore[assignment]
        try:
            await store.upsert_user_registry("AI3I", now, privilege="user", email="ai3i@example.test")
            await store.set_user_pref("AI3I", "password", "secret", now)
            await store.set_user_pref("AI3I", "email_verified_epoch", str(now), now)

            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/auth/login",
                json.dumps({"call": "AI3I", "password": "secret"}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
            assert code == 202
            payload = json.loads(body.decode("utf-8"))
            assert payload["mfa_required"] is True
            assert sent and sent[0][0] == "ai3i@example.test"
            challenge = next(iter(srv._mfa._challenges.values()))

            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/auth/login",
                json.dumps(
                    {
                        "call": "AI3I",
                        "password": "secret",
                        "challenge_id": payload["challenge_id"],
                        "otp": challenge.code,
                    }
                ).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["ok"] is True
            assert data["token"]
        finally:
            await store.close()

    asyncio.run(run())


def test_public_web_login_can_use_totp_authenticator(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_web_totp.db")
        cfg = _mk_config(db)
        cfg.mfa.enabled = True
        cfg.mfa.require_for_users = True
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        srv = PublicWebServer(cfg, store, datetime.now(timezone.utc))
        try:
            await store.upsert_user_registry("AI3I", now, privilege="user", email="ai3i@example.test")
            await store.set_user_pref("AI3I", "password", "secret", now)
            await store.set_user_pref("AI3I", "mfa_totp_secret", "JBSWY3DPEHPK3PXP", now)
            await store.set_user_pref("AI3I", "mfa_email_otp", "required", now)
            await store.set_user_pref("AI3I", "email_verified_epoch", str(now), now)

            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/auth/login",
                json.dumps({"call": "AI3I", "password": "secret"}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
            assert code == 202
            payload = json.loads(body.decode("utf-8"))
            assert payload["mfa_required"] is True
            assert payload["mfa_method"] == "totp"
            assert "challenge_id" not in payload

            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/auth/login",
                json.dumps({"call": "AI3I", "password": "secret", "otp": totp_code("JBSWY3DPEHPK3PXP")}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["ok"] is True
            assert data["token"]
        finally:
            await store.close()

    asyncio.run(run())


def test_public_web_login_honors_per_user_mfa_override(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_web_mfa_override.db")
        cfg = _mk_config(db)
        cfg.smtp.host = "smtp.example.test"
        cfg.smtp.from_addr = "cluster@example.test"
        cfg.mfa.enabled = True
        cfg.mfa.require_for_users = False
        cfg.node.verified_email_required_for_web = True
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        sent: list[tuple[str, str, str]] = []
        srv = PublicWebServer(cfg, store, datetime.now(timezone.utc))
        srv._mfa._sender = lambda rcpt, subject, body: sent.append((rcpt, subject, body))  # type: ignore[assignment]
        try:
            await store.upsert_user_registry("AI3I", now, privilege="user", email="ai3i@example.test")
            await store.set_user_pref("AI3I", "password", "secret", now)
            await store.set_user_pref("AI3I", "mfa_email_otp", "required", now)

            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/auth/login",
                json.dumps({"call": "AI3I", "password": "secret"}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
            assert code == 202
            payload = json.loads(body.decode("utf-8"))
            assert payload["mfa_required"] is True
            assert payload["mfa_method"] == "email"
            assert sent and sent[0][0] == "ai3i@example.test"
            challenge = next(iter(srv._mfa._challenges.values()))

            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/auth/login",
                json.dumps(
                    {
                        "call": "AI3I",
                        "password": "secret",
                        "challenge_id": payload["challenge_id"],
                        "otp": challenge.code,
                    }
                ).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
            assert code == 200
            assert await store.get_user_pref("AI3I", "email_verified_epoch") is not None

            await store.set_user_pref("AI3I", "mfa_email_otp", "off", now)
            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/auth/login",
                json.dumps({"call": "AI3I", "password": "secret"}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["ok"] is True
        finally:
            await store.close()

    asyncio.run(run())


def test_email_otp_manager_enforces_resend_cooldown() -> None:
    async def run() -> None:
        sent: list[tuple[str, str, str]] = []
        store = SpotStore("/tmp/unused.db")
        cfg = _mk_config("/tmp/unused.db").mfa
        cfg.enabled = True
        cfg.resend_cooldown_seconds = 60
        mgr = EmailOtpManager(cfg, lambda rcpt, subject, body: sent.append((rcpt, subject, body)), store)
        try:
            await mgr.issue(call="AI3I", email="ai3i@example.test", purpose="public-web")
            try:
                await mgr.issue(call="AI3I", email="ai3i@example.test", purpose="public-web")
                assert False, "expected resend cooldown to block repeated issue"
            except RuntimeError as exc:
                assert str(exc) == "otp recently issued"
        finally:
            await store.close()

    asyncio.run(run())


def test_email_otp_manager_rolls_back_failed_delivery(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "mfa_failed_delivery.db")
        store = SpotStore(db)
        cfg = _mk_config(db).mfa
        cfg.enabled = True
        cfg.resend_cooldown_seconds = 60
        mgr = EmailOtpManager(cfg, lambda _rcpt, _subject, _body: (_ for _ in ()).throw(RuntimeError("smtp down")), store)
        try:
            try:
                await mgr.issue(call="AI3I", email="ai3i@example.test", purpose="public-web")
                assert False, "expected delivery failure"
            except RuntimeError as exc:
                assert str(exc) == "smtp down"
            assert mgr._challenges == {}
            assert mgr._recent_issue == {}
            assert await store.get_mfa_challenge("missing") is None

            sent: list[tuple[str, str, str]] = []
            mgr._sender = lambda rcpt, subject, body: sent.append((rcpt, subject, body))  # type: ignore[assignment]
            challenge_id, _expires = await mgr.issue(call="AI3I", email="ai3i@example.test", purpose="public-web")
            assert sent
            assert await store.get_mfa_challenge(challenge_id) is not None
        finally:
            await store.close()

    asyncio.run(run())


def test_public_web_mfa_challenge_survives_server_restart(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_web_mfa_restart.db")
        cfg = _mk_config(db)
        cfg.smtp.host = "smtp.example.test"
        cfg.smtp.from_addr = "cluster@example.test"
        cfg.mfa.enabled = True
        cfg.mfa.require_for_users = True
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        sent: list[tuple[str, str, str]] = []
        srv1 = PublicWebServer(cfg, store, datetime.now(timezone.utc))
        srv1._mfa._sender = lambda rcpt, subject, body: sent.append((rcpt, subject, body))  # type: ignore[assignment]
        try:
            await store.upsert_user_registry("AI3I", now, privilege="user", email="ai3i@example.test")
            await store.set_user_pref("AI3I", "password", "secret", now)
            await store.set_user_pref("AI3I", "email_verified_epoch", str(now), now)

            code, _, body = await _http_request_ex(
                srv1,
                "POST",
                "/api/auth/login",
                json.dumps({"call": "AI3I", "password": "secret"}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
            assert code == 202
            payload = json.loads(body.decode("utf-8"))
            assert sent
            match = re.search(r"\b(\d{6,8})\b", sent[0][2])
            assert match is not None
            otp = match.group(1)

            srv2 = PublicWebServer(cfg, store, datetime.now(timezone.utc))
            code, _, body = await _http_request_ex(
                srv2,
                "POST",
                "/api/auth/login",
                json.dumps(
                    {
                        "call": "AI3I",
                        "password": "secret",
                        "challenge_id": payload["challenge_id"],
                        "otp": otp,
                    }
                ).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["ok"] is True
        finally:
            await store.close()

    asyncio.run(run())


def test_public_web_detects_additional_modes(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_extra_modes.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime(2026, 3, 11, 12, 0, tzinfo=timezone.utc).timestamp())
        srv = PublicWebServer(cfg, store, datetime.now(timezone.utc))
        try:
            await store.add_spot(Spot(50000.0, "K1ABC", now, "Q65 test", "N0CALL", "N2WQ-1", ""))
            await store.add_spot(Spot(144174.0, "K1DEF", now - 60, "MSK144 cq", "N0CALL", "N2WQ-1", ""))
            await store.add_spot(Spot(14072.0, "K1GHI", now - 120, "OLIVIA park", "N0CALL", "N2WQ-1", ""))
            await store.add_spot(Spot(7105.0, "K1JKL", now - 180, "VARA gateway", "N0CALL", "N2WQ-1", ""))

            code, _, body = await _http_request(srv, "/api/spots?limit=10")
            assert code == 200
            rows = json.loads(body.decode("utf-8"))
            modes = {row["dx_call"]: row["mode"] for row in rows}
            assert modes["K1ABC"] == "Q65"
            assert modes["K1DEF"] == "MSK144"
            assert modes["K1GHI"] == "OLIVIA"
            assert modes["K1JKL"] == "VARA"
            activities = {row["dx_call"]: row["activity"] for row in rows}
            assert activities["K1GHI"] == "POTA"
        finally:
            await store.close()

    asyncio.run(run())


def test_public_web_taxonomy_comes_from_strings_catalog(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_taxonomy.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        strings_path = tmp_path / "strings.toml"
        strings_path.write_text(
            """
[public_web.taxonomy]
mode_order = ["TRX"]
rare_entities = ["Castle Island"]

[[public_web.taxonomy.mode_rules]]
pattern = '\\bTRX\\b'
value = "TRX"
button = "TRX"

[[public_web.taxonomy.activity_rules]]
pattern = '\\bCASTLE\\b'
value = "CASTLE"
button = "CASTLE"

[[public_web.taxonomy.comment_tags]]
pattern = '\\bCASTLE\\b'
label = "CASTLE"
color = "#123456"
""".strip()
            + "\n",
            encoding="utf-8",
        )
        now = int(datetime(2026, 3, 11, 12, 0, tzinfo=timezone.utc).timestamp())
        srv = PublicWebServer(cfg, store, datetime.now(timezone.utc), strings_path=str(strings_path))
        try:
            await store.add_spot(Spot(14074.0, "K1ABC", now, "TRX CASTLE", "N0CALL", "N2WQ-1", ""))

            code, _, body = await _http_request(srv, "/api/spots?limit=5")
            assert code == 200
            rows = json.loads(body.decode("utf-8"))
            assert rows[0]["mode"] == "TRX"
            assert rows[0]["activity"] == "CASTLE"

            code, _, body = await _http_request(srv, "/api/public/taxonomy")
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["mode_filters"] == ["TRX"]
            assert data["activity_filters"] == ["RARE", "CASTLE"]
            assert data["comment_tags"][0]["label"] == "CASTLE"
            assert data["rare_entities"] == ["Castle Island"]
        finally:
            await store.close()

    asyncio.run(run())


def test_bundled_strings_catalog_keeps_expected_comment_labels(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_taxonomy_bundled.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        strings_path = Path(__file__).resolve().parents[1] / "config" / "strings.toml"
        srv = PublicWebServer(cfg, store, datetime.now(timezone.utc), strings_path=str(strings_path))
        try:
            code, _, body = await _http_request(srv, "/api/public/taxonomy")
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            labels = {row["label"] for row in data["comment_tags"]}
            assert {"CQ", "DIGITAL", "VOICE", "QRM", "QRT", "PILEUP", "LoTW", "RBN", "TNX"} <= labels
        finally:
            await store.close()

    asyncio.run(run())


def test_public_static_keeps_button_icons_but_not_taxonomy_emoji_labels() -> None:
    text = Path("/home/jdlewis/GitHub/pyCluster/web/public_dxweb/static/index.html").read_text(encoding="utf-8")

    assert 'id="greyline-toggle"' in text and "🌗" in text
    assert 'id="audio-icon"' in text
    assert 'id="theme-icon"' in text
    assert "⬇ CSV" in text and "⬇ ADIF" in text
    assert 'id="spot-rbn-btn" type="button" title="Show RBN/Skimmer spots">RBN</button>' in text
    assert "commentTagFilter = commentTagFilter === 'RBN' ? 'ALL' : 'RBN';" in text
    assert 'id="sm-close" type="button" title="Close" aria-label="Close spot details">✕</button>' in text
    assert 'id="profile-close" type="button" title="Close" aria-label="Close profile">✕</button>' in text
    assert 'id="login-close" type="button" title="Close" aria-label="Close login">✕</button>' in text
    assert 'id="register-close" type="button" title="Close" aria-label="Close registration">✕</button>' in text

    comment_tags = text.split("let COMMENT_TAGS = [", 1)[1].split("];", 1)[0]
    assert not re.search(r"label:'[^']*[\U0001F000-\U0001FAFF\u2600-\u27BF]", comment_tags)


def test_public_web_stop_closes_tracked_ws_clients(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_ws_stop.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = PublicWebServer(cfg, store, datetime.now(timezone.utc))

        class _DummyWriter:
            def __init__(self) -> None:
                self.closed = False

            def close(self) -> None:
                self.closed = True

        async def _never() -> None:
            await asyncio.sleep(60)

        writer = _DummyWriter()
        task = asyncio.create_task(_never())
        srv._ws_writers.add(writer)  # type: ignore[arg-type]
        srv._ws_clients.add(task)
        try:
            await srv.stop()
            assert writer.closed is True
            assert task.cancelled() is True
            assert not srv._ws_clients
            assert not srv._ws_writers
        finally:
            await store.close()

    asyncio.run(run())


def test_public_web_auth_and_posting(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_auth.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry("AI3I", now, privilege="user", email="ai3i@example.test")
        await store.set_user_pref("AI3I", "password", "secret", now)
        await store.set_user_pref("AI3I", "email_verified_epoch", str(now), now)
        srv = PublicWebServer(cfg, store, datetime.now(timezone.utc))
        try:
            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/auth/login",
                json.dumps({"call": "AI3I", "password": "secret"}).encode("utf-8"),
                {"Content-Type": "application/json"},
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            token = data["token"]
            assert data["access"]["login"] is True
            assert data["access"]["spots"] is True

            code, _, body = await _http_request_ex(
                srv,
                "GET",
                "/api/auth/me",
                headers={"X-Web-Token": token},
            )
            assert code == 200
            me = json.loads(body.decode("utf-8"))
            assert me["call"] == "AI3I"
            assert me["access"]["chat"] is True
            assert me["profile"]["email"] == "ai3i@example.test"
            assert me["profile"]["mfa"]["enabled"] is False

            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/profile",
                json.dumps({"name": "John", "qth": "Western Pennsylvania", "qra": "FN00FS", "email": "new@example.test"}).encode("utf-8"),
                {"Content-Type": "application/json", "X-Web-Token": token},
            )
            assert code == 200
            profile = json.loads(body.decode("utf-8"))["profile"]
            assert profile["email"] == "new@example.test"
            assert profile["qth"] == "Western Pennsylvania"
            assert profile["qra"] == "FN00FS"
            row = await store.get_user_registry("AI3I")
            assert row is not None
            assert row["email"] == "new@example.test"
            assert row["qth"] == "Western Pennsylvania"
            assert row["qra"] == "FN00FS"

            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/profile",
                json.dumps({"email": "not-an-email"}).encode("utf-8"),
                {"Content-Type": "application/json", "X-Web-Token": token},
            )
            assert code == 400
            assert json.loads(body.decode("utf-8"))["error"] == "valid email required"

            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/spot",
                json.dumps({"freq_khz": 14074.0, "dx_call": "N0TST", "info": "WEB TEST"}).encode("utf-8"),
                {"Content-Type": "application/json", "X-Web-Token": token},
            )
            assert code == 200
            assert json.loads(body.decode("utf-8"))["ok"] is True

            rows = await store.latest_spots(limit=1)
            assert rows[0]["dx_call"] == "N0TST"

            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/chat",
                json.dumps({"text": "hello from web"}).encode("utf-8"),
                {"Content-Type": "application/json", "X-Web-Token": token},
            )
            assert code == 200
            assert json.loads(body.decode("utf-8"))["category"] == "chat"

            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/announce",
                json.dumps({"text": "web announce", "scope": "FULL"}).encode("utf-8"),
                {"Content-Type": "application/json", "X-Web-Token": token},
            )
            assert code == 200
            assert json.loads(body.decode("utf-8"))["category"] == "announce"

            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/wcy",
                json.dumps({"text": "A=8 K=2"}).encode("utf-8"),
                {"Content-Type": "application/json", "X-Web-Token": token},
            )
            assert code == 403
            assert "WCY posting is not available from the public web" in body.decode("utf-8")
            assert await store.list_bulletins("wcy", limit=1) == []
        finally:
            await store.close()

    asyncio.run(run())


def test_public_web_user_presets_are_stored_per_call(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_user_presets.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry("AI3I", now, privilege="user", email="ai3i@example.test")
        await store.set_user_pref("AI3I", "password", "secret", now)
        await store.set_user_pref("AI3I", "email_verified_epoch", str(now), now)
        srv = PublicWebServer(cfg, store, datetime.now(timezone.utc))
        try:
            code, _, _ = await _http_request_ex(srv, "GET", "/api/presets")
            assert code == 401

            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/auth/login",
                json.dumps({"call": "AI3I", "password": "secret"}).encode("utf-8"),
                {"Content-Type": "application/json"},
            )
            assert code == 200
            token = json.loads(body.decode("utf-8"))["token"]

            payload = {
                "watch_profiles": [
                    {"name": "DX Watch", "rules": [{"type": "call", "value": "CY0S", "sound": True, "toast": True}]}
                ],
                "filter_presets": [
                    {"name": "Digital NA", "band": "20m", "mode": "FT8", "commentTagFilter": "ALL"}
                ],
                "watch_rules": [
                    {"type": "call", "value": "cy0s", "hits": 2, "last": "2026-05-19T00:00:00Z", "sound": True, "toast": False}
                ],
                "watch_matches": [
                    {"dx_call": "cy0s", "rule_type": "call", "rule_value": "cy0s", "band": "20m", "mode": "CW", "time": "2026-05-19T00:00:00Z", "spotter": "k1abc"}
                ],
            }
            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/presets",
                json.dumps(payload).encode("utf-8"),
                {"Content-Type": "application/json", "X-Web-Token": token},
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["watch_profiles"][0]["name"] == "DX Watch"
            assert data["filter_presets"][0]["name"] == "Digital NA"
            assert data["watch_rules"][0]["value"] == "CY0S"
            assert data["watch_matches"][0]["spotter"] == "K1ABC"

            raw = await store.get_user_pref("AI3I", "public.presets")
            assert raw is not None
            saved = json.loads(raw)
            assert saved["watch_profiles"][0]["rules"][0]["value"] == "CY0S"
            assert saved["watch_rules"][0]["value"] == "CY0S"
            assert saved["watch_matches"][0]["dx_call"] == "CY0S"

            code, _, body = await _http_request_ex(
                srv,
                "GET",
                "/api/presets",
                headers={"X-Web-Token": token},
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["filter_presets"][0]["mode"] == "FT8"
            assert data["watch_rules"][0]["hits"] == 2
            assert data["watch_matches"][0]["rule_value"] == "CY0S"
        finally:
            await store.close()

    asyncio.run(run())


def test_public_web_spot_filters_are_persisted_and_applied_to_logged_in_spots(tmp_path, monkeypatch) -> None:
    def _lookup(call: str):
        if call.upper().startswith("EU"):
            return SimpleNamespace(name="Germany", continent="EU", cq_zone=14, itu_zone=28, lat=51.0, lon=10.0)
        return SimpleNamespace(name="United States", continent="NA", cq_zone=5, itu_zone=8, lat=40.0, lon=-75.0)

    async def run() -> None:
        db = str(tmp_path / "public_web_persisted_filters.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry("AI3I", now, privilege="user", email="ai3i@example.test")
        await store.set_user_pref("AI3I", "password", "secret", now)
        await store.set_user_pref("AI3I", "email_verified_epoch", str(now), now)
        await store.add_spot(Spot(14074.0, "K1ABC", now, "FT8", "EU1SPT", "AI3I-15", ""))
        await store.add_spot(Spot(7074.0, "K1ABE", now - 2, "FT8", "EU2SPT", "AI3I-15", ""))
        await store.add_spot(Spot(14075.0, "K1ABD", now - 1, "FT8", "N0SPT", "AI3I-15", ""))
        srv = PublicWebServer(cfg, store, datetime.now(timezone.utc))
        srv._cty_loaded = True
        monkeypatch.setattr(public_web_mod, "lookup", _lookup)
        try:
            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/auth/login",
                json.dumps({"call": "AI3I", "password": "secret"}).encode("utf-8"),
                {"Content-Type": "application/json"},
            )
            assert code == 200
            token = json.loads(body.decode("utf-8"))["token"]

            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/filters/spots",
                json.dumps({"action": "accept", "slot": 8, "expr": "spotter_cont EU"}).encode("utf-8"),
                {"Content-Type": "application/json", "X-Web-Token": token},
            )
            assert code == 200
            assert await store.list_filter_rules("AI3I")
            await store.set_filter_rule("AI3I", "spots", "accept", 8, "spotter_cont EU and on 20m,40m", now)

            code, _, body = await _http_request_ex(
                srv,
                "GET",
                "/api/spots?limit=10",
                headers={"X-Web-Token": token},
            )
            assert code == 200
            rows = json.loads(body.decode("utf-8"))
            assert [row["spotter"] for row in rows] == ["EU1SPT", "EU2SPT"]

            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/filters/spots",
                json.dumps({"action": "clear", "slot": 8}).encode("utf-8"),
                {"Content-Type": "application/json", "X-Web-Token": token},
            )
            assert code == 200
            assert not await store.list_filter_rules("AI3I")
        finally:
            await store.close()

    asyncio.run(run())


def test_public_web_spot_filter_expressions_accept_zone_ranges(tmp_path, monkeypatch) -> None:
    def _lookup(call: str):
        if call.upper().startswith("W6"):
            return SimpleNamespace(name="United States", continent="NA", cq_zone=3, itu_zone=6, lat=34.0, lon=-118.0)
        return SimpleNamespace(name="United States", continent="NA", cq_zone=5, itu_zone=8, lat=40.0, lon=-75.0)

    db = str(tmp_path / "public_web_zone_ranges.db")
    cfg = _mk_config(db)
    store = SpotStore(db)
    srv = PublicWebServer(cfg, store, datetime.now(timezone.utc))
    srv._cty_loaded = True
    monkeypatch.setattr(public_web_mod, "lookup", _lookup)
    try:
        spot = {
            "freq": 14074.0,
            "band": "20m",
            "dx_call": "K1ABC",
            "spotter": "W6SPT",
            "comment": "FT8",
            "dx_cqz": 5,
            "dx_ituz": 8,
            "spotter_continent": "NA",
        }
        assert srv._spot_payload_matches_expr(spot, "spotter_zone 3-5")
        assert srv._spot_payload_matches_expr(spot, "call_zone 3-5")
        assert srv._spot_payload_matches_expr(spot, "on 20m,40m")
        assert not srv._spot_payload_matches_expr(spot, "spotter_zone 6-8")
        assert not srv._spot_payload_matches_expr(spot, "on 40m,80m")
    finally:
        asyncio.run(store.close())


def test_public_web_spots_hide_rbn_for_anonymous_and_honor_rbn_access(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_web_rbn_access.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry("AI3I", now, privilege="user", email="ai3i@example.test")
        await store.set_user_pref("AI3I", "password", "secret", now)
        await store.set_user_pref("AI3I", "email_verified_epoch", str(now), now)
        await store.set_user_pref("AI3I", "access.web.rbn", "off", now)
        await store.add_spot(Spot(14074.0, "K1ABC", now, "CQ TEST 18 dB", "SKIMMER1", "AI3I-15", ""))
        await store.add_spot(Spot(14075.0, "K1XYZ", now - 1, "FT8", "W1AW", "AI3I-15", ""))
        srv = PublicWebServer(cfg, store, datetime.now(timezone.utc))
        try:
            code, _, body = await _http_request(srv, "/api/spots?limit=10")
            assert code == 200
            rows = json.loads(body.decode("utf-8"))
            assert [row["dx_call"] for row in rows] == ["K1XYZ"]

            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/auth/login",
                json.dumps({"call": "AI3I", "password": "secret"}).encode("utf-8"),
                {"Content-Type": "application/json"},
            )
            assert code == 200
            token = json.loads(body.decode("utf-8"))["token"]

            code, _, body = await _http_request_ex(
                srv,
                "GET",
                "/api/spots?limit=10",
                headers={"X-Web-Token": token},
            )
            assert code == 200
            rows = json.loads(body.decode("utf-8"))
            assert [row["dx_call"] for row in rows] == ["K1XYZ"]

            await store.set_user_pref("AI3I", "access.web.rbn", "on", now)
            code, _, body = await _http_request_ex(
                srv,
                "GET",
                "/api/spots?limit=10",
                headers={"X-Web-Token": token},
            )
            assert code == 200
            rows = json.loads(body.decode("utf-8"))
            assert [row["dx_call"] for row in rows] == ["K1ABC", "K1XYZ"]
            assert rows[0]["is_rbn"] is True
        finally:
            await store.close()

    asyncio.run(run())


def test_public_web_user_can_manage_own_mfa(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_user_mfa_self_service.db")
        cfg = _mk_config(db)
        cfg.smtp.host = "smtp.example.test"
        cfg.smtp.from_addr = "cluster@example.test"
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry("AI3I", now, privilege="user", email="ai3i@example.test")
        await store.set_user_pref("AI3I", "password", "secret", now)
        await store.set_user_pref("AI3I", "email_verified_epoch", str(now), now)
        sent: list[tuple[str, str, str]] = []
        srv = PublicWebServer(cfg, store, datetime.now(timezone.utc))
        srv._mfa._sender = lambda rcpt, subject, body: sent.append((rcpt, subject, body))  # type: ignore[assignment]
        try:
            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/auth/login",
                json.dumps({"call": "AI3I", "password": "secret"}).encode("utf-8"),
                {"Content-Type": "application/json"},
            )
            assert code == 200
            token = json.loads(body.decode("utf-8"))["token"]

            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/profile/mfa",
                json.dumps({"action": "email"}).encode("utf-8"),
                {"Content-Type": "application/json", "X-Web-Token": token},
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["mfa"]["email_otp"] == "required"
            assert data["mfa"]["enabled"] is True
            assert await store.get_user_pref("AI3I", "mfa_email_otp") == "required"

            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/profile/mfa",
                json.dumps({"action": "verify"}).encode("utf-8"),
                {"Content-Type": "application/json", "X-Web-Token": token},
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["email_sent"] is True
            assert data["challenge_id"]
            assert sent and sent[-1][0] == "ai3i@example.test"
            challenge = await store.get_mfa_challenge(data["challenge_id"])
            assert challenge is not None

            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/profile/mfa",
                json.dumps({"action": "verify", "challenge_id": data["challenge_id"], "otp": str(challenge["code"])}).encode("utf-8"),
                {"Content-Type": "application/json", "X-Web-Token": token},
            )
            assert code == 200
            assert json.loads(body.decode("utf-8"))["verified"] is True

            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/profile/mfa",
                json.dumps({"action": "authenticator"}).encode("utf-8"),
                {"Content-Type": "application/json", "X-Web-Token": token},
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["otpauth_uri"].startswith("otpauth://totp/")
            assert data["qr_svg"].startswith("<svg ")
            assert data["mfa"]["totp_enabled"] is False
            pending_secret = await store.get_user_pref("AI3I", "mfa_totp_pending_secret")
            assert pending_secret
            assert await store.get_user_pref("AI3I", "mfa_totp_secret") is None

            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/profile/mfa",
                json.dumps({"action": "verify", "otp": "000000"}).encode("utf-8"),
                {"Content-Type": "application/json", "X-Web-Token": token},
            )
            assert code == 400
            assert json.loads(body.decode("utf-8"))["error"] == "invalid authenticator code"

            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/profile/mfa",
                json.dumps({"action": "verify", "otp": totp_code(pending_secret)}).encode("utf-8"),
                {"Content-Type": "application/json", "X-Web-Token": token},
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["verified"] is True
            assert data["mfa"]["totp_enabled"] is True
            assert await store.get_user_pref("AI3I", "mfa_totp_secret") == pending_secret
            assert await store.get_user_pref("AI3I", "mfa_totp_pending_secret") is None
            assert await store.get_user_pref("AI3I", "mfa_email_otp") is None

            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/profile/mfa",
                json.dumps({"action": "off"}).encode("utf-8"),
                {"Content-Type": "application/json", "X-Web-Token": token},
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["mfa"]["enabled"] is False
            assert await store.get_user_pref("AI3I", "mfa_email_otp") == "off"
            assert await store.get_user_pref("AI3I", "mfa_totp_secret") is None
        finally:
            await store.close()

    asyncio.run(run())


def test_public_web_profile_exposes_watch_seed_from_buddies_and_spot_filters(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_watch_seed.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry("AI3I", now, privilege="user", email="ai3i@example.test")
        await store.set_user_pref("AI3I", "password", "secret", now)
        await store.set_user_pref("AI3I", "email_verified_epoch", str(now), now)
        await store.add_buddy("AI3I", "K1ABC", now)
        await store.set_filter_rule("AI3I", "spots", "accept", 1, "on 20m", now)
        await store.set_filter_rule("AI3I", "spots", "accept", 2, "by W3LPL", now)
        await store.set_filter_rule("AI3I", "spots", "accept", 3, "call_zone 5", now)
        await store.set_filter_rule("AI3I", "spots", "accept", 4, "call_dxcc canada", now)
        await store.set_filter_rule("AI3I", "spots", "reject", 5, "by N0CALL", now)
        srv = PublicWebServer(cfg, store, datetime.now(timezone.utc))
        try:
            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/auth/login",
                json.dumps({"call": "AI3I", "password": "secret"}).encode("utf-8"),
                {"Content-Type": "application/json"},
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            seed = data["profile"]["watch_seed"]
            assert {"type": "call", "value": "K1ABC", "source": "buddy"} in seed
            assert {"type": "band", "value": "20M", "source": "accept/spots 1"} in seed
            assert {"type": "spotter", "value": "W3LPL", "source": "accept/spots 2"} in seed
            assert {"type": "cqzone", "value": "5", "source": "accept/spots 3"} in seed
            assert {"type": "entity", "value": "CANADA", "source": "accept/spots 4"} in seed
            assert not any(item["value"] == "N0CALL" for item in seed)

            token = data["token"]
            code, _, body = await _http_request_ex(
                srv,
                "GET",
                "/api/auth/me",
                headers={"X-Web-Token": token},
            )
            assert code == 200
            me = json.loads(body.decode("utf-8"))
            assert me["profile"]["watch_seed"] == seed
        finally:
            await store.close()

    asyncio.run(run())


def test_public_web_access_policy_controls_login_and_posting(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_access.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry("AI3I", now, privilege="user", email="ai3i@example.test")
        await store.set_user_pref("AI3I", "password", "secret", now)
        await store.set_user_pref("AI3I", "email_verified_epoch", str(now), now)
        await store.set_user_pref("AI3I", "access.web.login", "off", now)
        srv = PublicWebServer(cfg, store, datetime.now(timezone.utc))
        try:
            code, _, _ = await _http_request_ex(
                srv,
                "POST",
                "/api/auth/login",
                json.dumps({"call": "AI3I", "password": "secret"}).encode("utf-8"),
                {"Content-Type": "application/json"},
            )
            assert code == 403

            await store.set_user_pref("AI3I", "access.web.login", "on", now)
            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/auth/login",
                json.dumps({"call": "AI3I", "password": "secret"}).encode("utf-8"),
                {"Content-Type": "application/json"},
            )
            assert code == 200
            token = json.loads(body.decode("utf-8"))["token"]
            row = await store.get_user_registry("AI3I")
            assert row is not None
            assert str(row["last_login_peer"]).startswith("public-web")
            assert "public-web" in str(row["last_login_peer"])

            await store.set_user_pref("AI3I", "access.web.spots", "off", now)
            code, _, _ = await _http_request_ex(
                srv,
                "POST",
                "/api/spot",
                json.dumps({"freq_khz": 14074.0, "dx_call": "N0TST", "info": "blocked"}).encode("utf-8"),
                {"Content-Type": "application/json", "X-Web-Token": token},
            )
            assert code == 403
        finally:
            await store.close()

    asyncio.run(run())


def test_public_web_login_requires_registration_and_valid_email(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_auth_registration_required.db")
        cfg = _mk_config(db)
        cfg.node.verified_email_required_for_web = True
        cfg.smtp.host = "smtp.example.test"
        cfg.smtp.from_addr = "cluster@example.test"
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        sent: list[tuple[str, str, str]] = []
        srv = PublicWebServer(cfg, store, datetime.now(timezone.utc))
        srv._mfa._sender = lambda rcpt, subject, body: sent.append((rcpt, subject, body))  # type: ignore[assignment]
        try:
            await store.set_user_pref("AI3I", "password", "secret", now)
            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/auth/login",
                json.dumps({"call": "AI3I", "password": "secret"}).encode("utf-8"),
                {"Content-Type": "application/json"},
            )
            assert code == 403
            assert json.loads(body.decode("utf-8"))["error"] == "registration required"

            await store.upsert_user_registry("AI3I", now, privilege="user", email="")
            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/auth/login",
                json.dumps({"call": "AI3I", "password": "secret"}).encode("utf-8"),
                {"Content-Type": "application/json"},
            )
            assert code == 403
            assert json.loads(body.decode("utf-8"))["error"] == "valid email required"

            await store.upsert_user_registry("AI3I", now, privilege="user", email="ai3i@example.test")
            await store.delete_user_pref("AI3I", "password")
            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/auth/login",
                json.dumps({"call": "AI3I", "password": "secret"}).encode("utf-8"),
                {"Content-Type": "application/json"},
            )
            assert code == 403
            assert json.loads(body.decode("utf-8"))["error"] == "password setup required"

            await store.set_user_pref("AI3I", "password", "secret", now)
            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/auth/login",
                json.dumps({"call": "AI3I", "password": "secret"}).encode("utf-8"),
                {"Content-Type": "application/json"},
            )
            assert code == 202
            verify = json.loads(body.decode("utf-8"))
            assert verify["mfa_required"] is True
            assert verify["mfa_method"] == "email"
            assert sent and sent[0][0] == "ai3i@example.test"
            challenge = next(iter(srv._mfa._challenges.values()))

            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/auth/login",
                json.dumps(
                    {
                        "call": "AI3I",
                        "password": "secret",
                        "challenge_id": verify["challenge_id"],
                        "otp": challenge.code,
                    }
                ).encode("utf-8"),
                {"Content-Type": "application/json"},
            )
            assert code == 200
            assert json.loads(body.decode("utf-8"))["ok"] is True
            assert await store.get_user_pref("AI3I", "email_verified_epoch") is not None

            await store.set_user_pref("AI3I", "email_verified_epoch", str(now), now)
            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/auth/login",
                json.dumps({"call": "AI3I", "password": "secret"}).encode("utf-8"),
                {"Content-Type": "application/json"},
            )
            assert code == 200
            assert json.loads(body.decode("utf-8"))["ok"] is True
        finally:
            await store.close()

    asyncio.run(run())


def test_public_web_registration_request_verifies_email_and_queues_pending_request(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_registration_request.db")
        cfg = _mk_config(db)
        cfg.smtp.host = "smtp.example.test"
        cfg.smtp.from_addr = "cluster@example.test"
        store = SpotStore(db)
        sent: list[tuple[str, str, str]] = []
        srv = PublicWebServer(cfg, store, datetime.now(timezone.utc))
        srv._mfa._sender = lambda rcpt, subject, body: sent.append((rcpt, subject, body))  # type: ignore[assignment]
        srv._smtp.send_code = lambda rcpt, subject, body: sent.append((rcpt, subject, body))  # type: ignore[assignment]
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.upsert_user_registry("AI3I", now, privilege="sysop", email="sysop@example.test")
            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/register/request",
                json.dumps(
                    {
                        "call": "N1NEW",
                        "name": "New User",
                        "homenode": "W1AW",
                        "qth": "Hartford",
                        "qra": "FN31",
                        "email": "new@example.test",
                        "password": "secret-pass",
                        "password_confirm": "secret-pass",
                        "note": "Please approve me",
                    }
                ).encode("utf-8"),
                {"Content-Type": "application/json"},
            )
            assert code == 202
            data = json.loads(body.decode("utf-8"))
            challenge_id = data["challenge_id"]
            row = await store.get_mfa_challenge(challenge_id)
            assert row is not None
            otp = str(row["code"])

            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/register/request",
                json.dumps(
                    {
                        "call": "N1NEW",
                        "name": "New User",
                        "homenode": "W1AW",
                        "qth": "Hartford",
                        "qra": "FN31",
                        "email": "new@example.test",
                        "password": "secret-pass",
                        "password_confirm": "secret-pass",
                        "note": "Please approve me",
                        "challenge_id": challenge_id,
                        "otp": otp,
                    }
                ).encode("utf-8"),
                {"Content-Type": "application/json"},
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["ok"] is True
            req = await store.get_registration_request("N1NEW")
            assert req is not None
            assert str(req["status"]) == "pending"
            assert str(req["email"]) == "new@example.test"
            assert int(req["email_verified"]) == 1
            user = await store.get_user_registry("N1NEW")
            assert user is not None
            assert str(user["privilege"]) == ""
            assert str(user["email"]) == "new@example.test"
            assert await store.get_user_pref("N1NEW", "registration_state") == "verified"
            assert await store.get_user_pref("N1NEW", "email_verified_epoch") is not None
            stored_password = await store.get_user_pref("N1NEW", "password")
            assert stored_password is not None
            assert verify_password("secret-pass", str(stored_password))
            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/auth/login",
                json.dumps({"call": "N1NEW", "password": "secret-pass"}).encode("utf-8"),
                {"Content-Type": "application/json"},
            )
            assert code == 403
            assert json.loads(body.decode("utf-8"))["error"] == "registration pending"
            await store.set_registration_request_status("N1NEW", status="approved", epoch=now, reviewer="AI3I")
            await store.upsert_user_registry("N1NEW", now, privilege="user", email="new@example.test")
            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/auth/login",
                json.dumps({"call": "N1NEW", "password": "secret-pass"}).encode("utf-8"),
                {"Content-Type": "application/json"},
            )
            assert code == 200
            assert json.loads(body.decode("utf-8"))["ok"] is True
            assert any(rcpt == "sysop@example.test" for rcpt, _subject, _body in sent)
            assert any(rcpt == "new@example.test" for rcpt, _subject, _body in sent)
        finally:
            await store.close()

    asyncio.run(run())


def test_public_web_registration_rejects_password_mismatch(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_registration_password_mismatch.db")
        cfg = _mk_config(db)
        cfg.smtp.host = "smtp.example.test"
        cfg.smtp.from_addr = "cluster@example.test"
        store = SpotStore(db)
        srv = PublicWebServer(cfg, store, datetime.now(timezone.utc))
        try:
            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/register/request",
                json.dumps(
                    {
                        "call": "N1NEW",
                        "name": "New User",
                        "email": "new@example.test",
                        "password": "secret-pass",
                        "password_confirm": "different-pass",
                    }
                ).encode("utf-8"),
                {"Content-Type": "application/json"},
            )
            assert code == 400
            assert json.loads(body.decode("utf-8"))["error"] == "passwords do not match"
            assert await store.get_user_registry("N1NEW") is None
            assert await store.get_registration_request("N1NEW") is None
        finally:
            await store.close()

    asyncio.run(run())


def test_public_web_registration_rejects_invalid_callsign(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_registration_invalid_call.db")
        cfg = _mk_config(db)
        cfg.smtp.host = "smtp.example.test"
        cfg.smtp.from_addr = "cluster@example.test"
        store = SpotStore(db)
        srv = PublicWebServer(cfg, store, datetime.now(timezone.utc))
        try:
            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/register/request",
                json.dumps(
                    {
                        "call": "JOHN",
                        "name": "John",
                        "email": "john@example.test",
                    }
                ).encode("utf-8"),
                {"Content-Type": "application/json"},
            )
            assert code == 400
            assert json.loads(body.decode("utf-8"))["error"] == "invalid callsign"
            assert await store.get_user_registry("JOHN") is None
            assert await store.get_registration_request("JOHN") is None
        finally:
            await store.close()

    asyncio.run(run())


def test_public_web_registration_expired_code_reports_retry_message(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_registration_expired_code.db")
        cfg = _mk_config(db)
        cfg.smtp.host = "smtp.example.test"
        cfg.smtp.from_addr = "cluster@example.test"
        store = SpotStore(db)
        srv = PublicWebServer(cfg, store, datetime.now(timezone.utc))
        srv._mfa._sender = lambda _rcpt, _subject, _body: None  # type: ignore[assignment]
        try:
            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/register/request",
                json.dumps(
                    {
                        "call": "N1NEW",
                        "name": "New User",
                        "email": "new@example.test",
                        "password": "secret-pass",
                        "password_confirm": "secret-pass",
                    }
                ).encode("utf-8"),
                {"Content-Type": "application/json"},
            )
            assert code == 202
            data = json.loads(body.decode("utf-8"))
            challenge_id = str(data["challenge_id"])
            row = await store.get_mfa_challenge(challenge_id)
            assert row is not None
            now = int(datetime.now(timezone.utc).timestamp())
            await store.save_mfa_challenge(
                challenge_id=challenge_id,
                call="N1NEW",
                purpose="public-register",
                code=str(row["code"]),
                expires_epoch=now - 60,
                attempts_left=5,
                issued_epoch=now - 600,
            )
            srv._mfa._challenges.pop(challenge_id, None)

            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/register/request",
                json.dumps(
                    {
                        "call": "N1NEW",
                        "name": "New User",
                        "email": "new@example.test",
                        "password": "secret-pass",
                        "password_confirm": "secret-pass",
                        "challenge_id": challenge_id,
                        "otp": str(row["code"]),
                    }
                ).encode("utf-8"),
                {"Content-Type": "application/json"},
            )
            assert code == 401
            assert json.loads(body.decode("utf-8"))["error"] == "verification code expired; request a new code"
            assert await store.get_registration_request("N1NEW") is None
        finally:
            await store.close()

    asyncio.run(run())


def test_public_web_non_authenticated_users_are_read_only_by_default(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_access_default.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry("AI3I", now, privilege="", email="ai3i@example.test")
        await store.set_user_pref("AI3I", "password", "secret", now)
        await store.set_user_pref("AI3I", "email_verified_epoch", str(now), now)
        srv = PublicWebServer(cfg, store, datetime.now(timezone.utc))
        try:
            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/auth/login",
                json.dumps({"call": "AI3I", "password": "secret"}).encode("utf-8"),
                {"Content-Type": "application/json"},
            )
            assert code == 200
            token = json.loads(body.decode("utf-8"))["token"]

            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/spot",
                json.dumps({"freq_khz": 14074.0, "dx_call": "N0TST", "info": "blocked"}).encode("utf-8"),
                {"Content-Type": "application/json", "X-Web-Token": token},
            )
            assert code == 403
            assert json.loads(body.decode("utf-8"))["error"] == "spot posting not allowed via web"
        finally:
            await store.close()

    asyncio.run(run())


def test_public_web_explicit_ssid_user_does_not_inherit_base_call_access(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_ssid_access_inheritance.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry("AI3I", now, privilege="user", email="ai3i@example.test")
        await store.upsert_user_registry("AI3I-1", now, privilege="", email="ai3i-1@example.test")
        await store.set_user_pref("AI3I", "access.web.spots", "on", now)
        srv = PublicWebServer(cfg, store, datetime.now(timezone.utc))
        try:
            assert await srv._access_allowed("AI3I", "web", "spots") is True
            assert await srv._access_allowed("AI3I-1", "web", "spots") is False
        finally:
            await store.close()

    asyncio.run(run())


def test_public_web_cluster_peer_access_is_always_allowed(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_cluster_access.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry("AI3I-15", now, privilege="")
        await store.set_user_pref("AI3I-15", "node_family", "pycluster", now)
        await store.set_user_pref("AI3I-15", "blocked_login", "on", now)
        await store.set_user_pref("AI3I-15", "access.web.spots", "off", now)
        srv = PublicWebServer(cfg, store, datetime.now(timezone.utc))
        try:
            assert await srv._access_allowed("AI3I-15", "web", "login") is True
            assert await srv._access_allowed("AI3I-15", "web", "spots") is True
        finally:
            await store.close()

    asyncio.run(run())


def test_public_web_blocked_login_is_denied(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_blocked_login.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry("AI3I", now, privilege="user", email="ai3i@example.test")
        await store.set_user_pref("AI3I", "password", "secret", now)
        await store.set_user_pref("AI3I", "email_verified_epoch", str(now), now)
        await store.set_user_pref("AI3I", "blocked_login", "on", now)
        srv = PublicWebServer(cfg, store, datetime.now(timezone.utc))
        try:
            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/auth/login",
                json.dumps({"call": "AI3I", "password": "secret"}).encode("utf-8"),
                {"Content-Type": "application/json"},
            )
            assert code == 403
            assert json.loads(body.decode("utf-8"))["error"] == "login blocked"
        finally:
            await store.close()

    asyncio.run(run())


def test_public_web_spot_throttle_returns_429(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_spot_throttle.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry("AI3I", now, privilege="user", email="ai3i@example.test")
        await store.set_user_pref("AI3I", "password", "secret", now)
        await store.set_user_pref("AI3I", "email_verified_epoch", str(now), now)
        await store.set_user_pref(cfg.node.node_call, "spot_throttle.max_per_window", "1", now)
        await store.set_user_pref(cfg.node.node_call, "spot_throttle.window_seconds", "300", now)
        srv = PublicWebServer(cfg, store, datetime.now(timezone.utc))
        try:
            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/auth/login",
                json.dumps({"call": "AI3I", "password": "secret"}).encode("utf-8"),
                {"Content-Type": "application/json"},
            )
            assert code == 200
            token = json.loads(body.decode("utf-8"))["token"]

            code, _, _ = await _http_request_ex(
                srv,
                "POST",
                "/api/spot",
                json.dumps({"freq_khz": 14074.0, "dx_call": "N0TST", "info": "one"}).encode("utf-8"),
                {"Content-Type": "application/json", "X-Web-Token": token},
            )
            assert code == 200

            code, _, body = await _http_request_ex(
                srv,
                "POST",
                "/api/spot",
                json.dumps({"freq_khz": 14075.0, "dx_call": "N0TSU", "info": "two"}).encode("utf-8"),
                {"Content-Type": "application/json", "X-Web-Token": token},
            )
            assert code == 429
            resp = json.loads(body.decode("utf-8"))
            assert resp["error"] == "spot rate limit exceeded"
            assert resp["limit"]["max_per_window"] == 1
            assert resp["limit"]["window_seconds"] == 300
        finally:
            await store.close()

    asyncio.run(run())


def test_public_web_spot_payload_falls_back_to_wpxloc(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_web_wpx.db")
        cfg = _mk_config(db)
        cfg.public_web.wpxloc_raw_path = _write_wpxloc(tmp_path)
        store = SpotStore(db)
        srv = PublicWebServer(cfg, store, datetime.now(timezone.utc))
        try:
            from pycluster.wpxloc import load_wpxloc
            load_wpxloc(cfg.public_web.wpxloc_raw_path)
            srv._wpx_loaded = True
            now = int(datetime.now(timezone.utc).timestamp())
            await store.add_spot(Spot(7168.0, "RG65SM", now, "CQ", "F8DRA", "PEER2", ""))
            code, _, body = await _http_request(srv, "/api/spots?limit=10")
            assert code == 200
            rows = json.loads(body.decode("utf-8"))
            assert rows[0]["dx_entity"] == "European Russia"
            assert rows[0]["dx_cqz"] == 29
            assert rows[0]["dx_ituz"] == 16
        finally:
            await srv.stop()
            await store.close()

    asyncio.run(run())
