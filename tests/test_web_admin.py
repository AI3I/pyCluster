from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import tomllib

from pycluster import __version__
import pycluster.web_admin as web_admin_mod
from pycluster.ctydat import load_cty
from pycluster.auth import is_password_hash, verify_password
from pycluster.config import AppConfig, NodeConfig, PublicWebConfig, StoreConfig, TelnetConfig, WebConfig
from pycluster.mfa import SMTPMailer, totp_code
from pycluster.models import Spot
from pycluster.store import SpotStore
from pycluster.web_admin import WebAdminServer


def _mk_config(db_path: str, admin_token: str = "") -> AppConfig:
    return AppConfig(
        node=NodeConfig(),
        telnet=TelnetConfig(host="127.0.0.1", port=0, idle_timeout_seconds=30),
        web=WebConfig(host="0.0.0.0", port=0, admin_token=admin_token),
        public_web=PublicWebConfig(),
        store=StoreConfig(sqlite_path=db_path),
    )


async def _http_request(
    srv: WebAdminServer,
    method: str,
    target: str,
    headers: dict[str, str] | None = None,
    body: bytes | None = None,
) -> tuple[int, dict[str, str], bytes]:
    class _DummyWriter:
        def __init__(self) -> None:
            self.buf = bytearray()
            self._closed = False

        def write(self, data: bytes) -> None:
            self.buf.extend(data)

        async def drain(self) -> None:
            return

        def get_extra_info(self, _name: str, _default=None):
            return None

        def close(self) -> None:
            self._closed = True

        async def wait_closed(self) -> None:
            return

    reader = asyncio.StreamReader()
    writer = _DummyWriter()
    h = {"Host": "test.local", "Connection": "close"}
    if headers:
        h.update(headers)
    if "X-Admin-Token" in h:
        h.pop("X-Admin-Token", None)
        tok, _exp = srv._issue_web_token("SYSOP", is_sysop=True)
        h["X-Web-Token"] = tok
    b = body or b""
    if method.upper() == "POST":
        h["Content-Length"] = str(len(b))
    req = [f"{method} {target} HTTP/1.1\r\n"]
    req.extend(f"{k}: {v}\r\n" for k, v in h.items())
    req.append("\r\n")
    reader.feed_data("".join(req).encode("ascii") + b)
    reader.feed_eof()
    await srv._handle(reader, writer)  # type: ignore[arg-type]

    raw = bytes(writer.buf)
    head, _, out = raw.partition(b"\r\n\r\n")
    lines = head.decode("ascii", errors="replace").split("\r\n")
    status_line = lines[0].strip()
    code = int(status_line.split()[1])
    rh: dict[str, str] = {}
    for ln in lines[1:]:
        txt = ln.strip()
        if not txt:
            continue
        if ":" in txt:
            k, v = txt.split(":", 1)
            rh[k.strip().lower()] = v.strip()
    return code, rh, out


def test_web_admin_static_includes_mobile_breakpoints() -> None:
    text = Path("/home/jdlewis/GitHub/pyCluster/src/pycluster/web_admin.py").read_text(encoding="utf-8")
    assert "@media (max-width: 900px)" in text
    assert "@media (max-width: 560px)" in text
    assert ".actions button{flex:1 1 160px}" in text
    assert ".tablewrap table{min-width:720px}" in text
    assert ".node-tabs,.subtabs,.users-browser-tabs{" in text
    assert ".browser-toolbar .browser-search," in text


def test_web_admin_static_groups_users_and_telemetry_into_subtabs() -> None:
    text = Path("/home/jdlewis/GitHub/pyCluster/src/pycluster/web_admin.py").read_text(encoding="utf-8")
    assert 'data-user-browser="local"' in text
    assert 'data-user-browser="blocked"' in text
    assert 'data-user-browser="clusters"' in text
    assert 'data-user-browser="sysops"' in text
    assert 'data-user-browser="requests"' in text
    assert 'id="user-browser-local"' in text
    assert 'id="user-browser-blocked"' in text
    assert 'id="user-browser-clusters"' in text
    assert 'id="user-browser-sysops"' in text
    assert 'id="user-browser-requests"' in text
    assert 'data-telemetry-panel="overview"' in text
    assert 'data-telemetry-panel="audit"' in text
    assert 'data-telemetry-panel="security"' in text
    assert 'id="telemetry-panel-overview"' in text
    assert 'id="telemetry-panel-audit"' in text
    assert 'id="telemetry-panel-security"' in text
    assert '<h3>Recent Authentication Failures</h3>' in text
    assert "function setUserBrowserPanel(panel)" in text
    assert "function setTelemetryPanel(panel)" in text
    assert "if (key === 'sysop-web') return 'Operator Console';" in text
    assert "if (key === 'registration_request_required') return 'Registration request required';" in text


def test_web_admin_static_uses_clearer_statusline_and_maintenance_actions() -> None:
    text = Path("/home/jdlewis/GitHub/pyCluster/src/pycluster/web_admin.py").read_text(encoding="utf-8")
    assert "background:transparent;" in text
    assert "border:0;" in text
    assert "color:var(--text-secondary);" in text
    assert ".light .statusline{" in text
    assert ".statusline.error{" in text
    maintenance_idx = text.index('id="node-group-maintenance"')
    cleanup_idx = text.index('id="runCleanup"')
    check_idx = text.index('id="checkUpgrade"')
    upgrade_idx = text.index('id="runUpgrade"')
    assert maintenance_idx < cleanup_idx
    assert maintenance_idx < check_idx
    assert maintenance_idx < upgrade_idx


def test_web_admin_static_uses_full_width_user_action_bar() -> None:
    text = Path("/home/jdlewis/GitHub/pyCluster/src/pycluster/web_admin.py").read_text(encoding="utf-8")
    assert ".users-actionbar{" in text
    assert 'class="users-actionbar"' in text
    assert 'class="users-action-group"' in text
    assert "display:flex;" in text
    assert "flex-wrap:wrap;" in text


def test_web_admin_static_peer_table_uses_content_width_columns() -> None:
    text = Path("/home/jdlewis/GitHub/pyCluster/src/pycluster/web_admin.py").read_text(encoding="utf-8")
    assert ".peer-table{" in text
    assert "width:max-content;" in text
    assert "table-layout:auto;" in text
    assert 'class="peer-table"' in text
    assert 'id="peerDelete"' in text
    assert "byId('peer').value = '';" in text
    assert "j('/api/peer/delete'" in text


def test_web_admin_static_exposes_qrz_settings() -> None:
    text = Path("/home/jdlewis/GitHub/pyCluster/src/pycluster/web_admin.py").read_text(encoding="utf-8")
    assert 'data-node-group="qrz"' in text
    assert 'id="node-group-qrz"' in text
    assert 'id="qrz_username"' in text
    assert 'id="qrz_password"' in text
    assert 'id="qrz_agent"' in text
    assert 'id="qrz_api_url"' in text
    assert "qrz_username: byId('qrz_username').value.trim()" in text


def test_web_admin_static_exposes_satellite_settings() -> None:
    text = Path("/home/jdlewis/GitHub/pyCluster/src/pycluster/web_admin.py").read_text(encoding="utf-8")
    assert 'data-node-group="satellite"' in text
    assert 'id="node-group-satellite"' in text
    assert 'id="satellite_keps_path"' in text
    assert 'id="satellite_prediction_hours"' in text
    assert 'id="satellite_pass_step_seconds"' in text
    assert 'id="satellite_min_elevation_deg"' in text
    assert "satellite_keps_path: byId('satellite_keps_path').value.trim()" in text


def test_web_admin_static_exposes_totp_mfa_controls() -> None:
    text = Path("/home/jdlewis/GitHub/pyCluster/src/pycluster/web_admin.py").read_text(encoding="utf-8")
    assert 'id="enrollTotp"' in text
    assert "/api/users/mfa/totp/enroll" in text
    assert "Authenticator setup URI" in text
    assert "mfa_method" in text


def test_web_admin_static_shows_registration_state_controls() -> None:
    text = Path("/home/jdlewis/GitHub/pyCluster/src/pycluster/web_admin.py").read_text(encoding="utf-8")
    assert 'id="user_verified_state" type="checkbox" disabled' in text
    assert 'id="user_unlocked_state" type="checkbox" disabled' in text
    assert '<label for="user_unlocked_state">Locked</label>' in text
    assert 'id="markVerified">Verify Now<' not in text
    assert 'id="unlockRegistration">Unlock Now<' not in text
    assert '<h3>Access Matrix</h3>' in text
    assert text.index('<h3>Access Matrix</h3>') < text.index('id="user_verified_state"')
    assert text.index('<h3>Access Matrix</h3>') < text.index('id="user_mfa_email_otp"')
    assert text.index('<h3>Access Matrix</h3>') < text.index('id="user_privilege"')
    assert 'id="mfa_require_for_sysop" type="checkbox" checked' not in text
    assert "function setRegistrationActionState(verified, locked, enabled)" in text
    assert "verifiedState.checked = !!verified" in text
    assert "unlockedState.checked = !!locked" in text


def test_web_admin_static_includes_location_detail_field() -> None:
    text = Path("/home/jdlewis/GitHub/pyCluster/src/pycluster/web_admin.py").read_text(encoding="utf-8")
    assert 'id="user_location"' in text
    assert "Location Detail" in text
    assert "location: byId('user_location').value.trim()" in text


def test_public_web_static_offsets_toasts_clear_of_sidebar() -> None:
    text = Path("/home/jdlewis/GitHub/pyCluster/web/public_dxweb/static/index.html").read_text(encoding="utf-8")
    assert "--sidebar-width: 320px;" in text
    assert "--sidebar-toast-offset: calc(var(--sidebar-width) + 28px);" in text
    assert "right:var(--sidebar-toast-offset);" in text
    assert "bottom:100px;" in text
    assert "#toast-wrap { right:16px; bottom:100px; }" in text


def test_public_web_static_keeps_login_actions_out_of_header() -> None:
    text = Path("/home/jdlewis/GitHub/pyCluster/web/public_dxweb/static/index.html").read_text(encoding="utf-8")
    assert 'id="header-auth-btn"' not in text
    assert "function updateHeaderAuthButton()" not in text
    assert 'id="footer-login"' in text


def test_public_web_static_uses_pill_footer_auth_buttons() -> None:
    text = Path("/home/jdlewis/GitHub/pyCluster/web/public_dxweb/static/index.html").read_text(encoding="utf-8")
    assert ".footer-user-login {" in text
    assert ".footer-user-register {" in text
    assert "border-radius:999px;" in text
    assert "color:#86efac;" in text
    assert "color:#fecaca;" in text
    assert "(must be approved locally)" not in text
    assert "footer-user-actions" in text


def test_public_web_static_supports_sidebar_hide_toggle() -> None:
    text = Path("/home/jdlewis/GitHub/pyCluster/web/public_dxweb/static/index.html").read_text(encoding="utf-8")
    assert 'id="sidebar-toggle"' in text
    assert 'id="toast-toggle"' in text
    assert "localStorage.getItem('sidebarHidden')" in text
    assert "localStorage.getItem('toastPopups')" in text
    assert "document.body.classList.toggle('sidebar-hidden', sidebarHidden);" in text
    assert 'id="footer">' not in text or "footer-controls" in text
    assert '<button id="toast-toggle" class="on" type="button" title="Hide spot popups" aria-label="Hide spot popups"><span class="footer-control-label">Hide Popups</span></button>' in text
    assert "const label = toastPopupsEnabled ? 'Hide Popups' : 'Show Popups';" in text
    assert '<button id="sidebar-toggle" type="button" title="Hide the sidebar" aria-label="Hide the sidebar"><span class="footer-control-label">Hide Sidebar</span></button>' in text
    assert '<span class="footer-control-label">Greyline</span>' in text
    assert '<span class="footer-control-label">Sound</span>' in text
    assert '<span class="footer-control-label">Theme</span>' in text
    assert "if (!toastPopupsEnabled) return;" in text
    assert "if (toastPopupsEnabled && matched.toast !== false)" in text


def test_public_web_greyline_mask_closes_through_dark_pole() -> None:
    text = Path("/home/jdlewis/GitHub/pyCluster/web/public_dxweb/static/index.html").read_text(encoding="utf-8")
    assert "const tLat  = Math.atan2(-Math.cos(dR), Math.tan(subLatR))" in text
    assert "const pole = sub.lat >= 0 ? -89.9 : 89.9;" in text
    assert "Close through the dark pole" in text


def test_web_admin_static_exposes_taxonomy_editor() -> None:
    text = Path("/home/jdlewis/GitHub/pyCluster/src/pycluster/web_admin.py").read_text(encoding="utf-8")
    assert 'data-view="taxonomy"' in text
    assert 'id="taxonomy"' in text
    assert 'id="taxonomy_comment_tags"' in text
    assert "loadTaxonomyEditor" in text
    assert "saveTaxonomy" in text
    assert text.index('data-view="telemetry"') < text.index('data-view="taxonomy"')
    assert text.index('<section class="panel view-section" id="telemetry">') < text.index('<section class="panel view-section" id="taxonomy">')


def test_web_admin_static_exposes_mail_tab_smtp_test() -> None:
    text = Path("/home/jdlewis/GitHub/pyCluster/src/pycluster/web_admin.py").read_text(encoding="utf-8")
    assert 'data-node-group="smtp">Mail (SMTP)</button>' in text
    assert 'id="smtp_test_email"' in text
    assert 'id="sendSmtpTest"' in text
    assert "j('/api/node/smtp-test'" in text


def test_web_admin_node_presentation_defaults_leave_auth_unchecked(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_admin_auth_defaults.db")
        cfg = _mk_config(db, admin_token="adm")
        store = SpotStore(db)
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
        )
        try:
            code, _, body = await _http_request(
                srv,
                "GET",
                "/api/node/presentation",
                headers={"X-Admin-Token": "adm"},
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["require_password"] is False
            assert data["registration_required"] is False
            assert data["verified_email_required_for_web"] is False
            assert data["verified_email_required_for_telnet"] is False
            assert data["mfa_enabled"] is False
            assert data["mfa_require_for_sysop"] is False
            assert data["mfa_require_for_users"] is False
        finally:
            await store.close()

    asyncio.run(run())


def test_web_admin_taxonomy_roundtrip(tmp_path) -> None:
    async def run() -> None:
        repo_root = tmp_path / "repo"
        config_dir = repo_root / "config"
        config_dir.mkdir(parents=True)
        config_path = config_dir / "pycluster.toml"
        config_path.write_text("[node]\nnode_call = \"AI3I-15\"\n\n[web]\nadmin_token = \"adm\"\n", encoding="utf-8")
        db = str(tmp_path / "taxonomy_roundtrip.db")
        cfg = _mk_config(db, admin_token="adm")
        store = SpotStore(db)
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
            config_path=str(config_path),
        )
        try:
            code, _, body = await _http_request(srv, "GET", "/api/node/taxonomy", headers={"X-Admin-Token": "adm"})
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            labels = {row["label"] for row in data["comment_tags"]}
            assert {"DIGITAL", "PILEUP", "ATNO", "LoTW", "TNX"} <= labels

            payload = {
                "mode_order": ["TRX"],
                "mode_rules": [{"pattern": "\\bTRX\\b", "value": "TRX", "button": "TRX"}],
                "activity_rules": [{"pattern": "\\bCASTLE\\b", "value": "CASTLE", "button": "CASTLE"}],
                "comment_tags": [{"pattern": "\\bCASTLE\\b", "label": "CASTLE", "button": "CASTLE", "color": "#123456"}],
                "rare_entities": ["Castle Island"],
            }
            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/node/taxonomy",
                headers={"X-Admin-Token": "adm", "Content-Type": "application/json"},
                body=json.dumps(payload).encode("utf-8"),
            )
            assert code == 200
            saved = json.loads(body.decode("utf-8"))
            assert saved["ok"] is True
            assert saved["comment_tags"][0]["label"] == "CASTLE"
            strings_path = config_dir / "strings.toml"
            text = strings_path.read_text(encoding="utf-8")
            assert "[public_web.taxonomy]" in text
            assert 'label = "CASTLE"' in text
            assert 'color = "#123456"' in text
        finally:
            await store.close()

    asyncio.run(run())


def test_web_admin_static_switches_to_editor_when_user_selected() -> None:
    text = Path("/home/jdlewis/GitHub/pyCluster/src/pycluster/web_admin.py").read_text(encoding="utf-8")
    assert 'id="userEditorTitle"' in text
    assert 'id="userMailStatus"' in text
    assert 'id="userRegistrationStatus"' in text
    assert "Select a user below to open the editor." in text


def test_web_admin_static_uses_five_row_user_pages_and_fixed_browser_geometry() -> None:
    text = Path("/home/jdlewis/GitHub/pyCluster/src/pycluster/web_admin.py").read_text(encoding="utf-8")
    assert "const USER_PAGE_SIZE = 5;" in text
    assert ".users-browser-stage{" in text
    assert "min-height:248px;" in text
    assert ".users-browser-stage .tablewrap{" in text
    assert "min-height:176px;" in text
    assert 'class="browser-toolbar"' in text
    assert 'id="userSearch"' in text
    assert 'id="userPrev"' in text
    assert 'id="userNext"' in text


def test_web_login_and_spot_post(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_spot.db")
        cfg = _mk_config(db, admin_token="adm")
        store = SpotStore(db)
        published: list[Spot] = []
        relayed: list[Spot] = []

        async def _pub(spot: Spot) -> int:
            published.append(spot)
            return 1

        async def _relay(spot: Spot) -> None:
            relayed.append(spot)

        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
            publish_spot_fn=_pub,
            relay_spot_fn=_relay,
        )
        now = int(datetime.now(timezone.utc).timestamp())
        await store.set_user_pref("N0CALL", "password", "pw1", now)
        await store.upsert_user_registry("N0CALL", now, privilege="user", email="n0call@example.test")
        await store.set_user_pref("N0CALL", "email_verified_epoch", str(now), now)
        try:
            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/auth/login",
                headers={"Content-Type": "application/json"},
                body=json.dumps({"call": "N0CALL", "password": "bad"}).encode("utf-8"),
            )
            assert code == 401

            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/auth/login",
                headers={"Content-Type": "application/json"},
                body=json.dumps({"call": "N0CALL", "password": "pw1"}).encode("utf-8"),
            )
            assert code == 200
            tok = json.loads(body.decode("utf-8"))["token"]

            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/spot",
                headers={"Content-Type": "application/json", "X-Web-Token": tok},
                body=json.dumps({"freq_khz": 14074.0, "dx_call": "K1ABC", "info": "FT8"}).encode("utf-8"),
            )
            assert code == 200
            resp = json.loads(body.decode("utf-8"))
            assert resp["ok"] is True
            assert resp["posted_by"] == "N0CALL"

            assert await store.count_spots() == 1
            assert len(published) == 1
            assert len(relayed) == 1
        finally:
            await store.close()

    asyncio.run(run())


def test_registration_approval_creates_limited_user_record(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "approve_registration.db")
        cfg = _mk_config(db, admin_token="adm")
        store = SpotStore(db)
        srv = WebAdminServer(config=cfg, store=store, started_at=datetime.now(timezone.utc), session_count_fn=lambda: 0)
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_registration_request(
            "N0CALL",
            now,
            display_name="Joe",
            home_node="AI3I-15",
            qth="Milwaukee, WI",
            qra="EN63AA",
            email="joe@example.test",
            note="",
            source="public-web",
            email_verified=True,
            status="pending",
        )
        try:
            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/registrations/approve",
                headers={"Content-Type": "application/json", "X-Admin-Token": "adm"},
                body=json.dumps({"call": "N0CALL"}).encode("utf-8"),
            )
            assert code == 200
            payload = json.loads(body.decode("utf-8"))
            assert payload["ok"] is True
            assert payload["user"]["privilege"] == ""
            assert payload["user"]["access"]["telnet"]["login"] is True
            assert payload["user"]["access"]["telnet"]["spots"] is False
            assert payload["user"]["access"]["web"]["announce"] is False
        finally:
            await store.close()

    asyncio.run(run())


def test_user_save_preserves_explicit_grid_square_when_qth_is_already_set(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "user_save_qra.db")
        cfg = _mk_config(db, admin_token="adm")
        store = SpotStore(db)
        srv = WebAdminServer(config=cfg, store=store, started_at=datetime.now(timezone.utc), session_count_fn=lambda: 0)
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry("N0CALL", now, display_name="Joe", qth="Milwaukee, WI", qra="EN63AA", email="joe@example.test")
        try:
            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/users",
                headers={"Content-Type": "application/json", "X-Admin-Token": "adm"},
                body=json.dumps(
                    {
                        "original_call": "N0CALL",
                        "call": "N0CALL",
                        "display_name": "Joe",
                        "qth": "Milwaukee, WI",
                        "qra": "FN31PR",
                        "location": "Downtown",
                        "email": "joe@example.test",
                        "privilege": "",
                        "access": {},
                    }
                ).encode("utf-8"),
            )
            assert code == 200
            row = await store.get_user_registry("N0CALL")
            assert row is not None
            assert str(row["qra"]) == "FN31PR"
            assert str(row["qth"]) == "Milwaukee, WI"
            assert await store.get_user_pref("N0CALL", "location") == "Downtown"
        finally:
            await store.close()

    asyncio.run(run())


def test_api_spots_marks_suspicious_calls_for_review(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_spot_review.db")
        cfg = _mk_config(db, admin_token="adm")
        cty_file = tmp_path / "cty.dat"
        cty_file.write_text("Testland: 05: 08: NA: 40.00: 75.00: 5.0: K:\n    K,=VER20260404;\n", encoding="ascii")
        cfg.public_web.cty_dat_path = str(cty_file)
        store = SpotStore(db)
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
        )
        now = int(datetime.now(timezone.utc).timestamp())
        orig_lookup = web_admin_mod.cty_lookup
        orig_loaded = web_admin_mod.cty_loaded
        orig_wpx_lookup = web_admin_mod.wpx_lookup
        orig_wpx_loaded = web_admin_mod.wpx_loaded
        web_admin_mod.cty_lookup = lambda call: object() if call in {"K1ABC", "N0CALL"} else None
        web_admin_mod.cty_loaded = lambda: True
        web_admin_mod.wpx_lookup = lambda call: None
        web_admin_mod.wpx_loaded = lambda: False
        try:
            await store.add_spot(Spot(14074.0, "K1ABC", now, "FT8", "N0CALL", "PEER1", ""))
            await store.add_spot(Spot(7168.0, "RG65SM", now, "CQ", "F8DRA", "PEER2", ""))
            code, _, body = await _http_request(srv, "GET", "/api/spots?limit=10", headers={"X-Admin-Token": "adm"})
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert len(data) == 2
            by_call = {row["dx_call"]: row for row in data}
            assert by_call["K1ABC"]["dx_review"]["suspicious"] is False
            assert by_call["RG65SM"]["dx_review"]["suspicious"] is True
            assert "unrecognized_prefix" in by_call["RG65SM"]["dx_review"]["reasons"]
        finally:
            web_admin_mod.cty_lookup = orig_lookup
            web_admin_mod.cty_loaded = orig_loaded
            web_admin_mod.wpx_lookup = orig_wpx_lookup
            web_admin_mod.wpx_loaded = orig_wpx_loaded
            await store.close()

    asyncio.run(run())


def test_proto_state_counts_pc18_handshake_as_known(tmp_path) -> None:
    db = str(tmp_path / "web_proto_pc18.db")
    cfg = _mk_config(db, admin_token="adm")
    store = SpotStore(db)
    srv = WebAdminServer(
        config=cfg,
        store=store,
        started_at=datetime.now(timezone.utc),
        session_count_fn=lambda: 0,
    )
    now = int(datetime.now(timezone.utc).timestamp())
    node_cfg = {
        "proto.peer.kc9gwk-1.pc18.family": "pycluster",
        "proto.peer.kc9gwk-1.pc18.proto": "5457",
        "proto.peer.kc9gwk-1.pc18.software": "pyCluster 1.0.6",
        "proto.peer.kc9gwk-1.last_epoch": str(now),
        "proto.peer.kc9gwk-1.last_pc_type": "PC18",
    }
    try:
        proto = srv._proto_state_for_peer(node_cfg, "KC9GWK-1", now)
        assert proto["known"] is True
        assert proto["health"] == "ok"
        assert proto["last_pc_type"] == "PC18"
    finally:
        asyncio.run(store.close())


def test_api_spots_uses_advisory_when_cty_data_is_unavailable(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_spot_review_advisory.db")
        cfg = _mk_config(db, admin_token="adm")
        store = SpotStore(db)
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
        )
        now = int(datetime.now(timezone.utc).timestamp())
        orig_lookup = web_admin_mod.cty_lookup
        orig_loaded = web_admin_mod.cty_loaded
        orig_wpx_lookup = web_admin_mod.wpx_lookup
        orig_wpx_loaded = web_admin_mod.wpx_loaded
        web_admin_mod.cty_lookup = lambda call: None
        web_admin_mod.cty_loaded = lambda: False
        web_admin_mod.wpx_lookup = lambda call: None
        web_admin_mod.wpx_loaded = lambda: False
        try:
            await store.add_spot(Spot(7168.0, "RG65SM", now, "CQ", "F8DRA", "PEER2", ""))
            code, _, body = await _http_request(srv, "GET", "/api/spots?limit=10", headers={"X-Admin-Token": "adm"})
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            review = data[0]["dx_review"]
            assert review["suspicious"] is False
            assert review["reasons"] == []
            assert "prefix_data_unavailable" in review["advisory"]
        finally:
            web_admin_mod.cty_lookup = orig_lookup
            web_admin_mod.cty_loaded = orig_loaded
            web_admin_mod.wpx_lookup = orig_wpx_lookup
            web_admin_mod.wpx_loaded = orig_wpx_loaded
            await store.close()

    asyncio.run(run())


def test_web_admin_login_can_require_email_otp(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_admin_mfa.db")
        cfg = _mk_config(db, admin_token="")
        cfg.smtp.host = "smtp.example.test"
        cfg.smtp.from_addr = "cluster@example.test"
        cfg.mfa.enabled = True
        cfg.mfa.require_for_sysop = True
        store = SpotStore(db)
        sent: list[tuple[str, str, str]] = []
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
        )
        srv._mfa._sender = lambda rcpt, subject, body: sent.append((rcpt, subject, body))  # type: ignore[assignment]
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry("AI3I", now, privilege="sysop", email="ai3i@example.test")
        await store.set_user_pref("AI3I", "password", "pw1", now)
        await store.set_user_pref("AI3I", "email_verified_epoch", str(now), now)
        try:
            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/auth/login",
                headers={"Content-Type": "application/json"},
                body=json.dumps({"call": "AI3I", "password": "pw1"}).encode("utf-8"),
            )
            assert code == 202
            payload = json.loads(body.decode("utf-8"))
            assert payload["mfa_required"] is True
            assert sent and sent[0][0] == "ai3i@example.test"
            challenge = next(iter(srv._mfa._challenges.values()))

            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/auth/login",
                headers={"Content-Type": "application/json"},
                body=json.dumps(
                    {
                        "call": "AI3I",
                        "password": "pw1",
                        "challenge_id": payload["challenge_id"],
                        "otp": challenge.code,
                    }
                ).encode("utf-8"),
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["ok"] is True
            assert data["sysop"] is True
        finally:
            await store.close()

    asyncio.run(run())


def test_web_admin_login_can_use_totp_authenticator(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_admin_totp.db")
        cfg = _mk_config(db, admin_token="")
        cfg.mfa.enabled = True
        cfg.mfa.require_for_sysop = True
        store = SpotStore(db)
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
        )
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry("AI3I", now, privilege="sysop", email="")
        await store.set_user_pref("AI3I", "password", "pw1", now)
        await store.set_user_pref("AI3I", "mfa_totp_secret", "JBSWY3DPEHPK3PXP", now)
        try:
            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/auth/login",
                headers={"Content-Type": "application/json"},
                body=json.dumps({"call": "AI3I", "password": "pw1"}).encode("utf-8"),
            )
            assert code == 202
            payload = json.loads(body.decode("utf-8"))
            assert payload["mfa_required"] is True
            assert payload["mfa_method"] == "totp"
            assert "challenge_id" not in payload

            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/auth/login",
                headers={"Content-Type": "application/json"},
                body=json.dumps({"call": "AI3I", "password": "pw1", "otp": totp_code("JBSWY3DPEHPK3PXP")}).encode("utf-8"),
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["ok"] is True
            assert data["sysop"] is True
        finally:
            await store.close()

    asyncio.run(run())


def test_web_admin_login_honors_per_user_mfa_override(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_admin_mfa_override.db")
        cfg = _mk_config(db, admin_token="")
        cfg.smtp.host = "smtp.example.test"
        cfg.smtp.from_addr = "cluster@example.test"
        cfg.mfa.enabled = True
        cfg.mfa.require_for_sysop = True
        store = SpotStore(db)
        sent: list[tuple[str, str, str]] = []
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
        )
        srv._mfa._sender = lambda rcpt, subject, body: sent.append((rcpt, subject, body))  # type: ignore[assignment]
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry("AI3I", now, privilege="sysop", email="ai3i@example.test")
        await store.set_user_pref("AI3I", "password", "pw1", now)
        await store.set_user_pref("AI3I", "mfa_email_otp", "off", now)
        await store.set_user_pref("AI3I", "email_verified_epoch", str(now), now)
        try:
            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/auth/login",
                headers={"Content-Type": "application/json"},
                body=json.dumps({"call": "AI3I", "password": "pw1"}).encode("utf-8"),
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["ok"] is True
            assert data["sysop"] is True
            assert sent == []
        finally:
            await store.close()

    asyncio.run(run())


def test_web_admin_login_requires_registration_and_valid_email(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_admin_registration_required.db")
        cfg = _mk_config(db, admin_token="")
        cfg.node.verified_email_required_for_web = True
        store = SpotStore(db)
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
        )
        now = int(datetime.now(timezone.utc).timestamp())
        try:
            await store.set_user_pref("AI3I", "password", "pw1", now)
            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/auth/login",
                headers={"Content-Type": "application/json"},
                body=json.dumps({"call": "AI3I", "password": "pw1"}).encode("utf-8"),
            )
            assert code == 403
            assert json.loads(body.decode("utf-8"))["error"] == "registration required"

            await store.upsert_user_registry("AI3I", now, privilege="user", email="")
            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/auth/login",
                headers={"Content-Type": "application/json"},
                body=json.dumps({"call": "AI3I", "password": "pw1"}).encode("utf-8"),
            )
            assert code == 403
            assert json.loads(body.decode("utf-8"))["error"] == "valid email required"

            await store.upsert_user_registry("AI3I", now, privilege="user", email="ai3i@example.test")
            await store.delete_user_pref("AI3I", "password")
            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/auth/login",
                headers={"Content-Type": "application/json"},
                body=json.dumps({"call": "AI3I", "password": "pw1"}).encode("utf-8"),
            )
            assert code == 403
            assert json.loads(body.decode("utf-8"))["error"] == "email verification required"

            await store.set_user_pref("AI3I", "email_verified_epoch", str(now), now)
            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/auth/login",
                headers={"Content-Type": "application/json"},
                body=json.dumps({"call": "AI3I", "password": "pw1"}).encode("utf-8"),
            )
            assert code == 403
            assert json.loads(body.decode("utf-8"))["error"] == "password setup required"

            await store.set_user_pref("AI3I", "password", "pw1", now)
            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/auth/login",
                headers={"Content-Type": "application/json"},
                body=json.dumps({"call": "AI3I", "password": "pw1"}).encode("utf-8"),
            )
            assert code == 200
            assert json.loads(body.decode("utf-8"))["ok"] is True
        finally:
            await store.close()

    asyncio.run(run())


def test_web_admin_sysop_login_bypasses_email_verification_gate(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_admin_sysop_email_gate.db")
        cfg = _mk_config(db, admin_token="")
        store = SpotStore(db)
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
        )
        now = int(datetime.now(timezone.utc).timestamp())
        try:
            await store.upsert_user_registry("AI3I", now, privilege="sysop", email="")
            await store.set_user_pref("AI3I", "password", "pw1", now)
            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/auth/login",
                headers={"Content-Type": "application/json"},
                body=json.dumps({"call": "AI3I", "password": "pw1"}).encode("utf-8"),
            )
            assert code == 200
            payload = json.loads(body.decode("utf-8"))
            assert payload["ok"] is True
            assert payload["sysop"] is True
        finally:
            await store.close()

    asyncio.run(run())


def test_web_admin_spot_throttle_returns_429(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_admin_spot_throttle.db")
        cfg = _mk_config(db, admin_token="")
        store = SpotStore(db)
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
        )
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry("N0CALL", now, privilege="user", email="n0call@example.test")
        await store.set_user_pref("N0CALL", "password", "pw1", now)
        await store.set_user_pref("N0CALL", "email_verified_epoch", str(now), now)
        await store.set_user_pref(cfg.node.node_call, "spot_throttle.max_per_window", "1", now)
        await store.set_user_pref(cfg.node.node_call, "spot_throttle.window_seconds", "300", now)
        try:
            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/auth/login",
                headers={"Content-Type": "application/json"},
                body=json.dumps({"call": "N0CALL", "password": "pw1"}).encode("utf-8"),
            )
            assert code == 200
            tok = json.loads(body.decode("utf-8"))["token"]

            code, _, _ = await _http_request(
                srv,
                "POST",
                "/api/spot",
                headers={"Content-Type": "application/json", "X-Web-Token": tok},
                body=json.dumps({"freq_khz": 14074.0, "dx_call": "K1ABC", "info": "FT8"}).encode("utf-8"),
            )
            assert code == 200

            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/spot",
                headers={"Content-Type": "application/json", "X-Web-Token": tok},
                body=json.dumps({"freq_khz": 14075.0, "dx_call": "K1ABD", "info": "FT8"}).encode("utf-8"),
            )
            assert code == 429
            resp = json.loads(body.decode("utf-8"))
            assert resp["error"] == "spot rate limit exceeded"
        finally:
            await store.close()

    asyncio.run(run())


def test_web_access_policy_controls_login_and_posting(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_access_policy.db")
        cfg = _mk_config(db, admin_token="")
        store = SpotStore(db)
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
        )
        now = int(datetime.now(timezone.utc).timestamp())
        await store.set_user_pref("N0CALL", "password", "pw1", now)
        await store.upsert_user_registry("N0CALL", now, privilege="user", email="n0call@example.test")
        await store.set_user_pref("N0CALL", "email_verified_epoch", str(now), now)
        try:
            await store.set_user_pref("N0CALL", "access.web.login", "off", now)
            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/auth/login",
                headers={"Content-Type": "application/json"},
                body=json.dumps({"call": "N0CALL", "password": "pw1"}).encode("utf-8"),
            )
            assert code == 401
            assert json.loads(body.decode("utf-8"))["error"] == "web login not allowed"

            await store.set_user_pref("N0CALL", "access.web.login", "on", now)
            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/auth/login",
                headers={"Content-Type": "application/json"},
                body=json.dumps({"call": "N0CALL", "password": "pw1"}).encode("utf-8"),
            )
            assert code == 200
            tok = json.loads(body.decode("utf-8"))["token"]
            row = await store.get_user_registry("N0CALL")
            assert row is not None
            assert str(row["last_login_peer"]).startswith("sysop-web")
            assert "sysop-web" in str(row["last_login_peer"])

            await store.set_user_pref("N0CALL", "access.web.spots", "off", now)
            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/spot",
                headers={"Content-Type": "application/json", "X-Web-Token": tok},
                body=json.dumps({"freq_khz": 14074.0, "dx_call": "K1ABC", "info": "FT8"}).encode("utf-8"),
            )
            assert code == 403
            assert json.loads(body.decode("utf-8"))["error"] == "spot posting not allowed via web"

            await store.set_user_pref("N0CALL", "access.web.chat", "off", now)
            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/chat",
                headers={"Content-Type": "application/json", "X-Web-Token": tok},
                body=json.dumps({"text": "hello chat"}).encode("utf-8"),
            )
            assert code == 403
            assert json.loads(body.decode("utf-8"))["error"] == "chat posting not allowed via web"

            await store.set_user_pref("N0CALL", "access.web.announce", "off", now)
            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/announce",
                headers={"Content-Type": "application/json", "X-Web-Token": tok},
                body=json.dumps({"text": "net tonight", "scope": "FULL"}).encode("utf-8"),
            )
            assert code == 403
            assert json.loads(body.decode("utf-8"))["error"] == "announce posting not allowed via web"
        finally:
            await store.close()

    asyncio.run(run())


def test_web_admin_requires_sysop_session_for_admin_endpoints(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_admin_auth.db")
        cfg = _mk_config(db, admin_token="adm")
        store = SpotStore(db)
        ops: list[tuple[str, str, str]] = []

        async def _stats():
            return {"peer1": {"policy_dropped": 2, "policy_reasons": {"route_filter": 2}}}

        async def _clear(_peer: str | None) -> int:
            return 1

        async def _connect(peer: str, dsn: str, profile: str = "dxspider", persist: bool = True, password: str = "") -> None:
            ops.append(("connect", peer, dsn, profile, persist, password))

        async def _disconnect(peer: str) -> bool:
            ops.append(("disconnect", peer, ""))
            return peer == "peer1"

        async def _set_profile(peer: str, profile: str) -> bool:
            ops.append(("profile", peer, profile))
            return peer == "peer1"

        async def _save(peer: str, dsn: str, profile: str = "dxspider", reconnect: bool = True, password: str = "") -> None:
            ops.append(("save", peer, dsn, profile, reconnect, password))

        async def _delete(peer: str) -> bool:
            ops.append(("delete", peer, ""))
            return peer == "peer1"

        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
            link_stats_fn=_stats,
            link_clear_policy_fn=_clear,
            link_connect_fn=_connect,
            link_disconnect_fn=_disconnect,
            link_set_profile_fn=_set_profile,
            link_save_peer_fn=_save,
            link_delete_peer_fn=_delete,
        )
        try:
            code, _, _ = await _http_request(srv, "GET", "/api/stats")
            assert code == 401

            now = int(datetime.now(timezone.utc).timestamp())
            await store.set_user_pref("K1SYS", "password", "pw2", now)
            await store.upsert_user_registry("K1SYS", now, privilege="sysop", email="k1sys@example.test")
            await store.set_user_pref("K1SYS", "email_verified_epoch", str(now), now)
            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/auth/login",
                headers={"Content-Type": "application/json"},
                body=json.dumps({"call": "K1SYS", "password": "pw2"}).encode("utf-8"),
            )
            assert code == 200
            tok = json.loads(body.decode("utf-8"))["token"]

            code, _, body = await _http_request(srv, "GET", "/api/stats", headers={"X-Web-Token": tok})
            assert code == 200
            assert "node" in json.loads(body.decode("utf-8"))

            code, _, _ = await _http_request(srv, "GET", "/api/policydrop")
            assert code == 401
            code, _, body = await _http_request(
                srv, "GET", "/api/policydrop", headers={"X-Admin-Token": "adm"}
            )
            assert code == 200
            rows = json.loads(body.decode("utf-8"))
            assert rows and rows[0]["peer"] == "peer1"

            code, _, _ = await _http_request(srv, "GET", "/api/proto/thresholds")
            assert code == 401
            code, _, body = await _http_request(
                srv, "GET", "/api/proto/thresholds", headers={"X-Admin-Token": "adm"}
            )
            assert code == 200
            cfg_rows = json.loads(body.decode("utf-8"))
            assert cfg_rows["stale_mins"] == 30
            assert cfg_rows["flap_score"] == 3

            code, _, _ = await _http_request(srv, "POST", "/api/policydrop/reset")
            assert code == 401
            code, _, body = await _http_request(
                srv, "POST", "/api/policydrop/reset", headers={"X-Admin-Token": "adm"}
            )
            assert code == 200
            assert json.loads(body.decode("utf-8"))["cleared_peers"] == 1

            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/proto/thresholds",
                headers={"X-Admin-Token": "adm", "Content-Type": "application/json"},
                body=json.dumps({"stale_mins": 5, "flap_score": 8, "flap_window_secs": 120}).encode("utf-8"),
            )
            assert code == 200
            updated = json.loads(body.decode("utf-8"))
            assert updated["ok"] is True
            assert updated["stale_mins"] == 5
            assert updated["flap_score"] == 8

            code, _, _ = await _http_request(srv, "GET", "/api/proto/summary")
            assert code == 401
            code, _, body = await _http_request(
                srv, "GET", "/api/proto/summary", headers={"X-Admin-Token": "adm"}
            )
            assert code == 200
            summary = json.loads(body.decode("utf-8"))
            assert "peers" in summary and "history_events" in summary

            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/peer/save",
                headers={"X-Admin-Token": "adm", "Content-Type": "application/json"},
                body=json.dumps({"peer": "peer1", "dsn": "tcp://127.0.0.1:7300", "password": "sekret", "profile": "dxspider"}).encode("utf-8"),
            )
            assert code == 200
            assert json.loads(body.decode("utf-8"))["ok"] is True

            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/peer/save",
                headers={"X-Admin-Token": "adm", "Content-Type": "application/json"},
                body=json.dumps({"peer": "inbound1", "dsn": "", "profile": "dxspider", "reconnect": False}).encode("utf-8"),
            )
            assert code == 200
            assert json.loads(body.decode("utf-8"))["ok"] is True

            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/peer/connect",
                headers={"X-Admin-Token": "adm", "Content-Type": "application/json"},
                body=json.dumps({"peer": "peer1", "dsn": "tcp://127.0.0.1:7300", "password": "sekret", "profile": "dxspider"}).encode("utf-8"),
            )
            assert code == 200
            assert json.loads(body.decode("utf-8"))["ok"] is True

            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/peer/profile",
                headers={"X-Admin-Token": "adm", "Content-Type": "application/json"},
                body=json.dumps({"peer": "peer1", "profile": "arcluster"}).encode("utf-8"),
            )
            assert code == 200
            assert json.loads(body.decode("utf-8"))["ok"] is True

            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/peer/disconnect",
                headers={"X-Admin-Token": "adm", "Content-Type": "application/json"},
                body=json.dumps({"peer": "peer1"}).encode("utf-8"),
            )
            assert code == 200
            assert json.loads(body.decode("utf-8"))["ok"] is True

            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/peer/delete",
                headers={"X-Admin-Token": "adm", "Content-Type": "application/json"},
                body=json.dumps({"peer": "peer1"}).encode("utf-8"),
            )
            assert code == 200
            assert json.loads(body.decode("utf-8"))["ok"] is True
            assert ("save", "peer1", "tcp://127.0.0.1:7300", "dxspider", True, "sekret") in ops
            assert ("save", "inbound1", "", "dxspider", False, "") in ops
            assert ("connect", "peer1", "tcp://127.0.0.1:7300", "dxspider", True, "sekret") in ops
            assert ("profile", "peer1", "arcluster") in ops
            assert ("disconnect", "peer1", "") in ops
            assert ("delete", "peer1", "") in ops
        finally:
            await store.close()

    asyncio.run(run())


def test_web_admin_callsign_login_requires_sysop_for_admin_endpoints(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_admin_callsign_auth.db")
        cfg = _mk_config(db, admin_token="adm")
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())

        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
        )
        try:
            await store.set_user_pref("N0CALL", "password", "pw1", now)
            await store.set_user_pref("K1SYS", "password", "pw2", now)
            await store.upsert_user_registry("N0CALL", now, privilege="user", email="n0call@example.test")
            await store.upsert_user_registry("K1SYS", now, privilege="sysop", email="k1sys@example.test")
            await store.set_user_pref("N0CALL", "email_verified_epoch", str(now), now)
            await store.set_user_pref("K1SYS", "email_verified_epoch", str(now), now)

            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/auth/login",
                headers={"Content-Type": "application/json"},
                body=json.dumps({"call": "N0CALL", "password": "pw1"}).encode("utf-8"),
            )
            assert code == 200
            user_login = json.loads(body.decode("utf-8"))
            assert user_login["sysop"] is False

            code, _, _ = await _http_request(
                srv,
                "GET",
                "/api/stats",
                headers={"X-Web-Token": user_login["token"]},
            )
            assert code == 401

            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/auth/login",
                headers={"Content-Type": "application/json"},
                body=json.dumps({"call": "K1SYS", "password": "pw2"}).encode("utf-8"),
            )
            assert code == 200
            sysop_login = json.loads(body.decode("utf-8"))
            assert sysop_login["sysop"] is True

            code, _, body = await _http_request(
                srv,
                "GET",
                "/api/stats",
                headers={"X-Web-Token": sysop_login["token"]},
            )
            assert code == 200
            assert "node" in json.loads(body.decode("utf-8"))
        finally:
            await store.close()

    asyncio.run(run())


def test_web_admin_logout_revokes_sysop_token(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_admin_logout.db")
        cfg = _mk_config(db, admin_token="adm")
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
        )
        try:
            await store.set_user_pref("K1SYS", "password", "pw2", now)
            await store.upsert_user_registry("K1SYS", now, privilege="sysop", email="k1sys@example.test")
            await store.set_user_pref("K1SYS", "email_verified_epoch", str(now), now)
            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/auth/login",
                headers={"Content-Type": "application/json"},
                body=json.dumps({"call": "K1SYS", "password": "pw2"}).encode("utf-8"),
            )
            assert code == 200
            tok = json.loads(body.decode("utf-8"))["token"]

            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/auth/logout",
                headers={"X-Web-Token": tok},
            )
            assert code == 200
            assert json.loads(body.decode("utf-8"))["ok"] is True

            code, _, _ = await _http_request(
                srv,
                "GET",
                "/api/stats",
                headers={"X-Web-Token": tok},
            )
            assert code == 401
        finally:
            await store.close()

    asyncio.run(run())


def test_web_admin_sysop_bootstrap_login_accepts_sysop_pseudocall(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_admin_sysop_bootstrap.db")
        cfg = _mk_config(db, admin_token="adm")
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
        )
        try:
            await store.set_user_pref("SYSOP", "password", "pw-sysop", now)
            await store.upsert_user_registry("SYSOP", now, privilege="sysop")
            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/auth/login",
                headers={"Content-Type": "application/json"},
                body=json.dumps({"call": "SYSOP", "password": "pw-sysop"}).encode("utf-8"),
            )
            assert code == 200
            payload = json.loads(body.decode("utf-8"))
            assert payload["call"] == "SYSOP"
            assert payload["sysop"] is True
        finally:
            await store.close()

    asyncio.run(run())


def test_web_admin_maintenance_cleanup_runs_when_enabled(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_admin_cleanup.db")
        cfg = _mk_config(db, admin_token="adm")
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        old_epoch = now - 40 * 86400
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
        )
        try:
            await store.set_user_pref(cfg.node.node_call, "retention.enabled", "on", now)
            await store.set_user_pref(cfg.node.node_call, "retention.spots_days", "30", now)
            await store.set_user_pref(cfg.node.node_call, "retention.stale_users_enabled", "on", now)
            await store.set_user_pref(cfg.node.node_call, "retention.stale_users_days", "30", now)
            await store.add_spot(
                Spot(
                    freq_khz=14074.0,
                    dx_call="K1OLD",
                    epoch=old_epoch,
                    info="FT8",
                    spotter="N0CALL",
                    source_node="N2WQ-1",
                    raw="",
                )
            )
            await store.upsert_user_registry("K1OLD", old_epoch, display_name="Old User", privilege="user")
            await store.record_login("K1OLD", old_epoch, "telnet")
            await store.set_user_pref("K1OLD", "password", "pw-old", old_epoch)
            await store.upsert_user_registry("K1LIVE", now, display_name="Recent User", privilege="user")
            await store.record_login("K1LIVE", now, "telnet")
            await store.upsert_user_registry("SYSOP", old_epoch, display_name="Sysop", privilege="sysop")
            await store.record_login("SYSOP", old_epoch, "telnet")
            await store.upsert_user_registry("K1BLOCK", old_epoch, display_name="Blocked User", privilege="user")
            await store.record_login("K1BLOCK", old_epoch, "telnet")
            await store.set_user_pref("K1BLOCK", "blocked_login", "on", old_epoch)
            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/maintenance/cleanup",
                headers={"X-Admin-Token": "adm"},
            )
            assert code == 200
            resp = json.loads(body.decode("utf-8"))
            assert resp["ok"] is True
            assert resp["removed"]["spots"] == 1
            assert resp["removed"]["users"] == 1
            assert await store.count_spots() == 0
            assert await store.get_user_registry("K1OLD") is None
            assert await store.get_user_pref("K1OLD", "password") is None
            assert await store.get_user_registry("K1LIVE") is not None
            assert await store.get_user_registry("SYSOP") is not None
            assert await store.get_user_registry("K1BLOCK") is not None
        finally:
            await store.close()

    asyncio.run(run())


def test_api_peers_includes_desired_reconnect_state(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_peer_desired.db")
        cfg = _mk_config(db, admin_token="adm")
        store = SpotStore(db)

        async def _stats():
            return {
                "AI3I-15": {
                    "profile": "spider",
                    "inbound": False,
                    "parsed_frames": 1,
                    "sent_frames": 1,
                    "policy_dropped": 0,
                }
            }

        async def _desired():
            return [
                {
                    "peer": "AI3I-15",
                    "dsn": "pycluster://dxspider.ai3i.net:7300?login=AI3I-16&client=AI3I-15",
                    "profile": "pycluster",
                    "reconnect_enabled": True,
                    "retry_count": 2,
                    "next_retry_epoch": 1773275000,
                    "last_connect_epoch": 1773274000,
                    "last_error": "timed out",
                    "pending_mail": 3,
                    "route_issues": 1,
                    "desired": True,
                    "connected": False,
                }
            ]

        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
            link_stats_fn=_stats,
            link_desired_peers_fn=_desired,
        )
        now = int(datetime.now(timezone.utc).timestamp())
        await store.set_user_pref(cfg.node.node_call, "proto.peer.ai3i-15.pc18.summary", "pyCluster 1.0.6", now)
        try:
            code, _, body = await _http_request(srv, "GET", "/api/peers", headers={"X-Admin-Token": "adm"})
            assert code == 200
            rows = json.loads(body.decode("utf-8"))
            assert len(rows) == 1
            row = rows[0]
            assert row["peer"] == "AI3I-15"
            assert row["desired"] is True
            assert row["connected"] is True
            assert row["reconnect_enabled"] is True
            assert row["retry_count"] == 2
            assert row["last_error"] == "timed out"
            assert row["pending_mail"] == 3
            assert row["route_issues"] == 1
            assert row["profile"] == "pycluster"
            assert row["transport"] == "pycluster"
            assert row["path_hint"] == "host dxspider.ai3i.net:7300"
            assert row["proto"]["pc18_summary"] == "pyCluster 1.0.6"
        finally:
            await store.close()

    asyncio.run(run())


def test_web_admin_user_notes_and_block_reason_round_trip(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_user_notes.db")
        cfg = _mk_config(db, admin_token="adm")
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry("N0CALL", now, privilege="")
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
        )
        try:
            payload = {
                "call": "N0CALL",
                "display_name": "Op Name",
                "blocked_reason": "General operator note",
                "privilege": "",
                "access": {},
            }
            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/users",
                headers={"X-Admin-Token": "adm", "Content-Type": "application/json"},
                body=json.dumps(payload).encode("utf-8"),
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))["user"]
            assert data["blocked_login"] is False
            assert data["user_note"] == "General operator note"
            assert data["blocked_reason"] == ""
            assert await store.get_user_pref("N0CALL", "note") == "General operator note"
            assert await store.get_user_pref("N0CALL", "blocked_reason") is None

            payload["privilege"] = "blocked"
            payload["blocked_reason"] = "Abuse report"
            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/users",
                headers={"X-Admin-Token": "adm", "Content-Type": "application/json"},
                body=json.dumps(payload).encode("utf-8"),
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))["user"]
            assert data["blocked_login"] is True
            assert data["user_note"] == "Abuse report"
            assert data["blocked_reason"] == "Abuse report"
            assert await store.get_user_pref("N0CALL", "note") == "Abuse report"
            assert await store.get_user_pref("N0CALL", "blocked_reason") == "Abuse report"

            code, _, body = await _http_request(
                srv,
                "GET",
                "/api/users?limit=5&offset=0",
                headers={"X-Admin-Token": "adm"},
            )
            assert code == 200
            rows = json.loads(body.decode("utf-8"))["rows"]
            row = next(r for r in rows if r["call"] == "N0CALL")
            assert row["user_note"] == "Abuse report"
            assert row["blocked_reason"] == "Abuse report"
        finally:
            await store.close()

    asyncio.run(run())


def test_smtp_mailer_sets_required_rfc5322_headers(monkeypatch) -> None:
    sent = {}

    class _FakeSMTP:
        def __init__(self, host: str, port: int, timeout: int = 10) -> None:
            sent["host"] = host
            sent["port"] = port
            sent["timeout"] = timeout

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def starttls(self, context=None) -> None:
            sent["starttls"] = True

        def login(self, username: str, password: str) -> None:
            sent["login"] = (username, password)

        def send_message(self, msg) -> None:
            sent["msg"] = msg

    monkeypatch.setattr("pycluster.mfa.smtplib.SMTP", _FakeSMTP)

    cfg = _mk_config("/tmp/smtp_headers.db").smtp
    cfg.host = "smtp.example.test"
    cfg.port = 587
    cfg.from_addr = "pycluster@example.test"
    cfg.from_name = "pyCluster"
    cfg.starttls = True
    cfg.use_ssl = False
    cfg.username = ""
    cfg.password = ""
    cfg.timeout_seconds = 10

    SMTPMailer(cfg).send_code("user@example.test", "Test subject", "Test body")

    msg = sent["msg"]
    assert msg["From"] == "pyCluster <pycluster@example.test>"
    assert msg["To"] == "user@example.test"
    assert msg["Subject"] == "Test subject"
    assert msg["Date"]
    assert msg["Message-ID"]


def test_web_admin_node_presentation_includes_dataset_status(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_node_datasets.db")
        cty_path = Path(__file__).resolve().parents[1] / "fixtures" / "live" / "dxspider" / "cty.dat"
        cfg = _mk_config(db, admin_token="adm")
        cfg.public_web.cty_dat_path = str(cty_path)
        cfg.public_web.wpxloc_raw_path = str(tmp_path / "missing-wpxloc.raw")
        load_cty(str(cty_path))
        store = SpotStore(db)
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
        )
        try:
            code, _, body = await _http_request(srv, "GET", "/api/node/presentation", headers={"X-Admin-Token": "adm"})
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["datasets"]["cty"]["loaded"] is True
            assert data["datasets"]["cty"]["version"].startswith("VER")
            assert data["datasets"]["wpxloc"]["status"] == "missing"
        finally:
            await store.close()

    asyncio.run(run())

def test_web_admin_node_presentation_round_trip(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_node_presentation.db")
        cfg = _mk_config(db, admin_token="adm")
        cfg.node.node_call = "AI3I-15"
        cfg.node.welcome_title = "Welcome"
        cfg.node.motd = "Default MOTD"
        cfg_path = tmp_path / "pycluster.toml"
        cfg_path.write_text(
            "[node]\n"
            "node_call = \"AI3I-15\"\n"
            "node_alias = \"N0NODE\"\n"
            "owner_name = \"Cluster Sysop\"\n"
            "qth = \"Unknown\"\n"
            "node_locator = \"\"\n"
            "motd = \"Default MOTD\"\n"
            "branding_name = \"pyCluster\"\n"
            "welcome_title = \"Welcome\"\n"
            "welcome_body = \"\"\n"
            "login_tip = \"Tip\"\n"
            "show_status_after_login = true\n"
            "require_password = true\n"
            "support_contact = \"\"\n"
            "website_url = \"\"\n"
            "prompt_template = \"[{timestamp}] {node}{suffix}\"\n\n"
            "[telnet]\n"
            "host = \"127.0.0.1\"\n"
            "port = 0\n"
            "ports = []\n"
            "max_clients = 100\n"
            "idle_timeout_seconds = 30\n"
            "max_line_length = 512\n\n"
            "[web]\n"
            "host = \"0.0.0.0\"\n"
            "port = 0\n"
            "admin_token = \"adm\"\n\n"
            "[public_web]\n"
            "enabled = false\n"
            "host = \"127.0.0.1\"\n"
            "port = 8081\n"
            "static_dir = \"\"\n"
            "cty_dat_path = \"\"\n\n"
            "[store]\n"
            f"sqlite_path = {json.dumps(db)}\n",
            encoding="utf-8",
        )
        store = SpotStore(db)
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
            config_path=str(cfg_path),
        )
        try:
            code, _, body = await _http_request(
                srv,
                "GET",
                "/api/node/presentation",
                headers={"X-Admin-Token": "adm"},
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["node_call"] == "AI3I-15"
            assert data["welcome_title"] == "Welcome"
            assert data["motd"] == "Default MOTD"
            assert data["software_version"] == f"pyCluster {__version__}"
            assert data["retention_stale_users_enabled"] is False
            assert data["retention_stale_users_days"] == 365

            payload = {
                "node_call": "AI3I-7",
                "node_alias": "AI3I",
                "owner_name": "John D. Lewis",
                "qth": "Western Pennsylvania",
                "node_locator": "FN00FS",
                "telnet_ports": "7300,7373,8000",
                "branding_name": "pyCluster",
                "welcome_title": "Welcome back",
                "welcome_body": "Friendly DX for everyone.",
                "login_tip": "Tip: try sh/dx 10",
                "show_status_after_login": False,
                "require_password": True,
                "registration_required": True,
                "verified_email_required_for_web": True,
                "verified_email_required_for_telnet": True,
                "initial_grace_logins": 5,
                "smtp_host": "smtp.example.test",
                "smtp_port": 465,
                "smtp_username": "mailer",
                "smtp_password": "app-pass",
                "smtp_from_addr": "pycluster@example.test",
                "smtp_from_name": "pyCluster Ops",
                "smtp_starttls": False,
                "smtp_use_ssl": True,
                "smtp_timeout_seconds": 15,
                "qrz_username": "n9jr",
                "qrz_password": "qrz-pass",
                "qrz_agent": "pyCluster-test",
                "qrz_api_url": "https://xmldata.qrz.com/xml/current/",
                "satellite_keps_path": "./data/amateur.txt",
                "satellite_prediction_hours": 48,
                "satellite_pass_step_seconds": 120,
                "satellite_min_elevation_deg": 5.5,
                "mfa_enabled": True,
                "mfa_require_for_sysop": True,
                "mfa_require_for_users": False,
                "mfa_issuer": "AI3I Cluster",
                "mfa_otp_ttl_seconds": 900,
                "mfa_otp_length": 8,
                "mfa_max_attempts": 4,
                "mfa_resend_cooldown_seconds": 45,
                "retention_stale_users_enabled": True,
                "retention_stale_users_days": 180,
                "support_contact": "dxcluster@ai3i.net",
                "website_url": "https://github.com/AI3I/pyCluster",
                "motd": "Warm MOTD",
                "prompt_template": "[{timestamp}] {node}{suffix}",
            }
            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/node/presentation",
                headers={"X-Admin-Token": "adm", "Content-Type": "application/json"},
                body=json.dumps(payload).encode("utf-8"),
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["node_call"] == "AI3I-7"
            assert data["node_locator"] == "FN00FS"
            assert data["telnet_ports"] == "7300,7373,8000"
            assert data["welcome_title"] == "Welcome back"
            assert data["show_status_after_login"] is False
            assert data["require_password"] is True
            assert data["registration_required"] is True
            assert data["verified_email_required_for_web"] is True
            assert data["verified_email_required_for_telnet"] is True
            assert data["initial_grace_logins"] == 5
            assert data["smtp_host"] == "smtp.example.test"
            assert data["smtp_port"] == 465
            assert data["smtp_username"] == "mailer"
            assert data["smtp_password"] == "app-pass"
            assert data["smtp_from_addr"] == "pycluster@example.test"
            assert data["smtp_from_name"] == "pyCluster Ops"
            assert data["smtp_starttls"] is False
            assert data["smtp_use_ssl"] is True
            assert data["smtp_timeout_seconds"] == 15
            assert data["qrz_username"] == "n9jr"
            assert data["qrz_password"] == "qrz-pass"
            assert data["qrz_agent"] == "pyCluster-test"
            assert data["qrz_api_url"] == "https://xmldata.qrz.com/xml/current/"
            assert data["satellite_keps_path"] == "./data/amateur.txt"
            assert data["satellite_prediction_hours"] == 48
            assert data["satellite_pass_step_seconds"] == 120
            assert data["satellite_min_elevation_deg"] == 5.5
            assert data["mfa_enabled"] is True
            assert data["mfa_require_for_sysop"] is True
            assert data["mfa_require_for_users"] is False
            assert data["mfa_issuer"] == "AI3I Cluster"
            assert data["mfa_otp_ttl_seconds"] == 900
            assert data["mfa_otp_length"] == 8
            assert data["mfa_max_attempts"] == 4
            assert data["mfa_resend_cooldown_seconds"] == 45
            assert data["retention_stale_users_enabled"] is True
            assert data["retention_stale_users_days"] == 180
            assert data["motd"] == "Warm MOTD"
            assert data["prompt_template"] == "[{timestamp}] {node}{suffix}"

            assert await store.get_user_pref("AI3I-15", "node_call") is None
            assert await store.get_user_pref("AI3I-15", "node_locator") is None
            assert await store.get_user_pref("AI3I-15", "telnet_ports") is None
            assert await store.get_user_pref("AI3I-15", "welcome_title") is None
            assert await store.get_user_pref("AI3I-15", "require_password") is None
            assert await store.get_user_pref("AI3I-15", "prompt_template") is None
            saved = tomllib.loads(cfg_path.read_text(encoding="utf-8"))
            assert saved["node"]["node_call"] == "AI3I-7"
            assert saved["node"]["node_locator"] == "FN00FS"
            assert saved["node"]["welcome_title"] == "Welcome back"
            assert saved["node"]["require_password"] is True
            assert saved["node"]["registration_required"] is True
            assert saved["node"]["verified_email_required_for_web"] is True
            assert saved["node"]["verified_email_required_for_telnet"] is True
            assert saved["node"]["initial_grace_logins"] == 5
            assert saved["node"]["prompt_template"] == "[{timestamp}] {node}{suffix}"
            assert saved["telnet"]["ports"] == [7300, 7373, 8000]
            assert saved["smtp"]["host"] == "smtp.example.test"
            assert saved["smtp"]["port"] == 465
            assert saved["smtp"]["use_ssl"] is True
            assert saved["qrz"]["username"] == "n9jr"
            assert saved["qrz"]["password"] == "qrz-pass"
            assert saved["qrz"]["agent"] == "pyCluster-test"
            assert saved["satellite"]["keps_path"] == "./data/amateur.txt"
            assert saved["satellite"]["prediction_hours"] == 48
            assert saved["satellite"]["pass_step_seconds"] == 120
            assert saved["satellite"]["min_elevation_deg"] == 5.5
            assert saved["mfa"]["enabled"] is True
            assert saved["mfa"]["issuer"] == "AI3I Cluster"
            assert saved["mfa"]["resend_cooldown_seconds"] == 45
        finally:
            await store.close()

    asyncio.run(run())


def test_web_admin_user_email_change_resets_verification_state(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_admin_email_reset.db")
        cfg = _mk_config(db, admin_token="")
        store = SpotStore(db)
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
        )
        now = int(datetime.now(timezone.utc).timestamp())
        try:
            await store.upsert_user_registry("K1ABC", now, privilege="user", email="old@example.test")
            await store.set_user_pref("K1ABC", "email_verified_epoch", str(now), now)
            await store.set_user_pref("K1ABC", "registration_state", "verified", now)

            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/users",
                headers={"X-Admin-Token": "adm", "Content-Type": "application/json"},
                body=json.dumps(
                    {
                        "call": "K1ABC",
                        "display_name": "",
                        "home_node": "",
                        "address": "",
                        "qth": "",
                        "qra": "",
                        "email": "new@example.test",
                        "privilege": "user",
                        "blocked_reason": "",
                        "mfa_email_otp": "default",
                    }
                ).encode("utf-8"),
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["user"]["email"] == "new@example.test"
            assert await store.get_user_pref("K1ABC", "email_verified_epoch") is None
            assert await store.get_user_pref("K1ABC", "registration_state") == "pending"
            assert await store.get_user_pref("K1ABC", "grace_logins_remaining") == str(cfg.node.initial_grace_logins)
        finally:
            await store.close()

    asyncio.run(run())


def test_web_admin_console_page_includes_software_version_slot(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_admin_console_page.db")
        cfg = _mk_config(db, admin_token="adm")
        store = SpotStore(db)
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
        )
        try:
            code, _, body = await _http_request(srv, "GET", "/")
            assert code == 200
            html = body.decode("utf-8")
            assert 'id="navVersion"' in html
            assert "Software</label>" in html
            assert 'id="retention_stale_users_enabled"' in html
            assert 'id="smtp_host"' in html
            assert 'id="mfa_enabled"' in html
            assert 'data-node-group="general"' in html
            assert 'data-node-group="auth"' in html
            assert 'data-node-group="smtp"' in html
            assert 'data-node-group="maintenance"' in html
            assert 'data-view="taxonomy"' in html
            assert 'id="node-group-general"' in html
            assert 'id="node-group-auth"' in html
            assert 'id="node-group-smtp"' in html
            assert 'id="node-group-maintenance"' in html
            assert 'id="taxonomy"' in html
            assert 'id="upgradeStatus"' in html
            assert 'id="checkUpgrade"' in html
            assert 'id="runUpgrade"' in html
            assert 'class="tablewrap compact"' in html
            assert 'id="userMailStatus"' in html
        finally:
            await store.close()

    asyncio.run(run())


def test_web_admin_upgrade_status_and_request(tmp_path, monkeypatch) -> None:
    async def run() -> None:
        repo_root = tmp_path / "repo"
        (repo_root / "config").mkdir(parents=True)
        cfg_path = repo_root / "config" / "pycluster.toml"
        cfg_path.write_text("", encoding="utf-8")
        db = str(tmp_path / "web_upgrade.db")
        cfg = _mk_config(db, admin_token="adm")
        store = SpotStore(db)
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
            config_path=str(cfg_path),
        )
        monkeypatch.setattr("pycluster.web_admin.detect_upgrade_availability", lambda repo_root, current_version: {
            "current_version": current_version,
            "latest_local_tag": "v1.0.6",
            "latest_remote_tag": "v1.0.7",
            "available": True,
            "available_version": "1.0.7",
            "remote_checked": True,
            "remote_error": "",
        })
        monkeypatch.setattr("pycluster.web_admin.migration_hooks", lambda repo_root: ["run_upgrade_1_0_1"])
        try:
            code, _, body = await _http_request(
                srv,
                "GET",
                "/api/upgrade/status",
                headers={"X-Admin-Token": "adm"},
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["availability"]["available"] is True
            assert data["availability"]["available_version"] == "1.0.7"
            assert data["migrations"] == ["run_upgrade_1_0_1"]
            assert data["status"]["state"] == "idle"

            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/upgrade/request",
                headers={"X-Admin-Token": "adm", "X-Web-Token": "tok"},
                body=b"{}",
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["ok"] is True
            queued = json.loads((repo_root / "data" / "upgrade-request.json").read_text(encoding="utf-8"))
            assert queued["current_version"] == __version__

            (repo_root / "data").mkdir(parents=True, exist_ok=True)
            (repo_root / "data" / "upgrade-status.json").write_text(
                json.dumps({"state": "running", "requested_by": "AI3I", "log_path": "/tmp/upgrade.log"}),
                encoding="utf-8",
            )
            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/upgrade/request",
                headers={"X-Admin-Token": "adm", "X-Web-Token": "tok"},
                body=b"{}",
            )
            assert code == 409
            assert json.loads(body.decode("utf-8"))["error"] == "upgrade already running"
        finally:
            await store.close()

    asyncio.run(run())


def test_web_admin_node_presentation_rebinds_telnet_ports(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_node_ports.db")
        cfg = _mk_config(db, admin_token="adm")
        store = SpotStore(db)
        rebound: list[tuple[int, ...]] = []

        async def _rebind(ports: tuple[int, ...]) -> tuple[int, ...]:
            rebound.append(tuple(ports))
            return tuple(ports)

        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
            telnet_rebind_fn=_rebind,
        )
        try:
            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/node/presentation",
                headers={"X-Admin-Token": "adm", "Content-Type": "application/json"},
                body=json.dumps({"telnet_ports": "7300,7373,8000"}).encode("utf-8"),
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["telnet_ports"] == "7300,7373,8000"
            assert rebound == [(7300, 7373, 8000)]
        finally:
            await store.close()

    asyncio.run(run())


def test_web_admin_login_failure_logs_structured_authfail(tmp_path, caplog) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_admin_authfail.db")
        cfg = _mk_config(db, admin_token="adm")
        store = SpotStore(db)
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
        )
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.upsert_user_registry("AI3I", now, privilege="sysop", email="ai3i@example.test")
            await store.set_user_pref("AI3I", "password", "correct", now)
            await store.set_user_pref("AI3I", "email_verified_epoch", str(now), now)
            with caplog.at_level(logging.WARNING, logger="pycluster.web_admin"):
                code, _, body = await _http_request(
                    srv,
                    "POST",
                    "/api/auth/login",
                    headers={
                        "Content-Type": "application/json",
                        "X-Forwarded-For": "198.51.100.24",
                    },
                    body=json.dumps({"call": "AI3I", "password": "wrong"}).encode("utf-8"),
                )
            assert code == 401
            assert json.loads(body.decode("utf-8"))["error"] == "login failed"
            assert "AUTHFAIL channel=sysop-web ip=198.51.100.24 call=AI3I reason=bad_password" in caplog.text
        finally:
            await store.close()

    asyncio.run(run())


def test_web_admin_security_endpoint_reads_authfail_log(tmp_path, monkeypatch) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_security.db")
        cfg = _mk_config(db, admin_token="adm")
        store = SpotStore(db)
        auth_log = tmp_path / "authfail.log"
        monkeypatch.setattr(web_admin_mod, "AUTHFAIL_LOG_PATH", auth_log)
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
        )
        try:
            auth_log.write_text(
                "2026-03-14 13:08:29,406 WARNING AUTHFAIL channel=sysop-web ip=198.51.100.24 call=AI3I reason=bad_password\n"
                "2026-03-14 13:08:29,517 WARNING AUTHFAIL channel=public-web ip=203.0.113.77 call=AI3I reason=invalid_credentials\n",
                encoding="utf-8",
            )
            code, _, body = await _http_request(
                srv,
                "GET",
                "/api/security?limit=10",
                headers={"X-Admin-Token": "adm"},
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["auth_failures"][0]["channel"] == "public-web"
            assert data["auth_failures"][0]["ip"] == "203.0.113.77"
            assert data["auth_failures"][1]["channel"] == "sysop-web"
            assert isinstance(data["bans"], list)
        finally:
            await store.close()

    asyncio.run(run())


def test_web_chat_and_bulletin_posts(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_text_posts.db")
        cfg = _mk_config(db, admin_token="")
        store = SpotStore(db)
        chats: list[tuple[str, str]] = []
        relayed_chats: list[tuple[str, str]] = []
        bullets: list[tuple[str, str, str, str]] = []
        relayed_bullets: list[tuple[str, str, str, str]] = []

        async def _pub_chat(sender: str, text: str) -> int:
            chats.append((sender, text))
            return 1

        async def _relay_chat(sender: str, text: str) -> None:
            relayed_chats.append((sender, text))

        async def _pub_b(cat: str, sender: str, scope: str, text: str) -> int:
            bullets.append((cat, sender, scope, text))
            return 1

        async def _relay_b(cat: str, sender: str, scope: str, text: str) -> None:
            relayed_bullets.append((cat, sender, scope, text))

        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
            publish_chat_fn=_pub_chat,
            relay_chat_fn=_relay_chat,
            publish_bulletin_fn=_pub_b,
            relay_bulletin_fn=_relay_b,
        )
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry("N0CALL", now, privilege="user", email="n0call@example.test")
        await store.set_user_pref("N0CALL", "email_verified_epoch", str(now), now)
        await store.set_user_pref("N0CALL", "password", "pw1", now)
        code, _, body = await _http_request(
            srv,
            "POST",
            "/api/auth/login",
            headers={"Content-Type": "application/json"},
            body=json.dumps({"call": "N0CALL", "password": "pw1"}).encode("utf-8"),
        )
        assert code == 200
        tok = json.loads(body.decode("utf-8"))["token"]

        try:
            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/chat",
                headers={"Content-Type": "application/json", "X-Web-Token": tok},
                body=json.dumps({"text": "hello chat"}).encode("utf-8"),
            )
            assert code == 200
            assert json.loads(body.decode("utf-8"))["category"] == "chat"

            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/announce",
                headers={"Content-Type": "application/json", "X-Web-Token": tok},
                body=json.dumps({"text": "net tonight", "scope": "FULL"}).encode("utf-8"),
            )
            assert code == 200
            resp = json.loads(body.decode("utf-8"))
            assert resp["category"] == "announce"
            assert resp["scope"] == "FULL"

            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/wcy",
                headers={"Content-Type": "application/json", "X-Web-Token": tok},
                body=json.dumps({"text": "A=8 K=2"}).encode("utf-8"),
            )
            assert code == 200
            assert json.loads(body.decode("utf-8"))["category"] == "wcy"

            rows_chat = await store.list_bulletins("chat", limit=5)
            rows_ann = await store.list_bulletins("announce", limit=5)
            rows_wcy = await store.list_bulletins("wcy", limit=5)
            assert rows_chat and "hello chat" in str(rows_chat[0]["body"])
            assert rows_ann and str(rows_ann[0]["scope"]) == "FULL"
            assert rows_wcy and "A=8 K=2" in str(rows_wcy[0]["body"])
            assert chats and relayed_chats
            assert bullets and relayed_bullets
        finally:
            await store.close()

    asyncio.run(run())


def test_web_peers_includes_proto_state(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_peers_proto.db")
        cfg = _mk_config(db, admin_token="")
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())

        async def _stats():
            return {
                "peer1": {
                    "profile": "spider",
                    "inbound": False,
                    "connected_epoch": now - 3600,
                    "last_tx_epoch": now - 60,
                    "last_rx_epoch": now - 120,
                    "parsed_frames": 12,
                    "sent_frames": 8,
                    "policy_dropped": 1,
                    "last_pc_type": "PC51",
                    "policy_reasons": {"route_filter": 1},
                }
            }

        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
            link_stats_fn=_stats,
        )
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc24.call", "OH8X", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc24.flag", "1", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc50.call", "W3LPL", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc50.count", "63", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc51.to", "AI3I-15", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc51.from", "WB3FFV-2", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc51.value", "1", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.change_count", "4", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.flap_score", "2", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.last_epoch", str(now), now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.last_change_epoch", str(now), now)
        await store.set_user_pref(
            cfg.node.node_call,
            "proto.peer.peer1.history",
            json.dumps([{"epoch": now, "key": "pc24.flag", "from": "0", "to": "1"}]),
            now,
        )
        try:
            code, _, _ = await _http_request(srv, "GET", "/api/peers")
            assert code == 401
            code, _, body = await _http_request(srv, "GET", "/api/peers", headers={"X-Admin-Token": "adm"})
            assert code == 200
            rows = json.loads(body.decode("utf-8"))
            assert len(rows) == 1
            assert rows[0]["transport"] == ""
            assert rows[0]["path_hint"] == ""
            assert rows[0]["last_tx_epoch"] == now - 60
            assert rows[0]["last_rx_epoch"] == now - 120
            assert rows[0]["link"]["health"] == "connected"
            assert rows[0]["link"]["activity"] == "bidirectional"
            assert rows[0]["link"]["summary"] == "bidirectional traffic"
            assert rows[0]["link"]["tx_age_min"] == 1
            assert rows[0]["link"]["rx_age_min"] == 2
            p = rows[0]["proto"]
            assert p["known"] is True
            assert p["health"] == "ok"
            assert p["change_count"] == 4
            assert p["flap_score"] == 2
            assert p["history_count"] == 1
            assert p["last_event"]["key"] == "pc24.flag"
            assert p["pc24"]["call"] == "OH8X"
            assert p["pc50"]["count"] == "63"
            assert p["pc51"]["to"] == "AI3I-15"
        finally:
            await store.close()

    asyncio.run(run())


def test_web_peers_reports_transmit_active_receive_quiet_link(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_peers_rx_quiet.db")
        cfg = _mk_config(db, admin_token="adm")
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())

        async def _stats():
            return {
                "KC9GWK-1": {
                    "profile": "pycluster",
                    "inbound": True,
                    "connected_epoch": now - 3600,
                    "last_tx_epoch": now - 45,
                    "last_rx_epoch": None,
                    "parsed_frames": 0,
                    "sent_frames": 42,
                    "tx_by_type": {"PC61": 40, "PC20": 2},
                    "rx_by_type": {},
                }
            }

        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
            link_stats_fn=_stats,
        )
        await store.set_user_pref(cfg.node.node_call, "proto.peer.kc9gwk-1.pc18.family", "pycluster", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.kc9gwk-1.pc18.summary", "pyCluster 1.0.6", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.kc9gwk-1.last_epoch", str(now - 86400), now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.kc9gwk-1.last_pc_type", "PC18", now)
        try:
            code, _, body = await _http_request(srv, "GET", "/api/peers", headers={"X-Admin-Token": "adm"})
            assert code == 200
            row = json.loads(body.decode("utf-8"))[0]
            assert row["peer"] == "KC9GWK-1"
            assert row["connected"] is True
            assert row["proto"]["health"] == "stale"
            assert row["link"]["health"] == "connected"
            assert row["link"]["activity"] == "transmit_active"
            assert row["link"]["summary"] == "transmit active; receive quiet"
            assert row["link"]["tx_age_min"] == 0
            assert row["link"]["rx_age_min"] == -1
        finally:
            await store.close()

    asyncio.run(run())


def test_web_proto_history_endpoint(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_proto_hist.db")
        cfg = _mk_config(db, admin_token="adm")
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
            link_stats_fn=lambda: asyncio.sleep(0, result={}),
        )
        await store.set_user_pref(
            cfg.node.node_call,
            "proto.peer.peer1.history",
            json.dumps(
                [
                    {"epoch": now - 20, "key": "pc24.flag", "from": "0", "to": "1"},
                    {"epoch": now - 10, "key": "pc24.flag", "from": "1", "to": "0"},
                ]
            ),
            now,
        )
        await store.set_user_pref(
            cfg.node.node_call,
            "proto.peer.peer2.history",
            json.dumps([{"epoch": now - 5, "key": "pc51.value", "from": "0", "to": "1"}]),
            now,
        )
        try:
            code, _, _ = await _http_request(srv, "GET", "/api/proto/history")
            assert code == 401
            code, _, body = await _http_request(
                srv, "GET", "/api/proto/history?peer=peer1&limit=5", headers={"X-Admin-Token": "adm"}
            )
            assert code == 200
            rows = json.loads(body.decode("utf-8"))
            assert len(rows) == 2
            assert all(r["peer"] == "peer1" for r in rows)
            assert rows[0]["epoch"] >= rows[1]["epoch"]
        finally:
            await store.close()

    asyncio.run(run())


def test_web_peers_displays_saved_inbound_peer_without_live_session(tmp_path) -> None:
    async def _desired():
        return [
            {
                "peer": "N9JR-2",
                "dsn": "",
                "profile": "dxspider",
                "reconnect_enabled": False,
                "desired": True,
                "connected": False,
            }
        ]

    async def run() -> None:
        db = str(tmp_path / "web_inbound_peer.db")
        cfg = _mk_config(db, admin_token="adm")
        store = SpotStore(db)
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
            link_stats_fn=lambda: asyncio.sleep(0, result={}),
            link_desired_peers_fn=_desired,
        )
        try:
            code, _, body = await _http_request(srv, "GET", "/api/peers", headers={"X-Admin-Token": "adm"})
            assert code == 200
            rows = json.loads(body.decode("utf-8"))
            assert rows[0]["peer"] == "N9JR-2"
            assert rows[0]["desired"] is True
            assert rows[0]["connected"] is False
            assert rows[0]["inbound"] is True
            assert rows[0]["dsn"] == ""
        finally:
            await store.close()

    asyncio.run(run())


def test_web_proto_alerts_ignore_expired_flap_scores(tmp_path) -> None:
    async def _stats():
        return {
            "peer1": {"profile": "spider", "inbound": False, "parsed_frames": 1, "sent_frames": 1, "policy_dropped": 0},
        }

    async def run() -> None:
        db = str(tmp_path / "web_proto_expired_flap.db")
        cfg = _mk_config(db, admin_token="adm")
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        old = now - 900
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
            link_stats_fn=_stats,
        )
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc24.call", "K1ABC", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc24.flag", "1", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.flap_score", "7", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.last_epoch", str(now), now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.last_change_epoch", str(old), now)
        try:
            code, _, body = await _http_request(srv, "GET", "/api/proto/alerts", headers={"X-Admin-Token": "adm"})
            assert code == 200
            rows = json.loads(body.decode("utf-8"))
            assert rows == []

            code, _, body = await _http_request(srv, "GET", "/api/peers", headers={"X-Admin-Token": "adm"})
            assert code == 200
            peers = json.loads(body.decode("utf-8"))
            assert peers[0]["proto"]["health"] == "ok"
            assert peers[0]["proto"]["flap_score"] == 7
        finally:
            await store.close()

    asyncio.run(run())


def test_web_proto_history_reset_endpoint(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_proto_hist_reset.db")
        cfg = _mk_config(db, admin_token="adm")
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
            link_stats_fn=lambda: asyncio.sleep(0, result={}),
        )
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.history", json.dumps([{"epoch": now, "key": "pc24.flag", "from": "0", "to": "1"}]), now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.change_count", "1", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer2.history", json.dumps([{"epoch": now, "key": "pc51.value", "from": "0", "to": "1"}]), now)
        try:
            code, _, _ = await _http_request(srv, "POST", "/api/proto/history/reset?peer=peer1")
            assert code == 401

            code, _, body = await _http_request(
                srv, "POST", "/api/proto/history/reset?peer=peer1", headers={"X-Admin-Token": "adm"}
            )
            assert code == 200
            resp = json.loads(body.decode("utf-8"))
            assert resp["ok"] is True
            assert resp["removed"] >= 2
            assert resp["removed_peers"] == 1
            prefs = await store.list_user_prefs(cfg.node.node_call)
            assert "proto.peer.peer1.history" not in prefs
            assert "proto.peer.peer2.history" in prefs

            code, _, body = await _http_request(
                srv, "POST", "/api/proto/history/reset?all=1", headers={"X-Admin-Token": "adm"}
            )
            assert code == 200
            resp = json.loads(body.decode("utf-8"))
            assert resp["removed_peers"] >= 1
            prefs = await store.list_user_prefs(cfg.node.node_call)
            assert "proto.peer.peer2.history" not in prefs
        finally:
            await store.close()

    asyncio.run(run())


def test_web_users_registry_listing_and_update(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_users_registry.db")
        cfg = _mk_config(db, admin_token="adm")
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
        )
        orig = web_admin_mod.resolve_location_to_coords
        web_admin_mod.resolve_location_to_coords = lambda text: (42.3601, -71.0589) if text == "Boston" else None
        try:
            await store.set_user_pref("AI3I", "password", "pw", now)
            await store.upsert_user_registry("AI3I", now, display_name="John", qth="PA", email="ai3i@example.org", privilege="sysop")
            await store.upsert_user_registry("K1ABC", now, display_name="Alice", qth="MA", privilege="user")
            await store.upsert_user_registry("W3XYZ", now, display_name="Bob", qth="MD", privilege="user")
            await store.add_message("N0CALL", "K1ABC", now, "hello inbox", origin_node="AI3I-15", route_node="", delivery_state="delivered", delivered_epoch=now)
            await store.add_message("K1ABC", "PEERCALL", now, "hello outbox", origin_node="AI3I-15", route_node="PEER1", delivery_state="pending")
            sent_id = await store.add_message("K1ABC", "PEER404", now, "cannot route", origin_node="AI3I-15", route_node="PEER404", delivery_state="undeliverable", error_text="no configured route to peer")
            await store.set_message_delivery(sent_id, "undeliverable", route_node="PEER404", error_text="no configured route to peer")

            code, _, body = await _http_request(srv, "GET", "/api/users?limit=2&offset=0", headers={"X-Admin-Token": "adm"})
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["total"] == 3
            assert data["limit"] == 2
            assert len(data["rows"]) == 2
            assert any(row["call"] == "AI3I" and row["has_password"] is True for row in data["rows"])

            code, _, body = await _http_request(srv, "GET", "/api/users?privilege=sysop", headers={"X-Admin-Token": "adm"})
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["total"] == 1
            assert data["rows"][0]["call"] == "AI3I"

            code, _, body = await _http_request(srv, "GET", "/api/users?search=K1ABC", headers={"X-Admin-Token": "adm"})
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["rows"][0]["mail_inbox_total"] == 1
            assert data["rows"][0]["mail_inbox_unread"] == 1
            assert data["rows"][0]["mail_outbox_pending"] == 1
            assert data["rows"][0]["mail_outbox_issues"] == 1
            assert data["rows"][0]["mail_last_error"] == "no configured route to peer"
            assert data["rows"][0]["mfa_email_otp"] == "default"
            assert data["rows"][0]["principal_call"] == "K1ABC"
            assert data["rows"][0]["registration_state"] == "pending"
            assert data["rows"][0]["email_verified"] is False
            assert data["rows"][0]["grace_logins_remaining"] == 0

            await store.set_user_pref("K1ABC", "node_family", "pycluster", now)
            code, _, body = await _http_request(srv, "GET", "/api/users?clusters=1", headers={"X-Admin-Token": "adm"})
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["clusters"] is True
            assert len(data["rows"]) == 1
            assert data["rows"][0]["call"] == "K1ABC"
            assert data["rows"][0]["node_family"] == "pycluster"

            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/users",
                headers={"X-Admin-Token": "adm", "Content-Type": "application/json"},
                body=json.dumps(
                    {
                        "call": "K1ABC",
                        "display_name": "Alice Updated",
                        "qth": "Boston",
                        "email": "alice@example.org",
                        "privilege": "sysop",
                        "mfa_email_otp": "required",
                    }
                ).encode("utf-8"),
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["ok"] is True
            assert data["user"]["display_name"] == "Alice Updated"
            assert data["user"]["mfa_email_otp"] == "required"
            row = await store.get_user_registry("K1ABC")
            assert row is not None
            assert str(row["privilege"]) == "sysop"
            assert await store.get_user_pref("K1ABC", "mfa_email_otp") == "required"

            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/users/delete",
                headers={"X-Admin-Token": "adm", "Content-Type": "application/json"},
                body=json.dumps({"call": "W3XYZ"}).encode("utf-8"),
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["ok"] is True
            assert await store.get_user_registry("W3XYZ") is None
        finally:
            web_admin_mod.resolve_location_to_coords = orig
            await store.close()

    asyncio.run(run())


def test_web_admin_can_send_smtp_test_email(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_admin_mfa_test_email.db")
        cfg = _mk_config(db, admin_token="adm")
        cfg.smtp.host = "smtp.example.test"
        cfg.smtp.from_addr = "cluster@example.test"
        store = SpotStore(db)
        sent: list[tuple[str, str, str]] = []
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
        )
        srv._smtp.send_code = lambda rcpt, subject, body: sent.append((rcpt, subject, body))  # type: ignore[method-assign]
        try:
            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/mfa/test-email",
                headers={"X-Admin-Token": "adm", "Content-Type": "application/json"},
                body=json.dumps({"call": "AI3I", "email": "ai3i@example.test"}).encode("utf-8"),
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["ok"] is True
            assert data["call"] == "AI3I"
            assert data["email"] == "ai3i@example.test"
            assert sent and sent[0][0] == "ai3i@example.test"
            assert "SMTP test" in sent[0][1]

            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/node/smtp-test",
                headers={"X-Admin-Token": "adm", "Content-Type": "application/json"},
                body=json.dumps({"email": "sysop@example.test"}).encode("utf-8"),
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["ok"] is True
            assert data["call"] == cfg.node.node_call
            assert data["email"] == "sysop@example.test"
            assert sent[-1][0] == "sysop@example.test"
        finally:
            await store.close()

    asyncio.run(run())


def test_web_users_password_set_and_clear(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_users_passwords.db")
        cfg = _mk_config(db, admin_token="adm")
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
        )
        try:
            await store.upsert_user_registry("AI3I", now, privilege="sysop")

            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/users/password",
                headers={"X-Admin-Token": "adm", "Content-Type": "application/json"},
                body=json.dumps({"call": "AI3I", "password": "pw123"}).encode("utf-8"),
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["ok"] is True
            assert data["user"]["has_password"] is True
            saved = await store.get_user_pref("AI3I", "password")
            assert is_password_hash(saved)
            assert verify_password("pw123", saved)

            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/users/password/clear",
                headers={"X-Admin-Token": "adm", "Content-Type": "application/json"},
                body=json.dumps({"call": "AI3I"}).encode("utf-8"),
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["ok"] is True
            assert data["user"]["has_password"] is False
            assert await store.get_user_pref("AI3I", "password") is None
        finally:
            await store.close()

    asyncio.run(run())


def test_web_users_can_reset_mfa(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_users_reset_mfa.db")
        cfg = _mk_config(db, admin_token="adm")
        cfg.smtp.host = "smtp.example.test"
        cfg.smtp.from_addr = "cluster@example.test"
        cfg.mfa.enabled = True
        cfg.mfa.require_for_sysop = True
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
        )
        srv._mfa._sender = lambda _rcpt, _subject, _body: None  # type: ignore[assignment]
        try:
            await store.upsert_user_registry("AI3I", now, privilege="sysop", email="ai3i@example.test")
            await store.set_user_pref("AI3I", "password", "pw123", now)
            await store.set_user_pref("AI3I", "mfa_email_otp", "required", now)
            await store.set_user_pref("AI3I", "mfa_totp_secret", "JBSWY3DPEHPK3PXP", now)
            challenge_id, _expires = await srv._mfa.issue(call="AI3I", email="ai3i@example.test", purpose="sysop-web")
            assert await store.get_mfa_challenge(challenge_id) is not None

            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/users/mfa/reset",
                headers={"X-Admin-Token": "adm", "Content-Type": "application/json"},
                body=json.dumps({"call": "AI3I"}).encode("utf-8"),
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["ok"] is True
            assert data["principal"] == "AI3I"
            assert data["user"]["mfa_email_otp"] == "off"
            assert await store.get_user_pref("AI3I", "mfa_email_otp") == "off"
            assert await store.get_user_pref("AI3I", "mfa_totp_secret") is None
            assert await store.get_mfa_challenge(challenge_id) is None
        finally:
            await store.close()

    asyncio.run(run())


def test_web_users_can_enroll_totp_mfa(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_users_totp_enroll.db")
        cfg = _mk_config(db, admin_token="adm")
        cfg.mfa.issuer = "AI3I Cluster"
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
        )
        try:
            await store.upsert_user_registry("AI3I", now, privilege="sysop")
            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/users/mfa/totp/enroll",
                headers={"X-Admin-Token": "adm", "Content-Type": "application/json"},
                body=json.dumps({"call": "AI3I-7"}).encode("utf-8"),
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["ok"] is True
            assert data["principal"] == "AI3I"
            assert data["secret"]
            assert data["otpauth_uri"].startswith("otpauth://totp/")
            assert "AI3I%20Cluster" in data["otpauth_uri"]
            assert await store.get_user_pref("AI3I", "mfa_totp_secret") == data["secret"]
            assert await store.get_user_pref("AI3I", "mfa_email_otp") == "required"
            assert data["user"]["mfa_totp_enabled"] is True
        finally:
            await store.close()

    asyncio.run(run())


def test_web_users_can_send_verify_and_unlock_registration(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_users_verify_unlock.db")
        cfg = _mk_config(db, admin_token="adm")
        cfg.smtp.host = "smtp.example.test"
        cfg.smtp.from_addr = "cluster@example.test"
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        sent: list[tuple[str, str, str]] = []
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
        )
        srv._mfa._sender = lambda rcpt, subject, body: sent.append((rcpt, subject, body))  # type: ignore[assignment]
        try:
            await store.upsert_user_registry("AI3I", now, privilege="sysop", email="ai3i@example.test")
            await store.set_user_pref("AI3I", "registration_state", "locked", now)
            await store.set_user_pref("AI3I", "grace_logins_remaining", "0", now)

            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/users/verification/send",
                headers={"X-Admin-Token": "adm", "Content-Type": "application/json"},
                body=json.dumps({"call": "AI3I"}).encode("utf-8"),
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["ok"] is True
            assert data["principal"] == "AI3I"
            assert sent and sent[0][0] == "ai3i@example.test"

            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/users/verification/unlock",
                headers={"X-Admin-Token": "adm", "Content-Type": "application/json"},
                body=json.dumps({"call": "AI3I"}).encode("utf-8"),
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["ok"] is True
            assert data["user"]["registration_state"] == "pending"
            assert data["user"]["email_verified"] is False
            assert data["user"]["grace_logins_remaining"] == cfg.node.initial_grace_logins
            assert await store.get_user_pref("AI3I", "email_verified_epoch") is None
            assert await store.get_user_pref("AI3I", "registration_state") == "pending"

            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/users/verification/verify",
                headers={"X-Admin-Token": "adm", "Content-Type": "application/json"},
                body=json.dumps({"call": "AI3I"}).encode("utf-8"),
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["ok"] is True
            assert data["user"]["registration_state"] == "verified"
            assert data["user"]["email_verified"] is True
            assert await store.get_user_pref("AI3I", "registration_state") == "verified"
            assert await store.get_user_pref("AI3I", "email_verified_epoch") is not None
        finally:
            await store.close()

    asyncio.run(run())


def test_web_admin_registration_queue_can_list_approve_and_deny(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_registration_queue.db")
        cfg = _mk_config(db, admin_token="adm")
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
        )
        try:
            await store.upsert_registration_request(
                "N1NEW",
                now,
                display_name="New User",
                home_node="W1AW",
                qth="Hartford",
                qra="FN31",
                email="new@example.test",
                note="Please approve",
                source="public-web",
                email_verified=True,
                status="pending",
            )
            await store.upsert_registration_request(
                "N1DENY",
                now,
                display_name="Denied User",
                email="deny@example.test",
                source="telnet",
                email_verified=True,
                status="pending",
            )

            code, _, body = await _http_request(srv, "GET", "/api/registrations?status=pending", headers={"X-Admin-Token": "adm"})
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["total"] == 2
            assert {row["call"] for row in data["rows"]} == {"N1NEW", "N1DENY"}

            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/registrations/approve",
                headers={"X-Admin-Token": "adm", "Content-Type": "application/json"},
                body=json.dumps({"call": "N1NEW"}).encode("utf-8"),
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["ok"] is True
            assert data["user"]["email"] == "new@example.test"
            assert data["user"]["registration_state"] == "verified"
            row = await store.get_user_registry("N1NEW")
            assert row is not None
            assert str(row["display_name"]) == "New User"
            assert await store.get_user_pref("N1NEW", "email_verified_epoch") is not None
            req = await store.get_registration_request("N1NEW")
            assert req is not None
            assert str(req["status"]) == "approved"

            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/registrations/deny",
                headers={"X-Admin-Token": "adm", "Content-Type": "application/json"},
                body=json.dumps({"call": "N1DENY"}).encode("utf-8"),
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["ok"] is True
            req = await store.get_registration_request("N1DENY")
            assert req is not None
            assert str(req["status"]) == "denied"
            assert await store.get_user_registry("N1DENY") is None
        finally:
            await store.close()

    asyncio.run(run())


def test_web_users_can_rename_existing_callsign(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_users_rename.db")
        cfg = _mk_config(db, admin_token="adm")
        store = SpotStore(db)
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
        )
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.upsert_user_registry("OLD1AA", now, display_name="Alice", qth="PA", privilege="sysop")
            await store.set_user_pref("OLD1AA", "password", "pw123", now)

            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/users",
                headers={"X-Admin-Token": "adm", "Content-Type": "application/json"},
                body=json.dumps(
                    {
                        "original_call": "OLD1AA",
                        "call": "NEW1AA",
                        "display_name": "Alice",
                        "qth": "PA",
                        "privilege": "sysop",
                    }
                ).encode("utf-8"),
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["ok"] is True
            assert data["user"]["call"] == "NEW1AA"
            assert data["user"]["has_password"] is True
            assert await store.get_user_registry("OLD1AA") is None
            row = await store.get_user_registry("NEW1AA")
            assert row is not None
            assert str(row["display_name"]) == "Alice"
            saved = await store.get_user_pref("NEW1AA", "password")
            assert is_password_hash(saved)
            assert verify_password("pw123", saved)
            assert await store.get_user_pref("OLD1AA", "password") is None
        finally:
            await store.close()

    asyncio.run(run())


def test_web_users_access_matrix_applies_to_base_and_ssids(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_users_block.db")
        cfg = _mk_config(db, admin_token="adm")
        store = SpotStore(db)
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
        )
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.upsert_user_registry("ZZ1AA", now, privilege="user")
            await store.set_user_pref("ZZ1AA", "password", "pw123", now)

            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/users",
                headers={"X-Admin-Token": "adm", "Content-Type": "application/json"},
                body=json.dumps(
                    {
                        "call": "ZZ1AA",
                        "display_name": "Blocked User",
                        "privilege": "user",
                        "access": {
                            "telnet": {"login": False},
                            "web": {"login": False},
                        },
                    }
                ).encode("utf-8"),
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["user"]["access"]["telnet"]["login"] is False
            assert await store.get_user_pref("ZZ1AA", "access.telnet.login") == "off"
            assert await store.get_user_pref("ZZ1AA", "access.web.login") == "off"

            code, _, _ = await _http_request(
                srv,
                "POST",
                "/api/auth/login",
                headers={"Content-Type": "application/json"},
                body=json.dumps({"call": "ZZ1AA", "password": "pw123"}).encode("utf-8"),
            )
            assert code == 401

            code, _, _ = await _http_request(
                srv,
                "POST",
                "/api/auth/login",
                headers={"Content-Type": "application/json"},
                body=json.dumps({"call": "ZZ1AA-2", "password": ""}).encode("utf-8"),
            )
            assert code == 401
        finally:
            await store.close()

    asyncio.run(run())


def test_web_users_home_node_maps_to_homenode_pref(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_users_homenode.db")
        cfg = _mk_config(db, admin_token="adm")
        store = SpotStore(db)
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
        )
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.upsert_user_registry("AI3I", now, privilege="sysop")
            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/users",
                headers={"X-Admin-Token": "adm", "Content-Type": "application/json"},
                body=json.dumps(
                    {
                        "call": "N0TEST",
                        "display_name": "Test User",
                        "home_node": "AI3I-15",
                        "privilege": "user",
                    }
                ).encode("utf-8"),
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["user"]["home_node"] == "AI3I-15"
            assert await store.get_user_pref("N0TEST", "homenode") == "AI3I-15"
        finally:
            await store.close()

    asyncio.run(run())


def test_web_proto_summary_values(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_proto_summary.db")
        cfg = _mk_config(db, admin_token="")
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())

        async def _stats():
            return {
                "peer1": {"profile": "spider", "inbound": False, "parsed_frames": 1, "sent_frames": 1, "policy_dropped": 0},
                "peer2": {"profile": "spider", "inbound": False, "parsed_frames": 1, "sent_frames": 1, "policy_dropped": 0},
            }

        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
            link_stats_fn=_stats,
        )
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc24.call", "K1ABC", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc24.flag", "1", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.history", json.dumps([{"epoch": now, "key": "pc24.flag", "from": "0", "to": "1"}]), now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.last_epoch", str(now), now)
        try:
            code, _, _ = await _http_request(srv, "GET", "/api/proto/summary")
            assert code == 401
            code, _, body = await _http_request(srv, "GET", "/api/proto/summary", headers={"X-Admin-Token": "adm"})
            assert code == 200
            summary = json.loads(body.decode("utf-8"))
            assert summary["peers"] == 2
            assert summary["known"] == 1
            assert summary["unknown"] == 1
            assert summary["history_events"] == 1
            assert summary["history_peers"] == 1
            assert summary["latest_history_epoch"] == now
        finally:
            await store.close()

    asyncio.run(run())


def test_web_proto_events_endpoint(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_proto_events.db")
        cfg = _mk_config(db, admin_token="adm")
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
            link_stats_fn=lambda: asyncio.sleep(0, result={}),
        )
        await store.set_user_pref(
            cfg.node.node_call,
            "proto.peer.peer1.history",
            json.dumps(
                [
                    {"epoch": now - 40, "key": "pc24.flag", "from": "0", "to": "1"},
                    {"epoch": now - 20, "key": "pc51.value", "from": "0", "to": "1"},
                ]
            ),
            now,
        )
        await store.set_user_pref(
            cfg.node.node_call,
            "proto.peer.peer2.history",
            json.dumps([{"epoch": now - 10, "key": "pc50.count", "from": "64", "to": "63"}]),
            now,
        )
        try:
            code, _, _ = await _http_request(srv, "GET", "/api/proto/events")
            assert code == 401

            code, _, body = await _http_request(
                srv,
                "GET",
                "/api/proto/events?peer=peer1&key=pc51&since_mins=5&limit=10",
                headers={"X-Admin-Token": "adm"},
            )
            assert code == 200
            rows = json.loads(body.decode("utf-8"))
            assert len(rows) == 1
            assert rows[0]["peer"] == "peer1"
            assert rows[0]["key"] == "pc51.value"
        finally:
            await store.close()

    asyncio.run(run())


def test_web_proto_alerts_endpoint(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_proto_alerts.db")
        cfg = _mk_config(db, admin_token="adm")
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        old = now - 86400

        async def _stats():
            return {
                "peer1": {"profile": "spider", "inbound": False, "parsed_frames": 1, "sent_frames": 1, "policy_dropped": 0},
                "peer2": {"profile": "spider", "inbound": False, "parsed_frames": 1, "sent_frames": 1, "policy_dropped": 0},
                "peer3": {"profile": "spider", "inbound": False, "parsed_frames": 1, "sent_frames": 1, "policy_dropped": 0},
            }

        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
            link_stats_fn=_stats,
        )
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc24.call", "K1ABC", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc24.flag", "0", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.last_epoch", str(now), now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer2.pc24.call", "K2ABC", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer2.pc24.flag", "1", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer2.flap_score", "7", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer2.last_epoch", str(now), now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer2.last_change_epoch", str(now), now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer3.pc24.call", "K3ABC", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer3.pc24.flag", "1", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer3.last_epoch", str(old), now)
        try:
            code, _, _ = await _http_request(srv, "GET", "/api/proto/alerts")
            assert code == 401

            code, _, _ = await _http_request(srv, "GET", "/api/proto/acks")
            assert code == 401

            code, _, body = await _http_request(srv, "GET", "/api/proto/alerts", headers={"X-Admin-Token": "adm"})
            assert code == 200
            rows = json.loads(body.decode("utf-8"))
            assert len(rows) == 2
            h = {r["peer"]: r["health"] for r in rows}
            assert "peer1" not in h
            assert h["peer2"] == "flapping"
            assert h["peer3"] == "stale"

            code, _, body = await _http_request(
                srv, "POST", "/api/proto/alerts/ack?peer=peer1", headers={"X-Admin-Token": "adm"}
            )
            assert code == 200
            resp = json.loads(body.decode("utf-8"))
            assert resp["ok"] is True
            assert resp["acked_peers"] >= 1

            code, _, body = await _http_request(srv, "GET", "/api/proto/acks", headers={"X-Admin-Token": "adm"})
            assert code == 200
            rows = json.loads(body.decode("utf-8"))
            p1 = [r for r in rows if r["peer"] == "peer1"]
            assert p1 and p1[0]["ack_epoch"] > 0 and p1[0]["suppressed"] is True

            code, _, body = await _http_request(srv, "GET", "/api/proto/alerts", headers={"X-Admin-Token": "adm"})
            assert code == 200
            rows = json.loads(body.decode("utf-8"))
            assert all(r["peer"] != "peer1" for r in rows)

            code, _, body = await _http_request(
                srv, "GET", "/api/proto/alerts?include_acked=1", headers={"X-Admin-Token": "adm"}
            )
            assert code == 200
            rows = json.loads(body.decode("utf-8"))
            p1 = [r for r in rows if r["peer"] == "peer1"]
            assert p1 and p1[0]["suppressed"] is True and p1[0]["health"] == "acked"

            code, _, body = await _http_request(
                srv, "GET", "/api/proto/alerts?stale_mins=0", headers={"X-Admin-Token": "adm"}
            )
            assert code == 200
            rows = json.loads(body.decode("utf-8"))
            assert all(r["peer"] != "peer1" for r in rows)

            code, _, body = await _http_request(
                srv, "POST", "/api/proto/alerts/unack?peer=peer1", headers={"X-Admin-Token": "adm"}
            )
            assert code == 200
            assert json.loads(body.decode("utf-8"))["removed"] >= 1
        finally:
            await store.close()

    asyncio.run(run())


def test_web_protocol_page_focuses_on_alerts_and_history(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_proto_page.db")
        cfg = _mk_config(db, admin_token="adm")
        store = SpotStore(db)
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
        )
        try:
            code, headers, body = await _http_request(srv, "GET", "/")
            assert code == 200
            assert headers.get("content-type", "").startswith("text/html")
            html = body.decode("utf-8")
            assert "Protocol Health" in html
            assert "Protocol Alerts" in html
            assert "Protocol History" in html
            assert 'id="protoPeers"' in html
            assert 'id="protoAlertSummary"' in html
            assert "Policy Drops" in html
            assert "Loading policy drops..." in html
            assert "j('/api/proto/summary')" in html
            assert "j('/api/policydrop' + (peer ? '?peer=' + peer : ''))" in html
        finally:
            await store.close()

    asyncio.run(run())


def test_api_spots_accepts_wpxloc_recognized_call_when_cty_is_missing(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_spot_review_wpx.db")
        cfg = _mk_config(db, admin_token="adm")
        cfg.public_web.wpxloc_raw_path = str(tmp_path / "wpxloc.raw")
        Path(cfg.public_web.wpxloc_raw_path).write_text(
            "UA European-Russia 054 29 16 -3.0 55 45 0 N 37 37 0 E @\n& =RG65SM\n",
            encoding="ascii",
        )
        store = SpotStore(db)
        srv = WebAdminServer(
            config=cfg,
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
        )
        now = int(datetime.now(timezone.utc).timestamp())
        orig_lookup = web_admin_mod.cty_lookup
        orig_loaded = web_admin_mod.cty_loaded
        orig_wpx_lookup = web_admin_mod.wpx_lookup
        orig_wpx_loaded = web_admin_mod.wpx_loaded
        web_admin_mod.cty_lookup = lambda call: None
        web_admin_mod.cty_loaded = lambda: False
        web_admin_mod.wpx_lookup = lambda call: object() if call == "RG65SM" else None
        web_admin_mod.wpx_loaded = lambda: True
        try:
            await store.add_spot(Spot(7168.0, "RG65SM", now, "CQ", "F8DRA", "PEER2", ""))
            code, _, body = await _http_request(srv, "GET", "/api/spots?limit=10", headers={"X-Admin-Token": "adm"})
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            review = data[0]["dx_review"]
            assert review["suspicious"] is False
            assert review["reasons"] == []
            assert review["advisory"] == []
        finally:
            web_admin_mod.cty_lookup = orig_lookup
            web_admin_mod.cty_loaded = orig_loaded
            web_admin_mod.wpx_lookup = orig_wpx_lookup
            web_admin_mod.wpx_loaded = orig_wpx_loaded
            await store.close()

    asyncio.run(run())
