from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import tomllib

import pycluster.web_admin as web_admin_mod
from pycluster.auth import is_password_hash, verify_password
from pycluster.config import AppConfig, NodeConfig, PublicWebConfig, StoreConfig, TelnetConfig, WebConfig
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
        await store.upsert_user_registry("N0CALL", now, privilege="user")
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

        async def _connect(peer: str, dsn: str, profile: str = "dxspider") -> None:
            ops.append(("connect", peer, dsn, profile))

        async def _disconnect(peer: str) -> bool:
            ops.append(("disconnect", peer, ""))
            return peer == "peer1"

        async def _set_profile(peer: str, profile: str) -> bool:
            ops.append(("profile", peer, profile))
            return peer == "peer1"

        async def _save(peer: str, dsn: str, profile: str = "dxspider", reconnect: bool = True) -> None:
            ops.append(("save", peer, dsn, profile, reconnect))

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
        )
        try:
            code, _, _ = await _http_request(srv, "GET", "/api/stats")
            assert code == 401

            now = int(datetime.now(timezone.utc).timestamp())
            await store.set_user_pref("K1SYS", "password", "pw2", now)
            await store.upsert_user_registry("K1SYS", now, privilege="sysop")
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
                body=json.dumps({"peer": "peer1", "dsn": "tcp://127.0.0.1:7300", "profile": "dxspider"}).encode("utf-8"),
            )
            assert code == 200
            assert json.loads(body.decode("utf-8"))["ok"] is True

            code, _, body = await _http_request(
                srv,
                "POST",
                "/api/peer/connect",
                headers={"X-Admin-Token": "adm", "Content-Type": "application/json"},
                body=json.dumps({"peer": "peer1", "dsn": "tcp://127.0.0.1:7300", "profile": "dxspider"}).encode("utf-8"),
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
            assert ("save", "peer1", "tcp://127.0.0.1:7300", "dxspider", True) in ops
            assert ("connect", "peer1", "tcp://127.0.0.1:7300", "dxspider") in ops
            assert ("profile", "peer1", "arcluster") in ops
            assert ("disconnect", "peer1", "") in ops
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
            await store.upsert_user_registry("N0CALL", now, privilege="user")
            await store.upsert_user_registry("K1SYS", now, privilege="sysop")

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
            await store.upsert_user_registry("K1SYS", now, privilege="sysop")
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
            assert await store.count_spots() == 0
        finally:
            await store.close()

    asyncio.run(run())


def test_api_peers_includes_desired_reconnect_state(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "web_peer_desired.db")
        cfg = _mk_config(db, admin_token="adm")
        store = SpotStore(db)

        async def _stats():
            return {}

        async def _desired():
            return [
                {
                    "peer": "AI3I-15",
                    "dsn": "dxspider://dxspider.ai3i.net:7300?login=AI3I-16&client=AI3I-15",
                    "profile": "spider",
                    "reconnect_enabled": True,
                    "retry_count": 2,
                    "next_retry_epoch": 1773275000,
                    "last_connect_epoch": 1773274000,
                    "last_error": "timed out",
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
        try:
            code, _, body = await _http_request(srv, "GET", "/api/peers", headers={"X-Admin-Token": "adm"})
            assert code == 200
            rows = json.loads(body.decode("utf-8"))
            assert len(rows) == 1
            row = rows[0]
            assert row["peer"] == "AI3I-15"
            assert row["desired"] is True
            assert row["connected"] is False
            assert row["reconnect_enabled"] is True
            assert row["retry_count"] == 2
            assert row["last_error"] == "timed out"
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
            assert saved["node"]["prompt_template"] == "[{timestamp}] {node}{suffix}"
            assert saved["telnet"]["ports"] == [7300, 7373, 8000]
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
            await store.upsert_user_registry("AI3I", now, privilege="sysop")
            await store.set_user_pref("AI3I", "password", "correct", now)
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
        await store.upsert_user_registry("N0CALL", now, privilege="user")
        code, _, body = await _http_request(
            srv,
            "POST",
            "/api/auth/login",
            headers={"Content-Type": "application/json"},
            body=json.dumps({"call": "N0CALL", "password": ""}).encode("utf-8"),
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
                    }
                ).encode("utf-8"),
            )
            assert code == 200
            data = json.loads(body.decode("utf-8"))
            assert data["ok"] is True
            assert data["user"]["display_name"] == "Alice Updated"
            row = await store.get_user_registry("K1ABC")
            assert row is not None
            assert str(row["privilege"]) == "sysop"

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
