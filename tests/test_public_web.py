from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
import logging
from pathlib import Path

from pycluster.config import AppConfig, NodeConfig, PublicWebConfig, StoreConfig, TelnetConfig, WebConfig
from pycluster import __version__
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
            assert any(node["call"] == "W3LPL-2" for node in net["nodes"])
            assert any(node["call"] == "AI3I-16" and node["inbound"] is True for node in net["nodes"])
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


def test_public_web_login_failure_logs_structured_authfail(tmp_path, caplog) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_authfail.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        srv = PublicWebServer(cfg, store, datetime.now(timezone.utc))
        try:
            await store.upsert_user_registry("AI3I", now, privilege="user")
            await store.set_user_pref("AI3I", "password", "correct", now)
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
        await store.upsert_user_registry("AI3I", now, privilege="user")
        await store.set_user_pref("AI3I", "password", "secret", now)
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
        finally:
            await store.close()

    asyncio.run(run())


def test_public_web_access_policy_controls_login_and_posting(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_access.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry("AI3I", now, privilege="user")
        await store.set_user_pref("AI3I", "password", "secret", now)
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


def test_public_web_non_authenticated_users_are_read_only_by_default(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_access_default.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry("AI3I", now, privilege="")
        await store.set_user_pref("AI3I", "password", "secret", now)
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


def test_public_web_blocked_login_is_denied(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "public_blocked_login.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry("AI3I", now, privilege="user")
        await store.set_user_pref("AI3I", "password", "secret", now)
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
        await store.upsert_user_registry("AI3I", now, privilege="user")
        await store.set_user_pref("AI3I", "password", "secret", now)
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
