"""Regressions for the hardening pass on auth, the HTTP front ends and deploy units."""

from __future__ import annotations

import ast
import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path

from pycluster.auth import hash_password
from pycluster.mfa import matched_totp_counter, totp_code, verify_totp_once
from pycluster.store import SpotStore
from pycluster.web_admin import WebAdminServer

from test_web_admin import _http_request, _mk_config


ROOT = Path(__file__).resolve().parents[1]


# --- deploy: the root-run upgrade worker must not import from the service-account tree ---


def test_upgrade_unit_pythonpath_is_not_the_service_account_tree() -> None:
    unit = (ROOT / "deploy" / "systemd" / "pycluster-upgrade.service").read_text(encoding="utf-8")

    # This unit has no User=, so it runs as root. PYTHONPATH entries precede
    # lib-dynload on sys.path, so pointing it at the pycluster-writable app dir
    # would let that account land a grp.py/fcntl.py and execute it as root.
    assert "User=" not in unit
    assert "Environment=PYTHONPATH=/usr/src/pyCluster/src" in unit
    assert "PYTHONPATH=/home/pycluster" not in unit
    assert "ExecStartPre=/usr/local/libexec/pycluster-check-upgrade-source /usr/src/pyCluster" in unit


def test_only_the_upgrade_unit_runs_without_a_user() -> None:
    for path in sorted((ROOT / "deploy" / "systemd").glob("*.service")):
        unit = path.read_text(encoding="utf-8")
        if path.name == "pycluster-upgrade.service":
            continue
        assert "User=pycluster" in unit, path.name


# --- web_admin: SYSOP must not be exempt from having a password ---


def test_sysop_login_rejected_when_no_password_is_seeded(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "nopw.sqlite3")
        store = SpotStore(db)
        srv = WebAdminServer(
            config=_mk_config(db),
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
        )
        try:
            assert await store.get_user_pref("SYSOP", "password") is None
            code, _hdrs, body = await _http_request(
                srv,
                "POST",
                "/api/auth/login",
                headers={"Content-Type": "application/json"},
                body=json.dumps({"call": "SYSOP", "password": "anything-at-all"}).encode("utf-8"),
            )
            assert code == 403, body
            assert b"password setup required" in body
            assert b"token" not in body
        finally:
            await store.close()

    asyncio.run(run())


def test_sysop_login_rejects_a_wrong_password(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "badpw.sqlite3")
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        srv = WebAdminServer(
            config=_mk_config(db),
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
        )
        try:
            await store.upsert_user_registry("SYSOP", now, privilege="sysop")
            await store.set_user_pref("SYSOP", "password", hash_password("correct-horse"), now)

            code, _hdrs, body = await _http_request(
                srv,
                "POST",
                "/api/auth/login",
                headers={"Content-Type": "application/json"},
                body=json.dumps({"call": "SYSOP", "password": "wrong"}).encode("utf-8"),
            )
            assert code == 401, body

            code, _hdrs, body = await _http_request(
                srv,
                "POST",
                "/api/auth/login",
                headers={"Content-Type": "application/json"},
                body=json.dumps({"call": "SYSOP", "password": "correct-horse"}).encode("utf-8"),
            )
            assert code == 200, body
            payload = json.loads(body.decode("utf-8"))
            assert payload["sysop"] is True
        finally:
            await store.close()

    asyncio.run(run())


# --- web_admin: request head/body limits ---


def test_web_admin_body_is_capped(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "cap.sqlite3")
        store = SpotStore(db)
        srv = WebAdminServer(
            config=_mk_config(db),
            store=store,
            started_at=datetime.now(timezone.utc),
            session_count_fn=lambda: 0,
        )
        try:
            reader = asyncio.StreamReader()

            class _W:
                def __init__(self) -> None:
                    self.buf = bytearray()

                def write(self, data: bytes) -> None:
                    self.buf.extend(data)

                async def drain(self) -> None:
                    return

                def get_extra_info(self, _name: str, _default=None):
                    return None

                def close(self) -> None:
                    return

                async def wait_closed(self) -> None:
                    return

            writer = _W()
            # Claims 8 MB but sends 1 KB; the server must not sit waiting for
            # the difference, and must not try to buffer 8 MB.
            reader.feed_data(
                b"POST /api/auth/login HTTP/1.1\r\n"
                b"Content-Type: application/json\r\n"
                b"Content-Length: 8388608\r\n\r\n" + b"x" * 1024
            )
            reader.feed_eof()
            await srv._handle(reader, writer)  # type: ignore[arg-type]
            assert writer.buf.startswith(b"HTTP/1.1 413 Content Too Large")
            assert len(writer.buf) < 64 * 1024
        finally:
            await store.close()

    asyncio.run(run())


def test_public_web_clamps_content_length() -> None:
    from pycluster.httputil import (
        MAX_REQUEST_BODY_BYTES,
        RequestBodyTooLarge,
        clamp_content_length,
        request_content_length,
    )

    assert clamp_content_length("100") == 100
    assert clamp_content_length("8388608") == MAX_REQUEST_BODY_BYTES
    assert clamp_content_length("-5") == 0
    assert clamp_content_length("not-a-number") == 0
    assert clamp_content_length(None) == 0
    assert request_content_length("100") == 100
    try:
        request_content_length(str(MAX_REQUEST_BODY_BYTES + 1))
    except RequestBodyTooLarge:
        pass
    else:
        raise AssertionError("oversized request was not rejected")
    for invalid in ("-1", "not-a-number"):
        try:
            request_content_length(invalid)
        except ValueError:
            pass
        else:
            raise AssertionError(f"invalid Content-Length accepted: {invalid}")


# --- MFA: a TOTP code must not be replayable inside its window ---


def test_totp_code_cannot_be_replayed(tmp_path) -> None:
    async def run() -> None:
        store = SpotStore(str(tmp_path / "totp.sqlite3"))
        try:
            secret = "JBSWY3DPEHPK3PXPJBSWY3DPEHPK3PXP"
            now = 1_700_000_000
            code = totp_code(secret, now=now)

            assert await verify_totp_once(store, "AI3I", secret, code, now=now) is True
            # Same code, same window: must now be refused.
            assert await verify_totp_once(store, "AI3I", secret, code, now=now) is False
            # A later window still works.
            later = now + 60
            assert await verify_totp_once(store, "AI3I", secret, totp_code(secret, now=later), now=later) is True
        finally:
            await store.close()

    asyncio.run(run())


def test_totp_replay_consumption_is_atomic_and_secret_scoped(tmp_path) -> None:
    async def run() -> None:
        store = SpotStore(str(tmp_path / "totp-atomic.sqlite3"))
        try:
            first = "JBSWY3DPEHPK3PXPJBSWY3DPEHPK3PXP"
            second = "KRSXG5DSNFXGOIDBNZSCA5DINFZSAYJA"
            now = 1_700_000_000
            outcomes = await asyncio.gather(*(
                verify_totp_once(store, "AI3I", first, totp_code(first, now=now), now=now)
                for _ in range(8)
            ))
            assert outcomes.count(True) == 1
            assert outcomes.count(False) == 7
            # Re-enrollment with a different secret in the same time step is valid.
            assert await verify_totp_once(
                store, "AI3I", second, totp_code(second, now=now), now=now
            ) is True
        finally:
            await store.close()

    asyncio.run(run())


def test_matched_totp_counter_reports_the_step() -> None:
    secret = "JBSWY3DPEHPK3PXPJBSWY3DPEHPK3PXP"
    now = 1_700_000_000
    assert matched_totp_counter(secret, totp_code(secret, now=now), now=now) == now // 30
    assert matched_totp_counter(secret, "000000", now=now, window=0) is None


# --- static invariants that the earlier bugs violated ---


def test_web_admin_imports_every_name_it_uses() -> None:
    """`subprocess` was used by the fail2ban reader but never imported.

    The NameError was swallowed by a bare `except Exception`, so the ban table
    silently rendered empty in production while tests monkeypatched it away.
    """
    source = (ROOT / "src" / "pycluster" / "web_admin.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imported.add((alias.asname or alias.name).split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                imported.add(alias.asname or alias.name)
    assert "subprocess" in imported


def test_sysop_console_escapes_quotes_in_html_attributes() -> None:
    """esc() feeds double-quoted attributes, including peer-supplied values."""
    source = (ROOT / "src" / "pycluster" / "web_admin.py").read_text(encoding="utf-8")
    start = source.index("function esc(v) {")
    body = source[start : source.index("}", start)]
    assert "&quot;" in body
    assert "&#39;" in body


def test_public_ui_escapes_quotes_in_html_attributes() -> None:
    source = (ROOT / "web" / "public_dxweb" / "static" / "index.html").read_text(encoding="utf-8")
    start = source.index("function esc(s)")
    body = source[start : source.index("\n", start)]
    assert "&quot;" in body
    assert "&#39;" in body


def test_no_duplicate_method_definitions_in_source() -> None:
    for path in sorted((ROOT / "src" / "pycluster").glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))

        def scan(node, where: str) -> None:
            seen: dict[str, int] = {}
            for child in node.body:
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                    assert child.name not in seen, f"{path.name}:{where}.{child.name} redefined at line {child.lineno}"
                    seen[child.name] = child.lineno
                    if isinstance(child, ast.ClassDef):
                        scan(child, f"{where}.{child.name}")

        scan(tree, path.stem)


def test_store_sets_a_busy_timeout() -> None:
    source = (ROOT / "src" / "pycluster" / "store.py").read_text(encoding="utf-8")
    assert "PRAGMA busy_timeout" in source


def test_store_busy_timeout_is_applied(tmp_path) -> None:
    store = SpotStore(str(tmp_path / "busy.sqlite3"))
    try:
        row = store._conn.execute("PRAGMA busy_timeout").fetchone()
        assert int(row[0]) >= 1000
    finally:
        asyncio.run(store.close())
