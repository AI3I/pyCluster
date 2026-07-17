from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import pytest

from pycluster import __version__
from pycluster import telnet_server as telnet_server_mod
from pycluster.auth import is_password_hash, verify_password
from pycluster.config import AppConfig, NodeConfig, PublicWebConfig, StoreConfig, TelnetConfig, WebConfig
from pycluster.ctydat import load_cty, lookup
from pycluster.wpxloc import load_wpxloc, lookup as wpx_lookup
from pycluster.models import Spot
from pycluster.mfa import totp_code
from pycluster.registration import mark_email_verified
from pycluster.telnet_server import Session, TelnetClusterServer
from pycluster.store import SpotStore

_SAMPLE_TLE = (
    "ISS (ZARYA)\n"
    "1 25544U 98067A   24100.50000000  .00016717  00000+0  10270-3 0  9000\n"
    "2 25544  51.6400 120.0000 0005000  40.0000 320.0000 15.50000000  9000\n"
)


class _DummyWriter:
    def __init__(self) -> None:
        self.buffer = bytearray()

    def write(self, _b: bytes) -> None:
        self.buffer.extend(_b)

    async def drain(self) -> None:
        return



def _mk_config(tmp_db: str) -> AppConfig:
    return AppConfig(
        node=NodeConfig(),
        telnet=TelnetConfig(),
        web=WebConfig(),
        public_web=PublicWebConfig(),
        store=StoreConfig(sqlite_path=tmp_db),
    )


def _write_cty(tmp_path: Path) -> str:
    path = tmp_path / "cty.dat"
    path.write_text(
        "United States: 5: 8: NA: 37.0: 95.0: 5.0: K:\n"
        " K, N, W, =K1ABC;\n"
        "Hawaii: 31: 61: OC: 21.0: 157.0: 10.0: KH6:\n"
        " KH6;\n"
        "Canada: 5: 9: NA: 45.0: 73.0: 5.0: VE:\n"
        " VE, =VE3XYZ;\n"
        "Japan: 25: 45: AS: 35.0: 139.0: -9.0: JA:\n"
        " JA, 7K;\n",
        encoding="ascii",
    )
    return str(path)




def _write_wpxloc(tmp_path: Path) -> str:
    path = tmp_path / "wpxloc.raw"
    path.write_text(
        "K United-States 291 5 8 5.0 37 0 0 N 95 0 0 W @\n"
        "& =K1ABC\n"
        "UA European-Russia 054 29 16 -3.0 55 45 0 N 37 37 0 E @\n"
        "& =RG65SM =RG65SA\n",
        encoding="ascii",
    )
    return str(path)

def test_dispatch_show_and_aliases(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "cmd.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))

        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )

        try:
            keep, out = await srv._execute_command("N0CALL", "sh/version")
            assert keep is True
            assert f"pyCluster version {__version__}" in out
            assert "John D. Lewis (AI3I)" in out
            assert "https://github.com/AI3I/pyCluster" in out

            keep, out = await srv._execute_command("N0CALL", "show clu")
            assert keep is True
            assert "local /" in out and "Uptime" in out

            keep, out = await srv._execute_command("N0CALL", "users")
            assert keep is True
            assert "N0CALL" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_muf_path_report_defaults_to_forward_hourly_rows(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "muf_path_forward_hours.db")
        cty_path = _write_cty(tmp_path)
        cfg = AppConfig(
            node=NodeConfig(node_call="AI3I-16", node_locator="FN20"),
            telnet=TelnetConfig(),
            web=WebConfig(),
            public_web=PublicWebConfig(cty_dat_path=cty_path),
            store=StoreConfig(sqlite_path=db),
        )
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.add_bulletin("wwv", "WWV", "LOCAL", now - 7200, "SFI=150 A=6 K=2 Quiet")
            _, out = await srv._execute_command("N0CALL", "show/muf K")
            rows = [
                line for line in out.splitlines()
                if len(line.split()) >= 4
                and line.split()[0].isdigit()
                and line.split()[1].isdigit()
                and "." in line.split()[2]
            ]
            assert len(rows) == 12
            current_hour = datetime.now(timezone.utc).hour
            hours = [int(line.split()[0]) for line in rows[:4]]
            assert hours == [(current_hour + idx) % 24 for idx in range(4)]
        finally:
            await store.close()

    asyncio.run(run())


def test_show_commands_uses_hot_reloaded_strings_catalog(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "strings_cmd.db")
        cfg = _mk_config(db)
        strings_path = tmp_path / "strings.toml"
        strings_path.write_text(
            "[commands.show.sections]\nshow = \"Inspect commands:\"\n\n[commands.summary]\nshow_wcy = \"Read WCY bulletins from the catalog.\"\n",
            encoding="utf-8",
        )
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc), strings_path=str(strings_path))
        try:
            _, out = await srv._execute_command("N0CALL", "show/commands")
            assert "Inspect commands:" in out
            assert "Read WCY bulletins from the catalog." in out

            strings_path.write_text(
                "[commands.show.sections]\nshow = \"Display commands:\"\n\n[commands.summary]\nshow_wcy = \"WCY from reloaded catalog.\"\n",
                encoding="utf-8",
            )
            _, out = await srv._execute_command("N0CALL", "show/commands")
            assert "Display commands:" in out
            assert "WCY from reloaded catalog." in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_wcy_and_wwv_use_template_catalog(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "strings_events.db")
        cfg = _mk_config(db)
        strings_path = tmp_path / "strings.toml"
        strings_path.write_text(
            "[show.wcy]\nheader = \"WCY TABLE\"\nempty = \"No WCY bulletins stored.\"\n\n[show.wwv]\nheader = \"WWV TABLE\"\nempty = \"No WWV bulletins stored.\"\n",
            encoding="utf-8",
        )
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc), strings_path=str(strings_path))
        try:
            _, out = await srv._execute_command("N0CALL", "show/wcy")
            assert "No WCY bulletins stored." in out
            _, out = await srv._execute_command("N0CALL", "show/wwv")
            assert "No WWV bulletins stored." in out

            now = int(datetime.now(timezone.utc).timestamp())
            await store.add_bulletin("wcy", "DK0WCY", "LOCAL", now, "SFI=120 A=4 K=2 spots=33 expk=3 aurora=LOW xray=QUIET storm=NONE")
            await store.add_bulletin("wwv", "WWV", "LOCAL", now, "SFI=121 A=5 K=3 Solar flux rising slowly")
            _, out = await srv._execute_command("N0CALL", "show/wcy")
            assert out.startswith("WCY TABLE\r\n")
            _, out = await srv._execute_command("N0CALL", "show/wwv")
            assert out.startswith("\r\nWWV TABLE\r\n")
        finally:
            await store.close()

    asyncio.run(run())


def test_welcome_block_uses_template_catalog(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "strings_welcome.db")
        cfg = _mk_config(db)
        cfg.node.node_call = "AI3I-15"
        cfg.node.qth = "Western Pennsylvania"
        cfg.node.welcome_title = "Welcome"
        cfg.node.website_url = "https://example.org"
        cfg.node.support_contact = "ops@example.org"
        strings_path = tmp_path / "strings.toml"
        strings_path.write_text(
            "[welcome.greeting]\nnamed = \"Hello {call} from {title}.\"\n\n[welcome]\nconnected = \"Connected to {node_call} in {qth}.\"\nstatus = \"Status: {local_users} users, uptime {uptime}\"\nwebsite = \"Site: {website}\"\ncontact = \"Help: {support}\"\n\n[welcome.motd]\ndivider = \"----\"\n",
            encoding="utf-8",
        )
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc), strings_path=str(strings_path))
        try:
            out = await srv._welcome_block("N0CALL")
            assert "Hello N0CALL from Welcome." in out
            assert "Connected to AI3I-15 in Western Pennsylvania." in out
            assert "Site: https://example.org" in out
            assert "Help: ops@example.org" in out
            assert "Status: 0 users, uptime" in out
            assert "----" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_prompt_uses_configured_node_call_only(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "prompt.db")
        cfg = _mk_config(db)
        cfg.node.node_call = "AI3I-15"
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        try:
            prompt = await srv._prompt("N0CALL")
            assert prompt.startswith("[") and "] AI3I-15> " in prompt and prompt.endswith("> ")
            await store.set_user_pref(cfg.node.node_call, "node_call", "AI3I-7", int(datetime.now(timezone.utc).timestamp()))
            prompt = await srv._prompt("N0CALL")
            assert prompt.startswith("[") and "] AI3I-15> " in prompt and prompt.endswith("> ")
        finally:
            await store.close()

    asyncio.run(run())


def test_sysop_prompt_uses_hash_suffix(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "prompt_sysop.db")
        cfg = _mk_config(db)
        cfg.node.node_call = "AI3I-16"
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.upsert_user_registry("AI3I", now, privilege="sysop")
            prompt_sysop = await srv._prompt("AI3I")
            assert prompt_sysop.startswith("[") and "] AI3I-16# " in prompt_sysop and prompt_sysop.endswith("# ")
            prompt_user = await srv._prompt("N0CALL")
            assert prompt_user.startswith("[") and "] AI3I-16> " in prompt_user and prompt_user.endswith("> ")
        finally:
            await store.close()

    asyncio.run(run())


def test_prompt_supports_callsign_token(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "prompt_callsign.db")
        cfg = _mk_config(db)
        cfg.node.node_call = "AI3I-15"
        cfg.node.prompt_template = "[{timestamp}] {callsign}@{node}{suffix}"
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        try:
            prompt = await srv._prompt("K1ABC")
            assert "] K1ABC@AI3I-15> " in prompt
        finally:
            await store.close()

    asyncio.run(run())


def test_write_prompt_for_session_starts_on_new_line_after_async_output(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "prompt_async_line.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        writer = _DummyWriter()
        sess = Session(
            call="N0CALL",
            writer=writer,
            connected_at=datetime.now(timezone.utc),
            async_line_open=True,
        )
        try:
            await srv._write_prompt_for_session(sess)
            rendered = writer.buffer.decode("utf-8", errors="replace")
            assert rendered.startswith("\r\n[")
            assert sess.async_line_open is False
        finally:
            await store.close()

    asyncio.run(run())


def test_execute_blank_line_does_not_emit_prompt_output(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "blank_prompt.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        try:
            keep, out = await srv._execute_command("N0CALL", "")
            assert keep is True
            assert out == ""
        finally:
            await store.close()

    asyncio.run(run())


def test_bridge_node_login_promotes_client_handshake(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "bridge_node_login.db")
        cfg = _mk_config(db)
        cfg.node.node_call = "AI3I-15"
        store = SpotStore(db)
        seen: list[tuple[str, str, list[str] | None]] = []
        try:
            async def _node_login(
                call: str,
                peer_name: str,
                _reader: asyncio.StreamReader,
                _writer,
                initial_lines: list[str] | None,
            ) -> bool:
                seen.append((call, peer_name, initial_lines))
                return True

            srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc), on_node_login_fn=_node_login)
            writer = _DummyWriter()

            async def _fake_readline(_reader: asyncio.StreamReader) -> str | None:
                return "client AI3I-15 telnet"

            srv._readline = _fake_readline  # type: ignore[method-assign]
            ok = await srv._bridge_node_login("AI3I-16", asyncio.StreamReader(), writer)  # type: ignore[arg-type]

            text = writer.buffer.decode("utf-8", errors="replace")
            assert ok is True
            assert seen == [("AI3I-16", "AI3I-15", None)]
            assert "Hello AI3I-16" in text
            assert "AI3I-15> " in text
        finally:
            await store.close()

    asyncio.run(run())


def test_welcome_block_uses_node_presentation_settings(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "welcome.db")
        cfg = _mk_config(db)
        cfg.node.node_call = "AI3I-15"
        cfg.node.qth = "Western Pennsylvania"
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        now = int(datetime.now(timezone.utc).timestamp())
        await store.set_user_pref(cfg.node.node_call, "branding_name", "pyCluster", now)
        await store.set_user_pref(cfg.node.node_call, "welcome_title", "Welcome back", now)
        await store.set_user_pref(cfg.node.node_call, "welcome_body", "Friendly DX for everyone.", now)
        await store.set_user_pref(cfg.node.node_call, "support_contact", "dxcluster@ai3i.net", now)
        await store.set_user_pref(cfg.node.node_call, "website_url", "https://github.com/AI3I/pyCluster", now)
        await store.set_user_pref(cfg.node.node_call, "motd", "Be kind. Spot accurately.", now)
        try:
            out = await srv._welcome_block("N0CALL")
            assert "Welcome back, N0CALL." in out
            assert "You're connected to AI3I-15, a pyCluster node in Western Pennsylvania." in out
            assert "Friendly DX for everyone." in out
            assert "Web: https://github.com/AI3I/pyCluster" in out
            assert "Contact: dxcluster@ai3i.net" in out
            assert "Be kind. Spot accurately." in out
            assert "Cluster status:" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_motd_prefers_node_presentation_override(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "motd_override.db")
        cfg = _mk_config(db)
        cfg.node.motd = "Config MOTD"
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        now = int(datetime.now(timezone.utc).timestamp())
        await store.set_user_pref(cfg.node.node_call, "motd", "Database MOTD", now)
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            _, out = await srv._execute_command("N0CALL", "show/motd")
            assert "Database MOTD" in out
            assert "Config MOTD" not in out
        finally:
            await store.close()

    asyncio.run(run())


def test_set_unset_flags(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "set.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))

        sess = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        srv._sessions[1] = sess

        try:
            _, out = await srv._execute_command("N0CALL", "unset/echo")
            assert "Echo set to off for N0CALL." in out
            assert sess.echo is False

            _, out = await srv._execute_command("N0CALL", "set/language de")
            assert "Language set to de" in out
            assert sess.language == "de"

            _, out = await srv._execute_command("N0CALL", "set/here")
            assert "Here set to on for N0CALL." in out
            assert sess.here is True

            _, out = await srv._execute_command("N0CALL", "set/arcluster")
            assert "Profile for N0CALL set to arcluster." in out
            assert sess.peer_profile == "arcluster"

            _, out = await srv._execute_command("N0CALL", "unset/arcluster")
            assert "Profile for N0CALL set to spider." in out
            assert sess.peer_profile == "spider"
        finally:
            await store.close()

    asyncio.run(run())


def test_set_maxconnect_command(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "maxc.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.upsert_user_registry("N0CALL", now, privilege="sysop")
            _, out = await srv._execute_command("N0CALL", "set/maxconnect 2")
            assert "Maximum connections for N0CALL set to 2." in out
            assert await store.get_user_pref("N0CALL", "maxconnect") == "2"

            _, out = await srv._execute_command("N0CALL", "show/users")
            assert "privilege sysop" in out
            assert "max" in out and "connections 2" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_command_case_insensitive_and_abbrev_dispatch(tmp_path, monkeypatch) -> None:
    async def run() -> None:
        db = str(tmp_path / "abbrev.db")
        cfg = _mk_config(db)
        cfg.satellite.keps_path = str(tmp_path / "abbrev-keps.txt")
        monkeypatch.setattr(telnet_server_mod, "_download_text_url", lambda _url: _SAMPLE_TLE)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv._execute_command("N0CALL", "sHoW mOtD")
            assert out.strip() != ""

            _, out = await srv._execute_command("N0CALL", "Se/Ta")
            assert "Talk set to on for N0CALL." in out
            _, out = await srv._execute_command("N0CALL", "uNsE/tA")
            assert "Talk set to off for N0CALL." in out

            _, out = await srv._execute_command("N0CALL", "Sh/Dx")
            assert "No spots available" in out

            _, out = await srv._execute_command("N0CALL", "gE/KeP")
            assert "Keplerian elements request accepted." in out

            _, out = await srv._execute_command("N0CALL", "Sh/PrOtO-AcKs")
            assert "No protocol alert acknowledgements." in out
            _, out = await srv._execute_command("N0CALL", "SH/PRACK")
            assert "No protocol alert acknowledgements." in out

            _, out = await srv._execute_command("N0CALL", "s/dx")
            assert out == "?\r\n"
            _, out = await srv._execute_command("N0CALL", "show/d")
            assert out == "?\r\n"
        finally:
            await store.close()

    asyncio.run(run())


def test_grouped_command_shortcut_resolution_matrix(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "shortcut_matrix.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        try:
            reg = srv._build_registry()
            groups = sorted({k.split("/", 1)[0] for k in reg if "/" in k})

            def _hsig(fn):
                return (getattr(fn, "__func__", fn), getattr(fn, "__self__", None))

            for group in groups:
                subs = [k.split("/", 1)[1] for k in reg if k.startswith(group + "/")]
                norm_sig: dict[str, set[tuple[object, object]]] = {}
                for sub in subs:
                    n = srv._normalize_cmd_token(sub)
                    if not n:
                        continue
                    norm_sig.setdefault(n, set()).add(_hsig(reg[f"{group}/{sub}"]))

                for sub in subs:
                    full_key = f"{group}/{sub}"
                    target_sig = _hsig(reg[full_key])
                    ns = srv._normalize_cmd_token(sub)
                    if not ns:
                        continue

                    # Full condensed token should resolve whenever unambiguous by handler.
                    if len(norm_sig.get(ns, set())) == 1:
                        got = srv._resolve_subcommand(group, ns, reg)
                        assert got is not None
                        assert _hsig(reg[got]) == target_sig

                    # Minimal shortcut prefix should resolve when a unique handler family exists.
                    for i in range(2, len(ns) + 1):
                        p = ns[:i]
                        pref_sigs: set[tuple[object, object]] = set()
                        for n2, sigs in norm_sig.items():
                            if n2.startswith(p):
                                pref_sigs.update(sigs)
                        if len(pref_sigs) == 1:
                            got = srv._resolve_subcommand(group, p, reg)
                            assert got is not None
                            assert _hsig(reg[got]) == target_sig
                            break
        finally:
            await store.close()

    asyncio.run(run())


def test_show_shortcuts_catalog_and_execution(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "shortcuts_catalog.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            _, out = await srv._execute_command("N0CALL", "show/shortcuts proto")
            assert "Capital letters show the shorthand pyCluster guarantees." in out
            assert "show/protoack" in out
            assert "set/protoack" in out
            assert "unset/protoack" in out
            assert "clear/protohistory" in out

            reg = srv._build_registry()
            cat = dict(srv._build_shortcut_catalog(reg))
            canonical = sorted(srv._canonical_grouped_keys(reg))
            grouped_keys = sorted(k for k in cat if "/" in k)
            assert grouped_keys == canonical

            # Every generated shortcut must resolve to its command handler (or equivalent alias).
            for key, short in cat.items():
                if "/" in key:
                    grp, pref = short.split("/", 1)
                    full_group = srv._resolve_group_token(grp)
                    assert full_group is not None
                    got = srv._resolve_subcommand(full_group, pref, reg)
                    assert got is not None
                    assert srv._resolver_pick_equivalent({got, key}, reg) is not None
                else:
                    assert srv._resolve_top_token(short) == key

            # Published shortcuts must be unique.
            assert len(set(cat.values())) == len(cat)

            # Execute a few dynamic examples from generated catalog.
            show_proto_key = "show/protoack" if "show/protoack" in cat else "show/protoacks"
            _, out = await srv._execute_command("N0CALL", cat[show_proto_key])
            assert "No protocol alert acknowledgements." in out

            now = int(datetime.now(timezone.utc).timestamp())
            await store.set_user_pref("N0CALL", "privilege", "sysop", now)
            _, out = await srv._execute_command("N0CALL", f"{cat['set/protoack']} a")
            assert "Protocol alerts acknowledged for" in out
            _, out = await srv._execute_command("N0CALL", f"{cat['unset/protoack']} *")
            assert "Cleared protocol alert acknowledgements" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_b_alias_resolves_to_bye() -> None:
    cfg = _mk_config(":memory:")
    store = SpotStore(":memory:")
    try:
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        assert srv._resolve_top_token("b") == "bye"
    finally:
        asyncio.run(store.close())


def test_all_token_shortcuts_across_command_families(tmp_path) -> None:
    async def _stats():
        return {
            "peer1": {"profile": "spider", "inbound": False, "parsed_frames": 1, "sent_frames": 1, "policy_dropped": 0},
            "peer2": {"profile": "spider", "inbound": False, "parsed_frames": 1, "sent_frames": 1, "policy_dropped": 0},
        }

    async def run() -> None:
        db = str(tmp_path / "all_tokens.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc24.call", "K1ABC", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc24.flag", "0", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.last_epoch", str(now), now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.history", json.dumps([{"epoch": now, "key": "pc24.flag", "from": "1", "to": "0"}]), now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer2.pc24.call", "K2ABC", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer2.pc24.flag", "1", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer2.last_epoch", str(now), now)

        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc), link_stats_fn=_stats)
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            _, out = await srv._execute_command("N0CALL", "set/relay a off")
            assert "Relay policy set to off for all traffic." in out
            _, out = await srv._execute_command("N0CALL", "unset/relay *")
            assert "Relay policy restored to defaults for all traffic" in out

            _, out = await srv._execute_command("N0CALL", "set/relaypeer peer1 a off")
            assert "Relay policy for peer1 all traffic set to off." in out
            _, out = await srv._execute_command("N0CALL", "unset/relaypeer peer1 *")
            assert "Relay policy for peer1 restored to defaults" in out

            _, out = await srv._execute_command("N0CALL", "set/ingestpeer peer2 a off")
            assert "Ingest policy for peer2 all traffic set to off." in out
            _, out = await srv._execute_command("N0CALL", "unset/ingestpeer peer2 *")
            assert "Ingest policy for peer2 restored to defaults" in out

            _, out = await srv._execute_command("N0CALL", "set/protoack a")
            assert "permission denied" in out
            await store.set_user_pref("N0CALL", "privilege", "sysop", now)

            _, out = await srv._execute_command("N0CALL", "set/protoack *")
            assert "Protocol alerts acknowledged for" in out
            _, out = await srv._execute_command("N0CALL", "show/protoalerts +a")
            assert "health acked" in out
            _, out = await srv._execute_command("N0CALL", "unset/protoack a")
            assert "Cleared protocol alert acknowledgements" in out

            _, out = await srv._execute_command("N0CALL", "unset/protothreshold a")
            assert "All protocol threshold overrides cleared" in out
            _, out = await srv._execute_command("N0CALL", "clear/protohistory a")
            assert "Cleared " in out
            assert "protocol history" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_top_level_prefix_resolution_and_group_safety(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "top_prefix.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            # Unique top-level prefixes should resolve.
            _, out = await srv._execute_command("N0CALL", "disc N2WQ-1")
            assert "disconnect" in out.lower() or "not connected" in out.lower()

            _, out = await srv._execute_command("N0CALL", "dbsho")
            assert "Database summary for" in out
            assert "Registered Users:" in out

            # Ambiguous top-level prefix should not resolve.
            _, out = await srv._execute_command("N0CALL", "di")
            assert out == "?\r\n"

            # Separator-insensitive aliases should resolve.
            _, out = await srv._execute_command("N0CALL", "sendconfig")
            assert "Node configuration:" in out
            assert "Node Call:" in out
            _, out = await srv._execute_command("N0CALL", "exportusers")
            assert "permission denied" in out
            _, out = await srv._execute_command("N0CALL", "sendc")
            assert out == "?\r\n"
            _, out = await srv._execute_command("N0CALL", "dxqsl_i /tmp/demo.dxqsl")
            assert out == "?\r\n"

            # Group shortcut must remain group-resolved (not top-level shutdown).
            _, out = await srv._execute_command("N0CALL", "sh/time")
            assert "Z" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_shortcuts_includes_top_level_commands(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "shortcuts_top.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            _, out = await srv._execute_command("N0CALL", "show/shortcuts dbsho")
            assert "dbshow" in out
            assert "runs" in out
            row = next((ln for ln in out.splitlines() if "runs" in ln and "dbsho" in ln.lower()), "")
            assert row
            rhs = row.split("runs", 1)[1].strip()
            short = rhs.split()[0]
            assert srv._resolve_top_token(short) == "dbshow"
        finally:
            await store.close()

    asyncio.run(run())


def test_show_connect_uses_link_stats(tmp_path) -> None:
    async def _stats():
        return {
            "peer1": {"parsed_frames": 12, "dropped_frames": 1, "policy_dropped": 2, "profile": "arcluster", "inbound": False},
            "in:(127.0.0.1, 9000)": {"parsed_frames": 4, "dropped_frames": 0, "policy_dropped": 0, "profile": "spider", "inbound": True},
        }

    async def run() -> None:
        db = str(tmp_path / "connect.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc), link_stats_fn=_stats)
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv._execute_command("N0CALL", "show/connect")
            assert "peer1" in out
            assert "RX 12" in out and "TX 0" in out
            assert "profile arcluster" in out
            assert "inbound" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_connect_and_route_include_proto_peer_state(tmp_path) -> None:
    async def _stats():
        return {
            "peer1": {
                "parsed_frames": 9,
                "sent_frames": 5,
                "dropped_frames": 0,
                "policy_dropped": 0,
                "policy_reasons": {},
                "profile": "spider",
                "inbound": False,
                "last_pc_type": "PC51",
            }
        }

    async def run() -> None:
        db = str(tmp_path / "connect_proto.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc24.call", "OH8X", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc24.flag", "1", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc50.call", "W3LPL", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc50.count", "63", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc51.to", "AI3I-15", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc51.from", "WB3FFV-2", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc51.value", "1", now)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc), link_stats_fn=_stats)
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv._execute_command("N0CALL", "show/connect")
            assert "Protocol:" in out
            assert "PC24 OH8X / 1" in out
            assert "PC50 W3LPL / 63" in out
            assert "PC51 AI3I-15 from WB3FFV-2 value 1" in out

            _, out = await srv._execute_command("N0CALL", "show/route")
            assert "Last PC51" in out
            assert "Protocol:" in out
            assert "PC24 OH8X / 1" in out
            assert "Reconnect:" not in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_links_and_node_use_desired_peers_and_explicit_identity(tmp_path) -> None:
    async def _stats():
        return {
            "AI3I-15": {
                "parsed_frames": 812,
                "sent_frames": 94,
                "dropped_frames": 0,
                "policy_dropped": 0,
                "policy_reasons": {},
                "profile": "spider",
                "inbound": False,
                "last_pc_type": "PC11",
                "last_rx_epoch": int(datetime.now(timezone.utc).timestamp()),
            }
        }

    async def _desired():
        return [
            {
                "peer": "AI3I-15",
                "profile": "spider",
                "connected": True,
                "desired": True,
                "last_connect_epoch": int(datetime.now(timezone.utc).timestamp()),
                "reconnect_enabled": True,
                "retry_count": 0,
                "next_retry_epoch": 0,
                "pending_mail": 2,
                "route_issues": 0,
            },
            {
                "peer": "PYC-2",
                "profile": "pycluster",
                "connected": False,
                "desired": True,
                "last_connect_epoch": 0,
                "reconnect_enabled": True,
                "retry_count": 2,
                "next_retry_epoch": int(datetime.now(timezone.utc).timestamp()) + 300,
                "last_error": "timed out",
                "pending_mail": 3,
                "route_issues": 1,
            },
        ]

    async def run() -> None:
        db = str(tmp_path / "links_view.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        await store.set_user_pref(cfg.node.node_call, "proto.peer.ai3i-15.pc18.family", "spider", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.ai3i-15.pc18.summary", "DXSpider 1.57 build 633", now)
        srv = TelnetClusterServer(
            cfg,
            store,
            datetime.now(timezone.utc),
            link_stats_fn=_stats,
            link_desired_peers_fn=_desired,
        )
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv._execute_command("N0CALL", "show/links")
            assert "Peer         Family" in out
            assert "AI3I-15" in out
            assert "spider" in out
            assert "DXSpider 1.57 build 633" in out
            assert "PYC-2" in out
            assert "down" in out
            assert "Mail Queue: 2" in out
            assert "Retry Count: 2" in out
            assert "Route Issues: 1" in out
            assert "Last Error: timed out" in out

            _, out = await srv._execute_command("N0CALL", "show/node")
            assert "Topology" in out
            assert cfg.node.node_call in out
            assert "AI3I-15 [up spider]" in out
            assert "PYC-2 [down pycluster]" in out

            _, out = await srv._execute_command("N0CALL", "show/route")
            assert "Pending Mail: 2" in out
            assert "Pending Mail: 3" in out
            assert "Route Issues: 1" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_cluster_uses_roster_counts_not_frame_counts(tmp_path) -> None:
    async def _stats():
        return {
            "peer1": {"parsed_frames": 12, "sent_frames": 3, "profile": "spider", "inbound": False},
            "peer2": {"parsed_frames": 7, "sent_frames": 2, "profile": "pycluster", "inbound": True},
        }

    async def run() -> None:
        db = str(tmp_path / "show_cluster.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc), link_stats_fn=_stats)
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        srv._sessions[2] = Session(call="K1ABC", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc16.user_count", "4", now)
            _, out = await srv._execute_command("N0CALL", "show/cluster")
            assert "2 nodes, 2 local / 6 total users" in out
            assert "Remote users reported: 4" in out
            assert "21 total users" not in out
            assert "Max users seen: 6" in out
            assert "Uptime:" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_route_merges_peer_rows_case_insensitively(tmp_path) -> None:
    async def _stats():
        return {
            "AI3I-15": {
                "parsed_frames": 12,
                "sent_frames": 4,
                "dropped_frames": 0,
                "policy_dropped": 0,
                "policy_reasons": {},
                "profile": "spider",
                "inbound": False,
                "last_pc_type": "PC11",
                "last_rx_epoch": int(datetime.now(timezone.utc).timestamp()),
            }
        }

    async def _desired():
        return [
            {
                "peer": "ai3i-15",
                "profile": "spider",
                "connected": True,
                "desired": True,
                "reconnect_enabled": True,
                "retry_count": 1,
                "next_retry_epoch": 0,
                "pending_mail": 2,
                "route_issues": 1,
                "last_error": "slow link",
            }
        ]

    async def run() -> None:
        db = str(tmp_path / "route_casefold.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(
            cfg,
            store,
            datetime.now(timezone.utc),
            link_stats_fn=_stats,
            link_desired_peers_fn=_desired,
        )
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            _, out = await srv._execute_command("N0CALL", "show/route")
            assert out.count("AI3I-15") + out.count("ai3i-15") == 1
            assert "Pending Mail: 2" in out
            assert "Route Issues: 1" in out
            assert "Last Error: slow link" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_links_and_node_merge_peer_rows_case_insensitively(tmp_path) -> None:
    async def _stats():
        return {
            "AI3I-15": {
                "parsed_frames": 12,
                "sent_frames": 4,
                "dropped_frames": 0,
                "policy_dropped": 0,
                "policy_reasons": {},
                "profile": "spider",
                "inbound": False,
                "last_pc_type": "PC11",
                "last_rx_epoch": int(datetime.now(timezone.utc).timestamp()),
            }
        }

    async def _desired():
        return [
            {
                "peer": "ai3i-15",
                "profile": "spider",
                "connected": True,
                "desired": True,
                "reconnect_enabled": True,
                "retry_count": 1,
                "next_retry_epoch": 0,
                "pending_mail": 2,
                "route_issues": 1,
                "last_error": "slow link",
            }
        ]

    async def run() -> None:
        db = str(tmp_path / "links_casefold.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(
            cfg,
            store,
            datetime.now(timezone.utc),
            link_stats_fn=_stats,
            link_desired_peers_fn=_desired,
        )
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            _, out = await srv._execute_command("N0CALL", "show/links")
            assert out.count("AI3I-15") + out.count("ai3i-15") == 1
            assert "Mail Queue: 2" in out
            assert "Route Issues: 1" in out
            assert "Last Error: slow link" in out

            _, out = await srv._execute_command("N0CALL", "show/node")
            assert out.count("AI3I-15 [up spider]") + out.count("ai3i-15 [up spider]") == 1
        finally:
            await store.close()

    asyncio.run(run())


def test_show_proto_command_reports_health_and_filter(tmp_path) -> None:
    async def _stats():
        return {
            "peer1": {
                "parsed_frames": 9,
                "sent_frames": 5,
                "dropped_frames": 0,
                "policy_dropped": 0,
                "policy_reasons": {},
                "profile": "spider",
                "inbound": False,
                "last_pc_type": "PC51",
            },
            "peer2": {
                "parsed_frames": 3,
                "sent_frames": 1,
                "dropped_frames": 0,
                "policy_dropped": 0,
                "policy_reasons": {},
                "profile": "dxnet",
                "inbound": False,
                "last_pc_type": "PC24",
            },
        }

    async def run() -> None:
        db = str(tmp_path / "show_proto.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc24.call", "OH8X", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc24.flag", "1", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc50.call", "W3LPL", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc50.count", "63", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc51.to", "AI3I-15", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc51.from", "WB3FFV-2", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc51.value", "1", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.last_epoch", str(now), now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer2.pc24.call", "K1ABC", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer2.pc24.flag", "0", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer2.pc51.value", "0", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer2.last_epoch", str(now), now)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc), link_stats_fn=_stats)
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv._execute_command("N0CALL", "show/proto")
            assert "Protocol peer state:" in out
            assert "peer1" in out and "health ok" in out
            assert "PC50  Call: W3LPL  Nodes: 63" in out
            assert "peer2" in out and "health degraded" in out

            _, out = await srv._execute_command("N0CALL", "show/proto peer2")
            assert "peer2" in out
            assert "peer1" not in out

            _, out = await srv._execute_command("N0CALL", "show/proto missing")
            assert "No protocol peer data for filter 'missing'" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_proto_stale_and_stat_proto(tmp_path) -> None:
    async def _stats():
        return {
            "peer1": {"parsed_frames": 1, "sent_frames": 1, "dropped_frames": 0, "policy_dropped": 0, "profile": "spider", "inbound": False},
            "peer2": {"parsed_frames": 1, "sent_frames": 1, "dropped_frames": 0, "policy_dropped": 0, "profile": "spider", "inbound": False},
            "peer3": {"parsed_frames": 1, "sent_frames": 1, "dropped_frames": 0, "policy_dropped": 0, "profile": "spider", "inbound": False},
        }

    async def run() -> None:
        db = str(tmp_path / "show_proto_stale.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        old = now - 7200
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc24.call", "K1ABC", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc24.flag", "1", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.last_epoch", str(now), now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer2.pc24.call", "K2XYZ", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer2.pc24.flag", "1", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer2.last_epoch", str(old), now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer3.pc24.call", "K3BAD", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer3.pc24.flag", "0", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer3.last_epoch", str(now), now)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc), link_stats_fn=_stats)
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            _, out = await srv._execute_command("N0CALL", "show/proto --stale-mins 1")
            assert "peer2" in out and "health stale" in out
            assert "age" in out and "minutes" in out

            _, out = await srv._execute_command("N0CALL", "show/proto --stale-mins x")
            assert "Usage: show/proto [peer] [--stale-mins <minutes>]" in out

            _, out = await srv._execute_command("N0CALL", "stat/proto")
            assert "Protocol status: 3 peers, with 3 known, 2 ok, 0 degraded, 0 flapping, 1 stale, and 0 unknown." in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_proto_flapping_health(tmp_path) -> None:
    async def _stats():
        return {
            "peer1": {"parsed_frames": 1, "sent_frames": 1, "dropped_frames": 0, "policy_dropped": 0, "profile": "spider", "inbound": False},
        }

    async def run() -> None:
        db = str(tmp_path / "show_proto_flap.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc24.call", "K1ABC", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc24.flag", "1", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.flap_score", "5", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.change_count", "9", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.last_epoch", str(now), now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.last_change_epoch", str(now), now)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc), link_stats_fn=_stats)
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            _, out = await srv._execute_command("N0CALL", "show/proto")
            assert "health flapping" in out
            assert "changes 9 flap score 5" in out

            _, out = await srv._execute_command("N0CALL", "stat/proto")
            assert "Protocol status: 1 peers, with 1 known, 0 ok, 0 degraded, 1 flapping, 0 stale, and 0 unknown." in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_proto_ignores_expired_flap_scores(tmp_path) -> None:
    async def _stats():
        return {
            "peer1": {"parsed_frames": 1, "sent_frames": 1, "dropped_frames": 0, "policy_dropped": 0, "profile": "spider", "inbound": False},
        }

    async def run() -> None:
        db = str(tmp_path / "show_proto_expired_flap.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        old = now - 900
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc24.call", "K1ABC", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc24.flag", "1", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.flap_score", "5", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.change_count", "9", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.last_epoch", str(now), now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.last_change_epoch", str(old), now)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc), link_stats_fn=_stats)
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            _, out = await srv._execute_command("N0CALL", "show/proto")
            assert "health ok" in out
            assert "changes 9 flap score 5" in out

            _, out = await srv._execute_command("N0CALL", "stat/proto")
            assert "Protocol status: 1 peers, with 1 known, 1 ok, 0 degraded, 0 flapping, 0 stale, and 0 unknown." in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_proto_history_flag(tmp_path) -> None:
    async def _stats():
        return {
            "peer1": {"parsed_frames": 1, "sent_frames": 1, "dropped_frames": 0, "policy_dropped": 0, "profile": "spider", "inbound": False},
        }

    async def run() -> None:
        db = str(tmp_path / "show_proto_hist.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc24.call", "K1ABC", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc24.flag", "1", now)
        await store.set_user_pref(
            cfg.node.node_call,
            "proto.peer.peer1.history",
            json.dumps(
                [
                    {"epoch": now - 5, "key": "pc24.flag", "from": "0", "to": "1"},
                    {"epoch": now - 1, "key": "pc51.value", "from": "0", "to": "1"},
                ]
            ),
            now,
        )
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.last_epoch", str(now), now)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc), link_stats_fn=_stats)
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            _, out = await srv._execute_command("N0CALL", "show/proto --history")
            assert "History:" in out
            assert "pc24.flag 0 -> 1" in out
            assert "pc51.value 0 -> 1" in out

            _, out = await srv._execute_command("N0CALL", "show/proto --history --history-limit 1")
            assert "pc51.value 0 -> 1" in out
            assert "pc24.flag 0 -> 1" not in out
        finally:
            await store.close()

    asyncio.run(run())


def test_clear_protohistory_requires_sysop_and_clears(tmp_path) -> None:
    async def _stats():
        return {
            "peer1": {"parsed_frames": 1, "sent_frames": 1, "dropped_frames": 0, "policy_dropped": 0, "profile": "spider", "inbound": False},
        }

    async def run() -> None:
        db = str(tmp_path / "clear_protohistory.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.history", json.dumps([{"epoch": now, "key": "pc24.flag", "from": "0", "to": "1"}]), now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.change_count", "1", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.flap_score", "1", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer2.history", json.dumps([{"epoch": now, "key": "pc51.value", "from": "0", "to": "1"}]), now)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc), link_stats_fn=_stats)
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            _, out = await srv._execute_command("N0CALL", "clear/protohistory peer1")
            assert "permission denied" in out

            await store.set_user_pref("N0CALL", "privilege", "sysop", now)
            _, out = await srv._execute_command("N0CALL", "clear/prhist peer1")
            assert "Cleared " in out
            assert "for peer1." in out
            prefs = await store.list_user_prefs(cfg.node.node_call)
            assert "proto.peer.peer1.history" not in prefs
            assert "proto.peer.peer1.change_count" not in prefs
            assert "proto.peer.peer2.history" in prefs
        finally:
            await store.close()

    asyncio.run(run())


def test_stat_protohistory_command(tmp_path) -> None:
    async def _stats():
        return {
            "peer1": {"parsed_frames": 1, "sent_frames": 1, "dropped_frames": 0, "policy_dropped": 0, "profile": "spider", "inbound": False},
            "peer2": {"parsed_frames": 1, "sent_frames": 1, "dropped_frames": 0, "policy_dropped": 0, "profile": "spider", "inbound": False},
        }

    async def run() -> None:
        db = str(tmp_path / "stat_protohistory.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        await store.set_user_pref(
            cfg.node.node_call,
            "proto.peer.peer1.history",
            json.dumps(
                [
                    {"epoch": now - 60, "key": "pc24.flag", "from": "0", "to": "1"},
                    {"epoch": now - 10, "key": "pc24.flag", "from": "1", "to": "0"},
                ]
            ),
            now,
        )
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc), link_stats_fn=_stats)
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            _, out = await srv._execute_command("N0CALL", "stat/protohistory")
            assert "Protocol history status: 2 peers, 1 with history, 2 events, and last epoch " in out
            _, out = await srv._execute_command("N0CALL", "stat/protohistory peer1")
            assert "Protocol history status: 1 peers, 1 with history, 2 events, and last epoch " in out
        finally:
            await store.close()

    asyncio.run(run())


def test_stat_protoevents_command(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "stat_protoevents.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        await store.set_user_pref(
            cfg.node.node_call,
            "proto.peer.peer1.history",
            json.dumps(
                [
                    {"epoch": now - 30, "key": "pc24.flag", "from": "0", "to": "1"},
                    {"epoch": now - 20, "key": "pc24.flag", "from": "1", "to": "0"},
                    {"epoch": now - 10, "key": "pc51.value", "from": "0", "to": "1"},
                ]
            ),
            now,
        )
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            _, out = await srv._execute_command("N0CALL", "stat/protoevents")
            assert "Protocol event status: 3 events across 2 keys. Top activity: pc24.flag:2,pc51.value:1." in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_and_stat_protoalerts(tmp_path) -> None:
    async def _stats():
        return {
            "peer1": {"profile": "spider", "inbound": False, "parsed_frames": 1, "sent_frames": 1, "policy_dropped": 0},
            "peer2": {"profile": "spider", "inbound": False, "parsed_frames": 1, "sent_frames": 1, "policy_dropped": 0},
            "peer3": {"profile": "spider", "inbound": False, "parsed_frames": 1, "sent_frames": 1, "policy_dropped": 0},
        }

    async def run() -> None:
        db = str(tmp_path / "protoalerts.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        old = now - 7200
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc24.call", "K1ABC", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc24.flag", "0", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.last_epoch", str(now), now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer2.pc24.call", "K2ABC", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer2.pc24.flag", "1", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer2.flap_score", "9", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer2.last_epoch", str(now), now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer2.last_change_epoch", str(now), now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer3.pc24.call", "K3ABC", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer3.pc24.flag", "1", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer3.last_epoch", str(old), now)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc), link_stats_fn=_stats)
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            _, out = await srv._execute_command("N0CALL", "show/protoalerts")
            assert "peer1" not in out
            assert "peer2" in out and "health flapping" in out
            assert "peer3" in out and "health stale" in out
            assert "last " in out
            assert "age" in out and "minutes" in out

            _, out = await srv._execute_command("N0CALL", "show/protoalerts peer2")
            assert "peer2" in out and "peer1" not in out

            _, out = await srv._execute_command("N0CALL", "stat/protoalerts")
            assert "Protocol alert status: 2 total, 0 degraded, 1 flapping, 1 stale, and 0 acknowledged." in out
            _, out = await srv._execute_command("N0CALL", "stat/protoalerts peer2")
            assert "Protocol alert status: 1 total, 0 degraded, 1 flapping, 0 stale, and 0 acknowledged." in out

            _, out = await srv._execute_command("N0CALL", "set/protoack peer1")
            assert "permission denied" in out
            await store.set_user_pref("N0CALL", "privilege", "sysop", now)
            _, out = await srv._execute_command("N0CALL", "set/prack peer1")
            assert "Protocol alerts acknowledged for peer1." in out

            _, out = await srv._execute_command("N0CALL", "show/protoalerts")
            assert "peer1" not in out
            _, out = await srv._execute_command("N0CALL", "show/protoalerts a")
            assert "peer1" in out and "health acked" in out
            _, out = await srv._execute_command("N0CALL", "show/protoack")
            assert "peer1" in out and "suppressed 1" in out
            _, out = await srv._execute_command("N0CALL", "show/protoacks peer2")
            assert "No protocol alert acknowledgements for filter 'peer2'" in out

            _, out = await srv._execute_command("N0CALL", "stat/protoalerts")
            assert "1 acknowledged." in out
            _, out = await srv._execute_command("N0CALL", "stat/protoack")
            assert "Protocol acknowledgement status: 1 total, 1 suppressed, and 0 expired." in out
            _, out = await srv._execute_command("N0CALL", "stat/protoalerts peer1")
            assert "Protocol alert status: 1 total, 0 degraded, 0 flapping, 0 stale, and 1 acknowledged." in out
            _, out = await srv._execute_command("N0CALL", "unset/prack *")
            assert "Cleared protocol alert acknowledgements for 1 peer(s)." in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_protoevents_command(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "show_protoevents.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        await store.set_user_pref(
            cfg.node.node_call,
            "proto.peer.peer1.history",
            json.dumps(
                [
                    {"epoch": now - 60, "key": "pc24.flag", "from": "0", "to": "1"},
                    {"epoch": now - 10, "key": "pc50.count", "from": "64", "to": "63"},
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
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            _, out = await srv._execute_command("N0CALL", "show/protoevents 2")
            assert "Protocol history events (2):" in out
            assert "peer2" in out and "pc51.value" in out and "0 -> 1" in out
            assert "peer1" in out and "pc50.count" in out and "64 -> 63" in out
            assert not ("peer1" in out and "pc24.flag" in out and "0 -> 1" in out)

            _, out = await srv._execute_command("N0CALL", "show/protoevents peer1 5")
            assert "peer1" in out and "pc24.flag" in out and "0 -> 1" in out
            assert "peer2" not in out

            _, out = await srv._execute_command("N0CALL", "show/protoevents --key pc50 5")
            assert "pc50.count" in out and "64 -> 63" in out
            assert "pc24.flag" not in out

            _, out = await srv._execute_command("N0CALL", "show/protoevents --since 1")
            assert "Protocol history events (" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_proto_threshold_commands_and_show(tmp_path) -> None:
    async def _stats():
        return {
            "peer1": {"parsed_frames": 1, "sent_frames": 1, "dropped_frames": 0, "policy_dropped": 0, "profile": "spider", "inbound": False},
        }

    async def run() -> None:
        db = str(tmp_path / "proto_threshold_cmds.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc24.call", "K1ABC", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc24.flag", "1", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.flap_score", "4", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.last_epoch", str(now), now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.last_change_epoch", str(now), now)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc), link_stats_fn=_stats)
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            _, out = await srv._execute_command("N0CALL", "set/protothreshold flap_score 10")
            assert "permission denied" in out
            await store.set_user_pref("N0CALL", "privilege", "sysop", now)

            _, out = await srv._execute_command("N0CALL", "set/protothreshold flap_score 10")
            assert "Protocol threshold flap score set to 10." in out

            _, out = await srv._execute_command("N0CALL", "show/protoconfig")
            assert "flap_score: 10 (node)" in out
            assert "stale_mins: 30 (default)" in out

            _, out = await srv._execute_command("N0CALL", "show/proto")
            assert "health ok" in out

            _, out = await srv._execute_command("N0CALL", "unset/protothreshold flap_score")
            assert "Protocol threshold flap score restored to default." in out
            _, out = await srv._execute_command("N0CALL", "show/proto")
            assert "health flapping" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_proto_threshold_separator_compat(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "proto_threshold_compat.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            await store.set_user_pref("N0CALL", "privilege", "sysop", now)

            _, out = await srv._execute_command("N0CALL", "set/proto-threshold flap-score 10")
            assert "Protocol threshold flap score set to 10." in out

            _, out = await srv._execute_command("N0CALL", "set/protothreshold stalemins 45")
            assert "Protocol threshold stale mins set to 45." in out

            _, out = await srv._execute_command("N0CALL", "set/protothreshold flap_window_secs 600")
            assert "Protocol threshold flap window secs set to 600." in out

            _, out = await srv._execute_command("N0CALL", "show/proto-thresholds")
            assert "flap_score: 10 (node)" in out
            assert "stale_mins: 45 (node)" in out
            assert "flap_window_secs: 600 (node)" in out

            _, out = await srv._execute_command("N0CALL", "unset/proto-thresholds flapwindowsecs")
            assert "Protocol threshold flap window secs restored to default." in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_proto_thresholds_can_be_set_via_set_var(tmp_path) -> None:
    async def _stats():
        return {
            "peer1": {"parsed_frames": 1, "sent_frames": 1, "dropped_frames": 0, "policy_dropped": 0, "profile": "spider", "inbound": False},
        }

    async def run() -> None:
        db = str(tmp_path / "show_proto_thresholds.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc24.call", "K1ABC", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc24.flag", "1", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.flap_score", "4", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.last_epoch", str(now), now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.last_change_epoch", str(now), now)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc), link_stats_fn=_stats)
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            _, out = await srv._execute_command("N0CALL", "show/proto")
            assert "health flapping" in out

            _, out = await srv._execute_command(
                "N0CALL", f"set/var {cfg.node.node_call} proto.threshold.flap_score 10"
            )
            assert f"Variable proto.threshold.flap_score updated for {cfg.node.node_call}." in out

            _, out = await srv._execute_command("N0CALL", "show/proto")
            assert "health ok" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_set_profile_for_peer_callback(tmp_path) -> None:
    updates: list[tuple[str, str]] = []

    async def _set_peer(peer: str, profile: str) -> bool:
        updates.append((peer, profile))
        return peer == "peer1"

    async def run() -> None:
        db = str(tmp_path / "peerprof.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(
            cfg,
            store,
            datetime.now(timezone.utc),
            link_set_profile_fn=_set_peer,
        )
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv._execute_command("N0CALL", "set/arcluster peer1")
            assert "Profile for peer peer1 set to arcluster." in out
            assert updates[-1] == ("peer1", "arcluster")

            _, out = await srv._execute_command("N0CALL", "set/dxnet missing")
            assert "not found" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_filter_commands_roundtrip(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "filter.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv._execute_command("N0CALL", "accept/spots 1 on 40m")
            assert "Accept filter for spots saved" in out
            _, out = await srv._execute_command("N0CALL", "reject/spots 2 by K1")
            assert "Reject filter for spots saved" in out

            _, out = await srv._execute_command("N0CALL", "show/filter")
            assert "Filters for N0CALL" in out
            assert "Echo on" in out
            assert "accept/spots 1 on 40m" in out
            assert "reject/spots 2 by K1" in out

            _, out = await srv._execute_command("N0CALL", "clear/spots 1")
            assert "Cleared spots filters for N0CALL (slot 1)." in out
            _, out = await srv._execute_command("N0CALL", "show/filter")
            assert "accept/spots 1 on 40m" not in out
            assert "reject/spots 2 by K1" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_filter_sorted_and_preview_test_modes(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "filter_preview.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            await srv._execute_command("N0CALL", "accept/spots 5 on 20m")
            await srv._execute_command("N0CALL", "reject/spots 1 by N9")
            await srv._execute_command("N0CALL", "accept/route 2 peer east*")
            await srv._execute_command("N0CALL", "reject/route 1 peer west*")

            _, out = await srv._execute_command("N0CALL", "show/filter")
            i_rej = out.find("reject/spots 1 by N9")
            i_acc = out.find("accept/spots 5 on 20m")
            assert i_rej >= 0 and i_acc >= 0 and i_rej < i_acc
            assert "show/filter test spots" in out
            assert "show/filter test announce" in out
            assert "add --verbose after family" in out

            _, out = await srv._execute_command("N0CALL", "show/filter test spots 14074 W1AW N9XYZ FT8")
            assert "Decision: deny" in out
            _, out = await srv._execute_command("N0CALL", "show/filter test route --verbose east-hub")
            assert "Decision: allow" in out
            assert "Winning Rule: Accept rule matched in slot 2: peer east*" in out
            _, out = await srv._execute_command("N0CALL", "show/filter test route --verbose west-hub")
            assert "Decision: deny" in out
            assert "Winning Rule: Reject rule matched in slot 1: peer west*" in out

            _, out = await srv._execute_command("N0CALL", "show/filter test wx --verbose N0ABC local weather")
            assert "show/filter test <spots|rbn|route|announce>" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_publish_spot_applies_accept_and_reject_filters(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "spot_filter_publish.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        w1 = _DummyWriter()
        w2 = _DummyWriter()
        srv._sessions[1] = Session(call="N0CALL", writer=w1, connected_at=datetime.now(timezone.utc))
        srv._sessions[2] = Session(call="K1ABC", writer=w2, connected_at=datetime.now(timezone.utc))
        try:
            await srv._execute_command("N0CALL", "accept/spots 1 on 20m")
            await srv._execute_command("N0CALL", "reject/spots 0 by N9")

            now = int(datetime.now(timezone.utc).timestamp())
            spot20 = Spot(14074.0, "W1AW", now, "FT8", "N0AAA", "N2WQ-1", "")
            await srv.publish_spot(spot20)
            assert b"W1AW" in bytes(w1.buffer)
            assert b"W1AW" in bytes(w2.buffer)

            before_n0 = len(w1.buffer)
            before_k1 = len(w2.buffer)
            spot40 = Spot(7050.0, "K3LR", now, "CW", "N0AAA", "N2WQ-1", "")
            await srv.publish_spot(spot40)
            assert len(w1.buffer) == before_n0
            assert len(w2.buffer) > before_k1

            before_n0 = len(w1.buffer)
            before_k1 = len(w2.buffer)
            spot_reject = Spot(14020.0, "K1ZZ", now, "CW", "N9XYZ", "N2WQ-1", "")
            await srv.publish_spot(spot_reject)
            assert len(w1.buffer) == before_n0
            assert len(w2.buffer) > before_k1
        finally:
            await store.close()

    asyncio.run(run())


def test_show_dx_ignores_spot_filters_and_show_mydx_applies_them(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "spot_filter_show.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.add_spot(Spot(14074.0, "W1AW", now, "FT8", "K1AAA", "N2WQ-1", ""))
            await store.add_spot(Spot(7050.0, "K3LR", now, "CW", "K1AAA", "N2WQ-1", ""))
            await srv._execute_command("N0CALL", "accept/spots 1 on 20m")

            _, out = await srv._execute_command("N0CALL", "show/dx 20")
            assert "W1AW" in out
            assert "K3LR" in out

            _, out = await srv._execute_command("N0CALL", "show/mydx 20")
            assert "W1AW" in out
            assert "K3LR" not in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_dx_shortcuts_do_not_return_rbn_history(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "show_dx_shortcuts_rbn.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.add_spot(Spot(14011.2, "WS3W", now, "CW 6dB Q:3 Z:5", "KD2OGR-#", "RBN", ""))
            await store.add_spot(Spot(50140.0, "N3ALN", now + 1, "", "N3ALN", "N2WQ-1", ""))

            _, out = await srv._execute_command("N0CALL", "sh/dx")
            assert "N3ALN" in out
            assert "WS3W" not in out
            assert "RBN reports" not in out

            _, out = await srv._execute_command("N0CALL", "sh/mydx")
            assert "N3ALN" in out
            assert "WS3W" not in out
            assert "RBN reports" not in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_mydx_repeated_calls_return_same_filtered_history(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "show_mydx_repeat.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N9JR-10", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            now = int(datetime(2026, 7, 7, 22, 16, tzinfo=timezone.utc).timestamp())
            spots = [
                Spot(7036.0, "WM3PEN", now, "13 Colonies", "WA4NNA", "N2WQ-1", ""),
                Spot(14035.5, "WB0RLJ", now - 5, "NE POTA US-4011", "W1SAV", "N2WQ-1", ""),
                Spot(14288.0, "K2C", now - 10, "", "NP3PR", "N2WQ-1", ""),
                Spot(14011.2, "N9JR", now - 15, "CW 6dB Q:3 Z:5", "WS3W-#", "RBN", ""),
            ]
            for spot in spots:
                await store.add_spot(spot)
            await srv._execute_command("N9JR-10", "set/rbn")

            _, first = await srv._execute_command("N9JR-10", "sh/mydx")
            _, second = await srv._execute_command("N9JR-10", "sh/mydx")

            assert first == second
            assert "WM3PEN" in first
            assert "WB0RLJ" in first
            assert "No spots available" not in second
            assert "RBN reports" not in first
        finally:
            await store.close()

    asyncio.run(run())


def test_show_mydx_uses_database_history_when_recent_rbn_spots_dominate(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "show_mydx_rbn_saturated_history.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N9JR-10", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            now = int(datetime(2026, 7, 14, 16, 50, tzinfo=timezone.utc).timestamp())
            for idx in range(2500):
                await store.add_spot(Spot(14011.0 + (idx / 1000), "N9JR", now + idx, "CW 8dB Q:1 Z:5", f"K{idx:03d}-#", "RBN", ""))
            for idx in range(20):
                await store.add_spot(Spot(50313.0 + idx, f"K1H{idx:02d}", now - 300 - idx, "FT8 EN33<>FN20", "N0YXO", "N2WQ-1", ""))
            await srv._execute_command("N9JR-10", "set/rbn")

            _, out = await srv._execute_command("N9JR-10", "sh/mydx/20")

            assert "K1H00" in out
            assert "K1H19" in out
            assert "N9JR" not in out
            assert "No spots available" not in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_mydx_filtering_fills_requested_count_from_deeper_history(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "spot_filter_show_limit.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            for idx in range(5):
                await store.add_spot(Spot(7050.0 + idx, f"K3L{idx}", now + idx, "CW", "K1AAA", "N2WQ-1", ""))
            await store.add_spot(Spot(14074.0, "W1AW", now - 1, "FT8", "K1AAA", "N2WQ-1", ""))
            await store.add_spot(Spot(14326.8, "W0MES", now - 2, "IA", "WA2MCR", "N2WQ-1", ""))
            await srv._execute_command("N0CALL", "accept/spots 1 on 20m")

            _, out = await srv._execute_command("N0CALL", "show/mydx 2")
            assert "W1AW" in out
            assert "W0MES" in out
            assert "K3L" not in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_mydx_filtering_scans_past_high_volume_nonmatches(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "spot_filter_show_deep_history.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            for idx in range(10_500):
                await store.add_spot(Spot(7050.0 + (idx % 100), f"K3L{idx}", now + idx, "CW", "K1AAA", "N2WQ-1", ""))
            await store.add_spot(Spot(14074.0, "A71XX", now - 60, "FT8", "K1AAA", "N2WQ-1", ""))
            await srv._execute_command("N0CALL", "accept/spots 1 on 20m")

            _, out = await srv._execute_command("N0CALL", "show/mydx 1")
            assert "A71XX" in out
            assert "K3L" not in out
            assert "No spots available" not in out
        finally:
            await store.close()

    asyncio.run(run())


def test_spot_filters_support_call_zone_call_itu_and_call_dxcc(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "spot_filter_zones.db")
        cfg = _mk_config(db)
        cfg.public_web.cty_dat_path = _write_cty(tmp_path)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        w1 = _DummyWriter()
        srv._sessions[1] = Session(call="N0CALL", writer=w1, connected_at=datetime.now(timezone.utc))
        try:
            await srv._execute_command("N0CALL", "accept/spots 1 call_zone 5")
            now = int(datetime.now(timezone.utc).timestamp())
            allow = Spot(14074.0, "VE3XYZ", now, "FT8", "K1AAA", "N2WQ-1", "")
            deny = Spot(14074.0, "JA1ABC", now, "FT8", "K1AAA", "N2WQ-1", "")
            await srv.publish_spot(allow)
            assert b"VE3XYZ" in bytes(w1.buffer)
            before = len(w1.buffer)
            await srv.publish_spot(deny)
            assert len(w1.buffer) == before

            _, out = await srv._execute_command("N0CALL", "show/filter test spots --verbose 14074 VE3XYZ K1AAA FT8")
            assert "Decision: allow" in out
            assert "Winning Rule: Accept rule matched in slot 1: call_zone 5" in out

            await srv._execute_command("N0CALL", "clear/spots")
            await srv._execute_command("N0CALL", "accept/spots 1 call_itu 9")
            before = len(w1.buffer)
            await srv.publish_spot(allow)
            assert b"VE3XYZ" in bytes(w1.buffer[before:])
            before = len(w1.buffer)
            await srv.publish_spot(Spot(14074.0, "JA1ABC", now, "FT8", "K1AAA", "N2WQ-1", ""))
            assert len(w1.buffer) == before

            _, out = await srv._execute_command("N0CALL", "show/filter test spots --verbose 14074 VE3XYZ K1AAA FT8")
            assert "Decision: allow" in out
            assert "Winning Rule: Accept rule matched in slot 1: call_itu 9" in out

            await srv._execute_command("N0CALL", "clear/spots")
            await srv._execute_command("N0CALL", "accept/spots 1 call_dxcc canada")
            before = len(w1.buffer)
            await srv.publish_spot(allow)
            assert b"VE3XYZ" in bytes(w1.buffer[before:])
            before = len(w1.buffer)
            await srv.publish_spot(Spot(14074.0, "JA1ABC", now, "FT8", "K1AAA", "N2WQ-1", ""))
            assert len(w1.buffer) == before

            _, out = await srv._execute_command("N0CALL", "show/filter test spots --verbose 14074 VE3XYZ K1AAA FT8")
            assert "Decision: allow" in out
            assert "Winning Rule: Accept rule matched in slot 1: call_dxcc canada" in out

            await srv._execute_command("N0CALL", "clear/spots")
            await srv._execute_command("N0CALL", "accept/spots 1 call_dxcc ve")
            before = len(w1.buffer)
            await srv.publish_spot(allow)
            assert b"VE3XYZ" in bytes(w1.buffer[before:])
        finally:
            await store.close()

    asyncio.run(run())


def test_spot_filters_support_compound_web_expressions(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "spot_filter_compound.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        try:
            assert srv._spot_matches_expr(14074.0, "K1ABC", "EU1SPT", "FT8", "on 20m and info FT8")
            assert srv._spot_matches_expr(14074.0, "K1ABC", "EU1SPT", "FT8", "on 20m,40m and info FT8")
            assert srv._spot_matches_expr(7074.0, "K1ABC", "EU1SPT", "FT8", "on 20m,40m and info FT8")
            assert not srv._spot_matches_expr(7074.0, "K1ABC", "EU1SPT", "FT8", "on 20m and info FT8")
            assert not srv._spot_matches_expr(14074.0, "K1ABC", "EU1SPT", "CW", "on 20m and info FT8")
            assert not srv._spot_matches_expr(10136.0, "K1ABC", "EU1SPT", "FT8", "on 20m,40m and info FT8")
        finally:
            await store.close()

    asyncio.run(run())


def test_show_dx_supports_exact_prefix_spotter_and_day_filters(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "show_dx_filters.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            recent = int(datetime.now(timezone.utc).timestamp())
            old = recent - 3 * 86400
            await store.add_spot(Spot(14074.0, "K1ABC", recent, "FT8", "N0CALL", "N2WQ-1", ""))
            await store.add_spot(Spot(14074.0, "K1ABD", recent, "FT8", "N0CALL", "N2WQ-1", ""))
            await store.add_spot(Spot(14074.0, "K1ABC", recent, "CW", "W1AW", "N2WQ-1", ""))
            await store.add_spot(Spot(14074.0, "K1ABC", old, "OLD", "N0CALL", "N2WQ-1", ""))

            _, out = await srv._execute_command("N0CALL", "show/dx K1ABC exact by N0CALL day 1")
            assert "K1ABC" in out
            assert "K1ABD" not in out
            assert " W1AW " not in out
            assert " OLD " not in out
        finally:
            await store.close()

    asyncio.run(run())


def test_publish_spot_and_show_dx_strip_ssid_in_display_only(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "spot_display_strip_ssid.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        writer = _DummyWriter()
        srv._sessions[1] = Session(call="N0CALL", writer=writer, connected_at=datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            spot = Spot(21351.0, "CY0S", now, "ssb", "W7XE-11", "WA9PIE-2", "")
            await srv.publish_spot(spot)
            live = bytes(writer.buffer).decode("utf-8", "replace")
            assert "DX de W7XE:" in live
            assert "W7XE-11" not in live

            await store.add_spot(spot)
            _, out = await srv._execute_command("N0CALL", "show/dx 1")
            assert "<W7XE>" in out
            assert "W7XE-11" not in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_dx_wildcard_and_show_dxcc_alias_match_same_spots(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "show_dxcc_alias.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.add_spot(Spot(14074.0, "K1ABC", now, "FT8", "N0CALL", "N2WQ-1", ""))
            await store.add_spot(Spot(14074.0, "K2XYZ", now, "FT8", "N0CALL", "N2WQ-1", ""))

            _, wildcard = await srv._execute_command("N0CALL", "show/dx K*")
            _, dxcc = await srv._execute_command("N0CALL", "show/dxcc K")

            assert "K1ABC" in wildcard
            assert "K2XYZ" in wildcard
            assert "K1ABC" in dxcc
            assert "K2XYZ" in dxcc
        finally:
            await store.close()

    asyncio.run(run())


def test_show_dx_uses_session_profile_formatting(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "show_dx_profiles.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        sess = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        srv._sessions[1] = sess
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            long_info = "LONG-INFO-12345678901234567890-TAIL"
            await store.add_spot(Spot(14074.0, "W1AW", now, long_info, "K1ABC", "N2WQ-1", ""))

            sess.peer_profile = "spider"
            _, spider = await srv._execute_command("N0CALL", "show/dx 1")
            assert "<K1ABC>" in spider
            assert datetime.now(timezone.utc).strftime("-%b-") in spider
            assert "TAIL" not in spider

            sess.peer_profile = "arcluster"
            _, arcluster = await srv._execute_command("N0CALL", "show/dx 1")
            assert "de K1ABC" in arcluster

            sess.peer_profile = "dxnet"
            _, dxnet = await srv._execute_command("N0CALL", "show/dx 1")
            assert "[K1ABC]" in dxnet

            sess.peer_profile = "clx"
            _, clx = await srv._execute_command("N0CALL", "show/dx 1")
            assert "by K1ABC" in clx
        finally:
            await store.close()

    asyncio.run(run())


def test_publish_spot_uses_live_dx_format_without_blank_lines(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "publish_live_dx.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        writer = _DummyWriter()
        srv._sessions[1] = Session(call="N0CALL", writer=writer, connected_at=datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await srv.publish_spot(Spot(7137.0, "V31YL", now, "Young Ladies WWA ssb", "IW1FRU", "AI3I-15", ""))
            await srv.publish_spot(Spot(10136.0, "KR4FTE", now, "Young Ladies WWA FT8", "IW1FRU", "AI3I-15", ""))
            out = bytes(writer.buffer).decode("utf-8", "replace")
            assert "DX de IW1FRU:" in out
            assert "\r\n\r\n" not in out
            assert "Young Ladies WWA FT8" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_publish_spot_suppressed_during_registration_interview(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "publish_spot_registration_suppressed.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        writer = _DummyWriter()
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=writer,
            connected_at=datetime.now(timezone.utc),
            suppress_async_spots=True,
        )
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            delivered = await srv.publish_spot(Spot(14074.0, "W1AW", now, "FT8", "K1ABC", "AI3I-15", ""))
            assert delivered == 0
            assert bytes(writer.buffer) == b""

            srv._sessions[1].suppress_async_spots = False
            delivered = await srv.publish_spot(Spot(14075.0, "K1ZZ", now, "FT8", "K1ABC", "AI3I-15", ""))
            assert delivered == 1
            assert b"K1ZZ" in bytes(writer.buffer)
        finally:
            await store.close()

    asyncio.run(run())


def test_wcy_filters_are_not_registered_or_applied_to_live_and_show(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "wcy_filters.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        w_n0 = _DummyWriter()
        w_k1 = _DummyWriter()
        srv._sessions[1] = Session(call="N0CALL", writer=w_n0, connected_at=datetime.now(timezone.utc))
        srv._sessions[2] = Session(call="K1ABC", writer=w_k1, connected_at=datetime.now(timezone.utc))
        try:
            _, out = await srv._execute_command("N0CALL", "accept/wcy 1 by K1")
            assert out.strip() == "?"

            await srv.publish_bulletin("wcy", "K1ABC", "LOCAL", "A=5 K=2")
            assert b"WCY K1ABC: A=5 K=2" in bytes(w_n0.buffer)

            before = len(w_n0.buffer)
            await srv.publish_bulletin("wcy", "W1AW", "LOCAL", "A=9 K=4")
            assert len(w_n0.buffer) > before

            now = int(datetime.now(timezone.utc).timestamp())
            await store.add_bulletin("wcy", "K1ABC", "LOCAL", now, "A=4 K=1")
            await store.add_bulletin("wcy", "W1AW", "LOCAL", now, "A=7 K=3")
            _, out = await srv._execute_command("N0CALL", "show/wcy")
            assert "K1ABC" in out
            assert "W1AW" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_spot_filter_slot_order_controls_decision(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "spot_filter_slot_order.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        w1 = _DummyWriter()
        srv._sessions[1] = Session(call="N0CALL", writer=w1, connected_at=datetime.now(timezone.utc))
        try:
            await srv._execute_command("N0CALL", "accept/spots 5 on 20m")
            await srv._execute_command("N0CALL", "reject/spots 1 by N0")
            now = int(datetime.now(timezone.utc).timestamp())
            s = Spot(14074.0, "W1AW", now, "FT8", "N0AAA", "N2WQ-1", "")
            await srv.publish_spot(s)
            assert b"W1AW" not in bytes(w1.buffer)

            await srv._execute_command("N0CALL", "accept/spots 0 by N0")
            await srv.publish_spot(s)
            assert b"W1AW" in bytes(w1.buffer)
        finally:
            await store.close()

    asyncio.run(run())


def test_msg_talk_announce_and_show_log(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "msg.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        w1 = _DummyWriter()
        w2 = _DummyWriter()
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=w1,
            connected_at=datetime.now(timezone.utc),
        )
        srv._sessions[2] = Session(
            call="K1ABC",
            writer=w2,
            connected_at=datetime.now(timezone.utc),
        )
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.upsert_user_registry("N0CALL", now, privilege="user")
            await store.upsert_user_registry("K1ABC", now, privilege="user")
            _, out = await srv._execute_command("N0CALL", "msg K1ABC hello there")
            assert "Message #" in out and "delivered to 1 session(s)." in out
            assert "Delivery state: delivered" in out
            assert b"MSG#" in bytes(w2.buffer)
            assert b"N0CALL: hello there" in bytes(w2.buffer)

            _, out = await srv._execute_command("N0CALL", "talk K1ABC hi")
            assert "Talk delivered to 1 session(s)." in out
            assert b"TALK N0CALL: hi" in bytes(w2.buffer)

            _, out = await srv._execute_command("N0CALL", "announce full test notice")
            assert "Announcement accepted (full):" in out
            assert b"ANNOUNCE/FULL N0CALL: test notice" in bytes(w2.buffer)

            _, out = await srv._execute_command("N0CALL", "ann/full slash notice")
            assert "Announcement accepted (full):" in out
            assert b"ANNOUNCE/FULL N0CALL: slash notice" in bytes(w2.buffer)

            _, out = await srv._execute_command("N0CALL", "wx clear and cold")
            assert "wx: accepted" in out
            assert b"WX N0CALL: clear and cold" in bytes(w2.buffer)

            _, out = await srv._execute_command("K1ABC", "show/msgstatus")
            assert "Messages for K1ABC:" in out
            assert "Inbox states:" in out
            assert "Outbox states:" in out
            _, out = await srv._execute_command("K1ABC", "show/messages")
            assert "UNREAD" in out and "hello there" in out
            assert "delivered" in out
            assert "via -" in out
            _, out = await srv._execute_command("K1ABC", "mail")
            assert "hello there" in out

            _, out = await srv._execute_command("K1ABC", "read")
            assert "N0CALL" in out
            msg_id = int(out.splitlines()[0].split()[0])

            _, out = await srv._execute_command("K1ABC", f"read {msg_id}")
            assert f"Message #{msg_id}" in out
            assert "State: delivered" in out

            _, out = await srv._execute_command("K1ABC", f"reply {msg_id} roger")
            assert "Reply #" in out
            assert "delivered to " in out
            assert "Delivery state: delivered" in out

            _, out = await srv._execute_command("N0CALL", "read")
            assert "K1ABC" in out

            _, out = await srv._execute_command("N0CALL", "show/outbox")
            assert "Outbox:" in out
            assert "K1ABC" in out
            assert "delivered" in out

            _, out = await srv._execute_command("N0CALL", "show/log 10")
            assert "announce" in out.lower()
            assert "msg" in out.lower()
            assert "talk" in out.lower()
        finally:
            await store.close()

    asyncio.run(run())


def test_msg_to_remote_home_node_stays_pending_until_mail_transport_exists(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "msg_remote.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.upsert_user_registry("N0CALL", now, privilege="user")
            await store.upsert_user_registry("K1ABC", now, privilege="user", home_node="PEER1")

            _, out = await srv._execute_command("N0CALL", "msg K1ABC hello remote")
            assert "Message #" in out
            assert "Delivery state: pending" in out
            msg_id = int(out.split("#", 1)[1].split()[0])
            row = await store.get_message(msg_id)
            assert row is not None
            assert row["route_node"] == "PEER1"
            assert row["delivery_state"] == "pending"

            _, out = await srv._execute_command("N0CALL", "show/msgstatus")
            assert "Outbox states:" in out
            assert "pending 1" in out
            assert "Pending routes: PEER1: 1" in out

            _, out = await srv._execute_command("N0CALL", "show/outbox")
            assert "PEER1" in out
            assert "pending" in out
            assert "hello remote" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_msgstatus_and_outbox_surface_delivery_errors(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "mail_errors.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.upsert_user_registry("N0CALL", now, privilege="user")
            msg_id = await store.add_message(
                sender="N0CALL",
                recipient="K1ABC",
                epoch=now,
                body="cannot route this",
                origin_node=cfg.node.node_call,
                route_node="PEER404",
                delivery_state="undeliverable",
                error_text="No configured route to that peer.",
            )
            await store.set_message_delivery(
                msg_id,
                "undeliverable",
                route_node="PEER404",
                error_text="No configured route to that peer.",
            )

            _, out = await srv._execute_command("N0CALL", "show/msgstatus")
            assert "undeliverable 1" in out
            assert "Latest outbox error: K1ABC via PEER404: No configured route to that peer." in out

            _, out = await srv._execute_command("N0CALL", "show/outbox")
            assert "PEER404" in out
            assert "undeliver" in out.lower()
            assert "error No configured route to that peer." in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_msgstatus_surfaces_reconnect_pending_context(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "show_msgstatus_reconnect.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.add_message(
                "N0CALL",
                "K1ABC",
                now,
                "queued for reconnect",
                origin_node=cfg.node.node_call,
                route_node="PEER1",
                delivery_state="pending",
                error_text="Peer is disconnected and reconnect is disabled.",
            )

            _, out = await srv._execute_command("N0CALL", "show/msgstatus")
            assert "Pending routes: PEER1: 1" in out
            assert "Latest outbox error: K1ABC via PEER1: Peer is disconnected and reconnect is" in out
            assert "disabled." in out

            _, out = await srv._execute_command("N0CALL", "show/outbox")
            assert "via PEER1" in out
            assert "error Peer is disconnected and reconnect is disabled." in out
        finally:
            await store.close()

    asyncio.run(run())


def test_mail_shorthand_aliases_work(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "mail_short.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        w1 = _DummyWriter()
        w2 = _DummyWriter()
        srv._sessions[1] = Session(call="N0CALL", writer=w1, connected_at=datetime.now(timezone.utc))
        srv._sessions[2] = Session(call="K1ABC", writer=w2, connected_at=datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.upsert_user_registry("N0CALL", now, privilege="user")
            await store.upsert_user_registry("K1ABC", now, privilege="user")

            _, out = await srv._execute_command("N0CALL", "s K1ABC quick test")
            assert "Message #" in out
            assert "Delivery state: delivered" in out

            _, out = await srv._execute_command("N0CALL", "sp K1ABC another test")
            assert "Message #" in out
            assert "Delivery state: delivered" in out

            _, out = await srv._execute_command("K1ABC", "r")
            assert "N0CALL" in out

            msg_id = int(out.splitlines()[0].split()[0])
            _, out = await srv._execute_command("K1ABC", f"rep {msg_id} roger copy")
            assert "Reply #" in out
            assert "Delivery state: delivered" in out

            _, out = await srv._execute_command("N0CALL", "show/shortcuts")
            assert "Send" in out or "SEnd" in out
            assert "Read" in out or "REad" in out
            assert "REPly" in out or "Reply" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_prefix_lastspot_bands(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "qrz.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            from pycluster.models import parse_spot_record

            await store.add_spot(
                parse_spot_record("7109.9^K3AJ^1772335320^RTTY^WW5L^226^226^N2WQ-1^8^5^7^4^^^75.23.154.42")
            )
            _, out = await srv._execute_command("N0CALL", "show/prefix K3")
            assert "Prefix K3 has 1 local spot entry." in out

            _, out = await srv._execute_command("N0CALL", "show/lastspot K3AJ")
            assert "was last spotted on" in out
            assert "Frequency: 7109.9 kHz" in out

            _, out = await srv._execute_command("N0CALL", "show/bands")
            assert "40m" in out
            assert "hf" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_qrz_uses_remote_xml_lookup(tmp_path, monkeypatch) -> None:
    class _FakeResp:
        def __init__(self, body: str) -> None:
            self._body = body.encode("utf-8")

        def read(self) -> bytes:
            return self._body

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    calls: list[str] = []

    def _fake_urlopen(req, timeout=10):
        calls.append(req.full_url)
        if "username=" in req.full_url:
            return _FakeResp(
                "<QRZDatabase><Session><Key>session123</Key><Count>1</Count><SubExp>Wed Jan 1 12:34:03 2030</SubExp></Session></QRZDatabase>"
            )
        return _FakeResp(
            "<QRZDatabase><Session><Key>session123</Key><Count>1</Count></Session>"
            "<Callsign><call>K1ABC</call><fname>John</fname><name>Doe</name><addr2>Boston</addr2>"
            "<state>MA</state><country>United States</country><grid>FN42</grid><dxcc>291</dxcc>"
            "<cqzone>5</cqzone><ituzone>8</ituzone><lat>42.0</lat><lon>-71.0</lon></Callsign></QRZDatabase>"
        )

    async def run() -> None:
        db = str(tmp_path / "qrz_remote.db")
        cfg = _mk_config(db)
        cfg.qrz.username = "demo"
        cfg.qrz.password = "secret"
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        monkeypatch.setattr("pycluster.qrz.urllib.request.urlopen", _fake_urlopen)
        try:
            _, out = await srv._execute_command("N0CALL", "show/qrz K1ABC")
            assert "QRZ lookup for K1ABC:" in out
            assert "     Name : John Doe" in out
            assert "      QTH : Boston" in out
            assert "     Grid : FN42" in out
            assert "  CQ Zone : 5" in out
            assert "ITU Zone : 8" in out
            assert len(calls) == 2
        finally:
            await store.close()

    asyncio.run(run())


def test_show_aliases_mydx_newconfiguration_and_dxcc(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "show_aliases.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            from pycluster.models import parse_spot_record

            await store.add_spot(
                parse_spot_record("14074.0^K1ABC^1772335320^FT8^N0CALL^226^226^N2WQ-1^8^5^7^4^^^127.0.0.1")
            )
            await store.add_spot(
                parse_spot_record("7020.0^W1AW^1772335320^CW^N0CALL^226^226^N2WQ-1^8^5^7^4^^^127.0.0.1")
            )

            _, out = await srv._execute_command("N0CALL", "show/mydx")
            assert "K1ABC" in out

            _, out = await srv._execute_command("N0CALL", "show/newconfiguration")
            assert "Node configuration:" in out
            assert "Node Call:" in out and "Telnet Listener:" in out and "System Operator Web:" in out

            _, out = await srv._execute_command("N0CALL", "show/dxcc K1")
            assert "K1ABC" in out
            assert "W1AW" not in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_dxcc_uses_cty_data_when_available(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "show_dxcc_cty.db")
        cfg = _mk_config(db)
        cfg.public_web.cty_dat_path = _write_cty(tmp_path)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            _, out = await srv._execute_command("N0CALL", "show/dxcc K1")
            assert "DXCC K1: United States" in out
            assert "Continent: NA" in out
            assert "CQ Zone: 5" in out
            assert "ITU Zone: 8" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_dx_appends_cty_suffix_when_enabled(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "show_dx_cty_suffix.db")
        cfg = _mk_config(db)
        cfg.public_web.cty_dat_path = _write_cty(tmp_path)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        writer = _DummyWriter()
        srv._sessions[1] = Session(call="N0CALL", writer=writer, connected_at=datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.add_spot(Spot(14074.0, "K1ABC", now, "FT8", "JA1AAA", "AI3I-16", ""))

            _, out = await srv._execute_command("N0CALL", "set/dxcq")
            assert "DX CQ set to on for N0CALL." in out
            _, out = await srv._execute_command("N0CALL", "set/dxitu")
            assert "DX ITU set to on for N0CALL." in out

            _, out = await srv._execute_command("N0CALL", "show/dx 1")
            assert "CQ5 ITU8" in out

            await srv.publish_spot(Spot(14074.0, "K1ABC", now, "FT8", "JA1AAA", "AI3I-16", ""))
            live = bytes(writer.buffer).decode("utf-8", "replace")
            assert "CQ5 ITU8" in live
        finally:
            await store.close()

    asyncio.run(run())


def test_cty_loader_supports_recent_tx5_tx7_entities(tmp_path) -> None:
    cty_path = tmp_path / "cty.dat"
    cty_path.write_text(
        "Austral Islands: 32: 63: OC: -23.37: 149.48: 10.0: TX5:\n"
        "    TX5EU,TX5N;\n"
        "Clipperton Island: 10: 11: NA: 10.30: 109.22: 8.0: TX5:\n"
        "    TX5S;\n"
        "Marquesas Islands: 31: 63: OC: -9.00: 139.50: 9.5: TX7:\n"
        "    TX7N;\n",
        encoding="ascii",
    )
    load_cty(str(cty_path))
    assert lookup("TX5EU").name == "Austral Islands"
    assert lookup("TX5N").name == "Austral Islands"
    assert lookup("TX5S").name == "Clipperton Island"
    assert lookup("TX7N").name == "Marquesas Islands"


def test_telnet_dataset_status_reloads_updated_cty_file(tmp_path) -> None:
    db = str(tmp_path / "cty_reload.db")
    cty_path = Path(_write_cty(tmp_path))
    body = cty_path.read_text(encoding="ascii")
    cty_path.write_text(body + "VER20260404\n", encoding="ascii")
    cfg = _mk_config(db)
    cfg.public_web.cty_dat_path = str(cty_path)
    store = SpotStore(db)
    srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
    try:
        assert srv._dataset_status()["cty"]["version"] == "VER20260404"
        cty_path.write_text(body + "VER20260414\n", encoding="ascii")
        assert srv._dataset_status()["cty"]["version"] == "VER20260414"
    finally:
        asyncio.run(store.close())


def test_rbn_preferences_and_filter_aliases_apply_to_spots(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "rbn_filters.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        writer = _DummyWriter()
        srv._sessions[1] = Session(call="N0CALL", writer=writer, connected_at=datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            rbn_spot = Spot(14074.0, "K1ABC", now, "CQ TEST 18 dB", "SKIMMER1", "AI3I-16", "")
            other_rbn_spot = Spot(21074.0, "K9DEF", now - 2, "CQ TEST 22 dB", "SKIMMER2", "AI3I-16", "")
            normal_spot = Spot(14074.0, "K1XYZ", now, "FT8", "W1AW", "AI3I-16", "")

            await store.add_spot(rbn_spot)
            await store.add_spot(other_rbn_spot)
            await store.add_spot(normal_spot)
            _, out = await srv._execute_command("N0CALL", "show/dx 10")
            assert "K1ABC" not in out
            assert "K9DEF" not in out
            assert "K1XYZ" in out

            _, out = await srv._execute_command("N0CALL", "unset/rbn")
            assert "RBN set to off for N0CALL." in out
            _, out = await srv._execute_command("N0CALL", "show/dx 10")
            assert "K1ABC" not in out
            assert "K9DEF" not in out
            assert "K1XYZ" in out

            _, out = await srv._execute_command("N0CALL", "set/rbn")
            assert "RBN set to on for N0CALL." in out
            _, out = await srv._execute_command("N0CALL", "show/dx 10")
            assert "K1ABC" not in out
            assert "K9DEF" not in out
            assert "K1XYZ" in out
            _, out = await srv._execute_command("N0CALL", "show/mydx 10")
            assert "K1ABC" not in out
            assert "K9DEF" not in out
            assert "K1XYZ" in out
            _, out = await srv._execute_command("N0CALL", "accept/rbn 1 call K1ABC")
            assert "filter for rbn saved" in out
            rules = await store.list_filter_rules("N0CALL")
            assert any(str(row["family"]) == "rbn" and str(row["expr"]) == "call K1ABC" for row in rules)
            _, out = await srv._execute_command("N0CALL", "accept/rbn callsign K1ABC")
            assert "filter for rbn saved" in out
            rules = await store.list_filter_rules("N0CALL")
            assert any(str(row["family"]) == "rbn" and str(row["expr"]) == "callsign K1ABC" for row in rules)
            _, out = await srv._execute_command("N0CALL", "show/filter test rbn --verbose 14074 K1ABC SKIMMER1 'CQ TEST 18 dB'")
            assert "Decision: allow" in out
            _, out = await srv._execute_command("N0CALL", "show/mydx 10")
            assert "K1ABC" in out
            assert "K9DEF" not in out
            assert "K1XYZ" in out
            _, out = await srv._execute_command("N0CALL", "clear/rbn")
            assert "Cleared" in out and "rbn filters" in out
            _, out = await srv._execute_command("N0CALL", "reject/rbn 2")
            assert "filter for rbn saved" in out

            before = len(writer.buffer)
            await srv.publish_spot(rbn_spot)
            await srv.publish_spot(normal_spot)
            live = bytes(writer.buffer[before:]).decode("utf-8", "replace")
            assert "K1ABC" not in live
            assert "K1XYZ" in live

            _, out = await srv._execute_command("N0CALL", "clear/rbn")
            assert "Cleared" in out and "rbn filters" in out
            before = len(writer.buffer)
            await srv.publish_spot(rbn_spot)
            await srv._flush_rbn_live_queue()
            live = bytes(writer.buffer[before:]).decode("utf-8", "replace")
            assert "K1ABC" in live
        finally:
            await store.close()

    asyncio.run(run())


def test_live_rbn_spots_are_grouped_into_dxspider_style_summary(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "live_rbn_grouped.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        writer = _DummyWriter()
        srv._sessions[1] = Session(call="N0CALL", writer=writer, connected_at=datetime.now(timezone.utc))
        try:
            now = int(datetime(2026, 5, 6, 18, 16, tzinfo=timezone.utc).timestamp())
            await srv._execute_command("N0CALL", "set/rbn")
            samples = [
                Spot(14011.2, "N9JR", now, "CW 6 dB 21 WPM CQ", "WS3W", "RBN", ""),
                Spot(14011.2, "N9JR", now + 2, "CW 17 dB 21 WPM CQ", "W1NT", "RBN", ""),
                Spot(14011.2, "N9JR", now + 62, "CW 36 dB 21 WPM CQ", "KD7EFG", "RBN", ""),
            ]
            before = len(writer.buffer)
            for spot in samples:
                await srv.publish_spot(spot)
            delivered = await srv._flush_rbn_live_queue()
            live = bytes(writer.buffer[before:]).decode("utf-8", "replace")

            assert delivered == 1
            assert live.count("N9JR") == 1
            assert "WS3W-#" in live
            assert "CW 6dB Q:3" in live
            assert "21 WPM CQ" not in live
            assert "W1NT-#" not in live
            assert "KD7EFG-#" not in live
        finally:
            await store.close()

    asyncio.run(run())


def test_live_rbn_command_flush_waits_for_batch_dwell(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "live_rbn_flush_dwell.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        writer = _DummyWriter()
        srv._sessions[1] = Session(call="N0CALL", writer=writer, connected_at=datetime.now(timezone.utc))
        try:
            now = int(datetime(2026, 5, 6, 18, 16, tzinfo=timezone.utc).timestamp())
            await srv._execute_command("N0CALL", "set/rbn")
            await srv.publish_spot(Spot(14011.2, "N9JR", now, "CW 6 dB 21 WPM CQ", "WS3W", "RBN", ""))
            before = len(writer.buffer)

            assert await srv._flush_rbn_live_queue(force=False) == 0
            assert len(writer.buffer) == before
            assert await srv._flush_rbn_live_queue() == 1
            live = bytes(writer.buffer[before:]).decode("utf-8", "replace")
            assert "N9JR" in live
        finally:
            await store.close()

    asyncio.run(run())


def test_live_rbn_group_queue_is_bounded(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "live_rbn_group_cap.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._rbn_live_group_limit = 10
        writer = _DummyWriter()
        session = Session(call="N0CALL", writer=writer, connected_at=datetime.now(timezone.utc))
        srv._sessions[1] = session
        try:
            now = int(datetime(2026, 5, 6, 18, 16, tzinfo=timezone.utc).timestamp())
            for idx in range(25):
                srv._queue_rbn_live_spot(
                    1,
                    session,
                    Spot(14000.0 + idx, f"K{idx}ABC", now + idx, "CW 10 dB 21 WPM CQ", f"W{idx}XYZ-#", "RBN", ""),
                )

            assert len(srv._rbn_live_groups) <= srv._rbn_live_group_limit
            assert all(key[0] == 1 for key in srv._rbn_live_groups)
        finally:
            if srv._rbn_live_flush_task:
                srv._rbn_live_flush_task.cancel()
                try:
                    await srv._rbn_live_flush_task
                except asyncio.CancelledError:
                    pass
            await store.close()

    asyncio.run(run())


def test_live_rbn_summarized_upstream_reports_are_collapsed(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "live_rbn_summary_collapse.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        writer = _DummyWriter()
        srv._sessions[1] = Session(call="N0CALL", writer=writer, connected_at=datetime.now(timezone.utc))
        try:
            now = int(datetime(2026, 7, 8, 18, 36, tzinfo=timezone.utc).timestamp())
            await srv._execute_command("N0CALL", "set/rbn")
            samples = [
                Spot(14005.6, "N9JR", now, "CW 8dB Q:6* Z:5", "NU4F-#", "RBN", ""),
                Spot(14005.6, "N9JR", now + 3, "CW 15dB Q:5* Z:5", "K4PP-#", "RBN", ""),
                Spot(14005.6, "N9JR", now + 8, "CW 23dB Q:3 Z:5", "K5TR-#", "RBN", ""),
                Spot(14005.6, "N9JR", now + 10, "CW 6dB Q:1 Z:5", "W1NT-#", "RBN", ""),
            ]
            before = len(writer.buffer)
            for spot in samples:
                await srv.publish_spot(spot)
            delivered = await srv._flush_rbn_live_queue()
            live = bytes(writer.buffer[before:]).decode("utf-8", "replace")

            assert delivered == 1
            assert live.count("N9JR") == 1
            assert "21 WPM CQ" not in live
            assert "Q:15*" in live
        finally:
            await store.close()

    asyncio.run(run())


def test_legacy_rbn_scoped_spot_rules_do_not_open_full_rbn_stream(tmp_path) -> None:
    async def run() -> None:
        store = SpotStore(str(tmp_path / "legacy_rbn_scope.db"))
        srv = TelnetClusterServer(_mk_config(str(tmp_path / "legacy_rbn_scope.db")), store, datetime.now(timezone.utc))
        now = int(datetime.now(timezone.utc).timestamp())
        try:
            await store.set_filter_rule("N9JR", "spots", "reject", 1, "info _pota_", now)
            await store.set_filter_rule("N9JR", "spots", "accept", 1, "rbn call N9JR", now)
            await store.set_filter_rule("N9JR", "spots", "accept", 4, "by A", now)
            await store.set_filter_rule("N9JR", "spots", "accept", 5, "by V", now)
            await store.set_filter_rule("N9JR", "spots", "accept", 6, "by W", now)
            await store.set_user_pref("N9JR", "rbn", "on", now)

            assert await srv._spot_passes_filters("N9JR", 14024.6, "N9JR", "WZ7I", "CW 8 dB 22 WPM CQ") is True
            assert await srv._spot_passes_filters("N9JR", 14024.6, "W0KO", "WZ7I", "CW 8 dB 22 WPM CQ") is False
            assert await srv._spot_passes_filters("N9JR", 14024.6, "N9JR", "WZ7I", "CW 8 dB _POTA_") is False
            assert await srv._spot_passes_filters("N9JR", 14200.0, "K1ABC", "W1AW", "SSB") is True
        finally:
            await store.close()

    asyncio.run(run())


def test_telnet_password_reader_consumes_option_bytes_after_cr(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "telnet_password_reader.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        reader = asyncio.StreamReader()
        writer = _DummyWriter()
        try:
            reader.feed_data(b"secret\r\xff\xfc\x01\nsecret\r\n")
            first = await srv._read_password(reader, writer)  # type: ignore[arg-type]
            second = await srv._read_password(reader, writer)  # type: ignore[arg-type]
            assert first == "secret"
            assert second == "secret"
        finally:
            await store.close()

    asyncio.run(run())


def test_telnet_password_reader_handles_cr_nul_line_endings(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "telnet_password_reader_crnul.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        reader = asyncio.StreamReader()
        writer = _DummyWriter()
        try:
            reader.feed_data(b"secret\r\0secret\r\0")
            first = await srv._read_password(reader, writer)  # type: ignore[arg-type]
            second = await srv._read_password(reader, writer)  # type: ignore[arg-type]
            assert first == "secret"
            assert second == "secret"
        finally:
            await store.close()

    asyncio.run(run())


def test_telnet_register_command_queues_request_and_sends_notifications(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "telnet_register_command.db")
        cfg = _mk_config(db)
        cfg.smtp.host = "smtp.example.test"
        cfg.smtp.from_addr = "cluster@example.test"
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        sent: list[tuple[str, str, str]] = []
        srv._smtp.send_code = lambda rcpt, subject, body: sent.append((rcpt, subject, body))  # type: ignore[assignment]
        now = int(datetime.now(timezone.utc).timestamp())
        try:
            await store.upsert_user_registry("AI3I", now, privilege="sysop", email="sysop@example.test")
            await store.upsert_user_registry(
                "N1NEW",
                now,
                display_name="New User",
                home_node="W1AW",
                qth="Hartford",
                qra="FN31",
                email="new@example.test",
            )
            await store.set_user_pref("N1NEW", "password", "hash", now)
            await store.set_user_pref("N1NEW", "email_verified_epoch", str(now), now)
            await store.set_user_pref("N1NEW", "forward_lat", "41.7", now)
            await store.set_user_pref("N1NEW", "forward_lon", "-72.7", now)
            await store.set_user_pref("N1NEW", "mfa_email_otp", "off", now)
            keep, out = await srv._execute_command("N1NEW", "register")
            assert keep is True
            assert "Registration request submitted for N1NEW" in out
            req = await store.get_registration_request("N1NEW")
            assert req is not None
            assert str(req["status"]) == "pending"
            assert str(req["source"]) == "telnet"
            assert int(req["email_verified"]) == 1
            assert any(rcpt == "sysop@example.test" for rcpt, _subject, _body in sent)
            assert any(rcpt == "new@example.test" for rcpt, _subject, _body in sent)

            _keep, out = await srv._execute_command("N1NEW", "register")
            assert "already pending" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_telnet_register_verifies_email_before_sysop_queue(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "telnet_register_verify_before_queue.db")
        cfg = _mk_config(db)
        cfg.smtp.host = "smtp.example.test"
        cfg.smtp.from_addr = "cluster@example.test"
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        sent: list[tuple[str, str, str]] = []
        srv._smtp.send_code = lambda rcpt, subject, body: sent.append((rcpt, subject, body))  # type: ignore[assignment]
        srv._mfa._sender = srv._smtp.send_code
        now = int(datetime.now(timezone.utc).timestamp())
        try:
            await store.upsert_user_registry("AI3I", now, privilege="sysop", email="sysop@example.test")
            await store.upsert_user_registry(
                "N1NEW",
                now,
                display_name="New User",
                home_node="W1AW",
                qth="Hartford",
                qra="FN31",
                email="new@example.test",
            )
            await store.set_user_pref("N1NEW", "password", "hash", now)

            keep, out = await srv._execute_command("N1NEW", "register")
            assert keep is True
            assert "verification code has been sent" in out
            assert "REGISTER <code>" in out
            assert await store.get_registration_request("N1NEW") is None
            assert sent and sent[-1][0] == "new@example.test"
            assert not any(rcpt == "sysop@example.test" for rcpt, _subject, _body in sent)

            challenge_id = str(await store.get_user_pref("N1NEW", "registration_verify_challenge_id") or "")
            challenge = await store.get_mfa_challenge(challenge_id)
            assert challenge is not None
            keep, out = await srv._execute_command("N1NEW", f"register {challenge['code']}")
            assert keep is True
            assert "Registration request submitted for N1NEW" in out
            req = await store.get_registration_request("N1NEW")
            assert req is not None
            assert int(req["email_verified"]) == 1
            assert any(rcpt == "sysop@example.test" for rcpt, _subject, _body in sent)
        finally:
            await store.close()

    asyncio.run(run())


def test_telnet_register_rejects_invalid_callsign_before_registry_create(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "telnet_register_invalid_call.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        try:
            keep, out = await srv._execute_command("JOHN", "register")
            assert keep is True
            assert "Invalid callsign for self-registration: JOHN." in out
            assert await store.get_user_registry("JOHN") is None
            assert await store.get_registration_request("JOHN") is None
            keep, out = await srv._execute_command("JOHN1A", "register")
            assert keep is True
            assert "Invalid callsign for self-registration: JOHN1A." in out
            assert await store.get_user_registry("JOHN1A") is None
            assert await store.get_registration_request("JOHN1A") is None
        finally:
            await store.close()

    asyncio.run(run())


def test_telnet_register_expired_code_points_user_back_to_register(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "telnet_register_expired_code.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        now = int(datetime.now(timezone.utc).timestamp())
        try:
            await store.upsert_user_registry("N1NEW", now, email="new@example.test")
            await store.save_mfa_challenge(
                challenge_id="expired-registration",
                call="N1NEW",
                purpose="registration-approval",
                code="123456",
                expires_epoch=now - 60,
                attempts_left=5,
                issued_epoch=now - 600,
            )
            await store.set_user_pref("N1NEW", "registration_verify_challenge_id", "expired-registration", now)

            out = await srv._verify_approved_registration("N1NEW", "123456")
            assert "Verification code expired. Run REGISTER again to request a new code." in out
        finally:
            await store.close()

    asyncio.run(run())


def test_telnet_register_interactively_collects_required_profile(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "telnet_register_interactive.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        reader = asyncio.StreamReader()
        reader.feed_data(
            b"Joe Operator\r\nN0NODE-1\r\nMilwaukee, WI\r\nEN63AA\r\njoe@example.test\r\nsecret-pass\r\nsecret-pass\r\n"
        )
        writer = _DummyWriter()
        try:
            output = await srv._run_register_interactive("N1NEW", reader, writer)  # type: ignore[arg-type]
            assert "Registration request submitted for N1NEW" in output
            row = await store.get_user_registry("N1NEW")
            assert row is not None
            assert str(row["display_name"]) == "Joe Operator"
            assert str(row["home_node"]) == "N0NODE-1"
            assert str(row["qth"]) == "Milwaukee, WI"
            assert str(row["qra"]) == "EN63AA"
            assert str(row["email"]) == "joe@example.test"
            assert verify_password("secret-pass", str(await store.get_user_pref("N1NEW", "password") or ""))
            req = await store.get_registration_request("N1NEW")
            assert req is not None and str(req["status"]) == "pending"
        finally:
            await store.close()

    asyncio.run(run())


def test_registration_notice_is_suppressed_after_approval_or_verification(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "telnet_registration_notice.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        now = int(datetime.now(timezone.utc).timestamp())
        try:
            await store.upsert_user_registry("N1NEW", now, email="new@example.test")
            text = await srv._registration_notice_block("N1NEW", node_family="")
            assert "run REGISTER" in text
            await store.upsert_registration_request(
                "N1NEW",
                now,
                email="new@example.test",
                source="telnet",
                status="approved",
            )
            text = await srv._registration_notice_block("N1NEW", node_family="")
            assert "run REGISTER" not in text

            await store.upsert_user_registry("N1VER", now, email="verified@example.test")
            await mark_email_verified(store, "N1VER", now_epoch=now)
            text = await srv._registration_notice_block("N1VER", node_family="")
            assert text == ""
        finally:
            await store.close()

    asyncio.run(run())


def test_show_rbn_reports_recent_skimmer_hits_for_call(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "show_rbn.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        try:
            now = int(datetime(2026, 5, 6, 0, 52, tzinfo=timezone.utc).timestamp())
            await store.add_spot(Spot(7007.0, "N9JR", now, "CW 39dB Q:2 Z:4", "KO4BHX-#", "N9JR-2", ""))
            await store.add_spot(Spot(7007.0, "N9JR", now + 3, "CW 8 dB 22 WPM CQ", "N2CR", "N9JR-2", ""))
            await store.add_spot(Spot(7074.0, "N9JR", now + 1, "FT8", "W1AW", "N9JR-2", ""))
            await store.add_spot(Spot(7007.0, "K1ABC", now + 2, "CW 22dB", "KO4BHX-#", "N9JR-2", ""))

            _, out = await srv._execute_command("N9JR-5", "show/rbn")

            assert "RBN reports for N9JR (off):" in out
            assert "6-May-2026 0052Z" in out
            assert "7007.0" in out
            assert "N2CR-#" in out
            assert "CW 8dB Q:2 Z:4" in out
            assert "KO4BHX-#" not in out
            assert "W1AW" not in out
            assert "K1ABC" not in out

            _, out = await srv._execute_command("N0CALL", "show/rbn N9JR")
            assert "RBN reports for N9JR (off):" in out
            assert "N2CR-#" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_dxstats_hfstats_vhfstats(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "show_dxstats.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            from pycluster.models import parse_spot_record

            await store.add_spot(
                parse_spot_record("14074.0^K1ABC^1772335320^FT8^N0CALL^226^226^N2WQ-1^8^5^7^4^^^127.0.0.1")
            )
            await store.add_spot(
                parse_spot_record("144200.0^W1AW^1772335330^SSB^N0CALL^226^226^N2WQ-1^8^5^7^4^^^127.0.0.1")
            )

            _, out = await srv._execute_command("N0CALL", "show/dxstats")
            assert "DX summary:" in out
            assert "Total: 2" in out
            assert "HF: 1" in out and "VHF: 1" in out

            _, out = await srv._execute_command("N0CALL", "show/hfstats")
            assert "HF DX summary:" in out
            assert "Total: 1" in out
            _, out = await srv._execute_command("N0CALL", "show/vhfstats")
            assert "VHF DX summary:" in out
            assert "Total: 1" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_contest_satellite_and_425(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "show_contest_sat_425.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            from pycluster.models import parse_spot_record
            now = int(datetime.now(timezone.utc).timestamp())
            await store.upsert_user_registry("N0CALL", now, privilege="user")

            await store.add_spot(
                parse_spot_record("145990.0^AO-91^1772335320^SAT FM^N0CALL^226^226^N2WQ-1^8^5^7^4^^^127.0.0.1")
            )
            await store.add_spot(
                parse_spot_record("14074.0^K1ABC^1772335330^FT8^N0CALL^226^226^N2WQ-1^8^5^7^4^^^127.0.0.1")
            )
            await srv._execute_command("N0CALL", "announce full weekend contest starts")
            await srv._execute_command("N0CALL", "announce full routine ops note")

            _, out = await srv._execute_command("N0CALL", "show/satellite")
            assert "AO-91" in out
            assert "K1ABC" not in out

            keps = tmp_path / "keps.txt"
            keps.write_text(
                "ISS (ZARYA)\n"
                "1 25544U 98067A   24100.50000000  .00016717  00000+0  10270-3 0  9000\n"
                "2 25544  51.6400 120.0000 0005000  40.0000 320.0000 15.50000000  9000\n",
                encoding="ascii",
            )
            cfg.satellite.keps_path = str(keps)
            cfg.satellite.prediction_hours = 168
            cfg.satellite.pass_step_seconds = 300
            await srv._execute_command("N0CALL", "set/qra FN31PR")
            _, out = await srv._execute_command("N0CALL", "show/satellite ISS")
            assert "Satellite passes for ISS (ZARYA)" in out
            assert "AOS UTC" in out
            assert "MaxEl" in out

            _, out = await srv._execute_command("N0CALL", "show/contest")
            assert "contest starts" in out
            assert "routine ops note" not in out

            _, out = await srv._execute_command("N0CALL", "show/425")
            assert "AO-91" in out or "K1ABC" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_qra_apropos_and_notimpl(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "show_misc.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv._execute_command("N0CALL", "show/qra")
            assert "QRA is not set for N0CALL." in out

            _, out = await srv._execute_command("N0CALL", "set/qra FN42")
            assert "QRA set to FN42 for N0CALL." in out
            _, out = await srv._execute_command("N0CALL", "show/qra")
            assert "QRA for N0CALL: FN42" in out

            _, out = await srv._execute_command("N0CALL", "show/apropos startup")
            assert "startup" in out
            assert "sysop/" not in out
            _, out = await srv._execute_command("N0CALL", "show/apropos sysop")
            assert "sysop/" not in out

            _, out = await srv._execute_command("N0CALL", "show/notimpl")
            assert "Not-implemented commands" in out
            assert "clear/dupefile" not in out

            _, out = await srv._execute_command("N0CALL", "show/commands route")
            assert "show/route" in out
            assert "accept/route" in out
            _, out = await srv._execute_command("N0CALL", "show/commands set")
            assert "SET commands" in out
            assert "set/qra" in out
            assert "set/homenode" in out
            _, out = await srv._execute_command("N0CALL", "set")
            assert "SET commands" in out
            _, out = await srv._execute_command("N0CALL", "commands startup")
            assert "startup" in out
            assert "Unset startup." in out or "Set startup." in out
            _, out = await srv._execute_command("N0CALL", "show/commands sendconf")
            assert "send_config" not in out
            now = int(datetime.now(timezone.utc).timestamp())
            await store.upsert_user_registry("N0CALL", now, privilege="sysop")
            _, out = await srv._execute_command("N0CALL", "show/commands sendconf")
            assert "send_config" in out
            _, out = await srv._execute_command("N0CALL", "show/capabilities")
            assert "Command capabilities:" in out
            assert "Commands:" in out
            assert "Show:" in out and "Set:" in out and "Stat:" in out

            _, out = await srv._execute_command("N0CALL", "show/sun")
            assert "Reference: QRA FN42" in out and "Solar Hour:" in out and "Phase:" in out
            _, out = await srv._execute_command("N0CALL", "show/grayline")
            assert "Reference: QRA FN42" in out and "Grayline status:" in out and ("sunrise in" in out or "sunset in" in out)
            _, out = await srv._execute_command("N0CALL", "show/moon")
            assert "Reference: QRA FN42" in out and "Age:" in out and "Illumination:" in out
            assert "Elevation:" in out and "Azimuth:" in out
            assert "Moonrise:" in out and "Moonset:" in out
            _, out = await srv._execute_command("N0CALL", "show/heading G")
            assert "Heading to " in out or "No heading data for G." in out
            if "Heading to " in out:
                assert "Reference: QRA FN42" in out or "Reference: location " in out

            _, out = await srv._execute_command("N0CALL", "show/muf")
            assert "MUF estimate unavailable" in out
            await srv._execute_command("N0CALL", "wwv SFI=150 A=6 K=2")
            _, out = await srv._execute_command("N0CALL", "show/muf")
            assert "SFI: 150" in out and "Estimated MUF3000:" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_moon_supports_dxcc_target_with_rise_set_and_elevation(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "show_moon_target.db")
        cty_path = _write_cty(tmp_path)
        cfg = AppConfig(
            node=NodeConfig(node_call="AI3I-16", node_locator="FN20"),
            telnet=TelnetConfig(),
            web=WebConfig(),
            public_web=PublicWebConfig(cty_dat_path=cty_path),
            store=StoreConfig(sqlite_path=db),
        )
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        try:
            _, out = await srv._execute_command("N0CALL", "sh/moon KH6")
            assert "Moon status:" in out
            assert "Reference: Hawaii" in out
            assert "Elevation:" in out and "Azimuth:" in out
            assert "Moonrise:" in out and "Moonset:" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_dxspider_shorthand_slash_forms_and_muf_history(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "shorthand_slash_forms.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.add_spot(Spot(24915.0, "K1ABC", now, "FT8", "N0CALL", "N2WQ-1", ""))
            await store.add_spot(Spot(14074.0, "W1AW", now, "FT8", "N0CALL", "N2WQ-1", ""))
            await store.add_bulletin("wcy", "DK0WCY", "LOCAL", now, "SFI=162 A=16 K=4 spots=121 expk=0 aurora=act xray=act storm=no")
            await store.add_bulletin("wwv", "WWV", "LOCAL", now, "SFI=150 A=6 K=2 No Storms -> No Storms")
            await store.add_bulletin("wwv", "WWV", "LOCAL", now - 3600, "SFI=145 A=5 K=1 Quiet")

            _, out = await srv._execute_command("N0CALL", "sh/dx/1 on 12m")
            assert "K1ABC" in out
            assert "W1AW" not in out

            _, out = await srv._execute_command("N0CALL", "sh/wcy/1")
            assert "Date        Hour   SFI   A   K Exp.K" in out
            assert "DK0WCY" in out

            _, out = await srv._execute_command("N0CALL", "sh/wwv/2")
            assert "Date        Hour   SFI   A   K Forecast" in out
            assert "No Storms -> No Storms" in out
            assert "Quiet" in out

            _, out = await srv._execute_command("N0CALL", "sh/muf 2")
            assert "Date        Hour   SFI   A   K MUF3000" in out
            assert "150" in out
            assert "145" in out

            _, out = await srv._execute_command("N0CALL", "sh/muf/2 l")
            assert "Date        Hour   SFI   A   K MUF3000 Forecast" in out
            assert "No Storms -> No Storms" in out
            assert "<WWV>" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_wwv_defaults_to_recent_20_rows(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "show_wwv_limit.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            for idx in range(25):
                await store.add_bulletin("wwv", "WWV", "LOCAL", now - idx * 60, f"SFI={120 + idx} A=5 K=2 Quiet")
            _, out = await srv._execute_command("N0CALL", "show/wwv")
            assert out.count("<WWV>") == 20
            assert "SFI=144" not in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_muf_dxspider_style_path_report(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "muf_dxspider_path.db")
        cty_path = _write_cty(tmp_path)
        wpx_path = _write_wpxloc(tmp_path)
        cfg = AppConfig(
            node=NodeConfig(node_call="AI3I-16", node_locator="FN20"),
            telnet=TelnetConfig(),
            web=WebConfig(),
            public_web=PublicWebConfig(cty_dat_path=cty_path, wpxloc_raw_path=wpx_path),
            store=StoreConfig(sqlite_path=db),
        )
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.upsert_user_registry("N0CALL", now, qra="FN42")
            await store.add_bulletin("wwv", "WWV", "LOCAL", now, "SFI=150 A=6 K=2 Quiet")
            await store.add_bulletin("wwv", "WWV", "LOCAL", now - 3600, "SFI=145 A=5 K=1 No Storms")

            _, out = await srv._execute_command("N0CALL", "show/muf K 2")
            assert "RxSens: -128 dBM SFI:" in out
            assert "Power :   26 dBW" in out
            assert "Location                       Lat / Long           Azim" in out
            assert "United States" in out
            assert "UT LT  MUF Zen" in out

            _, out = await srv._execute_command("N0CALL", "show/muf RG65SM 2 long")
            assert "RxSens: -128 dBM SFI:" in out
            assert "Location                       Lat / Long           Azim" in out
            assert "European Russia" in out
            assert "UT LT  MUF Zen" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_muf_path_uses_midpoint_solar_zenith_for_signal_estimates(tmp_path) -> None:
    db = str(tmp_path / "muf_midpoint_zenith.db")
    cfg = _mk_config(db)
    store = SpotStore(db)
    srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
    try:
        lat, lon = srv._path_midpoint(42.0, -71.0, 39.0, -98.0)
        noon_utc = datetime(2026, 6, 21, 18, 0, tzinfo=timezone.utc)
        midnight_utc = datetime(2026, 6, 21, 6, 0, tzinfo=timezone.utc)

        zen_noon = srv._solar_zenith_angle(noon_utc, lat, lon)
        zen_midnight = srv._solar_zenith_angle(midnight_utc, lat, lon)

        assert zen_noon < zen_midnight
        assert srv._effective_muf_for_zenith(26.0, zen_noon) > srv._effective_muf_for_zenith(26.0, zen_midnight)
        assert srv._signal_report_for_muf(14.0, 26.0, zen_noon)
        assert srv._signal_report_for_muf(14.0, 26.0, zen_midnight) == ""
        assert srv._signal_report_for_muf(1.8, 26.0, zen_noon) == ""
        assert srv._signal_report_for_muf(3.5, 26.0, zen_noon) == ""
        assert srv._signal_report_for_muf(1.8, 26.0, zen_midnight, (zen_midnight, zen_noon)) == ""
    finally:
        asyncio.run(store.close())


def test_show_wcy_falls_back_to_derived_wwv_when_no_wcy_entries(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "show_wcy_derived_from_wwv.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.add_bulletin("wwv", "VE7CC", "LOCAL", now, "SFI=108 A=4 K=1 No Storms -> Moderate w/G2")
            _, out = await srv._execute_command("N0CALL", "show/wcy")
            assert "Derived from WWV feed" in out
            assert "Date        Hour   SFI   A   K Exp.K" in out
            assert "<VE7CC>" in out
            assert "108" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_wcy_derived_fallback_ignores_removed_wcy_wwv_filters(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "show_wcy_derived_filters.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.add_bulletin("wwv", "VE7CC", "LOCAL", now, "SFI=108 A=4 K=1 No Storms -> Moderate w/G2")

            _, out = await srv._execute_command("N0CALL", "accept/wcy 1 by VE7")
            assert out.strip() == "?"
            _, out = await srv._execute_command("N0CALL", "accept/wwv 1 by W1")
            assert out.strip() == "?"

            _, out = await srv._execute_command("N0CALL", "show/wcy")
            assert "Derived from WWV feed" in out
            assert "VE7CC" in out

            _, out = await srv._execute_command("N0CALL", "show/wwv")
            assert "VE7CC" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_help_and_nowrap_behavior(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "help_nowrap.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv._execute_command("N0CALL", "help")
            assert "set/nowrap" in out
            assert "show/links" in out
            assert "apropos route" in out
            assert "System Operator:" not in out
            assert max(len(line) for line in out.splitlines() if line) <= 80

            _, out = await srv._execute_command("N0CALL", "set/nowrap")
            assert "Line wrapping disabled" in out
            _, out = await srv._execute_command("N0CALL", "help")
            assert "Everyday commands:" in out
            assert "set/nowrap     disable 80-column wrapping" in out

            _, out = await srv._execute_command("N0CALL", "unset/nowrap")
            assert "Line wrapping restored" in out

            now = int(datetime.now(timezone.utc).timestamp())
            await store.upsert_user_registry("N0CALL", now, privilege="sysop")
            _, out = await srv._execute_command("N0CALL", "help")
            assert "System Operator:" in out
            assert "sysop/users" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_dup_controls_and_clear_dupefile(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "dupe_ctl.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv._execute_command("N0CALL", "set/dupspots")
            assert "Duplicate Spots set to on for N0CALL." in out
            assert await store.spot_dupe_enabled() is True
            _, out = await srv._execute_command("N0CALL", "set/dupann")
            assert "Duplicate Ann set to on for N0CALL." in out

            _, out = await srv._execute_command("N0CALL", "show/dupspots")
            assert "Duplicate Spots for N0CALL: on" in out
            _, out = await srv._execute_command("N0CALL", "show/dupann")
            assert "Duplicate Ann for N0CALL: on" in out

            _, out = await srv._execute_command("N0CALL", "clear/dupefile")
            assert "Duplicate spot tracking reset" in out
            assert await store.spot_dupe_enabled() is False
            _, out = await srv._execute_command("N0CALL", "show/dupspots")
            assert "Duplicate Spots for N0CALL: off" in out
            _, out = await srv._execute_command("N0CALL", "show/dupann")
            assert "Duplicate Ann for N0CALL: off" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_named_status_target_call_requires_sysop(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "show_named_target.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        await store.set_user_pref("K1ABC", "talk", "off", now)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            _, out = await srv._execute_command("N0CALL", "show/talk K1ABC")
            assert "permission denied" in out
            await store.set_user_pref("N0CALL", "privilege", "sysop", now)
            _, out = await srv._execute_command("N0CALL", "show/talk K1ABC")
            assert "TALK for K1ABC: off" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_set_named_var_validation_and_normalization(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "set_named_validation.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            _, out = await srv._execute_command("N0CALL", "set/debug yes")
            assert "Debug set to on for N0CALL." in out
            _, out = await srv._execute_command("N0CALL", "set/debug 0")
            assert "Debug set to off for N0CALL." in out

            _, out = await srv._execute_command("N0CALL", "set/pinginterval nope")
            assert "Usage: set/pinginterval <integer>" in out
            _, out = await srv._execute_command("N0CALL", "set/pinginterval 2")
            assert "Ping Interval set to 5 for N0CALL." in out
            _, out = await srv._execute_command("N0CALL", "set/obscount 50000")
            assert "Obscount set to 9999 for N0CALL." in out
        finally:
            await store.close()

    asyncio.run(run())


def test_connect_disconnect_links_commands(tmp_path) -> None:
    calls: list[tuple[str, str]] = []
    peers = {"peer1": True}

    async def _connect(peer: str, dsn: str) -> None:
        calls.append((peer, dsn))

    async def _disconnect(peer: str) -> bool:
        return peer in peers

    async def _stats():
        return {
            "peer1": {
                "parsed_frames": 12,
                "sent_frames": 8,
                "dropped_frames": 0,
                "policy_dropped": 1,
                "policy_reasons": {"route_filter": 1, "relay_peer_chat_disabled": 2},
                "profile": "spider",
                "inbound": False,
                "last_pc_type": "PC92",
                "last_rx_epoch": int(datetime.now(timezone.utc).timestamp()),
            }
        }

    async def _desired():
        return [
            {
                "peer": "peer1",
                "profile": "spider",
                "connected": True,
                "desired": True,
                "last_connect_epoch": int(datetime.now(timezone.utc).timestamp()),
            }
        ]

    async def run() -> None:
        db = str(tmp_path / "conn.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc18.family", "spider", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer1.pc18.summary", "DXSpider 1.57 build 633", now)
        srv = TelnetClusterServer(
            cfg,
            store,
            datetime.now(timezone.utc),
            link_stats_fn=_stats,
            link_desired_peers_fn=_desired,
            link_connect_fn=_connect,
            link_disconnect_fn=_disconnect,
        )
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv._execute_command("N0CALL", "connect peer1 127.0.0.1 7300")
            assert "Connection attempt started for peer1" in out
            assert calls[-1] == ("peer1", "tcp://127.0.0.1:7300")

            _, out = await srv._execute_command("N0CALL", "links")
            assert "peer1" in out
            assert "Peer         Family" in out
            assert "DXSpider 1.57 build 633" in out

            _, out = await srv._execute_command("N0CALL", "show/connect")
            assert "policy dropped 1" in out
            assert "profile spider" in out

            _, out = await srv._execute_command("N0CALL", "show/route")
            assert "RX     12" in out and "TX      8" in out and "Last PC92" in out
            assert "Reasons: relay_peer_chat_disabled 2, route_filter 1" in out

            _, out = await srv._execute_command("N0CALL", "show/policydrop")
            assert "Policy drop reasons:" in out
            assert "peer1: total 1" in out
            assert "relay_peer_chat_disabled: 2" in out
            assert "route_filter: 1" in out

            _, out = await srv._execute_command("N0CALL", "show/policydrop peer1")
            assert "peer1: total 1" in out
            _, out = await srv._execute_command("N0CALL", "show/policydrop missing")
            assert "No policy drop data for peer filter 'missing'" in out

            _, out = await srv._execute_command("N0CALL", "show/hops")
            assert "Hop metrics (1):" in out
            assert "hop metric" in out and "policy drops" in out

            _, out = await srv._execute_command("N0CALL", "stat/route")
            assert "There is 1 live peer link right now." in out

            _, out = await srv._execute_command("N0CALL", "disconnect peer1")
            assert "Disconnected peer1." in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_policydrop_reset_requires_sysop_and_clears_counts(tmp_path) -> None:
    stats_data = {
        "peer1": {
            "parsed_frames": 12,
            "sent_frames": 8,
            "dropped_frames": 0,
            "policy_dropped": 3,
            "policy_reasons": {"route_filter": 2, "relay_peer_chat_disabled": 1},
            "profile": "spider",
            "inbound": False,
            "last_pc_type": "PC92",
        },
        "peer2": {
            "parsed_frames": 4,
            "sent_frames": 3,
            "dropped_frames": 0,
            "policy_dropped": 1,
            "policy_reasons": {"profile_tx_block": 1},
            "profile": "dxnet",
            "inbound": False,
            "last_pc_type": "PC61",
        },
    }

    async def _stats():
        return stats_data

    async def _clear(peer_filter: str | None) -> int:
        flt = (peer_filter or "").lower()
        n = 0
        for name, st in stats_data.items():
            if flt and flt not in name.lower():
                continue
            if int(st.get("policy_dropped", 0)) <= 0 and not st.get("policy_reasons"):
                continue
            st["policy_dropped"] = 0
            st["policy_reasons"] = {}
            n += 1
        return n

    async def run() -> None:
        db = str(tmp_path / "policydrop_reset.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(
            cfg,
            store,
            datetime.now(timezone.utc),
            link_stats_fn=_stats,
            link_clear_policy_fn=_clear,
        )
        sess = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        srv._sessions[1] = sess
        try:
            _, out = await srv._execute_command("N0CALL", "show/policydrop")
            assert "peer1: total 3" in out
            assert "peer2: total 1" in out

            _, out = await srv._execute_command("N0CALL", "show/policydrop --reset peer1")
            assert "requires sysop" in out

            sess.vars["privilege"] = "sysop"
            _, out = await srv._execute_command("N0CALL", "show/policydrop --reset")
            assert "requires <peer> or all|a|*" in out
            _, out = await srv._execute_command("N0CALL", "show/policydrop --reset peer1")
            assert "Policy drop counters reset for 1 peer(s) matching peer1." in out

            _, out = await srv._execute_command("N0CALL", "show/policydrop --reset a")
            assert "Policy drop counters reset for 1 peer(s)." in out

            _, out = await srv._execute_command("N0CALL", "show/policydrop peer1")
            assert "No policy drop data for peer filter 'peer1'" in out
            _, out = await srv._execute_command("N0CALL", "show/policydrop")
            assert "No policy drop data" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_misc_top_level_and_bulletin_commands(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "misc.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        w1 = _DummyWriter()
        w2 = _DummyWriter()
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=w1,
            connected_at=datetime.now(timezone.utc),
        )
        srv._sessions[2] = Session(
            call="K1ABC",
            writer=w2,
            connected_at=datetime.now(timezone.utc),
        )
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.upsert_user_registry("N0CALL", now, privilege="user")
            _, out = await srv._execute_command("N0CALL", "show/version")
            assert "pyCluster version" in out
            assert "Author: John D. Lewis (AI3I)" in out
            assert "Project: https://github.com/AI3I/pyCluster" in out
            _, out = await srv._execute_command("N0CALL", "show/spout")
            assert "The itsy bitsy DXSpider climbed up the telnet spout," in out
            assert "Out came pyCluster and dried up all the bugs," in out
            _, out = await srv._execute_command("N0CALL", "show/n9jr")
            assert "In Honor of Mr. Joseph E. (Joe) Reed, N9JR:" in out
            assert "Behind every polished piece of software stands someone" in out
            assert "Thank you, Joe." in out
            _, out = await srv._execute_command("N0CALL", "show/ai3i")
            assert "A Word About the Author - John D. Lewis, AI3I:" in out
            assert "he was the one to build it." in out
            assert "assuming the cluster is up." in out
            _, out = await srv._execute_command("N0CALL", "ping K1ABC")
            assert "PONG K1ABC" in out

            _, out = await srv._execute_command("N0CALL", "who")
            assert "N0CALL" in out and "K1ABC" in out

            _, out = await srv._execute_command("N0CALL", "status")
            assert "local /" in out and "Uptime" in out
            _, out = await srv._execute_command("N0CALL", "uptime")
            assert "Uptime:" in out and "Started:" in out
            _, out = await srv._execute_command("N0CALL", "show/uptime")
            assert "Uptime:" in out and "Now:" in out

            _, out = await srv._execute_command("N0CALL", "chat test room")
            assert "Chat delivered to 1 session(s)." in out
            assert b"CHAT N0CALL: test room" in bytes(w2.buffer)

            _, out = await srv._execute_command("N0CALL", "wcy K=3 A=8")
            assert "WCY accepted" in out
            _, out = await srv._execute_command("N0CALL", "wwv SFI=150")
            assert "WWV accepted" in out
            _, out = await srv._execute_command("N0CALL", "wx here 72F")
            assert "accepted (local-safe)" in out
            _, out = await srv._execute_command("N0CALL", "announce full ops notice")
            assert "announce/full accepted" in out

            _, out = await srv._execute_command("N0CALL", "show/wcy")
            assert "K=3 A=8" in out
            _, out = await srv._execute_command("N0CALL", "show/wwv")
            assert "SFI=150" in out
            _, out = await srv._execute_command("N0CALL", "show/wx")
            assert "here 72F" in out
            _, out = await srv._execute_command("N0CALL", "show/announce")
            assert "[FULL]" in out
            assert "ops notice" in out

            _, out = await srv._execute_command("N0CALL", "apropos route")
            assert "show/route" in out
            _, out = await srv._execute_command("N0CALL", "show/stats")
            assert "Runtime summary:" in out
            assert "Users:" in out and "Spots:" in out
            assert "Messages:" in out and "Peers:" in out and "Policy Drops:" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_bulletins_persist_across_server_restart(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "persist.db")
        cfg = _mk_config(db)
        store1 = SpotStore(db)
        srv1 = TelnetClusterServer(cfg, store1, datetime.now(timezone.utc))
        srv1._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv1._execute_command("N0CALL", "wcy A=12 K=4")
            assert "WCY accepted" in out
        finally:
            await store1.close()

        store2 = SpotStore(db)
        srv2 = TelnetClusterServer(cfg, store2, datetime.now(timezone.utc))
        srv2._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv2._execute_command("N0CALL", "show/wcy")
            assert "A=12 K=4" in out
        finally:
            await store2.close()

    asyncio.run(run())


def test_dxspider_wcy_and_wwv_command_syntax_is_canonicalized(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "dxspider_geomag_cmds.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            _, out = await srv._execute_command("N0CALL", "wcy k=3,expk=2,a=18,r=105,sf=120,sa=qui,gmf=maj,au=no")
            assert "WCY accepted" in out
            _, out = await srv._execute_command("N0CALL", "wwv sf=120,a=24,k=4,Moderate w/G2 -> Minor w/G1")
            assert "WWV accepted" in out

            rows = await store.list_bulletins("wcy", limit=1)
            assert str(rows[0]["body"]) == "SFI=120 A=18 K=3 ExpK=2 R=105 SA=qui GMF=maj Aurora=no"
            rows = await store.list_bulletins("wwv", limit=1)
            assert str(rows[0]["body"]) == "SFI=120 A=24 K=4 Moderate w/G2 -> Minor w/G1"

            _, out = await srv._execute_command("N0CALL", "show/wcy 1")
            assert "Date        Hour   SFI   A   K Exp.K   R SA    GMF   Aurora   Logger" in out
            assert "105 qui" in out
            _, out = await srv._execute_command("N0CALL", "show/wwv 1")
            assert out.startswith("\r\nDate")
            assert "Date        Hour   SFI   A   K Forecast" in out
            assert "Moderate w/G2 -> Minor w/G1" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_set_unset_and_extended_group_families(tmp_path, monkeypatch) -> None:
    async def run() -> None:
        db = str(tmp_path / "ext.db")
        cfg = _mk_config(db)
        cfg.satellite.keps_path = str(tmp_path / "downloaded-keps.txt")
        monkeypatch.setattr(telnet_server_mod, "_download_text_url", lambda _url: _SAMPLE_TLE)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            await store.set_user_pref("N0CALL", "privilege", "sysop", int(datetime.now(timezone.utc).timestamp()))
            _, out = await srv._execute_command("N0CALL", "set/talk")
            assert "Talk set to on for N0CALL." in out
            _, out = await srv._execute_command("N0CALL", "show/talk")
            assert "TALK for N0CALL: on" in out
            _, out = await srv._execute_command("N0CALL", "unset/talk")
            assert "Talk set to off for N0CALL." in out

            _, out = await srv._execute_command("N0CALL", "set/qra FN42")
            assert "QRA set to FN42 for N0CALL." in out
            _, out = await srv._execute_command("N0CALL", "show/station")
            assert "Grid Square (QRA): FN42" in out

            _, out = await srv._execute_command("N0CALL", "create/user W1AW")
            assert "User record created for W1AW." in out
            _, out = await srv._execute_command("N0CALL", "delete/user W1AW")
            assert "User W1AW removed." in out
            _, out = await srv._execute_command("N0CALL", "forward/latlong 42 -71")
            assert "Forward latitude/longitude set to" in out
            _, out = await srv._execute_command("N0CALL", "get/keps")
            assert "Keplerian elements request accepted." in out
            assert (tmp_path / "downloaded-keps.txt").exists()
        finally:
            await store.close()

    asyncio.run(run())


def test_forward_commands_persist(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "forward.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv._execute_command("N0CALL", "forward/latlong 42.123 -71.456")
            assert "Forward latitude/longitude set to" in out
            assert await store.get_user_pref("N0CALL", "forward_lat") == "42.1230"
            assert await store.get_user_pref("N0CALL", "forward_lon") == "-71.4560"

            _, out = await srv._execute_command("N0CALL", "forward/opername Jane Doe")
            assert "Forward operator name set to Jane Doe." in out
            assert await store.get_user_pref("N0CALL", "forward_opername") == "Jane Doe"
        finally:
            await store.close()

    asyncio.run(run())


def test_top_level_compat_batch_commands(tmp_path, monkeypatch) -> None:
    async def run() -> None:
        db = str(tmp_path / "topcompat.db")
        cfg = _mk_config(db)
        cfg.satellite.keps_path = str(tmp_path / "topcompat-keps.txt")
        monkeypatch.setattr(telnet_server_mod, "_download_text_url", lambda _url: _SAMPLE_TLE)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv._execute_command("N0CALL", "agwrestart")
            assert "permission denied" in out
            await store.set_user_pref("N0CALL", "privilege", "sysop", int(datetime.now(timezone.utc).timestamp()))
            _, out = await srv._execute_command("N0CALL", "agwrestart")
            assert "AGW restart requested at" in out
            assert "Restart request count: 1." in out

            _, out = await srv._execute_command("N0CALL", "dbcreate")
            assert "Database structures verified:" in out
            _, out = await srv._execute_command("N0CALL", "dbupdate")
            assert "Database refresh complete:" in out

            dump = str(tmp_path / "dump.sql")
            _, out = await srv._execute_command("N0CALL", f"dbexport {dump}")
            assert "Database export written to" in out
            assert "Export size:" in out
            assert "dump.sql" in out
            assert Path(dump).exists()

            users_csv = str(tmp_path / "users.csv")
            _, out = await srv._execute_command("N0CALL", f"export_users {users_csv}")
            assert "User export written to" in out
            assert "Exported " in out
            assert Path(users_csv).exists()

            _, out = await srv._execute_command("N0CALL", "send_config")
            assert "Node configuration:" in out
            assert "Node Call:" in out
            cfg_out = str(tmp_path / "config.out")
            _, out = await srv._execute_command("N0CALL", f"send_config {cfg_out}")
            assert "Configuration snapshot written to" in out
            assert Path(cfg_out).exists()
            _, out = await srv._execute_command("N0CALL", "pc")
            assert "PC capability summary:" in out
            _, out = await srv._execute_command("N0CALL", "pc 24")
            assert "PC24 support is available for dx" in out
            _, out = await srv._execute_command("N0CALL", "demonstrate show/time")
            assert "demonstrate: show/time" in out
            assert "Z" in out

            _, out = await srv._execute_command("N0CALL", "debug on")
            assert "Debug set to on for N0CALL." in out
            _, out = await srv._execute_command("N0CALL", "debug")
            assert "Debug for N0CALL: on" in out
            _, out = await srv._execute_command("N0CALL", "debug off")
            assert "Debug set to off for N0CALL." in out

            _, out = await srv._execute_command("N0CALL", "rcmd SH/DX 5")
            assert "Rcmd set to SH/DX 5 for N0CALL." in out
            _, out = await srv._execute_command("N0CALL", "rcmd")
            assert "Remote command settings for N0CALL:" in out
            assert "Remote Command: SH/DX 5" in out

            _, out = await srv._execute_command("N0CALL", "privilege")
            assert "Access level for N0CALL: sysop" in out

            _, out = await srv._execute_command("N0CALL", "save")
            assert "Saved " in out

            _, out = await srv._execute_command("N0CALL", "sysop")
            assert "No registered user record was found for N0CALL." in out

            _, out = await srv._execute_command("N0CALL", "get/keps")
            assert "Keplerian elements request accepted." in out
            assert await store.get_user_pref("N0CALL", "keps_last_request_epoch") is not None
            assert await store.get_user_pref("N0CALL", "keps_last_update_epoch") is not None
            assert (tmp_path / "topcompat-keps.txt").exists()
        finally:
            await store.close()

    asyncio.run(run())


def test_automatic_keps_refresh_updates_missing_or_stale_file(tmp_path, monkeypatch) -> None:
    async def run() -> None:
        db = str(tmp_path / "auto_keps.db")
        cfg = _mk_config(db)
        cfg.node.node_call = "AI3I-16"
        cfg.satellite.keps_path = str(tmp_path / "auto-keps.txt")
        monkeypatch.setattr(telnet_server_mod, "_download_text_url", lambda _url: _SAMPLE_TLE)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        try:
            await srv._refresh_keps_if_stale()
            target = tmp_path / "auto-keps.txt"
            assert target.exists()
            assert "ISS (ZARYA)" in target.read_text(encoding="utf-8")
            assert await store.get_user_pref("AI3I-16", "keps_last_auto_epoch") is not None
            assert await store.get_user_pref("AI3I-16", "keps_last_update_epoch") is not None
            assert await store.get_user_pref("AI3I-16", "keps_last_status") == "ok"
        finally:
            await store.close()

    asyncio.run(run())


def test_automatic_keps_refresh_skips_fresh_file(tmp_path, monkeypatch) -> None:
    async def run() -> None:
        db = str(tmp_path / "auto_keps_fresh.db")
        cfg = _mk_config(db)
        cfg.satellite.keps_path = str(tmp_path / "fresh-keps.txt")
        target = tmp_path / "fresh-keps.txt"
        target.write_text(_SAMPLE_TLE, encoding="utf-8")

        def _unexpected_download(_url: str) -> str:
            raise AssertionError("fresh keps file should not be downloaded")

        monkeypatch.setattr(telnet_server_mod, "_download_text_url", _unexpected_download)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        try:
            await srv._refresh_keps_if_stale()
            assert await store.get_user_pref(cfg.node.node_call.upper(), "keps_last_auto_epoch") is None
        finally:
            await store.close()

    asyncio.run(run())


def test_db_compat_commands(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "dbcompat.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.set_user_pref("N0CALL", "privilege", "sysop", now)
            await store.set_user_pref("N0CALL", "foo", "bar", 1772330000)
            _, out = await srv._execute_command("N0CALL", "dbdelkey N0CALL foo")
            assert "Database key foo removed for N0CALL." in out
            assert await store.get_user_pref("N0CALL", "foo") is None

            sample = tmp_path / "sampspot.dat"
            sample.write_text(
                "14074.0^K1ABC^1772337000^FT8^N0CALL^226^226^N2WQ-1^8^5^7^4^^^75.23.154.42\n"
            )
            _, out = await srv._execute_command("N0CALL", f"dbimport {sample}")
            assert "Database import complete." in out
            assert "Imported 1 record(s); skipped 0." in out

            _, out = await srv._execute_command("N0CALL", "set/user N0CALL")
            assert "User record created or updated for N0CALL." in out
            await srv._execute_command("N0CALL", "set/var N0CALL test=one")
            await srv._execute_command("N0CALL", "set/uservar N0CALL note=two")
            await srv._execute_command("N0CALL", "set/usdb N0CALL qth FN42")
            await srv._execute_command("N0CALL", "set/buddy K1ABC")
            _, out = await srv._execute_command("N0CALL", "dbremove user N0CALL")
            assert "Removed " in out
            assert "stored item(s) for N0CALL:" in out
            assert "preferences " in out
            assert "variables " in out
            assert await store.list_user_prefs("N0CALL") == {}
            assert await store.list_user_vars("N0CALL") == {}
            assert await store.list_usdb_entries("N0CALL") == {}
            assert await store.list_buddies("N0CALL") == []

            _, out = await srv._execute_command("N0CALL", "dxqsl_export /tmp/dxqsl.dat")
            assert "DXQSL export written to /tmp/dxqsl.dat." in out
            _, out = await srv._execute_command("N0CALL", "dxqsl_import /tmp/does-not-exist.dxqsl")
            assert "dxqsl_import: file not found" in out
            dxqsl = tmp_path / "dxqsl.dat"
            dxqsl.write_text("dummy")
            _, out = await srv._execute_command("N0CALL", f"dxqsl_import {dxqsl}")
            assert "DXQSL import loaded from " in out
            assert await store.get_user_pref("N0CALL", "dxqsl_export_path") == "/tmp/dxqsl.dat"
            assert await store.get_user_pref("N0CALL", "dxqsl_import_path") == str(dxqsl)
            assert await store.get_user_pref("N0CALL", "dxqsl_export_epoch") is not None
            assert await store.get_user_pref("N0CALL", "dxqsl_import_epoch") is not None

            await store.set_user_pref("N0CALL", "privilege", "sysop", now)
            _, out = await srv._execute_command("N0CALL", "spoof K1ABC as N0CALL")
            assert "Injected chat as K1ABC." in out
        finally:
            await store.close()

    asyncio.run(run())

def test_dbremove_granular_tables(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "dbremove_granular.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.set_user_pref("N0CALL", "privilege", "sysop", now)
            await store.set_user_pref("K1ABC", "p1", "v1", now)
            await store.set_user_var("K1ABC", "v1", "x", now)
            await store.set_usdb_entry("K1ABC", "qth", "FN42", now)
            await store.add_buddy("K1ABC", "N0CALL", now)
            await store.add_startup_command("K1ABC", "show/time", now)
            await store.set_filter_rule("K1ABC", "spots", "accept", 1, "K*", now)

            _, out = await srv._execute_command("N0CALL", "dbremove prefs K1ABC")
            assert "Removed 1 prefs entry for K1ABC." in out
            assert await store.list_user_prefs("K1ABC") == {}
            assert await store.list_user_vars("K1ABC") != {}

            _, out = await srv._execute_command("N0CALL", "dbremove vars K1ABC")
            assert "Removed 1 vars entry for K1ABC." in out
            assert await store.list_user_vars("K1ABC") == {}

            _, out = await srv._execute_command("N0CALL", "dbremove usdb K1ABC")
            assert "Removed 1 usdb entry for K1ABC." in out
            assert await store.list_usdb_entries("K1ABC") == {}

            _, out = await srv._execute_command("N0CALL", "dbremove buddy K1ABC")
            assert "Removed 1 buddy entry for K1ABC." in out
            assert await store.list_buddies("K1ABC") == []

            _, out = await srv._execute_command("N0CALL", "dbremove startup K1ABC")
            assert "Removed 1 startup entry for K1ABC." in out
            assert await store.list_startup_commands("K1ABC") == []

            _, out = await srv._execute_command("N0CALL", "dbremove filters K1ABC")
            assert "Removed 1 filters entry for K1ABC." in out
            assert await store.list_filter_rules("K1ABC") == []
        finally:
            await store.close()

    asyncio.run(run())


def test_stat_queue_channel_aggregate_link_metrics(tmp_path) -> None:
    async def _stats() -> dict[str, dict[str, object]]:
        return {
            "peer1": {
                "inbound": True,
                "parsed_frames": 10,
                "sent_frames": 7,
                "dropped_frames": 2,
                "policy_dropped": 1,
            },
            "peer2": {
                "inbound": False,
                "parsed_frames": 5,
                "sent_frames": 9,
                "dropped_frames": 1,
                "policy_dropped": 3,
            },
        }

    async def run() -> None:
        db = str(tmp_path / "stat_queue.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc), link_stats_fn=_stats)
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            _, out = await srv._execute_command("N0CALL", "stat/queue")
            assert "Queue status: there are 2 peers, 7 queued items, RX 15, TX 16, dropped 3, and 4 policy drops." in out

            _, out = await srv._execute_command("N0CALL", "stat/channel")
            assert "Channel status: there are 2 peers, with 1 accepted, 1 dial-out, RX 15, TX 16, dropped 3, and 4 policy drops." in out
        finally:
            await store.close()

    asyncio.run(run())


def test_save_syncs_session_state(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "save_sync.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        sess = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        sess.echo = False
        sess.here = True
        sess.beep = True
        sess.language = "fr"
        sess.peer_profile = "arcluster"
        sess.catchup = True
        sess.vars["rcmd"] = "SH/DX 10"
        sess.vars["page_size"] = "30"
        srv._sessions[1] = sess
        try:
            _, out = await srv._execute_command("N0CALL", "save")
            assert "Saved " in out and "for N0CALL" in out
            assert await store.get_user_pref("N0CALL", "echo") == "off"
            assert await store.get_user_pref("N0CALL", "beep") == "on"
            assert await store.get_user_pref("N0CALL", "language") == "fr"
            assert await store.get_user_pref("N0CALL", "profile") == "arcluster"
            assert await store.get_user_pref("N0CALL", "catchup") == "on"
            assert await store.get_user_pref("N0CALL", "rcmd") == "SH/DX 10"
            assert await store.get_user_pref("N0CALL", "page_size") == "30"
            assert await store.get_user_pref("N0CALL", "last_save_epoch") is not None
        finally:
            await store.close()

    asyncio.run(run())


def test_pc_command_reflects_and_sets_relay_mapping(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "pc_map.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            _, out = await srv._execute_command("N0CALL", "pc 24")
            assert "PC24 support is available for dx" in out
            assert "current state is on" in out

            _, out = await srv._execute_command("N0CALL", "pc 24 off")
            assert "current state is off" in out
            assert await store.get_user_pref("N0CALL", "relay.spots") == "off"

            _, out = await srv._execute_command("N0CALL", "show/relay")
            assert "SPOTS: off from your setting." in out

            _, out = await srv._execute_command("N0CALL", "pc 61 on")
            assert "PC61 support is available for route" in out
            assert "current state is on" in out
            assert await store.get_user_pref("N0CALL", "routepc19") == "on"
        finally:
            await store.close()

    asyncio.run(run())


def test_send_config_write_requires_sysop(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "send_cfg.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            out_path = tmp_path / "cfg.txt"
            _, out = await srv._execute_command("N0CALL", f"send_config {out_path}")
            assert "permission denied" in out
            assert not out_path.exists()

            now = int(datetime.now(timezone.utc).timestamp())
            await store.set_user_pref("N0CALL", "privilege", "sysop", now)
            _, out = await srv._execute_command("N0CALL", f"send_config {out_path}")
            assert "Configuration snapshot written to " in out
            assert out_path.exists()
        finally:
            await store.close()

    asyncio.run(run())


def test_show_named_gateways_and_dbshow_dbavail(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "named_status.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.set_user_pref("N0CALL", "dxqsl_export_path", "/tmp/dxqsl.out", now)
            await store.set_user_pref("N0CALL", "db0sdx.host", "db0sdx.de", now)
            await store.set_user_pref("N0CALL", "db0sdx.port", "41112", now)
            await store.set_user_pref("N0CALL", "db0sdx", "on", now)
            await store.set_user_pref("N0CALL", "badword.example", "x", now)
            await store.add_deny_rule("badword", "pirate*", now)
            await store.add_spot(Spot(14074.0, "K1ABC", now, "FT8", "N0CALL", "N2WQ-1", ""))
            await store.add_message("K1ABC", "N0CALL", now, "hi")
            await store.add_bulletin("announce", "N0CALL", "LOCAL", now, "contest soon")

            _, out = await srv._execute_command("N0CALL", "show/dxqsl")
            assert "DXQSL status for N0CALL:" in out
            assert "Export Path: /tmp/dxqsl.out" in out
            assert "Ready: no" in out

            _, out = await srv._execute_command("N0CALL", "show/db0sdx")
            assert "db0sdx gateway status:" in out
            assert "Enabled: on" in out
            assert "Host: db0sdx.de" in out

            _, out = await srv._execute_command("N0CALL", "show/cmdcache")
            assert "Command cache:" in out
            assert "State: warm" in out

            _, out = await srv._execute_command("N0CALL", "load/dxqsl")
            assert "DXQSL settings loaded for N0CALL:" in out
            assert "Export Enabled: yes" in out
            assert "Import Enabled: no" in out

            _, out = await srv._execute_command("N0CALL", "load/badwords")
            assert "Loaded 1 bad-word rule entry." in out

            _, out = await srv._execute_command("N0CALL", "load/db")
            assert "Database loaded from " in out
            assert "1 spots" in out or "1 spot" in out

            _, out = await srv._execute_command("N0CALL", "dbshow")
            assert "Database summary for " in out
            assert "Spots: 1" in out

            _, out = await srv._execute_command("N0CALL", "dbshow messages")
            assert "Messages: 1 total, 1 unread." in out

            _, out = await srv._execute_command("N0CALL", "dbavail")
            assert "SQLite database at " in out
            assert "Status: available" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_safe_nested_dispatch_commands(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "nested.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv._execute_command("N0CALL", "do show/time")
            assert "Z" in out
            _, out = await srv._execute_command("N0CALL", "run set/talk")
            assert "Talk set to on for N0CALL." in out
            _, out = await srv._execute_command("N0CALL", "type announce hello")
            assert "that command is not allowed" in out
            _, out = await srv._execute_command("N0CALL", "merge do show/time")
            assert "nested control commands are not allowed" in out
            now = int(datetime.now(timezone.utc).timestamp())
            await store.set_user_pref("N0CALL", "privilege", "sysop", now)
            keep, out = await srv._execute_command("N0CALL", "shutdown")
            assert keep is False
            assert "Shutdown requested: listener stopped" in out
            _, out = await srv._execute_command("N0CALL", "kill")
            assert "Usage: kill <call|all>" in out
            _, out = await srv._execute_command("N0CALL", "init")
            assert "Reloaded preferences and filters for " in out
            _, out = await srv._execute_command("N0CALL", "rinit")
            assert "Listener restart skipped because telnet is not running." in out
        finally:
            await store.close()

    asyncio.run(run())


def test_control_kill_disconnects_target_sessions(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "kill_control.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        srv._sessions[2] = Session(call="K1ABC", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        srv._sessions[3] = Session(call="K1ABC", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.set_user_pref("N0CALL", "privilege", "sysop", now)
            keep, out = await srv._execute_command("N0CALL", "kill K1ABC")
            assert keep is True
            assert "Disconnected 2 session(s) for K1ABC." in out
            assert len(srv._sessions) == 1

            keep, out = await srv._execute_command("N0CALL", "kill all")
            assert keep is True
            assert "Disconnected 0 session(s) for ALL." in out
            assert len(srv._sessions) == 1
        finally:
            await store.close()

    asyncio.run(run())


def test_control_policy_toggle_and_show(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "control_policy.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        srv._sessions[2] = Session(call="K1ABC", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            _, out = await srv._execute_command("N0CALL", "set/control off")
            assert "permission denied" in out

            now = int(datetime.now(timezone.utc).timestamp())
            await store.set_user_pref("N0CALL", "privilege", "sysop", now)

            _, out = await srv._execute_command("N0CALL", "show/control")
            assert "System control is on." in out

            _, out = await srv._execute_command("N0CALL", "set/control off")
            assert "System control commands disabled." in out

            keep, out = await srv._execute_command("N0CALL", "kill K1ABC")
            assert keep is True
            assert "kill is currently disabled by system control policy" in out
            assert len(srv._sessions) == 2

            _, out = await srv._execute_command("N0CALL", "show/control")
            assert "System control is off." in out
            assert "set/control off" in out

            _, out = await srv._execute_command("N0CALL", "set/control on")
            assert "System control commands enabled." in out
            keep, out = await srv._execute_command("N0CALL", "kill K1ABC")
            assert keep is True
            assert "Disconnected 1 session(s) for K1ABC." in out
            assert len(srv._sessions) == 1

            _, out = await srv._execute_command("N0CALL", "unset/control")
            assert "default enabled state" in out
            assert "Overrides removed:" in out
            _, out = await srv._execute_command("N0CALL", "show/control")
            assert "System control is on." in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_control_reset_requires_sysop_and_clears(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "control_reset.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        srv._sessions[2] = Session(call="K1ABC", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.set_user_pref("N0CALL", "privilege", "sysop", now)
            _, _ = await srv._execute_command("N0CALL", "set/control off")
            _, _ = await srv._execute_command("N0CALL", "set/control on")

            _, out = await srv._execute_command("K1ABC", "show/control --reset")
            assert "permission denied" in out

            _, out = await srv._execute_command("N0CALL", "show/control")
            assert "set/control off" in out
            assert "set/control on" in out
            assert "System control is on." in out

            _, out = await srv._execute_command("N0CALL", "show/control --reset")
            assert "show/control --reset removed 2" in out
            _, out = await srv._execute_command("N0CALL", "show/control")
            assert "Recent control events: 1" in out
            assert "show/control --reset removed 2" in out
            assert "System control is on." in out
        finally:
            await store.close()

    asyncio.run(run())


def test_relay_policy_commands(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "relay_cmd.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv._execute_command("N0CALL", "show/relay")
            assert "SPOTS: on by default." in out
            assert "CHAT: on by default." in out
            assert "Route PC19: off" in out

            _, out = await srv._execute_command("N0CALL", "set/relay chat off")
            assert "Relay policy for chat set to off." in out
            _, out = await srv._execute_command("N0CALL", "set/relay spots off")
            assert "Relay policy for spots set to off." in out
            _, out = await srv._execute_command("N0CALL", "set/relay all on")
            assert "Relay policy set to on for all traffic." in out
            _, out = await srv._execute_command("N0CALL", "unset/relay wcy")
            assert "Relay policy for wcy restored to default" in out
            _, out = await srv._execute_command("N0CALL", "show/relay")
            assert "SPOTS: on from your setting." in out
            assert "CHAT: on from your setting." in out
            assert "WCY: on by default." in out
        finally:
            await store.close()

    asyncio.run(run())


def test_dx_command_posts_and_show_shorthand_still_works(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "dx_post.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.upsert_user_registry("N0CALL", now, privilege="user")
            _, out = await srv._execute_command("N0CALL", "dx 14074.0 K1ABC FT8 test")
            assert "Spot posted on 14074.0 kHz for K1ABC." in out
            assert await store.count_spots() == 1
            _, out = await srv._execute_command("N0CALL", "dx")
            assert "No spots available" not in out
            assert "K1ABC" in out
            _, out = await srv._execute_command("N0CALL", "dx K1")
            assert "K1ABC" in out
            _, out = await srv._execute_command("N0CALL", "dx 14074.0 K1ABC FT8 test")
            assert "Spot was not accepted" in out
            assert await store.count_spots() == 1
        finally:
            await store.close()

    asyncio.run(run())


def test_relaypeer_commands(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "relaypeer_cmd.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv._execute_command("N0CALL", "set/relaypeer peer1 off")
            assert "Relay policy for peer1 all traffic set to off." in out
            _, out = await srv._execute_command("N0CALL", "set/relaypeer peer2 chat off")
            assert "Relay policy for peer2 chat set to off." in out
            _, out = await srv._execute_command("N0CALL", "show/relaypeer peer1")
            assert "ALL: off from your setting." in out
            _, out = await srv._execute_command("N0CALL", "show/relaypeer PEER1")
            assert "ALL: off from your setting." in out
            _, out = await srv._execute_command("N0CALL", "show/relaypeer")
            assert "relay.peer.peer1: off" in out
            _, out = await srv._execute_command("N0CALL", "unset/relaypeer peer2 chat")
            assert "Relay policy for peer2 chat restored to default" in out
            _, out = await srv._execute_command("N0CALL", "show/relaypeer peer2")
            assert "CHAT: on by default." in out
        finally:
            await store.close()

    asyncio.run(run())


def test_ingestpeer_commands(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "ingestpeer_cmd.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv._execute_command("N0CALL", "set/ingestpeer peer1 off")
            assert "Ingest policy for peer1 all traffic set to off." in out
            _, out = await srv._execute_command("N0CALL", "set/ingestpeer peer2 spots off")
            assert "Ingest policy for peer2 spots set to off." in out
            _, out = await srv._execute_command("N0CALL", "set/ingestpeer peer2 wcy off")
            assert "Ingest policy for peer2 wcy set to off." in out
            _, out = await srv._execute_command("N0CALL", "show/ingestpeer peer1")
            assert "ALL: off from your setting." in out
            _, out = await srv._execute_command("N0CALL", "show/ingestpeer PEER1")
            assert "ALL: off from your setting." in out
            _, out = await srv._execute_command("N0CALL", "show/ingestpeer")
            assert "ingest.peer.peer1: off" in out
            _, out = await srv._execute_command("N0CALL", "unset/ingestpeer peer2 spots")
            assert "Ingest policy for peer2 spots restored to default" in out
            _, out = await srv._execute_command("N0CALL", "unset/ingestpeer peer2 wcy")
            assert "Ingest policy for peer2 wcy restored to default" in out
            _, out = await srv._execute_command("N0CALL", "show/ingestpeer peer2")
            assert "SPOTS: on by default." in out
            assert "WCY: on by default." in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_policy_command(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "policy_cmd.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            await srv._execute_command("N0CALL", "set/routepc19")
            await srv._execute_command("N0CALL", "set/relay chat off")
            await srv._execute_command("N0CALL", "set/relaypeer peer1 off")
            await srv._execute_command("N0CALL", "set/ingestpeer peer2 spots off")
            _, out = await srv._execute_command("N0CALL", "show/policy")
            assert "Policy for N0CALL" in out
            assert "Route PC19: on" in out
            assert "CHAT: off from your setting." in out
            assert "SPOTS: on unless a peer override says otherwise." in out
            assert "Relay Peer Overrides: 1" in out
            assert "Ingest Peer Overrides: 1" in out
            assert "relay.peer.peer1: off" in out
            assert "ingest.peer.peer2.spots: off" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_load_and_stat_named_commands(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "load_stat.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            await srv._execute_command("N0CALL", "chat hello")
            await srv._execute_command("N0CALL", "wcy K=2 A=6")
            await srv._execute_command("N0CALL", "set/usdb qth Boston")
            await srv._execute_command("N0CALL", "msg all test")

            _, out = await srv._execute_command("N0CALL", "load/usdb")
            assert "USDB loaded for N0CALL:" in out

            _, out = await srv._execute_command("N0CALL", "load/messages")
            assert "Message state loaded for N0CALL:" in out

            _, out = await srv._execute_command("N0CALL", "stat/msg")
            assert "Message summary:" in out and "unread." in out

            _, out = await srv._execute_command("N0CALL", "stat/wcy")
            assert "WCY summary: 1 stored entry." in out

            _, out = await srv._execute_command("N0CALL", "stat/db")
            assert "The database currently holds" in out and "registry record" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_stat_route_user_pc19list_and_load_aliases_bands_prefixes(tmp_path) -> None:
    async def _stats():
        return {
            "peer1": {"inbound": False},
            "in:(127.0.0.1,9000)": {"inbound": True},
        }

    async def run() -> None:
        db = str(tmp_path / "stat_extra.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc), link_stats_fn=_stats)
        s1 = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        s2 = Session(call="K1ABC", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        srv._sessions[1] = s1
        srv._sessions[2] = s2
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.set_user_pref("N0CALL", "routepc19", "on", now)
            await store.add_spot(Spot(14074.0, "K1ABC", now, "FT8", "N0CALL", "N2WQ-1", ""))
            await store.add_spot(Spot(7020.0, "W1AW", now, "CW", "N0CALL", "N2WQ-1", ""))

            _, out = await srv._execute_command("N0CALL", "stat/routenode")
            assert "There are 2 route nodes: 1 accepted and 1 dial-out." in out

            _, out = await srv._execute_command("N0CALL", "stat/routeuser")
            assert "There are 2 active user sessions across 2 peer links." in out

            _, out = await srv._execute_command("N0CALL", "stat/pc19list")
            assert "PC19 routing is enabled for 1 call:" in out and "N0CALL" in out

            _, out = await srv._execute_command("N0CALL", "load/bands")
            assert "band definitions" in out
            assert "2 observed spot" in out
            _, out = await srv._execute_command("N0CALL", "load/aliases")
            assert "Alias cache loaded:" in out
            _, out = await srv._execute_command("N0CALL", "load/prefixes")
            assert "observed prefix sample" in out
            assert "K1A,W1A" in out or "W1A,K1A" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_user_prefs_persist_across_server_instances(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "prefs.db")
        cfg = _mk_config(db)

        store1 = SpotStore(db)
        srv1 = TelnetClusterServer(cfg, store1, datetime.now(timezone.utc))
        sess1 = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        srv1._sessions[1] = sess1
        try:
            _, out = await srv1._execute_command("N0CALL", "set/language de")
            assert "Language set to de" in out
            _, out = await srv1._execute_command("N0CALL", "set/talk")
            assert "Talk set to on for N0CALL." in out
            _, out = await srv1._execute_command("N0CALL", "set/arcluster")
            assert "Profile for N0CALL set to arcluster." in out
            _, out = await srv1._execute_command("N0CALL", "set/beep")
            assert "Beep set to on for N0CALL." in out
            _, out = await srv1._execute_command("N0CALL", "set/qth Boston")
            assert "QTH set to Boston for N0CALL." in out
            _, out = await srv1._execute_command("N0CALL", "join vhf")
            assert "Joined group vhf." in out
        finally:
            await store1.close()

        store2 = SpotStore(db)
        srv2 = TelnetClusterServer(cfg, store2, datetime.now(timezone.utc))
        sess2 = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        srv2._sessions[1] = sess2
        try:
            await srv2._apply_prefs_to_session(sess2)

            assert sess2.language == "de"
            assert sess2.peer_profile == "arcluster"
            assert sess2.beep is True
            assert sess2.vars.get("talk") == "on"
            assert sess2.vars.get("qth") == "Boston"
            assert sess2.vars.get("groups.joined") == "vhf"

            _, out = await srv2._execute_command("N0CALL", "show/talk")
            assert "TALK for N0CALL: on" in out
            _, out = await srv2._execute_command("N0CALL", "show/groups")
            assert "Group settings for N0CALL:" in out
            assert "Groups Joined: vhf" in out
            _, out = await srv2._execute_command("N0CALL", "show/filter")
            assert "Language set to de" in out
            assert "Profile for N0CALL set to arcluster." in out
        finally:
            await store2.close()

    asyncio.run(run())


def test_filter_rules_persist_across_server_instances(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "filter_persist.db")
        cfg = _mk_config(db)

        store1 = SpotStore(db)
        srv1 = TelnetClusterServer(cfg, store1, datetime.now(timezone.utc))
        srv1._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv1._execute_command("N0CALL", "accept/spots 1 on 40m")
            assert "Accept filter for spots saved" in out
            _, out = await srv1._execute_command("N0CALL", "reject/spots 2 by K1")
            assert "Reject filter for spots saved" in out
        finally:
            await store1.close()

        store2 = SpotStore(db)
        srv2 = TelnetClusterServer(cfg, store2, datetime.now(timezone.utc))
        srv2._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv2._execute_command("N0CALL", "show/filter")
            assert "accept/spots 1 on 40m" in out
            assert "reject/spots 2 by K1" in out

            _, out = await srv2._execute_command("N0CALL", "clear/spots 1")
            assert "Cleared spots filters for N0CALL (slot 1)." in out
            _, out = await srv2._execute_command("N0CALL", "show/filter")
            assert "accept/spots 1 on 40m" not in out
            assert "reject/spots 2 by K1" in out
        finally:
            await store2.close()

    asyncio.run(run())


def test_bad_rule_commands_and_show_lists(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "badrules.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv._execute_command("N0CALL", "set/baddx K1BAD*")
            assert "Blocked dx rule added: K1BAD*" in out
            _, out = await srv._execute_command("N0CALL", "set/badspotter N0SPAM")
            assert "Blocked spotter rule added: N0SPAM" in out
            _, out = await srv._execute_command("N0CALL", "set/badnode BADNODE*")
            assert "Blocked node rule added: BADNODE*" in out
            _, out = await srv._execute_command("N0CALL", "set/badword pirate")
            assert "Blocked word rule added: pirate" in out

            _, out = await srv._execute_command("N0CALL", "show/baddx")
            assert "K1BAD*" in out
            _, out = await srv._execute_command("N0CALL", "show/badspotter")
            assert "N0SPAM" in out
            _, out = await srv._execute_command("N0CALL", "show/badnode")
            assert "BADNODE*" in out
            _, out = await srv._execute_command("N0CALL", "show/badword")
            assert "pirate" in out

            _, out = await srv._execute_command("N0CALL", "unset/baddx K1BAD*")
            assert "Removed 1 baddx entry." in out
            _, out = await srv._execute_command("N0CALL", "unset/badword all")
            assert "Removed 1 badword entry." in out
        finally:
            await store.close()

    asyncio.run(run())


def test_buddy_commands_persist_across_server_instances(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "buddy.db")
        cfg = _mk_config(db)

        store1 = SpotStore(db)
        srv1 = TelnetClusterServer(cfg, store1, datetime.now(timezone.utc))
        srv1._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv1._execute_command("N0CALL", "set/buddy K1ABC W1AW")
            assert "Added 2 buddy entries for N0CALL." in out
            _, out = await srv1._execute_command("N0CALL", "show/buddy")
            assert "Buddy list for N0CALL" in out
            assert "K1ABC" in out and "W1AW" in out
        finally:
            await store1.close()

        store2 = SpotStore(db)
        srv2 = TelnetClusterServer(cfg, store2, datetime.now(timezone.utc))
        srv2._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv2._execute_command("N0CALL", "show/buddy")
            assert "Buddy list for N0CALL" in out
            assert "K1ABC" in out and "W1AW" in out

            _, out = await srv2._execute_command("N0CALL", "unset/buddy K1ABC")
            assert "Removed 1 buddy entry for N0CALL." in out
            _, out = await srv2._execute_command("N0CALL", "show/buddy")
            assert "K1ABC" not in out and "W1AW" in out
        finally:
            await store2.close()

    asyncio.run(run())


def test_usdb_commands_persist_across_server_instances(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "usdb.db")
        cfg = _mk_config(db)

        store1 = SpotStore(db)
        srv1 = TelnetClusterServer(cfg, store1, datetime.now(timezone.utc))
        srv1._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv1._execute_command("N0CALL", "set/usdb state MA")
            assert "USDB field state updated for N0CALL." in out
            _, out = await srv1._execute_command("N0CALL", "set/usdb county Middlesex")
            assert "USDB field county updated for N0CALL." in out
            _, out = await srv1._execute_command("N0CALL", "show/usdb")
            assert "USDB entries for N0CALL" in out
            assert "state: MA" in out
            assert "county: Middlesex" in out
            _, out = await srv1._execute_command("N0CALL", "show/station")
            assert "USDB state: MA" in out
        finally:
            await store1.close()

        store2 = SpotStore(db)
        srv2 = TelnetClusterServer(cfg, store2, datetime.now(timezone.utc))
        srv2._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv2._execute_command("N0CALL", "show/usdb")
            assert "state: MA" in out
            assert "county: Middlesex" in out

            _, out = await srv2._execute_command("N0CALL", "delete/usdb state")
            assert "Removed USDB field state for N0CALL." in out
            _, out = await srv2._execute_command("N0CALL", "show/usdb")
            assert "state: MA" not in out
            assert "county: Middlesex" in out
        finally:
            await store2.close()

    asyncio.run(run())


def test_var_commands_persist_across_server_instances(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "var.db")
        cfg = _mk_config(db)

        store1 = SpotStore(db)
        srv1 = TelnetClusterServer(cfg, store1, datetime.now(timezone.utc))
        srv1._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv1._execute_command("N0CALL", "set/var color blue")
            assert "Variable color updated for N0CALL." in out
            _, out = await srv1._execute_command("N0CALL", "set/var page=40")
            assert "Variable page updated for N0CALL." in out
            _, out = await srv1._execute_command("N0CALL", "show/var")
            assert "color: blue" in out
            assert "page: 40" in out
        finally:
            await store1.close()

        store2 = SpotStore(db)
        srv2 = TelnetClusterServer(cfg, store2, datetime.now(timezone.utc))
        srv2._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv2._execute_command("N0CALL", "show/var")
            assert "color: blue" in out
            assert "page: 40" in out

            _, out = await srv2._execute_command("N0CALL", "show/var color")
            assert "Variable color for N0CALL: blue" in out

            _, out = await srv2._execute_command("N0CALL", "unset/var color")
            assert "Variable color cleared for N0CALL." in out
            _, out = await srv2._execute_command("N0CALL", "show/var")
            assert "color: blue" not in out
            assert "page: 40" in out
        finally:
            await store2.close()

    asyncio.run(run())


def test_user_registry_commands_persist_across_server_instances(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "registry.db")
        cfg = _mk_config(db)

        store1 = SpotStore(db)
        srv1 = TelnetClusterServer(cfg, store1, datetime.now(timezone.utc))
        srv1._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            await store1.set_user_pref("N0CALL", "privilege", "sysop", int(datetime.now(timezone.utc).timestamp()))
            _, out = await srv1._execute_command("N0CALL", "create/user K1ABC")
            assert "User record created for K1ABC." in out
            _, out = await srv1._execute_command("N0CALL", "set/user K1ABC name Alice Example")
            assert "name updated for K1ABC." in out
            _, out = await srv1._execute_command("N0CALL", "set/user K1ABC qth Cambridge")
            assert "qth updated for K1ABC." in out
            _, out = await srv1._execute_command("N0CALL", "show/registered K1ABC")
            assert "Registered user K1ABC" in out
            assert "Name: Alice Example" in out
            assert "Location (QTH): Cambridge" in out
        finally:
            await store1.close()

        store2 = SpotStore(db)
        srv2 = TelnetClusterServer(cfg, store2, datetime.now(timezone.utc))
        srv2._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            await store2.set_user_pref("N0CALL", "privilege", "sysop", int(datetime.now(timezone.utc).timestamp()))
            _, out = await srv2._execute_command("N0CALL", "show/registered")
            assert "K1ABC" in out

            _, out = await srv2._execute_command("N0CALL", "delete/user K1ABC")
            assert "User K1ABC removed." in out
            _, out = await srv2._execute_command("N0CALL", "show/registered K1ABC")
            assert "No registered user record was found for K1ABC." in out
        finally:
            await store2.close()

    asyncio.run(run())


def test_show_registered_uses_registry_home_node_fallback(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "registry_home_node_detail.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.upsert_user_registry(
                "K1ABC",
                now,
                display_name="Alice Example",
                home_node="N2NODE",
            )
            _, out = await srv._execute_command("N0CALL", "show/registered K1ABC")
            assert "Home Node: N2NODE" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_node_uses_registry_home_node_fallback(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "show_node_home_node.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.upsert_user_registry("K1ABC", now, home_node="N2NODE")
            _, out = await srv._execute_command("N0CALL", "show/node K1ABC")
            assert "Home Node  : N2NODE" in out
            assert "Node Family:" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_home_node_preferences_persist_and_render(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "homeprefs.db")
        cfg = _mk_config(db)

        store1 = SpotStore(db)
        srv1 = TelnetClusterServer(cfg, store1, datetime.now(timezone.utc))
        srv1._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv1._execute_command("N0CALL", "set/user N0CALL")
            assert "User record created or updated for N0CALL." in out
            _, out = await srv1._execute_command("N0CALL", "set/homebbs K1BBS")
            assert "Homebbs set to K1BBS for N0CALL." in out or "Home BBS set to K1BBS for N0CALL." in out
            _, out = await srv1._execute_command("N0CALL", "set/homenode N2NODE")
            assert "Homenode set to N2NODE for N0CALL." in out or "Home Node set to N2NODE for N0CALL." in out
            _, out = await srv1._execute_command("N0CALL", "set/node W3NODE")
            assert "Node set to W3NODE for N0CALL." in out
        finally:
            await store1.close()

        store2 = SpotStore(db)
        srv2 = TelnetClusterServer(cfg, store2, datetime.now(timezone.utc))
        srv2._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv2._execute_command("N0CALL", "show/registered N0CALL")
            assert "Home BBS: K1BBS" in out
            assert "Home Node: N2NODE" in out
            assert "Node: W3NODE" in out

            _, out = await srv2._execute_command("N0CALL", "show/node N0CALL")
            assert "Home BBS   : K1BBS" in out
            assert "Home Node  : N2NODE" in out
            assert "Node       : W3NODE" in out
            assert "Node Family:" in out

            _, out = await srv2._execute_command("N0CALL", "show/station")
            assert "Home BBS: K1BBS" in out
            assert "Home Node: N2NODE" in out
            assert "Node: W3NODE" in out
        finally:
            await store2.close()

    asyncio.run(run())


def test_delete_user_supports_wildcard_pattern_and_cleans_local_data(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "delete_pattern.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        now = int(datetime.now(timezone.utc).timestamp())
        try:
            await store.set_user_pref("N0CALL", "privilege", "sysop", now)
            await store.upsert_user_registry("MAILTST", now, display_name="Mail Test")
            await store.upsert_user_registry("MAILTST-1", now, display_name="Mail Test SSID")
            await store.set_user_pref("MAILTST", "password", "secret", now)
            await store.set_user_pref("MAILTST-1", "blocked_login", "on", now)

            _, out = await srv._execute_command("N0CALL", "delete/user MAILTST*")
            assert "Removed 2 user(s): MAILTST, MAILTST-1." in out

            assert await store.get_user_registry("MAILTST") is None
            assert await store.get_user_registry("MAILTST-1") is None
            assert await store.get_user_pref("MAILTST", "password") is None
            assert await store.get_user_pref("MAILTST-1", "blocked_login") is None
        finally:
            await store.close()

    asyncio.run(run())


def test_sysop_namespace_handles_user_management(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "sysop_namespace.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._mfa._sender = lambda _rcpt, _subject, _body: None  # type: ignore[assignment]
        srv._sessions[1] = Session(
            call="AI3I",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.upsert_user_registry("AI3I", now, privilege="sysop")

            _, out = await srv._execute_command("AI3I", "sysop/password K1ABC supersecret")
            assert "Password updated for K1ABC." in out

            _, out = await srv._execute_command("AI3I", "sysop/homenode K1ABC AI3I-16")
            assert "Home node for K1ABC set to AI3I-16." in out

            _, out = await srv._execute_command("AI3I", "sysop/blocklogin K1ABC on")
            assert "Login blocked for K1ABC and all SSIDs." in out

            _, out = await srv._execute_command("AI3I", "sysop/showuser K1ABC")
            assert "Registered user K1ABC" in out
            assert "Home Node: AI3I-16" in out
            assert "Login Access: blocked" in out

            _, out = await srv._execute_command("AI3I", "sysop/sysops")
            assert "System Operators:" in out
            assert "AI3I" in out

            _, out = await srv._execute_command("AI3I", "sysop/audit")
            assert "System Operator Audit" in out
            assert "sysop/password K1ABC" in out
            assert "sysop/blocklogin K1ABC on" in out

            saved = await store.get_user_pref("K1ABC", "password")
            assert is_password_hash(saved)
            assert verify_password("supersecret", saved)
            assert await store.get_user_pref("K1ABC", "blocked_login") == "on"

            _, out = await srv._execute_command("AI3I", "sysop/clearpassword K1ABC")
            assert "Password cleared for K1ABC." in out
            assert await store.get_user_pref("K1ABC", "password") is None

            await store.set_user_pref("K1ABC", "mfa_email_otp", "required", now)
            challenge_id, _expires = await srv._mfa.issue(call="K1ABC", email="k1abc@example.test", purpose="telnet")
            assert await store.get_mfa_challenge(challenge_id) is not None
            _, out = await srv._execute_command("AI3I", "sysop/clearmfa K1ABC")
            assert "MFA reset for K1ABC." in out
            assert "MFA is now off." in out
            assert await store.get_user_pref("K1ABC", "mfa_email_otp") == "off"
            assert await store.get_mfa_challenge(challenge_id) is None
        finally:
            await store.close()

    asyncio.run(run())


def test_user_can_manage_own_mfa_from_telnet(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "telnet_user_mfa_self_service.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.upsert_user_registry("K1ABC", now, privilege="user", email="k1abc@example.test")

            _, out = await srv._execute_command("K1ABC", "mfa")
            assert "MFA for K1ABC: disabled" in out

            _, out = await srv._execute_command("K1ABC", "set/mfa email")
            assert "Email MFA enabled for K1ABC." in out
            assert await store.get_user_pref("K1ABC", "mfa_email_otp") == "required"

            _, out = await srv._execute_command("K1ABC", "set/mfa authenticator")
            assert "Authenticator MFA enabled for K1ABC." in out
            assert "Setup key:" in out
            assert await store.get_user_pref("K1ABC", "mfa_totp_secret")

            await store.delete_user_pref("K1ABC", "mfa_totp_secret")
            _, out = await srv._execute_command("K1ABC", "set/totp")
            assert "Authenticator MFA enabled for K1ABC." in out
            assert "Setup key:" in out
            assert await store.get_user_pref("K1ABC", "mfa_totp_secret")

            _, out = await srv._execute_command("K1ABC", "unset/totp")
            assert "Authenticator MFA disabled for K1ABC." in out
            assert await store.get_user_pref("K1ABC", "mfa_email_otp") == "required"
            assert await store.get_user_pref("K1ABC", "mfa_totp_secret") is None

            _, out = await srv._execute_command("K1ABC", "unset/mfa")
            assert "MFA disabled for K1ABC." in out
            assert await store.get_user_pref("K1ABC", "mfa_email_otp") == "off"
            assert await store.get_user_pref("K1ABC", "mfa_totp_secret") is None
        finally:
            await store.close()

    asyncio.run(run())


def test_ssid_user_manages_own_mfa_without_touching_base_call(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "telnet_ssid_user_mfa_self_service.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.upsert_user_registry("N9JR", now, privilege="sysop", email="n9jr@example.test")
            await store.upsert_user_registry("N9JR-10", now, privilege="user", email="")

            _, out = await srv._execute_command("N9JR-10", "mfa")
            assert "MFA for N9JR-10: disabled" in out

            _, out = await srv._execute_command("N9JR-10", "set/mfa email")
            assert "Email MFA enabled for N9JR-10." in out
            assert await store.get_user_pref("N9JR-10", "mfa_email_otp") == "required"
            assert await store.get_user_pref("N9JR", "mfa_email_otp") is None

            _, out = await srv._execute_command("N9JR-10", "set/mfa authenticator")
            assert "Authenticator MFA enabled for N9JR-10." in out
            assert "Setup key:" in out
            assert await store.get_user_pref("N9JR-10", "mfa_totp_secret")
            assert await store.get_user_pref("N9JR", "mfa_totp_secret") is None

            _, out = await srv._execute_command("N9JR-10", "unset/mfa")
            assert "MFA disabled for N9JR-10." in out
            assert await store.get_user_pref("N9JR-10", "mfa_email_otp") == "off"
            assert await store.get_user_pref("N9JR-10", "mfa_totp_secret") is None
            assert await store.get_user_pref("N9JR", "mfa_email_otp") is None
            assert await store.get_user_pref("N9JR", "mfa_totp_secret") is None
        finally:
            await store.close()

    asyncio.run(run())


def test_telnet_totp_fallbacks_to_email_after_repeated_bad_codes(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "telnet_totp_bad_code_fallback.db")
        cfg = _mk_config(db)
        cfg.mfa.enabled = True
        cfg.mfa.require_for_users = True
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.upsert_user_registry("K1ABC", now, privilege="user", email="k1abc@example.test")
            await store.set_user_pref("K1ABC", "mfa_totp_secret", "JBSWY3DPEHPK3PXP", now)

            final = b""
            for _idx in range(3):
                reader = asyncio.StreamReader()
                writer = _DummyWriter()
                reader.feed_data(b"000000\r\n")
                reader.feed_eof()
                ok = await srv._prompt_email_otp("K1ABC", reader, writer, is_sysop=False)  # type: ignore[arg-type]
                assert ok is False
                final = bytes(writer.buffer)

            assert await store.get_user_pref("K1ABC", "mfa_totp_secret") is None
            assert await store.get_user_pref("K1ABC", "mfa_totp_failed_count") is None
            assert await store.get_user_pref("K1ABC", "mfa_email_otp") == "required"
            assert b"Authenticator MFA has been disabled" in final
            assert b"run set/totp" in final
        finally:
            await store.close()

    asyncio.run(run())


def test_telnet_login_rejects_exact_ssid_locked_account(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "telnet_exact_ssid_locked.db")
        cfg = AppConfig(
            node=NodeConfig(node_call="AI3I-16"),
            telnet=TelnetConfig(host="127.0.0.1", port=0, idle_timeout_seconds=30),
            web=WebConfig(host="127.0.0.1", port=0),
            public_web=PublicWebConfig(),
            store=StoreConfig(sqlite_path=db),
        )
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry("N9JR", now, privilege="user", email="n9jr@example.test")
        await store.upsert_user_registry("N9JR-10", now, privilege="user", email="n9jr10@example.test")
        await store.set_user_pref("N9JR-10", "registration_state", "locked", now)
        try:
            await srv.start()
        except OSError:
            pytest.skip("socket bind unavailable in sandbox")
        try:
            sock = (srv._server.sockets or [None])[0]
            assert sock is not None
            host, port = sock.getsockname()[0], sock.getsockname()[1]

            reader, writer = await asyncio.open_connection(host, port)
            await asyncio.wait_for(reader.readuntil(b"login: "), timeout=2.0)
            writer.write(b"N9JR-10\r\n")
            await writer.drain()
            output = await asyncio.wait_for(reader.read(4096), timeout=2.0)
            assert b"Account N9JR-10 is locked" in output
            writer.close()
            await writer.wait_closed()
        finally:
            await srv.stop()
            await store.close()

    asyncio.run(run())


def test_global_mfa_default_waits_for_user_mfa_material(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "telnet_mfa_default_material.db")
        cfg = _mk_config(db)
        cfg.mfa.enabled = True
        cfg.mfa.require_for_users = True
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.upsert_user_registry("K1ABC", now, privilege="user", email="")
            assert await srv._mfa_required_for_call("K1ABC", is_sysop=False) is False

            await store.upsert_user_registry("K1ABC", now, privilege="user", email="k1abc@example.test")
            assert await srv._mfa_required_for_call("K1ABC", is_sysop=False) is True
        finally:
            await store.close()

    asyncio.run(run())


def test_sysop_peer_commands_cover_accounts_and_saved_peers(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "sysop_peer_commands.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        saved: dict[str, dict[str, object]] = {}
        ops: list[tuple[object, ...]] = []

        async def _save(peer: str, dsn: str, profile: str = "dxspider", reconnect: bool = True, password: str | None = "") -> None:
            saved[peer] = {
                "peer": peer,
                "dsn": dsn,
                "profile": profile,
                "reconnect": "on" if reconnect else "off",
                "password": password,
            }
            ops.append(("save", peer, dsn, profile, reconnect, password))

        async def _delete(peer: str) -> bool:
            ops.append(("delete", peer))
            return saved.pop(peer, None) is not None

        async def _desired() -> list[dict[str, object]]:
            return list(saved.values())

        async def _stats() -> dict[str, dict[str, object]]:
            return {"PEER1": {"profile": "pycluster", "inbound": False}}

        async def _connect(peer: str, dsn: str) -> None:
            ops.append(("connect", peer, dsn))

        async def _disconnect(peer: str) -> bool:
            ops.append(("disconnect", peer))
            return peer == "PEER1"

        async def _profile(peer: str, profile: str) -> bool:
            ops.append(("profile", peer, profile))
            return peer == "PEER1"

        srv = TelnetClusterServer(
            cfg,
            store,
            datetime.now(timezone.utc),
            link_stats_fn=_stats,
            link_set_profile_fn=_profile,
            link_connect_fn=_connect,
            link_disconnect_fn=_disconnect,
            link_desired_peers_fn=_desired,
            link_save_peer_fn=_save,
            link_delete_peer_fn=_delete,
        )
        srv._sessions[1] = Session(
            call="AI3I",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.upsert_user_registry("AI3I", now, privilege="sysop")

            _, out = await srv._execute_command("AI3I", "sysop/peeraccount add N9JR-2 pycluster")
            assert "Peer account N9JR-2 configured as pycluster." in out
            assert await store.get_user_pref("N9JR-2", "node_family") == "pycluster"

            _, out = await srv._execute_command("AI3I", "sysop/peeraccount password N9JR-2 sharedsecret")
            assert "Peer account password updated for N9JR-2." in out
            saved_hash = await store.get_user_pref("N9JR-2", "password")
            assert is_password_hash(saved_hash)
            assert verify_password("sharedsecret", saved_hash)

            _, out = await srv._execute_command("AI3I", "sysop/peeraccount show N9JR-2")
            assert "Node Family: pycluster" in out
            assert "Password Set: yes" in out

            _, out = await srv._execute_command("AI3I", "sysop/peer add PEER1 tcp://example.net:7300 pycluster")
            assert "Saved peer PEER1 with profile pycluster." in out
            assert saved["PEER1"]["dsn"] == "tcp://example.net:7300"

            _, out = await srv._execute_command("AI3I", "sysop/peer set PEER1 retry off")
            assert "Saved peer PEER1: retry updated." in out
            assert saved["PEER1"]["reconnect"] == "off"
            assert saved["PEER1"]["password"] is None

            _, out = await srv._execute_command("AI3I", "sysop/peer show PEER1")
            assert "PEER1" in out
            assert "saved profile pycluster" in out

            _, out = await srv._execute_command("AI3I", "sysop/peerprofile PEER1 dxspider")
            assert "Profile for peer PEER1 set to dxspider." in out
            assert ("profile", "PEER1", "dxspider") in ops

            _, out = await srv._execute_command("AI3I", "sysop/peer connect PEER1")
            assert "Connection attempt started for PEER1" in out
            assert ("connect", "PEER1", "tcp://example.net:7300") in ops

            _, out = await srv._execute_command("AI3I", "sysop/peer disconnect PEER1")
            assert "Disconnected PEER1." in out

            _, out = await srv._execute_command("AI3I", "sysop/peer delete PEER1")
            assert "Deleted peer PEER1." in out
        finally:
            await store.close()

    asyncio.run(run())


def test_sysop_services_and_restart(tmp_path) -> None:
    restarted: list[str] = []

    async def _component_status():
        return [
            {"component": "telnet", "state": "up", "detail": "7300,7373,8000"},
            {"component": "sysopweb", "state": "up", "detail": "127.0.0.1:8080"},
            {"component": "publicweb", "state": "down", "detail": "127.0.0.1:8081"},
        ]

    async def _component_restart(name: str):
        restarted.append(name)
        if name not in {"telnet", "sysopweb", "all"}:
            return False, "Usage: sysop/restart <telnet|sysopweb|all>"
        return True, f"{name} restarted."

    async def run() -> None:
        db = str(tmp_path / "sysop_services.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(
            cfg,
            store,
            datetime.now(timezone.utc),
            component_status_fn=_component_status,
            component_restart_fn=_component_restart,
        )
        srv._sessions[1] = Session(
            call="AI3I",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.upsert_user_registry("AI3I", now, privilege="sysop")

            _, out = await srv._execute_command("AI3I", "sysop/services")
            assert "Service Status" in out
            assert "telnet" in out and "7300,7373,8000" in out
            assert "sysopweb" in out

            _, out = await srv._execute_command("AI3I", "sysop/restart telnet")
            assert "telnet restarted." in out
            assert restarted[-1] == "telnet"
        finally:
            await store.close()

    asyncio.run(run())


def test_sysop_namespace_hidden_and_denied_for_normal_users(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "sysop_namespace_hidden.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv._execute_command("N0CALL", "show/commands")
            assert "sysop/password" not in out
            assert "sysop/users" not in out
            assert "sysop/audit" not in out

            _, out = await srv._execute_command("N0CALL", "sysop/users")
            assert "permission denied" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_log_category_filter(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "log_filter.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            await srv._execute_command("N0CALL", "wcy A=3")
            await srv._execute_command("N0CALL", "wwv SFI=120")
            await srv._execute_command("N0CALL", "wx fair")
            _, out = await srv._execute_command("N0CALL", "show/log wwv 10")
            assert "wwv:" in out.lower()
            assert "wx:" not in out.lower()
        finally:
            await store.close()

    asyncio.run(run())


def test_sysop_access_matrix_and_telnet_post_policy(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "sysop_access.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(
            call="AI3I",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.upsert_user_registry("AI3I", now, privilege="sysop")

            _, out = await srv._execute_command("AI3I", "sysop/access K1ABC")
            assert "Access for K1ABC" in out
            assert "telnet" in out and "web" in out
            assert "login" in out and "spots" in out and "announce" in out

            _, out = await srv._execute_command("AI3I", "sysop/setaccess K1ABC telnet spots off")
            assert "spots disabled for K1ABC on telnet." in out
            _, out = await srv._execute_command("AI3I", "sysop/setaccess K1ABC telnet chat off")
            assert "chat disabled for K1ABC on telnet." in out
            _, out = await srv._execute_command("AI3I", "sysop/setaccess K1ABC telnet announce off")
            assert "announce disabled for K1ABC on telnet." in out

            srv._sessions[2] = Session(
                call="K1ABC",
                writer=_DummyWriter(),
                connected_at=datetime.now(timezone.utc),
            )
            _, out = await srv._execute_command("K1ABC", "dx 14074.0 N0TST test")
            assert "dx: not allowed via telnet" in out
            _, out = await srv._execute_command("K1ABC", "talk ALL hello")
            assert "talk: not allowed via telnet" in out
            _, out = await srv._execute_command("K1ABC", "announce full hello")
            assert "announce: not allowed via telnet" in out

            _, out = await srv._execute_command("AI3I", "sysop/access K1ABC")
            assert "spots" in out and "off" in out
            assert "chat" in out and "off" in out
            assert "announce" in out and "off" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_telnet_login_denied_when_telnet_access_disabled(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "login_access.db")
        cfg = AppConfig(
            node=NodeConfig(node_call="AI3I-16", registration_required=False, verified_email_required_for_telnet=False),
            telnet=TelnetConfig(host="127.0.0.1", port=0, idle_timeout_seconds=30),
            web=WebConfig(host="127.0.0.1", port=0),
            public_web=PublicWebConfig(),
            store=StoreConfig(sqlite_path=db),
        )
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        now = int(datetime.now(timezone.utc).timestamp())
        await store.set_user_pref("N0CALL", "access.telnet.login", "off", now)
        try:
            await srv.start()
        except OSError:
            pytest.skip("socket bind unavailable in sandbox")
        try:
            sock = (srv._server.sockets or [None])[0]
            assert sock is not None
            host, port = sock.getsockname()[0], sock.getsockname()[1]
            reader, writer = await asyncio.open_connection(host, port)
            prompt = await asyncio.wait_for(reader.readuntil(b"login: "), timeout=2.0)
            assert b"login:" in prompt
            writer.write(b"N0CALL\r\n")
            await writer.drain()
            deny = await asyncio.wait_for(reader.read(4096), timeout=2.0)
            assert b"Login not allowed via telnet" in deny
            writer.close()
            await writer.wait_closed()
        finally:
            await srv.stop()
            await store.close()

    asyncio.run(run())


def test_non_authenticated_users_are_read_only_by_default(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "default_access_template.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(
            call="AI3I",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        srv._sessions[2] = Session(
            call="K1ABC",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.upsert_user_registry("AI3I", now, privilege="sysop")
            await store.upsert_user_registry("K1ABC", now, privilege="")

            _, out = await srv._execute_command("AI3I", "sysop/access K1ABC")
            assert "Login channels: T W" in out
            assert "spots" in out and "off" in out
            assert "announce" in out and "off" in out

            _, out = await srv._execute_command("K1ABC", "dx 14074.0 N0TST test")
            assert "dx: not allowed via telnet" in out

            _, out = await srv._execute_command("K1ABC", "announce full hello")
            assert "announce: not allowed via telnet" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_explicit_ssid_user_does_not_inherit_base_call_access(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "ssid_access_inheritance.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="AI3I", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        srv._sessions[2] = Session(call="AI3I-1", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.upsert_user_registry("AI3I", now, privilege="user")
            await store.upsert_user_registry("AI3I-1", now, privilege="")

            assert await srv._access_allowed("AI3I", "telnet", "spots") is True
            assert await srv._access_allowed("AI3I-1", "telnet", "spots") is False
            assert await srv._privilege_level_for("AI3I-1") == 0

            _, out = await srv._execute_command("AI3I-1", "dx 14074.0 N0TST test")
            assert "dx: not allowed via telnet" in out

            await store.upsert_user_registry("AI3I", now, privilege="sysop")
            _, out = await srv._execute_command("AI3I-1", "sysop/users")
            assert "permission denied" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_explicit_ssid_user_does_not_inherit_base_call_block(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "ssid_block_inheritance.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.upsert_user_registry("N9JR", now, privilege="sysop")
            await store.upsert_user_registry("N9JR-10", now, privilege="user")
            await store.upsert_user_registry("N9JR-13", now, privilege="user")
            await store.set_user_pref("N9JR-13", "blocked_login", "on", now)

            assert await srv._access_allowed("N9JR", "telnet", "login") is True
            assert await srv._access_allowed("N9JR-10", "telnet", "login") is True
            assert await srv._access_allowed("N9JR-13", "telnet", "login") is False
        finally:
            await store.close()

    asyncio.run(run())


def test_cluster_peer_access_is_always_allowed(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "cluster_peer_access.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.upsert_user_registry("AI3I-15", now, privilege="")
            await store.set_user_pref("AI3I-15", "node_family", "pycluster", now)
            await store.set_user_pref("AI3I-15", "blocked_login", "on", now)
            await store.set_user_pref("AI3I-15", "access.telnet.spots", "off", now)

            assert await srv._access_allowed("AI3I-15", "telnet", "login") is True
            assert await srv._access_allowed("AI3I-15", "telnet", "spots") is True
            assert await srv._access_subject("AI3I-15") == ("", False)
        finally:
            await store.close()

    asyncio.run(run())


def test_sysop_path_reports_user_and_peer_paths(tmp_path) -> None:
    async def _stats():
        return {
            "AI3I-16": {
                "inbound": True,
                "profile": "dxspider",
                "transport": "tcp",
                "path_hint": "ipv4 203.0.113.10:53214 -> 198.51.100.5:7300",
            }
        }

    async def run() -> None:
        db = str(tmp_path / "sysop_path.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc), link_stats_fn=_stats)
        srv._sessions[1] = Session(
            call="AI3I",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.upsert_user_registry("AI3I", now, privilege="sysop")
            await store.upsert_user_registry("K1ABC", now, privilege="user")
            await store.record_login("K1ABC", now, "telnet ipv4 198.51.100.24:54012 -> 198.51.100.5:7373")

            _, out = await srv._execute_command("AI3I", "sysop/path K1ABC")
            assert "Path for K1ABC:" in out
            assert "Path: telnet ipv4 198.51.100.24:54012 -> 198.51.100.5:7373" in out

            _, out = await srv._execute_command("AI3I", "sysop/path AI3I-16")
            assert "Path for peer AI3I-16:" in out
            assert "Transport: tcp" in out
            assert "Path: ipv4 203.0.113.10:53214 -> 198.51.100.5:7300" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_contact_fields_persist_and_render(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "contact.db")
        cfg = _mk_config(db)

        store1 = SpotStore(db)
        srv1 = TelnetClusterServer(cfg, store1, datetime.now(timezone.utc))
        srv1._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv1._execute_command("N0CALL", "set/address 123 Main St")
            assert "Address updated for N0CALL." in out
            _, out = await srv1._execute_command("N0CALL", "set/email op@example.net")
            assert "Email updated for N0CALL." in out
        finally:
            await store1.close()

        store2 = SpotStore(db)
        srv2 = TelnetClusterServer(cfg, store2, datetime.now(timezone.utc))
        srv2._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv2._execute_command("N0CALL", "show/registered N0CALL")
            assert "Address: 123 Main St" in out
            assert "Email: op@example.net" in out

            _, out = await srv2._execute_command("N0CALL", "show/station")
            assert "Address: 123 Main St" in out
            assert "Email: op@example.net" in out

            _, out = await srv2._execute_command("N0CALL", "unset/email")
            assert "Email cleared for N0CALL." in out
            _, out = await srv2._execute_command("N0CALL", "show/registered N0CALL")
            assert "Email: not set" in out
            assert "Email: op@example.net" not in out
        finally:
            await store2.close()

    asyncio.run(run())


def test_set_page_limits_long_outputs(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "page.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv._execute_command("N0CALL", "set/page 2")
            assert "Page length set to 2 lines" in out

            _, out = await srv._execute_command("N0CALL", "set/buddy K1AAA K1BBB K1CCC")
            assert "Added 3 buddy entries for N0CALL." in out
            _, out = await srv._execute_command("N0CALL", "show/buddy")
            assert "K1AAA" in out and "K1BBB" in out
            assert "K1CCC" not in out

            _, out = await srv._execute_command("N0CALL", "set/var a 1")
            assert "Variable a updated for N0CALL." in out
            _, out = await srv._execute_command("N0CALL", "set/var b 2")
            assert "Variable b updated for N0CALL." in out
            _, out = await srv._execute_command("N0CALL", "set/var c 3")
            assert "Variable c updated for N0CALL." in out
            _, out = await srv._execute_command("N0CALL", "show/var")
            assert "a: 1" in out and "b: 2" in out
            assert "c: 3" not in out

            # produce several events, verify default page limit on log
            await srv._execute_command("N0CALL", "wcy A=3")
            await srv._execute_command("N0CALL", "wwv SFI=120")
            await srv._execute_command("N0CALL", "wx fair")
            _, out = await srv._execute_command("N0CALL", "show/log")
            assert len([ln for ln in out.splitlines() if ln.strip()]) <= 2

            _, out = await srv._execute_command("N0CALL", "show/log 5")
            assert len([ln for ln in out.splitlines() if ln.strip()]) >= 3
        finally:
            await store.close()

    asyncio.run(run())


def test_logininfo_controls_registered_and_users_output(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "logininfo.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        srv._sessions[2] = Session(
            call="K1ABC",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            await store.upsert_user_registry("N0CALL", 1772343000, display_name="Op One")
            await store.upsert_user_registry("K1ABC", 1772343001, display_name="Op Two")
            await store.record_login("K1ABC", 1772343100, "('203.0.113.1', 7300)")

            _, out = await srv._execute_command("N0CALL", "show/registered K1ABC")
            assert "last_login=" not in out
            assert "last_peer=" not in out

            _, out = await srv._execute_command("N0CALL", "set/logininfo")
            assert "Logininfo set to on for N0CALL." in out

            _, out = await srv._execute_command("N0CALL", "show/registered K1ABC")
            assert "Last Login:" in out
            assert "Last Peer: ipv4 203.0.113.1:7300" in out

            _, out = await srv._execute_command("N0CALL", "show/users")
            assert "last login" in out or "Last " in out

            _, out = await srv._execute_command("N0CALL", "show/registered")
            assert "Last Login" in out.splitlines()[1]
        finally:
            await store.close()

    asyncio.run(run())


def test_startup_commands_manage_and_execute(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "startup.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv._execute_command("N0CALL", "set/startup")
            assert "Startup commands enabled for N0CALL." in out
            _, out = await srv._execute_command("N0CALL", "set/startup show/time")
            assert "Added startup command #" in out
            _, out = await srv._execute_command("N0CALL", "set/startup show/date")
            assert "Added startup command #" in out
            _, out = await srv._execute_command("N0CALL", "set/startup announce test")
            assert "Added startup command #" in out

            _, out = await srv._execute_command("N0CALL", "show/startup")
            assert "Startup for N0CALL: on" in out
            assert "Startup for N0CALL: on" in out
            assert "Startup Commands: 3" in out
            assert "show/time" in out and "show/date" in out

            outs = await srv._run_startup_commands("N0CALL")
            joined = "".join(outs)
            assert "Z" in joined
            assert any(f"-{month}-" in joined for month in ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"))
            assert "skipped unsafe command" in joined

            _, out = await srv._execute_command("N0CALL", "unset/startup")
            assert "Startup commands disabled for N0CALL." in out
            outs2 = await srv._run_startup_commands("N0CALL")
            assert outs2 == []
        finally:
            await store.close()

    asyncio.run(run())


def test_show_set_unset_status_commands_correlate_with_readback(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "status_command_readback.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            cases = [
                ("set/dxcq", "show/dxcq", "DX CQ for N0CALL: on"),
                ("set/dxitu", "show/dxitu", "DX ITU for N0CALL: on"),
                ("set/dxgrid", "show/dxgrid", "DX Grid for N0CALL: on"),
                ("set/logininfo", "show/logininfo", "Login Info for N0CALL: on"),
                ("set/register", "show/register", "Register for N0CALL: on"),
                ("set/prompt", "show/prompt", "Prompt for N0CALL: on"),
                ("set/localnode", "show/localnode", "Local Node for N0CALL: on"),
                ("set/passphrase secret words", "show/passphrase", "Passphrase for N0CALL: secret words"),
                ("set/usstate MA", "show/usstate", "US State for N0CALL: MA"),
            ]
            for set_cmd, show_cmd, expected_show in cases:
                _, out = await srv._execute_command("N0CALL", set_cmd)
                assert "set to" in out
                _, out = await srv._execute_command("N0CALL", show_cmd)
                assert expected_show in out

            unset_cases = [
                ("unset/logininfo", "show/logininfo", "Login Info for N0CALL: off"),
                ("unset/register", "show/register", "Register for N0CALL: off"),
                ("unset/prompt", "show/prompt", "Prompt for N0CALL: off"),
                ("unset/localnode", "show/localnode", "Local Node for N0CALL: off"),
                ("unset/passphrase", "show/passphrase", "Passphrase for N0CALL: off"),
                ("unset/usstate", "show/usstate", "US State for N0CALL: off"),
            ]
            for unset_cmd, show_cmd, expected_show in unset_cases:
                _, out = await srv._execute_command("N0CALL", unset_cmd)
                assert "set to off" in out or "cleared" in out.lower()
                _, out = await srv._execute_command("N0CALL", show_cmd)
                assert expected_show in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_registered_includes_location_detail_fields(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "registered_location.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.upsert_user_registry(
                "K1ABC",
                now,
                display_name="Pat Example",
                qth="Boston, MA",
                qra="FN42LI",
                email="pat@example.net",
            )
            await store.set_user_pref("K1ABC", "location", "Boston, MA", now)
            await store.set_user_pref("K1ABC", "usstate", "MA", now)
            _, out = await srv._execute_command("N0CALL", "show/registered K1ABC")
            assert "Location Detail: Boston, MA" in out
            assert "US State: MA" in out

            _, out = await srv._execute_command("N0CALL", "show/station K1ABC")
            assert "Location Detail: Boston, MA" in out
            assert "US State: MA" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_login_startup_dx_and_wwv_output_precedes_prompt(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "login_startup_prompt_order.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        writer = _DummyWriter()
        srv._sessions[1] = Session(
            call="N0CALL",
            writer=writer,
            connected_at=datetime.now(timezone.utc),
        )
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.add_spot(Spot(14236.0, "VP2ETE", now, "57 in Boston", "N1CFB", "N2WQ-1", ""))
            await store.add_bulletin("wwv", "AE5E", "LOCAL", now, "SFI=116 A=5 K=2 No Storms -> No Storms")
            await store.set_user_pref("N0CALL", "startup", "on", now)
            await store.add_startup_command("N0CALL", "show/dx 1", now)
            await store.add_startup_command("N0CALL", "show/wwv 1", now)

            await srv._write(writer, await srv._welcome_block("N0CALL"))
            for out in await srv._run_startup_commands("N0CALL"):
                await srv._write(writer, out)
            notice = await srv._registration_notice_block("N0CALL", node_family="")
            if notice:
                await srv._write(writer, notice + "\r\n")
            await srv._write_prompt_for_session(srv._sessions[1])

            rendered = writer.buffer.decode("utf-8", errors="replace")
            prompt = await srv._prompt("N0CALL")
            assert "\r\n 14236.0  VP2ETE" in rendered
            assert rendered.index(" 14236.0  VP2ETE") < rendered.index("Date        Hour   SFI   A   K Forecast")
            assert rendered.index("Date        Hour   SFI   A   K Forecast") < rendered.index(prompt)
            assert rendered.index("Date        Hour   SFI   A   K Forecast") < rendered.index("run REGISTER")
            assert f"{prompt}Date        Hour" not in rendered
            assert f"{prompt} 14236.0" not in rendered
        finally:
            await store.close()

    asyncio.run(run())


def test_startup_commands_persist_across_server_instances(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "startup_persist.db")
        cfg = _mk_config(db)

        store1 = SpotStore(db)
        srv1 = TelnetClusterServer(cfg, store1, datetime.now(timezone.utc))
        srv1._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            await srv1._execute_command("N0CALL", "set/startup")
            await srv1._execute_command("N0CALL", "set/startup show/time")
            await srv1._execute_command("N0CALL", "set/startup show/date")
        finally:
            await store1.close()

        store2 = SpotStore(db)
        srv2 = TelnetClusterServer(cfg, store2, datetime.now(timezone.utc))
        srv2._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv2._execute_command("N0CALL", "show/startup")
            assert "Startup for N0CALL: on" in out
            assert "Startup Commands: 2" in out
            assert "show/time" in out and "show/date" in out
        finally:
            await store2.close()

    asyncio.run(run())


def test_show_program_time_and_date_include_readback(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "program_time_date.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            _, out = await srv._execute_command("N0CALL", "show/program")
            assert "Name: pyCluster" in out
            assert "Mode: DXSpider compatibility" in out
            assert "Version:" in out

            _, out = await srv._execute_command("N0CALL", "show/time")
            assert "UTC time:" in out

            _, out = await srv._execute_command("N0CALL", "show/date")
            assert "UTC date:" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_maxconnect_enforced_on_login(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "maxconn_login.db")
        cfg = AppConfig(
            node=NodeConfig(),
            telnet=TelnetConfig(host="127.0.0.1", port=0, idle_timeout_seconds=30),
            web=WebConfig(),
            public_web=PublicWebConfig(),
            store=StoreConfig(sqlite_path=db),
        )
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        await store.set_user_pref("N0CALL", "maxconnect", "1", 1772345000)
        now = int(datetime.now(timezone.utc).timestamp())
        await store.record_login("N0CALL", now, "test-setup")
        try:
            await srv.start()
        except OSError:
            pytest.skip("socket bind unavailable in sandbox")
        try:
            sock = (srv._server.sockets or [None])[0]
            assert sock is not None
            host, port = sock.getsockname()[0], sock.getsockname()[1]

            r1, w1 = await asyncio.open_connection(host, port)
            p1 = await asyncio.wait_for(r1.readuntil(b"login: "), timeout=2.0)
            assert b"login:" in p1
            w1.write(b"N0CALL\r\n")
            await w1.drain()
            hello = await asyncio.wait_for(r1.read(4096), timeout=2.0)
            assert b"Welcome, N0CALL." in hello
            assert b"N0NODE-1>" in hello

            r2, w2 = await asyncio.open_connection(host, port)
            p2 = await asyncio.wait_for(r2.readuntil(b"login: "), timeout=2.0)
            assert b"login:" in p2
            w2.write(b"N0CALL\r\n")
            await w2.drain()
            deny = await asyncio.wait_for(r2.read(4096), timeout=2.0)
            assert b"Too many connections for N0CALL" in deny
            assert b"Maximum allowed: 1" in deny

            w2.close()
            await w2.wait_closed()
            w1.close()
            await w1.wait_closed()
        finally:
            await srv.stop()
            await store.close()

    asyncio.run(run())


def test_uservar_commands_persist_and_render(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "uservar.db")
        cfg = _mk_config(db)

        store1 = SpotStore(db)
        srv1 = TelnetClusterServer(cfg, store1, datetime.now(timezone.utc))
        srv1._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv1._execute_command("N0CALL", "set/user N0CALL")
            assert "User record created or updated for N0CALL." in out
            _, out = await srv1._execute_command("N0CALL", "set/uservar monitor all")
            assert "User variable monitor updated for N0CALL." in out
            _, out = await srv1._execute_command("N0CALL", "set/uservar color=amber")
            assert "User variable color updated for N0CALL." in out
        finally:
            await store1.close()

        store2 = SpotStore(db)
        srv2 = TelnetClusterServer(cfg, store2, datetime.now(timezone.utc))
        srv2._sessions[1] = Session(
            call="N0CALL",
            writer=_DummyWriter(),
            connected_at=datetime.now(timezone.utc),
        )
        try:
            _, out = await srv2._execute_command("N0CALL", "show/registered N0CALL")
            assert "uservar.monitor: all" in out
            assert "uservar.color: amber" in out

            _, out = await srv2._execute_command("N0CALL", "show/station")
            assert "uservar.monitor: all" in out
            assert "uservar.color: amber" in out

            _, out = await srv2._execute_command("N0CALL", "unset/uservar monitor")
            assert "User variable monitor cleared for N0CALL." in out
            _, out = await srv2._execute_command("N0CALL", "show/registered N0CALL")
            assert "uservar.monitor: all" not in out
            assert "uservar.color: amber" in out
        finally:
            await store2.close()

    asyncio.run(run())


def test_telnet_login_prompts_for_password_when_required(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "login_password.db")
        cfg = AppConfig(
            node=NodeConfig(node_call="AI3I-15", require_password=True),
            telnet=TelnetConfig(host="127.0.0.1", port=0, idle_timeout_seconds=30),
            web=WebConfig(host="127.0.0.1", port=0),
            public_web=PublicWebConfig(),
            store=StoreConfig(sqlite_path=db),
        )
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        now = int(datetime.now(timezone.utc).timestamp())
        await store.set_user_pref("N0CALL", "password", "pw1", now)
        await store.record_login("N0CALL", now, "test-setup")
        try:
            await srv.start()
        except OSError:
            pytest.skip("socket bind unavailable in sandbox")
        try:
            sock = (srv._server.sockets or [None])[0]
            assert sock is not None
            host, port = sock.getsockname()[0], sock.getsockname()[1]

            r1, w1 = await asyncio.open_connection(host, port)
            prompt = await asyncio.wait_for(r1.readuntil(b"login: "), timeout=2.0)
            assert b"login:" in prompt
            w1.write(b"N0CALL\r\n")
            await w1.drain()
            pw = await asyncio.wait_for(r1.readuntil(b"password: "), timeout=2.0)
            assert b"password:" in pw
            w1.write(b"pw1\r\n")
            await w1.drain()
            hello = await asyncio.wait_for(r1.read(4096), timeout=2.0)
            assert b"Welcome" in hello
            assert b"AI3I-15>" in hello
            w1.close()
            await w1.wait_closed()

            r2, w2 = await asyncio.open_connection(host, port)
            await asyncio.wait_for(r2.readuntil(b"login: "), timeout=2.0)
            w2.write(b"N0CALL\r\n")
            await w2.drain()
            await asyncio.wait_for(r2.readuntil(b"password: "), timeout=2.0)
            w2.write(b"bad\r\n")
            await w2.drain()
            deny = await asyncio.wait_for(r2.read(4096), timeout=2.0)
            assert b"Login failed" in deny
            w2.close()
            await w2.wait_closed()
        finally:
            await srv.stop()
            await store.close()

    asyncio.run(run())


def test_telnet_bad_passwords_lock_account_until_sysop_unlock(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "login_bad_password_locks.db")
        cfg = AppConfig(
            node=NodeConfig(node_call="AI3I-15", require_password=True),
            telnet=TelnetConfig(host="127.0.0.1", port=0, idle_timeout_seconds=30),
            web=WebConfig(host="127.0.0.1", port=0),
            public_web=PublicWebConfig(),
            store=StoreConfig(sqlite_path=db),
        )
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry("N0CALL", now, privilege="user")
        await store.set_user_pref("N0CALL", "password", "pw1", now)
        await store.set_user_pref("N0CALL", "email_verified_epoch", str(now), now)
        try:
            await srv.start()
        except OSError:
            pytest.skip("socket bind unavailable in sandbox")
        try:
            sock = (srv._server.sockets or [None])[0]
            assert sock is not None
            host, port = sock.getsockname()[0], sock.getsockname()[1]

            for _idx in range(5):
                reader, writer = await asyncio.open_connection(host, port)
                await asyncio.wait_for(reader.readuntil(b"login: "), timeout=2.0)
                writer.write(b"N0CALL\r\n")
                await writer.drain()
                await asyncio.wait_for(reader.readuntil(b"password: "), timeout=2.0)
                writer.write(b"bad-password\r\n")
                await writer.drain()
                deny = await asyncio.wait_for(reader.read(4096), timeout=2.0)
                assert b"Login failed" in deny
                writer.close()
                await writer.wait_closed()

            assert await store.get_user_pref("N0CALL", "registration_state") == "locked"
            assert await store.get_user_pref("N0CALL", "failed_password_count") == "5"

            reader, writer = await asyncio.open_connection(host, port)
            await asyncio.wait_for(reader.readuntil(b"login: "), timeout=2.0)
            writer.write(b"N0CALL\r\n")
            await writer.drain()
            locked = await asyncio.wait_for(reader.read(4096), timeout=2.0)
            assert b"Account N0CALL is locked" in locked
            assert b"password:" not in locked
            writer.close()
            await writer.wait_closed()
        finally:
            await srv.stop()
            await store.close()

    asyncio.run(run())


def test_telnet_failed_password_counter_sets_locked_state(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "failed_password_counter.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        try:
            for _idx in range(5):
                await srv._record_telnet_password_failure("N0CALL-1", ("203.0.113.10", 50000))

            assert await store.get_user_pref("N0CALL", "registration_state") == "locked"
            assert await store.get_user_pref("N0CALL", "failed_password_count") == "5"
            row = await store.get_user_registry("N0CALL")
            assert row is not None

            await srv._clear_telnet_password_failures("N0CALL-1")
            assert await store.get_user_pref("N0CALL", "failed_password_count") is None
            assert await store.get_user_pref("N0CALL", "failed_password_locked_epoch") is None
        finally:
            await store.close()

    asyncio.run(run())


def test_telnet_first_login_forces_password_creation(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "first_login_password.db")
        cfg = AppConfig(
            node=NodeConfig(node_call="AI3I-16", require_password=True),
            telnet=TelnetConfig(host="127.0.0.1", port=0, idle_timeout_seconds=30),
            web=WebConfig(host="127.0.0.1", port=0),
            public_web=PublicWebConfig(),
            store=StoreConfig(sqlite_path=db),
        )
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        try:
            await srv.start()
        except OSError:
            pytest.skip("socket bind unavailable in sandbox")
        try:
            sock = (srv._server.sockets or [None])[0]
            assert sock is not None
            host, port = sock.getsockname()[0], sock.getsockname()[1]

            r1, w1 = await asyncio.open_connection(host, port)
            await asyncio.wait_for(r1.readuntil(b"login: "), timeout=2.0)
            w1.write(b"N0CALL\r\n")
            await w1.drain()
            first = await asyncio.wait_for(r1.readuntil(b"new password: "), timeout=2.0)
            assert b"A password is required before continuing." in first
            w1.write(b"pw1\r\n")
            await w1.drain()
            confirm = await asyncio.wait_for(r1.readuntil(b"confirm password: "), timeout=2.0)
            assert b"confirm password:" in confirm
            w1.write(b"pw1\r\n")
            await w1.drain()
            hello = await asyncio.wait_for(r1.read(4096), timeout=2.0)
            assert b"Password set for N0CALL." in hello
            assert b"Welcome" in hello
            saved = await store.get_user_pref("N0CALL", "password")
            assert is_password_hash(saved)
            assert verify_password("pw1", saved)
            w1.close()
            await w1.wait_closed()

            r2, w2 = await asyncio.open_connection(host, port)
            await asyncio.wait_for(r2.readuntil(b"login: "), timeout=2.0)
            w2.write(b"N0CALL\r\n")
            await w2.drain()
            await asyncio.wait_for(r2.readuntil(b"password: "), timeout=2.0)
            w2.write(b"pw1\r\n")
            await w2.drain()
            hello2 = await asyncio.wait_for(r2.read(4096), timeout=2.0)
            assert b"Welcome" in hello2
            w2.close()
            await w2.wait_closed()
        finally:
            await srv.stop()
            await store.close()

    asyncio.run(run())


def test_telnet_login_without_required_password_skips_first_time_password_setup(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "login_optional_password.db")
        cfg = AppConfig(
            node=NodeConfig(node_call="AI3I-16", require_password=False, registration_required=False, verified_email_required_for_telnet=False),
            telnet=TelnetConfig(host="127.0.0.1", port=0, idle_timeout_seconds=30),
            web=WebConfig(host="127.0.0.1", port=0),
            public_web=PublicWebConfig(),
            store=StoreConfig(sqlite_path=db),
        )
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        try:
            await srv.start()
        except OSError:
            pytest.skip("socket bind unavailable in sandbox")
        try:
            sock = (srv._server.sockets or [None])[0]
            assert sock is not None
            host, port = sock.getsockname()[0], sock.getsockname()[1]

            r1, w1 = await asyncio.open_connection(host, port)
            await asyncio.wait_for(r1.readuntil(b"login: "), timeout=2.0)
            w1.write(b"N0CALL\r\n")
            await w1.drain()
            hello = await asyncio.wait_for(r1.read(4096), timeout=2.0)
            assert b"new password:" not in hello
            assert b"password:" not in hello
            assert b"Welcome" in hello
            assert await store.get_user_pref("N0CALL", "password") is None
            w1.close()
            await w1.wait_closed()
        finally:
            await srv.stop()
            await store.close()

    asyncio.run(run())


def test_telnet_login_email_verification_expired_code_points_user_back_to_register(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "telnet_login_verify_expired_code.db")
        cfg = _mk_config(db)
        cfg.smtp.host = "smtp.example.test"
        cfg.smtp.from_addr = "cluster@example.test"
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        reader = asyncio.StreamReader()
        reader.feed_data(b"123456\r\n")
        writer = _DummyWriter()
        now = int(datetime.now(timezone.utc).timestamp())

        async def _issue(*, call: str, email: str, purpose: str):
            await store.save_mfa_challenge(
                challenge_id="expired-login-verification",
                call=call,
                purpose=purpose,
                code="123456",
                expires_epoch=now - 60,
                attempts_left=5,
                issued_epoch=now - 600,
            )
            return "expired-login-verification", now - 60

        srv._mfa.issue = _issue  # type: ignore[method-assign]
        try:
            await store.upsert_user_registry("N1NEW", now, email="new@example.test")
            ok = await srv._require_verified_email_for_login("N1NEW", reader, writer)  # type: ignore[arg-type]
            assert ok is False
            assert "Verification code expired. Run REGISTER again to request a new code." in writer.buffer.decode("utf-8", "ignore")
        finally:
            await store.close()

    asyncio.run(run())


def test_telnet_prelogin_registration_expired_code_points_user_back_to_register(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "telnet_prelogin_register_expired_code.db")
        cfg = _mk_config(db)
        cfg.smtp.host = "smtp.example.test"
        cfg.smtp.from_addr = "cluster@example.test"
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        reader = asyncio.StreamReader()
        reader.feed_data(
            b"y\r\nNew User\r\nW1AW\r\nHartford\r\nFN31\r\nnew@example.test\r\nPlease approve me\r\n123456\r\n"
        )
        writer = _DummyWriter()
        now = int(datetime.now(timezone.utc).timestamp())

        async def _issue(*, call: str, email: str, purpose: str):
            await store.save_mfa_challenge(
                challenge_id="expired-prelogin-registration",
                call=call,
                purpose=purpose,
                code="123456",
                expires_epoch=now - 60,
                attempts_left=5,
                issued_epoch=now - 600,
            )
            return "expired-prelogin-registration", now - 60

        srv._mfa.issue = _issue  # type: ignore[method-assign]
        try:
            ok = await srv._prompt_registration_request_before_login("N1NEW", reader, writer)  # type: ignore[arg-type]
            assert ok is False
            assert "Verification code expired. Run REGISTER again to request a new code." in writer.buffer.decode("utf-8", "ignore")
            assert await store.get_registration_request("N1NEW") is None
        finally:
            await store.close()

    asyncio.run(run())


def test_telnet_registration_required_implies_password_even_if_toggle_off(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "registration_implies_password.db")
        cfg = AppConfig(
            node=NodeConfig(
                node_call="AI3I-16",
                require_password=False,
                registration_required=True,
                verified_email_required_for_telnet=False,
            ),
            telnet=TelnetConfig(host="127.0.0.1", port=0, idle_timeout_seconds=30),
            web=WebConfig(host="127.0.0.1", port=0),
            public_web=PublicWebConfig(),
            store=StoreConfig(sqlite_path=db),
        )
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry("N0CALL", now, privilege="user", email="n0call@example.test")
        await store.set_user_pref("N0CALL", "email_verified_epoch", str(now), now)
        try:
            await srv.start()
        except OSError:
            pytest.skip("socket bind unavailable in sandbox")
        try:
            sock = (srv._server.sockets or [None])[0]
            assert sock is not None
            host, port = sock.getsockname()[0], sock.getsockname()[1]

            r1, w1 = await asyncio.open_connection(host, port)
            await asyncio.wait_for(r1.readuntil(b"login: "), timeout=2.0)
            w1.write(b"N0CALL\r\n")
            await w1.drain()
            first = await asyncio.wait_for(r1.readuntil(b"new password: "), timeout=2.0)
            assert b"A password is required before continuing." in first
            w1.close()
            await w1.wait_closed()
        finally:
            await srv.stop()
            await store.close()

    asyncio.run(run())


def test_telnet_registration_required_can_queue_registration_request(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "telnet_registration_request.db")
        cfg = AppConfig(
            node=NodeConfig(
                node_call="AI3I-16",
                require_password=False,
                registration_required=True,
                verified_email_required_for_telnet=False,
            ),
            telnet=TelnetConfig(host="127.0.0.1", port=0, idle_timeout_seconds=30),
            web=WebConfig(host="127.0.0.1", port=0),
            public_web=PublicWebConfig(),
            store=StoreConfig(sqlite_path=db),
        )
        cfg.smtp.host = "smtp.example.test"
        cfg.smtp.from_addr = "cluster@example.test"
        store = SpotStore(db)
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry("AI3I", now, privilege="sysop", email="sysop@example.test")
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._mfa._sender = lambda _rcpt, _subject, _body: None  # type: ignore[assignment]
        try:
            await srv.start()
        except OSError:
            pytest.skip("socket bind unavailable in sandbox")
        try:
            sock = (srv._server.sockets or [None])[0]
            assert sock is not None
            host, port = sock.getsockname()[0], sock.getsockname()[1]

            r1, w1 = await asyncio.open_connection(host, port)
            await asyncio.wait_for(r1.readuntil(b"login: "), timeout=2.0)
            w1.write(b"N1NEW\r\n")
            await w1.drain()
            await asyncio.wait_for(r1.readuntil(b"[Y/n]: "), timeout=2.0)
            w1.write(b"y\r\n")
            await w1.drain()
            for answer in (
                b"New User\r\n",
                b"W1AW\r\n",
                b"Hartford\r\n",
                b"FN31\r\n",
                b"new@example.test\r\n",
                b"Please approve me\r\n",
            ):
                await asyncio.sleep(0)
                w1.write(answer)
                await w1.drain()
            await asyncio.wait_for(r1.readuntil(b"verification code: "), timeout=2.0)
            rows = await store.list_registration_requests(status="pending", limit=5, offset=0)
            assert rows == []
            async with store._lock:
                pending = store._conn.execute(
                    "SELECT challenge_id, code FROM mfa_challenges WHERE call = ? AND purpose = ? ORDER BY issued_epoch DESC LIMIT 1",
                    ("N1NEW", "telnet-register"),
                ).fetchone()
            assert pending is not None
            otp = str(pending["code"])
            w1.write((otp + "\r\n").encode("ascii"))
            await w1.drain()
            final = await asyncio.wait_for(r1.read(4096), timeout=2.0)
            assert b"Registration request submitted for N1NEW." in final
            req = await store.get_registration_request("N1NEW")
            assert req is not None
            assert str(req["status"]) == "pending"
            assert str(req["email"]) == "new@example.test"
            assert await store.get_user_registry("N1NEW") is None
            w1.close()
            await w1.wait_closed()
        finally:
            await srv.stop()
            await store.close()

    asyncio.run(run())


def test_telnet_first_login_password_mismatch_reprompts(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "first_login_password_retry.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        reader = asyncio.StreamReader()
        writer = _DummyWriter()
        try:
            reader.feed_data(b"one\r\ntwo\r\nthree\r\nthree\r\n")
            ok = await srv._prompt_new_password("N0CALL", reader, writer)  # type: ignore[arg-type]
            text = writer.buffer.decode("utf-8", errors="replace")
            assert ok is True
            assert "Passwords did not match. Try again." in text
            assert text.count("new password: ") == 2
            saved = await store.get_user_pref("N0CALL", "password")
            assert is_password_hash(saved)
            assert verify_password("three", saved)
        finally:
            await store.close()

    asyncio.run(run())


def test_telnet_set_password_requires_confirmation(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "set_password_confirm.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        try:
            _, out = await srv._execute_command("N0CALL", "set/password one")
            assert "Usage: set/password <newpass> <confirm-newpass>" in out
            assert await store.get_user_pref("N0CALL", "password") is None

            _, out = await srv._execute_command("N0CALL", "set/password one two")
            assert "Passwords did not match." in out
            assert await store.get_user_pref("N0CALL", "password") is None

            _, out = await srv._execute_command("N0CALL", "set/password three three")
            assert "Password updated for N0CALL." in out
            saved = await store.get_user_pref("N0CALL", "password")
            assert is_password_hash(saved)
            assert verify_password("three", saved)
        finally:
            await store.close()

    asyncio.run(run())


def test_telnet_set_password_interactive_mismatch_reprompts(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "set_password_interactive_retry.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        reader = asyncio.StreamReader()
        writer = _DummyWriter()
        try:
            reader.feed_data(b"one\r\ntwo\r\nthree\r\nthree\r\n")
            ok = await srv._prompt_change_password("N0CALL", reader, writer)  # type: ignore[arg-type]
            text = writer.buffer.decode("utf-8", errors="replace")
            assert ok is True
            assert "Enter and confirm a new password." in text
            assert "Passwords did not match. Try again." in text
            assert text.count("new password: ") == 2
            saved = await store.get_user_pref("N0CALL", "password")
            assert is_password_hash(saved)
            assert verify_password("three", saved)
        finally:
            await store.close()

    asyncio.run(run())


def test_telnet_first_login_password_mismatch_socket_stays_open(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "first_login_password_retry_socket.db")
        cfg = AppConfig(
            node=NodeConfig(node_call="AI3I-16", require_password=True, registration_required=False, verified_email_required_for_telnet=False),
            telnet=TelnetConfig(host="127.0.0.1", port=0, idle_timeout_seconds=30),
            web=WebConfig(host="127.0.0.1", port=0),
            public_web=PublicWebConfig(),
            store=StoreConfig(sqlite_path=db),
        )
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry("G0AA", now, display_name="Test User")
        await store.record_login("G0AA", now, "test-setup")
        try:
            await srv.start()
        except OSError:
            pytest.skip("socket bind unavailable in sandbox")
        try:
            sock = (srv._server.sockets or [None])[0]
            assert sock is not None
            host, port = sock.getsockname()[0], sock.getsockname()[1]
            reader, writer = await asyncio.open_connection(host, port)
            await asyncio.wait_for(reader.readuntil(b"login: "), timeout=2.0)
            writer.write(b"g0aa\r\n")
            await writer.drain()
            await asyncio.wait_for(reader.readuntil(b"new password: "), timeout=2.0)
            writer.write(b"one\r\n")
            await writer.drain()
            await asyncio.wait_for(reader.readuntil(b"confirm password: "), timeout=2.0)
            writer.write(b"two\r\n")
            await writer.drain()
            retry = await asyncio.wait_for(reader.readuntil(b"new password: "), timeout=2.0)
            assert b"Passwords did not match. Try again." in retry
            writer.write(b"three\r\n")
            await writer.drain()
            await asyncio.wait_for(reader.readuntil(b"confirm password: "), timeout=2.0)
            writer.write(b"three\r\n")
            await writer.drain()
            hello = await asyncio.wait_for(reader.readuntil(b"AI3I-16> "), timeout=2.0)
            assert b"Password set for G0AA." in hello
            assert b"Welcome" in hello
            saved = await store.get_user_pref("G0AA", "password")
            assert is_password_hash(saved)
            assert verify_password("three", saved)
            writer.close()
            await writer.wait_closed()
        finally:
            await srv.stop()
            await store.close()

    asyncio.run(run())


def test_telnet_idle_timeout_sends_keepalive_after_login(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "telnet_idle_keepalive.db")
        cfg = _mk_config(db)
        cfg.telnet.keepalive_interval_seconds = 0.01
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        reader = asyncio.StreamReader()
        writer = _DummyWriter()

        async def _feed_later() -> None:
            await asyncio.sleep(0.04)
            reader.feed_data(b"show/version\r\n")

        try:
            task = asyncio.create_task(_feed_later())
            line = await asyncio.wait_for(
                srv._readline(
                    reader,
                    writer,  # type: ignore[arg-type]
                    idle_keepalive=True,
                    idle_keepalive_text=lambda: srv._idle_keepalive_prompt("N0CALL"),
                ),
                timeout=1.0,
            )
            await task
            assert line == "show/version"
            assert b"N0CALL" not in bytes(writer.buffer)
            assert b"> " in bytes(writer.buffer)
            assert bytes(writer.buffer).endswith(b"> ")
            assert b"\r\n\r\n" not in bytes(writer.buffer)
            assert bytes((srv._TELNET_IAC, srv._TELNET_NOP)) not in bytes(writer.buffer)
        finally:
            await store.close()

    asyncio.run(run())


def test_telnet_idle_keepalive_stops_on_eof(tmp_path) -> None:
    async def run() -> None:
        cfg = _mk_config(str(tmp_path / "telnet_keepalive_eof.db"))
        cfg.telnet.keepalive_interval_seconds = 0.01
        store = SpotStore(cfg.store.sqlite_path)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        reader = asyncio.StreamReader()
        reader.feed_eof()
        writer = _DummyWriter()
        try:
            line = await asyncio.wait_for(
                srv._readline(reader, writer, idle_keepalive=True, idle_keepalive_text="\r\n> "),
                timeout=0.2,
            )
            assert line is None
            assert bytes(writer.buffer) == b""
        finally:
            await store.close()

    asyncio.run(run())


def test_telnet_first_login_runs_registration_interview_for_normal_users(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "first_login_registration_interview.db")
        cfg = AppConfig(
            node=NodeConfig(node_call="AI3I-16", require_password=False, registration_required=False, verified_email_required_for_telnet=False),
            telnet=TelnetConfig(host="127.0.0.1", port=0, idle_timeout_seconds=30),
            web=WebConfig(host="127.0.0.1", port=0),
            public_web=PublicWebConfig(),
            store=StoreConfig(sqlite_path=db),
        )
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry("N0CALL", now, privilege="user", email="")
        try:
            await srv.start()
        except OSError:
            pytest.skip("socket bind unavailable in sandbox")
        try:
            sock = (srv._server.sockets or [None])[0]
            assert sock is not None
            host, port = sock.getsockname()[0], sock.getsockname()[1]

            r1, w1 = await asyncio.open_connection(host, port)
            await asyncio.wait_for(r1.readuntil(b"login: "), timeout=2.0)
            w1.write(b"N0CALL\r\n")
            await w1.drain()
            hello = await asyncio.wait_for(r1.readuntil(b"Name: "), timeout=2.0)
            assert b"Welcome" in hello
            assert b"Let's finish your registration profile for N0CALL." in hello
            w1.write(b"Alice Example\r\n")
            await w1.drain()
            await asyncio.wait_for(r1.readuntil(b"Home node: "), timeout=2.0)
            w1.write(b"W1AW\r\n")
            await w1.drain()
            await asyncio.wait_for(r1.readuntil(b"Grid square: "), timeout=2.0)
            w1.write(b"FN42\r\n")
            await w1.drain()
            await asyncio.wait_for(r1.readuntil(b"QTH / location: "), timeout=2.0)
            w1.write(b"\r\n")
            await w1.drain()
            await asyncio.wait_for(r1.readuntil(b"Email address: "), timeout=2.0)
            w1.write(b"alice@example.test\r\n")
            await w1.drain()
            await asyncio.wait_for(r1.readuntil(b"new password: "), timeout=2.0)
            w1.write(b"pw1\r\n")
            await w1.drain()
            await asyncio.wait_for(r1.readuntil(b"confirm password: "), timeout=2.0)
            w1.write(b"pw1\r\n")
            await w1.drain()
            tail = await asyncio.wait_for(r1.read(4096), timeout=2.0)
            assert b"Registration interview complete." in tail
            assert b"Registration checklist for N0CALL:" in tail
            assert b"QTH: set/qth" in tail
            assert b"Password: set/password" not in tail
            row = await store.get_user_registry("N0CALL")
            assert row is not None
            assert str(row["display_name"]) == "Alice Example"
            assert str(row["home_node"]) == "W1AW"
            assert str(row["qra"]) == "FN42"
            assert str(row["email"]) == "alice@example.test"
            assert await store.get_user_pref("N0CALL", "homenode") == "W1AW"
            assert verify_password("pw1", str(await store.get_user_pref("N0CALL", "password")))
            w1.close()
            await w1.wait_closed()
        finally:
            await srv.stop()
            await store.close()

    asyncio.run(run())


def test_first_login_interview_uses_entered_qra_before_location_estimate(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "interview_qra_before_qth.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry(
            "N0CALL",
            now,
            display_name="Alice Example",
            home_node="W1AW",
            email="alice@example.test",
            privilege="user",
        )
        reader = asyncio.StreamReader()
        writer = _DummyWriter()
        reader.feed_data(b"EN63AA\r\nMilwaukee, WI\r\n")
        reader.feed_eof()
        try:
            ok = await srv._run_first_login_interview(
                "N0CALL",
                reader,
                writer,  # type: ignore[arg-type]
                node_family="",
                password_set=True,
            )
            assert ok is True
            prompts = bytes(writer.buffer)
            assert prompts.index(b"Grid square: ") < prompts.index(b"QTH / location: ")
            row = await store.get_user_registry("N0CALL")
            assert row is not None
            assert str(row["qra"]) == "EN63AA"
            assert str(row["qth"]) == "Milwaukee, WI"
        finally:
            await store.close()

    asyncio.run(run())


def test_first_login_interview_defaults_blank_qra_to_node_locator(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "interview_qra_node_default.db")
        cfg = _mk_config(db)
        cfg.node.node_locator = "FN20"
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry(
            "N0CALL",
            now,
            display_name="Alice Example",
            home_node="W1AW",
            email="alice@example.test",
            privilege="user",
        )
        reader = asyncio.StreamReader()
        writer = _DummyWriter()
        reader.feed_data(b"\r\n\r\n\r\n")
        reader.feed_eof()
        try:
            ok = await srv._run_first_login_interview(
                "N0CALL",
                reader,
                writer,  # type: ignore[arg-type]
                node_family="",
                password_set=True,
            )
            assert ok is True
            row = await store.get_user_registry("N0CALL")
            assert row is not None
            assert str(row["qra"]) == "FN20"
        finally:
            await store.close()

    asyncio.run(run())


def test_first_login_interview_does_not_offer_mfa_before_email_verified(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "interview_no_mfa_before_verified.db")
        cfg = _mk_config(db)
        cfg.smtp.host = "smtp.example.test"
        cfg.smtp.from_addr = "cluster@example.test"
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry(
            "N0CALL",
            now,
            display_name="Alice Example",
            home_node="W1AW",
            qth="Milwaukee, WI",
            qra="EN63AA",
            email="alice@example.test",
            privilege="user",
        )
        await store.set_user_pref("N0CALL", "homenode", "W1AW", now)
        await store.set_user_pref("N0CALL", "forward_lat", "43.0389", now)
        await store.set_user_pref("N0CALL", "forward_lon", "-87.9065", now)
        await store.set_user_pref("N0CALL", "location", "Milwaukee, WI", now)
        reader = asyncio.StreamReader()
        writer = _DummyWriter()
        reader.feed_eof()
        try:
            ok = await srv._run_first_login_interview(
                "N0CALL",
                reader,
                writer,  # type: ignore[arg-type]
                node_family="",
                password_set=True,
            )
            assert ok is True
            assert b"Enable email MFA now?" not in bytes(writer.buffer)
            assert await store.get_user_pref("N0CALL", "mfa_email_otp") is None
        finally:
            await store.close()

    asyncio.run(run())


def test_first_login_interview_prompts_for_missing_password(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "interview_password_setup.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry(
            "N0CALL",
            now,
            display_name="Alice Example",
            home_node="W1AW",
            qth="Milwaukee, WI",
            qra="EN63AA",
            email="alice@example.test",
            privilege="user",
        )
        await store.set_user_pref("N0CALL", "homenode", "W1AW", now)
        await store.set_user_pref("N0CALL", "forward_lat", "43.0389", now)
        await store.set_user_pref("N0CALL", "forward_lon", "-87.9065", now)
        await store.set_user_pref("N0CALL", "location", "Milwaukee, WI", now)
        reader = asyncio.StreamReader()
        writer = _DummyWriter()
        reader.feed_data(b"pw1\r\npw1\r\n")
        reader.feed_eof()
        try:
            ok = await srv._run_first_login_interview(
                "N0CALL",
                reader,
                writer,  # type: ignore[arg-type]
                node_family="",
                password_set=False,
            )
            assert ok is True
            output = bytes(writer.buffer)
            assert b"new password: " in output
            assert b"Password setup is still required" not in output
            assert verify_password("pw1", str(await store.get_user_pref("N0CALL", "password")))
        finally:
            await store.close()

    asyncio.run(run())


def test_first_login_interview_does_not_offer_mfa_when_node_policy_requires_it(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "interview_mfa_node_policy.db")
        cfg = _mk_config(db)
        cfg.smtp.host = "smtp.example.test"
        cfg.smtp.from_addr = "cluster@example.test"
        cfg.mfa.enabled = True
        cfg.mfa.require_for_users = True
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry(
            "N0CALL",
            now,
            display_name="Alice Example",
            home_node="W1AW",
            qth="Milwaukee, WI",
            qra="EN63AA",
            email="alice@example.test",
            privilege="user",
        )
        await store.set_user_pref("N0CALL", "homenode", "W1AW", now)
        await store.set_user_pref("N0CALL", "forward_lat", "43.0389", now)
        await store.set_user_pref("N0CALL", "forward_lon", "-87.9065", now)
        await store.set_user_pref("N0CALL", "email_verified_epoch", str(now), now)
        reader = asyncio.StreamReader()
        writer = _DummyWriter()
        reader.feed_eof()
        try:
            ok = await srv._run_first_login_interview(
                "N0CALL",
                reader,
                writer,  # type: ignore[arg-type]
                node_family="",
                password_set=True,
            )
            assert ok is True
            assert b"Enable email MFA now?" not in bytes(writer.buffer)
            assert await store.get_user_pref("N0CALL", "mfa_email_otp") is None
        finally:
            await store.close()

    asyncio.run(run())


def test_telnet_password_prompt_stays_clean_for_raw_tcp_clients(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "login_echo_negotiation.db")
        cfg = AppConfig(
            node=NodeConfig(node_call="AI3I-15", require_password=True),
            telnet=TelnetConfig(host="127.0.0.1", port=0, idle_timeout_seconds=30),
            web=WebConfig(host="127.0.0.1", port=0),
            public_web=PublicWebConfig(),
            store=StoreConfig(sqlite_path=db),
        )
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        now = int(datetime.now(timezone.utc).timestamp())
        await store.set_user_pref("N0CALL", "password", "pw1", now)
        await store.record_login("N0CALL", now, "test-setup")
        try:
            await srv.start()
        except OSError:
            pytest.skip("socket bind unavailable in sandbox")
        try:
            sock = (srv._server.sockets or [None])[0]
            assert sock is not None
            host, port = sock.getsockname()[0], sock.getsockname()[1]

            r1, w1 = await asyncio.open_connection(host, port)
            await asyncio.wait_for(r1.readuntil(b"login: "), timeout=2.0)
            w1.write(b"N0CALL\r\n")
            await w1.drain()
            pw = await asyncio.wait_for(r1.readuntil(b"password: "), timeout=2.0)
            assert b"password:" in pw
            assert b"\xff\xfb\x01" in pw
            w1.write(b"pw1\r\n")
            await w1.drain()
            hello = await asyncio.wait_for(r1.read(4096), timeout=2.0)
            assert b"pw1" not in hello
            assert b"Welcome" in hello
            w1.close()
            await w1.wait_closed()
        finally:
            await srv.stop()
            await store.close()

    asyncio.run(run())


def test_telnet_password_prompt_negotiates_echo_for_telnet_clients(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "login_echo_telnet_client.db")
        cfg = AppConfig(
            node=NodeConfig(node_call="AI3I-15", require_password=True),
            telnet=TelnetConfig(host="127.0.0.1", port=0, idle_timeout_seconds=30),
            web=WebConfig(host="127.0.0.1", port=0),
            public_web=PublicWebConfig(),
            store=StoreConfig(sqlite_path=db),
        )
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        await store.set_user_pref("N0CALL", "password", "pw1", int(datetime.now(timezone.utc).timestamp()))
        try:
            await srv.start()
        except OSError:
            pytest.skip("socket bind unavailable in sandbox")
        try:
            sock = (srv._server.sockets or [None])[0]
            assert sock is not None
            host, port = sock.getsockname()[0], sock.getsockname()[1]

            r1, w1 = await asyncio.open_connection(host, port)
            await asyncio.wait_for(r1.readuntil(b"login: "), timeout=2.0)
            w1.write(b"\xff\xfd\x01")
            w1.write(b"N0CALL\r\n")
            await w1.drain()
            pw = await asyncio.wait_for(r1.readuntil(b"password: "), timeout=2.0)
            assert b"\xff\xfb\x01" in pw
            assert b"\xff\xfb\x03" in pw
            assert b"\xff\xfd\x03" in pw
            assert pw.endswith(b"password: ")
            w1.write(b"pw1\r\n")
            await w1.drain()
            hello = await asyncio.wait_for(r1.read(4096), timeout=2.0)
            assert b"\xff\xfc\x01" in hello
            assert b"\xff\xfc\x03" in hello
            assert b"\xff\xfe\x03" in hello
            assert b"Welcome" in hello
            w1.close()
            await w1.wait_closed()
        finally:
            await srv.stop()
            await store.close()

    asyncio.run(run())


def test_telnet_password_echo_negotiation_is_initiated_by_server(tmp_path) -> None:
    class _BufWriter:
        def __init__(self) -> None:
            self.buf = bytearray()

        def write(self, data: bytes) -> None:
            self.buf.extend(data)

        async def drain(self) -> None:
            return

    async def run() -> None:
        db = str(tmp_path / "login_echo_gate.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        reader = asyncio.StreamReader()
        writer = _BufWriter()
        try:
            # Initiate echo suppression even before the client advertises
            # telnet options; several telnet clients do not negotiate early.
            await srv._set_telnet_password_echo(reader, writer, suppress=True)  # type: ignore[arg-type]
            assert bytes(writer.buf) == b"\xff\xfb\x01\xff\xfb\x03\xff\xfd\x03"
            writer.buf.clear()

            # Client sends IAC DO ECHO — marks connection as telnet
            reader.feed_data(b"\xff\xfd\x01")
            assert await srv._read_telnet_byte(reader, 0) == b""

            # Repeated toggles still produce the explicit suppression/restore sequence.
            await srv._set_telnet_password_echo(reader, writer, suppress=True)  # type: ignore[arg-type]
            await srv._set_telnet_password_echo(reader, writer, suppress=False)  # type: ignore[arg-type]
            assert bytes(writer.buf) == (
                b"\xff\xfb\x01\xff\xfb\x03\xff\xfd\x03"
                b"\xff\xfc\x01\xff\xfc\x03\xff\xfe\x03"
            )
        finally:
            await store.close()

    asyncio.run(run())


def test_telnet_login_can_require_email_otp(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "telnet_mfa.db")
        cfg = AppConfig(
            node=NodeConfig(node_call="AI3I-16"),
            telnet=TelnetConfig(host="127.0.0.1", port=0, idle_timeout_seconds=30),
            web=WebConfig(host="127.0.0.1", port=0),
            public_web=PublicWebConfig(),
            store=StoreConfig(sqlite_path=db),
        )
        cfg.smtp.host = "smtp.example.test"
        cfg.smtp.from_addr = "cluster@example.test"
        cfg.mfa.enabled = True
        cfg.mfa.require_for_users = True
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        sent: list[tuple[str, str, str]] = []
        srv._mfa._sender = lambda rcpt, subject, body: sent.append((rcpt, subject, body))  # type: ignore[assignment]
        await store.upsert_user_registry("N0CALL", int(datetime.now(timezone.utc).timestamp()), privilege="user", email="n0call@example.test")
        now = int(datetime.now(timezone.utc).timestamp())
        await store.set_user_pref("N0CALL", "password", "pw1", now)
        await store.set_user_pref("N0CALL", "email_verified_epoch", str(now), now)
        try:
            await srv.start()
        except OSError:
            pytest.skip("socket bind unavailable in sandbox")
        try:
            sock = (srv._server.sockets or [None])[0]
            assert sock is not None
            host, port = sock.getsockname()[0], sock.getsockname()[1]

            r1, w1 = await asyncio.open_connection(host, port)
            await asyncio.wait_for(r1.readuntil(b"login: "), timeout=2.0)
            w1.write(b"N0CALL\r\n")
            await w1.drain()
            await asyncio.wait_for(r1.readuntil(b"password: "), timeout=2.0)
            w1.write(b"pw1\r\n")
            await w1.drain()
            otp_prompt = await asyncio.wait_for(r1.readuntil(b"otp: "), timeout=2.0)
            assert b"otp:" in otp_prompt
            challenge = next(iter(srv._mfa._challenges.values()))
            assert sent and sent[0][0] == "n0call@example.test"
            w1.write((challenge.code + "\r\n").encode("ascii"))
            await w1.drain()
            hello = await asyncio.wait_for(r1.read(4096), timeout=2.0)
            assert b"Welcome" in hello
            w1.close()
            await w1.wait_closed()
        finally:
            await srv.stop()
            await store.close()

    asyncio.run(run())


def test_telnet_login_email_mfa_uses_base_call_email_for_ssid(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "telnet_mfa_ssid_email.db")
        cfg = AppConfig(
            node=NodeConfig(node_call="AI3I-16"),
            telnet=TelnetConfig(host="127.0.0.1", port=0, idle_timeout_seconds=30),
            web=WebConfig(host="127.0.0.1", port=0),
            public_web=PublicWebConfig(),
            store=StoreConfig(sqlite_path=db),
        )
        cfg.smtp.host = "smtp.example.test"
        cfg.smtp.from_addr = "cluster@example.test"
        cfg.mfa.enabled = True
        cfg.mfa.require_for_users = False
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        sent: list[tuple[str, str, str]] = []
        srv._mfa._sender = lambda rcpt, subject, body: sent.append((rcpt, subject, body))  # type: ignore[assignment]
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry("N9JR", now, privilege="user", email="n9jr@example.test")
        await store.upsert_user_registry("N9JR-10", now, privilege="user", email="")
        await store.set_user_pref("N9JR-10", "password", "pw1", now)
        await store.set_user_pref("N9JR-10", "mfa_email_otp", "required", now)
        await store.set_user_pref("N9JR-10", "email_verified_epoch", str(now), now)
        try:
            await srv.start()
        except OSError:
            pytest.skip("socket bind unavailable in sandbox")
        try:
            sock = (srv._server.sockets or [None])[0]
            assert sock is not None
            host, port = sock.getsockname()[0], sock.getsockname()[1]

            r1, w1 = await asyncio.open_connection(host, port)
            await asyncio.wait_for(r1.readuntil(b"login: "), timeout=2.0)
            w1.write(b"N9JR-10\r\n")
            await w1.drain()
            await asyncio.wait_for(r1.readuntil(b"password: "), timeout=2.0)
            w1.write(b"pw1\r\n")
            await w1.drain()
            otp_prompt = await asyncio.wait_for(r1.readuntil(b"otp: "), timeout=2.0)
            assert b"otp:" in otp_prompt
            assert sent and sent[0][0] == "n9jr@example.test"
            challenge = next(iter(srv._mfa._challenges.values()))
            w1.write((challenge.code + "\r\n").encode("ascii"))
            await w1.drain()
            hello = await asyncio.wait_for(r1.read(4096), timeout=2.0)
            assert b"Welcome" in hello
            w1.close()
            await w1.wait_closed()
        finally:
            await srv.stop()
            await store.close()

    asyncio.run(run())


def test_telnet_login_can_use_totp_authenticator(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "telnet_totp.db")
        cfg = AppConfig(
            node=NodeConfig(node_call="AI3I-16"),
            telnet=TelnetConfig(host="127.0.0.1", port=0, idle_timeout_seconds=30),
            web=WebConfig(host="127.0.0.1", port=0),
            public_web=PublicWebConfig(),
            store=StoreConfig(sqlite_path=db),
        )
        cfg.mfa.enabled = True
        cfg.mfa.require_for_users = True
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry("N0CALL", now, privilege="user", email="")
        await store.set_user_pref("N0CALL", "password", "pw1", now)
        await store.set_user_pref("N0CALL", "mfa_totp_secret", "JBSWY3DPEHPK3PXP", now)
        await store.set_user_pref("N0CALL", "mfa_email_otp", "required", now)
        try:
            await srv.start()
        except OSError:
            pytest.skip("socket bind unavailable in sandbox")
        try:
            sock = (srv._server.sockets or [None])[0]
            assert sock is not None
            host, port = sock.getsockname()[0], sock.getsockname()[1]

            r1, w1 = await asyncio.open_connection(host, port)
            await asyncio.wait_for(r1.readuntil(b"login: "), timeout=2.0)
            w1.write(b"N0CALL\r\n")
            await w1.drain()
            await asyncio.wait_for(r1.readuntil(b"password: "), timeout=2.0)
            w1.write(b"pw1\r\n")
            await w1.drain()
            prompt = await asyncio.wait_for(r1.readuntil(b"authenticator code: "), timeout=2.0)
            assert b"authenticator code:" in prompt
            w1.write((totp_code("JBSWY3DPEHPK3PXP") + "\r\n").encode("ascii"))
            await w1.drain()
            hello = await asyncio.wait_for(r1.read(4096), timeout=2.0)
            assert b"Welcome" in hello
            w1.close()
            await w1.wait_closed()
        finally:
            await srv.stop()
            await store.close()

    asyncio.run(run())


def test_telnet_login_honors_per_user_mfa_override(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "telnet_mfa_override.db")
        cfg = AppConfig(
            node=NodeConfig(node_call="AI3I-16"),
            telnet=TelnetConfig(host="127.0.0.1", port=0, idle_timeout_seconds=30),
            web=WebConfig(host="127.0.0.1", port=0),
            public_web=PublicWebConfig(),
            store=StoreConfig(sqlite_path=db),
        )
        cfg.smtp.host = "smtp.example.test"
        cfg.smtp.from_addr = "cluster@example.test"
        cfg.mfa.enabled = True
        cfg.mfa.require_for_users = False
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        sent: list[tuple[str, str, str]] = []
        srv._mfa._sender = lambda rcpt, subject, body: sent.append((rcpt, subject, body))  # type: ignore[assignment]
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry("N0CALL", now, privilege="user", email="n0call@example.test")
        await store.set_user_pref("N0CALL", "password", "pw1", now)
        await store.set_user_pref("N0CALL", "mfa_email_otp", "required", now)
        await store.set_user_pref("N0CALL", "email_verified_epoch", str(now), now)
        try:
            await srv.start()
        except OSError:
            pytest.skip("socket bind unavailable in sandbox")
        try:
            sock = (srv._server.sockets or [None])[0]
            assert sock is not None
            host, port = sock.getsockname()[0], sock.getsockname()[1]

            r1, w1 = await asyncio.open_connection(host, port)
            await asyncio.wait_for(r1.readuntil(b"login: "), timeout=2.0)
            w1.write(b"N0CALL\r\n")
            await w1.drain()
            await asyncio.wait_for(r1.readuntil(b"password: "), timeout=2.0)
            w1.write(b"pw1\r\n")
            await w1.drain()
            otp_prompt = await asyncio.wait_for(r1.readuntil(b"otp: "), timeout=2.0)
            assert b"otp:" in otp_prompt
            challenge = next(iter(srv._mfa._challenges.values()))
            w1.write((challenge.code + "\r\n").encode("ascii"))
            await w1.drain()
            hello = await asyncio.wait_for(r1.read(4096), timeout=2.0)
            assert b"Welcome" in hello
            w1.close()
            await w1.wait_closed()

            await store.set_user_pref("N0CALL", "mfa_email_otp", "off", now)
            r2, w2 = await asyncio.open_connection(host, port)
            await asyncio.wait_for(r2.readuntil(b"login: "), timeout=2.0)
            w2.write(b"N0CALL\r\n")
            await w2.drain()
            await asyncio.wait_for(r2.readuntil(b"password: "), timeout=2.0)
            w2.write(b"pw1\r\n")
            await w2.drain()
            hello2 = await asyncio.wait_for(r2.read(4096), timeout=2.0)
            assert b"Welcome" in hello2
            w2.close()
            await w2.wait_closed()
        finally:
            await srv.stop()
            await store.close()

    asyncio.run(run())


def test_telnet_login_requires_email_verification_for_unverified_user(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "telnet_email_verification.db")
        cfg = AppConfig(
            node=NodeConfig(node_call="AI3I-16", require_password=False, initial_grace_logins=5, verified_email_required_for_telnet=True),
            telnet=TelnetConfig(host="127.0.0.1", port=0, idle_timeout_seconds=30),
            web=WebConfig(host="127.0.0.1", port=0),
            public_web=PublicWebConfig(),
            store=StoreConfig(sqlite_path=db),
        )
        cfg.smtp.host = "smtp.example.test"
        cfg.smtp.from_addr = "cluster@example.test"
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        sent: list[tuple[str, str, str]] = []
        srv._mfa._sender = lambda rcpt, subject, body: sent.append((rcpt, subject, body))  # type: ignore[assignment]
        now = int(datetime.now(timezone.utc).timestamp())
        await store.upsert_user_registry("N0CALL", now, privilege="user", email="n0call@example.test")
        await store.record_login("N0CALL", now, "test-setup")
        try:
            await srv.start()
        except OSError:
            pytest.skip("socket bind unavailable in sandbox")
        try:
            sock = (srv._server.sockets or [None])[0]
            assert sock is not None
            host, port = sock.getsockname()[0], sock.getsockname()[1]

            r1, w1 = await asyncio.open_connection(host, port)
            await asyncio.wait_for(r1.readuntil(b"login: "), timeout=2.0)
            w1.write(b"N0CALL\r\n")
            await w1.drain()
            verify_prompt = await asyncio.wait_for(r1.readuntil(b"verification code: "), timeout=2.0)
            assert b"A verification code has been sent" in verify_prompt
            challenge = next(iter(srv._mfa._challenges.values()))
            assert sent and sent[0][0] == "n0call@example.test"
            w1.write((challenge.code + "\r\n").encode("ascii"))
            await w1.drain()
            hello = await asyncio.wait_for(r1.read(4096), timeout=2.0)
            assert b"Email address verified for N0CALL." in hello
            assert b"Welcome" in hello
            assert await store.get_user_pref("N0CALL", "email_verified_epoch") is not None
            w1.close()
            await w1.wait_closed()
        finally:
            await srv.stop()
            await store.close()

    asyncio.run(run())


def test_telnet_server_supports_multiple_listener_ports(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "multi_listener.db")
        cfg = AppConfig(
            node=NodeConfig(),
            telnet=TelnetConfig(host="127.0.0.1", port=0, ports=(0, 0), idle_timeout_seconds=30),
            web=WebConfig(host="127.0.0.1", port=0),
            public_web=PublicWebConfig(),
            store=StoreConfig(sqlite_path=db),
        )
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        try:
            await srv.start()
        except OSError:
            pytest.skip("socket bind unavailable in sandbox")
        try:
            ports = sorted(
                int(sock.getsockname()[1])
                for listener in srv._servers
                for sock in (listener.sockets or [])
            )
            assert len(ports) == 2
            assert ports[0] != ports[1]
        finally:
            await srv.stop()
            await store.close()

    asyncio.run(run())


def test_sysop_spotlimit_enforces_telnet_post_throttle(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "spotlimit_telnet.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="AI3I", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        srv._sessions[2] = Session(call="K1ABC", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.upsert_user_registry("AI3I", now, privilege="sysop")
            await store.upsert_user_registry("K1ABC", now, privilege="user")

            _, out = await srv._execute_command("AI3I", "sysop/spotlimit default 2 300")
            assert "max=2" in out

            _, out = await srv._execute_command("AI3I", "sysop/spotlimit K1ABC")
            assert "Max Per Window: 2" in out
            assert "Window Seconds: 300" in out

            _, out = await srv._execute_command("K1ABC", "dx 14074.0 N0TST one")
            assert "Spot posted" in out
            _, out = await srv._execute_command("K1ABC", "dx 14075.0 N0TSU two")
            assert "Spot posted" in out
            _, out = await srv._execute_command("K1ABC", "dx 14076.0 N0TSV three")
            assert "rate limited" in out
            assert await store.count_spots() == 2
        finally:
            await store.close()

    asyncio.run(run())


def test_wpxloc_fixture_lookup_supports_exact_call(tmp_path) -> None:
    path = _write_wpxloc(tmp_path)
    load_wpxloc(path)
    row = wpx_lookup("RG65SM")
    assert row is not None
    assert row.name == "European Russia"
    assert row.cq_zone == 29
    assert row.itu_zone == 16


def test_show_heading_uses_wpxloc_when_cty_is_unavailable(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "show_heading_wpx.db")
        cfg = _mk_config(db)
        cfg.public_web.wpxloc_raw_path = _write_wpxloc(tmp_path)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        now = int(datetime.now(timezone.utc).timestamp())
        try:
            await store.upsert_user_registry("N0CALL", now, qra="FN42LI")
            _, out = await srv._execute_command("N0CALL", "show/heading RG65SM")
            assert "Heading to European Russia (RG65SM):" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_sun_reports_sunrise_and_sunset(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "show_sun.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.upsert_user_registry("N0CALL", now, qra="FN31PR")
            _, out = await srv._execute_command("N0CALL", "show/sun")
            assert "Sunrise:" in out
            assert "Sunset:" in out
            assert "Next Event:" in out
            assert ".00h" not in out
            assert re.search(r"Next Event: sunrise in \d+h( \d{2}m)?|Next Event: sunset in \d+h( \d{2}m)?|Next Event: (sunrise|sunset) in \d+m", out)
        finally:
            await store.close()

    asyncio.run(run())


def test_dx_line_suffix_includes_us_state_when_enabled(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "usstate_suffix.db")
        cfg = _mk_config(db)
        cfg.public_web.cty_dat_path = _write_cty(tmp_path)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.set_user_pref("N0CALL", "usstate", "WI", now)
            await store.set_usdb_entry("K1ABC", "state", "MA", now)
            suffix = await srv._dx_line_suffix_for_call("N0CALL", "K1ABC")
            assert suffix.strip() == "MA"
        finally:
            await store.close()

    asyncio.run(run())


def test_live_wwv_bulletin_uses_aligned_table_format(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "live_wwv_table.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        writer = _DummyWriter()
        srv._sessions[1] = Session(call="N0CALL", writer=writer, connected_at=datetime.now(timezone.utc))
        try:
            delivered = await srv.publish_bulletin("wwv", "WWV", "LOCAL", "SFI=121 A=5 K=3 Moderate w/G2 -> Minor w/G1")
            assert delivered == 1
            text = bytes(writer.buffer).decode("utf-8", errors="replace")
            assert "Date        Hour   SFI   A   K Forecast" in text
            assert "Moderate w/G2 -> Minor w/G1" in text
            assert "<WWV>" in text
        finally:
            await store.close()

    asyncio.run(run())


def test_show_sun_target_uses_geo_lookup_instead_of_node_fallback(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "show_sun_target.db")
        cty_path = _write_cty(tmp_path)
        cfg = AppConfig(
            node=NodeConfig(node_call="AI3I-16", node_locator="FN20"),
            telnet=TelnetConfig(),
            web=WebConfig(),
            public_web=PublicWebConfig(cty_dat_path=cty_path),
            store=StoreConfig(sqlite_path=db),
        )
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        try:
            _, out = await srv._execute_command("N0CALL", "show/sun K")
            assert "Reference: United States" in out
            assert "node grid square FN20" not in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_muf_path_report_uses_west_longitudes_and_varies_by_hour(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "muf_path_variation.db")
        cty_path = _write_cty(tmp_path)
        wpx_path = _write_wpxloc(tmp_path)
        cfg = AppConfig(
            node=NodeConfig(node_call="AI3I-16", node_locator="EN63AA"),
            telnet=TelnetConfig(),
            web=WebConfig(),
            public_web=PublicWebConfig(cty_dat_path=cty_path, wpxloc_raw_path=wpx_path),
            store=StoreConfig(sqlite_path=db),
        )
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        try:
            now = int(datetime(2026, 4, 10, 18, 0, tzinfo=timezone.utc).timestamp())
            await store.add_bulletin("wwv", "WWV", "LOCAL", now, "SFI=98 A=6 K=2 Quiet")
            await store.add_bulletin("wwv", "WWV", "LOCAL", now - 3 * 3600, "SFI=98 A=6 K=2 Quiet")
            await store.add_bulletin("wwv", "WWV", "LOCAL", now - 6 * 3600, "SFI=98 A=6 K=2 Quiet")
            _, out = await srv._execute_command("N0CALL", "show/muf K 3")
            assert "87 57 W" in out
            rows = [
                line for line in out.splitlines()
                if len(line.split()) >= 4
                and line.split()[0].isdigit()
                and line.split()[1].isdigit()
                and "." in line.split()[2]
            ]
            assert len(rows) >= 3
            muf_values = {line.split()[2] for line in rows[:3]}
            assert len(muf_values) > 1
        finally:
            await store.close()

    asyncio.run(run())
