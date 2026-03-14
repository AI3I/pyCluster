from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path
import pytest

from pycluster.config import AppConfig, NodeConfig, PublicWebConfig, StoreConfig, TelnetConfig, WebConfig
from pycluster.ctydat import load_cty, lookup
from pycluster.models import Spot
from pycluster.telnet_server import Session, TelnetClusterServer
from pycluster.store import SpotStore


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
        "Japan: 25: 45: AS: 35.0: 139.0: -9.0: JA:\n"
        " JA, 7K;\n",
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
            assert "pyCluster version 1.0.0" in out
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


def test_prompt_uses_configured_node_call_only(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "prompt.db")
        cfg = _mk_config(db)
        cfg.node.node_call = "AI3I-15"
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        try:
            assert await srv._prompt("N0CALL") == "AI3I-15> "
            await store.set_user_pref(cfg.node.node_call, "node_call", "AI3I-7", int(datetime.now(timezone.utc).timestamp()))
            assert await srv._prompt("N0CALL") == "AI3I-7> "
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
            assert await srv._prompt("AI3I") == "AI3I-16# "
            assert await srv._prompt("N0CALL") == "AI3I-16> "
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
            assert "echo=off" in out
            assert sess.echo is False

            _, out = await srv._execute_command("N0CALL", "set/language de")
            assert "Language set to de" in out
            assert sess.language == "de"

            _, out = await srv._execute_command("N0CALL", "set/here")
            assert "here=on" in out
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
            _, out = await srv._execute_command("N0CALL", "set/maxconnect 2")
            assert "Maximum connections for N0CALL set to 2." in out
            assert await store.get_user_pref("N0CALL", "maxconnect") == "2"

            _, out = await srv._execute_command("N0CALL", "show/users")
            assert "maxc=2" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_command_case_insensitive_and_abbrev_dispatch(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "abbrev.db")
        cfg = _mk_config(db)
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
            assert "talk=on" in out
            _, out = await srv._execute_command("N0CALL", "uNsE/tA")
            assert "talk=off" in out

            _, out = await srv._execute_command("N0CALL", "Sh/Dx")
            assert "No spots available" in out

            _, out = await srv._execute_command("N0CALL", "gE/KeP")
            assert "get/keps: Ok" in out

            _, out = await srv._execute_command("N0CALL", "Sh/PrOtO-AcKs")
            assert "No proto acks" in out
            _, out = await srv._execute_command("N0CALL", "SH/PRACK")
            assert "No proto acks" in out

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
            assert "No proto acks" in out

            now = int(datetime.now(timezone.utc).timestamp())
            await store.set_user_pref("N0CALL", "privilege", "sysop", now)
            _, out = await srv._execute_command("N0CALL", f"{cat['set/protoack']} a")
            assert "Protocol alerts acknowledged for" in out
            _, out = await srv._execute_command("N0CALL", f"{cat['unset/protoack']} *")
            assert "Cleared protocol alert acknowledgements" in out
        finally:
            await store.close()

    asyncio.run(run())


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
            assert "health=acked" in out
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
            assert "dbshow: engine=sqlite" in out

            # Ambiguous top-level prefix should not resolve.
            _, out = await srv._execute_command("N0CALL", "di")
            assert out == "?\r\n"

            # Separator-insensitive aliases should resolve.
            _, out = await srv._execute_command("N0CALL", "sendconfig")
            assert "node_call=" in out
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
            assert "=>" in out
            row = next((ln for ln in out.splitlines() if "dbshow" in ln and "=>" in ln), "")
            assert row
            rhs = row.split("=>", 1)[1].strip()
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
            assert "rx=" in out and "tx=" in out
            assert "profile=arcluster" in out
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
            assert "proto=pc24=OH8X:1|pc50=W3LPL:63|pc51=AI3I-15>WB3FFV-2:1" in out

            _, out = await srv._execute_command("N0CALL", "show/route")
            assert "last=PC51" in out
            assert "proto=pc24=OH8X:1|pc50=W3LPL:63|pc51=AI3I-15>WB3FFV-2:1" in out
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
            },
            {
                "peer": "PYC-2",
                "profile": "pycluster",
                "connected": False,
                "desired": True,
                "last_connect_epoch": 0,
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

            _, out = await srv._execute_command("N0CALL", "show/node")
            assert "Topology" in out
            assert cfg.node.node_call in out
            assert "AI3I-15 [up spider]" in out
            assert "PYC-2 [down pycluster]" in out
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
            assert "peer1" in out and "health=ok" in out
            assert "PC50  Call: W3LPL  Nodes: 63" in out
            assert "peer2" in out and "health=degraded" in out

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
            assert "peer2" in out and "health=stale" in out
            assert "age_min=" in out

            _, out = await srv._execute_command("N0CALL", "show/proto --stale-mins x")
            assert "Usage: show/proto [peer] [--stale-mins <minutes>]" in out

            _, out = await srv._execute_command("N0CALL", "stat/proto")
            assert "Protocol summary: peers=3 known=3 ok=2 degraded=0 flapping=0 stale=1 unknown=0" in out
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
            assert "health=flapping" in out
            assert "changes=9 flap=5" in out

            _, out = await srv._execute_command("N0CALL", "stat/proto")
            assert "Protocol summary: peers=1 known=1 ok=0 degraded=0 flapping=1 stale=0 unknown=0" in out
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
            assert "history:" in out
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
            assert "Protocol history summary: peers=2 with_history=1 events=2 last_epoch=" in out
            _, out = await srv._execute_command("N0CALL", "stat/protohistory peer1")
            assert "Protocol history summary: peers=1 with_history=1 events=2 last_epoch=" in out
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
            assert "Protocol event summary: events=3 keys=2 top=pc24.flag:2,pc51.value:1" in out
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
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer3.pc24.call", "K3ABC", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer3.pc24.flag", "1", now)
        await store.set_user_pref(cfg.node.node_call, "proto.peer.peer3.last_epoch", str(old), now)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc), link_stats_fn=_stats)
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            _, out = await srv._execute_command("N0CALL", "show/protoalerts")
            assert "peer1" not in out
            assert "peer2" in out and "health=flapping" in out
            assert "peer3" in out and "health=stale" in out

            _, out = await srv._execute_command("N0CALL", "show/protoalerts peer2")
            assert "peer2" in out and "peer1" not in out

            _, out = await srv._execute_command("N0CALL", "stat/protoalerts")
            assert "Protocol alert summary: total=2 degraded=0 flapping=1 stale=1 acked=0" in out
            _, out = await srv._execute_command("N0CALL", "stat/protoalerts peer2")
            assert "Protocol alert summary: total=1 degraded=0 flapping=1 stale=0 acked=0" in out

            _, out = await srv._execute_command("N0CALL", "set/protoack peer1")
            assert "permission denied" in out
            await store.set_user_pref("N0CALL", "privilege", "sysop", now)
            _, out = await srv._execute_command("N0CALL", "set/prack peer1")
            assert "Protocol alerts acknowledged for peer1." in out

            _, out = await srv._execute_command("N0CALL", "show/protoalerts")
            assert "peer1" not in out
            _, out = await srv._execute_command("N0CALL", "show/protoalerts a")
            assert "peer1" in out and "health=acked" in out
            _, out = await srv._execute_command("N0CALL", "show/protoack")
            assert "peer1" in out and "suppressed=1" in out
            _, out = await srv._execute_command("N0CALL", "show/protoacks peer2")
            assert "No proto acks for filter 'peer2'" in out

            _, out = await srv._execute_command("N0CALL", "stat/protoalerts")
            assert "acked=1" in out
            _, out = await srv._execute_command("N0CALL", "stat/protoack")
            assert "Protocol ack summary: total=1 suppressed=1 expired=0" in out
            _, out = await srv._execute_command("N0CALL", "stat/protoalerts peer1")
            assert "Protocol alert summary: total=1 degraded=0 flapping=0 stale=0 acked=1" in out
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
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc), link_stats_fn=_stats)
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            _, out = await srv._execute_command("N0CALL", "set/protothreshold flap_score 10")
            assert "permission denied" in out
            await store.set_user_pref("N0CALL", "privilege", "sysop", now)

            _, out = await srv._execute_command("N0CALL", "set/protothreshold flap_score 10")
            assert "Protocol threshold flap score set to 10." in out

            _, out = await srv._execute_command("N0CALL", "show/protoconfig")
            assert "flap_score=10 (node)" in out
            assert "stale_mins=30 (default)" in out

            _, out = await srv._execute_command("N0CALL", "show/proto")
            assert "health=ok" in out

            _, out = await srv._execute_command("N0CALL", "unset/protothreshold flap_score")
            assert "Protocol threshold flap score restored to default." in out
            _, out = await srv._execute_command("N0CALL", "show/proto")
            assert "health=flapping" in out
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
            assert "flap_score=10 (node)" in out
            assert "stale_mins=45 (node)" in out
            assert "flap_window_secs=600 (node)" in out

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
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc), link_stats_fn=_stats)
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            _, out = await srv._execute_command("N0CALL", "show/proto")
            assert "health=flapping" in out

            _, out = await srv._execute_command(
                "N0CALL", f"set/var {cfg.node.node_call} proto.threshold.flap_score 10"
            )
            assert f"Variable proto.threshold.flap_score updated for {cfg.node.node_call}." in out

            _, out = await srv._execute_command("N0CALL", "show/proto")
            assert "health=ok" in out
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
            assert "Echo=on" in out
            assert "accept/spots 1 on 40m" in out
            assert "reject/spots 2 by K1" in out

            _, out = await srv._execute_command("N0CALL", "clear/spots 1")
            assert "clear/spots" in out
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
            await srv._execute_command("N0CALL", "accept/wx 3 by N0")
            await srv._execute_command("N0CALL", "reject/wx 1 by W1")

            _, out = await srv._execute_command("N0CALL", "show/filter")
            i_rej = out.find("reject/spots 1 by N9")
            i_acc = out.find("accept/spots 5 on 20m")
            assert i_rej >= 0 and i_acc >= 0 and i_rej < i_acc
            assert "show/filter test spots" in out
            assert "show/filter test <announce|wcy|wwv|wx>" in out
            assert "add --verbose after family" in out

            _, out = await srv._execute_command("N0CALL", "show/filter test spots 14074 W1AW N9XYZ FT8")
            assert "Decision: deny" in out
            _, out = await srv._execute_command("N0CALL", "show/filter test route --verbose east-hub")
            assert "Decision: allow" in out
            assert "Winning Rule: matched=accept slot=2 expr=peer east*" in out
            _, out = await srv._execute_command("N0CALL", "show/filter test route --verbose west-hub")
            assert "Decision: deny" in out
            assert "Winning Rule: matched=reject slot=1 expr=peer west*" in out

            _, out = await srv._execute_command("N0CALL", "show/filter test wx --verbose N0ABC local weather")
            assert "Decision: allow" in out
            assert "Winning Rule: matched=accept slot=3 expr=by N0" in out
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


def test_show_dx_applies_spot_filters(tmp_path) -> None:
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
            assert "K3LR" not in out
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
            assert "-Mar-" in spider
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


def test_wcy_filters_apply_to_live_and_show(tmp_path) -> None:
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
            await srv._execute_command("N0CALL", "accept/wcy 1 by K1")

            await srv.publish_bulletin("wcy", "K1ABC", "LOCAL", "A=5 K=2")
            assert b"WCY K1ABC: A=5 K=2" in bytes(w_n0.buffer)

            before = len(w_n0.buffer)
            await srv.publish_bulletin("wcy", "W1AW", "LOCAL", "A=9 K=4")
            assert len(w_n0.buffer) == before

            now = int(datetime.now(timezone.utc).timestamp())
            await store.add_bulletin("wcy", "K1ABC", "LOCAL", now, "A=4 K=1")
            await store.add_bulletin("wcy", "W1AW", "LOCAL", now, "A=7 K=3")
            _, out = await srv._execute_command("N0CALL", "show/wcy")
            assert "K1ABC" in out
            assert "W1AW" not in out
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
            _, out = await srv._execute_command("N0CALL", "msg K1ABC hello there")
            assert "Message #" in out and "delivered to 1 session(s)." in out
            assert b"MSG#" in bytes(w2.buffer)
            assert b"N0CALL: hello there" in bytes(w2.buffer)

            _, out = await srv._execute_command("N0CALL", "talk K1ABC hi")
            assert "Talk delivered to 1 session(s)." in out
            assert b"TALK N0CALL: hi" in bytes(w2.buffer)

            _, out = await srv._execute_command("N0CALL", "announce full test notice")
            assert "Announcement accepted (full):" in out

            _, out = await srv._execute_command("K1ABC", "show/msgstatus")
            assert "Messages for K1ABC:" in out
            _, out = await srv._execute_command("K1ABC", "show/messages")
            assert "UNREAD" in out and "hello there" in out
            _, out = await srv._execute_command("K1ABC", "mail")
            assert "hello there" in out

            _, out = await srv._execute_command("K1ABC", "read")
            assert "N0CALL" in out
            msg_id = int(out.splitlines()[0].split()[0])

            _, out = await srv._execute_command("K1ABC", f"read {msg_id}")
            assert f"Message #{msg_id}" in out

            _, out = await srv._execute_command("K1ABC", f"reply {msg_id} roger")
            assert "Reply #" in out
            assert "delivered to " in out

            _, out = await srv._execute_command("N0CALL", "read")
            assert "K1ABC" in out

            _, out = await srv._execute_command("N0CALL", "show/log 10")
            assert "announce" in out.lower()
            assert "msg" in out.lower()
            assert "talk" in out.lower()
        finally:
            await store.close()

    asyncio.run(run())


def test_show_prefix_qrz_bands(tmp_path) -> None:
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

            _, out = await srv._execute_command("N0CALL", "show/qrz K3AJ")
            assert "was last spotted on" in out
            assert "Frequency: 7109.9 kHz" in out

            _, out = await srv._execute_command("N0CALL", "show/bands")
            assert "40m" in out
            assert "hf" in out
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
            assert "node_call=" in out and "telnet=" in out and "web=" in out

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
            assert "continent=NA" in out
            assert "cq=5" in out
            assert "itu=8" in out
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
            assert "dxcq=on" in out
            _, out = await srv._execute_command("N0CALL", "set/dxitu")
            assert "dxitu=on" in out

            _, out = await srv._execute_command("N0CALL", "show/dx 1")
            assert "CQ5 ITU8" in out

            await srv.publish_spot(Spot(14074.0, "K1ABC", now, "FT8", "JA1AAA", "AI3I-16", ""))
            live = bytes(writer.buffer).decode("utf-8", "replace")
            assert "CQ5 ITU8" in live
        finally:
            await store.close()

    asyncio.run(run())


def test_repo_cty_fixture_includes_recent_tx5_tx7_entities() -> None:
    cty_path = Path(__file__).resolve().parents[1] / "fixtures" / "live" / "dxspider" / "cty.dat"
    load_cty(str(cty_path))
    assert lookup("TX5EU").name == "Austral Islands"
    assert lookup("TX5N").name == "Austral Islands"
    assert lookup("TX5S").name == "Clipperton Island"
    assert lookup("TX7N").name == "Marquesas Islands"


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
            normal_spot = Spot(14074.0, "K1XYZ", now, "FT8", "W1AW", "AI3I-16", "")

            await store.add_spot(rbn_spot)
            await store.add_spot(normal_spot)
            _, out = await srv._execute_command("N0CALL", "show/dx 10")
            assert "K1ABC" in out
            assert "K1XYZ" in out

            _, out = await srv._execute_command("N0CALL", "unset/rbn")
            assert "rbn=off" in out
            _, out = await srv._execute_command("N0CALL", "show/dx 10")
            assert "K1ABC" not in out
            assert "K1XYZ" in out

            _, out = await srv._execute_command("N0CALL", "set/rbn")
            assert "rbn=on" in out
            _, out = await srv._execute_command("N0CALL", "reject/rbn 2")
            assert "reject/rbn" in out

            before = len(writer.buffer)
            await srv.publish_spot(rbn_spot)
            await srv.publish_spot(normal_spot)
            live = bytes(writer.buffer[before:]).decode("utf-8", "replace")
            assert "K1ABC" not in live
            assert "K1XYZ" in live

            _, out = await srv._execute_command("N0CALL", "clear/rbn")
            assert "clear/rbn" in out
            before = len(writer.buffer)
            await srv.publish_spot(rbn_spot)
            live = bytes(writer.buffer[before:]).decode("utf-8", "replace")
            assert "K1ABC" in live
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
            assert "QRA for N0CALL: (none)" in out

            _, out = await srv._execute_command("N0CALL", "set/qra FN42")
            assert "Qra set to FN42 for N0CALL." in out
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
            _, out = await srv._execute_command("N0CALL", "commands startup")
            assert "set/startup" in out
            _, out = await srv._execute_command("N0CALL", "show/commands sendconf")
            assert "send_config" not in out
            now = int(datetime.now(timezone.utc).timestamp())
            await store.upsert_user_registry("N0CALL", now, privilege="sysop")
            _, out = await srv._execute_command("N0CALL", "show/commands sendconf")
            assert "send_config" in out
            _, out = await srv._execute_command("N0CALL", "show/capabilities")
            assert "capabilities: commands=" in out
            assert "show=" in out and "set=" in out and "stat=" in out

            _, out = await srv._execute_command("N0CALL", "show/sun")
            assert "set forward/latlong first" in out
            _, out = await srv._execute_command("N0CALL", "forward/latlong 42 -71")
            assert "Forward latitude/longitude set to" in out
            _, out = await srv._execute_command("N0CALL", "show/sun")
            assert "Solar Hour:" in out and "Phase:" in out
            _, out = await srv._execute_command("N0CALL", "show/grayline")
            assert "Grayline status:" in out and ("sunrise in" in out or "sunset in" in out)
            _, out = await srv._execute_command("N0CALL", "show/moon")
            assert "Age:" in out and "Illumination:" in out

            _, out = await srv._execute_command("N0CALL", "show/muf")
            assert "no recent WWV SFI data" in out
            await srv._execute_command("N0CALL", "wwv SFI=150 A=6 K=2")
            _, out = await srv._execute_command("N0CALL", "show/muf")
            assert "SFI: 150" in out and "Estimated MUF3000:" in out
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
            assert "Dup spots set to on for N0CALL." in out
            assert await store.spot_dupe_enabled() is True
            _, out = await srv._execute_command("N0CALL", "set/dupann")
            assert "Dup ann set to on for N0CALL." in out

            _, out = await srv._execute_command("N0CALL", "show/dupspots")
            assert "dup_spots=on" in out
            _, out = await srv._execute_command("N0CALL", "show/dupann")
            assert "dup_ann=on" in out

            _, out = await srv._execute_command("N0CALL", "clear/dupefile")
            assert "Duplicate spot tracking reset" in out
            assert await store.spot_dupe_enabled() is False
            _, out = await srv._execute_command("N0CALL", "show/dupspots")
            assert "dup_spots=off" in out
            _, out = await srv._execute_command("N0CALL", "show/dupann")
            assert "dup_ann=off" in out
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
            assert "debug=on" in out
            _, out = await srv._execute_command("N0CALL", "set/debug 0")
            assert "debug=off" in out

            _, out = await srv._execute_command("N0CALL", "set/pinginterval nope")
            assert "Usage: set/pinginterval <integer>" in out
            _, out = await srv._execute_command("N0CALL", "set/pinginterval 2")
            assert "pinginterval=5" in out
            _, out = await srv._execute_command("N0CALL", "set/obscount 50000")
            assert "obscount=9999" in out
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
            }
        }

    async def run() -> None:
        db = str(tmp_path / "conn.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(
            cfg,
            store,
            datetime.now(timezone.utc),
            link_stats_fn=_stats,
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
            assert "policy_drop=" in out
            assert "profile=spider" in out

            _, out = await srv._execute_command("N0CALL", "show/route")
            assert "rx=" in out and "tx=" in out and "last=PC92" in out
            assert "reasons=relay_peer_chat_disabled:2,route_filter:1" in out

            _, out = await srv._execute_command("N0CALL", "show/policydrop")
            assert "Policy drop reasons:" in out
            assert "peer1: total=1" in out
            assert "relay_peer_chat_disabled=2" in out
            assert "route_filter=1" in out

            _, out = await srv._execute_command("N0CALL", "show/policydrop peer1")
            assert "peer1: total=1" in out
            _, out = await srv._execute_command("N0CALL", "show/policydrop missing")
            assert "No policy drop data for peer filter 'missing'" in out

            _, out = await srv._execute_command("N0CALL", "show/hops")
            assert "hop_metric=" in out and "policy_drop=" in out

            _, out = await srv._execute_command("N0CALL", "stat/route")
            assert "stat/route: 1" in out

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
            assert "peer1: total=3" in out
            assert "peer2: total=1" in out

            _, out = await srv._execute_command("N0CALL", "show/policydrop --reset peer1")
            assert "requires sysop" in out

            sess.vars["privilege"] = "sysop"
            _, out = await srv._execute_command("N0CALL", "show/policydrop --reset")
            assert "requires <peer> or all|a|*" in out
            _, out = await srv._execute_command("N0CALL", "show/policydrop --reset peer1")
            assert "policydrop reset peers=1 filter=peer1" in out

            _, out = await srv._execute_command("N0CALL", "show/policydrop --reset a")
            assert "policydrop reset peers=1" in out

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
            _, out = await srv._execute_command("N0CALL", "ping K1ABC")
            assert "PONG K1ABC" in out

            _, out = await srv._execute_command("N0CALL", "who")
            assert "N0CALL" in out and "K1ABC" in out

            _, out = await srv._execute_command("N0CALL", "status")
            assert "local /" in out and "Uptime" in out
            _, out = await srv._execute_command("N0CALL", "uptime")
            assert "uptime=" in out and "started=" in out
            _, out = await srv._execute_command("N0CALL", "show/uptime")
            assert "uptime=" in out and "now=" in out

            _, out = await srv._execute_command("N0CALL", "chat test room")
            assert "chat delivered=1" in out
            assert b"CHAT N0CALL: test room" in bytes(w2.buffer)

            _, out = await srv._execute_command("N0CALL", "wcy K=3 A=8")
            assert "accepted (local-safe)" in out
            _, out = await srv._execute_command("N0CALL", "wwv SFI=150")
            assert "accepted (local-safe)" in out
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
            assert "Users=" in out and "Spots=" in out
            assert "Messages=" in out and "Peers=" in out and "PolicyDrop=" in out
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
            assert "accepted (local-safe)" in out
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


def test_set_unset_and_extended_group_families(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "ext.db")
        cfg = _mk_config(db)
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
            assert "talk=on" in out
            _, out = await srv._execute_command("N0CALL", "show/talk")
            assert "talk=on" in out
            _, out = await srv._execute_command("N0CALL", "unset/talk")
            assert "talk=off" in out

            _, out = await srv._execute_command("N0CALL", "set/qra FN42")
            assert "qra=FN42" in out
            _, out = await srv._execute_command("N0CALL", "show/station")
            assert "qra=FN42" in out

            _, out = await srv._execute_command("N0CALL", "create/user W1AW")
            assert "User record created for W1AW." in out
            _, out = await srv._execute_command("N0CALL", "delete/user W1AW")
            assert "User W1AW removed." in out
            _, out = await srv._execute_command("N0CALL", "forward/latlong 42 -71")
            assert "Forward latitude/longitude set to" in out
            _, out = await srv._execute_command("N0CALL", "get/keps")
            assert "Keplerian elements request accepted." in out
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


def test_top_level_compat_batch_commands(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "topcompat.db")
        cfg = _mk_config(db)
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
            assert "AGW restart requested at epoch" in out
            assert "(count 1)." in out

            _, out = await srv._execute_command("N0CALL", "dbcreate")
            assert "Database structures verified:" in out
            _, out = await srv._execute_command("N0CALL", "dbupdate")
            assert "Database refresh complete:" in out

            dump = str(tmp_path / "dump.sql")
            _, out = await srv._execute_command("N0CALL", f"dbexport {dump}")
            assert "Database export written to" in out
            assert "dump.sql" in out
            assert Path(dump).exists()

            users_csv = str(tmp_path / "users.csv")
            _, out = await srv._execute_command("N0CALL", f"export_users {users_csv}")
            assert "User export written to" in out
            assert Path(users_csv).exists()

            _, out = await srv._execute_command("N0CALL", "send_config")
            assert "node_call=" in out
            cfg_out = str(tmp_path / "config.out")
            _, out = await srv._execute_command("N0CALL", f"send_config {cfg_out}")
            assert "Configuration snapshot written to" in out
            assert Path(cfg_out).exists()
            _, out = await srv._execute_command("N0CALL", "mrtg users")
            assert "uptime=" in out
            assert cfg.node.node_call in out
            _, out = await srv._execute_command("N0CALL", "pc")
            assert "pc: supported=" in out
            _, out = await srv._execute_command("N0CALL", "pc 24")
            assert "pc24: supported=yes" in out
            _, out = await srv._execute_command("N0CALL", "demonstrate show/time")
            assert "demonstrate: show/time" in out
            assert "Z" in out

            _, out = await srv._execute_command("N0CALL", "debug on")
            assert "debug=on" in out
            _, out = await srv._execute_command("N0CALL", "debug")
            assert "debug=on" in out
            _, out = await srv._execute_command("N0CALL", "debug off")
            assert "debug=off" in out

            _, out = await srv._execute_command("N0CALL", "rcmd SH/DX 5")
            assert "rcmd=SH/DX 5" in out
            _, out = await srv._execute_command("N0CALL", "rcmd")
            assert "rcmd=SH/DX 5" in out

            _, out = await srv._execute_command("N0CALL", "privilege")
            assert "Privilege for N0CALL set to" in out

            _, out = await srv._execute_command("N0CALL", "save")
            assert "Saved " in out

            _, out = await srv._execute_command("N0CALL", "sysop")
            assert "registered N0CALL" in out

            _, out = await srv._execute_command("N0CALL", "get/keps")
            assert "Keplerian elements request accepted." in out
            assert await store.get_user_pref("N0CALL", "keps_last_request_epoch") is not None
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
            assert "Database import complete: 1 imported, 0 skipped." in out

            _, out = await srv._execute_command("N0CALL", "set/user N0CALL")
            assert "User record created or updated for N0CALL." in out
            await srv._execute_command("N0CALL", "set/var N0CALL test=one")
            await srv._execute_command("N0CALL", "set/uservar N0CALL note=two")
            await srv._execute_command("N0CALL", "set/usdb N0CALL qth FN42")
            await srv._execute_command("N0CALL", "set/buddy K1ABC")
            _, out = await srv._execute_command("N0CALL", "dbremove user N0CALL")
            assert "Removed " in out
            assert "stored item(s) for N0CALL:" in out
            assert "prefs=" in out
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


def test_mrtg_metrics_and_agwrestart_counter(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "mrtg_metrics.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await store.set_user_pref("N0CALL", "privilege", "sysop", now)
            await store.add_message("K1ABC", "N0CALL", now, "hello")

            _, out = await srv._execute_command("N0CALL", "mrtg unread")
            lines = [ln for ln in out.splitlines() if ln.strip()]
            assert len(lines) >= 4
            assert lines[0] == "MRTG export:"
            assert any("Value: 1" in ln for ln in lines)
            assert any(f"Node: {cfg.node.node_call}" in ln for ln in lines)

            _, out = await srv._execute_command("N0CALL", "agwrestart")
            assert "(count 1)." in out
            _, out = await srv._execute_command("N0CALL", "agwrestart")
            assert "(count 2)." in out
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
            assert "Queue summary: peers=2" in out
            assert "queued=7" in out
            assert "rx=15" in out
            assert "tx=16" in out
            assert "dropped=3" in out
            assert "policy_drop=4" in out

            _, out = await srv._execute_command("N0CALL", "stat/channel")
            assert "Channel summary: peers=2" in out
            assert "inbound=1" in out
            assert "outbound=1" in out
            assert "rx=15" in out
            assert "tx=16" in out
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
            assert "Saved " in out
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
            assert "pc24: supported=yes" in out
            assert "feature=dx" in out
            assert "state=on" in out

            _, out = await srv._execute_command("N0CALL", "pc 24 off")
            assert "state=off" in out
            assert await store.get_user_pref("N0CALL", "relay.spots") == "off"

            _, out = await srv._execute_command("N0CALL", "show/relay")
            assert "SPOTS: off (user)" in out

            _, out = await srv._execute_command("N0CALL", "pc 61 on")
            assert "feature=route" in out
            assert "state=on" in out
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
            assert "dxqsl status:" in out
            assert "export_path=/tmp/dxqsl.out" in out
            assert "ready=no" in out

            _, out = await srv._execute_command("N0CALL", "show/db0sdx")
            assert "db0sdx gateway status:" in out
            assert "enabled=on" in out
            assert "host=db0sdx.de" in out

            _, out = await srv._execute_command("N0CALL", "show/cmdcache")
            assert "cmd_cache: commands=" in out
            assert "state=warm" in out

            _, out = await srv._execute_command("N0CALL", "load/dxqsl")
            assert "DXQSL settings loaded for N0CALL: export=yes, import=no." in out

            _, out = await srv._execute_command("N0CALL", "load/badwords")
            assert "Loaded 1 bad-word rule entry." in out

            _, out = await srv._execute_command("N0CALL", "load/db")
            assert "Database loaded from " in out
            assert "1 spots" in out or "1 spot" in out

            _, out = await srv._execute_command("N0CALL", "dbshow")
            assert "Database summary: sqlite at " in out
            assert "1 spots" in out

            _, out = await srv._execute_command("N0CALL", "dbshow messages")
            assert "Messages: 1 total, 1 unread." in out

            _, out = await srv._execute_command("N0CALL", "dbavail")
            assert "SQLite database at " in out
            assert "exists=yes" in out
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
            assert "talk=on" in out
            _, out = await srv._execute_command("N0CALL", "type announce hello")
            assert "blocked unsafe command" in out
            _, out = await srv._execute_command("N0CALL", "merge do show/time")
            assert "nested control commands are disabled" in out
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
            assert "kill: disabled by control policy" in out
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

            _, out = await srv._execute_command("N0CALL", "show/control --reset")
            assert "show/control --reset removed=" in out
            _, out = await srv._execute_command("N0CALL", "show/control")
            assert "Recent control events: 1" in out
            assert "show/control --reset removed=" in out
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
            assert "relay.spots=on (default)" in out
            assert "relay.chat=on (default)" in out
            assert "routepc19=off" in out

            _, out = await srv._execute_command("N0CALL", "set/relay chat off")
            assert "Relay policy for chat set to off." in out
            _, out = await srv._execute_command("N0CALL", "set/relay spots off")
            assert "Relay policy for spots set to off." in out
            _, out = await srv._execute_command("N0CALL", "set/relay all on")
            assert "Relay policy set to on for all traffic." in out
            _, out = await srv._execute_command("N0CALL", "unset/relay wcy")
            assert "Relay policy for wcy restored to default" in out
            _, out = await srv._execute_command("N0CALL", "show/relay")
            assert "relay.spots=on (user)" in out
            assert "relay.chat=on (user)" in out
            assert "relay.wcy=on (default)" in out
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
            _, out = await srv._execute_command("N0CALL", "dx 14074.0 K1ABC FT8 test")
            assert "dx posted 14074.0 K1ABC" in out
            assert await store.count_spots() == 1
            _, out = await srv._execute_command("N0CALL", "dx")
            assert "No spots available" not in out
            assert "K1ABC" in out
            _, out = await srv._execute_command("N0CALL", "dx K1")
            assert "K1ABC" in out
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
            assert "all=off (user)" in out
            _, out = await srv._execute_command("N0CALL", "show/relaypeer")
            assert "relay.peer.peer1=off" in out
            _, out = await srv._execute_command("N0CALL", "unset/relaypeer peer2 chat")
            assert "Relay policy for peer2 chat restored to default" in out
            _, out = await srv._execute_command("N0CALL", "show/relaypeer peer2")
            assert "chat=on (default)" in out
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
            assert "all=off (user)" in out
            _, out = await srv._execute_command("N0CALL", "show/ingestpeer")
            assert "ingest.peer.peer1=off" in out
            _, out = await srv._execute_command("N0CALL", "unset/ingestpeer peer2 spots")
            assert "Ingest policy for peer2 spots restored to default" in out
            _, out = await srv._execute_command("N0CALL", "unset/ingestpeer peer2 wcy")
            assert "Ingest policy for peer2 wcy restored to default" in out
            _, out = await srv._execute_command("N0CALL", "show/ingestpeer peer2")
            assert "spots=on (default)" in out
            assert "wcy=on (default)" in out
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
            assert "CHAT: off (user)" in out
            assert "Relay Peer Overrides: 1" in out
            assert "Ingest Peer Overrides: 1" in out
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
            assert "Message summary: total=" in out and "unread=" in out

            _, out = await srv._execute_command("N0CALL", "stat/wcy")
            assert "stat/wcy: 1" in out

            _, out = await srv._execute_command("N0CALL", "stat/db")
            assert "Database summary: spots=" in out and "registry=" in out
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
            assert "Route nodes: total=2" in out and "inbound=1" in out and "outbound=1" in out

            _, out = await srv._execute_command("N0CALL", "stat/routeuser")
            assert "Route users: users=2" in out and "peers=2" in out

            _, out = await srv._execute_command("N0CALL", "stat/pc19list")
            assert "PC19 routing enabled for 1 calls:" in out and "N0CALL" in out

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
            assert "talk=on" in out
            _, out = await srv1._execute_command("N0CALL", "set/arcluster")
            assert "Profile for N0CALL set to arcluster." in out
            _, out = await srv1._execute_command("N0CALL", "set/beep")
            assert "beep=on" in out
            _, out = await srv1._execute_command("N0CALL", "set/qth Boston")
            assert "qth=Boston" in out
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
            assert "talk=on" in out
            _, out = await srv2._execute_command("N0CALL", "show/groups")
            assert "groups.joined=vhf" in out
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
            assert "clear/spots" in out
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
            assert "removed=1" in out
            _, out = await srv._execute_command("N0CALL", "unset/badword all")
            assert "removed=1" in out
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
            assert "usdb.state=MA" in out
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
            assert "color=blue" in out
            assert "page=40" in out
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
            assert "color=blue" in out
            assert "page=40" in out

            _, out = await srv2._execute_command("N0CALL", "show/var color")
            assert "color=blue" in out

            _, out = await srv2._execute_command("N0CALL", "unset/var color")
            assert "Variable color cleared for N0CALL." in out
            _, out = await srv2._execute_command("N0CALL", "show/var")
            assert "color=blue" not in out
            assert "page=40" in out
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
            assert "registered K1ABC" in out
            assert "name=Alice Example" in out
            assert "qth=Cambridge" in out
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
            assert "(none)" in out
        finally:
            await store2.close()

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
            assert "homebbs=K1BBS" in out
            _, out = await srv1._execute_command("N0CALL", "set/homenode N2NODE")
            assert "homenode=N2NODE" in out
            _, out = await srv1._execute_command("N0CALL", "set/node W3NODE")
            assert "node=W3NODE" in out
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
            assert "homebbs=K1BBS" in out
            assert "homenode=N2NODE" in out
            assert "node=W3NODE" in out

            _, out = await srv2._execute_command("N0CALL", "show/node N0CALL")
            assert "homebbs   : K1BBS" in out
            assert "homenode  : N2NODE" in out
            assert "node      : W3NODE" in out
        finally:
            await store2.close()

    asyncio.run(run())


def test_sysop_namespace_handles_user_management(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "sysop_namespace.db")
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

            assert await store.get_user_pref("K1ABC", "password") == "supersecret"
            assert await store.get_user_pref("K1ABC", "blocked_login") == "on"

            _, out = await srv._execute_command("AI3I", "sysop/clearpassword K1ABC")
            assert "Password cleared for K1ABC." in out
            assert await store.get_user_pref("K1ABC", "password") is None
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
            node=NodeConfig(node_call="AI3I-16"),
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
            assert "address=123 Main St" in out
            assert "email=op@example.net" in out

            _, out = await srv2._execute_command("N0CALL", "show/station")
            assert "address=123 Main St" in out
            assert "email=op@example.net" in out

            _, out = await srv2._execute_command("N0CALL", "unset/email")
            assert "Email cleared for N0CALL." in out
            _, out = await srv2._execute_command("N0CALL", "show/registered N0CALL")
            assert "email=" in out
            assert "email=op@example.net" not in out
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
            assert "a=1" in out
            _, out = await srv._execute_command("N0CALL", "set/var b 2")
            assert "b=2" in out
            _, out = await srv._execute_command("N0CALL", "set/var c 3")
            assert "c=3" in out
            _, out = await srv._execute_command("N0CALL", "show/var")
            assert "a=1" in out and "b=2" in out
            assert "c=3" not in out

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
            assert "Last Peer: ('203.0.113.1', 7300)" in out

            _, out = await srv._execute_command("N0CALL", "show/users")
            assert "last=" in out or "Last " in out
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
            assert "show/time" in out and "show/date" in out

            outs = await srv._run_startup_commands("N0CALL")
            joined = "".join(outs)
            assert "Z" in joined
            assert "-Jan-" in joined or "-Feb-" in joined or "-Mar-" in joined or "-Apr-" in joined
            assert "skipped unsafe command" in joined

            _, out = await srv._execute_command("N0CALL", "unset/startup")
            assert "Startup commands disabled for N0CALL." in out
            outs2 = await srv._run_startup_commands("N0CALL")
            assert outs2 == []
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
            assert "show/time" in out and "show/date" in out
        finally:
            await store2.close()

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
            assert "uservar.monitor=all" in out
            assert "uservar.color=amber" in out

            _, out = await srv2._execute_command("N0CALL", "show/station")
            assert "uservar.monitor=all" in out
            assert "uservar.color=amber" in out

            _, out = await srv2._execute_command("N0CALL", "unset/uservar monitor")
            assert "User variable monitor cleared for N0CALL." in out
            _, out = await srv2._execute_command("N0CALL", "show/registered N0CALL")
            assert "uservar.monitor=all" not in out
            assert "uservar.color=amber" in out
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
            prompt = await asyncio.wait_for(r1.readuntil(b"login: "), timeout=2.0)
            assert b"login:" in prompt
            w1.write(b"N0CALL\r\n")
            await w1.drain()
            pw = await asyncio.wait_for(r1.readuntil(b"password: "), timeout=2.0)
            assert b"password:" in pw
            w1.write(b"pw1\r\n")
            await w1.drain()
            hello = await asyncio.wait_for(r1.read(4096), timeout=2.0)
            assert b"***" in hello
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


def test_telnet_first_login_forces_password_creation(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "first_login_password.db")
        cfg = AppConfig(
            node=NodeConfig(node_call="AI3I-16"),
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
            assert await store.get_user_pref("N0CALL", "password") == "pw1"
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
