from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path

import pycluster.telnet_server as telnet_server_mod
from pycluster.config import AppConfig, NodeConfig, PublicWebConfig, StoreConfig, TelnetConfig, WebConfig
from pycluster.maidenhead import coords_to_locator
from pycluster.store import SpotStore
from pycluster.telnet_server import Session, TelnetClusterServer


class _DummyWriter:
    def __init__(self) -> None:
        self.buffer = bytearray()

    def write(self, data: bytes) -> None:
        self.buffer.extend(data)

    async def drain(self) -> None:
        return None


def _mk_config(db: str) -> AppConfig:
    cty_path = Path(__file__).resolve().parents[1] / "fixtures/live/dxspider/cty.dat"
    return AppConfig(
        node=NodeConfig(node_call="N9JR-3", qth="Milwaukee, WI"),
        telnet=TelnetConfig(),
        web=WebConfig(),
        public_web=PublicWebConfig(cty_dat_path=str(cty_path)),
        store=StoreConfig(sqlite_path=db),
    )


def test_telnet_profile_commands_update_registry_and_console(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "issue_console_sync.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N9JR-5", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        orig = telnet_server_mod.estimate_location_from_locator
        telnet_server_mod.estimate_location_from_locator = lambda locator: "Milwaukee, WI"
        try:
            _, out = await srv._execute_command("N9JR-5", "set/name Joe")
            assert "name=Joe" in out
            _, out = await srv._execute_command("N9JR-5", "set/qth Milwaukee, WI")
            assert "qth=Milwaukee, WI" in out
            _, out = await srv._execute_command("N9JR-5", "set/qra EN63AA")
            assert "QRA set to EN63AA for N9JR-5." in out
            row = await store.get_user_registry("N9JR-5")
            assert row is not None
            assert str(row["display_name"]) == "Joe"
            assert str(row["qth"]) == "Milwaukee, WI"
            assert str(row["qra"]) == "EN63AA"
            station = await srv._execute_command("N9JR-5", "show/station")
            assert "Name: Joe" in station[1]
            assert "Grid Square (QRA): EN63AA" in station[1]
        finally:
            telnet_server_mod.estimate_location_from_locator = orig
            await store.close()

    asyncio.run(run())


def test_set_home_alias_and_who_lists_peers(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "issue_set_home.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(
            cfg,
            store,
            datetime.now(timezone.utc),
            link_stats_fn=lambda: asyncio.sleep(0, result={"AI3I-15": {"inbound": False, "profile": "dxspider"}}),
        )
        srv._sessions[1] = Session(call="N9JR-5", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            _, out = await srv._execute_command("N9JR-5", "set/home N9JR-3")
            assert "homenode=N9JR-3" in out
            row = await store.get_user_registry("N9JR-5")
            assert row is not None
            assert str(row["home_node"]) == "N9JR-3"
            _, out = await srv._execute_command("N9JR-5", "who")
            assert "N9JR-5" in out
            assert "AI3I-15" in out
        finally:
            await store.close()

    asyncio.run(run())


def test_show_heading_reports_bearing(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "issue_heading.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N9JR-5", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        orig = telnet_server_mod.estimate_location_from_locator
        telnet_server_mod.estimate_location_from_locator = lambda locator: "Grid EN63AA"
        try:
            await srv._execute_command("N9JR-5", "set/qra EN63AA")
            _, out = await srv._execute_command("N9JR-5", "show/heading G")
            assert "Heading to" in out
            assert "deg" in out
            assert "Reference: QRA EN63AA" in out
        finally:
            telnet_server_mod.estimate_location_from_locator = orig
            await store.close()

    asyncio.run(run())


def test_set_qra_backfills_location_when_unset(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "issue_qra_backfill.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N9JR-5", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        orig = telnet_server_mod.estimate_location_from_locator
        telnet_server_mod.estimate_location_from_locator = lambda locator: "Milwaukee, WI"
        try:
            _, out = await srv._execute_command("N9JR-5", "set/qra EN63AA")
            assert "QRA set to EN63AA" in out
            _, station = await srv._execute_command("N9JR-5", "show/station")
            assert "location=Milwaukee, WI" in station
            assert "qra=EN63AA" in station
        finally:
            telnet_server_mod.estimate_location_from_locator = orig
            await store.close()

    asyncio.run(run())


def test_set_location_updates_qra_and_takes_precedence(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "issue_location_precedence.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N9JR-5", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        orig = telnet_server_mod.resolve_location_to_coords
        orig_est = telnet_server_mod.estimate_location_from_locator
        telnet_server_mod.resolve_location_to_coords = lambda text: (42.3601, -71.0589)
        telnet_server_mod.estimate_location_from_locator = lambda locator: "Milwaukee, WI"
        try:
            await srv._execute_command("N9JR-5", "set/qra EN63AA")
            _, out = await srv._execute_command("N9JR-5", "set/location Boston, MA")
            assert "Location set to Boston, MA" in out
            row = await store.get_user_registry("N9JR-5")
            assert row is not None
            assert str(row["qra"]) == coords_to_locator(42.3601, -71.0589)
            _, heading = await srv._execute_command("N9JR-5", "show/heading G")
            assert "Reference: location Boston, MA" in heading
        finally:
            telnet_server_mod.resolve_location_to_coords = orig
            telnet_server_mod.estimate_location_from_locator = orig_est
            await store.close()

    asyncio.run(run())


def test_show_field_aliases_read_back_set_values(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "issue_show_fields.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N9JR-5", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        orig_est = telnet_server_mod.estimate_location_from_locator
        orig_res = telnet_server_mod.resolve_location_to_coords
        telnet_server_mod.estimate_location_from_locator = lambda locator: "Milwaukee, WI"
        telnet_server_mod.resolve_location_to_coords = lambda text: (42.3601, -71.0589) if text == "Boston, MA" else None
        try:
            assert "name=Joe" in (await srv._execute_command("N9JR-5", "set/name Joe"))[1]
            assert "qth=Milwaukee, WI" in (await srv._execute_command("N9JR-5", "set/qth Milwaukee, WI"))[1]
            assert "QRA set to EN63AA" in (await srv._execute_command("N9JR-5", "set/qra EN63AA"))[1]
            assert "Address updated" in (await srv._execute_command("N9JR-5", "set/address 123 Main St"))[1]
            assert "Email updated" in (await srv._execute_command("N9JR-5", "set/email joe@example.net"))[1]
            assert "Location set to Boston, MA" in (await srv._execute_command("N9JR-5", "set/location Boston, MA"))[1]

            assert "name=Joe" in (await srv._execute_command("N9JR-5", "show/name"))[1]
            assert "qth=Milwaukee, WI" in (await srv._execute_command("N9JR-5", "show/qth"))[1]
            assert "qra=" in (await srv._execute_command("N9JR-5", "show/qra"))[1]
            assert "address=123 Main St" in (await srv._execute_command("N9JR-5", "show/address"))[1]
            assert "email=joe@example.net" in (await srv._execute_command("N9JR-5", "show/email"))[1]
            assert "location=Boston, MA" in (await srv._execute_command("N9JR-5", "show/location"))[1]
        finally:
            telnet_server_mod.estimate_location_from_locator = orig_est
            telnet_server_mod.resolve_location_to_coords = orig_res
            await store.close()

    asyncio.run(run())


def test_telnet_login_line_strips_iac_negotiation_bytes(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "issue_telnet_login_iac.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        reader = asyncio.StreamReader()
        try:
            reader.feed_data(b"\xff\xfd\x03\xff\xfb\x01N9JR-5\r\n")
            reader.feed_eof()
            line = await srv._readline(reader)
            assert line == "N9JR-5"
            assert srv._sanitize_login_call(line) == "N9JR-5"
        finally:
            await store.close()

    asyncio.run(run())


def test_show_wm7d_returns_lookup_data_for_call(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "issue_wm7d_lookup.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N9JR-5", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        now = int(datetime.now(timezone.utc).timestamp())
        try:
            await store.upsert_user_registry(
                "N9JR",
                now,
                display_name="Joe Radio",
                qth="Milwaukee, WI",
                qra="EN63AA",
                home_node="N9JR-3",
                email="joe@example.net",
            )
            _, out = await srv._execute_command("N9JR-5", "show/wm7d N9JR")
            assert "WM7D lookup for N9JR:" in out
            assert "Name: Joe Radio" in out
            assert "QTH: Milwaukee, WI" in out
            assert "Grid: EN63AA" in out
            assert "Home Node: N9JR-3" in out
        finally:
            await store.close()

    asyncio.run(run())
