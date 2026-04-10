from __future__ import annotations

from datetime import datetime, timezone

from pycluster.config import AppConfig, NodeConfig, PublicWebConfig, StoreConfig, TelnetConfig, WebConfig
from pycluster.dxspider_archive import parse_dxspider_wcy_record, parse_dxspider_wwv_record
from pycluster.models import Spot, parse_spot_record
from pycluster.store import SpotStore
from pycluster.telnet_server import Session, TelnetClusterServer


class _DummyWriter:
    def __init__(self) -> None:
        self.buffer = bytearray()

    def write(self, b: bytes) -> None:
        self.buffer.extend(b)

    async def drain(self) -> None:
        return


def _mk_config(tmp_db: str, motd: str = "Welcome to pyCluster") -> AppConfig:
    return AppConfig(
        node=NodeConfig(motd=motd),
        telnet=TelnetConfig(host="127.0.0.1", port=0, idle_timeout_seconds=30),
        web=WebConfig(host="127.0.0.1", port=0),
        public_web=PublicWebConfig(),
        store=StoreConfig(sqlite_path=tmp_db),
    )


def test_parse_real_dxspider_spot_archive_line() -> None:
    line = "14022.0^5J500V^1572561000^Tnx,73 Gl^LU6DOT^76^124^EA7URM-5^12^9^14^13^^^164.132.170.110"
    spot = parse_spot_record(line)

    assert spot.freq_khz == 14022.0
    assert spot.dx_call == "5J500V"
    assert spot.epoch == 1572561000
    assert spot.info == "Tnx,73 Gl"
    assert spot.spotter == "LU6DOT"
    assert spot.source_node == "EA7URM-5"


def test_parse_real_dxspider_wwv_archive_line() -> None:
    line = "VA6AAA^1672532280^165^16^4^No Storms -> No Storms^VA6AAA-1^0"
    rec = parse_dxspider_wwv_record(line)

    assert rec.sender == "VA6AAA"
    assert rec.epoch == 1672532280
    assert rec.sfi == 165
    assert rec.a_index == 16
    assert rec.k_index == 4
    assert rec.source_node == "VA6AAA-1"
    assert rec.body == "SFI=165 A=16 K=4 No Storms -> No Storms"


def test_parse_real_dxspider_wcy_archive_line() -> None:
    line = "1672532280^162^16^4^0^121^act^act^no^DK0WCY^DB0SUE-7"
    rec = parse_dxspider_wcy_record(line)

    assert rec.epoch == 1672532280
    assert rec.sfi == 162
    assert rec.a_index == 16
    assert rec.k_index == 4
    assert rec.sunspots == 0
    assert rec.expk == 121
    assert rec.sender == "DK0WCY"
    assert rec.source_node == "DB0SUE-7"
    assert rec.body == "SFI=162 A=16 K=4 ExpK=121 R=0 SA=act GMF=act Aurora=no"


def test_show_wcy_and_wwv_accept_real_dxspider_fixture_bodies(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "real_dxspider_bodies.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            wwv = parse_dxspider_wwv_record("VA6AAA^1672532280^165^16^4^No Storms -> No Storms^VA6AAA-1^0")
            wcy = parse_dxspider_wcy_record("1672532280^162^16^4^0^121^act^act^no^DK0WCY^DB0SUE-7")
            await store.add_bulletin("wwv", wwv.sender, "LOCAL", wwv.epoch, wwv.body)
            await store.add_bulletin("wcy", wcy.sender, "LOCAL", wcy.epoch, wcy.body)

            _, wwv_out = await srv._execute_command("N0CALL", "show/wwv")
            _, wcy_out = await srv._execute_command("N0CALL", "show/wcy")

            assert "Date        Hour   SFI   A   K Forecast" in wwv_out
            assert "1-Jan-2023" in wwv_out
            assert "No Storms -> No Storms" in wwv_out
            assert "<VA6AAA>" in wwv_out

            assert "Date        Hour   SFI   A   K Exp.K" in wcy_out
            assert "1-Jan-2023" in wcy_out
            assert "162" in wcy_out and "121" in wcy_out
            assert "<DK0WCY>" in wcy_out
        finally:
            await store.close()

    import asyncio

    asyncio.run(run())


def test_show_motd_can_render_real_dxspider_text(tmp_path) -> None:
    async def run() -> None:
        motd = (
            "Hello, and welcome to DX Cluster node AI3I-15, running DX Spider version 1.55!\n\n"
            "Connections may be made directly to dxc.ai3i.net or dxcluster.ai3i.net via TCP\n"
            "ports 7373, 7300 and 8000!  Web access may be coming in the future."
        )
        db = str(tmp_path / "motd_real_dxspider.db")
        cfg = _mk_config(db, motd=motd)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            _, out = await srv._execute_command("N0CALL", "show/motd")
            assert "================================================================================" in out
            assert "DX Cluster node AI3I-15" in out
            assert "running DX Spider version 1.55" in out
            assert "ports 7373, 7300 and 8000" in out
        finally:
            await store.close()

    import asyncio

    asyncio.run(run())


def test_show_dx_orders_same_timestamp_rows_by_frequency(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "show_dx_order.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))
        srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            epoch = 1773173280
            await store.add_spot(Spot(432174.0, "YL2AJ", epoch, "KO18VM<TR>KO16OX FT8", "ES8PW", "N2WQ-1", ""))
            await store.add_spot(Spot(7135.5, "IU2VZW", epoch, "ssb Young Ladies WWA", "IW1FRU", "N2WQ-1", ""))

            _, out = await srv._execute_command("N0CALL", "show/dx 2")
            lines = [line for line in out.splitlines() if line.strip()]
            assert "7135.5" in lines[0]
            assert "432174.0" in lines[1]
        finally:
            await store.close()

    import asyncio

    asyncio.run(run())
