from __future__ import annotations

import asyncio
from pathlib import Path
import pytest

from pycluster.app import ClusterApp
from pycluster.config import AppConfig, NodeConfig, PublicWebConfig, StoreConfig, TelnetConfig, WebConfig
from pycluster.node_link import LinkPeer, NodeLinkEngine
from pycluster.protocol import WirePcFrame, parse_wire_pc_frame, parse_wire_protocol_frame, serialize_wire_pc_frame


class _ClosedInboundConnection:
    name = "REMOTE-1"
    transport = "tcp"
    path_hint = "test"

    async def readline(self) -> str | None:
        return None

    async def send_line(self, _line: str) -> None:
        return None

    async def close(self) -> None:
        return None


class _CaptureConnection:
    name = "capture"
    transport = "tcp"
    path_hint = "test"

    def __init__(self) -> None:
        self.lines: list[str] = []

    async def readline(self) -> str | None:
        return None

    async def send_line(self, line: str) -> None:
        self.lines.append(line)

    async def close(self) -> None:
        return None


def _mk_config(db_path: str) -> AppConfig:
    return AppConfig(
        node=NodeConfig(),
        telnet=TelnetConfig(host="127.0.0.1", port=0, idle_timeout_seconds=30),
        web=WebConfig(host="127.0.0.1", port=0),
        public_web=PublicWebConfig(),
        store=StoreConfig(sqlite_path=db_path),
    )


def test_wire_frame_parse_serialize() -> None:
    raw = "PC92^UF3K-1^0^D^^5R1BLH-1^H96^"
    frame = parse_wire_pc_frame(raw)
    assert frame is not None
    assert frame.pc_type == "PC92"
    assert frame.payload_fields[0] == "UF3K-1"
    assert serialize_wire_pc_frame(frame) == raw


def test_py_frame_requires_generic_wire_parser() -> None:
    raw = "PY00^1^HELLO^AI3I-90^1.0.12^py99-error^1785456000"
    assert parse_wire_pc_frame(raw) is None
    frame = parse_wire_protocol_frame(raw)
    assert frame is not None
    assert frame.pc_type == "PY00"
    assert frame.payload_fields[2] == "AI3I-90"
    assert serialize_wire_pc_frame(frame) == raw


def test_node_link_sanitizes_outbound_pc92_private_ips() -> None:
    async def run() -> None:
        engine = NodeLinkEngine(public_ip_address="44.2.3.4")
        conn = _CaptureConnection()
        await engine.accept_inbound("N9JR-2", conn, profile="dxspider")
        await engine.send(
            "N9JR-2",
            WirePcFrame("PC92", ["N9JR-2", "1", "A", "", "7N9JR-4:192.168.222.19", "5N9JR-3:fd00::4", "H96", ""]),
        )
        assert conn.lines == ["PC92^N9JR-2^1^A^^7N9JR-4:44.2.3.4^5N9JR-3:44.2.3.4^H96^"]

    asyncio.run(run())


def test_node_link_connect_trace_never_records_dsn_credentials(monkeypatch) -> None:
    async def run() -> None:
        async def _connect(_name: str, _dsn: str):
            return _ClosedInboundConnection()

        monkeypatch.setattr("pycluster.node_link.connect_from_dsn", _connect)
        events: list[tuple[str, str, str]] = []
        engine = NodeLinkEngine()

        async def _trace(peer: str, direction: str, text: str) -> None:
            events.append((peer, direction, text))

        engine.set_trace_hook(_trace)
        await engine.connect_dsn(
            "N9JR-2",
            "dxspider://user:DO_NOT_LEAK@192.168.222.2:7300?password=DO_NOT_LEAK",
        )
        await asyncio.sleep(0)
        await engine.stop()
        connect_events = [event for event in events if event[1] == "connect"]
        assert connect_events == [("N9JR-2", "connect", "dxspider ipv4 192.168.222.2:7300")]
        assert all("DO_NOT_LEAK" not in event[2] for event in events)

    asyncio.run(run())


def test_cluster_app_passes_detected_public_ip_to_node_link_when_config_blank(tmp_path, monkeypatch) -> None:
    db = str(tmp_path / "detected_public_ip.db")
    cfg = _mk_config(db)
    monkeypatch.setattr(
        "pycluster.app.detected_public_ip_addresses",
        lambda: {"ipv4": "44.9.8.7", "ipv6": "2606:4700:4700::1111"},
    )
    app = ClusterApp(cfg)
    try:
        assert app.node_link.public_ip_address == "44.9.8.7"
        assert app.node_link.public_ipv6_address == "2606:4700:4700::1111"
    finally:
        asyncio.run(app.store.close())


def test_node_link_loopback() -> None:
    async def run() -> None:
        listener = NodeLinkEngine()
        remote = NodeLinkEngine()
        try:
            await listener.start_listener("127.0.0.1", 0)
        except OSError as exc:
            pytest.skip(f"socket bind not available in this environment: {exc}")
        try:
            port = listener.listen_port()
            assert port is not None
            await remote.connect("loop", "127.0.0.1", port)
            await asyncio.sleep(0.05)

            await remote.send("loop", WirePcFrame("PC93", ["N0NODE-1", "0", "*", "N0NODE-1", "*", "hello", "", "127.0.0.1", "H1", ""]))
            msg = await listener.recv(timeout=1.0)
            assert msg is not None
            peer_name, frame, typed = msg
            assert frame.pc_type == "PC93"
            assert frame.payload_fields[5] == "hello"
            assert typed is not None

            stats = await listener.stats()
            total = sum(p["parsed_frames"] for p in stats.values())
            assert total >= 1
        finally:
            await remote.stop()
            await listener.stop()

    asyncio.run(run())


def test_node_link_profile_blocks_pc24_over_wire() -> None:
    async def run() -> None:
        listener = NodeLinkEngine()
        remote = NodeLinkEngine()
        try:
            try:
                await listener.start_listener("127.0.0.1", 0)
            except OSError as exc:
                pytest.skip(f"socket bind not available in this environment: {exc}")

            port = listener.listen_port()
            assert port is not None
            await remote.connect("uplink", "127.0.0.1", port)
            await asyncio.sleep(0.05)

            names = await listener.peer_names()
            assert len(names) == 1
            inbound = names[0]
            assert await listener.set_peer_profile(inbound, "dxnet") is True

            await remote.send("uplink", WirePcFrame("PC24", ["OH8X", "1", "H29", ""]))
            await remote.send(
                "uplink",
                WirePcFrame("PC61", ["14074.0", "K1ABC", "1-Mar-2026", "0000Z", "FT8", "N0CALL", "PEER1", "127.0.0.1", "H1", "~"]),
            )

            item = await listener.recv(timeout=1.0)
            assert item is not None
            _, frame, typed = item
            assert frame.pc_type == "PC61"
            assert typed is not None

            stats = await listener.stats()
            assert stats[inbound]["policy_reasons"]["profile_rx_block"] == 1
            assert stats[inbound]["parsed_frames"] >= 1
            assert stats[inbound]["rx_by_type"]["PC61"] >= 1
        finally:
            await remote.stop()
            await listener.stop()

    asyncio.run(run())


def test_node_link_remote_disconnect_cleans_up_inbound_peer() -> None:
    async def run() -> None:
        listener = NodeLinkEngine()
        remote = NodeLinkEngine()
        try:
            try:
                await listener.start_listener("127.0.0.1", 0)
            except OSError as exc:
                pytest.skip(f"socket bind not available in this environment: {exc}")

            port = listener.listen_port()
            assert port is not None
            await remote.connect("uplink", "127.0.0.1", port)
            await asyncio.sleep(0.05)

            assert len(await listener.peer_names()) == 1
            await remote.stop()

            deadline = asyncio.get_running_loop().time() + 1.0
            while asyncio.get_running_loop().time() < deadline:
                if await listener.peer_names() == []:
                    break
                await asyncio.sleep(0.01)
            assert await listener.peer_names() == []
        finally:
            await remote.stop()
            await listener.stop()

    asyncio.run(run())


def test_accepted_inbound_connection_is_removed_on_eof() -> None:
    async def run() -> None:
        engine = NodeLinkEngine()
        await engine.accept_inbound("REMOTE-1", _ClosedInboundConnection(), profile="dxspider")
        deadline = asyncio.get_running_loop().time() + 1.0
        while asyncio.get_running_loop().time() < deadline and await engine.peer_names():
            await asyncio.sleep(0.01)
        assert await engine.peer_names() == []
        await engine.stop()

    asyncio.run(run())


def test_node_link_send_failure_drops_dead_peer() -> None:
    class _FailingConn:
        async def send_line(self, _line: str) -> None:
            raise ConnectionResetError("Connection lost")

        async def close(self) -> None:
            return

    async def run() -> None:
        engine = NodeLinkEngine()
        engine._peers["peer1"] = LinkPeer(
            name="peer1",
            conn=_FailingConn(),
            inbound=False,
        )
        with pytest.raises(ConnectionResetError):
            await engine.send("peer1", WirePcFrame("PC20", [""]))
        assert await engine.peer_names() == []

    asyncio.run(run())


def test_node_link_broadcast_multi_peer_respects_profile_tx_policy() -> None:
    async def run() -> None:
        listener = NodeLinkEngine()
        remote1 = NodeLinkEngine()
        remote2 = NodeLinkEngine()
        try:
            try:
                await listener.start_listener("127.0.0.1", 0)
            except OSError as exc:
                pytest.skip(f"socket bind not available in this environment: {exc}")

            port = listener.listen_port()
            assert port is not None

            await remote1.start_listener("127.0.0.1", 0)
            await remote2.start_listener("127.0.0.1", 0)
            r1_port = remote1.listen_port()
            r2_port = remote2.listen_port()
            assert r1_port is not None and r2_port is not None

            await listener.connect("peer1", "127.0.0.1", r1_port)
            await listener.connect("peer2", "127.0.0.1", r2_port)
            await asyncio.sleep(0.05)

            assert await listener.set_peer_profile("peer2", "dxnet") is True

            sent = await listener.broadcast(WirePcFrame("PC24", ["OH8X", "1", "H29", ""]))
            assert sent == 1

            msg1 = await remote1.recv(timeout=1.0)
            assert msg1 is not None
            _, frame1, typed1 = msg1
            assert frame1.pc_type == "PC24"
            assert typed1 is not None

            msg2 = await remote2.recv(timeout=0.2)
            assert msg2 is None

            stats = await listener.stats()
            assert stats["peer1"]["sent_frames"] >= 1
            assert stats["peer2"]["policy_reasons"]["profile_tx_block"] == 1
        finally:
            await listener.stop()
            await remote1.stop()
            await remote2.stop()

    asyncio.run(run())


def test_node_link_multi_peer_rx_profiles_diverge_on_same_burst() -> None:
    async def run() -> None:
        listener = NodeLinkEngine()
        remote1 = NodeLinkEngine()
        remote2 = NodeLinkEngine()
        try:
            try:
                await listener.start_listener("127.0.0.1", 0)
            except OSError as exc:
                pytest.skip(f"socket bind not available in this environment: {exc}")

            port = listener.listen_port()
            assert port is not None
            await remote1.connect("peer-a", "127.0.0.1", port)
            await remote2.connect("peer-b", "127.0.0.1", port)
            await asyncio.sleep(0.05)

            names = sorted(await listener.peer_names())
            assert len(names) == 2
            blocked_peer = names[1]
            assert await listener.set_peer_profile(blocked_peer, "dxnet") is True

            await remote1.send("peer-a", WirePcFrame("PC24", ["OH8X", "1", "H29", ""]))
            await remote1.send(
                "peer-a",
                WirePcFrame("PC61", ["14074.0", "K1ABC", "1-Mar-2026", "0000Z", "FT8", "N0CALL", "PEERA", "127.0.0.1", "H1", "~"]),
            )
            await remote2.send("peer-b", WirePcFrame("PC24", ["W3LPL", "1", "H29", ""]))
            await remote2.send(
                "peer-b",
                WirePcFrame("PC61", ["7074.0", "W1AW", "1-Mar-2026", "0001Z", "FT8", "K1ABC", "PEERB", "127.0.0.1", "H1", "~"]),
            )

            received = []
            for _ in range(3):
                item = await listener.recv(timeout=1.0)
                assert item is not None
                received.append(item[1].pc_type)

            assert received.count("PC61") == 2
            assert received.count("PC24") == 1

            stats = await listener.stats()
            allowed_peer = next(name for name in names if name != blocked_peer)
            assert stats[allowed_peer]["rx_by_type"]["PC24"] == 1
            assert stats[allowed_peer]["rx_by_type"]["PC61"] == 1
            assert stats[blocked_peer]["rx_by_type"]["PC61"] == 1
            assert stats[blocked_peer]["policy_reasons"]["profile_rx_block"] == 1
        finally:
            await remote1.stop()
            await remote2.stop()
            await listener.stop()

    asyncio.run(run())


def test_app_protocol_trace_writes_rx_tx_lines(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "trace.db")
        app = ClusterApp(_mk_config(db))
        try:
            await app._trace_protocol_line("PEER1", "tx", "PC11^14074.0^N0TST^11-Mar-2026^1900Z^test^AI3I^AI3I-16^H1^~")
            await app._trace_protocol_line("PEER1", "rx", "PC51^AI3I-16^AI3I-15^1^")
            path = tmp_path.parent / "logs" / "proto" / datetime.now(timezone.utc).strftime("%Y") / f"{datetime.now(timezone.utc).timetuple().tm_yday:03d}.log"
            assert path.exists()
            text = path.read_text(encoding="utf-8")
            assert "PEER1 tx PC11^14074.0^N0TST" in text
            assert "PEER1 rx PC51^AI3I-16^AI3I-15^1^" in text
        finally:
            await app.stop()

    from datetime import datetime, timezone

    asyncio.run(run())


def test_node_link_reconnect_replaces_peer_session_and_resets_counters() -> None:
    async def run() -> None:
        listener = NodeLinkEngine()
        remote = NodeLinkEngine()
        try:
            try:
                await listener.start_listener("127.0.0.1", 0)
            except OSError as exc:
                pytest.skip(f"socket bind not available in this environment: {exc}")

            port = listener.listen_port()
            assert port is not None

            await remote.connect("uplink", "127.0.0.1", port)
            await asyncio.sleep(0.05)
            names1 = await listener.peer_names()
            assert len(names1) == 1
            peer1 = names1[0]

            assert await listener.set_peer_profile(peer1, "dxnet") is True
            await remote.send("uplink", WirePcFrame("PC24", ["OH8X", "1", "H29", ""]))
            await remote.send(
                "uplink",
                WirePcFrame("PC61", ["14074.0", "K1ABC", "1-Mar-2026", "0000Z", "FT8", "N0CALL", "PEER1", "127.0.0.1", "H1", "~"]),
            )
            item = await listener.recv(timeout=1.0)
            assert item is not None
            assert item[1].pc_type == "PC61"

            stats1 = await listener.stats()
            assert stats1[peer1]["policy_reasons"]["profile_rx_block"] == 1

            await remote.stop()
            deadline = asyncio.get_running_loop().time() + 1.0
            while asyncio.get_running_loop().time() < deadline:
                if await listener.peer_names() == []:
                    break
                await asyncio.sleep(0.01)
            assert await listener.peer_names() == []

            remote = NodeLinkEngine()
            await remote.connect("uplink2", "127.0.0.1", port)
            await asyncio.sleep(0.05)
            names2 = await listener.peer_names()
            assert len(names2) == 1
            peer2 = names2[0]
            stats2 = await listener.stats()
            assert stats2[peer2]["policy_dropped"] == 0
            assert stats2[peer2]["parsed_frames"] == 0

            await remote.send(
                "uplink2",
                WirePcFrame("PC61", ["7074.0", "W1AW", "1-Mar-2026", "0001Z", "FT8", "K1ABC", "PEER2", "127.0.0.1", "H1", "~"]),
            )
            item2 = await listener.recv(timeout=1.0)
            assert item2 is not None
            assert item2[1].pc_type == "PC61"

            stats3 = await listener.stats()
            assert stats3[peer2]["parsed_frames"] >= 1
            assert stats3[peer2]["policy_dropped"] == 0
        finally:
            await remote.stop()
            await listener.stop()

    asyncio.run(run())
