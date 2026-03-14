from __future__ import annotations

import asyncio

from pycluster.node_link import LinkPeer, NodeLinkEngine
from pycluster.protocol import WirePcFrame


class _DummyConn:
    def __init__(self) -> None:
        self.sent: list[str] = []
        self.lines: list[str | None] = []
        self.closed = False

    async def readline(self):
        if self.lines:
            return self.lines.pop(0)
        return None

    async def send_line(self, line: str) -> None:
        self.sent.append(line)

    async def close(self) -> None:
        self.closed = True
        return


def test_policy_drop_on_send_for_profile() -> None:
    async def run() -> None:
        eng = NodeLinkEngine()
        conn = _DummyConn()
        eng._peers["p1"] = LinkPeer(name="p1", conn=conn, inbound=False, profile="dxnet")

        await eng.send("p1", WirePcFrame("PC24", ["x"]))
        await eng.send("p1", WirePcFrame("PC61", ["a", "b"]))

        st = await eng.stats()
        assert st["p1"]["policy_dropped"] == 1
        assert st["p1"]["policy_reasons"]["profile_tx_block"] == 1
        assert st["p1"]["parsed_frames"] == 0
        assert st["p1"]["sent_frames"] == 1
        assert len(conn.sent) == 1
        assert conn.sent[0].startswith("PC61^")

    asyncio.run(run())


def test_set_peer_profile() -> None:
    async def run() -> None:
        eng = NodeLinkEngine()
        eng._peers["p1"] = LinkPeer(name="p1", conn=_DummyConn(), inbound=False, profile="spider")
        ok = await eng.set_peer_profile("p1", "arcluster")
        assert ok is True
        st = await eng.stats()
        assert st["p1"]["profile"] == "arcluster"
        ok2 = await eng.set_peer_profile("missing", "dxnet")
        assert ok2 is False

    asyncio.run(run())


def test_policy_drop_on_receive_for_profile_and_disconnect_cleanup() -> None:
    async def run() -> None:
        eng = NodeLinkEngine()
        conn = _DummyConn()
        conn.lines = [
            "PC24^OH8X^1^H29^",
            "PC61^14074.0^K1ABC^1-Mar-2026^0000Z^FT8^N0CALL^PEER1^127.0.0.1^H1^~",
            None,
        ]
        eng._peers["p1"] = LinkPeer(name="p1", conn=conn, inbound=False, profile="dxnet")

        await eng._peer_reader(eng._peers["p1"])

        item = await eng.recv(timeout=0.1)
        assert item is not None
        peer_name, frame, typed = item
        assert peer_name == "p1"
        assert frame.pc_type == "PC61"
        assert typed is not None

        st = await eng.stats()
        assert "p1" not in st
        assert conn.closed is True
        assert eng._frame_queue.empty() is True

    asyncio.run(run())


def test_clear_policy_drops_respects_peer_filter() -> None:
    async def run() -> None:
        eng = NodeLinkEngine()
        eng._peers["east"] = LinkPeer(name="east", conn=_DummyConn(), inbound=False, profile="spider")
        eng._peers["west"] = LinkPeer(name="west", conn=_DummyConn(), inbound=False, profile="spider")

        await eng.mark_policy_drop("east", "route_filter")
        await eng.mark_policy_drop("west", "profile_rx_block")

        cleared = await eng.clear_policy_drops("ea")
        assert cleared == 1

        st = await eng.stats()
        assert st["east"]["policy_dropped"] == 0
        assert st["east"]["policy_reasons"] == {}
        assert st["west"]["policy_dropped"] == 1
        assert st["west"]["policy_reasons"]["profile_rx_block"] == 1

    asyncio.run(run())
