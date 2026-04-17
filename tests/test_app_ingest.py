import asyncio
from datetime import datetime, timezone
import json
import pytest
import re

from pycluster import __version__
from pycluster.app import ClusterApp
from pycluster.config import AppConfig, NodeConfig, PublicWebConfig, StoreConfig, TelnetConfig, WebConfig
from pycluster.models import Spot
from pycluster.node_link import LinkPeer, NodeLinkEngine
from pycluster.protocol import Pc10Message, Pc11Message, Pc12Message, Pc23Message, Pc24Message, Pc50Message, Pc51Message, Pc61Message, Pc73Message, Pc93Message, WirePcFrame
from pycluster.telnet_server import Session


class _DummyWriter:
    def __init__(self) -> None:
        self.buffer = bytearray()
        self.closed = False
        self._extra: dict[str, object] = {}

    def write(self, b: bytes) -> None:
        self.buffer.extend(b)

    async def drain(self) -> None:
        return

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        return

    def get_extra_info(self, name: str, default=None):
        return self._extra.get(name, default)


class _DummyConn:
    async def readline(self):
        return None

    async def send_line(self, _line: str) -> None:
        return

    async def close(self) -> None:
        return


async def _wait_until(predicate, timeout: float = 1.0, step: float = 0.01) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(step)
    assert predicate()


async def _wait_until_async(predicate, timeout: float = 1.0, step: float = 0.01) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if await predicate():
            return
        await asyncio.sleep(step)
    assert await predicate()


def _mk_config(db_path: str) -> AppConfig:
    return AppConfig(
        node=NodeConfig(),
        telnet=TelnetConfig(host="127.0.0.1", port=0, idle_timeout_seconds=30),
        web=WebConfig(host="127.0.0.1", port=0),
        public_web=PublicWebConfig(),
        store=StoreConfig(sqlite_path=db_path),
    )


def test_ingest_pc61_adds_spot(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "ingest_pc61.db")
        app = ClusterApp(_mk_config(db))
        try:
            msg = Pc61Message.from_fields(
                ["14074.0", "K1ABC", "1-Mar-2026", "0000Z", "FT8", "N0CALL", "N2WQ-1", "127.0.0.1", "H1", "~"]
            )
            frame = WirePcFrame("PC61", msg.to_fields())
            await app._handle_node_link_item("PEER1", frame, msg)
            assert await app.store.count_spots() == 1
            rows = await app.store.latest_spots(limit=1)
            assert rows[0]["dx_call"] == "K1ABC"
            assert rows[0]["spotter"] == "N0CALL"
            assert rows[0]["source_node"] == "N2WQ-1"
        finally:
            await app.store.close()

    asyncio.run(run())


def test_connect_peer_sends_legacy_dxspider_init_frames(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "connect_peer_init.db")
        app = ClusterApp(_mk_config(db))
        connected: list[tuple[str, str, str]] = []
        sent: list[tuple[str, WirePcFrame]] = []
        try:
            async def _connect(name: str, dsn: str, profile: str = "spider") -> None:
                connected.append((name, dsn, profile))

            async def _send(peer: str, frame: WirePcFrame) -> None:
                sent.append((peer, frame))

            app.node_link.connect_dsn = _connect  # type: ignore[method-assign]
            app.node_link.send = _send  # type: ignore[method-assign]

            s1 = Session(call="AI3I", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
            app.telnet._sessions[1] = s1

            await app.connect_peer(
                "AI3I-15",
                "dxspider://dxspider.ai3i.net:7300?login=AI3I-16&client=AI3I-15",
            )

            assert connected == [
                (
                    "AI3I-15",
                    "dxspider://dxspider.ai3i.net:7300?login=AI3I-16&client=AI3I-15",
                    "spider",
                )
            ]
            prefs = await app.store.list_user_prefs(app.config.node.node_call)
            assert prefs["peer.outbound.ai3i-15.name"] == "AI3I-15"
            assert prefs["peer.outbound.ai3i-15.dsn"] == "dxspider://dxspider.ai3i.net:7300?login=AI3I-16&client=AI3I-15"
            assert prefs["peer.outbound.ai3i-15.reconnect"] == "on"
            node_call = app.config.node.node_call.upper()
            assert [frame.pc_type for _, frame in sent] == ["PC19", "PC16", "PC22"]
            assert sent[0][1].payload_fields == ["1", node_call, "0", "5457", "H1", ""]
            assert sent[1][1].payload_fields == [node_call, "AI3I - 1", "H1", ""]
            assert sent[2][1].payload_fields == [""]
        finally:
            await app.store.close()

    asyncio.run(run())


def test_accept_inbound_node_login_sends_legacy_banner_and_init(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "accept_inbound.db")
        app = ClusterApp(_mk_config(db))
        accepted: list[tuple[str, str]] = []
        legacy_init: list[str] = []
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await app.store.set_user_pref("AI3I-16", "node_family", "dxspider", now)

            async def _accept(name: str, _conn, profile: str = "dxspider") -> None:
                accepted.append((name, profile))

            async def _legacy(peer: str) -> None:
                legacy_init.append(peer)

            app.node_link.accept_inbound = _accept  # type: ignore[method-assign]
            app._send_legacy_init_config = _legacy  # type: ignore[method-assign]

            writer = _DummyWriter()
            ok = await app.accept_inbound_node_login("AI3I-16", "AI3I-15", asyncio.StreamReader(), writer)  # type: ignore[arg-type]

            text = writer.buffer.decode("utf-8", errors="replace")
            assert ok is True
            assert accepted == [("AI3I-16", "dxspider")]
            assert legacy_init == ["AI3I-16"]
            assert f"PC18^pyCluster {__version__}^" in text
            assert "PC20^" in text
        finally:
            await app.store.close()

    asyncio.run(run())


def test_disconnect_peer_forgets_persisted_target(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "disconnect_peer_target.db")
        app = ClusterApp(_mk_config(db))
        try:
            async def _connect(name: str, dsn: str, profile: str = "spider") -> None:
                return

            async def _send(_peer: str, _frame: WirePcFrame) -> None:
                return

            app.node_link.connect_dsn = _connect  # type: ignore[method-assign]
            app.node_link.send = _send  # type: ignore[method-assign]
            await app.connect_peer("AI3I-15", "dxspider://dxspider.ai3i.net:7300?login=AI3I-16&client=AI3I-15")
            ok = await app.disconnect_peer("AI3I-15")
            assert ok is False or ok is True
            prefs = await app.store.list_user_prefs(app.config.node.node_call)
            assert "peer.outbound.ai3i-15.name" not in prefs
            assert "peer.outbound.ai3i-15.dsn" not in prefs
        finally:
            await app.store.close()

    asyncio.run(run())


def test_reconnect_once_reattaches_persisted_peer_and_tracks_backoff(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "reconnect_once.db")
        app = ClusterApp(_mk_config(db))
        connected: list[tuple[str, str, str]] = []
        sent: list[tuple[str, WirePcFrame]] = []
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await app.store.set_user_pref(app.config.node.node_call, "peer.outbound.ai3i-15.name", "AI3I-15", now)
            await app.store.set_user_pref(app.config.node.node_call, "peer.outbound.ai3i-15.dsn", "dxspider://dxspider.ai3i.net:7300?login=AI3I-16&client=AI3I-15", now)
            await app.store.set_user_pref(app.config.node.node_call, "peer.outbound.ai3i-15.profile", "spider", now)
            await app.store.set_user_pref(app.config.node.node_call, "peer.outbound.ai3i-15.reconnect", "on", now)
            await app.store.set_user_pref(app.config.node.node_call, "peer.outbound.ai3i-15.retry_count", "0", now)
            await app.store.set_user_pref(app.config.node.node_call, "peer.outbound.ai3i-15.next_retry_epoch", "0", now)

            async def _connect(name: str, dsn: str, profile: str = "spider") -> None:
                connected.append((name, dsn, profile))

            async def _send(peer: str, frame: WirePcFrame) -> None:
                sent.append((peer, frame))

            app.node_link.connect_dsn = _connect  # type: ignore[method-assign]
            app.node_link.send = _send  # type: ignore[method-assign]

            await app.reconnect_once()
            assert connected == [
                (
                    "AI3I-15",
                    "dxspider://dxspider.ai3i.net:7300?login=AI3I-16&client=AI3I-15",
                    "spider",
                )
            ]
            assert [frame.pc_type for _, frame in sent] == ["PC19", "PC16", "PC22"]

            async def _fail(_name: str, _dsn: str, profile: str = "spider") -> None:
                raise RuntimeError("boom")

            app.node_link.connect_dsn = _fail  # type: ignore[method-assign]
            await app.disconnect_peer("AI3I-15", forget=False)
            await app.reconnect_once()
            prefs = await app.store.list_user_prefs(app.config.node.node_call)
            assert prefs["peer.outbound.ai3i-15.retry_count"] == "1"
            assert int(prefs["peer.outbound.ai3i-15.next_retry_epoch"]) >= now
            assert "boom" in prefs["peer.outbound.ai3i-15.last_error"]
        finally:
            await app.store.close()

    asyncio.run(run())


def test_desired_peer_status_includes_mail_queue_and_route_issues(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "desired_peer_mail_status.db")
        app = ClusterApp(_mk_config(db))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await app.store.set_user_pref(app.config.node.node_call, "peer.outbound.peer1.name", "PEER1", now)
            await app.store.set_user_pref(app.config.node.node_call, "peer.outbound.peer1.dsn", "tcp://example.invalid:7300", now)
            await app.store.set_user_pref(app.config.node.node_call, "peer.outbound.peer1.profile", "dxspider", now)
            await app.store.set_user_pref(app.config.node.node_call, "peer.outbound.peer1.reconnect", "on", now)
            await app.store.add_message(
                sender="N0CALL",
                recipient="K1ABC",
                epoch=now,
                body="pending route mail",
                origin_node=app.config.node.node_call,
                route_node="PEER1",
                delivery_state="pending",
            )
            await app.store.add_message(
                sender="N0CALL",
                recipient="K1DEF",
                epoch=now,
                body="failed route mail",
                origin_node=app.config.node.node_call,
                route_node="PEER1",
                delivery_state="undeliverable",
                error_text="no configured route to peer",
            )

            rows = await app.desired_peer_status()
            assert len(rows) == 1
            row = rows[0]
            assert row["peer"] == "PEER1"
            assert row["pending_mail"] == 1
            assert row["route_issues"] == 1
            assert row["connected"] is False
        finally:
            await app.store.close()

    asyncio.run(run())


def test_classify_pc93_bulletin_detects_parseable_wcy_payloads(tmp_path) -> None:
    db = str(tmp_path / "classify_wcy.db")
    app = ClusterApp(_mk_config(db))
    try:
        category, scope, body = app._classify_pc93_bulletin("N0BOT", "SFI=120 A=18 K=3 ExpK=2 R=105 SA=QUI GMF=MAJ Aurora=NO")
        assert category == "wcy"
        assert scope == "LOCAL"
        assert "ExpK=2" in body
    finally:
        asyncio.run(app.store.close())


def test_peer_password_is_stored_separately_from_dsn_and_injected_on_connect(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "peer_password_separate.db")
        app = ClusterApp(_mk_config(db))
        connected: list[tuple[str, str, str]] = []
        try:
            async def _connect(name: str, dsn: str, profile: str = "dxspider") -> None:
                connected.append((name, dsn, profile))

            app.node_link.connect_dsn = _connect  # type: ignore[method-assign]
            await app.save_peer_target(
                "AI3I-16",
                "dxspider://dxspider.ai3i.net:7300?login=AI3I-15&client=AI3I-16",
                profile="dxspider",
                reconnect=True,
                password="sekret",
            )

            prefs = await app.store.list_user_prefs(app.config.node.node_call)
            assert prefs["peer.outbound.ai3i-16.dsn"] == "dxspider://dxspider.ai3i.net:7300?login=AI3I-15&client=AI3I-16"
            assert prefs["peer.outbound.ai3i-16.password"] == "sekret"

            rows = await app.desired_peer_status()
            assert len(rows) == 1
            assert rows[0]["dsn"] == "dxspider://dxspider.ai3i.net:7300?login=AI3I-15&client=AI3I-16"
            assert rows[0]["password"] == "sekret"

            await app.connect_peer(
                "AI3I-16",
                "dxspider://dxspider.ai3i.net:7300?login=AI3I-15&client=AI3I-16",
                profile="dxspider",
                persist=False,
                password="sekret",
            )
            assert connected == [
                (
                    "AI3I-16",
                    "dxspider://dxspider.ai3i.net:7300?login=AI3I-15&client=AI3I-16&password=sekret",
                    "spider",
                )
            ]
        finally:
            await app.store.close()

    asyncio.run(run())


def test_ingest_pc61_rejects_invalid_and_disabled_inputs_with_policy_counts(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "ingest_pc61_policy.db")
        app = ClusterApp(_mk_config(db))
        try:
            app.node_link._peers["PEER1"] = LinkPeer(name="PEER1", conn=_DummyConn(), inbound=False)
            now = int(datetime.now(timezone.utc).timestamp())
            await app.store.set_user_pref(app.config.node.node_call, "ingest.peer.peer1.spots", "off", now)

            disabled = Pc61Message.from_fields(
                ["14074.0", "K1ABC", "1-Mar-2026", "0000Z", "FT8", "N0CALL", "PEER1", "127.0.0.1", "H1", "~"]
            )
            await app._handle_node_link_item("PEER1", WirePcFrame("PC61", disabled.to_fields()), disabled)

            await app.store.set_user_pref(app.config.node.node_call, "ingest.peer.peer1.spots", "on", now)
            invalid_call = Pc61Message.from_fields(
                ["14074.0", "@@@", "1-Mar-2026", "0000Z", "FT8", "N0CALL", "PEER1", "127.0.0.1", "H1", "~"]
            )
            await app._handle_node_link_item("PEER1", WirePcFrame("PC61", invalid_call.to_fields()), invalid_call)

            invalid_freq = Pc61Message.from_fields(
                ["BADFREQ", "K1ABC", "1-Mar-2026", "0000Z", "FT8", "N0CALL", "PEER1", "127.0.0.1", "H1", "~"]
            )
            await app._handle_node_link_item("PEER1", WirePcFrame("PC61", invalid_freq.to_fields()), invalid_freq)

            stats = await app.node_link.stats()
            assert stats["PEER1"]["policy_dropped"] == 3
            assert stats["PEER1"]["policy_reasons"]["ingest_spots_disabled"] == 1
            assert stats["PEER1"]["policy_reasons"]["ingest_spots_invalid_call"] == 1
            assert stats["PEER1"]["policy_reasons"]["ingest_spots_invalid_freq"] == 1
            assert await app.store.count_spots() == 0
        finally:
            await app.store.close()

    asyncio.run(run())


def test_ingest_pc11_adds_spot(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "ingest_pc11.db")
        app = ClusterApp(_mk_config(db))
        try:
            msg = Pc11Message.from_fields(
                ["7074.0", "K1ABC", "01-Mar-2026", "0000Z", "FT8", "N0CALL", "N2WQ-1", "H1", "~"]
            )
            frame = WirePcFrame("PC11", msg.to_fields())
            await app._handle_node_link_item("PEER1", frame, msg)
            assert await app.store.count_spots() == 1
            rows = await app.store.latest_spots(limit=1)
            assert rows[0]["dx_call"] == "K1ABC"
            assert rows[0]["spotter"] == "N0CALL"
            assert rows[0]["source_node"] == "N2WQ-1"
            prefs = await app.store.list_user_prefs(app.config.node.node_call)
            assert int(prefs.get("proto.peer.peer1.last_epoch", "0")) > 0
            assert prefs.get("proto.peer.peer1.last_pc_type") == "PC11"
        finally:
            await app.store.close()

    asyncio.run(run())


def test_ingest_pc11_accepts_plausible_special_event_style_call(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "ingest_pc11_special_event.db")
        app = ClusterApp(_mk_config(db))
        try:
            msg = Pc11Message.from_fields(
                ["7168.0", "RG65SM", "3-Apr-2026", "1649Z", "Wkd59 CQing", "F8DRA", "AI9T", "H1", "~"]
            )
            frame = WirePcFrame("PC11", msg.to_fields())
            await app._handle_node_link_item("PEER1", frame, msg)
            assert await app.store.count_spots() == 1
            rows = await app.store.latest_spots(limit=1)
            assert rows[0]["dx_call"] == "RG65SM"
            assert rows[0]["spotter"] == "F8DRA"
        finally:
            await app.store.close()

    asyncio.run(run())


def test_ingest_pc11_accepts_slashed_calls(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "ingest_pc11_slashed.db")
        app = ClusterApp(_mk_config(db))
        try:
            msg = Pc11Message.from_fields(
                ["50059.0", "W3NH/B", "11-Mar-2026", "2229Z", "EM84MO<ES>EM63PS", "W4LES", "EA6VQ-2", "H19", "~"]
            )
            frame = WirePcFrame("PC11", msg.to_fields())
            await app._handle_node_link_item("PEER1", frame, msg)
            assert await app.store.count_spots() == 1
            rows = await app.store.latest_spots(limit=1)
            assert rows[0]["dx_call"] == "W3NH/B"
            assert rows[0]["spotter"] == "W4LES"
            assert rows[0]["source_node"] == "EA6VQ-2"
        finally:
            await app.store.close()

    asyncio.run(run())


def test_ingest_pc24_pc50_pc51_record_proto_state(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "ingest_proto_state.db")
        app = ClusterApp(_mk_config(db))
        sent: list[tuple[str, WirePcFrame]] = []
        try:
            async def _send(peer: str, frame: WirePcFrame) -> None:
                sent.append((peer, frame))

            app.node_link.send = _send  # type: ignore[method-assign]
            m24 = Pc24Message.from_fields(["OH8X", "1", "H29", ""])
            await app._handle_node_link_item("PEER1", WirePcFrame("PC24", m24.to_fields()), m24)
            m50 = Pc50Message.from_fields(["W3LPL", "63", "H97", ""])
            await app._handle_node_link_item("PEER1", WirePcFrame("PC50", m50.to_fields()), m50)
            m51 = Pc51Message.from_fields([app.config.node.node_call, "WB3FFV-2", "1", ""])
            await app._handle_node_link_item("PEER1", WirePcFrame("PC51", m51.to_fields()), m51)
            # Toggle a protocol value twice to exercise transition tracking without
            # treating routine PC24 activity as link flapping.
            m24b = Pc24Message.from_fields(["OH8X", "0", "H29", ""])
            await app._handle_node_link_item("PEER1", WirePcFrame("PC24", m24b.to_fields()), m24b)
            m24c = Pc24Message.from_fields(["OH8X", "1", "H29", ""])
            await app._handle_node_link_item("PEER1", WirePcFrame("PC24", m24c.to_fields()), m24c)

            prefs = await app.store.list_user_prefs(app.config.node.node_call)
            assert prefs.get("proto.peer.peer1.pc24.call") == "OH8X"
            assert prefs.get("proto.peer.peer1.pc24.flag") == "1"
            assert prefs.get("proto.peer.peer1.pc50.call") == "W3LPL"
            assert prefs.get("proto.peer.peer1.pc50.count") == "63"
            assert prefs.get("proto.peer.peer1.pc51.to") == app.config.node.node_call
            assert prefs.get("proto.peer.peer1.pc51.from") == "WB3FFV-2"
            assert prefs.get("proto.peer.peer1.pc51.value") == "1"
            assert int(prefs.get("proto.peer.peer1.last_epoch", "0")) > 0
            assert int(prefs.get("proto.peer.peer1.change_count", "0")) >= 2
            assert int(prefs.get("proto.peer.peer1.flap_score", "0")) == 0
            assert int(prefs.get("proto.peer.peer1.change.pc24_flag", "0")) >= 2
            raw_hist = prefs.get("proto.peer.peer1.history", "[]")
            hist = json.loads(raw_hist)
            assert isinstance(hist, list)
            assert len(hist) >= 2
            assert any(str(ev.get("key", "")) == "pc24.flag" for ev in hist if isinstance(ev, dict))
            assert sent == [
                    (
                        "PEER1",
                        WirePcFrame("PC51", ["WB3FFV-2", app.config.node.node_call, "0", ""]),
                    )
                ]
        finally:
            await app.store.close()

    asyncio.run(run())


def test_ingest_pc61_pushes_to_telnet_sessions(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "ingest_pc61_push.db")
        app = ClusterApp(_mk_config(db))
        w1 = _DummyWriter()
        w2 = _DummyWriter()
        try:
            s1 = Session(call="N0CALL", writer=w1, connected_at=datetime.now(timezone.utc))
            s2 = Session(call="K1ABC", writer=w2, connected_at=datetime.now(timezone.utc))
            s2.vars["dx"] = "off"
            app.telnet._sessions[1] = s1
            app.telnet._sessions[2] = s2

            msg = Pc61Message.from_fields(
                ["14074.0", "W1AW", "1-Mar-2026", "0000Z", "FT8", "N9XYZ", "N2WQ-1", "127.0.0.1", "H1", "~"]
            )
            frame = WirePcFrame("PC61", msg.to_fields())
            await app._handle_node_link_item("PEER1", frame, msg)

            assert b"W1AW" in bytes(w1.buffer)
            assert b"W1AW" not in bytes(w2.buffer)
        finally:
            await app.store.close()

    asyncio.run(run())


def test_ingest_pc93_adds_chat_bulletin(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "ingest_pc93.db")
        app = ClusterApp(_mk_config(db))
        try:
            msg = Pc93Message.from_fields(
                ["N0NODE-1", "0", "*", "W1AW", "*", "hello from link", "", "127.0.0.1", "H1", ""]
            )
            frame = WirePcFrame("PC93", msg.to_fields())
            await app._handle_node_link_item("PEER2", frame, msg)
            rows = await app.store.list_bulletins("chat", limit=5)
            assert len(rows) == 1
            assert rows[0]["sender"] == "W1AW"
            assert "hello from link" in str(rows[0]["body"])
        finally:
            await app.store.close()

    asyncio.run(run())


def test_ingest_pc10_delivers_talk_without_creating_mail(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "ingest_pc10.db")
        app = ClusterApp(_mk_config(db))
        writer = _DummyWriter()
        try:
            app.telnet._sessions[1] = Session(
                call="AI3I",
                writer=writer,
                connected_at=datetime.now(timezone.utc),
            )
            msg = Pc10Message.from_fields(
                ["N0CALL", "AI3I-15", "hello from cluster mail", "*", "AI3I", "AI3I-16", "~"]
            )
            frame = WirePcFrame("PC10", msg.to_fields())
            await app._handle_node_link_item("PEER2", frame, msg)
            rows = await app.store.list_messages("AI3I", limit=5)
            assert rows == []
            assert b"TALK N0CALL: hello from cluster mail" in bytes(writer.buffer)
        finally:
            await app.store.close()

    asyncio.run(run())


def test_connect_peer_starts_pending_mail_with_pc28(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "flush_pc10.db")
        app = ClusterApp(_mk_config(db))
        sent: list[tuple[str, WirePcFrame]] = []
        try:
            async def _connect(name: str, dsn: str, profile: str = "spider") -> None:
                return

            async def _send(peer: str, frame: WirePcFrame) -> None:
                sent.append((peer, frame))

            app.node_link.connect_dsn = _connect  # type: ignore[method-assign]
            app.node_link.send = _send  # type: ignore[method-assign]
            now = int(datetime.now(timezone.utc).timestamp())
            msg_id = await app.store.add_message(
                sender="N0CALL",
                recipient="K1ABC",
                epoch=now,
                body="queued hello",
                origin_node="AI3I-15",
                route_node="PEER1",
                delivery_state="pending",
            )

            await app.connect_peer("PEER1", "spider://peer1")

            pc28 = [frame for peer, frame in sent if peer == "PEER1" and frame.pc_type == "PC28"]
            assert len(pc28) == 1
            assert pc28[0].payload_fields[0] == "PEER1"
            assert pc28[0].payload_fields[2] == "K1ABC"
            assert pc28[0].payload_fields[3] == "N0CALL"
            row = await app.store.get_message(msg_id)
            assert row is not None
            assert row["delivery_state"] == "pending"
            assert row["route_node"] == "PEER1"
        finally:
            await app.store.close()

    asyncio.run(run())


def test_pc30_pc31_pc33_complete_outbound_mail_route(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "mail_ack.db")
        app = ClusterApp(_mk_config(db))
        sent: list[tuple[str, WirePcFrame]] = []
        try:
            async def _send(peer: str, frame: WirePcFrame) -> None:
                sent.append((peer, frame))

            app.node_link.send = _send  # type: ignore[method-assign]
            now = int(datetime.now(timezone.utc).timestamp())
            msg_id = await app.store.add_message(
                sender="N0CALL",
                recipient="K1ABC",
                epoch=now,
                body="\n".join([f"line {n}" for n in range(1, 7)]),
                origin_node="AI3I-15",
                route_node="PEER1",
                delivery_state="pending",
            )

            row = await app.store.get_message(msg_id)
            assert row is not None
            await app._start_outbound_mail("PEER1", row)
            pc28 = sent[-1][1]
            assert pc28.pc_type == "PC28"
            stream = "42"

            await app._handle_node_link_item("PEER1", WirePcFrame("PC30", ["N0NODE-1", "PEER1", stream, ""]), None)
            tranche = [frame for peer, frame in sent if peer == "PEER1" and frame.pc_type == "PC29"]
            assert len(tranche) == 5
            assert tranche[0].payload_fields[3] == "line 1"
            assert tranche[-1].payload_fields[3] == "line 5"
            await app._handle_node_link_item("PEER1", WirePcFrame("PC31", ["N0NODE-1", "PEER1", stream, ""]), None)
            assert sent[-2][1].pc_type == "PC29"
            assert sent[-2][1].payload_fields[3] == "line 6"
            assert sent[-1][1].pc_type == "PC32"
            await app._handle_node_link_item("PEER1", WirePcFrame("PC33", ["N0NODE-1", "PEER1", stream, ""]), None)

            routed = await app.store.get_message(msg_id)
            assert routed is not None
            assert routed["delivery_state"] == "routed"
            assert routed["route_node"] == "PEER1"
        finally:
            await app.store.close()

    asyncio.run(run())


def test_pc28_pc29_pc32_store_inbound_mail(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "mail_inbound.db")
        app = ClusterApp(_mk_config(db))
        sent: list[tuple[str, WirePcFrame]] = []
        writer = _DummyWriter()
        try:
            async def _send(peer: str, frame: WirePcFrame) -> None:
                sent.append((peer, frame))

            app.node_link.send = _send  # type: ignore[method-assign]
            app.telnet._sessions[1] = Session(
                call="AI3I",
                writer=writer,
                connected_at=datetime.now(timezone.utc),
            )

            await app._handle_node_link_item(
                "AI3I-16",
                WirePcFrame("PC28", ["N0NODE-1", "AI3I-16", "AI3I", "N0CALL", "30-Mar-2026", "0010Z", "1", "Test Subject", " ", "5", "0", " ", "AI3I-16", "~"]),
                None,
            )
            assert sent[-1][1].pc_type == "PC30"
            stream = sent[-1][1].payload_fields[2]

            for idx in range(1, 5):
                await app._handle_node_link_item(
                    "AI3I-16",
                    WirePcFrame("PC29", ["N0NODE-1", "AI3I-16", stream, f"mail line {idx}", "~"]),
                    None,
                )
                assert sent[-1][1].pc_type == "PC30"
            await app._handle_node_link_item("AI3I-16", WirePcFrame("PC29", ["N0NODE-1", "AI3I-16", stream, "mail line 5", "~"]), None)
            assert sent[-1][1].pc_type == "PC31"
            await app._handle_node_link_item("AI3I-16", WirePcFrame("PC32", ["N0NODE-1", "AI3I-16", stream, ""]), None)
            assert sent[-1][1].pc_type == "PC33"

            rows = await app.store.list_messages("AI3I", limit=5)
            assert len(rows) == 1
            assert rows[0]["sender"] == "N0CALL"
            assert "Subject: Test Subject" in str(rows[0]["body"])
            assert "mail line 1" in str(rows[0]["body"])
            assert "mail line 5" in str(rows[0]["body"])
            assert b"MSG#" in bytes(writer.buffer)
        finally:
            await app.store.close()

    asyncio.run(run())


def test_connect_peer_resets_stale_mail_state_and_restarts_pending_route(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "mail_reset.db")
        app = ClusterApp(_mk_config(db))
        sent: list[tuple[str, WirePcFrame]] = []
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            msg_id = await app.store.add_message(
                sender="N0CALL",
                recipient="K1ABC",
                epoch=now,
                body="retry after reconnect",
                parent_id=None,
                origin_node=app.config.node.node_call,
                route_node="PEER1",
                delivery_state="pending",
            )
            app._outbound_mail[("PEER1", "1")] = {
                "message_id": msg_id,
                "peer": "PEER1",
                "sender": "N0CALL",
                "recipient": "K1ABC",
                "subject": " ",
                "lines": ["stale"],
                "index": 0,
                "stream": "1",
            }
            app._outbound_mail_pending_header["PEER1"] = [
                {
                    "message_id": msg_id,
                    "peer": "PEER1",
                    "sender": "N0CALL",
                    "recipient": "K1ABC",
                    "subject": " ",
                    "lines": ["stale"],
                    "index": 0,
                }
            ]

            async def _connect(_name: str, _dsn: str, profile: str = "dxspider") -> None:
                return

            async def _send(peer: str, frame: WirePcFrame) -> None:
                sent.append((peer, frame))

            app.node_link.connect_dsn = _connect  # type: ignore[method-assign]
            app.node_link.send = _send  # type: ignore[method-assign]

            await app.connect_peer("PEER1", "tcp://example.invalid:7300", persist=False)

            assert ("PEER1", "1") not in app._outbound_mail
            assert len(app._outbound_mail_pending_header.get("PEER1", [])) == 1
            pc28 = [frame for peer, frame in sent if peer == "PEER1" and frame.pc_type == "PC28"]
            assert len(pc28) == 1
            row = await app.store.get_message(msg_id)
            assert row is not None
            assert row["delivery_state"] == "pending"
        finally:
            await app.store.close()

    asyncio.run(run())


def test_relay_message_marks_undeliverable_without_known_peer_route(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "mail_undeliverable.db")
        app = ClusterApp(_mk_config(db))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            msg_id = await app.store.add_message(
                sender="N0CALL",
                recipient="K1ABC",
                epoch=now,
                body="no route for this peer",
                origin_node=app.config.node.node_call,
                route_node="PEER404",
                delivery_state="pending",
            )

            await app._relay_message_to_links("N0CALL", "K1ABC", "no route for this peer", msg_id, None)

            row = await app.store.get_message(msg_id)
            assert row is not None
            assert row["delivery_state"] == "undeliverable"
            assert row["route_node"] == "PEER404"
            assert row["error_text"] == "No configured route to that peer."
        finally:
            await app.store.close()

    asyncio.run(run())


def test_relay_message_marks_pending_with_reconnect_context_for_down_peer(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "mail_pending_peer_down.db")
        app = ClusterApp(_mk_config(db))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await app.store.set_user_pref(app.config.node.node_call, "peer.outbound.peer1.name", "PEER1", now)
            await app.store.set_user_pref(app.config.node.node_call, "peer.outbound.peer1.dsn", "tcp://example.invalid:7300", now)
            await app.store.set_user_pref(app.config.node.node_call, "peer.outbound.peer1.reconnect", "on", now)
            msg_id = await app.store.add_message(
                sender="N0CALL",
                recipient="K1ABC",
                epoch=now,
                body="route should wait for reconnect",
                origin_node=app.config.node.node_call,
                route_node="PEER1",
                delivery_state="pending",
            )

            await app._relay_message_to_links("N0CALL", "K1ABC", "route should wait for reconnect", msg_id, None)

            row = await app.store.get_message(msg_id)
            assert row is not None
            assert row["delivery_state"] == "pending"
            assert row["error_text"] == "Peer is disconnected and queued for reconnect."

            await app.store.set_user_pref(app.config.node.node_call, "peer.outbound.peer1.reconnect", "off", now)
            await app._relay_message_to_links("N0CALL", "K1ABC", "route should wait for reconnect", msg_id, None)
            row = await app.store.get_message(msg_id)
            assert row is not None
            assert row["delivery_state"] == "pending"
            assert row["error_text"] == "Peer is disconnected and reconnect is disabled."
        finally:
            await app.store.close()

    asyncio.run(run())


def test_ingest_pc28_drops_looped_mail_origin(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "mail_loop_drop.db")
        app = ClusterApp(_mk_config(db))
        try:
            app.node_link._peers["PEER1"] = LinkPeer(name="PEER1", conn=_DummyConn(), inbound=False)
            await app._handle_node_link_item(
                "PEER1",
                WirePcFrame("PC28", [app.config.node.node_call, "PEER1", "AI3I", "N0CALL", "30-Mar-2026", "0010Z", "1", "Looped", " ", "1", "0", " ", app.config.node.node_call, "~"]),
                None,
            )

            rows = await app.store.list_messages("AI3I", limit=5)
            assert rows == []
            stats = await app.node_link.stats()
            assert stats["PEER1"]["policy_dropped"] >= 1
            assert stats["PEER1"]["policy_reasons"]["ingest_pc28_loop"] >= 1
        finally:
            await app.store.close()

    asyncio.run(run())


def test_ingest_duplicate_cluster_mail_is_suppressed(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "mail_duplicate.db")
        app = ClusterApp(_mk_config(db))
        sent: list[tuple[str, WirePcFrame]] = []
        try:
            async def _send(peer: str, frame: WirePcFrame) -> None:
                sent.append((peer, frame))

            app.node_link.send = _send  # type: ignore[method-assign]

            await app._handle_node_link_item(
                "AI3I-16",
                WirePcFrame("PC28", ["N0NODE-1", "AI3I-16", "AI3I", "N0CALL", "30-Mar-2026", "0010Z", "1", "Dup Subject", " ", "2", "0", " ", "AI3I-16", "~"]),
                None,
            )
            stream = sent[-1][1].payload_fields[2]
            await app._handle_node_link_item("AI3I-16", WirePcFrame("PC29", ["N0NODE-1", "AI3I-16", stream, "mail line 1", "~"]), None)
            await app._handle_node_link_item("AI3I-16", WirePcFrame("PC29", ["N0NODE-1", "AI3I-16", stream, "mail line 2", "~"]), None)
            await app._handle_node_link_item("AI3I-16", WirePcFrame("PC32", ["N0NODE-1", "AI3I-16", stream, ""]), None)

            first_rows = await app.store.list_messages("AI3I", limit=5)
            assert len(first_rows) == 1

            await app._handle_node_link_item(
                "AI3I-16",
                WirePcFrame("PC28", ["N0NODE-1", "AI3I-16", "AI3I", "N0CALL", "30-Mar-2026", "0011Z", "1", "Dup Subject", " ", "2", "0", " ", "AI3I-16", "~"]),
                None,
            )
            stream2 = sent[-1][1].payload_fields[2]
            await app._handle_node_link_item("AI3I-16", WirePcFrame("PC29", ["N0NODE-1", "AI3I-16", stream2, "mail line 1", "~"]), None)
            await app._handle_node_link_item("AI3I-16", WirePcFrame("PC29", ["N0NODE-1", "AI3I-16", stream2, "mail line 2", "~"]), None)
            await app._handle_node_link_item("AI3I-16", WirePcFrame("PC32", ["N0NODE-1", "AI3I-16", stream2, ""]), None)

            rows = await app.store.list_messages("AI3I", limit=5)
            assert len(rows) == 1
        finally:
            await app.store.close()

    asyncio.run(run())


def test_ingest_pc93_pushes_chat_to_telnet_sessions(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "ingest_pc93_push.db")
        app = ClusterApp(_mk_config(db))
        w1 = _DummyWriter()
        w2 = _DummyWriter()
        try:
            app.telnet._sessions[1] = Session(call="W1AW", writer=w1, connected_at=datetime.now(timezone.utc))
            app.telnet._sessions[2] = Session(call="N0CALL", writer=w2, connected_at=datetime.now(timezone.utc))

            msg = Pc93Message.from_fields(
                ["N0NODE-1", "0", "*", "W1AW", "*", "hello from link", "", "127.0.0.1", "H1", ""]
            )
            frame = WirePcFrame("PC93", msg.to_fields())
            await app._handle_node_link_item("PEER2", frame, msg)

            assert b"CHAT W1AW: hello from link" in bytes(w2.buffer)
            assert b"CHAT W1AW: hello from link" not in bytes(w1.buffer)
        finally:
            await app.store.close()

    asyncio.run(run())


def test_ingest_pc93_respects_category_policy_and_empty_body(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "ingest_pc93_policy.db")
        app = ClusterApp(_mk_config(db))
        try:
            app.node_link._peers["PEER2"] = LinkPeer(name="PEER2", conn=_DummyConn(), inbound=False)
            now = int(datetime.now(timezone.utc).timestamp())
            await app.store.set_user_pref(app.config.node.node_call, "ingest.peer.peer2.wcy", "off", now)

            blocked = Pc93Message.from_fields(
                ["N0NODE-1", "0", "*", "W1AW", "*", "[WCY/LOCAL] A=8 K=2", "", "127.0.0.1", "H1", ""]
            )
            await app._handle_node_link_item("PEER2", WirePcFrame("PC93", blocked.to_fields()), blocked)

            empty = Pc93Message.from_fields(
                ["N0NODE-1", "0", "*", "W1AW", "*", "   ", "", "127.0.0.1", "H1", ""]
            )
            await app._handle_node_link_item("PEER2", WirePcFrame("PC93", empty.to_fields()), empty)

            stats = await app.node_link.stats()
            assert stats["PEER2"]["policy_dropped"] == 2
            assert stats["PEER2"]["policy_reasons"]["ingest_wcy_disabled"] == 1
            assert stats["PEER2"]["policy_reasons"]["ingest_pc93_empty"] == 1
            assert await app.store.list_bulletins("wcy", limit=5) == []
        finally:
            await app.store.close()

    asyncio.run(run())


def test_ingest_pc93_prefixed_wcy_maps_to_wcy_bulletin(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "ingest_pc93_wcy.db")
        app = ClusterApp(_mk_config(db))
        try:
            msg = Pc93Message.from_fields(
                ["N0NODE-1", "0", "*", "W1AW", "*", "[WCY/LOCAL] A=8 K=2 [via:PEER2]", "", "127.0.0.1", "H1", ""]
            )
            frame = WirePcFrame("PC93", msg.to_fields())
            await app._handle_node_link_item("PEER2", frame, msg)
            rows = await app.store.list_bulletins("wcy", limit=5)
            assert len(rows) == 1
            assert rows[0]["sender"] == "W1AW"
            assert rows[0]["scope"] == "LOCAL"
            assert "A=8 K=2" in str(rows[0]["body"])
        finally:
            await app.store.close()

    asyncio.run(run())


def test_ingest_pc93_prefixed_announce_maps_to_announce_bulletin(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "ingest_pc93_announce.db")
        app = ClusterApp(_mk_config(db))
        try:
            msg = Pc93Message.from_fields(
                ["N0NODE-1", "0", "*", "W1AW", "*", "[ANNOUNCE/FULL] net tonight [via:PEER2]", "", "127.0.0.1", "H1", ""]
            )
            frame = WirePcFrame("PC93", msg.to_fields())
            await app._handle_node_link_item("PEER2", frame, msg)
            rows = await app.store.list_bulletins("announce", limit=5)
            assert len(rows) == 1
            assert rows[0]["scope"] == "FULL"
            assert "net tonight" in str(rows[0]["body"])
        finally:
            await app.store.close()

    asyncio.run(run())


def test_ingest_pc93_suppresses_recent_duplicate_bulletin(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "ingest_pc93_duplicate.db")
        app = ClusterApp(_mk_config(db))
        try:
            app.node_link._peers["PEER2"] = LinkPeer(name="PEER2", conn=_DummyConn(), inbound=False)
            now = int(datetime.now(timezone.utc).timestamp())
            await app.store.add_bulletin("announce", "W1AW", "FULL", now, "net tonight")
            msg = Pc93Message.from_fields(
                ["N0NODE-1", "0", "*", "W1AW", "*", "[ANNOUNCE/FULL] net tonight [via:PEER2]", "", "127.0.0.1", "H1", ""]
            )
            await app._handle_node_link_item("PEER2", WirePcFrame("PC93", msg.to_fields()), msg)
            rows = await app.store.list_bulletins("announce", limit=5)
            assert len(rows) == 1
            stats = await app.node_link.stats()
            assert stats["PEER2"]["policy_reasons"]["ingest_pc93_duplicate"] == 1
        finally:
            await app.store.close()

    asyncio.run(run())


def test_ingest_pc93_suppresses_recent_duplicate_chat(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "ingest_pc93_duplicate_chat.db")
        app = ClusterApp(_mk_config(db))
        try:
            app.node_link._peers["PEER2"] = LinkPeer(name="PEER2", conn=_DummyConn(), inbound=False)
            now = int(datetime.now(timezone.utc).timestamp())
            await app.store.add_bulletin("chat", "W1AW", "LOCAL", now, "hello dupes")
            msg = Pc93Message.from_fields(
                ["N0NODE-1", "0", "*", "W1AW", "*", "hello dupes [via:PEER2]", "", "127.0.0.1", "H1", ""]
            )
            await app._handle_node_link_item("PEER2", WirePcFrame("PC93", msg.to_fields()), msg)
            rows = await app.store.list_bulletins("chat", limit=5)
            assert len(rows) == 1
            stats = await app.node_link.stats()
            assert stats["PEER2"]["policy_reasons"]["ingest_pc93_duplicate"] == 1
        finally:
            await app.store.close()

    asyncio.run(run())


def test_ingest_pc12_maps_to_announce_bulletin(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "ingest_pc12_announce.db")
        app = ClusterApp(_mk_config(db))
        try:
            msg = Pc12Message.from_fields(
                ["AI3I", "*", "reverse announce", " ", "AI3I-15", "0", "H30", "~"]
            )
            frame = WirePcFrame("PC12", msg.to_fields())
            await app._handle_node_link_item("PEER2", frame, msg)
            rows = await app.store.list_bulletins("announce", limit=5)
            assert len(rows) == 1
            assert rows[0]["sender"] == "AI3I"
            assert rows[0]["scope"] == "FULL"
            assert "reverse announce" in str(rows[0]["body"])
        finally:
            await app.store.close()

    asyncio.run(run())


def test_ingest_pc12_suppresses_recent_duplicate_bulletin(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "ingest_pc12_duplicate.db")
        app = ClusterApp(_mk_config(db))
        try:
            app.node_link._peers["PEER2"] = LinkPeer(name="PEER2", conn=_DummyConn(), inbound=False)
            now = int(datetime.now(timezone.utc).timestamp())
            await app.store.add_bulletin("announce", "AI3I", "FULL", now, "reverse announce")
            msg = Pc12Message.from_fields(
                ["AI3I", "*", "reverse announce", " ", "AI3I-15", "0", "H30", "~"]
            )
            await app._handle_node_link_item("PEER2", WirePcFrame("PC12", msg.to_fields()), msg)
            rows = await app.store.list_bulletins("announce", limit=5)
            assert len(rows) == 1
            stats = await app.node_link.stats()
            assert stats["PEER2"]["policy_reasons"]["ingest_pc12_duplicate"] == 1
        finally:
            await app.store.close()

    asyncio.run(run())


def test_ingest_pc12_wx_maps_to_wx_bulletin(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "ingest_pc12_wx.db")
        app = ClusterApp(_mk_config(db))
        try:
            msg = Pc12Message.from_fields(
                ["AI3I", "*", "weather update", " ", "AI3I-15", "1", "H30", "~"]
            )
            frame = WirePcFrame("PC12", msg.to_fields())
            await app._handle_node_link_item("PEER2", frame, msg)
            rows = await app.store.list_bulletins("wx", limit=5)
            assert len(rows) == 1
            assert rows[0]["sender"] == "AI3I"
            assert rows[0]["scope"] == "FULL"
            assert "weather update" in str(rows[0]["body"])
        finally:
            await app.store.close()

    asyncio.run(run())


def test_ingest_pc23_maps_to_wwv_bulletin(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "ingest_pc23_wwv.db")
        app = ClusterApp(_mk_config(db))
        try:
            msg = Pc23Message.from_fields(
                ["14-Mar-2026", "18", "120", "24", "4", "Moderate w/G2 -> Minor w/G1", "W0MU", "AI3I-16", "H96", ""]
            )
            await app._handle_node_link_item("PEER2", WirePcFrame("PC23", msg.to_fields()), msg)
            rows = await app.store.list_bulletins("wwv", limit=5)
            assert len(rows) == 1
            assert rows[0]["sender"] == "W0MU"
            assert "SFI=120 A=24 K=4 Moderate w/G2 -> Minor w/G1" == str(rows[0]["body"])
        finally:
            await app.store.close()

    asyncio.run(run())


def test_ingest_pc73_maps_to_wcy_bulletin(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "ingest_pc73_wcy.db")
        app = ClusterApp(_mk_config(db))
        try:
            msg = Pc73Message.from_fields(
                ["14-Mar-2026", "20", "120", "18", "3", "2", "105", "qui", "maj", "no", "DK0WCY", "AI3I-16", "H96", ""]
            )
            await app._handle_node_link_item("PEER2", WirePcFrame("PC73", msg.to_fields()), msg)
            rows = await app.store.list_bulletins("wcy", limit=5)
            assert len(rows) == 1
            assert rows[0]["sender"] == "DK0WCY"
            assert "SFI=120 A=18 K=3 ExpK=2 R=105 SA=qui GMF=maj Aurora=no" == str(rows[0]["body"])
        finally:
            await app.store.close()

    asyncio.run(run())


def test_outbound_relay_defaults_on_for_local_session_and_can_be_disabled(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "relay_gate.db")
        app = ClusterApp(_mk_config(db))
        captured = []

        async def _peer_names():
            return ["peer1"]

        async def _send(_peer, frame):
            captured.append(frame)

        app.node_link.peer_names = _peer_names  # type: ignore[method-assign]
        app.node_link.send = _send  # type: ignore[method-assign]
        app.telnet._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            await app.telnet._execute_command("N0CALL", "chat hello")
            assert len(captured) == 1
            assert captured[0].pc_type == "PC93"
            assert "hello" in "^".join(captured[0].payload_fields)
            assert "via:" in "^".join(captured[0].payload_fields)

            await app.telnet._execute_command("N0CALL", "unset/routepc19")
            await app.telnet._execute_command("N0CALL", "chat hello2")
            assert len(captured) == 1
        finally:
            await app.store.close()

    asyncio.run(run())


def test_outbound_bulletin_relay_with_category_prefix(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "relay_bulletin.db")
        app = ClusterApp(_mk_config(db))
        captured = []

        async def _peer_names():
            return ["peer1"]

        async def _send(_peer, frame):
            captured.append(frame)

        app.node_link.peer_names = _peer_names  # type: ignore[method-assign]
        app.node_link.send = _send  # type: ignore[method-assign]
        app.telnet._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await app.store.upsert_user_registry("N0CALL", now, privilege="user")
            await app.telnet._execute_command("N0CALL", "set/routepc19")
            await app.telnet._execute_command("N0CALL", "announce full test relay")
            await app.telnet._execute_command("N0CALL", "wcy A=8 K=2")
            texts = ["^".join(f.payload_fields) for f in captured]
            assert any("[ANNOUNCE/FULL] test relay" in t for t in texts)
            assert any("[WCY/LOCAL] A=8 K=2" in t for t in texts)
            assert all("via:" in t for t in texts)
        finally:
            await app.store.close()

    asyncio.run(run())


def test_outbound_wcy_and_wwv_use_dxspider_frames_for_dxspider_peers(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "relay_dxspider_geomag.db")
        app = ClusterApp(_mk_config(db))
        captured = []

        async def _peer_names():
            return ["peer1"]

        async def _send(_peer, frame):
            captured.append(frame)

        async def _stats():
            return {"peer1": {"profile": "dxspider"}}

        app.node_link.peer_names = _peer_names  # type: ignore[method-assign]
        app.node_link.send = _send  # type: ignore[method-assign]
        app.node_link.stats = _stats  # type: ignore[method-assign]
        app.telnet._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            await app.telnet._execute_command("N0CALL", "set/routepc19")
            await app.telnet._execute_command("N0CALL", "wcy k=3,expk=2,a=18,r=105,sf=120,sa=qui,gmf=maj,au=no")
            await app.telnet._execute_command("N0CALL", "wwv sf=120,a=24,k=4,Moderate w/G2 -> Minor w/G1")
            assert [frame.pc_type for frame in captured] == ["PC73", "PC23"]
        finally:
            await app.store.close()

    asyncio.run(run())


def test_legacy_pc16_sync_tracks_active_calls(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "legacy_pc16_sync.db")
        app = ClusterApp(_mk_config(db))
        captured = []

        async def _peer_names():
            return ["peer1"]

        async def _send(_peer, frame):
            captured.append(frame)

        app.node_link.peer_names = _peer_names  # type: ignore[method-assign]
        app.node_link.send = _send  # type: ignore[method-assign]
        app._legacy_dxspider_peers.add("peer1")
        app.telnet._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        app.telnet._sessions[2] = Session(call="K1ABC", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await app.store.set_user_pref("K1ABC", "routepc19", "off", now)
            await app._sync_legacy_user_roster()
            assert len(captured) == 1
            assert captured[0].pc_type == "PC16"
            payload = captured[0].payload_fields
            assert payload[0] == app.config.node.node_call.upper()
            assert "N0CALL - 1" in payload
            assert "K1ABC - 1" not in payload
        finally:
            await app.store.close()

    asyncio.run(run())


def test_legacy_pc16_sync_drops_dead_peer_without_traceback(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "legacy_pc16_dead_peer.db")
        app = ClusterApp(_mk_config(db))
        try:
            async def _peer_names():
                return ["peer1"]

            async def _send(_peer, _frame):
                raise ConnectionResetError("Connection lost")

            app.node_link.peer_names = _peer_names  # type: ignore[method-assign]
            app.node_link.send = _send  # type: ignore[method-assign]
            app._legacy_dxspider_peers.add("peer1")

            await app._sync_legacy_user_roster()

            assert "peer1" not in app._legacy_dxspider_peers
        finally:
            await app.store.close()

    asyncio.run(run())


def test_dxspider_heartbeat_sends_pc20_and_ignores_dead_peers(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "dxspider_heartbeat.db")
        app = ClusterApp(_mk_config(db))
        sent = []
        try:
            async def _stats():
                return {
                    "peer1": {"profile": "dxspider", "inbound": False},
                    "peer2": {"profile": "arcluster", "inbound": False},
                    "peer3": {"profile": "dxspider", "inbound": False},
                    "peer4": {"profile": "dxspider", "inbound": True},
                }

            async def _send(peer, frame):
                if peer == "peer3":
                    raise ConnectionResetError("Connection lost")
                sent.append((peer, frame.pc_type, list(frame.payload_fields)))

            app.node_link.stats = _stats  # type: ignore[method-assign]
            app.node_link.send = _send  # type: ignore[method-assign]

            count = await app.heartbeat_once()

            assert count == 2
            assert sent == [("peer1", "PC20", [""]), ("peer4", "PC20", [""])]
        finally:
            await app.store.close()

    asyncio.run(run())


def test_legacy_dxspider_peer_uses_pc11_for_spot_relay(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "legacy_pc11_spot.db")
        app = ClusterApp(_mk_config(db))
        captured = []

        async def _peer_names():
            return ["peer1", "peer2"]

        async def _send(peer, frame):
            captured.append((peer, frame))

        app.node_link.peer_names = _peer_names  # type: ignore[method-assign]
        app.node_link.send = _send  # type: ignore[method-assign]
        app._legacy_dxspider_peers.add("peer1")
        try:
            spot = Spot(
                freq_khz=14074.0,
                dx_call="N0TST",
                epoch=int(datetime.now(timezone.utc).timestamp()),
                info="relay test",
                spotter="N0CALL",
                source_node=app.config.node.node_call,
                raw="",
            )
            await app._relay_spot_to_links(spot)
            sent = {peer: frame.pc_type for peer, frame in captured}
            assert sent["peer1"] == "PC11"
            assert sent["peer2"] == "PC61"
        finally:
            await app.store.close()

    asyncio.run(run())


def test_inbound_pc61_spot_relays_to_other_peers_but_not_origin(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "inbound_pc61_relay.db")
        app = ClusterApp(_mk_config(db))
        captured = []
        try:
            async def _peer_names():
                return ["PEER1", "PEER2"]

            async def _send(peer, frame):
                captured.append((peer, frame))

            app.node_link.peer_names = _peer_names  # type: ignore[method-assign]
            app.node_link.send = _send  # type: ignore[method-assign]
            app._legacy_dxspider_peers.add("PEER2")

            msg = Pc61Message(
                freq_khz="14074.0",
                dx_call="W1AW",
                date_token="28-Mar-2026",
                time_token="2046Z",
                info="FT8",
                spotter="N9XYZ",
                source_node="REMOTE1",
                ip="127.0.0.1",
                hops_token="H1",
                trailer="~",
            )
            await app._handle_node_link_item("PEER1", WirePcFrame("PC61", msg.to_fields()), msg)

            assert [peer for peer, _ in captured] == ["PEER2"]
            frame = captured[0][1]
            assert frame.pc_type == "PC11"
            assert frame.payload_fields[1] == "W1AW"
            assert frame.payload_fields[5] == "N9XYZ"
            assert frame.payload_fields[6] == "REMOTE1"
        finally:
            await app.store.close()

    asyncio.run(run())



def test_outbound_relay_category_policy(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "relay_category.db")
        app = ClusterApp(_mk_config(db))
        captured = []

        async def _peer_names():
            return ["peer1"]

        async def _send(_peer, frame):
            captured.append(frame)

        app.node_link.peer_names = _peer_names  # type: ignore[method-assign]
        app.node_link.send = _send  # type: ignore[method-assign]
        app.telnet._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await app.store.set_user_pref("N0CALL", "routepc19", "on", now)
            await app.store.set_user_pref("N0CALL", "relay.chat", "off", now)
            await app.store.set_user_pref("N0CALL", "relay.wcy", "off", now)

            await app.telnet._execute_command("N0CALL", "chat no-relay")
            await app.telnet._execute_command("N0CALL", "wcy A=4 K=1")
            await app.telnet._execute_command("N0CALL", "wwv flux=120")

            texts = ["^".join(f.payload_fields) for f in captured]
            assert not any("no-relay" in t for t in texts)
            assert not any("[WCY/LOCAL] A=4 K=1" in t for t in texts)
            assert any("[WWV/LOCAL] flux=120" in t for t in texts)
        finally:
            await app.store.close()

    asyncio.run(run())


def test_inbound_pc93_loop_tag_is_ignored(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "loop_tag.db")
        cfg = _mk_config(db)
        cfg.node.node_call = "N0NODE-1"
        app = ClusterApp(cfg)
        try:
            msg = Pc93Message.from_fields(
                ["N0NODE-1", "0", "*", "W1AW", "*", "hello [via:N0NODE-1]", "", "127.0.0.1", "H1", ""]
            )
            frame = WirePcFrame("PC93", msg.to_fields())
            await app._handle_node_link_item("PEER2", frame, msg)
            rows = await app.store.list_bulletins("chat", limit=5)
            assert rows == []
        finally:
            await app.store.close()

    asyncio.run(run())


def test_outbound_spot_relay_respects_policy(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "relay_spot.db")
        app = ClusterApp(_mk_config(db))
        captured = []

        async def _peer_names():
            return ["peer1"]

        async def _send(_peer, frame):
            captured.append(frame)

        app.node_link.peer_names = _peer_names  # type: ignore[method-assign]
        app.node_link.send = _send  # type: ignore[method-assign]
        app.telnet._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await app.store.upsert_user_registry("N0CALL", now, privilege="user")
            await app.telnet._execute_command("N0CALL", "dx 14074.0 K1ABC test1")
            assert any(f.pc_type == "PC61" for f in captured)

            await app.telnet._execute_command("N0CALL", "unset/routepc19")
            before = len(captured)
            await app.telnet._execute_command("N0CALL", "dx 14074.0 K1ABC test2")
            assert len(captured) == before

            await app.telnet._execute_command("N0CALL", "set/routepc19")
            await app.telnet._execute_command("N0CALL", "dx 14074.0 K1ABC test2b")
            assert len(captured) > before

            now = int(datetime.now(timezone.utc).timestamp())
            await app.store.set_user_pref("N0CALL", "relay.spots", "off", now)
            before = len(captured)
            await app.telnet._execute_command("N0CALL", "dx 14074.0 K1ABC test3")
            assert len(captured) == before
        finally:
            await app.store.close()

    asyncio.run(run())


def test_inbound_pc61_same_source_node_is_ignored(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "loop_pc61.db")
        cfg = _mk_config(db)
        cfg.node.node_call = "N0NODE-1"
        app = ClusterApp(cfg)
        try:
            msg = Pc61Message.from_fields(
                ["14074.0", "K1ABC", "1-Mar-2026", "0000Z", "FT8", "N0CALL", "N0NODE-1", "127.0.0.1", "H1", "~"]
            )
            frame = WirePcFrame("PC61", msg.to_fields())
            await app._handle_node_link_item("PEER1", frame, msg)
            assert await app.store.count_spots() == 0
        finally:
            await app.store.close()

    asyncio.run(run())


def test_peer_specific_relay_policy_blocks_selected_peer(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "relay_peer_policy.db")
        app = ClusterApp(_mk_config(db))
        sent = []

        async def _peer_names():
            return ["peer1", "peer2"]

        async def _send(peer, frame):
            sent.append((peer, frame))
        drops = []
        async def _mark_drop(peer, reason):
            drops.append((peer, reason))

        app.node_link.peer_names = _peer_names  # type: ignore[method-assign]
        app.node_link.send = _send  # type: ignore[method-assign]
        app.node_link.mark_policy_drop = _mark_drop  # type: ignore[method-assign]
        app.telnet._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await app.store.set_user_pref("N0CALL", "routepc19", "on", now)
            await app.store.set_user_pref("N0CALL", "relay.peer.peer2", "off", now)
            await app.telnet._execute_command("N0CALL", "chat relay-test")
            peers = [p for p, _ in sent]
            assert "peer1" in peers
            assert "peer2" not in peers
            assert ("peer2", "relay_peer_chat_disabled") in drops
        finally:
            await app.store.close()

    asyncio.run(run())


def test_peer_category_policy_only_blocks_specific_category(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "relay_peer_cat.db")
        app = ClusterApp(_mk_config(db))
        sent = []

        async def _peer_names():
            return ["peer1", "peer2"]

        async def _send(peer, frame):
            sent.append((peer, frame.pc_type, "^".join(frame.payload_fields)))

        app.node_link.peer_names = _peer_names  # type: ignore[method-assign]
        app.node_link.send = _send  # type: ignore[method-assign]
        app.telnet._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await app.store.upsert_user_registry("N0CALL", now, privilege="user")
            await app.store.set_user_pref("N0CALL", "routepc19", "on", now)
            await app.store.set_user_pref("N0CALL", "relay.peer.peer2.spots", "off", now)

            await app.telnet._execute_command("N0CALL", "dx 14074.0 K1ABC testcat")
            await app.telnet._execute_command("N0CALL", "chat testchat")

            spot_peers = [p for p, pc, _ in sent if pc == "PC61"]
            chat_peers = [p for p, pc, _ in sent if pc == "PC93"]
            assert "peer1" in spot_peers and "peer2" not in spot_peers
            assert "peer1" in chat_peers and "peer2" in chat_peers
        finally:
            await app.store.close()

    asyncio.run(run())


def test_route_accept_filter_limits_relay_peers(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "route_accept_filter.db")
        app = ClusterApp(_mk_config(db))
        sent = []

        async def _peer_names():
            return ["east-hub", "west-hub", "peer9"]

        async def _send(peer, frame):
            sent.append((peer, frame.pc_type))
        drops = []
        async def _mark_drop(peer, reason):
            drops.append((peer, reason))

        app.node_link.peer_names = _peer_names  # type: ignore[method-assign]
        app.node_link.send = _send  # type: ignore[method-assign]
        app.node_link.mark_policy_drop = _mark_drop  # type: ignore[method-assign]
        app.telnet._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await app.store.set_user_pref("N0CALL", "routepc19", "on", now)
            await app.store.set_filter_rule("N0CALL", "route", "accept", 1, "peer east*", now)
            await app.telnet._execute_command("N0CALL", "chat relay-test")
            peers = [p for p, _ in sent]
            assert "east-hub" in peers
            assert "west-hub" not in peers
            assert "peer9" not in peers
            assert ("west-hub", "route_filter") in drops
            assert ("peer9", "route_filter") in drops
        finally:
            await app.store.close()

    asyncio.run(run())


def test_route_reject_filter_blocks_matching_peers(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "route_reject_filter.db")
        app = ClusterApp(_mk_config(db))
        sent = []

        async def _peer_names():
            return ["peer1", "peer2", "core-peer"]

        async def _send(peer, frame):
            sent.append((peer, frame.pc_type))

        app.node_link.peer_names = _peer_names  # type: ignore[method-assign]
        app.node_link.send = _send  # type: ignore[method-assign]
        app.telnet._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await app.store.set_user_pref("N0CALL", "routepc19", "on", now)
            await app.store.set_filter_rule("N0CALL", "route", "reject", 1, "peer peer*", now)
            await app.telnet._execute_command("N0CALL", "chat relay-test")
            peers = [p for p, _ in sent]
            assert "core-peer" in peers
            assert "peer1" not in peers
            assert "peer2" not in peers
        finally:
            await app.store.close()

    asyncio.run(run())


def test_route_filter_slot_order_prefers_lowest_slot_match(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "route_slot_order.db")
        app = ClusterApp(_mk_config(db))
        sent = []

        async def _peer_names():
            return ["peer1", "peer2"]

        async def _send(peer, frame):
            sent.append((peer, frame.pc_type))

        app.node_link.peer_names = _peer_names  # type: ignore[method-assign]
        app.node_link.send = _send  # type: ignore[method-assign]
        app.telnet._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await app.store.set_user_pref("N0CALL", "routepc19", "on", now)
            await app.store.set_filter_rule("N0CALL", "route", "reject", 5, "peer peer*", now)
            await app.store.set_filter_rule("N0CALL", "route", "accept", 1, "peer peer2", now)
            await app.telnet._execute_command("N0CALL", "chat relay-test")
            peers = [p for p, _ in sent]
            assert "peer2" in peers
            assert "peer1" not in peers
        finally:
            await app.store.close()

    asyncio.run(run())


def test_ingest_peer_policy_blocks_spots_from_peer(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "ingest_peer_spot.db")
        cfg = _mk_config(db)
        cfg.node.node_call = "N0NODE-1"
        app = ClusterApp(cfg)
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await app.store.set_user_pref(cfg.node.node_call, "ingest.peer.peer1.spots", "off", now)
            msg = Pc61Message.from_fields(
                ["14074.0", "K1ABC", "1-Mar-2026", "0000Z", "FT8", "N0CALL", "PEER1NODE", "127.0.0.1", "H1", "~"]
            )
            frame = WirePcFrame("PC61", msg.to_fields())
            await app._handle_node_link_item("peer1", frame, msg)
            assert await app.store.count_spots() == 0
        finally:
            await app.store.close()

    asyncio.run(run())


def test_ingest_peer_policy_blocks_chat_from_peer(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "ingest_peer_chat.db")
        cfg = _mk_config(db)
        cfg.node.node_call = "N0NODE-1"
        app = ClusterApp(cfg)
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await app.store.set_user_pref(cfg.node.node_call, "ingest.peer.peer2.chat", "off", now)
            msg = Pc93Message.from_fields(
                ["N0NODE-1", "0", "*", "W1AW", "*", "blocked chat", "", "127.0.0.1", "H1", ""]
            )
            frame = WirePcFrame("PC93", msg.to_fields())
            await app._handle_node_link_item("peer2", frame, msg)
            rows = await app.store.list_bulletins("chat", limit=5)
            assert rows == []
        finally:
            await app.store.close()

    asyncio.run(run())


def test_ingest_peer_policy_blocks_prefixed_category_from_peer(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "ingest_peer_wcy.db")
        cfg = _mk_config(db)
        cfg.node.node_call = "N0NODE-1"
        app = ClusterApp(cfg)
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            await app.store.set_user_pref(cfg.node.node_call, "ingest.peer.peer2.wcy", "off", now)
            msg = Pc93Message.from_fields(
                ["N0NODE-1", "0", "*", "W1AW", "*", "[WCY/LOCAL] blocked wcy", "", "127.0.0.1", "H1", ""]
            )
            frame = WirePcFrame("PC93", msg.to_fields())
            await app._handle_node_link_item("peer2", frame, msg)
            rows = await app.store.list_bulletins("wcy", limit=5)
            assert rows == []
        finally:
            await app.store.close()

    asyncio.run(run())


def test_app_start_ingest_loop_processes_node_link_frames_end_to_end(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "app_ingest_loop_e2e.db")
        cfg = _mk_config(db)
        cfg.node.node_call = "N0NODE-1"
        app = ClusterApp(cfg)
        spot_writer = _DummyWriter()
        chat_writer = _DummyWriter()
        async def _noop() -> None:
            return
        try:
            app.telnet._sessions[1] = Session(call="N0CALL", writer=spot_writer, connected_at=datetime.now(timezone.utc))
            app.telnet._sessions[2] = Session(call="K1ABC", writer=chat_writer, connected_at=datetime.now(timezone.utc))
            app.node_link._peers["PEER1"] = LinkPeer(name="PEER1", conn=_DummyConn(), inbound=False)
            app.telnet.start = _noop  # type: ignore[method-assign]
            app.telnet.stop = _noop  # type: ignore[method-assign]
            app.web.start = _noop  # type: ignore[method-assign]
            app.web.stop = _noop  # type: ignore[method-assign]

            await app.start()

            spot_msg = Pc61Message.from_fields(
                ["14074.0", "W1AW", "1-Mar-2026", "0000Z", "FT8", "N9XYZ", "PEER1NODE", "127.0.0.1", "H1", "~"]
            )
            chat_msg = Pc93Message.from_fields(
                ["PEER1NODE", "0", "*", "W1AW", "*", "hello from background ingest", "", "127.0.0.1", "H1", ""]
            )
            app.node_link._frame_queue.put_nowait(("PEER1", WirePcFrame("PC61", spot_msg.to_fields()), spot_msg))
            app.node_link._frame_queue.put_nowait(("PEER1", WirePcFrame("PC93", chat_msg.to_fields()), chat_msg))

            await _wait_until(lambda: b"W1AW" in bytes(spot_writer.buffer) and b"CHAT W1AW: hello from background ingest" in bytes(chat_writer.buffer))

            assert await app.store.count_spots() == 1
            spots = await app.store.latest_spots(limit=1)
            assert spots[0]["dx_call"] == "W1AW"
            chats = await app.store.list_bulletins("chat", limit=5)
            assert len(chats) == 1
            assert "hello from background ingest" in str(chats[0]["body"])
        finally:
            await app.stop()

    asyncio.run(run())


def test_app_start_ingest_loop_records_policy_drops_for_blocked_frames(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "app_ingest_loop_policy.db")
        cfg = _mk_config(db)
        cfg.node.node_call = "N0NODE-1"
        app = ClusterApp(cfg)
        async def _noop() -> None:
            return
        try:
            app.node_link._peers["PEER2"] = LinkPeer(name="PEER2", conn=_DummyConn(), inbound=False)
            now = int(datetime.now(timezone.utc).timestamp())
            await app.store.set_user_pref(cfg.node.node_call, "ingest.peer.peer2.spots", "off", now)
            app.telnet.start = _noop  # type: ignore[method-assign]
            app.telnet.stop = _noop  # type: ignore[method-assign]
            app.web.start = _noop  # type: ignore[method-assign]
            app.web.stop = _noop  # type: ignore[method-assign]

            await app.start()

            blocked_spot = Pc61Message.from_fields(
                ["14074.0", "K1ABC", "1-Mar-2026", "0000Z", "FT8", "N0CALL", "PEER2NODE", "127.0.0.1", "H1", "~"]
            )
            empty_chat = Pc93Message.from_fields(
                ["PEER2NODE", "0", "*", "W1AW", "*", "   ", "", "127.0.0.1", "H1", ""]
            )
            app.node_link._frame_queue.put_nowait(("PEER2", WirePcFrame("PC61", blocked_spot.to_fields()), blocked_spot))
            app.node_link._frame_queue.put_nowait(("PEER2", WirePcFrame("PC93", empty_chat.to_fields()), empty_chat))

            async def _policy_ready() -> bool:
                stats = await app.node_link.stats()
                peer = stats.get("PEER2", {})
                reasons = peer.get("policy_reasons", {}) if isinstance(peer, dict) else {}
                return (
                    int(peer.get("policy_dropped", 0)) == 2
                    and reasons.get("ingest_spots_disabled") == 1
                    and reasons.get("ingest_pc93_empty") == 1
                )

            deadline = asyncio.get_running_loop().time() + 1.0
            while asyncio.get_running_loop().time() < deadline:
                if await _policy_ready():
                    break
                await asyncio.sleep(0.01)
            assert await _policy_ready()
            assert await app.store.count_spots() == 0
            assert await app.store.list_bulletins("chat", limit=5) == []
        finally:
            await app.stop()

    asyncio.run(run())


def test_app_node_link_listener_ingests_on_wire_pc61(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "app_wire_pc61.db")
        cfg = _mk_config(db)
        cfg.node.node_call = "N0NODE-1"
        app = ClusterApp(cfg)
        remote = NodeLinkEngine()
        writer = _DummyWriter()

        async def _noop() -> None:
            return

        try:
            try:
                await app.node_link.start_listener("127.0.0.1", 0)
            except OSError as exc:
                pytest.skip(f"socket bind not available in this environment: {exc}")

            port = app.node_link.listen_port()
            assert port is not None

            app.telnet._sessions[1] = Session(call="N0CALL", writer=writer, connected_at=datetime.now(timezone.utc))
            app.telnet.start = _noop  # type: ignore[method-assign]
            app.telnet.stop = _noop  # type: ignore[method-assign]
            app.web.start = _noop  # type: ignore[method-assign]
            app.web.stop = _noop  # type: ignore[method-assign]

            await app.start()
            await remote.connect("uplink", "127.0.0.1", port)
            await asyncio.sleep(0.05)

            spot_msg = Pc61Message.from_fields(
                ["14074.0", "W1AW", "1-Mar-2026", "0000Z", "FT8", "N9XYZ", "REMOTE1", "127.0.0.1", "H1", "~"]
            )
            await remote.send("uplink", WirePcFrame("PC61", spot_msg.to_fields()))

            await _wait_until(lambda: b"W1AW" in bytes(writer.buffer))
            assert await app.store.count_spots() == 1

            stats = await app.node_link.stats()
            inbound = [st for st in stats.values() if bool(st.get("inbound"))]
            assert inbound
            assert sum(int(st.get("parsed_frames", 0)) for st in inbound) >= 1
            assert sum(int(st.get("rx_by_type", {}).get("PC61", 0)) for st in inbound) >= 1
        finally:
            await remote.stop()
            await app.stop()

    asyncio.run(run())


def test_app_relay_chat_reaches_remote_engine_over_wire(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "app_wire_chat.db")
        cfg = _mk_config(db)
        cfg.node.node_call = "N0NODE-1"
        app = ClusterApp(cfg)
        remote = NodeLinkEngine()

        async def _noop() -> None:
            return

        try:
            try:
                await app.node_link.start_listener("127.0.0.1", 0)
            except OSError as exc:
                pytest.skip(f"socket bind not available in this environment: {exc}")

            port = app.node_link.listen_port()
            assert port is not None

            app.telnet._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
            app.telnet.start = _noop  # type: ignore[method-assign]
            app.telnet.stop = _noop  # type: ignore[method-assign]
            app.web.start = _noop  # type: ignore[method-assign]
            app.web.stop = _noop  # type: ignore[method-assign]

            await app.start()
            await remote.connect("uplink", "127.0.0.1", port)
            await asyncio.sleep(0.05)

            await app.telnet._execute_command("N0CALL", "set/routepc19")
            await app.telnet._execute_command("N0CALL", "chat hello-wire")

            item = await remote.recv(timeout=1.0)
            assert item is not None
            _, frame, typed = item
            assert frame.pc_type == "PC93"
            assert typed is not None
            payload = "^".join(frame.payload_fields)
            assert "hello-wire" in payload
            assert "[via:N0NODE-1]" in payload

            stats = await app.node_link.stats()
            inbound = [st for st in stats.values() if bool(st.get("inbound"))]
            assert inbound
            assert sum(int(st.get("sent_frames", 0)) for st in inbound) >= 1
        finally:
            await remote.stop()
            await app.stop()

    asyncio.run(run())


def test_app_relay_chat_fanout_blocks_selected_wire_peer(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "app_wire_chat_fanout.db")
        cfg = _mk_config(db)
        cfg.node.node_call = "N0NODE-1"
        app = ClusterApp(cfg)
        remote1 = NodeLinkEngine()
        remote2 = NodeLinkEngine()

        async def _noop() -> None:
            return

        try:
            try:
                await app.node_link.start_listener("127.0.0.1", 0)
            except OSError as exc:
                pytest.skip(f"socket bind not available in this environment: {exc}")

            port = app.node_link.listen_port()
            assert port is not None

            app.telnet._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
            app.telnet.start = _noop  # type: ignore[method-assign]
            app.telnet.stop = _noop  # type: ignore[method-assign]
            app.web.start = _noop  # type: ignore[method-assign]
            app.web.stop = _noop  # type: ignore[method-assign]

            await app.start()
            await remote1.connect("r1", "127.0.0.1", port)
            await asyncio.sleep(0.05)
            names_after_first = set(await app.node_link.peer_names())
            assert len(names_after_first) == 1
            peer1 = next(iter(names_after_first))

            await remote2.connect("r2", "127.0.0.1", port)
            await asyncio.sleep(0.05)
            names_after_second = set(await app.node_link.peer_names())
            assert len(names_after_second) == 2
            peer2 = next(iter(names_after_second - names_after_first))

            now = int(datetime.now(timezone.utc).timestamp())
            await app.store.set_user_pref("N0CALL", "routepc19", "on", now)
            await app.store.set_user_pref("N0CALL", f"relay.peer.{peer2.lower()}.chat", "off", now)

            await app.telnet._execute_command("N0CALL", "chat hello-fanout")

            item1 = await remote1.recv(timeout=1.0)
            assert item1 is not None
            _, frame1, _ = item1
            assert frame1.pc_type == "PC93"
            assert "hello-fanout" in "^".join(frame1.payload_fields)

            item2 = await remote2.recv(timeout=0.2)
            assert item2 is None

            stats = await app.node_link.stats()
            assert stats[peer1]["sent_frames"] >= 1
            assert stats[peer2]["policy_reasons"]["relay_peer_chat_disabled"] == 1
        finally:
            await remote1.stop()
            await remote2.stop()
            await app.stop()

    asyncio.run(run())


def test_app_wire_multi_peer_mixed_chat_and_spot_policies(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "app_wire_mixed_fanout.db")
        cfg = _mk_config(db)
        cfg.node.node_call = "N0NODE-1"
        app = ClusterApp(cfg)
        remote1 = NodeLinkEngine()
        remote2 = NodeLinkEngine()

        async def _noop() -> None:
            return

        try:
            try:
                await app.node_link.start_listener("127.0.0.1", 0)
            except OSError as exc:
                pytest.skip(f"socket bind not available in this environment: {exc}")

            port = app.node_link.listen_port()
            assert port is not None

            app.telnet._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
            app.telnet.start = _noop  # type: ignore[method-assign]
            app.telnet.stop = _noop  # type: ignore[method-assign]
            app.web.start = _noop  # type: ignore[method-assign]
            app.web.stop = _noop  # type: ignore[method-assign]

            await app.start()
            await remote1.connect("mix1", "127.0.0.1", port)
            await remote2.connect("mix2", "127.0.0.1", port)
            await asyncio.sleep(0.05)

            peers = sorted(await app.node_link.peer_names())
            assert len(peers) == 2
            peer1, peer2 = peers

            now = int(datetime.now(timezone.utc).timestamp())
            await app.store.upsert_user_registry("N0CALL", now, privilege="user")
            await app.store.set_user_pref("N0CALL", "routepc19", "on", now)
            await app.store.set_user_pref("N0CALL", f"relay.peer.{peer2.lower()}.chat", "off", now)
            await app.store.set_user_pref("N0CALL", f"relay.peer.{peer1.lower()}.spots", "off", now)

            await app.telnet._execute_command("N0CALL", "chat mixed-chat")
            await app.telnet._execute_command("N0CALL", "dx 14074.0 K1ABC mixed-spot")

            r1_first = await remote1.recv(timeout=1.0)
            r2_first = await remote2.recv(timeout=1.0)
            assert r1_first is not None
            assert r2_first is not None

            r1_payload = "^".join(r1_first[1].payload_fields)
            r2_payload = "^".join(r2_first[1].payload_fields)
            assert r1_first[1].pc_type == "PC93"
            assert "mixed-chat" in r1_payload
            assert r2_first[1].pc_type == "PC61"
            assert "mixed-spot" in r2_payload

            assert await remote1.recv(timeout=0.2) is None
            assert await remote2.recv(timeout=0.2) is None

            stats = await app.node_link.stats()
            assert stats[peer1]["policy_reasons"]["relay_peer_spots_disabled"] == 1
            assert stats[peer2]["policy_reasons"]["relay_peer_chat_disabled"] == 1
            assert stats[peer1]["sent_frames"] >= 1
            assert stats[peer2]["sent_frames"] >= 1
        finally:
            await remote1.stop()
            await remote2.stop()
            await app.stop()

    asyncio.run(run())


def test_app_wire_proto_state_burst_records_peer_history(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "app_wire_proto_state.db")
        cfg = _mk_config(db)
        cfg.node.node_call = "N0NODE-1"
        app = ClusterApp(cfg)
        remote = NodeLinkEngine()

        async def _noop() -> None:
            return

        async def _peer_tag() -> str | None:
            stats = await app.node_link.stats()
            names = [name for name, st in stats.items() if bool(st.get("inbound"))]
            if len(names) != 1:
                return None
            return re.sub(r"[^a-z0-9_.-]", "_", names[0].lower())

        async def _proto_ready() -> bool:
            tag = await _peer_tag()
            if not tag:
                return False
            prefs = await app.store.list_user_prefs(cfg.node.node_call)
            return (
                prefs.get(f"proto.peer.{tag}.pc24.flag") == "1"
                and prefs.get(f"proto.peer.{tag}.pc50.count") == "63"
                and prefs.get(f"proto.peer.{tag}.pc51.value") == "1"
                and int(prefs.get(f"proto.peer.{tag}.change_count", "0")) >= 2
            )

        try:
            try:
                await app.node_link.start_listener("127.0.0.1", 0)
            except OSError as exc:
                pytest.skip(f"socket bind not available in this environment: {exc}")

            port = app.node_link.listen_port()
            assert port is not None

            app.telnet.start = _noop  # type: ignore[method-assign]
            app.telnet.stop = _noop  # type: ignore[method-assign]
            app.web.start = _noop  # type: ignore[method-assign]
            app.web.stop = _noop  # type: ignore[method-assign]

            await app.start()
            await remote.connect("uplink", "127.0.0.1", port)
            await asyncio.sleep(0.05)

            await remote.send("uplink", WirePcFrame("PC24", ["OH8X", "1", "H29", ""]))
            await remote.send("uplink", WirePcFrame("PC50", ["W3LPL", "63", "H97", ""]))
            await remote.send("uplink", WirePcFrame("PC51", [cfg.node.node_call, "WB3FFV-2", "1", ""]))
            await remote.send("uplink", WirePcFrame("PC24", ["OH8X", "0", "H29", ""]))
            await remote.send("uplink", WirePcFrame("PC24", ["OH8X", "1", "H29", ""]))

            await _wait_until_async(_proto_ready, timeout=2.0)

            tag = await _peer_tag()
            assert tag is not None
            prefs = await app.store.list_user_prefs(cfg.node.node_call)
            assert prefs.get(f"proto.peer.{tag}.pc24.call") == "OH8X"
            assert prefs.get(f"proto.peer.{tag}.pc24.flag") == "1"
            assert prefs.get(f"proto.peer.{tag}.pc50.call") == "W3LPL"
            assert prefs.get(f"proto.peer.{tag}.pc50.count") == "63"
            assert prefs.get(f"proto.peer.{tag}.pc51.to") == cfg.node.node_call
            assert prefs.get(f"proto.peer.{tag}.pc51.from") == "WB3FFV-2"
            assert prefs.get(f"proto.peer.{tag}.pc51.value") == "1"
            assert int(prefs.get(f"proto.peer.{tag}.flap_score", "0")) == 0
            raw_hist = prefs.get(f"proto.peer.{tag}.history", "[]")
            hist = json.loads(raw_hist)
            assert isinstance(hist, list)
            assert any(str(ev.get("key", "")) == "pc24.flag" for ev in hist if isinstance(ev, dict))

            stats = await app.node_link.stats()
            inbound = [st for st in stats.values() if bool(st.get("inbound"))]
            assert inbound
            assert sum(int(st.get("rx_by_type", {}).get("PC24", 0)) for st in inbound) >= 3
            assert sum(int(st.get("rx_by_type", {}).get("PC50", 0)) for st in inbound) >= 1
            assert sum(int(st.get("rx_by_type", {}).get("PC51", 0)) for st in inbound) >= 1
            assert sum(int(st.get("tx_by_type", {}).get("PC51", 0)) for st in inbound) >= 1
        finally:
            await remote.stop()
            await app.stop()

    asyncio.run(run())


def test_app_wire_reconnect_ingests_clean_new_session(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "app_wire_reconnect.db")
        cfg = _mk_config(db)
        cfg.node.node_call = "N0NODE-1"
        app = ClusterApp(cfg)
        writer = _DummyWriter()
        remote = NodeLinkEngine()

        async def _noop() -> None:
            return

        async def _single_inbound_name() -> str | None:
            stats = await app.node_link.stats()
            names = [name for name, st in stats.items() if bool(st.get("inbound"))]
            return names[0] if len(names) == 1 else None

        async def _no_inbound_peers() -> bool:
            return await app.node_link.peer_names() == []

        try:
            try:
                await app.node_link.start_listener("127.0.0.1", 0)
            except OSError as exc:
                pytest.skip(f"socket bind not available in this environment: {exc}")

            port = app.node_link.listen_port()
            assert port is not None

            app.telnet._sessions[1] = Session(call="N0CALL", writer=writer, connected_at=datetime.now(timezone.utc))
            app.telnet.start = _noop  # type: ignore[method-assign]
            app.telnet.stop = _noop  # type: ignore[method-assign]
            app.web.start = _noop  # type: ignore[method-assign]
            app.web.stop = _noop  # type: ignore[method-assign]

            await app.start()

            await remote.connect("first", "127.0.0.1", port)
            await asyncio.sleep(0.05)
            first_peer = await _single_inbound_name()
            assert first_peer is not None

            now = int(datetime.now(timezone.utc).timestamp())
            await app.store.set_user_pref(cfg.node.node_call, f"ingest.peer.{first_peer.lower()}.spots", "off", now)
            blocked = Pc61Message.from_fields(
                ["14074.0", "K1ABC", "1-Mar-2026", "0000Z", "FT8", "N0CALL", "FIRSTNODE", "127.0.0.1", "H1", "~"]
            )
            await remote.send("first", WirePcFrame("PC61", blocked.to_fields()))

            async def _blocked_seen() -> bool:
                stats = await app.node_link.stats()
                peer = stats.get(first_peer, {})
                reasons = peer.get("policy_reasons", {}) if isinstance(peer, dict) else {}
                return reasons.get("ingest_spots_disabled") == 1

            await _wait_until_async(_blocked_seen, timeout=2.0)
            assert await app.store.count_spots() == 0

            await remote.stop()
            await _wait_until_async(_no_inbound_peers, timeout=2.0)
        finally:
            await remote.stop()

        remote = NodeLinkEngine()
        try:
            await remote.connect("second", "127.0.0.1", port)
            await asyncio.sleep(0.05)
            second_peer = await _single_inbound_name()
            assert second_peer is not None
            assert second_peer.startswith("in:")

            allowed = Pc61Message.from_fields(
                ["7074.0", "W1AW", "1-Mar-2026", "0001Z", "FT8", "K1ABC", "SECONDNODE", "127.0.0.1", "H1", "~"]
            )
            await remote.send("second", WirePcFrame("PC61", allowed.to_fields()))

            await _wait_until(lambda: b"W1AW" in bytes(writer.buffer), timeout=2.0)
            assert await app.store.count_spots() == 1
            stats = await app.node_link.stats()
            peer_stats = stats.get(second_peer, {})
            assert int(peer_stats.get("policy_dropped", 0)) == 0
            assert int(peer_stats.get("parsed_frames", 0)) >= 1
        finally:
            await remote.stop()
            await app.stop()

    asyncio.run(run())


def test_app_restart_preserves_persistent_state_but_not_live_peer_state(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "app_restart_persist.db")
        cfg1 = _mk_config(db)
        cfg1.node.node_call = "N0NODE-1"
        app1 = ClusterApp(cfg1)
        writer = _DummyWriter()

        async def _noop() -> None:
            return

        try:
            app1.telnet._sessions[1] = Session(call="N0CALL", writer=writer, connected_at=datetime.now(timezone.utc))
            app1.telnet.start = _noop  # type: ignore[method-assign]
            app1.telnet.stop = _noop  # type: ignore[method-assign]
            app1.web.start = _noop  # type: ignore[method-assign]
            app1.web.stop = _noop  # type: ignore[method-assign]
            app1.node_link._peers["PEER1"] = LinkPeer(name="PEER1", conn=_DummyConn(), inbound=False)

            await app1.start()

            proto24 = Pc24Message.from_fields(["OH8X", "1", "H29", ""])
            proto50 = Pc50Message.from_fields(["W3LPL", "63", "H97", ""])
            proto51 = Pc51Message.from_fields(["AI3I-15", "WB3FFV-2", "1", ""])
            spot = Pc61Message.from_fields(
                ["14074.0", "W1AW", "1-Mar-2026", "0000Z", "FT8", "N9XYZ", "REMOTE1", "127.0.0.1", "H1", "~"]
            )
            await app1._handle_node_link_item("PEER1", WirePcFrame("PC24", proto24.to_fields()), proto24)
            await app1._handle_node_link_item("PEER1", WirePcFrame("PC50", proto50.to_fields()), proto50)
            await app1._handle_node_link_item("PEER1", WirePcFrame("PC51", proto51.to_fields()), proto51)
            await app1._handle_node_link_item("PEER1", WirePcFrame("PC61", spot.to_fields()), spot)

            now = int(datetime.now(timezone.utc).timestamp())
            await app1.store.set_user_pref("N0CALL", "routepc19", "on", now)
            await app1.store.set_user_pref("N0CALL", "relay.chat", "off", now)

            assert await app1.store.count_spots() == 1
            prefs1 = await app1.store.list_user_prefs(cfg1.node.node_call)
            assert prefs1.get("proto.peer.peer1.pc24.flag") == "1"
            assert prefs1.get("proto.peer.peer1.pc50.count") == "63"
        finally:
            await app1.stop()

        cfg2 = _mk_config(db)
        cfg2.node.node_call = "N0NODE-1"
        app2 = ClusterApp(cfg2)
        try:
            # Persistent DB-backed state should survive restart.
            assert await app2.store.count_spots() == 1
            rows = await app2.store.latest_spots(limit=1)
            assert rows[0]["dx_call"] == "W1AW"
            prefs2 = await app2.store.list_user_prefs(cfg2.node.node_call)
            assert prefs2.get("proto.peer.peer1.pc24.call") == "OH8X"
            assert prefs2.get("proto.peer.peer1.pc24.flag") == "1"
            assert prefs2.get("proto.peer.peer1.pc50.call") == "W3LPL"
            assert prefs2.get("proto.peer.peer1.pc50.count") == "63"
            assert prefs2.get("proto.peer.peer1.pc51.to") == "AI3I-15"
            assert prefs2.get("proto.peer.peer1.pc51.from") == "WB3FFV-2"
            assert prefs2.get("proto.peer.peer1.pc51.value") == "1"
            assert await app2.store.get_user_pref("N0CALL", "routepc19") == "on"
            assert await app2.store.get_user_pref("N0CALL", "relay.chat") == "off"

            # Live node-link peer state and telnet sessions must not survive restart.
            assert await app2.node_link.peer_names() == []
            assert app2.telnet.session_count == 0
        finally:
            await app2.store.close()

    asyncio.run(run())


def test_app_restart_then_wire_ingest_uses_existing_db_cleanly(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "app_restart_wire.db")
        cfg1 = _mk_config(db)
        cfg1.node.node_call = "N0NODE-1"
        app1 = ClusterApp(cfg1)

        async def _noop() -> None:
            return

        try:
            app1.telnet.start = _noop  # type: ignore[method-assign]
            app1.telnet.stop = _noop  # type: ignore[method-assign]
            app1.web.start = _noop  # type: ignore[method-assign]
            app1.web.stop = _noop  # type: ignore[method-assign]
            await app1.start()
            await app1.store.set_user_pref("N0CALL", "routepc19", "on", int(datetime.now(timezone.utc).timestamp()))
        finally:
            await app1.stop()

        cfg2 = _mk_config(db)
        cfg2.node.node_call = "N0NODE-1"
        app2 = ClusterApp(cfg2)
        remote = NodeLinkEngine()
        writer = _DummyWriter()
        try:
            try:
                await app2.node_link.start_listener("127.0.0.1", 0)
            except OSError as exc:
                pytest.skip(f"socket bind not available in this environment: {exc}")

            port = app2.node_link.listen_port()
            assert port is not None

            app2.telnet._sessions[1] = Session(call="N0CALL", writer=writer, connected_at=datetime.now(timezone.utc))
            app2.telnet.start = _noop  # type: ignore[method-assign]
            app2.telnet.stop = _noop  # type: ignore[method-assign]
            app2.web.start = _noop  # type: ignore[method-assign]
            app2.web.stop = _noop  # type: ignore[method-assign]

            await app2.start()
            assert await app2.store.get_user_pref("N0CALL", "routepc19") == "on"
            assert await app2.node_link.peer_names() == []

            await remote.connect("after-restart", "127.0.0.1", port)
            await asyncio.sleep(0.05)
            msg = Pc61Message.from_fields(
                ["7074.0", "K1ABC", "1-Mar-2026", "0001Z", "FT8", "N0CALL", "RESTARTHOST", "127.0.0.1", "H1", "~"]
            )
            await remote.send("after-restart", WirePcFrame("PC61", msg.to_fields()))

            await _wait_until(lambda: b"K1ABC" in bytes(writer.buffer), timeout=2.0)
            assert await app2.store.count_spots() == 1
            stats = await app2.node_link.stats()
            inbound = [st for st in stats.values() if bool(st.get("inbound"))]
            assert len(inbound) == 1
            assert int(inbound[0].get("parsed_frames", 0)) >= 1
        finally:
            await remote.stop()
            await app2.stop()

    asyncio.run(run())
