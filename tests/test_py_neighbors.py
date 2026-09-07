from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from pycluster.app import ClusterApp
from pycluster.config import AppConfig, NodeConfig, PublicWebConfig, StoreConfig, TelnetConfig, WebConfig
from pycluster.protocol import WirePcFrame
from pycluster.py_protocol import (
    PY_NEIGHBORS_TYPE,
    PyNeighborRecord,
    PyNeighborsMessage,
)


def _mk_config(db_path: str) -> AppConfig:
    return AppConfig(
        node=NodeConfig(),
        telnet=TelnetConfig(host="127.0.0.1", port=0, idle_timeout_seconds=30),
        web=WebConfig(host="127.0.0.1", port=0),
        public_web=PublicWebConfig(),
        store=StoreConfig(sqlite_path=db_path),
    )


def _msg(*records: PyNeighborRecord, node_call: str = "N0NODE-1") -> PyNeighborsMessage:
    return PyNeighborsMessage(node_call, records, 1785456000, 1785456000 + 3600)


def test_frame_type_and_round_trip() -> None:
    message = _msg(
        PyNeighborRecord("K0MVH-1", "dxspider", "DXSpider 1.55 build 0.291", "connected"),
        PyNeighborRecord("W3XYZ-2", "arcluster", "AR-Cluster 6.3", "configured"),
    )
    fields = message.to_fields()
    assert PY_NEIGHBORS_TYPE == "PY14"
    assert fields[1] == "NEIGHBORS"
    decoded = PyNeighborsMessage.from_fields(fields)
    assert decoded == message
    assert [record.call for record in decoded.neighbors] == ["K0MVH-1", "W3XYZ-2"]


def test_records_are_canonically_ordered() -> None:
    unordered = _msg(
        PyNeighborRecord("W3XYZ-2", "clx", "CLX v9.6.4", "connected"),
        PyNeighborRecord("K0MVH-1", "dxspider", "", "connected"),
    )
    decoded = PyNeighborsMessage.from_fields(unordered.to_fields())
    assert [record.call for record in decoded.neighbors] == ["K0MVH-1", "W3XYZ-2"]


def test_empty_neighbor_list_is_valid() -> None:
    decoded = PyNeighborsMessage.from_fields(_msg().to_fields())
    assert decoded.neighbors == ()


@pytest.mark.parametrize(
    "record",
    [
        PyNeighborRecord("K0MVH-1", "pycluster", "", "connected"),
        PyNeighborRecord("K0MVH-1", "nonsense", "", "connected"),
        PyNeighborRecord("K0MVH-1", "dxspider", "", "wandering"),
        PyNeighborRecord("not a call", "dxspider", "", "connected"),
        PyNeighborRecord("N0NODE-1", "dxspider", "", "connected"),
    ],
)
def test_invalid_records_are_rejected(record: PyNeighborRecord) -> None:
    with pytest.raises(ValueError):
        _msg(record).to_fields()


def test_duplicate_neighbors_are_rejected() -> None:
    duplicated = _msg(
        PyNeighborRecord("K0MVH-1", "dxspider", "", "connected"),
        PyNeighborRecord("K0MVH-1", "clx", "", "configured"),
    )
    with pytest.raises(ValueError):
        duplicated.to_fields()


def test_pycluster_neighbors_are_excluded_from_the_report(tmp_path) -> None:
    """pyCluster peers already travel in PY01 direct_peers."""

    async def run() -> None:
        app = ClusterApp(_mk_config(str(tmp_path / "build.db")))
        try:
            async def _stats() -> dict[str, dict[str, object]]:
                return {
                    "K0MVH-1": {"profile": "dxspider"},
                    "VE7CC-1": {"profile": "pycluster"},
                    "N2ABC": {"profile": "clx"},
                }

            app.node_link.stats = _stats  # type: ignore[method-assign]
            message = await app._build_py_neighbors()
            assert [(r.call, r.family, r.state) for r in message.neighbors] == [
                ("K0MVH-1", "dxspider", "connected"),
                ("N2ABC", "clx", "connected"),
            ]
            # Must survive its own validation, or metadata sending would break.
            assert PyNeighborsMessage.from_fields(message.to_fields()).neighbors == message.neighbors
        finally:
            await app.store.close()

    asyncio.run(run())


def test_observed_pc18_banner_outranks_configured_profile(tmp_path) -> None:
    async def run() -> None:
        app = ClusterApp(_mk_config(str(tmp_path / "banner.db")))
        try:
            async def _stats() -> dict[str, dict[str, object]]:
                return {"K0MVH-1": {"profile": "dxspider"}}

            app.node_link.stats = _stats  # type: ignore[method-assign]
            await app._handle_node_link_item(
                "K0MVH-1",
                WirePcFrame("PC18", ["AR-Cluster Version 6.3.5", "5457", ""]),
                None,
            )
            message = await app._build_py_neighbors()
            assert [(r.call, r.family) for r in message.neighbors] == [("K0MVH-1", "arcluster")]
            assert message.neighbors[0].software.startswith("AR-Cluster")
        finally:
            await app.store.close()

    asyncio.run(run())


def test_unrecognized_software_is_reported_as_unknown(tmp_path) -> None:
    async def run() -> None:
        app = ClusterApp(_mk_config(str(tmp_path / "unknown.db")))
        try:
            async def _stats() -> dict[str, dict[str, object]]:
                return {"K9ZZZ": {"profile": ""}}

            app.node_link.stats = _stats  # type: ignore[method-assign]
            message = await app._build_py_neighbors()
            assert [(r.call, r.family) for r in message.neighbors] == [("K9ZZZ", "unknown")]
        finally:
            await app.store.close()

    asyncio.run(run())


def test_capability_gates_the_family(tmp_path) -> None:
    async def run() -> None:
        cfg = _mk_config(str(tmp_path / "gate.db"))
        cfg.py_protocol.enabled = True
        app = ClusterApp(cfg)
        try:
            assert "neighbors" in app._local_py_capabilities()
        finally:
            await app.store.close()

        cfg_off = _mk_config(str(tmp_path / "gate_off.db"))
        cfg_off.py_protocol.enabled = True
        cfg_off.py_protocol.share_neighbors = False
        app_off = ClusterApp(cfg_off)
        try:
            assert "neighbors" not in app_off._local_py_capabilities()
        finally:
            await app_off.store.close()

    asyncio.run(run())


def test_received_neighbors_are_persisted_for_the_peer(tmp_path) -> None:
    async def run() -> None:
        cfg = _mk_config(str(tmp_path / "receive.db"))
        cfg.py_protocol.enabled = True
        app = ClusterApp(cfg)
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            app._pycluster_identified_peers.add("AI3I-90")
            app._py_remote_capabilities["AI3I-90"] = frozenset({"neighbors", "py99-error"})
            app._py_negotiated_capabilities["AI3I-90"] = frozenset({"neighbors", "py99-error"})
            message = PyNeighborsMessage(
                "AI3I-90",
                (PyNeighborRecord("K0MVH-1", "dxspider", "DXSpider 1.55", "connected"),),
                now,
                now + 3600,
            )
            await app._handle_py_frame("AI3I-90", WirePcFrame(PY_NEIGHBORS_TYPE, message.to_fields()))
            prefs = await app.store.list_user_prefs(app.config.node.node_call)
            records = prefs.get("proto.peer.ai3i-90.py.neighbors.records", "")
            assert "K0MVH-1" in records
            assert "dxspider" in records
            assert prefs.get("proto.peer.ai3i-90.py.neighbors.count") == "1"
        finally:
            await app.store.close()

    asyncio.run(run())


def test_malformed_neighbors_frame_is_dropped(tmp_path) -> None:
    async def run() -> None:
        cfg = _mk_config(str(tmp_path / "malformed.db"))
        cfg.py_protocol.enabled = True
        app = ClusterApp(cfg)
        try:
            drops: list[tuple[str, str]] = []

            async def _mark(peer: str, reason: str) -> None:
                drops.append((peer, reason))

            app.node_link.mark_policy_drop = _mark  # type: ignore[method-assign]
            app._pycluster_identified_peers.add("AI3I-90")
            app._py_remote_capabilities["AI3I-90"] = frozenset({"neighbors"})
            app._py_negotiated_capabilities["AI3I-90"] = frozenset({"neighbors"})
            await app._handle_py_frame("AI3I-90", WirePcFrame(PY_NEIGHBORS_TYPE, ["2", "NEIGHBORS", "!!!"]))
            assert drops == [("AI3I-90", "invalid_py_neighbors")]
            prefs = await app.store.list_user_prefs(app.config.node.node_call)
            assert "proto.peer.ai3i-90.py.neighbors.records" not in prefs
        finally:
            await app.store.close()

    asyncio.run(run())


def test_large_neighbor_set_is_trimmed_to_the_frame_limit(tmp_path) -> None:
    """A hub with many legacy links must send a shorter list, not an oversized frame."""

    async def run() -> None:
        cfg = _mk_config(str(tmp_path / "trim.db"))
        cfg.py_protocol.max_frame_bytes = 512
        app = ClusterApp(cfg)
        try:
            async def _stats() -> dict[str, dict[str, object]]:
                return {f"K{idx}ABC-1": {"profile": "dxspider"} for idx in range(60)}

            app.node_link.stats = _stats  # type: ignore[method-assign]
            message = await app._build_py_neighbors()
            assert 0 < len(message.neighbors) < 60
            frame = WirePcFrame(PY_NEIGHBORS_TYPE, message.to_fields())
            assert app._py_frame_fits(frame)
        finally:
            await app.store.close()

    asyncio.run(run())


def test_connected_links_survive_trimming_before_configured_ones(tmp_path) -> None:
    async def run() -> None:
        cfg = _mk_config(str(tmp_path / "trim_priority.db"))
        cfg.py_protocol.max_frame_bytes = 400
        app = ClusterApp(cfg)
        try:
            async def _stats() -> dict[str, dict[str, object]]:
                return {"W9LIVE-1": {"profile": "dxspider"}}

            async def _targets() -> dict[str, dict[str, object]]:
                return {f"K{idx}IDLE-1": {"profile": "dxspider"} for idx in range(40)}

            app.node_link.stats = _stats  # type: ignore[method-assign]
            app._desired_peer_targets = _targets  # type: ignore[method-assign]
            message = await app._build_py_neighbors()
            calls = [record.call for record in message.neighbors]
            assert "W9LIVE-1" in calls
            assert len(calls) < 41
        finally:
            await app.store.close()

    asyncio.run(run())
