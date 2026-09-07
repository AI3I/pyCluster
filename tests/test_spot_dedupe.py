import asyncio
from datetime import datetime, timezone
from pathlib import Path

from pycluster.models import Spot
from pycluster.spot_dedupe import (
    DEFAULT_MAX_EPOCH_SKEW_SECONDS,
    SpotDeduper,
    clamp_spot_epoch,
    spot_identity_key,
)
from pycluster.store import SpotStore


def _spot(freq_khz=14025.0, dx_call="DL1ABC", epoch=1772337000, info="CW CQ", spotter="K1AA", source_node="N2WQ-1"):
    return Spot(freq_khz, dx_call, epoch, info, spotter, source_node, "")


def test_identity_key_ignores_comment_and_relay_path() -> None:
    raw = _spot(info="CW 22 dB 25 WPM CQ", source_node="N2WQ-1")
    summarised = _spot(info="CW 22dB Q:3 Z:14,15", source_node="VE7CC-1")
    stripped = _spot(info="CW 22dB Q3 Z1415", source_node="K0MVH-1")
    assert spot_identity_key(raw) == spot_identity_key(summarised) == spot_identity_key(stripped)


def test_identity_key_separates_spotters() -> None:
    assert spot_identity_key(_spot(spotter="W1AW")) != spot_identity_key(_spot(spotter="K1AA"))


def test_clamp_spot_epoch_rejects_future_but_keeps_history() -> None:
    now = 1772337000
    assert clamp_spot_epoch(now - 3600, now) == now - 3600
    assert clamp_spot_epoch(now + DEFAULT_MAX_EPOCH_SKEW_SECONDS - 1, now) == now + DEFAULT_MAX_EPOCH_SKEW_SECONDS - 1
    assert clamp_spot_epoch(now + 1800, now) == now
    assert clamp_spot_epoch("not-a-number", now) == now


def test_deduper_absorbs_frequency_jitter_within_tolerance() -> None:
    d = SpotDeduper()
    now = 1772337000
    assert d.check(_spot(freq_khz=7003.5), now) is False
    assert d.check(_spot(freq_khz=7003.6), now) is True
    assert d.check(_spot(freq_khz=7003.4), now) is True
    assert d.check(_spot(freq_khz=7004.5), now) is False


def test_deduper_expiry_uses_arrival_not_spot_epoch() -> None:
    """A peer with a fast clock must not evict other peers' entries."""
    d = SpotDeduper()
    now = 1772337000
    assert d.check(_spot(dx_call="JA1ZZZ", freq_khz=14200.0), now) is False
    # A spot claiming to be 30 minutes in the future, arriving right now.
    assert d.check(_spot(dx_call="PY2QQ", freq_khz=21300.0, epoch=now + 1800, spotter="K0MVH-1"), now) is False
    assert len(d) == 2
    assert d.check(_spot(dx_call="JA1ZZZ", freq_khz=14200.0, epoch=1772337005), now + 5) is True


def test_deduper_respots_after_ttl() -> None:
    d = SpotDeduper(ttl_seconds=900)
    now = 1772337000
    assert d.check(_spot(epoch=now), now) is False
    assert d.check(_spot(epoch=now + 800), now + 800) is True
    assert d.check(_spot(epoch=now + 1801), now + 1801) is False


def test_deduper_keeps_separate_time_clusters_for_one_identity() -> None:
    """Bulk history arrives at one instant but spans days."""
    d = SpotDeduper()
    arrival = 1772337000
    base = 1700000000
    for day in range(5):
        assert d.check(_spot(epoch=base + day * 86400), arrival) is False
    assert d.check(_spot(epoch=base + 30), arrival) is True
    assert d.check(_spot(epoch=base + 3 * 86400 + 30), arrival) is True


def test_deduper_local_path_allows_corrected_comment() -> None:
    d = SpotDeduper()
    now = 1772337000
    assert d.check(_spot(info="test1"), now, match_comment=True) is False
    assert d.check(_spot(info="test2"), now, match_comment=True) is False
    assert d.check(_spot(info="test1"), now, match_comment=True) is True
    # The network path stays comment-blind even after those local spots.
    assert d.check(_spot(info="totally different"), now) is True


def test_deduper_disabled_admits_everything() -> None:
    d = SpotDeduper()
    d.enabled = False
    now = 1772337000
    assert d.check(_spot(), now) is False
    assert d.check(_spot(), now) is False


def test_deduper_evicts_by_arrival_when_over_capacity() -> None:
    d = SpotDeduper(max_entries=10)
    now = 1772337000
    for idx in range(50):
        d.check(_spot(dx_call=f"K{idx}AAA", freq_khz=14000.0 + idx), now + idx)
    assert len(d) <= 10


def test_store_local_spot_allows_corrected_comment(tmp_path: Path) -> None:
    async def run() -> None:
        store = SpotStore(str(tmp_path / "local.db"))
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            first = Spot(14074.0, "K1ABC", now, "test1", "N0CALL", "N0NODE-1", "")
            corrected = Spot(14074.0, "K1ABC", now, "test2", "N0CALL", "N0NODE-1", "")
            repeat = Spot(14074.0, "K1ABC", now, "test1", "N0CALL", "N0NODE-1", "")
            assert await store.add_spot(first, local=True) is True
            assert await store.add_spot(corrected, local=True) is True
            assert await store.add_spot(repeat, local=True) is False
            # The same spot echoed back off the network is still suppressed.
            echoed = Spot(14074.0, "K1ABC", now + 3, "TEST-2", "N0CALL", "VE7CC-1", "")
            assert await store.add_spot(echoed) is False
            assert await store.count_spots() == 2
        finally:
            await store.close()

    asyncio.run(run())
