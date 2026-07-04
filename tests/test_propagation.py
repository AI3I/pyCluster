from datetime import datetime, timezone

from pycluster.propagation import minimuf35_muf, minimuf_sunspots_from_sfi


def test_minimuf35_keeps_wisconsin_to_england_path_nonzero_after_utc_midnight() -> None:
    midnight = datetime(2026, 6, 29, 0, 0, tzinfo=timezone.utc)
    muf = minimuf35_muf(150, midnight, 43.0, -89.0, 52.0, -1.0)

    assert muf is not None
    assert muf > 15.0


def test_minimuf_sunspot_curve_matches_expected_flux_ranges() -> None:
    assert minimuf_sunspots_from_sfi(60) == 0.0
    assert round(minimuf_sunspots_from_sfi(150) or 0.0) == 103
    assert minimuf_sunspots_from_sfi(220) is not None
