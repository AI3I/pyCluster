from datetime import datetime, timezone

from pycluster.propagation import minimuf35_muf, minimuf_sunspots_from_sfi


def test_minimuf35_keeps_wisconsin_to_england_path_nonzero_after_utc_midnight() -> None:
    midnight = datetime(2026, 6, 29, 0, 0, tzinfo=timezone.utc)
    muf = minimuf35_muf(150, midnight, 43.0, -89.0, 52.0, -1.0)

    assert muf is not None
    assert muf > 15.0


def test_minimuf35_matches_dxspider_reference_vector() -> None:
    expected = {
        0: 22.11041835,
        4: 15.33323265,
        8: 18.96571089,
        12: 25.48434133,
        16: 28.21882961,
        20: 27.67456352,
        23: 24.16899286,
    }
    for hour, reference in expected.items():
        when = datetime(2026, 6, 29, hour, tzinfo=timezone.utc)
        actual = minimuf35_muf(150, when, 43.0, -89.0, 52.0, -1.0)
        assert actual is not None
        assert abs(actual - reference) < 1e-7


def test_minimuf_sunspot_curve_matches_expected_flux_ranges() -> None:
    assert minimuf_sunspots_from_sfi(60) == 0.0
    assert round(minimuf_sunspots_from_sfi(150) or 0.0) == 103
    assert minimuf_sunspots_from_sfi(220) is not None
