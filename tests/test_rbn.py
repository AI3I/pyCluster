from datetime import datetime, timezone

from pycluster.rbn import parse_rbn_dx_line


def test_parse_rbn_dx_line_from_dx_cluster_output() -> None:
    now = datetime(2026, 5, 6, 0, 53, tzinfo=timezone.utc)

    spot = parse_rbn_dx_line(
        "DX de KO4BHX-#:   7007.0  N9JR         CW  39dB Q:2 Z:4               0052Z",
        now=now,
        source_node="RBN",
    )

    assert spot is not None
    assert spot.freq_khz == 7007.0
    assert spot.dx_call == "N9JR"
    assert spot.spotter == "KO4BHX-#"
    assert spot.info == "CW  39dB Q:2 Z:4"
    assert spot.source_node == "RBN"
    assert spot.epoch == int(datetime(2026, 5, 6, 0, 52, tzinfo=timezone.utc).timestamp())


def test_parse_rbn_dx_line_ignores_non_dx_text() -> None:
    assert parse_rbn_dx_line("N9JR-5 de N9JR-2  6-May-2026 0051Z dxspider >") is None


def test_parse_rbn_dx_line_from_public_raw_feed_samples() -> None:
    now = datetime(2026, 5, 10, 16, 38, tzinfo=timezone.utc)

    samples = [
        ("DX de OE9GHV-#:  7029.70  UT1KT          CW    31 dB  22 WPM  CQ      1637Z", "OE9GHV-#", 7029.70),
        ("DX de RK3TD-2-#:  7028.10  UT8AS          CW    19 dB  29 WPM  CQ      1637Z", "RK3TD-2-#", 7028.10),
    ]

    for line, spotter, freq in samples:
        spot = parse_rbn_dx_line(line, now=now, source_node="RBN")
        assert spot is not None
        assert spot.spotter == spotter
        assert spot.freq_khz == freq
        assert spot.info.endswith("CQ")
        assert spot.epoch == int(datetime(2026, 5, 10, 16, 37, tzinfo=timezone.utc).timestamp())
