from pycluster.peer_profiles import (
    allowed_types_for_profile,
    format_dx_line_for_profile,
    format_live_dx_line_for_profile,
    normalize_profile,
    profile_allows_pc,
)


def test_normalize_profile() -> None:
    assert normalize_profile("pycluster") == "pycluster"
    assert normalize_profile("ARCLUSTER") == "arcluster"
    assert normalize_profile("unknown") == "pycluster"


def test_allowed_types() -> None:
    assert profile_allows_pc("pycluster", "PC24") is True
    assert profile_allows_pc("dxspider", "PC24") is True
    assert profile_allows_pc("dxnet", "PC61") is True
    assert profile_allows_pc("dxnet", "PC24") is False
    assert "PC50" in allowed_types_for_profile("clx")


def test_format_dx_by_profile() -> None:
    base = dict(freq_khz=7109.9, dx_call="K3AJ", when="1-Mar-2026 0322Z", info="RTTY", spotter="WW5L")
    spider = format_dx_line_for_profile("dxspider", **base)
    assert "<WW5L>" in spider
    assert "de WW5L" in format_dx_line_for_profile("arcluster", **base)
    assert "[WW5L]" in format_dx_line_for_profile("dxnet", **base)
    assert "by WW5L" in format_dx_line_for_profile("clx", **base)


def test_format_live_dx_line() -> None:
    base = dict(freq_khz=7109.9, dx_call="K3AJ", when="0322Z", info="RTTY", spotter="WW5L")
    line = format_live_dx_line_for_profile("dxspider", **base)
    assert line.startswith("DX de WW5L")
    assert "0322Z" in line
    assert len(line) <= 80


def test_format_live_dx_line_is_column_aligned() -> None:
    line = format_live_dx_line_for_profile(
        "spider",
        freq_khz=21074.0,
        dx_call="N4GJE",
        when="2152Z",
        info="FT8 FF51 db-14 From FF51",
        spotter="LU6YR",
    )
    assert line.startswith("DX de LU6YR")
    assert line[19:27].strip() == "21074.0"
    assert line[29:41].strip() == "N4GJE"
    assert line.endswith("2152Z")
    assert len(line) <= 80


def test_format_live_dx_line_keeps_suffix_within_80_columns() -> None:
    line = format_live_dx_line_for_profile(
        "spider",
        freq_khz=14074.0,
        dx_call="K1ABC",
        when="2152Z",
        info="FT8 VERY LONG COMMENT THAT SHOULD SHRINK TO FIT",
        spotter="JA1AAA",
        suffix="CQ5 ITU8",
    )
    assert line.startswith("DX de JA1AAA:")
    assert line.endswith("2152Z CQ5 ITU8")
    assert len(line) <= 80


def test_format_dx_line_normalizes_nbsp_mojibake() -> None:
    line = format_live_dx_line_for_profile(
        "dxspider",
        freq_khz=14276.0,
        dx_call="W1XZZ",
        when="1341Z",
        info="US-8382\u00a0Rehoboth State Forest",
        spotter="KK4WP",
    )
    assert "US-8382 Rehoboth State Forest" in line
    assert "\u00a0" not in line

    line = format_live_dx_line_for_profile(
        "dxspider",
        freq_khz=14276.0,
        dx_call="W1XZZ",
        when="1341Z",
        info="US-8382ï¿½Rehoboth State Forest",
        spotter="KK4WP",
    )
    assert "US-8382 Rehoboth State Forest" in line
    assert "ï¿½" not in line
