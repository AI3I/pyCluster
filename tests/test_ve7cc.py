from __future__ import annotations

from datetime import datetime, timezone

from pycluster.ve7cc import CC11Location, format_cc11


def test_format_cc11_has_expected_fields_and_sanitizes_delimiters() -> None:
    epoch = int(datetime(2026, 8, 18, 0, 41, tzinfo=timezone.utc).timestamp())
    line = format_cc11(
        freq_khz=28074.0,
        dx_call="VP2MAA",
        epoch=epoch,
        info="CQ ^ test\nline",
        spotter="K6FA",
        source_node="WA9PIE-2",
        spotted=CC11Location("208", "11", "8", "", "Montserrat-VP2M", "FK86"),
        spotting=CC11Location("226", "6", "3", "CA", "California-K", "DM05"),
    )

    fields = line.split("^")
    assert len(fields) == 20
    assert fields[:7] == ["CC11", "28074.0", "VP2MAA", "18-Aug-2026", "0041Z", "CQ test line", "K6FA"]
    assert fields[7:] == [
        "208", "226", "WA9PIE-2", "11", "8", "6", "3", "", "CA",
        "Montserrat-VP2M", "California-K", "FK86", "DM05",
    ]
