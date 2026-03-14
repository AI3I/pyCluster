from __future__ import annotations

from datetime import datetime, timezone

from pycluster.shdx import parse_sh_dx_args


def test_parse_basic_limit_and_prefix() -> None:
    q = parse_sh_dx_args("5 K1")
    assert q.limit == 5
    assert q.prefix_pattern == "K1%"


def test_parse_filters() -> None:
    now = datetime(2026, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
    q = parse_sh_dx_args("10 by WW5L on 40m day 2 info RTTY", now_utc=now)
    assert q.limit == 10
    assert q.spotter == "WW5L"
    assert q.freq_low == 7000.0
    assert q.freq_high == 7300.0
    assert q.info_contains == "RTTY"
    assert q.since_epoch == int(now.timestamp()) - 2 * 86400


def test_parse_exact() -> None:
    q = parse_sh_dx_args("K3AJ exact")
    assert q.prefix_exact is True
    assert q.prefix_pattern == "K3AJ%"
