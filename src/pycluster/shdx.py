from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import re


BAND_RANGES = {
    "160m": (1800.0, 2000.0),
    "80m": (3500.0, 4000.0),
    "60m": (5250.0, 5450.0),
    "40m": (7000.0, 7300.0),
    "30m": (10100.0, 10150.0),
    "20m": (14000.0, 14350.0),
    "17m": (18068.0, 18168.0),
    "15m": (21000.0, 21450.0),
    "12m": (24890.0, 24990.0),
    "10m": (28000.0, 29700.0),
    "6m": (50000.0, 54000.0),
    "2m": (144000.0, 148000.0),
    "hf": (1800.0, 30000.0),
    "vhf": (30000.0, 300000.0),
}


@dataclass(slots=True)
class ShDxQuery:
    limit: int = 10
    prefix_pattern: str | None = None
    prefix_exact: bool = False
    spotter: str | None = None
    freq_low: float | None = None
    freq_high: float | None = None
    info_contains: str | None = None
    since_epoch: int | None = None


def _is_int(s: str) -> bool:
    try:
        int(s)
        return True
    except ValueError:
        return False


def _parse_freq_range(token: str) -> tuple[float, float] | None:
    t = token.strip().lower()
    if not t:
        return None

    # support hf/cw style by taking primary band prefix before slash
    if "/" in t and not re.match(r"^\d+(?:\.\d+)?[/-]\d+(?:\.\d+)?$", t):
        t = t.split("/", 1)[0]

    if t in BAND_RANGES:
        return BAND_RANGES[t]

    m = re.match(r"^(\d+(?:\.\d+)?)[/-](\d+(?:\.\d+)?)$", t)
    if m:
        lo = float(m.group(1))
        hi = float(m.group(2))
        if lo > hi:
            lo, hi = hi, lo
        return lo, hi

    m = re.match(r"^(\d+(?:\.\d+)?)$", t)
    if m:
        v = float(m.group(1))
        return v, v

    return None


def _to_like_from_wildcard(pattern: str) -> str:
    # DX-style wildcard convenience
    p = pattern.replace("*", "%").replace("?", "_")
    if "%" not in p and "_" not in p:
        p = p + "%"
    return p


def parse_sh_dx_args(arg: str | None, now_utc: datetime | None = None) -> ShDxQuery:
    q = ShDxQuery()
    if not arg:
        return q

    now = now_utc or datetime.now(timezone.utc)
    tokens = [t for t in arg.split() if t]
    i = 0

    while i < len(tokens):
        t = tokens[i]
        tl = t.lower()

        if _is_int(t) and q.limit == 10 and i == 0:
            q.limit = max(1, min(int(t), 50))
            i += 1
            continue

        if tl == "exact":
            q.prefix_exact = True
            i += 1
            continue

        if tl in {"by", "spotter"} and i + 1 < len(tokens):
            q.spotter = tokens[i + 1].upper()
            i += 2
            continue

        if tl == "on" and i + 1 < len(tokens):
            fr = _parse_freq_range(tokens[i + 1])
            if fr:
                q.freq_low, q.freq_high = fr
            i += 2
            continue

        if tl == "info" and i + 1 < len(tokens):
            q.info_contains = tokens[i + 1]
            i += 2
            continue

        if tl == "day" and i + 1 < len(tokens) and _is_int(tokens[i + 1]):
            days = max(0, min(int(tokens[i + 1]), 30))
            q.since_epoch = int(now.timestamp()) - days * 86400
            i += 2
            continue

        # first free token is treated as prefix/call pattern
        if q.prefix_pattern is None:
            q.prefix_pattern = _to_like_from_wildcard(t.upper())
            i += 1
            continue

        i += 1

    return q
