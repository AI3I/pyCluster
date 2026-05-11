from __future__ import annotations

from datetime import datetime, timedelta, timezone
import re

from .models import Spot, is_plausible_spot_call, is_plausible_spotter_call, normalize_call


_DX_LINE_RE = re.compile(
    r"^DX\s+de\s+(?P<spotter>\S+):\s+"
    r"(?P<freq>\d+(?:\.\d+)?)\s+"
    r"(?P<dx>\S+)"
    r"(?:\s+(?P<body>.*?))?"
    r"\s+(?P<hhmm>\d{4})Z(?:\s+|$)",
    re.IGNORECASE,
)


def is_rbn_spot(dx_call: str, spotter: str, info: str) -> bool:
    text = f"{dx_call} {spotter} {info}".upper()
    if re.search(r"\bRBN\b|\bSKIMMER\b|\bWPM\b|\bQ:\d+\b|\bZ:\d+\b", text):
        return True
    if re.search(r"\b(?:CW|FT8|FT4|FT2|RTTY|PSK|BEACON)\s+[-+]?\d{1,3}\s*DB\b", text):
        return True
    if re.search(r"\b(?:CQ|TEST)\b", text) and re.search(r"\b[-+]?\d{1,3}\s*DB\b", text):
        return True
    if normalize_call(spotter).endswith("-#") and re.search(r"\b[-+]?\d{1,3}\s*DB\b", text):
        return True
    return False


def _spot_epoch_from_hhmm(hhmm: str, now: datetime) -> int:
    base = now.astimezone(timezone.utc)
    hour = int(hhmm[:2])
    minute = int(hhmm[2:])
    when = base.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if when - base > timedelta(hours=12):
        when -= timedelta(days=1)
    elif base - when > timedelta(hours=12):
        when += timedelta(days=1)
    return int(when.timestamp())


def parse_rbn_dx_line(line: str, *, now: datetime | None = None, source_node: str = "RBN") -> Spot | None:
    text = (line or "").strip()
    m = _DX_LINE_RE.match(text)
    if not m:
        return None
    spotter = normalize_call(m.group("spotter"))
    dx_call = normalize_call(m.group("dx"))
    if not is_plausible_spot_call(dx_call) or not is_plausible_spotter_call(spotter):
        return None
    try:
        freq_khz = float(m.group("freq"))
    except ValueError:
        return None
    body = (m.group("body") or "").strip()
    now_utc = now or datetime.now(timezone.utc)
    epoch = _spot_epoch_from_hhmm(m.group("hhmm"), now_utc)
    return Spot(
        freq_khz=freq_khz,
        dx_call=dx_call,
        epoch=epoch,
        info=body,
        spotter=spotter,
        source_node=normalize_call(source_node or "RBN"),
        raw=text,
    )
