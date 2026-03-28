from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import re


CALL_RE = re.compile(r"^[A-Z0-9/]+(?:-[0-9]{1,2})?$")


@dataclass(slots=True)
class Spot:
    freq_khz: float
    dx_call: str
    epoch: int
    info: str
    spotter: str
    source_node: str
    raw: str

    @property
    def when_utc(self) -> datetime:
        return datetime.fromtimestamp(self.epoch, tz=timezone.utc)


def normalize_call(value: str) -> str:
    return value.strip().upper()


def display_call(value: str) -> str:
    call = normalize_call(value)
    if "-" in call:
        return call.split("-", 1)[0]
    return call


def is_valid_call(value: str) -> bool:
    call = normalize_call(value)
    if not CALL_RE.match(call):
        return False
    if "//" in call or call.startswith("/") or call.endswith("/"):
        return False
    core = call
    if "-" in core:
        core, ssid = core.rsplit("-", 1)
        if not ssid.isdigit():
            return False
    if "/" in core:
        parts = [p for p in core.split("/") if p]
        if len(parts) < 2:
            return False
        if not all(part.isalnum() for part in parts):
            return False
    else:
        parts = [core]
    joined = "".join(parts)
    return any(ch.isalpha() for ch in joined) and any(ch.isdigit() for ch in joined)


def parse_spot_record(line: str) -> Spot:
    parts = line.rstrip("\n").split("^")
    if len(parts) < 8:
        raise ValueError("spot line has fewer than 8 caret-separated fields")

    freq_khz = float(parts[0])
    dx_call = normalize_call(parts[1])
    epoch = int(parts[2])
    info = parts[3]
    spotter = normalize_call(parts[4])
    source_node = normalize_call(parts[7])

    return Spot(
        freq_khz=freq_khz,
        dx_call=dx_call,
        epoch=epoch,
        info=info,
        spotter=spotter,
        source_node=source_node,
        raw=line.rstrip("\n"),
    )
