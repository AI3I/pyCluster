from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(slots=True)
class Entity:
    name: str
    cq_zone: int
    itu_zone: int
    continent: str
    lat: float
    lon: float
    prefix: str


_prefix_map: dict[str, Entity] = {}
_exact_map: dict[str, Entity] = {}
_STRIP_SUFFIXES = {
    "P", "M", "MM", "AM", "QRP", "A", "B", "LH", "LGT", "JOTA",
    "0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
}


def load_cty(path: str) -> None:
    global _prefix_map, _exact_map
    _prefix_map = {}
    _exact_map = {}
    with open(path, "r", encoding="ascii", errors="ignore") as f:
        content = f.read()
    blocks = re.split(r"\n(?=[^\s])", content)
    for block in blocks:
        block = block.strip()
        if not block:
            continue
        lines = block.split("\n")
        header = lines[0]
        m = re.match(
            r"^(.+?):\s+(\d+):\s+(\d+):\s+(\w{2}):\s+([\d.-]+):\s+([\d.-]+):\s+([\d.-]+):\s+(\S+):\s*$",
            header,
        )
        if not m:
            continue
        name, cq, itu, cont, lat, lon_cty, _utc, main_prefix = m.groups()
        entity = Entity(
            name=name.strip(),
            cq_zone=int(cq),
            itu_zone=int(itu),
            continent=cont,
            lat=float(lat),
            lon=-float(lon_cty),
            prefix=main_prefix,
        )
        prefix_text = " ".join(lines[1:]).replace(";", "")
        for item in re.split(r"[,\s]+", prefix_text):
            item = item.strip()
            if not item:
                continue
            item = re.sub(r"[(\[][^\])}]*[)\]]", "", item)
            if item.startswith("="):
                key = item[1:].upper()
                if key:
                    _exact_map[key] = entity
            else:
                key = item.upper()
                if key:
                    _prefix_map[key] = entity


def lookup(callsign: str) -> Entity | None:
    call = callsign.upper().strip()
    if not call:
        return None
    if call in _exact_map:
        return _exact_map[call]
    parts = call.split("/")
    if len(parts) == 1:
        base = call
    elif len(parts) == 2:
        left, right = parts
        if right in _STRIP_SUFFIXES or (len(right) <= 2 and right.isalpha()):
            base = left
        else:
            base = right if len(right) <= len(left) else left
    else:
        base = parts[0]
    if base in _exact_map:
        return _exact_map[base]
    for length in range(len(base), 0, -1):
        prefix = base[:length]
        if prefix in _prefix_map:
            return _prefix_map[prefix]
    return None
