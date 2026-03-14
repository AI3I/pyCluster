"""
BigCTY cty.dat parser for callsign → DXCC entity lookup.
"""
import re
from dataclasses import dataclass
from typing import Optional

@dataclass
class Entity:
    name: str
    cq_zone: int
    itu_zone: int
    continent: str
    lat: float   # degrees N (positive = North)
    lon: float   # degrees E (positive = East, already converted from cty.dat convention)
    prefix: str

_prefix_map: dict[str, Entity] = {}
_exact_map: dict[str, Entity] = {}

def load_cty(path: str) -> None:
    """Load and parse a BigCTY cty.dat file into prefix/exact lookup maps."""
    global _prefix_map, _exact_map
    _prefix_map = {}
    _exact_map = {}

    with open(path, 'r', encoding='ascii', errors='ignore') as f:
        content = f.read()

    # Split on lines that start at column 0 (not whitespace) — each is a new entity header
    blocks = re.split(r'\n(?=[^\s])', content)

    for block in blocks:
        block = block.strip()
        if not block:
            continue

        lines = block.split('\n')
        header = lines[0]

        # Header format: "Name: CQ: ITU: Cont: Lat: Lon: UTC: Prefix:"
        m = re.match(
            r'^(.+?):\s+(\d+):\s+(\d+):\s+(\w{2}):\s+([\d.-]+):\s+([\d.-]+):\s+([\d.-]+):\s+(\S+):\s*$',
            header
        )
        if not m:
            continue

        name, cq, itu, cont, lat, lon_cty, _utc, main_prefix = m.groups()
        # cty.dat convention: positive longitude = West, so negate for standard GIS (positive = East)
        entity = Entity(
            name=name.strip(),
            cq_zone=int(cq),
            itu_zone=int(itu),
            continent=cont,
            lat=float(lat),
            lon=-float(lon_cty),
            prefix=main_prefix,
        )

        # Parse the comma/whitespace-separated prefix list from continuation lines
        prefix_text = ' '.join(lines[1:]).replace(';', '')
        for item in re.split(r'[,\s]+', prefix_text):
            item = item.strip()
            if not item:
                continue
            # Strip inline zone overrides like (14) or [3]
            item = re.sub(r'[(\[][^\])}]*[)\]]', '', item)
            if item.startswith('='):
                # Exact callsign match
                key = item[1:].upper()
                if key:
                    _exact_map[key] = entity
            else:
                key = item.upper()
                if key:
                    _prefix_map[key] = entity


# Standard portable/mobile suffixes that should be stripped before prefix lookup
_STRIP_SUFFIXES = {
    'P', 'M', 'MM', 'AM', 'QRP', 'A', 'B', 'LH', 'LGT', 'JOTA',
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
}

def lookup(callsign: str) -> Optional[Entity]:
    """Return the DXCC Entity for a given callsign, or None if unknown."""
    call = callsign.upper().strip()
    if not call:
        return None

    # Check exact match first (handles /P callsigns listed explicitly in cty.dat)
    if call in _exact_map:
        return _exact_map[call]

    # Decompose on slash to handle PREFIX/CALL or CALL/SUFFIX
    parts = call.split('/')
    if len(parts) == 1:
        base = call
    elif len(parts) == 2:
        left, right = parts
        # If right is a known suffix, use left as the base callsign
        if right in _STRIP_SUFFIXES or (len(right) <= 2 and right.isalpha()):
            base = left
        else:
            # right might be a country prefix (e.g. "K/DL1ABC" → DL prefix)
            # Use whichever part looks more like a prefix (shorter)
            base = right if len(right) <= len(left) else left
    else:
        # Multiple slashes — use first segment
        base = parts[0]

    # Exact match on resolved base
    if base in _exact_map:
        return _exact_map[base]

    # Longest-prefix match
    for length in range(len(base), 0, -1):
        prefix = base[:length]
        if prefix in _prefix_map:
            return _prefix_map[prefix]

    return None
