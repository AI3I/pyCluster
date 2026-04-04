from __future__ import annotations

from dataclasses import dataclass
import re


@dataclass(slots=True)
class PrefixLocation:
    name: str
    cq_zone: int
    itu_zone: int
    continent: str
    lat: float
    lon: float
    prefix: str


_prefix_map: dict[str, PrefixLocation] = {}
_exact_map: dict[str, PrefixLocation] = {}
_STRIP_SUFFIXES = {
    "P", "M", "MM", "AM", "QRP", "A", "B", "LH", "LGT", "JOTA",
    "0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
}
_LINE_RE = re.compile(
    r"^(?P<prefixes>\S+)\s+"
    r"(?P<name>.+?)\s+"
    r"(?P<dxcc>\d+)\s+"
    r"(?P<cq>\d+)\s+"
    r"(?P<itu>\d+)\s+"
    r"(?P<utc>-?\d+(?:\.\d+)?)\s+"
    r"(?P<lat_deg>\d+)\s+"
    r"(?P<lat_min>\d+(?:\.\d+)?)\s+"
    r"(?P<lat_sec>\d+(?:\.\d+)?)\s+"
    r"(?P<lat_hemi>[NS])\s+"
    r"(?P<lon_deg>\d+)\s+"
    r"(?P<lon_min>\d+(?:\.\d+)?)\s+"
    r"(?P<lon_sec>\d+(?:\.\d+)?)\s+"
    r"(?P<lon_hemi>[EW])\s*(?P<master>@)?\s*$"
)


def _deg_min_to_float(deg: str, minutes: str, seconds: str, hemi: str) -> float:
    value = float(deg) + (float(minutes) / 60.0) + (float(seconds) / 3600.0)
    if hemi in {"S", "W"}:
        value = -value
    return value


def _store_token(token: str, loc: PrefixLocation) -> None:
    raw = token.strip().upper()
    if not raw:
        return
    exact = raw.startswith("=")
    key = raw[1:] if exact else raw
    if not key:
        return
    if exact:
        _exact_map.setdefault(key, loc)
    else:
        _prefix_map.setdefault(key, loc)


def load_wpxloc(path: str) -> None:
    global _prefix_map, _exact_map
    _prefix_map = {}
    _exact_map = {}
    current: PrefixLocation | None = None
    with open(path, "r", encoding="ascii", errors="ignore") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("&"):
                if current is None:
                    continue
                for token in re.split(r"[\s,]+", line[1:].strip()):
                    _store_token(token, current)
                continue
            m = _LINE_RE.match(line)
            if not m:
                continue
            tokens = [tok.strip().upper() for tok in m.group("prefixes").split(",") if tok.strip()]
            if not tokens:
                continue
            current = PrefixLocation(
                name=m.group("name").strip().replace("-", " "),
                cq_zone=int(m.group("cq")),
                itu_zone=int(m.group("itu")),
                continent="",
                lat=_deg_min_to_float(m.group("lat_deg"), m.group("lat_min"), m.group("lat_sec"), m.group("lat_hemi")),
                lon=_deg_min_to_float(m.group("lon_deg"), m.group("lon_min"), m.group("lon_sec"), m.group("lon_hemi")),
                prefix=tokens[0],
            )
            for token in tokens:
                _store_token(token, current)


def is_loaded() -> bool:
    return bool(_prefix_map or _exact_map)


def lookup(callsign: str) -> PrefixLocation | None:
    call = str(callsign or "").strip().upper()
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
