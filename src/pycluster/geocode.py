from __future__ import annotations

import json
import re
import urllib.parse
import urllib.request

from .maidenhead import coords_to_locator, extract_locator, locator_to_coords


_LATLON_RE = re.compile(
    r"^\s*(?P<lat>[+-]?\d+(?:\.\d+)?)\s*[, ]\s*(?P<lon>[+-]?\d+(?:\.\d+)?)\s*$"
)

_UA = "pyCluster/1.0 (+https://github.com/AI3I/pyCluster)"


def parse_latlon(text: str) -> tuple[float, float] | None:
    m = _LATLON_RE.match(text or "")
    if not m:
        return None
    lat = float(m.group("lat"))
    lon = float(m.group("lon"))
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
        return None
    return lat, lon


def resolve_location_to_coords(text: str, timeout: float = 0.75) -> tuple[float, float] | None:
    raw = (text or "").strip()
    if not raw:
        return None
    direct = parse_latlon(raw)
    if direct is not None:
        return direct
    locator = extract_locator(raw)
    if locator:
        return locator_to_coords(locator)
    if len(raw) < 5 and "," not in raw and " " not in raw:
        return None
    query = urllib.parse.urlencode({"q": raw, "format": "jsonv2", "limit": "1"})
    req = urllib.request.Request(
        f"https://nominatim.openstreetmap.org/search?{query}",
        headers={"User-Agent": _UA},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.load(resp)
    except Exception:
        return None
    if not isinstance(payload, list) or not payload:
        return None
    row = payload[0]
    try:
        lat = float(row.get("lat", ""))
        lon = float(row.get("lon", ""))
    except Exception:
        return None
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
        return None
    return lat, lon


def estimate_location_from_locator(locator: str, timeout: float = 0.75) -> str:
    loc = extract_locator(locator)
    if not loc:
        return ""
    coords = locator_to_coords(loc)
    if coords is None:
        return ""
    lat, lon = coords
    query = urllib.parse.urlencode({"lat": f"{lat:.6f}", "lon": f"{lon:.6f}", "format": "jsonv2"})
    req = urllib.request.Request(
        f"https://nominatim.openstreetmap.org/reverse?{query}",
        headers={"User-Agent": _UA},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.load(resp)
    except Exception:
        return f"Grid {loc}"
    if not isinstance(payload, dict):
        return f"Grid {loc}"
    address = payload.get("address")
    if not isinstance(address, dict):
        return str(payload.get("display_name", "")).split(",", 2)[0].strip() or f"Grid {loc}"
    parts: list[str] = []
    for key in ("city", "town", "village", "hamlet", "county", "state"):
        value = str(address.get(key, "")).strip()
        if value and value not in parts:
            parts.append(value)
        if len(parts) >= 2:
            break
    if parts:
        return ", ".join(parts)
    return str(payload.get("display_name", "")).split(",", 2)[0].strip() or f"Grid {loc}"


def coords_to_location_grid(lat: float, lon: float) -> str:
    return coords_to_locator(lat, lon)
