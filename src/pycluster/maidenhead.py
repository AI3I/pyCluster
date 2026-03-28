from __future__ import annotations

import re


_LOCATOR_RE = re.compile(r"\b([A-R]{2}\d{2}(?:[A-X]{2}(?:\d{2})?)?)\b", re.IGNORECASE)


def extract_locator(text: str) -> str:
    match = _LOCATOR_RE.search(str(text or "").strip().upper())
    return match.group(1).upper() if match else ""


def locator_to_coords(locator: str) -> tuple[float, float] | None:
    txt = re.sub(r"[^A-Z0-9]", "", str(locator or "").strip().upper())
    if len(txt) < 4 or len(txt) % 2 != 0:
        return None
    txt = txt[:8]
    lon = -180.0
    lat = -90.0
    lon_step = 20.0
    lat_step = 10.0
    try:
        lon += (ord(txt[0]) - ord("A")) * lon_step
        lat += (ord(txt[1]) - ord("A")) * lat_step
        lon_step /= 10.0
        lat_step /= 10.0
        lon += int(txt[2]) * lon_step
        lat += int(txt[3]) * lat_step
        if len(txt) >= 6:
            lon_step /= 24.0
            lat_step /= 24.0
            lon += (ord(txt[4]) - ord("A")) * lon_step
            lat += (ord(txt[5]) - ord("A")) * lat_step
        if len(txt) >= 8:
            lon_step /= 10.0
            lat_step /= 10.0
            lon += int(txt[6]) * lon_step
            lat += int(txt[7]) * lat_step
    except (TypeError, ValueError):
        return None
    return lat + lat_step / 2.0, lon + lon_step / 2.0


def coords_to_locator(lat: float, lon: float, precision_pairs: int = 3) -> str:
    lat_v = max(-90.0, min(90.0, float(lat)))
    lon_v = max(-180.0, min(180.0, float(lon)))
    if lon_v == 180.0:
        lon_v = 179.999999
    if lat_v == 90.0:
        lat_v = 89.999999
    lon_adj = lon_v + 180.0
    lat_adj = lat_v + 90.0
    parts: list[str] = []
    lon_step = 20.0
    lat_step = 10.0

    field_lon = int(lon_adj // lon_step)
    field_lat = int(lat_adj // lat_step)
    parts.extend([chr(ord("A") + field_lon), chr(ord("A") + field_lat)])
    lon_adj -= field_lon * lon_step
    lat_adj -= field_lat * lat_step

    if precision_pairs >= 2:
        lon_step /= 10.0
        lat_step /= 10.0
        square_lon = int(lon_adj // lon_step)
        square_lat = int(lat_adj // lat_step)
        parts.extend([str(square_lon), str(square_lat)])
        lon_adj -= square_lon * lon_step
        lat_adj -= square_lat * lat_step

    if precision_pairs >= 3:
        lon_step /= 24.0
        lat_step /= 24.0
        subsquare_lon = int(lon_adj // lon_step)
        subsquare_lat = int(lat_adj // lat_step)
        parts.extend([chr(ord("A") + subsquare_lon), chr(ord("A") + subsquare_lat)])

    if precision_pairs >= 4:
        lon_adj -= subsquare_lon * lon_step
        lat_adj -= subsquare_lat * lat_step
        lon_step /= 10.0
        lat_step /= 10.0
        ext_lon = int(lon_adj // lon_step)
        ext_lat = int(lat_adj // lat_step)
        parts.extend([str(ext_lon), str(ext_lat)])

    return "".join(parts)
