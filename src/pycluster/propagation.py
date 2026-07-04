from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import math
import xml.etree.ElementTree as ET

from .geomag import WwvReading, parse_wwv_text


@dataclass(slots=True)
class SolarSnapshot:
    sfi: int | None = None
    a_index: int | None = None
    k_index: int | None = None
    sunspots: int | None = None
    xray: str = ""
    solarwind: str = ""
    aurora: str = ""
    updated: str = ""
    source: str = ""
    forecast: str = ""
    epoch: int = 0
    conditions: dict[str, str] = field(default_factory=dict)
    vhf: list[dict[str, str]] = field(default_factory=list)


def estimate_muf3000(sfi: int | float | None) -> float | None:
    if sfi is None:
        return None
    return 8.0 + 0.12 * float(sfi)


def estimate_sunspots_from_sfi(sfi: int | float | None) -> int | None:
    if sfi is None:
        return None
    return max(0, int(round((float(sfi) - 85.0) * 2.0)))


def minimuf_sunspots_from_sfi(sfi: int | float | None) -> float | None:
    if sfi is None:
        return None
    flux = float(sfi)
    if flux < 65:
        return 0.0
    if flux < 110:
        temp = flux - 200.6
        return 108.36 - 0.005896 * temp * temp
    if flux < 213:
        return 60.0 + 1.0680 * (flux - 110.0)
    temp = flux - 652.9
    return 384.0 - 0.0011059 * temp * temp


def _sgn(value: float) -> int:
    if value == 0:
        return 0
    return 1 if value > 0 else -1


def minimuf35_muf(
    sfi: int | float | None,
    when: datetime,
    lat1_deg: float,
    lon1_deg: float,
    lat2_deg: float,
    lon2_deg: float,
) -> float | None:
    """Return a MINIMUF 3.5-style path MUF in MHz.

    This follows the QST December 1982 MINIMUF 3.5 geometry used by
    traditional DXCluster implementations. Input longitudes are normal
    east-positive degrees; the model internally uses west-positive radians.
    """
    ssn = minimuf_sunspots_from_sfi(sfi)
    if ssn is None:
        return None
    dt = when.astimezone(timezone.utc)
    month = dt.month
    day = dt.day
    hour = dt.hour
    lat1 = math.radians(float(lat1_deg))
    lat2 = math.radians(float(lat2_deg))
    lon1 = math.radians(-float(lon1_deg))
    lon2 = math.radians(-float(lon2_deg))
    halfpi = math.pi / 2.0
    pi2 = math.pi * 2.0

    ftemp = math.sin(lat1) * math.sin(lat2) + math.cos(lat1) * math.cos(lat2) * math.cos(lon2 - lon1)
    ftemp = max(-1.0, min(1.0, ftemp))
    dist = math.acos(ftemp)
    if dist <= 0.0:
        return None
    k6 = max(1.0, 1.59 * dist)
    p = math.sin(lat2)
    q = math.cos(lat2)
    denom = q * math.sin(dist)
    if abs(denom) < 1e-9:
        return None
    a = (math.sin(lat1) - p * math.cos(dist)) / denom
    y1 = 0.0172 * (10.0 + (month - 1) * 30.4 + day)
    y2 = 0.409 * math.cos(y1)
    ftemp = min(halfpi, 2.5 * dist / k6)
    ftemp = math.sin(ftemp)
    m9 = 1.0 + 2.5 * ftemp * math.sqrt(ftemp)
    muf = 100.0

    step = abs(0.9999 - 1.0 / k6)
    if step <= 0.0:
        step = 1.0
    k1 = 1.0 / (2.0 * k6)
    while k1 <= 1.0 - 1.0 / (2.0 * k6) + 1e-9:
        gtemp = dist * k1
        ftemp = p * math.cos(gtemp) + q * math.sin(gtemp) * a
        ftemp = max(-1.0, min(1.0, ftemp))
        y3 = halfpi - math.acos(ftemp)
        root = max(1e-12, 1.0 - ftemp * ftemp)
        denom2 = q * math.sqrt(root)
        if abs(denom2) < 1e-9:
            k1 += step
            continue
        ftemp = (math.cos(gtemp) - ftemp * p) / denom2
        ftemp = max(-1.0, min(1.0, ftemp))
        ftemp = lon2 + _sgn(math.sin(lon1 - lon2)) * math.acos(ftemp)
        if ftemp < 0:
            ftemp += pi2
        if ftemp >= pi2:
            ftemp -= pi2
        ftemp = 3.82 * ftemp + 12.0 + 0.13 * (math.sin(y1) + 1.2 * math.sin(2.0 * y1))
        k8 = ftemp - 12.0 * (1 + _sgn(ftemp - 24.0)) * _sgn(abs(ftemp - 24.0))
        if math.cos(y3 + y2) <= -0.26:
            k9 = 0.0
            g0 = 0.0
        else:
            denom3 = math.cos(y2) * math.cos(y3) + 0.001
            ftemp = (-0.26 + math.sin(y2) * math.sin(y3)) / denom3
            root = max(1e-12, abs(1.0 - ftemp * ftemp))
            k9 = 12.0 - math.atan(ftemp / math.sqrt(root)) * 7.639437
            if abs(k9) < 1e-9:
                g0 = 0.0
            else:
                t = k8 - k9 / 2.0 + 12.0 * (1 - _sgn(k8 - k9 / 2.0)) * _sgn(abs(k8 - k9 / 2.0))
                t4 = k8 + k9 / 2.0 - 12.0 * (1 + _sgn(k8 + k9 / 2.0 - 24.0)) * _sgn(abs(k8 + k9 / 2.0 - 24.0))
                c0 = abs(math.cos(y3 + y2))
                t9 = max(0.1, 9.7 * (c0 ** 9.6))
                g8 = math.pi * t9 / k9
                if (t4 < t and (hour - t4) * (t - hour) > 0.0) or (t4 >= t and (hour - t) * (t4 - hour) <= 0.0):
                    ftemp = hour + 12.0 * (1 + _sgn(t4 - hour)) * _sgn(abs(t4 - hour))
                    ftemp = (t4 - ftemp) / 2.0
                    g0 = c0 * (g8 * (math.exp(-k9 / t9) + 1.0)) * math.exp(ftemp) / (1.0 + g8 * g8)
                else:
                    ftemp = hour + 12.0 * (1 + _sgn(t - hour)) * _sgn(abs(t - hour))
                    gtemp = math.pi * (ftemp - t) / k9
                    ftemp = (t - ftemp) / t9
                    g0 = c0 * (math.sin(gtemp) + g8 * (math.exp(ftemp) - math.cos(gtemp))) / (1.0 + g8 * g8)
                    floor = c0 * (g8 * (math.exp(-k9 / t9) + 1.0)) * math.exp((k9 - 24.0) / 2.0) / (1.0 + g8 * g8)
                    if g0 < floor:
                        g0 = floor
        ftemp = (1.0 + ssn / 250.0) * m9 * math.sqrt(6.0 + 58.0 * math.sqrt(max(0.0, g0)))
        ftemp *= 1.0 - 0.1 * math.exp((k9 - 24.0) / 3.0)
        ftemp *= 1.0 + 0.1 * (1 - _sgn(lat1) * _sgn(lat2))
        ftemp *= 1.0 - 0.1 * (1 + _sgn(abs(math.sin(y3)) - math.cos(y3)))
        if ftemp < muf:
            muf = ftemp
        k1 += step
    return max(0.0, muf)


def effective_muf_for_zenith(muf_mhz: float, zenith_deg: float) -> float:
    if zenith_deg >= 90.0:
        return 0.0
    daylight_factor = max(0.0, math.cos(math.radians(max(0.0, zenith_deg))))
    return muf_mhz * (0.45 + 0.55 * daylight_factor)


def signal_report_for_muf(
    freq_mhz: float,
    muf_mhz: float,
    zenith_deg: float,
    zenith_samples: tuple[float, ...] | None = None,
) -> str:
    effective_muf = effective_muf_for_zenith(muf_mhz, zenith_deg)
    samples = zenith_samples or (zenith_deg,)
    daylight_strength = max(max(0.0, math.cos(math.radians(max(0.0, min(90.0, item))))) for item in samples)
    if freq_mhz <= 4.0 and daylight_strength > 0.05:
        return ""
    d_layer_penalty = 0.0
    if daylight_strength > 0.0 and freq_mhz < 10.5:
        d_layer_penalty = ((10.5 - freq_mhz) / 8.7) * daylight_strength * 28.0
    score = (effective_muf - freq_mhz) - d_layer_penalty
    if score < -1.0:
        return ""
    if score < 0.5:
        return "sS0"
    if score < 2.0:
        return "mS1"
    if score < 4.0:
        return "S1+"
    if score < 6.0:
        return "S2"
    if score < 8.0:
        return "S2+"
    return "S6"


def snapshot_from_wwv(reading: WwvReading, *, epoch: int = 0, updated: str = "", source: str = "wwv") -> SolarSnapshot:
    return SolarSnapshot(
        sfi=int(reading.sfi),
        a_index=int(reading.a_index),
        k_index=int(reading.k_index),
        sunspots=estimate_sunspots_from_sfi(reading.sfi),
        updated=updated,
        source=source,
        forecast=reading.forecast,
        epoch=int(epoch or 0),
    )


def latest_wwv_snapshot(rows, *, now_epoch: int | None = None, max_age_seconds: int = 12 * 60 * 60) -> SolarSnapshot | None:
    now = int(now_epoch if now_epoch is not None else datetime.now(timezone.utc).timestamp())
    for row in rows:
        try:
            epoch = int(row["epoch"])
            body = str(row["body"] or "")
        except Exception:
            continue
        if max_age_seconds > 0 and epoch > 0 and now - epoch > max_age_seconds:
            continue
        reading = parse_wwv_text(body)
        if reading is None:
            continue
        sender = ""
        try:
            sender = str(row["sender"] or "").strip()
        except Exception:
            sender = ""
        updated = datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%d %H:%MZ") if epoch > 0 else ""
        return snapshot_from_wwv(reading, epoch=epoch, updated=updated, source=f"wwv:{sender}" if sender else "wwv")
    return None


def parse_hamqsl_solar_xml(xml_bytes: bytes) -> SolarSnapshot:
    root = ET.fromstring(xml_bytes)
    sd = root.find("solardata")

    def text(tag: str) -> str:
        el = sd.find(tag) if sd is not None else None
        return el.text.strip() if (el is not None and el.text) else ""

    def int_text(tag: str) -> int | None:
        raw = text(tag)
        if not raw:
            return None
        try:
            return int(float(raw))
        except ValueError:
            return None

    conditions: dict[str, str] = {}
    if sd is not None:
        for band in sd.findall("calculatedconditions/band"):
            conditions[f"{band.get('name', '')}_{band.get('time', '')}"] = band.text.strip() if band.text else ""
    vhf: list[dict[str, str]] = []
    if sd is not None:
        for phenomenon in sd.findall("calculatedvhfconditions/phenomenon"):
            vhf.append(
                {
                    "name": phenomenon.get("name", ""),
                    "location": phenomenon.get("location", ""),
                    "condition": phenomenon.text.strip() if phenomenon.text else "",
                }
            )
    return SolarSnapshot(
        sfi=int_text("solarflux"),
        a_index=int_text("aindex"),
        k_index=int_text("kindex"),
        sunspots=int_text("sunspots"),
        xray=text("xray"),
        solarwind=text("solarwind"),
        aurora=text("aurora"),
        updated=text("updated"),
        source="hamqsl",
        conditions=conditions,
        vhf=vhf,
    )


def merge_solar_snapshots(primary: SolarSnapshot | None, fallback: SolarSnapshot | None) -> SolarSnapshot | None:
    if primary is None:
        return fallback
    if fallback is None:
        return primary
    return SolarSnapshot(
        sfi=primary.sfi if primary.sfi is not None else fallback.sfi,
        a_index=primary.a_index if primary.a_index is not None else fallback.a_index,
        k_index=primary.k_index if primary.k_index is not None else fallback.k_index,
        sunspots=primary.sunspots if primary.sunspots is not None else fallback.sunspots,
        xray=primary.xray or fallback.xray,
        solarwind=primary.solarwind or fallback.solarwind,
        aurora=primary.aurora or fallback.aurora,
        updated=primary.updated or fallback.updated,
        source=primary.source or fallback.source,
        forecast=primary.forecast or fallback.forecast,
        epoch=primary.epoch or fallback.epoch,
        conditions=primary.conditions or fallback.conditions,
        vhf=primary.vhf or fallback.vhf,
    )


def snapshot_payload(snapshot: SolarSnapshot) -> dict[str, object]:
    muf = estimate_muf3000(snapshot.sfi)
    return {
        "sfi": "" if snapshot.sfi is None else str(snapshot.sfi),
        "sn": "" if snapshot.sunspots is None else str(snapshot.sunspots),
        "a": "" if snapshot.a_index is None else str(snapshot.a_index),
        "k": "" if snapshot.k_index is None else str(snapshot.k_index),
        "xray": snapshot.xray,
        "solarwind": snapshot.solarwind,
        "aurora": snapshot.aurora,
        "updated": snapshot.updated,
        "source": snapshot.source,
        "forecast": snapshot.forecast,
        "muf3000": "" if muf is None else f"{muf:.1f}",
        "conditions": snapshot.conditions,
        "vhf": snapshot.vhf,
    }
