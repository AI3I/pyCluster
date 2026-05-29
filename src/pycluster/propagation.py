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
