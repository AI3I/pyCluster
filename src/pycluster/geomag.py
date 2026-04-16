from __future__ import annotations

from dataclasses import dataclass
import re


@dataclass(slots=True)
class WcyReading:
    sfi: int
    a_index: int
    k_index: int
    expk: int
    sunspots: int
    sun_activity: str
    geomagnetic_field: str
    aurora: str

    @property
    def body(self) -> str:
        return (
            f"SFI={self.sfi} A={self.a_index} K={self.k_index} "
            f"ExpK={self.expk} R={self.sunspots} SA={self.sun_activity} "
            f"GMF={self.geomagnetic_field} Aurora={self.aurora}"
        )


@dataclass(slots=True)
class WwvReading:
    sfi: int
    a_index: int
    k_index: int
    forecast: str

    @property
    def body(self) -> str:
        return f"SFI={self.sfi} A={self.a_index} K={self.k_index} {self.forecast}".strip()


def derive_wcy_from_wwv(reading: WwvReading) -> WcyReading:
    forecast = str(reading.forecast or "").strip().lower()
    sunspots = max(0, int(round((reading.sfi - 85) * 2)))
    if any(word in forecast for word in ("major", "severe", "g3", "g4", "g5", "active")):
        sun_activity = "act"
    else:
        sun_activity = "qui"
    if any(word in forecast for word in ("major", "g3", "g4", "g5")):
        geomagnetic_field = "maj"
    elif any(word in forecast for word in ("moderate", "minor", "g1", "g2", "unsettled")):
        geomagnetic_field = "mod"
    else:
        geomagnetic_field = "qui"
    aurora = "yes" if reading.k_index >= 4 or "aurora" in forecast or "storm" in forecast else "no"
    return WcyReading(
        sfi=int(reading.sfi),
        a_index=int(reading.a_index),
        k_index=int(reading.k_index),
        expk=int(reading.k_index),
        sunspots=sunspots,
        sun_activity=sun_activity,
        geomagnetic_field=geomagnetic_field,
        aurora=aurora,
    )


def _parse_pairs(text: str) -> tuple[dict[str, str], str]:
    raw = (text or "").strip()
    if not raw:
        return {}, ""
    if "," not in raw:
        return {}, raw
    tokens = [tok.strip() for tok in raw.split(",") if tok.strip()]
    if not tokens:
        return {}, ""
    if all("=" in tok for tok in tokens):
        pairs = {}
        for tok in tokens:
            key, value = tok.split("=", 1)
            pairs[key.strip().lower()] = value.strip()
        return pairs, ""
    if len(tokens) > 1 and all("=" in tok for tok in tokens[:-1]):
        pairs = {}
        for tok in tokens[:-1]:
            key, value = tok.split("=", 1)
            pairs[key.strip().lower()] = value.strip()
        return pairs, tokens[-1].strip()
    return {}, raw


def _find_int(patterns: tuple[str, ...], text: str) -> int | None:
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return int(match.group(1))
    return None


def _find_word(patterns: tuple[str, ...], text: str) -> str | None:
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return None


def parse_wcy_text(text: str) -> WcyReading | None:
    raw = (text or "").strip()
    if not raw:
        return None
    pairs, _ = _parse_pairs(raw)
    if pairs:
        try:
            return WcyReading(
                sfi=int(pairs["sf"]),
                a_index=int(pairs["a"]),
                k_index=int(pairs["k"]),
                expk=int(pairs["expk"]),
                sunspots=int(pairs["r"]),
                sun_activity=pairs["sa"],
                geomagnetic_field=pairs["gmf"],
                aurora=pairs["au"],
            )
        except (KeyError, ValueError):
            return None

    sfi = _find_int((r"\bSFI\s*=\s*(\d+)\b", r"\bSF\s*=\s*(\d+)\b"), raw)
    a_index = _find_int((r"\bA\s*=\s*(\d+)\b",), raw)
    k_index = _find_int((r"\bK\s*=\s*(\d+)\b",), raw)
    expk = _find_int((r"\bEXPK\s*=\s*(\d+)\b", r"\bEXP\.?K\s*=\s*(\d+)\b"), raw)
    sunspots = _find_int((r"\bR\s*=\s*(\d+)\b", r"\bSPOTS\s*=\s*(\d+)\b"), raw)
    sun_activity = _find_word((r"\bSA\s*=\s*([A-Z]+)\b", r"\bAURORA\s*=\s*([A-Z]+)\b"), raw)
    geomagnetic_field = _find_word((r"\bGMF\s*=\s*([A-Z]+)\b", r"\bXRAY\s*=\s*([A-Z]+)\b"), raw)
    aurora = _find_word((r"\bSTORM\s*=\s*([A-Z]+)\b", r"\bAU\s*=\s*([A-Z]+)\b", r"\bAURORA\s*=\s*([A-Z]+)\b"), raw)
    if None in {sfi, a_index, k_index, expk, sunspots, sun_activity, geomagnetic_field, aurora}:
        return None
    return WcyReading(
        sfi=int(sfi),
        a_index=int(a_index),
        k_index=int(k_index),
        expk=int(expk),
        sunspots=int(sunspots),
        sun_activity=str(sun_activity).lower(),
        geomagnetic_field=str(geomagnetic_field).lower(),
        aurora=str(aurora).lower(),
    )


def parse_wwv_text(text: str) -> WwvReading | None:
    raw = (text or "").strip()
    if not raw:
        return None
    pairs, forecast = _parse_pairs(raw)
    if pairs:
        try:
            sfi = int(pairs["sf"])
            a_index = int(pairs["a"])
            k_index = int(pairs["k"])
        except (KeyError, ValueError):
            return None
        return WwvReading(sfi=sfi, a_index=a_index, k_index=k_index, forecast=forecast)

    sfi = _find_int((r"\bSFI\s*=\s*(\d+)\b", r"\bSF\s*=\s*(\d+)\b"), raw)
    a_index = _find_int((r"\bA\s*=\s*(\d+)\b",), raw)
    k_index = _find_int((r"\bK\s*=\s*(\d+)\b",), raw)
    if None in {sfi, a_index, k_index}:
        return None
    forecast = raw
    forecast = re.sub(r"^\s*(?:SFI|SF)\s*=\s*\d+\s*", "", forecast, flags=re.IGNORECASE)
    forecast = re.sub(r"\bA\s*=\s*\d+\s*", "", forecast, flags=re.IGNORECASE)
    forecast = re.sub(r"\bK\s*=\s*\d+\s*", "", forecast, flags=re.IGNORECASE).strip(" ,")
    return WwvReading(
        sfi=int(sfi),
        a_index=int(a_index),
        k_index=int(k_index),
        forecast=forecast,
    )


def canonicalize_wcy_text(text: str) -> str | None:
    reading = parse_wcy_text(text)
    return reading.body if reading is not None else None


def canonicalize_wwv_text(text: str) -> str | None:
    reading = parse_wwv_text(text)
    return reading.body if reading is not None else None
