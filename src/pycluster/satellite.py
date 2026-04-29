from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import math
from pathlib import Path


EARTH_RADIUS_KM = 6378.137
EARTH_MU_KM3_S2 = 398600.4418
EARTH_ROTATION_RAD_S = 7.2921150e-5


@dataclass(frozen=True, slots=True)
class TleRecord:
    name: str
    satnum: str
    epoch: datetime
    inclination_deg: float
    raan_deg: float
    eccentricity: float
    arg_perigee_deg: float
    mean_anomaly_deg: float
    mean_motion_rev_day: float


@dataclass(frozen=True, slots=True)
class SatellitePass:
    aos: datetime
    los: datetime
    max_time: datetime
    max_elevation_deg: float
    max_azimuth_deg: float


def load_tles(path: str | Path) -> list[TleRecord]:
    p = Path(path)
    if not p.exists():
        return []
    raw = [line.strip() for line in p.read_text(encoding="utf-8", errors="replace").splitlines() if line.strip()]
    records: list[TleRecord] = []
    i = 0
    while i < len(raw):
        name = ""
        if raw[i].startswith("1 ") and i + 1 < len(raw) and raw[i + 1].startswith("2 "):
            line1, line2 = raw[i], raw[i + 1]
            i += 2
        elif i + 2 < len(raw) and raw[i + 1].startswith("1 ") and raw[i + 2].startswith("2 "):
            name, line1, line2 = raw[i], raw[i + 1], raw[i + 2]
            i += 3
        else:
            i += 1
            continue
        try:
            records.append(parse_tle(name, line1, line2))
        except (ValueError, IndexError):
            continue
    return records


def find_tle(records: list[TleRecord], target: str) -> TleRecord | None:
    wanted = target.strip().upper()
    if not wanted:
        return None
    for rec in records:
        if wanted == rec.name.upper() or wanted == rec.satnum:
            return rec
    for rec in records:
        if wanted in rec.name.upper():
            return rec
    return None


def parse_tle(name: str, line1: str, line2: str) -> TleRecord:
    satnum = line1[2:7].strip()
    epoch_year = int(line1[18:20])
    epoch_day = float(line1[20:32])
    year = 2000 + epoch_year if epoch_year < 57 else 1900 + epoch_year
    epoch = datetime(year, 1, 1, tzinfo=timezone.utc) + timedelta(days=epoch_day - 1.0)
    tle_name = name.strip() or satnum
    return TleRecord(
        name=tle_name,
        satnum=satnum,
        epoch=epoch,
        inclination_deg=float(line2[8:16]),
        raan_deg=float(line2[17:25]),
        eccentricity=float("0." + line2[26:33].strip()),
        arg_perigee_deg=float(line2[34:42]),
        mean_anomaly_deg=float(line2[43:51]),
        mean_motion_rev_day=float(line2[52:63]),
    )


def predict_passes(
    tle: TleRecord,
    observer_lat_deg: float,
    observer_lon_deg: float,
    *,
    start: datetime | None = None,
    hours: int = 24,
    step_seconds: int = 60,
    min_elevation_deg: float = 0.0,
    limit: int = 3,
) -> list[SatellitePass]:
    start_utc = (start or datetime.now(timezone.utc)).astimezone(timezone.utc)
    step = max(15, int(step_seconds or 60))
    samples = max(1, int(hours * 3600 / step))
    passes: list[SatellitePass] = []
    current: dict[str, object] | None = None

    for idx in range(samples + 1):
        when = start_utc + timedelta(seconds=idx * step)
        az, el = look_angles(tle, observer_lat_deg, observer_lon_deg, when)
        if el >= min_elevation_deg:
            if current is None:
                current = {"aos": when, "los": when, "max_time": when, "max_el": el, "max_az": az}
            else:
                current["los"] = when
                if el > float(current["max_el"]):
                    current["max_el"] = el
                    current["max_az"] = az
                    current["max_time"] = when
        elif current is not None:
            passes.append(
                SatellitePass(
                    aos=current["aos"],  # type: ignore[arg-type]
                    los=current["los"],  # type: ignore[arg-type]
                    max_time=current["max_time"],  # type: ignore[arg-type]
                    max_elevation_deg=float(current["max_el"]),
                    max_azimuth_deg=float(current["max_az"]),
                )
            )
            current = None
            if len(passes) >= limit:
                break

    if current is not None and len(passes) < limit:
        passes.append(
            SatellitePass(
                aos=current["aos"],  # type: ignore[arg-type]
                los=current["los"],  # type: ignore[arg-type]
                max_time=current["max_time"],  # type: ignore[arg-type]
                max_elevation_deg=float(current["max_el"]),
                max_azimuth_deg=float(current["max_az"]),
            )
        )
    return passes[:limit]


def look_angles(tle: TleRecord, observer_lat_deg: float, observer_lon_deg: float, when: datetime) -> tuple[float, float]:
    sat_eci = _satellite_eci_km(tle, when.astimezone(timezone.utc))
    theta = _gmst_rad(when)
    sat_ecef = _eci_to_ecef(sat_eci, theta)
    obs_ecef = _observer_ecef_km(math.radians(observer_lat_deg), math.radians(observer_lon_deg))
    rx = sat_ecef[0] - obs_ecef[0]
    ry = sat_ecef[1] - obs_ecef[1]
    rz = sat_ecef[2] - obs_ecef[2]
    lat = math.radians(observer_lat_deg)
    lon = math.radians(observer_lon_deg)
    east = -math.sin(lon) * rx + math.cos(lon) * ry
    north = -math.sin(lat) * math.cos(lon) * rx - math.sin(lat) * math.sin(lon) * ry + math.cos(lat) * rz
    up = math.cos(lat) * math.cos(lon) * rx + math.cos(lat) * math.sin(lon) * ry + math.sin(lat) * rz
    rng = math.sqrt(east * east + north * north + up * up)
    elevation = math.degrees(math.asin(max(-1.0, min(1.0, up / rng))))
    azimuth = (math.degrees(math.atan2(east, north)) + 360.0) % 360.0
    return azimuth, elevation


def _satellite_eci_km(tle: TleRecord, when: datetime) -> tuple[float, float, float]:
    n = tle.mean_motion_rev_day * 2.0 * math.pi / 86400.0
    semi_major = (EARTH_MU_KM3_S2 / (n * n)) ** (1.0 / 3.0)
    dt = (when - tle.epoch).total_seconds()
    mean_anomaly = math.radians(tle.mean_anomaly_deg) + n * dt
    ecc = max(0.0, min(0.25, tle.eccentricity))
    eccentric_anomaly = _solve_kepler(mean_anomaly, ecc)
    x_orb = semi_major * (math.cos(eccentric_anomaly) - ecc)
    y_orb = semi_major * math.sqrt(1.0 - ecc * ecc) * math.sin(eccentric_anomaly)

    raan = math.radians(tle.raan_deg)
    inc = math.radians(tle.inclination_deg)
    argp = math.radians(tle.arg_perigee_deg)
    cos_o, sin_o = math.cos(raan), math.sin(raan)
    cos_i, sin_i = math.cos(inc), math.sin(inc)
    cos_w, sin_w = math.cos(argp), math.sin(argp)

    x = (cos_o * cos_w - sin_o * sin_w * cos_i) * x_orb + (-cos_o * sin_w - sin_o * cos_w * cos_i) * y_orb
    y = (sin_o * cos_w + cos_o * sin_w * cos_i) * x_orb + (-sin_o * sin_w + cos_o * cos_w * cos_i) * y_orb
    z = (sin_w * sin_i) * x_orb + (cos_w * sin_i) * y_orb
    return x, y, z


def _solve_kepler(mean_anomaly: float, eccentricity: float) -> float:
    e_anomaly = mean_anomaly
    for _ in range(8):
        e_anomaly -= (e_anomaly - eccentricity * math.sin(e_anomaly) - mean_anomaly) / (
            1.0 - eccentricity * math.cos(e_anomaly)
        )
    return e_anomaly


def _observer_ecef_km(lat: float, lon: float) -> tuple[float, float, float]:
    return (
        EARTH_RADIUS_KM * math.cos(lat) * math.cos(lon),
        EARTH_RADIUS_KM * math.cos(lat) * math.sin(lon),
        EARTH_RADIUS_KM * math.sin(lat),
    )


def _eci_to_ecef(eci: tuple[float, float, float], theta: float) -> tuple[float, float, float]:
    cos_t, sin_t = math.cos(theta), math.sin(theta)
    x, y, z = eci
    return cos_t * x + sin_t * y, -sin_t * x + cos_t * y, z


def _gmst_rad(when: datetime) -> float:
    unix_seconds = when.astimezone(timezone.utc).timestamp()
    jd = unix_seconds / 86400.0 + 2440587.5
    d = jd - 2451545.0
    gmst_deg = 280.46061837 + 360.98564736629 * d
    return math.radians(gmst_deg % 360.0)
