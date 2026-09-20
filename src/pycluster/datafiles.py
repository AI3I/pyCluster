from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
import re

from .satellite import load_tles

_VERSION_RE = re.compile(r"VER(\d{8})")
_TEXT_DATE_RE = re.compile(r"\b(\d{1,2}\s+[A-Za-z]+\s+\d{4})\b")
_STALE_DAYS = 180


@dataclass(slots=True)
class DataFileStatus:
    name: str
    path: str
    configured: bool
    exists: bool
    loaded: bool
    version: str
    version_date: str
    modified_iso: str
    size_bytes: int
    stale: bool
    status: str
    note: str

    def to_json(self) -> dict[str, object]:
        return asdict(self)


def _parse_version_date(raw: str) -> tuple[str, str]:
    m = _VERSION_RE.search(raw)
    if m:
        digits = m.group(1)
        try:
            dt = datetime.strptime(digits, "%Y%m%d").replace(tzinfo=timezone.utc)
        except ValueError:
            return f"VER{digits}", ""
        return f"VER{digits}", dt.date().isoformat()
    m = _TEXT_DATE_RE.search(raw)
    if m:
        text = m.group(1)
        for fmt in ("%d %B %Y", "%d %b %Y"):
            try:
                dt = datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            return text, dt.date().isoformat()
    return "", ""


def describe_data_file(name: str, path: str, *, loaded: bool = False) -> DataFileStatus:
    raw_path = str(path or "").strip()
    if not raw_path:
        return DataFileStatus(name=name, path="", configured=False, exists=False, loaded=False, version="", version_date="", modified_iso="", size_bytes=0, stale=False, status="unconfigured", note="No file is configured.")
    p = Path(raw_path)
    if not p.exists() or not p.is_file():
        return DataFileStatus(name=name, path=raw_path, configured=True, exists=False, loaded=False, version="", version_date="", modified_iso="", size_bytes=0, stale=False, status="missing", note="Configured file was not found.")
    st = p.stat()
    modified = datetime.fromtimestamp(st.st_mtime, tz=timezone.utc)
    version = ""
    version_date = ""
    try:
        sample = p.read_text(encoding="ascii", errors="ignore")[:262144]
    except Exception:
        sample = ""
    if sample:
        version, version_date = _parse_version_date(sample)
    ref_dt = modified
    if version_date:
        try:
            ref_dt = datetime.fromisoformat(version_date).replace(tzinfo=timezone.utc)
        except ValueError:
            ref_dt = modified
    age_days = max(0, int((datetime.now(timezone.utc) - ref_dt).total_seconds() // 86400))
    stale = age_days > _STALE_DAYS
    status = "loaded" if loaded else "available"
    note = f"Loaded from {raw_path}." if loaded else f"Available at {raw_path}."
    if stale:
        status = "stale" if loaded else "available_stale"
        note = f"File is {age_days} days old."
    return DataFileStatus(name=name, path=raw_path, configured=True, exists=True, loaded=loaded, version=version, version_date=version_date, modified_iso=modified.isoformat(), size_bytes=int(st.st_size), stale=stale, status=status, note=note)


def describe_cty_file(path: str, *, loaded: bool = False) -> DataFileStatus:
    return describe_data_file("CTY.DAT", path, loaded=loaded)


def describe_wpxloc_file(path: str, *, loaded: bool = False) -> DataFileStatus:
    return describe_data_file("wpxloc.raw", path, loaded=loaded)


def describe_keps_file(path: str) -> dict[str, object]:
    status = describe_data_file("KEPS", path).to_json()
    status.update(version="", version_date="", element_count=0, oldest_epoch="", newest_epoch="")
    if not status["exists"]:
        return status
    try:
        records = load_tles(path)
    except (OSError, ValueError):
        records = []
    if not records:
        status.update(status="invalid", stale=True, note="No readable orbital elements.")
        return status
    oldest = min(record.epoch for record in records)
    newest = max(record.epoch for record in records)
    age = max(0, int((datetime.now(timezone.utc) - oldest).total_seconds() // 86400))
    status.update(
        status="stale" if age > 14 else "available", stale=age > 14,
        version_date=newest.date().isoformat(), element_count=len(records),
        oldest_epoch=oldest.isoformat(), newest_epoch=newest.isoformat(),
        note=f"{len(records)} orbital elements; oldest is {age} days old. Element age is not download age.",
    )
    return status
