from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

from .store import SpotStore


_USER_LINE_RE = re.compile(r"^(?P<call>[A-Z0-9/-]+)\s+bless\(\s*\{(?P<body>.*)\}\s*,\s*'DXUser'\s*\)\s*$")
_PAIR_RE = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)\s*=>\s*('(?:[^'\\]|\\.)*'|-?\d+(?:\.\d+)?)")


@dataclass(slots=True)
class DxSpiderUserRecord:
    call: str
    name: str = ""
    qth: str = ""
    qra: str = ""
    homenode: str = ""
    lastoper: int = 0


def _decode_value(token: str) -> str:
    text = token.strip()
    if text.startswith("'") and text.endswith("'"):
        inner = text[1:-1]
        return inner.replace("\\'", "'").replace("\\\\", "\\")
    return text


def _clean_export_fragment(value: str, key: str) -> str:
    text = (value or "").strip()
    if not text:
        return ""
    marker = f"set/{key.lower()} "
    low = text.lower()
    if marker in low:
        idx = low.rfind(marker)
        tail = text[idx + len(marker) :].strip()
        if tail:
            return tail
    return text.strip()


def parse_dxspider_user_line(line: str) -> DxSpiderUserRecord | None:
    m = _USER_LINE_RE.match(line.strip())
    if not m:
        return None
    call = m.group("call").strip().upper()
    values: dict[str, str] = {}
    for key, raw in _PAIR_RE.findall(m.group("body")):
        values[key] = _decode_value(raw)
    return DxSpiderUserRecord(
        call=call,
        name=_clean_export_fragment(values.get("name", ""), "name"),
        qth=_clean_export_fragment(values.get("qth", ""), "qth"),
        qra=_clean_export_fragment(values.get("qra", ""), "qra").upper(),
        homenode=values.get("homenode", "").strip().upper(),
        lastoper=int(values.get("lastoper", "0") or "0"),
    )


async def import_dxspider_user_file(store: SpotStore, file_path: str | Path) -> tuple[int, int]:
    src = Path(file_path)
    imported = 0
    skipped = 0

    for line in src.read_text(encoding="utf-8", errors="replace").splitlines():
        rec = parse_dxspider_user_line(line)
        if rec is None:
            skipped += 1
            continue
        await store.upsert_user_registry(
            rec.call,
            rec.lastoper or 0,
            display_name=rec.name,
            qth=rec.qth,
            qra=rec.qra,
        )
        if rec.homenode:
            await store.set_user_pref(rec.call, "homenode", rec.homenode, rec.lastoper or 0)
        imported += 1

    return imported, skipped
