from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

from .protocol import decode_typed, encode_typed, parse_debug_pc_frame, serialize_debug_pc_frame


# Minimal baseline structure checks by frame type.
MIN_FIELDS_BY_PC = {
    "PC61": 7,
    "PC11": 7,
    "PC24": 3,
    "PC50": 2,
    "PC51": 3,
    "PC92": 4,
    "PC93": 7,
}


@dataclass(slots=True)
class ReplayStats:
    total: int
    parsed: int
    unparsed: int
    structural_failures: int
    roundtrip_failures: int
    by_type: dict[str, int]
    failures_by_type: dict[str, int]


def _iter_raw_frames(path: Path) -> list[str]:
    if path.suffix.lower() == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            out = []
            for item in data:
                if isinstance(item, dict) and isinstance(item.get("raw"), str):
                    out.append(item["raw"])
            return out
        return []
    return [ln.rstrip("\n") for ln in path.read_text(encoding="utf-8", errors="replace").splitlines() if ln.strip()]


def replay_frames(input_path: str | Path, output_report: str | Path | None = None) -> ReplayStats:
    p = Path(input_path)
    lines = _iter_raw_frames(p)

    total = len(lines)
    parsed = 0
    structural_failures = 0
    roundtrip_failures = 0
    by_type: Counter[str] = Counter()
    failures_by_type: Counter[str] = Counter()

    for raw in lines:
        frame = parse_debug_pc_frame(raw)
        if frame is None:
            continue

        parsed += 1
        by_type[frame.pc_type] += 1

        min_fields = MIN_FIELDS_BY_PC.get(frame.pc_type)
        if min_fields is not None and len(frame.payload_fields) < min_fields:
            structural_failures += 1
            failures_by_type[frame.pc_type] += 1

        # Typed roundtrip checks for currently supported families.
        typed = decode_typed(frame)
        if typed is not None:
            rebuilt_fields = encode_typed(frame.pc_type, typed)
            rebuilt_frame = frame.__class__(
                epoch=frame.epoch,
                arrow=frame.arrow,
                io=frame.io,
                link=frame.link,
                pc_type=frame.pc_type,
                payload_fields=rebuilt_fields,
            )
            if serialize_debug_pc_frame(rebuilt_frame) != serialize_debug_pc_frame(frame):
                roundtrip_failures += 1

    unparsed = total - parsed
    stats = ReplayStats(
        total=total,
        parsed=parsed,
        unparsed=unparsed,
        structural_failures=structural_failures,
        roundtrip_failures=roundtrip_failures,
        by_type=dict(by_type),
        failures_by_type=dict(failures_by_type),
    )

    if output_report is not None:
        report = {
            "input": str(p),
            "total": stats.total,
            "parsed": stats.parsed,
            "unparsed": stats.unparsed,
            "parse_rate": (stats.parsed / stats.total) if stats.total else 0.0,
            "structural_failures": stats.structural_failures,
            "roundtrip_failures": stats.roundtrip_failures,
            "by_type": dict(sorted(stats.by_type.items(), key=lambda kv: (-kv[1], kv[0]))),
            "failures_by_type": dict(sorted(stats.failures_by_type.items(), key=lambda kv: (-kv[1], kv[0]))),
            "checked_types": sorted(MIN_FIELDS_BY_PC.keys()),
        }
        out = Path(output_report)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    return stats


def format_stats(stats: ReplayStats) -> str:
    parse_rate = (stats.parsed / stats.total * 100.0) if stats.total else 0.0
    lines = [
        f"total={stats.total}",
        f"parsed={stats.parsed}",
        f"unparsed={stats.unparsed}",
        f"parse_rate={parse_rate:.2f}%",
        f"structural_failures={stats.structural_failures}",
        f"roundtrip_failures={stats.roundtrip_failures}",
    ]

    if stats.by_type:
        top = sorted(stats.by_type.items(), key=lambda kv: (-kv[1], kv[0]))[:10]
        lines.append("top_types=" + ", ".join(f"{k}:{v}" for k, v in top))

    if stats.failures_by_type:
        topf = sorted(stats.failures_by_type.items(), key=lambda kv: (-kv[1], kv[0]))[:10]
        lines.append("failures_by_type=" + ", ".join(f"{k}:{v}" for k, v in topf))

    return "\n".join(lines)
