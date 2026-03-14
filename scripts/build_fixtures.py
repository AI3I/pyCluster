#!/usr/bin/env python3
"""Normalize DXSpider artifacts into JSON fixtures for compatibility testing."""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class Paths:
    raw_root: Path
    out_root: Path


def read_lines(path: Path) -> list[str]:
    if not path.exists():
        return []
    return [line.rstrip("\n") for line in path.read_text(encoding="utf-8", errors="replace").splitlines()]


def to_int(value: str) -> int | None:
    if value == "":
        return None
    try:
        return int(value)
    except ValueError:
        return None


def to_float(value: str) -> float | None:
    if value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def epoch_to_iso(epoch: int | None) -> str | None:
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()


def parse_spots_dat(lines: list[str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for line in lines:
        if not line or "^" not in line:
            continue
        fields = line.split("^")
        rec: dict[str, Any] = {
            "raw": line,
            "freq_khz": to_float(fields[0]) if len(fields) > 0 else None,
            "dx_call": fields[1] if len(fields) > 1 else None,
            "epoch": to_int(fields[2]) if len(fields) > 2 else None,
            "iso_utc": None,
            "info": fields[3] if len(fields) > 3 else None,
            "spotter": fields[4] if len(fields) > 4 else None,
            "dxcc_num": to_int(fields[5]) if len(fields) > 5 else None,
            "spotter_dxcc_num": to_int(fields[6]) if len(fields) > 6 else None,
            "source_node": fields[7] if len(fields) > 7 else None,
            "rest": fields[8:],
        }
        rec["iso_utc"] = epoch_to_iso(rec["epoch"])
        out.append(rec)
    return out


def parse_spots_dys(lines: list[str]) -> dict[str, Any]:
    totals: list[int | None] = []
    rows: list[dict[str, Any]] = []

    for line in lines:
        if not line or "^" not in line:
            continue
        fields = line.split("^")
        name = fields[0]
        nums = [to_int(v) for v in fields[1:]]

        if name == "TOTALS":
            totals = nums
        else:
            rows.append({"call": name, "values": nums})

    return {"totals": totals, "rows": rows}


def parse_log_dat(lines: list[str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for line in lines:
        if not line or "^" not in line:
            continue
        fields = line.split("^")
        epoch = to_int(fields[0]) if len(fields) > 0 else None
        rec = {
            "raw": line,
            "epoch": epoch,
            "iso_utc": epoch_to_iso(epoch),
            "category": fields[1] if len(fields) > 1 else None,
            "fields": fields[2:],
            "text": "^".join(fields[2:]) if len(fields) > 2 else "",
        }
        out.append(rec)
    return out


_PC_RE = re.compile(r"^(?P<epoch>\d+)\^<[-]?\s+[IO]\s+(?P<link>[^\s]+)\s+(?P<pc>PC\d+[A-Z]?)\^(?P<payload>.*)$")


def parse_debug_dat(lines: list[str]) -> dict[str, Any]:
    protocol_frames: list[dict[str, Any]] = []
    non_frames: list[dict[str, Any]] = []

    for line in lines:
        if not line:
            continue
        m = _PC_RE.match(line)
        if m:
            epoch = to_int(m.group("epoch"))
            payload = m.group("payload")
            payload_fields = payload.split("^") if payload else []
            protocol_frames.append(
                {
                    "raw": line,
                    "epoch": epoch,
                    "iso_utc": epoch_to_iso(epoch),
                    "link": m.group("link"),
                    "pc_type": m.group("pc"),
                    "payload_fields": payload_fields,
                }
            )
        else:
            fields = line.split("^")
            epoch = to_int(fields[0]) if fields else None
            non_frames.append(
                {
                    "raw": line,
                    "epoch": epoch,
                    "iso_utc": epoch_to_iso(epoch),
                    "message": "^".join(fields[1:]) if len(fields) > 1 else line,
                }
            )

    return {"protocol_frames": protocol_frames, "other_debug_lines": non_frames}


def parse_wcy(lines: list[str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for line in lines:
        if not line:
            continue
        fields = line.split("^")
        epoch = to_int(fields[0]) if fields else None
        out.append(
            {
                "raw": line,
                "epoch": epoch,
                "iso_utc": epoch_to_iso(epoch),
                "values": fields[1:],
            }
        )
    return out


def parse_wwv(lines: list[str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for line in lines:
        if not line:
            continue
        fields = line.split("^")
        epoch = to_int(fields[1]) if len(fields) > 1 else None
        out.append(
            {
                "raw": line,
                "source": fields[0] if fields else None,
                "epoch": epoch,
                "iso_utc": epoch_to_iso(epoch),
                "values": fields[2:],
            }
        )
    return out


def parse_telnet_session(lines: list[str]) -> dict[str, Any]:
    text = "\n".join(lines)
    prompt_re = re.compile(r"(?m)^(?P<call>\S+) de (?P<node>\S+)\s+\d{1,2}-[A-Za-z]{3}-\d{4}\s+\d{4}Z\s+dxspider >$")
    prompts = [m.groupdict() for m in prompt_re.finditer(text)]

    command_lines = [ln.strip() for ln in lines if ln.strip() in {"show/version", "show/dx 3", "bye"}]

    return {
        "line_count": len(lines),
        "prompts": prompts,
        "commands_seen": command_lines,
        "has_login_prompt": "login:" in text,
        "has_welcome": "welcome to DX Cluster node" in text,
        "raw": lines,
    }


def parse_cmd_groups(lines: list[str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for line in lines:
        line = line.rstrip()
        if not line:
            continue
        m = re.match(r"^\s*(\d+)\s+(.+)$", line)
        if not m:
            continue
        out.append({"count": int(m.group(1)), "group": m.group(2)})
    return out


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=False) + "\n", encoding="utf-8")


def build(paths: Paths) -> None:
    raw = paths.raw_root
    out = paths.out_root

    spots_lines = read_lines(raw / "spots/latest_spots.dat")
    dys_lines = read_lines(raw / "spots/latest_spots.dys")
    log_lines = read_lines(raw / "log/latest_log.dat")
    debug_lines = read_lines(raw / "debug/latest_debug.dat")
    wcy_lines = read_lines(raw / "wcy/latest_wcy.dat")
    wwv_lines = read_lines(raw / "wwv/latest_wwv.dat")
    telnet_lines = read_lines(raw / "telnet/session.txt")
    cmd_group_lines = read_lines(raw / "inventory/cmd_groups.txt")
    cmd_file_lines = read_lines(raw / "inventory/cmd_files.txt")

    normalized = {
        "spots": parse_spots_dat(spots_lines),
        "spots_daily": parse_spots_dys(dys_lines),
        "log_events": parse_log_dat(log_lines),
        "debug": parse_debug_dat(debug_lines),
        "wcy": parse_wcy(wcy_lines),
        "wwv": parse_wwv(wwv_lines),
        "telnet_session": parse_telnet_session(telnet_lines),
        "command_groups": parse_cmd_groups(cmd_group_lines),
        "command_files": cmd_file_lines,
    }

    summary = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "counts": {
            "spots": len(normalized["spots"]),
            "spots_daily_rows": len(normalized["spots_daily"]["rows"]),
            "log_events": len(normalized["log_events"]),
            "debug_protocol_frames": len(normalized["debug"]["protocol_frames"]),
            "debug_other_lines": len(normalized["debug"]["other_debug_lines"]),
            "wcy_records": len(normalized["wcy"]),
            "wwv_records": len(normalized["wwv"]),
            "command_files": len(normalized["command_files"]),
        },
        "pc_type_counts": {},
    }

    pc_counts: dict[str, int] = {}
    for frame in normalized["debug"]["protocol_frames"]:
        pc = frame.get("pc_type")
        if not pc:
            continue
        pc_counts[pc] = pc_counts.get(pc, 0) + 1
    summary["pc_type_counts"] = dict(sorted(pc_counts.items(), key=lambda kv: (-kv[1], kv[0])))

    write_json(out / "fixtures.json", normalized)
    write_json(out / "summary.json", summary)
    write_json(out / "spots.json", normalized["spots"])
    write_json(out / "pc_frames.json", normalized["debug"]["protocol_frames"])


def main() -> None:
    parser = argparse.ArgumentParser(description="Build normalized DXCluster compatibility fixtures")
    parser.add_argument("--raw-root", default="/home/jdlewis/dxcluster-compat/fixtures/raw", help="Raw fixture root")
    parser.add_argument("--out-root", default="/home/jdlewis/dxcluster-compat/fixtures/normalized", help="Output fixture root")
    args = parser.parse_args()

    paths = Paths(raw_root=Path(args.raw_root), out_root=Path(args.out_root))
    build(paths)


if __name__ == "__main__":
    main()
