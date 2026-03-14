#!/usr/bin/env python3
"""Generate compatibility matrix markdown from normalized fixtures."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate DXCluster compatibility matrix")
    parser.add_argument("--raw-root", default="/home/jdlewis/dxcluster-compat/fixtures/raw")
    parser.add_argument("--norm-root", default="/home/jdlewis/dxcluster-compat/fixtures/normalized")
    parser.add_argument("--out", default="/home/jdlewis/dxcluster-compat/docs/compatibility-matrix.md")
    args = parser.parse_args()

    raw_root = Path(args.raw_root)
    norm_root = Path(args.norm_root)
    out = Path(args.out)

    summary = load_json(norm_root / "summary.json")
    fixtures = load_json(norm_root / "fixtures.json")

    cmd_groups = fixtures.get("command_groups", [])
    cmd_files = fixtures.get("command_files", [])
    telnet = fixtures.get("telnet_session", {})
    pc_counts = summary.get("pc_type_counts", {})

    top_groups = cmd_groups[:12]
    top_pc = list(pc_counts.items())[:12]

    lines: list[str] = []
    lines.append("# DXCluster Compatibility Matrix")
    lines.append("")
    lines.append(f"Generated UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append("## Baseline")
    lines.append("")
    lines.append("- Target node: `root@dxcluster.ai3i.net`")
    lines.append("- Baseline software: DXSpider 1.55 family (runtime reports build 0.203)")
    lines.append("- Fixture source: live node data archives + live telnet session")
    lines.append("")
    lines.append("## Data Coverage")
    lines.append("")
    for k, v in summary.get("counts", {}).items():
        lines.append(f"- `{k}`: {v}")

    lines.append("")
    lines.append("## Telnet Behavior (Observed)")
    lines.append("")
    lines.append(f"- Login prompt seen: `{telnet.get('has_login_prompt', False)}`")
    lines.append(f"- Welcome banner seen: `{telnet.get('has_welcome', False)}`")
    commands_seen = telnet.get("commands_seen", [])
    lines.append(f"- Commands observed in capture: `{', '.join(commands_seen) if commands_seen else 'none'}`")

    lines.append("")
    lines.append("## Command Surface (By Group)")
    lines.append("")
    lines.append("| Group | Count | Priority |")
    lines.append("|---|---:|---|")
    for item in top_groups:
        group = item["group"]
        count = item["count"]
        priority = "P0" if group in {"show", "set", "unset", "dx.pl", "connect.pl"} else "P1"
        lines.append(f"| `{group}` | {count} | {priority} |")

    lines.append("")
    lines.append("## Protocol Frames (Observed in Debug Logs)")
    lines.append("")
    lines.append("| PC Type | Count | Notes |")
    lines.append("|---|---:|---|")
    for pc, count in top_pc:
        note = "Implement early" if pc in {"PC92", "PC93", "PC61"} else "Implement after core"
        lines.append(f"| `{pc}` | {count} | {note} |")

    lines.append("")
    lines.append("## Minimum Compatible MVP")
    lines.append("")
    lines.append("1. Telnet login/banner/prompt matching the baseline transcript.")
    lines.append("2. Spot ingest and persistence compatible with caret-separated spot records.")
    lines.append("3. `show/version` and `show/dx` output shape compatible with baseline.")
    lines.append("4. Parser/serializer for `PC61`, `PC92`, `PC93` node traffic.")
    lines.append("5. Replay tests using captured `debug/latest_debug.dat` frames.")

    lines.append("")
    lines.append("## Reference Files")
    lines.append("")
    lines.append(f"- Raw manifest: `{raw_root / 'manifest.env'}`")
    lines.append(f"- Normalized fixtures: `{norm_root / 'fixtures.json'}`")
    lines.append(f"- Summary: `{norm_root / 'summary.json'}`")
    lines.append(f"- Command inventory entries: `{len(cmd_files)}`")

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
