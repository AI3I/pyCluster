from __future__ import annotations

import json
from pathlib import Path

from pycluster.replay import replay_frames


def test_replay_frames_json(tmp_path: Path) -> None:
    frames = [
        {
            "raw": "1772323200^<- I WB3FFV-2 PC61^1928.0^Z66BCC^ 1-Mar-2026^0000Z^ ^DL6NBC^DA0BCC-7^84.163.40.20^H28^~"
        },
        {
            "raw": "1772323200^<- I WB3FFV-2 PC92^UF3K-1^0^D^^5R1BLH-1^H96^"
        },
        {
            "raw": "1772335401^Start Protocol Engines ..."
        },
    ]
    src = tmp_path / "frames.json"
    src.write_text(json.dumps(frames), encoding="utf-8")

    report = tmp_path / "report.json"
    stats = replay_frames(src, report)

    assert stats.total == 3
    assert stats.parsed == 2
    assert stats.unparsed == 1
    assert stats.roundtrip_failures == 0
    assert stats.by_type["PC61"] == 1
    assert stats.by_type["PC92"] == 1
    assert report.exists()
