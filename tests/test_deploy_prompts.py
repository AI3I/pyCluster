from __future__ import annotations

from pathlib import Path
import subprocess


def test_prompt_value_is_visible_while_return_value_stays_clean() -> None:
    root = Path(__file__).resolve().parents[1]
    proc = subprocess.run(
        [
            "bash",
            "-c",
            ". deploy/lib.sh; value=$(prompt_value 'Public hostname:' 'cluster.example'); printf '<%s>' \"$value\"",
        ],
        cwd=root,
        input="\n",
        text=True,
        capture_output=True,
        check=True,
    )
    assert proc.stdout == "<cluster.example>"
    assert "Public hostname: [cluster.example]" in proc.stderr
