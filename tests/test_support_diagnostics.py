from __future__ import annotations

import os
from pathlib import Path
import sqlite3
import subprocess


def _run_report(root: Path, config: Path, runtime: Path, privacy: str) -> str:
    env = {**os.environ, "PYTHONPATH": str(root / "src")}
    result = subprocess.run(
        [
            "python3",
            str(root / "deploy/support_diagnostics.py"),
            "report",
            "--config",
            str(config),
            "--runtime-root",
            str(runtime),
            "--privacy",
            privacy,
        ],
        cwd=root,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout


def test_support_diagnostics_reports_pc92_address_path_without_credentials(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    runtime = tmp_path / "runtime"
    data = runtime / "data"
    proto = runtime / "logs/proto/2026"
    data.mkdir(parents=True)
    proto.mkdir(parents=True)
    config = runtime / "config.toml"
    config.write_text(
        '[node]\nnode_call = "N9JR-4"\npublic_ip_address = "8.8.8.8"\n\n'
        '[store]\nsqlite_path = "./data/pycluster.db"\n',
        encoding="utf-8",
    )
    database = data / "pycluster.db"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE user_prefs (call TEXT, pref_key TEXT, pref_value TEXT, updated_epoch INTEGER)"
        )
        connection.executemany(
            "INSERT INTO user_prefs VALUES (?, ?, ?, 0)",
            [
                ("N9JR-4", "peer.target.n9jr-2.name", "N9JR-2"),
                ("N9JR-4", "peer.target.n9jr-2.profile", "dxspider"),
                (
                    "N9JR-4",
                    "peer.target.n9jr-2.dsn",
                    "dxspider://user:DO_NOT_LEAK@192.168.222.2:7300?password=DO_NOT_LEAK",
                ),
                ("N9JR-4", "peer.target.n9jr-2.password", "DO_NOT_LEAK"),
            ],
        )
    (proto / "230.log").write_text(
        "2026-08-18T12:00:00+00:00 N9JR-2 rx "
        "PC92^N9JR-2^70411^A^^7N9JR-4:localhost^H99^\n"
        "2026-08-18T12:00:01+00:00 N9JR-2 rx "
        "PC92^N9JR-2^70412^A^^7N9JR-4:fd00,,4^H99^\n",
        encoding="utf-8",
    )

    redacted = _run_report(root, config, runtime, "redacted")
    assert "configured_ipv4=<ipv4-address-redacted> valid=True" in redacted
    assert "pc92_localhost_substitution=passed" in redacted
    assert "endpoint=dxspider://[host-redacted]:7300" in redacted
    assert "host_class=local-or-private-address" in redacted
    assert "frame=PC92 origin=N9JR-2" in redacted
    assert "7N9JR-4:[localhost]" in redacted
    assert "7N9JR-4:[ip-address-redacted]" in redacted
    assert "fd00,,4" not in redacted
    assert "DO_NOT_LEAK" not in redacted
    assert "192.168.222.2" not in redacted

    unredacted = _run_report(root, config, runtime, "unredacted")
    assert "configured_ipv4=8.8.8.8 valid=True" in unredacted
    assert "endpoint=dxspider://192.168.222.2:7300" in unredacted
    assert "DO_NOT_LEAK" not in unredacted
