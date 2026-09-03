from __future__ import annotations

import importlib.util
import sqlite3
from pathlib import Path

from pycluster.auth import is_password_hash, verify_password
from pycluster.config import load_config


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "migrate_legacy_state.py"
SPEC = importlib.util.spec_from_file_location("migrate_legacy_state_script", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
migrate_legacy_state = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(migrate_legacy_state)


def _write_config(path: Path, db_path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "[node]",
                'node_call = "AI3I-15"',
                "",
                "[telnet]",
                'host = "127.0.0.1"',
                "port = 7300",
                "",
                "[web]",
                'host = "127.0.0.1"',
                "port = 8080",
                "",
                "[public_web]",
                "enabled = false",
                'host = "127.0.0.1"',
                "port = 8081",
                'static_dir = ""',
                'cty_dat_path = ""',
                "",
                "[store]",
                f"sqlite_path = {db_path.as_posix()!r}",
                "",
            ]
        ),
        encoding="utf-8",
    )


def test_combined_legacy_migration_is_idempotent(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    config_path = config_dir / "pycluster.toml"
    db_path = tmp_path / "pycluster.db"
    _write_config(config_path, db_path)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "CREATE TABLE user_prefs (call TEXT NOT NULL, pref_key TEXT NOT NULL, "
            "pref_value TEXT NOT NULL, updated_epoch INTEGER NOT NULL DEFAULT 0)"
        )
        conn.execute(
            "INSERT INTO user_prefs(call, pref_key, pref_value) VALUES (?, ?, ?)",
            ("AI3I-99", "password", "plain-text-secret"),
        )
        conn.execute(
            "INSERT INTO user_prefs(call, pref_key, pref_value) VALUES (?, ?, ?)",
            (
                "AI3I-15",
                "peer.outbound.n9jr-3.dsn",
                "pycluster://peer.example:7300?login=AI3I-15&client=N9JR-3&password=DXCluster",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    first = migrate_legacy_state.migrate(str(db_path))
    second = migrate_legacy_state.migrate(str(db_path))

    assert first == {
        "password_hash_upgrades": 1,
        "peer_passwords_migrated": 1,
        "peer_password_rows_updated": 1,
    }
    assert second == {
        "password_hash_upgrades": 0,
        "peer_passwords_migrated": 0,
        "peer_password_rows_updated": 0,
    }
    conn = sqlite3.connect(db_path)
    try:
        password = conn.execute(
            "SELECT pref_value FROM user_prefs WHERE call = 'AI3I-99' AND pref_key = 'password'"
        ).fetchone()
        dsn = conn.execute(
            "SELECT pref_value FROM user_prefs WHERE call = 'AI3I-15' AND pref_key = 'peer.outbound.n9jr-3.dsn'"
        ).fetchone()
        peer_password = conn.execute(
            "SELECT pref_value FROM user_prefs WHERE call = 'AI3I-15' AND pref_key = 'peer.outbound.n9jr-3.password'"
        ).fetchone()
    finally:
        conn.close()

    assert password and is_password_hash(str(password[0]))
    assert verify_password("plain-text-secret", str(password[0]))
    assert dsn and "password=" not in str(dsn[0])
    assert peer_password == ("DXCluster",)
    assert load_config(config_path).qrz.username == ""
