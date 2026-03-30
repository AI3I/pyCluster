import importlib.util
import sqlite3
from pathlib import Path

from pycluster.auth import is_password_hash, verify_password
from pycluster.config import load_config


_SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "upgrade_1_0_1.py"
_SPEC = importlib.util.spec_from_file_location("upgrade_1_0_1_script", _SCRIPT_PATH)
assert _SPEC is not None and _SPEC.loader is not None
upgrade_1_0_1 = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(upgrade_1_0_1)


def _write_legacy_config(path: Path, db_path: Path) -> None:
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


def test_upgrade_1_0_1_migrates_legacy_state_and_old_config_remains_loadable(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    config_path = config_dir / "pycluster.toml"
    db_path = tmp_path / "pycluster.db"
    strings_template = tmp_path / "strings-template.toml"
    strings_template.write_text('show_qrz = "Show QRZ lookup."\n', encoding="utf-8")
    _write_legacy_config(config_path, db_path)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "CREATE TABLE user_prefs (call TEXT NOT NULL, pref_key TEXT NOT NULL, pref_value TEXT NOT NULL, updated_epoch INTEGER NOT NULL DEFAULT 0)"
        )
        conn.execute(
            "INSERT INTO user_prefs(call, pref_key, pref_value, updated_epoch) VALUES (?, ?, ?, ?)",
            ("K3WCF", "password", "plain-text-secret", 0),
        )
        conn.commit()
    finally:
        conn.close()

    args = upgrade_1_0_1._build_parser().parse_args(
        ["--config", str(config_path), "--strings-template", str(strings_template)]
    )
    rc = upgrade_1_0_1.main if False else None
    result_code = __import__("asyncio").run(upgrade_1_0_1._run(args))
    assert result_code == 0

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT pref_value FROM user_prefs WHERE call = ? AND pref_key = 'password'",
            ("K3WCF",),
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    stored = str(row[0])
    assert is_password_hash(stored)
    assert verify_password("plain-text-secret", stored)

    strings_path = config_dir / "strings.toml"
    assert strings_path.exists()
    assert "show_qrz" in strings_path.read_text(encoding="utf-8")

    cfg = load_config(config_path)
    assert cfg.qrz.username == ""
    assert cfg.qrz.password == ""
    assert cfg.qrz.api_url == "https://xmldata.qrz.com/xml/current/"
