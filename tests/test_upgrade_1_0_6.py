import importlib.util
import sqlite3
from pathlib import Path


_SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "upgrade_1_0_6.py"
_SPEC = importlib.util.spec_from_file_location("upgrade_1_0_6_script", _SCRIPT_PATH)
assert _SPEC is not None and _SPEC.loader is not None
upgrade_1_0_6 = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(upgrade_1_0_6)


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
                'cty_dat_path = "./fixtures/live/dxspider/cty.dat"',
                "",
                "[store]",
                f"sqlite_path = {db_path.as_posix()!r}",
                "",
            ]
        ),
        encoding="utf-8",
    )


def test_upgrade_1_0_6_moves_embedded_peer_passwords_out_of_dsn(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    config_path = config_dir / "pycluster.toml"
    db_path = tmp_path / "pycluster.db"
    _write_config(config_path, db_path)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "CREATE TABLE user_prefs (call TEXT NOT NULL, pref_key TEXT NOT NULL, pref_value TEXT NOT NULL, updated_epoch INTEGER NOT NULL DEFAULT 0)"
        )
        conn.execute(
            "INSERT INTO user_prefs(call, pref_key, pref_value, updated_epoch) VALUES (?, ?, ?, ?)",
            (
                "AI3I-15",
                "peer.outbound.kc9gwk-1.dsn",
                "pycluster://dx.kc9gwk.radio:7300?login=AI3I-15&client=KC9GWK-1&password=DXCluster",
                0,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    args = upgrade_1_0_6._build_parser().parse_args(["--config", str(config_path)])
    result_code = __import__("asyncio").run(upgrade_1_0_6._run(args))
    assert result_code == 0

    conn = sqlite3.connect(db_path)
    try:
        dsn = conn.execute(
            "SELECT pref_value FROM user_prefs WHERE call = ? AND pref_key = ?",
            ("AI3I-15", "peer.outbound.kc9gwk-1.dsn"),
        ).fetchone()
        pw = conn.execute(
            "SELECT pref_value FROM user_prefs WHERE call = ? AND pref_key = ?",
            ("AI3I-15", "peer.outbound.kc9gwk-1.password"),
        ).fetchone()
    finally:
        conn.close()

    assert dsn is not None
    assert pw is not None
    assert "password=" not in str(dsn[0])
    assert str(pw[0]) == "DXCluster"
