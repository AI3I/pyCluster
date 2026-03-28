import asyncio
import importlib.util
from pathlib import Path
import time

from pycluster.store import SpotStore


_SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "cleanup_retention.py"
_SPEC = importlib.util.spec_from_file_location("cleanup_retention_script", _SCRIPT_PATH)
assert _SPEC is not None and _SPEC.loader is not None
cleanup_retention = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(cleanup_retention)


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


def test_prune_old_files_removes_only_expired_entries(tmp_path: Path) -> None:
    root = tmp_path / "logs" / "proto" / "2026"
    root.mkdir(parents=True)
    old_file = root / "070.log"
    new_file = root / "087.log"
    old_file.write_text("old\n", encoding="utf-8")
    new_file.write_text("new\n", encoding="utf-8")
    now = int(time.time())
    old_mtime = now - 20 * 86400
    new_mtime = now - 2 * 86400
    old_file.touch()
    new_file.touch()
    old_file.chmod(0o644)
    new_file.chmod(0o644)
    os_utime = __import__("os").utime
    os_utime(old_file, (old_mtime, old_mtime))
    os_utime(new_file, (new_mtime, new_mtime))

    removed = cleanup_retention._prune_old_files(root.parent, now=now, keep_days=14)

    assert removed == 1
    assert not old_file.exists()
    assert new_file.exists()


def test_cleanup_retention_runs_and_prunes_proto_logs(tmp_path: Path) -> None:
    async def run() -> None:
        project_root = tmp_path / "project"
        project_root.mkdir()
        config_dir = project_root / "config"
        config_dir.mkdir()
        logs_dir = project_root / "logs" / "proto" / "2026"
        logs_dir.mkdir(parents=True)
        db_path = project_root / "data.db"
        config_path = config_dir / "pycluster.toml"
        _write_config(config_path, db_path)

        store = SpotStore(str(db_path))
        try:
            now = int(time.time())
            await store.set_user_pref("AI3I-15", "retention.enabled", "on", now)
            await store.set_user_pref("AI3I-15", "retention.proto_logs_days", "7", now)
            await store.set_user_pref("AI3I-15", "retention.spots_days", "30", now)
            await store.set_user_pref("AI3I-15", "retention.messages_days", "30", now)
            await store.set_user_pref("AI3I-15", "retention.bulletins_days", "30", now)
        finally:
            await store.close()

        old_log = logs_dir / "070.log"
        fresh_log = logs_dir / "087.log"
        old_log.write_text("old\n", encoding="utf-8")
        fresh_log.write_text("fresh\n", encoding="utf-8")
        old_mtime = now - 15 * 86400
        fresh_mtime = now - 1 * 86400
        os_utime = __import__("os").utime
        os_utime(old_log, (old_mtime, old_mtime))
        os_utime(fresh_log, (fresh_mtime, fresh_mtime))

        args = cleanup_retention.build_parser().parse_args(
            ["--config", str(config_path), "--project-root", str(project_root)]
        )
        rc = await cleanup_retention._main_async(args)
        assert rc == 0
        assert not old_log.exists()
        assert fresh_log.exists()

        store = SpotStore(str(db_path))
        try:
            result = await store.get_user_pref("AI3I-15", "retention.last_result")
            assert result is not None
            assert '"proto_logs":1' in result
        finally:
            await store.close()

    asyncio.run(run())
