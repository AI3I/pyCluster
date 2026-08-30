from __future__ import annotations

import importlib.util
from pathlib import Path


_SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "run_upgrade_request.py"
_SPEC = importlib.util.spec_from_file_location("run_upgrade_request_script", _SCRIPT_PATH)
assert _SPEC is not None and _SPEC.loader is not None
run_upgrade_request = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(run_upgrade_request)


def test_upgrade_request_selects_latest_newer_semver_tag() -> None:
    assert run_upgrade_request._latest_upgrade_tag(["v1.0.6", "v1.0.9", "not-a-version", "v1.0.8"], "1.0.6") == "v1.0.9"
    assert run_upgrade_request._latest_upgrade_tag(["v1.0.6", "v1.0.8"], "1.0.9") == ""


def test_upgrade_request_reads_source_version_after_checkout(tmp_path: Path) -> None:
    init_path = tmp_path / "src" / "pycluster" / "__init__.py"
    init_path.parent.mkdir(parents=True)
    init_path.write_text('__version__ = "1.0.8"\n', encoding="utf-8")

    assert run_upgrade_request._read_source_version(tmp_path) == "1.0.8"


def test_upgrade_systemd_worker_uses_source_checkout_and_live_runtime_paths() -> None:
    unit = (Path(__file__).resolve().parent.parent / "deploy" / "systemd" / "pycluster-upgrade.service").read_text(encoding="utf-8")

    assert "WorkingDirectory=/usr/src/pyCluster" in unit
    assert "/usr/src/pyCluster/scripts/run_upgrade_request.py" in unit
    assert "--repo-root /usr/src/pyCluster" in unit
    assert "--request /home/pycluster/pyCluster/data/upgrade-request.json" in unit
    assert "--status /home/pycluster/pyCluster/data/upgrade-status.json" in unit
    assert "--log /home/pycluster/pyCluster/logs/upgrade.log" in unit


def test_upgrade_detection_allows_known_root_owned_checkout_and_preserves_git_error(tmp_path, monkeypatch) -> None:
    from pycluster import upgrade_manager

    (tmp_path / ".git").mkdir()
    calls: list[list[str]] = []

    def fake_run(args, **_kwargs):
        calls.append(list(args))
        if "get-url" in args:
            return __import__("subprocess").CompletedProcess(args, 0, "https://token@example.test/AI3I/pyCluster.git\n", "")
        if "ls-remote" in args:
            return __import__("subprocess").CompletedProcess(args, 128, "", "fatal: unable to access remote\n")
        return __import__("subprocess").CompletedProcess(args, 0, "v1.0.12\n", "")

    monkeypatch.setattr(upgrade_manager.subprocess, "run", fake_run)
    status = upgrade_manager.detect_upgrade_availability(tmp_path, "1.0.12")

    assert status["source_checkout"] is True
    assert status["origin_url"] == "https://example.test/AI3I/pyCluster.git"
    assert status["remote_checked"] is False
    assert "fatal: unable to access remote" in str(status["remote_error"])
    assert all("safe.directory=" in call[2] for call in calls)


def test_upgrade_detection_treats_empty_remote_tag_list_as_success(tmp_path, monkeypatch) -> None:
    from pycluster import upgrade_manager

    (tmp_path / ".git").mkdir()

    def fake_run(args, **_kwargs):
        if "get-url" in args:
            return __import__("subprocess").CompletedProcess(args, 0, "https://example.test/repo.git\n", "")
        return __import__("subprocess").CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(upgrade_manager.subprocess, "run", fake_run)
    status = upgrade_manager.detect_upgrade_availability(tmp_path, "1.0.13")

    assert status["remote_checked"] is True
    assert status["remote_error"] == ""
    assert status["available"] is False


def test_source_repo_root_uses_deployment_receipt(tmp_path) -> None:
    from pycluster.upgrade_manager import source_repo_root

    runtime = tmp_path / "runtime"
    source = tmp_path / "source"
    (runtime / "data").mkdir(parents=True)
    (source / ".git").mkdir(parents=True)
    (runtime / "data" / "deployment-state.toml").write_text(
        f'source_root = "{source}"\n',
        encoding="utf-8",
    )

    assert source_repo_root(runtime) == source.resolve()
