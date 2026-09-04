from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys


_SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "run_upgrade_request.py"
_SPEC = importlib.util.spec_from_file_location("run_upgrade_request_script", _SCRIPT_PATH)
assert _SPEC is not None and _SPEC.loader is not None
run_upgrade_request = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(run_upgrade_request)


def test_upgrade_request_selects_latest_newer_semver_tag() -> None:
    assert run_upgrade_request._latest_upgrade_tag(["v1.0.6", "v1.0.9", "not-a-version", "v1.0.8"], "1.0.6") == "v1.0.9"
    assert run_upgrade_request._latest_upgrade_tag(["v1.0.6", "v1.0.8"], "1.0.9") == ""


def test_upgrade_worker_git_commands_trust_the_configured_source(tmp_path: Path, monkeypatch) -> None:
    captured: list[str] = []

    def fake_run(args, **_kwargs):
        captured.extend(args)
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(run_upgrade_request.subprocess, "run", fake_run)
    with (tmp_path / "upgrade.log").open("w+", encoding="utf-8") as log:
        run_upgrade_request._run_git(tmp_path, ["status", "--porcelain"], log)

    assert captured[:4] == ["git", "-c", f"safe.directory={tmp_path}", "-C"]


def test_upgrade_request_reads_source_version_after_checkout(tmp_path: Path) -> None:
    init_path = tmp_path / "src" / "pycluster" / "__init__.py"
    init_path.parent.mkdir(parents=True)
    init_path.write_text('__version__ = "1.0.8"\n', encoding="utf-8")

    assert run_upgrade_request._read_source_version(tmp_path) == "1.0.8"


def test_upgrade_worker_refuses_dirty_source_checkout(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / ".git").mkdir()
    calls: list[list[str]] = []

    def fake_git(_root, args, _log):
        calls.append(args)
        return " M local-change" if args == ["status", "--porcelain"] else ""

    monkeypatch.setattr(run_upgrade_request, "_run_git", fake_git)
    with (tmp_path / "upgrade.log").open("w+", encoding="utf-8") as log:
        try:
            run_upgrade_request._advance_checkout(tmp_path, {"current_version": "1.0.14"}, log)
        except RuntimeError as exc:
            assert "has local changes" in str(exc)
        else:
            raise AssertionError("dirty source checkout was accepted")

    assert calls == [["status", "--porcelain"]]


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
        if "status" in args:
            return __import__("subprocess").CompletedProcess(args, 0, "", "")
        if "ls-remote" in args:
            return __import__("subprocess").CompletedProcess(args, 128, "", "fatal: unable to access remote\n")
        return __import__("subprocess").CompletedProcess(args, 0, "v1.0.12\n", "")

    monkeypatch.setattr(upgrade_manager.subprocess, "run", fake_run)
    status = upgrade_manager.detect_upgrade_availability(tmp_path, "1.0.12")

    assert status["source_checkout"] is True
    assert status["source_dirty"] is False
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
        if "status" in args:
            return __import__("subprocess").CompletedProcess(args, 0, "", "")
        return __import__("subprocess").CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(upgrade_manager.subprocess, "run", fake_run)
    status = upgrade_manager.detect_upgrade_availability(tmp_path, "1.0.13")

    assert status["remote_checked"] is True
    assert status["remote_error"] == ""
    assert status["available"] is False


def test_upgrade_detection_reports_dirty_source(tmp_path, monkeypatch) -> None:
    from pycluster import upgrade_manager

    (tmp_path / ".git").mkdir()

    def fake_run(args, **_kwargs):
        if "get-url" in args:
            return subprocess.CompletedProcess(args, 0, "https://example.test/repo.git\n", "")
        if "status" in args:
            return subprocess.CompletedProcess(args, 0, "?? local-file\n", "")
        return subprocess.CompletedProcess(args, 0, "v1.0.15\n", "")

    monkeypatch.setattr(upgrade_manager.subprocess, "run", fake_run)
    status = upgrade_manager.detect_upgrade_availability(tmp_path, "1.0.14")

    assert status["source_dirty"] is True
    assert status["available"] is True


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


def test_source_repo_root_skips_inaccessible_receipt_checkout(tmp_path, monkeypatch) -> None:
    from pycluster.upgrade_manager import source_repo_root

    runtime = tmp_path / "runtime"
    source = tmp_path / "private-source"
    (runtime / "data").mkdir(parents=True)
    (runtime / ".git").mkdir()
    (source / ".git").mkdir(parents=True)
    (runtime / "data" / "deployment-state.toml").write_text(
        f'source_root = "{source}"\n',
        encoding="utf-8",
    )
    original_exists = Path.exists

    def guarded_exists(path: Path) -> bool:
        if path == source / ".git":
            raise PermissionError(13, "Permission denied", str(path))
        if path == Path("/usr/src/pyCluster/.git"):
            return False
        return original_exists(path)

    monkeypatch.setattr(Path, "exists", guarded_exists)

    assert source_repo_root(runtime) == runtime.resolve()


def test_upgrade_worker_fetches_new_release_and_runs_upgrade(tmp_path: Path) -> None:
    remote = tmp_path / "remote.git"
    source = tmp_path / "source"
    subprocess.run(["git", "init", "--bare", str(remote)], check=True, capture_output=True)
    subprocess.run(["git", "init", str(source)], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(source), "config", "user.name", "AI3I"], check=True)
    subprocess.run(["git", "-C", str(source), "config", "user.email", "ai3i@users.noreply.github.com"], check=True)
    subprocess.run(["git", "-C", str(source), "remote", "add", "origin", str(remote)], check=True)

    version_file = source / "src" / "pycluster" / "__init__.py"
    version_file.parent.mkdir(parents=True)
    version_file.write_text('__version__ = "1.0.14"\n', encoding="utf-8")
    deploy = source / "deploy" / "upgrade.sh"
    deploy.parent.mkdir()
    deploy.write_text("#!/usr/bin/env bash\nset -e\nprintf upgraded > upgrade-ran\n", encoding="utf-8")
    deploy.chmod(0o755)
    subprocess.run(["git", "-C", str(source), "add", "."], check=True)
    subprocess.run(["git", "-C", str(source), "commit", "-m", "1.0.14"], check=True, capture_output=True)
    old_commit = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip()
    subprocess.run(["git", "-C", str(source), "tag", "v1.0.14"], check=True)

    version_file.write_text('__version__ = "1.0.15"\n', encoding="utf-8")
    subprocess.run(["git", "-C", str(source), "add", "."], check=True)
    subprocess.run(["git", "-C", str(source), "commit", "-m", "1.0.15"], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(source), "tag", "v1.0.15"], check=True)
    subprocess.run(["git", "-C", str(source), "push", "origin", "HEAD", "--tags"], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(source), "checkout", "--force", old_commit], check=True, capture_output=True)

    request = tmp_path / "request.json"
    status = tmp_path / "status.json"
    log = tmp_path / "upgrade.log"
    lock = tmp_path / "upgrade.lock"
    request.write_text(
        json.dumps({"requested_by": "AI3I-99", "current_version": "1.0.14"}),
        encoding="utf-8",
    )
    owner = f"{__import__('pwd').getpwuid(os.getuid()).pw_name}:{__import__('grp').getgrgid(os.getgid()).gr_name}"
    result = subprocess.run(
        [
            sys.executable,
            str(_SCRIPT_PATH),
            "--repo-root",
            str(source),
            "--request",
            str(request),
            "--status",
            str(status),
            "--log",
            str(log),
            "--lock",
            str(lock),
            "--owner",
            owner,
        ],
        check=False,
        text=True,
        capture_output=True,
    )

    assert result.returncode == 0, result.stderr
    final = json.loads(status.read_text(encoding="utf-8"))
    assert final["state"] == "complete"
    assert final["target_tag"] == "v1.0.15"
    assert final["current_version"] == "1.0.15"
    assert (source / "upgrade-ran").read_text(encoding="utf-8") == "upgraded"
    assert not request.exists()
