#!/usr/bin/env python3
from __future__ import annotations

import argparse
import fcntl
import json
import os
from pathlib import Path
import pwd
import grp
import re
import subprocess
import sys
import time

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from pycluster import __version__
from pycluster.upgrade_manager import read_upgrade_status, upgrade_paths, write_upgrade_status

_VERSION_RE = re.compile(r"^v?(\d+)\.(\d+)\.(\d+)$")


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Run a queued pyCluster upgrade request")
    p.add_argument("--repo-root", default="/home/pycluster/pyCluster")
    p.add_argument("--request")
    p.add_argument("--status")
    p.add_argument("--log")
    p.add_argument("--lock")
    p.add_argument("--run-script")
    p.add_argument("--owner", default="pycluster:pycluster")
    return p


def _read_json(path: Path) -> dict[str, object]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _chown(path: Path, owner: str) -> None:
    user, _, group = owner.partition(":")
    uid = pwd.getpwnam(user).pw_uid if user else -1
    gid = grp.getgrnam(group or user).gr_gid if (group or user) else -1
    os.chown(path, uid, gid)


def _version_tuple(raw: str) -> tuple[int, int, int] | None:
    match = _VERSION_RE.match(str(raw or "").strip())
    if not match:
        return None
    return tuple(int(part) for part in match.groups())


def _latest_upgrade_tag(tags: list[str], current_version: str) -> str:
    current = _version_tuple(current_version)
    candidates: list[tuple[tuple[int, int, int], str]] = []
    for raw in tags:
        tag = str(raw or "").strip().split("/")[-1]
        version = _version_tuple(tag)
        if version is None:
            continue
        if current is None or version > current:
            candidates.append((version, tag))
    if not candidates:
        return ""
    candidates.sort(reverse=True)
    return candidates[0][1]


def _run_git(repo_root: Path, args: list[str], logf) -> str:
    logf.write(f"[pycluster-upgrade] git {' '.join(args)}\n")
    logf.flush()
    proc = subprocess.run(
        ["git", "-C", str(repo_root), *args],
        check=False,
        capture_output=True,
        text=True,
    )
    output = (proc.stdout or "") + (proc.stderr or "")
    if output:
        logf.write(output)
        if not output.endswith("\n"):
            logf.write("\n")
    logf.flush()
    if proc.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} failed with exit {proc.returncode}")
    return proc.stdout.strip()


def _read_source_version(repo_root: Path) -> str:
    init_path = repo_root / "src" / "pycluster" / "__init__.py"
    try:
        text = init_path.read_text(encoding="utf-8")
    except Exception:
        return __version__
    match = re.search(r"__version__\s*=\s*['\"]([^'\"]+)['\"]", text)
    return match.group(1).strip() if match else __version__


def _advance_checkout(repo_root: Path, request: dict[str, object], logf) -> str:
    if not (repo_root / ".git").exists():
        raise RuntimeError(f"upgrade source {repo_root} is not a git checkout")
    current_version = str(request.get("current_version") or __version__).strip()
    _run_git(repo_root, ["fetch", "--tags", "--prune", "origin"], logf)
    tags = _run_git(repo_root, ["tag", "--list", "v*"], logf).splitlines()
    target = _latest_upgrade_tag(tags, current_version)
    if target:
        _run_git(repo_root, ["checkout", "--force", target], logf)
        return target
    try:
        _run_git(repo_root, ["pull", "--ff-only"], logf)
    except RuntimeError as exc:
        logf.write(f"[pycluster-upgrade] no newer semver tag found; continuing without git fast-forward: {exc}\n")
        logf.flush()
    return ""


def main() -> int:
    args = _build_parser().parse_args()
    paths = upgrade_paths(args.repo_root)
    request_path = Path(args.request) if args.request else paths.request_path
    status_path = Path(args.status) if args.status else paths.status_path
    log_path = Path(args.log) if args.log else paths.log_path
    lock_path = Path(args.lock) if args.lock else paths.lock_path
    run_script = Path(args.run_script) if args.run_script else paths.run_script

    lock_path.parent.mkdir(parents=True, exist_ok=True)
    status_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    with lock_path.open("a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        request = _read_json(request_path)
        if not request:
            state = read_upgrade_status(status_path)
            if state.get("state") == "running":
                write_upgrade_status(status_path, {
                    **state,
                    "state": "failed",
                    "running": False,
                    "finished_at_epoch": int(time.time()),
                    "error": "upgrade request disappeared before execution",
                })
            return 0

        started = int(time.time())
        running = {
            "state": "running",
            "running": True,
            "requested_by": str(request.get("requested_by", "")).strip().upper(),
            "requested_at_epoch": int(request.get("requested_at_epoch") or started),
            "started_at_epoch": started,
            "current_version": str(request.get("current_version") or __version__).strip(),
            "log_path": str(log_path),
            "run_script": str(run_script),
        }
        write_upgrade_status(status_path, running)
        _chown(status_path, args.owner)
        try:
            request_path.unlink(missing_ok=True)
        except Exception:
            pass

        with log_path.open("a", encoding="utf-8") as logf:
            logf.write(f"[pycluster-upgrade] start {started} requested_by={running['requested_by']}\n")
            logf.flush()
            try:
                target_tag = _advance_checkout(paths.repo_root, request, logf)
                if target_tag:
                    running["target_tag"] = target_tag
                    write_upgrade_status(status_path, running)
                    _chown(status_path, args.owner)
                proc = subprocess.run(
                    [str(run_script)],
                    cwd=str(paths.repo_root),
                    stdout=logf,
                    stderr=subprocess.STDOUT,
                    text=True,
                )
                exit_code = int(proc.returncode)
                error = ""
            except Exception as exc:
                exit_code = 1
                error = str(exc)
                logf.write(f"[pycluster-upgrade] failed before deploy script: {error}\n")
                logf.flush()
            finished = int(time.time())
            installed_version = _read_source_version(paths.repo_root)
            final = {
                **running,
                "state": "complete" if exit_code == 0 else "failed",
                "running": False,
                "finished_at_epoch": finished,
                "exit_code": exit_code,
                "current_version": installed_version,
            }
            if error:
                final["error"] = error
            write_upgrade_status(status_path, final)
            _chown(status_path, args.owner)
            _chown(log_path, args.owner)
            return exit_code


if __name__ == "__main__":
    sys.exit(main())
