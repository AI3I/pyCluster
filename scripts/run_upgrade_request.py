#!/usr/bin/env python3
from __future__ import annotations

import argparse
import fcntl
import json
import os
from pathlib import Path
import pwd
import grp
import subprocess
import sys
import time

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from pycluster import __version__
from pycluster.upgrade_manager import read_upgrade_status, upgrade_paths, write_upgrade_status


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
            proc = subprocess.run(
                [str(run_script)],
                cwd=str(paths.repo_root),
                stdout=logf,
                stderr=subprocess.STDOUT,
                text=True,
            )
            finished = int(time.time())
            final = {
                **running,
                "state": "complete" if proc.returncode == 0 else "failed",
                "running": False,
                "finished_at_epoch": finished,
                "exit_code": int(proc.returncode),
                "current_version": __version__,
            }
            write_upgrade_status(status_path, final)
            _chown(status_path, args.owner)
            _chown(log_path, args.owner)
            return proc.returncode


if __name__ == "__main__":
    sys.exit(main())
