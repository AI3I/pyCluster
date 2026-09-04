from __future__ import annotations

from dataclasses import dataclass
import json
import os
import re
from pathlib import Path
import subprocess
import time
import tomllib


_VERSION_RE = re.compile(r"^v?(\d+)\.(\d+)\.(\d+)$")


@dataclass(slots=True)
class UpgradePaths:
    repo_root: Path
    request_path: Path
    status_path: Path
    log_path: Path
    lock_path: Path
    run_script: Path


def repo_root_from_config(config_path: str | Path | None) -> Path:
    if config_path:
        return Path(config_path).resolve().parent.parent
    return Path(__file__).resolve().parents[2]


def source_repo_root(runtime_root: str | Path) -> Path:
    configured = str(os.environ.get("PYCLUSTER_SOURCE_ROOT", "")).strip()
    candidates = [Path(configured)] if configured else []
    receipt = Path(runtime_root) / "data" / "deployment-state.toml"
    if receipt.exists():
        try:
            recorded = str(tomllib.loads(receipt.read_text(encoding="utf-8")).get("source_root", "")).strip()
            if recorded:
                candidates.append(Path(recorded))
        except Exception:
            pass
    candidates.append(Path("/usr/src/pyCluster"))
    candidates.append(Path(runtime_root))
    for candidate in candidates:
        try:
            if candidate and (candidate / ".git").exists():
                return candidate.resolve()
        except OSError:
            # A receipt may name an administrator-owned checkout that the
            # runtime service cannot traverse. That disables web upgrades; it
            # must not prevent the core and System Operator console starting.
            continue
    return Path(runtime_root).resolve()


def upgrade_paths(repo_root: str | Path) -> UpgradePaths:
    root = Path(repo_root).resolve()
    return UpgradePaths(
        repo_root=root,
        request_path=root / "data" / "upgrade-request.json",
        status_path=root / "data" / "upgrade-status.json",
        log_path=root / "logs" / "upgrade.log",
        lock_path=root / "data" / "upgrade.lock",
        run_script=root / "deploy" / "upgrade.sh",
    )


def _version_tuple(raw: str) -> tuple[int, int, int] | None:
    text = str(raw or "").strip()
    m = _VERSION_RE.match(text)
    if not m:
        return None
    return tuple(int(part) for part in m.groups())


def _run_git(repo_root: Path, *args: str) -> str:
    proc = subprocess.run(
        ["git", "-c", f"safe.directory={repo_root}", "-C", str(repo_root), *args],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        suffix = f": {detail}" if detail else ""
        raise RuntimeError(f"git {' '.join(args)} failed with exit {proc.returncode}{suffix}")
    return proc.stdout.strip()


def source_checkout_is_secure(repo_root: str | Path) -> bool:
    root = Path(repo_root).resolve()
    required = (root, root / ".git", root / "deploy" / "upgrade.sh")
    try:
        return all(path.exists() and path.stat().st_uid == 0 and not (path.stat().st_mode & 0o022) for path in required)
    except OSError:
        return False


def _latest_tag_from_lines(lines: list[str]) -> str:
    tags: list[tuple[tuple[int, int, int], str]] = []
    for line in lines:
        text = str(line or "").strip()
        if not text:
            continue
        tag = text.split("/")[-1]
        version = _version_tuple(tag)
        if version is None:
            continue
        tags.append((version, tag))
    if not tags:
        return ""
    tags.sort(reverse=True)
    return tags[0][1]


def detect_upgrade_availability(repo_root: str | Path, current_version: str) -> dict[str, object]:
    root = Path(repo_root).resolve()
    current_tag = f"v{str(current_version).strip()}"
    current_tuple = _version_tuple(current_tag)
    local_tag = ""
    remote_tag = ""
    remote_error = ""
    remote_checked = False
    source_checkout = (root / ".git").exists()
    source_secure = source_checkout_is_secure(root) if source_checkout else False
    source_dirty = False
    origin_url = ""
    if source_checkout:
        try:
            origin_url = _run_git(root, "remote", "get-url", "origin")
            origin_url = re.sub(r"(?i)(https?://)[^/@\s]+@", r"\1", origin_url)
            source_dirty = bool(_run_git(root, "status", "--porcelain"))
        except Exception as exc:
            remote_error = str(exc)
    else:
        remote_error = f"upgrade source {root} is not a git checkout"
    if source_checkout:
        try:
            local_tag = _latest_tag_from_lines(_run_git(root, "tag", "--list", "v*").splitlines())
        except Exception:
            local_tag = ""
    if source_checkout and not remote_error:
        try:
            remote_tag = _latest_tag_from_lines(_run_git(root, "ls-remote", "--tags", "--refs", "origin", "v*").splitlines())
            remote_checked = True
        except Exception as exc:
            remote_error = str(exc)
            remote_tag = ""
    candidate = remote_tag or local_tag
    candidate_tuple = _version_tuple(candidate)
    available = bool(candidate_tuple and current_tuple and candidate_tuple > current_tuple)
    return {
        "current_version": str(current_version).strip(),
        "current_tag": current_tag,
        "latest_local_tag": local_tag,
        "latest_remote_tag": remote_tag,
        "available": available,
        "available_version": candidate.lstrip("v") if available and candidate else "",
        "remote_checked": remote_checked,
        "remote_error": remote_error,
        "source_checkout": source_checkout,
        "source_dirty": source_dirty,
        "source_secure": source_secure,
        "source_repo_root": str(root),
        "origin_url": origin_url,
    }


def migration_hooks(repo_root: str | Path) -> list[str]:
    root = Path(repo_root).resolve()
    upgrade_sh = root / "deploy" / "upgrade.sh"
    if not upgrade_sh.exists():
        return []
    hooks: list[str] = []
    for line in upgrade_sh.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("run_upgrade_"):
            hooks.append(stripped)
    return hooks


def read_upgrade_status(status_path: str | Path) -> dict[str, object]:
    path = Path(status_path)
    if not path.exists():
        return {"state": "idle", "running": False}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"state": "unknown", "running": False, "error": "status unreadable"}
    if not isinstance(data, dict):
        return {"state": "unknown", "running": False, "error": "status invalid"}
    state = str(data.get("state", "idle")).strip() or "idle"
    out = dict(data)
    out["state"] = state
    out["running"] = state == "running"
    return out


def write_upgrade_status(status_path: str | Path, payload: dict[str, object]) -> None:
    path = Path(status_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    path.chmod(0o664)


def queue_upgrade_request(
    request_path: str | Path,
    *,
    requested_by: str,
    current_version: str,
    source_repo_root: str = "",
) -> dict[str, object]:
    now = int(time.time())
    payload = {
        "requested_by": str(requested_by or "").strip().upper(),
        "requested_at_epoch": now,
        "current_version": str(current_version).strip(),
        "source_repo_root": str(source_repo_root or "").strip(),
    }
    path = Path(request_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    path.chmod(0o664)
    return payload
