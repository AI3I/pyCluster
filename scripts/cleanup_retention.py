#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import sys
import time


def _script_project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _bootstrap_import_path() -> None:
    src = _script_project_root() / "src"
    src_str = str(src)
    if src_str not in sys.path:
        sys.path.insert(0, src_str)


_bootstrap_import_path()

from pycluster.config import load_config
from pycluster.store import SpotStore


def _default_project_root(config_path: Path) -> Path:
    parent = config_path.resolve().parent
    if parent.name == "config":
        return parent.parent
    return parent


def _as_int(value: str | None, default: int = 0, low: int = 0, high: int = 3650) -> int:
    try:
        parsed = int(str(value or "").strip())
    except Exception:
        return default
    return max(low, min(high, parsed))


def _prune_old_files(root: Path, *, now: int, keep_days: int) -> int:
    if keep_days <= 0 or not root.exists():
        return 0
    cutoff = now - keep_days * 86400
    removed = 0
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        try:
            mtime = int(path.stat().st_mtime)
        except OSError:
            continue
        if mtime >= cutoff:
            continue
        try:
            path.unlink()
        except OSError:
            continue
        removed += 1
    return removed


async def _main_async(args: argparse.Namespace) -> int:
    config_path = Path(args.config).expanduser().resolve()
    project_root = Path(args.project_root).expanduser().resolve() if args.project_root else _default_project_root(config_path)
    cfg = load_config(config_path)
    db_path = Path(cfg.store.sqlite_path)
    if not db_path.is_absolute():
        db_path = (project_root / db_path).resolve()
    store = SpotStore(str(db_path))
    now = int(time.time())
    try:
        prefs = await store.list_user_prefs(cfg.node.node_call)
        enabled = str(prefs.get("retention.enabled", "")).strip().lower() in {"1", "on", "yes", "true"}
        spots_days = _as_int(prefs.get("retention.spots_days"), 30)
        messages_days = _as_int(prefs.get("retention.messages_days"), 90)
        bulletins_days = _as_int(prefs.get("retention.bulletins_days"), 30)
        proto_logs_days = _as_int(prefs.get("retention.proto_logs_days"), 14)
        logs_root = project_root / "logs" / "proto"
        run_db_retention = enabled or args.force
        run_proto_log_retention = proto_logs_days > 0 or args.force
        if not run_db_retention and not run_proto_log_retention:
            payload = {
                "ok": True,
                "enabled": False,
                "ran": False,
                "node_call": cfg.node.node_call,
                "sqlite_path": str(db_path),
            }
            print(json.dumps(payload, indent=2, sort_keys=True))
            return 0
        removed = {"spots": 0, "messages": 0, "bulletins": 0, "proto_logs": 0}
        if run_db_retention:
            removed.update(
                await store.apply_retention(
                    now,
                    spots_days=spots_days,
                    messages_days=messages_days,
                    bulletins_days=bulletins_days,
                )
            )
        if run_proto_log_retention:
            removed["proto_logs"] = _prune_old_files(logs_root, now=now, keep_days=proto_logs_days)
        await store.set_user_pref(cfg.node.node_call, "retention.last_run_epoch", str(now), now)
        await store.set_user_pref(
            cfg.node.node_call,
            "retention.last_result",
            json.dumps(removed, separators=(",", ":"), ensure_ascii=True),
            now,
        )
        payload = {
            "ok": True,
            "enabled": enabled,
            "ran": True,
            "db_retention_ran": run_db_retention,
            "proto_log_retention_ran": run_proto_log_retention,
            "node_call": cfg.node.node_call,
            "sqlite_path": str(db_path),
            "removed": removed,
            "spots_days": spots_days,
            "messages_days": messages_days,
            "bulletins_days": bulletins_days,
            "proto_logs_days": proto_logs_days,
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0
    finally:
        await store.close()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Apply pyCluster age-based retention cleanup")
    p.add_argument("--config", required=True, help="Path to pyCluster config/pycluster.toml")
    p.add_argument("--project-root", default="", help="pyCluster project root for resolving relative store paths")
    p.add_argument("--force", action="store_true", help="Run cleanup even if retention is currently disabled")
    return p


def main() -> int:
    return asyncio.run(_main_async(build_parser().parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
