#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import time

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
        if not enabled and not args.force:
            payload = {
                "ok": True,
                "enabled": False,
                "ran": False,
                "node_call": cfg.node.node_call,
                "sqlite_path": str(db_path),
            }
            print(json.dumps(payload, indent=2, sort_keys=True))
            return 0
        removed = await store.apply_retention(
            now,
            spots_days=spots_days,
            messages_days=messages_days,
            bulletins_days=bulletins_days,
        )
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
            "node_call": cfg.node.node_call,
            "sqlite_path": str(db_path),
            "removed": removed,
            "spots_days": spots_days,
            "messages_days": messages_days,
            "bulletins_days": bulletins_days,
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
