#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import os
import sys
import time

from pycluster.config import load_config
from pycluster.dxspider_migrate import discover_dxspider_local_data, migrate_dxspider_local_data
from pycluster.store import SpotStore


def _default_project_root(config_path: Path) -> Path:
    parent = config_path.resolve().parent
    if parent.name == "config":
        return parent.parent
    return parent


def _store_path(config_path: Path, project_root: Path) -> Path:
    cfg = load_config(config_path)
    raw = Path(cfg.store.sqlite_path)
    if raw.is_absolute():
        return raw
    return (project_root / raw).resolve()


async def _main_async(args: argparse.Namespace) -> int:
    config_path = Path(args.config).expanduser().resolve()
    project_root = Path(args.project_root).expanduser().resolve() if args.project_root else _default_project_root(config_path)
    os.chdir(project_root)

    store_path = _store_path(config_path, project_root)
    source_root, local_data_dir = discover_dxspider_local_data(args.source)
    badip_fail2ban_path = (project_root / "config" / "fail2ban-badip.local").resolve()

    if args.dry_run:
        report = {
            "source_root": str(source_root),
            "local_data_dir": str(local_data_dir),
            "project_root": str(project_root),
            "config": str(config_path),
            "sqlite_path": str(store_path),
            "badip_fail2ban_file": str(badip_fail2ban_path),
            "mode": "dry-run",
        }
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0

    cfg = load_config(config_path)
    store = SpotStore(str(store_path))
    try:
        report = await migrate_dxspider_local_data(
            store,
            cfg.node.node_call,
            source_root,
            now_epoch=int(time.time()),
            badip_fail2ban_file=badip_fail2ban_path,
        )
    finally:
        await store.close()

    payload = report.to_dict()
    payload["config"] = str(config_path)
    payload["project_root"] = str(project_root)
    payload["sqlite_path"] = str(store_path)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Import DXSpider local data into a pyCluster SQLite store")
    p.add_argument("--config", required=True, help="Path to pyCluster config/pycluster.toml")
    p.add_argument("--source", required=True, help="DXSpider root or local_data directory")
    p.add_argument("--project-root", default="", help="pyCluster project root for resolving relative config paths")
    p.add_argument("--dry-run", action="store_true", help="Validate paths and print the resolved targets without importing")
    return p


def main() -> int:
    args = build_parser().parse_args()
    return asyncio.run(_main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
