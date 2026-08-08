#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio

from pycluster.config import load_config
from pycluster.store import SpotStore


async def _run(config_path: str) -> int:
    config = load_config(config_path)
    store = SpotStore(config.store.sqlite_path)
    try:
        removed = await store.purge_persisted_rbn_spots()
    finally:
        await store.close()
    print(f"Removed {removed} persisted RBN spots; future RBN spots are live-only.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Remove obsolete persisted RBN spot history.")
    parser.add_argument("--config", required=True, help="Path to pycluster.toml")
    args = parser.parse_args()
    return asyncio.run(_run(args.config))


if __name__ == "__main__":
    raise SystemExit(main())
