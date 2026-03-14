#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import secrets
import string
from pathlib import Path

from pycluster.config import load_config
from pycluster.store import SpotStore


ALPHABET = string.ascii_letters + string.digits


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Seed a default SYSOP account for first-time pyCluster administration.")
    p.add_argument("--config", required=True, help="Path to pycluster.toml")
    p.add_argument("--output", required=True, help="Path to write initial SYSOP credentials")
    return p


def _random_password(length: int = 16) -> str:
    while True:
        password = "".join(secrets.choice(ALPHABET) for _ in range(length))
        if (
            any(c.islower() for c in password)
            and any(c.isupper() for c in password)
            and any(c.isdigit() for c in password)
        ):
            return password


async def _run(config_path: str, output_path: str) -> int:
    cfg = load_config(config_path)
    store = SpotStore(cfg.store.sqlite_path)
    try:
        row = await store.get_user_registry("SYSOP")
        current_password = await store.get_user_pref("SYSOP", "password")
        if row and str(row["privilege"] or "").strip().lower() == "sysop" and str(current_password or "").strip():
            return 0

        now = int(__import__("time").time())
        await store.upsert_user_registry(
            "SYSOP",
            now,
            display_name=cfg.node.owner_name or "System Operator",
            home_node=cfg.node.node_call,
            qth=cfg.node.qth,
            qra=cfg.node.node_locator,
            privilege="sysop",
        )
        password = _random_password(16)
        await store.set_user_pref("SYSOP", "password", password, now)
    finally:
        await store.close()

    out = Path(output_path)
    out.write_text(
        "\n".join(
            [
                "pyCluster initial System Operator account",
                f"Callsign: SYSOP",
                f"Password: {password}",
                f"Config: {config_path}",
                "",
                "Change this password immediately after first login.",
                "",
            ]
        ),
        encoding="utf-8",
    )
    out.chmod(0o600)
    return 0


def main() -> None:
    args = _build_parser().parse_args()
    raise SystemExit(asyncio.run(_run(args.config, args.output)))


if __name__ == "__main__":
    main()
