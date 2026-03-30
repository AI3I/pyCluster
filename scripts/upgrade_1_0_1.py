#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
import sys
from pathlib import Path


def _bootstrap_import_path() -> None:
    here = Path(__file__).resolve()
    root = here.parent.parent
    src = root / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))


_bootstrap_import_path()

from pycluster.auth import hash_password, is_password_hash  # noqa: E402
from pycluster.config import load_config  # noqa: E402


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Upgrade pyCluster state for the 1.0.1 release.")
    p.add_argument("--config", required=True, help="Path to pycluster.toml")
    p.add_argument("--strings-template", default="", help="Optional path to a default strings.toml to seed if missing")
    return p


def _migrate_passwords(sqlite_path: str) -> int:
    conn = sqlite3.connect(sqlite_path)
    try:
        info = conn.execute("PRAGMA table_info(user_prefs)").fetchall()
        columns = {str(row[1]) for row in info}
        if {"call", "pref_key", "pref_value"} <= columns:
            key_col = "pref_key"
            value_col = "pref_value"
        elif {"call", "key", "value"} <= columns:
            key_col = "key"
            value_col = "value"
        else:
            return 0
        cur = conn.execute(
            f"SELECT call, {key_col}, {value_col} FROM user_prefs WHERE {key_col} = 'password'"
        )
        rows = cur.fetchall()
        updated = 0
        for call, key, value in rows:
            raw = str(value or "").strip()
            if not raw or is_password_hash(raw):
                continue
            conn.execute(
                f"UPDATE user_prefs SET {value_col} = ? WHERE call = ? AND {key_col} = ?",
                (hash_password(raw), str(call), str(key)),
            )
            updated += 1
        conn.commit()
        return updated
    finally:
        conn.close()


def _seed_strings(template_path: str, config_path: str) -> bool:
    if not template_path:
        return False
    template = Path(template_path)
    if not template.exists():
        return False
    target = Path(config_path).resolve().with_name("strings.toml")
    if target.exists():
        return False
    target.write_text(template.read_text(encoding="utf-8"), encoding="utf-8")
    try:
        target.chmod(0o640)
    except OSError:
        pass
    return True


async def _run(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    password_updates = _migrate_passwords(cfg.store.sqlite_path)
    seeded_strings = _seed_strings(args.strings_template, args.config)
    print(
        json.dumps(
            {
                "password_hash_upgrades": password_updates,
                "seeded_strings_toml": seeded_strings,
            },
            separators=(",", ":"),
        )
    )
    return 0


def main() -> None:
    args = _build_parser().parse_args()
    raise SystemExit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
