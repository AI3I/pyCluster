#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
import sys
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


def _bootstrap_import_path() -> None:
    here = Path(__file__).resolve()
    root = here.parent.parent
    src = root / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))


_bootstrap_import_path()

from pycluster.config import load_config  # noqa: E402


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Upgrade pyCluster state for the 1.0.6 release.")
    p.add_argument("--config", required=True, help="Path to pycluster.toml")
    return p


def _peer_password_key(pref_key: str) -> str:
    if not pref_key.endswith(".dsn"):
        return pref_key
    return pref_key[:-4] + ".password"


def _split_password_from_dsn(raw: str) -> tuple[str, str]:
    text = str(raw or "").strip()
    if not text:
        return "", ""
    try:
        parts = urlsplit(text)
    except Exception:
        return text, ""
    pairs = parse_qsl(parts.query, keep_blank_values=True)
    kept: list[tuple[str, str]] = []
    password = ""
    for key, value in pairs:
        if key.lower() == "password" and not password:
            password = value
            continue
        kept.append((key, value))
    if not password:
        return text, ""
    clean = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(kept), parts.fragment))
    return clean, password


def _migrate_peer_passwords(sqlite_path: str) -> dict[str, int]:
    conn = sqlite3.connect(sqlite_path)
    try:
        info = conn.execute("PRAGMA table_info(user_prefs)").fetchall()
        columns = {str(row[1]) for row in info}
        if {"call", "pref_key", "pref_value"} <= columns:
            call_col = "call"
            key_col = "pref_key"
            value_col = "pref_value"
            updated_col = "updated_epoch"
        elif {"call", "key", "value"} <= columns:
            call_col = "call"
            key_col = "key"
            value_col = "value"
            updated_col = "updated_epoch"
        else:
            return {"peer_passwords_migrated": 0, "peer_password_rows_updated": 0}
        rows = conn.execute(
            f"SELECT {call_col}, {key_col}, {value_col} FROM user_prefs WHERE {key_col} LIKE 'peer.outbound.%.dsn'"
        ).fetchall()
        migrated = 0
        updated = 0
        for call, pref_key, pref_value in rows:
            clean_dsn, password = _split_password_from_dsn(str(pref_value or ""))
            if not password:
                continue
            pass_key = _peer_password_key(str(pref_key))
            cur = conn.execute(
                f"SELECT {value_col} FROM user_prefs WHERE {call_col} = ? AND {key_col} = ?",
                (str(call), pass_key),
            ).fetchone()
            existing = str(cur[0] or "").strip() if cur else ""
            if not existing:
                if cur:
                    conn.execute(
                        f"UPDATE user_prefs SET {value_col} = ?, {updated_col} = strftime('%s','now') WHERE {call_col} = ? AND {key_col} = ?",
                        (password, str(call), pass_key),
                    )
                else:
                    conn.execute(
                        f"INSERT INTO user_prefs({call_col}, {key_col}, {value_col}, {updated_col}) VALUES (?, ?, ?, strftime('%s','now'))",
                        (str(call), pass_key, password),
                    )
                migrated += 1
            if clean_dsn != str(pref_value):
                conn.execute(
                    f"UPDATE user_prefs SET {value_col} = ?, {updated_col} = strftime('%s','now') WHERE {call_col} = ? AND {key_col} = ?",
                    (clean_dsn, str(call), str(pref_key)),
                )
                updated += 1
        conn.commit()
        return {"peer_passwords_migrated": migrated, "peer_password_rows_updated": updated}
    finally:
        conn.close()


async def _run(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    result = _migrate_peer_passwords(cfg.store.sqlite_path)
    print(json.dumps(result, separators=(",", ":")))
    return 0


def main() -> None:
    args = _build_parser().parse_args()
    raise SystemExit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
