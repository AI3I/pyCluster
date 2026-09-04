#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
import sys
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from pycluster.auth import hash_password, is_password_hash  # noqa: E402
from pycluster.config import load_config  # noqa: E402


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Apply idempotent migrations for legacy pyCluster state.")
    parser.add_argument("--config", required=True, help="Path to pycluster.toml")
    return parser


def _preference_columns(conn: sqlite3.Connection) -> tuple[str, str, str] | None:
    columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(user_prefs)").fetchall()}
    if {"call", "pref_key", "pref_value"} <= columns:
        return "call", "pref_key", "pref_value"
    if {"call", "key", "value"} <= columns:
        return "call", "key", "value"
    return None


def _migrate_passwords(conn: sqlite3.Connection, columns: tuple[str, str, str]) -> int:
    call_col, key_col, value_col = columns
    rows = conn.execute(
        f"SELECT {call_col}, {key_col}, {value_col} FROM user_prefs WHERE {key_col} = 'password'"
    ).fetchall()
    updated = 0
    for call, key, value in rows:
        raw = str(value or "").strip()
        if not raw or is_password_hash(raw):
            continue
        conn.execute(
            f"UPDATE user_prefs SET {value_col} = ? WHERE {call_col} = ? AND {key_col} = ?",
            (hash_password(raw), str(call), str(key)),
        )
        updated += 1
    return updated


def _split_peer_password(raw: str) -> tuple[str, str]:
    text = str(raw or "").strip()
    if not text:
        return "", ""
    try:
        parts = urlsplit(text)
    except Exception:
        return text, ""
    kept: list[tuple[str, str]] = []
    password = ""
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        if key.lower() == "password" and not password:
            password = value
        else:
            kept.append((key, value))
    if not password:
        return text, ""
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(kept), parts.fragment)), password


def _migrate_peer_passwords(conn: sqlite3.Connection, columns: tuple[str, str, str]) -> tuple[int, int]:
    call_col, key_col, value_col = columns
    rows = conn.execute(
        f"SELECT {call_col}, {key_col}, {value_col} FROM user_prefs "
        f"WHERE {key_col} LIKE 'peer.outbound.%.dsn'"
    ).fetchall()
    migrated = 0
    cleaned = 0
    for call, pref_key, pref_value in rows:
        clean_dsn, password = _split_peer_password(str(pref_value or ""))
        if not password:
            continue
        password_key = str(pref_key)[:-4] + ".password"
        existing = conn.execute(
            f"SELECT {value_col} FROM user_prefs WHERE {call_col} = ? AND {key_col} = ?",
            (str(call), password_key),
        ).fetchone()
        if not existing:
            conn.execute(
                f"INSERT INTO user_prefs({call_col}, {key_col}, {value_col}, updated_epoch) "
                "VALUES (?, ?, ?, strftime('%s','now'))",
                (str(call), password_key, password),
            )
            migrated += 1
        elif not str(existing[0] or "").strip():
            conn.execute(
                f"UPDATE user_prefs SET {value_col} = ?, updated_epoch = strftime('%s','now') "
                f"WHERE {call_col} = ? AND {key_col} = ?",
                (password, str(call), password_key),
            )
            migrated += 1
        if clean_dsn != str(pref_value):
            conn.execute(
                f"UPDATE user_prefs SET {value_col} = ?, updated_epoch = strftime('%s','now') "
                f"WHERE {call_col} = ? AND {key_col} = ?",
                (clean_dsn, str(call), str(pref_key)),
            )
            cleaned += 1
    return migrated, cleaned


def migrate(sqlite_path: str) -> dict[str, int]:
    conn = sqlite3.connect(sqlite_path, timeout=5.0)
    try:
        columns = _preference_columns(conn)
        if columns is None:
            return {"password_hash_upgrades": 0, "peer_passwords_migrated": 0, "peer_password_rows_updated": 0}
        password_updates = _migrate_passwords(conn, columns)
        peer_migrations, peer_updates = _migrate_peer_passwords(conn, columns)
        conn.commit()
        return {
            "password_hash_upgrades": password_updates,
            "peer_passwords_migrated": peer_migrations,
            "peer_password_rows_updated": peer_updates,
        }
    finally:
        conn.close()


async def _run(args: argparse.Namespace) -> int:
    result = migrate(load_config(args.config).store.sqlite_path)
    print(json.dumps(result, separators=(",", ":")))
    return 0


def main() -> None:
    raise SystemExit(asyncio.run(_run(_build_parser().parse_args())))


if __name__ == "__main__":
    main()
