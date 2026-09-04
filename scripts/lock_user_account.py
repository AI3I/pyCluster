#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import sqlite3
import sys
import time


CALL_RE = re.compile(r"^[A-Z0-9]{1,12}(?:-[0-9]{1,2})?$")


def _valid_call(value: str) -> str:
    call = str(value or "").strip().upper()
    if not CALL_RE.match(call):
        raise argparse.ArgumentTypeError("invalid callsign")
    return call


def _set_pref(conn: sqlite3.Connection, call: str, key: str, value: str, now: int) -> None:
    conn.execute(
        """
        INSERT INTO user_prefs(call, pref_key, pref_value, updated_epoch)
        VALUES(?, ?, ?, ?)
        ON CONFLICT(call, pref_key) DO UPDATE SET
            pref_value = excluded.pref_value,
            updated_epoch = excluded.updated_epoch
        """,
        (call, key, value, now),
    )


def lock_account(db_path: str, call: str, reason: str) -> None:
    now = int(time.time())
    with sqlite3.connect(db_path, timeout=5.0) as conn:
        _set_pref(conn, call, "registration_state", "locked", now)
        _set_pref(conn, call, "failed_password_locked_epoch", str(now), now)
        _set_pref(conn, call, "failed_password_count", "0", now)
        if reason:
            _set_pref(conn, call, "blocked_reason", reason[:240], now)
        conn.commit()


def unlock_account(db_path: str, call: str) -> None:
    now = int(time.time())
    with sqlite3.connect(db_path, timeout=5.0) as conn:
        _set_pref(conn, call, "registration_state", "verified", now)
        conn.execute(
            "DELETE FROM user_prefs WHERE call = ? AND pref_key IN ('failed_password_locked_epoch', 'failed_password_count', 'failed_mfa_locked_epoch', 'failed_mfa_count')",
            (call,),
        )
        conn.commit()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Lock or unlock a pyCluster user account from the host shell.")
    parser.add_argument("--db", required=True, help="Path to pyCluster SQLite database")
    parser.add_argument("--call", required=True, type=_valid_call, help="Exact callsign or callsign-SSID to update")
    parser.add_argument("--reason", default="Locked by fail2ban after repeated authentication failures")
    parser.add_argument("--unlock", action="store_true", help="Unlock the account instead of locking it")
    args = parser.parse_args(argv)

    try:
        if args.unlock:
            unlock_account(args.db, args.call)
            print(f"Unlocked {args.call}")
        else:
            lock_account(args.db, args.call, args.reason)
            print(f"Locked {args.call}")
    except sqlite3.Error as exc:
        print(f"database error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
