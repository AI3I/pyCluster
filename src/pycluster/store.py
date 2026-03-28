from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable
import asyncio
import fnmatch

from .models import Spot
from .shdx import ShDxQuery


SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS spots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    freq_khz REAL NOT NULL,
    dx_call TEXT NOT NULL,
    epoch INTEGER NOT NULL,
    info TEXT NOT NULL,
    spotter TEXT NOT NULL,
    source_node TEXT NOT NULL,
    raw TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_spots_epoch ON spots(epoch DESC);
CREATE INDEX IF NOT EXISTS idx_spots_dx_call ON spots(dx_call);

CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sender TEXT NOT NULL,
    recipient TEXT NOT NULL,
    epoch INTEGER NOT NULL,
    body TEXT NOT NULL,
    read_epoch INTEGER,
    parent_id INTEGER
);
CREATE INDEX IF NOT EXISTS idx_messages_recipient ON messages(recipient, id DESC);
CREATE INDEX IF NOT EXISTS idx_messages_sender ON messages(sender, id DESC);

CREATE TABLE IF NOT EXISTS bulletins (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    category TEXT NOT NULL,
    sender TEXT NOT NULL,
    scope TEXT NOT NULL,
    epoch INTEGER NOT NULL,
    body TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_bulletins_cat_id ON bulletins(category, id DESC);

CREATE TABLE IF NOT EXISTS user_prefs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    call TEXT NOT NULL,
    pref_key TEXT NOT NULL,
    pref_value TEXT NOT NULL,
    updated_epoch INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_user_prefs_call_key ON user_prefs(call, pref_key);
CREATE INDEX IF NOT EXISTS idx_user_prefs_call ON user_prefs(call);

CREATE TABLE IF NOT EXISTS filter_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    call TEXT NOT NULL,
    family TEXT NOT NULL,
    action TEXT NOT NULL,
    slot INTEGER NOT NULL,
    expr TEXT NOT NULL,
    updated_epoch INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_filter_rules_unique ON filter_rules(call, family, action, slot);
CREATE INDEX IF NOT EXISTS idx_filter_rules_call ON filter_rules(call);

CREATE TABLE IF NOT EXISTS deny_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    pattern TEXT NOT NULL,
    updated_epoch INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_deny_rules_unique ON deny_rules(kind, pattern);
CREATE INDEX IF NOT EXISTS idx_deny_rules_kind ON deny_rules(kind);

CREATE TABLE IF NOT EXISTS buddy_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    call TEXT NOT NULL,
    buddy_call TEXT NOT NULL,
    updated_epoch INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_buddy_entries_unique ON buddy_entries(call, buddy_call);
CREATE INDEX IF NOT EXISTS idx_buddy_entries_call ON buddy_entries(call);

CREATE TABLE IF NOT EXISTS usdb_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    call TEXT NOT NULL,
    field TEXT NOT NULL,
    value TEXT NOT NULL,
    updated_epoch INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_usdb_entries_unique ON usdb_entries(call, field);
CREATE INDEX IF NOT EXISTS idx_usdb_entries_call ON usdb_entries(call);

CREATE TABLE IF NOT EXISTS user_vars (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    call TEXT NOT NULL,
    var_key TEXT NOT NULL,
    var_value TEXT NOT NULL,
    updated_epoch INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_user_vars_unique ON user_vars(call, var_key);
CREATE INDEX IF NOT EXISTS idx_user_vars_call ON user_vars(call);

CREATE TABLE IF NOT EXISTS user_registry (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    call TEXT NOT NULL,
    display_name TEXT NOT NULL DEFAULT '',
    home_node TEXT NOT NULL DEFAULT '',
    address TEXT NOT NULL DEFAULT '',
    qth TEXT NOT NULL DEFAULT '',
    qra TEXT NOT NULL DEFAULT '',
    email TEXT NOT NULL DEFAULT '',
    privilege TEXT NOT NULL DEFAULT '',
    last_login_epoch INTEGER NOT NULL DEFAULT 0,
    last_login_peer TEXT NOT NULL DEFAULT '',
    registered_epoch INTEGER NOT NULL,
    updated_epoch INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_user_registry_call ON user_registry(call);

CREATE TABLE IF NOT EXISTS user_startup_commands (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    call TEXT NOT NULL,
    command TEXT NOT NULL,
    updated_epoch INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_startup_call_id ON user_startup_commands(call, id);
"""


class SpotStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = asyncio.Lock()
        self._spot_dupe_enabled = True
        self._spot_dupe_ttl_seconds = 900
        self._spot_dupe_cache: dict[str, int] = {}
        self._conn.executescript(SCHEMA_SQL)
        # Migration-safe add for older DBs created before address field existed.
        try:
            self._conn.execute("ALTER TABLE user_registry ADD COLUMN address TEXT NOT NULL DEFAULT ''")
        except sqlite3.OperationalError:
            pass
        try:
            self._conn.execute("ALTER TABLE user_registry ADD COLUMN home_node TEXT NOT NULL DEFAULT ''")
        except sqlite3.OperationalError:
            pass
        try:
            self._conn.execute("ALTER TABLE user_registry ADD COLUMN last_login_epoch INTEGER NOT NULL DEFAULT 0")
        except sqlite3.OperationalError:
            pass
        try:
            self._conn.execute("ALTER TABLE user_registry ADD COLUMN last_login_peer TEXT NOT NULL DEFAULT ''")
        except sqlite3.OperationalError:
            pass
        self._conn.commit()

    def _normalize_privilege(self, privilege: str | None) -> str:
        p = str(privilege or "").strip().lower()
        if p == "admin":
            return "sysop"
        if p in {"", "user", "op", "operator", "sysop"}:
            if p in {"op", "operator"}:
                return "user"
            return p
        return ""

    async def close(self) -> None:
        async with self._lock:
            self._conn.close()

    async def optimize(self) -> dict[str, int]:
        async with self._lock:
            # Keep this lightweight for small Pi-class systems.
            self._conn.execute("PRAGMA optimize")
            counts: dict[str, int] = {}
            for table in ("spots", "messages", "bulletins", "user_prefs"):
                row = self._conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()
                counts[table] = int(row["n"] if row is not None else 0)
            self._conn.commit()
        return counts

    async def apply_retention(
        self,
        now_epoch: int,
        *,
        spots_days: int = 0,
        messages_days: int = 0,
        bulletins_days: int = 0,
    ) -> dict[str, int]:
        async with self._lock:
            removed = {"spots": 0, "messages": 0, "bulletins": 0}
            plans = [
                ("spots", "spots", spots_days),
                ("messages", "messages", messages_days),
                ("bulletins", "bulletins", bulletins_days),
            ]
            for key, table, days in plans:
                keep_days = max(0, int(days or 0))
                if keep_days <= 0:
                    continue
                cutoff = int(now_epoch - keep_days * 86400)
                cur = self._conn.execute(f"DELETE FROM {table} WHERE epoch < ?", (cutoff,))
                removed[key] = int(cur.rowcount if cur.rowcount is not None else 0)
            self._conn.commit()
            self._conn.execute("PRAGMA optimize")
            self._conn.commit()
        return removed

    async def add_spot(self, spot: Spot) -> bool:
        async with self._lock:
            if self._spot_blocked_nolock(spot):
                return False
            if self._spot_dupe_enabled and self._spot_duplicate_nolock(spot):
                return False
            self._conn.execute(
                """
                INSERT INTO spots(freq_khz, dx_call, epoch, info, spotter, source_node, raw)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (spot.freq_khz, spot.dx_call, spot.epoch, spot.info, spot.spotter, spot.source_node, spot.raw),
            )
            self._conn.commit()
        return True

    async def add_spots(self, spots: Iterable[Spot]) -> int:
        async with self._lock:
            values = []
            for s in spots:
                if self._spot_blocked_nolock(s):
                    continue
                if self._spot_dupe_enabled and self._spot_duplicate_nolock(s):
                    continue
                values.append((s.freq_khz, s.dx_call, s.epoch, s.info, s.spotter, s.source_node, s.raw))
            if not values:
                return 0
            self._conn.executemany(
                """
                INSERT INTO spots(freq_khz, dx_call, epoch, info, spotter, source_node, raw)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                values,
            )
            self._conn.commit()
        return len(values)

    def _spot_dupe_key(self, spot: Spot) -> str:
        freq = f"{spot.freq_khz:.1f}"
        info = (spot.info or "").strip().lower()
        dx_call = (spot.dx_call or "").strip().upper()
        # De-dupe across multiple linked nodes by the DX/frequency/comment tuple
        # rather than the spotter, so the same spot relayed by different peers
        # or seen from different upstream paths does not double-render locally.
        return f"{dx_call}|{freq}|{info}"

    def _prune_spot_dupes_nolock(self, now_epoch: int) -> None:
        if not self._spot_dupe_cache:
            return
        cutoff = now_epoch - max(0, self._spot_dupe_ttl_seconds)
        stale = [k for k, ts in self._spot_dupe_cache.items() if ts < cutoff]
        for k in stale:
            self._spot_dupe_cache.pop(k, None)

    def _spot_duplicate_nolock(self, spot: Spot) -> bool:
        now_epoch = int(spot.epoch)
        self._prune_spot_dupes_nolock(now_epoch)
        k = self._spot_dupe_key(spot)
        prev = self._spot_dupe_cache.get(k)
        if prev is not None and now_epoch - prev <= self._spot_dupe_ttl_seconds:
            return True
        self._spot_dupe_cache[k] = now_epoch
        return False

    async def set_spot_dupe_enabled(self, enabled: bool) -> None:
        async with self._lock:
            self._spot_dupe_enabled = bool(enabled)

    async def spot_dupe_enabled(self) -> bool:
        async with self._lock:
            return self._spot_dupe_enabled

    async def clear_spot_dupes(self) -> int:
        async with self._lock:
            n = len(self._spot_dupe_cache)
            self._spot_dupe_cache.clear()
            return n

    def _deny_rules_nolock(self) -> dict[str, list[str]]:
        cur = self._conn.execute(
            """
            SELECT kind, pattern
            FROM deny_rules
            ORDER BY kind, pattern
            """
        )
        out: dict[str, list[str]] = {"baddx": [], "badspotter": [], "badnode": [], "badword": []}
        for r in cur.fetchall():
            kind = str(r["kind"]).strip().lower()
            pat = str(r["pattern"]).strip()
            if kind in out and pat:
                out[kind].append(pat)
        return out

    def _matches_any_glob(self, value: str, patterns: list[str]) -> bool:
        v = value.upper()
        for p in patterns:
            pp = p.upper()
            if fnmatch.fnmatchcase(v, pp):
                return True
            if "*" not in pp and "?" not in pp and v == pp:
                return True
        return False

    def _spot_blocked_nolock(self, spot: Spot) -> bool:
        rules = self._deny_rules_nolock()
        if self._matches_any_glob(spot.dx_call, rules["baddx"]):
            return True
        if self._matches_any_glob(spot.spotter, rules["badspotter"]):
            return True
        if self._matches_any_glob(spot.source_node, rules["badnode"]):
            return True
        info_l = (spot.info or "").lower()
        for word in rules["badword"]:
            if word.lower() in info_l:
                return True
        return False

    async def latest_spots(self, limit: int = 20) -> list[sqlite3.Row]:
        limit = max(1, min(limit, 200))
        async with self._lock:
            cur = self._conn.execute(
                """
                SELECT freq_khz, dx_call, epoch, info, spotter, source_node, raw
                FROM spots
                ORDER BY epoch DESC, id DESC
                LIMIT ?
                """,
                (limit,),
            )
            return cur.fetchall()

    async def search_spots(self, query: ShDxQuery) -> list[sqlite3.Row]:
        clauses: list[str] = []
        params: list[object] = []

        if query.prefix_pattern:
            if query.prefix_exact:
                # Exact match mode for bare callsign/prefix token.
                pat = query.prefix_pattern.replace("%", "").replace("_", "")
                clauses.append("dx_call = ?")
                params.append(pat)
            else:
                clauses.append("dx_call LIKE ?")
                params.append(query.prefix_pattern)

        if query.spotter:
            clauses.append("spotter = ?")
            params.append(query.spotter)

        if query.freq_low is not None:
            clauses.append("freq_khz >= ?")
            params.append(query.freq_low)

        if query.freq_high is not None:
            clauses.append("freq_khz <= ?")
            params.append(query.freq_high)

        if query.info_contains:
            clauses.append("info LIKE ?")
            params.append(f"%{query.info_contains}%")

        if query.since_epoch is not None:
            clauses.append("epoch >= ?")
            params.append(query.since_epoch)

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        limit = max(1, min(query.limit, 200))
        params.append(limit)

        sql = (
            "SELECT freq_khz, dx_call, epoch, info, spotter, source_node, raw "
            "FROM spots "
            f"{where} "
            "ORDER BY epoch DESC, freq_khz ASC, id DESC "
            "LIMIT ?"
        )

        async with self._lock:
            cur = self._conn.execute(sql, tuple(params))
            return cur.fetchall()

    async def count_spots(self) -> int:
        async with self._lock:
            cur = self._conn.execute("SELECT COUNT(*) AS c FROM spots")
            row = cur.fetchone()
            return int(row["c"]) if row else 0

    async def count_spots_in_range(self, freq_low_khz: float, freq_high_khz: float) -> int:
        lo = float(freq_low_khz)
        hi = float(freq_high_khz)
        if lo > hi:
            lo, hi = hi, lo
        async with self._lock:
            cur = self._conn.execute(
                "SELECT COUNT(*) AS c FROM spots WHERE freq_khz >= ? AND freq_khz <= ?",
                (lo, hi),
            )
            row = cur.fetchone()
            return int(row["c"]) if row else 0

    async def count_spots_by_prefix(self, prefix: str) -> int:
        p = (prefix or "").strip().upper()
        if not p:
            return 0
        async with self._lock:
            cur = self._conn.execute(
                "SELECT COUNT(*) AS c FROM spots WHERE dx_call LIKE ?",
                (p + "%",),
            )
            row = cur.fetchone()
            return int(row["c"]) if row else 0

    async def latest_spot_for_call(self, call: str) -> sqlite3.Row | None:
        c = (call or "").strip().upper()
        if not c:
            return None
        async with self._lock:
            cur = self._conn.execute(
                """
                SELECT freq_khz, dx_call, epoch, info, spotter, source_node
                FROM spots
                WHERE dx_call = ?
                ORDER BY epoch DESC, id DESC
                LIMIT 1
                """,
                (c,),
            )
            return cur.fetchone()

    async def add_message(
        self,
        sender: str,
        recipient: str,
        epoch: int,
        body: str,
        parent_id: int | None = None,
    ) -> int:
        async with self._lock:
            cur = self._conn.execute(
                """
                INSERT INTO messages(sender, recipient, epoch, body, read_epoch, parent_id)
                VALUES (?, ?, ?, ?, NULL, ?)
                """,
                (sender.upper(), recipient.upper(), epoch, body, parent_id),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    async def list_messages(self, recipient: str, limit: int = 50) -> list[sqlite3.Row]:
        limit = max(1, min(limit, 200))
        async with self._lock:
            cur = self._conn.execute(
                """
                SELECT id, sender, recipient, epoch, body, read_epoch, parent_id
                FROM messages
                WHERE recipient = ? OR recipient = 'ALL'
                ORDER BY id DESC
                LIMIT ?
                """,
                (recipient.upper(), limit),
            )
            return cur.fetchall()

    async def get_message_for_recipient(self, recipient: str, msg_id: int) -> sqlite3.Row | None:
        async with self._lock:
            cur = self._conn.execute(
                """
                SELECT id, sender, recipient, epoch, body, read_epoch, parent_id
                FROM messages
                WHERE id = ? AND (recipient = ? OR recipient = 'ALL')
                """,
                (msg_id, recipient.upper()),
            )
            return cur.fetchone()

    async def mark_message_read(self, msg_id: int, read_epoch: int) -> None:
        async with self._lock:
            self._conn.execute("UPDATE messages SET read_epoch = ? WHERE id = ?", (read_epoch, msg_id))
            self._conn.commit()

    async def message_counts(self, recipient: str) -> tuple[int, int]:
        r = recipient.upper()
        async with self._lock:
            cur = self._conn.execute(
                """
                SELECT
                  COUNT(*) AS total,
                  SUM(CASE WHEN read_epoch IS NULL THEN 1 ELSE 0 END) AS unread
                FROM messages
                WHERE recipient = ? OR recipient = 'ALL'
                """,
                (r,),
            )
            row = cur.fetchone()
            if not row:
                return 0, 0
            total = int(row["total"] or 0)
            unread = int(row["unread"] or 0)
            return total, unread

    async def add_bulletin(self, category: str, sender: str, scope: str, epoch: int, body: str) -> int:
        cat = category.strip().lower()
        if not cat:
            raise ValueError("category cannot be empty")
        async with self._lock:
            cur = self._conn.execute(
                """
                INSERT INTO bulletins(category, sender, scope, epoch, body)
                VALUES (?, ?, ?, ?, ?)
                """,
                (cat, sender.upper(), scope.upper(), epoch, body),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    async def list_bulletins(self, category: str, limit: int = 20) -> list[sqlite3.Row]:
        cat = category.strip().lower()
        if not cat:
            return []
        limit = max(1, min(limit, 200))
        async with self._lock:
            cur = self._conn.execute(
                """
                SELECT id, category, sender, scope, epoch, body
                FROM bulletins
                WHERE category = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (cat, limit),
            )
            return cur.fetchall()

    async def set_user_pref(self, call: str, key: str, value: str, epoch: int) -> None:
        c = call.strip().upper()
        k = key.strip().lower()
        if not c or not k:
            return
        async with self._lock:
            self._conn.execute(
                """
                INSERT INTO user_prefs(call, pref_key, pref_value, updated_epoch)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(call, pref_key)
                DO UPDATE SET
                    pref_value = excluded.pref_value,
                    updated_epoch = excluded.updated_epoch
                """,
                (c, k, value, epoch),
            )
            self._conn.commit()

    async def delete_user_pref(self, call: str, key: str) -> int:
        c = call.strip().upper()
        k = key.strip().lower()
        if not c or not k:
            return 0
        async with self._lock:
            cur = self._conn.execute(
                "DELETE FROM user_prefs WHERE call = ? AND pref_key = ?",
                (c, k),
            )
            self._conn.commit()
            return int(cur.rowcount or 0)

    async def get_user_pref(self, call: str, key: str) -> str | None:
        c = call.strip().upper()
        k = key.strip().lower()
        if not c or not k:
            return None
        async with self._lock:
            cur = self._conn.execute(
                """
                SELECT pref_value
                FROM user_prefs
                WHERE call = ? AND pref_key = ?
                LIMIT 1
                """,
                (c, k),
            )
            row = cur.fetchone()
            return str(row["pref_value"]) if row else None

    async def list_user_prefs(self, call: str) -> dict[str, str]:
        c = call.strip().upper()
        if not c:
            return {}
        async with self._lock:
            cur = self._conn.execute(
                """
                SELECT pref_key, pref_value
                FROM user_prefs
                WHERE call = ?
                """,
                (c,),
            )
            rows = cur.fetchall()
            return {str(r["pref_key"]): str(r["pref_value"]) for r in rows}

    async def set_filter_rule(
        self,
        call: str,
        family: str,
        action: str,
        slot: int,
        expr: str,
        epoch: int,
    ) -> None:
        c = call.strip().upper()
        fam = family.strip().lower()
        act = action.strip().lower()
        if not c or not fam or act not in {"accept", "reject"}:
            return
        s = max(0, min(int(slot), 9))
        async with self._lock:
            self._conn.execute(
                """
                INSERT INTO filter_rules(call, family, action, slot, expr, updated_epoch)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(call, family, action, slot)
                DO UPDATE SET
                    expr = excluded.expr,
                    updated_epoch = excluded.updated_epoch
                """,
                (c, fam, act, s, expr, epoch),
            )
            self._conn.commit()

    async def clear_filter_rules(self, call: str, family: str, slot: int | str = "all") -> None:
        c = call.strip().upper()
        fam = family.strip().lower()
        if not c or not fam:
            return
        async with self._lock:
            if slot == "all":
                self._conn.execute(
                    "DELETE FROM filter_rules WHERE call = ? AND family = ?",
                    (c, fam),
                )
            else:
                s = max(0, min(int(slot), 9))
                self._conn.execute(
                    "DELETE FROM filter_rules WHERE call = ? AND family = ? AND slot = ?",
                    (c, fam, s),
                )
            self._conn.commit()

    async def list_filter_rules(self, call: str) -> list[sqlite3.Row]:
        c = call.strip().upper()
        if not c:
            return []
        async with self._lock:
            cur = self._conn.execute(
                """
                SELECT call, family, action, slot, expr
                FROM filter_rules
                WHERE call = ?
                ORDER BY family, action, slot
                """,
                (c,),
            )
            return cur.fetchall()

    async def add_deny_rule(self, kind: str, pattern: str, epoch: int) -> None:
        k = kind.strip().lower()
        p = pattern.strip()
        if k not in {"baddx", "badspotter", "badnode", "badword"} or not p:
            return
        async with self._lock:
            self._conn.execute(
                """
                INSERT INTO deny_rules(kind, pattern, updated_epoch)
                VALUES (?, ?, ?)
                ON CONFLICT(kind, pattern)
                DO UPDATE SET updated_epoch = excluded.updated_epoch
                """,
                (k, p, epoch),
            )
            self._conn.commit()

    async def remove_deny_rule(self, kind: str, pattern: str | None = None) -> int:
        k = kind.strip().lower()
        if k not in {"baddx", "badspotter", "badnode", "badword"}:
            return 0
        async with self._lock:
            if pattern is None or pattern.strip().lower() == "all":
                cur = self._conn.execute("DELETE FROM deny_rules WHERE kind = ?", (k,))
            else:
                cur = self._conn.execute("DELETE FROM deny_rules WHERE kind = ? AND pattern = ?", (k, pattern.strip()))
            self._conn.commit()
            return int(cur.rowcount or 0)

    async def list_deny_rules(self, kind: str) -> list[str]:
        k = kind.strip().lower()
        if k not in {"baddx", "badspotter", "badnode", "badword"}:
            return []
        async with self._lock:
            cur = self._conn.execute(
                """
                SELECT pattern
                FROM deny_rules
                WHERE kind = ?
                ORDER BY pattern
                """,
                (k,),
            )
            return [str(r["pattern"]) for r in cur.fetchall()]

    async def add_buddy(self, call: str, buddy_call: str, epoch: int) -> None:
        c = call.strip().upper()
        b = buddy_call.strip().upper()
        if not c or not b:
            return
        async with self._lock:
            self._conn.execute(
                """
                INSERT INTO buddy_entries(call, buddy_call, updated_epoch)
                VALUES (?, ?, ?)
                ON CONFLICT(call, buddy_call)
                DO UPDATE SET updated_epoch = excluded.updated_epoch
                """,
                (c, b, epoch),
            )
            self._conn.commit()

    async def remove_buddy(self, call: str, buddy_call: str | None = None) -> int:
        c = call.strip().upper()
        if not c:
            return 0
        async with self._lock:
            if buddy_call is None or buddy_call.strip().lower() == "all":
                cur = self._conn.execute("DELETE FROM buddy_entries WHERE call = ?", (c,))
            else:
                cur = self._conn.execute(
                    "DELETE FROM buddy_entries WHERE call = ? AND buddy_call = ?",
                    (c, buddy_call.strip().upper()),
                )
            self._conn.commit()
            return int(cur.rowcount or 0)

    async def list_buddies(self, call: str) -> list[str]:
        c = call.strip().upper()
        if not c:
            return []
        async with self._lock:
            cur = self._conn.execute(
                """
                SELECT buddy_call
                FROM buddy_entries
                WHERE call = ?
                ORDER BY buddy_call
                """,
                (c,),
            )
            return [str(r["buddy_call"]) for r in cur.fetchall()]

    async def set_usdb_entry(self, call: str, field: str, value: str, epoch: int) -> None:
        c = call.strip().upper()
        f = field.strip().lower()
        if not c or not f:
            return
        async with self._lock:
            self._conn.execute(
                """
                INSERT INTO usdb_entries(call, field, value, updated_epoch)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(call, field)
                DO UPDATE SET
                    value = excluded.value,
                    updated_epoch = excluded.updated_epoch
                """,
                (c, f, value, epoch),
            )
            self._conn.commit()

    async def delete_usdb_entries(self, call: str, field: str | None = None) -> int:
        c = call.strip().upper()
        if not c:
            return 0
        async with self._lock:
            if field is None or field.strip().lower() == "all":
                cur = self._conn.execute("DELETE FROM usdb_entries WHERE call = ?", (c,))
            else:
                cur = self._conn.execute(
                    "DELETE FROM usdb_entries WHERE call = ? AND field = ?",
                    (c, field.strip().lower()),
                )
            self._conn.commit()
            return int(cur.rowcount or 0)

    async def list_usdb_entries(self, call: str) -> dict[str, str]:
        c = call.strip().upper()
        if not c:
            return {}
        async with self._lock:
            cur = self._conn.execute(
                """
                SELECT field, value
                FROM usdb_entries
                WHERE call = ?
                ORDER BY field
                """,
                (c,),
            )
            return {str(r["field"]): str(r["value"]) for r in cur.fetchall()}

    async def set_user_var(self, call: str, key: str, value: str, epoch: int) -> None:
        c = call.strip().upper()
        k = key.strip().lower()
        if not c or not k:
            return
        async with self._lock:
            self._conn.execute(
                """
                INSERT INTO user_vars(call, var_key, var_value, updated_epoch)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(call, var_key)
                DO UPDATE SET
                    var_value = excluded.var_value,
                    updated_epoch = excluded.updated_epoch
                """,
                (c, k, value, epoch),
            )
            self._conn.commit()

    async def delete_user_vars(self, call: str, key: str | None = None) -> int:
        c = call.strip().upper()
        if not c:
            return 0
        async with self._lock:
            if key is None or key.strip().lower() == "all":
                cur = self._conn.execute("DELETE FROM user_vars WHERE call = ?", (c,))
            else:
                cur = self._conn.execute(
                    "DELETE FROM user_vars WHERE call = ? AND var_key = ?",
                    (c, key.strip().lower()),
                )
            self._conn.commit()
            return int(cur.rowcount or 0)

    async def list_user_vars(self, call: str) -> dict[str, str]:
        c = call.strip().upper()
        if not c:
            return {}
        async with self._lock:
            cur = self._conn.execute(
                """
                SELECT var_key, var_value
                FROM user_vars
                WHERE call = ?
                ORDER BY var_key
                """,
                (c,),
            )
            return {str(r["var_key"]): str(r["var_value"]) for r in cur.fetchall()}

    async def upsert_user_registry(
        self,
        call: str,
        epoch: int,
        *,
        display_name: str | None = None,
        home_node: str | None = None,
        address: str | None = None,
        qth: str | None = None,
        qra: str | None = None,
        email: str | None = None,
        privilege: str | None = None,
    ) -> None:
        c = call.strip().upper()
        if not c:
            return
        async with self._lock:
            cur = self._conn.execute(
                """
                SELECT display_name, home_node, address, qth, qra, email, privilege, last_login_epoch, last_login_peer, registered_epoch
                FROM user_registry
                WHERE call = ?
                LIMIT 1
                """,
                (c,),
            )
            row = cur.fetchone()
            name_v = str(row["display_name"]) if row and display_name is None else (display_name or "")
            home_node_v = str(row["home_node"]) if row and home_node is None else (home_node or "")
            addr_v = str(row["address"]) if row and address is None else (address or "")
            qth_v = str(row["qth"]) if row and qth is None else (qth or "")
            qra_v = str(row["qra"]) if row and qra is None else (qra or "")
            email_v = str(row["email"]) if row and email is None else (email or "")
            priv_v = str(row["privilege"]) if row and privilege is None else self._normalize_privilege(privilege)
            priv_v = self._normalize_privilege(priv_v)
            last_login_epoch_v = int(row["last_login_epoch"]) if row else 0
            last_login_peer_v = str(row["last_login_peer"]) if row else ""
            reg_epoch = int(row["registered_epoch"]) if row else epoch

            self._conn.execute(
                """
                INSERT INTO user_registry(
                    call, display_name, home_node, address, qth, qra, email, privilege,
                    last_login_epoch, last_login_peer, registered_epoch, updated_epoch
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(call)
                DO UPDATE SET
                    display_name = excluded.display_name,
                    home_node = excluded.home_node,
                    address = excluded.address,
                    qth = excluded.qth,
                    qra = excluded.qra,
                    email = excluded.email,
                    privilege = excluded.privilege,
                    last_login_epoch = excluded.last_login_epoch,
                    last_login_peer = excluded.last_login_peer,
                    registered_epoch = excluded.registered_epoch,
                    updated_epoch = excluded.updated_epoch
                """,
                (
                    c, name_v, home_node_v, addr_v, qth_v, qra_v, email_v, priv_v,
                    last_login_epoch_v, last_login_peer_v, reg_epoch, epoch,
                ),
            )
            self._conn.commit()

    async def rename_user_registry(self, old_call: str, new_call: str, epoch: int) -> bool:
        old_c = old_call.strip().upper()
        new_c = new_call.strip().upper()
        if not old_c or not new_c or old_c == new_c:
            return False
        async with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM user_registry WHERE call = ? LIMIT 1",
                (old_c,),
            ).fetchone()
            if row is None:
                return False
            clash = self._conn.execute(
                "SELECT 1 FROM user_registry WHERE call = ? LIMIT 1",
                (new_c,),
            ).fetchone()
            if clash is not None:
                raise ValueError("target callsign already exists")
            self._conn.execute(
                "UPDATE user_registry SET call = ?, updated_epoch = ? WHERE call = ?",
                (new_c, epoch, old_c),
            )
            for table in (
                "user_prefs",
                "filter_rules",
                "buddy_entries",
                "usdb_entries",
                "user_vars",
                "user_startup_commands",
            ):
                self._conn.execute(f"UPDATE {table} SET call = ? WHERE call = ?", (new_c, old_c))
            self._conn.commit()
            return True

    async def rename_call_namespace(self, old_call: str, new_call: str) -> bool:
        old_c = old_call.strip().upper()
        new_c = new_call.strip().upper()
        if not old_c or not new_c or old_c == new_c:
            return False
        tables = (
            "user_prefs",
            "filter_rules",
            "buddy_entries",
            "usdb_entries",
            "user_vars",
            "user_startup_commands",
        )
        async with self._lock:
            has_old = False
            for table in tables:
                row = self._conn.execute(f"SELECT 1 FROM {table} WHERE call = ? LIMIT 1", (old_c,)).fetchone()
                if row is not None:
                    has_old = True
                    break
            if not has_old:
                return False
            for table in tables:
                clash = self._conn.execute(f"SELECT 1 FROM {table} WHERE call = ? LIMIT 1", (new_c,)).fetchone()
                if clash is not None:
                    raise ValueError(f"target callsign already exists in {table}")
            for table in tables:
                self._conn.execute(f"UPDATE {table} SET call = ? WHERE call = ?", (new_c, old_c))
            self._conn.commit()
            return True

    async def delete_user_registry(self, call: str) -> int:
        c = call.strip().upper()
        if not c:
            return 0
        async with self._lock:
            cur = self._conn.execute("DELETE FROM user_registry WHERE call = ?", (c,))
            self._conn.commit()
            return int(cur.rowcount or 0)

    async def delete_user_data(self, call: str, scopes: set[str] | None = None) -> dict[str, int]:
        c = call.strip().upper()
        if not c:
            return {"prefs": 0, "vars": 0, "usdb": 0, "buddy": 0, "startup": 0, "filters": 0}
        wanted = scopes or {"prefs", "vars", "usdb", "buddy", "startup", "filters"}
        async with self._lock:
            counts: dict[str, int] = {}
            counts["prefs"] = 0
            counts["vars"] = 0
            counts["usdb"] = 0
            counts["buddy"] = 0
            counts["startup"] = 0
            counts["filters"] = 0
            if "prefs" in wanted:
                cur = self._conn.execute("DELETE FROM user_prefs WHERE call = ?", (c,))
                counts["prefs"] = int(cur.rowcount or 0)
            if "vars" in wanted:
                cur = self._conn.execute("DELETE FROM user_vars WHERE call = ?", (c,))
                counts["vars"] = int(cur.rowcount or 0)
            if "usdb" in wanted:
                cur = self._conn.execute("DELETE FROM usdb_entries WHERE call = ?", (c,))
                counts["usdb"] = int(cur.rowcount or 0)
            if "buddy" in wanted:
                cur = self._conn.execute("DELETE FROM buddy_entries WHERE call = ?", (c,))
                counts["buddy"] = int(cur.rowcount or 0)
            if "startup" in wanted:
                cur = self._conn.execute("DELETE FROM user_startup_commands WHERE call = ?", (c,))
                counts["startup"] = int(cur.rowcount or 0)
            if "filters" in wanted:
                cur = self._conn.execute("DELETE FROM filter_rules WHERE call = ?", (c,))
                counts["filters"] = int(cur.rowcount or 0)
            self._conn.commit()
        return counts

    async def get_user_registry(self, call: str) -> sqlite3.Row | None:
        c = call.strip().upper()
        if not c:
            return None
        async with self._lock:
            cur = self._conn.execute(
                """
                SELECT call, display_name, home_node, address, qth, qra, email, privilege,
                       last_login_epoch, last_login_peer, registered_epoch, updated_epoch
                FROM user_registry
                WHERE call = ?
                LIMIT 1
                """,
                (c,),
            )
            return cur.fetchone()

    async def list_user_registry(
        self,
        limit: int = 200,
        *,
        offset: int = 0,
        privilege: str = "",
        search: str = "",
    ) -> list[sqlite3.Row]:
        lim = max(1, min(limit, 1000))
        off = max(0, int(offset))
        priv = self._normalize_privilege(privilege)
        srch = str(search or "").strip().upper()
        clauses: list[str] = []
        params: list[object] = []
        if priv:
            clauses.append("LOWER(privilege) = ?")
            params.append(priv)
        if srch:
            clauses.append("(call LIKE ? OR display_name LIKE ? OR home_node LIKE ? OR qth LIKE ? OR email LIKE ?)")
            pat = f"%{srch}%"
            params.extend([pat, pat, pat, pat, pat])
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        async with self._lock:
            cur = self._conn.execute(
                f"""
                SELECT call, display_name, home_node, address, qth, qra, email, privilege,
                       last_login_epoch, last_login_peer, registered_epoch, updated_epoch
                FROM user_registry
                {where}
                ORDER BY call
                LIMIT ? OFFSET ?
                """,
                (*params, lim, off),
            )
            return cur.fetchall()

    async def count_user_registry(self, *, privilege: str = "", search: str = "") -> int:
        priv = self._normalize_privilege(privilege)
        srch = str(search or "").strip().upper()
        clauses: list[str] = []
        params: list[object] = []
        if priv:
            clauses.append("LOWER(privilege) = ?")
            params.append(priv)
        if srch:
            clauses.append("(call LIKE ? OR display_name LIKE ? OR home_node LIKE ? OR qth LIKE ? OR email LIKE ?)")
            pat = f"%{srch}%"
            params.extend([pat, pat, pat, pat, pat])
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        async with self._lock:
            cur = self._conn.execute(
                f"""
                SELECT COUNT(*) AS c
                FROM user_registry
                {where}
                """,
                tuple(params),
            )
            row = cur.fetchone()
            return int(row["c"] or 0) if row else 0

    async def record_login(self, call: str, epoch: int, peer: str) -> None:
        c = call.strip().upper()
        if not c:
            return
        async with self._lock:
            cur = self._conn.execute(
                """
                SELECT display_name, home_node, address, qth, qra, email, privilege, registered_epoch
                FROM user_registry
                WHERE call = ?
                LIMIT 1
                """,
                (c,),
            )
            row = cur.fetchone()
            if row:
                self._conn.execute(
                    """
                    UPDATE user_registry
                    SET last_login_epoch = ?, last_login_peer = ?, updated_epoch = ?
                    WHERE call = ?
                    """,
                    (epoch, peer, epoch, c),
                )
            else:
                self._conn.execute(
                    """
                    INSERT INTO user_registry(
                        call, display_name, home_node, address, qth, qra, email, privilege,
                        last_login_epoch, last_login_peer, registered_epoch, updated_epoch
                    ) VALUES (?, '', '', '', '', '', '', '', ?, ?, ?, ?)
                    """,
                    (c, epoch, peer, epoch, epoch),
                )
            self._conn.commit()

    async def add_startup_command(self, call: str, command: str, epoch: int) -> int:
        c = call.strip().upper()
        cmd = command.strip()
        if not c or not cmd:
            return 0
        async with self._lock:
            cur = self._conn.execute(
                """
                INSERT INTO user_startup_commands(call, command, updated_epoch)
                VALUES (?, ?, ?)
                """,
                (c, cmd, epoch),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    async def list_startup_commands(self, call: str, limit: int = 100) -> list[sqlite3.Row]:
        c = call.strip().upper()
        if not c:
            return []
        lim = max(1, min(limit, 500))
        async with self._lock:
            cur = self._conn.execute(
                """
                SELECT id, command, updated_epoch
                FROM user_startup_commands
                WHERE call = ?
                ORDER BY id
                LIMIT ?
                """,
                (c, lim),
            )
            return cur.fetchall()

    async def remove_startup_command(self, call: str, cmd_id: int) -> int:
        c = call.strip().upper()
        if not c or cmd_id <= 0:
            return 0
        async with self._lock:
            cur = self._conn.execute(
                "DELETE FROM user_startup_commands WHERE call = ? AND id = ?",
                (c, cmd_id),
            )
            self._conn.commit()
            return int(cur.rowcount or 0)

    async def clear_startup_commands(self, call: str) -> int:
        c = call.strip().upper()
        if not c:
            return 0
        async with self._lock:
            cur = self._conn.execute("DELETE FROM user_startup_commands WHERE call = ?", (c,))
            self._conn.commit()
            return int(cur.rowcount or 0)

    async def export_sql_dump(self, file_path: str) -> int:
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        async with self._lock:
            lines = list(self._conn.iterdump())
        text = "\n".join(lines) + "\n"
        path.write_text(text, encoding="utf-8")
        return len(lines)

    async def export_users_csv(self, file_path: str) -> int:
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        async with self._lock:
            cur = self._conn.execute(
                """
                SELECT call, display_name, home_node, address, qth, qra, email, privilege,
                       last_login_epoch, last_login_peer, registered_epoch, updated_epoch
                FROM user_registry
                ORDER BY call
                """
            )
            rows = cur.fetchall()
        header = (
            "call,display_name,home_node,address,qth,qra,email,privilege,"
            "last_login_epoch,last_login_peer,registered_epoch,updated_epoch\n"
        )
        out = [header]
        for r in rows:
            vals = [
                str(r["call"] or ""),
                str(r["display_name"] or ""),
                str(r["home_node"] or ""),
                str(r["address"] or ""),
                str(r["qth"] or ""),
                str(r["qra"] or ""),
                str(r["email"] or ""),
                str(r["privilege"] or ""),
                str(int(r["last_login_epoch"] or 0)),
                str(r["last_login_peer"] or ""),
                str(int(r["registered_epoch"] or 0)),
                str(int(r["updated_epoch"] or 0)),
            ]
            esc = ['"' + v.replace('"', '""') + '"' for v in vals]
            out.append(",".join(esc) + "\n")
        path.write_text("".join(out), encoding="utf-8")
        return len(rows)
