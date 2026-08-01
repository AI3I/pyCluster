from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable
import asyncio
import fnmatch
import json

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
CREATE INDEX IF NOT EXISTS idx_spots_spotter_epoch ON spots(spotter, epoch DESC);

CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sender TEXT NOT NULL,
    recipient TEXT NOT NULL,
    epoch INTEGER NOT NULL,
    body TEXT NOT NULL,
    read_epoch INTEGER,
    parent_id INTEGER,
    origin_node TEXT NOT NULL DEFAULT '',
    route_node TEXT NOT NULL DEFAULT '',
    delivery_state TEXT NOT NULL DEFAULT 'local',
    delivered_epoch INTEGER,
    error_text TEXT NOT NULL DEFAULT ''
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

CREATE TABLE IF NOT EXISTS mfa_challenges (
    challenge_id TEXT PRIMARY KEY,
    call TEXT NOT NULL,
    purpose TEXT NOT NULL,
    code TEXT NOT NULL,
    expires_epoch INTEGER NOT NULL,
    attempts_left INTEGER NOT NULL,
    issued_epoch INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_mfa_challenges_expires ON mfa_challenges(expires_epoch);
CREATE INDEX IF NOT EXISTS idx_mfa_challenges_call_purpose ON mfa_challenges(call, purpose, issued_epoch DESC);

CREATE TABLE IF NOT EXISTS registration_requests (
    call TEXT PRIMARY KEY,
    display_name TEXT NOT NULL DEFAULT '',
    home_node TEXT NOT NULL DEFAULT '',
    qth TEXT NOT NULL DEFAULT '',
    qra TEXT NOT NULL DEFAULT '',
    email TEXT NOT NULL DEFAULT '',
    note TEXT NOT NULL DEFAULT '',
    source TEXT NOT NULL DEFAULT '',
    email_verified INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending',
    requested_epoch INTEGER NOT NULL,
    reviewed_epoch INTEGER NOT NULL DEFAULT 0,
    reviewer TEXT NOT NULL DEFAULT '',
    review_note TEXT NOT NULL DEFAULT '',
    updated_epoch INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_registration_requests_status_epoch ON registration_requests(status, requested_epoch DESC);

CREATE TABLE IF NOT EXISTS py_nodes (
    node_call TEXT PRIMARY KEY,
    node_id TEXT NOT NULL,
    origin_node TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    software_version TEXT NOT NULL,
    protocol_version TEXT NOT NULL,
    public_web_url TEXT NOT NULL DEFAULT '',
    locator TEXT NOT NULL DEFAULT '',
    qth TEXT NOT NULL DEFAULT '',
    sysop_contact TEXT NOT NULL DEFAULT '',
    services_json TEXT NOT NULL DEFAULT '[]',
    capabilities_json TEXT NOT NULL DEFAULT '[]',
    source_node TEXT NOT NULL,
    learned_from TEXT NOT NULL,
    hop_count INTEGER NOT NULL DEFAULT 0,
    confidence TEXT NOT NULL,
    first_seen INTEGER NOT NULL,
    last_seen INTEGER NOT NULL,
    updated_epoch INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    raw_digest TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_py_nodes_expires ON py_nodes(expires_at);
CREATE INDEX IF NOT EXISTS idx_py_nodes_learned_from ON py_nodes(learned_from);
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
        for stmt in (
            "ALTER TABLE messages ADD COLUMN origin_node TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE messages ADD COLUMN route_node TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE messages ADD COLUMN delivery_state TEXT NOT NULL DEFAULT 'local'",
            "ALTER TABLE messages ADD COLUMN delivered_epoch INTEGER",
            "ALTER TABLE messages ADD COLUMN error_text TEXT NOT NULL DEFAULT ''",
        ):
            try:
                self._conn.execute(stmt)
            except sqlite3.OperationalError:
                pass
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_messages_route_state ON messages(route_node, delivery_state, id ASC)"
        )
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
            for table in ("spots", "messages", "bulletins", "user_prefs", "py_nodes"):
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

    async def upsert_py_node_record(self, record: dict[str, object], now_epoch: int) -> str:
        """Store a validated PY node record and report accepted, refreshed, or rejected."""
        node_call = str(record["node_call"]).strip().upper()
        confidence = str(record["confidence"]).strip().lower()
        if confidence not in {"direct", "reported", "local"}:
            raise ValueError("invalid PY node confidence")
        values = {
            "node_call": node_call,
            "node_id": str(record["node_id"]),
            "origin_node": str(record.get("origin_node") or node_call).strip().upper(),
            "sequence": int(record["sequence"]),
            "software_version": str(record["software_version"]),
            "protocol_version": str(record["protocol_version"]),
            "public_web_url": str(record.get("public_web_url") or ""),
            "locator": str(record.get("locator") or ""),
            "qth": str(record.get("qth") or ""),
            "sysop_contact": str(record.get("sysop_contact") or ""),
            "services_json": json.dumps(sorted(set(record.get("services") or [])), separators=(",", ":")),
            "capabilities_json": json.dumps(sorted(set(record.get("capabilities") or [])), separators=(",", ":")),
            "source_node": str(record.get("source_node") or "").strip().upper(),
            "learned_from": str(record.get("learned_from") or "").strip().upper(),
            "hop_count": int(record.get("hop_count") or 0),
            "confidence": confidence,
            "last_seen": int(now_epoch),
            "updated_epoch": int(record["updated_epoch"]),
            "expires_at": int(record["expires_at"]),
            "raw_digest": str(record["raw_digest"]).strip().lower(),
        }
        async with self._lock:
            existing = self._conn.execute(
                "SELECT * FROM py_nodes WHERE node_call = ? LIMIT 1", (node_call,)
            ).fetchone()
            if existing is not None:
                existing_rank = {"reported": 1, "direct": 2, "local": 3}.get(
                    str(existing["confidence"]), 0
                )
                incoming_rank = {"reported": 1, "direct": 2, "local": 3}[confidence]
                existing_valid = int(existing["expires_at"] or 0) > int(now_epoch)
                same_identity = str(existing["node_id"]) == values["node_id"]
                if existing_valid and incoming_rank < existing_rank:
                    return "rejected-confidence"
                authoritative_upgrade = incoming_rank > existing_rank
                if (
                    existing_valid
                    and not authoritative_upgrade
                    and same_identity
                    and values["sequence"] < int(existing["sequence"])
                ):
                    return "rejected-stale"
                if (
                    existing_valid
                    and not authoritative_upgrade
                    and same_identity
                    and values["sequence"] == int(existing["sequence"])
                    and values["raw_digest"] != str(existing["raw_digest"])
                ):
                    return "rejected-conflict"
                if (
                    existing_valid
                    and not authoritative_upgrade
                    and same_identity
                    and values["sequence"] == int(existing["sequence"])
                    and values["raw_digest"] == str(existing["raw_digest"])
                ):
                    self._conn.execute(
                        """
                        UPDATE py_nodes
                        SET last_seen = ?,
                            updated_epoch = MAX(updated_epoch, ?),
                            expires_at = MAX(expires_at, ?)
                        WHERE node_call = ?
                        """,
                        (int(now_epoch), values["updated_epoch"], values["expires_at"], node_call),
                    )
                    self._conn.commit()
                    return "refreshed"
                first_seen = int(existing["first_seen"] or now_epoch)
            else:
                first_seen = int(now_epoch)
            self._conn.execute(
                """
                INSERT INTO py_nodes(
                    node_call, node_id, origin_node, sequence, software_version, protocol_version,
                    public_web_url, locator, qth, sysop_contact, services_json, capabilities_json,
                    source_node, learned_from, hop_count, confidence, first_seen, last_seen,
                    updated_epoch, expires_at, raw_digest
                ) VALUES (
                    :node_call, :node_id, :origin_node, :sequence, :software_version, :protocol_version,
                    :public_web_url, :locator, :qth, :sysop_contact, :services_json, :capabilities_json,
                    :source_node, :learned_from, :hop_count, :confidence, :first_seen, :last_seen,
                    :updated_epoch, :expires_at, :raw_digest
                )
                ON CONFLICT(node_call) DO UPDATE SET
                    node_id=excluded.node_id, origin_node=excluded.origin_node,
                    sequence=excluded.sequence, software_version=excluded.software_version,
                    protocol_version=excluded.protocol_version, public_web_url=excluded.public_web_url,
                    locator=excluded.locator, qth=excluded.qth, sysop_contact=excluded.sysop_contact,
                    services_json=excluded.services_json, capabilities_json=excluded.capabilities_json,
                    source_node=excluded.source_node, learned_from=excluded.learned_from,
                    hop_count=excluded.hop_count, confidence=excluded.confidence,
                    first_seen=excluded.first_seen, last_seen=excluded.last_seen,
                    updated_epoch=excluded.updated_epoch, expires_at=excluded.expires_at,
                    raw_digest=excluded.raw_digest
                """,
                {**values, "first_seen": first_seen},
            )
            self._conn.commit()
        return "accepted"

    @staticmethod
    def _py_node_row(row: sqlite3.Row) -> dict[str, object]:
        out = dict(row)
        for source, target in (("services_json", "services"), ("capabilities_json", "capabilities")):
            try:
                out[target] = json.loads(str(out.pop(source)))
            except (TypeError, ValueError, json.JSONDecodeError):
                out[target] = []
        return out

    async def get_py_node_record(self, node_call: str) -> dict[str, object] | None:
        call = str(node_call or "").strip().upper()
        async with self._lock:
            row = self._conn.execute(
                "SELECT * FROM py_nodes WHERE node_call = ? LIMIT 1", (call,)
            ).fetchone()
        return self._py_node_row(row) if row is not None else None

    async def refresh_py_node_lease(
        self,
        node_call: str,
        node_id: str,
        sequence: int,
        raw_digest: str,
        expires_at: int,
        now_epoch: int,
    ) -> bool:
        """Refresh expiry only when a digest exactly identifies the stored origin record."""
        call = str(node_call or "").strip().upper()
        expiry = int(expires_at)
        now = int(now_epoch)
        if expiry <= now or expiry > now + 30 * 86400:
            return False
        async with self._lock:
            cur = self._conn.execute(
                """
                UPDATE py_nodes
                SET updated_epoch = MAX(
                        updated_epoch,
                        ? - MIN(2592000, MAX(1, expires_at - updated_epoch))
                    ),
                    expires_at = MAX(expires_at, ?),
                    last_seen = ?
                WHERE node_call = ? AND node_id = ? AND sequence = ? AND raw_digest = ?
                """,
                (expiry, expiry, now, call, str(node_id), int(sequence), str(raw_digest).strip().lower()),
            )
            self._conn.commit()
            return bool(cur.rowcount)

    async def list_py_node_records(self, now_epoch: int, *, include_expired: bool = False) -> list[dict[str, object]]:
        where = "" if include_expired else "WHERE expires_at > ?"
        params: tuple[object, ...] = () if include_expired else (int(now_epoch),)
        async with self._lock:
            rows = self._conn.execute(
                f"SELECT * FROM py_nodes {where} ORDER BY node_call", params
            ).fetchall()
        return [self._py_node_row(row) for row in rows]

    async def prune_expired_py_nodes(self, now_epoch: int) -> int:
        async with self._lock:
            cur = self._conn.execute("DELETE FROM py_nodes WHERE expires_at <= ?", (int(now_epoch),))
            self._conn.commit()
            return int(cur.rowcount or 0)

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

    async def add_spots_returning_inserted(self, spots: Iterable[Spot]) -> list[Spot]:
        async with self._lock:
            inserted: list[Spot] = []
            for s in spots:
                if self._spot_blocked_nolock(s):
                    continue
                if self._spot_dupe_enabled and self._spot_duplicate_nolock(s):
                    continue
                self._conn.execute(
                    """
                    INSERT INTO spots(freq_khz, dx_call, epoch, info, spotter, source_node, raw)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (s.freq_khz, s.dx_call, s.epoch, s.info, s.spotter, s.source_node, s.raw),
                )
                inserted.append(s)
            if inserted:
                self._conn.commit()
        return inserted

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

    async def spots_since_epoch(self, cutoff_epoch: int) -> list[sqlite3.Row]:
        async with self._lock:
            cur = self._conn.execute(
                """
                SELECT freq_khz, dx_call, epoch, info, spotter, source_node, raw
                FROM spots
                WHERE epoch >= ?
                ORDER BY epoch DESC, id DESC
                """,
                (int(cutoff_epoch),),
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
        limit = max(1, min(query.limit, 2000))
        offset = max(0, int(getattr(query, "offset", 0) or 0))
        params.append(limit)
        params.append(offset)

        sql = (
            "SELECT freq_khz, dx_call, epoch, info, spotter, source_node, raw "
            "FROM spots "
            f"{where} "
            "ORDER BY epoch DESC, freq_khz ASC, id DESC "
            "LIMIT ? OFFSET ?"
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

    async def count_recent_spots_by_spotter(self, spotter: str, cutoff_epoch: int) -> int:
        target = (spotter or "").strip().upper()
        if not target:
            return 0
        base = target.split("-", 1)[0]
        async with self._lock:
            cur = self._conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM spots
                WHERE epoch >= ?
                  AND (spotter = ? OR spotter LIKE ?)
                """,
                (int(cutoff_epoch), base, base + "-%"),
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
        origin_node: str = "",
        route_node: str = "",
        delivery_state: str = "local",
        delivered_epoch: int | None = None,
        error_text: str = "",
    ) -> int:
        async with self._lock:
            cur = self._conn.execute(
                """
                INSERT INTO messages(
                    sender, recipient, epoch, body, read_epoch, parent_id,
                    origin_node, route_node, delivery_state, delivered_epoch, error_text
                )
                VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?)
                """,
                (
                    sender.upper(),
                    recipient.upper(),
                    epoch,
                    body,
                    parent_id,
                    origin_node.upper(),
                    route_node.upper(),
                    delivery_state,
                    delivered_epoch,
                    error_text,
                ),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    async def list_messages(self, recipient: str, limit: int = 50) -> list[sqlite3.Row]:
        limit = max(1, min(limit, 200))
        async with self._lock:
            cur = self._conn.execute(
                """
                SELECT id, sender, recipient, epoch, body, read_epoch, parent_id,
                       origin_node, route_node, delivery_state, delivered_epoch, error_text
                FROM messages
                WHERE recipient = ? OR recipient = 'ALL'
                ORDER BY id DESC
                LIMIT ?
                """,
                (recipient.upper(), limit),
            )
            return cur.fetchall()

    async def list_sent_messages(self, sender: str, limit: int = 50) -> list[sqlite3.Row]:
        limit = max(1, min(limit, 200))
        async with self._lock:
            cur = self._conn.execute(
                """
                SELECT id, sender, recipient, epoch, body, read_epoch, parent_id,
                       origin_node, route_node, delivery_state, delivered_epoch, error_text
                FROM messages
                WHERE sender = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (sender.upper(), limit),
            )
            return cur.fetchall()

    async def get_message_for_recipient(self, recipient: str, msg_id: int) -> sqlite3.Row | None:
        async with self._lock:
            cur = self._conn.execute(
                """
                SELECT id, sender, recipient, epoch, body, read_epoch, parent_id,
                       origin_node, route_node, delivery_state, delivered_epoch, error_text
                FROM messages
                WHERE id = ? AND (recipient = ? OR recipient = 'ALL')
                """,
                (msg_id, recipient.upper()),
            )
            return cur.fetchone()

    async def get_message(self, msg_id: int) -> sqlite3.Row | None:
        async with self._lock:
            cur = self._conn.execute(
                """
                SELECT id, sender, recipient, epoch, body, read_epoch, parent_id,
                       origin_node, route_node, delivery_state, delivered_epoch, error_text
                FROM messages
                WHERE id = ?
                """,
                (msg_id,),
            )
            return cur.fetchone()

    async def find_message_duplicate(
        self,
        *,
        sender: str,
        recipient: str,
        body: str,
        origin_node: str = "",
        within_seconds: int = 7 * 86400,
        now_epoch: int | None = None,
    ) -> sqlite3.Row | None:
        cutoff = 0
        if now_epoch is not None:
            cutoff = max(0, int(now_epoch) - max(0, int(within_seconds or 0)))
        async with self._lock:
            cur = self._conn.execute(
                """
                SELECT id, sender, recipient, epoch, body, read_epoch, parent_id,
                       origin_node, route_node, delivery_state, delivered_epoch, error_text
                FROM messages
                WHERE sender = ?
                  AND recipient = ?
                  AND body = ?
                  AND origin_node = ?
                  AND epoch >= ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (
                    sender.upper(),
                    recipient.upper(),
                    body,
                    origin_node.upper(),
                    int(cutoff),
                ),
            )
            return cur.fetchone()

    async def mark_message_read(self, msg_id: int, read_epoch: int) -> None:
        async with self._lock:
            self._conn.execute("UPDATE messages SET read_epoch = ? WHERE id = ?", (read_epoch, msg_id))
            self._conn.commit()

    async def set_message_delivery(
        self,
        msg_id: int,
        state: str,
        *,
        delivered_epoch: int | None = None,
        route_node: str | None = None,
        error_text: str | None = None,
    ) -> None:
        clauses = ["delivery_state = ?"]
        params: list[object] = [state]
        if delivered_epoch is not None:
            clauses.append("delivered_epoch = ?")
            params.append(delivered_epoch)
        if route_node is not None:
            clauses.append("route_node = ?")
            params.append(route_node.upper())
        if error_text is not None:
            clauses.append("error_text = ?")
            params.append(error_text)
        params.append(msg_id)
        async with self._lock:
            self._conn.execute(f"UPDATE messages SET {', '.join(clauses)} WHERE id = ?", params)
            self._conn.commit()

    async def list_pending_messages_for_route(self, route_node: str, limit: int = 100) -> list[sqlite3.Row]:
        limit = max(1, min(limit, 500))
        async with self._lock:
            cur = self._conn.execute(
                """
                SELECT id, sender, recipient, epoch, body, read_epoch, parent_id,
                       origin_node, route_node, delivery_state, delivered_epoch, error_text
                FROM messages
                WHERE route_node = ? AND delivery_state = 'pending'
                ORDER BY id ASC
                LIMIT ?
                """,
                (route_node.upper(), limit),
            )
            return cur.fetchall()

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

    async def message_state_counts(self, recipient: str) -> dict[str, int]:
        r = recipient.upper()
        async with self._lock:
            cur = self._conn.execute(
                """
                SELECT delivery_state, COUNT(*) AS total
                FROM messages
                WHERE recipient = ? OR recipient = 'ALL'
                GROUP BY delivery_state
                """,
                (r,),
            )
            rows = cur.fetchall()
            return {str(row["delivery_state"] or "local"): int(row["total"] or 0) for row in rows}

    async def sent_message_state_counts(self, sender: str) -> dict[str, int]:
        s = sender.upper()
        async with self._lock:
            cur = self._conn.execute(
                """
                SELECT delivery_state, COUNT(*) AS total
                FROM messages
                WHERE sender = ?
                GROUP BY delivery_state
                """,
                (s,),
            )
            rows = cur.fetchall()
            return {str(row["delivery_state"] or "local"): int(row["total"] or 0) for row in rows}

    async def route_message_state_counts(self, route_node: str) -> dict[str, int]:
        route = str(route_node or "").strip().upper()
        if not route:
            return {}
        async with self._lock:
            cur = self._conn.execute(
                """
                SELECT delivery_state, COUNT(*) AS total
                FROM messages
                WHERE route_node = ?
                GROUP BY delivery_state
                """,
                (route,),
            )
            rows = cur.fetchall()
            return {str(row["delivery_state"] or "local"): int(row["total"] or 0) for row in rows}

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

    async def find_recent_bulletin_duplicate(
        self,
        category: str,
        sender: str,
        scope: str,
        body: str,
        *,
        since_epoch: int,
    ) -> int | None:
        cat = category.strip().lower()
        snd = sender.strip().upper()
        scp = scope.strip().upper()
        txt = body.strip()
        if not cat or not snd or not scp or not txt:
            return None
        async with self._lock:
            cur = self._conn.execute(
                """
                SELECT id
                FROM bulletins
                WHERE category = ?
                  AND sender = ?
                  AND scope = ?
                  AND body = ?
                  AND epoch >= ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (cat, snd, scp, txt, int(since_epoch)),
            )
            row = cur.fetchone()
            return int(row["id"]) if row else None

    async def save_mfa_challenge(
        self,
        *,
        challenge_id: str,
        call: str,
        purpose: str,
        code: str,
        expires_epoch: int,
        attempts_left: int,
        issued_epoch: int,
    ) -> None:
        async with self._lock:
            self._conn.execute(
                """
                INSERT INTO mfa_challenges(
                    challenge_id, call, purpose, code, expires_epoch, attempts_left, issued_epoch
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(challenge_id)
                DO UPDATE SET
                    call = excluded.call,
                    purpose = excluded.purpose,
                    code = excluded.code,
                    expires_epoch = excluded.expires_epoch,
                    attempts_left = excluded.attempts_left,
                    issued_epoch = excluded.issued_epoch
                """,
                (
                    challenge_id,
                    call.upper(),
                    purpose,
                    code,
                    int(expires_epoch),
                    int(attempts_left),
                    int(issued_epoch),
                ),
            )
            self._conn.commit()

    async def get_mfa_challenge(self, challenge_id: str) -> sqlite3.Row | None:
        async with self._lock:
            cur = self._conn.execute(
                """
                SELECT challenge_id, call, purpose, code, expires_epoch, attempts_left, issued_epoch
                FROM mfa_challenges
                WHERE challenge_id = ?
                """,
                (challenge_id,),
            )
            return cur.fetchone()

    async def update_mfa_challenge_attempts(self, challenge_id: str, attempts_left: int) -> None:
        async with self._lock:
            self._conn.execute(
                "UPDATE mfa_challenges SET attempts_left = ? WHERE challenge_id = ?",
                (int(attempts_left), challenge_id),
            )
            self._conn.commit()

    async def delete_mfa_challenge(self, challenge_id: str) -> None:
        async with self._lock:
            self._conn.execute("DELETE FROM mfa_challenges WHERE challenge_id = ?", (challenge_id,))
            self._conn.commit()

    async def delete_mfa_challenges_for_call(self, call: str, *, include_ssids: bool = True) -> int:
        target = str(call or "").strip().upper()
        if not target:
            return 0
        base = target.split("-", 1)[0]
        async with self._lock:
            if include_ssids:
                cur = self._conn.execute(
                    "DELETE FROM mfa_challenges WHERE call = ? OR call = ? OR call LIKE ?",
                    (target, base, base + "-%"),
                )
            else:
                cur = self._conn.execute("DELETE FROM mfa_challenges WHERE call = ?", (target,))
            self._conn.commit()
            return int(cur.rowcount or 0)

    async def fallback_totp_to_email(
        self,
        call: str,
        epoch: int,
        *,
        keep_challenge_id: str = "",
    ) -> None:
        """Atomically replace TOTP with required email MFA for one exact account."""
        target = str(call or "").strip().upper()
        if not target:
            raise ValueError("callsign is required")
        async with self._lock:
            self._conn.execute("BEGIN")
            try:
                self._conn.execute(
                    "DELETE FROM user_prefs WHERE call = ? AND pref_key IN (?, ?)",
                    (target, "mfa_totp_secret", "mfa_totp_failed_count"),
                )
                self._conn.execute(
                    """
                    INSERT INTO user_prefs(call, pref_key, pref_value, updated_epoch)
                    VALUES(?, 'mfa_email_otp', 'required', ?)
                    ON CONFLICT(call, pref_key) DO UPDATE SET
                        pref_value = excluded.pref_value,
                        updated_epoch = excluded.updated_epoch
                    """,
                    (target, int(epoch)),
                )
                if keep_challenge_id:
                    self._conn.execute(
                        "DELETE FROM mfa_challenges WHERE call = ? AND challenge_id <> ?",
                        (target, keep_challenge_id),
                    )
                else:
                    self._conn.execute("DELETE FROM mfa_challenges WHERE call = ?", (target,))
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    async def delete_expired_mfa_challenges(self, now_epoch: int) -> int:
        async with self._lock:
            cur = self._conn.execute("DELETE FROM mfa_challenges WHERE expires_epoch < ?", (int(now_epoch),))
            self._conn.commit()
            return int(cur.rowcount or 0)

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

    async def upsert_registration_request(
        self,
        call: str,
        epoch: int,
        *,
        display_name: str = "",
        home_node: str = "",
        qth: str = "",
        qra: str = "",
        email: str = "",
        note: str = "",
        source: str = "",
        email_verified: bool = False,
        status: str = "pending",
        reviewer: str = "",
        review_note: str = "",
        reviewed_epoch: int = 0,
    ) -> None:
        c = call.strip().upper()
        if not c:
            return
        stat = str(status or "pending").strip().lower()
        if stat not in {"pending", "approved", "denied"}:
            stat = "pending"
        async with self._lock:
            cur = self._conn.execute(
                """
                SELECT requested_epoch
                FROM registration_requests
                WHERE call = ?
                LIMIT 1
                """,
                (c,),
            )
            row = cur.fetchone()
            requested_epoch = int(row["requested_epoch"]) if row is not None else int(epoch)
            self._conn.execute(
                """
                INSERT INTO registration_requests(
                    call, display_name, home_node, qth, qra, email, note, source,
                    email_verified, status, requested_epoch, reviewed_epoch,
                    reviewer, review_note, updated_epoch
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(call)
                DO UPDATE SET
                    display_name = excluded.display_name,
                    home_node = excluded.home_node,
                    qth = excluded.qth,
                    qra = excluded.qra,
                    email = excluded.email,
                    note = excluded.note,
                    source = excluded.source,
                    email_verified = excluded.email_verified,
                    status = excluded.status,
                    requested_epoch = excluded.requested_epoch,
                    reviewed_epoch = excluded.reviewed_epoch,
                    reviewer = excluded.reviewer,
                    review_note = excluded.review_note,
                    updated_epoch = excluded.updated_epoch
                """,
                (
                    c,
                    display_name.strip(),
                    home_node.strip().upper(),
                    qth.strip(),
                    qra.strip().upper(),
                    email.strip(),
                    note.strip(),
                    source.strip().lower(),
                    1 if email_verified else 0,
                    stat,
                    requested_epoch,
                    int(reviewed_epoch or 0),
                    reviewer.strip().upper(),
                    review_note.strip(),
                    int(epoch),
                ),
            )
            self._conn.commit()

    async def get_registration_request(self, call: str) -> sqlite3.Row | None:
        c = call.strip().upper()
        if not c:
            return None
        async with self._lock:
            cur = self._conn.execute(
                """
                SELECT call, display_name, home_node, qth, qra, email, note, source,
                       email_verified, status, requested_epoch, reviewed_epoch,
                       reviewer, review_note, updated_epoch
                FROM registration_requests
                WHERE call = ?
                LIMIT 1
                """,
                (c,),
            )
            return cur.fetchone()

    async def list_registration_requests(self, *, status: str = "", limit: int = 200, offset: int = 0) -> list[sqlite3.Row]:
        lim = max(1, min(limit, 1000))
        off = max(0, int(offset))
        stat = str(status or "").strip().lower()
        params: list[object] = []
        where = ""
        if stat:
            where = "WHERE status = ?"
            params.append(stat)
        async with self._lock:
            cur = self._conn.execute(
                f"""
                SELECT call, display_name, home_node, qth, qra, email, note, source,
                       email_verified, status, requested_epoch, reviewed_epoch,
                       reviewer, review_note, updated_epoch
                FROM registration_requests
                {where}
                ORDER BY requested_epoch DESC, call
                LIMIT ? OFFSET ?
                """,
                (*params, lim, off),
            )
            return cur.fetchall()

    async def set_registration_request_status(
        self,
        call: str,
        *,
        status: str,
        epoch: int,
        reviewer: str = "",
        review_note: str = "",
    ) -> int:
        c = call.strip().upper()
        stat = str(status or "").strip().lower()
        if not c or stat not in {"pending", "approved", "denied"}:
            return 0
        async with self._lock:
            cur = self._conn.execute(
                """
                UPDATE registration_requests
                SET status = ?, reviewed_epoch = ?, reviewer = ?, review_note = ?, updated_epoch = ?
                WHERE call = ?
                """,
                (stat, int(epoch), reviewer.strip().upper(), review_note.strip(), int(epoch), c),
            )
            self._conn.commit()
            return int(cur.rowcount or 0)

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

    async def delete_user_account(
        self,
        call: str,
        *,
        include_registry: bool = True,
        include_registration_request: bool = True,
    ) -> dict[str, int]:
        c = call.strip().upper()
        counts = {
            "registry": 0,
            "prefs": 0,
            "vars": 0,
            "usdb": 0,
            "buddy": 0,
            "startup": 0,
            "filters": 0,
            "registration_requests": 0,
            "mfa_challenges": 0,
        }
        if not c:
            return counts
        async with self._lock:
            if include_registry:
                cur = self._conn.execute("DELETE FROM user_registry WHERE call = ?", (c,))
                counts["registry"] = int(cur.rowcount or 0)
            cur = self._conn.execute("DELETE FROM user_prefs WHERE call = ?", (c,))
            counts["prefs"] = int(cur.rowcount or 0)
            cur = self._conn.execute("DELETE FROM user_vars WHERE call = ?", (c,))
            counts["vars"] = int(cur.rowcount or 0)
            cur = self._conn.execute("DELETE FROM usdb_entries WHERE call = ?", (c,))
            counts["usdb"] = int(cur.rowcount or 0)
            cur = self._conn.execute("DELETE FROM buddy_entries WHERE call = ?", (c,))
            counts["buddy"] = int(cur.rowcount or 0)
            cur = self._conn.execute("DELETE FROM user_startup_commands WHERE call = ?", (c,))
            counts["startup"] = int(cur.rowcount or 0)
            cur = self._conn.execute("DELETE FROM filter_rules WHERE call = ?", (c,))
            counts["filters"] = int(cur.rowcount or 0)
            if include_registration_request:
                cur = self._conn.execute("DELETE FROM registration_requests WHERE call = ?", (c,))
                counts["registration_requests"] = int(cur.rowcount or 0)
            base = c.split("-", 1)[0]
            cur = self._conn.execute(
                "DELETE FROM mfa_challenges WHERE call = ? OR call = ? OR call LIKE ?",
                (c, base, base + "-%"),
            )
            counts["mfa_challenges"] = int(cur.rowcount or 0)
            self._conn.commit()
        return counts

    async def match_user_registry_calls(self, pattern: str, *, limit: int = 5000) -> list[str]:
        pat = str(pattern or "").strip().upper()
        if not pat:
            return []
        lim = max(1, min(int(limit), 10000))
        async with self._lock:
            cur = self._conn.execute(
                """
                SELECT call
                FROM user_registry
                ORDER BY call
                LIMIT ?
                """,
                (lim,),
            )
            rows = cur.fetchall()
        return [str(row["call"]).upper() for row in rows if fnmatch.fnmatchcase(str(row["call"]).upper(), pat)]

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

    async def recent_logins(self, limit: int = 50) -> list[sqlite3.Row]:
        lim = max(1, min(int(limit), 500))
        async with self._lock:
            cur = self._conn.execute(
                """
                SELECT call, display_name, privilege, last_login_epoch, last_login_peer
                FROM user_registry
                WHERE last_login_epoch > 0
                ORDER BY last_login_epoch DESC, call
                LIMIT ?
                """,
                (lim,),
            )
            return cur.fetchall()

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
