#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import re
from typing import Callable

from pycluster.config import AppConfig, NodeConfig, StoreConfig, TelnetConfig, WebConfig
from pycluster.store import SpotStore
from pycluster.telnet_server import Session, TelnetClusterServer


CATALOG_DEFAULT = "/home/jdlewis/GitHub/pyCluster/docs/dxspider-command-catalog.md"
OUT_DEFAULT = "/home/jdlewis/GitHub/pyCluster/docs/dxspider-parity-matrix.md"


PARTIAL_EXPR_TOKENS = ("_cmd_compat_disabled",)
GENERIC_EXPR_TOKENS = (
    "_cmd_show_named_status",
    "_cmd_load_named",
    "_cmd_stat_named",
)
PRIVILEGE_COMPLETE_COMMANDS = {
    "agwrestart",
    "dbcreate",
    "dbdelkey",
    "dbexport",
    "dbimport",
    "dbremove",
    "dbupdate",
    "export",
    "export_users",
    "init",
    "kill",
    "rinit",
    "shutdown",
    "spoof",
    "sysop",
}


class _DummyWriter:
    def write(self, _b: bytes) -> None:
        return

    async def drain(self) -> None:
        return


@dataclass
class Row:
    command: str
    status: str
    resolved: str
    note: str


PROBE_OVERRIDES: dict[str, str] = {
    "announce": "announce local parity probe",
    "apropos": "apropos proto",
    "chat": "chat parity probe",
    "dbdelkey": "dbdelkey N0CALL parity_key",
    "dbimport": "dbimport /tmp/parity_import.txt",
    "dbremove": "dbremove user N0CALL",
    "demonstrate": "demonstrate show/time",
    "do": "do show/time",
    "dxqsl_import": "dxqsl_import /tmp/dxqsl.dat",
    "export": "export /tmp/parity_export.sql",
    "export_users": "export_users /tmp/parity_users.csv",
    "join": "join dx",
    "kill": "kill all",
    "leave": "leave dx",
    "merge": "merge show/time",
    "msg": "msg all parity message",
    "read": "read 1",
    "reply": "reply 1 parity reply",
    "run": "run show/time",
    "send": "send all parity send",
    "spoof": "spoof K1ABC parity",
    "talk": "talk all parity talk",
    "type": "type show/time",
    "wcy": "wcy K=3 A=8 parity",
    "wwv": "wwv SFI=120 A=5 K=2 parity",
    "wx": "wx local parity weather",
    "show/qrz": "show/qrz K1ABC",
    "show/prefix": "show/prefix K1",
    "show/dxcc": "show/dxcc K",
    "show/registered": "show/registered N0CALL",
    "show/node": "show/node N0CALL",
    "show/usdb": "show/usdb N0CALL",
    "show/var": "show/var N0CALL",
    "show/buddy": "show/buddy N0CALL",
    "show/filter": "show/filter N0CALL",
    "show/startup": "show/startup N0CALL",
    "show/messages": "show/messages 5",
    "set/address": "set/address 123 Main St",
    "set/email": "set/email op@example.net",
    "set/homebbs": "set/homebbs K1BBS",
    "set/homenode": "set/homenode N1NODE",
    "set/node": "set/node N1NODE",
    "set/maxconnect": "set/maxconnect 3",
    "set/page": "set/page 40",
    "set/user": "set/user N0CALL name Parity User",
    "set/usdb": "set/usdb qth Boston",
    "set/var": "set/var parity on",
    "set/uservar": "set/uservar monitor all",
    "set/buddy": "set/buddy K1ABC",
    "set/baddx": "set/baddx K1BAD*",
    "set/badnode": "set/badnode BADNODE*",
    "set/badspotter": "set/badspotter BADSPOT*",
    "set/badword": "set/badword pirate",
    "set/startup": "set/startup show/time",
    "set/relay": "set/relay all on",
    "set/relaypeer": "set/relaypeer peer1 all on",
    "set/ingestpeer": "set/ingestpeer peer1 all on",
    "set/protoack": "set/protoack all",
    "set/protothreshold": "set/protothreshold flap_score 5",
    "unset/email": "unset/email",
    "unset/relay": "unset/relay all",
    "unset/relaypeer": "unset/relaypeer peer1 all",
    "unset/ingestpeer": "unset/ingestpeer peer1 all",
    "unset/protoack": "unset/protoack all",
    "unset/protothreshold": "unset/protothreshold all",
    "unset/var": "unset/var parity",
    "unset/uservar": "unset/uservar monitor",
    "unset/startup": "unset/startup",
    "clear/protohistory": "clear/protohistory all",
    "clear/spots": "clear/spots all",
    "clear/announce": "clear/announce all",
    "clear/route": "clear/route all",
    "clear/wcy": "clear/wcy all",
    "clear/wwv": "clear/wwv all",
    "load/aliases": "load/aliases N0CALL",
    "load/messages": "load/messages N0CALL",
    "load/usdb": "load/usdb N0CALL",
    "load/forward": "load/forward N0CALL",
    "load/keps": "load/keps N0CALL",
    "load/swop": "load/swop N0CALL",
}


def _probe_command(cmd: str) -> str:
    if cmd in PROBE_OVERRIDES:
        return PROBE_OVERRIDES[cmd]
    if cmd.startswith("show/"):
        if cmd.endswith("/qrz"):
            return f"{cmd} K1ABC"
        if cmd.endswith("/prefix"):
            return f"{cmd} K1"
        if cmd.endswith("/dxcc"):
            return f"{cmd} K"
    if cmd.startswith("set/") and cmd not in {"set/echo", "set/here", "set/beep"}:
        # many set/* accept bare toggle; probe with a simple value to avoid usage-only path
        return f"{cmd} on"
    if cmd.startswith("unset/"):
        return cmd
    return cmd


def _parse_catalog_commands(path: Path) -> list[str]:
    cmds: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        m = re.match(r"^\s*-\s+`([^`]+)`\s*$", line)
        if m:
            cmds.append(m.group(1).strip())
    return cmds


def _parse_registry_expr_map(telnet_path: Path) -> dict[str, str]:
    expr: dict[str, str] = {}
    in_reg = False
    for line in telnet_path.read_text(encoding="utf-8").splitlines():
        if "def _build_registry(" in line:
            in_reg = True
            continue
        if in_reg and line.strip() == "}":
            break
        if in_reg:
            m = re.search(r'"([^"]+)":\s*(.+),\s*$', line)
            if m:
                expr[m.group(1)] = m.group(2).strip()
    return expr


def _status_rank(s: str) -> int:
    if s == "missing":
        return 0
    if s == "partial":
        return 1
    return 2


async def _audit(catalog_path: Path) -> list[Row]:
    db = "/tmp/pycluster_parity_audit.db"
    cfg = AppConfig(node=NodeConfig(), telnet=TelnetConfig(), web=WebConfig(), store=StoreConfig(sqlite_path=db))
    store = SpotStore(db)
    srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc))

    reg = srv._build_registry()
    expr_map = _parse_registry_expr_map(Path("/home/jdlewis/GitHub/pyCluster/src/pycluster/telnet_server.py"))
    rows: list[Row] = []

    try:
        now = int(datetime.now(timezone.utc).timestamp())
        await store.set_user_pref("N0CALL", "privilege", "sysop", now)
        Path("/tmp/parity_import.txt").write_text("parity import\n", encoding="utf-8")
        for cmd in _parse_catalog_commands(catalog_path):
            resolved = cmd
            note = ""
            status = "complete"
            probe = _probe_command(cmd)
            srv._sessions.clear()
            srv._sessions[1] = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
            await srv._apply_prefs_to_session(srv._sessions[1])
            srv._sessions[1].vars["privilege"] = "sysop"

            try:
                _keep, out = await srv._execute_command("N0CALL", probe)
            except Exception as exc:  # pragma: no cover - defensive audit fallback
                rows.append(Row(command=cmd, status="missing", resolved="-", note=f"exception: {exc.__class__.__name__}"))
                continue

            txt = out.strip().lower()
            if txt == "?" or "not implemented yet" in txt:
                rows.append(Row(command=cmd, status="missing", resolved="-", note="unresolved/not implemented"))
                continue

            if "/" in cmd:
                group, sub = cmd.split("/", 1)
                key = srv._resolve_subcommand(group, sub, reg)
                if not key:
                    rows.append(Row(command=cmd, status="missing", resolved="-", note="no grouped resolution"))
                    continue
                resolved = key
                expr = expr_map.get(key, "")
                if "_cmd_not_implemented" in expr:
                    status = "missing"
                    note = "registry not implemented"
                elif any(tok in expr for tok in PARTIAL_EXPR_TOKENS) or "no local data" in txt:
                    status = "partial"
                    note = "generic compatibility handler"
                elif any(tok in expr for tok in GENERIC_EXPR_TOKENS):
                    status = "partial"
                    note = "shared generic handler"
            else:
                if "disabled" in txt:
                    status = "partial"
                    note = "intentionally disabled for safety"
                elif txt.startswith("usage:"):
                    status = "partial"
                    note = "implemented entrypoint; probe argument mismatch"
                elif "permission denied" in txt:
                    if cmd in PRIVILEGE_COMPLETE_COMMANDS:
                        note = "real behavior path; privilege-gated"
                    else:
                        status = "partial"
                        note = "implemented entrypoint; privilege-gated"

            rows.append(Row(command=cmd, status=status, resolved=resolved, note=note))
    finally:
        await store.close()

    return rows


def _render(rows: list[Row]) -> str:
    now = datetime.now(timezone.utc).isoformat()
    total = len(rows)
    complete = [r for r in rows if r.status == "complete"]
    partial = [r for r in rows if r.status == "partial"]
    missing = [r for r in rows if r.status == "missing"]

    lines: list[str] = []
    lines.append("# DXSpider Command Parity Matrix (1.55/1.57)")
    lines.append("")
    lines.append(f"Generated UTC: {now}")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- Total catalog commands: {total}")
    lines.append(f"- Complete: {len(complete)}")
    lines.append(f"- Partial: {len(partial)}")
    lines.append(f"- Missing: {len(missing)}")
    lines.append("")
    lines.append("## Criteria")
    lines.append("")
    lines.append("- `complete`: resolves to an implemented command path and returns concrete behavior.")
    lines.append("- `partial`: resolves, but currently generic/limited/privilege-gated/arg-dependent behavior.")
    lines.append("- `missing`: unresolved or explicit not-implemented path.")
    lines.append("")

    def section(name: str, data: list[Row]) -> None:
        lines.append(f"## {name} ({len(data)})")
        lines.append("")
        lines.append("| Command | Status | Resolved | Note |")
        lines.append("|---|---|---|---|")
        for r in sorted(data, key=lambda x: x.command):
            note = r.note or ""
            lines.append(f"| `{r.command}` | `{r.status}` | `{r.resolved}` | {note} |")
        lines.append("")

    section("Missing", missing)
    section("Partial", partial)
    section("Complete", complete)

    lines.append("## Prioritized Next Work")
    lines.append("")
    top = sorted([r for r in rows if r.status != "complete"], key=lambda r: (_status_rank(r.status), r.command))[:40]
    for r in top:
        lines.append(f"- `{r.command}` ({r.status})")
    lines.append("")
    return "\n".join(lines)


async def _main_async(catalog: Path, out: Path) -> None:
    rows = await _audit(catalog)
    out.write_text(_render(rows), encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate DXSpider 1.55/1.57 command parity matrix")
    ap.add_argument("--catalog", default=CATALOG_DEFAULT)
    ap.add_argument("--out", default=OUT_DEFAULT)
    args = ap.parse_args()
    asyncio.run(_main_async(Path(args.catalog), Path(args.out)))


if __name__ == "__main__":
    main()
