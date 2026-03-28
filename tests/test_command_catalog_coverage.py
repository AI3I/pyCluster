from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
import re

from pycluster.config import AppConfig, NodeConfig, PublicWebConfig, StoreConfig, TelnetConfig, WebConfig
from pycluster.models import Spot
from pycluster.store import SpotStore
from pycluster.telnet_server import Session, TelnetClusterServer


ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "docs" / "dxspider-command-catalog.md"


class _DummyWriter:
    def write(self, _b: bytes) -> None:
        return

    async def drain(self) -> None:
        return


def _mk_config(tmp_db: str) -> AppConfig:
    return AppConfig(
        node=NodeConfig(),
        telnet=TelnetConfig(),
        web=WebConfig(),
        public_web=PublicWebConfig(),
        store=StoreConfig(sqlite_path=tmp_db),
    )


def _parse_catalog_commands(path: Path) -> list[str]:
    cmds: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        m = re.match(r"^\s*-\s+`([^`]+)`\s*$", line)
        if m:
            cmds.append(m.group(1).strip())
    return cmds


PROBE_OVERRIDES: dict[str, str] = {
    "announce": "announce local coverage probe",
    "apropos": "apropos proto",
    "chat": "chat coverage probe",
    "connect": "connect peer1 tcp://127.0.0.1:9",
    "dbdelkey": "dbdelkey N0CALL parity_key",
    "dbexport": "dbexport /tmp/pycluster-dbexport.sql",
    "dbimport": "dbimport /tmp/pycluster-dbimport.txt",
    "dbremove": "dbremove user N0CALL",
    "demonstrate": "demonstrate show/time",
    "disconnect": "disconnect peer1",
    "do": "do show/time",
    "dx": "dx 14074.0 K1ABC FT8 coverage",
    "dxqsl_export": "dxqsl_export /tmp/pycluster-dxqsl.dat",
    "dxqsl_import": "dxqsl_import /tmp/pycluster-dxqsl.dat",
    "export": "export /tmp/pycluster-export.sql",
    "export_users": "export_users /tmp/pycluster-users.csv",
    "join": "join dx",
    "kill": "kill all",
    "leave": "leave dx",
    "merge": "merge show/time",
    "msg": "msg all coverage message",
    "read": "read 1",
    "reply": "reply 1 coverage reply",
    "run": "run show/time",
    "send": "send all coverage send",
    "send_config": "send_config /tmp/pycluster-config.txt",
    "spoof": "spoof K1ABC coverage",
    "talk": "talk all coverage talk",
    "type": "type show/time",
    "wcy": "wcy K=3 A=8 coverage",
    "wwv": "wwv SFI=120 A=5 K=2 coverage",
    "wx": "wx local coverage weather",
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
    "set/user": "set/user N0CALL name Coverage User",
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
        return f"{cmd} on"
    if cmd.startswith("unset/"):
        return cmd
    return cmd


def test_dxspider_catalog_commands_execute_without_fallbacks(tmp_path) -> None:
    async def _stats():
        return {
            "peer1": {"inbound": False, "parsed_frames": 2, "sent_frames": 3, "dropped_frames": 0, "policy_dropped": 0},
            "in:(127.0.0.1,9000)": {"inbound": True, "parsed_frames": 1, "sent_frames": 0, "dropped_frames": 0, "policy_dropped": 0},
        }

    async def run() -> None:
        db = str(tmp_path / "catalog.db")
        cfg = _mk_config(db)
        store = SpotStore(db)
        srv = TelnetClusterServer(cfg, store, datetime.now(timezone.utc), link_stats_fn=_stats)
        now = int(datetime.now(timezone.utc).timestamp())

        Path("/tmp/pycluster-dbimport.txt").write_text("coverage import\n", encoding="utf-8")
        Path("/tmp/pycluster-dxqsl.dat").write_text("coverage dxqsl\n", encoding="utf-8")

        await store.add_spot(Spot(14074.0, "K1ABC", now, "FT8", "N0CALL", "N2WQ-1", ""))
        await store.add_spot(Spot(7020.0, "W1AW", now, "CW", "N0CALL", "N2WQ-1", ""))
        await store.add_message("K1ABC", "N0CALL", now, "hello")
        await store.add_bulletin("announce", "N0CALL", "LOCAL", now, "contest soon")
        await store.add_bulletin("wcy", "N0CALL", "LOCAL", now, "K=2 A=6")
        await store.upsert_user_registry("N0CALL", now, privilege="sysop")
        await store.set_user_pref("N0CALL", "privilege", "sysop", now)

        failures: list[str] = []
        try:
            for cmd in _parse_catalog_commands(CATALOG):
                srv._sessions.clear()
                sess = Session(call="N0CALL", writer=_DummyWriter(), connected_at=datetime.now(timezone.utc))
                sess.vars["privilege"] = "sysop"
                srv._sessions[1] = sess
                await srv._apply_prefs_to_session(sess)
                keep, out = await srv._execute_command("N0CALL", _probe_command(cmd))
                low = out.strip().lower()
                if low == "?" or "not implemented yet" in low or low.startswith("exception:"):
                    failures.append(f"{cmd}: {out.strip()}")
                if not keep and cmd not in {"bye", "shutdown"}:
                    failures.append(f"{cmd}: unexpected disconnect")
        finally:
            await store.close()

        assert not failures, "catalog command failures:\n" + "\n".join(failures)

    asyncio.run(run())
