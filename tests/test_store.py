import asyncio
from pathlib import Path

from pycluster.models import parse_spot_record
from pycluster.shdx import parse_sh_dx_args
from pycluster.store import SpotStore


def test_store_round_trip(tmp_path: Path) -> None:
    async def run() -> None:
        db = tmp_path / "test.db"
        store = SpotStore(str(db))
        try:
            spot = parse_spot_record(
                "7109.9^K3AJ^1772335320^RTTY^WW5L^226^226^N2WQ-1^8^5^7^4^^^75.23.154.42"
            )
            inserted = await store.add_spot(spot)
            assert inserted is True
            rows = await store.latest_spots(limit=1)
            assert len(rows) == 1
            assert rows[0]["dx_call"] == "K3AJ"
            assert await store.count_spots() == 1
        finally:
            await store.close()

    asyncio.run(run())


def test_store_search_filters(tmp_path: Path) -> None:
    async def run() -> None:
        db = tmp_path / "test.db"
        store = SpotStore(str(db))
        try:
            rows = [
                "7109.9^K3AJ^1772335320^RTTY^WW5L^226^226^N2WQ-1^8^5^7^4^^^75.23.154.42",
                "14025.0^K1GU^1772335400^CW^NQ4J^226^226^VE7CC-1^8^5^7^4^^^70.32.192.118",
                "1842.0^W8MET^1772323200^^N4YDU^226^226^VE7CC-1^8^4^8^4^^^45.37.193.205",
            ]
            await store.add_spots(parse_spot_record(r) for r in rows)

            q1 = parse_sh_dx_args("5 by WW5L on 40m")
            r1 = await store.search_spots(q1)
            assert len(r1) == 1
            assert r1[0]["dx_call"] == "K3AJ"

            q2 = parse_sh_dx_args("K1 exact")
            r2 = await store.search_spots(q2)
            assert len(r2) == 0

            q3 = parse_sh_dx_args("K1")
            r3 = await store.search_spots(q3)
            assert len(r3) == 1
            assert r3[0]["dx_call"] == "K1GU"
        finally:
            await store.close()

    asyncio.run(run())


def test_store_bulletins_round_trip(tmp_path: Path) -> None:
    async def run() -> None:
        db = tmp_path / "test.db"
        store = SpotStore(str(db))
        try:
            b1 = await store.add_bulletin("announce", "N0CALL", "FULL", 1772335500, "test notice")
            b2 = await store.add_bulletin("wcy", "N0CALL", "LOCAL", 1772335600, "A=8 K=3")
            assert b1 > 0 and b2 > b1

            ann = await store.list_bulletins("announce", limit=5)
            assert len(ann) == 1
            assert ann[0]["sender"] == "N0CALL"
            assert ann[0]["scope"] == "FULL"
            assert ann[0]["body"] == "test notice"

            wcy = await store.list_bulletins("wcy", limit=5)
            assert len(wcy) == 1
            assert wcy[0]["body"] == "A=8 K=3"
        finally:
            await store.close()

    asyncio.run(run())


def test_store_user_prefs_round_trip(tmp_path: Path) -> None:
    async def run() -> None:
        db = tmp_path / "test.db"
        store = SpotStore(str(db))
        try:
            await store.set_user_pref("n0call", "talk", "on", 1772335700)
            await store.set_user_pref("N0CALL", "language", "de", 1772335800)
            await store.set_user_pref("N0CALL", "talk", "off", 1772335900)

            talk = await store.get_user_pref("N0CALL", "talk")
            lang = await store.get_user_pref("N0CALL", "language")
            assert talk == "off"
            assert lang == "de"

            prefs = await store.list_user_prefs("n0call")
            assert prefs["talk"] == "off"
            assert prefs["language"] == "de"
        finally:
            await store.close()

    asyncio.run(run())


def test_store_filter_rules_round_trip(tmp_path: Path) -> None:
    async def run() -> None:
        db = tmp_path / "test.db"
        store = SpotStore(str(db))
        try:
            await store.set_filter_rule("N0CALL", "spots", "accept", 1, "on 40m", 1772336000)
            await store.set_filter_rule("N0CALL", "spots", "reject", 2, "by K1", 1772336100)
            await store.set_filter_rule("N0CALL", "spots", "accept", 1, "on 20m", 1772336200)

            rows = await store.list_filter_rules("n0call")
            assert len(rows) == 2
            assert any(r["action"] == "accept" and r["slot"] == 1 and r["expr"] == "on 20m" for r in rows)
            assert any(r["action"] == "reject" and r["slot"] == 2 for r in rows)

            await store.clear_filter_rules("N0CALL", "spots", 1)
            rows = await store.list_filter_rules("N0CALL")
            assert len(rows) == 1
            assert rows[0]["action"] == "reject"

            await store.clear_filter_rules("N0CALL", "spots", "all")
            rows = await store.list_filter_rules("N0CALL")
            assert len(rows) == 0
        finally:
            await store.close()

    asyncio.run(run())


def test_store_deny_rules_and_spot_filtering(tmp_path: Path) -> None:
    async def run() -> None:
        db = tmp_path / "test.db"
        store = SpotStore(str(db))
        try:
            await store.add_deny_rule("baddx", "K1BAD*", 1772337000)
            await store.add_deny_rule("badspotter", "N0SPAM", 1772337001)
            await store.add_deny_rule("badnode", "BADNODE*", 1772337002)
            await store.add_deny_rule("badword", "pirate", 1772337003)

            rows = [
                "7109.9^K1GOOD^1772337100^RTTY^WW5L^226^226^N2WQ-1^8^5^7^4^^^75.23.154.42",
                "14074.0^K1BAD1^1772337200^FT8^WW5L^226^226^N2WQ-1^8^5^7^4^^^75.23.154.42",
                "14075.0^K1GOOD^1772337300^FT8^N0SPAM^226^226^N2WQ-1^8^5^7^4^^^75.23.154.42",
                "14076.0^K1GOOD^1772337400^FT8^WW5L^226^226^BADNODE-1^8^5^7^4^^^75.23.154.42",
                "14077.0^K1GOOD^1772337500^pirate station^WW5L^226^226^N2WQ-1^8^5^7^4^^^75.23.154.42",
            ]
            await store.add_spots(parse_spot_record(r) for r in rows)
            assert await store.count_spots() == 1

            keep = await store.latest_spots(limit=5)
            assert keep[0]["dx_call"] == "K1GOOD"
            assert keep[0]["spotter"] == "WW5L"
        finally:
            await store.close()

    asyncio.run(run())


def test_store_spot_dedupe_toggle_and_clear(tmp_path: Path) -> None:
    async def run() -> None:
        db = tmp_path / "dupe.db"
        store = SpotStore(str(db))
        try:
            s = parse_spot_record("14074.0^K1ABC^1772337000^FT8^N0CALL^226^226^N2WQ-1^8^5^7^4^^^75.23.154.42")
            assert await store.add_spot(s) is True
            assert await store.add_spot(s) is False
            assert await store.count_spots() == 1

            await store.set_spot_dupe_enabled(False)
            assert await store.spot_dupe_enabled() is False
            await store.add_spot(s)
            assert await store.count_spots() == 2

            await store.set_spot_dupe_enabled(True)
            assert await store.spot_dupe_enabled() is True
            cleared = await store.clear_spot_dupes()
            assert cleared >= 1
        finally:
            await store.close()

    asyncio.run(run())


def test_store_spot_dedupe_across_different_spotters(tmp_path: Path) -> None:
    async def run() -> None:
        db = tmp_path / "dupe_cross_peer.db"
        store = SpotStore(str(db))
        try:
            s1 = parse_spot_record("14074.0^K1ABC^1772337000^FT8 CQ TEST^N0CALL^226^226^N2WQ-1^8^5^7^4^^^75.23.154.42")
            s2 = parse_spot_record("14074.0^K1ABC^1772337001^FT8 CQ TEST^W1AW^226^226^VE7CC-1^8^5^7^4^^^75.23.154.42")
            assert await store.add_spot(s1) is True
            assert await store.add_spot(s2) is False
            assert await store.count_spots() == 1
        finally:
            await store.close()

    asyncio.run(run())


def test_store_buddy_entries_round_trip(tmp_path: Path) -> None:
    async def run() -> None:
        db = tmp_path / "test.db"
        store = SpotStore(str(db))
        try:
            await store.add_buddy("N0CALL", "K1ABC", 1772338000)
            await store.add_buddy("N0CALL", "W1AW", 1772338001)
            await store.add_buddy("N0CALL", "K1ABC", 1772338002)

            rows = await store.list_buddies("N0CALL")
            assert rows == ["K1ABC", "W1AW"]

            removed = await store.remove_buddy("N0CALL", "K1ABC")
            assert removed == 1
            rows = await store.list_buddies("N0CALL")
            assert rows == ["W1AW"]

            removed = await store.remove_buddy("N0CALL", "all")
            assert removed == 1
            rows = await store.list_buddies("N0CALL")
            assert rows == []
        finally:
            await store.close()

    asyncio.run(run())


def test_store_usdb_entries_round_trip(tmp_path: Path) -> None:
    async def run() -> None:
        db = tmp_path / "test.db"
        store = SpotStore(str(db))
        try:
            await store.set_usdb_entry("N0CALL", "state", "MA", 1772339000)
            await store.set_usdb_entry("N0CALL", "county", "Middlesex", 1772339001)
            await store.set_usdb_entry("N0CALL", "state", "NH", 1772339002)

            rows = await store.list_usdb_entries("N0CALL")
            assert rows["state"] == "NH"
            assert rows["county"] == "Middlesex"

            removed = await store.delete_usdb_entries("N0CALL", "state")
            assert removed == 1
            rows = await store.list_usdb_entries("N0CALL")
            assert "state" not in rows
            assert "county" in rows

            removed = await store.delete_usdb_entries("N0CALL", "all")
            assert removed == 1
            rows = await store.list_usdb_entries("N0CALL")
            assert rows == {}
        finally:
            await store.close()

    asyncio.run(run())


def test_store_user_vars_round_trip(tmp_path: Path) -> None:
    async def run() -> None:
        db = tmp_path / "test.db"
        store = SpotStore(str(db))
        try:
            await store.set_user_var("N0CALL", "theme", "classic", 1772340000)
            await store.set_user_var("N0CALL", "page_size", "50", 1772340001)
            await store.set_user_var("N0CALL", "theme", "wide", 1772340002)

            rows = await store.list_user_vars("N0CALL")
            assert rows["theme"] == "wide"
            assert rows["page_size"] == "50"

            removed = await store.delete_user_vars("N0CALL", "theme")
            assert removed == 1
            rows = await store.list_user_vars("N0CALL")
            assert "theme" not in rows
            assert "page_size" in rows

            removed = await store.delete_user_vars("N0CALL", "all")
            assert removed == 1
            rows = await store.list_user_vars("N0CALL")
            assert rows == {}
        finally:
            await store.close()

    asyncio.run(run())


def test_store_user_registry_round_trip(tmp_path: Path) -> None:
    async def run() -> None:
        db = tmp_path / "test.db"
        store = SpotStore(str(db))
        try:
            await store.upsert_user_registry("N0CALL", 1772341000, display_name="John Doe", address="1 Main St", qth="Boston")
            await store.upsert_user_registry("N0CALL", 1772341100, email="john@example.net", privilege="sysop")

            row = await store.get_user_registry("N0CALL")
            assert row is not None
            assert row["display_name"] == "John Doe"
            assert row["address"] == "1 Main St"
            assert row["qth"] == "Boston"
            assert row["email"] == "john@example.net"
            assert row["privilege"] == "sysop"

            rows = await store.list_user_registry()
            assert len(rows) == 1
            assert rows[0]["call"] == "N0CALL"

            removed = await store.delete_user_registry("N0CALL")
            assert removed == 1
            row2 = await store.get_user_registry("N0CALL")
            assert row2 is None
        finally:
            await store.close()

    asyncio.run(run())


def test_store_record_login_updates_registry(tmp_path: Path) -> None:
    async def run() -> None:
        db = tmp_path / "test.db"
        store = SpotStore(str(db))
        try:
            await store.record_login("N0CALL", 1772342000, "('127.0.0.1', 5555)")
            row = await store.get_user_registry("N0CALL")
            assert row is not None
            assert int(row["last_login_epoch"]) == 1772342000
            assert "127.0.0.1" in str(row["last_login_peer"])

            await store.record_login("N0CALL", 1772342100, "('127.0.0.1', 5556)")
            row2 = await store.get_user_registry("N0CALL")
            assert row2 is not None
            assert int(row2["last_login_epoch"]) == 1772342100
            assert "5556" in str(row2["last_login_peer"])
        finally:
            await store.close()

    asyncio.run(run())


def test_store_startup_commands_round_trip(tmp_path: Path) -> None:
    async def run() -> None:
        db = tmp_path / "test.db"
        store = SpotStore(str(db))
        try:
            i1 = await store.add_startup_command("N0CALL", "show/time", 1772344000)
            i2 = await store.add_startup_command("N0CALL", "show/date", 1772344001)
            assert i1 > 0 and i2 > i1

            rows = await store.list_startup_commands("N0CALL")
            assert len(rows) == 2
            assert rows[0]["command"] == "show/time"

            removed = await store.remove_startup_command("N0CALL", int(rows[0]["id"]))
            assert removed == 1
            rows = await store.list_startup_commands("N0CALL")
            assert len(rows) == 1

            cleared = await store.clear_startup_commands("N0CALL")
            assert cleared == 1
            rows = await store.list_startup_commands("N0CALL")
            assert rows == []
        finally:
            await store.close()

    asyncio.run(run())
