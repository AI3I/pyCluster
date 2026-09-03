import asyncio
from datetime import datetime, timezone
from pathlib import Path

from pycluster.models import Spot, parse_spot_record
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


def test_store_totp_email_fallback_is_exact_account_and_atomic(tmp_path: Path) -> None:
    async def run() -> None:
        store = SpotStore(str(tmp_path / "mfa-fallback.db"))
        now = 1785456000
        try:
            for call, secret in (
                ("AI3I", "BASESECRET"),
                ("AI3I-94", "EXACTSECRET"),
                ("AI3I-95", "SIBLINGSECRET"),
            ):
                await store.set_user_pref(call, "mfa_totp_secret", secret, now)
                await store.set_user_pref(call, "mfa_email_otp", "off", now)
                await store.save_mfa_challenge(
                    challenge_id=f"challenge-{call}",
                    call=call,
                    purpose="telnet",
                    code="123456",
                    expires_epoch=now + 300,
                    attempts_left=3,
                    issued_epoch=now,
                )

            await store.fallback_totp_to_email("AI3I-94", now + 1)

            assert await store.get_user_pref("AI3I-94", "mfa_totp_secret") is None
            assert await store.get_user_pref("AI3I-94", "mfa_email_otp") == "required"
            assert await store.get_mfa_challenge("challenge-AI3I-94") is None
            assert await store.get_user_pref("AI3I", "mfa_totp_secret") == "BASESECRET"
            assert await store.get_user_pref("AI3I", "mfa_email_otp") == "off"
            assert await store.get_mfa_challenge("challenge-AI3I") is not None
            assert await store.get_user_pref("AI3I-95", "mfa_totp_secret") == "SIBLINGSECRET"
            assert await store.get_user_pref("AI3I-95", "mfa_email_otp") == "off"
            assert await store.get_mfa_challenge("challenge-AI3I-95") is not None
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


def test_delete_user_account_removes_registry_and_all_user_data(tmp_path: Path) -> None:
    async def run() -> None:
        db = tmp_path / "delete_user_account.db"
        store = SpotStore(str(db))
        try:
            now = 1772336200
            await store.upsert_user_registry("N0CALL", now, display_name="Joe", qra="FN31PR", email="joe@example.test")
            await store.set_user_pref("N0CALL", "password", "pw", now)
            await store.set_user_pref("N0CALL", "location", "Milwaukee", now)
            await store.set_usdb_entry("N0CALL", "state", "WI", now)
            await store.add_buddy("N0CALL", "K1ABC", now)
            await store.add_startup_command("N0CALL", "show/dx", now)
            await store.set_filter_rule("N0CALL", "spots", "accept", 1, "on 20m", now)
            await store.upsert_registration_request(
                "N0CALL",
                now,
                display_name="Joe",
                home_node="W1AW",
                qth="Milwaukee",
                qra="EN53",
                email="old@example.test",
                note="pending",
                source="telnet",
                email_verified=True,
                status="pending",
            )
            await store.save_mfa_challenge(
                challenge_id="old-code",
                call="N0CALL",
                purpose="telnet-register",
                code="123456",
                expires_epoch=now + 300,
                attempts_left=3,
                issued_epoch=now,
            )

            counts = await store.delete_user_account("N0CALL")
            assert counts["registry"] == 1
            assert counts["prefs"] >= 2
            assert counts["usdb"] == 1
            assert counts["buddy"] == 1
            assert counts["startup"] == 1
            assert counts["filters"] == 1
            assert counts["registration_requests"] == 1
            assert counts["mfa_challenges"] == 1

            assert await store.get_user_registry("N0CALL") is None
            assert await store.get_registration_request("N0CALL") is None
            assert await store.get_mfa_challenge("old-code") is None
            assert await store.list_user_prefs("N0CALL") == {}
            assert await store.list_usdb_entries("N0CALL") == {}
            assert await store.list_buddies("N0CALL") == []
            assert await store.list_startup_commands("N0CALL") == []
            assert await store.list_filter_rules("N0CALL") == []
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


def test_store_batch_insert_returns_only_inserted_spots(tmp_path: Path) -> None:
    async def run() -> None:
        db = tmp_path / "batch_inserted.db"
        store = SpotStore(str(db))
        try:
            first = parse_spot_record("14074.0^K1ABC^1772337000^FT8^N0CALL^226^226^N2WQ-1^8^5^7^4^^^75.23.154.42")
            duplicate = parse_spot_record("14074.0^K1ABC^1772337000^FT8^N0CALL^226^226^N2WQ-1^8^5^7^4^^^75.23.154.42")
            second = parse_spot_record("14075.0^K1XYZ^1772337001^FT8^N0CALL^226^226^N2WQ-1^8^5^7^4^^^75.23.154.42")

            inserted = await store.add_spots_returning_inserted([first, duplicate, second])

            assert [spot.dx_call for spot in inserted] == ["K1ABC", "K1XYZ"]
            assert await store.count_spots() == 2
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


def test_store_apply_retention_prunes_old_rows(tmp_path: Path) -> None:
    async def run() -> None:
        db = tmp_path / "retention.db"
        store = SpotStore(str(db))
        try:
            old_epoch = 1770000000
            new_epoch = old_epoch + 40 * 86400
            await store.add_spot(parse_spot_record(f"14074.0^K1OLD^{old_epoch}^FT8^N0CALL^226^226^N2WQ-1^8^5^7^4^^^75.23.154.42"))
            await store.add_spot(parse_spot_record(f"14075.0^K1NEW^{new_epoch}^FT8^N0CALL^226^226^N2WQ-1^8^5^7^4^^^75.23.154.42"))
            await store.add_message("N0A", "N0B", old_epoch, "old message")
            await store.add_message("N0A", "N0B", new_epoch, "new message")
            await store.add_bulletin("announce", "N0A", "FULL", old_epoch, "old bulletin")
            await store.add_bulletin("announce", "N0A", "FULL", new_epoch, "new bulletin")

            removed = await store.apply_retention(
                new_epoch,
                spots_days=30,
                messages_days=30,
                bulletins_days=30,
            )
            assert removed == {"spots": 1, "messages": 1, "bulletins": 1}
            assert await store.count_spots() == 1
            msgs = await store.list_messages("N0B", limit=10)
            assert len(msgs) == 1 and msgs[0]["body"] == "new message"
            bulls = await store.list_bulletins("announce", limit=10)
            assert len(bulls) == 1 and bulls[0]["body"] == "new bulletin"
        finally:
            await store.close()

    asyncio.run(run())


def test_store_purges_only_persisted_rbn_history(tmp_path: Path) -> None:
    async def run() -> None:
        store = SpotStore(str(tmp_path / "purge_rbn.db"))
        now = int(datetime.now(timezone.utc).timestamp())
        try:
            await store.add_spot(Spot(14025.0, "AI3I-90", now, "CW 20 dB", "WZ7I-#", "RBN", ""))
            await store.add_spot(Spot(14026.0, "AI3I-91", now, "CW", "AI3I-92", "AI3I-15", ""))
            assert await store.purge_persisted_rbn_spots() == 1
            rows = await store.latest_spots(limit=10)
            assert [row["dx_call"] for row in rows] == ["AI3I-91"]
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


def test_py_node_catalog_enforces_confidence_sequence_and_expiry(tmp_path: Path) -> None:
    async def run() -> None:
        store = SpotStore(str(tmp_path / "py_nodes.db"))
        base = {
            "node_call": "AI3I-92",
            "node_id": "22345678-1234-5678-9234-567812345678",
            "origin_node": "AI3I-92",
            "sequence": 2,
            "software_version": "1.0.12",
            "protocol_version": "1",
            "public_web_url": "",
            "locator": "FN00FS",
            "qth": "Test",
            "sysop_contact": "",
            "services": ["telnet"],
            "capabilities": ["topology-records"],
            "source_node": "AI3I-92",
            "learned_from": "AI3I-92",
            "hop_count": 0,
            "confidence": "direct",
            "updated_epoch": 1785456000,
            "expires_at": 1785459600,
            "raw_digest": "a" * 64,
        }
        try:
            assert await store.upsert_py_node_record(base, 1785456000) == "accepted"
            assert await store.upsert_py_node_record({**base, "sequence": 1}, 1785456001) == "rejected-stale"
            assert await store.upsert_py_node_record(
                {**base, "raw_digest": "b" * 64}, 1785456002
            ) == "rejected-conflict"
            relayed = {
                **base,
                "sequence": 3,
                "raw_digest": "c" * 64,
                "confidence": "reported",
                "learned_from": "AI3I-93",
            }
            assert await store.upsert_py_node_record(relayed, 1785456003) == "rejected-confidence"
            row = await store.get_py_node_record("AI3I-92")
            assert row is not None
            assert row["confidence"] == "direct"
            assert row["services"] == ["telnet"]
            assert await store.py_node_route_counts(1785456004) == {"AI3I-92": 2}
            assert await store.withdraw_py_node_record(
                "AI3I-92", str(base["node_id"]), "AI3I-92", 1785456004
            )
            promoted = await store.get_py_node_record("AI3I-92")
            assert promoted is not None
            assert promoted["learned_from"] == "AI3I-93"
            assert promoted["confidence"] == "reported"
            assert await store.prune_expired_py_nodes(1785459600) == 1
            assert await store.list_py_node_records(1785459600) == []

            expiring_direct = {**base, "node_call": "AI3I-95", "origin_node": "AI3I-95",
                               "node_id": "52345678-1234-5678-9234-567812345678",
                               "learned_from": "AI3I-95", "expires_at": 1785456100}
            live_alternate = {**expiring_direct, "confidence": "reported", "learned_from": "AI3I-94",
                              "source_node": "AI3I-94", "hop_count": 1,
                              "sequence": 3, "raw_digest": "d" * 64,
                              "expires_at": 1785459700}
            assert await store.upsert_py_node_record(expiring_direct, 1785456000) == "accepted"
            assert await store.upsert_py_node_record(live_alternate, 1785456001) == "rejected-confidence"
            assert await store.prune_expired_py_nodes(1785456100) == 1
            promoted_after_expiry = await store.get_py_node_record("AI3I-95")
            assert promoted_after_expiry is not None
            assert promoted_after_expiry["learned_from"] == "AI3I-94"
            assert promoted_after_expiry["confidence"] == "reported"

            reported_first = {
                **base,
                "node_call": "AI3I-93",
                "origin_node": "AI3I-93",
                "node_id": "32345678-1234-5678-9234-567812345678",
                "confidence": "reported",
                "source_node": "AI3I-94",
                "learned_from": "AI3I-94",
                "hop_count": 2,
            }
            assert await store.upsert_py_node_record(reported_first, 1785456000) == "accepted"
            direct = {
                **reported_first,
                "confidence": "direct",
                "source_node": "AI3I-93",
                "learned_from": "AI3I-93",
                "hop_count": 0,
            }
            assert await store.upsert_py_node_record(direct, 1785456001) == "accepted"
            upgraded = await store.get_py_node_record("AI3I-93")
            assert upgraded is not None
            assert upgraded["confidence"] == "direct"
            assert upgraded["learned_from"] == "AI3I-93"
            assert await store.refresh_py_node_lease(
                "AI3I-93",
                str(direct["node_id"]),
                int(direct["sequence"]),
                str(direct["raw_digest"]),
                1785463200,
                1785456002,
            )
            renewed = await store.get_py_node_record("AI3I-93")
            assert renewed is not None
            assert renewed["expires_at"] == 1785463200
            assert renewed["updated_epoch"] == 1785459600
            assert int(renewed["expires_at"]) - int(renewed["updated_epoch"]) == 3600
            assert not await store.refresh_py_node_lease(
                "AI3I-93", str(direct["node_id"]), 999, str(direct["raw_digest"]), 1785466800, 1785456003
            )
        finally:
            await store.close()

    asyncio.run(run())
