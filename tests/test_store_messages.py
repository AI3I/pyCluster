import asyncio

from pycluster.store import SpotStore


def test_message_store_flow(tmp_path) -> None:
    async def run() -> None:
        db = str(tmp_path / "messages.db")
        store = SpotStore(db)
        try:
            m1 = await store.add_message(sender="N0A", recipient="N0B", epoch=100, body="hello")
            assert m1 > 0
            m2 = await store.add_message(sender="N0C", recipient="ALL", epoch=101, body="broadcast")
            assert m2 > m1

            rows = await store.list_messages("N0B", limit=10)
            ids = [int(r["id"]) for r in rows]
            assert m1 in ids and m2 in ids

            row = await store.get_message_for_recipient("N0B", m1)
            assert row is not None
            assert row["read_epoch"] is None

            await store.mark_message_read(m1, 200)
            row2 = await store.get_message_for_recipient("N0B", m1)
            assert row2 is not None
            assert int(row2["read_epoch"]) == 200

            total, unread = await store.message_counts("N0B")
            assert total >= 2
            assert unread >= 1
        finally:
            await store.close()

    asyncio.run(run())
