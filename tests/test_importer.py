import asyncio
from pathlib import Path

from pycluster.importer import import_spot_file
from pycluster.store import SpotStore


def test_importer_accepts_wire_and_debug_pc61(tmp_path: Path) -> None:
    async def run() -> None:
        db = tmp_path / "import_pc61.db"
        src = tmp_path / "spots.txt"
        src.write_text(
            "\n".join(
                [
                    "PC61^14074.0^K1ABC^1-Mar-2026^0000Z^FT8^N0CALL^N2WQ-1^127.0.0.1^H1^~",
                    "1772323200^<- I WB3FFV-2 PC61^1928.0^Z66BCC^1-Mar-2026^0000Z^^DL6NBC^DA0BCC-7^84.163.40.20^H28^~",
                    "not-a-spot",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        store = SpotStore(str(db))
        try:
            imported, skipped = await import_spot_file(store, src)
            assert imported == 2
            assert skipped == 1
            rows = await store.latest_spots(limit=5)
            calls = {str(r["dx_call"]) for r in rows}
            assert "K1ABC" in calls
            assert "Z66BCC" in calls
        finally:
            await store.close()

    asyncio.run(run())
