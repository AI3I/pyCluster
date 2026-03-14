from __future__ import annotations

import asyncio
from pathlib import Path

from pycluster.dxspider_userfile import import_dxspider_user_file, parse_dxspider_user_line
from pycluster.store import SpotStore


def test_parse_real_dxspider_user_line_basic() -> None:
    line = (
        "9A5ALL\tbless( {name => 'Tomy',long => '16.45',qra => 'JN86FJ',sort => 'U',"
        "lastoper => 1743635985,lat => '46.4',homenode => 'GB7DJK',qth => 'Cakovec',call => '9A5ALL'}, 'DXUser' )"
    )
    rec = parse_dxspider_user_line(line)
    assert rec is not None
    assert rec.call == "9A5ALL"
    assert rec.name == "Tomy"
    assert rec.qth == "Cakovec"
    assert rec.qra == "JN86FJ"
    assert rec.homenode == "GB7DJK"
    assert rec.lastoper == 1743635985


def test_parse_real_dxspider_user_line_cleans_embedded_set_fragments() -> None:
    line = (
        "AI5KP\tbless( {call => 'AI5KP',qth => 'set/qth prairie grove, ar',lastoper => 1739379572,"
        "sort => 'U',qra => 'EM25',lat => '32.3166666666667',name => 'set/name john koch',"
        "long => '-101.383333333333'}, 'DXUser' )"
    )
    rec = parse_dxspider_user_line(line)
    assert rec is not None
    assert rec.call == "AI5KP"
    assert rec.qth == "prairie grove, ar"
    assert rec.name == "john koch"
    assert rec.qra == "EM25"


def test_parse_real_dxspider_user_line_cleans_concatenated_qth_fragment() -> None:
    line = (
        "DK2OM\tbless( {call => 'DK2OM',qth => 'Siegbachset/qth Siegbach',homenode => 'F5LEN',"
        "lat => '50.7333333333333',lastoper => 1674587977,sort => 'U',qra => 'JO40ER',"
        "long => '8.38333333333333',name => 'Wolf'}, 'DXUser' )"
    )
    rec = parse_dxspider_user_line(line)
    assert rec is not None
    assert rec.call == "DK2OM"
    assert rec.qth == "Siegbach"
    assert rec.homenode == "F5LEN"


def test_import_dxspider_user_file_populates_registry_and_homenode(tmp_path: Path) -> None:
    async def run() -> None:
        db = tmp_path / "dxspider_users.db"
        src = tmp_path / "user_asc_sample.txt"
        src.write_text(
            "\n".join(
                [
                    "9A5ALL\tbless( {name => 'Tomy',long => '16.45',qra => 'JN86FJ',sort => 'U',lastoper => 1743635985,lat => '46.4',homenode => 'GB7DJK',qth => 'Cakovec',call => '9A5ALL'}, 'DXUser' )",
                    "AI5KP\tbless( {call => 'AI5KP',qth => 'set/qth prairie grove, ar',lastoper => 1739379572,sort => 'U',qra => 'EM25',lat => '32.3166666666667',name => 'set/name john koch',long => '-101.383333333333'}, 'DXUser' )",
                    "not a dxspider user line",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        store = SpotStore(str(db))
        try:
            imported, skipped = await import_dxspider_user_file(store, src)
            assert imported == 2
            assert skipped == 1

            row = await store.get_user_registry("9A5ALL")
            assert row is not None
            assert row["display_name"] == "Tomy"
            assert row["qth"] == "Cakovec"
            assert row["qra"] == "JN86FJ"
            assert await store.get_user_pref("9A5ALL", "homenode") == "GB7DJK"

            row2 = await store.get_user_registry("AI5KP")
            assert row2 is not None
            assert row2["display_name"] == "john koch"
            assert row2["qth"] == "prairie grove, ar"
            assert row2["qra"] == "EM25"
        finally:
            await store.close()

    asyncio.run(run())
