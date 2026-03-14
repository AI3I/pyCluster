from __future__ import annotations

import asyncio
from pathlib import Path

from pycluster.dxspider_migrate import (
    discover_dxspider_local_data,
    export_badip_fail2ban_file,
    migrate_dxspider_local_data,
    parse_badip_file,
    parse_badword_file,
    parse_dxspider_connect_file,
)
from pycluster.store import SpotStore


def test_discover_dxspider_local_data_accepts_root_or_local_data(tmp_path: Path) -> None:
    root = tmp_path / "spider"
    local_data = root / "local_data"
    local_data.mkdir(parents=True)
    src_root, found = discover_dxspider_local_data(root)
    assert src_root == root.resolve()
    assert found == local_data.resolve()
    src_root2, found2 = discover_dxspider_local_data(local_data)
    assert src_root2 == local_data.resolve()
    assert found2 == local_data.resolve()


def test_parse_badword_file_reads_perl_hash_entries(tmp_path: Path) -> None:
    src = tmp_path / "badword"
    src.write_text(
        "bless( {\n"
        "  FUCK => 1000423046,\n"
        "  TOSSPOT => 1002054338,\n"
        "}, 'DXUser' )\n",
        encoding="utf-8",
    )
    got = parse_badword_file(src)
    assert got == [("FUCK", 1000423046), ("TOSSPOT", 1002054338)]


def test_parse_badip_file_ignores_comments_and_blanks(tmp_path: Path) -> None:
    src = tmp_path / "badip.local"
    src.write_text("# comment\n\n203.0.113.10\n203.0.113.10\n198.51.100.0/24\n", encoding="utf-8")
    assert parse_badip_file(src) == ["203.0.113.10", "198.51.100.0/24"]


def test_export_badip_fail2ban_file_keeps_exact_ips_and_skips_cidr(tmp_path: Path) -> None:
    src = tmp_path / "badip.local"
    dst = tmp_path / "fail2ban-badip.local"
    src.write_text("203.0.113.10\n198.51.100.0/24\n203.0.113.10\n", encoding="utf-8")
    imported, skipped = export_badip_fail2ban_file(src, dst)
    assert imported == 1
    assert skipped == ["198.51.100.0/24"]
    text = dst.read_text(encoding="utf-8")
    assert "203.0.113.10" in text
    assert "198.51.100.0/24" not in text


def test_parse_dxspider_connect_file_maps_simple_telnet_link(tmp_path: Path) -> None:
    src = tmp_path / "wb3ffv-2"
    src.write_text(
        "timeout 60\n"
        "abort (Busy|Sorry|Fail)\n"
        "connect telnet dxc.wb3ffv.us 7300\n"
        "'login: ' 'AI3I-15'\n"
        "client wb3ffv-2 telnet\n",
        encoding="utf-8",
    )
    record = parse_dxspider_connect_file(src)
    assert record is not None
    assert record.name == "WB3FFV-2"
    assert record.login_call == "AI3I-15"
    assert record.client_call == "WB3FFV-2"
    assert record.host == "dxc.wb3ffv.us"
    assert record.port == 7300
    assert record.dsn == "dxspider://dxc.wb3ffv.us:7300?login=AI3I-15&client=WB3FFV-2"


def test_migrate_dxspider_local_data_imports_users_motd_and_badwords(tmp_path: Path) -> None:
    async def run() -> None:
        root = tmp_path / "spider"
        local_data = root / "local_data"
        local_data.mkdir(parents=True)
        connect = root / "connect"
        connect.mkdir(parents=True)
        (local_data / "user_asc").write_text(
            "9A5ALL\tbless( {name => 'Tomy',qra => 'JN86FJ',lastoper => 1743635985,homenode => 'GB7DJK',qth => 'Cakovec',call => '9A5ALL'}, 'DXUser' )\n",
            encoding="utf-8",
        )
        (local_data / "motd").write_text("Welcome to Example DXSpider\nBe kind.\n", encoding="utf-8")
        (local_data / "badword").write_text("bless( {\n  FUCK => 1000423046,\n}, 'DXUser' )\n", encoding="utf-8")
        (local_data / "badip.local").write_text("203.0.113.10\n", encoding="utf-8")
        (connect / "wb3ffv-2").write_text(
            "timeout 60\n"
            "abort (Busy|Sorry|Fail)\n"
            "connect telnet dxc.wb3ffv.us 7300\n"
            "'login: ' 'AI3I-15'\n"
            "client wb3ffv-2 telnet\n",
            encoding="utf-8",
        )

        db = tmp_path / "migrate.db"
        badip_export = tmp_path / "config" / "fail2ban-badip.local"
        store = SpotStore(str(db))
        try:
            report = await migrate_dxspider_local_data(
                store,
                "N0CALL-1",
                root,
                now_epoch=1770000000,
                badip_fail2ban_file=badip_export,
            )
            assert report.users_imported == 1
            assert report.users_skipped == 0
            assert report.motd_imported is True
            assert report.badwords_imported == 1
            assert report.peers_imported == 1
            assert report.peers_skipped == 0
            assert report.peer_names == ["WB3FFV-2"]
            assert report.badip_entries_seen == 1
            assert report.badip_fail2ban_entries == 1
            assert report.badip_fail2ban_skipped == []
            assert report.warnings == []

            row = await store.get_user_registry("9A5ALL")
            assert row is not None
            assert row["display_name"] == "Tomy"
            assert row["qth"] == "Cakovec"
            assert row["qra"] == "JN86FJ"
            assert await store.get_user_pref("9A5ALL", "homenode") == "GB7DJK"
            assert await store.get_user_pref("N0CALL-1", "motd") == "Welcome to Example DXSpider\nBe kind."
            assert await store.list_deny_rules("badword") == ["FUCK"]
            assert await store.get_user_pref("N0CALL-1", "peer.outbound.wb3ffv-2.name") == "WB3FFV-2"
            assert await store.get_user_pref("N0CALL-1", "peer.outbound.wb3ffv-2.profile") == "dxspider"
            assert await store.get_user_pref("N0CALL-1", "peer.outbound.wb3ffv-2.reconnect") == "on"
            assert (
                await store.get_user_pref("N0CALL-1", "peer.outbound.wb3ffv-2.dsn")
                == "dxspider://dxc.wb3ffv.us:7300?login=AI3I-15&client=WB3FFV-2"
            )
            assert badip_export.read_text(encoding="utf-8").strip().endswith("203.0.113.10")
        finally:
            await store.close()

    asyncio.run(run())
