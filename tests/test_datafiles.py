from datetime import datetime, timedelta, timezone

from pycluster.datafiles import describe_cty_file, describe_keps_file


def test_dataset_release_is_not_local_file_update_date(tmp_path):
    path = tmp_path / "cty.dat"
    path.write_text("VER20260906\n", encoding="ascii")
    status = describe_cty_file(str(path))
    assert status.version_date == "2026-09-06"
    assert status.modified_iso


def test_keps_reports_element_age_not_download_age(tmp_path):
    path = tmp_path / "keps.txt"
    old = datetime.now(timezone.utc) - timedelta(days=30)
    epoch = f"{old.year % 100:02d}{old.timetuple().tm_yday:03d}.00000000"
    path.write_text(
        "TEST\n"
        f"1 25544U 98067A   {epoch}  .00016717  00000+0  10270-3 0  9993\n"
        "2 25544  51.6416  40.5919 0006703  50.1588  69.3786 15.50000000432109\n",
        encoding="ascii",
    )
    status = describe_keps_file(str(path))
    assert status["element_count"] == 1
    assert status["stale"] is True
    assert status["oldest_epoch"].startswith(old.date().isoformat())
    assert status["modified_iso"].startswith(datetime.now(timezone.utc).date().isoformat())
    path.write_text("not orbital data", encoding="ascii")
    assert describe_keps_file(str(path))["status"] == "invalid"
    assert describe_keps_file(str(tmp_path / "absent"))["status"] == "missing"
