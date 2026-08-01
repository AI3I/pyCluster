from pathlib import Path

import pytest

from pycluster.config import config_override_paths, config_save_path, load_config, save_config


def _write_base_config(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "[node]",
                'node_call = "AI3I-15"',
                'qth = "Western Pennsylvania"',
                "",
                "[telnet]",
                'host = "0.0.0.0"',
                "port = 7300",
                "",
                "[web]",
                'host = "127.0.0.1"',
                "port = 8080",
                "",
                "[public_web]",
                "enabled = false",
                'host = "127.0.0.1"',
                "port = 8081",
                'static_dir = ""',
                'cty_dat_path = ""',
                "",
                "[store]",
                'sqlite_path = "./data/pycluster.db"',
                "",
                "[qrz]",
                'username = ""',
                'password = ""',
                'agent = ""',
                "",
                "[rbn]",
                "enabled = false",
                'host = ""',
                "",
            ]
        ),
        encoding="utf-8",
    )


def test_config_override_paths_points_to_sibling_local_file(tmp_path: Path) -> None:
    path = tmp_path / "config" / "pycluster.toml"
    assert config_override_paths(path) == (tmp_path / "config" / "pycluster.local.toml",)
    assert config_save_path(tmp_path / "config" / "pycluster.local.toml") == tmp_path / "config" / "pycluster.local.toml"


def test_load_config_merges_sibling_local_override(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    base = config_dir / "pycluster.toml"
    local = config_dir / "pycluster.local.toml"
    _write_base_config(base)
    local.write_text(
        "\n".join(
            [
                "[node]",
                'qth = "Boston, MA"',
                'prompt_template = "{node}{suffix}"',
                "",
                "[telnet]",
                "port = 7373",
                "",
                "[public_web]",
                "enabled = true",
                "",
                "[qrz]",
                'username = "AI3I"',
                "",
            ]
        ),
        encoding="utf-8",
    )

    cfg = load_config(base)

    assert cfg.node.node_call == "AI3I-15"
    assert cfg.node.qth == "Boston, MA"
    assert cfg.node.prompt_template == "{node}{suffix}"
    assert cfg.telnet.port == 7373
    assert cfg.public_web.enabled is True
    assert cfg.store.sqlite_path == "./data/pycluster.db"
    assert cfg.qrz.username == "AI3I"
    assert cfg.rbn.enabled is False
    assert cfg.py_protocol.enabled is False


def test_save_config_atomically_writes_local_override_and_preserves_base(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    base = config_dir / "pycluster.toml"
    _write_base_config(base)
    original = base.read_text(encoding="utf-8")
    cfg = load_config(base)
    cfg.node.qth = "Updated QTH"
    cfg.smtp.password = "local-secret"

    save_config(base, cfg)

    local = config_dir / "pycluster.local.toml"
    assert config_save_path(base) == local
    assert base.read_text(encoding="utf-8") == original
    assert local.exists()
    assert local.stat().st_mode & 0o777 == 0o640
    loaded = load_config(base)
    assert loaded.node.qth == "Updated QTH"
    assert loaded.smtp.password == "local-secret"


def test_save_config_failure_preserves_previous_local_file(tmp_path: Path, monkeypatch) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    base = config_dir / "pycluster.toml"
    local = config_dir / "pycluster.local.toml"
    _write_base_config(base)
    local.write_text("[node]\nqth = \"Original\"\n", encoding="utf-8")
    cfg = load_config(base)
    cfg.node.qth = "Replacement"

    def _fail_replace(_src, _dst) -> None:
        raise OSError("test")

    monkeypatch.setattr("pycluster.config.os.replace", _fail_replace)
    with pytest.raises(OSError, match="test"):
        save_config(base, cfg)

    assert local.read_text(encoding="utf-8") == "[node]\nqth = \"Original\"\n"


def test_load_config_migrates_legacy_public_ipv6_address(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    base = config_dir / "pycluster.toml"
    _write_base_config(base)
    text = base.read_text(encoding="utf-8")
    base.write_text(text.replace('[node]\n', '[node]\npublic_ip_address = "2606:4700:4700::1111"\n'), encoding="utf-8")

    cfg = load_config(base)

    assert cfg.node.public_ip_address == ""
    assert cfg.node.public_ipv6_address == "2606:4700:4700::1111"


def test_tracked_default_config_uses_neutral_runtime_data_paths() -> None:
    cfg = load_config(Path("/home/jdlewis/GitHub/pyCluster/config/pycluster.toml"))

    assert cfg.public_web.cty_dat_path == "./data/cty.dat"
    assert cfg.public_web.wpxloc_raw_path == "./data/wpxloc.raw"
    assert cfg.satellite.keps_path == "./data/keps.txt"
    assert cfg.rbn.enabled is False
    assert cfg.rbn.port == 7000
    assert cfg.rbn.startup_commands == ()
    assert cfg.py_protocol.enabled is False
    assert cfg.py_protocol.share_node_info is True
    assert cfg.py_protocol.share_topology is False
    assert cfg.py_protocol.max_frame_bytes == 2048


def test_load_config_parses_rbn_startup_commands(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    base = config_dir / "pycluster.toml"
    _write_base_config(base)
    text = base.read_text(encoding="utf-8").replace(
        "[rbn]\nenabled = false\nhost = \"\"",
        "[rbn]\nenabled = true\nhost = \"rbn.example.invalid\"\nports = [7000, 7001]\nstartup_commands = [\"set/skimmer\", \"set/skimmer cw\"]\nfeeds = [{ name = \"CW/RTTY\", host = \"telnet.reversebeacon.net\", port = 7000 }, { name = \"FT8\", host = \"telnet.reversebeacon.net\", port = 7001 }]",
    )
    base.write_text(text, encoding="utf-8")

    cfg = load_config(base)

    assert cfg.rbn.enabled is True
    assert cfg.rbn.host == "rbn.example.invalid"
    assert cfg.rbn.ports == (7000, 7001)
    assert [feed.name for feed in cfg.rbn.feeds] == ["CW/RTTY", "FT8"]
    assert [feed.port for feed in cfg.rbn.feeds] == [7000, 7001]
    assert cfg.rbn.startup_commands == ("set/skimmer", "set/skimmer cw")


def test_load_config_parses_and_bounds_py_protocol_settings(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    base = config_dir / "pycluster.toml"
    _write_base_config(base)
    with base.open("a", encoding="utf-8") as handle:
        handle.write(
            "\n[py_protocol]\n"
            "enabled = true\n"
            "public_web_url = \"https://node.example.net/\"\n"
            "share_public_web_url = false\n"
            "share_locator = true\n"
            "share_qth = true\n"
            "share_sysop_contact = true\n"
            "share_topology = true\n"
            "share_notices = true\n"
            "notice_severity = \"maintenance\"\n"
            "notice_message = \"  Planned   maintenance  \"\n"
            "notice_expires_epoch = 1788048000\n"
            "max_hops = 999\n"
            "max_records_per_frame = 0\n"
            "max_frame_bytes = 128\n"
            "max_bytes_per_minute = 100\n"
            "refresh_seconds = 10\n"
            "record_ttl_seconds = 20\n"
        )

    cfg = load_config(base)

    assert cfg.py_protocol.enabled is True
    assert cfg.py_protocol.public_web_url == "https://node.example.net/"
    assert cfg.py_protocol.share_public_web_url is False
    assert cfg.py_protocol.share_locator is True
    assert cfg.py_protocol.share_qth is True
    assert cfg.py_protocol.share_sysop_contact is True
    assert cfg.py_protocol.share_topology is True
    assert cfg.py_protocol.share_notices is True
    assert cfg.py_protocol.notice_severity == "maintenance"
    assert cfg.py_protocol.notice_message == "Planned maintenance"
    assert cfg.py_protocol.notice_expires_epoch == 1788048000
    assert cfg.py_protocol.max_hops == 32
    assert cfg.py_protocol.max_records_per_frame == 1
    assert cfg.py_protocol.max_frame_bytes == 256
    assert cfg.py_protocol.max_bytes_per_minute == 256
    assert cfg.py_protocol.refresh_seconds == 60
    assert cfg.py_protocol.record_ttl_seconds == 120


def test_load_config_defaults_wpxloc_to_cty_sibling(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    base = config_dir / "pycluster.toml"
    _write_base_config(base)
    text = base.read_text(encoding="utf-8").replace("cty_dat_path = \"\"", "cty_dat_path = \"./fixtures/live/dxspider/cty.dat\"")
    base.write_text(text, encoding="utf-8")

    cfg = load_config(base)

    assert cfg.public_web.cty_dat_path == "./fixtures/live/dxspider/cty.dat"
    assert cfg.public_web.wpxloc_raw_path == "fixtures/live/dxspider/wpxloc.raw" or cfg.public_web.wpxloc_raw_path == "./fixtures/live/dxspider/wpxloc.raw"
