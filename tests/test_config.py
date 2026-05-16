from pathlib import Path

from pycluster.config import config_override_paths, load_config


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


def test_tracked_default_config_uses_neutral_runtime_data_paths() -> None:
    cfg = load_config(Path("/home/jdlewis/GitHub/pyCluster/config/pycluster.toml"))

    assert cfg.public_web.cty_dat_path == "./data/cty.dat"
    assert cfg.public_web.wpxloc_raw_path == "./data/wpxloc.raw"
    assert cfg.satellite.keps_path == "./data/keps.txt"
    assert cfg.rbn.enabled is False
    assert cfg.rbn.port == 7000
    assert cfg.rbn.startup_commands == ()


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
