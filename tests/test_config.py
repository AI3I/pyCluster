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
