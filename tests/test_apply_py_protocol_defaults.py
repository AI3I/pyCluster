from __future__ import annotations

import importlib.util
from pathlib import Path

from pycluster.config import load_config, save_config


_SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "apply_py_protocol_defaults.py"
_SPEC = importlib.util.spec_from_file_location("apply_py_protocol_defaults_script", _SCRIPT_PATH)
assert _SPEC and _SPEC.loader
apply_py_protocol_defaults = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(apply_py_protocol_defaults)


def _write_legacy_config(path: Path, website_url: str = "https://cluster.example.net") -> None:
    path.write_text(
        "\n".join(
            [
                "[node]",
                'node_call = "AI3I-90"',
                f'website_url = "{website_url}"',
                "",
                "[telnet]",
                "",
                "[web]",
                "",
                "[public_web]",
                "enabled = true",
                "",
                "[store]",
                'sqlite_path = "./data/test.db"',
                "",
                "[py_protocol]",
                "enabled = false",
                "share_node_info = false",
                "share_topology = false",
                "share_health = false",
                "share_datasets = false",
                "share_rbn_status = false",
                "share_policy = false",
                "share_clock = false",
                "share_notices = false",
                "",
            ]
        ),
        encoding="utf-8",
    )


def test_apply_defaults_enables_legacy_install_once_and_inherits_public_url(tmp_path: Path) -> None:
    config_path = tmp_path / "pycluster.toml"
    _write_legacy_config(config_path)

    result = apply_py_protocol_defaults.apply_defaults(config_path)
    config = load_config(config_path)

    assert result == {"changed": True, "defaults_version": 1, "public_web_url_inherited": True}
    assert config.py_protocol.defaults_version == 1
    assert config.py_protocol.enabled is True
    assert config.py_protocol.public_web_url == "https://cluster.example.net"
    for field in apply_py_protocol_defaults.PY_SHARING_FIELDS:
        assert getattr(config.py_protocol, field) is True

    config.py_protocol.enabled = False
    config.py_protocol.share_topology = False
    save_config(config_path, config)
    second = apply_py_protocol_defaults.apply_defaults(config_path)
    preserved = load_config(config_path)

    assert second == {"changed": False, "defaults_version": 1, "public_web_url_inherited": False}
    assert preserved.py_protocol.enabled is False
    assert preserved.py_protocol.share_topology is False


def test_apply_defaults_does_not_advertise_project_or_private_urls(tmp_path: Path) -> None:
    for index, website_url in enumerate(("https://github.com/AI3I/pyCluster", "http://127.0.0.1:8081")):
        config_path = tmp_path / f"pycluster-{index}.toml"
        _write_legacy_config(config_path, website_url)

        result = apply_py_protocol_defaults.apply_defaults(config_path)
        config = load_config(config_path)

        assert result["public_web_url_inherited"] is False
        assert config.py_protocol.public_web_url == ""
