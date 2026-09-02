#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ipaddress
import json
import sys
import tomllib
from pathlib import Path
from urllib.parse import urlsplit


def _bootstrap_import_path() -> None:
    root = Path(__file__).resolve().parent.parent
    src = root / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))


_bootstrap_import_path()

from pycluster.config import config_override_paths, load_config, save_config  # noqa: E402


PY_DEFAULTS_VERSION = 1
PY_SHARING_FIELDS = (
    "share_node_info",
    "share_public_web_url",
    "share_locator",
    "share_qth",
    "share_sysop_contact",
    "share_topology",
    "share_health",
    "share_datasets",
    "share_rbn_status",
    "share_policy",
    "share_clock",
    "share_notices",
)
PROJECT_HOSTS = {"github.com", "www.github.com", "groups.io", "www.groups.io", "pyclusterapp.slack.com"}


def _configured_defaults_version(config_path: Path) -> int:
    version = 0
    for path in (config_path, *config_override_paths(config_path)):
        if not path.exists():
            continue
        with path.open("rb") as handle:
            raw = tomllib.load(handle)
        section = raw.get("py_protocol")
        if not isinstance(section, dict) or "defaults_version" not in section:
            continue
        try:
            version = max(version, int(section["defaults_version"]))
        except (TypeError, ValueError):
            continue
    return version


def _inheritable_public_url(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    parsed = urlsplit(text)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
        return ""
    hostname = parsed.hostname.lower().rstrip(".")
    if hostname in PROJECT_HOSTS or hostname == "localhost" or hostname.endswith(".local"):
        return ""
    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        address = None
    if address is not None and not address.is_global:
        return ""
    return text


def apply_defaults(config_path: Path) -> dict[str, object]:
    previous_version = _configured_defaults_version(config_path)
    if previous_version >= PY_DEFAULTS_VERSION:
        return {"changed": False, "defaults_version": previous_version, "public_web_url_inherited": False}

    config = load_config(config_path)
    config.py_protocol.defaults_version = PY_DEFAULTS_VERSION
    config.py_protocol.enabled = True
    for field in PY_SHARING_FIELDS:
        setattr(config.py_protocol, field, True)

    inherited_url = False
    if config.public_web.enabled and not config.py_protocol.public_web_url.strip():
        public_url = _inheritable_public_url(config.node.website_url)
        if public_url:
            config.py_protocol.public_web_url = public_url
            inherited_url = True

    save_config(config_path, config)
    return {
        "changed": True,
        "defaults_version": PY_DEFAULTS_VERSION,
        "public_web_url_inherited": inherited_url,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply the current default PY protocol policy once per installation.")
    parser.add_argument("--config", required=True, type=Path, help="Path to the base pycluster.toml")
    args = parser.parse_args()
    print(json.dumps(apply_defaults(args.config), separators=(",", ":"), sort_keys=True))


if __name__ == "__main__":
    main()
