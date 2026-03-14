from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import tomllib


@dataclass(slots=True)
class TelnetConfig:
    host: str = "0.0.0.0"
    port: int = 7300
    ports: tuple[int, ...] = ()
    max_clients: int = 100
    idle_timeout_seconds: int = 0
    max_line_length: int = 512


@dataclass(slots=True)
class WebConfig:
    host: str = "127.0.0.1"
    port: int = 8080
    admin_token: str = ""


@dataclass(slots=True)
class PublicWebConfig:
    enabled: bool = False
    host: str = "127.0.0.1"
    port: int = 8081
    static_dir: str = ""
    cty_dat_path: str = ""


@dataclass(slots=True)
class NodeConfig:
    node_call: str = "N0NODE-1"
    node_alias: str = "N0NODE"
    owner_name: str = "Cluster Sysop"
    qth: str = "Unknown"
    node_locator: str = ""
    motd: str = "Welcome to pyCluster"
    branding_name: str = "pyCluster"
    welcome_title: str = "Welcome"
    welcome_body: str = ""
    login_tip: str = "Tip: help shows commands, sh/dx shows recent spots."
    show_status_after_login: bool = True
    require_password: bool = True
    support_contact: str = ""
    website_url: str = ""


@dataclass(slots=True)
class StoreConfig:
    sqlite_path: str = "./data/pycluster.db"


@dataclass(slots=True)
class AppConfig:
    node: NodeConfig
    telnet: TelnetConfig
    web: WebConfig
    public_web: PublicWebConfig
    store: StoreConfig


def node_presentation_defaults(node: NodeConfig) -> dict[str, str]:
    return {
        "node_call": node.node_call,
        "node_alias": node.node_alias,
        "owner_name": node.owner_name,
        "qth": node.qth,
        "node_locator": node.node_locator,
        "branding_name": node.branding_name,
        "welcome_title": node.welcome_title,
        "welcome_body": node.welcome_body,
        "login_tip": node.login_tip,
        "show_status_after_login": "on" if node.show_status_after_login else "off",
        "require_password": "on" if node.require_password else "off",
        "support_contact": node.support_contact,
        "website_url": node.website_url,
        "motd": node.motd,
    }


def parse_telnet_ports(raw: object, fallback: int = 7300) -> tuple[int, ...]:
    vals: list[int] = []
    if isinstance(raw, str):
        parts = [p.strip() for p in raw.split(",")]
        raw_items: list[object] = [p for p in parts if p]
    elif isinstance(raw, (list, tuple)):
        raw_items = list(raw)
    elif raw is None:
        raw_items = []
    else:
        raw_items = [raw]
    for item in raw_items:
        try:
            port = int(str(item).strip())
        except (TypeError, ValueError):
            continue
        if 0 <= port <= 65535 and port not in vals:
            vals.append(port)
    if not vals:
        vals.append(int(fallback))
    return tuple(vals)


def _load_section(raw: dict, key: str) -> dict:
    v = raw.get(key, {})
    if not isinstance(v, dict):
        raise ValueError(f"[{key}] must be a table")
    return v


def load_config(path: str | Path) -> AppConfig:
    p = Path(path)
    data = tomllib.loads(p.read_text(encoding="utf-8"))

    node = NodeConfig(**_load_section(data, "node"))
    telnet_raw = _load_section(data, "telnet")
    if "ports" in telnet_raw:
        telnet_raw["ports"] = parse_telnet_ports(telnet_raw.get("ports"), fallback=int(telnet_raw.get("port", 7300)))
    telnet = TelnetConfig(**telnet_raw)
    web = WebConfig(**_load_section(data, "web"))
    public_web = PublicWebConfig(**_load_section(data, "public_web")) if "public_web" in data else PublicWebConfig()
    store = StoreConfig(**_load_section(data, "store"))

    return AppConfig(node=node, telnet=telnet, web=web, public_web=public_web, store=store)
