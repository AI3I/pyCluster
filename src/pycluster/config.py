from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
import json
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
    wpxloc_raw_path: str = ""


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
    require_password: bool = False
    registration_required: bool = False
    verified_email_required_for_web: bool = False
    verified_email_required_for_telnet: bool = False
    initial_grace_logins: int = 5
    support_contact: str = ""
    website_url: str = ""
    prompt_template: str = "[{timestamp}] {node}{suffix}"


@dataclass(slots=True)
class StoreConfig:
    sqlite_path: str = "./data/pycluster.db"


@dataclass(slots=True)
class QRZConfig:
    username: str = ""
    password: str = ""
    agent: str = ""
    api_url: str = "https://xmldata.qrz.com/xml/current/"


@dataclass(slots=True)
class SMTPConfig:
    host: str = ""
    port: int = 587
    username: str = ""
    password: str = ""
    from_addr: str = ""
    from_name: str = "pyCluster"
    starttls: bool = True
    use_ssl: bool = False
    timeout_seconds: int = 10


@dataclass(slots=True)
class SatelliteConfig:
    keps_path: str = "./data/keps.txt"
    prediction_hours: int = 24
    pass_step_seconds: int = 60
    min_elevation_deg: float = 0.0


@dataclass(slots=True)
class MFAConfig:
    enabled: bool = False
    require_for_sysop: bool = False
    require_for_users: bool = False
    issuer: str = "pyCluster"
    otp_ttl_seconds: int = 600
    otp_length: int = 6
    max_attempts: int = 5
    resend_cooldown_seconds: int = 30


@dataclass(slots=True)
class AppConfig:
    node: NodeConfig
    telnet: TelnetConfig
    web: WebConfig
    public_web: PublicWebConfig
    store: StoreConfig
    qrz: QRZConfig = field(default_factory=QRZConfig)
    smtp: SMTPConfig = field(default_factory=SMTPConfig)
    satellite: SatelliteConfig = field(default_factory=SatelliteConfig)
    mfa: MFAConfig = field(default_factory=MFAConfig)


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
        "registration_required": "on" if node.registration_required else "off",
        "verified_email_required_for_web": "on" if node.verified_email_required_for_web else "off",
        "verified_email_required_for_telnet": "on" if node.verified_email_required_for_telnet else "off",
        "initial_grace_logins": str(int(node.initial_grace_logins)),
        "support_contact": node.support_contact,
        "website_url": node.website_url,
        "prompt_template": node.prompt_template,
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
        if 0 <= port <= 65535 and (port == 0 or port not in vals):
            vals.append(port)
    if not vals:
        vals.append(int(fallback))
    return tuple(vals)


def _load_section(raw: dict, key: str) -> dict:
    v = raw.get(key, {})
    if not isinstance(v, dict):
        raise ValueError(f"[{key}] must be a table")
    return v


def _load_toml(path: Path) -> dict:
    return tomllib.loads(path.read_text(encoding="utf-8"))


def _merge_config_dict(base: dict, override: dict) -> dict:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_config_dict(dict(merged[key]), value)
        else:
            merged[key] = value
    return merged


def config_override_paths(path: str | Path) -> tuple[Path, ...]:
    p = Path(path)
    if p.suffix == ".toml":
        local_name = f"{p.stem}.local{p.suffix}"
    else:
        local_name = p.name + ".local"
    sibling = p.with_name(local_name)
    paths: list[Path] = []
    if sibling != p:
        paths.append(sibling)
    return tuple(paths)


def _default_wpxloc_raw_path(public_web: PublicWebConfig) -> str:
    current = str(public_web.wpxloc_raw_path or "").strip()
    if current:
        return current
    cty_path = str(public_web.cty_dat_path or "").strip()
    if not cty_path:
        return ""
    cty = Path(cty_path)
    return str(cty.with_name("wpxloc.raw"))


def load_config(path: str | Path) -> AppConfig:
    p = Path(path)
    data = _load_toml(p)
    for override_path in config_override_paths(p):
        if override_path.exists():
            data = _merge_config_dict(data, _load_toml(override_path))

    node = NodeConfig(**_load_section(data, "node"))
    telnet_raw = _load_section(data, "telnet")
    if "ports" in telnet_raw:
        telnet_raw["ports"] = parse_telnet_ports(telnet_raw.get("ports"), fallback=int(telnet_raw.get("port", 7300)))
    telnet = TelnetConfig(**telnet_raw)
    web = WebConfig(**_load_section(data, "web"))
    public_web = PublicWebConfig(**_load_section(data, "public_web")) if "public_web" in data else PublicWebConfig()
    public_web.wpxloc_raw_path = _default_wpxloc_raw_path(public_web)
    store = StoreConfig(**_load_section(data, "store"))

    qrz = QRZConfig(**_load_section(data, "qrz")) if "qrz" in data else QRZConfig()
    smtp = SMTPConfig(**_load_section(data, "smtp")) if "smtp" in data else SMTPConfig()
    satellite = SatelliteConfig(**_load_section(data, "satellite")) if "satellite" in data else SatelliteConfig()
    mfa = MFAConfig(**_load_section(data, "mfa")) if "mfa" in data else MFAConfig()

    return AppConfig(node=node, telnet=telnet, web=web, public_web=public_web, store=store, qrz=qrz, smtp=smtp, satellite=satellite, mfa=mfa)


def _toml_value(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(_toml_value(v) for v in value) + "]"
    raise TypeError(f"unsupported TOML value type: {type(value)!r}")


def dump_config(config: AppConfig) -> str:
    data = {
        "node": asdict(config.node),
        "telnet": asdict(config.telnet),
        "web": asdict(config.web),
        "public_web": asdict(config.public_web),
        "store": asdict(config.store),
        "qrz": asdict(config.qrz),
        "smtp": asdict(config.smtp),
        "satellite": asdict(config.satellite),
        "mfa": asdict(config.mfa),
    }
    lines: list[str] = []
    for section in ("node", "telnet", "web", "public_web", "store", "qrz", "smtp", "satellite", "mfa"):
        lines.append(f"[{section}]")
        for key, value in data[section].items():
            lines.append(f"{key} = {_toml_value(value)}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def save_config(path: str | Path, config: AppConfig) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(dump_config(config), encoding="utf-8")
