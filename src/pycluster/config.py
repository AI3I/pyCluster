from __future__ import annotations

from dataclasses import asdict, dataclass, field
import ipaddress
import json
import os
from pathlib import Path
import tempfile
import tomllib


@dataclass(slots=True)
class TelnetConfig:
    host: str = "0.0.0.0"
    port: int = 7300
    ports: tuple[int, ...] = ()
    max_clients: int = 100
    idle_timeout_seconds: int = 0
    keepalive_interval_seconds: int = 300
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
    welcome_title: str = "Hello"
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
    public_ip_address: str = ""
    public_ipv6_address: str = ""
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
class RBNConfig:
    enabled: bool = False
    host: str = ""
    port: int = 7000
    ports: tuple[int, ...] = ()
    callsign: str = ""
    password: str = ""
    source_node: str = "RBN"
    startup_commands: tuple[str, ...] = ()
    reconnect_seconds: int = 60
    feeds: tuple["RBNFeedConfig", ...] = ()


@dataclass(slots=True)
class RBNFeedConfig:
    name: str = ""
    host: str = ""
    port: int = 7000


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
class PyProtocolConfig:
    enabled: bool = False
    public_web_url: str = ""
    share_node_info: bool = True
    share_public_web_url: bool = True
    share_locator: bool = False
    share_qth: bool = False
    share_sysop_contact: bool = False
    share_topology: bool = False
    share_health: bool = False
    share_datasets: bool = False
    share_rbn_status: bool = False
    share_policy: bool = False
    share_clock: bool = False
    share_notices: bool = False
    notice_severity: str = "normal"
    notice_message: str = ""
    notice_expires_epoch: int = 0
    max_hops: int = 8
    max_records_per_frame: int = 25
    max_frame_bytes: int = 2048
    max_bytes_per_minute: int = 65536
    refresh_seconds: int = 900
    record_ttl_seconds: int = 86400

    def __post_init__(self) -> None:
        self.notice_severity = str(self.notice_severity or "normal").strip().lower()
        if self.notice_severity not in {"normal", "maintenance", "upgrading", "degraded", "testing"}:
            self.notice_severity = "normal"
        self.notice_message = " ".join(str(self.notice_message or "").split())[:240]
        self.notice_expires_epoch = max(0, int(self.notice_expires_epoch or 0))
        self.max_hops = max(1, min(32, int(self.max_hops)))
        self.max_records_per_frame = max(1, min(100, int(self.max_records_per_frame)))
        self.max_frame_bytes = max(256, min(65536, int(self.max_frame_bytes)))
        self.max_bytes_per_minute = max(self.max_frame_bytes, min(10 * 1024 * 1024, int(self.max_bytes_per_minute)))
        self.refresh_seconds = max(60, min(86400, int(self.refresh_seconds)))
        self.record_ttl_seconds = max(self.refresh_seconds * 2, min(30 * 86400, int(self.record_ttl_seconds)))


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
    rbn: RBNConfig = field(default_factory=RBNConfig)
    mfa: MFAConfig = field(default_factory=MFAConfig)
    py_protocol: PyProtocolConfig = field(default_factory=PyProtocolConfig)


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
        "public_ip_address": node.public_ip_address,
        "public_ipv6_address": node.public_ipv6_address,
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

    node_raw = _load_section(data, "node")
    if "public_ipv6_address" not in node_raw:
        legacy_ip = str(node_raw.get("public_ip_address", "") or "").strip()
        if legacy_ip:
            try:
                parsed = ipaddress.ip_address(legacy_ip)
            except ValueError:
                parsed = None
            if parsed is not None and parsed.version == 6:
                node_raw["public_ip_address"] = ""
                node_raw["public_ipv6_address"] = legacy_ip
    node = NodeConfig(**node_raw)
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
    rbn_raw = _load_section(data, "rbn") if "rbn" in data else {}
    if "startup_commands" in rbn_raw:
        raw_commands = rbn_raw.get("startup_commands")
        if isinstance(raw_commands, str):
            rbn_raw["startup_commands"] = tuple(cmd.strip() for cmd in raw_commands.splitlines() if cmd.strip())
        elif isinstance(raw_commands, (list, tuple)):
            rbn_raw["startup_commands"] = tuple(str(cmd).strip() for cmd in raw_commands if str(cmd).strip())
    if "ports" in rbn_raw:
        rbn_raw["ports"] = parse_telnet_ports(rbn_raw.get("ports"), int(rbn_raw.get("port", 7000) or 7000))
    if "feeds" in rbn_raw:
        feeds: list[RBNFeedConfig] = []
        raw_feeds = rbn_raw.get("feeds")
        if isinstance(raw_feeds, list):
            for row in raw_feeds:
                if not isinstance(row, dict):
                    continue
                name = str(row.get("name", "")).strip()
                host = str(row.get("host", "")).strip()
                try:
                    port = int(row.get("port", 7000))
                except (TypeError, ValueError):
                    port = 7000
                if host and 0 < port <= 65535:
                    feeds.append(RBNFeedConfig(name=name, host=host, port=port))
        rbn_raw["feeds"] = tuple(feeds)
    rbn = RBNConfig(**rbn_raw)
    mfa = MFAConfig(**_load_section(data, "mfa")) if "mfa" in data else MFAConfig()
    py_protocol = PyProtocolConfig(**_load_section(data, "py_protocol")) if "py_protocol" in data else PyProtocolConfig()

    return AppConfig(node=node, telnet=telnet, web=web, public_web=public_web, store=store, qrz=qrz, smtp=smtp, satellite=satellite, rbn=rbn, mfa=mfa, py_protocol=py_protocol)


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
    if isinstance(value, dict):
        return "{ " + ", ".join(f"{key} = {_toml_value(row)}" for key, row in value.items()) + " }"
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
        "rbn": asdict(config.rbn),
        "mfa": asdict(config.mfa),
        "py_protocol": asdict(config.py_protocol),
    }
    lines: list[str] = []
    for section in ("node", "telnet", "web", "public_web", "store", "qrz", "smtp", "satellite", "rbn", "mfa", "py_protocol"):
        lines.append(f"[{section}]")
        for key, value in data[section].items():
            lines.append(f"{key} = {_toml_value(value)}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def config_save_path(path: str | Path) -> Path:
    """Keep generated runtime settings out of the tracked/base configuration."""
    p = Path(path)
    if p.stem.endswith(".local"):
        return p
    overrides = config_override_paths(path)
    return overrides[0] if overrides else p


def save_config(path: str | Path, config: AppConfig) -> None:
    p = config_save_path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    mode = (p.stat().st_mode & 0o777) if p.exists() else 0o640
    fd, tmp_name = tempfile.mkstemp(prefix=f".{p.name}.", suffix=".tmp", dir=p.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(dump_config(config))
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(tmp_name, mode)
        os.replace(tmp_name, p)
        dir_fd = os.open(p.parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except Exception:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise
