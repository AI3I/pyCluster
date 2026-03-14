from __future__ import annotations

from dataclasses import asdict, dataclass, field
import ipaddress
from pathlib import Path
import re
import time
from urllib.parse import quote

from .dxspider_userfile import import_dxspider_user_file
from .store import SpotStore


_BADWORD_RE = re.compile(r"^\s*'?([^'\s][^'=]*)'?\s*=>\s*(\d+)\s*,?\s*$")
_CONNECT_TELNET_RE = re.compile(r"^\s*connect\s+telnet\s+(?P<host>\S+)\s+(?P<port>\d+)\s*$", re.IGNORECASE)
_LOGIN_PROMPT_RE = re.compile(r"^\s*'[^']*login:[^']*'\s+'(?P<call>[^']+)'\s*$", re.IGNORECASE)
_PASSWORD_PROMPT_RE = re.compile(
    r"^\s*'[^']*pass(?:word|phrase)?[^']*'\s+'(?P<password>[^']+)'\s*$",
    re.IGNORECASE,
)
_CLIENT_RE = re.compile(r"^\s*client\s+(?P<peer>\S+)\s+telnet\s*$", re.IGNORECASE)
_PEER_PREF_PREFIX = "peer.outbound."


@dataclass(slots=True)
class DxSpiderConnectRecord:
    name: str
    host: str
    port: int
    login_call: str
    client_call: str
    password: str = ""
    profile: str = "dxspider"
    reconnect: bool = True

    @property
    def dsn(self) -> str:
        query = [
            f"login={quote(self.login_call, safe='')}",
            f"client={quote(self.client_call, safe='')}",
        ]
        if self.password:
            query.append(f"password={quote(self.password, safe='')}")
        return f"{self.profile}://{self.host}:{self.port}?{'&'.join(query)}"


@dataclass(slots=True)
class DxSpiderMigrationReport:
    source_root: str
    local_data_dir: str
    user_file: str = ""
    users_imported: int = 0
    users_skipped: int = 0
    motd_file: str = ""
    motd_imported: bool = False
    badword_file: str = ""
    badwords_imported: int = 0
    peers_dir: str = ""
    peers_imported: int = 0
    peers_skipped: int = 0
    peer_names: list[str] = field(default_factory=list)
    badip_file: str = ""
    badip_entries_seen: int = 0
    badip_fail2ban_file: str = ""
    badip_fail2ban_entries: int = 0
    badip_fail2ban_skipped: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def discover_dxspider_local_data(source: str | Path) -> tuple[Path, Path]:
    root = Path(source).expanduser().resolve()
    if root.is_file():
        root = root.parent
    if root.is_dir() and root.name == "local_data":
        return root, root
    candidates = [
        root / "local_data",
        root / "spider" / "local_data",
        root / "home" / "sysop" / "spider" / "local_data",
    ]
    for candidate in candidates:
        if candidate.is_dir():
            return root, candidate
    raise FileNotFoundError(f"could not find DXSpider local_data under {root}")


def preferred_user_export(local_data_dir: Path) -> Path | None:
    for name in ("user_asc", "user_json"):
        candidate = local_data_dir / name
        if candidate.is_file():
            return candidate
    return None


def parse_badword_file(path: str | Path) -> list[tuple[str, int]]:
    out: list[tuple[str, int]] = []
    seen: set[str] = set()
    for raw_line in Path(path).read_text(encoding="utf-8", errors="replace").splitlines():
        match = _BADWORD_RE.match(raw_line)
        if not match:
            continue
        word = match.group(1).strip().strip("'").strip('"')
        if not word:
            continue
        key = word.upper()
        if key in seen:
            continue
        seen.add(key)
        out.append((word, int(match.group(2))))
    return out


def parse_badip_file(path: str | Path) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw_line in Path(path).read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line in seen:
            continue
        seen.add(line)
        out.append(line)
    return out


def export_badip_fail2ban_file(source: str | Path, destination: str | Path) -> tuple[int, list[str]]:
    entries = parse_badip_file(source)
    out_path = Path(destination)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    exact_ips: list[str] = []
    skipped: list[str] = []
    seen: set[str] = set()
    for entry in entries:
        try:
            ipaddress.ip_address(entry)
        except ValueError:
            skipped.append(entry)
            continue
        if entry in seen:
            continue
        seen.add(entry)
        exact_ips.append(entry)
    lines = [
        "# Imported from DXSpider badip.local",
        "# Exact IPs are applied to pyCluster fail2ban jails; CIDR entries remain reported but are not applied.",
        "",
    ]
    lines.extend(exact_ips)
    out_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return len(exact_ips), skipped


def _peer_pref_key(name: str, field: str) -> str:
    slug = re.sub(r"[^a-z0-9_.-]", "_", name.lower())
    return f"{_PEER_PREF_PREFIX}{slug}.{field}"


async def save_outbound_peer_target(
    store: SpotStore,
    node_call: str,
    name: str,
    dsn: str,
    *,
    profile: str = "dxspider",
    reconnect: bool = True,
    epoch: int | None = None,
) -> None:
    now = int(epoch or time.time())
    values = {
        _peer_pref_key(name, "name"): name,
        _peer_pref_key(name, "dsn"): dsn,
        _peer_pref_key(name, "profile"): profile,
        _peer_pref_key(name, "reconnect"): "on" if reconnect else "off",
        _peer_pref_key(name, "retry_count"): "0",
        _peer_pref_key(name, "next_retry_epoch"): "0",
    }
    for key, value in values.items():
        await store.set_user_pref(node_call, key, value, now)
    await store.delete_user_pref(node_call, _peer_pref_key(name, "last_error"))


def parse_dxspider_connect_file(path: str | Path) -> DxSpiderConnectRecord | None:
    connect_match = None
    login_call = ""
    client_call = ""
    password = ""
    source = Path(path)
    for raw_line in source.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = _CONNECT_TELNET_RE.match(line)
        if match:
            connect_match = match
            continue
        match = _LOGIN_PROMPT_RE.match(line)
        if match:
            login_call = match.group("call").strip().upper()
            continue
        match = _PASSWORD_PROMPT_RE.match(line)
        if match:
            password = match.group("password").strip()
            continue
        match = _CLIENT_RE.match(line)
        if match:
            client_call = match.group("peer").strip().upper()
            continue
    if connect_match is None or not login_call or not client_call:
        return None
    return DxSpiderConnectRecord(
        name=source.name.upper(),
        host=connect_match.group("host"),
        port=int(connect_match.group("port")),
        login_call=login_call,
        client_call=client_call,
        password=password,
    )


async def migrate_dxspider_local_data(
    store: SpotStore,
    node_call: str,
    source: str | Path,
    *,
    now_epoch: int | None = None,
    badip_fail2ban_file: str | Path | None = None,
    import_users: bool = True,
    import_motd: bool = True,
    import_badwords: bool = True,
    import_peers: bool = True,
) -> DxSpiderMigrationReport:
    source_root, local_data_dir = discover_dxspider_local_data(source)
    now = int(now_epoch or time.time())
    report = DxSpiderMigrationReport(
        source_root=str(source_root),
        local_data_dir=str(local_data_dir),
    )

    if import_users:
        user_file = preferred_user_export(local_data_dir)
        if user_file is None:
            report.warnings.append("no DXSpider user export found (expected user_asc or user_json)")
        else:
            report.user_file = str(user_file)
            imported, skipped = await import_dxspider_user_file(store, user_file)
            report.users_imported = imported
            report.users_skipped = skipped

    if import_motd:
        motd_file = local_data_dir / "motd"
        if motd_file.is_file():
            motd = motd_file.read_text(encoding="utf-8", errors="replace").rstrip()
            if motd:
                await store.set_user_pref(node_call, "motd", motd, now)
                report.motd_file = str(motd_file)
                report.motd_imported = True

    if import_badwords:
        badword_file = local_data_dir / "badword"
        if badword_file.is_file():
            report.badword_file = str(badword_file)
            entries = parse_badword_file(badword_file)
            for word, epoch in entries:
                await store.add_deny_rule("badword", word, epoch or now)
            report.badwords_imported = len(entries)

    if import_peers:
        connect_dir = source_root / "connect"
        if connect_dir.is_dir():
            report.peers_dir = str(connect_dir)
            for candidate in sorted(connect_dir.iterdir()):
                if candidate.name.startswith(".") or not candidate.is_file():
                    continue
                record = parse_dxspider_connect_file(candidate)
                if record is None:
                    report.peers_skipped += 1
                    report.warnings.append(f"unsupported connect script: {candidate.name}")
                    continue
                await save_outbound_peer_target(
                    store,
                    node_call,
                    record.name,
                    record.dsn,
                    profile=record.profile,
                    reconnect=record.reconnect,
                    epoch=now,
                )
                report.peers_imported += 1
                report.peer_names.append(record.name)

    badip_file = local_data_dir / "badip.local"
    if badip_file.is_file():
        report.badip_file = str(badip_file)
        badips = parse_badip_file(badip_file)
        report.badip_entries_seen = len(badips)
        if badip_fail2ban_file:
            report.badip_fail2ban_file = str(Path(badip_fail2ban_file))
            imported, skipped = export_badip_fail2ban_file(badip_file, badip_fail2ban_file)
            report.badip_fail2ban_entries = imported
            report.badip_fail2ban_skipped = skipped
            if skipped:
                report.warnings.append(
                    "some badip.local entries were CIDR ranges or unsupported formats and were not applied to fail2ban"
                )
        elif badips:
            report.warnings.append(
                "badip.local entries were detected but were not imported; pyCluster currently relies on fail2ban and local host controls for IP blocking"
            )

    return report
