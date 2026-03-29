from __future__ import annotations

import re
import ipaddress
from urllib.parse import urlparse

_SOCKET_TUPLE_RE = re.compile(r"\('([^']+)',\s*([0-9]+)(?:,\s*[0-9]+,\s*[0-9]+)?\)")


def _host_kind(host: str) -> str:
    raw = str(host or "").strip()
    if not raw:
        return "endpoint"
    try:
        ip = ipaddress.ip_address(raw)
    except ValueError:
        if raw.lower() == "localhost":
            return "loopback"
        return "host"
    if ip.is_loopback:
        return "loopback"
    return "ipv6" if ip.version == 6 else "ipv4"


def format_endpoint(endpoint: object | None) -> str:
    if isinstance(endpoint, tuple) and endpoint:
        host = str(endpoint[0] or "").strip()
        port = endpoint[1] if len(endpoint) > 1 else None
        if not host:
            return ""
        host_txt = f"[{host}]" if ":" in host and not host.startswith("[") else host
        return f"{host_txt}:{int(port)}" if isinstance(port, int) else host_txt
    if endpoint is None:
        return ""
    return str(endpoint).strip()


def describe_socket_path(peer: object | None, local: object | None = None) -> str:
    remote_txt = format_endpoint(peer)
    local_txt = format_endpoint(local)
    host = ""
    if isinstance(peer, tuple) and peer:
        host = str(peer[0] or "").strip()
    elif isinstance(peer, str):
        host = str(peer).strip()
    kind = _host_kind(host)
    if remote_txt and local_txt:
        return f"{kind} {remote_txt} -> {local_txt}"
    if remote_txt:
        return f"{kind} {remote_txt}"
    if local_txt:
        return local_txt
    return ""


def describe_session_path(channel: str, peer: object | None, local: object | None = None, forwarded_for: str = "") -> str:
    chan = str(channel or "").strip().lower() or "session"
    forwarded = str(forwarded_for or "").strip()
    if forwarded:
        host = forwarded.split(",", 1)[0].strip()
        kind = _host_kind(host)
        local_txt = format_endpoint(local)
        if local_txt:
            return f"{chan} proxied {kind} {host} -> {local_txt}"
        return f"{chan} proxied {kind} {host}"
    desc = describe_socket_path(peer, local)
    return f"{chan} {desc}".strip() if desc else chan


def describe_transport_dsn(dsn: str) -> tuple[str, str]:
    raw = str(dsn or "").strip()
    if not raw:
        return "", ""
    u = urlparse(raw)
    scheme = (u.scheme or "").lower()
    if scheme in {"tcp", "dxspider", "spidertelnet"} and u.hostname:
        host = str(u.hostname)
        kind = _host_kind(host)
        host_txt = f"[{host}]" if ":" in host and not host.startswith("[") else host
        endpoint = f"{host_txt}:{u.port}" if u.port is not None else host_txt
        mapped = "dxspider" if scheme in {"dxspider", "spidertelnet"} else "tcp"
        return mapped, f"{kind} {endpoint}"
    if scheme in {"kiss", "kiss_serial"}:
        return "kiss_serial", str(u.path or "").strip()
    if scheme in {"ax25", "ax25_socket"}:
        src = str(u.hostname or "").strip().upper()
        dst = str(u.path[1:] if u.path.startswith("/") else u.path or "").strip().upper()
        if src and dst:
            return "ax25_socket", f"{src}->{dst}"
        return "ax25_socket", src or dst
    return scheme or "transport", raw


def normalize_recorded_path(text: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""

    def _replace(match: re.Match[str]) -> str:
        host = match.group(1)
        port = int(match.group(2))
        endpoint = format_endpoint((host, port))
        kind = _host_kind(host)
        return f"{kind} {endpoint}".strip()

    return _SOCKET_TUPLE_RE.sub(_replace, raw)
