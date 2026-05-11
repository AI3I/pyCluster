from __future__ import annotations

ACCESS_CHANNELS: tuple[str, ...] = ("telnet", "web")
ACCESS_CAPABILITIES: tuple[str, ...] = ("login", "spots", "rbn", "chat", "announce", "wx", "wcy", "wwv")
CLUSTER_NODE_FAMILIES: frozenset[str] = frozenset({"pycluster", "dxspider", "dxnet", "arcluster", "clx"})

_NON_AUTH_DEFAULTS: dict[str, bool] = {
    "login": True,
    "spots": False,
    "rbn": False,
    "chat": True,
    "announce": False,
    "wx": True,
    "wcy": True,
    "wwv": True,
}


def normalize_access_privilege(privilege: str | None) -> str:
    level = str(privilege or "").strip().lower()
    if level == "admin":
        return "sysop"
    if level in {"op", "operator"}:
        return "user"
    if level in {"", "user", "sysop"}:
        return level
    return ""


def default_access_allowed(privilege: str | None, blocked_login: bool, channel: str, capability: str) -> bool:
    if blocked_login:
        return False
    level = normalize_access_privilege(privilege)
    if level in {"user", "sysop"}:
        return True
    return bool(_NON_AUTH_DEFAULTS.get(capability, False))
