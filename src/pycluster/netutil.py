from __future__ import annotations

import ipaddress
import json
import subprocess


def valid_global_ip(raw: object, *, version: int | None = None) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    try:
        ip = ipaddress.ip_address(text)
    except ValueError:
        return ""
    if version is not None and ip.version != version:
        return ""
    return text if ip.is_global else ""


def detected_public_ip_addresses() -> dict[str, str]:
    found: dict[int, str] = {}

    def add(raw: object) -> None:
        text = str(raw or "").strip()
        if not text:
            return
        text = text.split("%", 1)[0]
        try:
            ip = ipaddress.ip_address(text)
        except ValueError:
            return
        if ip.is_global and ip.version not in found:
            found[ip.version] = str(ip)

    try:
        proc = subprocess.run(
            ["ip", "-j", "addr", "show", "scope", "global"],
            check=False,
            capture_output=True,
            text=True,
            timeout=2,
        )
        if proc.returncode == 0 and proc.stdout.strip():
            rows = json.loads(proc.stdout)
            if isinstance(rows, list):
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    for addr in row.get("addr_info", []) or ():
                        if isinstance(addr, dict):
                            add(addr.get("local"))
    except Exception:
        pass

    if 4 not in found or 6 not in found:
        try:
            proc = subprocess.run(["hostname", "-I"], check=False, capture_output=True, text=True, timeout=2)
            if proc.returncode == 0:
                for token in proc.stdout.split():
                    add(token)
        except Exception:
            pass

    return {"ipv4": found.get(4, ""), "ipv6": found.get(6, "")}
