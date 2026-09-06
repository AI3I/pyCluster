"""Canonical addresses and trusted-proxy client identity for local access policy."""

import ipaddress


def address(value: str):
    parsed = ipaddress.ip_address(value.split('%', 1)[0])
    return parsed.ipv4_mapped if isinstance(parsed, ipaddress.IPv6Address) and parsed.ipv4_mapped else parsed


def network(value: str):
    parsed = ipaddress.ip_network(value.strip(), strict=False)
    if isinstance(parsed, ipaddress.IPv6Network) and parsed.prefixlen >= 96 and parsed.network_address.ipv4_mapped:
        return ipaddress.ip_network((parsed.network_address.ipv4_mapped, parsed.prefixlen - 96))
    return parsed


def client_address(peer, forwarded: str, trusted_proxies) -> str:
    raw = str(peer[0]) if isinstance(peer, tuple) and peer else str(peer or '')
    try:
        current = address(raw)
        trusted = [network(item) for item in trusted_proxies]
    except (ValueError, TypeError):
        return raw or '-'
    # Walk from the socket towards the client, stopping at the first untrusted hop.
    for hop in reversed(forwarded.split(',')):
        if not any(current in subnet for subnet in trusted):
            break
        try:
            current = address(hop.strip())
        except ValueError:
            break
    return str(current)
