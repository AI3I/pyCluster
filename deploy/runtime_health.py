#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import socket
import sys
import time
import urllib.request


def _bootstrap_import_path() -> None:
    root = Path(__file__).resolve().parent.parent
    src = root / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))


_bootstrap_import_path()

from pycluster.config import load_config  # noqa: E402


def _probe_hosts(configured: str) -> tuple[str, ...]:
    host = str(configured or "").strip().strip("[]")
    if not host:
        return ("::1", "127.0.0.1")
    if host == "0.0.0.0":
        return ("127.0.0.1",)
    if host == "::":
        return ("::1",)
    return (host,)


def _tcp_ready(hosts: tuple[str, ...], port: int, timeout: float) -> bool:
    for host in hosts:
        try:
            with socket.create_connection((host, int(port)), timeout=timeout):
                return True
        except OSError:
            continue
    return False


def _http_ready(hosts: tuple[str, ...], port: int, timeout: float) -> bool:
    for host in hosts:
        rendered = f"[{host}]" if ":" in host else host
        try:
            request = urllib.request.Request(f"http://{rendered}:{int(port)}/health")
            with urllib.request.urlopen(request, timeout=timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
            if response.status == 200 and payload.get("ok") is True:
                return True
        except (OSError, ValueError, json.JSONDecodeError):
            continue
    return False


def probe_once(config_path: Path, *, connect_timeout: float = 1.0) -> dict[str, bool]:
    config = load_config(config_path)
    results: dict[str, bool] = {}
    telnet_hosts = _probe_hosts(config.telnet.host)
    for port in config.telnet.ports or (config.telnet.port,):
        results[f"telnet:{port}"] = _tcp_ready(telnet_hosts, int(port), connect_timeout)
    results[f"sysop-web:{config.web.port}"] = _http_ready(
        _probe_hosts(config.web.host), config.web.port, connect_timeout
    )
    if config.public_web.enabled:
        results[f"public-web:{config.public_web.port}"] = _http_ready(
            _probe_hosts(config.public_web.host), config.public_web.port, connect_timeout
        )
    return results


def wait_until_ready(config_path: Path, *, timeout: float, interval: float) -> dict[str, bool]:
    deadline = time.monotonic() + max(0.0, timeout)
    results: dict[str, bool] = {}
    while True:
        results = probe_once(config_path)
        if results and all(results.values()):
            return results
        if time.monotonic() >= deadline:
            return results
        time.sleep(max(0.05, interval))


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify pyCluster's configured local runtime listeners.")
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--timeout", type=float, default=0.0, help="Seconds to wait for every listener.")
    parser.add_argument("--interval", type=float, default=1.0, help="Seconds between retries.")
    args = parser.parse_args()
    if os.environ.get("PYCLUSTER_SKIP_RUNTIME_HEALTH", "").strip() == "1":
        print("runtime-health skipped by PYCLUSTER_SKIP_RUNTIME_HEALTH")
        return 0
    try:
        results = wait_until_ready(args.config.resolve(), timeout=args.timeout, interval=args.interval)
    except Exception as exc:
        print(f"runtime-health config/error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    for target, ready in results.items():
        print(f"{target}={'ready' if ready else 'unavailable'}")
    return 0 if results and all(results.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
