from __future__ import annotations

import asyncio
import ipaddress
import inspect
import re
import socket
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


@pytest.fixture(autouse=True)
def require_loopback_test_listeners(monkeypatch, request):
    monkeypatch.setenv("PYCLUSTER_SKIP_RUNTIME_HEALTH", "1")
    original_start_server = asyncio.start_server
    original_socket = socket.socket
    original_getaddrinfo = socket.getaddrinfo
    listener_test = request.node.get_closest_marker("socket_listener") is not None

    class GuardedSocket(original_socket):
        @staticmethod
        def _require_network() -> None:
            if not listener_test:
                pytest.fail("default tests may not perform network socket operations; use the socket_listener marker")

        def bind(self, address):
            self._require_network()
            if self.family in {socket.AF_INET, socket.AF_INET6}:
                host = str(address[0] if isinstance(address, tuple) and address else address or "").strip()
                try:
                    is_loopback = ipaddress.ip_address(host).is_loopback
                except ValueError:
                    is_loopback = host.lower() == "localhost"
                if not is_loopback:
                    raise AssertionError(f"tests may only bind loopback TCP/UDP listeners, not {address!r}")
            return super().bind(address)

        def connect(self, address):
            self._require_network()
            return super().connect(address)

        def connect_ex(self, address):
            self._require_network()
            return super().connect_ex(address)

        def sendto(self, *args, **kwargs):
            self._require_network()
            return super().sendto(*args, **kwargs)

    def guarded_socket(family=socket.AF_INET, type=socket.SOCK_STREAM, proto=0, fileno=None):
        if not listener_test and family in {socket.AF_INET, socket.AF_INET6}:
            pytest.fail("default tests may not create network sockets; use the socket_listener marker")
        return GuardedSocket(family, type, proto, fileno)

    def guarded_getaddrinfo(*args, **kwargs):
        if not listener_test:
            pytest.fail("default tests may not perform DNS resolution; use the socket_listener marker")
        return original_getaddrinfo(*args, **kwargs)

    async def guarded_start_server(client_connected_cb, host=None, port=None, **kwargs):
        if not listener_test:
            pytest.skip("TCP listener tests require the explicit socket_listener marker")
        hosts = host if isinstance(host, (list, tuple)) else (host,)
        for value in hosts:
            text = str(value or "").strip()
            try:
                is_loopback = ipaddress.ip_address(text).is_loopback
            except ValueError:
                is_loopback = text.lower() == "localhost"
            if not is_loopback:
                raise AssertionError(f"tests may only open loopback TCP listeners, not {value!r}")
        return await original_start_server(client_connected_cb, host=host, port=port, **kwargs)

    monkeypatch.setattr(asyncio, "start_server", guarded_start_server)
    monkeypatch.setattr(socket, "socket", guarded_socket)
    monkeypatch.setattr(socket, "getaddrinfo", guarded_getaddrinfo)
    monkeypatch.setattr("pycluster.app.detected_public_ip_addresses", lambda: {"ipv4": "", "ipv6": ""})
    monkeypatch.setattr("pycluster.web_admin.WebAdminServer._fail2ban_ban_rows", lambda _self: [])


def pytest_collection_modifyitems(items) -> None:
    marker = pytest.mark.socket_listener
    for item in items:
        try:
            source = inspect.getsource(item.obj)
        except (OSError, TypeError):
            continue
        if (
            re.search(r"await\s+[A-Za-z_][A-Za-z0-9_.]*\.start\s*\(", source)
            or "await asyncio.start_server(" in source
            or "start_listener(" in source
            or "create_datagram_endpoint(" in source
            or "socket.socket(" in source
        ):
            item.add_marker(marker)
