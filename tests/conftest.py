from __future__ import annotations

import asyncio
import ipaddress
import inspect
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


@pytest.fixture(autouse=True)
def require_loopback_test_listeners(monkeypatch):
    original_start_server = asyncio.start_server

    async def guarded_start_server(client_connected_cb, host=None, port=None, **kwargs):
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


def pytest_collection_modifyitems(items) -> None:
    marker = pytest.mark.socket_listener
    for item in items:
        try:
            source = inspect.getsource(item.obj)
        except (OSError, TypeError):
            continue
        if (
            "await srv.start()" in source
            or "await asyncio.start_server(" in source
            or "start_listener(" in source
        ):
            item.add_marker(marker)
