from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace


def _load_module():
    path = Path(__file__).resolve().parents[1] / "deploy" / "runtime_health.py"
    spec = importlib.util.spec_from_file_location("runtime_health_script", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_probe_hosts_maps_wildcards_to_loopback() -> None:
    module = _load_module()
    assert module._probe_hosts("") == ("::1", "127.0.0.1")
    assert module._probe_hosts("0.0.0.0") == ("127.0.0.1",)
    assert module._probe_hosts("::") == ("::1",)
    assert module._probe_hosts("192.0.2.10") == ("192.0.2.10",)


def test_probe_once_checks_every_configured_surface(tmp_path, monkeypatch) -> None:
    module = _load_module()
    config = SimpleNamespace(
        telnet=SimpleNamespace(host="0.0.0.0", ports=(7300, 7373), port=7300),
        web=SimpleNamespace(host="127.0.0.1", port=8080),
        public_web=SimpleNamespace(enabled=True, host="::", port=8081),
    )
    monkeypatch.setattr(module, "load_config", lambda _path: config)
    tcp_calls = []
    http_calls = []
    monkeypatch.setattr(module, "_tcp_ready", lambda hosts, port, timeout: tcp_calls.append((hosts, port)) or True)
    monkeypatch.setattr(module, "_http_ready", lambda hosts, port, timeout: http_calls.append((hosts, port)) or True)

    results = module.probe_once(tmp_path / "pycluster.toml")

    assert results == {
        "telnet:7300": True,
        "telnet:7373": True,
        "sysop-web:8080": True,
        "public-web:8081": True,
    }
    assert tcp_calls == [(("127.0.0.1",), 7300), (("127.0.0.1",), 7373)]
    assert http_calls == [(("127.0.0.1",), 8080), (("::1",), 8081)]


def test_wait_until_ready_retries_until_all_surfaces_pass(tmp_path, monkeypatch) -> None:
    module = _load_module()
    results = iter(({"telnet:7300": True, "sysop-web:8080": False}, {"telnet:7300": True, "sysop-web:8080": True}))
    monkeypatch.setattr(module, "probe_once", lambda _path: next(results))
    monkeypatch.setattr(module.time, "sleep", lambda _seconds: None)

    assert module.wait_until_ready(tmp_path / "pycluster.toml", timeout=5, interval=0.1) == {
        "telnet:7300": True,
        "sysop-web:8080": True,
    }
