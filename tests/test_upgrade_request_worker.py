from __future__ import annotations

import importlib.util
from pathlib import Path


_SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "run_upgrade_request.py"
_SPEC = importlib.util.spec_from_file_location("run_upgrade_request_script", _SCRIPT_PATH)
assert _SPEC is not None and _SPEC.loader is not None
run_upgrade_request = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(run_upgrade_request)


def test_upgrade_request_selects_latest_newer_semver_tag() -> None:
    assert run_upgrade_request._latest_upgrade_tag(["v1.0.6", "v1.0.9", "not-a-version", "v1.0.8"], "1.0.6") == "v1.0.9"
    assert run_upgrade_request._latest_upgrade_tag(["v1.0.6", "v1.0.8"], "1.0.9") == ""


def test_upgrade_request_reads_source_version_after_checkout(tmp_path: Path) -> None:
    init_path = tmp_path / "src" / "pycluster" / "__init__.py"
    init_path.parent.mkdir(parents=True)
    init_path.write_text('__version__ = "1.0.8"\n', encoding="utf-8")

    assert run_upgrade_request._read_source_version(tmp_path) == "1.0.8"


def test_upgrade_systemd_worker_uses_source_checkout_and_live_runtime_paths() -> None:
    unit = (Path(__file__).resolve().parent.parent / "deploy" / "systemd" / "pycluster-upgrade.service").read_text(encoding="utf-8")

    assert "WorkingDirectory=/usr/src/pyCluster" in unit
    assert "/usr/src/pyCluster/scripts/run_upgrade_request.py" in unit
    assert "--repo-root /usr/src/pyCluster" in unit
    assert "--request /home/pycluster/pyCluster/data/upgrade-request.json" in unit
    assert "--status /home/pycluster/pyCluster/data/upgrade-status.json" in unit
    assert "--log /home/pycluster/pyCluster/logs/upgrade.log" in unit
