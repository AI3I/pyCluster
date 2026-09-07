from __future__ import annotations

from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def _stub_dir(tmp_path: Path, *, active_unit: str = "", ntp: str = "no", synchronized: str = "no", have_timedatectl: bool = True) -> Path:
    stubs = tmp_path / "bin"
    stubs.mkdir(exist_ok=True)
    systemctl = stubs / "systemctl"
    systemctl.write_text(
        "#!/usr/bin/env bash\n"
        f'active_unit="{active_unit}"\n'
        'case "$1" in\n'
        '  is-active) [ -n "$active_unit" ] && [ "$2" = "$active_unit" ] && exit 0; exit 3 ;;\n'
        '  list-unit-files) [ -n "$active_unit" ] && [ "$2" = "$active_unit" ] && exit 0; exit 1 ;;\n'
        '  enable|start) echo "systemctl $*" >>"$STUB_LOG"; exit 0 ;;\n'
        'esac\n'
        "exit 0\n",
        encoding="utf-8",
    )
    systemctl.chmod(0o755)
    if have_timedatectl:
        timedatectl = stubs / "timedatectl"
        timedatectl.write_text(
            "#!/usr/bin/env bash\n"
            'if [ "$1" = "set-ntp" ]; then echo "timedatectl $*" >>"$STUB_LOG"; exit 0; fi\n'
            'case "$*" in\n'
            f'  *NTPSynchronized*) printf "{synchronized}\\n" ;;\n'
            f'  *NTP*) printf "{ntp}\\n" ;;\n'
            'esac\n'
            "exit 0\n",
            encoding="utf-8",
        )
        timedatectl.chmod(0o755)
    return stubs


def _run(script: str, tmp_path: Path, stubs: Path) -> subprocess.CompletedProcess[str]:
    log = tmp_path / "stub.log"
    log.touch()
    return subprocess.run(
        ["bash", "-c", f'export PATH="{stubs}:$PATH"; export STUB_LOG="{log}"; . deploy/lib.sh; {script}'],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )


def test_status_line_reports_synchronized_provider(tmp_path: Path) -> None:
    stubs = _stub_dir(tmp_path, active_unit="chronyd.service", ntp="yes", synchronized="yes")
    proc = _run("time_sync_status_line", tmp_path, stubs)
    assert proc.stdout == "chronyd.service (synchronized)"


def test_status_line_reports_enabled_but_not_yet_converged(tmp_path: Path) -> None:
    stubs = _stub_dir(tmp_path, active_unit="systemd-timesyncd.service", ntp="yes", synchronized="no")
    proc = _run("time_sync_status_line", tmp_path, stubs)
    assert proc.stdout == "systemd-timesyncd.service (enabled, not yet synchronized)"


def test_status_line_reports_unsynchronized_host(tmp_path: Path) -> None:
    stubs = _stub_dir(tmp_path, active_unit="", ntp="no", synchronized="no")
    proc = _run("time_sync_status_line", tmp_path, stubs)
    assert proc.stdout == "none (NOT synchronized)"


def test_status_line_is_unknown_without_timedatectl(tmp_path: Path) -> None:
    stubs = _stub_dir(tmp_path, active_unit="", have_timedatectl=False)
    proc = _run('PATH="' + str(stubs) + '"; time_sync_status_line', tmp_path, stubs)
    assert proc.stdout == "none (unknown)"


def test_packages_are_not_installed_when_a_provider_is_active(tmp_path: Path) -> None:
    """Replacing a working systemd-timesyncd with chrony would be disruptive."""
    stubs = _stub_dir(tmp_path, active_unit="systemd-timesyncd.service", ntp="yes", synchronized="yes")
    proc = _run(
        'install_packages() { echo "INSTALL:$*"; }; ensure_time_sync_packages',
        tmp_path,
        stubs,
    )
    assert "INSTALL:" not in proc.stdout


def test_chrony_is_installed_when_nothing_synchronizes(tmp_path: Path) -> None:
    stubs = _stub_dir(tmp_path, active_unit="", ntp="no", synchronized="no")
    proc = _run(
        'install_packages() { echo "INSTALL:$*"; }; ensure_time_sync_packages',
        tmp_path,
        stubs,
    )
    assert "INSTALL:chrony" in proc.stdout


def test_enable_warns_but_does_not_fail_without_a_provider(tmp_path: Path) -> None:
    """Containers inherit the host clock; refusing to deploy there would be wrong."""
    stubs = _stub_dir(tmp_path, active_unit="", ntp="no", synchronized="no")
    proc = _run("enable_time_sync_service", tmp_path, stubs)
    assert proc.returncode == 0
    assert "no time synchronization service is running" in proc.stderr
    assert "duplicate suppression" in proc.stderr


def test_enable_starts_and_reports_an_available_provider(tmp_path: Path) -> None:
    stubs = _stub_dir(tmp_path, active_unit="chronyd.service", ntp="yes", synchronized="yes")
    proc = _run("enable_time_sync_service", tmp_path, stubs)
    assert proc.returncode == 0
    assert "time synchronization: chronyd.service (synchronized)" in proc.stdout
    log = (tmp_path / "stub.log").read_text(encoding="utf-8")
    assert "systemctl enable chronyd.service" in log
    assert "systemctl start chronyd.service" in log
    assert "timedatectl set-ntp true" in log


def test_install_and_upgrade_run_the_time_sync_hooks() -> None:
    for name in ("install.sh", "upgrade.sh"):
        script = (ROOT / "deploy" / name).read_text(encoding="utf-8")
        assert "ensure_time_sync_packages" in script, name
        assert "enable_time_sync_service" in script, name


def test_doctor_and_support_bundle_report_clock_state() -> None:
    doctor = (ROOT / "deploy" / "doctor.sh").read_text(encoding="utf-8")
    assert "time_sync_status_line" in doctor
    assert 'status "time sync"' in doctor
    bundle = (ROOT / "deploy" / "support-bundle.sh").read_text(encoding="utf-8")
    assert "chronyc tracking" in bundle
