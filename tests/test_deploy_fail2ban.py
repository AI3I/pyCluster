from pathlib import Path


def test_fail2ban_scanner_jail_and_install_hooks_exist() -> None:
    lib = Path("/home/jdlewis/GitHub/pyCluster/deploy/lib.sh").read_text(encoding="utf-8")
    scanner_filter = Path("/home/jdlewis/GitHub/pyCluster/deploy/fail2ban/filter.d/pycluster-auth-scanner.conf").read_text(encoding="utf-8")
    scanner_jail = Path("/home/jdlewis/GitHub/pyCluster/deploy/fail2ban/jail.d/pycluster-scanner.local").read_text(encoding="utf-8")
    core_jail = Path("/home/jdlewis/GitHub/pyCluster/deploy/fail2ban/jail.d/pycluster-core.local").read_text(encoding="utf-8")

    assert "pycluster-auth-scanner.conf" in lib
    assert "pycluster-scanner.local" in lib
    assert "pycluster-telnet-scanner" in lib

    assert "channel=telnet" in scanner_filter
    assert "invalid_callsign" in scanner_filter
    assert "registration_request_required" in scanner_filter
    assert "telnet_login_not_allowed" in scanner_filter

    assert "[pycluster-telnet-scanner]" in scanner_jail
    assert "maxretry = 2" in scanner_jail
    assert "findtime = 2m" in scanner_jail
    assert "bantime = 24h" in scanner_jail

    assert "maxretry = 4" in core_jail
    assert "findtime = 5m" in core_jail
    assert "bantime = 2h" in core_jail
