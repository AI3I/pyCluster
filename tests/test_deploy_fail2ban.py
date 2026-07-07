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


def test_setup_nginx_disables_distribution_default_site() -> None:
    setup = Path("/home/jdlewis/GitHub/pyCluster/deploy/setup-nginx.sh").read_text(encoding="utf-8")

    assert 'rm -f "$NGINX_CONFIG_DIR/default.conf"' in setup
    assert "rm -f /etc/nginx/sites-enabled/default" in setup
    assert "proxy_pass http://127.0.0.1:${upstream_port};" in setup
    assert "write_sysop_placeholder_config" in setup
    assert 'local conf="$NGINX_CONFIG_DIR/pycluster-sysop.conf"' in setup
    assert "--nginx-server-root /etc/nginx" in setup


def test_data_refresh_service_uses_generic_names_and_migrates_legacy_timer() -> None:
    lib = Path("/home/jdlewis/GitHub/pyCluster/deploy/lib.sh").read_text(encoding="utf-8")
    doctor = Path("/home/jdlewis/GitHub/pyCluster/deploy/doctor.sh").read_text(encoding="utf-8")
    uninstall = Path("/home/jdlewis/GitHub/pyCluster/deploy/uninstall.sh").read_text(encoding="utf-8")
    timer = Path("/home/jdlewis/GitHub/pyCluster/deploy/systemd/pycluster-data-refresh.timer").read_text(encoding="utf-8")
    service = Path("/home/jdlewis/GitHub/pyCluster/deploy/systemd/pycluster-data-refresh.service").read_text(encoding="utf-8")

    assert "PYCLUSTER_DATA_REFRESH_TIMER_NAME" in lib
    assert "pycluster-data-refresh.timer" in lib
    assert "normalize_country_data_config_paths()" in lib
    assert "re.IGNORECASE" in lib
    assert "fixtures/live/dxspider" in lib
    assert "seed_runtime_data_from_fixtures()" not in lib
    assert '"./data/{filename}"' in lib
    assert "pycluster-data-refresh.service" in timer
    assert "OnBootSec=2min" in timer
    assert "OnCalendar=*-*-* 00/6:15:00" in timer
    assert "RandomizedDelaySec=30m" in timer
    assert "runtime data refresh" in timer
    assert "runtime data refresh" in service
    assert "systemctl disable --now \"$PYCLUSTER_LEGACY_CTY_REFRESH_TIMER_NAME\"" in lib
    assert "data refresh timer" in doctor
    assert "PYCLUSTER_LEGACY_CTY_REFRESH_TIMER_NAME" in uninstall
    assert "pycluster-auth-scanner.conf" in uninstall
    assert "pycluster-scanner.local" in uninstall


def test_upgrade_and_repair_refresh_invalid_strings_catalog() -> None:
    lib = Path("/home/jdlewis/GitHub/pyCluster/deploy/lib.sh").read_text(encoding="utf-8")
    upgrade = Path("/home/jdlewis/GitHub/pyCluster/deploy/upgrade.sh").read_text(encoding="utf-8")
    repair = Path("/home/jdlewis/GitHub/pyCluster/deploy/repair.sh").read_text(encoding="utf-8")

    assert "validate_or_refresh_strings_toml()" in lib
    assert "tomllib.loads(Path(sys.argv[1]).read_text" in lib
    assert "invalid strings.toml detected" in lib
    assert "cp -a \"$dest\" \"$backup\"" in lib
    assert "install -o \"$PYCLUSTER_USER\" -g \"$PYCLUSTER_GROUP\" -m 0640 \"$src\" \"$dest\"" in lib
    assert "validate_or_refresh_strings_toml" in upgrade
    assert "validate_or_refresh_strings_toml" in repair
