from pathlib import Path
import sqlite3
import subprocess
import sys


def test_fail2ban_scanner_jail_and_install_hooks_exist() -> None:
    lib = Path("/home/jdlewis/GitHub/pyCluster/deploy/lib.sh").read_text(encoding="utf-8")
    scanner_filter = Path("/home/jdlewis/GitHub/pyCluster/deploy/fail2ban/filter.d/pycluster-auth-scanner.conf").read_text(encoding="utf-8")
    core_filter = Path("/home/jdlewis/GitHub/pyCluster/deploy/fail2ban/filter.d/pycluster-auth-core.conf").read_text(encoding="utf-8")
    web_filter = Path("/home/jdlewis/GitHub/pyCluster/deploy/fail2ban/filter.d/pycluster-auth-web.conf").read_text(encoding="utf-8")
    account_action = Path("/home/jdlewis/GitHub/pyCluster/deploy/fail2ban/action.d/pycluster-lock-account.conf").read_text(encoding="utf-8")
    scanner_jail = Path("/home/jdlewis/GitHub/pyCluster/deploy/fail2ban/jail.d/pycluster-scanner.local").read_text(encoding="utf-8")
    core_jail = Path("/home/jdlewis/GitHub/pyCluster/deploy/fail2ban/jail.d/pycluster-core.local").read_text(encoding="utf-8")

    assert "pycluster-auth-scanner.conf" in lib
    assert "pycluster-lock-account.conf" in lib
    assert "pycluster-scanner.local" in lib
    assert "pycluster-telnet-scanner" in lib

    assert "channel=telnet" in scanner_filter
    assert "invalid_callsign" in scanner_filter
    assert "registration_request_required" not in scanner_filter
    assert "telnet_login_not_allowed" not in scanner_filter
    assert "[sshd]" not in lib

    assert "[pycluster-telnet-scanner]" in scanner_jail
    assert "maxretry = 2" in scanner_jail
    assert "findtime = 2m" in scanner_jail
    assert "bantime = 24h" in scanner_jail

    assert "maxretry = 5" in core_jail
    assert "findtime = 5m" in core_jail
    assert "bantime = 2h" in core_jail
    assert "<F-USER>" in core_filter
    assert "<F-USER>" in web_filter
    assert "lock_user_account.py" in account_action
    assert "--call <F-USER>" in account_action


def test_fail2ban_account_lock_helper_updates_user_prefs(tmp_path) -> None:
    db = tmp_path / "pycluster.db"
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            CREATE TABLE user_prefs (
                call TEXT NOT NULL,
                pref_key TEXT NOT NULL,
                pref_value TEXT NOT NULL,
                updated_epoch INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(call, pref_key)
            )
            """
        )
        conn.commit()

    script = Path("/home/jdlewis/GitHub/pyCluster/scripts/lock_user_account.py")
    result = subprocess.run(
        [sys.executable, str(script), "--db", str(db), "--call", "AI3I-90", "--reason", "test lock"],
        check=True,
        text=True,
        capture_output=True,
    )
    assert "Locked AI3I-90" in result.stdout
    with sqlite3.connect(db) as conn:
        rows = dict(conn.execute("SELECT pref_key, pref_value FROM user_prefs WHERE call = 'AI3I-90'"))
    assert rows["registration_state"] == "locked"
    assert rows["failed_password_locked_epoch"]
    assert rows["failed_password_count"] == "0"
    assert rows["blocked_reason"] == "test lock"

    result = subprocess.run(
        [sys.executable, str(script), "--db", str(db), "--call", "AI3I-90", "--unlock"],
        check=True,
        text=True,
        capture_output=True,
    )
    assert "Unlocked AI3I-90" in result.stdout
    with sqlite3.connect(db) as conn:
        rows = dict(conn.execute("SELECT pref_key, pref_value FROM user_prefs WHERE call = 'AI3I-90'"))
    assert rows["registration_state"] == "verified"
    assert "failed_password_locked_epoch" not in rows


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
    assert "retention timer" in doctor
    assert "upgrade watcher" in doctor
    assert "from pycluster.config import load_config" in doctor
    assert 'status "telnet listener"' in doctor
    assert 'status "sysop web listener"' in doctor
    assert 'status "public web listener"' in doctor
    assert "public_web_probe_url" in doctor
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


def test_deploy_lifecycle_stops_live_services_and_removes_upgrade_units() -> None:
    lib = Path("/home/jdlewis/GitHub/pyCluster/deploy/lib.sh").read_text(encoding="utf-8")
    upgrade = Path("/home/jdlewis/GitHub/pyCluster/deploy/upgrade.sh").read_text(encoding="utf-8")
    repair = Path("/home/jdlewis/GitHub/pyCluster/deploy/repair.sh").read_text(encoding="utf-8")
    uninstall = Path("/home/jdlewis/GitHub/pyCluster/deploy/uninstall.sh").read_text(encoding="utf-8")

    assert upgrade.index("stop_service") < upgrade.index("backup_runtime_snapshot") < upgrade.index("sync_tree")
    assert repair.index("stop_service") < repair.index("backup_runtime_snapshot") < repair.index("sync_tree")
    assert uninstall.index("stop_service") < uninstall.index("backup_runtime_snapshot") < uninstall.index("disable_service")
    assert "arm_maintenance_failure_recovery" in upgrade
    assert "disarm_maintenance_failure_recovery" in upgrade
    assert "restore_maintenance_service_state" in lib
    assert "systemctl kill -s SIGKILL" not in lib
    assert 'systemctl disable --now "$PYCLUSTER_UPGRADE_PATH_NAME"' in lib
    assert 'rm -f "$PYCLUSTER_SYSTEMD_DIR/$PYCLUSTER_UPGRADE_SERVICE_NAME"' in uninstall
    assert 'rm -f "$PYCLUSTER_SYSTEMD_DIR/$PYCLUSTER_UPGRADE_PATH_NAME"' in uninstall


def test_nginx_setup_validates_hosts_and_rolls_back_failed_changes() -> None:
    setup = Path("/home/jdlewis/GitHub/pyCluster/deploy/setup-nginx.sh").read_text(encoding="utf-8")

    assert "validate_host_set" in setup
    assert "rollback_nginx_setup" in setup
    assert "trap rollback_nginx_setup ERR EXIT INT TERM" in setup
    assert setup.index("backup_nginx_target") < setup.index('rm -f "$NGINX_CONFIG_DIR/default.conf"')
    assert setup.rindex("nginx -t") < setup.rindex("trap - ERR EXIT INT TERM")
