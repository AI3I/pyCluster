#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=deploy/lib.sh
. "$SCRIPT_DIR/lib.sh"

require_root

KEEP_DATA="${KEEP_DATA:-1}"
KEEP_CONFIG="${KEEP_CONFIG:-1}"

log "uninstalling pyCluster from $PYCLUSTER_APP_DIR"
arm_maintenance_failure_recovery
stop_service
backup_runtime_snapshot uninstall-preflight
disarm_maintenance_failure_recovery
disable_service
rm -f "$PYCLUSTER_SYSTEMD_DIR/$PYCLUSTER_SERVICE_NAME"
rm -f "$PYCLUSTER_SYSTEMD_DIR/$PYCLUSTER_WEB_SERVICE_NAME"
rm -f "$PYCLUSTER_SYSTEMD_DIR/$PYCLUSTER_DATA_REFRESH_SERVICE_NAME"
rm -f "$PYCLUSTER_SYSTEMD_DIR/$PYCLUSTER_DATA_REFRESH_TIMER_NAME"
rm -f "$PYCLUSTER_SYSTEMD_DIR/$PYCLUSTER_LEGACY_CTY_REFRESH_SERVICE_NAME"
rm -f "$PYCLUSTER_SYSTEMD_DIR/$PYCLUSTER_LEGACY_CTY_REFRESH_TIMER_NAME"
rm -f "$PYCLUSTER_SYSTEMD_DIR/$PYCLUSTER_RETENTION_SERVICE_NAME"
rm -f "$PYCLUSTER_SYSTEMD_DIR/$PYCLUSTER_RETENTION_TIMER_NAME"
rm -f "$PYCLUSTER_SYSTEMD_DIR/$PYCLUSTER_REGISTRATION_REMINDERS_SERVICE_NAME"
rm -f "$PYCLUSTER_SYSTEMD_DIR/$PYCLUSTER_REGISTRATION_REMINDERS_TIMER_NAME"
rm -f "$PYCLUSTER_SYSTEMD_DIR/$PYCLUSTER_UPGRADE_SERVICE_NAME"
rm -f "$PYCLUSTER_SYSTEMD_DIR/$PYCLUSTER_UPGRADE_PATH_NAME"
rm -f "$PYCLUSTER_FAIL2BAN_DIR/filter.d/pycluster-auth-core.conf"
rm -f "$PYCLUSTER_FAIL2BAN_DIR/filter.d/pycluster-auth-web.conf"
rm -f "$PYCLUSTER_FAIL2BAN_DIR/filter.d/pycluster-auth-telnet.conf"
rm -f "$PYCLUSTER_FAIL2BAN_DIR/filter.d/pycluster-auth-scanner.conf"
rm -f "$PYCLUSTER_FAIL2BAN_DIR/action.d/pycluster-lock-account.conf"
rm -f "$PYCLUSTER_FAIL2BAN_DIR/jail.d/pycluster-core.local"
rm -f "$PYCLUSTER_FAIL2BAN_DIR/jail.d/pycluster-web.local"
rm -f "$PYCLUSTER_FAIL2BAN_DIR/jail.d/pycluster-telnet.local"
rm -f "$PYCLUSTER_FAIL2BAN_DIR/jail.d/pycluster-scanner.local"
rm -f "$PYCLUSTER_FAIL2BAN_DIR/jail.d/pycluster-disable-defaults.local"
rm -f "$PYCLUSTER_LOGROTATE_DIR/pycluster"
rm -f "$PYCLUSTER_SYSOP_BOOTSTRAP_NOTE"
rm -f "$PYCLUSTER_FAIL2BAN_BADIP_STATE"
systemctl daemon-reload
systemctl restart fail2ban >/dev/null 2>&1 || true

if [ -d "$PYCLUSTER_APP_DIR" ]; then
  if [ "$KEEP_DATA" = "1" ] || [ "$KEEP_CONFIG" = "1" ]; then
    tmp_keep="$(mktemp -d)"
    if [ "$KEEP_DATA" = "1" ] && [ -d "$PYCLUSTER_APP_DIR/data" ]; then
      mv "$PYCLUSTER_APP_DIR/data" "$tmp_keep/data"
    fi
    if [ "$KEEP_CONFIG" = "1" ] && [ -d "$PYCLUSTER_APP_DIR/config" ]; then
      mv "$PYCLUSTER_APP_DIR/config" "$tmp_keep/config"
    fi
    rm -rf "$PYCLUSTER_APP_DIR"
    install -d -o "$PYCLUSTER_USER" -g "$PYCLUSTER_GROUP" "$PYCLUSTER_APP_DIR"
    if [ -d "$tmp_keep/data" ]; then
      mv "$tmp_keep/data" "$PYCLUSTER_APP_DIR/data"
    fi
    if [ -d "$tmp_keep/config" ]; then
      mv "$tmp_keep/config" "$PYCLUSTER_APP_DIR/config"
    fi
    chown -R "$PYCLUSTER_USER:$PYCLUSTER_GROUP" "$PYCLUSTER_APP_DIR"
    rmdir "$tmp_keep" 2>/dev/null || true
  else
    rm -rf "$PYCLUSTER_APP_DIR"
  fi
fi

log "uninstall complete (KEEP_DATA=$KEEP_DATA KEEP_CONFIG=$KEEP_CONFIG)"
