#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=deploy/lib.sh
. "$SCRIPT_DIR/lib.sh"

status() {
  printf '%-24s %s\n' "$1" "$2"
}

selinux_state="unavailable"
if command -v getenforce >/dev/null 2>&1; then
  selinux_state="$(getenforce 2>/dev/null || printf 'unknown')"
fi

app_user_ok="no"
id -u "$PYCLUSTER_USER" >/dev/null 2>&1 && app_user_ok="yes"

service_state="missing"
if systemctl list-unit-files "$PYCLUSTER_SERVICE_NAME" >/dev/null 2>&1; then
  service_state="$(systemctl is-active "$PYCLUSTER_SERVICE_NAME" 2>/dev/null || true)"
  [ -n "$service_state" ] || service_state="inactive"
fi

web_service_state="missing"
if systemctl list-unit-files "$PYCLUSTER_WEB_SERVICE_NAME" >/dev/null 2>&1; then
  web_service_state="$(systemctl is-active "$PYCLUSTER_WEB_SERVICE_NAME" 2>/dev/null || true)"
  [ -n "$web_service_state" ] || web_service_state="inactive"
fi

data_timer_state="missing"
if systemctl list-unit-files "$PYCLUSTER_DATA_REFRESH_TIMER_NAME" >/dev/null 2>&1; then
  data_timer_state="$(systemctl is-active "$PYCLUSTER_DATA_REFRESH_TIMER_NAME" 2>/dev/null || true)"
  [ -n "$data_timer_state" ] || data_timer_state="inactive"
elif systemctl list-unit-files "$PYCLUSTER_LEGACY_CTY_REFRESH_TIMER_NAME" >/dev/null 2>&1; then
  data_timer_state="$(systemctl is-active "$PYCLUSTER_LEGACY_CTY_REFRESH_TIMER_NAME" 2>/dev/null || true)"
  [ -n "$data_timer_state" ] || data_timer_state="inactive"
  data_timer_state="$data_timer_state (legacy $PYCLUSTER_LEGACY_CTY_REFRESH_TIMER_NAME)"
fi

retention_timer_state="missing"
if systemctl list-unit-files "$PYCLUSTER_RETENTION_TIMER_NAME" >/dev/null 2>&1; then
  retention_timer_state="$(systemctl is-active "$PYCLUSTER_RETENTION_TIMER_NAME" 2>/dev/null || true)"
  [ -n "$retention_timer_state" ] || retention_timer_state="inactive"
fi

upgrade_path_state="missing"
if systemctl list-unit-files "$PYCLUSTER_UPGRADE_PATH_NAME" >/dev/null 2>&1; then
  upgrade_path_state="$(systemctl is-active "$PYCLUSTER_UPGRADE_PATH_NAME" 2>/dev/null || true)"
  [ -n "$upgrade_path_state" ] || upgrade_path_state="inactive"
fi

fail2ban_state="missing"
if systemctl list-unit-files fail2ban.service >/dev/null 2>&1; then
  fail2ban_state="$(systemctl is-active fail2ban.service 2>/dev/null || true)"
  [ -n "$fail2ban_state" ] || fail2ban_state="inactive"
fi

config_ok="no"
[ -f "$PYCLUSTER_CONFIG_DEST" ] && config_ok="yes"

db_path=""
cty_path=""
wpx_path=""
wpx_note=""
keps_path=""
public_web_port="8081"
if [ -f "$PYCLUSTER_CONFIG_DEST" ]; then
  cfg_output="$(
    cd "$PYCLUSTER_APP_DIR" &&
    PYTHONPATH=src "${PYCLUSTER_PYTHON_LINK:-/usr/bin/python3}" - "$PYCLUSTER_CONFIG_DEST" <<'PY'
import sys
from pycluster.config import load_config

cfg = load_config(sys.argv[1])
print(cfg.store.sqlite_path)
print(cfg.public_web.cty_dat_path)
print(cfg.public_web.wpxloc_raw_path)
print(cfg.satellite.keps_path)
print(cfg.public_web.port)
PY
  )" || config_ok="invalid"
  if [ "$config_ok" = "yes" ]; then
    readarray -t cfg_values <<<"$cfg_output"
    db_path="${cfg_values[0]:-}"
    cty_path="${cfg_values[1]:-}"
    wpx_path="${cfg_values[2]:-}"
    keps_path="${cfg_values[3]:-}"
    public_web_port="${cfg_values[4]:-8081}"
  fi
fi

if [ -n "$db_path" ] && [ "${db_path#/}" = "$db_path" ]; then
  db_path="$PYCLUSTER_APP_DIR/${db_path#./}"
fi

if [ -n "$cty_path" ] && [ "${cty_path#/}" = "$cty_path" ]; then
  cty_path="$PYCLUSTER_APP_DIR/${cty_path#./}"
fi

if [ -z "$wpx_path" ] && [ -n "$cty_path" ]; then
  wpx_path="$(dirname "$cty_path")/wpxloc.raw"
  wpx_note="derived from cty.dat sibling path"
fi

if [ -n "$wpx_path" ] && [ "${wpx_path#/}" = "$wpx_path" ]; then
  wpx_path="$PYCLUSTER_APP_DIR/${wpx_path#./}"
fi

if [ -n "$keps_path" ] && [ "${keps_path#/}" = "$keps_path" ]; then
  keps_path="$PYCLUSTER_APP_DIR/${keps_path#./}"
fi

db_ok="no"
[ -n "$db_path" ] && [ -f "$db_path" ] && db_ok="yes"

cty_ok="no"
[ -n "$cty_path" ] && [ -f "$cty_path" ] && cty_ok="yes"

wpx_ok="no"
[ -n "$wpx_path" ] && [ -f "$wpx_path" ] && wpx_ok="yes"

keps_ok="no"
keps_age="unknown"
if [ -n "$keps_path" ] && [ -f "$keps_path" ]; then
  keps_ok="yes"
  keps_age="$(( ($(date +%s) - $(stat -c %Y "$keps_path")) / 86400 ))d old"
fi

sysop_bootstrap="no"
[ -f "$PYCLUSTER_SYSOP_BOOTSTRAP_NOTE" ] && sysop_bootstrap="yes"

api_stats="unavailable"
if [ "$web_service_state" = "active" ]; then
  api_stats="$(curl -fsS "http://127.0.0.1:${public_web_port}/api/stats?hours=24" 2>/dev/null || printf 'unavailable')"
fi

public_branding="unavailable"
if [ "$web_service_state" = "active" ]; then
  public_branding="$(curl -fsS "http://127.0.0.1:${public_web_port}/api/public/branding" 2>/dev/null || printf 'unavailable')"
fi

status "user" "$PYCLUSTER_USER ($app_user_ok)"
status "app dir" "$PYCLUSTER_APP_DIR"
status "config" "$PYCLUSTER_CONFIG_DEST ($config_ok)"
status "database" "${db_path:-unset} ($db_ok)"
status "cty.dat" "${cty_path:-unset} ($cty_ok)"
status "wpxloc.raw" "${wpx_path:-unset} ($wpx_ok)${wpx_note:+ [$wpx_note]}"
status "keps" "${keps_path:-unset} ($keps_ok, $keps_age)"
status "core service" "$PYCLUSTER_SERVICE_NAME ($service_state)"
status "web service" "$PYCLUSTER_WEB_SERVICE_NAME ($web_service_state)"
status "data refresh timer" "$PYCLUSTER_DATA_REFRESH_TIMER_NAME ($data_timer_state)"
status "retention timer" "$PYCLUSTER_RETENTION_TIMER_NAME ($retention_timer_state)"
status "upgrade watcher" "$PYCLUSTER_UPGRADE_PATH_NAME ($upgrade_path_state)"
status "fail2ban" "fail2ban.service ($fail2ban_state)"
status "selinux" "$selinux_state"
status "sysop bootstrap" "$PYCLUSTER_SYSOP_BOOTSTRAP_NOTE ($sysop_bootstrap)"
status "api stats" "$api_stats"
status "public branding" "$public_branding"
