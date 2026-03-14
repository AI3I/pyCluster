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

cty_timer_state="missing"
if systemctl list-unit-files "$PYCLUSTER_CTY_REFRESH_TIMER_NAME" >/dev/null 2>&1; then
  cty_timer_state="$(systemctl is-active "$PYCLUSTER_CTY_REFRESH_TIMER_NAME" 2>/dev/null || true)"
  [ -n "$cty_timer_state" ] || cty_timer_state="inactive"
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
if [ -f "$PYCLUSTER_CONFIG_DEST" ]; then
  readarray -t cfg_values < <("${PYCLUSTER_PYTHON_LINK:-/usr/bin/python3}" - <<PY
import tomllib
from pathlib import Path
p = Path("$PYCLUSTER_CONFIG_DEST")
cfg = tomllib.loads(p.read_text(encoding="utf-8"))
print(cfg.get("store", {}).get("sqlite_path", ""))
print(cfg.get("public_web", {}).get("cty_dat_path", ""))
PY
)
  db_path="${cfg_values[0]:-}"
  cty_path="${cfg_values[1]:-}"
fi

if [ -n "$db_path" ] && [ "${db_path#/}" = "$db_path" ]; then
  db_path="$PYCLUSTER_APP_DIR/${db_path#./}"
fi

if [ -n "$cty_path" ] && [ "${cty_path#/}" = "$cty_path" ]; then
  cty_path="$PYCLUSTER_APP_DIR/${cty_path#./}"
fi

db_ok="no"
[ -n "$db_path" ] && [ -f "$db_path" ] && db_ok="yes"

cty_ok="no"
[ -n "$cty_path" ] && [ -f "$cty_path" ] && cty_ok="yes"

sysop_bootstrap="no"
[ -f "$PYCLUSTER_SYSOP_BOOTSTRAP_NOTE" ] && sysop_bootstrap="yes"

api_stats="unavailable"
if [ "$service_state" = "active" ]; then
  api_stats="$(curl -fsS http://127.0.0.1:8080/api/stats 2>/dev/null || printf 'unavailable')"
fi

public_branding="unavailable"
if [ "$web_service_state" = "active" ]; then
  public_branding="$(curl -fsS http://127.0.0.1:8081/api/public/branding 2>/dev/null || printf 'unavailable')"
fi

status "user" "$PYCLUSTER_USER ($app_user_ok)"
status "app dir" "$PYCLUSTER_APP_DIR"
status "config" "$PYCLUSTER_CONFIG_DEST ($config_ok)"
status "database" "${db_path:-unset} ($db_ok)"
status "cty.dat" "${cty_path:-unset} ($cty_ok)"
status "core service" "$PYCLUSTER_SERVICE_NAME ($service_state)"
status "web service" "$PYCLUSTER_WEB_SERVICE_NAME ($web_service_state)"
status "cty timer" "$PYCLUSTER_CTY_REFRESH_TIMER_NAME ($cty_timer_state)"
status "fail2ban" "fail2ban.service ($fail2ban_state)"
status "selinux" "$selinux_state"
status "sysop bootstrap" "$PYCLUSTER_SYSOP_BOOTSTRAP_NOTE ($sysop_bootstrap)"
status "api stats" "$api_stats"
status "public branding" "$public_branding"
