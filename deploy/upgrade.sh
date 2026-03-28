#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=deploy/lib.sh
. "$SCRIPT_DIR/lib.sh"

require_root
ensure_base_packages
ensure_supported_python
log "upgrading pyCluster in $PYCLUSTER_APP_DIR"
ensure_group
ensure_user
ensure_layout
sync_tree
install_config_if_missing
install_optional_config_if_missing "config/strings.toml" "strings.toml"
ensure_selinux_contexts
install_or_refresh_service
ensure_fail2ban_packages
install_or_refresh_fail2ban
install_or_refresh_logrotate
enable_service
run_upgrade_1_0_1
refresh_cty_best_effort
restart_service_hard
restart_web_service_hard
enable_fail2ban_service
apply_imported_fail2ban_badips
wait_for_systemd_active "$PYCLUSTER_SERVICE_NAME" 45 || die "service failed to restart"
wait_for_systemd_active "$PYCLUSTER_WEB_SERVICE_NAME" 45 || die "web service failed to restart"
log "upgrade complete"
