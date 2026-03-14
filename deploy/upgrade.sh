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
ensure_selinux_contexts
install_or_refresh_service
ensure_fail2ban_packages
install_or_refresh_fail2ban
enable_service
refresh_cty_best_effort
restart_service_hard
restart_web_service_hard
enable_fail2ban_service
wait_for_systemd_active "$PYCLUSTER_SERVICE_NAME" 45 || die "service failed to restart"
wait_for_systemd_active "$PYCLUSTER_WEB_SERVICE_NAME" 45 || die "web service failed to restart"
log "upgrade complete"
