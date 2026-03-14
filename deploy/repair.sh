#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=deploy/lib.sh
. "$SCRIPT_DIR/lib.sh"

require_root
log "repairing pyCluster deployment in $PYCLUSTER_APP_DIR"
ensure_group
ensure_user
ensure_layout
sync_tree
install_config_if_missing
install_or_refresh_service
enable_service
restart_service_hard
restart_web_service_hard
systemctl is-active --quiet "$PYCLUSTER_SERVICE_NAME" || die "service failed to recover"
systemctl is-active --quiet "$PYCLUSTER_WEB_SERVICE_NAME" || die "web service failed to recover"
log "repair complete"
