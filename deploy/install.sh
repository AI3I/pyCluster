#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=deploy/lib.sh
. "$SCRIPT_DIR/lib.sh"

require_root
ensure_base_packages
ensure_supported_python
refresh_source_tags_best_effort
log "installing pyCluster into $PYCLUSTER_APP_DIR"
arm_maintenance_failure_recovery
stop_service
backup_runtime_snapshot install-preflight
ensure_group
ensure_user
ensure_layout
sync_tree
ensure_selinux_contexts
install_config_if_missing
normalize_country_data_config_paths
install_optional_config_if_missing "config/strings.toml" "strings.toml"
install_optional_config_if_missing "config/pycluster.local.toml.example" "pycluster.local.toml.example"
apply_py_protocol_defaults
install_or_refresh_service
ensure_fail2ban_packages
install_or_refresh_fail2ban
install_or_refresh_logrotate
enable_service
refresh_runtime_data_best_effort
bootstrap_sysop_account
show_sysop_bootstrap_note
restart_service_hard
restart_web_service_hard
enable_fail2ban_service
apply_imported_fail2ban_badips
wait_for_systemd_active "$PYCLUSTER_SERVICE_NAME" 45 || die "service failed to start"
wait_for_systemd_active "$PYCLUSTER_WEB_SERVICE_NAME" 45 || die "web service failed to start"
wait_for_runtime_ready 45 || { report_runtime_failure; die "configured runtime listeners failed health verification"; }
maybe_run_setup_nginx
write_deployment_state install
disarm_maintenance_failure_recovery
log "install complete"
