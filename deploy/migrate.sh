#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=deploy/lib.sh
. "$SCRIPT_DIR/lib.sh"

usage() {
  cat <<'EOF'
Usage:
  sudo ./deploy/migrate.sh --from-dxspider /spider [--config /home/pycluster/pyCluster/config/pycluster.toml] [--dry-run]

Imports the first supported DXSpider data set into pyCluster:
  - users from user_asc or user_json
  - homenode preferences
  - MOTD
  - bad-word rules
  - simple outbound peer definitions from connect/*

Notes:
  - exact IPs from badip.local are exported to config/fail2ban-badip.local and applied to the pyCluster fail2ban jails
  - CIDR entries from badip.local are reported but not applied
  - passwords are not migrated
EOF
}

DXSPIDER_SOURCE=""
CONFIG_PATH=""
DRY_RUN=0

while [ "$#" -gt 0 ]; do
  case "$1" in
    --from-dxspider|--source)
      DXSPIDER_SOURCE="${2:-}"
      shift 2
      ;;
    --config)
      CONFIG_PATH="${2:-}"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

[ -n "$DXSPIDER_SOURCE" ] || { usage; die "--from-dxspider is required"; }

require_root
ensure_base_packages
ensure_supported_python

ROOT="$(repo_root)"
PROJECT_ROOT="$ROOT"
if [ -z "$CONFIG_PATH" ]; then
  CONFIG_PATH="$PYCLUSTER_CONFIG_DEST"
fi

CMD=(
  "$PYCLUSTER_PYTHON_LINK"
  "$ROOT/scripts/migrate_dxspider.py"
  --config "$CONFIG_PATH"
  --project-root "$PROJECT_ROOT"
  --source "$DXSPIDER_SOURCE"
)
if [ "$DRY_RUN" = "1" ]; then
  CMD+=(--dry-run)
fi

log "migrating DXSpider data from $DXSPIDER_SOURCE"
(
  cd "$PROJECT_ROOT"
  PYTHONPATH="$PROJECT_ROOT/src" "${CMD[@]}"
)
if [ "$DRY_RUN" != "1" ]; then
  apply_imported_fail2ban_badips
fi
