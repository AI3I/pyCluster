#!/usr/bin/env bash
set -euo pipefail

PYCLUSTER_USER="${PYCLUSTER_USER:-pycluster}"
PYCLUSTER_GROUP="${PYCLUSTER_GROUP:-$PYCLUSTER_USER}"
PYCLUSTER_HOME="${PYCLUSTER_HOME:-/home/$PYCLUSTER_USER}"
PYCLUSTER_APP_DIR="${PYCLUSTER_APP_DIR:-$PYCLUSTER_HOME/pyCluster}"
PYCLUSTER_SERVICE_NAME="${PYCLUSTER_SERVICE_NAME:-pycluster.service}"
PYCLUSTER_WEB_SERVICE_NAME="${PYCLUSTER_WEB_SERVICE_NAME:-pyclusterweb.service}"
PYCLUSTER_CTY_REFRESH_SERVICE_NAME="${PYCLUSTER_CTY_REFRESH_SERVICE_NAME:-pycluster-cty-refresh.service}"
PYCLUSTER_CTY_REFRESH_TIMER_NAME="${PYCLUSTER_CTY_REFRESH_TIMER_NAME:-pycluster-cty-refresh.timer}"
PYCLUSTER_SYSTEMD_DIR="${PYCLUSTER_SYSTEMD_DIR:-/etc/systemd/system}"
PYCLUSTER_CONFIG_SRC="${PYCLUSTER_CONFIG_SRC:-config/pycluster.toml}"
PYCLUSTER_CONFIG_DEST="${PYCLUSTER_CONFIG_DEST:-$PYCLUSTER_APP_DIR/config/pycluster.toml}"

repo_root() {
  local src
  src="${BASH_SOURCE[0]}"
  while [ -h "$src" ]; do
    src="$(readlink "$src")"
  done
  cd "$(dirname "$src")/.." && pwd
}

log() {
  printf '[pycluster] %s\n' "$*"
}

die() {
  printf '[pycluster] ERROR: %s\n' "$*" >&2
  exit 1
}

require_root() {
  [ "$(id -u)" -eq 0 ] || die "run as root"
}

ensure_group() {
  getent group "$PYCLUSTER_GROUP" >/dev/null || groupadd --system "$PYCLUSTER_GROUP"
}

ensure_user() {
  if ! id -u "$PYCLUSTER_USER" >/dev/null 2>&1; then
    useradd \
      --system \
      --create-home \
      --home-dir "$PYCLUSTER_HOME" \
      --gid "$PYCLUSTER_GROUP" \
      --shell /bin/bash \
      "$PYCLUSTER_USER"
  fi
}

ensure_layout() {
  install -d -o "$PYCLUSTER_USER" -g "$PYCLUSTER_GROUP" "$PYCLUSTER_APP_DIR"
  install -d -o "$PYCLUSTER_USER" -g "$PYCLUSTER_GROUP" "$PYCLUSTER_APP_DIR/data"
  install -d -o "$PYCLUSTER_USER" -g "$PYCLUSTER_GROUP" "$PYCLUSTER_APP_DIR/logs"
  install -d -o "$PYCLUSTER_USER" -g "$PYCLUSTER_GROUP" "$PYCLUSTER_APP_DIR/config"
}

sync_tree() {
  local root
  root="$(repo_root)"
  rsync -a \
    --delete \
    --exclude '.git/' \
    --exclude '.pytest_cache/' \
    --exclude '__pycache__/' \
    --exclude '*.pyc' \
    "$root"/ "$PYCLUSTER_APP_DIR"/
  chown -R "$PYCLUSTER_USER:$PYCLUSTER_GROUP" "$PYCLUSTER_APP_DIR"
}

install_config_if_missing() {
  local root
  root="$(repo_root)"
  if [ ! -f "$PYCLUSTER_CONFIG_DEST" ]; then
    install -o "$PYCLUSTER_USER" -g "$PYCLUSTER_GROUP" -m 0640 \
      "$root/$PYCLUSTER_CONFIG_SRC" "$PYCLUSTER_CONFIG_DEST"
  fi
}

install_or_refresh_service() {
  local root
  root="$(repo_root)"
  install -o root -g root -m 0644 \
    "$root/deploy/systemd/pycluster.service" \
    "$PYCLUSTER_SYSTEMD_DIR/$PYCLUSTER_SERVICE_NAME"
  install -o root -g root -m 0644 \
    "$root/deploy/systemd/pyclusterweb.service" \
    "$PYCLUSTER_SYSTEMD_DIR/$PYCLUSTER_WEB_SERVICE_NAME"
  install -o root -g root -m 0644 \
    "$root/deploy/systemd/pycluster-cty-refresh.service" \
    "$PYCLUSTER_SYSTEMD_DIR/$PYCLUSTER_CTY_REFRESH_SERVICE_NAME"
  install -o root -g root -m 0644 \
    "$root/deploy/systemd/pycluster-cty-refresh.timer" \
    "$PYCLUSTER_SYSTEMD_DIR/$PYCLUSTER_CTY_REFRESH_TIMER_NAME"
  systemctl daemon-reload
}

service_is_active() {
  systemctl is-active --quiet "$PYCLUSTER_SERVICE_NAME"
}

web_service_is_active() {
  systemctl is-active --quiet "$PYCLUSTER_WEB_SERVICE_NAME"
}

restart_service_hard() {
  if service_is_active; then
    systemctl kill -s SIGKILL "$PYCLUSTER_SERVICE_NAME" || true
    sleep 1
  fi
  systemctl start "$PYCLUSTER_SERVICE_NAME"
}

restart_web_service_hard() {
  if web_service_is_active; then
    systemctl kill -s SIGKILL "$PYCLUSTER_WEB_SERVICE_NAME" || true
    sleep 1
  fi
  systemctl start "$PYCLUSTER_WEB_SERVICE_NAME"
}

enable_service() {
  systemctl enable "$PYCLUSTER_SERVICE_NAME" >/dev/null
  systemctl enable "$PYCLUSTER_WEB_SERVICE_NAME" >/dev/null
  systemctl enable --now "$PYCLUSTER_CTY_REFRESH_TIMER_NAME" >/dev/null
}

disable_service() {
  systemctl disable "$PYCLUSTER_SERVICE_NAME" >/dev/null 2>&1 || true
  systemctl disable "$PYCLUSTER_WEB_SERVICE_NAME" >/dev/null 2>&1 || true
  systemctl disable --now "$PYCLUSTER_CTY_REFRESH_TIMER_NAME" >/dev/null 2>&1 || true
}

stop_service() {
  systemctl stop "$PYCLUSTER_SERVICE_NAME" >/dev/null 2>&1 || true
  systemctl stop "$PYCLUSTER_WEB_SERVICE_NAME" >/dev/null 2>&1 || true
  systemctl stop "$PYCLUSTER_CTY_REFRESH_TIMER_NAME" >/dev/null 2>&1 || true
}

refresh_cty_best_effort() {
  local cmd
  cmd=(/usr/bin/python3 "$PYCLUSTER_APP_DIR/scripts/update_cty.py" --config "$PYCLUSTER_CONFIG_DEST")
  if cd "$PYCLUSTER_APP_DIR" && runuser -u "$PYCLUSTER_USER" -- "${cmd[@]}"; then
    log "CTY.DAT refresh succeeded"
  else
    log "CTY.DAT refresh skipped or failed; keeping bundled copy"
  fi
}
