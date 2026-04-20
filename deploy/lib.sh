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
PYCLUSTER_RETENTION_SERVICE_NAME="${PYCLUSTER_RETENTION_SERVICE_NAME:-pycluster-retention.service}"
PYCLUSTER_RETENTION_TIMER_NAME="${PYCLUSTER_RETENTION_TIMER_NAME:-pycluster-retention.timer}"
PYCLUSTER_UPGRADE_SERVICE_NAME="${PYCLUSTER_UPGRADE_SERVICE_NAME:-pycluster-upgrade.service}"
PYCLUSTER_UPGRADE_PATH_NAME="${PYCLUSTER_UPGRADE_PATH_NAME:-pycluster-upgrade.path}"
PYCLUSTER_SYSTEMD_DIR="${PYCLUSTER_SYSTEMD_DIR:-/etc/systemd/system}"
PYCLUSTER_CONFIG_SRC="${PYCLUSTER_CONFIG_SRC:-config/pycluster.toml}"
PYCLUSTER_CONFIG_DEST="${PYCLUSTER_CONFIG_DEST:-$PYCLUSTER_APP_DIR/config/pycluster.toml}"
PYCLUSTER_PKG_AUTO_INSTALL="${PYCLUSTER_PKG_AUTO_INSTALL:-1}"
PYCLUSTER_PYTHON_LINK="${PYCLUSTER_PYTHON_LINK:-/usr/local/bin/pycluster-python}"
PYCLUSTER_FAIL2BAN_DIR="${PYCLUSTER_FAIL2BAN_DIR:-/etc/fail2ban}"
PYCLUSTER_LOGROTATE_DIR="${PYCLUSTER_LOGROTATE_DIR:-/etc/logrotate.d}"
PYCLUSTER_FAIL2BAN_BADIP_LIST="${PYCLUSTER_FAIL2BAN_BADIP_LIST:-$PYCLUSTER_APP_DIR/config/fail2ban-badip.local}"
PYCLUSTER_FAIL2BAN_BADIP_STATE="${PYCLUSTER_FAIL2BAN_BADIP_STATE:-$PYCLUSTER_APP_DIR/data/fail2ban-badip-applied.txt}"
PYCLUSTER_SYSOP_BOOTSTRAP_NOTE="${PYCLUSTER_SYSOP_BOOTSTRAP_NOTE:-/root/pycluster-initial-sysop.txt}"
PYCLUSTER_BACKUP_DIR="${PYCLUSTER_BACKUP_DIR:-/root/pycluster-backups}"
PYCLUSTER_TMP_SWAPFILE="${PYCLUSTER_TMP_SWAPFILE:-/swapfile-pycluster}"
PYCLUSTER_TMP_SWAP_MB="${PYCLUSTER_TMP_SWAP_MB:-1024}"

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

warn() {
  printf '[pycluster] WARNING: %s\n' "$*" >&2
}

die() {
  printf '[pycluster] ERROR: %s\n' "$*" >&2
  exit 1
}

timestamp_utc() {
  date -u +%Y%m%d_%H%M%S
}

is_interactive_tty() {
  [ -t 0 ] && [ -t 1 ]
}

prompt_yes_no() {
  local prompt="$1"
  local default="${2:-}"
  local answer="" hint=""
  case "$default" in
    y|Y|yes|YES) hint=" [Y/n]" ;;
    n|N|no|NO) hint=" [y/N]" ;;
    *) hint=" [y/n]" ;;
  esac
  while true; do
    printf '%s%s ' "$prompt" "$hint"
    IFS= read -r answer || return 1
    answer="${answer:-$default}"
    case "${answer,,}" in
      y|yes) return 0 ;;
      n|no) return 1 ;;
    esac
  done
}

prompt_value() {
  local prompt="$1"
  local default="${2:-}"
  local answer=""
  if [ -n "$default" ]; then
    printf '%s [%s] ' "$prompt" "$default"
  else
    printf '%s ' "$prompt"
  fi
  IFS= read -r answer || return 1
  printf '%s' "${answer:-$default}"
}

os_release_value() {
  local key="$1"
  [ -r /etc/os-release ] || return 1
  awk -F= -v key="$key" '$1 == key {gsub(/^"/, "", $2); gsub(/"$/, "", $2); print $2}' /etc/os-release
}

os_id() {
  os_release_value ID
}

os_like() {
  os_release_value ID_LIKE
}

pkg_manager() {
  if command -v apt-get >/dev/null 2>&1; then
    printf 'apt'
    return
  fi
  if command -v dnf >/dev/null 2>&1; then
    printf 'dnf'
    return
  fi
  if command -v yum >/dev/null 2>&1; then
    printf 'yum'
    return
  fi
  return 1
}

mem_total_mb() {
  awk '/MemTotal:/ {printf "%d\n", $2 / 1024}' /proc/meminfo 2>/dev/null || printf '0\n'
}

swap_total_mb() {
  awk '/SwapTotal:/ {printf "%d\n", $2 / 1024}' /proc/meminfo 2>/dev/null || printf '0\n'
}

maybe_enable_temp_swap() {
  local mem_mb swap_mb
  mem_mb="$(mem_total_mb)"
  swap_mb="$(swap_total_mb)"
  if [ "${mem_mb:-0}" -ge 1400 ] || [ "${swap_mb:-0}" -gt 0 ]; then
    return 0
  fi
  if [ -e "$PYCLUSTER_TMP_SWAPFILE" ]; then
    return 0
  fi
  if ! command -v swapon >/dev/null 2>&1 || ! command -v mkswap >/dev/null 2>&1; then
    return 0
  fi
  log "enabling temporary swap (${PYCLUSTER_TMP_SWAP_MB}MB) for package installation"
  if command -v fallocate >/dev/null 2>&1; then
    fallocate -l "${PYCLUSTER_TMP_SWAP_MB}M" "$PYCLUSTER_TMP_SWAPFILE"
  else
    dd if=/dev/zero of="$PYCLUSTER_TMP_SWAPFILE" bs=1M count="$PYCLUSTER_TMP_SWAP_MB" status=none
  fi
  chmod 600 "$PYCLUSTER_TMP_SWAPFILE"
  mkswap "$PYCLUSTER_TMP_SWAPFILE" >/dev/null
  swapon "$PYCLUSTER_TMP_SWAPFILE"
}

disable_temp_swap() {
  if [ -e "$PYCLUSTER_TMP_SWAPFILE" ]; then
    swapoff "$PYCLUSTER_TMP_SWAPFILE" >/dev/null 2>&1 || true
    rm -f "$PYCLUSTER_TMP_SWAPFILE"
  fi
}

install_packages() {
  [ "$PYCLUSTER_PKG_AUTO_INSTALL" = "1" ] || return 0
  [ "$#" -gt 0 ] || return 0
  local mgr
  mgr="$(pkg_manager)" || die "no supported package manager found"
  case "$mgr" in
    apt)
      export DEBIAN_FRONTEND=noninteractive
      apt-get update
      apt-get install -y "$@"
      ;;
    dnf)
      maybe_enable_temp_swap
      if ! dnf install -y --setopt=install_weak_deps=False "$@"; then
        disable_temp_swap
        return 1
      fi
      disable_temp_swap
      ;;
    yum)
      maybe_enable_temp_swap
      if ! yum install -y "$@"; then
        disable_temp_swap
        return 1
      fi
      disable_temp_swap
      ;;
  esac
}

ensure_base_packages() {
  local mgr
  mgr="$(pkg_manager)" || die "no supported package manager found"
  case "$mgr" in
    apt)
      install_packages rsync python3 ca-certificates curl git
      ;;
    dnf|yum)
      install_packages rsync python3 ca-certificates curl git policycoreutils
      ;;
  esac
}

python_version_ok() {
  local bin="$1"
  [ -x "$bin" ] || return 1
  "$bin" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)' >/dev/null 2>&1
}

selected_python_bin() {
  local bin
  for bin in \
    /usr/bin/python3.13 \
    /usr/bin/python3.12 \
    /usr/bin/python3.11 \
    /usr/local/bin/python3.13 \
    /usr/local/bin/python3.12 \
    /usr/local/bin/python3.11 \
    /usr/bin/python3
  do
    if python_version_ok "$bin"; then
      printf '%s' "$bin"
      return 0
    fi
  done
  return 1
}

ensure_supported_python() {
  local mgr current
  if current="$(selected_python_bin)"; then
    ln -sf "$current" "$PYCLUSTER_PYTHON_LINK"
    return 0
  fi

  mgr="$(pkg_manager)" || die "no supported package manager found"
  case "$mgr" in
    apt)
      die "Python 3.11+ is required; install a newer Python runtime on this host"
      ;;
    dnf|yum)
      install_packages python3.12 || install_packages python3.11
      ;;
  esac

  current="$(selected_python_bin)" || die "unable to locate Python 3.11+ after package install"
  ln -sf "$current" "$PYCLUSTER_PYTHON_LINK"
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
  install -d -o "$PYCLUSTER_USER" -g "$PYCLUSTER_GROUP" -m 0750 /var/log/pycluster
  touch /var/log/pycluster/authfail.log
  chown "$PYCLUSTER_USER:$PYCLUSTER_GROUP" /var/log/pycluster/authfail.log
  chmod 0640 /var/log/pycluster/authfail.log
}

ensure_runtime_ownership() {
  if [ -d "$PYCLUSTER_APP_DIR" ]; then
    chown -R "$PYCLUSTER_USER:$PYCLUSTER_GROUP" "$PYCLUSTER_APP_DIR"
  fi
}

backup_runtime_snapshot() {
  local label="$1"
  local archive parent base rel
  local -a paths=()
  [ -d "$PYCLUSTER_APP_DIR" ] || return 0
  parent="$(dirname "$PYCLUSTER_APP_DIR")"
  base="$(basename "$PYCLUSTER_APP_DIR")"
  for rel in config data logs; do
    if [ -e "$PYCLUSTER_APP_DIR/$rel" ]; then
      paths+=("$base/$rel")
    fi
  done
  [ "${#paths[@]}" -gt 0 ] || return 0
  install -d -m 0700 "$PYCLUSTER_BACKUP_DIR"
  archive="$PYCLUSTER_BACKUP_DIR/${label}_$(timestamp_utc).tar.gz"
  tar -C "$parent" -czf "$archive" "${paths[@]}"
  chmod 0600 "$archive" >/dev/null 2>&1 || true
  log "backup snapshot written to $archive"
}

ensure_selinux_contexts() {
  if command -v restorecon >/dev/null 2>&1; then
    restorecon -RF "$PYCLUSTER_HOME" >/dev/null 2>&1 || true
  fi
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
    --exclude 'config/' \
    --exclude 'data/' \
    --exclude 'logs/' \
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

install_optional_config_if_missing() {
  local relative_src="$1"
  local dest_name="$2"
  local root dest
  root="$(repo_root)"
  dest="$(dirname "$PYCLUSTER_CONFIG_DEST")/$dest_name"
  if [ ! -f "$dest" ] && [ -f "$root/$relative_src" ]; then
    install -o "$PYCLUSTER_USER" -g "$PYCLUSTER_GROUP" -m 0640 \
      "$root/$relative_src" "$dest"
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
  install -o root -g root -m 0644 \
    "$root/deploy/systemd/pycluster-retention.service" \
    "$PYCLUSTER_SYSTEMD_DIR/$PYCLUSTER_RETENTION_SERVICE_NAME"
  install -o root -g root -m 0644 \
    "$root/deploy/systemd/pycluster-retention.timer" \
    "$PYCLUSTER_SYSTEMD_DIR/$PYCLUSTER_RETENTION_TIMER_NAME"
  install -o root -g root -m 0644 \
    "$root/deploy/systemd/pycluster-upgrade.service" \
    "$PYCLUSTER_SYSTEMD_DIR/$PYCLUSTER_UPGRADE_SERVICE_NAME"
  install -o root -g root -m 0644 \
    "$root/deploy/systemd/pycluster-upgrade.path" \
    "$PYCLUSTER_SYSTEMD_DIR/$PYCLUSTER_UPGRADE_PATH_NAME"
  systemctl daemon-reload
}

service_is_active() {
  systemctl is-active --quiet "$PYCLUSTER_SERVICE_NAME"
}

web_service_is_active() {
  systemctl is-active --quiet "$PYCLUSTER_WEB_SERVICE_NAME"
}

wait_for_systemd_active() {
  local unit="$1"
  local timeout="${2:-30}"
  local start now state
  start="$(date +%s)"
  while true; do
    state="$(systemctl is-active "$unit" 2>/dev/null || true)"
    case "$state" in
      active)
        return 0
        ;;
      failed|inactive|deactivating)
        return 1
        ;;
    esac
    now="$(date +%s)"
    if [ $((now - start)) -ge "$timeout" ]; then
      return 1
    fi
    sleep 1
  done
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
  systemctl enable --now "$PYCLUSTER_RETENTION_TIMER_NAME" >/dev/null
  systemctl enable --now "$PYCLUSTER_UPGRADE_PATH_NAME" >/dev/null
}

disable_service() {
  systemctl disable "$PYCLUSTER_SERVICE_NAME" >/dev/null 2>&1 || true
  systemctl disable "$PYCLUSTER_WEB_SERVICE_NAME" >/dev/null 2>&1 || true
  systemctl disable --now "$PYCLUSTER_CTY_REFRESH_TIMER_NAME" >/dev/null 2>&1 || true
  systemctl disable --now "$PYCLUSTER_RETENTION_TIMER_NAME" >/dev/null 2>&1 || true
}

stop_service() {
  systemctl stop "$PYCLUSTER_SERVICE_NAME" >/dev/null 2>&1 || true
  systemctl stop "$PYCLUSTER_WEB_SERVICE_NAME" >/dev/null 2>&1 || true
  systemctl stop "$PYCLUSTER_CTY_REFRESH_TIMER_NAME" >/dev/null 2>&1 || true
  systemctl stop "$PYCLUSTER_RETENTION_TIMER_NAME" >/dev/null 2>&1 || true
}

refresh_cty_best_effort() {
  local cmd
  cmd=("$PYCLUSTER_PYTHON_LINK" "$PYCLUSTER_APP_DIR/scripts/update_cty.py" --config "$PYCLUSTER_CONFIG_DEST")
  if cd "$PYCLUSTER_APP_DIR" && runuser -u "$PYCLUSTER_USER" -- "${cmd[@]}"; then
    log "Country data refresh succeeded"
  else
    log "Country data refresh skipped or failed; keeping existing local copies"
  fi
}

install_or_refresh_fail2ban() {
  local root
  root="$(repo_root)"
  install -d -m 0755 "$PYCLUSTER_FAIL2BAN_DIR/filter.d" "$PYCLUSTER_FAIL2BAN_DIR/jail.d"
  install -o root -g root -m 0644 \
    "$root/deploy/fail2ban/filter.d/pycluster-auth-core.conf" \
    "$PYCLUSTER_FAIL2BAN_DIR/filter.d/pycluster-auth-core.conf"
  install -o root -g root -m 0644 \
    "$root/deploy/fail2ban/filter.d/pycluster-auth-web.conf" \
    "$PYCLUSTER_FAIL2BAN_DIR/filter.d/pycluster-auth-web.conf"
  install -o root -g root -m 0644 \
    "$root/deploy/fail2ban/filter.d/pycluster-auth-scanner.conf" \
    "$PYCLUSTER_FAIL2BAN_DIR/filter.d/pycluster-auth-scanner.conf"
  install -o root -g root -m 0644 \
    "$root/deploy/fail2ban/jail.d/pycluster-core.local" \
    "$PYCLUSTER_FAIL2BAN_DIR/jail.d/pycluster-core.local"
  install -o root -g root -m 0644 \
    "$root/deploy/fail2ban/jail.d/pycluster-web.local" \
    "$PYCLUSTER_FAIL2BAN_DIR/jail.d/pycluster-web.local"
  install -o root -g root -m 0644 \
    "$root/deploy/fail2ban/jail.d/pycluster-scanner.local" \
    "$PYCLUSTER_FAIL2BAN_DIR/jail.d/pycluster-scanner.local"
  cat >"$PYCLUSTER_FAIL2BAN_DIR/jail.d/pycluster-disable-defaults.local" <<'EOF'
[sshd]
enabled = false
EOF
}

install_or_refresh_logrotate() {
  local root
  root="$(repo_root)"
  install -d -m 0755 "$PYCLUSTER_LOGROTATE_DIR"
  if [ ! -f "$root/deploy/logrotate/pycluster" ]; then
    echo "[pycluster] warning: logrotate template is missing; leaving existing logrotate config in place" >&2
    return 0
  fi
  install -o root -g root -m 0644 \
    "$root/deploy/logrotate/pycluster" \
    "$PYCLUSTER_LOGROTATE_DIR/pycluster"
}

apply_imported_fail2ban_badips() {
  local client current prev tmp entry jail
  client="/usr/bin/fail2ban-client"
  if [ ! -x "$client" ]; then
    return 0
  fi
  if ! systemctl list-unit-files fail2ban.service >/dev/null 2>&1; then
    return 0
  fi
  install -d -o "$PYCLUSTER_USER" -g "$PYCLUSTER_GROUP" "$PYCLUSTER_APP_DIR/data"
  tmp="$(mktemp)"
  if [ -f "$PYCLUSTER_FAIL2BAN_BADIP_LIST" ]; then
    grep -v '^[[:space:]]*#' "$PYCLUSTER_FAIL2BAN_BADIP_LIST" | sed '/^[[:space:]]*$/d' | sort -u >"$tmp" || true
  else
    : >"$tmp"
  fi
  if [ -f "$PYCLUSTER_FAIL2BAN_BADIP_STATE" ]; then
    prev="$(mktemp)"
    sort -u "$PYCLUSTER_FAIL2BAN_BADIP_STATE" >"$prev" || true
  else
    prev="$(mktemp)"
    : >"$prev"
  fi
  for entry in $(comm -23 "$prev" "$tmp"); do
    for jail in pycluster-core-auth pycluster-web-auth pycluster-telnet-scanner; do
      "$client" set "$jail" unbanip "$entry" >/dev/null 2>&1 || true
    done
  done
  for entry in $(comm -13 "$prev" "$tmp"); do
    for jail in pycluster-core-auth pycluster-web-auth pycluster-telnet-scanner; do
      "$client" set "$jail" banip "$entry" >/dev/null 2>&1 || true
    done
  done
  install -d -o "$PYCLUSTER_USER" -g "$PYCLUSTER_GROUP" "$(dirname "$PYCLUSTER_FAIL2BAN_BADIP_STATE")"
  install -o "$PYCLUSTER_USER" -g "$PYCLUSTER_GROUP" -m 0640 "$tmp" "$PYCLUSTER_FAIL2BAN_BADIP_STATE"
  rm -f "$tmp" "$prev"
}

ensure_fail2ban_packages() {
  local mgr
  mgr="$(pkg_manager)" || die "no supported package manager found"
  case "$mgr" in
    apt)
      install_packages fail2ban
      ;;
    dnf|yum)
      if [ "$(os_id)" != "fedora" ]; then
        install_packages epel-release || true
      fi
      install_packages fail2ban
      ;;
  esac
}

enable_fail2ban_service() {
  if systemctl list-unit-files fail2ban.service >/dev/null 2>&1; then
    systemctl enable fail2ban >/dev/null 2>&1 || true
    systemctl restart fail2ban
  fi
}

bootstrap_sysop_account() {
  if (
    cd "$PYCLUSTER_APP_DIR" &&
    PYTHONPATH=src "$PYCLUSTER_PYTHON_LINK" scripts/bootstrap_sysop.py \
      --config "$PYCLUSTER_CONFIG_DEST" \
      --output "$PYCLUSTER_SYSOP_BOOTSTRAP_NOTE"
  ); then
    ensure_runtime_ownership
    chmod 0600 "$PYCLUSTER_SYSOP_BOOTSTRAP_NOTE" >/dev/null 2>&1 || true
    log "SYSOP bootstrap note available at $PYCLUSTER_SYSOP_BOOTSTRAP_NOTE"
  else
    die "failed to seed SYSOP bootstrap account"
  fi
}

run_upgrade_1_0_1() {
  local root strings_template
  root="$(repo_root)"
  strings_template="$root/config/strings.toml"
  (
    cd "$PYCLUSTER_APP_DIR" &&
    PYTHONPATH=src "$PYCLUSTER_PYTHON_LINK" scripts/upgrade_1_0_1.py \
      --config "$PYCLUSTER_CONFIG_DEST" \
      --strings-template "$strings_template"
  )
  ensure_runtime_ownership
}

run_upgrade_1_0_6() {
  (
    cd "$PYCLUSTER_APP_DIR" &&
    PYTHONPATH=src "$PYCLUSTER_PYTHON_LINK" scripts/upgrade_1_0_6.py \
      --config "$PYCLUSTER_CONFIG_DEST"
  )
  ensure_runtime_ownership
}

maybe_run_setup_nginx() {
  local root setup public_host sysop_host tls_mode email expose_sysop
  local -a cmd
  root="$(repo_root)"
  setup="$root/deploy/setup-nginx.sh"
  if [ ! -x "$setup" ]; then
    warn "setup-nginx.sh is missing or not executable; leaving web listeners bound to localhost only"
    return 0
  fi

  if ! is_interactive_tty; then
    log "web listeners remain on localhost only (127.0.0.1:8080 and 127.0.0.1:8081) until you run deploy/setup-nginx.sh"
    return 0
  fi

  if ! prompt_yes_no "Configure nginx reverse proxy for pyCluster now?" "y"; then
    log "skipping nginx setup; sysop web stays on 127.0.0.1:8080 and public web stays on 127.0.0.1:8081"
    return 0
  fi

  public_host="$(prompt_value "Public hostname for the user web UI (for example: cluster.example.net):")"
  [ -n "$public_host" ] || die "a public hostname is required for nginx setup"

  expose_sysop="n"
  if prompt_yes_no "Expose the sysop web UI through nginx on its own hostname?" "n"; then
    expose_sysop="y"
    sysop_host="$(prompt_value "Sysop hostname (for example: sysop.example.net):")"
    [ -n "$sysop_host" ] || die "a sysop hostname is required when sysop nginx exposure is enabled"
  else
    sysop_host=""
  fi

  tls_mode="none"
  if prompt_yes_no "Configure HTTPS on ports 80/443 with nginx?" "y"; then
    if prompt_yes_no "Use Let's Encrypt for TLS certificates?" "y"; then
      tls_mode="letsencrypt"
      email="$(prompt_value "Email address for Let's Encrypt notices:")"
      [ -n "$email" ] || die "an email address is required for Let's Encrypt"
    else
      tls_mode="self-signed"
      email=""
    fi
  else
    email=""
  fi

  log "running nginx setup"
  cmd=("$setup" --public-host "$public_host" --tls-mode "$tls_mode")
  if [ -n "$sysop_host" ]; then
    cmd+=(--sysop-host "$sysop_host")
  fi
  if [ -n "$email" ]; then
    cmd+=(--email "$email")
  fi
  "${cmd[@]}"
}

show_sysop_bootstrap_note() {
  if [ ! -f "$PYCLUSTER_SYSOP_BOOTSTRAP_NOTE" ]; then
    log "SYSOP bootstrap note not found at $PYCLUSTER_SYSOP_BOOTSTRAP_NOTE"
    return
  fi

  printf '\n'
  printf '################################################################################\n'
  printf '#                                                                              #\n'
  printf '#   READ THIS NOW: INITIAL SYSOP CREDENTIALS ARE PRINTED BELOW                 #\n'
  printf '#                                                                              #\n'
  printf '#   SAVE THESE CREDENTIALS BEFORE YOU LEAVE THIS INSTALLER                     #\n'
  printf '#                                                                              #\n'
  printf '################################################################################\n'
  printf '\n'
  printf '========================================================================\n'
  printf ' pyCluster Initial System Operator Credentials\n'
  printf '========================================================================\n'
  cat "$PYCLUSTER_SYSOP_BOOTSTRAP_NOTE"
  printf '\n'
  printf 'ACTION REQUIRED: review and save this file before continuing:\n'
  printf 'Backup file: %s\n' "$PYCLUSTER_SYSOP_BOOTSTRAP_NOTE"
  printf 'Suggested command: cat %s\n' "$PYCLUSTER_SYSOP_BOOTSTRAP_NOTE"
  printf '========================================================================\n'
  printf '\n'
  if [ -t 0 ]; then
    local ack=""
    while [ "$ack" != "READ" ]; do
      printf 'Type READ after you have reviewed and saved the bootstrap credentials: '
      IFS= read -r ack
    done
    printf '\n'
  fi
}
