#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=deploy/lib.sh
. "$SCRIPT_DIR/lib.sh"

PUBLIC_HOST="${PUBLIC_HOST:-_}"
PUBLIC_ALIASES="${PUBLIC_ALIASES:-}"
SYSOP_HOST="${SYSOP_HOST:-}"
SYSOP_ALIASES="${SYSOP_ALIASES:-}"
TLS_MODE="${TLS_MODE:-self-signed}"
LETSENCRYPT_EMAIL="${LETSENCRYPT_EMAIL:-}"
NGINX_CONFIG_DIR="${NGINX_CONFIG_DIR:-/etc/nginx/conf.d}"
SSL_DIR="${SSL_DIR:-/etc/ssl/pycluster}"
INTERACTIVE=0

usage() {
  cat <<EOF
Usage: sudo ./deploy/setup-nginx.sh [options]

Options:
  --public-host HOST         Public web hostname or '_' for default catch-all
  --public-aliases HOSTS     Optional comma-separated additional public hostnames
  --sysop-host HOST          Optional sysop web hostname
  --sysop-aliases HOSTS      Optional comma-separated additional sysop hostnames
  --tls-mode MODE            one of: none, self-signed, letsencrypt
  --email EMAIL              Required for letsencrypt mode
  --interactive              Prompt for nginx/TLS settings

This is the supported way to publish pyCluster with nginx on ports 80/443.
It assumes pyCluster owns that nginx deployment path on the host and fails
fast if another non-nginx service is already bound to the required ports.
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --public-host)
      PUBLIC_HOST="$2"
      shift 2
      ;;
    --public-aliases)
      PUBLIC_ALIASES="$2"
      shift 2
      ;;
    --sysop-host)
      SYSOP_HOST="$2"
      shift 2
      ;;
    --sysop-aliases)
      SYSOP_ALIASES="$2"
      shift 2
      ;;
    --tls-mode)
      TLS_MODE="$2"
      shift 2
      ;;
    --email)
      LETSENCRYPT_EMAIL="$2"
      shift 2
      ;;
    --interactive)
      INTERACTIVE=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown option: $1"
      ;;
  esac
done

require_root

if [ "$INTERACTIVE" = "1" ]; then
  if ! is_interactive_tty; then
    die "--interactive requires a TTY"
  fi
  PUBLIC_HOST="$(prompt_value "Public hostname for the user web UI (for example: cluster.example.net):" "${PUBLIC_HOST:-}")"
  [ -n "$PUBLIC_HOST" ] || die "public hostname is required"
  PUBLIC_ALIASES="$(prompt_value "Additional public hostnames/SANs (comma-separated, optional):" "${PUBLIC_ALIASES:-}")"
  if prompt_yes_no "Expose the sysop web UI through nginx on its own hostname?" "${SYSOP_HOST:+y}"; then
    SYSOP_HOST="$(prompt_value "Sysop hostname (for example: sysop.example.net):" "${SYSOP_HOST:-}")"
    [ -n "$SYSOP_HOST" ] || die "sysop hostname is required"
    SYSOP_ALIASES="$(prompt_value "Additional sysop hostnames/SANs (comma-separated, optional):" "${SYSOP_ALIASES:-}")"
  else
    SYSOP_HOST=""
    SYSOP_ALIASES=""
  fi
  if prompt_yes_no "Configure HTTPS on ports 80/443?" "y"; then
    if prompt_yes_no "Use Let's Encrypt certificates?" "y"; then
      TLS_MODE="letsencrypt"
      LETSENCRYPT_EMAIL="$(prompt_value "Email address for Let's Encrypt notices:" "${LETSENCRYPT_EMAIL:-}")"
      [ -n "$LETSENCRYPT_EMAIL" ] || die "letsencrypt mode requires an email address"
    else
      TLS_MODE="self-signed"
      LETSENCRYPT_EMAIL=""
    fi
  else
    TLS_MODE="none"
    LETSENCRYPT_EMAIL=""
  fi
fi

normalize_aliases() {
  local raw="$1"
  local out="" part trimmed
  IFS=',' read -r -a parts <<<"$raw"
  for part in "${parts[@]}"; do
    trimmed="$(printf '%s' "$part" | xargs)"
    [ -n "$trimmed" ] || continue
    if [ -n "$out" ]; then
      out="$out,$trimmed"
    else
      out="$trimmed"
    fi
  done
  printf '%s' "$out"
}

PUBLIC_ALIASES="$(normalize_aliases "$PUBLIC_ALIASES")"
SYSOP_ALIASES="$(normalize_aliases "$SYSOP_ALIASES")"

build_server_names() {
  local primary="$1"
  local aliases="$2"
  local out="$primary" part
  [ -n "$aliases" ] || {
    printf '%s' "$out"
    return
  }
  IFS=',' read -r -a parts <<<"$aliases"
  for part in "${parts[@]}"; do
    [ -n "$part" ] || continue
    case " $out " in
      *" $part "*) ;;
      *) out="$out $part" ;;
    esac
  done
  printf '%s' "$out"
}

build_host_array() {
  local primary="$1"
  local aliases="$2"
  local -a out=()
  local part
  [ "$primary" = "_" ] || out+=("$primary")
  if [ -n "$aliases" ]; then
    IFS=',' read -r -a parts <<<"$aliases"
    for part in "${parts[@]}"; do
      [ -n "$part" ] || continue
      out+=("$part")
    done
  fi
  printf '%s\n' "${out[@]}"
}

case "$TLS_MODE" in
  none|self-signed|letsencrypt) ;;
  *) die "invalid TLS mode: $TLS_MODE" ;;
esac

if [ "$TLS_MODE" = "letsencrypt" ]; then
  [ "$PUBLIC_HOST" != "_" ] || die "letsencrypt mode requires --public-host"
  [ -n "$LETSENCRYPT_EMAIL" ] || die "letsencrypt mode requires --email"
fi

ensure_base_packages
case "$(pkg_manager)" in
  apt)
    install_packages nginx openssl
    if [ "$TLS_MODE" = "letsencrypt" ]; then
      install_packages certbot python3-certbot-nginx
    fi
    ;;
  dnf|yum)
    install_packages nginx openssl
    if [ "$TLS_MODE" = "letsencrypt" ]; then
      if [ "$(os_id)" != "fedora" ]; then
        install_packages epel-release || true
      fi
      install_packages certbot python3-certbot-nginx
    fi
    ;;
esac

install -d -m 0755 "$NGINX_CONFIG_DIR"
install -d -m 0755 "$SSL_DIR"
rm -f "$NGINX_CONFIG_DIR/default.conf"

open_firewall() {
  if command -v firewall-cmd >/dev/null 2>&1 && systemctl is-active --quiet firewalld; then
    firewall-cmd --permanent --add-service=http >/dev/null || true
    firewall-cmd --permanent --add-service=https >/dev/null || true
    firewall-cmd --reload >/dev/null || true
  elif command -v ufw >/dev/null 2>&1; then
    ufw allow 'Nginx Full' >/dev/null 2>&1 || true
  fi
}

port_listener_report() {
  local want_csv="$1"
  if ! command -v ss >/dev/null 2>&1; then
    return 0
  fi
  ss -H -ltnp 2>/dev/null | awk -v want="$want_csv" '
    function wanted(port,   n,i,a) {
      n = split(want, a, ",")
      for (i = 1; i <= n; i++) {
        if (port == a[i]) return 1
      }
      return 0
    }
    {
      addr = $4
      sub(/^.*:/, "", addr)
      if (wanted(addr)) print
    }
  '
}

require_listener_ports() {
  local want_csv="$1"
  local report
  report="$(port_listener_report "$want_csv")"
  [ -n "$report" ] || return 0
  if printf '%s\n' "$report" | grep -viq nginx; then
    printf '%s\n' "$report" >&2
    die "required nginx listener ports ($want_csv) are already in use by another service; free ports 80/443 before running setup-nginx.sh"
  fi
  if ! systemctl is-active --quiet nginx; then
    printf '%s\n' "$report" >&2
    die "ports ($want_csv) appear to be owned by nginx, but nginx is not active under systemd; clean up the host web stack before running setup-nginx.sh"
  fi
}

selinux_for_nginx() {
  if command -v getenforce >/dev/null 2>&1 && [ "$(getenforce)" = "Enforcing" ]; then
    if command -v setsebool >/dev/null 2>&1; then
      setsebool -P httpd_can_network_connect 1
    fi
    if command -v restorecon >/dev/null 2>&1; then
      restorecon -RF /etc/nginx "$SSL_DIR" >/dev/null 2>&1 || true
    fi
  fi
}

write_http_config() {
  local name="$1"
  local hostnames="$2"
  local upstream_port="$3"
  local conf="$NGINX_CONFIG_DIR/pycluster-${name}.conf"
  cat > "$conf" <<EOF
map \$http_upgrade \$connection_upgrade_${name} {
    default upgrade;
    '' close;
}

server {
    listen 80;
    server_name ${hostnames};

    location / {
        proxy_pass http://127.0.0.1:${upstream_port};
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection \$connection_upgrade_${name};
    }
}
EOF
}

write_https_config() {
  local name="$1"
  local hostnames="$2"
  local upstream_port="$3"
  local cert="$4"
  local key="$5"
  local conf="$NGINX_CONFIG_DIR/pycluster-${name}.conf"
  cat > "$conf" <<EOF
map \$http_upgrade \$connection_upgrade_${name} {
    default upgrade;
    '' close;
}

server {
    listen 80;
    server_name ${hostnames};
    return 301 https://\$host\$request_uri;
}

server {
    listen 443 ssl http2;
    server_name ${hostnames};

    ssl_certificate ${cert};
    ssl_certificate_key ${key};
    ssl_session_timeout 1d;
    ssl_session_cache shared:SSL:10m;
    ssl_session_tickets off;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_prefer_server_ciphers off;

    location / {
        proxy_pass http://127.0.0.1:${upstream_port};
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto https;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection \$connection_upgrade_${name};
    }
}
EOF
}

issue_self_signed() {
  local host="$1"
  local aliases="$2"
  local prefix="$3"
  local cert="$SSL_DIR/${prefix}.crt"
  local key="$SSL_DIR/${prefix}.key"
  local san_list="" item
  if [ -z "$host" ] || [ "$host" = "_" ]; then
    host="pycluster.local"
  fi
  while IFS= read -r item; do
    [ -n "$item" ] || continue
    if [ -n "$san_list" ]; then
      san_list="${san_list},DNS:${item}"
    else
      san_list="DNS:${item}"
    fi
  done < <(build_host_array "$host" "$aliases")
  openssl req -x509 -nodes -newkey rsa:2048 -days 365 \
    -subj "/CN=${host}" \
    ${san_list:+-addext "subjectAltName = ${san_list}"} \
    -keyout "$key" -out "$cert" >/dev/null 2>&1
  chmod 0600 "$key"
  printf '%s\n%s\n' "$cert" "$key"
}

configure_site() {
  local name="$1"
  local host="$2"
  local aliases="$3"
  local port="$4"
  local cert
  local key
  local hostnames
  hostnames="$(build_server_names "$host" "$aliases")"

  case "$TLS_MODE" in
    none)
      write_http_config "$name" "$hostnames" "$port"
      ;;
    self-signed)
      mapfile -t paths < <(issue_self_signed "$host" "$aliases" "$name")
      cert="${paths[0]}"
      key="${paths[1]}"
      write_https_config "$name" "$hostnames" "$port" "$cert" "$key"
      ;;
    letsencrypt)
      write_http_config "$name" "$hostnames" "$port"
      ;;
  esac
}

configure_site public "$PUBLIC_HOST" "$PUBLIC_ALIASES" 8081
if [ -n "$SYSOP_HOST" ]; then
  configure_site sysop "$SYSOP_HOST" "$SYSOP_ALIASES" 8080
fi

if [ "$TLS_MODE" = "none" ]; then
  require_listener_ports "80"
else
  require_listener_ports "80,443"
fi

open_firewall
selinux_for_nginx
nginx -t
systemctl enable nginx >/dev/null
systemctl restart nginx

if [ "$TLS_MODE" = "letsencrypt" ]; then
  certbot_cmd=(certbot --nginx --non-interactive --agree-tos --redirect -m "$LETSENCRYPT_EMAIL")
  while IFS= read -r item; do
    [ -n "$item" ] || continue
    certbot_cmd+=(-d "$item")
  done < <(build_host_array "$PUBLIC_HOST" "$PUBLIC_ALIASES")
  "${certbot_cmd[@]}"
  if [ -n "$SYSOP_HOST" ]; then
    certbot_cmd=(certbot --nginx --non-interactive --agree-tos --redirect -m "$LETSENCRYPT_EMAIL")
    while IFS= read -r item; do
      [ -n "$item" ] || continue
      certbot_cmd+=(-d "$item")
    done < <(build_host_array "$SYSOP_HOST" "$SYSOP_ALIASES")
    "${certbot_cmd[@]}"
  fi
  systemctl reload nginx
fi

if [ "$TLS_MODE" = "none" ]; then
  log "nginx reverse proxy configured on port 80 (public_host=$PUBLIC_HOST public_aliases=${PUBLIC_ALIASES:-none} sysop_host=${SYSOP_HOST:-none} sysop_aliases=${SYSOP_ALIASES:-none})"
else
  log "nginx reverse proxy configured on ports 80/443 (tls_mode=$TLS_MODE public_host=$PUBLIC_HOST public_aliases=${PUBLIC_ALIASES:-none} sysop_host=${SYSOP_HOST:-none} sysop_aliases=${SYSOP_ALIASES:-none})"
fi
