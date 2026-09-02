#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=deploy/lib.sh
. "$SCRIPT_DIR/lib.sh"
set +e

usage() {
  cat <<'EOF'
Usage: sudo ./deploy/support-bundle.sh [options]

Create a pyCluster installation and host diagnostic report, with optional
sensitive database and offline lab artifacts.

Options:
  --redacted          Safe sharing mode: mask addresses and all config values
  --unredacted        Private diagnostic mode: show network identifiers and
                      non-secret config values (credentials remain redacted)
  --output PATH       Write the report to PATH (default: /tmp/pycluster-support-*.txt)
  --include-network   Include detailed routes, DNS, namespaces, and firewall rules
  --include-journal   Include recent pyCluster systemd journal entries
  --include-database  Export a consistent SQLite snapshot beside the report;
                      requires --unredacted and contains sensitive user data
  --database-output PATH
                      Write the SQLite snapshot to PATH instead of the default
  --include-instance  Create a sensitive lab-import archive containing runtime,
                      config, data, logs, database snapshot, and host integration
                      artifacts; requires --unredacted and implies --include-database
  --instance-output PATH
                      Write the lab-import archive to PATH instead of the default
  --no-journal        Explicitly omit journals (the default)
  --force             Replace an existing output file
  -h, --help          Show this help

Run without options to display this help. One of --redacted or --unredacted is
required when creating a report. The text report reads only narrowly scoped
configuration and operational database state; it never emits credentials,
private messages, mail, registration data, or user records. Optional database
and instance exports contain sensitive data. Review every artifact before
sharing it. Unredacted artifacts are intended only for trusted support.
EOF
}

output=""
privacy_mode=""
include_network=0
include_journal=0
include_database=0
database_output=""
include_instance=0
instance_output=""
instance_stage=""
force=0
cleanup_instance_stage() {
  if [ -n "$instance_stage" ] && [ -d "$instance_stage" ]; then
    rm -rf -- "$instance_stage"
  fi
}
trap cleanup_instance_stage EXIT
trap 'exit 130' INT
trap 'exit 143' HUP TERM
[ "$#" -gt 0 ] || { usage; exit 0; }
while [ "$#" -gt 0 ]; do
  case "$1" in
    --redacted)
      [ "$privacy_mode" != "unredacted" ] || { printf 'support-bundle: choose only one of --redacted or --unredacted\n' >&2; exit 2; }
      privacy_mode="redacted"
      shift
      ;;
    --unredacted)
      [ "$privacy_mode" != "redacted" ] || { printf 'support-bundle: choose only one of --redacted or --unredacted\n' >&2; exit 2; }
      privacy_mode="unredacted"
      shift
      ;;
    --output)
      [ "$#" -ge 2 ] || { printf 'support-bundle: --output requires a path\n' >&2; exit 2; }
      output="$2"
      shift 2
      ;;
    --include-network)
      include_network=1
      shift
      ;;
    --include-journal)
      include_journal=1
      shift
      ;;
    --include-database)
      include_database=1
      shift
      ;;
    --database-output)
      [ "$#" -ge 2 ] || { printf 'support-bundle: --database-output requires a path\n' >&2; exit 2; }
      database_output="$2"
      include_database=1
      shift 2
      ;;
    --include-instance)
      include_instance=1
      include_database=1
      shift
      ;;
    --instance-output)
      [ "$#" -ge 2 ] || { printf 'support-bundle: --instance-output requires a path\n' >&2; exit 2; }
      instance_output="$2"
      include_instance=1
      include_database=1
      shift 2
      ;;
    --no-journal)
      include_journal=0
      shift
      ;;
    --force)
      force=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf 'support-bundle: unknown option: %s\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
done
[ -n "$privacy_mode" ] || { printf 'support-bundle: --redacted or --unredacted is required\n\n' >&2; usage >&2; exit 2; }
[ "$include_database" -eq 0 ] || [ "$privacy_mode" = "unredacted" ] || {
  printf 'support-bundle: --include-database requires --unredacted because the snapshot contains sensitive data\n' >&2
  exit 2
}
[ "$include_instance" -eq 0 ] || [ "$privacy_mode" = "unredacted" ] || {
  printf 'support-bundle: --include-instance requires --unredacted because the archive contains the complete instance state\n' >&2
  exit 2
}

timestamp="$(date -u +%Y%m%d_%H%M%S)"
host_label="$(hostname 2>/dev/null | tr -cd 'A-Za-z0-9._-' | cut -c1-64)"
[ -n "$host_label" ] || host_label="unknown-host"
[ -n "$output" ] || output="${TMPDIR:-/tmp}/pycluster-support-${host_label}-${timestamp}.txt"
if [ "$include_database" -eq 1 ] && [ -z "$database_output" ]; then
  case "$output" in
    *.txt) database_output="${output%.txt}.sqlite3" ;;
    *) database_output="${output}.sqlite3" ;;
  esac
fi
if [ "$include_instance" -eq 1 ] && [ -z "$instance_output" ]; then
  case "$output" in
    *.txt) instance_output="${output%.txt}-instance.tar.gz" ;;
    *) instance_output="${output}-instance.tar.gz" ;;
  esac
fi
if [ -e "$output" ] && [ "$force" -ne 1 ]; then
  printf 'support-bundle: refusing to replace %s (use --force)\n' "$output" >&2
  exit 1
fi
if [ "$include_database" -eq 1 ] && [ -e "$database_output" ] && [ "$force" -ne 1 ]; then
  printf 'support-bundle: refusing to replace %s (use --force)\n' "$database_output" >&2
  exit 1
fi
if [ "$include_instance" -eq 1 ] && [ -e "$instance_output" ] && [ "$force" -ne 1 ]; then
  printf 'support-bundle: refusing to replace %s (use --force)\n' "$instance_output" >&2
  exit 1
fi
mkdir -p "$(dirname "$output")" || exit 1
[ "$include_database" -eq 0 ] || mkdir -p "$(dirname "$database_output")" || exit 1
[ "$include_instance" -eq 0 ] || mkdir -p "$(dirname "$instance_output")" || exit 1
umask 077

invocation_root="$(repo_root)"
canonical_source="${PYCLUSTER_SOURCE_DIR:-/usr/src/pyCluster}"
if [ -d "$canonical_source" ]; then
  source_root="$canonical_source"
  source_basis="canonical source checkout"
else
  source_root="$invocation_root"
  source_basis="script location fallback; canonical source checkout missing"
fi

declare -a assessment_lines=()
assessment_failures=0
assessment_warnings=0
assess_ok() { assessment_lines+=("[OK] $*"); }
assess_warn() { assessment_lines+=("[WARN] $*"); assessment_warnings=$((assessment_warnings + 1)); }
assess_fail() { assessment_lines+=("[FAIL] $*"); assessment_failures=$((assessment_failures + 1)); }

section() {
  printf '\n================================================================================\n'
  printf '%s\n' "$1"
  printf '================================================================================\n'
}

run() {
  local title="$1"
  shift
  printf '\n--- %s ---\n' "$title"
  printf '$'
  printf ' %q' "$@"
  printf '\n'
  "$@" 2>&1
  local rc=$?
  [ "$rc" -eq 0 ] || printf '[command exit: %d]\n' "$rc"
  return 0
}

run_shell() {
  local title="$1" command="$2"
  printf '\n--- %s ---\n' "$title"
  printf '$ %s\n' "$command"
  bash -o pipefail -c "$command" 2>&1
  local rc=$?
  [ "$rc" -eq 0 ] || printf '[command exit: %d]\n' "$rc"
  return 0
}

run_shell_redacted() {
  local title="$1" command="$2"
  printf '\n--- %s ---\n' "$title"
  printf '$ %s\n' "$command"
  bash -o pipefail -c "$command" 2>&1 | redact_network_data
  local rc=${PIPESTATUS[0]}
  [ "$rc" -eq 0 ] || printf '[command exit: %d]\n' "$rc"
  return 0
}

redact_network_data() {
  sed -E \
    -e 's/([[:xdigit:]]{2}:){5}[[:xdigit:]]{2}/[mac-redacted]/g' \
    -e 's/([0-9]{1,3}\.){3}[0-9]{1,3}(\/[0-9]{1,2})?/[ipv4-redacted]/g' \
    -e 's/([[:xdigit:]]{1,4}:){4,7}[[:xdigit:]]{0,4}(\/[0-9]{1,3})?/[ipv6-redacted]/g' \
    -e 's/([[:xdigit:]]{0,4}:){1,7}:[[:xdigit:]]{0,4}(\/[0-9]{1,3})?/[ipv6-redacted]/g'
}

run_network_shell() {
  if [ "$privacy_mode" = "unredacted" ]; then
    run_shell "$1" "$2"
  else
    run_shell_redacted "$1" "$2"
  fi
}

file_stat() {
  local path="$1"
  if [ -e "$path" ] || [ -L "$path" ]; then
    stat -c '%A %a %U:%G %s bytes %y %n' "$path" 2>&1
  else
    printf 'MISSING %s\n' "$path"
  fi
}

safe_git_url() {
  sed -E 's#(https?://)[^/@[:space:]]+@#\1[redacted]@#'
}

read_version() {
  local root="$1"
  sed -nE 's/^__version__[[:space:]]*=[[:space:]]*"([^"]+)"/\1/p' "$root/src/pycluster/__init__.py" 2>/dev/null | head -n 1
}

source_version="$(read_version "$source_root")"
runtime_version="$(read_version "$PYCLUSTER_APP_DIR")"
source_commit=""
source_dirty="unknown"
if [ -d "$source_root/.git" ]; then
  source_commit="$(git -C "$source_root" rev-parse HEAD 2>/dev/null)"
  source_dirty="no"
  [ -z "$(git -C "$source_root" status --porcelain 2>/dev/null)" ] || source_dirty="yes"
fi

exec 3>&1
exec >"$output" 2>&1

printf 'pyCluster Support Report\n'
printf 'Generated UTC: %s\n' "$(date -u --iso-8601=seconds 2>/dev/null || date -u)"
printf 'Collector: %s\n' "$0"
printf 'Collector user: %s (uid=%s)\n' "$(id -un 2>/dev/null || printf unknown)" "$(id -u)"
printf 'Source root: %s (%s)\n' "$source_root" "$source_basis"
printf 'Runtime root: %s\n' "$PYCLUSTER_APP_DIR"
printf 'Privacy mode: %s\n' "$privacy_mode"
printf 'Network detail: %s\n' "$([ "$include_network" -eq 1 ] && printf included || printf limited)"
printf 'Recent journals: %s\n' "$([ "$include_journal" -eq 1 ] && printf included || printf omitted)"
printf 'Database snapshot: %s\n' "$([ "$include_database" -eq 1 ] && printf included || printf omitted)"
printf 'Lab-import archive: %s\n' "$([ "$include_instance" -eq 1 ] && printf included || printf omitted)"
printf '\nPRIVACY: Credential-like values are always redacted from the text report. This %s report %s\n' "$privacy_mode" "$([ "$privacy_mode" = unredacted ] && printf 'contains host/network identifiers and non-secret configuration values.' || printf 'masks addresses and configuration values where practical.')"
printf 'It excludes bootstrap credentials, user records, mail, and private messages.\n'
printf 'Review the complete report before sharing it.\n'

section "Host And Operating System"
run "OS release" cat /etc/os-release
run "Kernel" uname -a
run "Machine architecture" uname -m
run_shell "Hostname metadata" "hostnamectl 2>/dev/null | grep -E '^[[:space:]]*(Static hostname|Icon name|Chassis|Virtualization|Operating System|Kernel|Architecture|Hardware Vendor|Hardware Model):' || hostname"
run "Uptime" uptime
if command -v systemd-detect-virt >/dev/null 2>&1; then
  run "Virtualization" systemd-detect-virt
  run "Container detection" systemd-detect-virt --container
  run "VM detection" systemd-detect-virt --vm
fi
printf '\n--- DMI platform (serial numbers deliberately excluded) ---\n'
for dmi_file in sys_vendor product_name board_vendor board_name chassis_type; do
  if [ -r "/sys/class/dmi/id/$dmi_file" ]; then
    printf '%-18s %s\n' "$dmi_file" "$(tr -d '\000' <"/sys/class/dmi/id/$dmi_file")"
  fi
done
if [ -r /proc/1/cgroup ]; then
  run "PID 1 cgroup" sed -n '1,80p' /proc/1/cgroup
fi

section "Hardware And Capacity"
if command -v lscpu >/dev/null 2>&1; then
  run_shell "CPU summary" "lscpu | sed -n -E '/^(Architecture|CPU\\(s\\)|On-line CPU|Vendor ID|Model name|Thread|Core|Socket|Virtualization|Hypervisor vendor):/p'"
fi
run "Memory" free -h
run_shell "Raw memory totals" "grep -E '^(MemTotal|MemAvailable|SwapTotal|SwapFree):' /proc/meminfo"
run "Filesystem capacity" df -hT
run "Filesystem inode capacity" df -hiT
if command -v lsblk >/dev/null 2>&1; then
  run "Block devices (no serials)" lsblk -o NAME,TYPE,SIZE,FSTYPE,FSVER,MOUNTPOINTS
fi
if [ -d /proc/pressure ]; then run_shell "Linux pressure stall information" "for file in /proc/pressure/cpu /proc/pressure/io /proc/pressure/memory; do echo \"[\$file]\"; cat \"\$file\"; done"; fi
if command -v timedatectl >/dev/null 2>&1; then run "Clock and synchronization" timedatectl status; fi

section "Runtime And Package Tooling"
for command_name in python3 "$PYCLUSTER_PYTHON_LINK" git rsync curl openssl sqlite3 nginx systemctl; do
  printf '\n--- %s ---\n' "$command_name"
  if command -v "$command_name" >/dev/null 2>&1 || [ -x "$command_name" ]; then
    command -v "$command_name" 2>/dev/null || printf '%s\n' "$command_name"
    case "$command_name" in
      python3|"$PYCLUSTER_PYTHON_LINK") "$command_name" --version 2>&1 ;;
      git) git --version 2>&1 ;;
      rsync) rsync --version 2>&1 | head -n 2 ;;
      curl) curl --version 2>&1 | head -n 2 ;;
      openssl) openssl version -a 2>&1 ;;
      sqlite3) sqlite3 --version 2>&1 ;;
      nginx) nginx -v 2>&1 ;;
      systemctl) systemctl --version 2>&1 | head -n 3 ;;
    esac
  else
    printf 'not found\n'
  fi
done
printf '\n--- Distribution package state ---\n'
if command -v dpkg-query >/dev/null 2>&1; then
  dpkg-query -W -f='${binary:Package}\t${Version}\t${db:Status-Abbrev}\n' \
    python3 python3-venv nginx sqlite3 curl rsync git policycoreutils selinux-utils 2>&1
elif command -v rpm >/dev/null 2>&1; then
  rpm -q python3 nginx sqlite curl rsync git policycoreutils selinux-policy selinux-policy-targeted 2>&1
elif command -v apk >/dev/null 2>&1; then
  apk info -e python3 nginx sqlite curl rsync git 2>&1
else
  printf 'No supported package inventory tool found (dpkg-query, rpm, or apk).\n'
fi

section "Network And Listener Summary"
if command -v ip >/dev/null 2>&1; then
  run_network_shell "Interface names and states" "ip -brief link"
  if [ "$include_network" -eq 1 ]; then
    run_network_shell "Interface addresses" "ip -brief address"
    run_network_shell "IPv4 route table" "ip -4 route show table all"
    run_network_shell "IPv6 route table" "ip -6 route show table all"
    run_network_shell "IPv4 policy rules" "ip -4 rule show"
    run_network_shell "IPv6 policy rules" "ip -6 rule show"
    run_network_shell "Network namespaces" "ip netns list 2>/dev/null || true"
  else
    run_shell "Address-family summary" "ip -o address show | awk '{family[\$3]++; iface[\$2]=1} END {for (f in family) print f, family[f], \"address(es)\"; print length(iface), \"interface(s) with addresses\"}'"
    run_shell "Default-route summary" "printf 'IPv4 default routes: '; ip -4 route show default 2>/dev/null | wc -l; printf 'IPv6 default routes: '; ip -6 route show default 2>/dev/null | wc -l"
    printf '\nUse --include-network for addresses, routes, policy rules, DNS, namespaces, and firewall details.\n'
  fi
fi
if command -v ss >/dev/null 2>&1; then
  run_network_shell "Relevant listening TCP sockets" "ss -lntp 2>/dev/null | { head -n 1; grep -E '(:7300|:7373|:8000|:8080|:8081|pycluster|python|nginx)' || true; }"
  run_network_shell "Established pyCluster-related TCP sockets" "ss -ntp 2>/dev/null | { head -n 1; grep -E '(:7300|:7373|:8000|:8080|:8081|pycluster|python)' || true; }"
fi
run_shell "IPv4 and IPv6 kernel controls" "for key in net.ipv6.conf.all.disable_ipv6 net.ipv6.bindv6only net.ipv4.ip_forward net.ipv6.conf.all.forwarding net.ipv4.conf.all.rp_filter; do printf '%s=' \"\$key\"; sysctl -n \"\$key\" 2>/dev/null || printf unavailable; done"
for network_service in NetworkManager.service systemd-networkd.service systemd-resolved.service networking.service; do
  run "$network_service state" systemctl show "$network_service" --no-pager --property=Id,LoadState,ActiveState,SubState,UnitFileState
done
if [ "$include_network" -eq 1 ]; then
  if command -v resolvectl >/dev/null 2>&1; then run_network_shell "Resolver status" "resolvectl status 2>/dev/null"; fi
  if command -v nmcli >/dev/null 2>&1; then
    run_network_shell "NetworkManager devices" "nmcli -f DEVICE,TYPE,STATE,CONNECTION device status 2>/dev/null"
    run_network_shell "NetworkManager active connections" "nmcli -f NAME,UUID,TYPE,DEVICE connection show --active 2>/dev/null"
  fi
  if command -v networkctl >/dev/null 2>&1; then run_network_shell "systemd-networkd links" "networkctl list --no-pager 2>/dev/null"; fi
  if command -v ufw >/dev/null 2>&1; then run_network_shell "UFW status" "ufw status verbose"; fi
  if command -v firewall-cmd >/dev/null 2>&1; then
    run "firewalld state" firewall-cmd --state
    run_network_shell "firewalld active zones" "firewall-cmd --get-active-zones"
    run_network_shell "firewalld zone details" "firewall-cmd --list-all-zones"
  fi
  if command -v nft >/dev/null 2>&1; then run_network_shell "nftables ruleset" "nft list ruleset 2>/dev/null"; fi
  if command -v iptables-save >/dev/null 2>&1; then run_network_shell "iptables rules" "iptables-save 2>/dev/null"; fi
  if command -v ip6tables-save >/dev/null 2>&1; then run_network_shell "ip6tables rules" "ip6tables-save 2>/dev/null"; fi
else
  if command -v ufw >/dev/null 2>&1; then run "UFW service state" systemctl is-active ufw.service; fi
  if command -v firewall-cmd >/dev/null 2>&1; then
    printf '\nfirewalld detected; use --include-network to query its state and rules.\n'
  fi
fi

section "pyCluster Deployment Layout"
printf '\n--- Expected paths ---\n'
for path in \
  "$canonical_source" \
  "$canonical_source/.git" \
  "$PYCLUSTER_APP_DIR" \
  "$PYCLUSTER_APP_DIR/src/pycluster/__init__.py" \
  "$PYCLUSTER_APP_DIR/config" \
  "$PYCLUSTER_APP_DIR/data" \
  "$PYCLUSTER_APP_DIR/logs" \
  "$PYCLUSTER_CONFIG_DEST" \
  "$(dirname "$PYCLUSTER_CONFIG_DEST")/pycluster.local.toml" \
  "$(dirname "$PYCLUSTER_CONFIG_DEST")/strings.toml" \
  "$PYCLUSTER_APP_DIR/data/deployment-state.toml" \
  "$PYCLUSTER_PYTHON_LINK"; do
  file_stat "$path"
done

printf '\n--- Version comparison ---\n'
printf 'source version:  %s\n' "${source_version:-unavailable}"
printf 'runtime version: %s\n' "${runtime_version:-unavailable}"

effective_node_call=""
config_valid="unknown"
if [ -f "$PYCLUSTER_CONFIG_DEST" ] && [ -d "$PYCLUSTER_APP_DIR/src/pycluster" ]; then
  effective_node_call="$(
    cd "$PYCLUSTER_APP_DIR" &&
    PYTHONPATH=src "${PYCLUSTER_PYTHON_LINK:-/usr/bin/python3}" - "$PYCLUSTER_CONFIG_DEST" 2>/dev/null <<'PY'
import sys
from pycluster.config import load_config

print(load_config(sys.argv[1]).node.node_call)
PY
  )"
  if [ -n "$effective_node_call" ]; then config_valid="yes"; else config_valid="no"; fi
fi
printf 'effective node: %s\n' "${effective_node_call:-unavailable}"
printf 'configuration load: %s\n' "$config_valid"

printf '\n--- Source Git state ---\n'
if [ -d "$source_root/.git" ]; then
  git -C "$source_root" rev-parse --show-toplevel 2>&1
  git -C "$source_root" log -1 --format='commit=%H%nauthor=%an <%ae>%ndate=%cI%nsubject=%s' 2>&1
  printf 'origin='
  git -C "$source_root" remote get-url origin 2>&1 | safe_git_url
  printf 'working_tree:\n'
  git -C "$source_root" status --short 2>&1 | sed -n '1,120p'
else
  printf 'No Git metadata at %s\n' "$source_root"
fi

printf '\n--- Runtime Git metadata ---\n'
if [ -e "$PYCLUSTER_APP_DIR/.git" ]; then
  printf 'PRESENT (unexpected in a normal rsync deployment)\n'
  file_stat "$PYCLUSTER_APP_DIR/.git"
else
  printf 'absent (expected)\n'
fi

printf '\n--- Source/runtime immutable-tree comparison ---\n'
tree_diff="$(mktemp "${TMPDIR:-/tmp}/pycluster-tree-diff.XXXXXX")"
if [ -d "$source_root" ] && [ -d "$PYCLUSTER_APP_DIR" ]; then
  diff -qr \
    --exclude=.git --exclude=.pytest_cache --exclude=__pycache__ --exclude='*.pyc' \
    --exclude=config --exclude=data --exclude=logs \
    "$source_root" "$PYCLUSTER_APP_DIR" >"$tree_diff" 2>&1
  tree_diff_count="$(wc -l <"$tree_diff" | tr -d ' ')"
  printf 'difference lines: %s\n' "$tree_diff_count"
  sed -n '1,120p' "$tree_diff"
  [ "$tree_diff_count" -le 120 ] || printf '... output truncated; %s additional lines ...\n' "$((tree_diff_count - 120))"
else
  tree_diff_count="-1"
  printf 'comparison unavailable; source or runtime tree is missing\n'
fi
rm -f "$tree_diff"

printf '\n--- Unexpected runtime ownership (first 120) ---\n'
if [ -d "$PYCLUSTER_APP_DIR" ] && id -u "$PYCLUSTER_USER" >/dev/null 2>&1; then
  ownership_file="$(mktemp "${TMPDIR:-/tmp}/pycluster-ownership.XXXXXX")"
  find "$PYCLUSTER_APP_DIR" -xdev \( ! -user "$PYCLUSTER_USER" -o ! -group "$PYCLUSTER_GROUP" \) \
    -printf '%u:%g %m %p\n' >"$ownership_file" 2>/dev/null
  ownership_count="$(wc -l <"$ownership_file" | tr -d ' ')"
  printf 'unexpected ownership entries: %s\n' "$ownership_count"
  sed -n '1,120p' "$ownership_file"
  rm -f "$ownership_file"
else
  ownership_count="-1"
  printf 'ownership check unavailable\n'
fi

printf '\n--- Potential duplicate pyCluster directories ---\n'
duplicate_file="$(mktemp "${TMPDIR:-/tmp}/pycluster-duplicates.XXXXXX")"
find /usr/src /usr/local/src /opt /srv /home -maxdepth 5 -type d -iname 'pycluster' \
  -print >"$duplicate_file" 2>/dev/null
sort -u "$duplicate_file"
duplicate_count="$(sort -u "$duplicate_file" | wc -l | tr -d ' ')"
rm -f "$duplicate_file"

printf '\n--- Deployment receipt ---\n'
receipt="$PYCLUSTER_APP_DIR/data/deployment-state.toml"
receipt_commit=""
if [ -r "$receipt" ]; then
  cat "$receipt"
  receipt_commit="$(sed -nE 's/^source_commit[[:space:]]*=[[:space:]]*"([^"]+)"/\1/p' "$receipt" | head -n 1)"
else
  printf 'not found (legacy installs will not have one until install/upgrade/repair runs again)\n'
fi

section "Configuration Presence ($([ "$privacy_mode" = unredacted ] && printf 'Non-Secret Values Included' || printf 'Values Redacted'))"
for config_path in "$PYCLUSTER_CONFIG_DEST" "$(dirname "$PYCLUSTER_CONFIG_DEST")/pycluster.local.toml" "$(dirname "$PYCLUSTER_CONFIG_DEST")/strings.toml"; do
  printf '\n--- %s ---\n' "$config_path"
  file_stat "$config_path"
  if [ -r "$config_path" ]; then
    awk -v privacy_mode="$privacy_mode" '
      /^[[:space:]]*\[/ {print; next}
      /^[[:space:]]*[A-Za-z0-9_.-]+[[:space:]]*=/ {
        key=$0; sub(/[[:space:]]*=.*$/, "", key); gsub(/^[[:space:]]+|[[:space:]]+$/, "", key)
        lower=tolower(key)
        secret=(lower ~ /(password|passwd|secret|token|credential|private_key|api_key|dsn|startup_commands)/)
        if (privacy_mode == "unredacted" && !secret) print
        else if (secret) print key " = <credential-redacted>"
        else print key " = <redacted>"
      }
    ' "$config_path"
  fi
done

section "Protocol Address And Peer Diagnostics"
diagnostic_python="${PYCLUSTER_PYTHON_LINK:-}"
if [ ! -x "$diagnostic_python" ]; then diagnostic_python="$(command -v python3 2>/dev/null)"; fi
diagnostic_helper="$source_root/deploy/support_diagnostics.py"
if [ -n "$diagnostic_python" ] && [ -f "$PYCLUSTER_CONFIG_DEST" ] && [ -f "$diagnostic_helper" ]; then
  PYTHONPATH="$source_root/src:$PYCLUSTER_APP_DIR/src${PYTHONPATH:+:$PYTHONPATH}" \
    "$diagnostic_python" "$diagnostic_helper" report \
      --config "$PYCLUSTER_CONFIG_DEST" --runtime-root "$PYCLUSTER_APP_DIR" --privacy "$privacy_mode"
else
  printf 'Protocol diagnostics unavailable: Python runtime, helper, or configuration is missing.\n'
fi

section "systemd And Process State"
run "Failed systemd units" systemctl --failed --no-pager
run "pyCluster unit inventory" systemctl list-unit-files 'pycluster*' --no-pager
for unit in \
  "$PYCLUSTER_SERVICE_NAME" "$PYCLUSTER_WEB_SERVICE_NAME" \
  "$PYCLUSTER_DATA_REFRESH_TIMER_NAME" "$PYCLUSTER_RETENTION_TIMER_NAME" \
  "$PYCLUSTER_UPGRADE_PATH_NAME" fail2ban.service nginx.service; do
  run "$unit state" systemctl show "$unit" --no-pager \
    --property=Id,LoadState,ActiveState,SubState,UnitFileState,FragmentPath,User,Group,WorkingDirectory,ExecMainStatus,ExecMainStartTimestamp,ExecStart,Result,NRestarts,MemoryCurrent,MemoryPeak,CPUUsageNSec,TasksCurrent,LimitNOFILE,OOMPolicy
done
run_shell "pyCluster processes" "ps -eo user:20,pid,ppid,etime,comm | { head -n 1; grep -i '[p]ycluster' || true; }"
if command -v coredumpctl >/dev/null 2>&1; then run_shell "Recent pyCluster-related core dumps" "coredumpctl list --no-pager 2>/dev/null | { head -n 1; grep -Ei 'pycluster|python|nginx' | tail -n 40 || true; }"; fi
if command -v systemd-delta >/dev/null 2>&1; then run_shell "Relevant systemd unit overrides" "systemd-delta --no-pager 2>/dev/null | grep -Ei -C1 'pycluster' || true"; fi

printf '\n--- Installed unit-file comparison ---\n'
unit_mismatches=0
for unit in \
  pycluster.service pyclusterweb.service pycluster-data-refresh.service \
  pycluster-data-refresh.timer pycluster-retention.service pycluster-retention.timer \
  pycluster-upgrade.service pycluster-upgrade.path; do
  expected="$source_root/deploy/systemd/$unit"
  installed="$PYCLUSTER_SYSTEMD_DIR/$unit"
  if [ -f "$expected" ] && [ -f "$installed" ]; then
    if cmp -s "$expected" "$installed"; then
      printf 'MATCH %s\n' "$unit"
    else
      printf 'DIFFERENT %s (%s vs %s)\n' "$unit" "$expected" "$installed"
      unit_mismatches=$((unit_mismatches + 1))
      diff -u "$expected" "$installed" 2>&1 | sed -n '1,80p'
    fi
  else
    printf 'MISSING %s expected=%s installed=%s\n' "$unit" "$([ -f "$expected" ] && printf yes || printf no)" "$([ -f "$installed" ] && printf yes || printf no)"
    unit_mismatches=$((unit_mismatches + 1))
  fi
done

section "nginx, fail2ban, Logging, And SELinux"
if command -v nginx >/dev/null 2>&1; then run "nginx syntax" nginx -t; fi
run_shell "pyCluster nginx artifacts" "find /etc/nginx -maxdepth 4 -type f -iname '*pycluster*' -printf '%M %u:%g %s %TY-%Tm-%TdT%TH:%TM %p\\n' 2>/dev/null | sort"
run_shell "pyCluster fail2ban artifacts" "find /etc/fail2ban -maxdepth 4 -type f -iname '*pycluster*' -printf '%M %u:%g %s %TY-%Tm-%TdT%TH:%TM %p\\n' 2>/dev/null | sort"
run_shell "pyCluster logrotate artifacts" "find /etc/logrotate.d -maxdepth 1 -type f -iname '*pycluster*' -printf '%M %u:%g %s %TY-%Tm-%TdT%TH:%TM %p\\n' 2>/dev/null | sort"
if command -v fail2ban-client >/dev/null 2>&1; then run "fail2ban status" fail2ban-client status; fi

printf '\n--- SELinux capability inventory ---\n'
if command -v selinuxenabled >/dev/null 2>&1; then run "SELinux enabled probe" selinuxenabled; else printf 'selinuxenabled not found\n'; fi
if command -v getenforce >/dev/null 2>&1; then run "SELinux mode" getenforce; else printf 'getenforce not found\n'; fi
if command -v sestatus >/dev/null 2>&1; then run "SELinux status" sestatus; else printf 'sestatus not found\n'; fi
if [ -r /etc/selinux/config ]; then run_shell "SELinux persistent configuration" "grep -E '^[[:space:]]*(SELINUX|SELINUXTYPE)=' /etc/selinux/config"; fi
if command -v semodule >/dev/null 2>&1; then run_shell "Relevant SELinux policy modules" "semodule -lfull 2>/dev/null | grep -Ei '(pycluster|nginx|httpd|fail2ban)' || true"; fi
if command -v getsebool >/dev/null 2>&1; then
  run_shell "Relevant SELinux booleans" "getsebool -a 2>/dev/null | grep -E '^(httpd_can_network_connect|httpd_can_network_relay|httpd_can_sendmail|nis_enabled|daemons_enable_cluster_mode)[[:space:]]' || true"
fi
if command -v semanage >/dev/null 2>&1; then
  run_shell "Relevant SELinux port mappings" "semanage port -l 2>/dev/null | grep -E '^(http_port_t|http_cache_port_t|unreserved_port_t|commplex_main_port_t)[[:space:]]' || true"
  run_shell "pyCluster SELinux file-context rules" "semanage fcontext -l 2>/dev/null | grep -Ei '(/home/pycluster|/usr/src/pyCluster|/var/log/pycluster|pycluster)' || true"
fi
if command -v matchpathcon >/dev/null 2>&1; then
  run "Expected SELinux path contexts" matchpathcon -V "$PYCLUSTER_HOME" "$PYCLUSTER_APP_DIR" "$PYCLUSTER_CONFIG_DEST" /var/log/pycluster
fi
if command -v ls >/dev/null 2>&1; then
  run "Runtime SELinux labels" ls -Zd "$PYCLUSTER_HOME" "$PYCLUSTER_APP_DIR" "$PYCLUSTER_CONFIG_DEST" /var/log/pycluster
fi
if command -v ps >/dev/null 2>&1; then run_shell "Relevant SELinux process domains" "ps -eZ -o label,user,pid,ppid,comm,args 2>/dev/null | { head -n 1; grep -Ei '([p]ycluster|[n]ginx|[f]ail2ban)' || true; }"; fi
if command -v ss >/dev/null 2>&1; then run_network_shell "SELinux socket labels" "ss -lntpZ 2>/dev/null | { head -n 1; grep -E '(:7300|:7373|:8000|:8080|:8081|pycluster|python|nginx)' || true; }"; fi
if command -v auditctl >/dev/null 2>&1; then run "Linux audit subsystem status" auditctl -s; fi
if [ "$(id -u)" -eq 0 ] && command -v ausearch >/dev/null 2>&1; then
  run_network_shell "Recent relevant SELinux denials" "ausearch -m AVC,USER_AVC,SELINUX_ERR -ts recent 2>/dev/null | grep -Ei -C2 '(pycluster|python|nginx|fail2ban|name_connect|name_bind|7300|7373|8000|8080|8081)' | tail -n 240 || true"
fi

section "Built-In Doctor"
if [ -x "$source_root/deploy/doctor.sh" ]; then
  run "doctor.sh" "$source_root/deploy/doctor.sh"
else
  printf 'doctor.sh not found under source root\n'
fi

if [ "$include_journal" -eq 1 ]; then
  section "Recent Service Journals"
  run_network_shell "Core journal" "journalctl -u '$PYCLUSTER_SERVICE_NAME' -n 200 --no-pager -o short-iso"
  run_network_shell "Public web journal" "journalctl -u '$PYCLUSTER_WEB_SERVICE_NAME' -n 200 --no-pager -o short-iso"
  run_network_shell "Data refresh journal" "journalctl -u '$PYCLUSTER_DATA_REFRESH_SERVICE_NAME' -n 100 --no-pager -o short-iso"
  run_network_shell "Upgrade journal" "journalctl -u '$PYCLUSTER_UPGRADE_SERVICE_NAME' -n 100 --no-pager -o short-iso"
  run_network_shell "nginx journal" "journalctl -u nginx.service -n 100 --no-pager -o short-iso"
  run_network_shell "fail2ban journal" "journalctl -u fail2ban.service -n 100 --no-pager -o short-iso"
  run_network_shell "Kernel warnings and resource failures" "journalctl -k -b -p warning..alert -n 160 --no-pager -o short-iso 2>/dev/null | tail -n 160"
  run_network_shell "Recent OOM and killed-process evidence" "journalctl -b --no-pager -o short-iso 2>/dev/null | grep -Ei '(out of memory|oom-kill|killed process|memory cgroup out of memory)' | tail -n 120 || true"
fi

if [ "$include_database" -eq 1 ]; then
  section "Sensitive SQLite Snapshot"
  printf 'The separate SQLite snapshot contains user and operational data. Do not attach it to a public issue.\n'
  if [ -n "${diagnostic_python:-}" ] && [ -f "$PYCLUSTER_CONFIG_DEST" ] && [ -f "${diagnostic_helper:-}" ]; then
    PYTHONPATH="$source_root/src:$PYCLUSTER_APP_DIR/src${PYTHONPATH:+:$PYTHONPATH}" \
      "$diagnostic_python" "$diagnostic_helper" backup-database \
        --config "$PYCLUSTER_CONFIG_DEST" --runtime-root "$PYCLUSTER_APP_DIR" --output "$database_output"
    database_export_rc=$?
  else
    printf 'Database export unavailable: Python runtime, helper, or configuration is missing.\n'
    database_export_rc=1
  fi
fi

section "Installation Assessment"
if [ "$(id -u)" -eq 0 ]; then assess_ok "collector ran as root; privileged checks were available"; else assess_warn "collector did not run as root; some checks are incomplete"; fi
if [ -d "$canonical_source/.git" ]; then assess_ok "canonical source checkout exists at $canonical_source"; else assess_warn "canonical Git checkout is missing at $canonical_source"; fi
if [ "$source_dirty" = "yes" ]; then assess_warn "selected source checkout has uncommitted changes"; elif [ "$source_dirty" = "no" ]; then assess_ok "selected source checkout is clean"; fi
if [ -d "$PYCLUSTER_APP_DIR" ]; then assess_ok "runtime directory exists at $PYCLUSTER_APP_DIR"; else assess_fail "runtime directory is missing at $PYCLUSTER_APP_DIR"; fi
if id -u "$PYCLUSTER_USER" >/dev/null 2>&1; then assess_ok "service account $PYCLUSTER_USER exists"; else assess_fail "service account $PYCLUSTER_USER is missing"; fi
if [ -f "$PYCLUSTER_CONFIG_DEST" ]; then assess_ok "runtime base configuration exists"; else assess_fail "runtime base configuration is missing"; fi
if [ "$config_valid" = "yes" ]; then assess_ok "effective runtime configuration loads successfully"; elif [ -f "$PYCLUSTER_CONFIG_DEST" ]; then assess_warn "effective runtime configuration could not be loaded"; fi
case "${effective_node_call^^}" in
  N0CALL-1|N0NODE-1) assess_warn "example node identity $effective_node_call is still active; SysOp setup was not completed" ;;
  '') assess_warn "effective node identity could not be determined" ;;
  *) assess_ok "node identity is configured as $effective_node_call" ;;
esac
if [ -n "$source_version" ] && [ "$source_version" = "$runtime_version" ]; then assess_ok "source and runtime versions match ($source_version)"; else assess_warn "source/runtime versions differ or could not be read (source=${source_version:-unknown}, runtime=${runtime_version:-unknown})"; fi
if [ "${tree_diff_count:--1}" = "0" ]; then assess_ok "immutable runtime tree matches the selected source"; elif [ "${tree_diff_count:--1}" = "-1" ]; then assess_warn "immutable source/runtime comparison was unavailable"; else assess_warn "immutable source/runtime tree has ${tree_diff_count} difference line(s)"; fi
if [ "${ownership_count:--1}" = "0" ]; then assess_ok "runtime ownership matches $PYCLUSTER_USER:$PYCLUSTER_GROUP"; elif [ "${ownership_count:--1}" = "-1" ]; then assess_warn "runtime ownership check was unavailable"; else assess_warn "runtime contains ${ownership_count} path(s) with unexpected ownership"; fi
if [ -e "$PYCLUSTER_APP_DIR/.git" ]; then assess_warn "runtime contains Git metadata; supported installs sync from a separate source checkout"; else assess_ok "runtime is a deployed tree rather than a nested Git checkout"; fi
if [ "${unit_mismatches:-0}" -eq 0 ]; then assess_ok "installed systemd units match the selected source"; else assess_warn "${unit_mismatches} installed systemd unit(s) are missing or modified"; fi
if [ -r "$receipt" ]; then
  assess_ok "deployment receipt exists"
  if [ -n "$source_commit" ] && [ -n "$receipt_commit" ] && [ "$source_commit" = "$receipt_commit" ]; then
    assess_ok "deployment receipt commit matches the selected source"
  elif [ -n "$source_commit" ] && [ -n "$receipt_commit" ]; then
    assess_warn "deployment receipt commit does not match the selected source"
  fi
else
  assess_warn "deployment receipt is absent; this may be a legacy install or an untracked copy"
fi
if [ "${duplicate_count:-0}" -le 2 ]; then assess_ok "no excess pyCluster directory copies were found in common install roots"; else assess_warn "${duplicate_count} pyCluster-named directories were found; inspect duplicate-install listing"; fi

for line in "${assessment_lines[@]}"; do printf '%s\n' "$line"; done
printf '\nFailures: %d\nWarnings: %d\n' "$assessment_failures" "$assessment_warnings"
if [ "$assessment_failures" -gt 0 ]; then
  printf 'Overall: INCOMPLETE OR BROKEN INSTALLATION\n'
elif [ "$assessment_warnings" -gt 0 ]; then
  printf 'Overall: INSTALLED, BUT REVIEW WARNINGS BEFORE SUPPORT\n'
else
  printf 'Overall: SUPPORTED LAYOUT SIGNALS PRESENT\n'
fi
printf '\nAn assessment is evidence, not proof. A missing receipt alone does not mean the\n'
printf 'installation was copied manually; releases before this collector did not write one.\n'

if [ "$include_instance" -eq 1 ]; then
  section "Sensitive Lab-Import Archive"
  printf 'Creating a complete diagnostic clone for trusted offline testing.\n'
  printf 'The archive contains credentials, user data, mail, logs, and node identity.\n'
  instance_stage="$(mktemp -d "${TMPDIR:-/tmp}/pycluster-instance.XXXXXX")"
  instance_rc=0
  install -d -m 0700 "$instance_stage/runtime" "$instance_stage/database" "$instance_stage/host" "$instance_stage/source"
  {
    printf 'format=pycluster-support-instance-v1\n'
    printf 'generated_utc=%s\n' "$(date -u --iso-8601=seconds 2>/dev/null || date -u)"
    printf 'hostname=%s\n' "$host_label"
    printf 'node_call=%s\n' "${effective_node_call:-unavailable}"
    printf 'source_root=%s\n' "$source_root"
    printf 'runtime_root=%s\n' "$PYCLUSTER_APP_DIR"
    printf 'config_path=%s\n' "$PYCLUSTER_CONFIG_DEST"
    printf 'source_version=%s\n' "${source_version:-unavailable}"
    printf 'runtime_version=%s\n' "${runtime_version:-unavailable}"
    printf 'source_commit=%s\n' "${source_commit:-unavailable}"
    printf 'database_snapshot=database/pycluster.sqlite3\n'
    printf 'restore_policy=offline-lab-only; never overwrite a live installation automatically\n'
  } >"$instance_stage/MANIFEST.txt"

  if [ -d "$PYCLUSTER_APP_DIR" ]; then
    if command -v rsync >/dev/null 2>&1; then
      rsync -a \
        --exclude='.git/' --exclude='venv/' --exclude='.venv/' \
        --exclude='__pycache__/' --exclude='*.pyc' \
        --exclude='*.sqlite3' --exclude='*.sqlite3-wal' --exclude='*.sqlite3-shm' \
        --exclude='*.db' --exclude='*.db-wal' --exclude='*.db-shm' \
        "$PYCLUSTER_APP_DIR/" "$instance_stage/runtime/" || instance_rc=1
    else
      cp -a "$PYCLUSTER_APP_DIR/." "$instance_stage/runtime/" || instance_rc=1
      find "$instance_stage/runtime" -depth \
        \( -name .git -o -name venv -o -name .venv -o -name __pycache__ \) \
        -type d -exec rm -rf {} + 2>/dev/null
      find "$instance_stage/runtime" -type f \
        \( -name '*.pyc' -o -name '*.sqlite3' -o -name '*.sqlite3-wal' -o -name '*.sqlite3-shm' \
        -o -name '*.db' -o -name '*.db-wal' -o -name '*.db-shm' \) -delete 2>/dev/null
    fi
  else
    printf 'Runtime directory is missing; archive cannot reproduce the instance.\n'
    instance_rc=1
  fi
  if [ "${database_export_rc:-1}" -eq 0 ] && [ -f "$database_output" ]; then
    cp -a "$database_output" "$instance_stage/database/pycluster.sqlite3" || instance_rc=1
  else
    printf 'Consistent database snapshot is unavailable.\n'
    instance_rc=1
  fi

  for integration_root in /etc/systemd/system /etc/nginx /etc/fail2ban /etc/logrotate.d; do
    [ -d "$integration_root" ] || continue
    while IFS= read -r -d '' integration_file; do
      integration_dest="$instance_stage/host$integration_file"
      install -d -m 0700 "$(dirname "$integration_dest")"
      cp -a "$integration_file" "$integration_dest" || instance_rc=1
    done < <(find "$integration_root" -maxdepth 5 \( -type f -o -type l \) -iname '*pycluster*' -print0 2>/dev/null)
  done
  if [ -r /etc/selinux/config ]; then
    install -D -m 0600 /etc/selinux/config "$instance_stage/host/etc/selinux/config" || instance_rc=1
  fi
  if [ -d "$source_root/.git" ]; then
    git -C "$source_root" status --short >"$instance_stage/source/status.txt" 2>&1
    git -C "$source_root" log -1 --format=fuller >"$instance_stage/source/commit.txt" 2>&1
    git -C "$source_root" diff --binary >"$instance_stage/source/working-tree.patch" 2>&1
    git -C "$source_root" bundle create "$instance_stage/source/source.bundle" --branches --tags HEAD >/dev/null 2>&1 || instance_rc=1
  fi
  if command -v getfacl >/dev/null 2>&1; then
    getfacl -R -p "$PYCLUSTER_APP_DIR" >"$instance_stage/runtime-acls.txt" 2>/dev/null || true
  fi
  find "$instance_stage" -printf '%M %u:%g %s %p\n' | sort >"$instance_stage/CONTENTS.txt"

  if [ "$instance_rc" -eq 0 ]; then
    tar -C "$instance_stage" -czf "$instance_output" . || instance_rc=1
  fi
  cleanup_instance_stage
  instance_stage=""
  if [ "$instance_rc" -eq 0 ]; then
    chmod 0600 "$instance_output" 2>/dev/null || true
    if command -v sha256sum >/dev/null 2>&1; then
      sha256sum "$instance_output" >"${instance_output}.sha256"
      chmod 0600 "${instance_output}.sha256" 2>/dev/null || true
    fi
    printf 'Lab-import archive written: %s\n' "$instance_output"
    printf 'Archive size: %s bytes\n' "$(stat -c %s "$instance_output" 2>/dev/null || printf unknown)"
  else
    rm -f "$instance_output" "${instance_output}.sha256"
    printf 'Lab-import archive failed; partial output was removed.\n'
  fi
fi

chmod 0600 "$output" 2>/dev/null || true
printf 'Support report written to %s\n' "$output" >&3
if [ "$include_database" -eq 1 ] && [ "${database_export_rc:-1}" -eq 0 ]; then
  printf 'Sensitive SQLite snapshot written to %s\n' "$database_output" >&3
fi
if [ "$include_instance" -eq 1 ] && [ "${instance_rc:-1}" -eq 0 ]; then
  printf 'Sensitive lab-import archive written to %s\n' "$instance_output" >&3
fi
printf 'Review the report before sharing it publicly.\n' >&3
