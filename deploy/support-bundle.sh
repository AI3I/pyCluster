#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=deploy/lib.sh
. "$SCRIPT_DIR/lib.sh"
set +e

usage() {
  cat <<'EOF'
Usage: sudo ./deploy/support-bundle.sh [options]

Create a text-only pyCluster installation and host diagnostic report.

Options:
  --output PATH       Write the report to PATH (default: /tmp/pycluster-support-*.txt)
  --include-network   Include interface addresses, routes, and firewall rules
  --include-journal   Include recent pyCluster systemd journal entries
  --no-journal        Explicitly omit journals (the default)
  --force             Replace an existing output file
  -h, --help          Show this help

The collector does not directly read configuration values, databases, bootstrap
credentials, private messages, or user records. Review it before sharing:
hostnames, explicitly included logs, process details, and listener addresses may
still identify the system.
EOF
}

output=""
include_network=0
include_journal=0
force=0
while [ "$#" -gt 0 ]; do
  case "$1" in
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

timestamp="$(date -u +%Y%m%d_%H%M%S)"
host_label="$(hostname 2>/dev/null | tr -cd 'A-Za-z0-9._-' | cut -c1-64)"
[ -n "$host_label" ] || host_label="unknown-host"
[ -n "$output" ] || output="${TMPDIR:-/tmp}/pycluster-support-${host_label}-${timestamp}.txt"
if [ -e "$output" ] && [ "$force" -ne 1 ]; then
  printf 'support-bundle: refusing to replace %s (use --force)\n' "$output" >&2
  exit 1
fi
mkdir -p "$(dirname "$output")" || exit 1
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
printf 'Network detail: %s\n' "$([ "$include_network" -eq 1 ] && printf included || printf limited)"
printf 'Recent journals: %s\n' "$([ "$include_journal" -eq 1 ] && printf included || printf omitted)"
printf '\nPRIVACY: The collector does not directly read TOML values, database contents,\n'
printf 'bootstrap credentials, user records, or private messages. Review hostnames,\n'
printf 'explicitly included logs, process details, and\n'
printf 'listener addresses before sharing this report publicly.\n'

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
if command -v lsblk >/dev/null 2>&1; then
  run "Block devices (no serials)" lsblk -o NAME,TYPE,SIZE,FSTYPE,FSVER,MOUNTPOINTS
fi

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

section "Network And Listener Summary"
if command -v ip >/dev/null 2>&1; then
  if [ "$include_network" -eq 1 ]; then
    run "Interface addresses" ip -brief address
    run "IPv4 routes" ip -4 route show
    run "IPv6 routes" ip -6 route show
  else
    run_shell "Interface names and states (addresses omitted)" "ip -o link show | sed -E 's/link\\/(ether|loopback) [^ ]+/link\\/\\1 [redacted]/'"
    printf '\nUse --include-network to add interface addresses and routes.\n'
  fi
fi
if command -v ss >/dev/null 2>&1; then
  run_shell "Relevant listening sockets" "ss -lntp 2>/dev/null | { head -n 1; grep -E '(:7300|:7373|:8000|:8080|:8081|pycluster|python|nginx)' || true; }"
fi
if [ "$include_network" -eq 1 ]; then
  if command -v ufw >/dev/null 2>&1; then run "UFW status" ufw status verbose; fi
  if command -v firewall-cmd >/dev/null 2>&1; then
    run "firewalld state" firewall-cmd --state
    run "firewalld active zones" firewall-cmd --get-active-zones
    run "firewalld zone details" firewall-cmd --list-all-zones
  fi
else
  if command -v ufw >/dev/null 2>&1; then run "UFW service state" systemctl is-active ufw.service; fi
  if command -v firewall-cmd >/dev/null 2>&1; then run "firewalld state" firewall-cmd --state; fi
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

section "Configuration Presence (Values Redacted)"
for config_path in "$PYCLUSTER_CONFIG_DEST" "$(dirname "$PYCLUSTER_CONFIG_DEST")/pycluster.local.toml" "$(dirname "$PYCLUSTER_CONFIG_DEST")/strings.toml"; do
  printf '\n--- %s ---\n' "$config_path"
  file_stat "$config_path"
  if [ -r "$config_path" ]; then
    awk '
      /^[[:space:]]*\[/ {print; next}
      /^[[:space:]]*[A-Za-z0-9_.-]+[[:space:]]*=/ {
        key=$0; sub(/[[:space:]]*=.*$/, "", key); gsub(/^[[:space:]]+|[[:space:]]+$/, "", key)
        print key " = <redacted>"
      }
    ' "$config_path"
  fi
done

section "systemd And Process State"
run "pyCluster unit inventory" systemctl list-unit-files 'pycluster*' --no-pager
for unit in \
  "$PYCLUSTER_SERVICE_NAME" "$PYCLUSTER_WEB_SERVICE_NAME" \
  "$PYCLUSTER_DATA_REFRESH_TIMER_NAME" "$PYCLUSTER_RETENTION_TIMER_NAME" \
  "$PYCLUSTER_UPGRADE_PATH_NAME" fail2ban.service nginx.service; do
  run "$unit state" systemctl show "$unit" --no-pager \
    --property=Id,LoadState,ActiveState,SubState,UnitFileState,FragmentPath,User,Group,WorkingDirectory,ExecMainStatus,ExecMainStartTimestamp,ExecStart
done
run_shell "pyCluster processes" "ps -eo user:20,pid,ppid,etime,comm | { head -n 1; grep -i '[p]ycluster' || true; }"

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
if command -v getenforce >/dev/null 2>&1; then run "SELinux mode" getenforce; fi
if command -v sestatus >/dev/null 2>&1; then run "SELinux status" sestatus; fi
if command -v ls >/dev/null 2>&1; then
  run "Runtime SELinux labels" ls -Zd "$PYCLUSTER_HOME" "$PYCLUSTER_APP_DIR" /var/log/pycluster
fi
if [ "$(id -u)" -eq 0 ] && command -v ausearch >/dev/null 2>&1; then
  run_shell "Recent pyCluster SELinux denials" "ausearch -m AVC,USER_AVC,SELINUX_ERR -ts recent 2>/dev/null | grep -i -C2 pycluster | tail -n 160 || true"
fi

section "Built-In Doctor"
if [ -x "$source_root/deploy/doctor.sh" ]; then
  run "doctor.sh" "$source_root/deploy/doctor.sh"
else
  printf 'doctor.sh not found under source root\n'
fi

if [ "$include_journal" -eq 1 ]; then
  section "Recent Service Journals"
  run "Core journal" journalctl -u "$PYCLUSTER_SERVICE_NAME" -n 200 --no-pager -o short-iso
  run "Public web journal" journalctl -u "$PYCLUSTER_WEB_SERVICE_NAME" -n 200 --no-pager -o short-iso
  run "Data refresh journal" journalctl -u "$PYCLUSTER_DATA_REFRESH_SERVICE_NAME" -n 100 --no-pager -o short-iso
  run "Upgrade journal" journalctl -u "$PYCLUSTER_UPGRADE_SERVICE_NAME" -n 100 --no-pager -o short-iso
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

chmod 0600 "$output" 2>/dev/null || true
printf 'Support report written to %s\n' "$output" >&3
printf 'Review the report before sharing it publicly.\n' >&3
