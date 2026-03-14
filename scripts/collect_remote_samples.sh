#!/usr/bin/env bash
set -euo pipefail

HOST="${1:-root@dxcluster.ai3i.net}"
OUT_ROOT="${2:-/home/jdlewis/dxcluster-compat/fixtures/raw}"
CAPTURE_TELNET="${CAPTURE_TELNET:-0}"
SSH_OPTS=( -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 )

mkdir -p "$OUT_ROOT"

ssh_cmd() {
  ssh "${SSH_OPTS[@]}" "$HOST" "$@"
}

fetch_text_file() {
  local remote_path="$1"
  local out_path="$2"
  mkdir -p "$(dirname "$out_path")"
  ssh_cmd "cat '$remote_path'" > "$out_path"
}

echo "[1/7] Discovering latest data files on $HOST"
latest_spot_dat="$(ssh_cmd "find /home/dxcluster/spider/data/spots -type f -name '*.dat' | sort | tail -n 1")"
latest_spot_dys="$(ssh_cmd "find /home/dxcluster/spider/data/spots -type f -name '*.dys' | sort | tail -n 1")"
latest_log_dat="$(ssh_cmd "find /home/dxcluster/spider/data/log -type f -name '*.dat' | sort | tail -n 1")"
latest_debug_dat="$(ssh_cmd "find /home/dxcluster/spider/data/debug -type f -name '*.dat' | sort | tail -n 1")"
latest_wcy_dat="$(ssh_cmd "find /home/dxcluster/spider/data/wcy -type f -name '*.dat' | sort | tail -n 1")"
latest_wwv_dat="$(ssh_cmd "find /home/dxcluster/spider/data/wwv -type f -name '*.dat' | sort | tail -n 1")"

cat > "$OUT_ROOT/manifest.env" <<MANIFEST
HOST=$HOST
LATEST_SPOT_DAT=$latest_spot_dat
LATEST_SPOT_DYS=$latest_spot_dys
LATEST_LOG_DAT=$latest_log_dat
LATEST_DEBUG_DAT=$latest_debug_dat
LATEST_WCY_DAT=$latest_wcy_dat
LATEST_WWV_DAT=$latest_wwv_dat
COLLECTED_UTC=$(date -u +%Y-%m-%dT%H:%M:%SZ)
MANIFEST

echo "[2/7] Fetching latest records"
fetch_text_file "$latest_spot_dat" "$OUT_ROOT/spots/latest_spots.dat"
fetch_text_file "$latest_spot_dys" "$OUT_ROOT/spots/latest_spots.dys"
fetch_text_file "$latest_log_dat" "$OUT_ROOT/log/latest_log.dat"
fetch_text_file "$latest_debug_dat" "$OUT_ROOT/debug/latest_debug.dat"
fetch_text_file "$latest_wcy_dat" "$OUT_ROOT/wcy/latest_wcy.dat"
fetch_text_file "$latest_wwv_dat" "$OUT_ROOT/wwv/latest_wwv.dat"

echo "[3/7] Fetching command/help inventory"
mkdir -p "$OUT_ROOT/inventory"
ssh_cmd "cd /home/dxcluster/spider && find cmd -type f | sed 's#^cmd/##' | sort" > "$OUT_ROOT/inventory/cmd_files.txt"
ssh_cmd "cd /home/dxcluster/spider && find cmd -type f | sed 's#^cmd/##' | awk -F/ '{print \$1}' | sort | uniq -c | sort -nr" > "$OUT_ROOT/inventory/cmd_groups.txt"


echo "[4/7] Fetching local config snapshots"
mkdir -p "$OUT_ROOT/config"
fetch_text_file "/home/dxcluster/spider/local/DXVars.pm" "$OUT_ROOT/config/DXVars.pm"
fetch_text_file "/home/dxcluster/spider/local/Listeners.pm" "$OUT_ROOT/config/Listeners.pm"
fetch_text_file "/home/dxcluster/spider/local_cmd/crontab" "$OUT_ROOT/config/local_crontab"


echo "[5/7] Fetching protocol-related source snippets"
mkdir -p "$OUT_ROOT/source"
fetch_text_file "/home/dxcluster/spider/cmd/show/dx.pl" "$OUT_ROOT/source/show_dx.pl"
fetch_text_file "/home/dxcluster/spider/perl/DXProt.pm" "$OUT_ROOT/source/DXProt.pm"
fetch_text_file "/home/dxcluster/spider/perl/DXProtHandle.pm" "$OUT_ROOT/source/DXProtHandle.pm"
fetch_text_file "/home/dxcluster/spider/perl/DXProtout.pm" "$OUT_ROOT/source/DXProtout.pm"
fetch_text_file "/home/dxcluster/spider/perl/Version.pm" "$OUT_ROOT/source/Version.pm"


echo "[6/7] Capturing telnet transcript (optional)"
mkdir -p "$OUT_ROOT/telnet"
if [[ "$CAPTURE_TELNET" == "1" ]]; then
  # Read-only command capture only. No announce/dx spot/send commands.
  ssh_cmd "timeout 12 bash -lc '{ sleep 1; echo N0CALL; sleep 1; echo \"show/version\"; sleep 1; echo \"show/dx 3\"; sleep 1; echo bye; } | telnet 127.0.0.1 7300'" > "$OUT_ROOT/telnet/session.txt" 2>&1 || true
else
  echo "skipped (set CAPTURE_TELNET=1 to enable read-only capture)" > "$OUT_ROOT/telnet/session.txt"
fi

echo "[7/7] Writing metadata summary"
{
  echo "collector_version=1"
  echo "host=$HOST"
  echo "collected_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  wc -l "$OUT_ROOT"/spots/latest_spots.dat "$OUT_ROOT"/log/latest_log.dat "$OUT_ROOT"/debug/latest_debug.dat 2>/dev/null || true
} > "$OUT_ROOT/collection_summary.txt"

echo "Collection complete: $OUT_ROOT"
