#!/usr/bin/env bash
set -euo pipefail

REMOTE_HOST="${REMOTE_HOST:-root@dxcluster.ai3i.net}"
REMOTE_BASE="${REMOTE_BASE:-/spider/data}"
DEST_ROOT="${1:-./fixtures/live/dxspider}"

mkdir -p "${DEST_ROOT}"

current_year="$(date -u +%Y)"
current_month="$(date -u +%m).dat"
day_of_year="$(date -u +%j).dat"

rsync -az \
  -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" \
  "${REMOTE_HOST}:${REMOTE_BASE}/motd" \
  "${REMOTE_HOST}:${REMOTE_BASE}/user_asc" \
  "${REMOTE_HOST}:${REMOTE_BASE}/bands.pl" \
  "${REMOTE_HOST}:${REMOTE_BASE}/prefix_data.pl" \
  "${REMOTE_HOST}:${REMOTE_BASE}/cty.dat" \
  "${DEST_ROOT}/"

mkdir -p "${DEST_ROOT}/spots/${current_year}" "${DEST_ROOT}/wcy/${current_year}" "${DEST_ROOT}/wwv/${current_year}"

rsync -az \
  -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" \
  "${REMOTE_HOST}:${REMOTE_BASE}/spots/${current_year}/${day_of_year}" \
  "${DEST_ROOT}/spots/${current_year}/"

rsync -az \
  -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" \
  "${REMOTE_HOST}:${REMOTE_BASE}/wcy/${current_year}/${current_month}" \
  "${DEST_ROOT}/wcy/${current_year}/"

rsync -az \
  -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" \
  "${REMOTE_HOST}:${REMOTE_BASE}/wwv/${current_year}/${current_month}" \
  "${DEST_ROOT}/wwv/${current_year}/"

printf 'synced_to=%s\n' "${DEST_ROOT}"
printf 'spot_file=%s/spots/%s/%s\n' "${DEST_ROOT}" "${current_year}" "${day_of_year}"
printf 'wcy_file=%s/wcy/%s/%s\n' "${DEST_ROOT}" "${current_year}" "${current_month}"
printf 'wwv_file=%s/wwv/%s/%s\n' "${DEST_ROOT}" "${current_year}" "${current_month}"
