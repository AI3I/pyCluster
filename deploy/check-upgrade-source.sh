#!/usr/bin/env bash
set -euo pipefail

root="${1:-}"
[ -n "$root" ] && [ "${root#/}" != "$root" ] && [ -d "$root" ] || {
  echo "invalid upgrade source: $root" >&2
  exit 1
}

# Every ancestor must prevent an unprivileged account from replacing the
# checked tree after this preflight and before the root worker starts.
path="$root"
while :; do
  owner="$(stat -c '%u' "$path")"
  mode="$(stat -c '%a' "$path")"
  if [ "$owner" != "0" ] || [ $((8#$mode & 022)) -ne 0 ]; then
    echo "upgrade source path is not root-controlled: $path" >&2
    exit 1
  fi
  [ "$path" = "/" ] && break
  path="$(dirname "$path")"
done

# Python imports and root-run Git operations must not consume a writable,
# non-root-owned, or symlinked object from the checkout.
bad="$(find "$root" -xdev \( -type l -o ! -user root -o -perm /022 \) -print -quit)"
if [ -n "$bad" ]; then
  echo "upgrade source contains an untrusted object: $bad" >&2
  exit 1
fi

[ -f "$root/scripts/run_upgrade_request.py" ] || {
  echo "upgrade worker is missing from $root" >&2
  exit 1
}
