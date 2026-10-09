#!/usr/bin/env bash
# Runs every check that needs no model key and no Docker, against TEMPORAL_ADDRESS (default
# 127.0.0.1:7233). CI runs this. Prints the tail of each failure and exits non-zero if any fail.
# Needs `npm ci && npm run setup-fork` and a Temporal server (scripts/temporal-dev.sh).

set -uo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

logs="$(mktemp -d)"
failed=()
for path in checks/*-check.mts; do
  check="$(basename "$path")"
  case "$check" in
    # Drives a real model, so it needs a key.
    detached-check.mts) continue ;;
    # Reads /proc, which only Linux has.
    liveness-linux-check.mts) [ "$(uname -s)" = "Linux" ] || continue ;;
  esac
  log="$logs/${check%.mts}.log"
  if node --import tsx "$path" > "$log" 2>&1; then
    echo "ok    $check"
  else
    echo "FAIL  $check"
    tail -40 "$log" | sed 's/^/      /'
    failed+=("$check")
  fi
done

if [ "${#failed[@]}" -gt 0 ]; then
  echo "${#failed[@]} failed: ${failed[*]}"
  exit 1
fi
echo "all checks passed"
