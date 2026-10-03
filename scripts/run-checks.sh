#!/usr/bin/env bash
# Every check that needs neither a model key nor Docker, one after another, against the server at
# TEMPORAL_ADDRESS (default 127.0.0.1:7233). Exits non-zero if any fails, and prints the tail of
# each failure's output. This is what CI runs, so a lock, claim or tree regression cannot merge
# green just because nothing ran it.
#
# Needs `npm ci && npm run setup-fork` first, and a Temporal server (scripts/temporal-dev.sh).

set -uo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

logs="$(mktemp -d)"
failed=()
for check in *-check.mts; do
  case "$check" in
    # Drives a real model, so it needs a key.
    detached-check.mts) continue ;;
    # Reads /proc, which only Linux has.
    liveness-linux-check.mts) [ "$(uname -s)" = "Linux" ] || continue ;;
  esac
  log="$logs/${check%.mts}.log"
  if node --import tsx "$check" > "$log" 2>&1; then
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
