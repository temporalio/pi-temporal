#!/usr/bin/env bash
set -euo pipefail

# End to end with no typing: start a worker, submit one turn, wait for the answer, report.
# Needs a Temporal server (scripts/temporal-dev.sh) and a model key. Exits non-zero on failure,
# so it works as a check rather than something to read.
#
# This drives the standalone worker on purpose. The worker inside pi cannot be tested headlessly:
# print mode exits as soon as the command returns, and the worker goes with it.

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

port="${TEMPORAL_PORT:-7233}"
export TEMPORAL_ADDRESS="${TEMPORAL_ADDRESS:-127.0.0.1:$port}"
export PI_TEMPORAL_PROVIDER="${PI_TEMPORAL_PROVIDER:-openai}"
export PI_MODEL="${PI_MODEL:-gpt-4o-mini}"
export PI_PROJECT_DIR="${PI_PROJECT_DIR:-$(mktemp -d)}"

if [ -z "${OPENAI_API_KEY:-}" ]; then
  key_file="${OPENAI_API_KEY_FILE:-$HOME/.config/ai363/llm.key}"
  [ -f "$key_file" ] || {
    echo "set OPENAI_API_KEY, or OPENAI_API_KEY_FILE to a file holding one" >&2
    exit 1
  }
  OPENAI_API_KEY="$(tr -d '[:space:]' < "$key_file")"
  export OPENAI_API_KEY
fi

[ -d node_modules ] || { echo "run 'npm ci && npm run setup-fork' first" >&2; exit 1; }

session="smoke-$$"
sessions_dir="${PI_SESSION_DIR:-$HOME/.pi-temporal/sessions}"
file="$sessions_dir/$session.jsonl"
word="SMOKE$$"
log="$(mktemp)"

npx tsx src/worker.ts > "$log" 2>&1 &
worker=$!
cleanup() {
  kill "$worker" 2>/dev/null || true
  wait "$worker" 2>/dev/null || true
}
trap cleanup EXIT

echo "worker pid $worker, session $session, project $PI_PROJECT_DIR"
npx tsx submit.mts "$session" "Reply with the single word $word."

for _ in $(seq 1 60); do
  sleep 2
  [ -f "$file" ] && grep -q "$word" "$file" && break
done

if [ -f "$file" ] && npx tsx inspect.mts "$file" | grep -q "$word"; then
  echo "PASS  $(npx tsx inspect.mts "$file")"
  exit 0
fi

echo "FAIL  no answer in $file" >&2
echo "--- worker log ---" >&2
tail -30 "$log" >&2
exit 1
