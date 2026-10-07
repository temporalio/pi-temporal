#!/usr/bin/env bash
set -euo pipefail

# End-to-end smoke test: start a worker, submit one turn, wait for the answer. Exits non-zero on
# failure. Needs a Temporal server (scripts/temporal-dev.sh) and a model key.
# Uses the standalone worker, since pi's print mode exits and takes the embedded worker with it.

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

port="${TEMPORAL_PORT:-7233}"
export TEMPORAL_ADDRESS="${TEMPORAL_ADDRESS:-127.0.0.1:$port}"
export PI_TEMPORAL_PROVIDER="${PI_TEMPORAL_PROVIDER:-openai}"
export PI_MODEL="${PI_MODEL:-gpt-4o-mini}"
export PI_PROJECT_DIR="${PI_PROJECT_DIR:-$(mktemp -d)}"

if [ -z "${OPENAI_API_KEY:-}" ]; then
  key_file="${OPENAI_API_KEY_FILE:-}"
  [ -n "$key_file" ] && [ -f "$key_file" ] || {
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

# Plain node in its own process group. Killing an npx wrapper would leave node polling the queue.
set -m
node --import tsx src/worker.ts > "$log" 2>&1 &
worker=$!
set +m
cleanup() {
  kill -- "-$worker" 2>/dev/null || true
  wait "$worker" 2>/dev/null || true
}
trap cleanup EXIT

echo "worker pid $worker, session $session, project $PI_PROJECT_DIR"
node --import tsx scripts/submit.mts "$session" "Reply with the single word $word."

for _ in $(seq 1 60); do
  sleep 2
  [ -f "$file" ] && grep -q "$word" "$file" && break
done

if [ -f "$file" ] && node --import tsx scripts/inspect.mts "$file" | grep -q "$word"; then
  echo "PASS  $(node --import tsx scripts/inspect.mts "$file")"
  exit 0
fi

echo "FAIL  no answer in $file" >&2
echo "--- worker log ---" >&2
tail -30 "$log" >&2
exit 1
