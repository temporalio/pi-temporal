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
case "$PI_TEMPORAL_PROVIDER" in
  openai) key_var=OPENAI_API_KEY; default_model=gpt-4o-mini ;;
  anthropic) key_var=ANTHROPIC_API_KEY; default_model=haiku ;;
  *) echo "PI_TEMPORAL_PROVIDER must be openai or anthropic" >&2; exit 1 ;;
esac
export PI_MODEL="${PI_MODEL:-$default_model}"

# Ours to delete only if we made it.
made_project=""
if [ -z "${PI_PROJECT_DIR:-}" ]; then
  made_project="$(mktemp -d)"
  export PI_PROJECT_DIR="$made_project"
fi

# The worker reads the key file itself, so the key isn't copied into this env.
key_file_var="${key_var}_FILE"
if [ -z "${!key_var:-}" ] && ! [ -f "${!key_file_var:-}" ]; then
  echo "set $key_var, or $key_file_var to a file holding one" >&2
  exit 1
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
  rm -f "$log"
  [ -z "$made_project" ] || rm -rf "$made_project"
}
trap cleanup EXIT

echo "worker pid $worker, session $session, project $PI_PROJECT_DIR"
node --import tsx scripts/submit.mts "$session" "Reply with the single word $word."

# Match the assistant's answer, not the file, which holds the prompt with the same word.
answered() {
  local seen
  [ -f "$file" ] && seen="$(node --import tsx scripts/inspect.mts "$file")" \
    && grep -q "\"lastAssistant\":\"[^\"]*$word" <<< "$seen"
}

for _ in $(seq 1 60); do
  sleep 2
  answered && break
done

if answered; then
  echo "PASS  $(node --import tsx scripts/inspect.mts "$file")"
  exit 0
fi

echo "FAIL  no answer in $file" >&2
echo "--- worker log ---" >&2
tail -30 "$log" >&2
exit 1
