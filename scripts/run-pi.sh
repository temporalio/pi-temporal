#!/usr/bin/env bash
set -euo pipefail

# Runs the fork's pi with the durable extension loaded, pointed at the local Temporal server.
# Type /durable <task> once it is up. Extra arguments go through to pi.
#
# The durable task's tools run in the directory you launch this from, so launch it in the project
# you want the task to work on.
#
#   TEMPORAL_PORT          which server to talk to, default 7233
#   OPENAI_API_KEY         the key for the durable task's model
#   OPENAI_API_KEY_FILE    a file holding one instead
#   PI_MODEL               model for the durable task, default gpt-4o-mini
#   PI_TEMPORAL_PROVIDER   its provider, default openai

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cli="$root/.fork/pi/packages/coding-agent/dist/cli.js"

[ -f "$cli" ] || {
  echo "no pi build at $cli. Run 'npm ci && npm run setup-fork' first." >&2
  exit 1
}

port="${TEMPORAL_PORT:-7233}"
export TEMPORAL_ADDRESS="${TEMPORAL_ADDRESS:-127.0.0.1:$port}"

# Pinned so a test does not depend on which model the TUI happens to have selected. The key has to
# belong to the same provider.
export PI_TEMPORAL_PROVIDER="${PI_TEMPORAL_PROVIDER:-openai}"
export PI_MODEL="${PI_MODEL:-gpt-4o-mini}"

if [ -z "${OPENAI_API_KEY:-}" ]; then
  key_file="${OPENAI_API_KEY_FILE:-$HOME/.config/ai363/llm.key}"
  [ -f "$key_file" ] || {
    echo "set OPENAI_API_KEY, or OPENAI_API_KEY_FILE to a file holding one" >&2
    exit 1
  }
  OPENAI_API_KEY="$(tr -d '[:space:]' < "$key_file")"
  export OPENAI_API_KEY
fi

exec node "$cli" -e "$root/extensions/temporal.ts" "$@"
