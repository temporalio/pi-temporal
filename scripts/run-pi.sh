#!/usr/bin/env bash
set -euo pipefail

# Runs the fork's pi with the pi-temporal extension, against the local Temporal server. Extra
# arguments go to pi. Launch it from the project you want `/background` tasks to work on.
#
#   TEMPORAL_PORT          which server to talk to, default 7233
#   OPENAI_API_KEY         the key for the background task's model
#   OPENAI_API_KEY_FILE    a file holding one instead
#   PI_MODEL               model for the background task, default gpt-4o-mini
#   PI_TEMPORAL_PROVIDER   its provider, default openai

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cli="$root/.fork/pi/packages/coding-agent/dist/cli.js"

[ -f "$cli" ] || {
  echo "no pi build at $cli. Run 'npm ci && npm run setup-fork' first." >&2
  exit 1
}

port="${TEMPORAL_PORT:-7233}"
export TEMPORAL_ADDRESS="${TEMPORAL_ADDRESS:-127.0.0.1:$port}"

# Pinned so tests don't depend on the TUI's selected model. The key must match the provider.
export PI_TEMPORAL_PROVIDER="${PI_TEMPORAL_PROVIDER:-openai}"
export PI_MODEL="${PI_MODEL:-gpt-4o-mini}"

if [ -z "${OPENAI_API_KEY:-}" ]; then
  key_file="${OPENAI_API_KEY_FILE:-}"
  [ -n "$key_file" ] && [ -f "$key_file" ] || {
    echo "set OPENAI_API_KEY, or OPENAI_API_KEY_FILE to a file holding one" >&2
    exit 1
  }
  OPENAI_API_KEY="$(tr -d '[:space:]' < "$key_file")"
  export OPENAI_API_KEY
fi

exec node "$cli" -e "$root/extensions/temporal.ts" "$@"
