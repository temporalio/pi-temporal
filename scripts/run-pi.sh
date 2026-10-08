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
case "$PI_TEMPORAL_PROVIDER" in
  openai) key_var=OPENAI_API_KEY; default_model=gpt-4o-mini ;;
  anthropic) key_var=ANTHROPIC_API_KEY; default_model=haiku ;;
  *) echo "PI_TEMPORAL_PROVIDER must be openai or anthropic" >&2; exit 1 ;;
esac
export PI_MODEL="${PI_MODEL:-$default_model}"

# pi reads the key from the environment, not from a file.
if [ -z "${!key_var:-}" ]; then
  key_file_var="${key_var}_FILE"
  key_file="${!key_file_var:-}"
  [ -n "$key_file" ] && [ -f "$key_file" ] || {
    echo "set $key_var, or $key_file_var to a file holding one" >&2
    exit 1
  }
  export "$key_var=$(tr -d '[:space:]' < "$key_file")"
fi

exec node "$cli" -e "$root/extensions/temporal.ts" "$@"
