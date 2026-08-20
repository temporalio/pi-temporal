#!/usr/bin/env bash
set -euo pipefail

# A local Temporal server to try this against. Reuses one that is already listening, so running it
# again in another terminal is harmless. Ctrl-C stops it.
#
#   TEMPORAL_PORT      gRPC port, default 7233
#   TEMPORAL_UI_PORT   web UI port, default the gRPC port plus 1000
#   TEMPORAL_DB_FILE   keep workflows across restarts (in-memory otherwise)

port="${TEMPORAL_PORT:-7233}"
ui_port="${TEMPORAL_UI_PORT:-$((port + 1000))}"

command -v temporal >/dev/null || {
  echo "the temporal CLI is not on PATH. 'brew install temporal', or see https://docs.temporal.io/cli" >&2
  exit 1
}

listening() { (exec 3<>"/dev/tcp/127.0.0.1/$1") >/dev/null 2>&1; }

if listening "$port"; then
  echo "already listening on 127.0.0.1:$port, reusing it"
  echo "TEMPORAL_ADDRESS=127.0.0.1:$port"
  exit 0
fi

args=(server start-dev --port "$port" --ui-port "$ui_port" --log-level warn)
# Without a db file the dev server forgets everything on restart, which is the wrong default when
# the thing being demonstrated is that work survives a process dying.
[ -n "${TEMPORAL_DB_FILE:-}" ] && args+=(--db-filename "$TEMPORAL_DB_FILE")

echo "Temporal on 127.0.0.1:$port    UI http://127.0.0.1:$ui_port"
exec temporal "${args[@]}"
