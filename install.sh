#!/usr/bin/env bash
set -euo pipefail

# One command from a fresh clone to a working checkout: dependencies, the pinned Pi fork, and a
# typecheck to prove the two agree. Safe to run again after a pull; the fork is only refetched
# when fork.pin moves.

cd "$(dirname "${BASH_SOURCE[0]}")"

for tool in git node npm; do
  command -v "$tool" >/dev/null || { echo "missing: $tool" >&2; exit 1; }
done

# The Temporal TS SDK and the fork's build both assume a current LTS.
major="$(node -p 'process.versions.node.split(".")[0]')"
if [ "$major" -lt 20 ]; then
  echo "node >= 20 required, found $(node -v)" >&2
  exit 1
fi

npm ci
npm run setup-fork
npm run typecheck

cat <<'EOF'

pi-temporal is ready. Next:

  scripts/temporal-dev.sh                     # a local Temporal server (UI on :8233)
  scripts/run-pi.sh                           # pi with Temporal underneath, from your project dir
  npm run worker                              # a worker for detached sessions
  npx tsx src/cli.ts start "your task here"   # hand a task to the workers

The chaos demo needs Docker and an Anthropic key:

  ANTHROPIC_API_KEY=... demo/run.sh
EOF
