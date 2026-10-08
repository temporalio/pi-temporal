#!/usr/bin/env bash
set -euo pipefail

# Installs dependencies and the pinned Pi fork, then typechecks. Safe to re-run. The fork is
# refetched only when fork.pin changes.

cd "$(dirname "${BASH_SOURCE[0]}")"

for tool in git node npm; do
  command -v "$tool" >/dev/null || { echo "missing: $tool" >&2; exit 1; }
done

# The Pi fork needs the newer of its own floor and the Temporal TS SDK's (20.3.0). Full version,
# not the major, since a minor below the floor installs but doesn't run.
need=22.19.0
if ! node -e '
  const parts = (v) => v.split(".").map(Number);
  const [have, need] = [parts(process.versions.node), parts(process.argv[1])];
  const diff = have.map((part, i) => part - need[i]).find((d) => d !== 0) ?? 0;
  process.exit(diff < 0 ? 1 : 0);
' "$need"; then
  echo "node >= $need required, found $(node -v)" >&2
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
