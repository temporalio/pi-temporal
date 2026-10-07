#!/usr/bin/env bash
# Kills a worker mid-tool and restarts the same container (as `docker start` or a Kubernetes
# restart does). It comes back with the same pid on the same boot, so its old writer marker looks
# live. Asserts the project directory is freed, not refused forever.
# Needs Docker only. No model key, no Temporal.
#
# Usage: docker/restart-check.sh

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

# shellcheck source=../scripts/pinned-fork.sh
. scripts/pinned-fork.sh
pinned_fork

name="pi-restart-check-$$"
cleanup() { docker rm -fv "$name" >/dev/null 2>&1; }
trap cleanup EXIT

build="$(docker build -q -f docker/Dockerfile -t pi-temporal:l3 . 2>&1)" \
  || { echo "build failed:"; echo "$build"; exit 1; }

# Mount this checkout's source. Mounts and /work survive the restart. /project is its own volume,
# so it can't be moved aside.
docker run -d --name "$name" \
  -v "$PWD/src:/app/src:ro" \
  -v "$PWD/checks/same-container-restart.mts:/app/checks/same-container-restart.mts:ro" \
  -v /project \
  -e PI_TEMPORAL_DATA=/work/data \
  pi-temporal:l3 \
  node --import tsx checks/same-container-restart.mts >/dev/null \
  || { echo "the container did not start"; exit 1; }

marked() { docker logs "$name" 2>&1 | grep '^MARKED' >/dev/null; }
for _ in $(seq 1 120); do
  marked && break
  sleep 1
done
marked || { echo "FAIL the first boot never marked a tool call"; docker logs "$name"; exit 1; }

docker kill "$name" >/dev/null
docker start "$name" >/dev/null || { echo "FAIL the container did not start again"; exit 1; }
for _ in $(seq 1 120); do
  [ "$(docker inspect -f '{{.State.Running}}' "$name")" = false ] && break
  sleep 1
done
code="$(docker inspect -f '{{if .State.Running}}timeout{{else}}{{.State.ExitCode}}{{end}}' "$name")"

docker logs "$name" 2>&1 | grep -E '^(MARKED|booted|PASS|FAIL|\{)'
if [ "$code" = "0" ]; then
  echo "restart-check: OK"
  exit 0
fi
echo "restart-check: failed (exit $code)"
exit 1
