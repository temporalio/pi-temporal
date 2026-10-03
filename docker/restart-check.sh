#!/usr/bin/env bash
# A worker killed mid-tool and brought back as the same container, which is what `docker start`, a
# restart policy and a Kubernetes container restart all do. It comes back as the same pid on the
# same boot, in the same groups, with the same environment, so every reading of the marker its
# predecessor left says that writer is still running unless something tells the two processes
# apart. What is asserted is that the directory comes back instead of staying refused for good.
#
# Needs Docker and no model key, and no Temporal: it drives the tree store directly.
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

# The source is mounted over the image's copy, so the code under test is this checkout's. Both
# mounts survive a restart of the container, and so does everything it wrote under /work. The
# project is a volume of its own, which cannot be moved aside.
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
