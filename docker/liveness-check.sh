#!/usr/bin/env bash
# Runs `liveness-linux-check.mts` (writer-marker liveness on Linux) inside a worker image. Needs
# no Temporal and no model key. The source is mounted in, so code changes need no rebuild.
#
# Usage: docker/liveness-check.sh

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

# shellcheck source=../scripts/pinned-fork.sh
. scripts/pinned-fork.sh

pinned_fork

# Rebuild every run (layer-cached) so the fork and runtime under the mounted source are current.
docker build -q -f docker/Dockerfile -t pi-temporal:l3 . >/dev/null \
  || { echo "build failed"; exit 1; }

# The image has no `USER`, so pick the non-root `node` user here. The check is about what a
# process without root can read in `/proc`.
exec docker run --rm --user node \
  -v "$PWD/src:/app/src:ro" \
  -v "$PWD/checks/liveness-linux-check.mts:/app/checks/liveness-linux-check.mts:ro" \
  -e PI_TEMPORAL_DATA=/tmp/data \
  pi-temporal:l3 \
  node --import tsx checks/liveness-linux-check.mts
