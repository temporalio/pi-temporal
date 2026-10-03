#!/usr/bin/env bash
# The writer-marker readings, asked on Linux, where a fleet asks them.
#
# `liveness-linux-check.mts` runs inside a worker image: no Temporal, no model key, no volumes
# shared with anything. The source is mounted over the image's copy so a code change does not need
# a rebuild.
#
# Usage: docker/liveness-check.sh

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

# shellcheck source=../scripts/pinned-fork.sh
. scripts/pinned-fork.sh

pinned_fork

# Every run, and layer-cached when nothing changed. The source this checks is mounted over the
# image below, but what it is mounted onto is the fork and the runtime, and an image older than
# those runs code nobody wrote today.
docker build -q -f docker/Dockerfile -t pi-temporal:l3 . >/dev/null \
  || { echo "build failed"; exit 1; }

# `--user root` is not asked for: the point is the readings this worker's own user can make.
exec docker run --rm \
  -v "$PWD/src:/app/src:ro" \
  -v "$PWD/checks/liveness-linux-check.mts:/app/checks/liveness-linux-check.mts:ro" \
  -e PI_TEMPORAL_DATA=/tmp/data \
  pi-temporal:l3 \
  node --import tsx checks/liveness-linux-check.mts
