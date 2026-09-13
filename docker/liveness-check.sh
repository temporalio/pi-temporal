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

if ! docker image inspect pi-temporal:l3 >/dev/null 2>&1; then
  echo "building pi-temporal:l3 (once)"
  docker build -f docker/Dockerfile -t pi-temporal:l3 . || exit 1
fi

# `--user root` is not asked for: the point is the readings this worker's own user can make.
exec docker run --rm \
  -v "$PWD/src:/app/src:ro" \
  -v "$PWD/liveness-linux-check.mts:/app/liveness-linux-check.mts:ro" \
  -e PI_TEMPORAL_DATA=/tmp/data \
  pi-temporal:l3 \
  node --import tsx liveness-linux-check.mts
