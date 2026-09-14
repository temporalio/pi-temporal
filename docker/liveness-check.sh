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

# The fork the image is built from, at the commit this driver pins. The image is rebuilt every run,
# so it always carries whatever `.fork/pi` holds, and what that holds is whatever `setup-fork.sh`
# last put there. A checkout left on an older commit builds an image that passes for current.
pinned_fork() {
  # shellcheck disable=SC1091
  . ./fork.pin
  local at
  at="$(git -C .fork/pi rev-parse -q --verify HEAD 2>/dev/null || true)"
  [ -n "$at" ] || { echo "no fork at .fork/pi: run npm run setup-fork"; exit 1; }
  [ "$at" = "$PI_FORK_REF" ] || {
    echo "the fork at .fork/pi is $at, and fork.pin says $PI_FORK_REF: run npm run setup-fork"
    exit 1
  }
  [ -z "$(git -C .fork/pi status --porcelain)" ] || {
    echo "the fork at .fork/pi has uncommitted changes, so the image would not be the pinned build"
    exit 1
  }
}

pinned_fork

# Every run, and layer-cached when nothing changed. The source this checks is mounted over the
# image below, but what it is mounted onto is the fork and the runtime, and an image older than
# those runs code nobody wrote today.
docker build -q -f docker/Dockerfile -t pi-temporal:l3 . >/dev/null || { echo "build failed"; exit 1; }

# `--user root` is not asked for: the point is the readings this worker's own user can make.
exec docker run --rm \
  -v "$PWD/src:/app/src:ro" \
  -v "$PWD/liveness-linux-check.mts:/app/liveness-linux-check.mts:ro" \
  -e PI_TEMPORAL_DATA=/tmp/data \
  pi-temporal:l3 \
  node --import tsx liveness-linux-check.mts
