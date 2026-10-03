#!/usr/bin/env bash
# Sourced, not run. Defines `pinned_fork`, which refuses to go on unless `.fork/pi` is the commit
# fork.pin names with nothing changed in it. Run from the repository root.
#
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
