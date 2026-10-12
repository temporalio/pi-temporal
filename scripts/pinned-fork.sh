#!/usr/bin/env bash
# Sourced, not run, from the repository root. `pinned_fork` exits unless `.fork/pi` is a clean
# checkout of the commit in fork.pin. Images and linked-package checks use `.fork/pi`, so a stale
# checkout would build or test the wrong commit.
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
    echo "the fork at .fork/pi has uncommitted changes: commit or stash them," \
      "then run npm run setup-fork"
    exit 1
  }
}
