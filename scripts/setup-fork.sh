#!/usr/bin/env bash
set -euo pipefail

# Fetches and builds the pinned Pi fork into .fork/pi, then links it into node_modules. Run it
# after npm ci, which wipes node_modules and would take the link with it.
#
# A symlink rather than a "file:" dependency on purpose. npm treats a path dependency as a
# workspace and pulls the fork's devDeps into this tree, and one of them (@types/ms) shadows the
# types @temporalio/common needs for Duration. A symlink also lets the fork resolve its own
# siblings (pi-agent-core and friends) from its workspace, which is where the fork API lives.

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=../fork.pin
. "$root/fork.pin"

dir="$root/.fork/pi"
mkdir -p "$dir"
[ -d "$dir/.git" ] || git init -q "$dir"

git -C "$dir" remote add origin "$PI_FORK_REPO" 2>/dev/null \
  || git -C "$dir" remote set-url origin "$PI_FORK_REPO"

# A shallow fetch of the exact commit, so the checkout does not drift when the branch moves.
if [ "$(git -C "$dir" rev-parse -q --verify HEAD || true)" != "$PI_FORK_REF" ]; then
  git -C "$dir" fetch --depth 1 origin "$PI_FORK_REF"
  git -C "$dir" checkout -q --detach FETCH_HEAD
fi

# The full build, not build:offline: packages/ai keeps its model data out of git, so a fresh
# checkout has to fetch it before anything compiles.
(cd "$dir" && npm ci && npm run build)

link="$root/node_modules/@earendil-works/pi-coding-agent"
mkdir -p "$(dirname "$link")"
rm -rf "$link"
ln -s "../../.fork/pi/packages/coding-agent" "$link"

echo "Pi fork ready at $dir ($PI_FORK_REF), linked into node_modules"
