#!/usr/bin/env bash
set -euo pipefail

# Fetches and builds the pinned Pi fork into .fork/pi and symlinks it into node_modules. Run it
# after `npm ci`, which removes the link.
# A symlink, not a `file:` dependency. npm would pull in the fork's devDeps, and `@types/ms`
# shadows the `Duration` types @temporalio/common needs. The fork also resolves its siblings.

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=../fork.pin
. "$root/fork.pin"

dir="$root/.fork/pi"
mkdir -p "$dir"
[ -d "$dir/.git" ] || git init -q "$dir"

git -C "$dir" remote add origin "$PI_FORK_REPO" 2>/dev/null \
  || git -C "$dir" remote set-url origin "$PI_FORK_REPO"

# Edited fork sources would be built and linked as if they were the pinned commit. Refused, not
# reset, so local work on the fork isn't lost.
if [ -n "$(git -C "$dir" status --porcelain 2>/dev/null)" ]; then
  echo "the fork at $dir has uncommitted changes, so it would not build the pinned commit." >&2
  echo "commit or stash them, or delete $dir to fetch it again." >&2
  exit 1
fi

# Shallow fetch of the exact commit, so the checkout doesn't drift.
if [ "$(git -C "$dir" rev-parse -q --verify HEAD || true)" != "$PI_FORK_REF" ]; then
  git -C "$dir" fetch --depth 1 origin "$PI_FORK_REF"
  git -C "$dir" checkout -q --detach FETCH_HEAD
fi

# Build offline, with the model data published for this pi-ai version. The model types come from
# that data, so today's catalog can break the build of a pin that built yesterday. A published
# version never changes.
(
  cd "$dir"
  npm ci
  ai="$(node -p 'const p = require("./packages/ai/package.json"); `${p.name}@${p.version}`')"
  data="$(mktemp -d)"
  trap 'rm -rf "$data"' EXIT
  npm pack --silent --pack-destination "$data" "$ai" >/dev/null
  tar -xzf "$data"/*.tgz -C "$data"
  rm -rf packages/ai/src/providers/data
  cp -R "$data/package/dist/providers/data" packages/ai/src/providers/data
  npm run build:offline
)

link="$root/node_modules/@earendil-works/pi-coding-agent"
mkdir -p "$(dirname "$link")"
rm -rf "$link"
ln -s "../../.fork/pi/packages/coding-agent" "$link"

echo "Pi fork ready at $dir ($PI_FORK_REF), linked into node_modules"
