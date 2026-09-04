#!/usr/bin/env bash
# The project's files moving between machines, which is the half the session log does not cover.
#
# Two workers, each a container with its own `/project`. They share the session directory and
# nothing else, so a file written on the first host reaches the second only by travelling with the
# session. A session writes a file, the host that wrote it is killed, and the next turn runs on a
# host whose project directory has never held anything.
#
# Usage: OPENAI_API_KEY=... docker/tree-check.sh
#
# KEEP=1 leaves the stack up to look at. NFS=1 puts the session directory on a real NFSv4 server
# instead of a local volume, which is the filesystem the lock's caveats are about.

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
COMPOSE="docker compose -f docker/compose.yml"
# The same checks over a real network filesystem. A local volume answers the exclusive-create and
# atomic-rename questions by construction, which is what the lock rests on, so it proves them only
# where a fleet does not run.
[ -n "${NFS:-}" ] && COMPOSE="$COMPOSE -f docker/compose.nfs.yml"
export PI_TEMPORAL_TASK_QUEUE="pi-tree-$$"
export PI_TEMPORAL_SHIP_TREE=1

fails=0
ok()  { printf 'PASS %s\n' "$1"; }
bad() { printf 'FAIL %s   (%s)\n' "$1" "${2:-}"; fails=$((fails + 1)); }

cleanup() { [ -n "${KEEP:-}" ] || $COMPOSE down -v >/dev/null 2>&1; }
trap cleanup EXIT

[ -n "${OPENAI_API_KEY:-}" ] || { echo "set OPENAI_API_KEY"; exit 1; }

$COMPOSE down -v >/dev/null 2>&1
docker build -q -f docker/Dockerfile -t pi-temporal:l3 . >/dev/null || { echo "build failed"; exit 1; }
# One worker for the first half, so the second one is provably a host that has never seen this
# project. The second half brings both up, which is the only way a step's activities get spread.
# The file server first, and waited for. The session directory is a volume the DAEMON mounts, and
# it does that while creating a container rather than while starting it, so `depends_on` is too
# late: the mount is attempted before anything has waited for the export to exist.
if [ -n "${NFS:-}" ]; then
  $COMPOSE up -d --wait nfs >/dev/null 2>&1 || { echo "the file server did not start"; exit 1; }
fi
$COMPOSE up -d temporal worker-a >/dev/null 2>&1 || { echo "stack failed to start"; exit 1; }

hostA=$($COMPOSE exec -T worker-a hostname 2>/dev/null | tr -d '\r')
[ -n "$hostA" ] && ok "worker A is up ($hostA)" || { bad "worker A came up"; exit 1; }

# Something already in the project, so the check covers a tree that has content before the agent
# touches it rather than only what the agent creates. On the CLIENT, because the client is what
# sends the project: no worker may establish it, since a worker is whichever one Temporal picked.
$COMPOSE run --rm -T --entrypoint sh client -c 'echo seeded > /project/seed.txt' >/dev/null 2>&1

sid=$($COMPOSE run --rm -T client start --project=/project \
  "Use the bash tool to run exactly: echo CARRIED > /project/note.txt. Then reply DONE." \
  2>/dev/null | tr -d '\r' | head -1)
[ -n "$sid" ] && ok "a client started the session ($sid)" || { bad "no session"; exit 1; }

# Wait for the turn rather than for a duration, then confirm the file is really on host A.
$COMPOSE run --rm -T client watch "$sid" >/dev/null 2>&1
wrote=$($COMPOSE exec -T worker-a sh -c 'cat /project/note.txt 2>&1' 2>/dev/null | tr -d '\r')
case "$wrote" in
  CARRIED*) ok "worker A wrote the file" ;;
  *) bad "worker A wrote the file" "$wrote"; exit 1 ;;
esac

shipped=$($COMPOSE exec -T worker-a sh -c "ls /sessions/$sid.jsonl.tree 2>/dev/null | wc -l" 2>/dev/null | tr -d '\r ')
[ "${shipped:-0}" -gt 0 ] && ok "the tree was shipped beside the session ($shipped files)" \
  || bad "nothing was shipped" "$shipped"

# --- the host that wrote it goes away, and one that has never seen the project takes over
docker kill "$($COMPOSE ps -q worker-a)" >/dev/null 2>&1
sleep 2
[ -z "$($COMPOSE ps -q --status running worker-a)" ] && ok "worker A's host is gone" || bad "worker A's host is gone"

$COMPOSE up -d worker-b >/dev/null 2>&1
sleep 8
hostB=$($COMPOSE exec -T worker-b hostname 2>/dev/null | tr -d '\r')
[ "$hostB" != "$hostA" ] && ok "worker B is a different host ($hostB)" || bad "worker B is a different host"
empty=$($COMPOSE exec -T worker-b sh -c 'ls -A /project | wc -l' 2>/dev/null | tr -d '\r ')
[ "${empty:-1}" = "0" ] && ok "worker B's project is empty before the turn" || bad "worker B's project was not empty" "$empty"

# --- the same session, on the other host. The file exists there only if the tree travelled.
$COMPOSE run --rm -T client start --project=/project \
  "Use the bash tool to run exactly: cat /project/note.txt /project/seed.txt. Report what it printed." \
  --session="$sid" >/dev/null 2>&1
$COMPOSE run --rm -T client watch "$sid" >/dev/null 2>&1

# Asked of worker B's own disk, not of the transcript: the transcript still holds turn 1, where the
# file did exist, so matching it anywhere would prove nothing about B.
landed=$($COMPOSE exec -T worker-b sh -c 'cat /project/note.txt 2>&1' 2>/dev/null | tr -d '\r')
case "$landed" in
  CARRIED*) ok "the file the agent wrote travelled to worker B" ;;
  *) bad "the file the agent wrote travelled to worker B" "$landed" ;;
esac
seed=$($COMPOSE exec -T worker-b sh -c 'cat /project/seed.txt 2>&1' 2>/dev/null | tr -d '\r')
case "$seed" in
  seeded*) ok "the rest of the project travelled too" ;;
  *) bad "the rest of the project travelled too" "$seed" ;;
esac

# --- both hosts polling, so a step's three activities can be dispatched to different ones. The
# model call, each tool call and the seal are separate dispatches: nothing pins them together.
$COMPOSE up -d worker-a >/dev/null 2>&1
sleep 8
$COMPOSE run --rm -T client start --project=/project \
  "Use the bash tool to run exactly: echo SPREAD > /project/spread.txt. Then reply DONE." \
  --session="$sid" >/dev/null 2>&1
$COMPOSE run --rm -T client watch "$sid" >/dev/null 2>&1
$COMPOSE run --rm -T client start --project=/project \
  "Use the bash tool to run exactly: cat /project/spread.txt. Report what it printed." \
  --session="$sid" >/dev/null 2>&1
$COMPOSE run --rm -T client watch "$sid" >/dev/null 2>&1

# What the host that ran the read saw, which is the claim. Asserting both hosts hold the file is
# stronger but flaky: nothing forces a turn's activities to spread, so a whole turn can land on one
# host and the check would fail with nothing wrong.
read_back=$($COMPOSE run --rm -T client watch "$sid" 2>&1 | tr -d '\r')
case "$read_back" in
  *"tool result: SPREAD"*) ok "with both hosts polling, the one that read it saw the other's work" ;;
  *) bad "with both hosts polling, the read saw the write" "$(printf '%s' "$read_back" | tail -3)" ;;
esac

echo
[ "$fails" -eq 0 ] && echo "tree-check: OK" || echo "tree-check: $fails failed"
exit $([ "$fails" -eq 0 ] && echo 0 || echo 1)
