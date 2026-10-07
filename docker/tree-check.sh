#!/usr/bin/env bash
# Checks that project files travel between hosts with the session. Each worker container has its
# own `/project`. A turn writes a file, that host is killed, and the next turn runs on a host
# whose project directory is empty.
#
# Usage: OPENAI_API_KEY=... docker/tree-check.sh
#
# KEEP=1 leaves the stack up. NFS=1 puts the session directory on a real NFSv4 server.

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
COMPOSE="docker compose -f docker/compose.yml"
[ -n "${NFS:-}" ] && COMPOSE="$COMPOSE -f docker/compose.nfs.yml"
export PI_TEMPORAL_TASK_QUEUE="pi-tree-$$"
export PI_TEMPORAL_SHIP_TREE=1

fails=0
ok()  { printf 'PASS %s\n' "$1"; }
bad() { printf 'FAIL %s   (%s)\n' "$1" "${2:-}"; fails=$((fails + 1)); }

cleanup() { [ -n "${KEEP:-}" ] || $COMPOSE down -v >/dev/null 2>&1; }
trap cleanup EXIT

[ -n "${OPENAI_API_KEY:-}" ] || { echo "set OPENAI_API_KEY"; exit 1; }

# shellcheck source=../scripts/pinned-fork.sh
. scripts/pinned-fork.sh

pinned_fork

$COMPOSE down -v >/dev/null 2>&1
docker build -q -f docker/Dockerfile -t pi-temporal:l3 . >/dev/null \
  || { echo "build failed"; exit 1; }
# One worker first, so worker B has provably never seen the project.
# Wait for the NFS server explicitly. The daemon mounts the volume at container create time,
# before `depends_on` waits for anything.
if [ -n "${NFS:-}" ]; then
  $COMPOSE up -d --wait nfs >/dev/null 2>&1 || { echo "the file server did not start"; exit 1; }
fi
$COMPOSE up -d temporal worker-a >/dev/null 2>&1 || { echo "stack failed to start"; exit 1; }

hostA=$($COMPOSE exec -T worker-a hostname 2>/dev/null | tr -d '\r')
[ -n "$hostA" ] && ok "worker A is up ($hostA)" || { bad "worker A came up"; exit 1; }

# Pre-existing content, seeded on the client, since the client is what sends the project.
$COMPOSE run --rm -T --entrypoint sh client -c 'echo seeded > /project/seed.txt' >/dev/null 2>&1

sid=$($COMPOSE run --rm -T client start --project=/project \
  "Use the bash tool to run exactly: echo CARRIED > /project/note.txt. Then reply DONE." \
  2>/dev/null | tr -d '\r' | head -1)
[ -n "$sid" ] && ok "a client started the session ($sid)" || { bad "no session"; exit 1; }

# Wait for the turn, then confirm the file is on host A.
$COMPOSE run --rm -T client watch "$sid" >/dev/null 2>&1
wrote=$($COMPOSE exec -T worker-a sh -c 'cat /project/note.txt 2>&1' 2>/dev/null | tr -d '\r')
case "$wrote" in
  CARRIED*) ok "worker A wrote the file" ;;
  *) bad "worker A wrote the file" "$wrote"; exit 1 ;;
esac

shipped=$($COMPOSE exec -T worker-a sh -c "ls /sessions/$sid.jsonl.tree 2>/dev/null | wc -l" \
  2>/dev/null | tr -d '\r ')
[ "${shipped:-0}" -gt 0 ] && ok "the tree was shipped beside the session ($shipped files)" \
  || bad "nothing was shipped" "$shipped"

# --- the host that wrote it goes away, and one that has never seen the project takes over
docker kill "$($COMPOSE ps -q worker-a)" >/dev/null 2>&1
sleep 2
[ -z "$($COMPOSE ps -q --status running worker-a)" ] \
  && ok "worker A's host is gone" || bad "worker A's host is gone"

$COMPOSE up -d worker-b >/dev/null 2>&1
sleep 8
hostB=$($COMPOSE exec -T worker-b hostname 2>/dev/null | tr -d '\r')
[ "$hostB" != "$hostA" ] \
  && ok "worker B is a different host ($hostB)" || bad "worker B is a different host"
empty=$($COMPOSE exec -T worker-b sh -c 'ls -A /project | wc -l' 2>/dev/null | tr -d '\r ')
[ "${empty:-1}" = "0" ] \
  && ok "worker B's project is empty before the turn" \
  || bad "worker B's project was not empty" "$empty"

# --- the same session on host B. The file is there only if the tree travelled.
task="Use the bash tool to run exactly: cat /project/note.txt /project/seed.txt. "
task+="Report what it printed."
$COMPOSE run --rm -T client start --project=/project "$task" \
  --session="$sid" >/dev/null 2>&1
$COMPOSE run --rm -T client watch "$sid" >/dev/null 2>&1

# Check B's disk, not the transcript, which still holds turn 1's output.
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

# --- both hosts polling, so a step's activities may land on different hosts.
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

# Assert what the reading host saw. Checking both hosts' disks would flake, since nothing forces
# a turn's activities to spread.
read_back=$($COMPOSE run --rm -T client watch "$sid" 2>&1 | tr -d '\r')
case "$read_back" in
  *"tool result: SPREAD"*)
    ok "with both hosts polling, the one that read it saw the other's work" ;;
  *)
    bad "with both hosts polling, the read saw the write" \
      "$(printf '%s' "$read_back" | tail -3)" ;;
esac

echo
[ "$fails" -eq 0 ] && echo "tree-check: OK" || echo "tree-check: $fails failed"
exit $([ "$fails" -eq 0 ] && echo 0 || echo 1)
