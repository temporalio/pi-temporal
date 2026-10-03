#!/usr/bin/env bash
# The handover across two machines rather than two processes.
#
# On one host, "another worker" is another process reading the same disk, and a check that kills one
# and starts the other proves less than it looks like. Here each worker is a container: its own
# filesystem, its own hostname, and no way to reach the other except through Temporal and the shared
# session directory. The evidence is Temporal's own: the activity restarts as attempt 2 under a
# different worker identity, and the identity is the container's hostname.
#
# Usage: OPENAI_API_KEY=... docker/cross-host-check.sh
#
# Not covered: /sessions is a local volume, so the O_EXCL caveats in session-lock.ts (NFSv3) are
# still untested. This shows separate hosts, not a separate filesystem implementation.

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
COMPOSE="docker compose -f docker/compose.yml"
export PI_TEMPORAL_TASK_QUEUE="pi-l3-$$"

fails=0
ok()  { printf 'PASS %s\n' "$1"; }
bad() { printf 'FAIL %s   (%s)\n' "$1" "${2:-}"; fails=$((fails + 1)); }

cleanup() { $COMPOSE down -v >/dev/null 2>&1; }
trap cleanup EXIT

[ -n "${OPENAI_API_KEY:-}" ] || { echo "set OPENAI_API_KEY"; exit 1; }

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

$COMPOSE down -v >/dev/null 2>&1
docker build -q -f docker/Dockerfile -t pi-temporal:l3 . >/dev/null \
  || { echo "build failed"; exit 1; }
$COMPOSE up -d temporal worker-a >/dev/null 2>&1 || { echo "stack failed to start"; exit 1; }

hostA=$($COMPOSE exec -T worker-a hostname 2>/dev/null | tr -d '\r')
[ -n "$hostA" ] && ok "worker A is a host of its own ($hostA)" \
  || { bad "worker A came up"; exit 1; }

# --- a client that is only ever a client hands over a task and exits
task="Run this exact command with the bash tool: sleep 45 && echo CROSS-HOST "
task+=">> /sessions/ran.txt. Then report it."
sid=$($COMPOSE run --rm -T client start "$task" \
  2>/dev/null | tr -d '\r' | head -1)
[ -n "$sid" ] && ok "a client container started the session ($sid)" \
  || { bad "client could not start a session"; exit 1; }

# --- kill worker A while the tool is genuinely in flight. Waiting on the transcript rather than on
# a clock, because a fixed sleep is how the kill ends up landing after the turn already finished.
inflight=""
for _ in $(seq 1 60); do
  if $COMPOSE exec -T worker-a sh -c \
      "grep -q toolCall /sessions/$sid.jsonl 2>/dev/null \
        && ! grep -q toolResult /sessions/$sid.jsonl"; then
    inflight=yes
    break
  fi
  sleep 2
done
[ -n "$inflight" ] && ok "a tool is in flight on worker A" || bad "a tool never started on worker A"

docker kill "$($COMPOSE ps -q worker-a)" >/dev/null 2>&1
sleep 2
[ -z "$($COMPOSE ps -q --status running worker-a)" ] && ok "worker A's host is gone" \
  || bad "worker A's host is gone"

# --- a second host, which has never seen this session or its files
$COMPOSE up -d worker-b >/dev/null 2>&1
hostB=$($COMPOSE exec -T worker-b hostname 2>/dev/null | tr -d '\r')
[ -n "$hostB" ] && [ "$hostB" != "$hostA" ] && ok "worker B is a different host ($hostB)" \
  || bad "worker B is a different host" "A=$hostA B=$hostB"

# --- follow it from a third container, and let it tell us when the turn ended
watched=$($COMPOSE run --rm -T client watch "$sid" 2>&1 | tr -d '\r')
case "$watched" in
  *answered*) ok "a client container followed the turn to its end" ;;
  *) bad "a client container followed the turn to its end" "$(printf '%s' "$watched" | tail -2)" ;;
esac
case "$watched" in
  *"outcome of this tool call is unknown"*)
    ok "the tool that may have run was reported unknown" ;;
  *) bad "the tool that may have run was reported unknown" "$(printf '%s' "$watched" | tail -2)" ;;
esac

# Counted on the shared volume rather than in the transcript. A second execution records no second
# result (the retry throws it away), so the transcript cannot tell "did not run again" from "ran
# again and we dropped it". The side effect can. This is the `git push` case.
ran=$($COMPOSE exec -T worker-b sh -c 'wc -l < /sessions/ran.txt 2>/dev/null || echo 0' \
  2>/dev/null | tr -d '\r ')
[ "${ran:-0}" = "1" ] && ok "the tool really ran once, not once per host" \
  || bad "the tool really ran once" "$ran lines"

# --- the part only Temporal can answer: two hosts ran this, and the second one retried
$COMPOSE exec -T temporal sh -c \
  "temporal workflow show --address 127.0.0.1:7233 -w pi-session-$sid -o json" \
  2>/dev/null > /tmp/pi-l3-history.json
summary=$(python3 - "$hostA" "$hostB" <<'PY'
import json, sys
hostA, hostB = sys.argv[1], sys.argv[2]
events = json.load(open('/tmp/pi-l3-history.json'))
events = events.get("events") or events.get("history", {}).get("events", [])
runs = []
for e in events:
    a = e.get("activityTaskStartedEventAttributes")
    if a:
        runs.append((a.get("identity", ""), a.get("attempt")))
hosts = {identity.split("@")[-1] for identity, _ in runs}
retried = any(attempt and attempt > 1 for _, attempt in runs)
print(f"{len(hosts)}|{int(retried)}|{sorted(hosts)}")
PY
)
count=${summary%%|*}
rest=${summary#*|}
retried=${rest%%|*}
[ "$count" = "2" ] && ok "two hosts ran this turn" || bad "two hosts ran this turn" "$summary"
[ "$retried" = "1" ] && ok "the step came back as a retry on the second host" \
  || bad "the step came back as a retry on the second host" "$summary"

# --- a turn that begins with no client at all. `start` needs something to run it; a schedule does
# not, so the session is created by the workflow rather than by whoever asked for it.
$COMPOSE run --rm -T client schedule \
  "Use the bash tool to run: echo SCHEDULED-RUN. Then report it." --every=30s --id=check-nightly \
  >/dev/null 2>&1
scheduled=""
for _ in $(seq 1 30); do
  found=$($COMPOSE exec -T temporal sh -c \
    "temporal workflow list --address 127.0.0.1:7233 \
      --query \"WorkflowType='piSession'\"" 2>/dev/null \
    | grep -o "check-nightly[^ ]*" | head -1)
  [ -n "$found" ] && { scheduled="$found"; break; }
  sleep 5
done
[ -n "$scheduled" ] && ok "a schedule started a session with no client running" \
  || bad "a schedule started a session with no client running"

if [ -n "$scheduled" ]; then
  # The id has to come back through the same door a client-started one does, or the only sessions
  # anyone can see are the ones a client started.
  sid="${scheduled#pi-session-}"
  listed=$($COMPOSE run --rm -T client running 2>/dev/null | tr -d '\r')
  case "$listed" in
    *"$sid"*) ok "the scheduled session is listed like any other" ;;
    *) bad "the scheduled session is listed like any other" "$listed" ;;
  esac
  followed=$($COMPOSE run --rm -T client watch "$sid" 2>&1 | tr -d '\r')
  # The tool's own result line. `watch` prints the prompt back verbatim, and the prompt contains
  # the sentinel, so matching it anywhere passes whether or not anything ran.
  case "$followed" in
    *"tool result: SCHEDULED-RUN"*) ok "its turn ran, with nothing but the schedule to start it" ;;
    *) bad "its turn ran" "$(printf '%s' "$followed" | tail -2)" ;;
  esac
fi
$COMPOSE run --rm -T client unschedule check-nightly >/dev/null 2>&1

echo
[ "$fails" -eq 0 ] && echo "cross-host-check: OK" || echo "cross-host-check: $fails failed"
exit $([ "$fails" -eq 0 ] && echo 0 || echo 1)
