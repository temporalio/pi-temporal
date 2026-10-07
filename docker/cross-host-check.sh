#!/usr/bin/env bash
# Handover between two containers that share only Temporal and /sessions. Kills worker A mid-tool
# and checks in Temporal's history that the activity retried on worker B's hostname.
#
# Usage: OPENAI_API_KEY=... docker/cross-host-check.sh
#
# /sessions is a local volume here. For NFS, see docker/tree-check.sh with NFS=1.

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

# shellcheck source=../scripts/pinned-fork.sh
. scripts/pinned-fork.sh

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

# --- kill worker A while the tool is in flight. Poll the transcript, since a fixed sleep can miss.
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

# Count the side effect, not transcript entries. A rerun's result is discarded, so only the
# shared file shows whether the tool ran twice.
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

# --- a turn started by a schedule, with no client running.
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
  # A scheduled session must show up in `running` like a client-started one.
  sid="${scheduled#pi-session-}"
  listed=$($COMPOSE run --rm -T client running 2>/dev/null | tr -d '\r')
  case "$listed" in
    *"$sid"*) ok "the scheduled session is listed like any other" ;;
    *) bad "the scheduled session is listed like any other" "$listed" ;;
  esac
  followed=$($COMPOSE run --rm -T client watch "$sid" 2>&1 | tr -d '\r')
  # Match the tool result line. The prompt also contains the sentinel and `watch` echoes it.
  case "$followed" in
    *"tool result: SCHEDULED-RUN"*) ok "its turn ran, with nothing but the schedule to start it" ;;
    *) bad "its turn ran" "$(printf '%s' "$followed" | tail -2)" ;;
  esac
fi
$COMPOSE run --rm -T client unschedule check-nightly >/dev/null 2>&1

echo
[ "$fails" -eq 0 ] && echo "cross-host-check: OK" || echo "cross-host-check: $fails failed"
exit $([ "$fails" -eq 0 ] && echo 0 || echo 1)
