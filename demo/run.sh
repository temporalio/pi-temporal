#!/usr/bin/env bash
# The chaos demo: one task, three Docker workers, and a loop that kills workers until the task is
# done. Temporal hands each interrupted step to another worker, and the project travels with the
# session, so the turn finishes without restarting.
#
# Usage: demo/run.sh   (with ANTHROPIC_API_KEY or ANTHROPIC_API_KEY_FILE set; see demo/README.md)

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

provider="${PI_TEMPORAL_PROVIDER:-anthropic}"
workers="${DEMO_WORKERS:-3}"
kill_min="${DEMO_KILL_MIN:-15}"
kill_max="${DEMO_KILL_MAX:-40}"
restart_after="${DEMO_RESTART_AFTER:-5}"
kill_active="${DEMO_KILL_ACTIVE:-70}"
restart_mode="${DEMO_RESTART_MODE:-replace}"
timeout_s="${DEMO_TIMEOUT:-1200}"
ui_port="${DEMO_UI_PORT:-8233}"
grpc_port="${DEMO_TEMPORAL_PORT:-7243}"

run_id="$(date +%Y%m%d-%H%M%S)"
logs="demo/logs/$run_id"
name="pi-demo-$$"
net="$name-net"
sessions="$name-sessions"
queue="$name"
session="demo-$run_id"
image="pi-temporal:demo"

case "$restart_mode" in
  replace | start) ;;
  *) echo "DEMO_RESTART_MODE is replace or start, not $restart_mode" >&2; exit 1 ;;
esac

say() { printf '%s  %s\n' "$(date +%H:%M:%S)" "$*"; }
die() { say "$*" >&2; exit 1; }

# --- preflight ---------------------------------------------------------------------------------

command -v docker >/dev/null || die "docker is not on PATH"
docker info >/dev/null 2>&1 || die "docker is installed but the daemon is not answering"
command -v python3 >/dev/null || die "python3 is needed to read Temporal's history"

case "$provider" in
  anthropic) key_var=ANTHROPIC_API_KEY ;;
  *) key_var=OPENAI_API_KEY ;;
esac
file_var="${key_var}_FILE"
if [ -z "${!key_var:-}" ]; then
  key_file="${!file_var:-}"
  [ -n "$key_file" ] && [ -f "$key_file" ] \
    || die "set $key_var, or $file_var to a file holding one: the workers need a model key"
  # Passed to containers by name, so the key never appears on a command line.
  export "$key_var"="$(tr -d '[:space:]' < "$key_file")"
fi
[ -n "${!key_var}" ] || die "$key_var is empty"

# shellcheck source=../scripts/pinned-fork.sh
. scripts/pinned-fork.sh
pinned_fork

mkdir -p "$logs"
cp -R demo/project "$logs/project"

# --- cleanup -----------------------------------------------------------------------------------

background=()
cleanup() {
  for pid in "${background[@]}"; do kill "$pid" 2>/dev/null; done
  for i in $(seq 1 "$workers"); do
    docker logs "$name-worker-$i" >> "$logs/worker-$i.log" 2>&1
  done
  docker logs "$name-temporal" > "$logs/temporal.log" 2>&1
  docker ps -aq --filter "name=^$name-" | xargs docker rm -f >/dev/null 2>&1
  docker network rm "$net" >/dev/null 2>&1
  docker volume rm "$sessions" >/dev/null 2>&1
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# --- the cluster -------------------------------------------------------------------------------

say "building the worker image from docker/Dockerfile"
docker build -q -f docker/Dockerfile -t "$image" . > "$logs/build.log" 2>&1 \
  || die "the image did not build; see $logs/build.log"

docker network create "$net" >/dev/null || die "could not create network $net"
docker volume create "$sessions" >/dev/null || die "could not create volume $sessions"

say "starting a Temporal dev server (UI on http://localhost:$ui_port)"
docker run -d --name "$name-temporal" --network "$net" \
  -p "$grpc_port:7233" -p "$ui_port:8233" \
  --entrypoint temporal temporalio/admin-tools:1.29 \
  server start-dev --ip 0.0.0.0 --ui-ip 0.0.0.0 --log-level warn >/dev/null \
  || die "the Temporal server did not start"
for _ in $(seq 1 60); do
  docker exec "$name-temporal" temporal operator cluster health --address 127.0.0.1:7233 \
    >/dev/null 2>&1 && break
  sleep 1
done
docker exec "$name-temporal" temporal operator cluster health --address 127.0.0.1:7233 \
  >/dev/null 2>&1 || die "the Temporal server never became healthy"

# Fleet settings, with one shared session directory.
common_env=(
  -e "TEMPORAL_ADDRESS=$name-temporal:7233"
  -e PI_TEMPORAL_PROFILE=fleet
  -e PI_SESSION_DIR=/sessions
  -e PI_PROJECT_DIR=/project
  -e "PI_TEMPORAL_TASK_QUEUE=$queue"
  -e "PI_TEMPORAL_PROVIDER=$provider"
  -e "$key_var"
)
[ -n "${PI_MODEL:-}" ] && common_env+=(-e PI_MODEL)

start_worker() {
  docker run -d --name "$name-worker-$1" --hostname "worker-$1" --network "$net" \
    -v "$sessions:/sessions" "${common_env[@]}" "$image" >/dev/null
}

# By default a killed worker returns as a new container with the same name and hostname. With
# DEMO_RESTART_MODE=start the same container restarts, keeping its pid, filesystem and markers.
bring_back() {
  if [ "$restart_mode" = start ]; then
    docker start "$name-worker-$1" >/dev/null
    return
  fi
  docker logs "$name-worker-$1" >> "$logs/worker-$1.log" 2>&1
  docker rm -f "$name-worker-$1" >/dev/null 2>&1
  start_worker "$1"
}

say "starting $workers workers: $(seq -s ' ' -f 'worker-%g' 1 "$workers")"
for i in $(seq 1 "$workers"); do
  start_worker "$i" || die "worker-$i did not start"
done

# A client with its own copy of the project, which it sends with the task.
client() {
  docker run --rm --network "$net" -v "$sessions:/sessions" \
    -v "$PWD/$logs/project:/project" "${common_env[@]}" \
    --entrypoint node "$image" --import tsx src/cli.ts "$@"
}

# --- the task ----------------------------------------------------------------------------------

# Three slow steps, so kills land mid-work.
task="Work in the current directory. Use the bash tool, one command per step, in this order. \
1) Write a Node.js program primes.js that prints the first 25 prime numbers, one per line. \
2) Run: sleep 30 && node primes.js > primes.txt \
3) Run: sleep 30 && wc -l < primes.txt > result.txt \
4) Run: sleep 30 && tail -n 1 primes.txt >> result.txt \
5) Run: cat result.txt \
Then answer with the two lines of result.txt and nothing else."

say "submitting one task as session $session"
client start "$task" --session="$session" --project=/project > "$logs/start.log" 2>&1 \
  || die "the task was not accepted; see $logs/start.log"
started_at="$(date +%s)"

client watch "$session" > "$logs/watch.log" 2>&1 &
background+=("$!")
tail -n +1 -F "$logs/watch.log" 2>/dev/null | sed -u 's/^/          | /' &
background+=("$!")

# --- progress ----------------------------------------------------------------------------------

history_json() {
  docker exec "$name-temporal" temporal workflow show \
    --address 127.0.0.1:7233 -w "pi-session-$session" -o json 2>/dev/null
}

# Which worker ran each activity attempt, from history. The identity is `pid@hostname`.
attempts() {
  history_json | python3 -c '
import json, sys
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(0)
events = data.get("events") or data.get("history", {}).get("events", [])
names, rows = {}, []
for e in events:
    eid = str(e.get("eventId"))
    sched = e.get("activityTaskScheduledEventAttributes")
    if sched:
        names[eid] = sched.get("activityType", {}).get("name", "?")
    start = e.get("activityTaskStartedEventAttributes")
    if start:
        host = start.get("identity", "?").split("@")[-1]
        name = names.get(str(start.get("scheduledEventId")), "?")
        rows.append((name, start.get("attempt", 1), host))
mode = sys.argv[1]
if mode == "hosts":
    print(" ".join(sorted({h for _, _, h in rows})))
elif mode == "done":
    print(sum(1 for e in events if e.get("activityTaskCompletedEventAttributes")))
elif mode == "lost":
    # An attempt whose worker died ends in a timeout.
    keys = ("activityTaskTimedOutEventAttributes", "activityTaskFailedEventAttributes")
    print(sum(1 for e in events if any(e.get(k) for k in keys)))
' "$1"
}

# Pending activities with their attempt and last worker. History lags, since it records an
# attempt only once it ends.
running() {
  docker exec "$name-temporal" temporal workflow describe \
    --address 127.0.0.1:7233 -w "pi-session-$session" -o json 2>/dev/null | python3 -c '
import json, sys
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(0)
pending = data.get("pendingActivities") or []
mode = sys.argv[1]
rows = []
for p in pending:
    name = (p.get("activityType") or {}).get("name", "?")
    host = (p.get("lastWorkerIdentity") or "").split("@")[-1]
    rows.append((name, p.get("attempt", 1), host))
if mode == "line":
    print(", ".join(f"{n} attempt {a} on {h or chr(63)}" for n, a, h in rows))
elif mode == "host":
    print(next((h for _, _, h in rows if h), ""))
' "$1"
}

turn_state() {
  docker exec "$name-temporal" temporal workflow query --address 127.0.0.1:7233 \
    -w "pi-session-$session" --name turnState -o json 2>/dev/null
}

finished_field() {
  python3 -c '
import json, sys
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(0)
result = data.get("queryResult", data)
if isinstance(result, list):
    result = result[0] if result else {}
finished = (result or {}).get("finished") or {}
print(finished.get(sys.argv[1], ""))
' "$1"
}

# --- chaos -------------------------------------------------------------------------------------

chaos() {
  local done_at_kill=-1
  while :; do
    sleep $((kill_min + RANDOM % (kill_max - kill_min + 1)))
    # Wait for progress since the last kill. Recovery takes a heartbeat timeout plus the lock's
    # stale window, so faster kills would stall the turn.
    local done_now
    done_now="$(attempts done)"
    [ "${done_now:-0}" -gt "$done_at_kill" ] || continue
    # Usually kill the worker running the current attempt, sometimes a random one.
    local victim
    victim="$(running host)"
    victim="${victim#worker-}"
    if [ -z "$victim" ] || [ $((RANDOM % 100)) -ge "$kill_active" ]; then
      victim=$((1 + RANDOM % workers))
    fi
    local doing
    doing="$(running line)"
    docker kill "$name-worker-$victim" >/dev/null 2>&1 || continue
    done_at_kill="${done_now:-0}"
    echo "$(date +%s) worker-$victim" >> "$logs/kills.log"
    say "chaos: killed worker-$victim${doing:+ while running: $doing}"
    sleep "$restart_after"
    local as="as a fresh container"
    [ "$restart_mode" = start ] && as="in the same container"
    bring_back "$victim" && say "chaos: worker-$victim is back, $as"
  done
}
: > "$logs/kills.log"
chaos &
background+=("$!")

outcome=""
last_report=0
while :; do
  now="$(date +%s)"
  state="$(turn_state)"
  outcome="$(printf '%s' "$state" | finished_field outcome)"
  [ -n "$outcome" ] && break
  if [ $((now - started_at)) -ge "$timeout_s" ]; then
    say "the turn did not finish within ${timeout_s}s"
    break
  fi
  if [ $((now - last_report)) -ge 20 ]; then
    now_running="$(running line)"
    [ -n "$now_running" ] && say "running: $now_running"
    last_report="$now"
  fi
  sleep 5
done

# --- summary -----------------------------------------------------------------------------------

for pid in "${background[@]}"; do kill "$pid" 2>/dev/null; done
background=()
elapsed=$(( $(date +%s) - started_at ))
kills="$(wc -l < "$logs/kills.log" | tr -d ' ')"
hosts="$(attempts hosts)"
lost="$(attempts lost)"
answer="$(printf '%s' "$state" | finished_field finalText)"

echo
say "turn outcome: ${outcome:-none}"
if [ -n "$answer" ]; then
  echo "final answer:"
  printf '%s\n' "$answer" | sed 's/^/  /'
fi

# result.txt is on any worker that restored the latest tree.
for i in $(seq 1 "$workers"); do
  if docker exec "$name-worker-$i" test -f /project/result.txt 2>/dev/null; then
    echo "result.txt, as worker-$i holds it:"
    docker exec "$name-worker-$i" cat /project/result.txt | sed 's/^/  /'
    break
  fi
done

echo
plural() { [ "$1" = 1 ] && printf '%s %s' "$1" "$2" || printf '%s %ss' "$1" "$2"; }
echo "summary: $(plural "$kills" kill) in ${elapsed}s. $(plural "$lost" "activity attempt") lost"
echo "         with their worker, and the turn went on without them."
echo "         Workers that ran attempts: ${hosts:-none}"
echo "logs: $logs"

[ "$outcome" = "answered" ] || exit 1
