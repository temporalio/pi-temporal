# Chaos demo

One task, three Workers in Docker, and a loop that kills a Worker every so often. The task is
submitted once and never restarted. When a Worker dies mid-step, the turn goes on with another
Worker, the project files travel with the session, and the turn finishes on whoever is left.

## Run it

```bash
./install.sh                     # once
ANTHROPIC_API_KEY=... demo/run.sh
```

`ANTHROPIC_API_KEY_FILE` works too. You need Docker and `python3`. A run takes a few minutes and
costs a few cents of `claude-haiku-4-5`.

The task writes a small program, runs it, saves its output, and answers with the result. Three
steps sleep for 30 seconds so kills land mid-work. Each kill waits until something has finished
since the last one, because recovering a step takes a heartbeat timeout plus the lease's stale
window.

## What to watch

- The chaos lines say which Worker was killed, what it was running, and when it came back.
- The `|` lines are the session as `pi-temporal watch` sees it.
- The `running:` lines show the Activities in flight, with attempt number and Worker. After a
  kill, the model call or seal that was running shows up again as attempt 2.
- The Temporal UI at http://localhost:8233 shows each attempt and the Worker that ran it.

At the end it prints the answer, the `result.txt` the task wrote, and a kill summary. It exits
non-zero if the turn didn't finish. Logs stay under `demo/logs/<run>/`.

## Knobs

| variable | what it changes | default |
|---|---|---|
| `DEMO_KILL_MIN`, `DEMO_KILL_MAX` | seconds between kills | 15, 40 |
| `DEMO_KILL_ACTIVE` | percent of kills aimed at the Worker running the current attempt | 70 |
| `DEMO_RESTART_AFTER` | seconds a killed Worker stays down | 5 |
| `DEMO_RESTART_MODE` | `replace` starts a new container, `start` restarts the killed one | `replace` |
| `DEMO_WORKERS` | number of Worker containers | 3 |
| `DEMO_TIMEOUT` | seconds to wait for the turn | 1200 |
| `DEMO_UI_PORT`, `DEMO_TEMPORAL_PORT` | host ports | 8233, 7243 |
| `PI_TEMPORAL_PROVIDER`, `PI_MODEL` | model provider and model | `anthropic`, `haiku` |
