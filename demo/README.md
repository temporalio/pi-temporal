# Chaos demo

Submit one task to three Workers in Docker, then kill Workers while it runs. The script submits
the task once. After a Worker dies, another Worker restores the project files and continues the
turn. An interrupted tool with no saved result is reported to the model as an unknown outcome.

## Run it

```bash
./install.sh                     # once
ANTHROPIC_API_KEY=... demo/run.sh
```

`ANTHROPIC_API_KEY_FILE` works too. You’ll need Docker and `python3`. The default model is
`claude-haiku-4-5`. Allow a few minutes for a run. Model charges depend on how much work is
interrupted and repeated.

The task writes and runs a small program, then saves its output and replies with the result.
Three commands sleep for 30 seconds to give the kill loop time to interrupt them. The loop waits
for progress between kills. Recovery needs a heartbeat timeout, and a tree store held by the
killed Worker must wait for its lease to go stale.

## What to watch

- The chaos lines identify the killed Worker and its active work. They also show when it returns.
- The `|` lines are the session as `pi-temporal watch` sees it.
- The `running:` lines show the Activities in flight, with attempt number and Worker. After a
  kill, a retried model call or seal appears with a higher attempt number.
- The Temporal UI at http://localhost:8233 shows each attempt and the Worker that ran it.

When the turn ends, the script prints the answer and the contents of `result.txt`. It also reports
the kills. If the turn didn’t finish, the script exits with a nonzero status. Logs stay under
`demo/logs/<run>/`.

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
