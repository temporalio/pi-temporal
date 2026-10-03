# Chaos demo

One task, three workers in Docker, and a loop that kills a worker every so often. The task is
submitted once and never restarted. When the worker running a step dies, Temporal gives that step
to another worker, the project's files travel with the session, and the turn finishes on whichever
workers are left.

## Run it

```bash
npm ci && npm run setup-fork     # once, if you have not already
ANTHROPIC_API_KEY=... demo/run.sh
```

`ANTHROPIC_API_KEY_FILE` pointing at a file with the key works too. Docker and `python3` have to be
on the machine. The run takes a few minutes and costs a few cents of `claude-haiku-4-5`.

It builds the worker image from `docker/Dockerfile`, starts a Temporal dev server and the workers
on one Docker network with a shared session volume, and submits a task with several tool calls
spread over several steps: write a small program, run it, write its output to a file, and answer
with the result. Three of the steps sleep for 30 seconds, so the kills land in the middle of
work. A kill waits until something has finished since the one before it: recovering a step takes
a heartbeat timeout and the session lock's stale window, and killing faster than that would only
show a turn that cannot move.

## What to watch

- The chaos lines: which worker was killed and what it was running, and when it came back. A
  killed worker comes back as a fresh container with the same hostname, the way a replaced host
  would.
- The `|` lines: the session as `pi-temporal watch` follows it, the tool calls and the answer.
- The `running:` lines: the activities in flight, the attempt each is on and the worker running
  it, read from Temporal. After a kill, the step that was running shows up again as attempt 2,
  on the replacement or on another worker.
- The Temporal UI at http://localhost:8233 while it runs: open the `pi-session-demo-...` workflow
  and look at the activity attempts. Each one names the worker that ran it, `pid@worker-N`.

At the end it prints the turn's outcome, the answer, the `result.txt` the task wrote, and a summary
of how many kills there were and which workers ran attempts. It exits non-zero if the turn did not
finish. Containers are removed on the way out. Logs, including every worker's output and the
copy of the project the client sent, are kept under `demo/logs/<run>/`.

## Knobs

| variable | what it changes | default |
|---|---|---|
| `DEMO_KILL_MIN`, `DEMO_KILL_MAX` | seconds between kills, picked at random in this range | 15, 40 |
| `DEMO_KILL_ACTIVE` | percent of kills aimed at the worker running the current attempt | 70 |
| `DEMO_RESTART_AFTER` | seconds a killed worker stays down | 5 |
| `DEMO_WORKERS` | how many worker containers | 3 |
| `DEMO_TIMEOUT` | seconds to wait for the turn before giving up | 1200 |
| `DEMO_UI_PORT`, `DEMO_TEMPORAL_PORT` | host ports for the UI and the server | 8233, 7243 |
| `PI_TEMPORAL_PROVIDER` | the model provider; `openai` reads `OPENAI_API_KEY` instead | `anthropic` |
| `PI_MODEL` | matched as a substring of the provider's model ids | `haiku` |
