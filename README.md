# pi-temporal

Run the [Pi coding agent](https://github.com/earendil-works/pi) on
[Temporal](https://temporal.io), so a turn survives the process that runs it.

Pi keeps the conversation. The session file stays the record and the TUI stays the TUI. Temporal
drives execution: a turn is a Workflow, and each model call and tool call can be its own Activity.
Kill the Worker mid-tool and another machine that has never seen the session finishes the turn.
The tool that was cut off is reported to the model as an unknown outcome, never run again on a
guess.

What you get:

- `/background <task>` hands a task to a Worker and returns. Quit `pi` and the turn keeps going on
  any Worker that polls the queue (`npm run worker`).
- `start`, `running`, `watch`, and `stop` work from any machine that reaches the cluster and the
  session directory.
- Schedules start turns with nobody around, and the `fleet` profile moves the project files with
  the session.
- Token and wall-clock budgets per turn and per session, for Worker sessions.

[docs/guarantees.md](docs/guarantees.md) lists what survives a crash and the check that proves it.

## Install

```
git clone https://github.com/temporalio/pi-temporal
cd pi-temporal
./install.sh
```

The script builds the Pi fork that `fork.pin` names, links it into `node_modules`, and
typechecks. Run it again after a pull. The fork is only refetched when the pin moves.

## Try it

Start a dev server, then `pi` in the project you want it to work on:

```
./scripts/temporal-dev.sh                 # 127.0.0.1:7233, UI on 8233
cd /path/to/your/project
/path/to/pi-temporal/scripts/run-pi.sh    # needs OPENAI_API_KEY or OPENAI_API_KEY_FILE
```

Each live turn gets a `piLocalTurn` Workflow. Kill `pi` during a tool call, reopen it with
`run-pi.sh -c`, and the interrupted call is settled.

Then type `/background Use the bash tool to write hello into note.txt, then reply DONE.` It
returns right away and a Worker runs the turn. `/background-status` shows progress and
`/background-stop` interrupts it.

To watch Workers get killed while a turn finishes anyway, run the [chaos demo](demo/README.md).

## Run it from anywhere

```bash
npx tsx src/cli.ts start "fix the failing test" --project=/path/to/repo
npx tsx src/cli.ts running
npx tsx src/cli.ts watch task-1a2b3c4d
npx tsx src/cli.ts stop task-1a2b3c4d
npx tsx src/cli.ts schedule "review yesterday's merges" --cron="0 9 * * *" --id=morning \
  --project=/path/to/repo
npx tsx src/cli.ts unschedule morning
npx tsx src/cli.ts doctor                 # resolved config and server reachability
```

The Workflow holds control state and the session file holds the conversation, so following a
session is a query plus a tail of its file. Put `PI_SESSION_DIR` on shared storage and the
machines that start, run, and watch a task can all be different.

The session you type in stays yours. `/background` and `start` create a new Worker-owned session
instead of moving the live one.

## How a turn runs

In a Worker session, one `runStep` Activity runs one step by default: a model call, its tool calls,
and a seal that records the results. A live turn runs as one `runLocalTurn` Activity. The Workflow
loops until a step says the turn is done. It holds no conversation state, because the transcript
decides what runs next.

`PI_TEMPORAL_STEPPED=1` splits each step into separate Activities:

```
runModelCall  ->  runToolCall (one per call)  ->  sealStep
```

That gives each tool call its own retry and timeout. In Worker sessions, a claim is written beside
the session file before a tool runs, so a retry that finds the claim without a result reports an
unknown outcome instead of running it twice.

## Deploying

`PI_TEMPORAL_PROFILE` picks one of two setups:

| | `local` (default) | `fleet` |
|---|---|---|
| what it's for | `pi` on your machine | Workers on machines nobody sits at |
| session directory | `~/.pi-temporal/sessions` | you set `PI_SESSION_DIR`, on shared storage |
| project files travel | no | yes |
| unit of work | a whole step | each model call, tool call, and seal |

`doctor` checks the profile's rules. It can't check that the directory is really shared, or
that the storage and clocks meet the lease assumptions in [docs/guarantees.md](docs/guarantees.md).

| variable | what it sets | default |
|---|---|---|
| `PI_TEMPORAL_TASK_QUEUE` | the Task Queue sessions use | `pi-session` |
| `PI_TEMPORAL_PROVIDER` | `openai` or `anthropic` (reads `<PROVIDER>_API_KEY` or `_API_KEY_FILE`) | `openai` |
| `PI_MODEL` | substring matched against the provider's model ids | `mini` / `haiku` |
| `PI_TEMPORAL_TOOL_TIMEOUT_MINUTES` | one tool call attempt, stepped Worker sessions | 30 |
| `PI_TEMPORAL_BUDGET_TOKENS`, `_SECONDS` | tokens and wall clock per turn | none |
| `PI_TEMPORAL_BUDGET_HARD_SECONDS` | deadline that stops a turn mid-call | none |
| `PI_TEMPORAL_BUDGET_SESSION_TOKENS`, `_SECONDS` | tokens and wall clock per session | none |
| `PI_SESSION_IDLE_TIMEOUT` | how long an idle session Workflow waits | `5 minutes` |
| `PI_TEMPORAL_DATA` | host directory for shadow repos and markers | `~/.pi-temporal` |
| `PI_TEMPORAL_DURABLE_TURNS` | `0` turns off the Workflow behind live turns | on |
| `PI_TEMPORAL_EMBEDDED_WORKER` | `0` when a standalone Worker (`npm run worker`) owns the queue | on |

For Temporal Cloud or mTLS:

```bash
TEMPORAL_ADDRESS=your-ns.a1b2c.tmprl.cloud:7233 TEMPORAL_NAMESPACE=your-ns.a1b2c \
  PI_TEMPORAL_API_KEY_FILE=/run/secrets/temporal-key
TEMPORAL_ADDRESS=temporal.internal:7233 PI_TEMPORAL_TLS_CERT=/run/secrets/tls.crt \
  PI_TEMPORAL_TLS_KEY=/run/secrets/tls.key PI_TEMPORAL_TLS_CA=/run/secrets/ca.crt
```

If Temporal can't be reached, the extension says so and runs plain Pi, without recovery.

## The Pi fork

The step-level API isn't in the published Pi package yet, so this repo builds the
[temporalio/pi](https://github.com/temporalio/pi) fork at the commit in `fork.pin`. The fork
adds `prepareStep`, `recordPrompt`, `modelCall` / `runToolCall` / `sealStep`, `setWriteGuard`,
and `pi.registerTurnExecutor`. [docs/upstream.md](docs/upstream.md) has the plan to upstream it.

To use the extension from a fork build of `pi` directly:

```
pi install git:git@github.com:temporalio/pi-temporal
pi install -l /path/to/pi-temporal   # this project only
```

## Checks

Each `checks/*-check.mts` is a standalone script for one contract and exits non-zero on failure.
`npm run checks` runs the ones that need no model key and no Docker. CI runs those plus
`docker/restart-check.sh`. Most
need a local Temporal server. `docker/` has the multi-container and NFS checks.

## Prior art

[osolmaz/pi-workflows](https://github.com/osolmaz/pi-workflows) makes user-defined workflow
graphs durable in Pi with its own file-queue engine. This project makes the agent's own turn loop
durable, on Temporal.
