# pi-temporal

A Worker can die during a [Pi coding agent](https://github.com/earendil-works/pi) task without
losing the conversation. [Temporal](https://temporal.io) dispatches the next unit of work, and
another Worker reads the session file to continue. Cross-host recovery needs shared session
storage. The `fleet` profile also moves the project files.

Pi keeps the conversation in its session file and keeps its terminal interface. Temporal tracks
execution through Workflows and Activities. If a tool started but left no saved result, recovery
reports an unknown outcome to the model. It doesn’t repeat the call on a guess.

Live turns recover when you reopen `pi`. `/background` tasks run in separate Worker sessions and
can continue after you quit.

- `/background <task>` hands a task to a Worker and returns. Quit `pi` and the turn keeps going on
  a Worker that polls the Task Queue (`npm run worker`).
- `start`, `running`, `watch`, and `stop` work from any machine that reaches the cluster and the
  session directory.
- Schedules start turns without an open terminal. The `fleet` profile ships project files between
  Workers.
- Worker sessions accept token and wall-clock budgets for each turn or the whole session.

[docs/guarantees.md](docs/guarantees.md) describes the recovery limits and the checks that cover
them.

## Install

```
git clone https://github.com/temporalio/pi-temporal
cd pi-temporal
./install.sh
```

The script builds the Pi fork that `fork.pin` names, links it into `node_modules`, and
checks types. Run it again after a pull. It fetches the fork again only when the pin changes.

## Try it

Start a development server, then run `pi` in your project.

```
./scripts/temporal-dev.sh                 # 127.0.0.1:7233, UI on 8233
cd /path/to/your/project
/path/to/pi-temporal/scripts/run-pi.sh    # needs OPENAI_API_KEY or OPENAI_API_KEY_FILE
```

Each live turn gets a `piLocalTurn` Workflow. Kill `pi` during a tool call, reopen it with
`run-pi.sh -c`, and Pi records an unknown outcome for an interrupted call with no saved result.

Type `/background Use the bash tool to write hello into note.txt, then reply DONE.`
The command returns while a Worker runs the turn. `/background-status` lists the tasks you're
waiting on, with their ids and task text, and `/background-stop` interrupts them.

The [chaos demo](demo/README.md) kills Workers during a task so you can watch recovery.

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

The Workflow holds control state. The session file holds the conversation. To follow a session,
the client queries the Workflow and tails the file. With `PI_SESSION_DIR` on shared storage, you
can submit a task from one machine and watch it from another. `watch` exits with a nonzero status
unless the turn was answered. `--timeout=<seconds>` bounds how long it follows.

`start` and `schedule` send `--project` only when it's a git checkout or has a `.gitignore`, and
never when it's your home directory. `/background` applies the same rule to its directory.

The session you type in stays yours. `/background` and `start` create a new Worker-owned session
instead of moving the live one.

## How a turn runs

A turn starts with a user prompt and ends with the final response. Each step contains one model
call and any tool calls it requests. The seal records those tool results in the transcript.

By default, a Worker session runs each step in one `runStep` Activity. One `piSession` Workflow
handles the session’s turns. A live turn uses its own `piLocalTurn` Workflow and runs in one
`runLocalTurn` Activity. The transcript decides what runs next.

`PI_TEMPORAL_STEPPED=1` splits the model call and seal into separate Activities. Each tool call
also gets its own Activity.

```
runModelCall  ->  runToolCall (one per call)  ->  sealStep
```

Each tool call then has its own retry and timeout. In Worker sessions, a dispatch claim is
written beside the session file before the tool runs. A retry that finds a claim without a result
reports an unknown outcome instead of repeating the call.

## Deploying

`PI_TEMPORAL_PROFILE` selects the deployment setup.

| | `local` (default) | `fleet` |
|---|---|---|
| use | `pi` on your machine | Workers on other hosts |
| session directory | `~/.pi-temporal/sessions` | you set `PI_SESSION_DIR`, on shared storage |
| project files travel | no | yes |
| unit of work | a whole step | each model call, tool call, and seal |

`doctor` checks the profile’s configuration rules. It can’t verify shared storage or the clock
assumptions described in [docs/guarantees.md](docs/guarantees.md).

| variable | what it sets | default |
|---|---|---|
| `PI_TEMPORAL_TASK_QUEUE` | the Task Queue sessions use | `pi-session` |
| `PI_TEMPORAL_PROVIDER` | `openai` or `anthropic` (reads `<PROVIDER>_API_KEY` or `_API_KEY_FILE`) | `openai` |
| `PI_MODEL` | substring matched against the provider’s model ids | `mini` / `haiku` |
| `PI_TEMPORAL_TOOL_TIMEOUT_MINUTES` | tool attempt timeout, stepped Worker sessions | 30 |
| `PI_TEMPORAL_BUDGET_TOKENS`, `_SECONDS` | tokens and wall clock per turn | none |
| `PI_TEMPORAL_BUDGET_HARD_SECONDS` | deadline that cancels the turn’s wait, even during a call | none |
| `PI_TEMPORAL_BUDGET_SESSION_TOKENS`, `_SECONDS` | tokens and wall clock per session | none |
| `PI_SESSION_IDLE_TIMEOUT` | how long an idle session Workflow waits | `5 minutes` |
| `PI_TEMPORAL_DATA` | host directory for shadow repos and markers | `~/.pi-temporal` |
| `PI_TEMPORAL_DURABLE_TURNS` | `0` turns off the Workflow behind live turns | on |
| `PI_TEMPORAL_EMBEDDED_WORKER` | `0` when a standalone Worker (`npm run worker`) owns the queue | on |

The hard deadline stops the Workflow from waiting for the call. An external command may keep
running after cancellation. Token and other wall-clock budgets are checked between steps or
sequential tool calls, so a running call can exceed them.

For Temporal Cloud or mTLS, use the API key or certificate settings below.

```bash
TEMPORAL_ADDRESS=your-ns.a1b2c.tmprl.cloud:7233 TEMPORAL_NAMESPACE=your-ns.a1b2c \
  PI_TEMPORAL_API_KEY_FILE=/run/secrets/temporal-key
TEMPORAL_ADDRESS=temporal.internal:7233 PI_TEMPORAL_TLS_CERT=/run/secrets/tls.crt \
  PI_TEMPORAL_TLS_KEY=/run/secrets/tls.key PI_TEMPORAL_TLS_CA=/run/secrets/ca.crt
```

If Temporal can’t be reached before a turn starts, the extension says so and runs that turn as
plain Pi, without recovery. If Temporal goes away mid-turn, the turn stops with an error, and the
steps it already recorded stay in the session.

## The Pi fork

The step-level API isn’t in the published Pi package yet, so this repo builds the
[temporalio/pi](https://github.com/temporalio/pi) fork at the commit in `fork.pin`. The fork
adds `prepareStep`, `recordPrompt`, `modelCall` / `runToolCall` / `sealStep`, `setWriteGuard`,
and `pi.registerTurnExecutor`. [docs/upstream.md](docs/upstream.md) has the plan to upstream it.

You can also install the extension into a fork build of `pi`.

```
pi install git:git@github.com:temporalio/pi-temporal
pi install -l /path/to/pi-temporal   # this project only
```

## Checks

Each `checks/*-check.mts` script checks one contract and exits with a nonzero status on failure.
`npm run checks` runs the checks that need neither a model key nor Docker. Most need a local
Temporal server. CI runs them and `docker/restart-check.sh`. The scripts in `docker/` cover
separate Worker containers and NFS storage.

## Prior art

[osolmaz/pi-workflows](https://github.com/osolmaz/pi-workflows) makes user-defined workflow
graphs durable in Pi with its own file-queue engine. This project uses Temporal to recover the
agent’s turn loop.
