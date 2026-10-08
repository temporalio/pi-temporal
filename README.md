# pi-temporal

A Worker can die during a [Pi coding agent](https://github.com/earendil-works/pi) task without
losing the conversation. [Temporal](https://temporal.io) dispatches the next unit of work, and
another Worker reads the session file to continue. Cross-host recovery needs shared session
storage. The `fleet` profile also moves the project files.

Pi keeps the conversation in its session file and keeps its terminal interface. Temporal tracks
execution through Workflows and Activities. If a tool started but left no saved result, recovery
reports an unknown outcome to the model. It doesn't repeat the call on a guess.

Live turns recover when you reopen `pi`. `/background` tasks run in separate Worker sessions and
can continue after you quit.

- `/background <task>` hands a task to a Worker and returns. Quit `pi` and the turn keeps going on
  a Worker that polls the Task Queue (`npm run worker`).
- `start`, `running`, `watch`, and `stop` work from any machine that reaches the cluster and the
  session directory.
- Schedules start turns without an open terminal. The `fleet` profile ships project files between
  Workers.
- Worker sessions accept token and wall-clock budgets for each turn or the whole session.

You can use this repo as a template for your own agent's loop. The Temporal code in `src/core/`
uses the `Agent` interface. Pi is one implementation.

- [docs/architecture.md](docs/architecture.md) describes state ownership and the optional modules.
  It also gives a reading order for the code.
- [docs/design-decisions.md](docs/design-decisions.md) says why each Temporal choice was made.
- [docs/adapting.md](docs/adapting.md) is what your agent must provide, and what you can delete.
- [docs/guarantees.md](docs/guarantees.md) says what survives which failure, with the check that
  shows it.

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

Each live turn gets a `piLocalTurn` Workflow, which gives it a record in Temporal and retries.
The turn runs in memory inside `pi`, so it can't move to another process. Kill `pi` during a tool
call, reopen it with `run-pi.sh -c`, and Pi records an unknown outcome for an interrupted call
with no saved result.

Type `/background Use the bash tool to write hello into note.txt, then reply DONE.`
The command returns while a Worker runs the turn. `/background-status` lists pending tasks with
their IDs and text. Use `/background-stop` to interrupt them.

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
the client tails the file and waits on the Workflow's `waitForQuiet` Update. With `PI_SESSION_DIR`
on shared storage, you can submit a task from one machine and watch it from another. `watch`
exits with a nonzero status unless the turn was answered. `--timeout=<seconds>` bounds how long it
follows.

`start` and `schedule` check the project's ignore rules before sending `--project`. They refuse
your home directory. `/background` applies the same checks to its directory.

The session you type in stays yours. `/background` and `start` create a new Worker-owned session
instead of moving the live one.

## How a turn runs

A turn starts with a user prompt and ends with the final response. Each step contains one model
call and any tool calls it requests. The seal records those tool results in the transcript.

By default, a Worker session runs each step in one `runStep` Activity. One `piSession` Workflow
handles the session's turns. A live turn uses its own `piLocalTurn` Workflow and runs in one
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

`doctor` checks the profile's configuration rules. It can't verify shared storage or the clock
assumptions described in [docs/guarantees.md](docs/guarantees.md).

| variable | what it sets | default |
|---|---|---|
| `PI_TEMPORAL_PROFILE` | `local` or `fleet`, the defaults below | `local` |
| `PI_SESSION_DIR` | where session files live, shared storage in a fleet | `~/.pi-temporal/sessions` |
| `PI_PROJECT_DIR` | the directory a standalone Worker's tools run in, required in `fleet` | the Worker's working directory |
| `PI_TEMPORAL_STEPPED` | `1` runs each model call, tool call, and seal as its own Activity | off, on in `fleet` |
| `PI_TEMPORAL_SHIP_TREE` | `1` ships the project's files between hosts | off, on in `fleet` |
| `PI_TEMPORAL_TASK_QUEUE` | the Task Queue sessions use | `pi-session` |
| `PI_TEMPORAL_PROVIDER` | `openai` or `anthropic` (reads `<PROVIDER>_API_KEY` or `_API_KEY_FILE`) | `openai` |
| `PI_MODEL` | substring matched against the provider's model ids | `mini` / `haiku` |
| `PI_TEMPORAL_TOOL_TIMEOUT_MINUTES` | tool attempt timeout, stepped Worker sessions | 30 |
| `PI_TEMPORAL_BUDGET_TOKENS`, `_SECONDS` | tokens and wall clock per turn | none |
| `PI_TEMPORAL_BUDGET_HARD_SECONDS` | deadline that cancels the turn's wait, even during a call | none |
| `PI_TEMPORAL_BUDGET_SESSION_TOKENS`, `_SECONDS` | tokens and wall clock per session | none |
| `PI_SESSION_IDLE_TIMEOUT` | how long an idle session Workflow waits | `5 minutes` |
| `PI_TEMPORAL_DATA` | host directory for shadow repos and markers | `~/.pi-temporal` |
| `PI_TEMPORAL_LIVE_TURNS` | `0` runs live turns as plain pi, with no Workflow | on |
| `PI_TEMPORAL_EMBEDDED_WORKER` | `0` when a standalone Worker (`npm run worker`) owns the queue, `1` to keep the Worker inside `pi` | on, off in `fleet` |
| `PI_TEMPORAL_MAX_ACTIVITIES` | Activity slots for each queue a Worker polls | 16 |
| `PI_TEMPORAL_SHUTDOWN_GRACE_SECONDS` | how long a stopping standalone Worker lets running Activities finish | 60 |
| `PI_TEMPORAL_DEPLOYMENT`, `PI_TEMPORAL_BUILD_ID` | turn on Worker Versioning for a standalone Worker. Set both or neither | none |
| `PI_TEMPORAL_API_KEY`, `_FILE` | API key for Temporal Cloud | none |
| `PI_TEMPORAL_TLS` | `1` connects over TLS without a client certificate | off |
| `PI_TEMPORAL_CODEC_KEY`, `_FILE` | 32 bytes, base64, to encrypt payloads in history | none |
| `PI_TEMPORAL_SEARCH_ATTRIBUTE` | `1` keeps session state in the `PiSessionState` search attribute | off |
| `PI_TEMPORAL_METRICS` | address for the Worker's Prometheus metrics, such as `0.0.0.0:9464` | none |
| `PI_TEMPORAL_WORKFLOW_BUNDLE` | a bundle from `npm run bundle`, so the Worker doesn't bundle at start | none |

The hard deadline stops the Workflow from waiting for the call. An external command may keep
running after cancellation. Token and other wall-clock budgets are checked between steps or
sequential tool calls, so a running call can exceed them.

The standard Temporal settings also apply. Use `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`,
`TEMPORAL_API_KEY`, the `TEMPORAL_TLS_*` variables, or a `TEMPORAL_PROFILE` in `temporal.toml`.
When both sources set a credential, the `PI_TEMPORAL_*` value takes priority. Other TLS settings
from the profile still apply, including its CA. Tools don't inherit either set of credentials.
They run as the same user, so they can still read them.
See [docs/guarantees.md](docs/guarantees.md).

For Temporal Cloud, export an API key with the namespace's address before you start a Worker or
run the CLI.

```bash
export TEMPORAL_ADDRESS=your-ns.a1b2c.tmprl.cloud:7233 TEMPORAL_NAMESPACE=your-ns.a1b2c
export PI_TEMPORAL_API_KEY_FILE=/run/secrets/temporal-key
```

`PI_TEMPORAL_SEARCH_ATTRIBUTE=1` needs the namespace to know `PiSessionState` first. Register it
once with the `temporal` CLI, or with `tcld` on Temporal Cloud.

```bash
temporal operator search-attribute create --name PiSessionState --type Keyword
tcld namespace search-attributes add --namespace your-ns.a1b2c --sa PiSessionState=Keyword
```

With `PI_TEMPORAL_CODEC_KEY` set, the Cloud UI shows payloads as ciphertext. To read them there,
run a codec server with the codec in `src/core/codec.ts` and the same key. Then set its URL in
the namespace's codec server setting. This repo doesn't ship a codec server.

For a cluster with mTLS, export the certificate pair instead.

```bash
export TEMPORAL_ADDRESS=temporal.internal:7233
export PI_TEMPORAL_TLS_CERT=/run/secrets/tls.crt PI_TEMPORAL_TLS_KEY=/run/secrets/tls.key
export PI_TEMPORAL_TLS_CA=/run/secrets/ca.crt
```

If Temporal can't be reached before a turn starts, the extension says so and runs that turn as
plain Pi, without recovery. If Temporal goes away mid-turn, the turn stops with an error, and the
steps it already recorded stay in the session.

`PI_TEMPORAL_METRICS` turns on the SDK's Prometheus metrics on each Worker. These are good first
alerts.

- `temporal_activity_schedule_to_start_latency` on host queues. A rise means a host is gone or
  full, and its steps wait out their queue timeout before they move.
- `temporal_activity_execution_failed`. Activity attempts that failed, such as a lost session
  file or a refused project.
- `temporal_workflow_task_execution_failed`. A Workflow Task that throws, such as after a bad
  deploy. Temporal retries it until a fixed Worker runs it, so the session waits.

## The Pi fork

The step-level API isn't in the published Pi package yet, so this repo builds the
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
separate Worker containers and NFS storage. [checks/README.md](checks/README.md) groups the checks
by how they test, so you can find examples for your own agent.

## Releasing

A tag `v<major>.<minor>.<patch>`, with an optional pre-release such as `-rc.1`, becomes a GitHub
release (`.github/workflows/release.yml`). The tag must match the `version` in `package.json`, and
it must point at a commit on `main`. The release carries generated notes and the Workflow bundle
with its checksum, built after the kept histories replay. A tag with a pre-release part is
published as a pre-release.

`package.json` is at `0.1.0`, so the first release only needs its tag.

```shell
git tag v0.1.0 origin/main
git push origin v0.1.0
```

A later release bumps the version first. Merge the bump to `main`, then tag that commit the same
way.

```shell
npm version 0.2.0 --no-git-tag-version
```

## Prior art

[osolmaz/pi-workflows](https://github.com/osolmaz/pi-workflows) makes user-defined workflow
graphs durable in Pi with its own file-queue engine. This project uses Temporal to recover the
agent's turn loop.
