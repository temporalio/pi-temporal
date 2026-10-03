# pi-temporal

Run the [Pi coding agent](https://github.com/earendil-works/pi) on
[Temporal](https://temporal.io), so a turn survives the process that runs it.

Pi keeps the conversation: the session file stays the record, the TUI stays the TUI. Temporal
drives execution: a turn is a workflow, and every model call and tool call can be its own
activity. Kill the worker mid-tool and a worker that has never seen the session finishes the
turn. The tool that was cut off is settled as an unknown outcome, not run twice blind.

What that buys:

- `/background <task>` gives a task its own worker-owned session and returns. Quit pi; the turn
  continues wherever a worker polls.
- `start`, `running`, `watch`, and `stop` work from any machine that reaches the cluster and the
  session directory. A session is not tied to the terminal that started it.
- A schedule starts a turn with nobody around, project included.
- `PI_TEMPORAL_SHIP_TREE=1` moves the project files with the session, so the worker that resumes
  a turn stands in the tree the last one left.
- Operator bounds: tokens and wall clock per turn and per session, and a hard deadline that stops
  a turn where it stands.

Every claim about what survives a crash is tied to a named check in
[docs/guarantees.md](docs/guarantees.md). The short version: completed work replays from Temporal
history, the session record settles what a crash left half-done, and a worker that lost its lease
cannot write.

## Try it

Two terminals. First the server:

```
./scripts/temporal-dev.sh          # 127.0.0.1:7233, UI on 8233
```

Then pi, launched in whatever project you want the task to work on. `$PI_TEMPORAL` is wherever
you cloned this:

```
cd /path/to/your/project
"$PI_TEMPORAL"/scripts/run-pi.sh
```

`run-pi.sh` needs `OPENAI_API_KEY`, or `OPENAI_API_KEY_FILE` pointing at a file holding one. It
runs the fork build from `.fork/pi`, so `npm ci && npm run setup-fork` has to have happened.

With Temporal connected, each live turn gets a workflow: `temporal workflow list` shows one
`piLocalTurn` per prompt. To see the recovery, kill pi during a tool call (`Use the bash tool to
run: sleep 45; echo late`), reopen with `scripts/run-pi.sh -c`, and watch the interrupted call
get settled.

For the other half, type `/background Use the bash tool to write hello into note.txt, then reply
DONE.` It returns straight away and a worker takes it from there. The tools run in the directory
you launched pi from, so `note.txt` lands there. `/background-status` shows what it waits on;
`/background-stop` interrupts it.

To check the whole path without typing:

```
./scripts/background-smoke.sh          # starts a worker, submits a turn, exits non-zero on failure
npx tsx checks/step-loop-check.mts     # one activity per step, and interrupts; no model key needed
```

## Run it from anywhere

`/background` lives inside a pi session, so those tasks could only be followed from the terminal
that started them. `src/cli.ts` is the other half:

```bash
npx tsx src/cli.ts start "port the auth module to the new API"
npx tsx src/cli.ts start "fix the failing test" --project=/path/to/repo   # tree on: sends the project
npx tsx src/cli.ts running
npx tsx src/cli.ts watch task-1a2b3c4d
npx tsx src/cli.ts stop task-1a2b3c4d
```

The workflow holds control state and answers `turnState`; the session file holds the
conversation. Following a session is a query plus a tail of its file, and both work from any
machine that reaches the cluster and `PI_SESSION_DIR`. Point that directory at shared storage
and the machine that starts a task, the machine that runs it, and the machine that watches it
need not be the same one.

A schedule starts a turn with nobody around:

```bash
npx tsx src/cli.ts schedule "review yesterday's merges" --cron="0 9 * * *" --id=morning \
  --project=/path/to/repo
npx tsx src/cli.ts unschedule morning
```

The client sends the project once, into a template store, and each firing copies it into its own
session. `unschedule` keeps the template, because an accepted firing may still need it; remove it
later with `pi-temporal forget schedule-<scheduleId>`.

The session you type in cannot itself be handed to a worker: the executor hook supplies control
of the current turn, not a transfer of session ownership, and moving it would mean moving the
transcript writer and the workspace too. `/background` and `start` avoid the transfer by creating
a worker-owned session.

## How a turn executes

One `runStep` activity does one step: record the prompt if it is not in the transcript, settle
what an earlier attempt left behind, then one model call, its tool calls, and a seal that records
the results and says whether the turn is done. The workflow loops until a step reports done. The
transcript decides what runs next; the workflow only counts so a runaway turn hits a ceiling.

`PI_TEMPORAL_STEPPED=1` splits the step into separate activities instead:

```
runModelCall  ->  runToolCall (one per call)  ->  sealStep
```

That exposes per-tool retry and timeout boundaries to workflow code, and it is what the `fleet`
profile runs. The seal is the only writer of a step's results, dispatches keep a persistent
admission claim beside the session file, and a user stop reaches both modes between units of
work. The reasoning behind each of those rules, and what each one costs, is in
[docs/guarantees.md](docs/guarantees.md).

## Deploying it

Two deployments, not a dozen knobs, because the settings are not independent.
`PI_TEMPORAL_PROFILE` picks one and the rest follow:

| | `local` (default) | `fleet` |
|---|---|---|
| what it is | pi on your machine, worker inside it | workers on machines nobody is sitting at |
| session directory | `~/.pi-temporal/sessions` | **you name it**, on storage every worker reaches |
| project files travel | no | yes |
| unit of work | a whole step | the model call, each tool call, the seal |

Anything above can still be set on its own; the profile only decides what it is when you do not.
`preflight` requires an explicit `PI_SESSION_DIR` and tree shipping in the `fleet` profile, and
refuses tree shipping without the stepped path, on the worker and on the client. It does not test
that the directory is shared or that filesystem and clock behavior meet the lease assumptions;
operators must check those properties.

Settings no profile decides, read where a session starts and carried in the session's options.
Each is a whole number, and a value that is not one is refused rather than ignored:

| variable | what it bounds | unset |
|---|---|---|
| `PI_TEMPORAL_TOOL_TIMEOUT_MINUTES` | one attempt of one tool call, in stepped mode | 30 minutes |
| `PI_TEMPORAL_BUDGET_TOKENS` | tokens one turn may spend | no bound |
| `PI_TEMPORAL_BUDGET_SECONDS` | wall clock for one turn, checked between units of work | no bound |
| `PI_TEMPORAL_BUDGET_HARD_SECONDS` | wall clock for one turn, stopping it where it is | no bound |
| `PI_TEMPORAL_BUDGET_SESSION_TOKENS` | tokens the whole session may spend | no bound |
| `PI_TEMPORAL_BUDGET_SESSION_SECONDS` | wall clock for the whole session | no bound |

Other settings, read by whichever process uses them:

| variable | what it sets | unset |
|---|---|---|
| `PI_TEMPORAL_TASK_QUEUE` | the queue sessions are started on and workers poll | `pi-session` |
| `PI_SESSION_IDLE_TIMEOUT` | how long a session's workflow waits for a prompt before retiring | `5 minutes` |
| `PI_TEMPORAL_PROVIDER` | the provider a worker's model comes from | `openai` |
| `PI_MODEL` | matched as a substring of the provider's model ids; the first match is used | `mini` |
| `PI_TEMPORAL_DATA` | the host directory for shadow repositories and writer markers | `~/.pi-temporal` |
| `PI_TEMPORAL_DURABLE_TURNS` | `0` turns off the workflow behind each live turn | on |
| `PI_TEMPORAL_EMBEDDED_WORKER` | `0` when a standalone worker owns the queue (`npm run worker`) | on |

Reaching a server that is not the dev server:

```bash
TEMPORAL_ADDRESS=your-ns.a1b2c.tmprl.cloud:7233 TEMPORAL_NAMESPACE=your-ns.a1b2c \
  PI_TEMPORAL_API_KEY_FILE=/run/secrets/temporal-key      # Temporal Cloud
TEMPORAL_ADDRESS=temporal.internal:7233 \
  PI_TEMPORAL_TLS_CERT=/run/secrets/tls.crt PI_TEMPORAL_TLS_KEY=/run/secrets/tls.key \
  PI_TEMPORAL_TLS_CA=/run/secrets/ca.crt                  # a cluster with mTLS
```

The key is read from a file rather than passed in argv, and nothing prints it. `npx tsx
src/cli.ts doctor` prints the resolved configuration, the profile checks, and server
reachability.

## The Pi fork it needs

The stepped surface is not in the published Pi package, so the dependency is a build of the fork,
pinned by commit in `fork.pin`:

```
npm ci
npm run setup-fork
```

`setup-fork` fetches that exact commit into `.fork/pi` (ignored), builds it, and links it into
`node_modules`. Run it again after any `npm ci`. The pin is `moe/pin-head`, a merge of the open
fork PRs, and moves to fork main once they merge:

- [temporalio/pi#9](https://github.com/temporalio/pi/pull/9), merged: one turn of the agent loop
  split into a model call, its tool calls, and a seal.
- [temporalio/pi#12](https://github.com/temporalio/pi/pull/12): `setWriteGuard`, which every
  write to the session file asks first.
- [temporalio/pi#2](https://github.com/temporalio/pi/pull/2): `prepareStep()`, which settles what
  a stopped turn left behind, and `recordPrompt(text)`.
- [temporalio/pi#4](https://github.com/temporalio/pi/pull/4): `modelCall`, `runToolCall`, and
  `sealStep`, handed to an extension through `pi.registerTurnExecutor` as `turn.steps`.

The seal reads no retry budget off the transcript. Each seal returns the count it spent, and the
workflow carries it into the next one. The plan for landing this surface upstream is in
[docs/upstream.md](docs/upstream.md).

The repo is a pi package, internal to the temporalio organization, so installing it into pi needs
access:

```
pi install git:git@github.com:temporalio/pi-temporal
pi install /path/to/pi-temporal      # a local checkout
pi install -l /path/to/pi-temporal   # this project only
```

If Temporal setup fails, the extension reports the failure and runs the local Pi path. That
fallback does not have Temporal recovery.

## Checks

The checks live in `checks/`, one standalone script per contract, PASS/FAIL on exit code.
`scripts/run-checks.sh` runs every one that needs neither a model key nor Docker, which is what
CI runs; most need only a local Temporal server. The serious ones run real workers in real
processes: `seal-check.mts` kills a seal after its writes and retries it on another worker,
`fence-check.mts` freezes a worker mid-call and proves its late write is refused,
`detached-check.mts` (model key needed) kills a worker mid-tool and watches a second machine
finish the turn. What each check covers, and what it deliberately does not, is in
[docs/guarantees.md](docs/guarantees.md).

## Layout

- `extensions/temporal.ts`: the pi extension: the turn executor, and `/background`.
- `src/workflow.ts`: `piSession`, the per-session durable executor.
- `src/activities.ts`: `runStep`, and the `runModelCall` / `runToolCall` / `sealStep` that split
  it; the session file is the log.
- `src/local-turn-workflow.ts` and `src/local-turn-activity.ts`: `piLocalTurn`, one workflow per
  turn of a live session.
- `src/l2-step.ts`: how a stepped step's calls fan out and how an interrupt reaches them.
- `src/pending.ts`: what a step knows about its calls before it is sealed.
- `src/worktree.ts`: the travelling project tree.
- `src/session-lock.ts`: the lease a writing worker holds.
- `src/config.ts`, `src/protocol.ts`, `src/client.ts`, `src/cli.ts`, `src/worker.ts`,
  `src/session-worker.ts`: wiring, shared types, and the processes.
- `checks/`: the checks, their scripted-model worker (`faux-worker.mts`), and the kept histories
  `replay-check.mts` replays.
- `scripts/`: fork setup, a local dev server, the smoke runs, `submit.mts`, `inspect.mts`.
- `docs/`: [guarantees.md](docs/guarantees.md), the durability contract and its evidence;
  [upstream.md](docs/upstream.md), the upstreaming plan.

## Prior art

[osolmaz/pi-workflows](https://github.com/osolmaz/pi-workflows) makes user-defined workflow
graphs durable in Pi with a home-grown file-queue engine. This project targets the agent's own
turn loop, durable via Temporal. We reuse its integration ideas, not its graph feature.
