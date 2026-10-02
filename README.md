# pi-temporal

A Temporal executor for the [Pi coding agent](https://github.com/earendil-works/pi),
packaged as an extension around the fork's SDK. Pi keeps its conversation record;
Temporal drives execution. This package requires the fork APIs listed below.

Worker mode uses one activity per step by default in the `local` profile. A step
contains a model call and its tools. `PI_TEMPORAL_STEPPED=1`, also the `fleet`
profile default, separates the model call, tool calls, and seal into activities.

Completed activity results replay from Temporal history. Retrying unfinished
work still depends on the session record, dispatch claims, and project storage.
A timeout does not prove that the old activity or its tool stopped.

## Depends on the Pi fork

This uses the fork APIs developed in three pull requests. The source used here is pinned in `fork.pin`; this review does not establish the API surface of the current published package. [temporalio/pi#2](https://github.com/temporalio/pi/pull/2) adds the four calls a stepped driver needs:

- `recordPrompt(text)` puts a prompt in the transcript without running it.
- `step()` runs one model call and its tools, and reports whether the turn is done.
- `prepareStep()` settles what a stopped turn left behind, without running to the end of it.
- `resumeInterruptedTurn()` is `prepareStep()` plus a run to the end of the turn, for a caller that wants the whole turn back in one call.

[temporalio/pi#3](https://github.com/temporalio/pi/pull/3), stacked on it, adds `pi.registerTurnExecutor`, which lets an extension take over when a session's own turns run.

[temporalio/pi#4](https://github.com/temporalio/pi/pull/4), stacked on that, splits a step into `modelCall`, `runToolCall` and `sealStep`, and hands the same three to a registered executor as `turn.steps`.

So the dependency is a build of the fork, pinned by commit in `fork.pin`:

```
npm ci
npm run setup-fork
```

`setup-fork` fetches that exact commit into `.fork/pi` (ignored), builds it, and links it into `node_modules`. CI uses the same setup commands. The pin selects fork source; the dependency lock and setup inputs also affect the build. The linked package resolves its sibling `@earendil-works/pi-agent-core` (which carries `Agent.step`) from the fork's own workspace, so the whole fork API is picked up.

Working in the fork itself needs one more thing, and it needs the network: its model catalog under
`packages/ai/src/providers/data/` is generated and ignored, so a clone that has never fetched it
fails `tsgo --noEmit` with errors about models nobody has heard of, and the fork's own pre-commit
hook fails with it. `npm run -w @earendil-works/pi-ai hydrate-model-data` writes it, after which
`npm run check` passes. That is upstream's arrangement, not something this pin can carry.

Run `setup-fork` after any `npm ci`, which wipes `node_modules` and takes the link with it. To select different fork source, edit `PI_FORK_REF` in `fork.pin` and run it again.

## Install it into pi

The repo is a pi package, so pi can install it:

```
pi install git:github.com/temporalio/pi-temporal
pi install /path/to/pi-temporal      # a local checkout
pi install -l /path/to/pi-temporal   # this project only
```

When the fork exposes `registerTurnExecutor`, the extension registers a turn
executor and starts a worker in the Pi process. A reachable Temporal server is
still required. `PI_TEMPORAL_DURABLE_TURNS=0` disables this path;
`PI_TEMPORAL_STEPPED=1` gives a live turn separate model, tool, and seal activities.

The activities use the in-memory session and a queue belonging to that process.
They cannot migrate to another process in this implementation. After a crash,
reopening the session can resume unfinished work through the executor hook.
Unsealed tool results kept only in memory are lost; unanswered calls are settled
as unknown outcomes before the model continues.

The original live test killed Pi during a tool call and reopened it with `pi -c`.
That is a historical test result, not evidence that every external effect can be
recovered. The model can inspect the effect's destination when a tool supports it.

If Temporal setup fails, the extension reports the failure and runs the local
Pi path. That fallback does not have Temporal recovery.

## Sending a task away: /background

Durability is not the same as offloading, so that has its own command:

- `/background <task>` gives a task its own session that a worker owns, and returns straight away.
- `/background-status` shows what this session is waiting on.
- `/background-stop` interrupts it.

A background task uses a separate worker-owned session. It can continue after Pi exits if
another worker polls the queue and can access the record and project. When the task finishes,
the answer arrives as context for your next prompt. The retry and migration limits below still apply.

Quitting Pi stops its embedded worker. The workflow remains in Temporal, but progress waits
for an eligible worker. An uncertain failure on a pinned tool ends that turn rather than moving
remaining tools to a directory an old attempt might still modify.

The worker needs the fork build. Set `PI_TEMPORAL_EMBEDDED_WORKER=0` when a standalone
worker owns the queue, and run `npm run worker` from the driver clone. Live-turn execution
still requires the fork hook. Compatibility with an unmodified published Pi is not checked here.

## Why the session you type in cannot be handed to a worker

The ordinary extension context exposes a read-only session manager. The fork's
`registerTurnExecutor` hook supplies control of the current turn, not a transfer
of session ownership. Moving it elsewhere would require transferring the
transcript writer and workspace too. `/background` avoids that transfer by
creating a worker-owned session.

## Try it

Two terminals. First the server:

```
./scripts/temporal-dev.sh          # 127.0.0.1:7233, UI on 8233
```

Then pi, launched in whatever project you want the task to work on. `$PI_TEMPORAL` is wherever you cloned this:

```
cd /path/to/your/project
"$PI_TEMPORAL"/scripts/run-pi.sh
```

With Temporal connected, each live turn gets a workflow: `temporal workflow list --address 127.0.0.1:7233` shows one
`piLocalTurn` per prompt. To see the recovery, kill pi during a tool call (`Use the bash tool to run:
sleep 45; echo late`), reopen with `scripts/run-pi.sh -c`, and watch the interrupted call get settled.

For the other half, type `/background Use the bash tool to write hello into note.txt, then reply DONE.`
It returns straight away and a worker takes it from there. The tools run in the directory you launched
pi from, so `note.txt` lands there.

`run-pi.sh` needs `OPENAI_API_KEY`, or `OPENAI_API_KEY_FILE` pointing at a file with one, and it pins
the model to `gpt-4o-mini` so a test does not depend on which model the TUI has selected. It runs the
fork build from `.fork/pi`, so `npm ci && npm run setup-fork` has to have happened.

To check the whole path without typing:

```
./scripts/background-smoke.sh         # starts a worker, submits a turn, waits, exits non-zero on failure
npx tsx step-loop-check.mts        # one activity per step, and interrupts; no model key needed
```

`background-smoke.sh` drives the standalone worker rather than the one inside pi, because print mode exits the moment the command returns and takes that worker with it.

## The idea

Pi owns the conversation schema and its parent-linked JSONL record. Temporal
keeps workflow progress and dispatches activities. The app's record also holds
execution facts used when an activity retries.

There are more than two durable states. Worker mode keeps dispatch claims and
results beside the transcript. Tree shipping adds shared bundles and host-local
notes. Recovery depends on the agreement between those states and Temporal
history, not just on each file surviving.

## Granularity: a step per activity

One `runStep` activity does one thing:

1. If the prompt is not in the transcript, `recordPrompt` puts it there. Nothing runs yet.
2. Otherwise `prepareStep` settles what an earlier attempt left behind. It returns false when the turn already has its answer, which is a retry landing after the last step finished.
3. `step()` runs one model call and the tools it asks for, and says whether the turn is done.

The workflow loops that until a step reports done, so the number of activities is the number of steps. Nothing in the activity reads the workflow's step number: the transcript decides what runs next, and the workflow only counts so a runaway turn hits a ceiling.

If a completed step did not report back, its retry reads the transcript and may advance the
next step. Temporal activity count therefore need not equal model-step count after recovery.
A partially recorded tool batch needs separate handling: `prepareStep` reports unanswered
calls as unknown outcomes. This does not make a whole step atomic.

## A tool call per activity

`PI_TEMPORAL_STEPPED=1` splits the step into three:

```
runModelCall  ->  runToolCall (one per call)  ->  sealStep
```

Off by default in the `local` profile and on by default in `fleet`. It applies to both halves: a `/background` task on a worker, and every turn of the session you are typing in.

The calls of a step overlap on the worker half, where each activity opens a session of its own. They do not on the live half: those calls all reach the one agent that pi process holds, and it admits a single unit of work at a time, so a second call arriving while the first runs would be refused and reported as an unknown outcome for a tool that never ran.

The split exposes per-tool retry and timeout boundaries to workflow code. It also creates a
place for durable approvals or budgets, but does not implement those features by itself.
Whole-step mode can keep dispatch and approval state in application storage; it does not expose
those pauses as separate workflow operations.

Two storage rules matter here.

**The seal is the only writer of the step's results.** Not of the transcript: the model call writes the assistant message. Pi's session file is a tree, and every entry takes its parent from the leaf the writer last saw. Two calls settling at once would each parent off the leaf they saw and branch the transcript, and Pi's own parallel path appends results in call order after the batch, which per-call activities would lose. So a call reports its result and the seal records the step's results together, in the order the model asked. The fork tests compare selected split and whole-step transcripts.

Worker activities that write the transcript take a lease beside the session file. Tree shipping
also uses a lock for the host directory and a shared tree-store lock. These cooperate with write
guards; they do not fence arbitrary tools or make a synchronous append atomic with lease validation.

**Worker dispatches keep a persistent admission claim.** A dispatch writes a note beside the session file before the tool can have any effect, and keeps the result there when it comes back. A second dispatch that finds a result returns it; one that finds only the note reports the outcome as unknown rather than running a `git push` that may already have landed. The attempt number would answer the same question far less precisely: it counts every way a dispatch can die, including the ones that never reached the tool.

The kept results live in `<session>.jsonl.pending/<turn>/<step>/`, scoped by both because a call id is only unique within the message that asked for it and a turn numbers its steps from one again. The next step's model call drops the results of the steps before it. The seal deliberately does not drop its own: a seal whose answer never reached Temporal runs again, and a batch it reads as empty is a batch it reads as wanting another step, even when a tool asked the turn to stop.

The notes are not dropped with the results, and that is the whole reason for the turn in the path. The attempt a note guards against is one that stalled: it comes back after the seal wrote the answer and after the cleanup that followed, and nothing else on disk can then tell its call from one nothing has run yet. An empty file per call is what keeping it costs. Under a scope of step alone the next turn's step 1 would read the last turn's step 1 as its own, and report a tool that never ran as already dispatched.

Worker tool results stay beside the session file to reduce history payloads. Built-in tools
truncate some outputs, but extensions can return different sizes. The driver must not assume
that every result is bounded to 50 KB.

Each tool call gets 30 minutes per attempt by default. A call that crosses it is not run again, since its note says it started, so it ends as an unknown outcome while the tool may still be running. A deployment with longer tools sets `PI_TEMPORAL_TOOL_TIMEOUT_MINUTES` on the client that starts the session.

What it costs: each activity opens the session file and builds an `AgentSession` of its own, so a step with four calls pays six session opens instead of one. Against a model call that takes seconds, the boundaries measure in milliseconds (see below), but the cost is real and it grows with the transcript.

## What is durable, and what is not

Completed activity outcomes survive through Temporal history. The app's record
lets a retry identify some work that finished without reporting to Temporal.
Neither record makes a tool effect and its result atomic.

Worker split mode saves a result after the tool returns. A later dispatch can
reuse that file. If the process dies before saving it, the persistent dispatch
claim leads to an unknown outcome. Whole-step mode and live-process mode can
lose completed but unsealed results. Live split mode keeps its intermediate
results in memory, not in the worker's pending directory.

On interruption, the split workflow attempts a seal in a non-cancellable scope.
The worker seal keeps completed results without restoring or capturing the
project or running post-step compaction. A failed cleanup seal is logged; this
path cannot promise that every cancellation records all results.

The worker session workflow continues as new between turns when the server
suggests it or `maxHistory` is reached. It carries queued input and the last
outcome. The live-turn workflow has a step ceiling but no continue-as-new path.
A single large turn can still exceed history or payload limits.

Worker transcript writes use `setWriteGuard` with the session lease's last
confirmed timestamp. The synchronous append path cannot read shared storage
atomically with its write. The 50-second validity window and 60-second reclaim
age leave an allowance for clock differences, not a storage-level fence or a
validated bound on clock skew.

### What recovers, and what a person has to answer for

Live-process mode uses the `pi` process's session. Worker mode reconstructs its
session per activity. The table separates their contracts and names the scope
of each check. A listed check is not proof of every interleaving in its row.
The review package records current execution and mutation evidence separately;
older live runs are labelled below.

| What fails | Live-process mode | Worker mode | Checks and scope |
|---|---|---|---|
| A prompt is accepted before execution starts | The hook records the prompt before checking its stop flag; process death before persistence is not covered. | Input accepted by Temporal waits for a worker. Initialization must register query and interrupt handlers before project adoption finishes. | `local-turn-check.mts` covers an early stop. `workflow-init-check.mts` covers initialization handlers, not every acceptance-to-wake crash. |
| The process dies between steps | Sealed transcript entries remain; reopening starts recovery through the hook. | Completed activities replay. Retry reads the transcript on an eligible worker, subject to the pinned-failure rule below. | `step-loop-check.mts` uses stub activities and no process death. `detached-check.mts` is a model-backed process test reported by the package author. |
| The process dies with a tool in flight | Unsealed in-memory results can be lost. Reopening reports unanswered calls as unknown. | The step is closed without that host: a recovery seal records the results it had, a claim without a result reports unknown, and the closure is written where every host reads it so the old one cannot publish for that step afterwards. The turn then goes on with the next step, on whatever worker is free. What does not move is the rest of that step, because the attempt started. | `pending-check.mts` checks file behavior. `lost-host-check.mts` covers the closure and the hand-back with fake activities and the real tree store. `detached-check.mts` kills a real worker mid-tool and asserts the turn is answered by another one, the results are recorded, and the tool runs once per time the model asked. |
| Two attempts of a writing activity overlap | The agent admits one unit at a time. The worker lease checks do not exercise this path. | Lease claims and write guards reduce overlap. Timestamp guards do not atomically fence the append or external tools. | `session-lock-check.mts`, `lock-gap-check.mts`, and `stall-check.mts` cover selected worker-lease interleavings and a stopped event loop. |
| A stale dispatch resumes after result cleanup | No persistent dispatch claim in this path. | Claims outlive result cleanup, so the same turn, step, and call cannot be freshly admitted again. | `stale-dispatch-check.mts` drives the worker activity with a fake session and stalls before claiming. |
| The user stops a turn | The workflow attempts to seal in-memory results. Process death can still lose them. | The workflow attempts to seal saved results without moving the project. | `local-turn-check.mts` covers the live loop with fake turns. `interrupted-seal-check.mts` exercises the worker seal with fake session persistence. `l2-step-check.mts` covers cancellation routing. |
| A host captures while behind the tree tip | Live activities do not ship the project. | Capture refuses; recovery can save unshipped work under `salvage/`. This guard is not tool isolation. | `worktree-check.mts` models two host directories. `storage-repair-check.mts` checks cleanup. `migration-rejoin-check.mts` covers a stale tool after host rejoin. |
| A tool that outlived its dispatch publishes afterwards | Not reachable: nothing outlives the process holding the session. | Its step was closed without it, so the capture is refused wherever it comes from and what it wrote is kept under `salvage/`. The tip rule alone cannot answer this one: the stale writer is still standing on the tree it read, so it publishes cleanly and reverts what replaced it. | `lost-host-check.mts` runs that ordering against the real tree store and fails if the closure is not written, or if the seal that closes the step publishes. Those two clauses are the contract both forks are held to; OpenCode's `packages/temporal/test/lost-host.test.ts` runs the same scenario against its own fence. |
| A host retains a directory for a retired session | No worker-owned directory note in this path. | Shared retirement state permits later cleanup. Changed local files can prevent release. | `storage-repair-check.mts` covers revival during sweep, a clean host behind the tip, and forgotten-session cleanup. |
| A turn spends more than it was meant to | Nothing bounds it: the loop is in the process the model is answering. | An operator's budget stops the turn on tokens or on wall clock, and the session takes the next prompt. Each model call reports what the step spent and what the session has been billed in total, and a session's bound is measured against that total, which is read off the record and survives a rollover, an idle retirement, and a turn some other client ran. A deadline (`hardSeconds`) stops the turn where it is instead of where it can, which is what a user pressing stop does. Off unless somebody sets it: a bound that ends real work is worse than none. | `budget-check.mts` drives every bound against a real server with stub activities; `spend-check.mts` drives the real activity to check what it reports. Neither checks a provider's billing. Without a deadline, a call that has started is never stopped, so a turn overshoots by whatever was running: a serial batch stops at the next call, a parallel one finishes. |
| History grows past one run | No continue-as-new. A step ceiling is not a payload bound. | Continue-as-new between turns carries accepted prompts and the last outcome. One long turn can still exceed history limits. | `rollover-check.mts` uses a small history threshold and stub activities; it does not exercise maximum payload size. |
| The pinned worker stops answering | Its queue cannot move the in-memory session. Reopen to recover. | Initial schedule-to-start failures can move serially after every pinned sibling settles. Other pinned failures close the step where it is, because the old tool may still be alive; the turn continues with the next step rather than ending. | `l2-step-check.mts` and `workflow-init-check.mts` check dispatch policy. `migration-rejoin-check.mts` fails if uncertain failures are migrated and the old host rejoins. `replay-check.mts` replays a history from before each rule. |
| A later turn reuses a directory an abandoned tool may still write | Not reachable: one process holds the session and its directory. | The host records each call that is inside its own execution and refuses the directory to any other step until that call returns. The refusal is an ordinary failure, so the work is scheduled again and another host takes it; only that directory is stranded. It clears itself where the host can show the call is over. Four readings answer that, because a tool can put down anything the worker gave it: its process is gone, nothing carrying the name the worker exported is running, nothing is left in its control group or its process group, and nothing is standing in the directory. The control group is Linux's and is readable whoever owns the process, which is what answers for a tool that went through `sudo`; standing in the directory means a working directory inside it or a file under it held open, which is what answers for a tool that daemonized and kept neither. The refusal says which of the four is keeping it. And where the directory is one this session built out of an empty one, there is no refusal at all: it is moved to `<dir>.stranded.<time>` and a fresh one is built here, so the writer nobody can account for goes on writing the directory it has open, under its new name, where nothing it does reaches what the session builds next. That is a closure rather than a narrowing, and it is why the four readings do not have to be conclusive. It needs a directory that can be renamed, which a project directory mounted as a volume is not, and it never moves a checkout the session adopted or a directory holding a call of the step now asking for it. Where it cannot move, the refusal stands and `pi-temporal release-tree` is what clears it, and a worker says so at startup rather than waiting for the failure: a project directory that is a mount point, or whose parent it may not write, is reported as a note when the worker comes up. The refusal itself carries a short retry delay rather than climbing the backoff a failing activity earns, because the host that refuses fastest would otherwise push the next attempt minutes out while a free host sits idle. | `quarantine-check.mts` drives the real tool activity and the tree store, covers each way a marker is retired, and asserts a refusal asks for a retry rather than a backoff. `quarantine-routing-check.mts` puts two hosts on one queue, in two processes, and watches the work land on the one that is not refused and the turn get its answer there. |

An unknown result can sometimes be resolved by querying the effect's destination
or using its idempotency key. This driver has no general resolver for arbitrary
commands. It reports uncertainty to the model, which may make a new call.

## A session that outlives its client

`/background` sends a task to a worker, but its commands live inside a pi session, so a task could
only be started, listed and followed from the terminal that started it. Close that terminal and the
task keeps going with nobody able to see it. `src/cli.ts` is the other half:

```bash
# hand a task over and walk away; prints the session id and exits
npx tsx src/cli.ts start "port the auth module to the new API"
# with the tree on, this also sends the project from the directory you are in
npx tsx src/cli.ts start "fix the failing test" --project=/path/to/repo

# what this deployment is running right now
npx tsx src/cli.ts running

# follow one from anywhere, and stop when the turn stops
npx tsx src/cli.ts watch task-1a2b3c4d
npx tsx src/cli.ts stop task-1a2b3c4d
```

There is no application HTTP server in this path. A Temporal server is still required. The workflow holds the
control state and answers `turnState`; the session file holds the conversation. So following a
session is a query plus a tail of its file, and both work from any machine that can reach the
cluster and `PI_SESSION_DIR`. Point that directory at shared storage and the machine that starts a
task, the machine that runs it, and the machine that watches it need not be the same one.

The follower distinguishes these states:

- **A closed run cannot keep a turn live.** A closed workflow answers a query by
  default, with the state it held when it closed, so a run the server terminated reported its turn
  as still running and a follower polled it forever. The client rejects queries against a run that
  is not open.
- **A query is answered by a worker**, so an open session whose workers are all down cannot answer.
  Each query carries its own deadline and the answer is one of three: the state, gone, or
  unreachable. Left as two, one dead worker turns `running` into a listing that hangs.
- **Unreachable is not finished.** A follower that treats "nobody answered" as "the turn ended"
  stops in the middle of a worker restart and reports a turn that is still going as done.
- **A turn is over when the workflow says so**, not when the workflow closes. The supervisor stays
  open for its idle timeout with nothing left to do, so `watch` reads the turn-level state instead.

### Verified

`detached-check.mts` is the model-backed process check. The package author reported this run: a client hands over a task and exits,
worker A starts the turn, A is killed with the tool still in flight, and worker B, which never saw
this session, finishes it. `running` lists the session and `watch` follows it across the handover
from a process that is only ever a client. The tool that was cut off is reported to the model as an
unknown outcome rather than re-run, and the model answered without asking for it again, so the
command ran once. What does not move is the rest of that step: its calls stay with the host that
has them, and the step is closed without it.

Two things that check gets right only because getting them wrong was silent. It kills on observing
a tool in flight rather than after a fixed delay, because a slow command in between pushes the kill
past the end of the turn and then no handover happens at all. And it runs the worker and the CLI as
single processes (`node --import tsx`), because `npx` spawns `tsx` spawns node, so killing the
process you hold leaves the one that matters running.

### Sessions that are already running

Which activities a step schedules is what a workflow writes down, so changing that rule changes
histories that already exist and a worker carrying this code would replay one into a nondeterminism
error. Both rules that changed it are behind `patched()`, so a run recorded under the old one keeps
what it recorded and a new one gets the current rule. Nothing has to be drained before the deploy.
`replay-check.mts` replays a history this code writes and a kept one from before each rule; removing
a patch fails it.

### Across two machines

On one host "another worker" is another process reading the same disk, which proves less than it
looks like. `docker/cross-host-check.sh` puts each worker in its own container: its own filesystem,
its own hostname, and no way to reach the other except through Temporal and the shared session
directory. The original test recorded these worker identities in Temporal history:

```
06:15:49  attempt 1  1@89cc9c4fa607     <- worker A, killed mid-tool
06:16:30  attempt 2  1@252525771cd2     <- worker B, which had never seen this session
```

`/sessions` is a local volume in that run. Separate containers exercise separate project
filesystems, but do not establish network-filesystem lock behavior. The later NFSv4 variant is
described below; these historical results were not repeated in this documentation pass.

## Taking the project with it

The session log travels because it is one file in a shared directory. The files the tools edit did
not, so a worker on a second machine started every step in whatever directory it was pointed at,
found nothing, and told the model the project was empty. `PI_TEMPORAL_SHIP_TREE=1` moves them too:

```bash
PI_TEMPORAL_SHIP_TREE=1 PI_SESSION_DIR=/shared/sessions PI_PROJECT_DIR=/work npm run worker
```

The shape is git's, because git already answers content addressing, an incremental transfer, and a
checkout that removes what a later tree dropped. After a step is written down, `src/worktree.ts`
captures the work tree and writes one bundle into `<session>.jsonl.tree/`. Before a step runs, a
host that is behind unbundles the ones it has not taken in (it records how far it got, so a long
session does not re-unbundle its whole history every activity) and checks the newest tree out. An
unchanged directory produces the tree the tip already names, so it ships nothing.

The rules that bound it:

- **It never touches the project's own `.git`.** The shadow repository is host-local and points at
  the work tree from outside, so a project that is not a git repository works the same as one that
  is, and one that is keeps its own history.
- **Only a host standing on the tip may move it.** A host that had fallen behind used to publish
  its own tree over the tip, which reverted everything shipped since on every host at their next
  restore. That was the worst bug this thing has had, and it was silent.
- **Restore requires an empty directory or a note from this session.** A host behind the tip
  compares its snapshot-visible files with that note before resetting. Local changes can be
  saved to salvage. This comparison does not cover files excluded from snapshots.
- **A host behind the tip can save unshipped changes.** Its snapshot-visible files may hold
  work that no other host received.
  They go to `<session>.jsonl.tree/salvage/` as a self-contained bundle, and the host comes to the
  tip. Recover one with `git bundle unbundle`. Automatic chain pruning leaves them; `forget`
  removes the tree store, including its salvage bundles.
- **Only client-provided project data may establish the project.** A worker must not seed from its own directory: every activity, the
  model call included, lands on whichever worker Temporal had free, so an activity that adopts its
  own directory puts the project wherever the first unit of work happened to go. `start --project=`
  sends it (the flag is required with the tree on, so nothing ships a home directory by accident),
  and `/background` sends the directory you asked from. A session with nothing established refuses
  every activity until a client sends it, which is loud rather than wrong.
- **A schedule copies a client-provided template.** Each firing adopts that shared store before
  its first step. It does not discover the project from the worker's directory.
- **A worker attempts cleanup at startup and when another session needs the directory.** It
  rechecks retirement under the locks. A revived session or changed local files prevent cleanup.
- **The chain restarts rather than growing for ever.** Every fortieth capture carries the whole tree
  and stands on nothing, and the bundles before it are removed once the tip names it. A session that
  runs for hours would otherwise keep every state it has ever been in, and a host joining late would
  unbundle all of them to catch up. Nothing under `salvage/` is touched.
- **A bundle nothing names is dropped, not obeyed.** A writer that died between renaming its bundle
  into place and naming it as the tip leaves one behind, and every host afterwards computes that
  same number. Refusing it wedged the session everywhere rather than on the host that crashed.
- **One session per directory.** A second is refused while the first is using it, in both
  directions. A session hands its directory back when it goes idle, and only then if this host built
  that directory out of an empty one, its captured tree matches the held note, and it comes out empty.
  A directory the host already had is somebody's working copy: what it holds includes the files git
  ignores, which no bundle carries and nothing else has a copy of, so that one keeps its files and
  only the note goes. The retirement runs on one host, and it says the session is over in the shared
  directory as well: every other host reads that the next time a session wants its directory, and
  hands its own back then. Without somewhere shared to ask, the note is the only answer and nothing
  can correct it, so a directory served one session and refused every later one.
- **A refused restore stops the step.** Running against files that are not the project tells the
  model those files are the project, which is worse than not running, so it fails. A retry can reach another eligible host, but the shared queue does not guarantee
  selection of a suitable one. A refused *capture* is different: the tool has already run and
  a retry would find its result rather than run it again, so throwing there costs an attempt and
  still ships nothing. It sets the work aside instead, and says so.
- **A step is pinned to the worker that ran its model call.** The worker polls a queue keyed by
  host and project directory. Its tools share that directory and may overlap. Pinned dispatches
  have a 30-second `scheduleToStartTimeout` and one attempt. If an initial dispatch never starts,
  the workflow waits for all pinned siblings to settle before sending remaining work serially to
  the shared queue. An uncertain pinned failure ends the turn. A timeout cannot distinguish a
  dead host from one whose old tool can still modify the directory. The tip guard does not solve
  this after that host rejoins and accepts the latest tip. Safe migration of that case needs
  workspace isolation or evidence that the old process and tool stopped.
  An interrupted step still attempts to record completed tool results. Its seal skips project
  restore, capture, and post-step work.

- **It is off by default in the `local` profile.** On a laptop the tools already run in the directory you meant, and
  shipping it there is disk spent on a problem that host does not have.

The snapshot excludes files ignored by the project. A rebuilt directory may need an install
step. Cleanup of a directory built by the worker can remove ignored files too; copy anything
that must survive outside that managed directory.

### Verified

`worktree-check.mts` covers the mechanics with two host directories and needs neither a server nor a key:
a file and a nested file arrive, a deletion arrives, an unchanged capture ships nothing, a
directory holding work nothing shipped is left alone, a second session cannot take one that is
already in use in either direction, a tool call cannot establish the project, a host behind the tip
comes to it with unshipped snapshot-visible work kept rather than published, a directory is
handed back only when its captured tree matches its note, a restore does not turn an adopted directory into one it may
empty, and a host that did not run the retirement can still serve the next session.

The assertion worth naming is "nothing the other host shipped is reverted". The check used to set
up exactly the interleaving that loses data, read the one file that survived it, and stay green
while the rest reverted one line away.

`docker/tree-check.sh` is the model-backed container check. The package author reported this run: worker A writes a file, worker A's container is killed,
and worker B, whose `/project` has never held anything, continues the same session and reads both
that file and the rest of the project back. The earlier reported mutation with `PI_TEMPORAL_SHIP_TREE=0` failed three tree assertions.
That mutation was not repeated in this documentation pass.

`NFS=1 docker/tree-check.sh` uses an NFSv4 server for `/sessions`. The package author reported
ten passing assertions on that setup. The lease uses exclusive claim creation and coherent
directory and timestamp reads; bundle publication also uses rename. One tested NFSv4 mount does
not prove those properties for every server or mount configuration. NFSv3 is untested here. Its
absence from the tests is not evidence that it lacks exclusive create or atomic rename.

## Deploying it

Two deployments, not a dozen knobs, because the settings are not independent. `PI_TEMPORAL_PROFILE`
picks one and the rest follow:

| | `local` (default) | `fleet` |
|---|---|---|
| what it is | pi on your machine, worker inside it | workers on machines nobody is sitting at |
| session directory | `~/.pi-temporal/sessions` | **you name it**, on storage every worker reaches |
| project files travel | no | yes |
| unit of work | a whole step | the model call, each tool call, the seal |

Anything above can still be set on its own; the profile only decides what it is when you do not.
`preflight` requires an explicit `PI_SESSION_DIR` and tree shipping in the `fleet` profile. It
does not test that the directory is shared, that hosts resolve the same path, or that their
filesystem and clock behavior meet the lease assumptions. Operators must check those properties.
The profile also refuses a shared-workspace deployment with shipping disabled, even if that
deployment could work under a different placement contract.

Settings no profile decides, read where a session starts (the client, the CLI, or `/background`)
and carried in the session's options, because the workflow may not read the environment. Each is
a whole number, and a value that is not one is refused rather than ignored:

| variable | what it bounds | unset |
|---|---|---|
| `PI_TEMPORAL_TOOL_TIMEOUT_MINUTES` | one attempt of one tool call, in stepped mode | 30 minutes |
| `PI_TEMPORAL_BUDGET_TOKENS` | tokens one turn may spend | no bound |
| `PI_TEMPORAL_BUDGET_SECONDS` | wall clock for one turn, checked between units of work | no bound |
| `PI_TEMPORAL_BUDGET_HARD_SECONDS` | wall clock for one turn, stopping it where it is | no bound |
| `PI_TEMPORAL_BUDGET_SESSION_TOKENS` | tokens the whole session may spend | no bound |
| `PI_TEMPORAL_BUDGET_SESSION_SECONDS` | wall clock for the whole session | no bound |

A tool call that crosses its timeout is not run again, because its dispatch note says it started,
so it ends as an unknown outcome while the tool may still be running. `doctor` prints what is set.

Reaching a server that is not the dev server:

```bash
TEMPORAL_ADDRESS=your-ns.a1b2c.tmprl.cloud:7233 TEMPORAL_NAMESPACE=your-ns.a1b2c \
  PI_TEMPORAL_API_KEY_FILE=/run/secrets/temporal-key      # Temporal Cloud
TEMPORAL_ADDRESS=temporal.internal:7233 \
  PI_TEMPORAL_TLS_CERT=/run/secrets/tls.crt PI_TEMPORAL_TLS_KEY=/run/secrets/tls.key \
  PI_TEMPORAL_TLS_CA=/run/secrets/ca.crt                  # a cluster with mTLS
```

The key is read from a file rather than passed in argv, and nothing prints it. Clients and workers use the same connection helper. Different environment values can still
point them at different clusters, namespaces, or queues.

Print the resolved configuration:

```bash
npx tsx src/cli.ts doctor
```

It reports the profile checks, API-key checks, and incomplete certificate pairs. Plaintext
to a non-loopback address is a note, not a refusal. It also calls `getSystemInfo` to check server reachability. It does not verify
fleet agreement or shared-storage semantics.

## A turn nobody started

`start` hands a task over and returns, but something still has to run it. A schedule does not:

```bash
npx tsx src/cli.ts schedule "review yesterday's merges" --cron="0 9 * * *" --id=morning
npx tsx src/cli.ts unschedule morning
```

With the tree on it also needs the project, which nothing is running at firing time to send:

```bash
npx tsx src/cli.ts schedule "review yesterday's merges" --cron="0 9 * * *" --id=morning \
  --project=/path/to/repo
```

The client sends it once, into a store beside the sessions, and each firing copies that store into
its own session before its first step. A worker still may not establish a project from the directory
it is standing in, which is the rule that keeps an empty `/project` from becoming the project
everywhere; copying a store a client wrote is a different act, and it is the one that lets a
schedule and the travelling tree compose. `unschedule` keeps the template because an accepted
firing may still be waiting for a worker to copy it. After every started firing finishes,
`pi-temporal forget schedule-<scheduleId>` removes the template.

Each firing is its own session, because the workflow takes the task in its input and derives its
own id from the firing it was given. Two things make that work rather than one. `initialPrompt` in
the workflow input is the task, so nothing has to be running to send a first prompt. And the
schedule names its workflow the way a session's workflow is always named, because Temporal appends
the firing time to it: without that, a scheduled run lands on an id `running` and `watch` do not
recognise, and the only sessions anyone could see would be the ones a client started.

## Reproducing

The following checks need no model key:

- `step-loop-check.mts`, `local-turn-check.mts`, `workflow-init-check.mts`, and
  `rollover-check.mts` need a Temporal server. They use stub activities or turns
  to check workflow control, initialization, and continue-as-new.
- `l2-step-check.mts` checks tool overlap, fallback policy, and cancellation with
  fake activities. `migration-rejoin-check.mts` also uses fake activities but real
  temporary project directories to test the stale-host migration case.
- `pending-check.mts` checks claims, result files, and cleanup.
  `stale-dispatch-check.mts` drives a worker dispatch through cleanup while a
  competing attempt waits before claiming.
- `session-lock-check.mts` covers selected acquisition, expiry, and refresh
  interleavings. `lock-gap-check.mts` adds a third contender.
  `stall-check.mts` stops a child process's event loop past the lease window and
  takes about 70 seconds. Passing them is not proof of strict mutual exclusion
  under arbitrary storage delays or clock skew.
- `worktree-check.mts` and `storage-repair-check.mts` use separate host data
  directories over a shared session directory. `quarantine-check.mts` covers the
  writer markers, including each way one is retired: the call returns, the
  writer left nothing running that carries its name and nothing in its group,
  the machine restarted, or an operator ran `release-tree`. It spawns and kills
  real processes to say so, including one that leaves its worker's group and one
  that is given no environment of its own, which is what says the name reaches a
  tool at all.
- `docker/liveness-check.sh` asks those same questions inside a container, because the readings
  are made from `/proc` on Linux and from `ps` and `lsof` everywhere else, and a laptop only ever
  runs the second. It needs neither a server nor a key. Both container checks build the image
  every run and refuse to start when `.fork/pi` is not the commit `fork.pin` names, or has
  uncommitted changes: the driver's source is mounted over the image, the fork is not, so a
  checkout left behind would be built in and read as current.
- `lost-host-check.mts` closes a step without its host, then lets the abandoned
  tool finish and try to publish. It uses fake activities and the real tree store,
  and asserts the turn is handed back rather than ended.
- `interrupted-seal-check.mts` exercises the worker seal with fake session
  persistence and checks that interruption does not touch project storage, and
  that a stop leaves the step open to its host where a lost host does not.
- `budget-check.mts` needs a Temporal server. It bounds a turn and a session by
  tokens, by wall clock and by a deadline, and checks where each stopped, with
  stubs reporting the spend. `spend-check.mts` needs neither: it drives the real
  activity against a faked session to check the two numbers it reports, the
  difference this step made and what the session has been billed in total.
- `replay-check.mts` needs a Temporal server. It records a history, replays it,
  and replays the histories under `histories/`, each recorded by the code that
  predates a rule that changed what a step schedules. Record another by
  reverting that rule, running this check with `REPLAY_HISTORY=` pointing at a
  file to keep, and putting it there.
- `unschedule-check.mts` checks that deleting a schedule retains a template an
  accepted firing can still need.

`detached-check.mts`, the smoke helpers, and container checks also need a model
key. Their historical runs are described above. `submit.mts` submits a prompt;
`inspect.mts` reads a session file.

To inspect workflow state, run
`temporal workflow query --workflow-id pi-session-<id> --name turnState`. It
reports queued input, the current step, and the last turn outcome.

For a process-death test, use `detached-check.mts`. It owns the worker process
IDs and kills their process groups. A name-based `pkill` can kill an unrelated
worker or leave a wrapper's child alive, invalidating the test.

## Layout

- `extensions/temporal.ts`: the pi extension: the turn executor, and `/background`.
- `src/config.ts`: Temporal + Pi wiring from env.
- `src/protocol.ts`: workflow ids, signal/query names, shared types.
- `src/local-turn-workflow.ts`: `piLocalTurn`: one workflow per turn of a live session.
- `src/local-turn-activity.ts`: works on the turn the pi process is holding: the whole turn, or its parts.
- `src/activities.ts`: `runStep`, and the `runModelCall` / `runToolCall` / `sealStep` that split it, via `@earendil-works/pi-coding-agent`, session file as the log.
- `src/l2-step.ts`: the stepped step body: how a step's calls are fanned out and how an interrupt reaches them.
- `src/pending.ts`: what a step knows about its calls before it is sealed, beside the session file.
- `src/workflow.ts`: `piSession`: per-session durable executor (submit prompt, step to the end of the turn, interrupt, idle-terminate).
- `src/session-worker.ts`: builds the worker; used by the standalone process and by the extension.
- `src/worker.ts`: the standalone worker process.
- `src/client.ts`: helpers to submit a prompt / interrupt a session.
- `src/demo.ts`: end-to-end smoke once a model key is set.

## What upstream would have to take

A sixth review measured this against the harness it forks rather than against
itself, which nobody had done. The seam is three methods; the patch is not.

Pi's fork, against a `main` level with `earendil-works/pi`:

| | files | lines |
|---|---|---|
| production | 9 | +1356 / -213 |
| tests it adds | 8 | +2088 |
| documentation | 1 | +49 |

That production figure moved twice this round. The review's reduction took an
unused stream adapter and seven internal exports out, from +1324 to +1258. The
preparation fix put +98 back, because carrying the completed turn and what its
preparation returned through the model call, the tools and the seal is state the
host has to hold. Net it is 32 lines above where the round started, and correct
where it was not.

The order to ask for it in, smallest first:

1. **`feat(coding-agent): reject session appends through a write guard`**, on
   branch `moe/session-write-guard`. Two files, 85 lines, no Temporal anywhere in
   it. Useful to anyone with a stale or read-only writer of a session file.
   Built on `main`, tested there, and its three tests were checked by removing
   the guard call to watch two of them fail.
2. **Share the model and tool phases of a turn.** The extraction, with the
   completed turn travelling through it. No replay, no executor registration.
   The ordinary `prompt()` path is the acceptance test.
3. **Expose the step cursor.** What 2 makes possible: pause after the model,
   settle tools, then close the step. This is the contract the preparation fix
   defines, and the one an external driver actually needs.
4. **Let an extension drive a turn.** Executor registration and `recordPrompt`,
   on top of 3.
5. **Resume an interrupted local turn.** The host's own crash recovery, if the
   maintainers want it. Dispatch claims, durable retry counts and Temporal's
   retry classification stay here, in the driver.

Only the first is built and tested in isolation. The rest is an order of
dependency, not four more branches.

## Status

The supplied tree contains both execution modes, detached commands, project
shipping, schedule templates, and deployment profiles. `fork.pin` selects the
fork required by the driver. The recovery table distinguishes implementation
contracts from what each named check exercises.

The review history changed several claims. A tool result is not recorded
atomically with its effect. The seal writes tool results, while the model call
writes the assistant message. Tree-store exclusion uses both host-local and
shared locks. A pinned timeout can leave a writer alive, so it cannot promise
migration merely because Temporal stopped waiting for that attempt.

Limits that remain:

- `forget` is explicit. It removes the tree store, including `salvage/`, so copy
  wanted recovery bundles first. A forgotten-session marker remains to permit
  cleanup of host-local notes.
- A schedule copies its template for every firing. `unschedule` retains the
  template for accepted firings. Remove it only after those firings no longer
  need it.
- The session lease depends on storage and clock behavior. The NFSv4 test covers
  one setup; NFSv3 and arbitrary clock skew are not covered.
- Live intermediate results are in memory. Worker pending claims remain after
  result cleanup. Those paths have different retention and recovery behavior.
- Whole-step worker retries rebuild the session, so the session's retry budget
  resets between activities. The workflow's step ceiling is the fallback bound.

Historical live measurements, reported during development with `gpt-4o-mini`:

- A worker turn reading two files used six activities. The tool calls were
  scheduled 17 microseconds apart and overlapped in a 31 ms interval. The seal
  took 8 ms; model calls took 3.1 and 2.6 seconds. These are one placement's
  measurements, not a deployment latency estimate.
- A worker killed after appending a line and while sleeping left one line. The
  recovered dispatch reported an unknown outcome. The model checked the file.
- An interrupted `sleep 90` left the workflow serving another prompt. The
  signal and `ActivityTaskCancelRequested` appeared in the same second.
- A live turn used five split activities. A separate live crash test reopened
  Pi with `-c` and reported the unanswered call as unknown before continuing.

Those runs document earlier behavior. They were not repeated by this prose
pass, and their retry outcomes do not override the current pinned-failure policy.

## Prior art

[osolmaz/pi-workflows](https://github.com/osolmaz/pi-workflows) makes user-defined workflow graphs durable in Pi with a home-grown file-queue engine. This project is a different target: the agent's own turn loop, durable via Temporal. We reuse its integration ideas (headless Pi driven from an outer host), not its graph feature.
