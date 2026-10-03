# What survives, and how we know

The durability contract: what each mode promises when a process dies, a worker stalls, or a
tool outlives its dispatch, and the named check that holds each promise. The README covers
use; this page covers the contract and the evidence.

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
3. `modelCall()` asks the model, `runToolCall()` runs each call it asked for, one at a time,
   and `sealStep()` records the results and says whether the turn is done. The seal is handed
   the retry count the workflow carried and returns what it spent.

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
| The user stops a turn | The workflow attempts to seal in-memory results. Process death can still lose them. | The workflow attempts to seal saved results without moving the project. A whole step stops between units inside its one activity: the unit that started finishes, the next does not start, and the step is sealed with what it has. | `local-turn-check.mts` covers the live loop with fake turns. `interrupted-seal-check.mts` exercises the worker seal with fake session persistence. `l2-step-check.mts` covers cancellation routing. `seal-check.mts` stops a whole step mid-call over a real session. |
| A host captures while behind the tree tip | Live activities do not ship the project. | Capture refuses; recovery can save unshipped work under `salvage/`. This guard is not tool isolation. | `worktree-check.mts` models two host directories. `storage-repair-check.mts` checks cleanup. `migration-rejoin-check.mts` covers a stale tool after host rejoin. |
| A tool that outlived its dispatch publishes afterwards | Not reachable: nothing outlives the process holding the session. | Its step was closed without it, so the capture is refused wherever it comes from and what it wrote is kept under `salvage/`. The tip rule alone cannot answer this one: the stale writer is still standing on the tree it read, so it publishes cleanly and reverts what replaced it. | `lost-host-check.mts` runs that ordering against the real tree store and fails if the closure is not written, or if the seal that closes the step publishes. Those two clauses are the contract both forks are held to; OpenCode's `packages/temporal/test/lost-host.test.ts` runs the same scenario against its own fence. |
| A host retains a directory for a retired session | No worker-owned directory note in this path. | Shared retirement state permits later cleanup. Changed local files can prevent release. | `storage-repair-check.mts` covers revival during sweep, a clean host behind the tip, and forgotten-session cleanup. |
| A turn spends more than it was meant to | Nothing bounds it: the loop is in the process the model is answering. | An operator's budget stops the turn on tokens or on wall clock, and the session takes the next prompt. Each model call reports what the step spent and what the session has been billed in total, and a session's token bound is measured against that total, which is read off the record and survives a rollover, an idle retirement, and a turn some other client ran. A session's seconds are the workflow's own count, carried across a rollover but not an idle retirement: a session woken again later starts that count from zero. A deadline (`hardSeconds`) stops the turn where it is instead of where it can, which is what a user pressing stop does. Off unless somebody sets it: a bound that ends real work is worse than none. | `budget-check.mts` drives every bound against a real server with stub activities; `spend-check.mts` drives the real activity to check what it reports. Neither checks a provider's billing. Without a deadline, a call that has started is never stopped, so a turn overshoots by whatever was running: a serial batch stops at the next call, a parallel one finishes. |
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

The snapshot excludes files ignored by the project. A nested git checkout inside the project
ships as a gitlink, a pointer to a commit, so it comes back empty on restore. A rebuilt directory
may need an install step. Cleanup of a directory built by the worker can remove ignored files too; copy anything
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
- Worker retries rebuild the session in both modes. The workflow carries the retry count each
  seal returns into the next one, so the budget holds across activities; the step ceiling is
  the bound behind it.

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

