# pi-temporal

A Temporal-backed durable executor for the [Pi coding agent](https://github.com/earendil-works/pi), shipped as a plugin around Pi's SDK. Same pattern we proved on the OpenCode fork: the agent's own loop runs under a durable executor, while the session record stays in the app's own log.

The durable unit is one step: a single model call and the tools it asks for. The workflow runs one Temporal activity per step, so a worker dying takes one step with it and every step before it stays done.

Behind `PI_TEMPORAL_STEPPED=1` the unit is smaller still: the model call, each tool call, and the seal are activities of their own. See [A tool call per activity](#a-tool-call-per-activity).

Status: verified end to end against a live Pi (SDK 0.84.2 fork). A turn that took three steps cost three activities. Killing the worker mid-step re-drove that step alone: the step before it stayed done and its `>>` append did not happen twice, the step in flight came back as attempt 2 on a fresh worker, the prompt was not re-added, and the turn ran on to its answer.

## Depends on the Pi fork

This needs three pull requests on the fork, none of which is in the published `@earendil-works/pi-coding-agent`. [temporalio/pi#2](https://github.com/temporalio/pi/pull/2) adds the four calls a stepped driver needs:

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

`setup-fork` fetches that exact commit into `.fork/pi` (ignored), builds it, and links it into `node_modules`. CI runs the same two commands, so a fresh clone and a CI run get the same build. The linked package resolves its sibling `@earendil-works/pi-agent-core` (which carries `Agent.step`) from the fork's own workspace, so the whole fork API is picked up.

Run `setup-fork` after any `npm ci`, which wipes `node_modules` and takes the link with it. To move to a newer commit of the PR, edit `PI_FORK_REF` in `fork.pin` and run it again.

## Install it into pi

The repo is a pi package, so pi can install it:

```
pi install git:github.com/temporalio/pi-temporal
pi install /path/to/pi-temporal      # a local checkout
pi install -l /path/to/pi-temporal   # this project only
```

Once it is installed, every turn of every session is durable. There is nothing to type and nothing
to launch: the first turn registers a turn executor and starts a worker in this process, so each
turn becomes a `piLocalTurn` workflow, and a turn a crash cut in half is finished when the session
is opened again. `PI_TEMPORAL_DURABLE_TURNS=0` turns it off, and `PI_TEMPORAL_STEPPED=1` makes each
tool call of a turn an activity of its own.

Be clear about what that is and is not. The turn runs in your pi process, against the live session,
so the transcript and the streaming are pi's own. It does not run somewhere else, and it cannot:
the session it belongs to is in memory here. So the workflow is a record of the turn and a retry
policy around it, and the recovery is "the next pi to open this session finishes the turn", not
"the turn carries on without you".

A crash test shows the part that matters: kill pi during a tool call, reopen with `pi -c`, and the
unanswered call is settled as "the outcome of this tool call is unknown", the turn runs on, and the
model checks the state rather than blindly running the command again.

If Temporal cannot be reached, the turn runs the way pi would have run it, and the session says so
once. Durability is not worth losing a turn over.

## Sending a task away: /background

Durability is not the same as offloading, so that has its own command:

- `/background <task>` gives a task its own session that a worker owns, and returns straight away.
- `/background-status` shows what this session is waiting on.
- `/background-stop` interrupts it.

The difference from an ordinary turn is who owns the session. A background task belongs to the
worker from the start, so it carries on after pi exits, and the durable unit is one step rather
than one turn: a worker dying loses the step in flight and nothing before it. When the task
finishes, the answer arrives as context for your next prompt, so you can just ask about it.

Quitting pi stops the worker inside it, but not the task. The workflow keeps it, and the next
worker to poll the queue picks the step up, which can be the one your next pi starts.

The worker calls `step()`, so this needs pi to be the fork build. The Temporal side does not, so on
stock pi the commands still work against a worker running elsewhere: set
`PI_TEMPORAL_EMBEDDED_WORKER=0` and run `npm run worker` from a clone. Do the same when a fleet
worker owns the queue, or when you want tasks to keep moving with no pi open.

## Why the session you type in cannot be handed to a worker

An extension gets a read-only session manager and no handle on the running `AgentSession`, so it
cannot drive the local loop itself. `registerTurnExecutor` is the way in, and it hands the turn
over in this process. Moving the turn to a worker instead would mean a second `AgentSession`
writing the same session file, which is two writers on one JSONL. That is why `/background` gives
a task its own session rather than borrowing yours.

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

Then just type. Every turn is a workflow: `temporal workflow list --address 127.0.0.1:7233` shows one
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

Two parts of the state, two systems:

- **Durable storage** stays with Pi. Pi's `SessionManager` already persists the conversation (messages, tool results, the tree) to a JSONL session file. That file is the source of truth. We do not move it into Temporal.
- **Durable execution** comes from Temporal. A per-session workflow drives Pi's turns and survives a crash: the turn re-runs on another worker and continues from the session file.

This is the `storage` vs `execution` split from the AI-399 write-up, applied to a harness that (unlike OpenCode) has no swappable `SessionExecution` abstraction. So we drive Pi from the outside via its SDK rather than replacing an internal interface.

## Granularity: a step per activity

One `runStep` activity does one thing:

1. If the prompt is not in the transcript, `recordPrompt` puts it there. Nothing runs yet.
2. Otherwise `prepareStep` settles what an earlier attempt left behind. It returns false when the turn already has its answer, which is a retry landing after the last step finished.
3. `step()` runs one model call and the tools it asks for, and says whether the turn is done.

The workflow loops that until a step reports done, so the number of activities is the number of steps. Nothing in the activity reads the workflow's step number: the transcript decides what runs next, and the workflow only counts so a runaway turn hits a ceiling.

That makes a retry cheap and safe for a reason worth spelling out. A step that finished but never reported back is indistinguishable, on disk, from the step after it, so re-running it does exactly what the next step would have done anyway. No work is repeated. The one case that is not automatic is a crash between a tool starting and its result landing, and that is what `prepareStep` is for.

## A tool call per activity

`PI_TEMPORAL_STEPPED=1` splits the step into three:

```
runModelCall  ->  runToolCall (one per call)  ->  sealStep
```

Off by default. It applies to both halves: a `/background` task on a worker, and every turn of the session you are typing in.

The calls of a step overlap on the worker half, where each activity opens a session of its own. They do not on the live half: those calls all reach the one agent that pi process holds, and it admits a single unit of work at a time, so a second call arriving while the first runs would be refused and reported as an unknown outcome for a tool that never ran.

The point is what can now sit between the model asking for a tool and the tool running. A per-tool retry policy, a per-tool timeout, an approval, a budget: under the whole-step mode there was nowhere to put any of them, because one activity covered the model call and the whole batch. It also makes a turn legible: history shows the tools by name, and a tool that hangs no longer holds the model call under the same timeout.

Two things are load-bearing and were easy to get wrong.

**The seal is the only writer of the step's results.** Not of the transcript: the model call writes the assistant message. Pi's session file is a tree, and every entry takes its parent from the leaf the writer last saw. Two calls settling at once would each parent off the leaf they saw and branch the transcript, and Pi's own parallel path appends results in call order after the batch, which per-call activities would lose. So a call reports its result and the seal records the step's results together, in the order the model asked. The transcript ends up the one Pi would have written, which its own tests pin.

Two attempts of the same activity can still both be alive, so every activity that writes takes a lock beside the session file first. A holder that stops refreshing it is reclaimed on age, which keeps one dead worker from taking the session with it. That includes the tool calls: they do not write the transcript, but they move the project's files, and the tree's own lock is host-local while two hosts publishing at once is the case that has to be excluded.

**A call that already started is not silently repeated.** A dispatch writes a note beside the session file before the tool can have any effect, and keeps the result there when it comes back. A second dispatch that finds a result returns it; one that finds only the note reports the outcome as unknown rather than running a `git push` that may already have landed. The attempt number would answer the same question far less precisely: it counts every way a dispatch can die, including the ones that never reached the tool.

The kept results live in `<session>.jsonl.pending/<turn>/<step>/`, scoped by both because a call id is only unique within the message that asked for it and a turn numbers its steps from one again. The next step's model call drops the results of the steps before it. The seal deliberately does not drop its own: a seal whose answer never reached Temporal runs again, and a batch it reads as empty is a batch it reads as wanting another step, even when a tool asked the turn to stop.

The notes are not dropped with the results, and that is the whole reason for the turn in the path. The attempt a note guards against is one that stalled: it comes back after the seal wrote the answer and after the cleanup that followed, and nothing else on disk can then tell its call from one nothing has run yet. An empty file per call is what keeping it costs. Under a scope of step alone the next turn's step 1 would read the last turn's step 1 as its own, and report a tool that never ran as already dispatched.

Each tool call gets 30 minutes per attempt by default. A call that crosses it is not run again, since its note says it started, so it ends as an unknown outcome while the tool may still be running. A deployment with longer tools sets `PI_TEMPORAL_TOOL_TIMEOUT_MINUTES` on the client that starts the session.

They stay out of Temporal's history on purpose: tool output is capped at 50KB by Pi, but a step's worth of it per activity result, per step, for the life of a session, is a history nobody wants to read.

What it costs: each activity opens the session file and builds an `AgentSession` of its own, so a step with four calls pays six session opens instead of one. Against a model call that takes seconds, the boundaries measure in milliseconds (see below), but the cost is real and it grows with the transcript.

## What is durable, and what is not

- **Between turns: clean.** A worker dying between turns loses nothing. The next prompt drives on any worker from the session file. A fresh worker that never saw the session serves it correctly.
- **Between steps: clean.** Each step is its own activity, so a worker dying loses at most the step in flight. The steps before it are on disk and are not re-run.
- **Mid-step: the tool is reported as unknown, not re-run.** A crash between a tool starting and its result landing leaves a tool call with no result. `prepareStep` settles it with "the outcome of this tool call is unknown", and the model decides whether to try again. Blindly re-running it is the wrong default for a coding agent: the `git push` may already have happened.
- **A step is not atomic.** Pi runs the tools of one step as a batch, so a crash part way through that batch leaves some tools run and some not. Under the whole-step mode none of the batch is in the transcript until the step ends, so a crash costs the work of every tool that had finished. The stepped mode is where the finished ones keep their results.
- **An interrupt keeps what finished.** The step is closed on the way out, so a call that returned before the stop keeps its result. Only the one that was still running reads as an unknown outcome.
- **Stepped mode keeps what a call produced.** A crash between a tool finishing and its result reaching Temporal loses the work under the whole-step mode: nothing recorded it. With a tool call per activity the result is kept beside the session file the moment the tool returns, so the retry finds it and the tool is not asked again. What is still lost is a tool that was inside its own execution when the process died, which is what an unknown outcome is for.

- **A long session rolls over.** History grows for the life of a run, and a run that outgrows it is
  terminated by the server, mid-turn. The workflow continues as new when nothing is in flight,
  carrying its queue, which is the whole of the control state. `continueAsNewSuggested` is what
  drives it in production; `maxHistory` in the workflow options is a tighter bound for an operator
  who wants one, and is what makes the rollover reachable in a check.
- **The write at the end of a model call is guarded too.** The lock check used to sit before the
  call, and the assistant message is written at the end of a stream that runs for minutes. The
  session asks on its way to every append now, through the fork's `setWriteGuard`. It answers from
  the last refresh the lock confirmed, because Pi's append path is synchronous and cannot await a
  read of the lock file, so the window is a refresh interval rather than a whole model call. It
  compares timestamps rather than reading a flag a timer sets: a process whose event loop stopped
  runs no timers, and that stall is the one case the guard exists for. It also gives up ten seconds
  before the age a contender reclaims at, because those two are measured on two hosts' clocks.

### What recovers, and what a person has to answer for

A fifth review asked for this as a table rather than as prose, and it is the right ask: "durable"
is not a property, it is a list of failures with an answer beside each one. Live-process mode is
the turn running inside the `pi` you typed into; worker mode is a session the deployment owns.
Every row names the check that fails without its answer.

| What fails | Live-process mode | Worker mode | Pinned by |
|---|---|---|---|
| A prompt is accepted and nothing wakes to run it | the turn is the process that took it, so there is nothing to wake | the prompt is workflow state, so it waits with no worker up, and a schedule firing carries the task in the workflow's own input | `workflow-init-check.mts` |
| The process running the turn dies between steps | the steps already sealed are on disk; the turn ends with the process | the next attempt reads the transcript and continues, on any worker, including one that never saw this session | `detached-check.mts`, `step-loop-check.mts` |
| It dies with a tool in flight | the call is settled as an unknown outcome and the model decides | same, and a tool that had finished keeps its result rather than being asked again | `pending-check.mts`, `detached-check.mts` |
| Two attempts of one writing activity are alive at once | one at a time through the lock beside the session file; a holder that was superseded is refused at the write rather than after it | same | `session-lock-check.mts`, `lock-gap-check.mts`, `stall-check.mts` |
| An attempt stalls past its own timeout and comes back after the answer is recorded | its dispatch claim fails, so it reports an unknown outcome instead of running the tool again | same | `stale-dispatch-check.mts` |
| The user stops the turn | the step is closed on the way out, so the calls that returned keep their results | same, and the project is not moved on the way out | `interrupted-seal-check.mts`, `l2-step-check.mts` |
| A host publishes the project tree while it is behind | not reachable: one process, one directory | refused, and its own unshipped work is set aside rather than lost | `worktree-check.mts`, `storage-repair-check.mts` |
| A host is left holding a directory for a session that is over | not reachable | the session records that it is over where every host reads it, and each hands its own directory back the next time one is wanted | `storage-repair-check.mts` |
| The session's history outgrows its run | not reachable: one turn, one run | continue-as-new when nothing is in flight, carrying the queue | `rollover-check.mts` |
| The worker a step was pinned to is gone | not reachable | what is left of the step runs on the shared queue with nothing run twice, once every pinned attempt has settled; a tree the stranded host publishes afterwards is refused and set aside | `l2-step-check.mts`, `worktree-check.mts`, `detached-check.mts` |

What none of this recovers, and no version of it can: a tool that was inside its own execution when
the process died. Nothing on disk says whether the `git push` landed. The model is told the outcome
is unknown and decides, which is the only honest answer a wrapper can give.

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

There is no server in this picture, because a worker-owned session has none. The workflow holds the
control state and answers `turnState`; the session file holds the conversation. So following a
session is a query plus a tail of its file, and both work from any machine that can reach the
cluster and `PI_SESSION_DIR`. Point that directory at shared storage and the machine that starts a
task, the machine that runs it, and the machine that watches it need not be the same one.

Three things worth knowing about the shape:

- **A closed run is a finished session, not a live one.** A closed workflow answers a query by
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

`detached-check.mts` runs the claim against real processes: a client hands over a task and exits,
worker A starts the turn, A is killed with the tool still in flight, and worker B, which never saw
this session, finishes it. `running` lists the session and `watch` follows it across the handover
from a process that is only ever a client. The tool that was cut off is reported to the model as an
unknown outcome rather than re-run, which is the rule this repo already holds everywhere else.

Two things that check gets right only because getting them wrong was silent. It kills on observing
a tool in flight rather than after a fixed delay, because a slow command in between pushes the kill
past the end of the turn and then no handover happens at all. And it runs the worker and the CLI as
single processes (`node --import tsx`), because `npx` spawns `tsx` spawns node, so killing the
process you hold leaves the one that matters running.

### Across two machines

On one host "another worker" is another process reading the same disk, which proves less than it
looks like. `docker/cross-host-check.sh` puts each worker in its own container: its own filesystem,
its own hostname, and no way to reach the other except through Temporal and the shared session
directory. The evidence is Temporal's own, because the worker identity is the container's hostname:

```
06:15:49  attempt 1  1@89cc9c4fa607     <- worker A, killed mid-tool
06:16:30  attempt 2  1@252525771cd2     <- worker B, which had never seen this session
```

`/sessions` is a local volume, so this shows separate hosts rather than a separate filesystem
implementation. The `O_EXCL` caveats in `session-lock.ts` still want a real network filesystem.

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
- **It never writes over somebody's checkout.** Before a reset it compares what is on disk with
  what it last agreed the directory held. An empty directory holds nothing, and treating that as a
  working copy is how the tree ends up never travelling.
- **Work a crash left behind is set aside, not dropped and not published.** A host that wrote and
  died before shipping holds files nothing else has, and the session has moved on without them.
  They go to `<session>.jsonl.tree/salvage/` as a self-contained bundle, and the host comes to the
  tip. Recover one with `git bundle unbundle`. Nothing prunes them.
- **Only a client may establish the project.** Nothing running on a worker can: every activity, the
  model call included, lands on whichever worker Temporal had free, so an activity that adopts its
  own directory puts the project wherever the first unit of work happened to go. `start --project=`
  sends it (the flag is required with the tree on, so nothing ships a home directory by accident),
  and `/background` sends the directory you asked from. A session with nothing established refuses
  every activity until a client sends it, which is loud rather than wrong.
- **A schedule cannot carry the project yet.** Each firing is its own session and nothing is running
  at firing time to send one, so `schedule` refuses with the tree on rather than creating sessions
  that fail on every activity.
- **A worker hands back what it held for finished sessions when it starts**, and lazily after that: a directory is freed when another session asks for that same one. Between the two, a worker that served fifty sessions is not sitting on fifty directories.
- **The chain restarts rather than growing for ever.** Every fortieth capture carries the whole tree
  and stands on nothing, and the bundles before it are removed once the tip names it. A session that
  runs for hours would otherwise keep every state it has ever been in, and a host joining late would
  unbundle all of them to catch up. Nothing under `salvage/` is touched.
- **A bundle nothing names is dropped, not obeyed.** A writer that died between renaming its bundle
  into place and naming it as the tip leaves one behind, and every host afterwards computes that
  same number. Refusing it wedged the session everywhere rather than on the host that crashed.
- **One session per directory.** A second is refused while the first is using it, in both
  directions. A session hands its directory back when it goes idle, and only then if this host built
  that directory out of an empty one, everything in it has shipped, and it actually comes out empty.
  A directory the host already had is somebody's working copy: what it holds includes the files git
  ignores, which no bundle carries and nothing else has a copy of, so that one keeps its files and
  only the note goes. The retirement runs on one host, and it says the session is over in the shared
  directory as well: every other host reads that the next time a session wants its directory, and
  hands its own back then. Without somewhere shared to ask, the note is the only answer and nothing
  can correct it, so a directory served one session and refused every later one.
- **A refused restore stops the step.** Running against files that are not the project tells the
  model those files are the project, which is worse than not running, so it fails and Temporal puts
  the work on a host that can do it. A refused *capture* is different: the tool has already run and
  a retry would find its result rather than run it again, so throwing there costs an attempt and
  still ships nothing. It sets the work aside instead, and says so.
- **A step stays on the worker that ran its model call.** Every worker polls a second queue of its
  own, keyed by host and project directory, and the model call reports it; the tools and the seal
  are addressed there. That worker is standing in the directory the tools are about to write, so
  they see each other through the filesystem and the tree never moves between them, which is what
  lets them run together while it travels. A pinned dispatch carries a 30 second
  `scheduleToStartTimeout` and one attempt. Unstarted calls wait for all pinned siblings before
  moving to the shared queue one at a time. A pinned attempt that fails moves too, once
  every pinned attempt of the step is over. What makes that safe is not a judgement about the host,
  which the workflow cannot see: the dispatch note has the retry report the call as unknown rather
  than run it again, and the tree store refuses a publish from a host that is not standing on the
  tip, setting its work aside instead. Refusing to move was the other answer, and it ended the turn
  on a worker dying mid-tool, which strands exactly the same work and loses the rest of the step as
  well. The one failure that does not move is the turn being stopped, which is not a failure.
  An interrupted step still records completed tool results. Its seal only writes the transcript,
  with project restore, project capture, and post-run work disabled.
- **It is off by default.** On a laptop the tools already run in the directory you meant, and
  shipping it there is disk spent on a problem that host does not have.

What it does not carry is what git would not: anything the project ignores. So a rebuilt tree may
want an install step, the same as a fresh clone would.

### Verified

`worktree-check.mts` covers the mechanics with two fake hosts and needs neither a server nor a key:
a file and a nested file arrive, a deletion arrives, an unchanged capture ships nothing, a
directory holding work nothing shipped is left alone, a second session cannot take one that is
already in use in either direction, a tool call cannot establish the project, a host behind the tip
comes to it with its own work kept rather than published, a directory is handed back only once
everything in it has shipped, a restore does not turn a directory this host adopted into one it may
empty, and a host that did not run the retirement can still serve the next session.

The assertion worth naming is "nothing the other host shipped is reverted". The check used to set
up exactly the interleaving that loses data, read the one file that survived it, and stay green
while the rest reverted one line away.

`docker/tree-check.sh` runs it for real: worker A writes a file, worker A's container is killed,
and worker B, whose `/project` has never held anything, continues the same session and reads both
that file and the rest of the project back. With `PI_TEMPORAL_SHIP_TREE=0` exactly the three tree
assertions fail.

`NFS=1 docker/tree-check.sh` runs the same ten assertions with `/sessions` on a real NFSv4 server
rather than a local volume. That is the part the lock rests on: an exclusive create has to be
exclusive and a rename has to be atomic, and a local volume answers both by construction, which is
no answer at all for the filesystem a fleet actually shares. The mount is the daemon's, so no worker
needs privileges of its own. NFSv3 still answers neither, and nothing here pretends to test it.

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
What a fleet cannot be talked out of is the two that make it a fleet: a session directory only one
machine can see is a worker that never picks anything up, and files that do not travel are a model
being told an empty directory is the project. `preflight` refuses both, and a worker that fails it
exits rather than accepting work it cannot do.

Reaching a server that is not the dev server:

```bash
TEMPORAL_ADDRESS=your-ns.a1b2c.tmprl.cloud:7233 TEMPORAL_NAMESPACE=your-ns.a1b2c \
  PI_TEMPORAL_API_KEY_FILE=/run/secrets/temporal-key      # Temporal Cloud
TEMPORAL_ADDRESS=temporal.internal:7233 \
  PI_TEMPORAL_TLS_CERT=/run/secrets/tls.crt PI_TEMPORAL_TLS_KEY=/run/secrets/tls.key \
  PI_TEMPORAL_TLS_CA=/run/secrets/ca.crt                  # a cluster with mTLS
```

The key is read from a file rather than passed in argv, and nothing prints it. Both halves build the
connection from the same function, so a client and a worker cannot disagree about how to reach the
cluster or which namespace they are in.

Ask before deploying rather than after:

```bash
npx tsx src/cli.ts doctor
```

It prints what this process resolved and names what is wrong with it, including the mistakes that
read as something else later: an API key against a dev server, a Cloud key with the `default`
namespace, an address that is not loopback with no credentials at all, half a certificate pair.

## A turn nobody started

`start` hands a task over and returns, but something still has to run it. A schedule does not:

```bash
npx tsx src/cli.ts schedule "review yesterday's merges" --cron="0 9 * * *" --id=morning
npx tsx src/cli.ts unschedule morning
```

With the tree on it also needs the project, which nothing is running at firing time to send:

```bash
npx tsx src/cli.ts schedule "review yesterday.s merges" --cron="0 9 * * *" --id=morning \
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

Helpers are at the repo root, none of which needs a model key:

- `submit.mts` submits one prompt, `inspect.mts` summarizes a session file.
- `step-loop-check.mts` runs the executor against a Temporal server with the activities stubbed, in both modes: one step at a time and in order, an interrupt that ends the turn and not the session.
- `local-turn-check.mts` does the same for a turn of a live session, with the turn itself faked: handed over once in whole-turn mode, and a model call, its calls and a seal per step in stepped mode. It also holds the two rules that half depends on: the calls of a step do not overlap there, and an interrupt stops the loop instead of buying another model call.
- `l2-step-check.mts` needs no server either. It drives the stepped step body against fake activities: calls overlap unless the batch says otherwise, a failed tool still lets the step close, and an interrupt is not swallowed.
- `session-lock-check.mts` covers the one-writer-at-a-time lock: two writers do not overlap, a dead holder's lock is reclaimed on age, a live holder's is not stolen, and a holder can tell it has lost the lock both ways it needs to ask (awaited, and synchronously from the refresher's last tick, which is what the session's own append path uses). It also covers the late reclaim itself: a contender stalled between measuring a stale lock's age and reclaiming it, while another reclaims it and takes it. The stall is injected, through `withSessionLock`'s last argument, because nothing outside the module can hold a contender at that point. What is asserted is the outcome rather than the stall: the slow one must not move a lock the quick one is holding and then take it. Reverting the content check makes it fail.
- `detached-check.mts` needs a server and a key. It is the only one that does, because what it
  proves is a session surviving the process holding it, which does not show up inside one process.
- `worktree-check.mts` needs neither a server nor a key. It covers moving the project between hosts,
  with the hosts faked as separate data directories over one shared session directory.
- `stall-check.mts` needs neither a server nor a key, and takes about 70 seconds: it stops a holder's event loop past the lock's stale window and asks what the holder believes when it comes back.
- `rollover-check.mts` needs a server, no key. It drives a session past a small `maxHistory` and holds the two things a rollover must not break: the run really does change, and every prompt it accepted is still answered afterwards.
- `pending-check.mts` needs neither a server nor a key. It covers the files a step keeps about its calls, which is what "a call that already started is not silently repeated" rests on: a fresh call looks fresh, scratch never reads as a result, and a sweep drops what the transcript answers and keeps what it does not.

To see what a session is doing without reading its file, ask the workflow: `temporal workflow query --workflow-id pi-session-<id> --name turnState` reports the queue, the step in flight, and how the last turn ended.

The crash test: start a worker; submit a turn that appends to a file with one bash call and sleeps in the next, one at a time; poll the session file until `toolCalls > toolResults` (a tool call in flight); `pkill -9 -f "pi-temporal.*src/worker.ts"`; wait past the 30s heartbeat timeout; start a fresh worker. `temporal workflow show` will have the earlier step completed on attempt 1 and the interrupted one on attempt 2, and the appended file will have one line, not two.

## Layout

- `extensions/temporal.ts` — the pi extension: the turn executor, and `/background`.
- `src/config.ts` — Temporal + Pi wiring from env.
- `src/protocol.ts` — workflow ids, signal/query names, shared types.
- `src/local-turn-workflow.ts` — `piLocalTurn`: one workflow per turn of a live session.
- `src/local-turn-activity.ts` — works on the turn the pi process is holding: the whole turn, or its parts.
- `src/activities.ts` — `runStep`, and the `runModelCall` / `runToolCall` / `sealStep` that split it, via `@earendil-works/pi-coding-agent`, session file as the log.
- `src/l2-step.ts` — the stepped step body: how a step's calls are fanned out and how an interrupt reaches them.
- `src/pending.ts` — what a step knows about its calls before it is sealed, beside the session file.
- `src/workflow.ts` — `piSession`: per-session durable executor (submit prompt, step to the end of the turn, interrupt, idle-terminate).
- `src/session-worker.ts` — builds the worker; used by the standalone process and by the extension.
- `src/worker.ts` — the standalone worker process.
- `src/client.ts` — helpers to submit a prompt / interrupt a session.
- `src/demo.ts` — end-to-end smoke once a model key is set.

## Status

- [x] Design + scaffold against Pi's real SDK (`createAgentSession`, `SessionManager`, `ModelRuntime`), typechecks.
- [x] Live happy-path turn (OpenAI via `ModelRuntime.getAvailable` + `setRuntimeApiKey`).
- [x] Crash test: turn re-executes on a fresh worker (activity attempt 2), no duplicate prompt.
- [x] Found the turn-level limit: mid-turn crash cannot resume cleanly on the stock SDK.
- [x] Added the fix on the Pi fork (`resumeInterruptedTurn`, `step`); proven by the fork's mock-model tests.
- [x] Wired `runPrompt` to call `resumeInterruptedTurn()` on retry instead of re-prompting.
- [x] Live re-verification: mid-turn crash recovers via `resumeInterruptedTurn()`, no duplicate prompt, tool balance intact.
- [x] Pinned the dependency to [temporalio/pi#2](https://github.com/temporalio/pi/pull/2) by commit, so CI builds it too.
- [x] Added `recordPrompt` and `prepareStep` to the fork, so a driver can step without ever running a whole turn.
- [x] A step per activity, checked against Temporal with a stubbed activity (`step-loop-check.mts`).
- [x] Live crash test on the stepped executor: step 1 not re-run, step 2 on attempt 2, tool balance intact, no duplicate prompt.
- [x] Packaged as a pi package: `pi install` registers the commands, and a task submitted from the TUI came back as context for the next prompt.
- [x] A worker inside pi, so a task runs with nothing else launched (verified with no worker process anywhere).
- [x] Every turn durable by default via `registerTurnExecutor`, with a crash mid tool call finished on reopen.
- [x] A tool call per activity, behind `PI_TEMPORAL_STEPPED=1`, on both halves.
- [x] The project's files travel with the session, behind `PI_TEMPORAL_SHIP_TREE=1`.
- [x] An independent review of the whole stack, and its findings closed: who may move the tree's tip, how a project enters the system, a fence on the tree store that crosses hosts, a directory that is handed back, history that is bounded, and the write at the end of a model call.

Still open, and named rather than buried:

- Nothing removes a finished session's `<session>.jsonl.tree/` on its own, because a session that went idle can be prompted again and those bundles are what its next turn restores from. `pi-temporal forget <sessionId>` does it for a session that is over, and refuses one that is still running or that nobody could answer for. Anything under `salvage/` stays either way: nothing else has a copy of it.
- The tree lock is host-local; what excludes two hosts is the session lock the activities take around their tree writes.
- The checks over NFSv4 answer the exclusive-create and atomic-rename questions the lock rests on. NFSv3 does not answer them, and nothing here tests it: `session-lock.ts` says so and means it.

Live, on `gpt-4o-mini`, with the stepped mode on:

- A worker-owned turn that read two files cost six activities: `runModelCall`, two `runToolCall`, `sealStep`, then `runModelCall` and `sealStep`. The two tool calls were scheduled 17 microseconds apart and ran in the same 31ms window. The seal took 8ms; the model calls took 3.1s and 2.6s, which is where a turn's time actually goes.
- Crash mid tool call: a bash call that appended one line and then slept for 60 seconds, with every worker killed while it slept. On a fresh worker the call came back as attempt 2, found its dispatch note, and reported the outcome as unknown rather than running again. The model then ran `cat counter.txt` itself, saw the one line, and answered. The file had one line, not two.
- Interrupt: with a `sleep 90` in flight, the signal and `ActivityTaskCancelRequested` landed in the same second, the turn was recorded as interrupted, and the workflow stayed running. The next prompt settled the unanswered call before it was recorded, so the session kept serving instead of failing on every later turn.
- A turn of the session you type in cost five activities: `runLocalModelCall`, `runLocalToolCall`, `runLocalSeal`, then `runLocalModelCall` and `runLocalSeal`. The transcript is the one pi writes without an executor.
- Crash mid tool call on that half too: pi killed while a tool slept, reopened with `-c`. The unanswered call was settled as an unknown outcome, the model checked the file rather than re-running the command, the interrupted turn finished, and the new prompt was answered.

## Prior art

[osolmaz/pi-workflows](https://github.com/osolmaz/pi-workflows) makes user-defined workflow graphs durable in Pi with a home-grown file-queue engine. This project is a different target: the agent's own turn loop, durable via Temporal. We reuse its integration ideas (headless Pi driven from an outer host), not its graph feature.
