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

**The seal is the step's only writer.** Pi's session file is a tree, and every entry takes its parent from the leaf the writer last saw. Two calls settling at once would each parent off the leaf they saw and branch the transcript, and Pi's own parallel path appends results in call order after the batch, which per-call activities would lose. So a call reports its result and the seal records the step's results together, in the order the model asked. The transcript ends up the one Pi would have written, which its own tests pin.

Two attempts of the same activity can still both be alive, so every activity that writes takes a lock beside the session file first. A holder that stops refreshing it is reclaimed on age, which keeps one dead worker from taking the session with it. The tool calls take no lock, because they do not write.

**A call that already started is not silently repeated.** A dispatch writes a note beside the session file before the tool can have any effect, and keeps the result there when it comes back. A second dispatch that finds a result returns it; one that finds only the note reports the outcome as unknown rather than running a `git push` that may already have landed. The attempt number would answer the same question far less precisely: it counts every way a dispatch can die, including the ones that never reached the tool.

The kept results live in `<session>.jsonl.pending/<step>/`, scoped by step because a call id is only unique within the message that asked for it, and the next step's model call drops the step before it. The seal deliberately does not drop its own: a seal whose answer never reached Temporal runs again, and a batch it reads as empty is a batch it reads as wanting another step, even when a tool asked the turn to stop.

They stay out of Temporal's history on purpose: tool output is capped at 50KB by Pi, but a step's worth of it per activity result, per step, for the life of a session, is a history nobody wants to read.

What it costs: each activity opens the session file and builds an `AgentSession` of its own, so a step with four calls pays six session opens instead of one. Against a model call that takes seconds, the boundaries measure in milliseconds (see below), but the cost is real and it grows with the transcript.

## What is durable, and what is not

- **Between turns: clean.** A worker dying between turns loses nothing. The next prompt drives on any worker from the session file. A fresh worker that never saw the session serves it correctly.
- **Between steps: clean.** Each step is its own activity, so a worker dying loses at most the step in flight. The steps before it are on disk and are not re-run.
- **Mid-step: the tool is reported as unknown, not re-run.** A crash between a tool starting and its result landing leaves a tool call with no result. `prepareStep` settles it with "the outcome of this tool call is unknown", and the model decides whether to try again. Blindly re-running it is the wrong default for a coding agent: the `git push` may already have happened.
- **A step is not atomic.** Pi runs the tools of one step as a batch, so a crash part way through that batch leaves some tools run and some not. Under the whole-step mode none of the batch is in the transcript until the step ends, so a crash costs the work of every tool that had finished. The stepped mode is where the finished ones keep their results.
- **An interrupt keeps what finished.** The step is closed on the way out, so a call that returned before the stop keeps its result. Only the one that was still running reads as an unknown outcome.
- **Stepped mode keeps what a call produced.** A crash between a tool finishing and its result reaching Temporal loses the work under the whole-step mode: nothing recorded it. With a tool call per activity the result is kept beside the session file the moment the tool returns, so the retry finds it and the tool is not asked again. What is still lost is a tool that was inside its own execution when the process died, which is what an unknown outcome is for.

## A session that outlives its client

`/background` sends a task to a worker, but its commands live inside a pi session, so a task could
only be started, listed and followed from the terminal that started it. Close that terminal and the
task keeps going with nobody able to see it. `src/cli.ts` is the other half:

```bash
# hand a task over and walk away; prints the session id and exits
npx tsx src/cli.ts start "port the auth module to the new API"

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

## Reproducing

Helpers are at the repo root, none of which needs a model key:

- `submit.mts` submits one prompt, `inspect.mts` summarizes a session file.
- `step-loop-check.mts` runs the executor against a Temporal server with the activities stubbed, in both modes: one step at a time and in order, an interrupt that ends the turn and not the session.
- `local-turn-check.mts` does the same for a turn of a live session, with the turn itself faked: handed over once in whole-turn mode, and a model call, its calls and a seal per step in stepped mode. It also holds the two rules that half depends on: the calls of a step do not overlap there, and an interrupt stops the loop instead of buying another model call.
- `l2-step-check.mts` needs no server either. It drives the stepped step body against fake activities: calls overlap unless the batch says otherwise, a failed tool still lets the step close, and an interrupt is not swallowed.
- `session-lock-check.mts` covers the one-writer-at-a-time lock: two writers do not overlap, a dead holder's lock is reclaimed on age, and a live holder's is not stolen.
- `detached-check.mts` needs a server and a key. It is the only one that does, because what it
  proves is a session surviving the process holding it, which does not show up inside one process.
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

Live, on `gpt-4o-mini`, with the stepped mode on:

- A worker-owned turn that read two files cost six activities: `runModelCall`, two `runToolCall`, `sealStep`, then `runModelCall` and `sealStep`. The two tool calls were scheduled 17 microseconds apart and ran in the same 31ms window. The seal took 8ms; the model calls took 3.1s and 2.6s, which is where a turn's time actually goes.
- Crash mid tool call: a bash call that appended one line and then slept for 60 seconds, with every worker killed while it slept. On a fresh worker the call came back as attempt 2, found its dispatch note, and reported the outcome as unknown rather than running again. The model then ran `cat counter.txt` itself, saw the one line, and answered. The file had one line, not two.
- Interrupt: with a `sleep 90` in flight, the signal and `ActivityTaskCancelRequested` landed in the same second, the turn was recorded as interrupted, and the workflow stayed running. The next prompt settled the unanswered call before it was recorded, so the session kept serving instead of failing on every later turn.
- A turn of the session you type in cost five activities: `runLocalModelCall`, `runLocalToolCall`, `runLocalSeal`, then `runLocalModelCall` and `runLocalSeal`. The transcript is the one pi writes without an executor.
- Crash mid tool call on that half too: pi killed while a tool slept, reopened with `-c`. The unanswered call was settled as an unknown outcome, the model checked the file rather than re-running the command, the interrupted turn finished, and the new prompt was answered.

## Prior art

[osolmaz/pi-workflows](https://github.com/osolmaz/pi-workflows) makes user-defined workflow graphs durable in Pi with a home-grown file-queue engine. This project is a different target: the agent's own turn loop, durable via Temporal. We reuse its integration ideas (headless Pi driven from an outer host), not its graph feature.
