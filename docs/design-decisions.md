# Why it's built this way

The choices below keep the conversation in the agent's session file while Temporal drives the
turn loop. Each section explains the tradeoff and when another approach could fit.

## The conversation lives outside the Workflow

The Workflow tracks queued prompts and the current step. It also tracks spend against budgets.
The agent's session file keeps the conversation. Coding tasks can produce large tool output, so
keeping that output in history would use Temporal's payload and history limits.

History carries IDs and small results, plus prompt text and final answers. Each step's input
goes into history, so only the first step of a turn carries the prompt text. Prompts are capped at
64K characters, and answers in history at 16K. The full answer stays in the session file. These
payloads can still contain secrets. The codec described below can encrypt them.

The cost is that every Activity opens the session file again, and the file must be on storage
every Worker can reach. If your agent's conversation is small and short, keeping it in Workflow
state is simpler and needs no shared storage.

## One Workflow per session, and Continue-As-New between turns

The Workflow ID is derived from the session ID, so a second client can't start another loop for
the same session. The Workflow continues as new only between turns, when the queue holds all control
state. Continuing mid-turn would need the step's state carried over too. Before it continues, or
exits when idle, it waits for every Update handler to finish, so no client is left waiting.

## Prompts are Updates, with a Signal to fall back on

A prompt is an Update-with-start (`submit`). The validator turns away an empty or oversized prompt
before it reaches history, and the prompt id is the Update id, so a client that resends gets the
first answer and the prompt runs once. The Update returns how many prompts are ahead. The start
sets `workflowIdConflictPolicy: "USE_EXISTING"`, so a running session takes the prompt. It also
sets `workflowIdReusePolicy: "ALLOW_DUPLICATE"`, so a closed session, such as one after an idle
exit, starts a new run under the same id.

Update-with-start alone is the plain pattern. The Signal fallback in `sendPrompt`
(`src/core/client.ts`) is optional. It exists so a prompt still lands when no Worker is up. An
Update needs a Worker to accept it. If none does within 10 seconds, the client sends the prompt as
a Signal, which the server keeps until a Worker comes. A Signal can't answer, so the Workflow logs
a refused prompt, bad or already seen, with `log.warn` and drops it. `submit` reports a refusal to
the caller instead. `watch` waits on a `waitForQuiet` Update instead of polling a Query.

## A whole step by default, a unit per tool call as an option

`runStep` runs the model call and its tools in one Activity, then seals the results. It opens the
session once per step.

Stepped mode separates `runModelCall` from `sealStep` and dispatches each tool through
`runToolCall`. Each call gets its own timeout and retry policy. A lost Worker interrupts one
Activity rather than the whole step. The cost is another session open per Activity. A step with
four tool calls opens the session six times.

## A tool runs at most once

Before a tool can act, its Activity creates a dispatch claim with an exclusive create. A retry
that finds the claim reports an unknown outcome to the model instead of running a `git push` a
second time. The model can then check what happened. The claim prevents another dispatch of the
same call when Temporal retries its Activity.

A failure before the claim is safe to retry anywhere, and it's typed `FailedBeforeClaim`, so the
Workflow may move the call to another queue. A failure after the claim is `FailedAfterClaim` and
not retried, since every retry would only find the claim.

Dispatch claims exist only in stepped mode. A whole-step `runStep` runs its tools inside one
Activity and writes no claims. Its retry relies on the agent's `prepareStep`, which settles every
call the earlier attempt left open as an unknown outcome and never runs it again.

## A fence token decides who may write the session file

Temporal already knows which attempt of which Activity is current. The Workflow numbers every
Activity that writes the session file, as `<run start>.<Activity number>`, and the Activity adds
its attempt. A later run, Activity, or attempt sorts higher. An Activity creates its token beside
the file, and the agent's write guard refuses an append once a higher token is there. A retry
takes over without waiting for a lease to expire.

Storage doesn't enforce the fence. An attempt that stalls between its check and its append can
still land that append. [Pi journal recovery](guarantees.md#pi-journal-recovery) defines the
filesystem requirements and NFS limits. If your session lives in a store with compare-and-set,
such as object storage with `If-Match` or a database row version, use the token there and the
gap closes.

The tree store keeps a lease instead (`src/tree/lease.ts`), because clients and hosts write it
outside any Workflow, so there's no Workflow to number them.

## Host queues, with a fallback

Tools use the project directory on their host. The model call therefore reports its Worker's
host queue, so the tools and seal return to that directory. A host-queue tool call gets one
attempt and a 30-second schedule-to-start timeout. That timeout proves the call never started,
so the step may move it to the shared queue. Any other failure might mean the tool is still
running, so the step
records what it has and goes on, and the lost host can't publish later.

This is Temporal's worker-specific Task Queue pattern. You need it only when Activities depend on
a host's local state. So a Worker polls a host queue only with `PI_TEMPORAL_SHIP_TREE=1`. With one
shared project directory, any Worker can run any unit.

## Retries and timeouts per unit

| unit | timeout | attempts | why |
|---|---|---|---|
| whole step, `runStep` | 30 minutes | 10 | the model call and its tools in one Activity, so one bound covers both |
| model call | 10 minutes | 10 | a hung stream should end long before the step's cap. Most retries are refusals before the call, which cost nothing |
| tool call, shared queue | `PI_TEMPORAL_TOOL_TIMEOUT_MINUTES` | 20 | failures before the claim, such as a host refusing the project |
| tool call, host queue | same | 1 | a second attempt's queue timeout couldn't rule out the first still running |
| seal | 30 minutes | 10, or 3 on a host queue | fenced and safe to repeat |
| `runStep`, model call, tool call, seal | 120 minutes total, or a longer tool timeout | | a unit that keeps timing out can't hold the session for days |
| `runStep`, model call, tool call, seal | 30-second heartbeat | | a Worker that died is found in seconds. Heartbeats come from a timer, not from progress, so a hung call runs to its own timeout |
| live turn, `runLocalTurn` | 24 hours | 3 | a whole agent run. The live turn's Workflow timeout of 24 hours bounds it too |
| live model call | 10 minutes | 3 | as in Worker sessions, a hung stream ends long before the turn |
| live tool call, live seal | 1 hour | 3 | a tool can run long, and the seal can run a provider retry and a compaction |
| every live unit | 30-second heartbeat, 1-minute schedule-to-start | | nothing polls a dead `pi`'s queue, so the unit fails fast and the next `pi` to open the session resumes the turn |
| retire, adopt a template | 5 and 30 minutes total | 3 and 10 | housekeeping, which must not hold a run open |

The agent retries its provider on its own, inside the seal, and counts those retries in
`agentState`. So the Activity retries are for a lost Worker or storage, not for the model.

## A stop reaches the work

A stop cancels the turn's scope. Tool and model Activities use `WAIT_CANCELLATION_COMPLETED`, so
the Workflow waits for each to stop and report. The Activity passes its cancellation signal to the
tool and the model call, which end like a user stop in the agent. The seal gets it too, since its
retry or compaction is another model call. The recovery seal then records what each tool reported
instead of unknown outcomes. Once a stop is asked the server doesn't retry a seal, so a lost seal
attempt comes back as a timeout, and the recovery seal runs for that as well. Cancellation reaches
an Activity only with a heartbeat's answer, so Workers send heartbeats at least every 3 seconds.

## Only a requested cancel is a stop

A deploy must not end turns. The SDK cancels a running Activity for several reasons: the Workflow
asked, the Worker shuts down, the attempt timed out, an operator paused or reset it, or the server
no longer knows the attempt (`notFound`, as after the Workflow is terminated). Only the first is a
user stop, and `Context.cancellationDetails.cancelRequested` says which. A stop ends the call, and
the seal records what it did. A shutdown leaves the Activity running, and the SDK first waits
`shutdownGraceTime`. Any other cancel aborts the model call or tool too, and a whole step runs no
more tools, since a retry takes the step or nobody waits for it. That attempt records no stop: the
fence guard refuses every write once it's cancelled, and a tool call keeps no result. The guard
matters even past the fence token, since a reset rewinds the attempt number and a retry can take
the same token.

The standalone Worker sets `shutdownGraceTime` to 60 seconds by default, through
`PI_TEMPORAL_SHUTDOWN_GRACE_SECONDS`. Most model calls finish in that time, so a step isn't cut off
and paid for twice. `docker/compose.yml` sets `stop_grace_period: 90s`, past the grace, so Docker
doesn't kill the process first. `shutdown-check` shows each kind of cancel.

## A bug fails the Workflow Task, not the turn

`runTurn` catches only a `TemporalFailure`, such as an Activity failure or a cancellation, and
records it as a failed turn. Any other error, such as a `TypeError` in Workflow code, is a bug.
`runTurn` throws it on, so it fails the Workflow Task. Temporal retries the task until a Worker
with fixed code runs it, and the session waits instead of losing the turn. Caught, the bug would
stay in history as a failed turn. In your own Workflow, catch only the failures you mean to handle.

## Tuning Workers

A Worker runs at most `PI_TEMPORAL_MAX_ACTIVITIES` Activities at once on each queue it polls. The
default is 16, and a host queue's poller gets its own 16. The SDK default is 100. That's too many
here, since each tool is a process on the host and every model call uses one API key. The slot
count is the bound on tool concurrency per host, so set it to what one host can run.

Model calls and tools share one Task Queue, so `maxTaskQueueActivitiesPerSecond` would limit both.
A team that needs a rate limit on model calls can put them on their own Task Queue and set the
limit there.

Workers send heartbeats at least every 3 seconds (`maxHeartbeatThrottleInterval`). The SDK would
send them about every 24 seconds here, and a stop would reach a running tool that much later.

Some file reads on the Activity path are synchronous. The fence check lists the fence directory
before each append, and Pi reads the whole session file when it opens one. They block the event
loop, and heartbeats with it. On local disk that's a few milliseconds. On slow NFS with a large
session and many slots, they can add up past the 30-second heartbeat timeout. Keep slots low on
slow storage, or watch heartbeat timeouts in the metrics.

## Worker Versioning, and `patched()` for long sessions

Worker Versioning is opt-in. Set `PI_TEMPORAL_DEPLOYMENT` and `PI_TEMPORAL_BUILD_ID` together on
the standalone Worker. Each build is then a Worker Deployment Version, and the server sends each
Workflow Task to a version its run may use. Each Workflow names its own behavior.

- `piSession` is `AUTO_UPGRADE`. A session lives for many turns. Pinned, it would keep its first
  build's Workers alive for as long as it runs. The cost is that every change to `piSession` must
  replay older histories, or sit behind `patched()`.
- `piLocalTurn` is `PINNED`. A live turn is short and bound to one `pi` process, so it finishes
  on the build that started it. A change to it never meets a history it can't replay.

Use `PINNED` for a Workflow that ends in minutes or hours, and `AUTO_UPGRADE` for one that lives
for days. A Workflow names its behavior only on a Worker in a deployment, because the server
refuses a behavior from an unversioned Worker. The Workers inside `pi` don't read these
variables, so they stay unversioned.

To release a new build, follow these steps.

1. Start Workers with the new `PI_TEMPORAL_BUILD_ID`, next to the old ones.
2. Make the new build current. New runs, and `AUTO_UPGRADE` sessions at their next Workflow Task,
   go to it. Pinned runs stay on the old build.

   ```bash
   temporal worker deployment set-current-version --deployment-name pi --build-id v2
   ```

   The server refuses while a Task Queue the old build polled has no poller in the new one.
   Host queues are named per host, so new hosts leave the old names unpolled. Add
   `--ignore-missing-task-queues` once the old hosts are gone for good.

3. Drain the old build. Stop its Workers once
   `temporal worker deployment describe-version --deployment-name pi --build-id v1` reports it
   drained.

A session that moves to the new build replays its history on the new code. Say a change adds an
Activity call between turns. History from the old build has no such call there, so the replay
fails. Gate the change with `patched()`, which is false for history written before the change.

```ts
import { patched } from "@temporalio/workflow";

// In the session loop, between two turns.
if (patched("snapshot-between-turns")) {
  await snapshotSession({ sessionFile: file });
}
await runTurn(queue.shift()!);
```

The session Workflow has five gates of this kind in `src/core/workflow.ts`, as worked examples:
`budget-before-step`, `seal-waits-for-cancel`, `adopt-not-cancellable`, `refuse-bad-input` and
`refuse-bad-prompt-id`.
Each has a kept history recorded on the code before it.

Remove the gate in two more releases.

1. Once no open run started before the gated build, replace `patched(...)` with
   `deprecatePatch("snapshot-between-turns")` and call the Activity always. A session reaches a
   new run at every idle exit and every Continue-As-New, so old runs end on their own.
2. Once no open run started before the `deprecatePatch()` build, delete the `deprecatePatch()`
   line.

`checks/histories/` keeps a history of each kind of session run. `replay-kept-check` replays them
with no server, and CI runs it as its own `replay` job. A history is kept once recorded and never
recorded again, since each stands for runs that may still be open. A change that breaks one would
break those runs on upgrade. `record-histories.mts` adds new ones next to the old.
`versioning-check` runs a session on a versioned Worker and reads the behavior the server records.

## Observability

The Workflow puts its state in the memo when it changes, so `running` is one List call. The step
number goes into the current details, which cost no history. Each Activity has a summary, such as
`tool bash (call_1)`, and each session a static summary. `PI_TEMPORAL_SEARCH_ATTRIBUTE=1` also
keeps the state in a `PiSessionState` keyword search attribute, for server-side filters. It's off
by default because the namespace must register the attribute first. `PI_TEMPORAL_METRICS` turns
on the SDK's Prometheus metrics. Activities log through the Activity logger, so each line carries
its Workflow and Activity ids.

`PI_TEMPORAL_TRACING=1` adds OpenTelemetry traces through the SDK's interceptors
(`src/core/tracing.ts`). The client starts a trace and passes it on in the call's headers. The
Workflow's interceptors run in the sandbox and hand finished spans to the Worker through a sink,
and the Activity interceptors continue the trace. So one trace shows a prompt's turn, step by
step, across Workers. The prebuilt bundle always has the Workflow interceptors, which makes it
about twice the size. The same image then traces or not by the variable alone. Without tracing,
the sink drops their spans. `tracing-check` shows one trace from the client to the Activity.

## Workflow input is untrusted

Anyone who can start a Workflow in the namespace picks its input, and the client in this repo is
only one way to do it. The session file in that input names where a Worker writes the session,
its dispatch claims and its fence tokens. So the core Activities require a `sessionRoot` and
refuse, without a retry, any session file or template that isn't a `*.jsonl` file directly in it.
The parent is compared by real path, so a link under the root can't lead out, and a root reached
through a link (macOS's `/var`, an NFS mount) still takes its files. A file nested deeper could sit
in another session's claim or fence directory, so it's refused too. The Worker makes the root at
setup if it's missing. Pi's Workers pass their session directory. `session-root-check` shows the
refusal.

## Payloads can be encrypted

Prompt and answer payloads pass through history, along with error text. Coding tasks can put
code or secrets in those payloads. `PI_TEMPORAL_CODEC_KEY` turns on an AES-GCM payload codec
in every client and Worker, and the server then stores only ciphertext. A failure's message and
stack trace are plain fields by default, and tool and provider errors can quote the task. So the
codec's failure converter moves them into a payload too (`encodeCommonAttributes`). The prebuilt
bundle doesn't carry it, so failures the Workflow raises itself, such as a refused prompt, stay
plain. They hold no task text. The bundle is built once for every Worker, with a key or without,
and the converter encodes whether or not a key is set. Built into the bundle, it would hide
every Workflow failure behind `Encoded failure` in the UI, even where no key is set. The UI
needs a codec server with the same key to show any of this. Search attributes, such as
`PiSessionState`, are never encrypted.

The codec still reads a plain payload, so history written before the key was set stays readable.
That also means it doesn't guard against someone who can write history directly. It protects
what the server stores, not the server's write path.

Each payload names the key that sealed it, by a hash of the key. To rotate, set the new key as
`PI_TEMPORAL_CODEC_KEY` and move the old one to `PI_TEMPORAL_CODEC_OLD_KEYS` on every client and
Worker. New payloads use the new key, and old ones still open. `codec-check` covers a rotation.

Keep an old key while any history that holds its payloads can still be read. Retention starts only
when a run closes, so the clock starts at the last close, not at the rotation. A session's runs
close at each idle exit and each Continue-As-New. Drop the key once every run that started before
the rotation has closed and the namespace's retention has passed since the last of them closed.
Archived histories need the key for as long as you keep the archive.
