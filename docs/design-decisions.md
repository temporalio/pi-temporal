# Why it's built this way

Each section names a Temporal choice, why this repo made it, and when you'd choose differently.
The code comments next to each choice say the same in fewer words.

## The conversation lives outside the Workflow

The Workflow keeps the prompt queue, the current step, spend, and budgets. The agent's session
file keeps the conversation. A coding agent's tool output is large and a session can run for
hours, so a conversation in Workflow state or history would hit Temporal's payload and history
limits within a busy turn. History carries ids and small results instead.

The cost is that every Activity opens the session file again, and the file must be on storage
every Worker can reach. If your agent's conversation is small and short, keeping it in Workflow
state is simpler and needs no shared storage.

## One Workflow per session, and Continue-As-New between turns

The session id is the Workflow ID, so a second client can't start a second loop for the same
session. The Workflow continues as new only between turns, when the queue is the whole control
state. Continuing mid-turn would need the step's state carried over too. Before it continues, or
exits when idle, it waits for every Update handler to finish, so no client is left waiting.

## Prompts are Updates, with a Signal to fall back on

A prompt is an Update-with-start (`submit`). The validator turns away an empty or oversized prompt
before it reaches history, and the prompt id is the Update id, so a client that resends gets the
first answer and the prompt runs once. The Update returns how many prompts are ahead.

An Update needs a Worker to accept it. If none does within 10 seconds, the client sends the
prompt as a Signal, which the server keeps until a Worker comes. The Workflow drops a Signal for a
prompt it already has. `watch` waits on a `waitForQuiet` Update instead of polling a Query.

## A whole step by default, a unit per tool call as an option

`runStep` runs one model call, its tools, and the seal in one Activity. It's the simplest shape
and opens the session once per step. Stepped mode (`runModelCall`, `runToolCall` each,
`sealStep`) gives each tool call its own timeout, retry, and Worker, and a lost Worker loses one
tool call instead of a step. It opens the session once per unit, so a step with four tool calls
opens it six times.

## A tool runs at most once

Before a tool can act, its Activity creates a dispatch claim with an exclusive create. A retry
that finds the claim reports an unknown outcome to the model instead of running a `git push` a
second time. The model can then check what happened. Temporal retries the Activity, and the claim
turns that retry into at-most-once for the tool's effects.

A failure before the claim is safe to retry anywhere, and it's typed `FailedBeforeClaim`, so the
Workflow may move the call to another queue. A failure after the claim is `FailedAfterClaim` and
not retried, since every retry would only find the claim.

## A fence token decides who may write the session file

Temporal already knows which attempt of which Activity is current. The Workflow numbers every
Activity that writes the session file, as `<run start>.<Activity number>`, and the Activity adds
its attempt. A later run, Activity, or attempt sorts higher. An Activity creates its token beside
the file, and the agent's write guard refuses an append once a higher token is there. A retry
takes over at once, with no lease to wait out and no clock to trust.

It's a checked fence, not one storage enforces, so an attempt that stalls between the check and
its append can land that one append. On NFS it needs `actimeo=0`. If your session lives in a
store with compare-and-set, such as object storage with `If-Match` or a database row version, use
the token there and the gap closes.

The tree store keeps a lease instead (`src/tree/lease.ts`), because clients and hosts write it
outside any Workflow, so there's no Workflow to number them.

## Host queues, with a fallback

A tool writes the project directory of the host it runs on. So the model call reports its Worker's
host queue, and the step's tools and seal go there. A host-queue tool call gets one attempt and a
30-second schedule-to-start timeout. That timeout proves the call never started, so the step may
move it to the shared queue. Any other failure might mean the tool is still running, so the step
records what it has and goes on, and the lost host can't publish later.

This is Temporal's worker-specific Task Queue pattern. You need it only when Activities depend on
a host's local state.

## Retries and timeouts per unit

| unit | timeout | attempts | why |
|---|---|---|---|
| model call | 10 minutes | 10 | a hung stream should end long before the step's cap. Most retries are refusals before the call, which cost nothing |
| tool call, shared queue | `PI_TEMPORAL_TOOL_TIMEOUT_MINUTES` | 20 | failures before the claim, such as a host refusing the project |
| tool call, host queue | same | 1 | a second attempt's queue timeout couldn't rule out the first still running |
| seal | 30 minutes | 10, or 3 on a host queue | fenced and safe to repeat |
| everything | 120 minutes total | | a unit that keeps timing out can't hold the session for days |

The agent retries its provider on its own, inside the seal, and counts those retries in
`agentState`. So the Activity retries are for a lost Worker or storage, not for the model.

## A stop reaches the work

A stop cancels the turn's scope. Tool and model Activities use `WAIT_CANCELLATION_COMPLETED`, so
the Workflow waits for each to stop and report. The Activity passes its cancellation signal to the
tool and the model call, which end like a user stop in the agent. The recovery seal then records
what each tool reported instead of unknown outcomes. Cancellation reaches an Activity only with a
heartbeat's answer, so Workers send heartbeats at least every 3 seconds.

## No patch gates until there are running sessions

`patched()` keeps old histories replaying after a change. This repo has none yet, so it has no
patch gates. Instead, `checks/histories/` keeps a history of each kind of session run, and
`replay-check` replays them all in CI. A change that breaks one would break running sessions on
upgrade. Gate it with `patched()`, or ship it as a new Worker Deployment Version and let running
sessions finish on the old one. A session reaches a new run at every idle exit and every
Continue-As-New, which are the natural points to pick up new code.

## Observability

The Workflow puts its state in the memo when it changes, so `running` is one List call. The step
number goes into the current details, which cost no history. Each Activity has a summary, such as
`tool bash (call_1)`, and each session a static summary. `PI_TEMPORAL_SEARCH_ATTRIBUTE=1` also
keeps the state in a `PiSessionState` keyword search attribute, for server-side filters. It's off
by default because the namespace must register the attribute first. `PI_TEMPORAL_METRICS` turns
on the SDK's Prometheus metrics. Activities log through the Activity logger, so each line carries
its Workflow and Activity ids.

## Payloads can be encrypted

Prompts, answers, and error text pass through history, and for a coding agent they often hold
code or secrets. `PI_TEMPORAL_CODEC_KEY` turns on an AES-GCM payload codec in every client and
Worker, and the server then stores only ciphertext. The UI needs a codec server with the same key
to show them.
