# Putting your own agent on it

Implement `Agent` from [`src/core/agent.ts`](../src/core/agent.ts) to use the Temporal loop with
your own agent. Start from [`examples/echo/`](../examples/echo/). It's a whole agent over its own
session file, with a Worker and a client, and it runs with only a dev server: no Pi, no model key,
no Docker. Pi's implementation in [`src/pi/agent.ts`](../src/pi/agent.ts) is about 220 lines.
The sections below describe the contract and the modules you can omit.
Echo builds without the [#46](https://github.com/temporalio/pi-temporal/issues/46) fix separate
a cut only when they find one at open, which breaks the late-cut rule below. Don't copy that.

## What your agent must provide

Your agent must reopen a session from a file and run one step at a time. Each append must pass
through a write guard. Most agent loops need changes to support this. Pi needed its fork.

| method | what it does |
|---|---|
| `open(file, guard)` | Opens the session. Every append calls `guard()` first and stops if it throws. |
| `prepareStep()` | Settles what a stopped turn left open. Says whether there's work, or `"busy"`. |
| `hasPrompt(id)`, `recordPrompt(id, text)` | Puts the prompt in the session once, without running the model. |
| `modelCall(signal)` | One model call. Records the response and its tool calls, runs none. |
| `runToolCall(id, signal)` | Runs one recorded call and reports the outcome. Writes nothing. |
| `sealStep(outcomes, options)` | Writes the step's outcomes in the model's order and says whether the turn is over. Stops its retry or compaction when `options.signal` aborts. |
| `answered`, `asked`, `unanswered`, `endsWithResponse`, `lastAnswer` | Reads the session, so a retry can tell what an earlier attempt did. |
| `waitForIdle()`, `dispose()` | Waits for work the session started after a seal, and closes it. |
| `spend()`, `latestEntry`, `appendEntry` | Token totals, and a place for the core's bookkeeping. |
| `openRecord(file, guard)` | The session's record alone, without the model, for one bookkeeping entry. |
| `unknownOutcome(call)`, `notRunOutcome(call)` | What the model is told about a call that may have run, or never started. |

Use [`examples/echo/worker.ts`](../examples/echo/worker.ts) as the wiring example. It imports
nothing outside `src/core/`.

```ts
const worker = await createSessionWorker({
  address,
  namespace,
  taskQueue,
  // Anyone who can start a Workflow picks its input. The Worker writes only under this directory.
  activities: () => makeCoreActivities({ agent: yourAgent(options), sessionRoot }),
});
await worker.run();
```

`createSessionWorker` registers the core session Workflow (`src/core/workflows.ts`) unless you
pass `workflowsPath`. Pi passes `src/workflow-bundle.ts`, which adds its live-turn Workflow.
To send a prompt, use `sendPrompt(client, { taskQueue, sessionId, input }, prompt)` from
`src/core/client.ts`, where `input` is the `SessionInput` the session starts with.
[`examples/echo/send.ts`](../examples/echo/send.ts) sends one and waits for the answer.

## What must hold

- Every append must call the guard first. An unguarded write can let a superseded attempt change
  the session.
- A crash can cut the last entry in half. Reading must skip a cut entry, and every later append
  must keep it separate from the new entries, including a complete batch. This must hold when
  another writer leaves the cut after the session opened or after an earlier successful append.
  Checking the tail before an append isn't enough, since the cut can land between the check and
  the append; separate every append unconditionally.
  Opening must not repair the file before the guard is installed. Writes must only append: a
  superseded writer can still write once after its guard, and a rewrite could erase what a newer
  writer added. These recovery rules require atomic appends; see the filesystem limits in
  [guarantees.md](guarantees.md#the-rules).
- Tool calls must return outcomes without writing to the session. Calls can run in parallel, so
  the seal must write their results together to avoid conflicting writes.
- Outcomes must survive `JSON.stringify` because they wait in a file until the seal reads them.
- `hasPrompt` must find a prompt written by `recordPrompt`, even after compaction. Otherwise a
  retry could record the prompt twice. Pi marks each prompt with its ID.
- `prepareStep` must settle calls that a stopped or crashed step left open as unknown outcomes,
  and never run them again. A whole-step `runStep` has no dispatch claims, so this is all that
  keeps its retry from running a tool twice.
- A retried model call must reuse its recorded response. Temporal can lose the Activity's
  completion after the response reaches the session file. Calling the model again would add a
  second response and another charge.
- A retried seal must recognize outcomes it already wrote. It must return the same decision
  about whether the turn is over, so a lost completion doesn't change the next step.
- When `signal` aborts, stop the model call or tool and report its outcome. The recovery seal
  needs that outcome to record what happened.
- Keep state between steps in `agentState`. Each Activity opens the session again, so a retry
  count kept only in memory would reset at each step.
- The session is one file path. Its sibling paths (`${sessionFile}.*`) must be writable too,
  since dispatch claims, kept results and fence tokens live there.
- A failure that won't change on retry must throw `ApplicationFailure.nonRetryable`, such as a
  bad key or a session file that can't be parsed. Any other error burns all of the Activity's
  retries first.
- Reading must refuse an entry that parses but isn't one of the agent's own, without a retry.
  Read as is, it crashes a later step with an error that every retry repeats. An append must
  check the line it will write the same way, after the guard and before writing, so the session
  never holds what the next read refuses. A seal checks all of its outcomes before it writes any.
  `echo-shape-check` shows the echo agent doing this.
- Retry the provider yourself, inside the step. The Activity retry policy assumes the agent
  does, and only retries what a lost Worker left behind.
- Return what each method promises.
  - `runToolCall` returns `undefined` when the session already holds a result for the call.
  - `recordPrompt` returns false when it didn't record the prompt. The turn fails without retry.
  - `prepareStep` returns `"busy"` when something else drives the session. The Activity fails
    and Temporal retries it.
  - `ModelCall.ended` means the response ended the run, such as an aborted call. Nothing is
    dispatched, but the step is still sealed.
  - `ModelCall.sequential` means the step's tools must run one at a time, in order.

The pinned Pi fork's `SessionManager` starts every append to an existing file with a newline.
Its session files contain blank separator lines and are not strict JSON Lines; readers and
session tooling must skip empty lines. `pi-journal-check` covers late cuts and complete batches.
Opening a current-version Pi session with a valid header is read-only. Empty-file initialization
and older-format migration still write without a guard, an exception to the contract above.
Migration rewrites the whole file and can erase newer entries; this is tracked in
[#54](https://github.com/temporalio/pi-temporal/issues/54).
See [guarantees.md](guarantees.md#the-rules) for filesystem and deployment limits.

## What you can delete

| if you don't need | delete |
|---|---|
| Workers on different hosts sharing a project | `src/tree/`, and the `store` option |
| a retry and timeout per tool call | stepped mode: `src/core/stepped-step.ts`, `makeSteppedStep` in `workflow.ts`, and the three stepped Activities. `src/pi/local-turn-workflow.ts` imports `dispatchStepCalls` from `stepped-step.ts`, so change it too, or delete live turns |
| Activities that use a host's local files | host queues: `src/core/queue.ts`, the `hostQueueFor` option of `createSessionWorker`, `onHost` and `retireOn` in `workflow.ts`, `onHost` and `viaHost` in `stepped-step.ts`, and the `hostQueue` option of `makeCoreActivities` in `activities.ts`. Only tree shipping turns them on |
| a Workflow behind each live turn | `src/pi/local-turn-*`, its export in `src/workflow-bundle.ts`, and its Worker in the extension. A Worker with no `workflowsPath` never loads it |
| Pi's names | they are in every recorded history: `WORKFLOW_TYPE` (`piSession`, also the Workflow function's name), the `pi-session-` id prefix, the memo name `SESSION_MEMO`, the `PiSessionState` search attribute, and the static summary in `sendPrompt`. Renaming them breaks `replay-kept-check`, so delete `checks/histories/` and record yours with `checks/record-histories.mts` |
| bounds on spend | `TurnBudget`, `overBudget`, and the deadline scope in `runTurn` |
| schedules | `adoptProject`, `template`, and `cli.ts schedule` |

## What to read next

[design-decisions.md](design-decisions.md) says why each piece is built the way it is.
[guarantees.md](guarantees.md) says what survives which failure, with the check that shows it.
