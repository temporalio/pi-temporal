# Putting your own agent on it

Implement `Agent` from [`src/core/agent.ts`](../src/core/agent.ts) to use the Temporal loop with
your own agent. Pi’s implementation in [`src/pi/agent.ts`](../src/pi/agent.ts) is about 230 lines.
The sections below describe the contract and the modules you can omit.

## What your agent must provide

Your agent must reopen a session from a file and run one step at a time. Each append must pass
through a write guard. Most agent loops need changes to support this. Pi needed its fork.

| method | what it does |
|---|---|
| `open(file, guard)` | Opens the session. Every append calls `guard()` first and stops if it throws. |
| `prepareStep()` | Settles what a stopped turn left open. Says whether there’s work, or `"busy"`. |
| `hasPrompt(id)`, `recordPrompt(id, text)` | Puts the prompt in the session once, without running the model. |
| `modelCall(signal)` | One model call. Records the response and its tool calls, runs none. |
| `runToolCall(id, signal)` | Runs one recorded call and reports the outcome. Writes nothing. |
| `sealStep(outcomes, options)` | Writes the step’s outcomes in the model’s order and says whether the turn is over. |
| `answered`, `asked`, `unanswered`, `endsWithResponse`, `lastAnswer` | Reads the session, so a retry can tell what an earlier attempt did. |
| `abort()`, `waitForIdle()`, `dispose()` | Stops the running work, waits for work the session started after a seal, and closes it. |
| `spend()`, `latestEntry`, `appendEntry` | Token totals, and a place for the core’s bookkeeping. |
| `openRecord(file, guard)` | The session’s record alone, without the model, for one bookkeeping entry. |
| `unknownOutcome(call)`, `notRunOutcome(call)` | What the model is told about a call that may have run, or never started. |

Use [`src/pi/activities.ts`](../src/pi/activities.ts) as the wiring example.

```ts
makeCoreActivities({ agent: yourAgent(options), hostQueue });
```

## What must hold

- Every append must call the guard first. An unguarded write can let a superseded attempt change
  the session.
- Tool calls must return outcomes without writing to the session. Calls can run in parallel, so
  the seal must write their results together to avoid conflicting writes.
- Outcomes must survive `JSON.stringify` because they wait in a file until the seal reads them.
- `hasPrompt` must find a prompt written by `recordPrompt`, even after compaction. Otherwise a
  retry could record the prompt twice. Pi marks each prompt with its ID.
- A retried model call must reuse its recorded response. Temporal can lose the Activity’s
  completion after the response reaches the session file. Calling the model again would add a
  second response and another charge.
- A retried seal must recognize outcomes it already wrote. It must return the same decision
  about whether the turn is over, so a lost completion doesn’t change the next step.
- When `signal` aborts, stop the model call or tool and report its outcome. The recovery seal
  needs that outcome to record what happened.
- Keep state between steps in `agentState`. Each Activity opens the session again, so a retry
  count kept only in memory would reset at each step.

## What you can delete

| if you don’t need | delete |
|---|---|
| Workers on different hosts sharing a project | `src/tree/`, and the `store` option |
| a retry and timeout per tool call | stepped mode: `src/core/stepped-step.ts`, `makeSteppedStep` in `workflow.ts`, and the three stepped Activities |
| a Workflow behind each live turn | `src/pi/local-turn-*`, its export in `src/workflow-bundle.ts`, and its Worker in the extension |
| bounds on spend | `TurnBudget`, `overBudget`, and the deadline scope in `runTurn` |
| schedules | `adoptProject`, `template`, and `cli.ts schedule` |

## What to read next

[design-decisions.md](design-decisions.md) says why each piece is built the way it is.
[guarantees.md](guarantees.md) says what survives which failure, with the check that shows it.
