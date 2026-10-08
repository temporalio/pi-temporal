# Putting your own agent on it

Everything in `src/core/` works for any agent that implements `Agent` from
[`src/core/agent.ts`](../src/core/agent.ts). Pi's implementation is
[`src/pi/agent.ts`](../src/pi/agent.ts), about 230 lines. This page is what yours must do, and what
you can delete.

## What your agent must provide

Your agent needs a session it can open again from a file, take one step at a time, and append to
behind a guard. Most agent loops need some change for that. Pi needed its fork.

| method | what it does |
|---|---|
| `open(file, guard)` | Opens the session. Every append calls `guard()` first and stops if it throws. |
| `prepareStep()` | Settles what a stopped turn left open. Says whether there's work, or `"busy"`. |
| `hasPrompt(id)`, `recordPrompt(id, text)` | Puts the prompt in the session once, without running the model. |
| `modelCall(signal)` | One model call. Records the response and its tool calls, runs none. |
| `runToolCall(id, signal)` | Runs one recorded call and reports the outcome. Writes nothing. |
| `sealStep(outcomes, options)` | Writes the step's outcomes in the model's order and says whether the turn is over. |
| `answered`, `asked`, `unanswered`, `endsWithResponse`, `lastAnswer` | Reads the session, so a retry can tell what an earlier attempt did. |
| `spend()`, `latestEntry`, `appendEntry` | Token totals, and a place for the core's bookkeeping. |
| `unknownOutcome(call)`, `notRunOutcome(call)` | What the model is told about a call that may have run, or never started. |

Then wire it the way [`src/pi/activities.ts`](../src/pi/activities.ts) does:

```ts
makeCoreActivities({ agent: yourAgent(options), hostQueue });
```

## What must hold

- **Every append goes through the guard.** The fence works only if nothing writes the session
  without asking it first.
- **A tool call writes nothing to the session.** It reports an outcome, and the seal writes all of
  a step's outcomes together. Siblings run in parallel, so two writers would conflict.
- **An outcome survives `JSON.stringify`.** It waits in a file between the tool call and the seal.
- **The prompt is recorded once.** `hasPrompt` must find a prompt `recordPrompt` wrote, even after
  your agent compacts the conversation. Pi marks it with the prompt id.
- **A stop is a stop.** When `signal` aborts, end the model call or tool like a user stop and
  report what happened.
- **Your agent's state between steps goes in `agentState`.** A session is opened again for every
  Activity, so a retry count kept in memory would reset each step.

## What you can delete

| if you don't need | delete |
|---|---|
| Workers on different hosts sharing a project | `src/tree/`, and the `store` option |
| a retry and timeout per tool call | stepped mode: `src/core/stepped-step.ts` and the three stepped Activities |
| a Workflow behind each live turn | `src/pi/local-turn-*` and its Worker in the extension |
| bounds on spend | `TurnBudget`, `overBudget`, and the deadline scope in `runTurn` |
| schedules | `adoptProject`, `template`, and `cli.ts schedule` |

## What to read next

[design-decisions.md](design-decisions.md) says why each piece is built the way it is.
[guarantees.md](guarantees.md) says what survives which failure, with the check that shows it.
