# pi-temporal

A Temporal-backed durable executor for the [Pi coding agent](https://github.com/earendil-works/pi), shipped as a plugin around Pi's SDK. Same pattern we proved on the OpenCode fork: the agent's own loop runs under a durable executor, while the session record stays in the app's own log.

Status: verified end to end against a live Pi (SDK 0.84.2 fork). Happy path works, and a worker killed mid-turn recovers on a fresh worker with no duplicate prompt. Both recovery paths are proven:

- Crash before anything persisted: the whole turn re-runs from scratch and completes.
- Crash with a dangling tool call already on disk: `resumeInterruptedTurn()` repairs it (fails the dangling tool, keeps completed results) and drives the turn to completion. The user prompt is not re-added and the side effect is not blindly re-run.

## Depends on the Pi fork

Mid-turn recovery needs the resume API added on the `temporalio/pi` fork branch `moe/step-and-resume` (`AgentSession.resumeInterruptedTurn()`, `AgentSession.step()`), which is not in the published `@earendil-works/pi-coding-agent`. Until a fork build is published, link it locally:

```
# in the fork
cd ~/repos/pi/packages/coding-agent && npm link
# in this repo
cd ~/repos/pi-temporal && npm link @earendil-works/pi-coding-agent
```

The linked package resolves its sibling `@earendil-works/pi-agent-core` (which carries `Agent.step` and the exported tool-result helpers) from the fork's own workspace, so the whole fork API is picked up.

## The idea

Two parts of the state, two systems:

- **Durable storage** stays with Pi. Pi's `SessionManager` already persists the conversation (messages, tool results, the tree) to a JSONL session file. That file is the source of truth. We do not move it into Temporal.
- **Durable execution** comes from Temporal. A per-session workflow drives Pi's turns and survives a crash: the turn re-runs on another worker and continues from the session file.

This is the `storage` vs `execution` split from the AI-399 write-up, applied to a harness that (unlike OpenCode) has no swappable `SessionExecution` abstraction. So we drive Pi from the outside via its SDK rather than replacing an internal interface.

## Granularity: turn-level for now

Pi's SDK exposes `session.prompt(text)`, which runs a whole turn (the full agent loop, model calls plus tools) to `agent_end`. It does not expose a single-step runner. So the durable unit here is one prompt-to-`agent_end`, driven as one Temporal activity. That is the coarser, turn-level shape (the OpenCode Phase-1 analog), not the per-step re-drive we did on the OpenCode engine.

Step-level durability would need Pi to expose "run one model call plus its tools, then return" so an outer driver can checkpoint between steps. That is an open item (see `docs/step-level.md` once it exists), and a good thing to raise with the Pi maintainers.

## What is durable, and what is not (verified by a crash test)

- **Between turns: clean.** A worker dying between turns loses nothing. The next prompt re-drives on any worker from the session file. A fresh worker that never saw the session serves it correctly.
- **Worker re-execution: works.** Kill the worker mid-turn and Temporal re-runs the `runPrompt` activity on another worker (observed: activity attempt 2 completes), with no duplicate prompt. That is durable execution doing its job.
- **Mid-turn recovery: handled via the fork's resume API.** A crash mid agent-run leaves Pi's session with a dangling tool-call assistant message and no final answer. The stock SDK cannot resume that. The fork adds `AgentSession.resumeInterruptedTurn()`, which repairs dangling tool calls (fails them, keeps completed tool results) and drives the turn to completion. On a retry, `runPrompt` sees the prompt marker already recorded and calls `resumeInterruptedTurn()` instead of re-prompting, so there is no duplicate prompt and no re-run side effect. The resume mechanic is proven by the fork's own unit tests (`packages/coding-agent/test/resume-interrupted-turn.test.ts`, mock model); the live crash test here is pending a working model key.

For per-step durability, drive `AgentSession.step()` (also on the fork) one step per activity, calling `resumeInterruptedTurn()` first when the transcript ends in a dangling tool call.

## Reproducing

`scripts/`-style helpers are at the repo root: `submit.mts` (submit one prompt) and `inspect.mts` (summarize a session file). The crash test: start a worker, submit a turn whose bash tool sleeps a few seconds, `pkill -9 -f "pi-temporal.*src/worker.ts"` while it is in flight, wait past the 30s heartbeat timeout, start a fresh worker, and inspect. You will see the activity reach attempt 2 and the between-turn case recover; the mid-turn case shows the dangling-tool limitation above.

## Layout

- `src/config.ts` — Temporal + Pi wiring from env.
- `src/protocol.ts` — workflow id, signal/update names, shared types.
- `src/activities.ts` — `runPrompt`: drives one Pi turn via `@earendil-works/pi-coding-agent`, session file as the log.
- `src/workflow.ts` — `piSession`: per-session durable executor (submit prompt, drive, interrupt, idle-terminate).
- `src/worker.ts` — worker hosting the workflow + activity.
- `src/client.ts` — helpers to submit a prompt / interrupt a session.
- `src/demo.ts` — end-to-end smoke once a model key is set.

## Status

- [x] Design + scaffold against Pi's real SDK (`createAgentSession`, `session.prompt`, `SessionManager`, `ModelRuntime`), typechecks.
- [x] Live happy-path turn (OpenAI via `ModelRuntime.getAvailable` + `setRuntimeApiKey`).
- [x] Crash test: turn re-executes on a fresh worker (activity attempt 2), no duplicate prompt.
- [x] Found the turn-level limit: mid-turn crash cannot resume cleanly on the stock SDK.
- [x] Added the fix on the Pi fork (`resumeInterruptedTurn`, `step`); proven by the fork's mock-model tests.
- [x] Wired `runPrompt` to call `resumeInterruptedTurn()` on retry instead of re-prompting.
- [x] Live re-verification: mid-turn crash recovers via `resumeInterruptedTurn()`, no duplicate prompt, tool balance intact.
- [ ] Package as an installable Pi extension (`pi install`), once a fork build is published.

## Prior art

[osolmaz/pi-workflows](https://github.com/osolmaz/pi-workflows) makes user-defined workflow graphs durable in Pi with a home-grown file-queue engine. This project is a different target: the agent's own turn loop, durable via Temporal. We reuse its integration ideas (headless Pi driven from an outer host), not its graph feature.
