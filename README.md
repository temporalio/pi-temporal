# pi-temporal

A Temporal-backed durable executor for the [Pi coding agent](https://github.com/earendil-works/pi), shipped as a plugin around Pi's SDK. Same pattern we proved on the OpenCode fork: the agent's own loop runs under a durable executor, while the session record stays in the app's own log.

Status: runs against a live Pi (SDK 0.84.2). A happy-path turn works end to end, and a turn re-executes on a fresh worker after a crash. The important negative result is that a mid-turn crash cannot be recovered cleanly at turn granularity (see below).

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
- **Mid-turn recovery: coarse, and this is the real limitation.** A crash mid agent-run leaves Pi's session with a dangling tool-call assistant message and no final answer. Pi's SDK exposes no way to resume an in-flight turn, so on retry we can only re-prompt (which duplicates the prompt and re-runs the side effect) or bail. There is no clean resume at turn granularity. Clean mid-turn recovery needs step-level control (run one model call plus its tools, checkpoint, continue), which the OpenCode engine exposed and Pi's SDK does not.

The concrete finding: `runPrompt` is one whole agent-run, so a mid-turn crash is not a resumable unit. This is the strongest argument for asking the Pi maintainers for a step boundary, or for driving Pi through a lower-level loop than `session.prompt`.

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
- [x] Found the turn-level limit: mid-turn crash cannot resume cleanly (dangling tool call, no Pi resume API).
- [ ] Idempotency for a mid-turn re-drive (blocked on step-level control from Pi).
- [ ] Package as an installable Pi extension (`pi install`).
- [ ] Raise step-level durability, or a "resume in-flight turn" API, with the Pi maintainers. **This is now the gating item, not a nice-to-have.**

## Prior art

[osolmaz/pi-workflows](https://github.com/osolmaz/pi-workflows) makes user-defined workflow graphs durable in Pi with a home-grown file-queue engine. This project is a different target: the agent's own turn loop, durable via Temporal. We reuse its integration ideas (headless Pi driven from an outer host), not its graph feature.
