# pi-temporal

A Temporal-backed durable executor for the [Pi coding agent](https://github.com/earendil-works/pi), shipped as a plugin around Pi's SDK. Same pattern we proved on the OpenCode fork: the agent's own loop runs under a durable executor, while the session record stays in the app's own log.

This is a work in progress. It has not yet been run against a live Pi.

## The idea

Two parts of the state, two systems:

- **Durable storage** stays with Pi. Pi's `SessionManager` already persists the conversation (messages, tool results, the tree) to a JSONL session file. That file is the source of truth. We do not move it into Temporal.
- **Durable execution** comes from Temporal. A per-session workflow drives Pi's turns and survives a crash: the turn re-runs on another worker and continues from the session file.

This is the `storage` vs `execution` split from the AI-399 write-up, applied to a harness that (unlike OpenCode) has no swappable `SessionExecution` abstraction. So we drive Pi from the outside via its SDK rather than replacing an internal interface.

## Granularity: turn-level for now

Pi's SDK exposes `session.prompt(text)`, which runs a whole turn (the full agent loop, model calls plus tools) to `agent_end`. It does not expose a single-step runner. So the durable unit here is one prompt-to-`agent_end`, driven as one Temporal activity. That is the coarser, turn-level shape (the OpenCode Phase-1 analog), not the per-step re-drive we did on the OpenCode engine.

Step-level durability would need Pi to expose "run one model call plus its tools, then return" so an outer driver can checkpoint between steps. That is an open item (see `docs/step-level.md` once it exists), and a good thing to raise with the Pi maintainers.

## What is durable, and what is not

- A worker dying between turns: the next turn re-drives on any worker from the session file. Safe.
- A worker dying mid-turn (mid agent-run): Temporal re-runs the whole `runPrompt` activity. The activity re-opens the session file and must not double-apply the prompt or double-run a side-effecting tool. The idempotency guard (a recorded prompt marker, checked before running) is the load-bearing part and is the first thing to verify against a live Pi.

## Layout

- `src/config.ts` — Temporal + Pi wiring from env.
- `src/protocol.ts` — workflow id, signal/update names, shared types.
- `src/activities.ts` — `runPrompt`: drives one Pi turn via `@earendil-works/pi-coding-agent`, session file as the log.
- `src/workflow.ts` — `piSession`: per-session durable executor (submit prompt, drive, interrupt, idle-terminate).
- `src/worker.ts` — worker hosting the workflow + activity.
- `src/client.ts` — helpers to submit a prompt / interrupt a session.
- `src/demo.ts` — end-to-end smoke once a model key is set.

## Status

- [x] Design + scaffold against Pi's real SDK (`createAgentSession`, `session.prompt`, `subscribe`, `SessionManager`).
- [ ] Verify turn-level re-drive against a live Pi (crash mid-turn, resume from the session file).
- [ ] Idempotency guard for a re-driven prompt (no double-apply, no double side effect).
- [ ] Package as an installable Pi extension (`pi install`) once the driver is proven.
- [ ] Raise step-level durability with the Pi maintainers.

## Prior art

[osolmaz/pi-workflows](https://github.com/osolmaz/pi-workflows) makes user-defined workflow graphs durable in Pi with a home-grown file-queue engine. This project is a different target: the agent's own turn loop, durable via Temporal. We reuse its integration ideas (headless Pi driven from an outer host), not its graph feature.
