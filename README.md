# pi-temporal

A Temporal-backed durable executor for the [Pi coding agent](https://github.com/earendil-works/pi), shipped as a plugin around Pi's SDK. Same pattern we proved on the OpenCode fork: the agent's own loop runs under a durable executor, while the session record stays in the app's own log.

The durable unit is one step: a single model call and the tools it asks for. The workflow runs one Temporal activity per step, so a worker dying takes one step with it and every step before it stays done.

Status: verified end to end against a live Pi (SDK 0.84.2 fork). A turn that took three steps cost three activities. Killing the worker mid-step re-drove that step alone: the step before it stayed done and its `>>` append did not happen twice, the step in flight came back as attempt 2 on a fresh worker, the prompt was not re-added, and the turn ran on to its answer.

## Depends on the Pi fork

Stepping calls four things the published `@earendil-works/pi-coding-agent` does not have. They come from [temporalio/pi#2](https://github.com/temporalio/pi/pull/2) (branch `moe/step-and-resume`):

- `recordPrompt(text)` puts a prompt in the transcript without running it.
- `step()` runs one model call and its tools, and reports whether the turn is done.
- `prepareStep()` settles what a stopped turn left behind, without running to the end of it.
- `resumeInterruptedTurn()` is `prepareStep()` plus a run to the end of the turn, for a caller that wants the whole turn back in one call.

So the dependency is a build of the fork, pinned by commit in `fork.pin`:

```
npm ci
npm run setup-fork
```

`setup-fork` fetches that exact commit into `.fork/pi` (ignored), builds it, and links it into `node_modules`. CI runs the same two commands, so a fresh clone and a CI run get the same build. The linked package resolves its sibling `@earendil-works/pi-agent-core` (which carries `Agent.step`) from the fork's own workspace, so the whole fork API is picked up.

Run `setup-fork` after any `npm ci`, which wipes `node_modules` and takes the link with it. To move to a newer commit of the PR, edit `PI_FORK_REF` in `fork.pin` and run it again.

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

## What is durable, and what is not

- **Between turns: clean.** A worker dying between turns loses nothing. The next prompt drives on any worker from the session file. A fresh worker that never saw the session serves it correctly.
- **Between steps: clean.** Each step is its own activity, so a worker dying loses at most the step in flight. The steps before it are on disk and are not re-run.
- **Mid-step: the tool is reported as unknown, not re-run.** A crash between a tool starting and its result landing leaves a tool call with no result. `prepareStep` settles it with "the outcome of this tool call is unknown", and the model decides whether to try again. Blindly re-running it is the wrong default for a coding agent: the `git push` may already have happened.
- **A step is not atomic.** Pi runs the tools of one step as a batch, so a crash part way through that batch leaves some tools run and some not. The settled ones keep their results; only the unsettled one is reported as unknown.

## Reproducing

Helpers are at the repo root: `submit.mts` (submit one prompt), `inspect.mts` (summarize a session file), and `step-loop-check.mts` (run the executor against a Temporal server with a stubbed activity: one activity per step, an interrupt that ends the turn and not the session; no model key needed).

The crash test: start a worker; submit a turn that appends to a file with one bash call and sleeps in the next, one at a time; poll the session file until `toolCalls > toolResults` (a tool call in flight); `pkill -9 -f "pi-temporal.*src/worker.ts"`; wait past the 30s heartbeat timeout; start a fresh worker. `temporal workflow show` will have the earlier step completed on attempt 1 and the interrupted one on attempt 2, and the appended file will have one line, not two.

## Layout

- `src/config.ts` — Temporal + Pi wiring from env.
- `src/protocol.ts` — workflow id, signal/update names, shared types.
- `src/activities.ts` — `runStep`: advances one Pi turn by one step via `@earendil-works/pi-coding-agent`, session file as the log.
- `src/workflow.ts` — `piSession`: per-session durable executor (submit prompt, step to the end of the turn, interrupt, idle-terminate).
- `src/worker.ts` — worker hosting the workflow + activity.
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
- [ ] Package as an installable Pi extension (`pi install`), once a fork build is published.

## Prior art

[osolmaz/pi-workflows](https://github.com/osolmaz/pi-workflows) makes user-defined workflow graphs durable in Pi with a home-grown file-queue engine. This project is a different target: the agent's own turn loop, durable via Temporal. We reuse its integration ideas (headless Pi driven from an outer host), not its graph feature.
