# Checks

Each `*-check.mts` script checks one contract and exits nonzero on failure. Its header says what
it needs. `npm run checks` runs every one that needs neither a model key nor Docker. Most need a
local Temporal server (`scripts/temporal-dev.sh`).

The groups below show test patterns you can copy for your own agent.

## Pure functions, no server

These checks run logic that needs no Temporal server.

`take-fence-check`, `pending-check`, `lease-check`, `lease-recovery-check`, `lock-gap-check`,
`stall-check`, `session-id-check`, `config-check`, `stale-tip-check`, `worktree-check`,
`storage-repair-check`, `liveness-linux-check`, `release-tag-check`, `session-dir-check`

## The step driver with injected fakes, no server

`src/core/stepped-step.ts` takes its Activities and SDK pieces as arguments, so it runs with fakes
and no Worker. This lets a check control routing and fallback failures. It can also test
cancellation without waiting for Activity timeouts.

`stepped-step-check`, `interrupted-seal-check`, `lost-host-check`, `migration-rejoin-check`

## Real Activities over a faked agent session

These checks call production Activities with a fake `AgentSession`. They control races and retry
results so a failure can be reproduced without a model call.

They run under `MockActivityEnvironment`, the SDK's way to give an Activity its context with no
Worker, so the Activities need no test-only code. A check that stands in for a retry gives each
call its own environment and `attempt`.

`dispatch-check`, `stale-dispatch-check`, `seal-claim-check`, `result-recovery-check`,
`compacted-prompt-check`, `spend-check`, `retire-seconds-check`, `quarantine-check`

`shutdown-check` cancels `runStep` through its `MockActivityEnvironment` for each reason the SDK
gives, and only a requested cancel stops it.

`writer-marker-check` and `session-root-check` run the core Activities over the echo agent, with no
Pi. `echo-journal-check` cuts the echo agent's session file mid-append.
`echo-shape-check` rejects malformed complete entries on load and before append, without a retry
or a write, and checks that valid entries remain readable. Neither echo check needs a server.

## The Workflow with stub Activities, on a dev server

These checks run a real Worker and Workflow with stub Activities. They cover prompt submission
and budgets, as well as routing and Continue-As-New.

`step-loop-check`, `submit-check`, `budget-check`, `continue-as-new-check`, `workflow-init-check`,
`local-turn-check`, `embedded-stop-check`, `codec-check`, `unschedule-check`, `cli-check`,
`tracing-check`

`versioning-check` runs a session on a Worker with Worker Versioning on. It makes a version
current and reads the versioning behavior the server records for the run.

## Time skipping

`time-skipping-check` uses `TestWorkflowEnvironment.createTimeSkipping()`, so an hour of idle time
passes in a moment. Use it for any timer path.

## Replay

`replay-kept-check` replays every history in `histories/` against the current code, with no
server. CI runs it as its own `replay` job. A history that stops replaying means running sessions
would break on upgrade. Keep each history once it's recorded, and don't record it again.
`record-histories.mts` adds new ones next to the old.

`replay-check` needs a dev server. It covers only a history the current code writes, from a step
whose host-queue tool failed after it started.

## Real processes, killed or paused

`faux-worker.mts` is a Worker with the real Activities over a real Pi session and a scripted
model, run as a child process so a check can `SIGKILL` or `SIGSTOP` it mid-Activity.

`seal-check`, `fence-check`, `quarantine-routing-check`, `detached-check`

`echo-check` does the same with no Pi. It runs `examples/echo/` as its README says, then
`SIGKILL`s a Worker while the echo tool runs, and checks that a new Worker reports the tool's
outcome as unknown instead of running it again. Start here to test your own agent.

## Docker

The scripts in `../docker/` run Workers in separate containers, over NFS, and across a container
restart. CI runs `restart-check.sh`. The others need a model key.
