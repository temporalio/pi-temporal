# Checks

Each `*-check.mts` script checks one contract and exits nonzero on failure. Its header says what
it needs. `npm run checks` runs every one that needs neither a model key nor Docker. Most need a
local Temporal server (`scripts/temporal-dev.sh`).

They're grouped here by how they test, since that's the part to copy for your own agent.

## Pure functions, no server

Logic with no Temporal in it, run directly.

`take-fence-check`, `pending-check`, `lease-check`, `lease-recovery-check`, `lock-gap-check`,
`stall-check`, `session-id-check`, `config-check`, `stale-tip-check`, `worktree-check`,
`storage-repair-check`, `quarantine-check`, `liveness-linux-check`

## The step driver with injected fakes, no server

`src/core/stepped-step.ts` takes its Activities and SDK pieces as arguments, so it runs with fakes
and no Worker. The quickest way to test routing, fallback, and cancellation.

`stepped-step-check`, `interrupted-seal-check`, `lost-host-check`, `migration-rejoin-check`

## Real Activities over a faked agent session

The production Activities, called directly, with a fake `AgentSession`. Shows what an Activity
does on a retry, a race, or a stale attempt.

`dispatch-check`, `stale-dispatch-check`, `seal-claim-check`, `result-recovery-check`,
`compacted-prompt-check`, `spend-check`, `retire-seconds-check`

## The Workflow with stub Activities, on a dev server

A real Worker and Workflow, with Activities that return what the case needs. Shows the Workflow's
own logic: queues, Updates, budgets, Continue-As-New, routing.

`step-loop-check`, `submit-check`, `budget-check`, `continue-as-new-check`, `workflow-init-check`,
`local-turn-check`, `embedded-stop-check`, `codec-check`, `unschedule-check`, `cli-check`

## Time skipping

`time-skipping-check` uses `TestWorkflowEnvironment.createTimeSkipping()`, so an hour of idle time
passes in a moment. Use it for any timer path.

## Replay

`replay-check` replays every history in `histories/` against today's code. A history that stops
replaying means running sessions would break on upgrade. `record-histories.mts` records new ones.

## Real processes, killed or paused

`faux-worker.mts` is a Worker with the real Activities over a real Pi session and a scripted
model, run as a child process so a check can `SIGKILL` or `SIGSTOP` it mid-Activity.

`seal-check`, `fence-check`, `quarantine-routing-check`, `detached-check`

## Docker

The scripts in `../docker/` run Workers in separate containers, over NFS, and across a container
restart. CI runs `restart-check.sh`. The others need a model key.
