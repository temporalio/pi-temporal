# What survives, and how we know

This page lists what each mode promises when a process dies, a Worker stalls, or a tool outlives
its dispatch, and which check holds each promise. A check covers the scenario it names, not every
interleaving.

## The model

Pi owns the conversation and its JSONL session file. Temporal owns Workflow progress and
dispatches Activities. Worker mode also keeps a few execution facts beside the session file:
dispatch claims and tool results, and with tree shipping, git bundles of the project. Recovery
depends on those agreeing with Temporal history.

There are two modes:

- **Live**: the turn runs inside your `pi` process, with a `piLocalTurn` Workflow per turn.
  Reopening `pi` settles what a crash left behind.
- **Worker**: `/background` and `start` create a session that Workers own. Any Worker on the
  Task Queue can pick up the next unit of work.

Worker mode runs one `runStep` Activity per step: a model call, its tool calls, and a seal. Live
mode runs the whole turn as one `runLocalTurn` Activity. With `PI_TEMPORAL_STEPPED=1`, both use
one Activity per model call, tool call, and seal.

## The rules

**The transcript decides what runs next.** The Workflow only counts steps, so a runaway turn hits
a ceiling. A retry reads the transcript and may find its work already done, so the number of
Activities need not match the number of model steps.

**The seal is the only writer of a step's tool results.** Pi's session file is a tree, and each
entry takes its parent from the leaf its writer last saw. Two calls writing at once would branch
it. So each call reports its result and the seal writes them all, in the order the model asked.

**A tool dispatch writes a claim before the tool can act.** Claims live in
`<session>.jsonl.pending/<turn>/<step>/<call>`. A retry that finds a result returns it. One that
finds only the claim reports an unknown outcome, instead of running a `git push` that may already
have landed. Claims outlive result cleanup, so a stalled attempt that wakes up late can't be
admitted again.

**A Worker writes the transcript only while it holds the session lease.** Every append goes
through `setWriteGuard`. The lease assumes exclusive create and coherent reads on the shared
directory. Its 50-second validity and 60-second reclaim windows allow for clock differences but
don't prove a bound. The guard stops the transcript write. It doesn't stop a tool's external
effects, which is what the claim is for.

**Worker tool calls get 30 minutes per attempt** in stepped mode. Set
`PI_TEMPORAL_TOOL_TIMEOUT_MINUTES` where the session starts to change it. A call that crosses it
isn't run again, since its claim says it started.

**Splitting costs session opens.** Each stepped Activity builds its own `AgentSession`, so a step
with four calls opens the session six times. That's milliseconds against a model call that takes
seconds, but it grows with the transcript.

## What recovers

| What fails | Live | Worker | Checks |
|---|---|---|---|
| Process dies between steps | Sealed entries stay. Reopening recovers. | Completed Activities replay. The retry reads the transcript. | `step-loop-check`, `detached-check` |
| Process dies mid-tool | Unsealed results are lost. Unanswered calls report unknown. | A recovery seal records what it has, a claim without a result reports unknown, and the turn goes on elsewhere. | `pending-check`, `lost-host-check`, `detached-check` |
| A seal dies after its writes | Not applicable | The retry on another Worker doesn't write twice or re-run the turn-end hook. | `seal-check`, `interrupted-seal-check` |
| Two attempts overlap | The agent admits one unit at a time. | The lease and write guard refuse a stale transcript write. They don't fence a tool's effects. | `session-lock-check`, `lock-gap-check`, `stall-check`, `fence-check` |
| A stale dispatch wakes after cleanup | No claims in this mode. | The claim is still there, so it isn't admitted. | `stale-dispatch-check`, `dispatch-check` |
| The user stops a turn | In-memory results are sealed if the process lives. | The unit that started finishes, the next doesn't start, and the step is sealed. | `local-turn-check`, `seal-check`, `l2-step-check` |
| A turn overspends | No bound. | Token, wall-clock, and deadline budgets stop it. The session takes the next prompt. | `budget-check`, `spend-check` |
| History grows | Step ceiling only. | Continue-as-New between turns. One huge turn can still hit limits. | `rollover-check` |
| A deploy changes what a step schedules | Not applicable | Old histories replay under `patched()`. | `replay-check` |

An unknown outcome goes back to the model, which can check the effect and decide. There's no
general resolver for arbitrary commands.

## Following a session

`watch` and `running` query the Workflow and tail the session file. A query has three answers:
the state, gone, or unreachable. Unreachable isn't finished, so a follower keeps waiting through a
Worker restart. A closed run is never read as a live turn, and a turn is over when the Workflow
says so, not when it closes.

## Moving the project

With `PI_TEMPORAL_SHIP_TREE=1`, each step's project tree is captured as a git bundle in
`<session>.jsonl.tree/`, and a host that's behind unbundles and checks out the newest tree before
it runs. A host-local shadow repo does this, so the project's own `.git` is never touched.

- **Only a host standing on the tip may move it.** A host that's behind is refused. Its unshipped
  work goes to `<session>.jsonl.tree/salvage/` as a bundle (`worktree-check`).
- **Only the client establishes the project.** `start --project=` and `/background` send it. A
  Worker never adopts its own directory, since the first Activity lands on whichever Worker is
  free (`worktree-check`).
- **A step stays on the Worker that ran its model call.** Calls that never started move to
  another Worker. If a started call's Worker stops answering, the step is closed without it and
  the turn continues with the next step elsewhere. The closure is written to the shared
  directory, so anything the old host publishes for that step afterwards is refused
  (`lost-host-check`, `migration-rejoin-check`).
- **A directory is refused while a call that outlived its Activity may still write it.** A marker
  records the call's process. The host clears it once nothing is left in its process, process group,
  or cgroup (Linux), nothing carries its exported name, and nothing has its working directory or an
  open file in the directory. A pid reused by a later process doesn't count. A directory the session
  built from empty is moved aside instead, so the stray writer can't reach the next one. `release-
  tree` clears a directory by hand (`quarantine-check`, `quarantine-routing-check`, `docker/restart-
  check.sh`).
- **One session per directory.** A session hands its directory back when it goes idle
  (`worktree-check`, `storage-repair-check`).

The snapshot skips ignored files and ships a nested git checkout as a gitlink. `forget` removes a
session's tree store, salvage included.

## What isn't covered

- Exactly-once effects. A tool result isn't recorded atomically with its effect.
- Shared storage and clocks in general. One NFSv4 setup was tested. NFSv3 and arbitrary clock skew
  weren't.
- A process death between `turn_end`'s entries and the `pi.turn-end-dispatched` entry. No check
  pins that window.
- Live-mode results that were in memory when the process died.

## Running the checks

`npm run checks` runs every check that needs neither a model key nor Docker. Most need a local
Temporal server (`scripts/temporal-dev.sh`). The real-process ones use a scripted model in
`checks/faux-worker.mts`.

Needing more:

- `checks/detached-check.mts` needs a model key. It kills a real Worker mid-tool and asserts
  another one finishes the turn, with the tool run once per time the model asked for it.
- `docker/cross-host-check.sh` and `docker/tree-check.sh` put each Worker in its own container.
  `NFS=1 docker/tree-check.sh` puts the session directory on NFSv4. Both need `OPENAI_API_KEY`.
- `docker/restart-check.sh` and `docker/liveness-check.sh` check the directory markers inside a
  container. CI runs `restart-check.sh`.

To see a session's state, run
`temporal workflow query --workflow-id pi-session-<id> --name turnState`.
