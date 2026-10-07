# What survives, and how we know

A crash can leave a tool’s effect on disk without a saved result. Recovery must account for that
gap before another Worker continues. This page describes each mode’s recovery behavior and links
it to a check. Each check covers its named scenario, not every possible interleaving.

## The model

Pi owns the conversation and its JSONL session file. Temporal owns Workflow progress and
dispatches Activities. Worker mode stores dispatch claims and tool results beside the session
file. Tree shipping adds git bundles of the project. Recovery depends on these records agreeing
with Temporal history.

Recovery depends on where the turn runs.

- Live turns run inside your `pi` process, with a `piLocalTurn` Workflow per turn. Reopen `pi`
  to recover after a crash.
- `/background` and `start` create Worker sessions. A `piSession` Workflow handles each session.
  Workers on its Task Queue can pick up work, subject to the host rules below.

A turn runs from a user prompt to the final response. Each step is one model call and the tool
calls it requests. The seal records the tool results.

Worker mode uses one `runStep` Activity per step by default. Live mode runs the whole turn in one
`runLocalTurn` Activity. With `PI_TEMPORAL_STEPPED=1`, the model call and seal each use an Activity,
and every tool call gets its own Activity.

## The rules

The transcript decides what runs next. The Workflow counts steps and stops a turn at its step
ceiling. A retry may read the transcript and find its work complete. Activity attempts therefore
don’t map one-to-one to model steps.

The seal alone writes a step’s tool results to the transcript. Pi’s session file is a tree.
Each entry takes its parent from the leaf its writer last saw, so concurrent writes would create
branches. Calls return their results to the seal, which writes them in the model’s request order.

A tool dispatch writes a claim before the tool can act. Claims live in
`<session>.jsonl.pending/<turn>/<step>/<call>`. A retry that finds a result returns it. One that
finds only the claim reports an unknown outcome, instead of running a `git push` that may already
have landed. Claims outlive result cleanup, so a stalled attempt that wakes up late can’t be
admitted again.

A Worker may append to the transcript only while it holds the session lease. Every append goes
through `setWriteGuard`. The lease assumes exclusive create and coherent reads on the shared
directory. Its 50-second validity and 60-second reclaim windows allow for clock differences but
don’t prove a bound. The guard stops the transcript write. It doesn’t stop a tool’s external
effects, which is what the claim is for.

The last released epoch stays on disk with an expired timestamp, so epochs only grow. A claim
counts only while no newer epoch exists, so a contender that paused and recreated an old epoch
backs off. A failed renewal doesn’t extend write permission. A failed read of a lease, a claim, a
kept result, or a tree record doesn’t admit a writer.

The lock directories outlive the session, `forget` included. They are `<session>.jsonl.lock`,
`<session>.jsonl.tree/writers.lock`, and the host-local `trees/<hash>/tree.lock`. Each keeps one
expired claim. Don’t delete them in cleanup scripts, or an epoch can start again from one.

Stepped Worker tool calls have a 30-minute timeout per attempt by default.
`PI_TEMPORAL_TOOL_TIMEOUT_MINUTES` changes it, for host-pinned calls too. A host-pinned call’s
total also has room for its queue wait. A timed-out call with a dispatch claim isn’t repeated,
because its effects may already have happened.

Each stepped Activity opens its own `AgentSession`. A step with four tool calls opens the session
six times. Session-open time grows with the transcript, so splitting a step adds work to each
model response.

## What recovers

| What fails | Live | Worker | Checks |
|---|---|---|---|
| Process dies between steps | Sealed entries stay. Reopening recovers. | Completed Activities replay. The retry reads the transcript. | `step-loop-check`, `detached-check` |
| Process dies mid-tool | Unsealed results are lost. Unanswered calls report unknown. | A recovery seal records what it has, a claim without a result reports unknown, and the turn goes on elsewhere. | `pending-check`, `lost-host-check`, `detached-check` |
| A seal dies after its writes | Not applicable | The retry on another Worker doesn’t write twice or re-run the turn-end hook. | `seal-check`, `interrupted-seal-check` |
| Two attempts overlap | The agent admits one unit at a time. | The lease and write guard refuse a stale transcript write. They don’t fence a tool’s effects. | `session-lock-check`, `lock-gap-check`, `stall-check`, `fence-check` |
| A stale dispatch wakes after cleanup | No claims in this mode. | The claim is still there, so it isn’t admitted. | `stale-dispatch-check`, `dispatch-check` |
| The user stops a turn | In-memory results are sealed if the process lives. | The unit that started finishes, the next doesn’t start, and the step is sealed. | `local-turn-check`, `seal-check`, `l2-step-check` |
| A turn overspends | No bound. | Soft budgets stop at a boundary. The hard deadline cancels waiting. External commands may continue. The session takes the next prompt. | `budget-check`, `spend-check` |
| History grows | Step ceiling only. | Continue-as-New between turns. One huge turn can still hit limits. | `rollover-check` |
| A deploy changes what a step schedules | Not applicable | Old histories replay under `patched()`. | `replay-check` |

The model receives the unknown outcome and can inspect the effect before deciding what to do.
There’s no general resolver for arbitrary commands. A later model request is a new dispatch,
so the model can still choose to repeat an action.

## Following a session

`watch` and `running` query the Workflow and tail the session file. A query returns the current
state or reports that the run is gone. If the query can’t reach a Worker, the follower keeps
waiting through the restart. It doesn’t treat an unreachable run as finished.

A closed run isn’t read as a live turn. A session Workflow can stay open between turns, so the
follower uses the reported turn state to decide when a turn has ended.

## Moving the project

With `PI_TEMPORAL_SHIP_TREE=1`, a shadow git repository captures each step’s project tree as a
bundle in `<session>.jsonl.tree/`. A host restores the latest tree before running work. The
shadow repository is local to the host and leaves the project’s own `.git` unchanged.

- Only a host at the current tree tip may publish the next snapshot. A host that’s behind saves
  its unshipped work in `<session>.jsonl.tree/salvage/` as a bundle (`worktree-check`).
- The client establishes the project through `start --project=` or `/background`. A
  Worker never adopts its own directory, since the first Activity lands on whichever Worker is
  free (`worktree-check`).
- A step stays on the Worker that ran its model call. Calls that never started can move to
  another Worker. If a started call’s Worker stops answering, the step is closed without it and
  the turn continues with the next step elsewhere. The closure is written to the shared
  directory, so anything the old host publishes for that step afterwards is refused
  (`lost-host-check`, `migration-rejoin-check`). A step the user stops is closed to its host
  the same way. The closure stays until `forget` removes the session’s tree store. A timed-out
  attempt has no lifetime bound, so later turns can’t make its closure safe to drop.
- A directory is refused while a call that outlived its Activity may still write it. A marker
  records the call’s process. The host clears it only after checking the process and its process
  group, plus the cgroup on Linux. It also checks for processes with the exported name, working
  directory, or open files in the directory. A reused PID doesn’t count as the original process.
  A live call touches its marker every 10 seconds. A marker from another PID namespace, e.g.
  before a container restart, counts as live until it goes quiet for 40 seconds. Children that
  outlive such a call are not seen from the new namespace.
  A directory the session built from empty can be moved aside and rebuilt. That separates writes
  through inherited working directories or open handles. A command that uses an absolute path
  can still reach the replacement directory. `release-tree` clears a directory by hand
  (`quarantine-check`, `quarantine-routing-check`, `docker/restart-check.sh`).
- One session owns each directory. It hands the directory back when it goes idle
  (`worktree-check`, `storage-repair-check`). It keeps the directory while a writer marker is
  live, even when the files match the last snapshot, since a tool can write after its turn ends.

The snapshot skips ignored files and ships a nested git checkout as a gitlink. `forget` removes a
session’s tree store, salvage included.

## What isn’t covered

- Exactly-once effects. A tool result isn’t recorded atomically with its effect.
- Shared storage and clocks in general. One NFSv4 setup was tested. NFSv3 and arbitrary clock skew
  weren’t.
- A process death between `turn_end`’s entries and the `pi.turn-end-dispatched` entry. No check
  pins that window.
- Live-mode results that were in memory when the process died.
- A tool still running on a Worker inside `pi` when you quit. `pi` waits a few seconds, then
  abandons it, and its outcome is reported unknown.

## Running the checks

`npm run checks` runs every check that needs neither a model key nor Docker. Most need a local
Temporal server (`scripts/temporal-dev.sh`). The real-process ones use a scripted model in
`checks/faux-worker.mts`.

These checks need a model key or Docker.

- `checks/detached-check.mts` needs a model key. It kills a real Worker mid-tool and asserts
  another one finishes the turn, with the tool run once per time the model asked for it.
- `docker/cross-host-check.sh` and `docker/tree-check.sh` put each Worker in its own container.
  `NFS=1 docker/tree-check.sh` puts the session directory on NFSv4. Both need `OPENAI_API_KEY`.
- `docker/restart-check.sh` and `docker/liveness-check.sh` check the directory markers inside a
  container. CI runs `restart-check.sh`.

To query a session’s state, run
`temporal workflow query --workflow-id pi-session-<id> --name turnState`.
