# How it fits together

This repo runs an AI coding agent's turn loop on Temporal. Pi is the agent here, but everything in
`src/core/` works for any agent that implements one small interface. This page is the map. For why
each piece is built the way it is, read [design-decisions.md](design-decisions.md). To put your own
agent on it, read [adapting.md](adapting.md).

## The split

The Workflow holds the control state. The agent's session file holds the conversation. Activities
do the work, and each one opens the session file again, so any Worker that can reach the file can
run the next unit.

```mermaid
flowchart LR
  client["client<br/>CLI, pi extension"]
  wf["piSession Workflow<br/>queue, budgets, step loop"]
  act["Activities<br/>src/core/activities.ts"]
  agent["Agent<br/>src/pi/agent.ts"]
  file[("session file<br/>&lt;id&gt;.jsonl")]
  fence[("&lt;id&gt;.jsonl.fence/")]
  pending[("&lt;id&gt;.jsonl.pending/")]
  tree[("&lt;id&gt;.jsonl.tree/<br/>git bundles")]
  client -- "submit: Update-with-start<br/>waitForQuiet: Update" --> wf
  wf -- "runStep, or runModelCall,<br/>runToolCall x N, sealStep" --> act
  act -- "open, model call,<br/>tool call, seal" --> agent
  agent -- "read and append,<br/>behind the write guard" --> file
  act -- "fence tokens" --> fence
  act -- "dispatch claims,<br/>kept results" --> pending
  act -. "optional: project files" .-> tree
```

The conversation stays out of Workflow history, but not all of it. A prompt's text and a turn's
final answer go into history, in Update, Signal, and Activity payloads, and so does error text.
Tool arguments, tool output, and the model's other responses stay in the session file. That keeps
a long session with large tool outputs inside Temporal's payload and history limits. What does go
into history can hold code or secrets, so set `PI_TEMPORAL_CODEC_KEY` to store it encrypted.

## Reading order

| file | what to look for |
|---|---|
| `src/workflow-bundle.ts` | The two Workflows a Worker registers. |
| `src/core/workflow.ts` | `piSession`. The prompt queue, the `submit` and `waitForQuiet` Updates, `runTurn`, budgets, Continue-As-New, and the idle exit. |
| `src/core/stepped-step.ts` | One step as a model call, a tool call each, and a seal. Host queues, the fallback to the shared queue, and the recovery seal. Has no SDK imports, so `stepped-step-check` runs it with no server. |
| `src/core/activities.ts` | The Activities, for any `Agent`. Fence tokens, dispatch claims, kept results, cancellation, heartbeats. |
| `src/core/agent.ts` | The `Agent` interface. What the Temporal side needs from an agent. |
| `src/core/fence.ts`, `src/core/pending.ts` | How a stale attempt is kept out of the session file, and how a tool runs at most once. |
| `src/core/client.ts`, `src/core/session-worker.ts` | How a prompt is sent, and how a Worker is built. |
| `src/pi/agent.ts` | Pi's `Agent`. The only place that knows Pi's session format. |
| `src/worker.ts`, `src/cli.ts`, `extensions/temporal.ts` | Entry points. The standalone Worker, the CLI, and the pi extension. |

## Who owns what

| state | owner | where |
|---|---|---|
| Which prompts are queued, which turn and step run now | the Workflow | Workflow state, carried across Continue-As-New |
| The conversation, including tool output | the agent | the session file |
| Which Activity may write the session file | the Workflow, by numbering | fence tokens beside the file |
| Whether a tool call started | the first dispatch | a claim file beside the file |
| A tool's result until the seal records it | the tool call's Activity | a result file beside the file |
| The project's files, when Workers are on different hosts | the tree store, optional | git bundles beside the file |
| Session state for listing | the Workflow | the memo, and optionally the `PiSessionState` search attribute |

## Two Workflows

`piSession` is the one to copy. A Worker on any host drives it, and it outlives the client.

`piLocalTurn` wraps one live turn of an open `pi` session. The turn runs in that process's memory,
on a Task Queue only that process polls. Temporal gives it a record, retries, and a stop, but it
can't move the turn to another process. It's Pi UX, kept in `src/pi/`.

## Optional modules

The default path is one Worker, `piSession`, and whole steps (`runStep`). Each of these is
switched on separately and can be deleted if you don't need it.

| module | switch | needed when |
|---|---|---|
| Stepped mode | `PI_TEMPORAL_STEPPED=1` | you want a retry and timeout per tool call |
| Host queues | always on for a Worker with a project directory | tools write a host's local files |
| Tree shipping, `src/tree/` | `PI_TEMPORAL_SHIP_TREE=1` | Workers on different hosts take turns on one project |
| Live turns, `src/pi/local-turn-*` | `PI_TEMPORAL_LIVE_TURNS` | you want a Workflow behind each live `pi` turn |
| Budgets | `PI_TEMPORAL_BUDGET_*` | a turn or session must stop at a bound |
| Schedules, `adoptProject` | `cli.ts schedule` | turns start with no client |
| Payload codec, `src/core/codec.ts` | `PI_TEMPORAL_CODEC_KEY` | history must not hold prompts in the clear |

## Words used here

| word | meaning |
|---|---|
| turn | One prompt, from the first model call to the final answer. |
| step | One model call and the tool calls it asks for. |
| seal | The unit that writes a step's tool results into the session, in the order the model asked. |
| recovery seal | A seal after a stop or a lost host. It records what each tool reported and runs nothing new. |
| dispatch claim | A file a tool call creates before the tool can act. A retry that finds it reports an unknown outcome. |
| kept result | A tool's outcome, held beside the session file until the seal writes it. |
| unknown outcome | What the model is told about a call that may have run and left no result. |
| fence token | A number the Workflow gives each writing Activity. A higher one stops a lower one's writes. |
| host queue | A Task Queue only one Worker polls, so a step's tools and seal run where its files are. |
| tree store | The project's files as git bundles, for Workers on different hosts. |
| lease, epoch | How the tree store keeps one writer at a time, since clients write it outside any Workflow. |
