// Names and shapes shared by the workflow, the client, and (type-only) the activities.
// Keep this import-free of the Pi SDK and Node builtins: the workflow bundles it into the
// Temporal sandbox.

export const WORKFLOW_TYPE = "piSession";
export const WORKFLOW_ID_PREFIX = "pi-session-";
// The failure type of a tool call that stopped before any attempt claimed it, so the tool cannot
// have started and the workflow may move a pinned step instead of closing the call as unknown.
export const FAILED_BEFORE_CLAIM = "FailedBeforeClaim";
// The failure type of a live tool call that failed because the user stopped the turn. The workflow
// treats it as the stop it is, not as a call that failed on its own.
export const TURN_STOPPED = "TurnStopped";

export const workflowId = (sessionId: string) => `${WORKFLOW_ID_PREFIX}${sessionId}`;

// wake carries the new prompt as a pointer (its id) plus the text. The text is small, so it can
// ride the workflow input; the large conversation state stays in Pi's session file.
export const SIGNALS = {
  submitPrompt: "submitPrompt",
  interrupt: "interrupt",
} as const;

export const QUERIES = {
  turnState: "turnState",
} as const;

// What the session is doing, for anyone watching from outside: the pi extension, or a person
// with the Temporal CLI. The conversation itself is in the session file, not here.
export interface TurnState {
  // Prompts accepted but not started.
  readonly queued: number;
  // The turn being driven right now, and which step it is on.
  readonly running?: { readonly promptId: string; readonly step: number };
  // The last turn to stop, and why it stopped.
  readonly finished?: {
    readonly promptId: string;
    // "interrupted" is the user pressing stop. A turn that died for any other reason is "failed",
    // because reporting a crash as a stop tells whoever is watching that they did it.
    // "budget" is the operator's bound reached, which is neither a failure nor somebody pressing
    // stop: the turn is left where it got to and the session takes the next prompt.
    readonly outcome: "answered" | "interrupted" | "failed" | "ceiling" | "budget";
    // What the session has spent by the time this turn ended, for a client that wants to show it
    // or an operator asking why a turn stopped.
    readonly spent?: Spent;
    // What went wrong, when something did. The session log holds the detail; this is for a
    // client that is only reading turnState.
    readonly error?: string;
    readonly finalText: string;
  };
}

export interface PromptInput {
  // Deterministic id for this prompt, so a re-driven activity can tell whether it already ran.
  readonly promptId: string;
  readonly text: string;
}

export interface RunStepInput extends PromptInput {
  // How many attempts of this step already failed. The workflow keeps it because the session that
  // would otherwise count it is rebuilt per activity, and the transcript it could be read off is
  // something a compaction rewrites.
  readonly retryAttempt?: number;
  readonly sessionId: string;
  // Absolute path to the Pi session JSONL. This file is the durable log; the activity opens it,
  // appends what the step produced, and it survives across workers on shared storage.
  readonly sessionFile: string;
  // Which step of the turn the workflow thinks this is. Nothing reads it: the transcript decides
  // what runs next. It rides along so a step is legible in the Temporal UI and in worker logs.
  readonly step: number;
}

/** What one unit of work cost, as the session's own accounting reports it. Reported per activity
 * rather than as a running total, because the workflow is what adds them up: the session file is
 * shared, so a total read from it includes turns this one did not run. */
export interface Spend {
  readonly tokens: number;
  // What the session priced those tokens at. Reported where the host knows it, so an operator
  // reading a stopped turn sees the number they care about rather than a proxy for it.
  readonly cost?: number;
}

export interface RunStepResult {
  // Whether the turn is finished. False means the workflow schedules another step.
  readonly done: boolean;
  // What the step's own retry budget is up to, for the workflow to carry into the next one.
  // Required, because a step that quietly forgets to report it takes the cap with it.
  readonly retryAttempt: number;
  // The assistant's final text, once the turn is done (the log is the truth; this is a courtesy).
  readonly finalText: string;
  // What this step spent, for the turn's budget. Absent when the session cannot say.
  readonly spent?: Spend;
  // And what the session has been billed in total, which is what the session's own bound is about.
  // Read off the record rather than added up here, so it survives everything the workflow does not:
  // a run that rolled over, a session that went idle and was woken again, a turn some other client
  // ran against the same file. Absent when the session cannot say.
  readonly total?: Spend;
}

export interface SessionTurnOptions {
  // How long the workflow stays alive with no work before it self-terminates. The next prompt
  // starts a fresh run that rebuilds nothing (the session file already holds the conversation).
  readonly idleTimeout?: string;
  // Drive each step as a model call, one activity per tool call, and a seal, instead of one
  // activity for the whole step. Off by default: the whole-step mode is what runs today.
  readonly stepped?: boolean;
  // A task the workflow already has when it starts, so a turn can begin with nothing running that
  // could have sent it: a Temporal schedule, or anything else that can start a workflow.
  readonly initialPrompt?: PromptInput;
  // Where to keep the session log when the id is derived rather than given. A scheduled start has
  // no client to choose either, and the workflow may not read the environment.
  readonly sessionDir?: string;
  // A project some client sent once, for a session that has nobody to send it. Every firing of a
  // schedule is its own session, so each takes a copy of this store before its first activity.
  readonly template?: string;
  // What a run that rolled over was still holding. The queue is the whole of the control state, so
  // handing it to the next run is what makes the rollover invisible to a client.
  readonly queued?: readonly PromptInput[];
  // What the session has spent, carried across a rollover for the same reason the queue is: a
  // session that rolled over has not started again.
  readonly spent?: Spent;
  // What the last turn came to, carried across a rollover. A client polling `turnState` for its own
  // prompt (the extension's `/background` does) otherwise never sees the answer: the new run starts
  // with nothing finished, and the watcher gives up when the session retires.
  readonly finished?: TurnState["finished"];
  // What one turn may spend before the workflow stops driving it. Off unless an operator sets it:
  // a bound that ends real work is worse than none, and only the operator knows which is which.
  // The step ceiling above is not this. It is a runaway guard with a fixed number; this is policy.
  readonly budget?: TurnBudget;
  // Roll over at this many history events, on top of the server's own suggestion. The suggestion is
  // what production runs on; this is for an operator who wants a tighter bound, and it is what
  // makes the rollover reachable in a check.
  readonly maxHistory?: number;
  // The bound on one tool call in stepped mode. A call that crosses it is not run again, because
  // its dispatch note says it started, so this is the longest any tool may take.
  readonly toolTimeoutMinutes?: number;
}

// A call the model asked for, recorded but not run. The arguments stay in the transcript: the
// workflow needs a name to report and an id to dispatch, and history is no place for a file.
export interface DeferredToolCall {
  readonly id: string;
  readonly name: string;
}

export interface ModelCallResult {
  // The step was over before it started, so nothing is dispatched and nothing is sealed. That is
  // a retry landing after the last step of the turn finished.
  readonly settled?: RunStepResult;
  readonly calls: readonly DeferredToolCall[];
  // The calls have to run one at a time, because a tool of this step says so.
  readonly sequential: boolean;
  // The response ended the run on its own. Nothing is dispatched, but the step is still sealed:
  // what answers for a failed model call (a retry, a compaction) happens there.
  readonly ended: boolean;
  // The queue this worker polls on its own. The rest of the step is addressed there, because this
  // is the host holding the directory the tools are about to write, which is what lets them run at
  // once. Absent when the worker has none, and then the step runs on the shared queue as before.
  //
  // Falling back is narrow on purpose: only a dispatch nobody started may move, and only once every
  // pinned sibling has settled. An attempt that started and failed may still have a tool writing
  // that directory, and nothing here can see it.
  readonly queue?: string;
  // What the model call spent. The tools and the seal cost nothing at the provider unless the seal
  // compacts, which is a model call of its own and reports its own.
  readonly spent?: Spend;
  // What the session has been billed in total, as the record holds it. See `RunStepResult`.
  readonly total?: Spend;
}

export interface ToolCallInput {
  readonly sessionId: string;
  readonly sessionFile: string;
  // The turn this call belongs to, which is the prompt's id. What a dispatch knows about a call is
  // kept under it, and a turn numbers its steps from one again, so without it a step of the next
  // turn reads the step before it as its own.
  readonly turn: string;
  readonly step: number;
  readonly call: DeferredToolCall;
}

// How a dispatch ended. It is the dispatch's own account of itself: the transcript says what the
// model was told, not whether the tool was skipped or its result was lost.
// - `settled`: the tool ran and its result is durable.
// - `already-settled`: a result was already recorded, so nothing ran. The at-least-once case.
// - `unknown`: a dispatch had started when this one began, so the tool can have taken effect.
//   Reported to the model as an unknown outcome rather than run a second time.
export type ToolCallOutcome = "settled" | "already-settled" | "unknown";

export interface ToolCallResult {
  readonly outcome: ToolCallOutcome;
}

export interface SealStepInput {
  readonly sessionId: string;
  readonly sessionFile: string;
  // Which turn's kept results to read. See `ToolCallInput`.
  readonly turn: string;
  readonly step: number;
  readonly retryAttempt?: number;
  // The turn was stopped, so record the results and nothing else. A provider retry or a
  // compaction on the way out is work nobody asked for, and the interrupt waits for it.
  readonly interrupted?: boolean;
  // This step is being closed without the host that was running it, which is not the same as being
  // stopped: nobody asked for it to end, and a tool over there may still be inside its execution.
  // Recorded where every host reads it, so what that one produces afterwards is kept rather than
  // published over the step that replaces this one.
  readonly lost?: boolean;
  // The step's calls, in the order the model asked for them. A call with no result is settled as
  // an unknown outcome, because a step that leaves one unanswered leaves a transcript no
  // provider accepts.
  readonly calls: readonly DeferredToolCall[];
}

/**
 * What a turn may spend. Enforced between steps, never inside one: a step that has started is left
 * to finish, because stopping it would leave a tool call the transcript cannot be made from.
 *
 * This is what the workflow having the loop buys. The agent is not asked to keep to it, and cannot
 * be: the model decides what to ask for, and the thing that says no has to be somewhere the model
 * does not reach.
 */
export interface TurnBudget {
  // Wall clock for the whole turn, in seconds, measured with the workflow's own clock so it reads
  // the same on a replay. A number rather than a duration string like the timeouts above it: the
  // parser for those lives outside what a workflow bundle should be reaching for, and a budget
  // nobody can be sure of the units of is worse than one that says them.
  readonly seconds?: number;
  // Tokens the turn's model calls may spend, added up as each one reports.
  readonly tokens?: number;
  // Wall clock again, but as a deadline rather than a bound checked between units of work. At
  // `seconds` the turn stops where it can; at this one it stops where it is, which is what a user
  // pressing stop does: the calls that have results keep them, the ones in flight come back to the
  // model as outcomes nobody can vouch for, and the tools over there keep running until they are
  // done. Nothing else here abandons work, so this is opt-in on top of the bound above.
  readonly hardSeconds?: number;
  // And the same two for the session, across every turn it runs. A per-turn bound says what one
  // answer may cost; this says what the whole session may, which is the number somebody is billed
  // for. Measured against what the session's own record says it has been billed, where the host
  // reports that, so it survives a rollover, an idle retirement, and anything else that runs the
  // session. Where it does not, the workflow's own count is used and carried across a rollover.
  readonly sessionSeconds?: number;
  readonly sessionTokens?: number;
}

/** What a session has spent so far, carried between runs of its workflow. */
export interface Spent {
  readonly tokens: number;
  readonly cost: number;
  // Wall clock the session's turns have used, rather than how long the session has existed: a
  // session sitting idle overnight has spent nothing.
  readonly seconds: number;
}

// A turn that never stops stepping is a bug (a model looping on the same tool, say), and the
// workflow is the only place that can see it. High enough that real work never reaches it.
export const MAX_STEPS_PER_TURN = 200;

// One workflow per turn of a live pi session, for the turn executor. The turn runs in the pi
// process that owns it, so this is a record and a retry policy around that turn, not a way to
// move it somewhere else.
export const LOCAL_TURN_WORKFLOW = "piLocalTurn";

export interface LocalTurnInput {
  readonly sessionId: string;
  // Identifies the live turn inside the process that owns it.
  readonly turnId: string;
  // The queue that process is polling. One queue per process, because only that process can run
  // this turn.
  readonly taskQueue: string;
  // Drive the turn a step at a time, so a tool call of a live session is its own unit of work.
  readonly stepped?: boolean;
}

// The turn a live process holds, addressed by the turn rather than by a session file: the
// transcript is in memory over there, and only that process can reach it.
export interface LocalStepInput {
  readonly turnId: string;
  readonly step: number;
}

export interface LocalToolCallInput extends LocalStepInput {
  readonly call: DeferredToolCall;
}

export interface LocalSealInput extends LocalStepInput {
  readonly calls: readonly DeferredToolCall[];
  readonly interrupted?: boolean;
}

export interface LocalModelCallResult {
  readonly calls: readonly DeferredToolCall[];
  readonly sequential: boolean;
  readonly ended: boolean;
  // The user stopped the turn. Nothing was asked of the model, and the step must not be sealed:
  // the last one already closed, and closing it again ends a turn that is over twice.
  readonly interrupted?: boolean;
}
