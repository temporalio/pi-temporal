// Names and shapes shared by the workflow, the client, and (type-only) the activities.
// Keep this import-free of the Pi SDK and Node builtins: the workflow bundles it into the
// Temporal sandbox.

export const WORKFLOW_TYPE = "piSession";
export const WORKFLOW_ID_PREFIX = "pi-session-";

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
    readonly outcome: "answered" | "interrupted" | "ceiling";
    readonly finalText: string;
  };
}

export interface PromptInput {
  // Deterministic id for this prompt, so a re-driven activity can tell whether it already ran.
  readonly promptId: string;
  readonly text: string;
}

export interface RunStepInput extends PromptInput {
  readonly sessionId: string;
  // Absolute path to the Pi session JSONL. This file is the durable log; the activity opens it,
  // appends what the step produced, and it survives across workers on shared storage.
  readonly sessionFile: string;
  // Which step of the turn the workflow thinks this is. Nothing reads it: the transcript decides
  // what runs next. It rides along so a step is legible in the Temporal UI and in worker logs.
  readonly step: number;
}

export interface RunStepResult {
  // Whether the turn is finished. False means the workflow schedules another step.
  readonly done: boolean;
  // The assistant's final text, once the turn is done (the log is the truth; this is a courtesy).
  readonly finalText: string;
}

export interface SessionTurnOptions {
  // How long the workflow stays alive with no work before it self-terminates. The next prompt
  // starts a fresh run that rebuilds nothing (the session file already holds the conversation).
  readonly idleTimeout?: string;
  // Drive each step as a model call, one activity per tool call, and a seal, instead of one
  // activity for the whole step. Off by default: the whole-step mode is what runs today.
  readonly stepped?: boolean;
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
}

export interface ToolCallInput {
  readonly sessionId: string;
  readonly sessionFile: string;
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
  readonly step: number;
  // The step's calls, in the order the model asked for them. A call with no result is settled as
  // an unknown outcome, because a step that leaves one unanswered leaves a transcript no
  // provider accepts.
  readonly calls: readonly DeferredToolCall[];
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
}

export interface LocalModelCallResult {
  readonly calls: readonly DeferredToolCall[];
  readonly sequential: boolean;
  readonly ended: boolean;
  // The user stopped the turn. Nothing was asked of the model, and the step must not be sealed:
  // the last one already closed, and closing it again ends a turn that is over twice.
  readonly interrupted?: boolean;
}
