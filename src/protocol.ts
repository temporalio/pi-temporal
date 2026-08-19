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
}

// A turn that never stops stepping is a bug (a model looping on the same tool, say), and the
// workflow is the only place that can see it. High enough that real work never reaches it.
export const MAX_STEPS_PER_TURN = 200;
