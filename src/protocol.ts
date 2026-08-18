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

export interface RunPromptInput extends PromptInput {
  readonly sessionId: string;
  // Absolute path to the Pi session JSONL. This file is the durable log; the activity opens it,
  // appends the turn, and it survives across workers on shared storage.
  readonly sessionFile: string;
}

export interface RunPromptResult {
  // Whether this call actually drove a turn, or short-circuited because the prompt was already
  // applied and completed (idempotent re-drive).
  readonly ran: boolean;
  // The assistant's final text for this turn, for the caller's convenience (the log is the truth).
  readonly finalText: string;
}

export interface SessionTurnOptions {
  // How long the workflow stays alive with no work before it self-terminates. The next prompt
  // starts a fresh run that rebuilds nothing (the session file already holds the conversation).
  readonly idleTimeout?: string;
}
