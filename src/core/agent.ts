// An adapter keeps the agent's session format out of the Temporal core. Pi's adapter lives in
// `src/pi/agent.ts`.
//
// The agent owns the conversation in a session file. The Workflow holds only control state,
// so a turn can move to any Worker that can open that file.

import type { AgentState, Spend } from "./protocol.js";

export type { AgentState };

/** A tool call as the model asked for it. Only these two fields go into Workflow history. */
export interface ToolCallRef {
  readonly id: string;
  readonly name: string;
}

/**
 * What one tool call produced, in the agent's own shape. Kept beside the session file between
 * the tool call and the seal, so it must survive `JSON.stringify`. The core never looks inside.
 */
export type ToolOutcome = unknown;

export interface ModelCall {
  readonly toolCalls: readonly ToolCallRef[];
  // A tool of this step must run alone, after the ones before it.
  readonly sequential: boolean;
  // The response ended the turn's run. Nothing is dispatched, but the step is still sealed.
  readonly ended: boolean;
}

export interface Sealed {
  // The turn is over.
  readonly done: boolean;
  readonly agentState?: AgentState;
}

/**
 * One open session. Every append it makes must call the guard it was opened with first, and stop
 * if the guard throws. That's how a superseded attempt is kept out of the file.
 */
export interface AgentSession {
  /**
   * Settle what a stopped turn left open, before new work. True when the session has work to do,
   * false when the last turn is already answered, and `"busy"` when something else drives it.
   */
  prepareStep(): boolean | "busy";
  /** Whether a prompt with this id is already in the session. */
  hasPrompt(promptId: string): boolean;
  /** Put the prompt in the session without running the model. False when it wasn't recorded. */
  recordPrompt(promptId: string, text: string): Promise<boolean>;
  /** One model call. It records the response and the tool calls it asks for, and runs none. */
  modelCall(signal?: AbortSignal): Promise<ModelCall>;
  /**
   * Run one tool call the latest response recorded. It reports the outcome and writes nothing.
   * Undefined when the session already holds a result for the call. Stops like a user stop when
   * `signal` aborts.
   */
  runToolCall(callId: string, signal?: AbortSignal): Promise<ToolOutcome | undefined>;
  /**
   * Record a step's outcomes, in the order the model asked for the calls, and decide whether the
   * turn goes on. `postRun` false records only: no retry, no compaction, as for a stopped turn.
   * Stops its retry or compaction like a user stop when `signal` aborts.
   */
  sealStep(
    outcomes: readonly ToolOutcome[],
    options: {
      readonly expectCalls: readonly string[];
      readonly agentState?: AgentState;
      readonly postRun: boolean;
      readonly signal?: AbortSignal;
    },
  ): Promise<Sealed>;
  /** Wait for work the session started on its own after a seal, such as a compaction. */
  waitForIdle(): Promise<void>;
  dispose(): void;

  /** Whether the latest response's call `callId` already has a result in the session. */
  answered(callId: string): boolean;
  /** Whether the latest response asked for `callId`. An older response can reuse the id. */
  asked(callId: string): boolean;
  /** Calls in the session that have no result yet. */
  unanswered(): readonly string[];
  /** Whether the latest entry in the session is a model response. */
  endsWithResponse(): boolean;
  /** The text of the latest model response, or empty. */
  lastAnswer(): string;
  /** The session's total spend so far, compacted history included. Undefined when unknown. */
  spend(): Spend | undefined;
  /** The latest entry of this type, skipping the ones `skip` matches. For bookkeeping only. */
  latestEntry(type: string, skip?: (data: unknown) => boolean): unknown;
  /** Append a bookkeeping entry. It goes through the guard like any other append. */
  appendEntry(type: string, data: unknown): void;
}

export interface Agent {
  /** Open the session at `sessionFile`, creating it when it's new. */
  open(sessionFile: string, guard: () => void): Promise<AgentSession>;
  /**
   * The session's record alone, for a bookkeeping entry, without the model. Undefined when the
   * session holds nothing yet.
   */
  openRecord(
    sessionFile: string,
    guard: () => void,
  ): Promise<Pick<AgentSession, "latestEntry" | "appendEntry"> | undefined>;
  /** The outcome for a call that may have run, with no result kept. */
  unknownOutcome(call: ToolCallRef): ToolOutcome;
  /** The outcome for a call nothing started. Unknown would tell the model it may have run. */
  notRunOutcome(call: ToolCallRef): ToolOutcome;
}

/**
 * Where tools write, when that has to travel between hosts. Optional. Without one, every Worker
 * must see the same project directory. `src/tree/store.ts` ships it as git bundles.
 */
export interface ProjectStore {
  /** Make this host's copy current before work runs here. A refusal sends the work elsewhere. */
  ensure(sessionFile: string, writer?: Writer): Promise<void>;
  /** Publish this host's copy after work changed it. */
  capture(sessionFile: string, of?: { current?: Writer; fence?: StepRef }): Promise<void>;
  /** Keep work that can't be published where an operator can find it. */
  setAside(sessionFile: string): Promise<void>;
  /** Mark a tool call as writing here, until `endWrite`. */
  beginWrite(writer: Writer): Promise<void>;
  endWrite(writer: Writer): Promise<void>;
  /** Refuse anything a step closed without its host publishes later. */
  closeStep(sessionFile: string, step: StepRef): Promise<void>;
  /** Hand this host's copy back once the session goes idle. True when it was freed. */
  retire(sessionFile: string): Promise<boolean>;
  /** Copy a template's project into a new session. True when it copied. */
  adopt(template: string, sessionFile: string): Promise<boolean>;
  /** Whether `err` is this host saying no, which another host may answer differently. */
  isRefusal(err: unknown): boolean;
  /** Whether `err` refuses this host for good, until an operator clears it. */
  isQuarantine(err: unknown): boolean;
}

/** A tool call, for telling one step's writers from another's. */
export interface Writer {
  readonly turn: string;
  readonly step: number;
  readonly callId: string;
}

export interface StepRef {
  readonly turn: string;
  readonly step: number;
}
