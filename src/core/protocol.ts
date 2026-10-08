// The Workflow bundles these shared types into the Temporal sandbox. Pi SDK and Node imports
// would make this module unsafe to load there.

export const WORKFLOW_TYPE = "piSession";
export const WORKFLOW_ID_PREFIX = "pi-session-";
// A tool call that failed before any attempt claimed it, so the tool never started and the
// Workflow may move a host-queue step.
export const FAILED_BEFORE_CLAIM = "FailedBeforeClaim";
/**
 * Where a run's fences start. A run that started no later than the one it continued from counts
 * on from that run's last fence, so its fences still sort after the old run's.
 */
export const fenceStart = (
  runStartMs: number,
  after?: { readonly ms: number; readonly seq: number },
): { ms: number; seq: number } =>
  after && after.ms >= runStartMs ? { ms: after.ms, seq: after.seq } : { ms: runStartMs, seq: 0 };

/** The Workflow's half of a fence: its run, and the Activity it is about to schedule. */
export const fencePrefix = (runStartMs: number, seq: number) =>
  `${String(runStartMs).padStart(13, "0")}.${String(seq).padStart(8, "0")}`;

// The keyword search attribute a session keeps its state in when `searchAttribute` is on.
export const SESSION_STATE_ATTRIBUTE = "PiSessionState";

// The memo key a session keeps its state under, `{ state: "running" | "idle", queued }`.
export const SESSION_MEMO = "piSession";

// A tool call that failed once its claim was taken. Not retried, since every retry would find the
// claim and only report an unknown outcome. History shows the failure, and the seal records it.
export const FAILED_AFTER_CLAIM = "FailedAfterClaim";
// A live tool call that failed because the user stopped the turn.
export const TURN_STOPPED = "TurnStopped";

export const workflowId = (sessionId: string) => `${WORKFLOW_ID_PREFIX}${sessionId}`;

/**
 * Why a session id can't name its log file, or undefined. The id becomes a file name in the
 * session directory, so it must stay one name there. Plain code, since the Workflow calls it too.
 */
export const sessionIdProblem = (sessionId: string): string | undefined => {
  if (!sessionId || sessionId === "." || sessionId === "..") return "it is empty or a dot name";
  if (/[/\\\0]/.test(sessionId)) return "it holds a path separator or NUL";
  // Room for the longest suffix a session's files get, within a 255-byte file name.
  if (utf8Length(sessionId) > 200) return "it is longer than 200 bytes";
  return undefined;
};

// Counted by hand, because the Workflow's sandbox has no `Buffer`.
const utf8Length = (text: string) => {
  let bytes = 0;
  for (const char of text) {
    const code = char.codePointAt(0) ?? 0;
    bytes += code < 0x80 ? 1 : code < 0x800 ? 2 : code < 0x10000 ? 3 : 4;
  }
  return bytes;
};

export const SIGNALS = {
  submitPrompt: "submitPrompt",
  interrupt: "interrupt",
} as const;

export const QUERIES = {
  turnState: "turnState",
} as const;

export const UPDATES = {
  // Queue a prompt. Rejected when it's empty or the session already has it.
  submit: "submit",
  // Resolves once nothing is running or queued, or when the run hands over to a new one.
  waitForQuiet: "waitForQuiet",
} as const;

// The failure type `submit` rejects a prompt with when the session already has it. A client that
// retried after a lost answer reads it as accepted.
export const DUPLICATE_PROMPT = "DuplicatePrompt";

export interface Submitted {
  // Prompts ahead of this one, the running turn included.
  readonly ahead: number;
}

export interface Quiet {
  // The run moved to a new one through Continue-As-New. Ask the session again.
  readonly moved?: boolean;
  readonly finished?: TurnState["finished"];
}

// What the session is doing, for outside watchers. The conversation itself is in the session file.
export interface TurnState {
  // Prompts accepted but not started.
  readonly queued: number;
  readonly running?: { readonly promptId: string; readonly step: number };
  // The last turn to stop, and why.
  readonly finished?: {
    readonly promptId: string;
    // "interrupted" means the user pressed stop. Any other death is "failed". "budget" means the
    // operator's bound was reached, and the session moves on to the next prompt.
    readonly outcome: "answered" | "interrupted" | "failed" | "ceiling" | "budget";
    // Session spend when this turn ended.
    readonly spent?: Spent;
    // Short error for clients that only read `turnState`. The session log has the detail.
    readonly error?: string;
    readonly finalText: string;
  };
}

export interface PromptInput {
  // Deterministic id, so a re-driven activity can tell whether it already ran.
  readonly promptId: string;
  readonly text: string;
}

export interface RunStepInput extends PromptInput {
  // Orders this Activity's writes to the session file against every other one. See `fence.ts`.
  readonly fence?: string;
  // The agent's own state from the step before. See `AgentState`.
  readonly agentState?: AgentState;
  // The session's turn time when this step was scheduled. Each step writes its total to the
  // session record, so a run woken after an idle exit still counts the earlier turns.
  readonly sessionSeconds?: number;
  readonly sessionId: string;
  // Absolute path to the Pi session JSONL, the durable log. Shared storage across workers.
  readonly sessionFile: string;
  // Keys the step's dispatch claims, kept results, and closure. The transcript still decides what
  // runs next.
  readonly step: number;
}

/**
 * The agent's own state between steps, such as a retry count. The Workflow carries it from each
 * step's result into the next step's input, since a session is opened again for every Activity.
 * Scoped to one turn.
 */
export type AgentState = Readonly<Record<string, unknown>>;

/** What one unit of work cost. Per activity, not a running total, because the session file is
 * shared and a total read from it includes turns this one did not run. */
export interface Spend {
  readonly tokens: number;
  // Present where the host knows the price.
  readonly cost?: number;
}

export interface RunStepResult {
  // False means the workflow schedules another step.
  readonly done: boolean;
  // For the next step. A result with none keeps what an earlier step reported.
  readonly agentState?: AgentState;
  // The assistant's final text once done. The log is the source of truth.
  readonly finalText: string;
  // This step's spend, for the turn budget.
  readonly spent?: Spend;
  // Session total read off the record, so it survives Continue-As-New, idle restarts, and other
  // clients.
  readonly total?: Spend;
  // The session's turn time so far, as the session record keeps it.
  readonly sessionSeconds?: number;
  // The host queue of the Worker that ran this step's tools, when it has one.
  readonly hostQueue?: string;
}

/**
 * How a session Workflow starts. One object, so a field can be added without breaking a running
 * session's history or a client built against an older shape.
 */
export interface SessionInput extends SessionTurnOptions {
  // Empty for a scheduled firing, which takes its id from its Workflow ID.
  readonly sessionId?: string;
  // Defaults to `<sessionDir>/<sessionId>.jsonl`.
  readonly sessionFile?: string;
}

export interface SessionTurnOptions {
  // Idle time before the workflow exits. The next prompt starts a fresh run from the session file.
  readonly idleTimeout?: string;
  // Drive each step as a model call, one activity per tool call, and a seal, instead of one
  // activity for the whole step. Off by default.
  readonly stepped?: boolean;
  // A prompt to run at start, for starters with no client, such as a Temporal schedule.
  readonly initialPrompt?: PromptInput;
  // Session log location when the id is derived. The workflow cannot read the environment.
  readonly sessionDir?: string;
  // A project store copied into each scheduled session before its first activity.
  readonly template?: string;
  // Queue carried across Continue-As-New. It is the whole control state.
  readonly queued?: readonly PromptInput[];
  // Spend carried across Continue-As-New.
  readonly spent?: Spent;
  // Last turn result carried across Continue-As-New, so a client polling `turnState` still sees it.
  readonly finished?: TurnState["finished"];
  // Prompt ids the session has taken, newest last, so a resent prompt isn't run twice. Carried
  // across Continue-As-New.
  readonly seenPrompts?: readonly string[];
  // The last fence the run before this one handed out, carried across Continue-As-New. A new run
  // can start in the same millisecond, and its fences must still sort after the old run's.
  readonly fencedAfter?: { readonly ms: number; readonly seq: number };
  // Host queues of the Workers that held this session's project, carried across Continue-As-New.
  // Each one is asked to hand its directory back when the session goes idle.
  readonly hostQueues?: readonly string[];
  // Operator spend bound per turn. Off by default, since a bound that ends real work is a policy
  // call. `MAX_STEPS_PER_TURN` is a separate runaway guard.
  readonly budget?: TurnBudget;
  // Continue-As-New at this many history events, in addition to the server's suggestion. Also lets
  // checks reach that path.
  readonly maxHistory?: number;
  // Also keep the session's state in the `PiSessionState` search attribute, so a List call can
  // filter on it. The namespace must have the attribute registered, so it's off by default.
  readonly searchAttribute?: boolean;
  // Bound on one tool call in stepped mode. A call that crosses it is not re-run, because its
  // dispatch claim says it started.
  readonly toolTimeoutMinutes?: number;
}

// A call the model asked for, recorded but not run. Arguments stay in the transcript, not history.
export interface DeferredToolCall {
  readonly id: string;
  readonly name: string;
}

export interface ModelCallResult {
  // A retry that landed after the turn's last step finished. Nothing is dispatched or sealed.
  readonly settled?: RunStepResult;
  readonly calls: readonly DeferredToolCall[];
  // A tool of this step requires sequential execution.
  readonly sequential: boolean;
  // The response ended the run. Nothing is dispatched, but the step is still sealed, since retries
  // and compaction happen there.
  readonly ended: boolean;
  // This worker's own queue. The rest of the step runs there, on the host that holds the project
  // directory. Absent when the worker has none, and the step uses the shared queue.
  //
  // Only a dispatch nobody started may move off it, and only after every sibling on the host queue
  // settled. A started attempt may still have a tool writing that directory.
  readonly queue?: string;
  // The model call's spend. A compacting seal reports its own.
  readonly spent?: Spend;
  // Session total from the record. See `RunStepResult`.
  readonly total?: Spend;
  // The session's turn time so far, as the session record keeps it.
  readonly sessionSeconds?: number;
}

export interface ToolCallInput {
  readonly sessionId: string;
  readonly sessionFile: string;
  // The prompt id. Steps restart at one each turn, so dispatch state is keyed by turn too.
  readonly turn: string;
  readonly step: number;
  readonly call: DeferredToolCall;
}

// How a dispatch ended. The transcript can't tell a skipped tool from a lost result, so this does.
// - `settled`: the tool ran and its result is durable.
// - `already-settled`: a result was already recorded, so nothing ran. The at-least-once case.
// - `unknown`: an earlier dispatch had started, so the tool may have taken effect. Reported to the
//   model as an unknown outcome rather than run twice.
export type ToolCallOutcome = "settled" | "already-settled" | "unknown";

export interface ToolCallResult {
  readonly outcome: ToolCallOutcome;
}

/** What an idle session leaves behind when its run exits. */
export interface RetireInput {
  readonly sessionFile: string;
  // Orders this Activity's writes to the session file against every other one. See `fence.ts`.
  readonly fence?: string;
  // The last turn and the session's time after it, as the Workflow counted. Absent when no turn
  // ran in this run or the one it continued from.
  readonly turn?: string;
  readonly sessionSeconds?: number;
}

export interface SealStepInput {
  // Orders this Activity's writes to the session file against every other one. See `fence.ts`.
  readonly fence?: string;
  readonly sessionId: string;
  readonly sessionFile: string;
  // Which turn's kept results to read. See `ToolCallInput`.
  readonly turn: string;
  readonly step: number;
  readonly agentState?: AgentState;
  readonly sessionSeconds?: number;
  // The turn was stopped. Record results only, with no provider retry or compaction.
  readonly interrupted?: boolean;
  // Closed without the host that ran it, for the logs. With tree shipping on, any interrupted seal
  // closes the step to its host, since a tool there may still be running.
  readonly lost?: boolean;
  // In the model's order. Every call gets a result, because providers reject a transcript with an
  // unanswered call. One with no kept result is unknown, or not run if the seal claims it first.
  readonly calls: readonly DeferredToolCall[];
}

/**
 * What a turn may spend. The workflow enforces it, since the model can't be trusted to. Most bounds
 * stop the turn between units of work. `hardSeconds` stops it at once.
 */
export interface TurnBudget {
  // Wall clock for the turn in seconds, on the workflow clock so replay agrees. A number, not a
  // duration string, because the duration parser is not workflow-safe.
  readonly seconds?: number;
  // Tokens the turn's model calls may spend.
  readonly tokens?: number;
  // A hard deadline. At `seconds` the turn stops between steps. At this one it stops at once, as
  // if the user pressed stop. In-flight calls come back as unknown and remote tools keep running.
  // Opt-in.
  readonly hardSeconds?: number;
  // The same bounds for the whole session. Measured against the session record where the host
  // reports it, otherwise the workflow's own count carried across Continue-As-New.
  readonly sessionSeconds?: number;
  readonly sessionTokens?: number;
}

/** What a session has spent so far, carried between runs of its workflow. */
export interface Spent {
  readonly tokens: number;
  readonly cost: number;
  // Time spent in turns, not session age. Idle time costs nothing.
  readonly seconds: number;
}

// Runaway guard (a model looping on one tool, say). High enough that real work never reaches it.
export const MAX_STEPS_PER_TURN = 200;

// One workflow per turn of a live pi session. The turn runs in the pi process that owns it, so this
// is a record and retry policy around it, not a way to move it.
export const LOCAL_TURN_WORKFLOW = "piLocalTurn";

export interface LocalTurnInput {
  readonly sessionId: string;
  // Identifies the live turn inside the process that owns it.
  readonly turnId: string;
  // That process's own queue. Only it can run this turn.
  readonly taskQueue: string;
  // Drive the turn a step at a time, so each tool call is its own unit of work.
  readonly stepped?: boolean;
}

// Addressed by turn, not session file. The transcript is in that process's memory.
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
  // The user stopped the turn. Do not seal: the last step already closed, and sealing again ends
  // the turn twice.
  readonly interrupted?: boolean;
}
