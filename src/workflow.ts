// The per-session durable executor. One workflow per Pi session. It owns the small control state
// (the pending-prompt queue and where a turn is up to); the large conversation state lives in Pi's
// session JSONL, which the runStep activity reads and writes. One step is one activity, so a
// worker crash re-drives that step alone, and every step before it stays done.
//
// Sandbox-safe: only @temporalio/workflow and type-only protocol imports. No Pi SDK, no Node.

import {
  ApplicationFailure,
  proxyActivities,
  defineSignal,
  defineQuery,
  setHandler,
  workflowInfo,
  condition,
  continueAsNew,
  CancellationScope,
  isCancellation,
  log,
  ActivityFailure,
  TimeoutFailure,
} from "@temporalio/workflow";
import { MAX_STEPS_PER_TURN, QUERIES, SIGNALS, WORKFLOW_ID_PREFIX } from "./protocol.js";
import type {
  PromptInput,
  RunStepInput,
  RunStepResult,
  SessionTurnOptions,
  TurnState,
} from "./protocol.js";
import { makeSteppedStep, type SteppedActivities } from "./l2-step.js";

const activityOptions = {
  // One step is a single model call plus the tools it asks for, so minutes, not hours. The
  // heartbeat is the real liveness bound and re-drives within seconds of a worker death.
  startToCloseTimeout: "30 minutes",
  heartbeatTimeout: "30 seconds",
  retry: { maximumAttempts: 100 },
} as const;

// A total cap on top of the per-attempt one, so a unit that keeps timing out cannot hold the turn
// and the session's queue for days.
const CAP_MINUTES = 120;
const cappedOptions = {
  ...activityOptions,
  scheduleToCloseTimeout: `${CAP_MINUTES} minutes`,
} as const;

const { runStep } = proxyActivities<{
  runStep(input: RunStepInput): Promise<RunStepResult>;
}>(cappedOptions);

const { runModelCall } = proxyActivities<SteppedActivities>(cappedOptions);

// Overridable per deployment with PI_TEMPORAL_TOOL_TIMEOUT_MINUTES, read where the session starts.
export const DEFAULT_TOOL_TIMEOUT_MINUTES = 30;

function toolCallActivities(timeoutMinutes: number) {
  // A call that fails for a reason no retry fixes must not hold the step, the turn and the
  // session's queue behind it. The step waits for every call, so this ceiling is the one on all
  // of it. A bound past the cap gets the room for at least one attempt.
  return proxyActivities<SteppedActivities>({
    ...activityOptions,
    startToCloseTimeout: `${timeoutMinutes} minutes`,
    scheduleToCloseTimeout: `${Math.max(CAP_MINUTES, timeoutMinutes)} minutes`,
    retry: { maximumAttempts: 20 },
  });
}

// The seal also runs what answers for a step that went wrong: a provider retry, and a compaction
// that is itself a model call over the whole context. So it keeps the step-sized backstop.
const { sealStep } = proxyActivities<SteppedActivities>(cappedOptions);

// How long a pinned activity waits for the worker that ran the model call to take it. That worker
// polls the queue on its own, so this is the time to notice it is gone rather than a queueing
// delay: while it stands, nobody else can take the work.
const PINNED_SCHEDULE_TO_START = "30 seconds";

/** The same two activities, addressed to one worker's own queue. Built per queue rather than once,
 * because the queue is not known until the model call reports it, and that report comes out of
 * history, so this is deterministic on replay. */
const pinnedTo = (taskQueue: string) => ({
  runToolCall: proxyActivities<SteppedActivities>({
    ...cappedOptions,
    retry: { maximumAttempts: 20 },
    taskQueue,
    scheduleToStartTimeout: PINNED_SCHEDULE_TO_START,
  }).runToolCall,
  sealStep: proxyActivities<SteppedActivities>({
    ...cappedOptions,
    taskQueue,
    scheduleToStartTimeout: PINNED_SCHEDULE_TO_START,
  }).sealStep,
});

/** Nobody took the work. The only failure a pinned dispatch answers by moving the work elsewhere:
 * it means the activity never started, so no tool can have run. */
const isUnclaimed = (err: unknown) =>
  err instanceof ActivityFailure &&
  err.cause instanceof TimeoutFailure &&
  err.cause.timeoutType === "SCHEDULE_TO_START";

// Copying a project a client left for a scheduled session. Short, and it has to finish before the
// first step, so a queue nobody polls must not hold the run open waiting for it.
const { adoptProject } = proxyActivities<{
  adoptProject(input: { sessionFile: string; template: string }): Promise<void>;
}>({
  startToCloseTimeout: "5 minutes",
  scheduleToCloseTimeout: "30 minutes",
  retry: { maximumAttempts: 10 },
});

// Retiring is housekeeping, so it gets a short leash: a session that has already stopped must not
// keep a run open waiting for it, and the next prompt works whether or not this succeeded.
const { retireSession } = proxyActivities<{
  retireSession(input: { sessionFile: string }): Promise<void>;
}>({
  startToCloseTimeout: "1 minute",
  // A total cap as well: a queue nobody is polling must not hold the run open, since anything
  // queued behind this is waiting on it.
  scheduleToCloseTimeout: "5 minutes",
  retry: { maximumAttempts: 3 },
});

export const submitPrompt = defineSignal<[PromptInput]>(SIGNALS.submitPrompt);
export const interrupt = defineSignal<[]>(SIGNALS.interrupt);
export const turnState = defineQuery<TurnState>(QUERIES.turnState);

export async function piSession(
  sessionId: string,
  sessionFile: string,
  options?: SessionTurnOptions,
): Promise<void> {
  // A schedule fires with fixed arguments, so it cannot name a session, and every firing has to be
  // its own. The workflow id is already unique per firing (Temporal suffixes a scheduled one) and
  // is the one name both sides agree on, so derive from it when nothing was given.
  const id = sessionId || workflowInfo().workflowId.replace(WORKFLOW_ID_PREFIX, "");
  if (!sessionFile && !options?.sessionDir) {
    // The worker's own directory would do, until two workers in different directories serve one
    // session from two files.
    throw ApplicationFailure.nonRetryable(
      "piSession was started with neither a session file nor a session directory",
      "NoSessionDir",
    );
  }
  const file = sessionFile || `${options?.sessionDir}/${id}.jsonl`;
  const idleTimeout = options?.idleTimeout ?? "5 minutes";
  // Same loop either way. Only what "one step" means differs, so wake, interrupt, the step
  // ceiling and idle retirement are unchanged.
  const runTurnStep: (input: RunStepInput) => Promise<RunStepResult> = options?.stepped
    ? makeSteppedStep({
        activities: {
          runModelCall,
          runToolCall: toolCallActivities(
            options.toolTimeoutMinutes ?? DEFAULT_TOOL_TIMEOUT_MINUTES,
          ).runToolCall,
          sealStep,
        },
        isCancellation,
        pinnedTo,
        isUnclaimed,
        nonCancellable: (fn) => CancellationScope.nonCancellable(fn),
        // The SDK's logger, so a line carries its workflow and run id and is suppressed on replay.
        log: (message, attributes) => log.info(message, attributes),
      })
    : runStep;
  // A project the client left for this session, for a start nothing was running to send one to.
  // Before any step, because every activity restores the tree before it runs and a session with
  // nothing established refuses them all. Carried across a rollover with everything else, and a
  // no-op once the session has a project of its own.
  if (options?.template) await adoptProject({ sessionFile: file, template: options.template });
  // Seeded from the input, which is what lets a turn start with no client: the task is already in
  // the workflow when it begins, rather than arriving as a signal from something still running.
  // `queued` is the other source, from a run that rolled over with work still in hand.
  const queue: PromptInput[] = [
    ...(options?.initialPrompt ? [options.initialPrompt] : []),
    ...(options?.queued ?? []),
  ];
  let current: CancellationScope | undefined;
  let running: TurnState["running"];
  let finished: TurnState["finished"] = options?.finished;

  setHandler(submitPrompt, (p) => {
    queue.push(p);
  });
  setHandler(interrupt, () => {
    current?.cancel();
  });
  setHandler(turnState, () => ({ queued: queue.length, running, finished }));

  for (;;) {
    const woke = await condition(() => queue.length > 0, idleTimeout);
    if (!woke && queue.length === 0) {
      // Idle: retire, and the next prompt starts a fresh run. Hand the project directory back on
      // the way out, so a worker is not still holding it for a session nobody is driving. Best
      // effort: it frees the host that runs this, and a host that served the session earlier and
      // does not draw this activity keeps its own directory until it serves this session again.
      await retireSession({ sessionFile: file }).catch((err) =>
        log.warn("could not retire the session's directory", { sessionId: id, err: String(err) }),
      );
      // Look again. Awaiting an activity here is what makes this necessary: the server used to
      // protect the old shape, because a signal landing while the same workflow task was in flight
      // failed its completion and replayed it with the signal in history. An await splits that into
      // two tasks, so a prompt accepted during the retirement would be pushed onto a queue nothing
      // reads again, and `start` would have told the caller it was accepted.
      if (queue.length > 0) continue;
      return;
    }

    // Nothing is in flight at this point, which is the only place a rollover is safe: the queue is
    // the whole of the control state, and a turn that is mid-step cannot be handed to a new run.
    // Without this, history grows for the life of the run and the server terminates it mid-turn.
    // A stepped step is a handful of events per tool, so a busy session reaches the limit in hours.
    const info = workflowInfo();
    if (info.continueAsNewSuggested || info.historyLength >= (options?.maxHistory ?? Infinity)) {
      log.info("rolling the session over", {
        sessionId: id,
        queued: queue.length,
        historyLength: info.historyLength,
      });
      await continueAsNew<typeof piSession>(id, file, {
        ...options,
        // Carried as the queue, not as the initial prompt: that one is a schedule's task, and
        // repeating it on every rollover would ask for the same work again.
        initialPrompt: undefined,
        queued: queue,
        finished,
      });
    }

    const prompt = queue.shift()!;
    let outcome: NonNullable<TurnState["finished"]>["outcome"] = "ceiling";
    let finalText = "";
    let error: string | undefined;
    // The step's retry budget, kept here because the session that would count it is rebuilt per
    // activity and the transcript it could be read off is something a compaction rewrites.
    let retryAttempt = 0;
    try {
      await CancellationScope.cancellable(async () => {
        current = CancellationScope.current();
        for (let step = 1; step <= MAX_STEPS_PER_TURN; step++) {
          running = { promptId: prompt.promptId, step };
          const input: RunStepInput = {
            sessionId: id,
            sessionFile: file,
            step,
            retryAttempt,
            ...prompt,
          };
          const result = await runTurnStep(input);
          retryAttempt = result.retryAttempt;
          if (result.done) {
            outcome = "answered";
            finalText = result.finalText;
            return;
          }
        }
        log.warn("turn hit the step ceiling and was left where it stopped", {
          sessionId: id,
          promptId: prompt.promptId,
          maxSteps: MAX_STEPS_PER_TURN,
        });
      });
    } catch (err) {
      // An interrupt cancels the in-flight step; the session keeps serving later prompts. A real
      // run error is already recorded in the session log, so we log-and-continue rather than fail
      // the whole session.
      if (isCancellation(err)) {
        outcome = "interrupted";
      } else {
        // A step that exhausted its retries, or blew its ceiling, is not somebody pressing stop.
        outcome = "failed";
        error = err instanceof Error ? err.message : String(err);
        log.warn("turn failed", { sessionId: id, promptId: prompt.promptId, error });
      }
    } finally {
      current = undefined;
      running = undefined;
      finished = { promptId: prompt.promptId, outcome, finalText, error };
    }
  }
}
