// The per-session durable executor. One workflow per Pi session. It owns the small control state
// (prompt queue, current step). The conversation lives in Pi's session JSONL. A step is one
// activity, or in stepped mode a model call, one activity per tool call, and a seal. A worker
// crash re-drives only the unit it was running.
//
// Sandbox-safe: only @temporalio/workflow and type-only protocol imports. No Pi SDK, no Node.

import {
  patched,
  ApplicationFailure,
  proxyActivities,
  sleep,
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
import {
  FAILED_BEFORE_CLAIM,
  MAX_STEPS_PER_TURN,
  QUERIES,
  SIGNALS,
  sessionIdProblem,
  WORKFLOW_ID_PREFIX,
} from "./protocol.js";
import type {
  PromptInput,
  RunStepInput,
  RunStepResult,
  SessionTurnOptions,
  Spend,
  TurnState,
} from "./protocol.js";
import { makeSteppedStep, type SteppedActivities } from "./l2-step.js";

const activityOptions = {
  // A step is one model call plus its tools. The heartbeat is the real liveness bound.
  startToCloseTimeout: "30 minutes",
  heartbeatTimeout: "30 seconds",
  retry: { maximumAttempts: 100 },
} as const;

// Total cap, so a unit that keeps timing out cannot hold the turn and queue for days.
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
  // The step waits on every call, so this bounds the whole step. A bound past the cap still gets
  // room for one attempt.
  return proxyActivities<SteppedActivities>({
    ...activityOptions,
    startToCloseTimeout: `${timeoutMinutes} minutes`,
    scheduleToCloseTimeout: `${Math.max(CAP_MINUTES, timeoutMinutes)} minutes`,
    retry: { maximumAttempts: 20 },
  });
}

// The seal may run a provider retry and a compaction, so it keeps the step-sized cap.
const { sealStep } = proxyActivities<SteppedActivities>(cappedOptions);

// A missing or saturated worker must not leave an unstarted dispatch queued indefinitely.
const PINNED_SCHEDULE_TO_START_SECONDS = 30;
const PINNED_SCHEDULE_TO_START = `${PINNED_SCHEDULE_TO_START_SECONDS} seconds`;

/** The same activities on one worker's own queue. The queue comes from the model call's result in
 * history, so building this per queue is deterministic on replay. */
const pinnedTo = (taskQueue: string, timeoutMinutes: number) => ({
  runToolCall: proxyActivities<SteppedActivities>({
    ...cappedOptions,
    startToCloseTimeout: `${timeoutMinutes} minutes`,
    // The total starts when the call is queued, so it has room for the queue wait on top of the
    // tool's own timeout. Otherwise a long tool loses the time it waited.
    scheduleToCloseTimeout: `${Math.max(
      CAP_MINUTES * 60,
      timeoutMinutes * 60 + PINNED_SCHEDULE_TO_START_SECONDS,
    )} seconds`,
    // A retry's queue timeout cannot rule out an earlier attempt still running.
    retry: { maximumAttempts: 1 },
    taskQueue,
    scheduleToStartTimeout: PINNED_SCHEDULE_TO_START,
  }).runToolCall,
  // A seal runs under the session lock and is safe to repeat, so it may retry. A queue timeout is
  // never retried, so a lost host still fails fast.
  sealStep: proxyActivities<SteppedActivities>({
    ...cappedOptions,
    retry: { maximumAttempts: 3 },
    taskQueue,
    scheduleToStartTimeout: PINNED_SCHEDULE_TO_START,
  }).sealStep,
});

/** A pinned tool call has one attempt, so this timeout excludes an earlier started attempt. A
 * seal may have had an earlier attempt, but a seal is safe to repeat. */
const isUnclaimed = (err: unknown) =>
  err instanceof ActivityFailure &&
  ((err.cause instanceof TimeoutFailure && err.cause.timeoutType === "SCHEDULE_TO_START") ||
    (err.cause instanceof ApplicationFailure && err.cause.type === FAILED_BEFORE_CLAIM));

// Copies a template project for a scheduled session. Capped so an unpolled queue can't block it.
const { adoptProject } = proxyActivities<{
  adoptProject(input: { sessionFile: string; template: string }): Promise<void>;
}>({
  startToCloseTimeout: "5 minutes",
  scheduleToCloseTimeout: "30 minutes",
  retry: { maximumAttempts: 10 },
});

// Housekeeping. Short leash, and the next prompt works whether or not it succeeded.
const { retireSession } = proxyActivities<{
  retireSession(input: { sessionFile: string }): Promise<void>;
}>({
  startToCloseTimeout: "1 minute",
  // An unpolled queue must not hold the run open.
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
  // A schedule can't name a session, and each firing needs its own. Temporal makes scheduled
  // workflow ids unique, so derive the session id from it.
  const id = sessionId || workflowInfo().workflowId.replace(WORKFLOW_ID_PREFIX, "");
  // No default to ".": that is each worker's own cwd, so two workers would use two files.
  if (!sessionFile && !options?.sessionDir && patched("a-session-log-needs-a-named-home")) {
    throw ApplicationFailure.nonRetryable(
      `session ${id} was started with no session file and no sessionDir`,
    );
  }
  // A schedule id is the user's and becomes the file name here. Only an unsafe one reaches the
  // patch, so runs with safe ids record nothing new.
  const problem = sessionFile ? undefined : sessionIdProblem(id);
  if (problem && patched("a-session-id-names-one-file")) {
    throw ApplicationFailure.nonRetryable(
      `session id ${JSON.stringify(id)} can't be used: ${problem}`,
    );
  }
  const file = sessionFile || `${options?.sessionDir ?? "."}/${id}.jsonl`;
  const idleTimeout = options?.idleTimeout ?? "5 minutes";
  // Set per turn, since it reads that turn's spend. The step driver is built once.
  let outOfBudget = (): boolean => false;
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
        outOfBudget: () => outOfBudget(),
        // No patch gate. Replay doesn't compare activity timeouts, so a running session takes the
        // configured timeout from its next pinned call on.
        pinnedTo: (queue) =>
          pinnedTo(queue, options.toolTimeoutMinutes ?? DEFAULT_TOOL_TIMEOUT_MINUTES),
        isUnclaimed,
        // False only when replaying older histories. See the dep.
        refusesStartedFailures: () => patched("pinned-started-failure-does-not-migrate"),
        resumesAfterLostHost: () => patched("lost-host-does-not-end-the-turn"),
        nonCancellable: (fn) => CancellationScope.nonCancellable(fn),
        // The SDK logger tags workflow and run ids and is quiet on replay.
        log: (message, attributes) => log.info(message, attributes),
      })
    : runStep;
  let projectAdopted = options?.template === undefined;
  // Seeded from input, so a turn can start with no client. `queued` comes from a rollover.
  const queue: PromptInput[] = [
    ...(options?.initialPrompt ? [options.initialPrompt] : []),
    ...(options?.queued ?? []),
  ];
  let current: CancellationScope | undefined;
  let running: TurnState["running"];
  let finished: TurnState["finished"] = options?.finished;
  // Session spend, carried across rollovers so the allowance doesn't reset.
  const spent: { tokens: number; cost: number; seconds: number } = {
    tokens: options?.spent?.tokens ?? 0,
    cost: options?.spent?.cost ?? 0,
    seconds: options?.spent?.seconds ?? 0,
  };

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
      // Idle: release the project directory and exit. The next prompt starts a fresh run. Best
      // effort, and it only frees the host that runs this activity.
      await retireSession({ sessionFile: file }).catch((err) =>
        log.warn("could not retire the session's directory", { sessionId: id, err: String(err) }),
      );
      // A prompt may have arrived during the await. Exiting now would drop an accepted prompt.
      if (queue.length > 0) continue;
      return;
    }

    // Roll over only between turns. The queue is then the whole control state. A busy stepped
    // session can hit the history limit in hours.
    const info = workflowInfo();
    if (info.continueAsNewSuggested || info.historyLength >= (options?.maxHistory ?? Infinity)) {
      log.info("rolling the session over", {
        sessionId: id,
        queued: queue.length,
        historyLength: info.historyLength,
      });
      await continueAsNew<typeof piSession>(id, file, {
        ...options,
        // The initial prompt is a schedule's task. Carry it only in the queue, or it repeats.
        initialPrompt: undefined,
        template: projectAdopted ? undefined : options?.template,
        queued: queue,
        finished,
        spent,
      });
    }

    const prompt = queue.shift()!;
    let outcome: NonNullable<TurnState["finished"]>["outcome"] = "ceiling";
    let finalText = "";
    let error: string | undefined;
    // Kept here because the session is rebuilt per activity and compaction rewrites the transcript.
    let retryAttempt = 0;
    let overflowRecoveryAttempted = false;
    // Turn-scoped so the `finally` counts spend even for failed or stopped turns.
    const startedAt = Date.now();
    let tokens = 0;
    let cost = 0;
    // Session total from the record, as of the last step that reported it. Session bounds use it.
    let recorded: Spend | undefined;
    // True when the turn's own deadline cancelled it, not the user.
    let deadline = false;
    // Cancelled in `finally`. A scope's timers die only when the scope is cancelled, so without
    // this a leftover timer would stop a later turn.
    let deadlineScope: CancellationScope | undefined;
    try {
      await CancellationScope.cancellable(async () => {
        current = CancellationScope.current();
        // Hard deadline: stop where the turn is, not at the next step boundary.
        if (options?.budget?.hardSeconds !== undefined && patched("a-turn-has-a-budget")) {
          const hardMs = options.budget.hardSeconds * 1000;
          const expire = () => {
            deadline = true;
            current?.cancel();
          };
          if (patched("a-deadline-dies-with-its-turn")) {
            deadlineScope = new CancellationScope();
            deadlineScope.run(() => sleep(hardMs)).then(expire, () => undefined);
          } else {
            // Unpatched shape, kept for replay of runs that recorded it.
            sleep(hardMs).then(expire, () => undefined);
          }
        }
        if (!projectAdopted && options?.template) {
          // Part of the turn, so stop and query apply while it waits.
          running = { promptId: prompt.promptId, step: 0 };
          await adoptProject({ sessionFile: file, template: options.template });
          projectAdopted = true;
        }
        // Workflow state, not file totals, since the session file is shared. `Date.now()` is the
        // workflow clock, so replay agrees. Passed to the step so it can stop mid-batch.
        outOfBudget = () => {
          const b = options?.budget;
          if (!b) return false;
          const turnSeconds = (Date.now() - startedAt) / 1000;
          return (
            (b.tokens !== undefined && tokens > b.tokens) ||
            (b.seconds !== undefined && turnSeconds > b.seconds) ||
            (b.sessionTokens !== undefined &&
              (recorded?.tokens ?? spent.tokens + tokens) > b.sessionTokens) ||
            (b.sessionSeconds !== undefined && spent.seconds + turnSeconds > b.sessionSeconds)
          );
        };
        for (let step = 1; step <= MAX_STEPS_PER_TURN; step++) {
          running = { promptId: prompt.promptId, step };
          const input: RunStepInput = {
            sessionId: id,
            sessionFile: file,
            step,
            retryAttempt,
            overflowRecoveryAttempted,
            ...prompt,
          };
          const result = await runTurnStep(input);
          retryAttempt = result.retryAttempt;
          // A result with nothing to say, such as one from an older worker, keeps what an earlier
          // seal reported. Reset to false, it would hand the turn a second compact-and-retry.
          overflowRecoveryAttempted = result.overflowRecoveryAttempted ?? overflowRecoveryAttempted;
          tokens += result.spent?.tokens ?? 0;
          cost += result.spent?.cost ?? 0;
          recorded = result.total ?? recorded;
          if (result.done) {
            outcome = "answered";
            finalText = result.finalText;
            return;
          }
          // Checked between steps. A started call is left to finish, since an unanswered call
          // would break the session's next turn.
          if (outOfBudget() && patched("a-turn-has-a-budget")) {
            outcome = "budget";
            log.warn("turn stopped where it was: it is out of budget", {
              sessionId: id,
              promptId: prompt.promptId,
              step,
              turn: { tokens, cost, seconds: Math.round((Date.now() - startedAt) / 1000) },
              session: recorded ?? { tokens: spent.tokens + tokens, cost: spent.cost + cost },
              recorded: recorded !== undefined,
              budget: options?.budget,
            });
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
      // A failed turn does not fail the session. The session keeps serving later prompts.
      if (isCancellation(err)) {
        outcome = deadline ? "budget" : "interrupted";
        if (deadline) {
          log.warn("turn stopped where it was: its deadline passed", {
            sessionId: id,
            promptId: prompt.promptId,
            seconds: options?.budget?.hardSeconds,
          });
        }
      } else {
        outcome = "failed";
        error = err instanceof Error ? err.message : String(err);
        log.warn("turn failed", { sessionId: id, promptId: prompt.promptId, error });
      }
    } finally {
      deadlineScope?.cancel();
      current = undefined;
      running = undefined;
      // Counted here so failed and interrupted turns still count. The provider billed them.
      spent.tokens += tokens;
      spent.cost += cost;
      spent.seconds += (Date.now() - startedAt) / 1000;
      finished = { promptId: prompt.promptId, outcome, finalText, error, spent: { ...spent } };
    }
  }
}
