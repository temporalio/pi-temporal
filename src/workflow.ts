// The per-session durable executor. One workflow per Pi session. It owns the small control state
// (the pending-prompt queue and where a turn is up to); the large conversation state lives in Pi's
// session JSONL, which the runStep activity reads and writes. One step is one activity, so a
// worker crash re-drives that step alone, and every step before it stays done.
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
  ApplicationFailure,
  TimeoutFailure,
} from "@temporalio/workflow";
import { MAX_STEPS_PER_TURN, QUERIES, SIGNALS, WORKFLOW_ID_PREFIX } from "./protocol.js";
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

// A missing or saturated worker must not leave an unstarted dispatch queued indefinitely.
const PINNED_SCHEDULE_TO_START = "30 seconds";

/** The same two activities, addressed to one worker's own queue. Built per queue rather than once,
 * because the queue is not known until the model call reports it, and that report comes out of
 * history, so this is deterministic on replay. */
const pinnedTo = (taskQueue: string) => ({
  runToolCall: proxyActivities<SteppedActivities>({
    ...cappedOptions,
    // A retry's queue timeout cannot rule out an earlier attempt still running.
    retry: { maximumAttempts: 1 },
    taskQueue,
    scheduleToStartTimeout: PINNED_SCHEDULE_TO_START,
  }).runToolCall,
  sealStep: proxyActivities<SteppedActivities>({
    ...cappedOptions,
    retry: { maximumAttempts: 1 },
    taskQueue,
    scheduleToStartTimeout: PINNED_SCHEDULE_TO_START,
  }).sealStep,
});

/** The pinned policy permits one attempt, so this timeout excludes an earlier started attempt. */
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
  // Refused rather than defaulted: "." is the working directory of whichever worker runs the step,
  // so two workers would serve one session from two different files. A start that names neither a
  // file nor a directory is the caller's bug, and the failure should land on the caller.
  if (!sessionFile && !options?.sessionDir && patched("a-session-log-needs-a-named-home")) {
    throw ApplicationFailure.nonRetryable(
      `session ${id} was started with no session file and no sessionDir`,
    );
  }
  const file = sessionFile || `${options?.sessionDir ?? "."}/${id}.jsonl`;
  const idleTimeout = options?.idleTimeout ?? "5 minutes";
  // Same loop either way. Only what "one step" means differs, so wake, interrupt, the step
  // ceiling and idle retirement are unchanged.
  // Set per turn, because what it reads is that turn's own spend. The step driver is built once.
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
        pinnedTo,
        isUnclaimed,
        // False only while replaying a history written before this rule existed. See the dep.
        refusesStartedFailures: () => patched("pinned-started-failure-does-not-migrate"),
        resumesAfterLostHost: () => patched("lost-host-does-not-end-the-turn"),
        nonCancellable: (fn) => CancellationScope.nonCancellable(fn),
        // The SDK's logger, so a line carries its workflow and run id and is suppressed on replay.
        log: (message, attributes) => log.info(message, attributes),
      })
    : runStep;
  let projectAdopted = options?.template === undefined;
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
  // What this session has spent, across every turn it has run, carried in from the run that rolled
  // over. A session does not get its allowance back by outgrowing a run's history.
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
    // The step's retry budget, kept here because the session that would count it is rebuilt per
    // activity and the transcript it could be read off is something a compaction rewrites.
    let retryAttempt = 0;
    // At the turn's scope, not the step's: what a turn spent has to be there for the `finally`
    // below, which runs whether the turn answered, failed or was stopped. The provider billed for
    // it either way.
    const startedAt = Date.now();
    let tokens = 0;
    let cost = 0;
    // What the record says the session has been billed, as of the last step that reported it. The
    // count this workflow keeps is about a run; this one is about the session, and it is the one a
    // session's own bound is measured against wherever the host can say it.
    let recorded: Spend | undefined;
    // Whether what cancelled this turn was its own deadline rather than somebody pressing stop.
    let deadline = false;
    // The deadline timer's own scope, cancelled in the `finally` below. A scope cancels its timers
    // when it is itself cancelled, not when its function returns, so a turn that answered before
    // the deadline would otherwise leave the timer pending, and it would fire into whichever turn
    // happened to be running then and stop it as if the user had.
    let deadlineScope: CancellationScope | undefined;
    try {
      await CancellationScope.cancellable(async () => {
        current = CancellationScope.current();
        // A deadline that ends the turn where it is, rather than at the next place it can stop.
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
            // The old shape, kept for runs that recorded it: the timer sat in the turn scope and
            // outlived a turn that answered early.
            sleep(hardMs).then(expire, () => undefined);
          }
        }
        if (!projectAdopted && options?.template) {
          // Initialization is part of the turn, so stop and query apply while it waits.
          running = { promptId: prompt.promptId, step: 0 };
          await adoptProject({ sessionFile: file, template: options.template });
          projectAdopted = true;
        }
        // What this turn has spent, and when it started spending it. Both are workflow state: the
        // session file is shared, so its own totals include turns this workflow never ran, and
        // `Date.now()` here is the workflow's clock, which reads the same on a replay.
        // A step that crosses a bound is the last one the workflow drives, and the one already in
        // flight is left to finish. Handing this to the step lets it stop dispatching the rest of a
        // batch instead, which is the difference between overshooting by a step and overshooting by
        // whatever is running.
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
            ...prompt,
          };
          const result = await runTurnStep(input);
          retryAttempt = result.retryAttempt;
          tokens += result.spent?.tokens ?? 0;
          cost += result.spent?.cost ?? 0;
          recorded = result.total ?? recorded;
          if (result.done) {
            outcome = "answered";
            finalText = result.finalText;
            return;
          }
          // Between steps, and inside one only as far as refusing to dispatch what has not started.
          // A call that has started is left to finish: stopping it would leave a call in the
          // transcript that no result answers, which is a payload no provider accepts, and the next
          // turn of this session would fail rather than this one.
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
      // An interrupt cancels the in-flight step; the session keeps serving later prompts. A real
      // run error is already recorded in the session log, so we log-and-continue rather than fail
      // the whole session.
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
        // A step that exhausted its retries, or blew its ceiling, is not somebody pressing stop.
        outcome = "failed";
        error = err instanceof Error ? err.message : String(err);
        log.warn("turn failed", { sessionId: id, promptId: prompt.promptId, error });
      }
    } finally {
      // Dead with its turn. A fired or already-cancelled timer makes this a no-op.
      deadlineScope?.cancel();
      current = undefined;
      running = undefined;
      // Added up here rather than per step, so a turn that failed or was interrupted still counts
      // what it spent: the provider billed for it either way.
      spent.tokens += tokens;
      spent.cost += cost;
      spent.seconds += (Date.now() - startedAt) / 1000;
      finished = { promptId: prompt.promptId, outcome, finalText, error, spent: { ...spent } };
    }
  }
}
