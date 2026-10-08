// The per-session durable executor. One workflow per Pi session. It owns the small control state
// (prompt queue, current step). The conversation lives in Pi's session JSONL. A step is one
// activity, or in stepped mode a model call, one activity per tool call, and a seal. A worker
// crash re-drives only the unit it was running.
//
// Sandbox-safe: only @temporalio/workflow and type-only protocol imports. No Pi SDK, no Node.

import {
  ApplicationFailure,
  proxyActivities,
  sleep,
  defineSignal,
  defineQuery,
  defineUpdate,
  allHandlersFinished,
  setHandler,
  workflowInfo,
  condition,
  continueAsNew,
  CancellationScope,
  isCancellation,
  log,
  setCurrentDetails,
  upsertMemo,
  ActivityCancellationType,
  ActivityFailure,
  TimeoutFailure,
} from "@temporalio/workflow";
import {
  FAILED_BEFORE_CLAIM,
  fencePrefix,
  MAX_STEPS_PER_TURN,
  DUPLICATE_PROMPT,
  QUERIES,
  SESSION_MEMO,
  SIGNALS,
  UPDATES,
  sessionIdProblem,
  WORKFLOW_ID_PREFIX,
} from "./protocol.js";
import type {
  AgentState,
  PromptInput,
  Quiet,
  RetireInput,
  Submitted,
  RunStepInput,
  RunStepResult,
  SessionTurnOptions,
  Spend,
  TurnState,
} from "./protocol.js";
import { makeSteppedStep, type SteppedActivities } from "./stepped-step.js";

const activityOptions = {
  // A step is one model call plus its tools. The heartbeat is the real liveness bound.
  startToCloseTimeout: "30 minutes",
  heartbeatTimeout: "30 seconds",
  // For a lost worker or a storage error. Pi retries the provider itself, inside the step, and
  // counts those retries in the transcript, so this layer doesn't need many.
  retry: { maximumAttempts: 10, initialInterval: "1 second", maximumInterval: "1 minute" },
} as const;

// Total cap, so a unit that keeps timing out cannot hold the turn and queue for days.
const CAP_MINUTES = 120;
const cappedOptions = {
  ...activityOptions,
  scheduleToCloseTimeout: `${CAP_MINUTES} minutes`,
} as const;

// A whole step runs its tools and its seal. A stop waits for both, so the step's results are
// recorded before the session moves on.
const { runStep } = proxyActivities<{
  runStep(input: RunStepInput): Promise<RunStepResult>;
}>({ ...cappedOptions, cancellationType: ActivityCancellationType.WAIT_CANCELLATION_COMPLETED });

const { runModelCall } = proxyActivities<SteppedActivities>(cappedOptions);

// A stop waits for each running tool to stop and report, so the seal that follows records what
// the tools did instead of unknown outcomes.
const TOOL_CANCELLATION = ActivityCancellationType.WAIT_CANCELLATION_COMPLETED;

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
    cancellationType: TOOL_CANCELLATION,
  });
}

// The seal may run a provider retry and a compaction, so it keeps the step-sized cap.
const { sealStep } = proxyActivities<SteppedActivities>(cappedOptions);

// A missing or saturated worker must not leave an unstarted dispatch queued indefinitely.
const HOST_SCHEDULE_TO_START_SECONDS = 30;
const HOST_SCHEDULE_TO_START = `${HOST_SCHEDULE_TO_START_SECONDS} seconds`;

/** The same activities on one worker's own queue. The queue comes from the model call's result in
 * history, so building this per queue is deterministic on replay. */
const onHost = (taskQueue: string, timeoutMinutes: number) => ({
  runToolCall: proxyActivities<SteppedActivities>({
    ...cappedOptions,
    startToCloseTimeout: `${timeoutMinutes} minutes`,
    // The total starts when the call is queued, so it has room for the queue wait on top of the
    // tool's own timeout. Otherwise a long tool loses the time it waited.
    scheduleToCloseTimeout: `${Math.max(
      CAP_MINUTES * 60,
      timeoutMinutes * 60 + HOST_SCHEDULE_TO_START_SECONDS,
    )} seconds`,
    // A retry's queue timeout cannot rule out an earlier attempt still running.
    retry: { maximumAttempts: 1 },
    cancellationType: TOOL_CANCELLATION,
    taskQueue,
    scheduleToStartTimeout: HOST_SCHEDULE_TO_START,
  }).runToolCall,
  // A seal is fenced and safe to repeat, so it may retry. A queue timeout is
  // never retried, so a lost host still fails fast.
  sealStep: proxyActivities<SteppedActivities>({
    ...cappedOptions,
    retry: { maximumAttempts: 3 },
    taskQueue,
    scheduleToStartTimeout: HOST_SCHEDULE_TO_START,
  }).sealStep,
});

/** A host-queue tool call has one attempt, so this timeout excludes an earlier started attempt. A
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
const retireOptions = {
  startToCloseTimeout: "1 minute",
  // An unpolled queue must not hold the run open.
  scheduleToCloseTimeout: "5 minutes",
  retry: { maximumAttempts: 3 },
} as const;
type Retire = { retireSession(input: RetireInput): Promise<void> };
const { retireSession } = proxyActivities<Retire>(retireOptions);
// On one host's own queue. A host that's gone doesn't hold the run open, and a later sweep or the
// session's retired marker frees its directory.
const retireOn = (taskQueue: string) =>
  proxyActivities<Retire>({
    ...retireOptions,
    taskQueue,
    scheduleToStartTimeout: HOST_SCHEDULE_TO_START,
    retry: { maximumAttempts: 1 },
  }).retireSession;

// For a client with no Worker to accept an Update. A Signal is kept with no Worker up. It can't be
// rejected, so a bad or repeated prompt is dropped.
export const submitPrompt = defineSignal<[PromptInput]>(SIGNALS.submitPrompt);
export const submit = defineUpdate<Submitted, [PromptInput]>(UPDATES.submit);
export const waitForQuiet = defineUpdate<Quiet, []>(UPDATES.waitForQuiet);

// How many prompt ids a session remembers to refuse a resend. Far more than any client retries.
const SEEN_PROMPTS = 200;
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
  if (!sessionFile && !options?.sessionDir) {
    throw ApplicationFailure.nonRetryable(
      `session ${id} was started with no session file and no sessionDir`,
    );
  }
  // A schedule id is the user's and becomes the file name here.
  const problem = sessionFile ? undefined : sessionIdProblem(id);
  if (problem) {
    throw ApplicationFailure.nonRetryable(
      `session id ${JSON.stringify(id)} can't be used: ${problem}`,
    );
  }
  const file = sessionFile || `${options?.sessionDir ?? "."}/${id}.jsonl`;
  const idleTimeout = options?.idleTimeout ?? "5 minutes";
  // One per Activity that writes the session file. See `fence.ts`.
  let fenced = 0;
  const fence = () => fencePrefix(workflowInfo().runStartTime.getTime(), ++fenced);
  // Set per turn, since it reads that turn's spend. The step driver is built once.
  let outOfBudget = (_pending?: Pick<RunStepResult, "spent" | "total">): boolean => false;
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
        outOfBudget: (pending) => outOfBudget(pending),
        fence,
        // Replay doesn't compare activity timeouts, so a running session takes a changed tool
        // timeout from its next host-queue call on.
        onHost: (queue) =>
          onHost(queue, options.toolTimeoutMinutes ?? DEFAULT_TOOL_TIMEOUT_MINUTES),
        isUnclaimed,
        nonCancellable: (fn) => CancellationScope.nonCancellable(fn),
        // The SDK logger tags workflow and run ids and is quiet on replay.
        log: (message, attributes) => log.info(message, attributes),
      })
    : runStep;
  let projectAdopted = options?.template === undefined;
  // Seeded from input, so a turn can start with no client. `queued` comes from a Continue-As-New.
  const queue: PromptInput[] = [
    ...(options?.initialPrompt ? [options.initialPrompt] : []),
    ...(options?.queued ?? []),
  ];
  let current: CancellationScope | undefined;
  let running: TurnState["running"];
  let finished: TurnState["finished"] = options?.finished;
  const hostQueues = new Set(options?.hostQueues ?? []);
  // Session spend, carried across Continue-As-New so the allowance doesn't reset.
  const spent: { tokens: number; cost: number; seconds: number } = {
    tokens: options?.spent?.tokens ?? 0,
    cost: options?.spent?.cost ?? 0,
    seconds: options?.spent?.seconds ?? 0,
  };

  // In the memo, so a List call shows every session's state without a Query to each. Upserted
  // only when the state changes, since each upsert is a history event. The step number is in the
  // current details, which cost no history.
  let shown = "";
  const show = () => {
    const state = running ? "running" : "idle";
    if (`${state}/${queue.length}` !== shown) {
      shown = `${state}/${queue.length}`;
      upsertMemo({ [SESSION_MEMO]: { state, queued: queue.length } });
    }
    setCurrentDetails(running ? `turn ${running.promptId}, step ${running.step}` : "idle");
  };

  const seen = [...(options?.seenPrompts ?? []), ...queue.map((p) => p.promptId)];
  const problemWith = (p: PromptInput) =>
    !p?.promptId ? "a prompt needs an id" : !p.text?.trim() ? "a prompt needs text" : undefined;
  const enqueue = (p: PromptInput) => {
    seen.push(p.promptId);
    if (seen.length > SEEN_PROMPTS) seen.splice(0, seen.length - SEEN_PROMPTS);
    queue.push(p);
    show();
  };
  // Set right before the run ends or continues as new, so a waiting client is answered and asks
  // the next run.
  let ending = false;

  setHandler(submitPrompt, (p) => {
    if (problemWith(p) === undefined && !seen.includes(p.promptId)) enqueue(p);
  });
  setHandler(
    submit,
    (p) => {
      enqueue(p);
      return { ahead: queue.length - 1 + (running ? 1 : 0) };
    },
    {
      validator: (p) => {
        const problem = problemWith(p);
        if (problem) throw ApplicationFailure.nonRetryable(problem, "BadPrompt");
        if (seen.includes(p.promptId)) {
          throw ApplicationFailure.nonRetryable(
            `prompt ${p.promptId} is already in this session`,
            DUPLICATE_PROMPT,
          );
        }
      },
    },
  );
  setHandler(waitForQuiet, async () => {
    await condition(() => ending || (!running && queue.length === 0));
    return !running && queue.length === 0 ? { finished } : { moved: true };
  });
  setHandler(interrupt, () => {
    current?.cancel();
  });
  setHandler(turnState, () => ({ queued: queue.length, running, finished }));

  for (;;) {
    const woke = await condition(() => queue.length > 0, idleTimeout);
    if (!woke && queue.length === 0) {
      // Idle: release the project directory and exit. The next prompt starts a fresh run. Best
      // effort. Each host that held the directory is asked on its own queue.
      // The Workflow's own count of the session's time, which also covers each turn's last seal
      // and failed attempts. Written now, since the record is all the next run has.
      const last = finished && { turn: finished.promptId, sessionSeconds: spent.seconds };
      const warn = (err: unknown) =>
        log.warn("could not retire the session's directory", { sessionId: id, err: String(err) });
      await Promise.all([
        retireSession({ sessionFile: file, ...last, fence: fence() }).catch(warn),
        ...[...hostQueues].map((queue) => retireOn(queue)({ sessionFile: file }).catch(warn)),
      ]);
      // A prompt may have arrived during the await. Exiting now would drop an accepted prompt.
      if (queue.length > 0) continue;
      // Answer waiting clients before the run closes. A prompt can still arrive meanwhile.
      ending = true;
      await condition(allHandlersFinished);
      if (queue.length > 0) {
        ending = false;
        continue;
      }
      return;
    }

    // Continue-As-New only between turns. The queue is then the whole control state. A busy stepped
    // session can hit the history limit in hours.
    const info = workflowInfo();
    if (info.continueAsNewSuggested || info.historyLength >= (options?.maxHistory ?? Infinity)) {
      log.info("continuing the session as new", {
        sessionId: id,
        queued: queue.length,
        historyLength: info.historyLength,
      });
      ending = true;
      await condition(allHandlersFinished);
      await continueAsNew<typeof piSession>(id, file, {
        ...options,
        // The initial prompt is a schedule's task. Carry it only in the queue, or it repeats.
        initialPrompt: undefined,
        template: projectAdopted ? undefined : options?.template,
        queued: queue,
        finished,
        spent,
        hostQueues: [...hostQueues],
        seenPrompts: seen,
      });
    }

    const prompt = queue.shift()!;
    let outcome: NonNullable<TurnState["finished"]>["outcome"] = "ceiling";
    let finalText = "";
    let error: string | undefined;
    // The agent's state between steps, kept here because the session is opened again per Activity.
    let agentState: AgentState | undefined;
    // Turn-scoped so the `finally` counts spend even for failed or stopped turns.
    const startedAt = Date.now();
    let tokens = 0;
    let cost = 0;
    // Session total from the record, as of the last step that reported it. Session bounds use it.
    let recorded: Spend | undefined;
    // The session's turn time as its record keeps it. A run started after an idle exit has no
    // `spent` from the run before, so the record is what carries the earlier turns.
    let recordedSeconds: number | undefined;
    const sessionSecondsBefore = () => Math.max(spent.seconds, recordedSeconds ?? 0);
    // True when the turn's own deadline cancelled it, not the user.
    let deadline = false;
    // Cancelled in `finally`. A scope's timers die only when the scope is cancelled, so without
    // this a leftover timer would stop a later turn.
    let deadlineScope: CancellationScope | undefined;
    try {
      await CancellationScope.cancellable(async () => {
        current = CancellationScope.current();
        // Hard deadline: stop where the turn is, not at the next step boundary.
        if (options?.budget?.hardSeconds !== undefined) {
          const hardMs = options.budget.hardSeconds * 1000;
          const expire = () => {
            deadline = true;
            current?.cancel();
          };
          deadlineScope = new CancellationScope();
          deadlineScope.run(() => sleep(hardMs)).then(expire, () => undefined);
        }
        if (!projectAdopted && options?.template) {
          // Part of the turn, so stop and query apply while it waits.
          running = { promptId: prompt.promptId, step: 0 };
          show();
          await adoptProject({ sessionFile: file, template: options.template });
          projectAdopted = true;
        }
        // Workflow state, not file totals, since the session file is shared. `Date.now()` is the
        // workflow clock, so replay agrees. Passed to the step so it can stop mid-batch.
        const over = (pending?: Pick<RunStepResult, "spent" | "total">) => {
          const b = options?.budget;
          if (!b) return false;
          const turnSeconds = (Date.now() - startedAt) / 1000;
          const turnTokens = tokens + (pending?.spent?.tokens ?? 0);
          // The session total a model call read already includes its own spend.
          const session = pending?.total ?? recorded;
          return (
            (b.tokens !== undefined && turnTokens > b.tokens) ||
            (b.seconds !== undefined && turnSeconds > b.seconds) ||
            (b.sessionTokens !== undefined &&
              (session?.tokens ?? spent.tokens + turnTokens) > b.sessionTokens) ||
            (b.sessionSeconds !== undefined &&
              sessionSecondsBefore() + turnSeconds > b.sessionSeconds)
          );
        };
        // Between the tools of one step, the step's own model call counts too.
        outOfBudget = (pending) => over(pending);
        for (let step = 1; step <= MAX_STEPS_PER_TURN; step++) {
          running = { promptId: prompt.promptId, step };
          show();
          const input: RunStepInput = {
            sessionId: id,
            sessionFile: file,
            step,
            agentState,
            sessionSeconds: sessionSecondsBefore() + (Date.now() - startedAt) / 1000,
            // For `runStep`, or for the model call of a stepped step.
            fence: fence(),
            ...prompt,
          };
          const result = await runTurnStep(input);
          // A result with nothing to say keeps what an earlier step reported. Reset, it could hand
          // the agent a second go at something it allows once per turn.
          agentState = result.agentState ?? agentState;
          tokens += result.spent?.tokens ?? 0;
          cost += result.spent?.cost ?? 0;
          recorded = result.total ?? recorded;
          recordedSeconds = result.sessionSeconds ?? recordedSeconds;
          if (result.hostQueue) hostQueues.add(result.hostQueue);
          if (result.done) {
            outcome = "answered";
            finalText = result.finalText;
            return;
          }
          // Checked between steps. A started call is left to finish, since an unanswered call
          // would break the session's next turn.
          if (outOfBudget()) {
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
      show();
      // Counted here so failed and interrupted turns still count. The provider billed them.
      spent.tokens += tokens;
      spent.cost += cost;
      spent.seconds = sessionSecondsBefore() + (Date.now() - startedAt) / 1000;
      finished = { promptId: prompt.promptId, outcome, finalText, error, spent: { ...spent } };
    }
  }
}
