// A live turn uses a private queue because only its Pi process owns the session in memory.
// Temporal records the turn and applies retries, but the turn cannot move between processes.
//
// Stepped mode gives each model call, tool call, and seal its own Activity. A process crash
// loses results that the seal has not recorded.
//
// The sandbox loads this file, so it must not import the Pi SDK or Node modules.

import {
  ActivityCancellationType,
  ActivityFailure,
  ApplicationFailure,
  CancellationScope,
  isCancellation,
  log,
  proxyActivities,
} from "@temporalio/workflow";
import { dispatchStepCalls } from "../core/stepped-step.js";
import { MAX_STEPS_PER_TURN, TURN_STOPPED } from "../core/protocol.js";
import type {
  LocalModelCallResult,
  LocalSealInput,
  LocalStepInput,
  LocalToolCallInput,
  LocalTurnInput,
  ToolCallResult,
} from "../core/protocol.js";

interface LocalActivities {
  runLocalTurn(input: LocalTurnInput): Promise<void>;
  runLocalModelCall(input: LocalStepInput): Promise<LocalModelCallResult>;
  runLocalToolCall(input: LocalToolCallInput): Promise<ToolCallResult>;
  runLocalSeal(input: LocalSealInput): Promise<{ done: boolean }>;
}

// A stop from inside pi does not cancel the Workflow. Only the failure type marks it.
const userStopped = (err: unknown) =>
  err instanceof ActivityFailure &&
  err.cause instanceof ApplicationFailure &&
  err.cause.type === TURN_STOPPED;

export async function piLocalTurn(input: LocalTurnInput): Promise<void> {
  const options = {
    taskQueue: input.taskQueue,
    // A whole agent run. The heartbeat is the liveness bound.
    startToCloseTimeout: "1 hour",
    heartbeatTimeout: "30 seconds",
    // If the pi process is gone, nothing polls its queue. Fail fast. The next pi to open the
    // session resumes the turn.
    scheduleToStartTimeout: "1 minute",
    retry: { maximumAttempts: 3 },
    // A stop waits for the running unit to stop and report, so its result is recorded.
    cancellationType: ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
  } as const;

  // One set of options for every unit. The seal can run a provider retry and a compaction, so it
  // needs the turn-sized timeout too.
  const { runLocalTurn, runLocalModelCall, runLocalToolCall, runLocalSeal } =
    proxyActivities<LocalActivities>(options);
  if (!input.stepped) {
    await runLocalTurn(input);
    return;
  }

  for (let step = 1; step <= MAX_STEPS_PER_TURN; step++) {
    const model = await runLocalModelCall({ turnId: input.turnId, step });
    if (model.interrupted) return;

    const stopped = await dispatchStepCalls(
      step,
      // Always sequential. The live agent admits one unit of work at a time and would refuse a
      // concurrent call, reporting an unknown outcome for a tool that never ran.
      { ...model, sequential: true },
      (call) => runLocalToolCall({ turnId: input.turnId, step, call }),
      {
        isCancellation: (err) => isCancellation(err) || userStopped(err),
        log: (message, attributes) => log.info(message, attributes),
      },
    );
    const seal = (interrupted: boolean) =>
      runLocalSeal({ turnId: input.turnId, step, calls: model.calls, interrupted });
    if (stopped) {
      // Seal first so finished calls keep their results. A seal failure must not replace the stop.
      await CancellationScope.nonCancellable(() => seal(true)).catch((err: unknown) => {
        log.warn("could not close a stopped step", { step, error: String(err) });
      });
      // A user stop closes the turn, it doesn't fail it.
      if (userStopped(stopped)) return;
      throw stopped;
    }
    const { done } = await seal(false);
    if (done) return;
  }

  log.warn("turn hit the step ceiling and was left where it stopped", {
    sessionId: input.sessionId,
    maxSteps: MAX_STEPS_PER_TURN,
  });
}
