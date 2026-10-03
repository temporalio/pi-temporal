// A turn of a live pi session, wrapped in a workflow. The turn itself runs in the pi process that
// owns the session, which the activities reach through a task queue only that process polls. So
// what this buys is a record of every turn and a retry policy around it, not portability: moving a
// turn would mean handing that session's ownership to another process, which this does not do.
//
// Stepped mode splits the turn the same way the worker-owned path does, into a model call, one
// activity per tool call, and a seal. It buys the same per-tool bounds and the same legible
// history. Reopening the session starts a new workflow from the transcript, so results the seal
// never recorded are gone with the old process.
//
// Sandbox-safe: only @temporalio/workflow and type-only protocol imports. No Pi SDK, no Node.

import {
  ActivityFailure,
  ApplicationFailure,
  CancellationScope,
  isCancellation,
  log,
  proxyActivities,
} from "@temporalio/workflow";
import { dispatchStepCalls } from "./l2-step.js";
import { MAX_STEPS_PER_TURN, TURN_STOPPED } from "./protocol.js";
import type {
  LocalModelCallResult,
  LocalSealInput,
  LocalStepInput,
  LocalToolCallInput,
  LocalTurnInput,
  ToolCallResult,
} from "./protocol.js";

interface LocalActivities {
  runLocalTurn(input: LocalTurnInput): Promise<void>;
  runLocalModelCall(input: LocalStepInput): Promise<LocalModelCallResult>;
  runLocalToolCall(input: LocalToolCallInput): Promise<ToolCallResult>;
  runLocalSeal(input: LocalSealInput): Promise<{ done: boolean }>;
}

// A call the user stopped from inside pi. Nothing cancelled the workflow, so only the failure's
// type says it was a stop, and a stop has to end the step the way a cancellation does.
const userStopped = (err: unknown) =>
  err instanceof ActivityFailure &&
  err.cause instanceof ApplicationFailure &&
  err.cause.type === TURN_STOPPED;

export async function piLocalTurn(input: LocalTurnInput): Promise<void> {
  const options = {
    taskQueue: input.taskQueue,
    // A turn is the whole agent run, so give it room; the heartbeat is the liveness bound.
    startToCloseTimeout: "1 hour",
    heartbeatTimeout: "30 seconds",
    // If the pi process is gone, nothing polls its queue and no retry can find the turn. Fail
    // rather than retry forever: the next pi to open the session resumes the turn as a new one.
    scheduleToStartTimeout: "1 minute",
    retry: { maximumAttempts: 3 },
  } as const;

  const { runLocalTurn } = proxyActivities<LocalActivities>(options);
  if (!input.stepped) {
    await runLocalTurn(input);
    return;
  }

  const { runLocalModelCall } = proxyActivities<LocalActivities>(options);
  const { runLocalToolCall } = proxyActivities<LocalActivities>(options);
  // The seal also runs a provider retry and a compaction, so it keeps the turn-sized backstop.
  const { runLocalSeal } = proxyActivities<LocalActivities>(options);

  for (let step = 1; step <= MAX_STEPS_PER_TURN; step++) {
    const model = await runLocalModelCall({ turnId: input.turnId, step });
    if (model.interrupted) return;

    const stopped = await dispatchStepCalls(
      step,
      // One at a time whatever the batch says. These calls all reach the one live agent this
      // process holds, and it admits a single unit of work at a time, so a second call that
      // arrived while the first was running would be refused and reported as an unknown outcome
      // for a tool that never ran. The worker half opens a session per activity and does overlap.
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
      // Close the step before letting the stop through, so the calls that finished keep their
      // results instead of reaching the model as unknown outcomes. The stop is what the caller
      // hears about, so a seal that fails on the way out does not replace it.
      await CancellationScope.nonCancellable(() => seal(true)).catch((err: unknown) => {
        log.warn("could not close a stopped step", { step, error: String(err) });
      });
      // A stop from inside pi ends the turn the way one between calls does: closed, not failed.
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
