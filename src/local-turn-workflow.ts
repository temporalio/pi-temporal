// A turn of a live pi session, wrapped in a workflow. The turn itself runs in the pi process that
// owns the session, which the activities reach through a task queue only that process polls. So
// what this buys is a record of every turn and a retry policy around it, not portability: a turn
// cannot move to another process, because the session it belongs to is in memory over there.
//
// Stepped mode splits the turn the same way the worker-owned path does, into a model call, one
// activity per tool call, and a seal. It buys the same per-tool bounds and the same legible
// history; it does not buy portability, which the local turn cannot have.
//
// Sandbox-safe: only @temporalio/workflow and type-only protocol imports. No Pi SDK, no Node.

import { isCancellation, log, proxyActivities } from "@temporalio/workflow";
import { dispatchStepCalls } from "./l2-step.js";
import { MAX_STEPS_PER_TURN } from "./protocol.js";
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

  // Separate proxies are the point of the split: a tool that hangs no longer holds the model call
  // and every other tool of the same step under one shared timeout.
  const { runLocalModelCall } = proxyActivities<LocalActivities>(options);
  const { runLocalToolCall } = proxyActivities<LocalActivities>(options);
  const { runLocalSeal } = proxyActivities<LocalActivities>({ ...options, startToCloseTimeout: "5 minutes" });

  for (let step = 1; step <= MAX_STEPS_PER_TURN; step++) {
    const model = await runLocalModelCall({ turnId: input.turnId, step });
    await dispatchStepCalls(
      step,
      model,
      (call) => runLocalToolCall({ turnId: input.turnId, step, call }),
      { isCancellation, log: (message, attributes) => log.info(message, attributes) },
    );
    const { done } = await runLocalSeal({ turnId: input.turnId, step, calls: model.calls });
    if (done) return;
  }

  log.warn("turn hit the step ceiling and was left where it stopped", {
    sessionId: input.sessionId,
    maxSteps: MAX_STEPS_PER_TURN,
  });
}
