// A turn of a live pi session, wrapped in a workflow. The turn itself runs in the pi process that
// owns the session, which the activity reaches through a task queue only that process polls. So
// what this buys is a record of every turn and a retry policy around it, not portability: a turn
// cannot move to another process, because the session it belongs to is in memory over there.
//
// Sandbox-safe: only @temporalio/workflow and type-only protocol imports. No Pi SDK, no Node.

import { proxyActivities } from "@temporalio/workflow";
import type { LocalTurnInput } from "./protocol.js";

export async function piLocalTurn(input: LocalTurnInput): Promise<void> {
  const { runLocalTurn } = proxyActivities<{ runLocalTurn(input: LocalTurnInput): Promise<void> }>({
    taskQueue: input.taskQueue,
    // A turn is the whole agent run, so give it room; the heartbeat is the liveness bound.
    startToCloseTimeout: "1 hour",
    heartbeatTimeout: "30 seconds",
    // If the pi process is gone, nothing polls its queue and no retry can find the turn. Fail
    // rather than retry forever: the next pi to open the session resumes the turn as a new one.
    scheduleToStartTimeout: "1 minute",
    retry: { maximumAttempts: 3 },
  });

  await runLocalTurn(input);
}
