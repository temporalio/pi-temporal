// Builds the worker that drives Pi steps. Used by the standalone worker and by the pi extension,
// which runs one in-process so `/background` works with no separate worker. Same workflow and
// activities in both. Only the lifetime differs.

import { fileURLToPath } from "node:url";
import { NativeConnection, Worker } from "@temporalio/worker";
import { makeActivities, type ActivityOptions } from "./activities.js";
import { queueForWorker } from "./queue.js";

export interface SessionWorkerOptions extends ActivityOptions {
  readonly address: string;
  // API key or mTLS settings, built by `connectionOptions` so client and worker always agree.
  readonly connect?: Parameters<typeof NativeConnection.connect>[0];
  readonly namespace: string;
  readonly taskQueue: string;
  // Extra activities, e.g. runLocalTurn, which needs the live turns of its own process.
  readonly activities?: Record<string, unknown>;
}

export interface SessionWorker {
  readonly worker: Worker;
  // Runs the worker and resolves when it has drained. Call stop() to end it.
  readonly run: () => Promise<void>;
  readonly stop: () => Promise<void>;
}

export async function createSessionWorker(opts: SessionWorkerOptions): Promise<SessionWorker> {
  const connection = await NativeConnection.connect(opts.connect ?? { address: opts.address });
  // This process's own queue, so a step can return to the worker that started it. Computed once
  // here so the poller and the activities can't report different names.
  const stepQueue = queueForWorker(opts.taskQueue, opts.projectDir);
  const activities = { ...makeActivities({ ...opts, stepQueue }), ...opts.activities };
  const worker = await Worker.create({
    connection,
    namespace: opts.namespace,
    taskQueue: opts.taskQueue,
    workflowsPath: fileURLToPath(new URL("./workflows.ts", import.meta.url)),
    activities,
  });
  // Activities only, for work that must run on this host. Without this poller every pinned step
  // would wait out schedule-to-start before falling back to the shared queue.
  const pinned = await Worker.create({
    connection,
    namespace: opts.namespace,
    taskQueue: stepQueue,
    activities,
  });

  let running: Promise<void> | undefined;
  let runningPinned: Promise<void> | undefined;
  return {
    worker,
    run: () => {
      runningPinned ??= pinned.run();
      runningPinned.catch(() => {
        // The caller awaits the other one. This must not go unhandled.
      });
      running ??= worker.run();
      return running;
    },
    stop: async () => {
      worker.shutdown();
      pinned.shutdown();
      await running?.catch(() => {
        // A worker shut down mid-poll rejects. That's the shutdown, not a failure.
      });
      await runningPinned?.catch(() => {});
      await connection.close();
    },
  };
}
