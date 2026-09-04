// Builds the worker that drives Pi steps. Two callers: the standalone worker process, and the pi
// extension, which runs one inside pi so /durable works without a worker you started yourself.
//
// Both get the same workflow and the same activity. The difference is only where the process
// lives, and how long it lives for.

import { fileURLToPath } from "node:url";
import { NativeConnection, Worker } from "@temporalio/worker";
import { makeActivities, type ActivityOptions } from "./activities.js";
import { queueForWorker } from "./queue.js";

export interface SessionWorkerOptions extends ActivityOptions {
  readonly address: string;
  // How the server is reached when it is not a plaintext dev server: an API key for Cloud, a
  // certificate pair for a cluster with mTLS. Built by `connectionOptions`, so the client and the
  // worker cannot disagree about it.
  readonly connect?: Parameters<typeof NativeConnection.connect>[0];
  readonly namespace: string;
  readonly taskQueue: string;
  // Activities beyond runStep. The turn executor adds runLocalTurn, which needs the live turns of
  // the process it runs in and so cannot come from here.
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
  // The queue this process polls on its own, so a step can be sent back to the worker that started
  // it. Derived here rather than passed in: it has to be the same name the activities report, and
  // one of the two computing it separately is a session that waits on a queue nobody polls.
  const stepQueue = queueForWorker(opts.taskQueue, opts.projectDir);
  const activities = { ...makeActivities({ ...opts, stepQueue }), ...opts.activities };
  const worker = await Worker.create({
    connection,
    namespace: opts.namespace,
    taskQueue: opts.taskQueue,
    workflowsPath: fileURLToPath(new URL("./workflows.ts", import.meta.url)),
    activities,
  });
  // Activities only. The workflow runs wherever it was started; what comes back here is the work
  // that has to be on this host, and without a poller every pinned step would pay the
  // schedule-to-start wait before falling back to the shared queue.
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
        // Reported by whichever of the two the caller awaits; this one must not go unhandled.
      });
      running ??= worker.run();
      return running;
    },
    stop: async () => {
      worker.shutdown();
      pinned.shutdown();
      await running?.catch(() => {
        // A worker shut down mid-poll rejects; that is the shutdown, not a failure.
      });
      await runningPinned?.catch(() => {});
      await connection.close();
    },
  };
}
