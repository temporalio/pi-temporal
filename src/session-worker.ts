// Builds the worker that drives Pi steps. Two callers: the standalone worker process, and the pi
// extension, which runs one inside pi so /durable works without a worker you started yourself.
//
// Both get the same workflow and the same activity. The difference is only where the process
// lives, and how long it lives for.

import { fileURLToPath } from "node:url";
import { NativeConnection, Worker } from "@temporalio/worker";
import { makeActivities, type ActivityOptions } from "./activities.js";

export interface SessionWorkerOptions extends ActivityOptions {
  readonly address: string;
  readonly namespace: string;
  readonly taskQueue: string;
}

export interface SessionWorker {
  readonly worker: Worker;
  // Runs the worker and resolves when it has drained. Call stop() to end it.
  readonly run: () => Promise<void>;
  readonly stop: () => Promise<void>;
}

export async function createSessionWorker(opts: SessionWorkerOptions): Promise<SessionWorker> {
  const connection = await NativeConnection.connect({ address: opts.address });
  const worker = await Worker.create({
    connection,
    namespace: opts.namespace,
    taskQueue: opts.taskQueue,
    workflowsPath: fileURLToPath(new URL("./workflow.ts", import.meta.url)),
    activities: makeActivities(opts),
  });

  let running: Promise<void> | undefined;
  return {
    worker,
    run: () => {
      running ??= worker.run();
      return running;
    },
    stop: async () => {
      worker.shutdown();
      await running?.catch(() => {
        // A worker shut down mid-poll rejects; that is the shutdown, not a failure.
      });
      await connection.close();
    },
  };
}
