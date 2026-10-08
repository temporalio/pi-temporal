// Builds the worker that drives Pi steps. Used by the standalone worker and by the pi extension,
// which runs one in-process so `/background` works with no separate worker. Same workflow and
// activities in both. Only the lifetime differs.

import { fileURLToPath } from "node:url";
import {
  NativeConnection,
  Worker,
  type WorkerOptions,
} from "@temporalio/worker";
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
  // How long `stop()` waits for in-flight activities before it gives up on them. Unset, it waits
  // for them all. An embedded worker sets it, so quitting pi can't hang on a long tool.
  readonly shutdownForceTime?: WorkerOptions["shutdownForceTime"];
}

export interface SessionWorker {
  readonly worker: Worker;
  // Runs the worker and resolves when it has drained. Call stop() to end it.
  readonly run: () => Promise<void>;
  readonly stop: () => Promise<void>;
}

export async function createSessionWorker(
  opts: SessionWorkerOptions,
): Promise<SessionWorker> {
  const connection = await NativeConnection.connect(
    opts.connect ?? { address: opts.address },
  );
  // This process's own queue, so a step can return to the worker that started it. Computed once
  // here so the poller and the activities can't report different names.
  const stepQueue = queueForWorker(opts.taskQueue, opts.projectDir);
  const activities = {
    ...makeActivities({ ...opts, stepQueue }),
    ...opts.activities,
  };
  // A failed start gives back what it opened. The extension tries again on the next task, and a
  // long-lived pi would otherwise keep a connection per failed attempt.
  let worker: Worker | undefined;
  let hostWorker: Worker;
  try {
    worker = await Worker.create({
      connection,
      namespace: opts.namespace,
      taskQueue: opts.taskQueue,
      workflowsPath: fileURLToPath(new URL("./workflows.ts", import.meta.url)),
      activities,
      shutdownForceTime: opts.shutdownForceTime,
    });
    // Activities only, for work that must run on this host. Without this poller every step on the
    // host queue would wait out schedule-to-start before falling back to the shared queue.
    hostWorker = await Worker.create({
      connection,
      namespace: opts.namespace,
      taskQueue: stepQueue,
      activities,
      shutdownForceTime: opts.shutdownForceTime,
    });
  } catch (err) {
    // A Worker holds its connection until its run ends, so one that never ran is run and shut
    // down at once. Only then does the connection close.
    if (worker) {
      const running = worker.run();
      if (worker.getState() === "RUNNING") worker.shutdown();
      await running.catch(() => {});
    }
    await connection.close().catch(() => {});
    throw err;
  }
  const shared: Worker = worker;

  let running: Promise<void> | undefined;
  let runningHost: Promise<void> | undefined;
  let stopping: Promise<void> | undefined;
  return {
    worker: shared,
    // Settles when either poller fails, or when both end on a shutdown. A dead host-queue poller
    // looks healthy otherwise, and every host-queue unit waits out its queue timeout.
    run: () => {
      runningHost ??= hostWorker.run();
      running ??= shared.run();
      return Promise.all([running, runningHost]).then(() => undefined);
    },
    // Once only. A worker that died is stopped by its owner, which may also stop it on exit.
    stop: () => (stopping ??= stopOnce()),
  };

  async function stopOnce() {
    // Each step runs even if an earlier one failed, so one bad resource can't leak the rest.
    let failure: unknown;
    const attempt = async (step: () => unknown) => {
      try {
        await step();
      } catch (err) {
        failure ??= err;
      }
    };
    // `shutdown()` throws unless the worker is running, e.g. when it already died.
    await attempt(() => shared.getState() === "RUNNING" && shared.shutdown());
    await attempt(() => hostWorker.getState() === "RUNNING" && hostWorker.shutdown());
    // A worker shut down mid-poll, or forced past `shutdownForceTime`, rejects. That's the
    // shutdown, not a failure.
    await running?.catch(() => {});
    await runningHost?.catch(() => {});
    await attempt(() => connection.close());
    if (failure !== undefined) throw failure;
  }
}
