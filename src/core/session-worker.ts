// Builds a Worker for the session Workflows. Used by the standalone Worker and by the pi extension,
// which runs one in-process so `/background` works with no separate Worker. The caller brings the
// Activities, so this file knows nothing about the agent.

import { fileURLToPath } from "node:url";
import {
  NativeConnection,
  Worker,
  type WorkerOptions,
} from "@temporalio/worker";
import { queueForWorker } from "./queue.js";

// A stop reaches a running Activity only with a heartbeat’s answer. By default the SDK sends
// heartbeats about every 24 seconds here, so a stopped tool would run on that long.
const HEARTBEAT_THROTTLE = "3 seconds";

export interface SessionWorkerOptions {
  readonly address: string;
  // API key or mTLS settings, built by `connectionOptions` so client and Worker always agree.
  readonly connect?: Parameters<typeof NativeConnection.connect>[0];
  readonly namespace: string;
  readonly taskQueue: string;
  // The Activities this Worker runs, given its host queue when it has one.
  readonly activities: (hostQueue: string | undefined) => Record<string, unknown>;
  // Also poll a host queue named for this project directory, so a step’s tools and seal can come
  // back to the host that holds the project. Leave it out when nothing is host-bound.
  readonly hostQueueFor?: string;
  // A Workflow bundle built ahead of time (`npm run bundle`). Without one, the Worker bundles the
  // source at start, which is fine for development.
  readonly workflowBundlePath?: string;
  // The same codec as the clients, or they can’t read each other’s payloads.
  readonly dataConverter?: WorkerOptions["dataConverter"];
  // How long `stop()` waits for in-flight Activities before it gives up on them. Unset, it waits
  // for them all. An embedded Worker sets it, so quitting pi can’t hang on a long tool.
  readonly shutdownForceTime?: WorkerOptions["shutdownForceTime"];
}

export interface SessionWorker {
  readonly worker: Worker;
  // Runs the Worker and resolves when it has drained. Call stop() to end it.
  readonly run: () => Promise<void>;
  readonly stop: () => Promise<void>;
}

export async function createSessionWorker(
  opts: SessionWorkerOptions,
): Promise<SessionWorker> {
  const connection = await NativeConnection.connect(
    opts.connect ?? { address: opts.address },
  );
  // This process’s own queue, so a step can come back to the Worker that started it. Computed
  // once here, so the poller and the Activities can’t report different names.
  const hostQueue =
    opts.hostQueueFor === undefined ? undefined : queueForWorker(opts.taskQueue, opts.hostQueueFor);
  const activities = opts.activities(hostQueue);
  // A failed start gives back what it opened. The extension tries again on the next task, and a
  // long-lived pi would otherwise keep a connection per failed attempt.
  let worker: Worker | undefined;
  let hostWorker: Worker | undefined;
  try {
    worker = await Worker.create({
      connection,
      namespace: opts.namespace,
      taskQueue: opts.taskQueue,
      ...(opts.workflowBundlePath
        ? { workflowBundle: { codePath: opts.workflowBundlePath } }
        : { workflowsPath: fileURLToPath(new URL("../workflow-bundle.ts", import.meta.url)) }),
      activities,
      dataConverter: opts.dataConverter,
      shutdownForceTime: opts.shutdownForceTime,
      maxHeartbeatThrottleInterval: HEARTBEAT_THROTTLE,
    });
    // Activities only, for work that must run on this host. Without this poller every step on the
    // host queue would wait out schedule-to-start before falling back to the shared queue.
    if (hostQueue !== undefined) {
      hostWorker = await Worker.create({
        connection,
        namespace: opts.namespace,
        taskQueue: hostQueue,
        activities,
        dataConverter: opts.dataConverter,
        shutdownForceTime: opts.shutdownForceTime,
        maxHeartbeatThrottleInterval: HEARTBEAT_THROTTLE,
      });
    }
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
      runningHost ??= hostWorker?.run();
      running ??= shared.run();
      return Promise.all([running, runningHost]).then(() => undefined);
    },
    // Once only. A Worker that died is stopped by its owner, which may also stop it on exit.
    stop: () => (stopping ??= stopOnce()),
  };

  async function stopOnce() {
    // Each step runs even if an earlier one failed, so one bad resource can’t leak the rest.
    let failure: unknown;
    const attempt = async (step: () => unknown) => {
      try {
        await step();
      } catch (err) {
        failure ??= err;
      }
    };
    // `shutdown()` throws unless the Worker is running, e.g. when it already died.
    await attempt(() => shared.getState() === "RUNNING" && shared.shutdown());
    await attempt(() => hostWorker?.getState() === "RUNNING" && hostWorker.shutdown());
    // A Worker shut down mid-poll, or forced past `shutdownForceTime`, rejects. That’s the
    // shutdown, not a failure.
    await running?.catch(() => {});
    await runningHost?.catch(() => {});
    await attempt(() => connection.close());
    if (failure !== undefined) throw failure;
  }
}
