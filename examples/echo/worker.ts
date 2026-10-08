// A Worker for echo sessions, built from `src/core` alone: no Pi, no config, no model key.
// Usage: npx tsx examples/echo/worker.ts

import { realpathSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { makeCoreActivities } from "../../src/core/activities.js";
import { createSessionWorker } from "../../src/core/session-worker.js";
import { echoAgent, type EchoTool } from "./agent.js";

export const settings = () => ({
  address: process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7233",
  namespace: process.env.TEMPORAL_NAMESPACE ?? "default",
  taskQueue: process.env.ECHO_TASK_QUEUE ?? "echo",
  sessionDir: process.env.ECHO_SESSION_DIR ?? join(tmpdir(), "echo-sessions"),
});

/** Starts polling. A check passes its own tool, to stop the Worker while it runs. */
export async function startEchoWorker(tool?: EchoTool) {
  const { address, namespace, taskQueue, sessionDir } = settings();
  const worker = await createSessionWorker({
    address,
    namespace,
    taskQueue,
    // No `workflowsPath`, so it registers the core session Workflow and nothing else.
    // Workflow input is the caller's to pick, so the Worker only writes under its own directory.
    activities: () => makeCoreActivities({ agent: echoAgent({ tool }), sessionRoot: sessionDir }),
  });
  console.log(`echo worker ${process.pid} polling ${taskQueue} on ${address}`);
  return worker;
}

if (process.argv[1] && realpathSync(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const worker = await startEchoWorker();
  await worker.run();
}
