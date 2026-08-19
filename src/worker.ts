// The worker: hosts the piSession workflow and the runStep activity. Run one or many; they pull
// the same task queue, so any worker can drive any session from the shared session directory.

import { fileURLToPath } from "node:url";
import { NativeConnection, Worker } from "@temporalio/worker";
import { fromEnv } from "./config.js";
import { makeActivities } from "./activities.js";

async function main() {
  const cfg = fromEnv();
  const connection = await NativeConnection.connect({ address: cfg.address });
  const projectDir = process.env.PI_PROJECT_DIR ?? process.cwd();
  const openaiKey = process.env.OPENAI_API_KEY;
  const modelHint = process.env.PI_MODEL;

  const worker = await Worker.create({
    connection,
    namespace: cfg.namespace,
    taskQueue: cfg.taskQueue,
    workflowsPath: fileURLToPath(new URL("./workflow.ts", import.meta.url)),
    activities: makeActivities({ projectDir, openaiKey, modelHint }),
  });

  console.log(`pi-temporal worker on ${cfg.address} / ${cfg.namespace} / ${cfg.taskQueue}`);
  console.log(`sessions: ${cfg.sessionDir}   project: ${projectDir}`);
  await worker.run();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
