// The standalone worker: hosts the piSession workflow and the runStep activity. Run one or many;
// they pull the same task queue, so any worker can drive any session from the shared session
// directory. The pi extension runs the same worker inside pi (see src/session-worker.ts); this
// one is for a fleet, or for keeping tasks moving with no pi open.

import { fromEnv } from "./config.js";
import { createSessionWorker } from "./session-worker.js";

async function main() {
  const cfg = fromEnv();
  const projectDir = process.env.PI_PROJECT_DIR ?? process.cwd();

  const { run } = await createSessionWorker({
    address: cfg.address,
    namespace: cfg.namespace,
    taskQueue: cfg.taskQueue,
    projectDir,
    provider: process.env.PI_TEMPORAL_PROVIDER,
    modelHint: process.env.PI_MODEL,
    apiKey: process.env.OPENAI_API_KEY,
  });

  console.log(`pi-temporal worker on ${cfg.address} / ${cfg.namespace} / ${cfg.taskQueue}`);
  console.log(`sessions: ${cfg.sessionDir}   project: ${projectDir}`);
  await run();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
