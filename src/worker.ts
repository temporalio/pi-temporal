// The standalone worker: hosts the piSession workflow and the runStep activity. Run one or many;
// they pull the same task queue, so any worker can drive any session from the shared session
// directory. The pi extension runs the same worker inside pi (see src/session-worker.ts); this
// one is for a fleet, or for keeping tasks moving with no pi open.

import { connectionOptions, describe, fromEnv, preflight } from "./config.js";
import { createSessionWorker } from "./session-worker.js";

async function main() {
  const cfg = fromEnv();
  const projectDir = process.env.PI_PROJECT_DIR ?? process.cwd();

  const problems = preflight(cfg);
  for (const problem of problems) console.error(`configuration: ${problem}`);
  // A worker that starts anyway is one that accepts work it cannot do, and the failure lands on
  // whoever prompted it rather than on whoever deployed it.
  if (problems.length > 0) process.exit(1);

  const { run } = await createSessionWorker({
    address: cfg.address,
    connect: connectionOptions(cfg),
    namespace: cfg.namespace,
    taskQueue: cfg.taskQueue,
    projectDir,
    provider: process.env.PI_TEMPORAL_PROVIDER,
    modelHint: process.env.PI_MODEL,
    apiKey: process.env.OPENAI_API_KEY,
    shipTree: cfg.shipTree,
  });

  console.log("pi-temporal worker");
  for (const [name, value] of Object.entries(describe(cfg))) console.log(`  ${name}: ${value}`);
  console.log(`  project: ${projectDir}`);
  await run();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
