// The standalone worker process. Run one or many on the same task queue, and any of them can drive
// any session from the shared session directory. The pi extension runs the same worker in-process.

import { resolve } from "node:path";
import { Runtime } from "@temporalio/worker";
import {
  connectionOptions,
  describe,
  dropFromEnv,
  fromEnv,
  modelApiKey,
  notes,
  preflight,
  TEMPORAL_CREDENTIAL_VARS,
} from "./config.js";
import { createSessionWorker } from "./core/session-worker.js";
import { makeActivities } from "./pi/activities.js";
import { dataConverterFor } from "./core/codec.js";
import * as worktree from "./tree/worktree.js";

async function main() {
  // Prometheus metrics from the SDK's core: task latencies, slots, poll and Activity counts. Set
  // before anything connects, since the runtime is set up once per process.
  const metrics = process.env.PI_TEMPORAL_METRICS;
  if (metrics) {
    Runtime.install({ telemetryOptions: { metrics: { prometheus: { bindAddress: metrics } } } });
  }
  const cfg = fromEnv();
  // Absolute, since git runs in the directory and also names it as the work tree.
  const projectDir = resolve(process.env.PI_PROJECT_DIR ?? process.cwd());

  for (const note of notes(cfg)) console.log(`  note: ${note}`);
  // A project directory that can't be moved aside stays blocked after a lost tool call until it's
  // cleared by hand. This is about the local filesystem, so it's not in `preflight`.
  if (cfg.shipTree) {
    const stuck = await worktree.cannotMoveAside(projectDir);
    if (stuck) {
      console.log(
        `  note: ${projectDir} cannot be set aside (${stuck}). A tool call that never comes back ` +
          `will refuse this directory until \`pi-temporal release-tree\` clears it. Mount the ` +
          `volume above the project rather than at it to avoid that.`,
      );
    }
  }
  const problems = preflight(cfg);
  // Not in `preflight` because only the worker reads `PI_PROJECT_DIR`. The cwd fallback differs per
  // worker, so a fleet would restore trees into unrelated directories.
  if (cfg.profile === "fleet" && !process.env.PI_PROJECT_DIR) {
    problems.push(
      "the fleet profile needs an explicit PI_PROJECT_DIR: the fallback is this worker's own " +
        "working directory, which is a different directory on every worker",
    );
  }
  for (const problem of problems) console.error(`configuration: ${problem}`);
  // Fail at deploy time, not on the first prompt.
  if (problems.length > 0) process.exit(1);

  // Hand back directories held for finished sessions. Otherwise they're only freed lazily.
  if (cfg.shipTree) {
    const freed = await worktree.sweep().catch(() => 0);
    if (freed > 0) console.log(`  handed back ${freed} directories held for finished sessions`);
  }

  const apiKey = modelApiKey(process.env.PI_TEMPORAL_PROVIDER);
  // Tools the agent runs inherit this process's env. They must not see the keys.
  dropFromEnv(TEMPORAL_CREDENTIAL_VARS);
  dropFromEnv([
    "OPENAI_API_KEY",
    "OPENAI_API_KEY_FILE",
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_API_KEY_FILE",
  ]);

  const { run } = await createSessionWorker({
    address: cfg.address,
    connect: connectionOptions(cfg),
    namespace: cfg.namespace,
    taskQueue: cfg.taskQueue,
    hostQueueFor: projectDir,
    dataConverter: dataConverterFor(cfg.codecKey),
    workflowBundlePath: process.env.PI_TEMPORAL_WORKFLOW_BUNDLE,
    activities: (hostQueue) =>
      makeActivities({
        projectDir,
        provider: process.env.PI_TEMPORAL_PROVIDER,
        modelHint: process.env.PI_MODEL,
        apiKey,
        shipTree: cfg.shipTree,
        hostQueue,
      }),
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
