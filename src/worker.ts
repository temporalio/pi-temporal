// The standalone worker: hosts the piSession workflow and the runStep activity. Run one or many;
// they pull the same task queue, so any worker can drive any session from the shared session
// directory. The pi extension runs the same worker inside pi (see src/session-worker.ts); this
// one is for a fleet, or for keeping tasks moving with no pi open.

import { connectionOptions, describe, fromEnv, modelApiKey, notes, preflight } from "./config.js";
import { createSessionWorker } from "./session-worker.js";
import * as worktree from "./worktree.js";

async function main() {
  const cfg = fromEnv();
  const projectDir = process.env.PI_PROJECT_DIR ?? process.cwd();

  for (const note of notes(cfg)) console.log(`  note: ${note}`);
  // Said here rather than in `preflight`, because it is about this worker's own filesystem and not
  // about its configuration. A directory that cannot be moved out of the way is one a tool call
  // that never comes back takes out of service until somebody clears it by hand; one that can be
  // moved costs nothing and strands nothing.
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
  // The worker's own check rather than `preflight`'s: `PI_PROJECT_DIR` is read here, and a client
  // has no use for it. The fallback is this process's working directory, which is a different
  // directory on every worker, so with the tree on it is either refused on every restore (it holds
  // files no session shipped) or quietly adopted as a managed project directory.
  if (cfg.profile === "fleet" && !process.env.PI_PROJECT_DIR) {
    problems.push(
      "the fleet profile needs an explicit PI_PROJECT_DIR: the fallback is this worker's own " +
        "working directory, which is a different directory on every worker",
    );
  }
  for (const problem of problems) console.error(`configuration: ${problem}`);
  // A worker that starts anyway is one that accepts work it cannot do, and the failure lands on
  // whoever prompted it rather than on whoever deployed it.
  if (problems.length > 0) process.exit(1);

  // Directories this host held for sessions that have since finished. The lazy path frees one when
  // another session wants that same directory, which is enough to keep serving and not enough to
  // keep tidy.
  if (cfg.shipTree) {
    const freed = await worktree.sweep().catch(() => 0);
    if (freed > 0) console.log(`  handed back ${freed} directories held for finished sessions`);
  }

  const { run } = await createSessionWorker({
    address: cfg.address,
    connect: connectionOptions(cfg),
    namespace: cfg.namespace,
    taskQueue: cfg.taskQueue,
    projectDir,
    provider: process.env.PI_TEMPORAL_PROVIDER,
    modelHint: process.env.PI_MODEL,
    apiKey: modelApiKey(process.env.PI_TEMPORAL_PROVIDER),
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
