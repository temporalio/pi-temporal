// Checks the session Workflow on a versioned Worker. With Worker Versioning on, each build is a
// Worker Deployment Version, and each Workflow says whether it moves to newer versions. A session
// must say AUTO_UPGRADE, since a long-lived session pinned to its first build would keep that
// build's Workers alive for as long as it runs.
//
// Shows the moving parts: a Worker with `workerDeploymentOptions` (through `createSessionWorker`),
// a version made current, and the behavior the server records for the run.
//
// Needs a Temporal server, no model key. Usage: npx tsx checks/versioning-check.mts

import { randomUUID } from "node:crypto";
import { Client, Connection } from "@temporalio/client";
import proto from "@temporalio/proto";
import { createSessionWorker } from "../src/core/session-worker.js";
import { UPDATES, workflowId } from "../src/core/protocol.js";
import type { Quiet, RunStepInput, RunStepResult, SessionInput } from "../src/core/protocol.js";

const address = process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7233";
const namespace = "default";
const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

const { VersioningBehavior } = proto.temporal.api.enums.v1;
const AUTO_UPGRADE = VersioningBehavior.VERSIONING_BEHAVIOR_AUTO_UPGRADE;
const suffix = randomUUID().slice(0, 8);
const taskQueue = `pi-versioning-${suffix}`;
const deployment = { name: `pi-versioning-${suffix}`, buildId: "build-1" };

const worker = await createSessionWorker({
  address,
  namespace,
  taskQueue,
  deployment,
  activities: () => ({
    async runStep(input: RunStepInput): Promise<RunStepResult> {
      return { done: true, finalText: `answered ${input.promptId}` };
    },
    async retireSession() {},
    async adoptProject() {},
  }),
});
const running = worker.run();
const connection = await Connection.connect({ address });
const client = new Client({ connection, namespace });

try {
  // A version exists once its Worker has polled. Until it's current, no task goes to it.
  for (let tried = 0; ; tried++) {
    try {
      await connection.workflowService.setWorkerDeploymentCurrentVersion({
        namespace,
        deploymentName: deployment.name,
        buildId: deployment.buildId,
        identity: "versioning-check",
      });
      break;
    } catch (err) {
      if (tried >= 60) throw err;
      await new Promise((resolve) => setTimeout(resolve, 500));
    }
  }

  const sessionId = `versioning-${suffix}`;
  const input: SessionInput = { sessionId, sessionFile: `/unused/${sessionId}.jsonl` };
  const handle = await client.workflow.signalWithStart("piSession", {
    taskQueue,
    workflowId: workflowId(sessionId),
    args: [input],
    signal: "submitPrompt",
    signalArgs: [{ promptId: "one", text: "one" }],
  });
  const quiet = await handle.executeUpdate<Quiet, []>(UPDATES.waitForQuiet);
  check("a session on a versioned Worker answers", quiet.finished?.outcome === "answered", quiet);

  const { raw } = await handle.describe();
  const info = raw.workflowExecutionInfo?.versioningInfo;
  check("and the server records it as AUTO_UPGRADE", info?.behavior === AUTO_UPGRADE, info);
  check(
    "on the version that ran it",
    info?.deploymentVersion?.buildId === deployment.buildId &&
      info?.deploymentVersion?.deploymentName === deployment.name,
    info?.deploymentVersion,
  );
  await handle.terminate("versioning-check done");
} finally {
  await worker.stop();
  await running.catch(() => undefined);
  await connection.close();
}

const bad = failures.length;
console.log(bad === 0 ? "versioning-check: OK" : `versioning-check: ${bad} failed`);
process.exit(bad === 0 ? 0 : 1);
