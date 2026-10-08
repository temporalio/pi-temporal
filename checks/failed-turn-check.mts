// Checks that a failed turn reports why the Activity failed, not only that it did, so `watch` and
// `/background` show the reason.
//
// Needs a Temporal server, no model key. Usage: npx tsx checks/failed-turn-check.mts

import { randomUUID } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { ApplicationFailure } from "@temporalio/common";
import { Client, Connection } from "@temporalio/client";
import { NativeConnection, Worker } from "@temporalio/worker";
import { QUERIES, SIGNALS, workflowId } from "../src/core/protocol.js";
import type { TurnState } from "../src/core/protocol.js";

const address = process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7233";
const queue = `pi-failed-turn-${randomUUID().slice(0, 8)}`;
const reason = "session file /elsewhere/s.jsonl is outside the session directory /sessions";

const connection = await Connection.connect({ address });
const client = new Client({ connection, namespace: "default" });
const native = await NativeConnection.connect({ address });
const worker = await Worker.create({
  connection: native,
  namespace: "default",
  taskQueue: queue,
  workflowsPath: fileURLToPath(new URL("../src/workflow-bundle.ts", import.meta.url)),
  activities: {
    async runStep() {
      throw ApplicationFailure.nonRetryable(reason, "SessionOutsideRoot");
    },
    async retireSession() {},
  },
});
const running = worker.run();

let finished: TurnState["finished"];
try {
  const sessionId = `failed-${randomUUID().slice(0, 8)}`;
  const handle = await client.workflow.start("piSession", {
    taskQueue: queue,
    workflowId: workflowId(sessionId),
    args: [{ sessionId, sessionFile: `/unused/${sessionId}.jsonl`, idleTimeout: "1 minute" }],
  });
  await handle.signal(SIGNALS.submitPrompt, { promptId: "p1", text: "run" });
  const deadline = Date.now() + 30_000;
  while (!finished && Date.now() < deadline) {
    finished = (await handle.query<TurnState>(QUERIES.turnState)).finished;
    if (!finished) await sleep(200);
  }
  await handle.terminate().catch(() => undefined);
} finally {
  worker.shutdown();
  await running.catch(() => undefined);
  await connection.close();
  await native.close();
}

const ok = finished?.outcome === "failed" && finished.error === reason;
console.log(
  `${ok ? "PASS" : "FAIL"} a failed turn reports the Activity's own reason` +
    (ok ? "" : ` (${JSON.stringify(finished)})`),
);
console.log(ok ? "failed-turn-check: OK" : "failed-turn-check: 1 failed");
process.exit(ok ? 0 : 1);
