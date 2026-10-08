// Checks that bad session start options are refused up front. A raw start fails the Workflow with
// a readable reason instead of failing every Workflow Task, and core `sendPrompt` throws before
// sending anything.
//
// Needs a Temporal server, no model key. Usage: npx tsx checks/session-input-check.mts

import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import { Client, Connection } from "@temporalio/client";
import { NativeConnection, Worker } from "@temporalio/worker";
import { sendPrompt, sessionExists } from "../src/core/client.js";
import { workflowId } from "../src/core/protocol.js";
import type { SessionInput } from "../src/core/protocol.js";

const address = process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7233";
const queue = `pi-session-input-${randomUUID().slice(0, 8)}`;

const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${String(detail)})`}`);
  if (!ok) failures.push(what);
};

const connection = await Connection.connect({ address });
const client = new Client({ connection, namespace: "default" });
const native = await NativeConnection.connect({ address });
const worker = await Worker.create({
  connection: native,
  namespace: "default",
  taskQueue: queue,
  workflowsPath: fileURLToPath(new URL("../src/workflow-bundle.ts", import.meta.url)),
  activities: { async retireSession() {} },
});
const running = worker.run();

const bad: [string, Partial<SessionInput>, RegExp][] = [
  ["an idle timeout that doesn't parse", { idleTimeout: "5 minuets" }, /idleTimeout/],
  ["a tool timeout that isn't positive", { toolTimeoutMinutes: -1 }, /toolTimeoutMinutes/],
  ["a budget value that isn't a number", { budget: { hardSeconds: "1" as never } }, /hardSeconds/],
  ["a negative budget value", { budget: { sessionTokens: -5 } }, /sessionTokens/],
];

try {
  for (const [what, options, reason] of bad) {
    const sessionId = `input-${randomUUID().slice(0, 8)}`;
    const input = { sessionId, sessionFile: `/unused/${sessionId}.jsonl`, ...options };

    // A raw start, as a client that skips `sendPrompt` makes.
    const handle = await client.workflow.start("piSession", {
      taskQueue: queue,
      workflowId: workflowId(`${sessionId}-raw`),
      args: [input],
    });
    // A bad value that reaches the session loop fails Workflow Tasks, and the run never closes.
    const outcome = await Promise.race([
      handle.result().then(
        () => "completed",
        (err: Error & { cause?: Error }) => err.cause?.message ?? err.message,
      ),
      new Promise<string>((resolve) => setTimeout(() => resolve("still open"), 10_000)),
    ]);
    await handle.terminate().catch(() => undefined);
    check(`the session refuses ${what}`, reason.test(outcome), outcome);

    // `sendPrompt` throws first, so no session starts.
    const sent = await sendPrompt(
      client,
      { taskQueue: queue, sessionId, input: input as SessionInput },
      { promptId: "p1", text: "run" },
    ).then(
      () => "sent",
      (err: Error) => err.message,
    );
    const started = await sessionExists(client, sessionId);
    if (started) await client.workflow.getHandle(workflowId(sessionId)).terminate();
    check(`sendPrompt refuses ${what}`, reason.test(sent) && started === false, sent);
  }
} finally {
  worker.shutdown();
  await running.catch(() => undefined);
  await connection.close();
  await native.close();
}

const failed = failures.length;
console.log(failed === 0 ? "session-input-check: OK" : `session-input-check: ${failed} failed`);
process.exit(failed === 0 ? 0 : 1);
