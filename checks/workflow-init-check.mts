// Checks `piSession` startup and host-queue dispatch against a live Temporal server. Interrupts
// during adoption and asserts no model call runs. Fails a host-queue tool attempt and asserts it is
// neither retried nor moved to the shared queue.

import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { Client, Connection } from "@temporalio/client";
import { NativeConnection, Worker } from "@temporalio/worker";
import { Context } from "@temporalio/activity";
import type { RunStepInput, TurnState } from "../src/core/protocol.js";

const address = process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7233";
const namespace = process.env.TEMPORAL_NAMESPACE ?? "default";
const queue = `pi-init-check-${randomUUID()}`;
const hostQueue = `${queue}-host`;
const connection = await Connection.connect({ address });
const native = await NativeConnection.connect({ address });
const client = new Client({ connection, namespace });
const checks: string[] = [];
const check = (name: string, pass: boolean) => {
  console.log(`${pass ? "PASS" : "FAIL"} ${name}`);
  if (!pass) checks.push(name);
};

let releaseAdoption!: () => void;
const adoption = new Promise<void>((resolve) => { releaseAdoption = resolve; });
let startedAdoption!: () => void;
const adoptionStarted = new Promise<void>((resolve) => { startedAdoption = resolve; });
let modelCalls = 0;
let hostAttempts = 0;
let sharedTools = 0;
const activities = {
  async adoptProject() {
    startedAdoption();
    await adoption;
  },
  async retireSession() {},
  async runStep(_input: RunStepInput) {
    modelCalls++;
    return { done: true, finalText: "answered" };
  },
  async runModelCall() {
    return {
      calls: [{ id: "call", name: "probe" }],
      sequential: false,
      ended: false,
      queue: hostQueue,
    };
  },
  async runToolCall() {
    throw new Error("a tool call must be answered by the queue it was addressed to");
  },
  async sealStep() {
    return { done: true, finalText: "closed" };
  },
};
const worker = await Worker.create({
  connection: native,
  namespace,
  taskQueue: queue,
  workflowsPath: fileURLToPath(new URL("../src/core/workflow.ts", import.meta.url)),
  activities: {
    ...activities,
    async runToolCall() {
      sharedTools++;
      return { outcome: "unknown" as const };
    },
  },
});
// A started failure must remain distinguishable from an unclaimed dispatch.
const host = await Worker.create({
  connection: native,
  namespace,
  taskQueue: hostQueue,
  activities: {
    ...activities,
    async runToolCall() {
      hostAttempts = Math.max(hostAttempts, Context.current().info.attempt);
      throw new Error("retryable probe");
    },
  },
});
const running = worker.run();
const runningHost = host.run();
const handles: Array<ReturnType<typeof client.workflow.getHandle>> = [];

try {
  const initialized = await client.workflow.start("piSession", {
    workflowId: `${queue}-adopt`,
    taskQueue: queue,
    args: [{
      sessionId: "scheduled",
      sessionFile: "/unused/session.jsonl",
      idleTimeout: "100 milliseconds",
      template: "/unused/template",
      initialPrompt: { promptId: "scheduled", text: "run" },
    }],
  });
  handles.push(initialized);
  await Promise.race([
    adoptionStarted,
    sleep(10_000).then(() => {
      throw new Error("adoption never started");
    }),
  ]);
  let state: TurnState | undefined;
  try { state = await initialized.query<TurnState>("turnState"); } catch {}
  check(
    "scheduled initialization answers its turn query",
    state?.running?.promptId === "scheduled",
  );
  await initialized.signal("interrupt");
  releaseAdoption();
  await initialized.result();
  check("a stop during initialization prevents the model call", modelCalls === 0);

  const retried = await client.workflow.start("piSession", {
    workflowId: `${queue}-retry`,
    taskQueue: queue,
    args: [{
      sessionId: "retry",
      sessionFile: "/unused/retry.jsonl",
      stepped: true,
      idleTimeout: "100 milliseconds",
      initialPrompt: { promptId: "retry", text: "run" },
    }],
  });
  handles.push(retried);
  for (let i = 0; i < 50 && hostAttempts === 0; i++) await sleep(100);
  const history = await retried.fetchHistory();
  const tool = history.events?.find((event) =>
    event.activityTaskScheduledEventAttributes?.activityType?.name === "runToolCall",
  )?.activityTaskScheduledEventAttributes;
  check("host-queue dispatch permits only one attempt", tool?.retryPolicy?.maximumAttempts === 1);
  await Promise.race([retried.result(), sleep(30_000)]);
  check("a failed host-queue attempt is not started again", hostAttempts === 1);
  check("a started failure does not migrate the step", sharedTools === 0);
} finally {
  releaseAdoption();
  for (const handle of handles) await handle.terminate("check cleanup").catch(() => {});
  worker.shutdown();
  host.shutdown();
  await Promise.all([running, runningHost]);
  await connection.close();
  await native.close();
}

assert.deepEqual(checks, []);
