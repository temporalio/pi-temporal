import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { Client, Connection } from "@temporalio/client";
import { NativeConnection, Worker } from "@temporalio/worker";
import { Context } from "@temporalio/activity";
import type { RunStepInput, TurnState } from "./src/protocol.js";

const address = process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7233";
const namespace = process.env.TEMPORAL_NAMESPACE ?? "default";
const queue = `pi-init-check-${randomUUID()}`;
const pinnedQueue = `${queue}-pinned`;
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
let toolAttempts = 0;
const activities = {
  async adoptProject() {
    startedAdoption();
    await adoption;
  },
  async retireSession() {},
  async runStep(_input: RunStepInput) {
    modelCalls++;
    return { done: true, retryAttempt: 0, finalText: "answered" };
  },
  async runModelCall() {
    return { calls: [{ id: "call", name: "probe" }], sequential: false, ended: false, queue: pinnedQueue };
  },
  async runToolCall() {
    toolAttempts = Math.max(toolAttempts, Context.current().info.attempt);
    throw new Error("retryable probe");
  },
  async sealStep() {
    return { done: true, retryAttempt: 0, finalText: "closed" };
  },
};
const worker = await Worker.create({
  connection: native,
  namespace,
  taskQueue: queue,
  workflowsPath: fileURLToPath(new URL("./src/workflow.ts", import.meta.url)),
  activities,
});
const pinned = await Worker.create({ connection: native, namespace, taskQueue: pinnedQueue, activities });
const running = worker.run();
const runningPinned = pinned.run();
const handles: Array<ReturnType<typeof client.workflow.getHandle>> = [];

try {
  const initialized = await client.workflow.start("piSession", {
    workflowId: `${queue}-adopt`,
    taskQueue: queue,
    args: ["scheduled", "/unused/session.jsonl", {
      idleTimeout: "100 milliseconds",
      template: "/unused/template",
      initialPrompt: { promptId: "scheduled", text: "run" },
    }],
  });
  handles.push(initialized);
  await Promise.race([adoptionStarted, sleep(10_000).then(() => { throw new Error("adoption never started"); })]);
  let state: TurnState | undefined;
  try { state = await initialized.query<TurnState>("turnState"); } catch {}
  check("scheduled initialization answers its turn query", state?.running?.promptId === "scheduled");
  await initialized.signal("interrupt");
  releaseAdoption();
  await initialized.result();
  check("a stop during initialization prevents the model call", modelCalls === 0);

  const retried = await client.workflow.start("piSession", {
    workflowId: `${queue}-retry`,
    taskQueue: queue,
    args: ["retry", "/unused/retry.jsonl", {
      stepped: true,
      idleTimeout: "100 milliseconds",
      initialPrompt: { promptId: "retry", text: "run" },
    }],
  });
  handles.push(retried);
  for (let i = 0; i < 50 && toolAttempts === 0; i++) await sleep(100);
  const history = await retried.fetchHistory();
  const tool = history.events?.find((event) =>
    event.activityTaskScheduledEventAttributes?.activityType?.name === "runToolCall",
  )?.activityTaskScheduledEventAttributes;
  check("pinned dispatch has one attempt before queue fallback", tool?.retryPolicy?.maximumAttempts === 1);
  if (tool?.retryPolicy?.maximumAttempts === 1) await retried.result();
  else await sleep(1200);
  check("a failed pinned attempt is not started again", toolAttempts === 1);
} finally {
  releaseAdoption();
  for (const handle of handles) await handle.terminate("check cleanup").catch(() => {});
  worker.shutdown();
  pinned.shutdown();
  await Promise.all([running, runningPinned]);
  await connection.close();
  await native.close();
}

assert.deepEqual(checks, []);
