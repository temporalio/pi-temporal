// Not a check. Records the histories `replay-check` replays, one per kind of session run, into
// `checks/histories/`, named with today's date so older ones are kept. Run it when a change to the
// Workflows is meant to change what they do, and commit the new files next to the old ones. A
// history from an older release that no longer replays means running sessions of that release
// would break on upgrade.
//
// Needs a Temporal server, no model key. Usage: npx tsx checks/record-histories.mts

import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { promisify } from "node:util";
import { mkdir, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { Context } from "@temporalio/activity";
import { Client, Connection, type WorkflowHandle } from "@temporalio/client";
import { NativeConnection, Worker } from "@temporalio/worker";
import { SIGNALS, UPDATES, workflowId } from "../src/core/protocol.js";
import type {
  ModelCallResult,
  Quiet,
  RunStepInput,
  RunStepResult,
  SessionInput,
  ToolCallInput,
  ToolCallResult,
} from "../src/core/protocol.js";

const address = process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7233";
const queue = `pi-histories-${randomUUID().slice(0, 8)}`;
const out = fileURLToPath(new URL("./histories/", import.meta.url));

// Waits like a real Activity: it heartbeats, so a stop reaches it, and it ends when stopped.
async function stoppable(ms: number) {
  const ctx = Context.current();
  const beat = setInterval(() => ctx.heartbeat(), 200);
  try {
    await ctx.sleep(ms);
  } finally {
    clearInterval(beat);
  }
}

const steps = new Map<string, number>();
const activities = {
  // Two steps to an answer, so a history shows the loop.
  async runStep(input: RunStepInput): Promise<RunStepResult> {
    const taken = (steps.get(input.promptId) ?? 0) + 1;
    steps.set(input.promptId, taken);
    if (input.text.includes("hang")) await stoppable(60_000);
    return { done: taken >= 2, finalText: taken >= 2 ? "answered" : "", agentState: { taken } };
  },
  async runModelCall(input: RunStepInput): Promise<ModelCallResult> {
    const calls = input.step === 1 ? [{ id: "a", name: "probe" }, { id: "b", name: "probe" }] : [];
    return { calls, sequential: false, ended: calls.length === 0 };
  },
  async runToolCall(input: ToolCallInput): Promise<ToolCallResult> {
    if (input.sessionId.includes("stopped")) await stoppable(60_000);
    return { outcome: "settled" };
  },
  async sealStep(): Promise<RunStepResult> {
    return { done: true, finalText: "closed" };
  },
  async retireSession() {},
  async adoptProject() {},
};

const connection = await Connection.connect({ address });
const client = new Client({ connection, namespace: "default" });
const native = await NativeConnection.connect({ address });
const worker = await Worker.create({
  connection: native,
  namespace: "default",
  taskQueue: queue,
  workflowsPath: fileURLToPath(new URL("../src/workflow-bundle.ts", import.meta.url)),
  activities,
  maxHeartbeatThrottleInterval: "1 second",
});
const running = worker.run();

const session = (name: string, options: Partial<SessionInput> = {}) =>
  client.workflow.start("piSession", {
    taskQueue: queue,
    workflowId: workflowId(`${name}-${randomUUID().slice(0, 8)}`),
    args: [
      {
        sessionId: name,
        sessionFile: `/unused/${name}.jsonl`,
        idleTimeout: "1 second",
        ...options,
      },
    ],
  });

const run = promisify(execFile);

// The CLI's JSON, the same form `temporal workflow show` gives an operator.
const save = async (name: string, handle: WorkflowHandle, runId?: string) => {
  await handle.result().catch(() => undefined);
  const { stdout } = await run("temporal", [
    "workflow", "show", "--workflow-id", handle.workflowId,
    ...(runId ? ["--run-id", runId] : []), "--output", "json", "--address", address,
  ], { maxBuffer: 64 * 1024 * 1024 });
  // Never overwrites. An older file stands for sessions an older release may still be running.
  const today = new Date().toISOString().slice(0, 10);
  await writeFile(`${out}${name}-${today}.json`, stdout, { flag: "wx" });
  console.log(`recorded ${name}`);
};

try {
  await mkdir(out, { recursive: true });

  // A whole-step turn, a resent prompt the session drops, and an idle exit.
  const whole = await session("whole-step");
  await whole.executeUpdate(UPDATES.submit, { args: [{ promptId: "p1", text: "run" }] });
  await whole.signal(SIGNALS.submitPrompt, { promptId: "p1", text: "run" });
  await save("whole-step", whole);

  // A stepped turn: a model call, two tool calls, a seal.
  const stepped = await session("stepped", { stepped: true });
  await stepped.signal(SIGNALS.submitPrompt, { promptId: "p1", text: "run" });
  await save("stepped", stepped);

  // A stop while a tool runs: the tool stops, the recovery seal records it.
  const stopped = await session("stopped", { stepped: true });
  await stopped.signal(SIGNALS.submitPrompt, { promptId: "p1", text: "run" });
  await new Promise((resolve) => setTimeout(resolve, 2_000));
  await stopped.signal(SIGNALS.interrupt);
  await save("stopped", stopped);

  // A hard deadline that ends a step in the middle.
  const deadline = await session("deadline", { budget: { hardSeconds: 1 } });
  await deadline.signal(SIGNALS.submitPrompt, { promptId: "p1", text: "hang" });
  await save("deadline", deadline);

  // Continue-As-New between turns, with a prompt carried over.
  const carried = await session("continue-as-new", { maxHistory: 20 });
  await carried.signal(SIGNALS.submitPrompt, { promptId: "p1", text: "one" });
  await carried.executeUpdate<Quiet, []>(UPDATES.waitForQuiet);
  await carried.signal(SIGNALS.submitPrompt, { promptId: "p2", text: "two" });
  await save("continue-as-new", carried, carried.firstExecutionRunId);
} finally {
  worker.shutdown();
  await running.catch(() => {});
  await connection.close();
  await native.close();
}
