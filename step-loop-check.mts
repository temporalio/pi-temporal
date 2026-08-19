// Checks the executor's shape: one activity per step, the loop stops when a step reports done,
// and an interrupt ends the turn without ending the session. The runStep activity is stubbed, so
// this needs a Temporal server but no model key. Usage: tsx step-loop-check.mts

import { randomUUID } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { Client, Connection } from "@temporalio/client";
import { NativeConnection, Worker } from "@temporalio/worker";
import { fromEnv, sessionFileFor } from "./src/config.js";
import { WORKFLOW_TYPE, workflowId } from "./src/protocol.js";
import type { PromptInput, RunStepInput, RunStepResult, SessionTurnOptions } from "./src/protocol.js";

const STEPS_TO_ANSWER = 3;
const cfg = fromEnv();
const taskQueue = `pi-step-check-${randomUUID().slice(0, 8)}`;
const options: SessionTurnOptions = { idleTimeout: "10 seconds" };

// What the stub saw, per prompt: the step number the workflow asked for, in order.
const seen = new Map<string, number[]>();
const stepsFor = (promptId: string) => seen.get(promptId) ?? [];

const activities = {
  async runStep(input: RunStepInput): Promise<RunStepResult> {
    const steps = seen.get(input.promptId) ?? [];
    steps.push(input.step);
    seen.set(input.promptId, steps);

    if (input.text === "hang") {
      await sleep(60_000);
      return { done: true, finalText: "" };
    }
    const done = steps.length === STEPS_TO_ANSWER;
    return { done, finalText: done ? "answer" : "" };
  },
};

async function waitFor(what: string, ok: () => boolean, ms = 20_000) {
  for (let waited = 0; waited < ms; waited += 200) {
    if (ok()) return;
    await sleep(200);
  }
  throw new Error(`timed out waiting for ${what}`);
}

async function main() {
  const nativeConnection = await NativeConnection.connect({ address: cfg.address });
  const worker = await Worker.create({
    connection: nativeConnection,
    namespace: cfg.namespace,
    taskQueue,
    workflowsPath: fileURLToPath(new URL("./src/workflow.ts", import.meta.url)),
    activities,
  });
  const running = worker.run();

  const connection = await Connection.connect({ address: cfg.address });
  const client = new Client({ connection, namespace: cfg.namespace });

  const sessionId = `step-check-${randomUUID().slice(0, 8)}`;
  const send = async (prompt: PromptInput) =>
    client.workflow.signalWithStart(WORKFLOW_TYPE, {
      taskQueue,
      workflowId: workflowId(sessionId),
      args: [sessionId, sessionFileFor(cfg.sessionDir, sessionId), options],
      signal: "submitPrompt",
      signalArgs: [prompt],
    });

  const failures: string[] = [];
  const check = (what: string, ok: boolean, detail: unknown) => {
    console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
    if (!ok) failures.push(what);
  };

  // A turn that needs three steps must cost three activities, numbered in order.
  const looping = { promptId: randomUUID(), text: "loop" };
  await send(looping);
  await waitFor("the turn to answer", () => stepsFor(looping.promptId).length >= STEPS_TO_ANSWER);
  await sleep(1000); // a fourth step would land in this window
  check("one activity per step, in order", JSON.stringify(stepsFor(looping.promptId)) === "[1,2,3]", stepsFor(looping.promptId));

  // An interrupt ends the turn, not the session.
  const hanging = { promptId: randomUUID(), text: "hang" };
  await send(hanging);
  await waitFor("the hanging step to start", () => stepsFor(hanging.promptId).length === 1);
  await client.workflow.getHandle(workflowId(sessionId)).signal("interrupt");
  await sleep(1000);
  check("the interrupt stopped the turn", stepsFor(hanging.promptId).length === 1, stepsFor(hanging.promptId));

  const after = { promptId: randomUUID(), text: "loop" };
  await send(after);
  await waitFor("the session to serve a later prompt", () => stepsFor(after.promptId).length >= STEPS_TO_ANSWER);
  check("the session survived the interrupt", JSON.stringify(stepsFor(after.promptId)) === "[1,2,3]", stepsFor(after.promptId));

  await client.workflow.getHandle(workflowId(sessionId)).terminate("step-loop-check done");
  worker.shutdown();
  await running;
  await connection.close();
  await nativeConnection.close();

  console.log(failures.length === 0 ? "step-loop-check: OK" : `step-loop-check: ${failures.length} failed`);
  process.exit(failures.length === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
