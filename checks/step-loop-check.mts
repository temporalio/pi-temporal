// Checks the workflow step loop against a live server with stubbed activities. Asserts one step
// at a time until done, and that an interrupt ends the turn but not the session. Runs the same
// checks in whole-step and stepped mode.
//
// Needs a Temporal server, no model key. Usage: tsx checks/step-loop-check.mts

import { randomUUID } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { Client, Connection } from "@temporalio/client";
import { Context } from "@temporalio/activity";
import { NativeConnection, Worker } from "@temporalio/worker";
import { fromEnv, sessionFileFor } from "../src/config.js";
import { WORKFLOW_TYPE, workflowId } from "../src/protocol.js";
import type {
  ModelCallResult,
  PromptInput,
  RunStepInput,
  RunStepResult,
  SealStepInput,
  SessionTurnOptions,
  ToolCallInput,
  ToolCallResult,
} from "../src/protocol.js";

const STEPS_TO_ANSWER = 3;
const cfg = fromEnv();

// Sessions whose tool calls should hang, so the interrupt has something in flight to cancel.
const hangingCalls = new Set<string>();

// Waits like a real activity: it heartbeats, so a stop reaches it, and it ends when stopped.
async function stoppable(ms: number) {
  const ctx = Context.current();
  const beat = setInterval(() => ctx.heartbeat(), 200);
  try {
    await ctx.sleep(ms);
  } finally {
    clearInterval(beat);
  }
}


const failures: string[] = [];
const check = (what: string, ok: boolean, detail: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

async function waitFor(what: string, ok: () => boolean, ms = 20_000) {
  for (let waited = 0; waited < ms; waited += 200) {
    if (ok()) return;
    await sleep(200);
  }
  throw new Error(`timed out waiting for ${what}`);
}

/** Stubs for one mode. Both record the step numbers each prompt was driven through. */
function stubs(stepped: boolean) {
  const seen = new Map<string, number[]>();
  const stepsFor = (promptId: string) => seen.get(promptId) ?? [];
  const note = (input: RunStepInput) => {
    const steps = seen.get(input.promptId) ?? [];
    steps.push(input.step);
    seen.set(input.promptId, steps);
    return steps.length;
  };

  const wholeStep = {
    async runStep(input: RunStepInput): Promise<RunStepResult> {
      const count = note(input);
      if (input.text === "hang") {
        await stoppable(60_000);
        return { done: true, retryAttempt: 0, finalText: "" };
      }
      const done = count === STEPS_TO_ANSWER;
      return { done, retryAttempt: 0, finalText: done ? "answer" : "" };
    },
  };

  // Stepped mode counts at the model call and hangs in the tool call.
  const split = {
    async runModelCall(input: RunStepInput): Promise<ModelCallResult> {
      note(input);
      return {
        calls: [{ id: `call-${input.step}`, name: "probe" }],
        sequential: false,
        ended: false,
      };
    },
    async runToolCall(input: ToolCallInput): Promise<ToolCallResult> {
      // The prompt text does not reach a tool call, so hanging is keyed by session.
      if (hangingCalls.has(input.sessionId)) await stoppable(60_000);
      return { outcome: "settled" };
    },
    async sealStep(input: SealStepInput): Promise<RunStepResult> {
      const done = input.step === STEPS_TO_ANSWER;
      return { done, retryAttempt: 0, finalText: done ? "answer" : "" };
    },
  };

  return { activities: stepped ? split : wholeStep, stepsFor };
}

async function runMode(stepped: boolean) {
  const label = stepped ? "stepped" : "whole-step";
  console.log(`\n--- ${label} ---`);

  const taskQueue = `pi-step-check-${randomUUID().slice(0, 8)}`;
  const options: SessionTurnOptions = { idleTimeout: "10 seconds", stepped };
  const { activities, stepsFor } = stubs(stepped);

  const nativeConnection = await NativeConnection.connect({ address: cfg.address });
  const worker = await Worker.create({
    connection: nativeConnection,
    namespace: cfg.namespace,
    taskQueue,
    workflowsPath: fileURLToPath(new URL("../src/workflow.ts", import.meta.url)),
    activities,
    maxHeartbeatThrottleInterval: "1 second",
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

  // A turn that needs three steps must cost three steps, numbered in order.
  const looping = { promptId: randomUUID(), text: "loop" };
  await send(looping);
  await waitFor("the turn to answer", () => stepsFor(looping.promptId).length >= STEPS_TO_ANSWER);
  await sleep(1000); // a fourth step would land in this window
  const looped = JSON.stringify(stepsFor(looping.promptId)) === "[1,2,3]";
  check(`${label}: one step at a time, in order`, looped, stepsFor(looping.promptId));

  // An interrupt ends the turn, not the session.
  hangingCalls.add(sessionId);
  const hanging = { promptId: randomUUID(), text: "hang" };
  await send(hanging);
  await waitFor("the hanging step to start", () => stepsFor(hanging.promptId).length === 1);
  await client.workflow.getHandle(workflowId(sessionId)).signal("interrupt");
  await sleep(1000);
  const held = stepsFor(hanging.promptId).length === 1;
  check(`${label}: the interrupt stopped the turn`, held, stepsFor(hanging.promptId));
  hangingCalls.delete(sessionId);

  const after = { promptId: randomUUID(), text: "loop" };
  await send(after);
  await waitFor("the session to serve a later prompt", () => {
    return stepsFor(after.promptId).length >= STEPS_TO_ANSWER;
  });
  const served = JSON.stringify(stepsFor(after.promptId)) === "[1,2,3]";
  check(`${label}: the session survived the interrupt`, served, stepsFor(after.promptId));

  await client.workflow.getHandle(workflowId(sessionId)).terminate("step-loop-check done");
  worker.shutdown();
  await running;
  await connection.close();
  await nativeConnection.close();
}

async function main() {
  await runMode(false);
  await runMode(true);

  const bad = failures.length;
  console.log(bad === 0 ? "\nstep-loop-check: OK" : `\nstep-loop-check: ${bad} failed`);
  process.exit(failures.length === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
