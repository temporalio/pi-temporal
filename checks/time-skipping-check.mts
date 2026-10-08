// Checks the session Workflow's timers with Temporal's time-skipping test server, so hours of
// idle time pass in a moment. Shows the SDK's way to test timer paths: an idle session exits
// after its idle timeout and records the session's time on the way out, and a prompt that comes
// just before the timeout keeps the session going. Activities are stubs.
//
// Needs no server: the test server is downloaded on first use. Usage:
// npx tsx checks/time-skipping-check.mts

import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import { Worker } from "@temporalio/worker";
import { SIGNALS, UPDATES, type Quiet, type RetireInput } from "../src/core/protocol.js";
import type { RunStepInput, RunStepResult, SessionInput } from "../src/core/protocol.js";

const retired: RetireInput[] = [];
const activities = {
  async runStep(input: RunStepInput): Promise<RunStepResult> {
    return { done: true, finalText: `answered ${input.text}` };
  },
  async retireSession(input: RetireInput) {
    retired.push(input);
  },
  async adoptProject() {},
};

const env = await TestWorkflowEnvironment.createTimeSkipping();
const taskQueue = `pi-time-${randomUUID().slice(0, 8)}`;
const worker = await Worker.create({
  connection: env.nativeConnection,
  taskQueue,
  workflowsPath: fileURLToPath(new URL("../src/workflow-bundle.ts", import.meta.url)),
  activities,
});

try {
  await worker.runUntil(async () => {
    const start = (id: string): SessionInput => ({
      sessionId: id,
      sessionFile: `/unused/${id}.jsonl`,
      idleTimeout: "1 hour",
    });

    // An hour of idle time, skipped. The session exits and leaves its time in the record.
    const idle = await env.client.workflow.start("piSession", {
      taskQueue,
      workflowId: `idle-${randomUUID()}`,
      args: [start("idle")],
    });
    await idle.signal(SIGNALS.submitPrompt, { promptId: "one", text: "one" });
    const began = Date.now();
    await idle.result();
    assert.ok(Date.now() - began < 30_000, "the idle hour was not skipped");
    assert.equal(retired.at(-1)?.turn, "one");
    console.log("PASS an idle session exits after its timeout, and records its time on the way");

    // A prompt 59 minutes in resets the wait, so the session is still there to answer it.
    const kept = await env.client.workflow.start("piSession", {
      taskQueue,
      workflowId: `kept-${randomUUID()}`,
      args: [start("kept")],
    });
    await kept.signal(SIGNALS.submitPrompt, { promptId: "first", text: "first" });
    await kept.executeUpdate<Quiet, []>(UPDATES.waitForQuiet);
    await env.sleep("59 minutes");
    await kept.signal(SIGNALS.submitPrompt, { promptId: "second", text: "second" });
    const quiet = await kept.executeUpdate<Quiet, []>(UPDATES.waitForQuiet);
    assert.equal(quiet.finished?.promptId, "second");
    console.log("PASS a prompt just before the timeout keeps the session going");
    await kept.result();
  });
} finally {
  await env.teardown();
}
