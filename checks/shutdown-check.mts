// Checks how each kind of cancel reaches a running step. The SDK cancels running Activities when
// their Worker shuts down, and when the attempt times out, is paused, is reset, or is gone from
// the server (`notFound`, as after the Workflow is terminated). A shutdown must leave the step
// running, so it finishes. The others abort the model call or tool, but the attempt is no longer
// the step's, so nothing it does may land in the session, and it is not a user stop. Only a cancel
// the Workflow asked for stops the turn and records that.
//
// Runs `runStep` and `runToolCall` under `MockActivityEnvironment`, the SDK's way to give an
// Activity a context without a Worker. No server and no model key.
// Usage: npx tsx checks/shutdown-check.mts

import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { AgentSession } from "@earendil-works/pi-coding-agent";
import { ActivityCancellationDetails } from "@temporalio/common";
import { MockActivityEnvironment } from "@temporalio/testing";
import { makeActivities } from "../src/pi/activities.js";
import * as pending from "../src/core/pending.js";
import type { RunStepResult } from "../src/core/protocol.js";

const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

const root = await mkdtemp(join(tmpdir(), "pi-shutdown-"));
let files = 0;

// Waits for the signal or a moment, whichever comes first. Reports whether it was aborted.
const work = (signal: AbortSignal | undefined, ms: number) =>
  new Promise<boolean>((resolve) => {
    signal?.addEventListener("abort", () => resolve(true), { once: true });
    setTimeout(() => resolve(signal?.aborted === true), ms);
  });

/**
 * A fake Pi session that asks the guard before every append, as Pi does, and lists what it wrote.
 * With `tool`, the model call asks for one tool call at once and the tool takes a moment.
 * Otherwise the model call takes the moment.
 */
function fakeSession(tool: boolean) {
  const seen = { modelAborted: false, toolAborted: false, writes: [] as string[] };
  const call = { type: "toolCall", id: "call-1", name: "bash", arguments: {} };
  const openSession = async (_file: string, guard?: () => void) =>
    ({
      state: { messages: [{ role: "assistant", content: tool ? [call] : "done" }] },
      prepareStep: () => true,
      recordPrompt: async () => true,
      modelCall: async (options?: { signal?: AbortSignal }) => {
        if (!tool) seen.modelAborted = await work(options?.signal, 200);
        guard?.();
        seen.writes.push(seen.modelAborted ? "aborted response" : "response");
        return tool
          ? { toolCalls: [{ id: call.id, name: call.name }], sequential: false, ended: false }
          : { toolCalls: [], sequential: false, ended: seen.modelAborted };
      },
      runToolCall: async (_id: string, options?: { signal?: AbortSignal }) => {
        seen.toolAborted = await work(options?.signal, 200);
        return { toolCallId: call.id, aborted: seen.toolAborted };
      },
      sealStep: async (outcomes: unknown[], options: { postRun?: boolean }) => {
        guard?.();
        seen.writes.push(`seal of ${outcomes.length} (postRun ${String(options.postRun)})`);
        return { done: true, retryAttempt: 0, overflowRecoveryAttempted: false };
      },
      async waitForIdle() {},
      getSessionStats: () => ({ tokens: { total: 0 }, cost: 0 }),
      dispose() {},
    }) as unknown as AgentSession;
  return { seen, openSession };
}

type Reason = "WORKER_SHUTDOWN" | "CANCELLED" | "TIMED_OUT" | "PAUSED" | "RESET" | "NOT_FOUND";

/** Runs one whole step and cancels the Activity partway through. */
async function cancelledStep(details: ActivityCancellationDetails, reason: Reason, tool = false) {
  const { seen, openSession } = fakeSession(tool);
  const activities = makeActivities({ projectDir: root }, { openSession });
  const env = new MockActivityEnvironment();
  setTimeout(() => env.cancel(reason, details), 50);
  const outcome: { result?: RunStepResult; failure?: unknown } = await env
    .run(activities.runStep, {
      sessionId: "session",
      sessionFile: join(root, `step-${++files}.jsonl`),
      promptId: "prompt",
      text: "run",
      step: 1,
    })
    .then(
      (result) => ({ result: result as RunStepResult }),
      (failure: unknown) => ({ failure }),
    );
  return { ...outcome, ...seen };
}

/** Runs one stepped tool call and cancels it partway through. */
async function cancelledToolCall(details: ActivityCancellationDetails, reason: Reason) {
  const { seen, openSession } = fakeSession(true);
  const activities = makeActivities({ projectDir: root }, { openSession });
  const sessionFile = join(root, `tool-${++files}.jsonl`);
  const env = new MockActivityEnvironment();
  setTimeout(() => env.cancel(reason, details), 50);
  const failure = await env
    .run(activities.runToolCall, {
      sessionId: "session",
      sessionFile,
      turn: "prompt",
      step: 1,
      call: { id: "call-1", name: "bash" },
    })
    .then(
      () => undefined,
      (err: unknown) => err ?? "failed",
    );
  const kept = await pending.readResult(sessionFile, "prompt", 1, "call-1");
  return { failure, kept, ...seen };
}

try {
  const shutdown = await cancelledStep(
    new ActivityCancellationDetails({ workerShutdown: true }),
    "WORKER_SHUTDOWN",
  );
  check("a Worker shutdown doesn't abort the model call", !shutdown.modelAborted, shutdown);
  check(
    "and the step finishes as if nothing happened",
    shutdown.result?.done === true && shutdown.failure === undefined,
    { result: shutdown.result, failure: String(shutdown.failure) },
  );
  const shutdownTool = await cancelledStep(
    new ActivityCancellationDetails({ workerShutdown: true }),
    "WORKER_SHUTDOWN",
    true,
  );
  check("nor the tool", !shutdownTool.toolAborted && shutdownTool.result?.done === true, {
    ...shutdownTool,
    failure: String(shutdownTool.failure),
  });

  // The attempt is no longer the step's: a retry or a newer attempt takes it, or the Workflow is
  // gone. What it was doing stops, and none of it lands in the session.
  for (const [why, details, reason] of [
    ["a timeout", { timedOut: true }, "TIMED_OUT"],
    ["a pause", { paused: true }, "PAUSED"],
    ["a reset", { reset: true }, "RESET"],
    ["an attempt the server no longer knows", { notFound: true }, "NOT_FOUND"],
  ] as const) {
    const cancel = new ActivityCancellationDetails(details);
    const model = await cancelledStep(cancel, reason);
    check(`${why} aborts the model call`, model.modelAborted, model);
    check(
      "and writes nothing to the session, not even a stop",
      model.writes.length === 0 && model.failure !== undefined,
      { writes: model.writes, failure: String(model.failure) },
    );

    const tool = await cancelledStep(cancel, reason, true);
    check(`${why} aborts a whole step's tool`, tool.toolAborted, tool);
    check(
      "and seals nothing after it",
      tool.writes.join() === "response" && tool.failure !== undefined,
      { writes: tool.writes, failure: String(tool.failure) },
    );

    const stepped = await cancelledToolCall(cancel, reason);
    check(`${why} aborts a stepped tool call`, stepped.toolAborted, stepped);
    check(
      "and keeps no result for the seal",
      stepped.kept === undefined && stepped.failure !== undefined,
      { kept: stepped.kept, failure: String(stepped.failure) },
    );
  }

  const asked = new ActivityCancellationDetails({ cancelRequested: true });
  const stop = await cancelledStep(asked, "CANCELLED");
  check("a cancel from the Workflow aborts the model call", stop.modelAborted, stop);
  check(
    "and ends the step as stopped, with the stop recorded",
    stop.failure !== undefined &&
      stop.writes.join() === "aborted response,seal of 0 (postRun false)",
    { writes: stop.writes, failure: String(stop.failure) },
  );
  const stopTool = await cancelledStep(asked, "CANCELLED", true);
  check(
    "and of a tool, whose stopped result is recorded",
    stopTool.toolAborted &&
      stopTool.failure !== undefined &&
      stopTool.writes.join() === "response,seal of 1 (postRun false)",
    { writes: stopTool.writes, failure: String(stopTool.failure) },
  );
  const stopStepped = await cancelledToolCall(asked, "CANCELLED");
  check(
    "and of a stepped tool call, whose result is kept for the seal",
    stopStepped.toolAborted && stopStepped.kept !== undefined,
    { kept: stopStepped.kept, failure: String(stopStepped.failure) },
  );

  // A seal can retry the provider or compact, which takes as long as a model call. A stop reaches
  // it the same way.
  {
    let sealAborted = false;
    const openSession = async () =>
      ({
        state: { messages: [] },
        sealStep: async (_outcomes: unknown[], options: { signal?: AbortSignal }) => {
          sealAborted = await work(options.signal, 200);
          return { done: true, retryAttempt: 0, overflowRecoveryAttempted: false };
        },
        async waitForIdle() {},
        dispose() {},
      }) as unknown as AgentSession;
    const activities = makeActivities({ projectDir: root }, { openSession });
    const env = new MockActivityEnvironment();
    setTimeout(() => env.cancel("CANCELLED", asked), 50);
    await env
      .run(activities.sealStep, {
        sessionId: "session",
        sessionFile: join(root, `seal-${++files}.jsonl`),
        turn: "prompt",
        step: 1,
        calls: [],
      })
      .catch(() => undefined);
    check("a cancel from the Workflow aborts a seal", sealAborted);
  }
} finally {
  await rm(root, { recursive: true, force: true });
}

const bad = failures.length;
console.log(bad === 0 ? "shutdown-check: OK" : `shutdown-check: ${bad} failed`);
process.exit(failures.length === 0 ? 0 : 1);
