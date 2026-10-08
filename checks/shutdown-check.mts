// Checks that only a cancel the Workflow asked for is a user stop. The SDK also cancels running
// Activities when their Worker shuts down, times out, is paused or is reset. A turn that took any
// of those as a stop would end as "interrupted", on every deploy for a start. Those must leave the
// model call running, so the step finishes or a retry takes it. A real cancel must still stop it.
//
// Runs `runStep` under `MockActivityEnvironment`, the SDK's way to give an Activity a context
// without a Worker. No server and no model key. Usage: npx tsx checks/shutdown-check.mts

import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { AgentSession } from "@earendil-works/pi-coding-agent";
import { ActivityCancellationDetails } from "@temporalio/common";
import { MockActivityEnvironment } from "@temporalio/testing";
import { makeActivities } from "../src/pi/activities.js";
import type { RunStepResult } from "../src/core/protocol.js";

const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

const root = await mkdtemp(join(tmpdir(), "pi-shutdown-"));

/** Runs one step whose model call takes a moment, and cancels the Activity partway through. */
async function cancelledStep(
  details: ActivityCancellationDetails,
  reason: "WORKER_SHUTDOWN" | "CANCELLED",
) {
  let aborted = false;
  const activities = makeActivities(
    { projectDir: root },
    {
      openSession: async () =>
        ({
          state: { messages: [{ role: "assistant", content: "done" }] },
          prepareStep: () => true,
          recordPrompt: async () => true,
          // Stops when the Activity's signal aborts, as Pi's own model call does.
          modelCall: async (options?: { signal?: AbortSignal }) => {
            await new Promise<void>((resolve) => {
              options?.signal?.addEventListener("abort", () => resolve(), { once: true });
              setTimeout(resolve, 200);
            });
            aborted = options?.signal?.aborted === true;
            return { toolCalls: [], sequential: false, ended: aborted };
          },
          runToolCall: async () => undefined,
          sealStep: async () => ({ done: true, retryAttempt: 0 }),
          async waitForIdle() {},
          getSessionStats: () => ({ tokens: { total: 0 }, cost: 0 }),
          dispose() {},
        }) as unknown as AgentSession,
    },
  );
  const env = new MockActivityEnvironment();
  setTimeout(() => env.cancel(reason, details), 50);
  const outcome: { result?: RunStepResult; failure?: unknown } = await env
    .run(activities.runStep, {
      sessionId: "session",
      sessionFile: join(root, `${reason}.jsonl`),
      promptId: "prompt",
      text: "run",
      step: 1,
    })
    .then(
      (result) => ({ result: result as RunStepResult }),
      (failure: unknown) => ({ failure }),
    );
  return { ...outcome, aborted };
}

try {
  const shutdown = await cancelledStep(
    new ActivityCancellationDetails({ workerShutdown: true }),
    "WORKER_SHUTDOWN",
  );
  check("a Worker shutdown doesn't abort the model call", !shutdown.aborted, shutdown);
  check(
    "and the step finishes as if nothing happened",
    shutdown.result?.done === true && shutdown.failure === undefined,
    { result: shutdown.result, failure: String(shutdown.failure) },
  );

  // A timeout, a pause or a reset hands the step to a retry. None of them is a user stop either.
  for (const [why, details] of [
    ["a timeout", { timedOut: true }],
    ["a pause", { paused: true }],
    ["a reset", { reset: true }],
  ] as const) {
    const other = await cancelledStep(new ActivityCancellationDetails(details), "CANCELLED");
    check(`${why} doesn't abort the model call either`, !other.aborted, other);
  }

  const stop = await cancelledStep(
    new ActivityCancellationDetails({ cancelRequested: true }),
    "CANCELLED",
  );
  check("a cancel from the Workflow aborts the model call", stop.aborted, stop);
  check("and ends the step as stopped", stop.failure !== undefined, String(stop.failure));
} finally {
  await rm(root, { recursive: true, force: true });
}

const bad = failures.length;
console.log(bad === 0 ? "shutdown-check: OK" : `shutdown-check: ${bad} failed`);
process.exit(failures.length === 0 ? 0 : 1);
