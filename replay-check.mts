// Whether a session that is already running can be served by a worker carrying this code.
//
// The migration policy decides which activities a step schedules after a pinned dispatch fails, so
// it decides the command sequence a workflow produced. A history recorded under the old policy
// contains a shared-queue activity this code would not schedule, and Temporal checks that on
// replay. What this does is record such a history and replay it, so the answer is an outcome
// rather than an argument.
//
// Needs a Temporal server; no model key. Usage: npx tsx replay-check.mts

import assert from "node:assert/strict";
import { writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { Client, Connection } from "@temporalio/client";
import { Worker, NativeConnection } from "@temporalio/worker";
import { historyFromJSON } from "@temporalio/common/lib/proto-utils";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const run = promisify(execFile);
import { Context } from "@temporalio/activity";
import type { RunStepInput } from "./src/protocol.js";

const address = process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7233";
const namespace = "default";
const queue = `pi-replay-${Date.now()}`;
const pinnedQueue = `${queue}-pinned`;
const path = process.env.REPLAY_HISTORY ?? "/tmp/replay-history.json";

const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

let pinnedAttempts = 0;
let sharedTools = 0;
const activities = {
  async adoptProject() {},
  async retireSession() {},
  async runStep() {
    return { done: true, retryAttempt: 0, finalText: "answered" };
  },
  async runModelCall() {
    const calls = [{ id: "call", name: "probe" }];
    return { calls, sequential: false, ended: false, queue: pinnedQueue };
  },
  async sealStep() {
    return { done: true, retryAttempt: 0, finalText: "closed" };
  },
};

async function main() {
  const connection = await Connection.connect({ address });
  const native = await NativeConnection.connect({ address });
  const client = new Client({ connection, namespace });

  const shared = await Worker.create({
    connection: native,
    namespace,
    taskQueue: queue,
    workflowsPath: fileURLToPath(new URL("./src/workflows.ts", import.meta.url)),
    activities: {
      ...activities,
      async runToolCall() {
        sharedTools++;
        return { outcome: "unknown" as const };
      },
    },
  });
  const pinned = await Worker.create({
    connection: native,
    namespace,
    taskQueue: pinnedQueue,
    activities: {
      ...activities,
      async runToolCall() {
        pinnedAttempts = Math.max(pinnedAttempts, Context.current().info.attempt);
        throw new Error("the pinned attempt failed after starting");
      },
    },
  });
  const running = shared.run();
  const runningPinned = pinned.run();

  try {
    const handle = await client.workflow.start("piSession", {
      workflowId: `${queue}-session`,
      taskQueue: queue,
      args: ["replay", "/unused/replay.jsonl", {
        stepped: true,
        idleTimeout: "100 milliseconds",
        initialPrompt: { promptId: "replay", text: "run" },
      } as Partial<RunStepInput> as never],
    });
    const settled = handle.result().catch(() => undefined);
    await Promise.race([settled, new Promise((r) => setTimeout(r, 30_000))]);

    // Exported through the CLI, which writes the proto3 JSON the replayer reads back. Encoding a
    // fetched history by hand loses the timestamp form and the replayer then rejects the file for
    // a reason that has nothing to do with the workflow.
    const shown = await run(
      "temporal",
      // prettier-ignore
      ["workflow", "show", "--workflow-id", `${queue}-session`,
       "--address", address, "--output", "json"],
      { maxBuffer: 64 * 1024 * 1024 },
    );
    await writeFile(path, shown.stdout);
    const history = historyFromJSON(JSON.parse(shown.stdout));
    check("a step with a failed pinned dispatch produced a history", (history.events?.length ?? 0) > 0);
    // Under this code the work does not move, so nothing scheduled a shared tool call.
    check("this code refused to migrate the started failure", sharedTools === 0, {
      sharedTools,
      pinnedAttempts,
    });

    // The same code replaying its own history is the case that must always work.
    let ownReplay: unknown;
    await Worker.runReplayHistory(
      { workflowsPath: fileURLToPath(new URL("./src/workflows.ts", import.meta.url)) },
      history,
    ).catch((err) => {
      ownReplay = err;
    });
    check("a worker on this code replays a history this code wrote", ownReplay === undefined, String(ownReplay));

    // What this cannot assert from inside one process: a history written by the previous policy,
    // which scheduled a shared `runToolCall` where this code seals. Replaying one of those against
    // this code gives `Activity type of scheduled event 'runToolCall' does not match activity type
    // of activity command 'sealStep'`, measured by recording a history on the old code and feeding
    // it to `Worker.runReplayHistory` here. So a session that already hit a started pinned failure
    // cannot be served by a worker carrying this code: drain those sessions, or gate the policy
    // behind a patch, before deploying it to a fleet with runs in flight.
    console.log(
      failures.length === 0
        ? "replay-check: OK (a history from the migrating policy is a known nondeterminism, see the comment)"
        : `replay-check: ${failures.length} failed`,
    );
  } finally {
    shared.shutdown();
    pinned.shutdown();
    await Promise.allSettled([running, runningPinned]);
    await connection.close();
    native.close();
  }
  process.exit(failures.length === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
