// Checks that a worker on this code replays a history this code wrote. A change that alters which
// commands a Workflow issues breaks running sessions. Once sessions run in production, put it
// behind `patched()`, or ship it as a new Worker Deployment Version, and keep its old history here.
//
// Needs a Temporal server, no model key. Usage: npx tsx checks/replay-check.mts

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
import type { RunStepInput } from "../src/protocol.js";

const address = process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7233";
const namespace = "default";
const queue = `pi-replay-${Date.now()}`;
const hostQueue = `${queue}-host`;
const path = process.env.REPLAY_HISTORY ?? "/tmp/replay-history.json";

const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

let hostAttempts = 0;
let sharedTools = 0;
const activities = {
  async adoptProject() {},
  async retireSession() {},
  async runStep() {
    return { done: true, retryAttempt: 0, finalText: "answered" };
  },
  async runModelCall() {
    const calls = [{ id: "call", name: "probe" }];
    return { calls, sequential: false, ended: false, queue: hostQueue };
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
    workflowsPath: fileURLToPath(new URL("../src/workflows.ts", import.meta.url)),
    activities: {
      ...activities,
      async runToolCall() {
        sharedTools++;
        return { outcome: "unknown" as const };
      },
    },
  });
  const host = await Worker.create({
    connection: native,
    namespace,
    taskQueue: hostQueue,
    activities: {
      ...activities,
      async runToolCall() {
        hostAttempts = Math.max(hostAttempts, Context.current().info.attempt);
        throw new Error("the host-queue attempt failed after starting");
      },
    },
  });
  const running = shared.run();
  const runningHost = host.run();

  try {
    const handle = await client.workflow.start("piSession", {
      workflowId: `${queue}-session`,
      taskQueue: queue,
      args: ["replay", "/unused/replay.jsonl", {
        stepped: true,
        toolTimeoutMinutes: 1,
        idleTimeout: "100 milliseconds",
        initialPrompt: { promptId: "replay", text: "run" },
      } as Partial<RunStepInput> as never],
    });
    const settled = handle.result().catch(() => undefined);
    await Promise.race([settled, new Promise((r) => setTimeout(r, 30_000))]);

    // Export through the CLI. Hand-encoding a fetched history breaks timestamps for the replayer.
    const shown = await run(
      "temporal",
      // prettier-ignore
      ["workflow", "show", "--workflow-id", `${queue}-session`,
       "--address", address, "--output", "json"],
      { maxBuffer: 64 * 1024 * 1024 },
    );
    await writeFile(path, shown.stdout);
    const history = historyFromJSON(JSON.parse(shown.stdout));
    const tool = history.events?.find(
      (event) => event.activityTaskScheduledEventAttributes?.activityType?.name === "runToolCall",
    )?.activityTaskScheduledEventAttributes;
    assert.equal(Number(tool?.startToCloseTimeout?.seconds), 60);
    assert.equal(Number(tool?.scheduleToCloseTimeout?.seconds), 7200);
    assert.equal(tool?.taskQueue?.name, hostQueue);
    console.log("PASS the host-queue tool uses its configured timeout");
    check(
      "a step with a failed host-queue dispatch produced a history",
      (history.events?.length ?? 0) > 0,
    );
    check("this code refused to migrate the started failure", sharedTools === 0, {
      sharedTools,
      hostAttempts,
    });

    let ownReplay: unknown;
    await Worker.runReplayHistory(
      { workflowsPath: fileURLToPath(new URL("../src/workflows.ts", import.meta.url)) },
      history,
    ).catch((err) => {
      ownReplay = err;
    });
    check(
      "a worker on this code replays a history this code wrote",
      ownReplay === undefined,
      String(ownReplay),
    );

    console.log(
      failures.length === 0
        ? "replay-check: OK"
        : `replay-check: ${failures.length} failed`,
    );
  } finally {
    shared.shutdown();
    host.shutdown();
    await Promise.allSettled([running, runningHost]);
    await connection.close();
    native.close();
  }
  process.exit(failures.length === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
