// Checks that a Worker on this code replays every history in `histories/`, plus one this code
// writes now. A history that stops replaying means a change altered which commands a Workflow
// issues, and running sessions would break on upgrade. Gate that change with `patched()`, or ship
// it as a new Worker Deployment Version. To add histories, run `record-histories.mts`.
//
// Needs a Temporal server, no model key. Usage: npx tsx checks/replay-check.mts

import assert from "node:assert/strict";
import { readdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Client, Connection } from "@temporalio/client";
import { Worker, NativeConnection } from "@temporalio/worker";
import { historyFromJSON } from "@temporalio/common/lib/proto-utils";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const run = promisify(execFile);
import { Context } from "@temporalio/activity";
import type { RunStepInput, SessionInput } from "../src/core/protocol.js";

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
    return { done: true, finalText: "answered" };
  },
  async runModelCall() {
    const calls = [{ id: "call", name: "probe" }];
    return { calls, sequential: false, ended: false, queue: hostQueue };
  },
  async sealStep() {
    return { done: true, finalText: "closed" };
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
    workflowsPath: fileURLToPath(new URL("../src/workflow-bundle.ts", import.meta.url)),
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
      args: [{
        sessionId: "replay",
        sessionFile: "/unused/replay.jsonl",
        stepped: true,
        toolTimeoutMinutes: 1,
        idleTimeout: "100 milliseconds",
        initialPrompt: { promptId: "replay", text: "run" },
      } satisfies SessionInput as never],
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
      { workflowsPath: fileURLToPath(new URL("../src/workflow-bundle.ts", import.meta.url)) },
      history,
    ).catch((err) => {
      ownReplay = err;
    });
    check(
      "a worker on this code replays a history this code wrote",
      ownReplay === undefined,
      String(ownReplay),
    );

    // The kept ones, from earlier code. Each stands for sessions that may still be running.
    const kept = fileURLToPath(new URL("./histories/", import.meta.url));
    const names = (await readdir(kept)).filter((name) => name.endsWith(".json")).sort();
    check("there are kept histories to replay", names.length > 0, names);
    for (const name of names) {
      const workflowId = name.replace(/\.json$/, "");
      const history = historyFromJSON(JSON.parse(await readFile(join(kept, name), "utf8")));
      let failure: unknown;
      await Worker.runReplayHistory(
        { workflowsPath: fileURLToPath(new URL("../src/workflow-bundle.ts", import.meta.url)) },
        history,
        workflowId,
      ).catch((err) => {
        failure = err;
      });
      check(`and the kept ${workflowId} history`, failure === undefined, String(failure));
    }

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
