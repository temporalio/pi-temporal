// Whether a session that is already running can be served by a worker carrying this code.
//
// The migration policy decides which activities a step schedules after a pinned dispatch fails, so
// it decides the command sequence a workflow already wrote down. A history recorded before that
// rule existed holds a shared dispatch where this code seals, and replaying it is a nondeterminism
// error unless the rule is behind a patch. It is: the workflow asks `patched()`, which answers
// false for exactly those runs, so they keep the behaviour they recorded.
//
// Both directions are checked here, because a patch that is never exercised is a patch nobody
// knows is wired up: a history this code wrote, and one from before the change.
//
// Needs a Temporal server; no model key. Usage: npx tsx replay-check.mts

import assert from "node:assert/strict";
import { readFile, writeFile } from "node:fs/promises";
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

    // And the ones from before each rule. Recorded by running this file against the code that
    // predates it and keeping the export; without its patch, each is where the nondeterminism
    // appeared. A rule that changes which activities a step schedules needs one of these, and a
    // patch nothing replays through is a patch nobody knows is wired up.
    for (const older of [
      { path: process.env.OLD_POLICY_HISTORY ?? "/tmp/old-policy-history.json",
        what: "before a started failure stopped migrating" },
      { path: process.env.END_TURN_HISTORY ?? "/tmp/end-turn-history.json",
        what: "before a lost host stopped ending the turn" },
    ]) {
      const before = await readFile(older.path, "utf8").catch(() => undefined);
      if (before === undefined) {
        console.log(`SKIP a history from ${older.what} (${older.path} is not here)`);
        continue;
      }
      let oldReplay: unknown;
      await Worker.runReplayHistory(
        { workflowsPath: fileURLToPath(new URL("./src/workflows.ts", import.meta.url)) },
        historyFromJSON(JSON.parse(before)),
      ).catch((err) => {
        oldReplay = err;
      });
      check(`and one written ${older.what}, through its patch`, oldReplay === undefined, String(oldReplay).slice(0, 200));
    }

    console.log(
      failures.length === 0
        ? "replay-check: OK"
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
