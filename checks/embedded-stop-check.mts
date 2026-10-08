// Checks that an embedded worker's `stop()` doesn't wait on a tool that never ends. pi stops its
// workers when it quits, so without a bound a hung tool would hang the quit. Also checks that a
// second `stop()` on a worker that already ended doesn't throw.
//
// Needs a Temporal server, no model key. Usage: npx tsx checks/embedded-stop-check.mts

import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Client, Connection } from "@temporalio/client";
import { makeLocalTurnActivities, type LiveTurns } from "../src/pi/local-turn-activity.js";
import { LOCAL_TURN_WORKFLOW } from "../src/core/protocol.js";
import { createSessionWorker } from "../src/core/session-worker.js";

const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};
const sleep = (ms: number) => new Promise((done) => setTimeout(done, ms));

const address = process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7233";
const queue = `pi-embedded-stop-${Date.now()}`;

async function main() {
  const root = await mkdtemp(join(tmpdir(), "pi-embedded-stop-"));
  process.env.PI_TEMPORAL_DATA = join(root, "data");
  const connection = await Connection.connect({ address });
  const client = new Client({ connection, namespace: "default" });

  // A turn whose tool never returns.
  let started = false;
  const live: LiveTurns = new Map();
  live.set("hung", {
    run: () => {
      started = true;
      return new Promise<void>(() => {});
    },
    steps: undefined as never,
  });

  const worker = await createSessionWorker({
    address,
    namespace: "default",
    taskQueue: queue,
    activities: () => makeLocalTurnActivities(live),
    shutdownForceTime: "2s",
  });
  const running = worker.run().catch(() => {});
  const handle = await client.workflow.start(LOCAL_TURN_WORKFLOW, {
    taskQueue: queue,
    workflowId: `${queue}-turn`,
    args: [{ sessionId: "s", turnId: "hung", taskQueue: queue }],
  });

  try {
    for (let i = 0; i < 100 && !started; i++) await sleep(100);
    check("the hung tool is running", started);

    const began = Date.now();
    const stopped = await Promise.race([
      worker.stop().then(
        () => "stopped",
        (err) => `threw: ${String(err)}`,
      ),
      sleep(20_000).then(() => "hung"),
    ]);
    const took = Date.now() - began;
    check("stop() returns despite the hung tool", stopped === "stopped", { stopped, took });
    check("within a few seconds of the force time", took < 10_000, took);
    // A stop that hung leaves the worker running, so don't wait on it.
    if (stopped === "stopped") {
      await running;
      const again = await worker.stop().then(
        () => "stopped",
        (err) => `threw: ${String(err)}`,
      );
      check("a second stop() on an ended worker doesn't throw", again === "stopped", again);
    }
  } finally {
    await handle.terminate("check cleanup").catch(() => {});
    await connection.close();
    await rm(root, { recursive: true, force: true });
  }

  console.log(
    failures.length === 0
      ? "embedded-stop-check: OK"
      : `embedded-stop-check: ${failures.length} failed`,
  );
  process.exit(failures.length === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
