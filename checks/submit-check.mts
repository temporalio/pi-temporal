// Checks how a prompt reaches a session. The session checks each prompt and runs a resent one
// once. With no Worker to accept the Update, the prompt still lands as a Signal and runs once a
// Worker comes. A client waiting for the session to go quiet gets the turn's outcome.
//
// Needs a Temporal server, no model key. Usage: npx tsx checks/submit-check.mts

import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Client, Connection } from "@temporalio/client";
import { NativeConnection, Worker } from "@temporalio/worker";
import { sendPrompt } from "../src/core/client.js";
import { fromEnv } from "../src/config.js";
import { UPDATES, workflowId } from "../src/core/protocol.js";
import type { Quiet, RunStepInput, RunStepResult } from "../src/core/protocol.js";

const address = process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7233";
const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

const ran = new Map<string, number>();
const activities = {
  async runStep(input: RunStepInput): Promise<RunStepResult> {
    ran.set(input.promptId, (ran.get(input.promptId) ?? 0) + 1);
    return { done: true, finalText: `answered ${input.promptId}` };
  },
  async retireSession() {},
  async adoptProject() {},
};

const root = await mkdtemp(join(tmpdir(), "pi-submit-"));
const queue = `pi-submit-${randomUUID().slice(0, 8)}`;
process.env.PI_TEMPORAL_TASK_QUEUE = queue;
process.env.PI_SESSION_DIR = join(root, "sessions");
process.env.PI_SESSION_IDLE_TIMEOUT = "30 seconds";
const cfg = fromEnv();
const connection = await Connection.connect({ address });
const client = new Client({ connection, namespace: "default" });
const native = await NativeConnection.connect({ address });
let worker: Worker | undefined;
let running: Promise<void> | undefined;
const startWorker = async () => {
  worker = await Worker.create({
    connection: native,
    namespace: "default",
    taskQueue: queue,
    workflowsPath: fileURLToPath(new URL("../src/workflow-bundle.ts", import.meta.url)),
    activities,
  });
  running = worker.run();
};
const quiet = (sessionId: string) =>
  client.workflow.getHandle(workflowId(sessionId)).executeUpdate<Quiet, []>(UPDATES.waitForQuiet);

const sessions: string[] = [];
try {
  // No Worker yet. The Update can't be accepted, so the prompt goes as a Signal.
  const early = `submit-early-${randomUUID().slice(0, 8)}`;
  sessions.push(early);
  const parked = { promptId: randomUUID(), text: "run when a worker comes" };
  const ahead = await sendPrompt(client, cfg, early, parked);
  check("with no Worker, a prompt is still sent", ahead === undefined, ahead);
  await startWorker();
  const late = await quiet(early);
  check(
    "and it runs once a Worker comes",
    late.finished?.promptId === parked.promptId && ran.get(parked.promptId) === 1,
    { late, ran: ran.get(parked.promptId) },
  );

  const session = `submit-${randomUUID().slice(0, 8)}`;
  sessions.push(session);
  const prompt = { promptId: randomUUID(), text: "run once" };
  const first = await sendPrompt(client, cfg, session, prompt);
  check("a prompt to a new session starts it, with nothing ahead", first === 0, first);
  const again = await sendPrompt(client, cfg, session, prompt);
  const done = await quiet(session);
  check("a resent prompt counts as sent", again === undefined || again === 0, again);
  check(
    "and runs once",
    ran.get(prompt.promptId) === 1 && done.finished?.outcome === "answered",
    { ran: ran.get(prompt.promptId), done },
  );

  const empty = await sendPrompt(client, cfg, session, { promptId: randomUUID(), text: "  " }).then(
    () => "",
    (err: unknown) => String(err),
  );
  check("an empty prompt is refused", /needs text/.test(empty), empty);
} finally {
  for (const id of sessions) {
    await client.workflow.getHandle(workflowId(id)).terminate("check cleanup").catch(() => {});
  }
  worker?.shutdown();
  await running?.catch(() => {});
  await connection.close();
  await native.close();
  await rm(root, { recursive: true, force: true });
}

console.log(failures.length === 0 ? "submit-check: OK" : `submit-check: ${failures.length} failed`);
process.exit(failures.length === 0 ? 0 : 1);
