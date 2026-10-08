// Checks the payload codec. With a key set, a session's history holds only ciphertext, and the
// client and Worker still read each other's payloads. Payloads written before the key stay
// readable.
//
// Needs a Temporal server, no model key. Usage: npx tsx checks/codec-check.mts

import assert from "node:assert/strict";
import { randomBytes, randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Client, Connection } from "@temporalio/client";
import { ApplicationFailure } from "@temporalio/common";
import { NativeConnection, Worker } from "@temporalio/worker";
import { AesGcmCodec, dataConverterFor } from "../src/core/codec.js";
import { sendPrompt } from "../src/core/client.js";
import { sessionStart } from "../src/client.js";
import { fromEnv } from "../src/config.js";
import { UPDATES, workflowId } from "../src/core/protocol.js";
import type { Quiet, RunStepInput, RunStepResult } from "../src/core/protocol.js";

const address = process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7233";
const key = randomBytes(32);
const secret = `the secret is ${randomUUID()}`;

// Round trip, and a plain payload from before the key passes through.
const codec = new AesGcmCodec(key);
const shown = (p: { metadata?: Record<string, Uint8Array> | null; data?: Uint8Array | null }) =>
  JSON.stringify({
    encoding: Buffer.from(p.metadata?.encoding ?? []).toString(),
    data: Buffer.from(p.data ?? []).toString(),
  });
const plain = {
  metadata: { encoding: Buffer.from("json/plain") },
  data: Buffer.from(`"${secret}"`),
};
const [sealed] = await codec.encode([plain]);
assert.ok(!Buffer.from(sealed.data!).toString().includes(secret));
assert.equal(shown((await codec.decode([sealed]))[0]), shown(plain));
assert.equal(shown((await codec.decode([plain]))[0]), shown(plain));
console.log("PASS a payload round-trips, and one from before the key reads as it was");

// A rotation. The new key encrypts, and the old one still opens what it sealed.
const next = randomBytes(32);
const rotated = new AesGcmCodec(next, [key]);
assert.equal(shown((await rotated.decode([sealed]))[0]), shown(plain));
const [resealed] = await rotated.encode([plain]);
assert.equal(shown((await new AesGcmCodec(next).decode([resealed]))[0]), shown(plain));
await assert.rejects(codec.decode([resealed]), /no codec key opens/);
console.log("PASS after a rotation, old payloads still open and new ones use the new key");

// Payloads sealed before keys were named say `default`. Every key is tried for them.
const legacy = {
  ...sealed,
  metadata: { ...sealed.metadata, "encryption-key-id": Buffer.from("default") },
};
assert.equal(shown((await rotated.decode([legacy]))[0]), shown(plain));
console.log("PASS a payload from before keys were named still opens");

const root = await mkdtemp(join(tmpdir(), "pi-codec-"));
const queue = `pi-codec-${randomUUID().slice(0, 8)}`;
process.env.PI_TEMPORAL_TASK_QUEUE = queue;
process.env.PI_SESSION_DIR = join(root, "sessions");
const cfg = { ...fromEnv(), codecKey: key };
const dataConverter = dataConverterFor({ codecKey: key });
const connection = await Connection.connect({ address });
const client = new Client({ connection, namespace: "default", dataConverter });
const native = await NativeConnection.connect({ address });
const worker = await Worker.create({
  connection: native,
  namespace: "default",
  taskQueue: queue,
  workflowsPath: fileURLToPath(new URL("../src/workflow-bundle.ts", import.meta.url)),
  dataConverter,
  activities: {
    async runStep(input: RunStepInput): Promise<RunStepResult> {
      // Tool and provider errors carry text from the task, so a failure's message is sealed too.
      if (input.text?.startsWith("fail ")) throw ApplicationFailure.nonRetryable(input.text);
      return { done: true, finalText: `${input.text} back` };
    },
    async retireSession() {},
    async adoptProject() {},
  },
});
const running = worker.run();
const session = `codec-${randomUUID().slice(0, 8)}`;
try {
  await sendPrompt(client, sessionStart(cfg, session), { promptId: randomUUID(), text: secret });
  const handle = client.workflow.getHandle(workflowId(session));
  const quiet = await handle.executeUpdate<Quiet, []>(UPDATES.waitForQuiet);
  assert.equal(quiet.finished?.finalText, `${secret} back`);
  console.log("PASS the client and the Worker read each other's encrypted payloads");

  const failing = `fail ${randomUUID()}`;
  await sendPrompt(client, sessionStart(cfg, session), { promptId: randomUUID(), text: failing });
  const failed = await handle.executeUpdate<Quiet, []>(UPDATES.waitForQuiet);
  assert.equal(failed.finished?.outcome, "failed");

  // Every payload in the history, wherever it sits: inputs, results, Update arguments, the memo,
  // and summaries.
  const encodings: string[] = [];
  const walk = (node: unknown) => {
    if (!node || typeof node !== "object") return;
    const payload = node as { metadata?: { encoding?: string }; data?: unknown };
    if (payload.metadata?.encoding !== undefined && "data" in payload) {
      encodings.push(Buffer.from(payload.metadata.encoding, "base64").toString());
    }
    for (const child of Object.values(node)) walk(child);
  };
  walk(JSON.parse(JSON.stringify(await handle.fetchHistory())));
  assert.ok(encodings.length > 0);
  assert.deepEqual([...new Set(encodings)], ["binary/encrypted"]);
  const raw = JSON.stringify(await handle.fetchHistory());
  assert.ok(!raw.includes(failing.slice(5)), "a failure message is in the history in plain text");
  console.log("PASS the history holds only ciphertext, failure messages included");
} finally {
  await client.workflow.getHandle(workflowId(session)).terminate("check cleanup").catch(() => {});
  worker.shutdown();
  await running.catch(() => {});
  await connection.close();
  await native.close();
  await rm(root, { recursive: true, force: true });
}
