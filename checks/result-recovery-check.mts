import assert from "node:assert/strict";
import fs from "node:fs/promises";
import { syncBuiltinESMExports } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { unknownToolCallOutcome, type AgentSession } from "@earendil-works/pi-coding-agent";
import { makeActivities } from "../src/activities.js";
import * as pending from "../src/pending.js";
import type { ToolCallInput } from "../src/protocol.js";

const root = await fs.mkdtemp(join(tmpdir(), "pi-result-recovery-"));
const originalWrite = fs.writeFile;
const input: ToolCallInput = {
  sessionId: "session",
  sessionFile: join(root, "session.jsonl"),
  turn: "prompt",
  step: 1,
  call: { id: "call", name: "probe" },
};

function barrier() {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => { release = resolve; });
  return { promise, release };
}

async function duplicateResult() {
  const started = barrier();
  const finish = barrier();
  const checked = barrier();
  const retry = barrier();
  let effects = 0;
  const template = unknownToolCallOutcome(input.call);
  const result = {
    ...template,
    message: { ...template.message, content: [{ type: "text" as const, text: "saved result" }] },
  };
  const activities = makeActivities({ projectDir: root }, {
    openSession: async () => ({
      state: { messages: [{ role: "assistant", content: [{ type: "toolCall", id: "call" }] }] },
      runToolCall: async () => {
        effects++;
        started.release();
        await finish.promise;
        return result;
      },
      dispose() {},
    }) as unknown as AgentSession,
  });
  const first = activities.runToolCall(input);
  await started.promise;
  fs.writeFile = (async (...args: Parameters<typeof fs.writeFile>) => {
    if (String(args[0]).endsWith("call.started")) {
      checked.release();
      await retry.promise;
    }
    return originalWrite(...args);
  }) as typeof fs.writeFile;
  syncBuiltinESMExports();
  const duplicate = activities.runToolCall(input);
  try {
    await checked.promise;
    finish.release();
    assert.equal((await first).outcome, "settled");
    retry.release();
    assert.equal((await duplicate).outcome, "unknown");
    assert.equal(effects, 1);
    assert.deepEqual(
      await pending.readResult(input.sessionFile, input.turn, input.step, input.call.id),
      result,
      "a duplicate claim cannot replace a recorded result with uncertainty",
    );
  } finally {
    finish.release();
    retry.release();
    await Promise.allSettled([first, duplicate]);
    fs.writeFile = originalWrite;
    syncBuiltinESMExports();
  }
  console.log("PASS a duplicate dispatch retains the completed result");
}

async function invalidCallIds() {
  for (const id of ["../outside", "../../outside", "a/b", "a\\b", "a\0b", "", "x".repeat(201)]) {
    await assert.rejects(pending.noteDispatch(input.sessionFile, "invalid", 1, id));
  }
  const files = await fs.readdir(join(`${input.sessionFile}.pending`, "invalid", "1"));
  assert.deepEqual(files, [], "invalid call IDs cannot write claims outside their step");
  assert.equal(await pending.noteDispatch(input.sessionFile, "valid", 1, "call_123-abc"), true);
  console.log("PASS invalid call IDs cannot change dispatch storage");
}

try {
  const checks = { duplicateResult, invalidCallIds };
  const selected = process.argv[2];
  for (const [name, check] of Object.entries(checks)) {
    if (!selected || selected === name) await check();
  }
} finally {
  fs.writeFile = originalWrite;
  syncBuiltinESMExports();
  await fs.rm(root, { recursive: true, force: true });
}
