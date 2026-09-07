// Dispatch admission has to outlive cleanup of the result it produced. An attempt that stalled
// before taking its claim comes back after the seal has written the answer and the next step has
// swept the results; if the sweep also took the admission, its call looks fresh and it runs the
// tool a second time. A fifth review reproduced that; the sweep keeps the notes now.
//
// Usage: npx tsx stale-dispatch-check.mts
import assert from "node:assert/strict";
import fs, { appendFile, mkdtemp, readFile, rm } from "node:fs/promises";
import { syncBuiltinESMExports } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { unknownToolCallOutcome, type AgentSession } from "@earendil-works/pi-coding-agent";
import { makeActivities } from "./src/activities.js";
import * as pending from "./src/pending.js";
import type { ToolCallInput, ToolCallResult } from "./src/protocol.js";

const root = await mkdtemp(join(tmpdir(), "pi-stale-dispatch-"));
const file = join(root, "session.jsonl");
const effectFile = join(root, "effects");
const input: ToolCallInput = { sessionId: "session", sessionFile: file, step: 1, call: { id: "call", name: "probe" } };
const activities = makeActivities({ projectDir: root }, {
  openSession: async () => ({
    state: { messages: [{ role: "assistant", content: [{ type: "toolCall", id: "call" }] }] },
    async runToolCall() {
      await appendFile(effectFile, "effect\n");
      return unknownToolCallOutcome(input.call);
    },
    dispose() {},
  }) as unknown as AgentSession,
});
let release!: () => void;
const gate = new Promise<void>((resolve) => { release = resolve; });
let reached!: () => void;
const waiting = new Promise<void>((resolve) => { reached = resolve; });
const originalMkdir = fs.mkdir;
let first: Promise<ToolCallResult> | undefined;
try {
  let paused = false;
  fs.mkdir = (async (...args: Parameters<typeof fs.mkdir>) => {
    if (!paused && args[0] === `${file}.pending/1`) {
      paused = true;
      reached();
      await gate;
    }
    return originalMkdir(...args);
  }) as typeof fs.mkdir;
  syncBuiltinESMExports();
  first = activities.runToolCall(input);
  await waiting;
  await activities.runToolCall(input);
  assert.equal(await readFile(effectFile, "utf8"), "effect\n");
  await pending.sweep(file, 2);
  release();
  await first;
  const effects = await readFile(effectFile, "utf8");
  console.log(`effect count after the stale attempt resumes: ${effects.trim().split("\n").length}`);
  assert.equal(effects, "effect\n", "a stale dispatch must not repeat a completed effect after cleanup");
  console.log("stale-dispatch-check: OK");
} finally {
  release();
  await first?.catch(() => {});
  fs.mkdir = originalMkdir;
  syncBuiltinESMExports();
  await rm(root, { recursive: true, force: true });
}
