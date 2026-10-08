// Checks that a dispatch claim outlives cleanup of the result it produced. Stalls one attempt
// before its claim, lets another run the tool, runs both the later-step and later-turn sweeps,
// then resumes the stale attempt and asserts the effect ran once.
//
// Usage: npx tsx checks/stale-dispatch-check.mts
import assert from "node:assert/strict";
import fs, { appendFile, mkdtemp, readFile, rm } from "node:fs/promises";
import { syncBuiltinESMExports } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { unknownToolCallOutcome, type AgentSession } from "@earendil-works/pi-coding-agent";
import { MockActivityEnvironment } from "@temporalio/testing";
import { makeActivities } from "../src/pi/activities.js";
import * as pending from "../src/core/pending.js";
import type { ToolCallInput, ToolCallResult } from "../src/core/protocol.js";

const root = await mkdtemp(join(tmpdir(), "pi-stale-dispatch-"));
const file = join(root, "session.jsonl");
const effectFile = join(root, "effects");
const input: ToolCallInput = {
  sessionId: "session",
  sessionFile: file,
  turn: "prompt",
  step: 1,
  call: { id: "call", name: "probe" },
};
const activities = makeActivities({ projectDir: root, sessionRoot: root }, {
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
    if (!paused && args[0] === pending.stepDirFor(file, input.turn, 1)) {
      paused = true;
      reached();
      await gate;
    }
    return originalMkdir(...args);
  }) as typeof fs.mkdir;
  syncBuiltinESMExports();
  const stale = new MockActivityEnvironment({ attempt: 1 });
  first = stale.run(activities.runToolCall, input) as Promise<ToolCallResult>;
  await waiting;
  await new MockActivityEnvironment({ attempt: 2 }).run(activities.runToolCall, input);
  assert.equal(await readFile(effectFile, "utf8"), "effect\n");
  await pending.sweep(file, input.turn, 2);
  await pending.sweepResults(file);
  release();
  await first;
  const effects = await readFile(effectFile, "utf8");
  console.log(`effect count after the stale attempt resumes: ${effects.trim().split("\n").length}`);
  assert.equal(
    effects,
    "effect\n",
    "a stale dispatch must not repeat a completed effect after cleanup",
  );
  console.log("stale-dispatch-check: OK");
} finally {
  release();
  await first?.catch(() => {});
  fs.mkdir = originalMkdir;
  syncBuiltinESMExports();
  await rm(root, { recursive: true, force: true });
}
