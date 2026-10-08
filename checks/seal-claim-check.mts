// Checks that a seal takes the claim of a call with no kept result. A tool attempt stalls after it
// opened the session but before its claim, the seal closes the step, then the attempt resumes. The
// tool must not run, and the seal must say the call did not run rather than unknown.
//
// Usage: node --import tsx checks/seal-claim-check.mts
import assert from "node:assert/strict";
import fs, { appendFile, mkdtemp, readFile, rm } from "node:fs/promises";
import { syncBuiltinESMExports } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  unknownToolCallOutcome,
  type AgentSession,
  type TurnToolCallOutcome,
} from "@earendil-works/pi-coding-agent";
import { MockActivityEnvironment } from "@temporalio/testing";
import { makeActivities } from "../src/pi/activities.js";
import * as pending from "../src/core/pending.js";
import type { ToolCallInput, ToolCallResult } from "../src/core/protocol.js";
import { textOf } from "../src/pi/messages.js";

const root = await mkdtemp(join(tmpdir(), "pi-seal-claim-"));
const file = join(root, "session.jsonl");
const effectFile = join(root, "effects");
const input: ToolCallInput = {
  sessionId: "session",
  sessionFile: file,
  turn: "prompt",
  step: 1,
  call: { id: "call", name: "probe" },
};
let sealed: TurnToolCallOutcome[] = [];
const activities = makeActivities({ projectDir: root, sessionRoot: root }, {
  openSession: async () => ({
    state: { messages: [{ role: "assistant", content: [{ type: "toolCall", id: "call" }] }] },
    async runToolCall() {
      await appendFile(effectFile, "effect\n");
      return unknownToolCallOutcome(input.call);
    },
    async sealStep(results: TurnToolCallOutcome[]) {
      sealed = results;
      return { done: false, retryAttempt: 0 };
    },
    async waitForIdle() {},
    dispose() {},
  }) as unknown as AgentSession,
});
let release!: () => void;
const gate = new Promise<void>((resolve) => { release = resolve; });
let reached!: () => void;
const waiting = new Promise<void>((resolve) => { reached = resolve; });
const originalMkdir = fs.mkdir;
let attempt: Promise<ToolCallResult> | undefined;
try {
  // The claim's `mkdir` is the last step before the exclusive create. Stall the attempt there.
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
  const tool = new MockActivityEnvironment();
  attempt = tool.run(activities.runToolCall, input) as Promise<ToolCallResult>;
  await waiting;

  await new MockActivityEnvironment().run(activities.sealStep, {
    sessionId: input.sessionId,
    sessionFile: file,
    turn: input.turn,
    step: 1,
    calls: [input.call],
    interrupted: true,
  });
  assert.equal(sealed.length, 1);
  assert.equal(sealed[0]?.message.isError, true);
  assert.match(textOf(sealed[0]?.message.content), /did not run/);
  console.log("PASS the seal records a call nothing started as not run");

  release();
  assert.deepEqual(await attempt, { outcome: "unknown" });
  const effects = await readFile(effectFile, "utf8").catch(() => "");
  assert.equal(effects, "", "a late attempt must not run the tool after the seal");
  console.log("PASS a late attempt finds the seal's claim and does not run the tool");
} finally {
  release();
  await attempt?.catch(() => {});
  fs.mkdir = originalMkdir;
  syncBuiltinESMExports();
  await rm(root, { recursive: true, force: true });
}
