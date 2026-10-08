// Checks what `runStep` reports a step cost. Runs the real activity over a faked session and
// asserts `spent` is this step's delta, `total` is the session's billed total, and both are
// undefined (not zero) when the session keeps no totals.
//
// No server and no model key. Usage: npx tsx checks/spend-check.mts

import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { AgentSession } from "@earendil-works/pi-coding-agent";
import { MockActivityEnvironment } from "@temporalio/testing";
import { makeActivities, type Activities } from "../src/pi/activities.js";
import type { RunStepInput, RunStepResult } from "../src/core/protocol.js";

const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

const runStep = (activities: Activities, input: RunStepInput, attempt = 1) =>
  new MockActivityEnvironment({ attempt }).run(activities.runStep, input) as Promise<RunStepResult>;

const root = await mkdtemp(join(tmpdir(), "pi-spend-"));
const sessionFile = join(root, "session.jsonl");

try {
  // Already billed for earlier turns. Pi's totals cover every entry, even compacted history.
  let billed = { tokens: 1_000, cost: 0.5 };
  const activities = makeActivities(
    { projectDir: root, sessionRoot: root },
    {
      openSession: async () =>
        ({
          // A long answer, to check that history keeps only a capped copy.
          state: { messages: [{ role: "assistant", content: "x".repeat(40_000) }] },
          prepareStep: () => true,
          recordPrompt: async () => true,
          // No tool calls, so the step seals as answered.
          modelCall: async () => {
            billed = { tokens: billed.tokens + 250, cost: billed.cost + 0.125 };
            return { toolCalls: [], sequential: false, ended: false };
          },
          runToolCall: async () => undefined,
          sealStep: async () => ({ done: true, retryAttempt: 0 }),
          async waitForIdle() {},
          getSessionStats: () => ({ tokens: { total: billed.tokens }, cost: billed.cost }),
          dispose() {},
        }) as unknown as AgentSession,
    },
  );

  const result = await runStep(activities, {
    sessionId: "session",
    sessionFile,
    promptId: "prompt",
    text: "run",
    step: 1,
  });

  check("a step reports what it spent", result.spent?.tokens === 250, result.spent);
  check(
    "and keeps a capped copy of a long answer, since it goes into history",
    result.finalText.length < 17_000 && result.finalText.includes("session file has all of it"),
    result.finalText.length,
  );
  check("in money as well as tokens", result.spent?.cost === 0.125, result.spent);
  // A run started after idle retirement can only learn the session total from here.
  check(
    "and what the session has been billed in total",
    result.total?.tokens === 1_250,
    result.total,
  );
  check("with the same two numbers", result.total?.cost === 0.625, result.total);

  // Missing totals must not read as zero, or a budget would never stop the turn.
  const quiet = makeActivities(
    { projectDir: root, sessionRoot: root },
    {
      openSession: async () =>
        ({
          state: { messages: [] },
          prepareStep: () => true,
          recordPrompt: async () => true,
          modelCall: async () => ({ toolCalls: [], sequential: false, ended: false }),
          runToolCall: async () => undefined,
          sealStep: async () => ({ done: true, retryAttempt: 0 }),
          async waitForIdle() {},
          dispose() {},
        }) as unknown as AgentSession,
    },
  );
  const silent = await runStep(quiet, {
    sessionId: "session",
    sessionFile: join(root, "quiet.jsonl"),
    promptId: "prompt",
    text: "run",
    step: 1,
  });
  check(
    "a session that keeps no totals reports none",
    silent.spent === undefined && silent.total === undefined,
    silent,
  );

  // The session's time lives in its record. A step that runs again must not see its own turn's
  // write, or the Workflow counts that turn twice.
  const entries: { type: string; customType?: string; data?: unknown }[] = [
    {
      type: "custom",
      customType: "pi-temporal.session-seconds",
      data: { turn: "earlier", seconds: 40 },
    },
  ];
  const kept = makeActivities(
    { projectDir: root, sessionRoot: root },
    {
      openSession: async () =>
        ({
          state: { messages: [] },
          sessionManager: {
            getBranch: () => entries,
            appendCustomEntry: (customType: string, data: unknown) => {
              entries.push({ type: "custom", customType, data });
              return String(entries.length);
            },
          },
          prepareStep: () => true,
          recordPrompt: async () => true,
          modelCall: async () => ({ toolCalls: [], sequential: false, ended: false }),
          runToolCall: async () => undefined,
          sealStep: async () => ({ done: true, retryAttempt: 0 }),
          async waitForIdle() {},
          dispose() {},
        }) as unknown as AgentSession,
    },
  );
  const step = {
    sessionId: "session",
    sessionFile: join(root, "kept.jsonl"),
    promptId: "this-turn",
    text: "run",
    step: 1,
    sessionSeconds: 45,
  };
  const once = await runStep(kept, step);
  const again = await runStep(kept, step, 2);
  check("a step reports the session's time before its turn", once.sessionSeconds === 40, once);
  check("and the same after it ran again", again.sessionSeconds === 40, again);
  const written = entries.filter((e) => (e.data as { turn?: string }).turn === "this-turn");
  check("each run of the step wrote its turn's total", written.length === 2, written);
} finally {
  await rm(root, { recursive: true, force: true });
}

console.log(failures.length === 0 ? "spend-check: OK" : `spend-check: ${failures.length} failed`);
process.exitCode = failures.length === 0 ? 0 : 1;
