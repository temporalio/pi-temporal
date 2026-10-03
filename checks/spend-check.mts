// What an activity says a step cost, which is what every bound above it is made of.
//
// Two numbers, and they answer different questions. What this step spent is a difference the
// activity takes across its own work, because the session's own totals are for the whole session
// and a turn's bound is about one turn. What the session has been billed is that total, read off
// the record rather than added up here, because nothing the workflow keeps survives the session
// going idle and being woken again as a fresh run.
//
// Driven through the real activity with the session faked, so what is checked is the activity
// reading its session and reporting it, which no stub-driven check can say anything about.
//
// No server and no model key. Usage: npx tsx checks/spend-check.mts

import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { AgentSession } from "@earendil-works/pi-coding-agent";
import { makeActivities } from "../src/activities.js";

const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

const root = await mkdtemp(join(tmpdir(), "pi-spend-"));
const sessionFile = join(root, "session.jsonl");

try {
  // A session that was already billed for earlier turns, and is billed more by this step. The
  // shape is Pi's own: totals over every entry, including history a compaction rewrote.
  let billed = { tokens: 1_000, cost: 0.5 };
  const activities = makeActivities(
    { projectDir: root },
    {
      openSession: async () =>
        ({
          state: { messages: [] },
          prepareStep: () => true,
          recordPrompt: async () => true,
          // The model call is what spends, and a step that asks for no tool seals as answered.
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

  const result = await activities.runStep({
    sessionId: "session",
    sessionFile,
    promptId: "prompt",
    text: "run",
    step: 1,
  });

  // The difference this step made, not the session's total: a turn's bound is about the turn.
  check("a step reports what it spent", result.spent?.tokens === 250, result.spent);
  check("in money as well as tokens", result.spent?.cost === 0.125, result.spent);
  // And the total, which is what a session's bound is measured against and what a run that starts
  // after an idle retirement has no other way to know.
  check(
    "and what the session has been billed in total",
    result.total?.tokens === 1_250,
    result.total,
  );
  check("with the same two numbers", result.total?.cost === 0.625, result.total);

  // A session that cannot say is reported as nothing rather than as zero: a bound that reads a
  // missing count as nothing spent is one that never stops a turn.
  const quiet = makeActivities(
    { projectDir: root },
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
  const silent = await quiet.runStep({
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
} finally {
  await rm(root, { recursive: true, force: true });
}

console.log(failures.length === 0 ? "spend-check: OK" : `spend-check: ${failures.length} failed`);
process.exitCode = failures.length === 0 ? 0 : 1;
