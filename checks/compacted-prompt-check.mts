// Checks that a compaction inside a turn does not make the turn record its prompt again. The
// compaction summarizes the prompt out of the context, but the session file still has it.
//
// Usage: node --import tsx checks/compacted-prompt-check.mts
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SessionManager, type AgentSession } from "@earendil-works/pi-coding-agent";
import { MockActivityEnvironment } from "@temporalio/testing";
import { makeActivities } from "../src/pi/activities.js";
import { textOf } from "../src/pi/messages.js";

const root = await mkdtemp(join(tmpdir(), "pi-compacted-prompt-"));
const file = join(root, "session.jsonl");
const promptId = "prompt";
// The marker `readyForStep` appends to a recorded prompt.
const marker = `​[pi-temporal:${promptId}]`;

try {
  const writer = SessionManager.open(file);
  writer.appendMessage({ role: "user", content: `run${marker}`, timestamp: Date.now() });
  let firstKept: string | undefined;
  for (const id of ["one", "two"]) {
    const callEntry = writer.appendMessage({
      role: "assistant",
      content: [{ type: "toolCall", id, name: "probe", arguments: {} }],
      api: "openai-responses",
      provider: "openai",
      model: "fake",
      usage: {
        input: 0,
        output: 0,
        cacheRead: 0,
        cacheWrite: 0,
        totalTokens: 0,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
      },
      stopReason: "toolUse",
      timestamp: Date.now(),
    });
    firstKept ??= callEntry;
    writer.appendMessage({
      role: "toolResult",
      toolCallId: id,
      toolName: "probe",
      content: [{ type: "text", text: "ok" }],
      isError: false,
      timestamp: Date.now(),
    });
  }
  writer.appendCompaction("the user asked to run probes", firstKept!, 1000);

  for (const step of [2, 1]) {
    const reopened = SessionManager.open(file);
    const context = reopened.buildSessionContext().messages;
    assert.ok(
      !context.some((m) => "content" in m && textOf(m.content).includes(marker)),
      "the compaction must summarize the prompt out of the context for this check to mean much",
    );
    let recorded = 0;
    let modelCalls = 0;
    const activities = makeActivities({ projectDir: root }, {
      openSession: async () => ({
        sessionManager: reopened,
        state: { messages: context },
        prepareStep: () => true,
        async recordPrompt() {
          recorded++;
          return true;
        },
        async modelCall() {
          modelCalls++;
          return { toolCalls: [], sequential: false, ended: true };
        },
        dispose() {},
      }) as unknown as AgentSession,
    });
    await new MockActivityEnvironment().run(activities.runModelCall, {
      sessionId: "session",
      sessionFile: file,
      step,
      promptId,
      text: "run",
    });
    assert.equal(recorded, 0, `step ${step} recorded the prompt a second time`);
    assert.equal(modelCalls, 1);
    console.log(`PASS step ${step} after a compaction does not record the prompt again`);
  }
} finally {
  await rm(root, { recursive: true, force: true });
}
