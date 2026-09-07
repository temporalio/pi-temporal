import assert from "node:assert/strict";
import fs, { appendFile, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { syncBuiltinESMExports } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { unknownToolCallOutcome, type AgentSession } from "@earendil-works/pi-coding-agent";
import { makeActivities } from "./src/activities.js";
import * as pending from "./src/pending.js";
import type { ToolCallInput } from "./src/protocol.js";

const root = await mkdtemp(join(tmpdir(), "pi-dispatch-"));
const effectFile = join(root, "effects");
const input: ToolCallInput = {
  sessionId: "session",
  sessionFile: join(root, "session.jsonl"),
  step: 1,
  call: { id: "call", name: "probe" },
};
const activities = makeActivities({ projectDir: root }, {
  openSession: async () => ({
    state: { messages: [{ role: "assistant", content: [{ type: "toolCall", id: "call" }] }] },
    runToolCall: async () => {
      await appendFile(effectFile, "effect\n");
      return unknownToolCallOutcome(input.call);
    },
    dispose() {},
  }) as unknown as AgentSession,
});

const originalWriteFile = fs.writeFile;
try {
  let arrivals = 0;
  let release!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  const timer = setTimeout(() => release(), 5000);
  // Both attempts reach the filesystem before either create can complete.
  fs.writeFile = (async (...args: Parameters<typeof fs.writeFile>) => {
    if (String(args[0]).endsWith("call.started")) {
      if (++arrivals === 2) release();
      await gate;
    }
    return originalWriteFile(...args);
  }) as typeof fs.writeFile;
  syncBuiltinESMExports();
  const results = await Promise.all([
    activities.runToolCall(input),
    activities.runToolCall(input),
  ]);
  clearTimeout(timer);
  fs.writeFile = originalWriteFile;
  syncBuiltinESMExports();
  assert.equal(arrivals, 2, "both activity attempts must reach the dispatch claim");
  assert.equal(await readFile(effectFile, "utf8"), "effect\n", "one activity may execute the effect");
  assert.deepEqual(results.map((result) => result.outcome).sort(), ["settled", "unknown"]);
  console.log("PASS overlapping activities execute one effect");

  const refused = { ...input, sessionFile: join(root, "refused.jsonl") };
  await writeFile(`${refused.sessionFile}.pending`, "not a directory");
  await assert.rejects(activities.runToolCall(refused));
  assert.equal(await readFile(effectFile, "utf8"), "effect\n", "a failed dispatch write must block the effect");
  console.log("PASS a failed dispatch write blocks the effect");

  const unreadable = join(root, "unreadable.jsonl");
  await mkdir(`${unreadable}.pending/1/call.started`, { recursive: true });
  await assert.rejects(pending.wasDispatched(unreadable, 1, "call"), { code: "EISDIR" });
  console.log("PASS an unreadable dispatch note is not treated as absent");
} finally {
  fs.writeFile = originalWriteFile;
  syncBuiltinESMExports();
  await rm(root, { recursive: true, force: true });
}
