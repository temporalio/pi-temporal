// Holds that a run that exits idle writes the Workflow's total of the session's time into the
// session record, and never lowers what the record already says. Needs neither a server nor a
// model key.
import assert from "node:assert/strict";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import { MockActivityEnvironment } from "@temporalio/testing";
import { makeActivities } from "../src/pi/activities.js";

const dir = await mkdtemp(join(tmpdir(), "pi-retire-seconds-"));
const file = join(dir, "s.jsonl");
const latest = () => {
  const entries = SessionManager.open(file).getBranch() as {
    type: string;
    customType?: string;
    data?: { seconds?: number };
  }[];
  return entries.filter((e) => e.customType === "pi-temporal.session-seconds").at(-1)?.data;
};

const { retireSession: retire } = makeActivities({
  projectDir: dir,
  shipTree: false,
  sessionRoot: dir,
});
const env = new MockActivityEnvironment();
const retireSession = (input: Parameters<typeof retire>[0]) => env.run(retire, input);
// A session with no record yet has nothing to add to.
await retireSession({ sessionFile: file, turn: "t1", sessionSeconds: 5 });
assert.equal(latest(), undefined);

// Pi writes the file only once the session holds a conversation.
const manager = SessionManager.open(file);
manager.appendMessage({ role: "user", content: "hi", timestamp: Date.now() } as never);
const nothing = { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 };
manager.appendMessage({
  role: "assistant",
  content: [{ type: "text", text: "hello" }],
  api: "none",
  provider: "none",
  model: "none",
  usage: { ...nothing, totalTokens: 0, cost: { ...nothing, total: 0 } },
  stopReason: "stop",
  timestamp: Date.now(),
} as never);
manager.appendCustomEntry("pi-temporal.session-seconds", { turn: "t1", seconds: 4 });
await retireSession({ sessionFile: file, turn: "t1", sessionSeconds: 5 });
assert.equal(latest()?.seconds, 5);
console.log("PASS an idle exit writes the Workflow's total");
await retireSession({ sessionFile: file, turn: "t1", sessionSeconds: 3 });
assert.equal(latest()?.seconds, 5);
console.log("PASS and never lowers what the record says");

// A file that can't be read is not a missing one. The Activity fails, so Temporal retries it.
const unreadable = join(file, "inside-a-file.jsonl");
const failed = await retireSession({ sessionFile: unreadable, turn: "t1", sessionSeconds: 5 }).then(
  () => "",
  (err: NodeJS.ErrnoException) => err.code ?? String(err),
);
assert.equal(failed, "ENOTDIR");
console.log("PASS a session file that can't be read fails the retirement instead of skipping it");
