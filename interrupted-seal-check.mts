import assert from "node:assert/strict";
import { appendFile, mkdir, mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { unknownToolCallOutcome, type AgentSession, type TurnToolCallOutcome } from "@earendil-works/pi-coding-agent";
import { makeActivities } from "./src/activities.js";
import { makeSteppedStep } from "./src/l2-step.js";
import * as pending from "./src/pending.js";

class Cancelled extends Error {}
const root = await mkdtemp(join(tmpdir(), "pi-interrupted-seal-"));
const file = join(root, "session.jsonl");
const project = join(root, "project");
const originalData = process.env.PI_TEMPORAL_DATA;
process.env.PI_TEMPORAL_DATA = join(root, "host");
const calls = [{ id: "finished", name: "probe" }, { id: "cancelled", name: "probe" }];
let finished!: () => void;
const completed = new Promise<void>((resolve) => { finished = resolve; });
let nonCancellable = false;
let pinnedSeals = 0;
let postRun: boolean | undefined;

try {
  await mkdir(project);
  await writeFile(join(project, "local.txt"), "local work\n");
  const activities = makeActivities({ projectDir: project, shipTree: true }, {
    openSession: async (_file, guard) => ({
      state: { messages: [] },
      async sealStep(results: TurnToolCallOutcome[], options: { postRun: boolean }) {
        assert.equal(nonCancellable, true);
        guard?.();
        postRun = options.postRun;
        await appendFile(file, `${JSON.stringify(results)}\n`);
        return { done: false, retryAttempt: 0 };
      },
      async waitForIdle() {},
      dispose() {},
    }) as unknown as AgentSession,
  });
  const step = makeSteppedStep({
    activities: {
      ...activities,
      runModelCall: async () => ({ calls, sequential: false, ended: false, queue: "pinned" }),
    },
    pinnedTo: () => ({
      async runToolCall(input) {
        if (input.call.id === "cancelled") {
          await completed;
          throw new Cancelled();
        }
        const result = unknownToolCallOutcome(input.call);
        result.message.content = [{ type: "text", text: "finished tool output" }];
        result.message.isError = false;
        await pending.noteDispatch(file, 1, input.call.id);
        await pending.keepResult(file, 1, input.call.id, result);
        finished();
        return { outcome: "settled" };
      },
      async sealStep() {
        pinnedSeals++;
        throw new Error("the cancelled step must use the transcript-only seal");
      },
    }),
    isCancellation: (error) => error instanceof Cancelled,
    isUnclaimed: () => false,
    nonCancellable: async (fn) => {
      nonCancellable = true;
      try { return await fn(); } finally { nonCancellable = false; }
    },
  });
  await assert.rejects(step({ sessionId: "session", sessionFile: file, step: 1, promptId: "prompt", text: "run" }), Cancelled);
  await pending.sweepAll(file);
  const results = JSON.parse(await readFile(file, "utf8")) as TurnToolCallOutcome[];
  assert.equal(results[0]?.message.toolCallId, "finished");
  assert.equal(results[0]?.message.isError, false);
  assert.deepEqual(results[0]?.message.content, [{ type: "text", text: "finished tool output" }]);
  assert.equal(results[1]?.message.toolCallId, "cancelled");
  assert.equal(postRun, false);
  assert.equal(pinnedSeals, 0);
  console.log("PASS a completed sibling survives cancellation and the next pending sweep");
  assert.equal(await readFile(join(project, "local.txt"), "utf8"), "local work\n");
  await assert.rejects(stat(`${file}.tree`), { code: "ENOENT" });
  await assert.rejects(stat(join(root, "host")), { code: "ENOENT" });
  console.log("PASS the interrupted seal does not restore or capture the project");
} finally {
  if (originalData === undefined) delete process.env.PI_TEMPORAL_DATA;
  else process.env.PI_TEMPORAL_DATA = originalData;
  await rm(root, { recursive: true, force: true });
}
