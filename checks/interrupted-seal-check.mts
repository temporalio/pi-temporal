import assert from "node:assert/strict";
import {
  appendFile,
  mkdir,
  mkdtemp,
  readdir,
  readFile,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  unknownToolCallOutcome,
  type AgentSession,
  type TurnToolCallOutcome,
} from "@earendil-works/pi-coding-agent";
import { makeActivities } from "../src/activities.js";
import { makeSteppedStep } from "../src/l2-step.js";
import * as pending from "../src/pending.js";

class Cancelled extends Error {}
for (const { failure, atSeal } of [
  { failure: new Cancelled(), atSeal: false },
  { failure: new Error("pinned tool attempt timed out"), atSeal: false },
  { failure: new Error("pinned seal attempt timed out"), atSeal: true },
  { failure: new Cancelled(), atSeal: true },
]) {
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
            if (atSeal) return { outcome: "unknown" };
            throw failure;
          }
          const result = unknownToolCallOutcome(input.call);
          result.message.content = [{ type: "text", text: "finished tool output" }];
          result.message.isError = false;
          await pending.noteDispatch(file, input.turn, 1, input.call.id);
          await pending.keepResult(file, input.turn, 1, input.call.id, result);
          finished();
          return { outcome: "settled" };
        },
        async sealStep() {
          pinnedSeals++;
          if (atSeal) throw failure;
          throw new Error("the failed step must use the transcript-only seal");
        },
      }),
      isCancellation: (error) => error instanceof Cancelled,
      isUnclaimed: () => false,
      nonCancellable: async (fn) => {
        nonCancellable = true;
        try { return await fn(); } finally { nonCancellable = false; }
      },
    });
    await assert.rejects(
      step({ sessionId: "session", sessionFile: file, step: 1, promptId: "prompt", text: "run" }),
      (error) => error === failure,
    );
    await pending.sweepResults(file);
    const results = JSON.parse(await readFile(file, "utf8")) as TurnToolCallOutcome[];
    assert.equal(results[0]?.message.toolCallId, "finished");
    assert.equal(results[0]?.message.isError, false);
    assert.deepEqual(results[0]?.message.content, [{ type: "text", text: "finished tool output" }]);
    assert.equal(results[1]?.message.toolCallId, "cancelled");
    assert.equal(postRun, false);
    assert.equal(pinnedSeals, atSeal ? 1 : 0);
    console.log(
      `PASS a completed result survives ${
        failure instanceof Cancelled ? "cancellation" : "a started-attempt failure"
      } during the ${atSeal ? "seal" : "tools"} and the next pending sweep`,
    );
    assert.equal(await readFile(join(project, "local.txt"), "utf8"), "local work\n");
    // Nothing was restored or captured: no bundle, no tip, and no host directory at all.
    assert.deepEqual(
      (await readdir(`${file}.tree`).catch(() => [])).filter((name) => name !== "closed.json"),
      [],
    );
    await assert.rejects(stat(join(root, "host")), { code: "ENOENT" });
    console.log("PASS the recovery seal does not restore or capture the project");
    // A step nobody stopped was closed without its host, so that host may not publish for it
    // afterwards. A stop is the other case: the tools were told to end and the work they did is
    // still theirs to ship.
    const closed = JSON.parse(await readFile(`${file}.tree/closed.json`, "utf8").catch(() => "[]"));
    assert.deepEqual(
      closed.map((entry: { turn: string; step: number }) => `${entry.turn}/${entry.step}`),
      failure instanceof Cancelled ? [] : ["prompt/1"],
    );
    console.log(
      failure instanceof Cancelled
        ? "PASS a stop leaves the step open to the host that was running it"
        : "PASS a step that lost its host is closed to it",
    );
  } finally {
    if (originalData === undefined) delete process.env.PI_TEMPORAL_DATA;
    else process.env.PI_TEMPORAL_DATA = originalData;
    await rm(root, { recursive: true, force: true });
  }
}
