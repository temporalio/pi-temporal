import assert from "node:assert/strict";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { makeSteppedStep } from "./src/l2-step.js";
import * as worktree from "./src/worktree.js";

const root = await mkdtemp(join(tmpdir(), "pi-migration-rejoin-"));
const oldData = process.env.PI_TEMPORAL_DATA;
process.env.PI_TEMPORAL_DATA = join(root, "data");
const a = join(root, "a");
const b = join(root, "b");
const sessionFile = join(root, "session.jsonl");
const timeout = new Error("started activity timed out");
const unclaimed = new Error("schedule-to-start timeout");
const calls = ["late", "write", "read"].map((id) => ({ id, name: "bash" }));
let sharedCalls = 0;
let recoverySeals = 0;
let finishOldAttempt: (() => Promise<void>) | undefined;

try {
  await mkdir(a);
  await mkdir(b);
  await writeFile(join(a, "seed.txt"), "seed\n");
  await worktree.capture(a, sessionFile, { seed: true });
  await worktree.ensure(b, sessionFile);

  const step = makeSteppedStep({
    activities: {
      runModelCall: async () => ({ calls, sequential: false, ended: false, queue: "host-a" }),
      runToolCall: async ({ call }) => {
        sharedCalls++;
        if (call.id === "late") return { outcome: "unknown" };
        if (call.id === "write") {
          await worktree.ensure(b, sessionFile);
          await writeFile(join(b, "accepted.txt"), "work from host B\n");
          await worktree.capture(b, sessionFile);
        } else {
          // The shared queue can select the timed-out host for a different call.
          await worktree.ensure(a, sessionFile);
          assert.equal(await readFile(join(a, "accepted.txt"), "utf8"), "work from host B\n");
          await finishOldAttempt!();
        }
        return { outcome: "settled" };
      },
      sealStep: async (input) => {
        if (input.interrupted) {
          recoverySeals++;
          return { done: true, retryAttempt: 0, finalText: "" };
        }
        await worktree.ensure(b, sessionFile);
        assert.equal(await readFile(join(b, "accepted.txt"), "utf8"), "work from host B\n");
        return { done: true, retryAttempt: 0, finalText: "" };
      },
    },
    pinnedTo: () => ({
      runToolCall: async ({ call }) => {
        if (call.id !== "late") throw unclaimed;
        // A Temporal timeout settles the promise without stopping this body.
        finishOldAttempt = async () => {
          await rm(join(a, "accepted.txt"), { force: true });
          await worktree.capture(a, sessionFile);
        };
        throw timeout;
      },
      sealStep: async () => { throw unclaimed; },
    }),
    isCancellation: () => false,
    isUnclaimed: (error) => error === unclaimed,
    nonCancellable: async (body) => body(),
  });

  await assert.rejects(step({
    sessionId: "session", sessionFile, promptId: "turn", text: "task", step: 1, retryAttempt: 0,
  }), (error) => error === timeout);
  assert.equal(sharedCalls, 0, "a started attempt must not overlap a migrated batch");
  assert.equal(recoverySeals, 1, "the failed step must preserve results without moving its project");
  assert.equal(await readFile(join(b, "seed.txt"), "utf8"), "seed\n");
  console.log("PASS: a timed-out attempt cannot publish over work from a migrated batch");
} finally {
  if (oldData === undefined) delete process.env.PI_TEMPORAL_DATA;
  else process.env.PI_TEMPORAL_DATA = oldData;
  await rm(root, { recursive: true, force: true });
}
