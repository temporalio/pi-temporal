// Checks that a step whose host timed out can be closed without ending the turn. Leaves the
// tool body running after the timeout, then asserts the closing seal publishes nothing and the
// abandoned tool's later capture is refused and kept under `salvage/`. The next step on another
// host then moves the project as usual.
//
// No server and no model key. Usage: npx tsx checks/lost-host-check.mts

import assert from "node:assert/strict";
import { mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { AgentSession, TurnToolCallOutcome } from "@earendil-works/pi-coding-agent";
import { MockActivityEnvironment } from "@temporalio/testing";
import { makeActivities } from "../src/pi/activities.js";
import { makeSteppedStep } from "../src/core/stepped-step.js";
import type { RunStepResult } from "../src/core/protocol.js";
import * as worktree from "../src/tree/worktree.js";

const root = await mkdtemp(join(tmpdir(), "pi-lost-host-"));
const originalData = process.env.PI_TEMPORAL_DATA;
process.env.PI_TEMPORAL_DATA = join(root, "data");
const a = join(root, "host-a");
const b = join(root, "host-b");
const sessionFile = join(root, "session.jsonl");
const turn = "prompt";
const failure = new Error("the host-queue attempt timed out with the tool still running");
const writer = { turn, step: 1, callId: "slow" };
let finishAbandoned: (() => Promise<unknown>) | undefined;
let seals = 0;

try {
  await mkdir(a);
  await mkdir(b);
  await writeFile(join(a, "seed.txt"), "seed\n");
  await worktree.capture(a, sessionFile, { seed: true });
  // Both hosts start on the tip.
  await worktree.ensure(b, sessionFile);

  const activities = makeActivities({ projectDir: a, shipTree: true }, {
    openSession: async () => ({
      state: { messages: [] },
      async sealStep(results: TurnToolCallOutcome[]) {
        seals++;
        return { done: false, retryAttempt: 0 };
      },
      async waitForIdle() {},
      dispose() {},
    }) as unknown as AgentSession,
  });

  const step = makeSteppedStep({
    activities: {
      ...activities,
      runModelCall: async () => ({
        calls: [{ id: "slow", name: "bash" }],
        sequential: false,
        ended: false,
        queue: "host-a",
      }),
    },
    onHost: () => ({
      runToolCall: async () => {
        // Temporal gives up on the attempt but the body keeps going. Its remaining work runs later.
        await worktree.beginWrite(a, writer);
        finishAbandoned = async () => {
          await writeFile(join(a, "from-the-abandoned-tool.txt"), "written after the step ended\n");
          await worktree.endWrite(a, writer);
          return await worktree
            .capture(a, sessionFile, { current: writer, fence: { turn, step: 1 } })
            .then(() => undefined, (err: unknown) => err);
        };
        throw failure;
      },
      sealStep: async () => {
        throw new Error("a step that lost its host must not seal on it");
      },
    }),
    isCancellation: () => false,
    isUnclaimed: () => false,
    nonCancellable: async (body) => body(),
  });

  const bundles = async () =>
    (await readdir(`${sessionFile}.tree`)).filter((name) => name.endsWith(".bundle")).length;
  const shipped = await bundles();

  // The step calls the real `sealStep` in this process, so it shares this Activity context.
  const result = (await new MockActivityEnvironment().run(step, {
    sessionId: "session", sessionFile, promptId: turn, text: "task", step: 1,
  })) as RunStepResult;
  assert.equal(seals, 1, "the step records what it had before it hands the turn back");
  assert.equal(result.done, false, "the turn goes on rather than ending with the worker");
  console.log("PASS a step that lost its host hands the turn back instead of failing it");

  // The old host may still be writing, so the closing seal must not publish.
  assert.equal(await bundles(), shipped, "the seal that closed the step published nothing");
  console.log("PASS and the seal that closed it published nothing");

  // The abandoned tool finishes before the tip moves, so only the closed-step fence can refuse it.
  const refused = await finishAbandoned!();
  assert.match(String(refused), /closed without this host/);
  console.log("PASS the host it lost cannot publish for that step afterwards");

  await worktree.ensure(b, sessionFile);
  await writeFile(join(b, "from-host-b.txt"), "work that replaced it\n");
  await worktree.capture(b, sessionFile, { fence: { turn, step: 2 } });
  await worktree.ensure(a, sessionFile).catch(() => undefined);
  assert.equal(await readFile(join(b, "from-host-b.txt"), "utf8"), "work that replaced it\n");
  assert.equal(await readFile(join(b, "seed.txt"), "utf8"), "seed\n");
  assert.equal(
    (await readdir(b)).includes("from-the-abandoned-tool.txt"),
    false,
    "what the abandoned tool wrote is kept on its own host, not published as the project",
  );
  // Refused work is kept, not dropped.
  assert.equal(
    (await readdir(join(`${sessionFile}.tree`, "salvage")).catch(() => [])).length,
    1,
    "the refused host keeps what its tool wrote where it can be recovered",
  );
  console.log("PASS the work that replaced it is the one the session keeps");
} finally {
  if (originalData === undefined) delete process.env.PI_TEMPORAL_DATA;
  else process.env.PI_TEMPORAL_DATA = originalData;
  await rm(root, { recursive: true, force: true });
}
