// The contract a step that lost its host is held to, end to end.
//
// It has two clauses, and they are the same two on both hosts of this project. The mechanisms
// differ because the substrates do: this one names the closed step in the shared directory beside
// the session file, and OpenCode's reuses the owner token that already fences its event log. A
// change to either belongs in both places, and the other one is
// `packages/temporal/test/lost-host.test.ts` in the OpenCode fork.
//
//   1. The seal that closes a step away from its host publishes nothing. Between the dispatch
//      failing and the closure being written there is a window where nothing fences the old host,
//      and the only thing that makes it harmless is that nobody else publishes during it.
//   2. Once the session has closed that step, what that host publishes for it is refused.
//
// A worker dying with a tool in flight costs that step, and it used to cost the whole turn with it.
// What made ending the turn the only safe answer was the tool: Temporal stops waiting for it, the
// process behind it keeps writing, and when it finishes it publishes against the tip it read, which
// reverts whatever ran in its place. The tip rule cannot catch that one, because the stale writer is
// standing exactly where it was told to stand.
//
// With both clauses in place the turn has nothing left to be protected from by ending, and it
// carries on with the next step. Take `closeStep` out of the seal and clause 2 fails: the abandoned
// tool publishes first and the work that replaced it is refused and set aside.
//
// The clause the two hosts do not share: this one sets the refused host's work aside under
// `salvage/`, where OpenCode leaves it on that host's disk and logs. Both keep it; neither
// publishes it.
//
// No server and no model key. Usage: npx tsx lost-host-check.mts

import assert from "node:assert/strict";
import { mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { AgentSession, TurnToolCallOutcome } from "@earendil-works/pi-coding-agent";
import { makeActivities } from "./src/activities.js";
import { makeSteppedStep } from "./src/l2-step.js";
import * as worktree from "./src/worktree.js";

const root = await mkdtemp(join(tmpdir(), "pi-lost-host-"));
const originalData = process.env.PI_TEMPORAL_DATA;
process.env.PI_TEMPORAL_DATA = join(root, "data");
const a = join(root, "host-a");
const b = join(root, "host-b");
const sessionFile = join(root, "session.jsonl");
const turn = "prompt";
const failure = new Error("the pinned attempt timed out with the tool still running");
const writer = { turn, step: 1, callId: "slow" };
let finishAbandoned: (() => Promise<unknown>) | undefined;
let seals = 0;

try {
  await mkdir(a);
  await mkdir(b);
  await writeFile(join(a, "seed.txt"), "seed\n");
  await worktree.capture(a, sessionFile, { seed: true });
  // The other host has served this session too, so both start on the tip. A host that never has is
  // `worktree-check`'s case, and it makes no difference to what this one is about.
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
    pinnedTo: () => ({
      runToolCall: async () => {
        // The host says a call is inside its own execution, and then Temporal gives up on the
        // attempt without stopping the body. Everything the body still has to do is left here.
        await worktree.beginWrite(a, writer);
        finishAbandoned = async () => {
          await writeFile(join(a, "from-the-abandoned-tool.txt"), "written after the step ended\n");
          await worktree.endWrite(a, writer.callId);
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
    resumesAfterLostHost: () => true,
    nonCancellable: async (body) => body(),
  });

  const bundles = async () =>
    (await readdir(`${sessionFile}.tree`)).filter((name) => name.endsWith(".bundle")).length;
  const shipped = await bundles();

  const result = await step({
    sessionId: "session", sessionFile, promptId: turn, text: "task", step: 1, retryAttempt: 0,
  });
  assert.equal(seals, 1, "the step records what it had before it hands the turn back");
  assert.equal(result.done, false, "the turn goes on rather than ending with the worker");
  console.log("PASS a step that lost its host hands the turn back instead of failing it");

  // Clause 1. That seal is standing in a directory that never ran the step's tools, and the host
  // that did may still be inside one, so it writes the step down and touches nothing else.
  assert.equal(await bundles(), shipped, "the seal that closed the step published nothing");
  console.log("PASS and the seal that closed it published nothing");

  // The abandoned tool finishes now, before anything else has moved the tip. This is the ordering
  // the tip rule cannot answer: the host is still standing on the tree it read, so its capture is
  // clean and it takes the project back to whatever it was doing.
  // Clause 2.
  const refused = await finishAbandoned!();
  assert.match(String(refused), /closed without this host/);
  console.log("PASS the host it lost cannot publish for that step afterwards");

  // And the next step, on a host that is not refused, moves the project as usual.
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
  // Refused is not the same as thrown away. What the tool did is the only record of what it did,
  // so restoring that host puts it where somebody can get it back.
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
