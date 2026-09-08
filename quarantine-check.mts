// The limit the round-six review left open. Refusing to move a step off a host protects the batch
// that step belongs to, and nothing else: the turn ends, the session takes another prompt, and that
// prompt is free to land on the same host and use the same directory while the abandoned tool is
// still inside its own execution. A settled workflow promise says Temporal stopped waiting. It does
// not say the body stopped.
//
// What is asserted here is the directory being refused, and what makes the refusal honest is that
// it survives the process: nothing here asks whether a pid is alive, because a tool's children can
// outlive the worker that spawned them and a dead pid would license reuse anyway.
//
// Usage: npx tsx quarantine-check.mts

import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { makeActivities } from "./src/activities.js";
import * as worktree from "./src/worktree.js";
import type { AgentSession } from "@earendil-works/pi-coding-agent";

const run = promisify(execFile);
const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

async function main() {
  const root = await mkdtemp(join(tmpdir(), "pi-quarantine-"));
  const sessions = join(root, "sessions");
  const project = join(root, "project");
  await mkdir(sessions, { recursive: true });
  await mkdir(project, { recursive: true });
  const sessionFile = join(sessions, "s1.jsonl");
  process.env.PI_TEMPORAL_DATA = join(root, "host-a");

  await run("git", ["init", "-q", project]);
  await writeFile(join(project, "README.md"), "one\n");
  await worktree.capture(project, sessionFile, { seed: true });

  // Turn 1, step 1: a tool starts writing and never comes back. Temporal times its attempt out,
  // the driver refuses to move the rest of that step, and the turn ends there.
  const stranded = { turn: "turn-1", step: 1, callId: "call-a" };
  await worktree.beginWrite(project, stranded);

  // Turn 2 arrives. It is a new turn, so the step guard the driver applies has nothing to say
  // about it, and the session is free to run it here.
  const next = { turn: "turn-2", step: 1, callId: "call-b" };
  let refusedRestore: unknown;
  await worktree.ensure(project, sessionFile, next).catch((err) => {
    refusedRestore = err;
  });
  check("a later turn is refused the directory", refusedRestore instanceof worktree.Quarantined, {
    refusedRestore: String(refusedRestore),
  });

  let refusedCapture: unknown;
  await worktree.capture(project, sessionFile, { current: next }).catch((err) => {
    refusedCapture = err;
  });
  check("and cannot publish from it either", refusedCapture instanceof worktree.Quarantined, {
    refusedCapture: String(refusedCapture),
  });

  // The reason for the refusal is in the message, because an operator is the one who acts on it.
  check(
    "the refusal names the call that never returned",
    String(refusedRestore).includes("call-a") && String(refusedRestore).includes("turn-1"),
    String(refusedRestore),
  );

  // A sibling of the same step is not a stranded writer. Two tools of one step run at once here on
  // purpose, and refusing them would be refusing the feature.
  const sibling = { turn: "turn-1", step: 1, callId: "call-c" };
  let siblingOk = true;
  await worktree.ensure(project, sessionFile, sibling).catch(() => {
    siblingOk = false;
  });
  check("a sibling of the same step is not refused", siblingOk);

  // The next step of the same turn is not the same step, and a writer the step before it left
  // behind is exactly as unaccounted for as one from another turn.
  let refusedNextStep = false;
  await worktree.ensure(project, sessionFile, { turn: "turn-1", step: 2, callId: "call-x" }).catch(() => {
    refusedNextStep = true;
  });
  check("the next step of the same turn is refused too", refusedNextStep);

  // The abandoned tool finally returns. Whatever it did, it is not doing it any more, so the
  // directory is usable again with no operator involved.
  await worktree.endWrite(project, stranded.callId);
  let reusable = true;
  await worktree.ensure(project, sessionFile, next).catch(() => {
    reusable = false;
  });
  check("a writer that comes back releases the directory", reusable);

  // A worker that died mid-tool leaves the marker behind for good, which is the case that needs a
  // person. What that person has is a command, and it says how many writers it forgot.
  await worktree.beginWrite(project, { turn: "turn-3", step: 1, callId: "call-d" });
  let refusedAfterDeath = false;
  await worktree.ensure(project, sessionFile, { turn: "turn-4", step: 1, callId: "call-e" }).catch(() => {
    refusedAfterDeath = true;
  });
  check("a marker no process removes keeps the directory refused", refusedAfterDeath);
  const cleared = await worktree.clearWriters(project);
  check("clearing it says what it forgot", cleared === 1, cleared);
  let afterClear = true;
  await worktree.ensure(project, sessionFile, { turn: "turn-4", step: 1, callId: "call-e" }).catch(() => {
    afterClear = false;
  });
  check("and the directory works again", afterClear);

  // And the activity is what writes the marker. Driven through the real tool activity, with the
  // session faked: what is asserted is that the marker exists while the tool body runs and is gone
  // once it returns, because everything above rests on the activity doing that.
  const marked: boolean[] = [];
  const seen = { turn: "turn-5", step: 1, callId: "call-f" };
  const activities = makeActivities(
    { projectDir: project, shipTree: true },
    {
      openSession: async () =>
        ({
          state: {
            messages: [
              { role: "user", content: "run", timestamp: Date.now() },
              { role: "assistant", content: [{ type: "toolCall", id: seen.callId, name: "probe" }] },
            ],
          },
          async runToolCall() {
            // Inside the body: this is the window the quarantine exists for.
            marked.push(
              (await worktree
                .ensure(project, sessionFile, { turn: "turn-6", step: 1, callId: "other" })
                .then(() => false)
                .catch(() => true)),
            );
            return { message: { role: "toolResult", toolCallId: seen.callId, content: [] } };
          },
          dispose() {},
        }) as unknown as AgentSession,
    },
  );
  await activities
    .runToolCall({
      sessionId: "s1",
      sessionFile,
      turn: seen.turn,
      step: seen.step,
      call: { id: seen.callId, name: "probe" },
    })
    .catch(() => undefined);
  check("the activity marks the directory while its tool runs", marked[0] === true, marked);
  let afterActivity = true;
  await worktree.ensure(project, sessionFile, { turn: "turn-6", step: 1, callId: "other" }).catch(() => {
    afterActivity = false;
  });
  check("and unmarks it when the tool returns", afterActivity);

  // The file the stranded tool wrote is still there. Quarantine refuses reuse; it does not throw
  // away what the tool did, which is the other half of not losing work.
  await writeFile(join(project, "late.txt"), "written by the stranded tool\n");
  check("nothing the stranded tool wrote is removed", (await readFile(join(project, "late.txt"), "utf8")).length > 0);

  await rm(root, { recursive: true, force: true });
  console.log(failures.length === 0 ? "quarantine-check: OK" : `quarantine-check: ${failures.length} failed`);
  process.exit(failures.length === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
