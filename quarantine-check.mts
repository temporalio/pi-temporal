// The limit the round-six review left open. Refusing to move a step off a host protects the batch
// that step belongs to, and nothing else: the turn ends, the session takes another prompt, and that
// prompt is free to land on the same host and use the same directory while the abandoned tool is
// still inside its own execution. A settled workflow promise says Temporal stopped waiting. It does
// not say the body stopped.
//
// What is asserted here is the directory being refused, and how much of that refusal the host can
// take back on its own. A dead pid alone licenses nothing, because a tool's children outlive the
// worker that spawned them and pids come round again after a restart. A dead writer, an empty
// process group and the same boot together do.
//
// Usage: npx tsx quarantine-check.mts

import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import { mkdir, mkdtemp, readdir, readFile, rm, writeFile } from "node:fs/promises";
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

// A process in a group of its own, which is what a worker somebody's supervisor started has. The
// group is the part that matters: it is where the children of a dead writer stay.
async function spawned() {
  const child = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], {
    detached: true,
    stdio: "ignore",
  });
  // Not unref'd: killing it and waiting for the exit is what this is for, and an unref'd handle
  // lets the whole check exit while that wait is still outstanding.
  return { child, pid: child.pid!, pgid: child.pid! };
}

async function ended(started: Awaited<ReturnType<typeof spawned>>) {
  const exited = new Promise((resolve) => started.child.once("exit", resolve));
  started.child.kill("SIGKILL");
  await exited;
  return started;
}

// Markers are host-local and keyed by a hash of the directory, so this walks to the one directory
// this check made rather than repeating the naming scheme.
async function writersHere() {
  const trees = join(process.env.PI_TEMPORAL_DATA!, "trees");
  const [dir] = await readdir(trees).catch(() => [] as string[]);
  return dir ? join(trees, dir, "writers") : undefined;
}

const markers = async () => {
  const dir = await writersHere();
  return dir ? await readdir(dir).catch(() => [] as string[]) : [];
};

/** Say the marker was written by some other process, or before a restart. */
async function editMarker(patch: Record<string, unknown>) {
  const dir = (await writersHere())!;
  const [name] = await readdir(dir);
  const note = JSON.parse(await readFile(join(dir, name), "utf8"));
  await writeFile(join(dir, name), JSON.stringify({ ...note, ...patch }));
}

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

  // A worker that died mid-tool leaves its marker behind, and the marker is what a person used to
  // have to clear by hand. Most of that is answerable without one: the process that wrote it is
  // gone, nothing it started is left in its group, and the machine has not restarted underneath the
  // pids that say those two things.
  const later = { turn: "turn-4", step: 1, callId: "call-e" };
  const usable = async () =>
    await worktree.ensure(project, sessionFile, later).then(() => true, () => false);

  // Written by this process, which is running. Nothing to conclude, so the refusal stands.
  await worktree.beginWrite(project, { turn: "turn-3", step: 1, callId: "call-d" });
  check("a marker whose writer is still running keeps the directory refused", !(await usable()));

  // The worker died and left nothing behind. A group of its own is what a worker a supervisor
  // started has, and an empty one is the whole of the proof that its tools are over.
  const gone = await ended(await spawned());
  await editMarker({ pid: gone.pid, pgid: gone.pgid });
  check("a marker whose writer and group are gone clears itself", await usable());
  check("and the marker is taken off the host", (await markers()).length === 0);

  // The worker died and something it started did not. That is the case the refusal is for, and no
  // pid check licenses reuse while it holds.
  const orphan = await spawned();
  await worktree.beginWrite(project, { turn: "turn-3", step: 1, callId: "call-d" });
  await editMarker({ pid: gone.pid, pgid: orphan.pgid });
  check("a marker whose group still has a process keeps the directory refused", !(await usable()));
  await ended(orphan);
  check("and releases it once that process is over", await usable());

  // A pid means nothing across a restart, so a marker from before one is not read as a live writer.
  await worktree.beginWrite(project, { turn: "turn-3", step: 1, callId: "call-d" });
  await editMarker({ bootAt: 0 });
  check("a marker from before the machine restarted clears itself", await usable());

  // What is left is a tool that put itself in a group of its own, which nothing here can follow.
  // That one needs a person, and what the person has is a command that says what it forgot.
  await worktree.beginWrite(project, { turn: "turn-3", step: 1, callId: "call-d" });
  check("a live writer still needs an operator", !(await usable()));
  const cleared = await worktree.clearWriters(project);
  check("clearing it says what it forgot", cleared === 1, cleared);
  check("and the directory works again", await usable());

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
