// The limit the round-six review left open. Refusing to move a step off a host protects the batch
// that step belongs to, and nothing else: the turn ends, the session takes another prompt, and that
// prompt is free to land on the same host and use the same directory while the abandoned tool is
// still inside its own execution. A settled workflow promise says Temporal stopped waiting. It does
// not say the body stopped.
//
// What is asserted here is the directory being refused, and how much of that refusal the host can
// take back on its own. A dead pid alone licenses nothing, because a tool's children outlive the
// worker that spawned them and pids come round again after a restart. A dead writer, nothing left
// running that carries its name, an empty process group and the same boot together do.
//
// Usage: npx tsx checks/quarantine-check.mts

import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { mkdir, mkdtemp, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { hostname, tmpdir, uptime } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { ApplicationFailure } from "@temporalio/common";
import { makeActivities } from "../src/activities.js";
import * as worktree from "../src/worktree.js";
import type { AgentSession } from "@earendil-works/pi-coding-agent";

const run = promisify(execFile);
const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

// A process in a group of its own, which is what a worker somebody's supervisor started has, and
// what a tool that daemonizes gives itself. It carries a worker name of this check's choosing, so
// what each case turns on is the one thing that case is about.
// Per run, because a name is what the host looks for and an assertion that fails before its child
// is killed leaves that child running. A fixed name would then answer for every later run.
const runToken = randomBytes(4).toString("hex");
const kids: ReturnType<typeof spawn>[] = [];

function spawned(worker?: string) {
  const child = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], {
    detached: true,
    stdio: "ignore",
    // Without one, it gets this process's environment, which is the case that says the name is
    // exported rather than only written into the marker.
    env:
      worker === undefined
        ? process.env
        : { ...process.env, PI_TEMPORAL_WORKER: `${worker}-${runToken}` },
  });
  kids.push(child);
  // Not unref'd: killing it and waiting for the exit is what this is for, and an unref'd handle
  // lets the whole check exit while that wait is still outstanding.
  return { child, pid: child.pid!, pgid: child.pid! };
}

/** The group this process is in, which is the one a marker cannot be judged by. */
async function ourGroup() {
  const { stdout } = await run("ps", ["-o", "pgid=", "-p", String(process.pid)]);
  return Number.parseInt(stdout.trim(), 10);
}

async function ended(started: ReturnType<typeof spawned>) {
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
  await worktree
    .ensure(project, sessionFile, { turn: "turn-1", step: 2, callId: "call-x" })
    .catch(() => {
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

  // The worker died and left nothing behind: no process of its own, nothing carrying its name, and
  // an empty group. That is the whole of the proof that its tools are over.
  const gone = await ended(spawned("gone"));
  const stale = { pid: gone.pid, pgid: gone.pgid, worker: `gone-${runToken}` };
  await editMarker(stale);
  check("a marker whose writer left nothing running clears itself", await usable());
  check("and the marker is taken off the host", (await markers()).length === 0);

  // The worker died and something it started did not. That is the case the refusal is for, and no
  // pid check licenses reuse while it holds.
  const orphan = spawned("orphaned");
  await worktree.beginWrite(project, { turn: "turn-3", step: 1, callId: "call-d" });
  await editMarker({ ...stale, pgid: orphan.pgid, worker: `orphaned-${runToken}` });
  check("a marker whose worker left work running keeps the directory refused", !(await usable()));
  await ended(orphan);
  check("and releases it once that work is over", await usable());

  // A tool that asks for a group of its own is out of the group check's reach. What it cannot put
  // down is the name its worker left in the environment it inherited.
  const escaped = spawned("escaped");
  await worktree.beginWrite(project, { turn: "turn-3", step: 1, callId: "call-d" });
  await editMarker({ ...stale, worker: `escaped-${runToken}` });
  check("a tool that left its worker's group is found by what it inherited", !(await usable()));
  await ended(escaped);
  check("and that one releases the directory too", await usable());

  // A tool that leaves the group and is handed an environment of somebody else's choosing has put
  // down everything the worker gave it. What it cannot put down and go on writing is the directory
  // it writes, so that is the last reading, and it is the one that does not depend on the tool
  // having kept anything.
  const standing = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], {
    detached: true,
    stdio: "ignore",
    cwd: project,
    env: { PATH: process.env.PATH ?? "" },
  });
  kids.push(standing);
  await worktree.beginWrite(project, { turn: "turn-3", step: 1, callId: "call-d" });
  await editMarker({ ...stale, worker: "nothing-carries-this" });
  check(
    "a tool standing in the directory keeps it refused, whatever it put down",
    !(await usable()),
  );
  const said = await worktree
    .ensure(project, sessionFile, later)
    .then(
      () => "",
      (err) => String(err),
    );
  check(
    "and the refusal says which process and why",
    said.includes(`pid ${standing.pid} is in the directory`),
    said.slice(0, 200),
  );
  await ended({ child: standing, pid: standing.pid!, pgid: standing.pid! });
  check("and releases it when that process leaves", await usable());

  // The name has to reach the tool, not only the marker, or none of the above is about anything
  // that runs here. This child is given no environment of its own, so the only way it can carry
  // the name is that the worker put it where a child would inherit it.
  const inheriting = spawned();
  await worktree.beginWrite(project, { turn: "turn-3", step: 1, callId: "call-d" });
  await editMarker({ ...stale, worker: process.env.PI_TEMPORAL_WORKER });
  check("a tool inherits the name from the worker that spawned it", !(await usable()), {
    exported: process.env.PI_TEMPORAL_WORKER !== undefined,
  });
  await ended(inheriting);
  check("and the directory is free once it exits", await usable());

  // A worker restarted from the same shell is in the group its predecessor was in, so the group
  // answers for this process rather than for the marker. What the dead worker started is what
  // decides, and it started nothing.
  await worktree.beginWrite(project, { turn: "turn-3", step: 1, callId: "call-d" });
  await editMarker({ ...stale, pgid: await ourGroup() });
  check("a marker from a predecessor in this process's own group clears itself", await usable());

  // The control group is Linux's, and a tool keeps it through `setsid` and through `sudo`, which
  // empties the environment it passes on. A Mac has no such thing to read, so the rule is asked on
  // its own, with the reading a Linux host would have made handed to it.
  const readings = {
    groups: new Set<number>(), workers: new Set<string>(), holding: [],
    ourGroup: await ourGroup(), ourCgroup: "/the-one-this-process-is-in",
  };
  const dead = {
    turn: "turn-3", step: 1, callId: "call-d", host: hostname(), pid: gone.pid,
    // The stamp the module compares against, not the wall clock: a marker dated now would read as
    // one from before a restart and never reach the rule this is about.
    bootAt: Math.round(Date.now() - uptime() * 1000), started: new Date().toISOString(),
  };
  const inCgroup = (note: Record<string, unknown>, live: string[]) =>
    worktree.insideBecause(
      { ...dead, ...note } as Parameters<typeof worktree.insideBecause>[0],
      { ...readings, cgroups: new Set(live) },
    );
  check(
    "a writer whose control group still holds something keeps the directory refused",
    inCgroup({ cgroup: "/system.slice/pi-worker-1.service" }, ["/system.slice/pi-worker-1.service"])
      ?.includes("still in /system.slice/pi-worker-1.service") === true,
  );
  check(
    "an empty one releases it",
    inCgroup({ cgroup: "/system.slice/pi-worker-1.service" }, [
      "/system.slice/pi-worker-2.service",
    ]) === undefined,
  );
  check(
    "and the group this process is in says nothing either way",
    inCgroup({ cgroup: readings.ourCgroup }, [readings.ourCgroup]) === undefined,
  );

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
              {
                role: "assistant",
                content: [{ type: "toolCall", id: seen.callId, name: "probe" }],
              },
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
  await worktree
    .ensure(project, sessionFile, { turn: "turn-6", step: 1, callId: "other" })
    .catch(() => {
      afterActivity = false;
    });
  check("and unmarks it when the tool returns", afterActivity);

  // And what the activity does with a refusal decides what it costs the session. A refused
  // directory is this host saying no, not a failing unit of work: the same dispatch runs fine
  // somewhere else, so the failure carries its own retry delay rather than climbing the backoff a
  // failing activity earns. Without it, the host that answers first and refuses fastest pushes the
  // next attempt minutes out while a free host sits idle.
  await worktree.beginWrite(project, { turn: "turn-7", step: 1, callId: "call-g" });
  const refusal = await activities
    .runToolCall({
      sessionId: "s1",
      sessionFile,
      turn: "turn-8",
      step: 1,
      call: { id: "call-h", name: "probe" },
    })
    .then(() => undefined, (err: unknown) => err);
  check(
    "a refusal asks for a retry rather than a backoff",
    refusal instanceof ApplicationFailure &&
      refusal.nextRetryDelay !== undefined &&
      refusal.nonRetryable !== true,
    String(refusal).slice(0, 120),
  );
  await worktree.clearWriters(project);

  // And the directory the session built for itself is not refused at all: it is moved out of the
  // way, and the session gets a fresh one here. The writer nobody can account for goes on writing
  // the directory it has open, under its new name, where nothing it does reaches what comes next.
  // That is the case the readings above cannot close, and it costs a rename.
  const built = join(root, "built-here");
  await mkdir(built, { recursive: true });
  await worktree.ensure(built, sessionFile);
  check(
    "a host builds an empty directory for the session",
    (await readdir(built)).includes("README.md"),
  );
  await worktree.beginWrite(built, { turn: "turn-10", step: 1, callId: "call-i" });
  await writeFile(join(built, "written-by-the-stranded-tool.txt"), "still writing\n");
  let movedOn = true;
  await worktree
    .ensure(built, sessionFile, { turn: "turn-11", step: 1, callId: "call-j" })
    .catch(() => {
      movedOn = false;
    });
  check("a directory this session built is moved aside rather than refused", movedOn);
  const movedTo = (await readdir(root)).filter((name) => name.startsWith("built-here.stranded."));
  check("and what the tool wrote is where somebody can find it", movedTo.length === 1, movedTo);
  check(
    "which is not where the session is now",
    (await readdir(join(root, movedTo[0] ?? "nowhere")).catch(() => [] as string[])).includes(
      "written-by-the-stranded-tool.txt",
    ) && !(await readdir(built)).includes("written-by-the-stranded-tool.txt"),
  );
  check(
    "the fresh one holds what the session shipped",
    (await readdir(built)).includes("README.md"),
  );
  check("and no marker is left on it", (await markers()).length === 0);

  // And never while one of this step's own tools is inside it: moving the directory then would take
  // the floor out from under a call that is running and accounted for.
  await worktree.beginWrite(built, { turn: "turn-12", step: 1, callId: "call-k" });
  await worktree.beginWrite(built, { turn: "turn-13", step: 1, callId: "call-l" });
  let movedUnderOurOwn = true;
  await worktree
    .ensure(built, sessionFile, { turn: "turn-13", step: 1, callId: "call-m" })
    .catch(() => {
      movedUnderOurOwn = false;
    });
  check(
    "a directory holding one of this step's own calls is refused, not moved",
    !movedUnderOurOwn,
  );
  check(
    "and it stays where it is",
    (await readdir(root)).filter((name) => name.startsWith("built-here.stranded.")).length === 1,
  );
  await worktree.clearWriters(built);

  // The file the stranded tool wrote is still there. Quarantine refuses reuse; it does not throw
  // away what the tool did, which is the other half of not losing work.
  await writeFile(join(project, "late.txt"), "written by the stranded tool\n");
  check(
    "nothing the stranded tool wrote is removed",
    (await readFile(join(project, "late.txt"), "utf8")).length > 0,
  );

  await rm(root, { recursive: true, force: true });
  console.log(
    failures.length === 0
      ? "quarantine-check: OK"
      : `quarantine-check: ${failures.length} failed`,
  );
  process.exitCode = failures.length === 0 ? 0 : 1;
}

main()
  .catch((err) => {
    console.error(err);
    process.exitCode = 1;
  })
  .finally(() => {
    // Whatever happened above, nothing this check started is left carrying a name a later run
    // would find.
    for (const kid of kids) kid.kill("SIGKILL");
    setTimeout(() => process.exit(process.exitCode ?? 0), 200);
  });
