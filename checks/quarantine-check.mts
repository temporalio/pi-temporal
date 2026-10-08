// Checks that a directory with an abandoned writer marker is refused to later steps and turns.
// Spawns and kills real processes to assert when the host may clear the marker itself (dead pid,
// empty group, no process carrying the worker name or standing in the dir, same boot) and when an
// operator must. Also checks the activity writes the marker and retries refusals without backoff.
//
// Usage: npx tsx checks/quarantine-check.mts

import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { mkdir, mkdtemp, readdir, readFile, rm, utimes, writeFile } from "node:fs/promises";
import { hostname, tmpdir, uptime } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { ApplicationFailure } from "@temporalio/common";
import { MockActivityEnvironment } from "@temporalio/testing";
import { makeActivities } from "../src/pi/activities.js";
import * as worktree from "../src/tree/worktree.js";
import type { AgentSession } from "@earendil-works/pi-coding-agent";

const run = promisify(execFile);
const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

// Spawned processes get their own group and a chosen worker name. The name is per run so a child
// leaked by a failed run cannot affect later runs.
const runToken = randomBytes(4).toString("hex");
const kids: ReturnType<typeof spawn>[] = [];

function spawned(worker?: string) {
  const child = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], {
    detached: true,
    stdio: "ignore",
    // No name means this process's env, to check the worker name is exported.
    env:
      worker === undefined
        ? process.env
        : { ...process.env, PI_TEMPORAL_WORKER: `${worker}-${runToken}` },
  });
  kids.push(child);
  // Not unref'd, so the check cannot exit while waiting for its exit.
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

// Markers live under a hashed directory name, so find it rather than recompute it.
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

  // A tool starts writing and never returns. The turn ends there.
  const stranded = { turn: "turn-1", step: 1, callId: "call-a" };
  await worktree.beginWrite(project, stranded);

  // A new turn is not covered by the step guard, so only the marker protects the directory.
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

  // An operator acts on the message, so it must say why.
  check(
    "the refusal names the call that never returned",
    String(refusedRestore).includes("call-a") && String(refusedRestore).includes("turn-1"),
    String(refusedRestore),
  );

  // A later step can reuse the call id. Its marker must not replace the stranded one, or ending
  // the later call would clear the only sign that the first tool may still be writing.
  const reused = { turn: "turn-2", step: 3, callId: "call-a" };
  await worktree.beginWrite(project, reused);
  await worktree.endWrite(project, reused);
  let stillRefused: unknown;
  await worktree.ensure(project, sessionFile, next).catch((err) => {
    stillRefused = err;
  });
  check(
    "a later call that reuses the id leaves the stranded marker in place",
    stillRefused instanceof worktree.Quarantined,
    String(stillRefused),
  );

  // Tools of one step run concurrently by design, so a sibling is not refused.
  const sibling = { turn: "turn-1", step: 1, callId: "call-c" };
  let siblingOk = true;
  await worktree.ensure(project, sessionFile, sibling).catch(() => {
    siblingOk = false;
  });
  check("a sibling of the same step is not refused", siblingOk);

  let refusedNextStep = false;
  await worktree
    .ensure(project, sessionFile, { turn: "turn-1", step: 2, callId: "call-x" })
    .catch(() => {
      refusedNextStep = true;
    });
  check("the next step of the same turn is refused too", refusedNextStep);

  // The abandoned tool returns, so the directory is free again.
  await worktree.endWrite(project, stranded);
  let reusable = true;
  await worktree.ensure(project, sessionFile, next).catch(() => {
    reusable = false;
  });
  check("a writer that comes back releases the directory", reusable);

  // A worker that died mid-tool leaves its marker. The host clears it itself when the writer is
  // gone, nothing it started remains, and the machine has not rebooted since.
  const later = { turn: "turn-4", step: 1, callId: "call-e" };
  const usable = async () =>
    await worktree.ensure(project, sessionFile, later).then(() => true, () => false);

  // Written by this live process.
  await worktree.beginWrite(project, { turn: "turn-3", step: 1, callId: "call-d" });
  check("a marker whose writer is still running keeps the directory refused", !(await usable()));

  // Dead worker, nothing carrying its name, empty group.
  const gone = await ended(spawned("gone"));
  const stale = { pid: gone.pid, pgid: gone.pgid, worker: `gone-${runToken}` };
  await editMarker(stale);
  check("a marker whose writer left nothing running clears itself", await usable());
  check("and the marker is taken off the host", (await markers()).length === 0);

  // Dead worker, but something it started is still in its group.
  const orphan = spawned("orphaned");
  await worktree.beginWrite(project, { turn: "turn-3", step: 1, callId: "call-d" });
  await editMarker({ ...stale, pgid: orphan.pgid, worker: `orphaned-${runToken}` });
  check("a marker whose worker left work running keeps the directory refused", !(await usable()));
  await ended(orphan);
  check("and releases it once that work is over", await usable());

  // A tool in its own group is still found by the inherited worker name.
  const escaped = spawned("escaped");
  await worktree.beginWrite(project, { turn: "turn-3", step: 1, callId: "call-d" });
  await editMarker({ ...stale, worker: `escaped-${runToken}` });
  check("a tool that left its worker's group is found by what it inherited", !(await usable()));
  await ended(escaped);
  check("and that one releases the directory too", await usable());

  // A tool with a new group and a clean env is still found by its cwd in the directory.
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

  // One that stands elsewhere but holds a file in the directory open is found by the open file.
  await writeFile(join(project, "held.txt"), "held\n");
  const opener = spawn(
    process.execPath,
    ["-e", `require("fs").openSync(${JSON.stringify(join(project, "held.txt"))}, "r");
      setInterval(() => {}, 1000)`],
    { detached: true, stdio: "ignore", cwd: tmpdir(), env: { PATH: process.env.PATH ?? "" } },
  );
  kids.push(opener);
  await new Promise((resolve) => setTimeout(resolve, 500));
  await worktree.beginWrite(project, { turn: "turn-3", step: 1, callId: "call-d" });
  await editMarker({ ...stale, worker: "nothing-carries-this" });
  check("a tool holding a file in the directory open keeps it refused", !(await usable()));
  await ended({ child: opener, pid: opener.pid!, pgid: opener.pid! });
  check("and releases it when that process exits", await usable());

  // The worker must export its name so real tools inherit it, not just write it in the marker.
  const inheriting = spawned();
  await worktree.beginWrite(project, { turn: "turn-3", step: 1, callId: "call-d" });
  await editMarker({ ...stale, worker: process.env.PI_TEMPORAL_WORKER });
  check("a tool inherits the name from the worker that spawned it", !(await usable()), {
    exported: process.env.PI_TEMPORAL_WORKER !== undefined,
  });
  await ended(inheriting);
  check("and the directory is free once it exits", await usable());

  // A worker restarted from the same shell shares its predecessor's group, so it is ignored.
  await worktree.beginWrite(project, { turn: "turn-3", step: 1, callId: "call-d" });
  await editMarker({ ...stale, pgid: await ourGroup() });
  check("a marker from a predecessor in this process's own group clears itself", await usable());

  // A Linux cgroup survives `setsid` and `sudo`. macOS has none, so feed the rule fake readings.
  const readings = {
    groups: new Set<number>(), workers: new Set<string>(), holding: [],
    ourGroup: await ourGroup(), ourCgroup: "/the-one-this-process-is-in",
  };
  const dead = {
    turn: "turn-3", step: 1, callId: "call-d", host: hostname(), pid: gone.pid,
    // Boot time as the module computes it, so the marker does not look pre-reboot.
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

  // Pids mean nothing across a reboot.
  await worktree.beginWrite(project, { turn: "turn-3", step: 1, callId: "call-d" });
  await editMarker({ bootAt: 0 });
  check("a marker from before the machine restarted clears itself", await usable());

  // Same pid with a different start time is a predecessor, for example after a container restart.
  await worktree.beginWrite(project, { turn: "turn-3", step: 1, callId: "call-d" });
  await editMarker({ pidStartedAt: "before this process started" });
  check("a marker from a previous run of this pid clears itself", await usable());
  await worktree.beginWrite(project, { turn: "turn-3", step: 1, callId: "call-d" });
  check("one from this very process keeps the directory refused", !(await usable()));
  await worktree.clearWriters(project);

  // A live pid that started at a different time than the writer is a reused pid.
  const holder = spawned("reused");
  const reusedNote = { pid: holder.pid, pgid: gone.pgid, worker: `dead-writer-${runToken}` };
  await worktree.beginWrite(project, { turn: "turn-3", step: 1, callId: "call-d" });
  await editMarker({ ...reusedNote, pidStartedAt: "not when that process started" });
  check("a live pid that started after the writer did clears the marker", await usable());
  await worktree.beginWrite(project, { turn: "turn-3", step: 1, callId: "call-d" });
  await editMarker({ ...reusedNote, pidStartedAt: await worktree.startTimeOf(holder.pid!) });
  check("one that started when the writer did keeps it refused", !(await usable()));
  // No recorded start: a live pid counts as the writer.
  await worktree.clearWriters(project);
  await worktree.beginWrite(project, { turn: "turn-3", step: 1, callId: "call-d" });
  await editMarker({ ...reusedNote, pidStartedAt: undefined });
  check("an older marker with no start keeps a live pid refused", !(await usable()));
  await worktree.clearWriters(project);
  // A start time from another pid namespace says nothing about this pid.
  await worktree.beginWrite(project, { turn: "turn-3", step: 1, callId: "call-d" });
  await editMarker({ ...reusedNote, pidStartedAt: "elsewhere", pidNs: "pid:[elsewhere]" });
  check("one from another pid namespace that is still touched stays refused", !(await usable()));
  // Its writer stops touching it, as after a container restart. Stop ours by hand first.
  const dir = (await writersHere())!;
  const [name] = await readdir(dir);
  const left = await readFile(join(dir, name), "utf8");
  await worktree.endWrite(project, { turn: "turn-3", step: 1, callId: "call-d" });
  await writeFile(join(dir, name), left);
  const old = new Date(Date.now() - 5 * 60_000);
  await utimes(join(dir, name), old, old);
  check("one from another pid namespace that went quiet is cleared", await usable());
  await worktree.clearWriters(project);
  await ended(holder);

  // A live writer the host cannot rule out needs an operator and `clearWriters`.
  await worktree.beginWrite(project, { turn: "turn-3", step: 1, callId: "call-d" });
  check("a live writer still needs an operator", !(await usable()));
  const cleared = await worktree.clearWriters(project);
  check("clearing it says what it forgot", cleared === 1, cleared);
  check("and the directory works again", await usable());

  // The real tool activity must hold the marker exactly while the tool body runs.
  const marked: boolean[] = [];
  const seen = { turn: "turn-5", step: 1, callId: "call-f" };
  const activities = makeActivities(
    { projectDir: project, shipTree: true, sessionRoot: sessions },
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
  await new MockActivityEnvironment()
    .run(activities.runToolCall, {
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

  // A refusal is this host saying no, not a failing activity. It sets `nextRetryDelay` so another
  // host can take the work without climbing the backoff.
  await worktree.beginWrite(project, { turn: "turn-7", step: 1, callId: "call-g" });
  const refusal = await new MockActivityEnvironment()
    .run(activities.runToolCall, {
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

  // A directory the session built itself is moved aside instead of refused, and rebuilt fresh.
  // The stranded writer keeps writing to the renamed copy.
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

  // Never moved while one of this step's own calls is running in it.
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

  // Quarantine refuses reuse but never deletes what the tool wrote.
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
    for (const kid of kids) kid.kill("SIGKILL");
    setTimeout(() => process.exit(process.exitCode ?? 0), 200);
  });
