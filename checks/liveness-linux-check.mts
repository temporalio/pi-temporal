// The readings the refusal rests on, run where they are actually made.
//
// Everything about a stranded writer is answered from `/proc` on Linux and from `ps` and `lsof`
// everywhere else, and those are different code. A fleet runs the first one; a laptop runs the
// second, which is the only one the other checks ever reach. So this is the same questions asked
// inside a container, where `/proc` is what answers them.
//
// Usage: docker/liveness-check.sh   (or `npx tsx checks/liveness-linux-check.mts` on a Linux host)

import { spawn } from "node:child_process";
import { mkdir, mkdtemp, open, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { hostname, tmpdir, uptime } from "node:os";
import { join } from "node:path";
import * as worktree from "../src/worktree.js";

const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  const suffix = ok ? "" : ` (${JSON.stringify(detail)?.slice(0, 300)})`;
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${suffix}`);
  if (!ok) failures.push(what);
};

const kids: ReturnType<typeof spawn>[] = [];
function spawned(options: { cwd?: string; env?: NodeJS.ProcessEnv; script?: string }) {
  const child = spawn(process.execPath, ["-e", options.script ?? "setInterval(() => {}, 1000)"], {
    detached: true,
    stdio: "ignore",
    cwd: options.cwd,
    env: options.env ?? { PATH: process.env.PATH ?? "" },
  });
  kids.push(child);
  return child;
}

async function ended(child: ReturnType<typeof spawn>) {
  const exited = new Promise((resolve) => child.once("exit", resolve));
  child.kill("SIGKILL");
  await exited;
}

async function main() {
  if (process.platform !== "linux") throw new Error("this one is about /proc, so it needs Linux");
  const root = await mkdtemp(join(tmpdir(), "pi-liveness-"));
  const project = join(root, "project");
  const sessions = join(root, "sessions");
  await mkdir(project, { recursive: true });
  await mkdir(sessions, { recursive: true });
  process.env.PI_TEMPORAL_DATA = join(root, "host");
  const sessionFile = join(sessions, "s1.jsonl");
  await writeFile(join(project, "README.md"), "one\n");
  await worktree.capture(project, sessionFile, { seed: true });

  const later = { turn: "turn-2", step: 1, callId: "call-later" };
  const usable = async () =>
    await worktree.ensure(project, sessionFile, later).then(() => true, () => false);
  const refusal = async () =>
    await worktree.ensure(project, sessionFile, later).then(() => "", (err) => String(err));

  // A pid that is gone, in a group of its own that is now empty, carrying a name nothing else has.
  const dead = spawned({ env: { PATH: process.env.PATH ?? "", PI_TEMPORAL_WORKER: "gone" } });
  const deadPid = dead.pid!;
  await ended(dead);
  const markers = async () => {
    const trees = join(process.env.PI_TEMPORAL_DATA!, "trees");
    const [dir] = await readdir(trees).catch(() => [] as string[]);
    return dir ? join(trees, dir, "writers") : undefined;
  };
  const editMarker = async (patch: Record<string, unknown>) => {
    const dir = (await markers())!;
    const [name] = await readdir(dir);
    const note = JSON.parse(await readFile(join(dir, name), "utf8"));
    await writeFile(join(dir, name), JSON.stringify({ ...note, ...patch }));
  };
  const stale = {
    pid: deadPid,
    pgid: deadPid,
    worker: "gone",
    cgroup: "/nothing-is-in-this-one",
    host: hostname(),
    bootAt: Math.round(Date.now() - uptime() * 1000),
  };

  await worktree.beginWrite(project, { turn: "turn-1", step: 1, callId: "call-a" });
  await editMarker(stale);
  check("the /proc walk clears a writer that left nothing behind", await usable());
  check(
    "and takes the marker off the host",
    (await readdir((await markers())!).catch(() => [])).length === 0,
  );

  // A tool that put itself in a group of its own and kept nothing the worker gave it, standing in
  // the directory it writes.
  const standing = spawned({ cwd: project });
  await worktree.beginWrite(project, { turn: "turn-1", step: 1, callId: "call-a" });
  await editMarker(stale);
  check("a process whose working directory is in it keeps it refused", !(await usable()));
  check(
    "and the refusal says which process and why",
    (await refusal()).includes(`pid ${standing.pid} is in the directory`),
    await refusal(),
  );
  await ended(standing);
  check("and releases it when that process leaves", await usable());

  // And one that moved out of the directory but still holds a file under it open, which is what a
  // tool that daemonized the textbook way looks like: `chdir("/")`, and the work carries on.
  const kept = join(project, "held-open.txt");
  await writeFile(kept, "written by a tool that is still inside\n");
  const holder = spawned({
    cwd: "/",
    script:
      `require("node:fs").openSync(${JSON.stringify(kept)}, "r"); setInterval(() => {}, 1000)`,
  });
  // Its file is opened a moment after it starts, so the reading has to be taken after that.
  await new Promise((r) => setTimeout(r, 1500));
  await worktree.beginWrite(project, { turn: "turn-1", step: 1, callId: "call-a" });
  await editMarker(stale);
  check("a process holding a file under it open keeps it refused", !(await usable()));
  check("and the refusal names the file", (await refusal()).includes(kept), await refusal());
  await ended(holder);
  check("and that one releases it too", await usable());

  // The control group this process is in is readable at all, which is what the rule that answers
  // for a tool running as somebody else rests on.
  const ours = (await readFile("/proc/self/cgroup", "utf8")).split("\n")[0];
  check("this host reports a control group to record", ours.split(":").length >= 3, ours);
  await worktree.beginWrite(project, { turn: "turn-1", step: 1, callId: "call-a" });
  const note = JSON.parse(
    await readFile(join((await markers())!, (await readdir((await markers())!))[0]), "utf8"),
  );
  check(
    "and a marker written here carries it",
    typeof note.cgroup === "string" && note.cgroup.length > 0,
    note,
  );
  // Every process in a container shares one, so a marker naming this one says nothing either way.
  await editMarker({ ...stale, cgroup: note.cgroup });
  check("a marker naming the group this process is in still clears", await usable());

  // And whether this host could move that directory out of the way at all, which is the difference
  // between a stranded tool costing a directory and costing it until somebody comes. The reading is
  // a prediction for an operator; what decides is the rename. Both cases exist on any Linux host.
  check(
    "a plain directory can be set aside",
    (await worktree.cannotMoveAside(project)) === undefined,
  );
  const mounted = await worktree.cannotMoveAside("/proc");
  check("a mount point cannot, and says so", mounted?.includes("mount point") === true, mounted);

  await rm(root, { recursive: true, force: true });
  console.log(
    failures.length === 0
      ? "liveness-linux-check: OK"
      : `liveness-linux-check: ${failures.length} failed`,
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
