// Checks the Linux `/proc` path of stranded-writer detection, which other checks on macOS never
// reach. Plants a stale writer marker, then asserts the directory stays refused while a process
// has its cwd or an open file in it, and clears once nothing does.
//
// Usage: docker/liveness-check.sh   (or `npx tsx checks/liveness-linux-check.mts` on a Linux host)

import { spawn } from "node:child_process";
import { mkdir, mkdtemp, open, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { hostname, tmpdir, uptime } from "node:os";
import { join } from "node:path";
import * as worktree from "../src/tree/worktree.js";

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

  // A detached tool with nothing from the worker, whose cwd is the project.
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

  // A daemonized tool: `chdir("/")` but still holding a file under the project open.
  const kept = join(project, "held-open.txt");
  await writeFile(kept, "written by a tool that is still inside\n");
  const holder = spawned({
    cwd: "/",
    script:
      `require("node:fs").openSync(${JSON.stringify(kept)}, "r"); setInterval(() => {}, 1000)`,
  });
  // Wait until the child has opened its file.
  await new Promise((r) => setTimeout(r, 1500));
  await worktree.beginWrite(project, { turn: "turn-1", step: 1, callId: "call-a" });
  await editMarker(stale);
  check("a process holding a file under it open keeps it refused", !(await usable()));
  check("and the refusal names the file", (await refusal()).includes(kept), await refusal());
  await ended(holder);
  check("and that one releases it too", await usable());

  // The cgroup rule for tools running as another user needs this to be readable.
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
  // Every process in a container shares this cgroup, so it proves nothing either way.
  await editMarker({ ...stale, cgroup: note.cgroup });
  check("a marker naming the group this process is in still clears", await usable());

  // `cannotMoveAside` is a hint for operators. The rename itself decides.
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
