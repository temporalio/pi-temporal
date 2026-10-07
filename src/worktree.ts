// Moves a session's project files between hosts, so a step that resumes on another worker finds
// the work the last one did. Each capture writes a git bundle beside the session log, and a host
// that is behind unbundles what it has not seen. The shadow repository is host-local and points at
// the work tree from outside, so the project's own `.git` is never touched.
//
// Rule: only a host standing on the tip may add to it, and nothing on a worker may establish it.
// The client sends the project. A stale tool that writes after its host is restored is not caught
// here, which is why the driver won't move a step off a host whose attempt started.
//
// A step closed away from its host is held to two clauses (see `lost-host-check.mts`):
//   1. The seal that closes it publishes nothing, since nothing fences the old host until then.
//   2. Once the session has closed that step, what that host publishes for it is refused.

import { execFile } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import {
  access,
  mkdir,
  readFile,
  readdir,
  readlink,
  realpath,
  rename,
  rm,
  stat,
  utimes,
  writeFile,
} from "node:fs/promises";
import { constants } from "node:fs";
import { homedir, hostname, uptime } from "node:os";
import { dirname, join, resolve } from "node:path";
import { promisify } from "node:util";
import { withSessionLock } from "./session-lock.js";

const execFileAsync = promisify(execFile);
const { W_OK } = constants;

// Per session, or two sessions capturing one directory race between `update-ref` and
// `bundle create`.
const snapRef = (sessionFile: string) =>
  `refs/pi-temporal/${createHash("sha256").update(sessionFile).digest("hex").slice(0, 16)}`;

// The tree this host last checked out or shipped. Kept per session and per directory, so one
// session never reads another's files as its own.
interface Held {
  readonly tree: string;
  // The highest bundle this host has taken in, so a restore unbundles only newer ones.
  readonly seq?: number;
  // True only when this host built the directory from an empty one. Only those may be emptied
  // again: an adopted directory may hold ignored files nothing else has a copy of. Absent means no.
  readonly built?: boolean;
  // The file name is a hash, so these two let a sweep ask about and act on a note it finds.
  readonly session?: string;
  readonly directory?: string;
}

// The newest state in the shared directory. `seq` is the order bundles must be unbundled in.
interface Tip {
  readonly tree: string;
  readonly commit: string;
  readonly seq: number;
}

const shareDir = (sessionFile: string) => `${sessionFile}.tree`;
const tipPath = (sessionFile: string) => join(shareDir(sessionFile), "tip.json");
// Zero-padded because the restore sorts these names lexically.
const bundleName = (seq: number) => `${String(seq).padStart(8, "0")}.bundle`;

// Host-local, one shadow repository per project path.
const treesRoot = () =>
  join(process.env.PI_TEMPORAL_DATA ?? join(homedir(), ".pi-temporal"), "trees");

// Keyed and run by the absolute path. A relative one would name a different host directory per
// cwd, and git, which runs in the directory and also names it as the work tree, would nest it.
function hostDir(projectDir: string) {
  const key = resolve(projectDir);
  return join(treesRoot(), createHash("sha256").update(key).digest("hex").slice(0, 16));
}

const gitDir = (projectDir: string) => join(hostDir(projectDir), "git");
const heldName = (sessionFile: string) =>
  `held-${createHash("sha256").update(sessionFile).digest("hex").slice(0, 16)}.json`;
const heldPath = (projectDir: string, sessionFile: string) =>
  join(hostDir(projectDir), heldName(sessionFile));

// In the shared directory, so every host can tell a session is over and drop its own note.
const retiredPath = (sessionFile: string) => join(shareDir(sessionFile), "retired.json");
// Payload deletion must leave enough evidence for hosts that have not released their directories.
const forgottenPath = (sessionFile: string) => `${shareDir(sessionFile)}.forgotten.json`;
const isRetired = async (sessionFile: string) =>
  (await readJson<unknown>(retiredPath(sessionFile))) !== undefined ||
  ((await readJson<unknown>(forgottenPath(sessionFile))) !== undefined &&
    await stat(tipPath(sessionFile)).then(
      () => false,
      (err: NodeJS.ErrnoException) => err.code === "ENOENT",
    ));
// A session doing work is not retired, whatever a marker from its last idle period says.
const revive = (sessionFile: string) => rm(retiredPath(sessionFile), { force: true });
// Two locks. The host-local one guards the directory and the shadow repository's index. The shared
// one stops two hosts writing one session's bundles at once.
const treeLockPath = (projectDir: string) => join(hostDir(projectDir), "tree");
const sharedLockPath = (sessionFile: string) => join(shareDir(sessionFile), "writers");

// Always in this order, to avoid deadlock. A stalled holder can lose either lease, the directory's
// or the session's tree store, so `owned` answers for both, and the body asks before each write.
const withTreeLocks = <T>(
  projectDir: string,
  sessionFile: string,
  body: (owned: () => Promise<boolean>) => Promise<T>,
) =>
  withSessionLock(treeLockPath(projectDir), (directoryOwned) =>
    withSessionLock(sharedLockPath(sessionFile), (storeOwned) =>
      body(async () => (await directoryOwned()) && (await storeOwned())),
    ),
  );

/** Throws once either tree lease is gone, so a stalled holder's late write can't land. */
const stillHolding = async (owned: () => Promise<boolean>, what: string, where: string) => {
  if (!(await owned())) {
    throw new Error(`lost a tree lease before ${what}: another writer has ${where}`);
  }
};

async function readJson<T>(path: string): Promise<T | undefined> {
  try {
    return JSON.parse(await readFile(path, "utf8")) as T;
  } catch (err) {
    // Missing state permits creation. Unreadable state can't permit a new writer.
    if ((err as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw err;
  }
}

// Random rather than the pid, because every container is pid 1.
const scratchToken = () => randomBytes(6).toString("hex");

// Through a scratch name, so a reader never sees half a document.
// A writer that died between writing its scratch file and renaming it leaves the scratch behind.
// Scans skip it, or a half-written file would wedge every directory scan.
const isScratch = (name: string) => name.endsWith(".writing");

// Missing is empty. Unreadable can't permit a new writer, so it throws.
const listDir = (dir: string) =>
  readdir(dir).catch((err: NodeJS.ErrnoException) => {
    if (err.code === "ENOENT") return [] as string[];
    throw err;
  });

async function writeJson(path: string, value: unknown) {
  await mkdir(dirname(path), { recursive: true });
  const scratch = `${path}.${scratchToken()}.writing`;
  await writeFile(scratch, JSON.stringify(value), "utf8");
  await rename(scratch, path);
}

// No inherited `GIT_*` and no system or user config, so two hosts make the same tree from the
// same files whatever the worker's environment says.
const gitEnv = () => ({
  ...Object.fromEntries(Object.entries(process.env).filter(([name]) => !name.startsWith("GIT_"))),
  GIT_CONFIG_NOSYSTEM: "1",
  GIT_CONFIG_GLOBAL: "/dev/null",
});

const git = (projectDir: string, args: string[], input?: string) => {
  const run = execFileAsync(
    "git",
    ["--git-dir", gitDir(projectDir), "-c", "core.autocrlf=false", ...args],
    {
      cwd: resolve(projectDir),
      maxBuffer: 64 * 1024 * 1024,
      env: {
        ...gitEnv(),
        GIT_WORK_TREE: resolve(projectDir),
        // Fixed identity, so a capture doesn't depend on who runs it.
        GIT_AUTHOR_NAME: "pi-temporal",
        GIT_AUTHOR_EMAIL: "pi-temporal@localhost",
        GIT_COMMITTER_NAME: "pi-temporal",
        GIT_COMMITTER_EMAIL: "pi-temporal@localhost",
      },
    },
  );
  run.child.stdin?.end(input);
  return run;
};

// Re-initializing is a no-op, so this is safe before every capture and restore.
async function ensureShadow(projectDir: string) {
  await mkdir(gitDir(projectDir), { recursive: true });
  await execFileAsync("git", ["init", "--bare", "-q", gitDir(projectDir)], { env: gitEnv() });
}

// Missing counts as empty. Unreadable counts as not empty, so we never write over it.
async function isEmptyDir(dir: string) {
  try {
    return (await readdir(dir)).length === 0;
  } catch (err) {
    return (err as NodeJS.ErrnoException).code === "ENOENT";
  }
}

// Thrown so Temporal retries the work on a host that holds the right files.
class WrongTree extends Error {}

// One marker per in-flight tool call, written before the tool runs and removed when it returns.
// A timed-out attempt leaves the body running, and this is the only local sign of that.
//
// A marker left from another step makes the directory refused until this host can show the writer
// is gone (see `insideBecause`). If it can't, `release-tree` clears it. Another host can still
// serve the session meanwhile.
const writersDir = (projectDir: string) => join(hostDir(projectDir), "writers");
// Keyed by turn, step, and call. A call id is unique only within one response, and a later step
// that reused it would otherwise overwrite, then clear, the marker of a tool that's still running.
const writerPath = (projectDir: string, writer: Writer) => {
  const key = `${writer.turn}\0${writer.step}\0${writer.callId}`;
  return join(
    writersDir(projectDir),
    `${createHash("sha256").update(key).digest("hex").slice(0, 32)}.json`,
  );
};

/** What a tool call is, for telling this step's writers from an earlier turn's. */
export interface Writer {
  readonly turn: string;
  readonly step: number;
  readonly callId: string;
}

export interface WriterNote extends Writer {
  readonly host: string;
  readonly pid: number;
  readonly pgid?: number;
  // Inherited through the environment, even across `setsid`, so it finds children that left the
  // process group.
  readonly worker?: string;
  // Survives `setsid` and `sudo`, and is readable for any owner. Linux only.
  readonly cgroup?: string;
  // So a pid from before a reboot is not read as live.
  readonly bootAt?: number;
  // Tells a worker restarted in the same container (same pid, same boot) from its predecessor.
  readonly pidStartedAt?: string;
  // Containers can share a hostname but not pids, so a start time is only comparable in one ns.
  readonly pidNs?: string;
  // How often a live writer touches its marker. The only sign of life a reader in another pid
  // namespace can see, e.g. after a container restart.
  readonly refreshMs?: number;
  readonly started: string;
}

const REFRESH_MS = 10_000;
// Missed refreshes before a marker from another pid namespace counts as left behind.
const QUIET_AFTER = 4;
const refreshing = new Map<string, NodeJS.Timeout>();

// In the environment so a tool's children inherit it and a scan can find them later. Fresh per
// process, and set at load before anything is spawned.
const WORKER_ENV = "PI_TEMPORAL_WORKER";
const worker = (process.env[WORKER_ENV] = randomBytes(8).toString("hex"));

// `os.uptime` is coarse and drifts, so compare boot stamps with a tolerance.
const bootAt = () => Math.round(Date.now() - uptime() * 1000);
const SAME_BOOT = 60_000;

const running = (pid: number) => {
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    // EPERM means it exists but belongs to someone else.
    return (err as NodeJS.ErrnoException).code === "EPERM";
  }
};

/** What is running on this host, read from `/proc` on Linux (slim images lack `ps`) and from `ps`
 * elsewhere. Undefined when it can't be read, which counts as "everything is still here". */
export interface LiveHere {
  readonly groups: Set<number>;
  readonly workers: Set<string>;
  // Linux only. The one reading that covers a process running as another user.
  readonly cgroups: Set<string>;
  // Processes with their cwd or an open file inside the directory. Needs nothing the tool kept.
  readonly holding: readonly { readonly pid: number; readonly how: string }[];
  // Our own group and cgroup. A marker naming either says nothing, since a restarted worker can
  // share them with its predecessor.
  readonly ourGroup?: number;
  readonly ourCgroup?: string;
}

async function liveHere(projectDir: string): Promise<LiveHere | undefined> {
  const groups = new Set<number>();
  const workers = new Set<string>();
  const cgroups = new Set<string>();
  const holding: { pid: number; how: string }[] = [];
  // Resolved, because the readings below come back resolved (macOS temp dirs are symlinked).
  const here = await realpath(projectDir).catch(() => projectDir);
  const inside = (path: string | undefined) =>
    path !== undefined && (path === here || path.startsWith(`${here}/`));
  const nameOf = (text: string) => {
    // NUL-separated in `/proc`, space-separated in `ps`.
    const at = text.indexOf(`${WORKER_ENV}=`);
    return at < 0 ? undefined : text.slice(at + WORKER_ENV.length + 1).split(/[\0\s]/)[0];
  };
  // Our own tools have markers, and our git subprocesses stand in the directory by design.
  const ours = (name: string | undefined) => name === worker;

  try {
    if (process.platform === "linux") {
      for (const name of await readdir("/proc")) {
        if (!/^\d+$/.test(name) || name === String(process.pid)) continue;
        const stat = await readFile(`/proc/${name}/stat`, "utf8").catch(() => undefined);
        // The command can hold spaces, so count fields after the last `)`: state, ppid, pgrp.
        const after = stat?.slice(stat.lastIndexOf(")") + 2).split(" ");
        const pgrp = after ? Number.parseInt(after[2], 10) : NaN;
        if (Number.isFinite(pgrp)) groups.add(pgrp);
        // Readable for any owner, unlike `environ` and `cwd`.
        const cgroup = await readFile(`/proc/${name}/cgroup`, "utf8").catch(() => undefined);
        const line = cgroup?.split("\n")[0];
        const path = line && line.slice(line.indexOf(":", line.indexOf(":") + 1) + 1);
        if (path) cgroups.add(path.trim());
        // Readable only for this user's processes. `sudo` children are covered by the cgroup.
        const held = nameOf(await readFile(`/proc/${name}/environ`, "utf8").catch(() => ""));
        if (held !== undefined) workers.add(held);
        if (ours(held)) continue;
        const pid = Number.parseInt(name, 10);
        const cwd = await readlink(`/proc/${name}/cwd`).catch(() => undefined);
        if (inside(cwd)) {
          holding.push({ pid, how: "its working directory is in it" });
          continue;
        }
        for (const fd of await readdir(`/proc/${name}/fd`).catch(() => [] as string[])) {
          const open = await readlink(`/proc/${name}/fd/${fd}`).catch(() => undefined);
          if (!inside(open)) continue;
          holding.push({ pid, how: `it holds ${open} open` });
          break;
        }
      }
      return {
        groups,
        workers,
        cgroups,
        holding,
        ourGroup: await group(),
        ourCgroup: await cgroup(),
      };
    }

    // `-E` prints the environment. Run `ps` and `lsof` without our name and outside the directory,
    // or they report themselves as leftovers.
    const { [WORKER_ENV]: _ours, ...env } = process.env;
    const asked = { maxBuffer: 32 * 1024 * 1024, cwd: "/", env };
    const { stdout } = await execFileAsync("ps", ["-A", "-E", "-o", "pid=,pgid=,command="], asked);
    const named = new Map<number, string | undefined>();
    for (const line of stdout.split("\n")) {
      const [pid, pgid] = line.trim().split(/\s+/, 2).map((n) => Number.parseInt(n, 10));
      if (!Number.isFinite(pid) || pid === process.pid) continue;
      if (Number.isFinite(pgid)) groups.add(pgid);
      const held = nameOf(line);
      if (held !== undefined) workers.add(held);
      named.set(pid, held);
    }
    // Ask for every cwd at once. `lsof +D` would stat the whole tree.
    const cwds = await execFileAsync("lsof", ["-a", "-d", "cwd", "-Fpn"], asked).catch(
      () => undefined,
    );
    let at: number | undefined;
    for (const line of cwds?.stdout.split("\n") ?? []) {
      if (line.startsWith("p")) at = Number.parseInt(line.slice(1), 10);
      if (!line.startsWith("n") || at === undefined || at === process.pid) continue;
      if (inside(line.slice(1)) && !ours(named.get(at))) {
        holding.push({ pid: at, how: "its working directory is in it" });
      }
    }
    return {
      groups,
      workers,
      cgroups,
      holding,
      ourGroup: await group(),
      ourCgroup: await cgroup(),
    };
  } catch {
    return undefined;
  }
}

async function readGroup(pid: number): Promise<number | undefined> {
  try {
    if (process.platform === "linux") {
      const stat = await readFile(`/proc/${pid}/stat`, "utf8");
      const pgrp = Number.parseInt(stat.slice(stat.lastIndexOf(")") + 2).split(" ")[2], 10);
      return Number.isFinite(pgrp) ? pgrp : undefined;
    }
    const { stdout } = await execFileAsync("ps", ["-o", "pgid=", "-p", String(pid)]);
    const pgid = Number.parseInt(stdout.trim(), 10);
    return Number.isFinite(pgid) ? pgid : undefined;
  } catch {
    return undefined;
  }
}

// Cached per process.
let ourGroup: Promise<number | undefined> | undefined;
const group = () => (ourGroup ??= readGroup(process.pid));

// Linux only.
async function readCgroup(): Promise<string | undefined> {
  if (process.platform !== "linux") return undefined;
  const text = await readFile("/proc/self/cgroup", "utf8").catch(() => undefined);
  const line = text?.split("\n")[0];
  // `<hierarchy>:<controllers>:<path>`. We want the path.
  if (!line) return undefined;
  return line.slice(line.indexOf(":", line.indexOf(":") + 1) + 1).trim() || undefined;
}

let ourCgroup: Promise<string | undefined> | undefined;
const cgroup = () => (ourCgroup ??= readCgroup());

/**
 * When a process started, as an opaque string only comparable on the same host. Undefined when it
 * can't be read, which counts as still running. Exported for a check.
 */
export async function startTimeOf(pid: number): Promise<string | undefined> {
  const stat = await readFile(`/proc/${pid}/stat`, "utf8").catch(() => undefined);
  if (stat !== undefined) {
    // Field 22 is the start time, counted after the parenthesized command name.
    return stat.slice(stat.lastIndexOf(")") + 2).split(" ")[19] || undefined;
  }
  if (process.platform === "linux") return undefined;
  try {
    // Pinned locale and zone, so readings from different workers compare equal.
    const { stdout } = await execFileAsync("ps", ["-o", "lstart=", "-p", String(pid)], {
      env: { ...process.env, LC_ALL: "C", TZ: "UTC" },
    });
    return stdout.trim() || undefined;
  } catch {
    return undefined;
  }
}

let ourStart: Promise<string | undefined> | undefined;
const startedAt = () => (ourStart ??= startTimeOf(process.pid));

let ourPidNs: Promise<string | undefined> | undefined;
const pidNs = () => (ourPidNs ??= readlink("/proc/self/ns/pid").catch(() => undefined));

// A start time is only comparable within one pid namespace.
async function samePids(note: WriterNote): Promise<boolean> {
  const here = await pidNs();
  if (process.platform === "linux" && here === undefined) return false;
  return note.pidNs === here;
}

/** Mark a tool call as about to write this directory. `endWrite` clears it. */
export async function beginWrite(projectDir: string, writer: Writer): Promise<void> {
  await mkdir(writersDir(projectDir), { recursive: true });
  const path = writerPath(projectDir, writer);
  await writeJson(path, {
    ...writer,
    host: hostname(),
    pid: process.pid,
    ...((await group()) === undefined ? {} : { pgid: await group() }),
    ...((await cgroup()) === undefined ? {} : { cgroup: await cgroup() }),
    worker,
    bootAt: bootAt(),
    ...((await startedAt()) === undefined ? {} : { pidStartedAt: await startedAt() }),
    ...((await pidNs()) === undefined ? {} : { pidNs: await pidNs() }),
    refreshMs: REFRESH_MS,
    started: new Date().toISOString(),
  } satisfies WriterNote);
  clearInterval(refreshing.get(path));
  const touch = () => void utimes(path, new Date(), new Date()).catch(() => {});
  refreshing.set(path, setInterval(touch, REFRESH_MS).unref());
}

export async function endWrite(projectDir: string, writer: Writer): Promise<void> {
  const path = writerPath(projectDir, writer);
  clearInterval(refreshing.get(path));
  refreshing.delete(path);
  await rm(path, { force: true });
}

/**
 * Why this marker may still be a live tool, for an operator to read, or undefined when it can't be.
 * Anything that can't be answered counts as still running. Exported so a check can pass in a Linux
 * reading on any platform.
 */
export function insideBecause(
  note: WriterNote,
  live: LiveHere | undefined,
  // Start time of whatever holds the pid now, when comparable with the marker's.
  pidStartedNow?: string,
  // Set only when the writer was in another pid namespace: how long since its marker was touched.
  quietMs?: number,
): string | undefined {
  // Two hosts sharing one data directory can't see each other's processes.
  if (note.host !== hostname()) return `it was written on ${note.host}, and this is ${hostname()}`;
  if (note.bootAt === undefined) return "it does not say which boot its pid belongs to";
  // Rebooted, so nothing it ran is still here.
  if (Math.abs(note.bootAt - bootAt()) > SAME_BOOT) return undefined;
  // A different start time means the pid was reused, e.g. by a worker restarted in the same
  // container. The checks below still run, since the writer's children can outlive it.
  const reused =
    note.pidStartedAt !== undefined &&
    pidStartedNow !== undefined &&
    pidStartedNow !== note.pidStartedAt;
  if (quietMs !== undefined && note.refreshMs !== undefined) {
    // Its pid names nothing here, so ask whether its writer still touches the marker.
    if (quietMs < note.refreshMs * QUIET_AFTER) {
      const ago = Math.round(quietMs / 1000);
      return `it ran in another pid namespace and touched its marker ${ago}s ago`;
    }
  } else if (running(note.pid) && !reused) {
    return `pid ${note.pid} is still running`;
  }
  // The writer is gone. Ask about what its children carry and where they stand, not about pids.
  if (live === undefined) return "this host could not be asked what is running on it";
  // Catches daemonized children, which leave the group but keep the environment.
  if (note.worker !== undefined && live.workers.has(note.worker)) {
    return "something it started is still running";
  }
  // For a tool run through `sudo`. Skipped when it's our own cgroup, which says nothing.
  const cgrouped =
    note.cgroup !== undefined && note.cgroup !== live.ourCgroup && live.cgroups.has(note.cgroup);
  if (cgrouped) {
    return `something is still in ${note.cgroup}`;
  }
  // For a tool given a foreign environment. Skipped when it's our own group.
  if (note.pgid !== undefined && note.pgid !== live.ourGroup && live.groups.has(note.pgid)) {
    return `something is still in process group ${note.pgid}`;
  }
  // Last, anything standing in the directory, which a writer cannot avoid.
  const standing = live.holding[0];
  return standing ? `pid ${standing.pid} is in the directory: ${standing.how}` : undefined;
}

// Markers that can't be a live tool any more are deleted.
async function writersHere(projectDir: string): Promise<(WriterNote & { because: string })[]> {
  const names = (await listDir(writersDir(projectDir))).filter((name) => !isScratch(name));
  if (names.length === 0) return [];
  const live = await liveHere(projectDir);
  const found: (WriterNote & { readonly because: string })[] = [];
  for (const name of names) {
    const path = join(writersDir(projectDir), name);
    const note = await readJson<WriterNote>(path);
    if (!note) continue;
    const same = await samePids(note);
    const startedNow =
      same && note.pidStartedAt !== undefined ? await startTimeOf(note.pid) : undefined;
    const touched = same ? undefined : await stat(path).then((s) => s.mtimeMs, () => undefined);
    const quiet = touched === undefined ? undefined : Math.max(0, Date.now() - touched);
    const because = insideBecause(note, live, startedNow, quiet);
    if (because !== undefined) found.push({ ...note, because });
    else await rm(path, { force: true });
  }
  return found;
}

/** Thrown when a directory is refused because a writer from an earlier step never came back. */
export class Quarantined extends Error {}

/** A refusal by design, as opposed to a failure that a retry can fix. */
export const isRefusal = (err: unknown) => err instanceof WrongTree || err instanceof Quarantined;

/**
 * Refuse a directory a writer from another step may still be inside. The current step's own
 * markers don't count, since its calls run in parallel by design.
 */
async function refuseWhenStranded(projectDir: string, current?: Writer): Promise<void> {
  const stranded = (await writersHere(projectDir)).filter(
    (note) => !current || note.turn !== current.turn || note.step !== current.step,
  );
  if (stranded.length === 0) return;
  const one = stranded[0];
  throw new Quarantined(
    `not using ${projectDir}: ${stranded.length} tool call(s) from an earlier step never ` +
      `returned (${one.callId} of turn ${one.turn} step ${one.step}, pid ${one.pid} on ` +
      `${one.host}, started ` +
      `${one.started}). Still refused because ${one.because}. Stop it, then clear the directory ` +
      `with \`pi-temporal release-tree ${projectDir}\`.`,
  );
}

/** Which step a capture belongs to, for a step whose host was taken off it. */
export interface Fence {
  readonly turn: string;
  readonly step: number;
}

// Steps closed without their host, in the shared directory because that host can't be reached.
// Its stale tool would otherwise publish against a tip that still looks live and revert newer work.
// One marker per closed step, named by a hash of turn and step. Closing and checking stay
// constant time however many closures a long session collects. `closed.json` is also read, for
// sessions whose closures are kept as one list there.
const legacyClosedPath = (sessionFile: string) => join(shareDir(sessionFile), "closed.json");
const closedDir = (sessionFile: string) => join(shareDir(sessionFile), "closed");
const closedMarker = (sessionFile: string, fence: Fence) => {
  const key = createHash("sha256").update(`${fence.turn}\0${fence.step}`).digest("hex");
  return join(closedDir(sessionFile), `${key.slice(0, 32)}.json`);
};

interface ClosedStep extends Fence {
  readonly at: string;
}

/**
 * Record that a step was closed without its host. Must be written before the replacement work
 * starts: a capture that beat it left a tip the replacement reads, and a later one is refused.
 */
export async function closeStep(sessionFile: string, fence: Fence): Promise<void> {
  await withSessionLock(sharedLockPath(sessionFile), async () => {
    if (await isClosed(sessionFile, fence)) return;
    // A timed-out attempt has no lifetime bound, so its closure must survive later turns.
    await mkdir(closedDir(sessionFile), { recursive: true });
    const closed: ClosedStep = { ...fence, at: new Date().toISOString() };
    await writeJson(closedMarker(sessionFile, fence), closed);
  });
}

// Called with the shared lock held, so it can't race `closeStep`. Only a missing marker is
// absence. A marker that can't be read refuses the capture, as unreadable tree state does.
async function isClosed(sessionFile: string, fence: Fence): Promise<boolean> {
  const found = await stat(closedMarker(sessionFile, fence)).then(
    () => true,
    (err: NodeJS.ErrnoException) => {
      if (err.code === "ENOENT") return false;
      throw err;
    },
  );
  if (found) return true;
  return ((await readJson<ClosedStep[]>(legacyClosedPath(sessionFile))) ?? []).some(
    (c) => c.turn === fence.turn && c.step === fence.step,
  );
}

/**
 * Relative paths stay on a renamed directory, but absolute paths aren't fenced. An adopted
 * checkout may hold ignored files with no other copy, so only a directory we built can move.
 */
async function moveAside(
  projectDir: string,
  sessionFile: string,
  current?: Writer,
): Promise<string | undefined> {
  const held = await readJson<Held>(heldPath(projectDir, sessionFile));
  if (!held?.built) return undefined;
  if (current) {
    const here = await writersHere(projectDir);
    const ours = here.some((note) => note.turn === current.turn && note.step === current.step);
    if (ours) return undefined;
  }
  const moved = `${projectDir}.stranded.${new Date().toISOString().replace(/[:.]/g, "-")}`;
  try {
    await rename(projectDir, moved);
  } catch {
    // Mount point or no permission. Nothing changed, so the caller still refuses.
    return undefined;
  }
  await mkdir(projectDir, { recursive: true });
  // The note and markers describe the moved directory, not the new empty one.
  await rm(heldPath(projectDir, sessionFile), { force: true });
  await rm(writersDir(projectDir), { recursive: true, force: true });
  return moved;
}

/**
 * Why `moveAside` would fail for this directory, or undefined. A startup warning only. The real
 * decision is the rename itself.
 */
export async function cannotMoveAside(projectDir: string): Promise<string | undefined> {
  try {
    const [here, up] = await Promise.all([stat(projectDir), stat(dirname(projectDir))]);
    // A mount point is on a different device from its parent.
    if (here.dev !== up.dev) return "it is a mount point, and a mount point cannot be renamed";
  } catch (err) {
    return `it could not be read: ${String((err as Error).message ?? err)}`;
  }
  try {
    await access(dirname(projectDir), W_OK);
  } catch {
    return "the directory it hangs under is not writable by this process";
  }
  return undefined;
}

/** Clear the refusal, for an operator who has stopped whatever was left running. */
export async function clearWriters(projectDir: string): Promise<number> {
  // No parsing first. This is the way out for a marker nothing else can read.
  const markers = (await listDir(writersDir(projectDir))).filter((name) => !isScratch(name));
  await rm(writersDir(projectDir), { recursive: true, force: true });
  return markers.length;
}

// What the directory holds now, as a tree id. Callers hold the tree lock.
async function treeHere(projectDir: string) {
  await ensureShadow(projectDir);
  // `add -A` keeps what the index already tracks, so a file ignored after it was captured would
  // keep shipping. Drop those entries first.
  const ignored = (await git(projectDir, ["ls-files", "-ci", "--exclude-standard", "-z"])).stdout;
  if (ignored !== "") {
    await git(projectDir, ["update-index", "--force-remove", "-z", "--stdin"], ignored);
  }
  await git(projectDir, ["add", "-A"]);
  return (await git(projectDir, ["write-tree"])).stdout.trim();
}

// Unbundles what this host has not taken in, in order, since each names the previous as a
// prerequisite. git accepts a bundle whose commits are already here, so any failure is real.
async function ingest(projectDir: string, sessionFile: string, from = 0) {
  await ensureShadow(projectDir);
  const names = (await listDir(shareDir(sessionFile)))
    .filter((name) => name.endsWith(".bundle"))
    .filter((name) => Number.parseInt(name, 10) > from)
    .sort();
  for (const name of names) {
    await git(projectDir, ["bundle", "unbundle", join(shareDir(sessionFile), name)]).catch(
      (err: Error) => {
        throw new Error(`could not unbundle ${name} for ${sessionFile}: ${err.message}`);
      },
    );
  }
}

// Every this many captures, a self-contained bundle restarts the chain and older ones are dropped.
const COMPACT_EVERY = 40;

// Only bundles. `salvage/` and the tip stay.
async function dropBundlesBefore(dir: string, seq: number) {
  const names = (await readdir(dir).catch(() => [] as string[])).filter((name) =>
    name.endsWith(".bundle"),
  );
  for (const name of names) {
    if (Number.parseInt(name, 10) < seq) await rm(join(dir, name), { force: true });
  }
}

// Commits the directory and publishes it. The caller has already checked it may.
async function publish(
  projectDir: string,
  sessionFile: string,
  tip: Tip | undefined,
  tree: string,
  built: boolean | undefined,
  owned: () => Promise<boolean>,
) {
  // A stalled holder's lease can be reclaimed, and its late writes would clobber the new holder's
  // bundle or tip. Checked before each write. Not atomic, so this only narrows the window.
  const stillHeld = async (what: string) => {
    if (await owned()) return;
    throw new Error(`lost the tree-store lease before ${what}: another writer has ${sessionFile}`);
  };
  const seq = (tip?.seq ?? 0) + 1;
  // A restart bundle is a root commit with the whole tree, so older bundles can be dropped.
  const restart = seq % COMPACT_EVERY === 0;
  const parent = tip && !restart ? ["-p", tip.commit] : [];
  const commit = (
    await git(projectDir, ["commit-tree", tree, ...parent, "-m", `pi-temporal ${tree.slice(0, 8)}`])
  ).stdout.trim();
  const ref = snapRef(sessionFile);
  await git(projectDir, ["update-ref", ref, commit]);

  const dir = shareDir(sessionFile);
  await mkdir(dir, { recursive: true });
  // The number is taken. If the tip is at or past it, another writer got ahead, so refuse.
  // Otherwise a writer died before naming its bundle as the tip. Drop that orphan, or every host
  // wedges on it. The shared lease is what makes this safe.
  if ((await readdir(dir).catch(() => [] as string[])).includes(bundleName(seq))) {
    const now = await readJson<Tip>(tipPath(sessionFile));
    if ((now?.seq ?? 0) >= seq) {
      throw new WrongTree(
        `not shipping ${projectDir}: the session is already at bundle ${now?.seq ?? seq}`,
      );
    }
    console.warn(
      `dropping bundle ${seq} for ${sessionFile}: nothing names it and the tip is behind it`,
    );
    await rm(join(dir, bundleName(seq)), { force: true });
  }
  // Scratch name and rename, so no host unbundles a half-written file.
  const scratch = join(dir, `${bundleName(seq)}.${scratchToken()}.writing`);
  const incremental = tip && !restart ? ["--not", tip.commit] : [];
  await git(projectDir, ["bundle", "create", scratch, ref, ...incremental]);
  await stillHeld("renaming its bundle into place");
  await rename(scratch, join(dir, bundleName(seq)));

  // Scratch first and the check right before the rename, so a lease lost meanwhile has only the
  // rename left to race.
  const tipScratch = `${tipPath(sessionFile)}.${scratchToken()}.writing`;
  await writeFile(tipScratch, JSON.stringify({ tree, commit, seq } satisfies Tip), "utf8");
  await stillHeld("naming the tip").catch(async (err: unknown) => {
    await rm(tipScratch, { force: true });
    throw err;
  });
  await rename(tipScratch, tipPath(sessionFile));
  await rm(forgottenPath(sessionFile), { force: true });
  // Only after the tip names the new bundle, or a crash leaves the session unrestorable.
  if (restart) await dropBundlesBefore(dir, seq);
  // `built` is carried over, never set here. Only a restore into an empty directory may set it.
  await writeJson(heldPath(projectDir, sessionFile), {
    tree,
    seq,
    built,
    session: sessionFile,
    directory: projectDir,
  } satisfies Held);
}

// Keeps unshipped work recoverable under `salvage/`, outside what `ingest` reads.
async function salvage(projectDir: string, sessionFile: string, tree: string) {
  const dir = join(shareDir(sessionFile), "salvage");
  await mkdir(dir, { recursive: true });
  // A root commit, so the bundle stands on its own.
  const commit = (
    await git(projectDir, ["commit-tree", tree, "-m", `pi-temporal salvage ${tree.slice(0, 8)}`])
  ).stdout.trim();
  // Not a child path of the session's own ref: git cannot hold both `refs/x` and `refs/x/y`.
  const ref = `${snapRef(sessionFile)}-salvage`;
  await git(projectDir, ["update-ref", ref, commit]);

  const name = `${tree.slice(0, 12)}.bundle`;
  const scratch = join(dir, `${name}.${scratchToken()}.writing`);
  await git(projectDir, ["bundle", "create", scratch, ref]);
  await rename(scratch, join(dir, name));
  console.warn(
    `set aside ${projectDir} as ${join(dir, name)}: it held work this session never shipped, ` +
      `and the session moved on without it. Recover with \`git bundle unbundle\`.`,
  );
}

// Empty a directory this host built, unless it holds unshipped work. Returns whether it is now
// empty, since a root `.git` survives and the next restore would refuse it.
async function handBack(projectDir: string, held: Held) {
  if (!held.built) return false;
  // A tool can outlive the turn while its directory still matches the last snapshot.
  if ((await writersHere(projectDir)).length > 0) return false;
  // Compare with this host's own tree, since it may be behind the final tip.
  if ((await treeHere(projectDir)) !== held.tree) return false;
  await git(projectDir, ["read-tree", "-u", "--reset", EMPTY_TREE]);
  // `-x` too. Safe because only directories this host built get here.
  await git(projectDir, ["clean", "-fdxq"]);
  return await isEmptyDir(projectDir);
}

// Whether another session is using this directory, after handing back retired ones. Retirement
// runs on one host, so the others clean up here. Called with the tree lock held.
async function heldByOthers(projectDir: string, sessionFile: string) {
  const mine = heldName(sessionFile);
  const names = (await listDir(hostDir(projectDir))).filter(
    (name) => name.startsWith("held-") && !isScratch(name) && name !== mine,
  );
  let holdouts = 0;
  for (const name of names) {
    const path = join(hostDir(projectDir), name);
    const note = await readJson<Held>(path);
    // Keep notes with no session and notes of live sessions.
    if (!note?.session || !(await isRetired(note.session))) {
      holdouts++;
      continue;
    }
    if (note.built && !(await handBack(projectDir, note))) {
      holdouts++;
      continue;
    }
    await rm(path, { force: true });
  }
  return holdouts > 0;
}

/**
 * Why `dir` must not be sent as an implicit project, or undefined. Everything not ignored ships,
 * so a home directory or one with no ignore rules could leak secrets.
 */
export async function projectRefusal(dir: string): Promise<string | undefined> {
  const resolved = await realpath(dir).catch(() => dir);
  const home = await realpath(homedir()).catch(() => homedir());
  if (resolved === home) {
    return `${dir} is your home directory, and every dotfile in it would ship`;
  }
  const exists = (name: string) => access(join(resolved, name)).then(() => true, () => false);
  if (!(await exists(".git")) && !(await exists(".gitignore"))) {
    return `${dir} holds no repository and no .gitignore, so nothing keeps secrets out of it`;
  }
  return undefined;
}

/**
 * Record the project's files against this session. Cheap when nothing changed: a tree id is
 * content-addressed, so an untouched directory produces the tree the tip already names.
 */
export async function capture(
  projectDir: string,
  sessionFile: string,
  // Whether this caller may establish the project. Only the client, never a worker.
  opts: {
    readonly seed?: boolean;
    readonly current?: Writer;
    // The step this capture belongs to, checked against closed steps.
    readonly fence?: Fence;
  } = {},
): Promise<void> {
  // One at a time per directory, since captures share the shadow index.
  await withTreeLocks(projectDir, sessionFile, async (owned) => {
    // A stray writer's files would be in the capture.
    await refuseWhenStranded(projectDir, opts.current);
    // The tip check can't catch this: a stale tool still stands on the tip it read.
    if (opts.fence && (await isClosed(sessionFile, opts.fence))) {
      throw new WrongTree(
        `not shipping ${projectDir}: turn ${opts.fence.turn} step ${opts.fence.step} was closed ` +
          `without this host, and what it produced is kept rather than published`,
      );
    }
    const tip = await readJson<Tip>(tipPath(sessionFile));
    const held = await readJson<Held>(heldPath(projectDir, sessionFile));

    if (!tip) {
      // Nothing shipped yet. Only a seeding caller may establish it.
      if (!opts.seed) return;
      if (await heldByOthers(projectDir, sessionFile)) {
        throw new WrongTree(`not shipping ${projectDir}: another session is working in it`);
      }
    } else if (held?.tree !== tip.tree) {
      // Only a host on the tip may add to it, or older files would overwrite newer work everywhere.
      throw new WrongTree(
        `not shipping ${projectDir}: this host holds ${held?.tree.slice(0, 8) ?? "nothing"}, ` +
          `and the session is at ${tip.tree.slice(0, 8)}`,
      );
    }
    // After the refusals, so a capture that is refused leaves a retired session retired.
    await revive(sessionFile);

    // The shadow index is a local write too, so it waits for the leases as well.
    await stillHolding(owned, "capturing", projectDir);
    const tree = await treeHere(projectDir);
    if (tip?.tree === tree) return;
    if (tip) await ingest(projectDir, sessionFile, held?.seq ?? 0);
    await publish(projectDir, sessionFile, tip, tree, held?.built, owned);
  });
}

/**
 * Bring this host's copy to the session's tip. Throws when the directory holds files this session
 * did not put there.
 */
export async function ensure(
  projectDir: string,
  sessionFile: string,
  current?: Writer,
): Promise<void> {
  await withTreeLocks(projectDir, sessionFile, async (owned) => {
    // Before any restore, or a stray writer's next capture would look current. Moving the
    // directory aside keeps a writer's relative paths away from the new one. Absolute paths still
    // reach it, so the move narrows the risk and doesn't close it.
    await refuseWhenStranded(projectDir, current).catch(async (err: unknown) => {
      if (!(err instanceof Quarantined)) throw err;
      const moved = await moveAside(projectDir, sessionFile, current);
      if (moved === undefined) throw err;
      console.warn(
        `moved ${projectDir} to ${moved}: a tool call from an earlier step never came back and ` +
          `this host cannot show it stopped. Relative paths stay in the moved directory. ` +
          `Absolute paths can still reach its replacement.`,
      );
    });
    // Read inside the lock, or a concurrent capture could move the tip under us.
    const tip = await readJson<Tip>(tipPath(sessionFile));

    // Before the tip check, so a session with nothing shipped can't run in another's directory.
    if (await heldByOthers(projectDir, sessionFile)) {
      throw new WrongTree(`not restoring ${projectDir}: another session is working in it`);
    }
    // A worker may not establish the project. The client sends it before the session starts.
    if (!tip) {
      throw new WrongTree(
        `no project established for ${sessionFile}: ` +
          `send it with \`pi-temporal start --project=...\``,
      );
    }

    const held = await readJson<Held>(heldPath(projectDir, sessionFile));
    // New to this session. Only an empty directory is ours to fill.
    if (!held && !(await isEmptyDir(projectDir))) {
      throw new WrongTree(`not restoring ${projectDir}: it holds files this session never shipped`);
    }
    // After the refusals, so a restore that is refused leaves a retired session retired. Before the
    // early return, since a host already on the tip is still serving the session.
    await revive(sessionFile);
    if (held?.tree === tip.tree) return;

    // The writes start here, and the lock may have been taken over while the reads above waited.
    await stillHolding(owned, "restoring", projectDir);
    if (held) {
      // Behind the tip with unshipped edits (a worker died before shipping). Publishing them would
      // revert the tip on every host, so set them aside and move to the tip.
      const here = await treeHere(projectDir);
      if (here !== held.tree) await salvage(projectDir, sessionFile, here);
    }

    await mkdir(projectDir, { recursive: true });
    await ingest(projectDir, sessionFile, held?.seq ?? 0);

    // `-u --reset` also removes files the newer tree dropped. Last check before the files change.
    await stillHolding(owned, "checking out the tip", projectDir);
    await git(projectDir, ["read-tree", "-u", "--reset", tip.tree]);
    // `!held` only gets here for an empty directory, so this host built it. Otherwise carry the
    // note's value as is, so an adopted checkout never becomes releasable.
    await writeJson(heldPath(projectDir, sessionFile), {
      tree: tip.tree,
      seq: tip.seq,
      built: held ? held.built : true,
      session: sessionFile,
      directory: projectDir,
    } satisfies Held);
  });
}

// git's empty tree. Checking it out empties the directory through git's own bookkeeping.
const EMPTY_TREE = "4b825dc642cb6eb9a060e54bf8d69288fbee4904";

/**
 * Empty this host's directory and drop the session's note, so the next session can use it.
 * Returns false when `handBack` refuses.
 */
export async function release(projectDir: string, sessionFile: string): Promise<boolean> {
  return await withTreeLocks(projectDir, sessionFile, async () => {
    const held = await readJson<Held>(heldPath(projectDir, sessionFile));
    if (!held) return false;
    // Keep the note if not emptied, or the next restore would refuse the leftovers.
    if (!(await handBack(projectDir, held))) return false;
    await rm(heldPath(projectDir, sessionFile), { force: true });
    return true;
  });
}

/**
 * Mark the session over in the shared directory, then release this host's directory. Other hosts
 * read the marker and release theirs later.
 */
export async function retire(projectDir: string, sessionFile: string): Promise<boolean> {
  // Under the shared lock, so it can't interleave with a capture or restore deciding to revive.
  await withSessionLock(sharedLockPath(sessionFile), async () => {
    if (await established(sessionFile)) {
      await writeJson(retiredPath(sessionFile), { at: new Date().toISOString() });
    }
  });
  return await release(projectDir, sessionFile);
}

/**
 * Copy a client-sent template store into this session's, for scheduled firings. Runs on a worker
 * but copies what a client sent, not the worker's directory. A no-op once the session has a tip.
 */
export async function adopt(template: string, sessionFile: string): Promise<boolean> {
  return await withSessionLock(sharedLockPath(sessionFile), async () => {
    if (await established(sessionFile)) return false;
    const tip = await readJson<Tip>(tipPath(template));
    if (!tip) throw new WrongTree(`no project was sent for ${template}`);
    const from = shareDir(template);
    const to = shareDir(sessionFile);
    await mkdir(to, { recursive: true });
    // Strict, so a template that can't be read isn't copied as one with no bundles.
    for (const name of (await listDir(from)).filter((n) => n.endsWith(".bundle"))) {
      // Scratch name, so no host unbundles a half-copied file.
      const scratch = join(to, `${name}.${scratchToken()}.writing`);
      await writeFile(scratch, await readFile(join(from, name)));
      await rename(scratch, join(to, name));
    }
    // Last, so a partial copy never leaves a tip naming a missing bundle.
    await writeJson(tipPath(sessionFile), tip);
    await rm(forgottenPath(sessionFile), { force: true });
    return true;
  });
}

/**
 * Drop this host's note without touching the shipped tree. For a client that seeded a template,
 * which is never retired and would otherwise block the directory.
 */
export const unclaim = (projectDir: string, sessionFile: string) =>
  rm(heldPath(projectDir, sessionFile), { force: true });

/**
 * Release every directory this host holds for a retired session, at worker start. Returns how many.
 * Same rules as `handBack`.
 */
export async function sweep(): Promise<number> {
  let freed = 0;
  for (const dir of await readdir(treesRoot()).catch(() => [] as string[])) {
    const names = (await readdir(join(treesRoot(), dir)).catch(() => [] as string[])).filter(
      (name) => name.startsWith("held-") && !isScratch(name),
    );
    for (const name of names) {
      const path = join(treesRoot(), dir, name);
      // A cleanup pass. A note it can't read keeps its directory, and the others still go.
      const note = await readJson<Held>(path).catch(() => undefined);
      if (!note?.session || !note.directory) continue;
      // An unreadable marker skips this note, not the whole pass.
      if (!(await isRetired(note.session).catch(() => false))) continue;
      const done = await withTreeLocks(note.directory, note.session, async () => {
        const current = await readJson<Held>(path);
        if (
          !current || current.session !== note.session || current.directory !== note.directory ||
          !(await isRetired(note.session!))
        ) return false;
        if (current.built && !(await handBack(note.directory!, current))) {
          return false;
        }
        await rm(path, { force: true });
        return true;
      }).catch(() => false);
      if (done) freed++;
    }
  }
  return freed;
}

/**
 * Whether this session already has a project. A client continuing a session must not send it
 * again, since its copy is older than the tip.
 */
export const established = async (sessionFile: string) =>
  (await readJson<Tip>(tipPath(sessionFile))) !== undefined;

/**
 * Salvage this host's directory without touching the tip, for a caller that could not ship.
 */
export async function setAside(projectDir: string, sessionFile: string): Promise<void> {
  await withTreeLocks(projectDir, sessionFile, async () => {
    await salvage(projectDir, sessionFile, await treeHere(projectDir));
  });
}

/**
 * Delete a finished session's tree store. Pass `projectDir` to also drop this host's note.
 */
export async function forget(sessionFile: string, projectDir?: string): Promise<void> {
  const drop = async () => {
    await writeJson(forgottenPath(sessionFile), { at: new Date().toISOString() });
    for (const name of await readdir(shareDir(sessionFile))) {
      // Keep the held lock, or another writer gets in mid-delete.
      if (name === "writers.lock") continue;
      await rm(join(shareDir(sessionFile), name), { recursive: true, force: true });
    }
    if (projectDir) await rm(heldPath(projectDir, sessionFile), { force: true });
    await rm(`${sessionFile}.pending`, { recursive: true, force: true });
  };
  if (projectDir) await withTreeLocks(projectDir, sessionFile, drop);
  else await withSessionLock(sharedLockPath(sessionFile), drop);
}
