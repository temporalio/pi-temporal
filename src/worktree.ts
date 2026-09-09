// Moves the project's files between hosts, so a session that resumes on another worker finds the
// work the last one did. The session log already travels: it is one file in a shared directory.
// The files the tools actually edit did not, so a worker on a second machine started every step in
// whatever directory it happened to be pointed at, and told the model the project was empty.
//
// The shape is git's, because git already answers the hard parts: content addressing, an
// incremental transfer, and a checkout that removes what a later tree dropped. Each capture writes
// one bundle into a directory beside the session log; a host that is behind unbundles the ones it
// has not seen and checks the newest tree out. The shadow repository is host-local and points at
// the work tree from outside, so the project's own `.git` is never touched and a directory that is
// not a repository works the same as one that is.
//
// Who may move the tip is the rule the rest follows from. Only a host standing on it may add to it,
// and nothing running on a worker may establish it: every activity, the model call included, lands
// on whichever worker Temporal had free, so an activity that adopts its own directory puts the
// project wherever the first unit of work happened to go. The client sends it, and a scheduled
// session copies the template the client left.
//
// What these guards do not do is isolate a tool that outlived its activity. A host that is behind
// is refused and its work set aside under `salvage/`, but a host the session later restores is
// brought to the tip, and a stale tool writing after that restore publishes against a note that
// now matches. That is why the driver refuses to move a step off a host whose attempt started.
//
// Two things answer for the tool itself, because the tip rule alone cannot. A marker says which
// calls are inside their own execution here, so the directory is refused to anything else until
// they come back or this host can show they are gone. And a step the session closed without its
// host is named in the shared directory, so what that host publishes for it afterwards is refused
// wherever it comes from, rather than winning because it happened to capture first.

import { execFile } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import { mkdir, readFile, readdir, rename, rm, stat, writeFile } from "node:fs/promises";
import { homedir, hostname, uptime } from "node:os";
import { dirname, join } from "node:path";
import { promisify } from "node:util";
import { withSessionLock } from "./session-lock.js";

const execFileAsync = promisify(execFile);

// A bundle has to name a ref, and the ref has to be per session: two sessions capturing the same
// directory would otherwise overwrite each other's between `update-ref` and `bundle create`, and
// the bundle would carry a commit its own tip does not name.
const snapRef = (sessionFile: string) =>
  `refs/pi-temporal/${createHash("sha256").update(sessionFile).digest("hex").slice(0, 16)}`;

// The tree this host last agreed the directory held, either because it checked it out or because
// it shipped it. Kept per session as well as per directory: a note saying "this is what is here"
// means nothing to a session that did not put it there, and sharing one let a second session read
// the first one's files as its own and reset over them.
interface Held {
  readonly tree: string;
  // The highest bundle this host has taken in. Without it every restore unbundles the whole
  // sequence again, which is one git subprocess per bundle per activity and gets slower for the
  // life of the session.
  readonly seq?: number;
  // Whether this host built the directory out of an empty one, as opposed to adopting a directory
  // that already had files. Only the first kind may ever be emptied again: the second is somebody's
  // working copy, and what it holds includes files git ignores, which no bundle carries and nothing
  // else has a copy of. Absent means no, which is what an older note without the field should mean.
  readonly built?: boolean;
  // Which session the note belongs to. The file name is a hash, so without this a host reading
  // somebody else's note cannot ask the shared directory anything about it, and a note nothing can
  // ask about is a claim nothing takes back.
  readonly session?: string;
  // And which directory it is about, for the same reason: the note lives under a hash of the path,
  // so a sweep that finds a note nobody needs any more cannot otherwise say where to act.
  readonly directory?: string;
}

// What the shared directory says the newest state is. `seq` orders the bundles, because a host has
// to unbundle them in the order they were written.
interface Tip {
  readonly tree: string;
  readonly commit: string;
  readonly seq: number;
}

const shareDir = (sessionFile: string) => `${sessionFile}.tree`;
const tipPath = (sessionFile: string) => join(shareDir(sessionFile), "tip.json");
// Zero-padded because the restore sorts these names lexically. Eight digits, so the ordering holds
// for every session a run can produce.
const bundleName = (seq: number) => `${String(seq).padStart(8, "0")}.bundle`;

// Host-local, and keyed by the path the tools run in. Two projects on one worker get a shadow
// repository each; the same project on two workers gets one on each of them.
const treesRoot = () => join(process.env.PI_TEMPORAL_DATA ?? join(homedir(), ".pi-temporal"), "trees");

function hostDir(projectDir: string) {
  return join(treesRoot(), createHash("sha256").update(projectDir).digest("hex").slice(0, 16));
}

const gitDir = (projectDir: string) => join(hostDir(projectDir), "git");
const heldName = (sessionFile: string) =>
  `held-${createHash("sha256").update(sessionFile).digest("hex").slice(0, 16)}.json`;
const heldPath = (projectDir: string, sessionFile: string) =>
  join(hostDir(projectDir), heldName(sessionFile));

// Said by the shared directory, so every host can read it and not only the one that ran the
// retirement. This is what makes the per-host note a cache: without somewhere to ask whether the
// session that wrote a note is over, the note is the only answer and nothing can correct it.
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
// Two locks, because there are two questions and they have different answers.
//
// The first is host-local, because a project directory is: it excludes one worker running steps of
// two sessions in the same directory, and it is what makes the shadow repository's index safe.
//
// The second lives in the shared directory, so it crosses hosts, and it is what stops two machines
// writing one session's bundles at once. The activities happened to hold the session's own lock
// around their tree writes, so that rule held for them and not for the client, which seeds a
// project holding neither. A lock the callers have to remember is a rule that is true until
// somebody adds a caller.
const treeLockPath = (projectDir: string) => join(hostDir(projectDir), "tree");
const sharedLockPath = (sessionFile: string) => join(shareDir(sessionFile), "writers");

// Always in this order. Two locks taken in two orders is the one way to turn exclusion into a
// deadlock, and the directory is the outer one because a host takes it for every session.
const withTreeLocks = <T>(projectDir: string, sessionFile: string, body: () => Promise<T>) =>
  withSessionLock(treeLockPath(projectDir), () =>
    withSessionLock(sharedLockPath(sessionFile), body),
  );

async function readJson<T>(path: string): Promise<T | undefined> {
  try {
    return JSON.parse(await readFile(path, "utf8")) as T;
  } catch {
    return undefined;
  }
}

// Random rather than the pid. Every container is pid 1, so two hosts writing the same name would
// otherwise pick the same scratch file and each would rename the other's half-written one.
const scratchToken = () => randomBytes(6).toString("hex");

// Through a scratch name, because a reader that finds half a document cannot tell it from a
// document that says something else.
async function writeJson(path: string, value: unknown) {
  await mkdir(dirname(path), { recursive: true });
  const scratch = `${path}.${scratchToken()}.writing`;
  await writeFile(scratch, JSON.stringify(value), "utf8");
  await rename(scratch, path);
}

const git = (projectDir: string, args: string[]) =>
  execFileAsync("git", ["--git-dir", gitDir(projectDir), ...args], {
    cwd: projectDir,
    maxBuffer: 64 * 1024 * 1024,
    env: {
      ...process.env,
      GIT_WORK_TREE: projectDir,
      // A capture must not depend on who is running it, and a container usually has neither set.
      GIT_AUTHOR_NAME: "pi-temporal",
      GIT_AUTHOR_EMAIL: "pi-temporal@localhost",
      GIT_COMMITTER_NAME: "pi-temporal",
      GIT_COMMITTER_EMAIL: "pi-temporal@localhost",
    },
  });

// `init` takes the directory as an argument rather than through `--git-dir`, and re-initializing
// an existing repository is a no-op, so this is safe to call before every capture and restore.
async function ensureShadow(projectDir: string) {
  await mkdir(gitDir(projectDir), { recursive: true });
  await execFileAsync("git", ["init", "--bare", "-q", gitDir(projectDir)]);
}

// Missing counts as empty: there is nothing there to protect and the checkout will create it.
// Unreadable counts as NOT empty, because a directory we cannot look into is not one to write over.
async function isEmptyDir(dir: string) {
  try {
    return (await readdir(dir)).length === 0;
  } catch (err) {
    return (err as NodeJS.ErrnoException).code === "ENOENT";
  }
}

// Refusing is not a thing to do quietly. A step that runs against files that are not the project
// gives the model a confident wrong answer, which is worse than not running at all, so this throws
// and lets Temporal put the work on a host that can do it.
class WrongTree extends Error {}

// One file per tool call that is inside its own execution, written before the tool can have any
// effect and removed when the body returns. It is the only thing on this host that tells a tool
// that finished from one Temporal stopped waiting for: an attempt that timed out settles the
// workflow's promise and leaves the body running, and nothing the workflow can see says which
// happened.
//
// A marker still here when another turn wants this directory is a writer nobody can account for, so
// the directory is refused rather than shared with it. The refusal outlives the process that made
// it, and takes itself back where this host can show that it is over: the writer's process is gone,
// nothing carrying its name is still running, nothing is left in its process group, and the machine
// has not restarted underneath the pids that say so. What is left after that is a tool that both
// left the group and was handed an environment of somebody else's choosing, and `release-tree` is
// the answer to that one. Until then the directory is refused, which does not strand the session,
// since another host can serve it.
const writersDir = (projectDir: string) => join(hostDir(projectDir), "writers");
const writerPath = (projectDir: string, callId: string) =>
  join(writersDir(projectDir), `${createHash("sha256").update(callId).digest("hex").slice(0, 16)}.json`);

/** What a tool call is, for telling this step's writers from an earlier turn's. */
export interface Writer {
  readonly turn: string;
  readonly step: number;
  readonly callId: string;
}

interface WriterNote extends Writer {
  readonly host: string;
  readonly pid: number;
  // The group the writer's process was in. What a tool starts stays in it unless it asks for a
  // group of its own, which is what every daemonizing wrapper does.
  readonly pgid?: number;
  // The worker that ran the call, as a name it put in its own environment. Everything a tool starts
  // inherits it, including through `setsid`, so this is what finds a child the group check lost.
  readonly worker?: string;
  // When this machine last started, so a pid from before a restart is not read as a live one.
  readonly bootAt?: number;
  readonly started: string;
}

// Put in the environment rather than kept in memory, because the point of it is to be inherited:
// a tool's children carry it wherever they end up, and a scan finds them after the worker that
// spawned them is gone. Fresh per process, so a worker never answers for its predecessor's
// children, and set at load rather than at the first write, before anything can be spawned.
const WORKER_ENV = "PI_TEMPORAL_WORKER";
const worker = (process.env[WORKER_ENV] = randomBytes(8).toString("hex"));

// os.uptime has second granularity and drifts a little between reads, so this is a stamp to compare
// with a tolerance, not an identifier. A restart moves it by the whole of the last uptime.
const bootAt = () => Math.round(Date.now() - uptime() * 1000);
const SAME_BOOT = 60_000;

const running = (pid: number) => {
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    // Somebody else's process is still a process.
    return (err as NodeJS.ErrnoException).code === "EPERM";
  }
};

/** What is running on this host: which groups hold something, and which workers' work is still in
 * them. Undefined when the host cannot be asked, which is answered as "everything is still here".
 * `ps` is not installed on a slim container image, which is where most of these run, so Linux is
 * read from `/proc` and everything else asks `ps`. */
async function liveHere(): Promise<{ groups: Set<number>; workers: Set<string> } | undefined> {
  const groups = new Set<number>();
  const workers = new Set<string>();
  const found = (text: string) => {
    // The environment is NUL-separated in `/proc` and space-separated in `ps`, so this reads the
    // name off either without pretending to parse the whole of it.
    const at = text.indexOf(`${WORKER_ENV}=`);
    if (at >= 0) workers.add(text.slice(at + WORKER_ENV.length + 1).split(/[\0\s]/)[0]);
  };
  try {
    if (process.platform === "linux") {
      for (const name of await readdir("/proc")) {
        if (!/^\d+$/.test(name) || name === String(process.pid)) continue;
        const stat = await readFile(`/proc/${name}/stat`, "utf8").catch(() => undefined);
        // A command can hold spaces and brackets, so the fields after it are counted from the last
        // close bracket: state, ppid, pgrp.
        const after = stat?.slice(stat.lastIndexOf(")") + 2).split(" ");
        const pgrp = after ? Number.parseInt(after[2], 10) : NaN;
        if (Number.isFinite(pgrp)) groups.add(pgrp);
        // Readable for this user's processes, which is what a tool of ours is. Anything else is
        // not something this worker started.
        found(await readFile(`/proc/${name}/environ`, "utf8").catch(() => ""));
      }
      return { groups, workers };
    }
    // `-E` prints each process's environment after its command, for the processes this user owns.
    // Without the name in its own environment: `ps` is a child of this process, so it inherits
    // whatever we hold, and it would otherwise report itself as work this worker left running.
    const { [WORKER_ENV]: _ours, ...env } = process.env;
    const { stdout } = await execFileAsync("ps", ["-A", "-E", "-o", "pid=,pgid=,command="], {
      maxBuffer: 32 * 1024 * 1024,
      env,
    });
    for (const line of stdout.split("\n")) {
      const [pid, pgid] = line.trim().split(/\s+/, 2).map((n) => Number.parseInt(n, 10));
      if (pid === process.pid) continue;
      if (Number.isFinite(pgid)) groups.add(pgid);
      found(line);
    }
    return { groups, workers };
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

// Once per process: a process cannot change the group it is in.
let ourGroup: Promise<number | undefined> | undefined;
const group = () => (ourGroup ??= readGroup(process.pid));

/** Say a tool call is about to write this directory. `endWrite` says it came back. */
export async function beginWrite(projectDir: string, writer: Writer): Promise<void> {
  await mkdir(writersDir(projectDir), { recursive: true });
  await writeJson(writerPath(projectDir, writer.callId), {
    ...writer,
    host: hostname(),
    pid: process.pid,
    ...((await group()) === undefined ? {} : { pgid: await group() }),
    worker,
    bootAt: bootAt(),
    started: new Date().toISOString(),
  } satisfies WriterNote);
}

/** Whatever it did to the directory, it is not still doing it. */
export async function endWrite(projectDir: string, callId: string): Promise<void> {
  await rm(writerPath(projectDir, callId), { force: true });
}

/**
 * Whether this marker can still be a tool inside its own execution, which is the only thing the
 * refusal is worth its cost for. Everything here is a reason to stop refusing, never a reason to
 * start: what cannot be answered is answered as still running.
 */
async function maybeInside(
  note: WriterNote,
  live: Awaited<ReturnType<typeof liveHere>>,
): Promise<boolean> {
  // A pid from another machine says nothing here, and two hosts sharing one data directory is the
  // only way to get one. Neither of them can see the other's processes.
  if (note.host !== hostname()) return true;
  // Written by a worker that did not date its marker, so there is nothing to tell a live pid from
  // a reused one.
  if (note.bootAt === undefined) return true;
  // The machine restarted. Nothing it was running came back with it.
  if (Math.abs(note.bootAt - bootAt()) > SAME_BOOT) return false;
  if (running(note.pid)) return true;
  // The writer is gone, and what a tool starts can outlive it. Nothing here is asked of the pids
  // themselves, which come round again; it is asked of what those processes are carrying.
  if (live === undefined) return true;
  // Anything the worker started, wherever it ended up. A tool that daemonizes leaves the group and
  // keeps the environment, which is why this is the check that decides most cases.
  if (note.worker !== undefined && live.workers.has(note.worker)) return true;
  // And the group, for a tool that was given an environment of somebody else's choosing. Only when
  // this process is somewhere else: a worker restarted from the same shell is in the group its
  // predecessor was in, and finding ourselves there is not evidence about anything.
  if (note.pgid !== undefined && note.pgid !== (await group())) return live.groups.has(note.pgid);
  return false;
}

// Markers that cannot be a live tool any more are dropped rather than reported: the refusal exists
// because nothing could prove the tool stopped, so where something can, it stops standing.
async function writersHere(projectDir: string): Promise<WriterNote[]> {
  const names = await readdir(writersDir(projectDir)).catch(() => [] as string[]);
  if (names.length === 0) return [];
  const live = await liveHere();
  const found: WriterNote[] = [];
  for (const name of names) {
    const path = join(writersDir(projectDir), name);
    const note = await readJson<WriterNote>(path);
    if (!note) continue;
    if (await maybeInside(note, live)) found.push(note);
    else await rm(path, { force: true });
  }
  return found;
}

/** Thrown when a directory is refused because a writer from an earlier step never came back. */
export class Quarantined extends Error {}

/**
 * Refuse a directory an unaccounted writer may still be inside. The step's own calls run at once
 * on this host by design, so their markers are not a reason to refuse; a marker from another step
 * or another turn is exactly the case the driver cannot see from the workflow.
 */
async function refuseWhenStranded(projectDir: string, current?: Writer): Promise<void> {
  const stranded = (await writersHere(projectDir)).filter(
    (note) => !current || note.turn !== current.turn || note.step !== current.step,
  );
  if (stranded.length === 0) return;
  const one = stranded[0];
  throw new Quarantined(
    `not using ${projectDir}: ${stranded.length} tool call(s) from an earlier step never returned ` +
      `(${one.callId} of turn ${one.turn} step ${one.step}, pid ${one.pid} on ${one.host}, started ` +
      `${one.started}). Stop them, then clear it with \`pi-temporal release-tree ${projectDir}\`.`,
  );
}

/** Which step a capture belongs to, for a step whose host was taken off it. */
export interface Fence {
  readonly turn: string;
  readonly step: number;
}

// Steps that were closed without their host. Said in the shared directory, because the host that
// has to hear it is the one nobody can reach: a tool the driver stopped waiting for keeps running,
// keeps holding a directory the session still names as current, and publishes when it finishes.
// Nothing local to that host knows the step ended, so the tip it publishes against looks live and
// the work that replaced it is reverted. A step named here has been recorded without that host, so
// a capture belonging to it is refused wherever it comes from.
const closedPath = (sessionFile: string) => join(shareDir(sessionFile), "closed.json");
// Long enough that a session's abandoned steps all stay named, short enough that this stays a file
// somebody can read. A step drops off the end only after hundreds of later ones closed the same way.
const CLOSED_KEPT = 500;

interface ClosedStep extends Fence {
  readonly at: string;
}

/**
 * Record that a step was closed without the host that was running it, so nothing that host still
 * has inside that step can move the project afterwards. Written before the work that replaces it
 * starts, which is what makes the order safe either way round: a capture that beat this note left
 * a tip the replacement reads, and one that comes after it is refused.
 */
export async function closeStep(sessionFile: string, fence: Fence): Promise<void> {
  await withSessionLock(sharedLockPath(sessionFile), async () => {
    const closed = (await readJson<ClosedStep[]>(closedPath(sessionFile))) ?? [];
    if (closed.some((c) => c.turn === fence.turn && c.step === fence.step)) return;
    const now = [...closed, { ...fence, at: new Date().toISOString() }];
    await writeJson(closedPath(sessionFile), now.slice(-CLOSED_KEPT));
  });
}

// Called with the shared lock held, so a capture cannot pass this check against a note that is
// being written for it.
const isClosed = async (sessionFile: string, fence: Fence) =>
  ((await readJson<ClosedStep[]>(closedPath(sessionFile))) ?? []).some(
    (c) => c.turn === fence.turn && c.step === fence.step,
  );

/** Clear the refusal, for an operator who has stopped whatever was left running. */
export async function clearWriters(projectDir: string): Promise<number> {
  const stranded = await writersHere(projectDir);
  await rm(writersDir(projectDir), { recursive: true, force: true });
  return stranded.length;
}

/**
 * Record the project's files against this session. Cheap when nothing changed: a tree id is
 * content-addressed, so an untouched directory produces the tree the tip already names and this
 * writes nothing.
 */
// What the directory holds right now, as a tree id. Needs the shadow repository, so callers hold
// the tree lock around it.
async function treeHere(projectDir: string) {
  await ensureShadow(projectDir);
  await git(projectDir, ["add", "-A"]);
  return (await git(projectDir, ["write-tree"])).stdout.trim();
}

// The bundles this host has not taken in yet, in order, because each one after the first names the
// previous commit as a prerequisite. Already having one is not an error worth stopping for.
async function ingest(projectDir: string, sessionFile: string, from = 0) {
  await ensureShadow(projectDir);
  const names = (await readdir(shareDir(sessionFile)).catch(() => []))
    .filter((name) => name.endsWith(".bundle"))
    .filter((name) => Number.parseInt(name, 10) > from)
    .sort();
  for (const name of names) {
    await git(projectDir, ["bundle", "unbundle", join(shareDir(sessionFile), name)]).catch(
      () => undefined,
    );
  }
}

// How many captures the chain grows for before one carries the whole tree and the rest are dropped.
// A step is a handful of captures, so this is tens of steps: long enough that most sessions never
// pay for a full tree, short enough that a session running for hours does not keep every state it
// has ever been in.
const COMPACT_EVERY = 40;

// Everything the self-contained bundle at `seq` made unnecessary. Nothing else in the directory is
// touched: `salvage/` is work nobody has another copy of, and the tip is what this was written for.
async function dropBundlesBefore(dir: string, seq: number) {
  const names = (await readdir(dir).catch(() => [] as string[])).filter((name) =>
    name.endsWith(".bundle"),
  );
  for (const name of names) {
    if (Number.parseInt(name, 10) < seq) await rm(join(dir, name), { force: true });
  }
}

// Commits whatever is in the directory and publishes it. The caller has already decided that is
// the right thing to do, which is the part with the rules in it.
async function publish(
  projectDir: string,
  sessionFile: string,
  tip: Tip | undefined,
  tree: string,
  built: boolean | undefined,
) {
  const seq = (tip?.seq ?? 0) + 1;
  // Every so often the chain restarts instead of growing. The bundle written here carries the whole
  // tree rather than the difference, and stands on nothing, so every bundle before it can go: a
  // host that is behind gets everything from this one alone. Without it a session's directory grows
  // for as long as the session lives, and a fresh host unbundles every capture ever made to catch
  // up. The commit is a root commit for the same reason `salvage` uses one: a bundle whose
  // prerequisites are missing cannot be unbundled at all.
  const restart = seq % COMPACT_EVERY === 0;
  const parent = tip && !restart ? ["-p", tip.commit] : [];
  const commit = (
    await git(projectDir, ["commit-tree", tree, ...parent, "-m", `pi-temporal ${tree.slice(0, 8)}`])
  ).stdout.trim();
  const ref = snapRef(sessionFile);
  await git(projectDir, ["update-ref", ref, commit]);

  const dir = shareDir(sessionFile);
  await mkdir(dir, { recursive: true });
  // The number is taken. Two different things look like this, and only one of them is a conflict.
  //
  // A writer that got ahead of us is: refuse, because continuing would leave the tip naming a
  // commit the surviving bundle does not carry. The tip says so.
  //
  // A writer that died between renaming its bundle into place and naming it as the tip is not. It
  // leaves a bundle nothing points at, and every host afterwards computes this same number, finds
  // it, and refuses. That wedges the session on every host for good: each refused capture then sets
  // its work aside and no host ever sees another's writes again. Take the orphan out and carry on.
  // Under the shared tree-store lease, which is what makes the decision safe: the session lock is
  // not held by every caller that gets here, so it is not what excludes a live writer.
  if ((await readdir(dir).catch(() => [] as string[])).includes(bundleName(seq))) {
    const now = await readJson<Tip>(tipPath(sessionFile));
    if ((now?.seq ?? 0) >= seq) {
      throw new WrongTree(
        `not shipping ${projectDir}: the session is already at bundle ${now?.seq ?? seq}`,
      );
    }
    console.warn(`dropping bundle ${seq} for ${sessionFile}: nothing names it and the tip is behind it`);
    await rm(join(dir, bundleName(seq)), { force: true });
  }
  // Written to a scratch name and renamed, so a host reading the directory never unbundles a file
  // that is still being written.
  const scratch = join(dir, `${bundleName(seq)}.${scratchToken()}.writing`);
  const incremental = tip && !restart ? ["--not", tip.commit] : [];
  await git(projectDir, ["bundle", "create", scratch, ref, ...incremental]);
  await rename(scratch, join(dir, bundleName(seq)));

  await writeJson(tipPath(sessionFile), { tree, commit, seq } satisfies Tip);
  await rm(forgottenPath(sessionFile), { force: true });
  // After the tip names the self-contained one, never before: a crash in between leaves bundles
  // nothing needs, and a crash the other way round leaves a session nobody can restore.
  if (restart) await dropBundlesBefore(dir, seq);
  // `built` is carried, never invented here. A capture says what the directory now holds, not where
  // it came from, and only a restore that creates a directory may answer that this host built it.
  // Inventing it here would give a later idle period permission to empty somebody's checkout.
  await writeJson(heldPath(projectDir, sessionFile), {
    tree,
    seq,
    built,
    session: sessionFile,
    directory: projectDir,
  } satisfies Held);
}

// Work this host holds that the session never shipped, put where it can be recovered instead of
// being reverted away. Kept out of the numbered sequence and out of the directory `ingest` reads,
// so getting it back is a deliberate act and no other host pays to carry it.
async function salvage(projectDir: string, sessionFile: string, tree: string) {
  const dir = join(shareDir(sessionFile), "salvage");
  await mkdir(dir, { recursive: true });
  // A root commit rather than one on the tip: the bundle then carries the whole tree and stands on
  // its own, which is what someone recovering it an hour later needs.
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

// Empty a directory this host built for a session, so the next one can have it. Refuses the two
// cases where emptying costs something nobody can get back: a directory that was somebody's before
// this session found it, and one holding work the session never shipped. Answers whether it came
// out empty, because neither command below removes a `.git` at the root and a directory that is not
// actually empty is one the next restore refuses.
async function handBack(projectDir: string, sessionFile: string, held: Held) {
  if (!held.built) return false;
  // A retired host may be behind the final tip. Its own accepted tree distinguishes local edits.
  if ((await treeHere(projectDir)) !== held.tree) return false;
  await git(projectDir, ["read-tree", "-u", "--reset", EMPTY_TREE]);
  // `-x` as well, because what the next session finds has to be an empty directory and not a nearly
  // empty one. On a tree this host built there is nothing to lose, which is why only those get here.
  await git(projectDir, ["clean", "-fdxq"]);
  return await isEmptyDir(projectDir);
}

// Whether another session is using this directory, after handing back the ones that are over. A
// session leaves a note on every host that served it, and the retirement runs on exactly one of
// them, so the rest used to keep a directory nobody was using and refuse every later session with
// it. The session says it is finished in the shared directory; each host repairs itself from that.
// Called with the tree lock held.
async function heldByOthers(projectDir: string, sessionFile: string) {
  const mine = heldName(sessionFile);
  const names = (await readdir(hostDir(projectDir)).catch(() => [] as string[])).filter(
    (name) => name.startsWith("held-") && name !== mine,
  );
  let holdouts = 0;
  for (const name of names) {
    const path = join(hostDir(projectDir), name);
    const note = await readJson<Held>(path);
    // A note that names no session cannot be asked about, and a session still running holds what it
    // holds. Both stay.
    if (!note?.session || !(await isRetired(note.session))) {
      holdouts++;
      continue;
    }
    if (note.built && !(await handBack(projectDir, note.session, note))) {
      holdouts++;
      continue;
    }
    await rm(path, { force: true });
  }
  return holdouts > 0;
}

/**
 * Record the project's files against this session. Cheap when nothing changed: a tree id is
 * content-addressed, so an untouched directory produces the tree the tip already names.
 */
export async function capture(
  projectDir: string,
  sessionFile: string,
  // Whether this caller may establish the project when nothing has shipped yet. Off for everything
  // a worker draws, because the first capture decides what the project *is* and a worker is chosen
  // by whatever was free. Letting one do it meant an empty `/project` on the host that happened to
  // draw the first tool became the project, on every host.
  opts: {
    readonly seed?: boolean;
    readonly current?: Writer;
    // Which step this capture belongs to. A step the session closed without this host is one this
    // host may no longer publish for.
    readonly fence?: Fence;
  } = {},
): Promise<void> {
  // One at a time per directory. Two captures share an index and a shadow repository, so a second
  // one collides on `index.lock`, and that failure reads as a session that stopped shipping.
  await withTreeLocks(projectDir, sessionFile, async () => {
    // What this directory holds is not this session's to publish while a writer from another step
    // is unaccounted for: its files are in there too.
    await refuseWhenStranded(projectDir, opts.current);
    // Nor is it this step's to publish once the session has closed that step somewhere else. This
    // is the case the tip check cannot answer: a tool that outlived its dispatch is still standing
    // on the tip it read, so it publishes cleanly and reverts whatever replaced it.
    if (opts.fence && (await isClosed(sessionFile, opts.fence))) {
      throw new WrongTree(
        `not shipping ${projectDir}: turn ${opts.fence.turn} step ${opts.fence.step} was closed ` +
          `without this host, and what it produced is kept rather than published`,
      );
    }
    // Work of any kind means this session is not the finished one its last idle period marked.
    await revive(sessionFile);
    const tip = await readJson<Tip>(tipPath(sessionFile));
    const held = await readJson<Held>(heldPath(projectDir, sessionFile));

    if (!tip) {
      // Nothing shipped yet. Whatever is here is the starting point, but only from a caller that
      // is entitled to say so, and only if nothing else is already working in this directory.
      if (!opts.seed) return;
      if (await heldByOthers(projectDir, sessionFile)) {
        throw new WrongTree(`not shipping ${projectDir}: another session is working in it`);
      }
    } else if (held?.tree !== tip.tree) {
      // Only a host standing on the tip may add to it. Committing from a host that never caught up
      // publishes its older files as the newer tree, and every other host then resets to them,
      // which loses the work they were shipped to carry.
      throw new WrongTree(
        `not shipping ${projectDir}: this host holds ${held?.tree.slice(0, 8) ?? "nothing"}, ` +
          `and the session is at ${tip.tree.slice(0, 8)}`,
      );
    }

    const tree = await treeHere(projectDir);
    if (tip?.tree === tree) return;
    if (tip) await ingest(projectDir, sessionFile, held?.seq ?? 0);
    await publish(projectDir, sessionFile, tip, tree, held?.built);
  });
}

/**
 * Bring this host's copy of the project to the newest state this session has shipped. Throws when
 * the directory holds something this session did not put there, because running against it would
 * describe somebody else's files as the project.
 */
export async function ensure(projectDir: string, sessionFile: string, current?: Writer): Promise<void> {
  await withTreeLocks(projectDir, sessionFile, async () => {
    // Before anything is written or checked out. A restore is what brings this host to the tip, and
    // doing that under an abandoned writer is what makes its next capture look current.
    await refuseWhenStranded(projectDir, current);
    await revive(sessionFile);
    // Read inside the lock. A capture on this host between the read and the lock would leave this
    // deciding against a tip that has already moved, and resetting the directory to the older tree.
    const tip = await readJson<Tip>(tipPath(sessionFile));

    // Before the tip check, not after it. A session with nothing shipped yet used to return here,
    // so its tools ran in a directory another session was working in and edited its files.
    if (await heldByOthers(projectDir, sessionFile)) {
      throw new WrongTree(`not restoring ${projectDir}: another session is working in it`);
    }
    // Nothing shipped, and nothing here may establish it. A tool call lands on whichever worker is
    // free, and so does the model call: an activity that adopts its own directory puts the project
    // wherever Temporal happened to send the first unit of work, which is how an empty `/project`
    // became the project on every host. The client sends it, before the session starts.
    if (!tip) {
      throw new WrongTree(
        `no project established for ${sessionFile}: send it with \`pi-temporal start --project=...\``,
      );
    }

    const held = await readJson<Held>(heldPath(projectDir, sessionFile));
    if (held?.tree === tip.tree) return;

    if (!held) {
      // Never seen by this session. Empty belongs to nobody, which is the ordinary shape of a host
      // that has never run this project. Anything else is somebody's checkout or another session's
      // work, and neither is ours to reset.
      if (!(await isEmptyDir(projectDir))) {
        throw new WrongTree(
          `not restoring ${projectDir}: it holds files this session never shipped`,
        );
      }
    } else {
      // Behind the tip, and holding files this session never shipped: a tool wrote and the worker
      // died before the result got out. Both are real work and they cannot both stay. Publishing
      // this tree is the tempting answer and the wrong one, because the tip is what every other
      // host has already agreed on, so moving it here reverts everything shipped since `held` on
      // all of them. The odd one out is set aside instead, and the directory comes to the tip.
      const here = await treeHere(projectDir);
      if (here !== held.tree) await salvage(projectDir, sessionFile, here);
    }

    await mkdir(projectDir, { recursive: true });
    await ingest(projectDir, sessionFile, held?.seq ?? 0);

    // `-u --reset` is what makes this a move rather than a merge: a file the newer tree dropped is
    // removed, which checking the paths out would leave behind.
    await git(projectDir, ["read-tree", "-u", "--reset", tip.tree]);
    // A restore into a directory this session had no note for is the one case that creates a
    // directory rather than adopting one, and `!held` only gets past the guard above when the
    // directory was empty. Otherwise carry what the note already said, including when it said
    // nothing: an adopted directory's note has no `built` field, and reading that absence as a
    // missing value to default hands somebody's checkout to `release`.
    await writeJson(heldPath(projectDir, sessionFile), {
      tree: tip.tree,
      seq: tip.seq,
      built: held ? held.built : true,
      session: sessionFile,
      directory: projectDir,
    } satisfies Held);
  });
}

// git's hash of the empty tree. Checking it out is how a directory is emptied without `rm -rf`,
// which would take the shadow repository's own bookkeeping with it.
const EMPTY_TREE = "4b825dc642cb6eb9a060e54bf8d69288fbee4904";

/**
 * Hand this host's directory back, so the next session can have it. What marks a directory taken is
 * this session's note, and the files in a directory this host built are this session's too, so both
 * go.
 *
 * Refuses on three counts: a directory this host did not build out of an empty one, a directory
 * holding work that never shipped, and a directory that did not actually come out empty. Returns
 * whether it was freed.
 */
export async function release(projectDir: string, sessionFile: string): Promise<boolean> {
  return await withTreeLocks(projectDir, sessionFile, async () => {
    const held = await readJson<Held>(heldPath(projectDir, sessionFile));
    if (!held) return false;
    // A directory that could not be emptied is one the next restore refuses, so dropping the note
    // there would wedge this host instead of handing it back. Keep the note and say no.
    if (!(await handBack(projectDir, sessionFile, held))) return false;
    await rm(heldPath(projectDir, sessionFile), { force: true });
    return true;
  });
}

/**
 * The session is over. Say so where every host can read it, then hand this host's directory back.
 *
 * The marker is the half that matters: the retirement runs on one host, and the others are holding
 * a directory each for a session nobody is driving. They read this the next time a session wants
 * the directory, and hand their own back then.
 */
export async function retire(projectDir: string, sessionFile: string): Promise<boolean> {
  if (await established(sessionFile)) {
    await writeJson(retiredPath(sessionFile), { at: new Date().toISOString() });
  }
  return await release(projectDir, sessionFile);
}

/**
 * Give a session the project a client put somewhere else, by copying that store into this session's
 * own. For a schedule: every firing is its own session and nothing is running at firing time to
 * send a project, so the client sends one when the schedule is made and each firing takes a copy.
 *
 * This runs on a worker and it does not break the rule that a worker may not establish a project.
 * What it publishes is not the directory this worker is standing in, which is the thing that rule
 * exists to keep out; it is a store some client wrote, chosen by whoever made the schedule.
 *
 * A no-op once the session has a tip of its own, so a re-driven activity does not copy twice.
 */
export async function adopt(template: string, sessionFile: string): Promise<boolean> {
  return await withSessionLock(sharedLockPath(sessionFile), async () => {
    if (await established(sessionFile)) return false;
    const tip = await readJson<Tip>(tipPath(template));
    if (!tip) throw new WrongTree(`no project was sent for ${template}`);
    const from = shareDir(template);
    const to = shareDir(sessionFile);
    await mkdir(to, { recursive: true });
    for (const name of (await readdir(from).catch(() => [] as string[])).filter((n) =>
      n.endsWith(".bundle"),
    )) {
      // Through a scratch name for the same reason a capture is: a host reading this directory must
      // never unbundle a file that is still being copied.
      const scratch = join(to, `${name}.${scratchToken()}.writing`);
      await writeFile(scratch, await readFile(join(from, name)));
      await rename(scratch, join(to, name));
    }
    // Last, so a copy that dies half way leaves a session with no project rather than one whose tip
    // names a bundle that is not there.
    await writeJson(tipPath(sessionFile), tip);
    await rm(forgottenPath(sessionFile), { force: true });
    return true;
  });
}

/**
 * Drop this host's claim on a directory without touching what the session shipped. For a client
 * that seeded a template: the note says "a session is working here", and nothing ever retires a
 * template, so leaving it would refuse every later session in the directory it was sent from.
 */
export const unclaim = (projectDir: string, sessionFile: string) =>
  rm(heldPath(projectDir, sessionFile), { force: true });

/**
 * Hand back every directory this host is still holding for a session that is over, and say how many
 * there were. Called when a worker starts.
 *
 * The lazy path only frees a directory when another session asks for that same one, which is enough
 * to keep a worker serving and not enough to keep it tidy: a worker that served fifty sessions in
 * fifty directories holds all fifty until somebody wants each one back. Same rules as everywhere
 * else, so nothing here empties a directory this host adopted or one holding work that never
 * shipped.
 */
export async function sweep(): Promise<number> {
  let freed = 0;
  for (const dir of await readdir(treesRoot()).catch(() => [] as string[])) {
    const names = (await readdir(join(treesRoot(), dir)).catch(() => [] as string[])).filter((name) =>
      name.startsWith("held-"),
    );
    for (const name of names) {
      const path = join(treesRoot(), dir, name);
      const note = await readJson<Held>(path);
      if (!note?.session || !note.directory || !(await isRetired(note.session))) continue;
      const done = await withTreeLocks(note.directory, note.session, async () => {
        const current = await readJson<Held>(path);
        if (
          !current || current.session !== note.session || current.directory !== note.directory ||
          !(await isRetired(note.session!))
        ) return false;
        if (current.built && !(await handBack(note.directory!, note.session!, current))) return false;
        await rm(path, { force: true });
        return true;
      }).catch(() => false);
      if (done) freed++;
    }
  }
  return freed;
}

/**
 * Whether this session already has a project. A client continuing a session must not send one
 * again: the workers have moved the tip since it started, and what the client holds is the state
 * the session began from, which a capture would then be refused for or, worse, revert to.
 */
export const established = async (sessionFile: string) =>
  (await readJson<Tip>(tipPath(sessionFile))) !== undefined;

/**
 * Keep what this host holds where it can be recovered, without touching the tip. For a caller that
 * could not ship and must not lose the work: a tool has already run, so the files are the only
 * record of what it did.
 */
export async function setAside(projectDir: string, sessionFile: string): Promise<void> {
  await withTreeLocks(projectDir, sessionFile, async () => {
    await salvage(projectDir, sessionFile, await treeHere(projectDir));
  });
}

/**
 * Drop what a session's tree cost, once the session is over. Pass the directory it ran in to let
 * the next session have it: what marks a directory taken is this session's note, so removing it is
 * what hands the directory back.
 */
export async function forget(sessionFile: string, projectDir?: string): Promise<void> {
  const drop = async () => {
    await writeJson(forgottenPath(sessionFile), { at: new Date().toISOString() });
    for (const name of await readdir(shareDir(sessionFile))) {
      // Removing the held lock would admit another writer while deletion is still in progress.
      if (name === "writers.lock") continue;
      await rm(join(shareDir(sessionFile), name), { recursive: true, force: true });
    }
    if (projectDir) await rm(heldPath(projectDir, sessionFile), { force: true });
  };
  if (projectDir) await withTreeLocks(projectDir, sessionFile, drop);
  else await withSessionLock(sharedLockPath(sessionFile), drop);
}
