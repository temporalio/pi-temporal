// Moves the project's files between hosts, so a session that resumes on another worker finds the
// work the last one did. The session log already travels: it is one file in a shared directory.
// The files the tools actually edit did not, so a worker on a second machine started every step in
// whatever directory it happened to be pointed at, and told the model the project was empty.
//
// The shape is git's, because git already answers the hard parts: content addressing, an
// incremental transfer, and a checkout that removes what a later tree dropped. Each capture writes
// one bundle into a directory beside the session log; a host that is behind unbundles the ones it
// has not seen and checks the newest tree out.
//
// Three things this deliberately does not do. It never touches the project's own `.git`: the
// shadow repository is host-local and points at the work tree from outside, so a project that is
// not a git repository works the same as one that is, and one that is keeps its own history. It
// never writes over somebody's checkout, and never drops work a crash caught before it shipped:
// that work is set aside under `salvage/` rather than reverted. And it never lets two sessions
// share one directory, which means one project directory serves one session at a time.
//
// Who may move the tip is the rule the rest follows from. Only a host standing on it may add to it,
// and nothing running on a worker may establish it: every activity, the model call included, lands
// on whichever worker Temporal had free, so an activity that adopts its own directory puts the
// project wherever the first unit of work happened to go. The client sends it, before the session
// starts, and a session with nothing established refuses every activity until it does.

import { execFile } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import { mkdir, readFile, readdir, rename, rm, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
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
// Wide enough that the lexical sort the restore depends on cannot run out. Four digits is about
// three hours of steps.
const bundleName = (seq: number) => `${String(seq).padStart(8, "0")}.bundle`;

// Host-local, and keyed by the path the tools run in. Two projects on one worker get a shadow
// repository each; the same project on two workers gets one on each of them.
function hostDir(projectDir: string) {
  const root = process.env.PI_TEMPORAL_DATA ?? join(homedir(), ".pi-temporal");
  return join(root, "trees", createHash("sha256").update(projectDir).digest("hex").slice(0, 16));
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
const isRetired = async (sessionFile: string) =>
  (await readJson<unknown>(retiredPath(sessionFile))) !== undefined;
// A session doing work is not retired, whatever a marker from its last idle period says.
const revive = (sessionFile: string) => rm(retiredPath(sessionFile), { force: true });
// Host-local, because a project directory is. One worker running two steps of two sessions is the
// case this excludes; two hosts cannot share the directory in the first place.
const treeLockPath = (projectDir: string) => join(hostDir(projectDir), "tree");

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

// Commits whatever is in the directory and publishes it. The caller has already decided that is
// the right thing to do, which is the part with the rules in it.
async function publish(
  projectDir: string,
  sessionFile: string,
  tip: Tip | undefined,
  tree: string,
  built: boolean | undefined,
) {
  const parent = tip ? ["-p", tip.commit] : [];
  const commit = (
    await git(projectDir, ["commit-tree", tree, ...parent, "-m", `pi-temporal ${tree.slice(0, 8)}`])
  ).stdout.trim();
  const ref = snapRef(sessionFile);
  await git(projectDir, ["update-ref", ref, commit]);

  const seq = (tip?.seq ?? 0) + 1;
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
  // Safe because the callers hold the session lock, so no live writer is inside that window.
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
  await git(projectDir, ["bundle", "create", scratch, ref, ...(tip ? ["--not", tip.commit] : [])]);
  await rename(scratch, join(dir, bundleName(seq)));

  await writeJson(tipPath(sessionFile), { tree, commit, seq } satisfies Tip);
  // `built` is carried, never invented here. A capture says what the directory now holds, not where
  // the directory came from, and the answer to that only changes when a restore creates one.
  await writeJson(heldPath(projectDir, sessionFile), {
    tree,
    seq,
    built,
    session: sessionFile,
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
  const tip = await readJson<Tip>(tipPath(sessionFile));
  if (!tip || !held.built) return false;
  if ((await treeHere(projectDir)) !== tip.tree) return false;
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
  // except the client and the model call, because the first capture decides what the project *is*
  // and a tool call is dispatched to whichever worker is free. Letting one do it meant an empty
  // `/project` on the host that happened to draw the first tool became the project, on every host.
  opts: { readonly seed?: boolean } = {},
): Promise<void> {
  // One at a time per directory. Two captures share an index and a shadow repository, so a second
  // one collides on `index.lock`, and that failure reads as a session that stopped shipping.
  await withSessionLock(treeLockPath(projectDir), async () => {
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
export async function ensure(projectDir: string, sessionFile: string): Promise<void> {
  await withSessionLock(treeLockPath(projectDir), async () => {
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
  return await withSessionLock(treeLockPath(projectDir), async () => {
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
  await withSessionLock(treeLockPath(projectDir), async () => {
    await salvage(projectDir, sessionFile, await treeHere(projectDir));
  });
}

/**
 * Drop what a session's tree cost, once the session is over. Pass the directory it ran in to let
 * the next session have it: what marks a directory taken is this session's note, so removing it is
 * what hands the directory back.
 */
export async function forget(sessionFile: string, projectDir?: string): Promise<void> {
  await rm(shareDir(sessionFile), { recursive: true, force: true });
  if (projectDir) await rm(heldPath(projectDir, sessionFile), { force: true });
}
