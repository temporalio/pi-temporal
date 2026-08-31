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
// never writes over work nothing has shipped, which covers both somebody's checkout and its own
// edits that never made it out. And it never lets two sessions share one directory.

import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
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
// it shipped it. What makes a reset safe is that the directory still matches this: anything else
// is work nothing has shipped, and moving the tree would throw it away.
interface Held {
  readonly tree: string;
}

// Which session a directory belongs to. Two sessions in one directory would edit each other's
// files and reset each other's trees, and neither would report anything wrong.
interface Claim {
  readonly session: string;
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
const heldPath = (projectDir: string) => join(hostDir(projectDir), "held.json");
const claimPath = (projectDir: string) => join(hostDir(projectDir), "claim.json");
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

// Through a scratch name, because a reader that finds half a document cannot tell it from a
// document that says something else.
async function writeJson(path: string, value: unknown) {
  await mkdir(dirname(path), { recursive: true });
  const scratch = `${path}.${process.pid}.writing`;
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

// Whether this directory is this session's to touch. The first session to use it takes it, and a
// second one is refused rather than allowed to edit and reset the first one's files.
async function ours(projectDir: string, sessionFile: string, what: string) {
  const claim = await readJson<Claim>(claimPath(projectDir));
  if (!claim || claim.session === sessionFile) return true;
  // Said out loud, because the alternative is a session quietly working on the wrong files.
  console.warn(
    `not ${what} ${projectDir}: it belongs to ${claim.session}, not ${sessionFile}`,
  );
  return false;
}

/**
 * Record the project's files against this session. Cheap when nothing changed: a tree id is
 * content-addressed, so an untouched directory produces the tree the tip already names and this
 * writes nothing.
 */
export async function capture(projectDir: string, sessionFile: string): Promise<void> {
  if (!(await ours(projectDir, sessionFile, "capturing"))) return;
  // One at a time per directory. Two captures share an index and a shadow repository, so a second
  // one collides on `index.lock` and the failure reads as a session that quietly stopped shipping.
  await withSessionLock(treeLockPath(projectDir), async () => {
    await ensureShadow(projectDir);
    // Everything git would track, which leaves out whatever the project ignores. A rebuilt tree
    // may still want an install step, the same as a fresh clone would.
    await git(projectDir, ["add", "-A"]);
    const tree = (await git(projectDir, ["write-tree"])).stdout.trim();

    const tip = await readJson<Tip>(tipPath(sessionFile));
    if (tip?.tree === tree) return;

    const parent = tip ? ["-p", tip.commit] : [];
    const commit = (
      await git(projectDir, [
        "commit-tree",
        tree,
        ...parent,
        "-m",
        `pi-temporal ${tree.slice(0, 8)}`,
      ])
    ).stdout.trim();
    const ref = snapRef(sessionFile);
    await git(projectDir, ["update-ref", ref, commit]);

    const seq = (tip?.seq ?? 0) + 1;
    const dir = shareDir(sessionFile);
    await mkdir(dir, { recursive: true });
    // Only what the last capture did not already ship. Written to a scratch name and renamed, so a
    // host reading the directory never unbundles a file that is still being written.
    const scratch = join(dir, `${bundleName(seq)}.${process.pid}.writing`);
    await git(projectDir, ["bundle", "create", scratch, ref, ...(tip ? ["--not", tip.commit] : [])]);
    await rename(scratch, join(dir, bundleName(seq)));

    await writeJson(tipPath(sessionFile), { tree, commit, seq } satisfies Tip);
    await writeJson(heldPath(projectDir), { tree } satisfies Held);
    await writeJson(claimPath(projectDir), { session: sessionFile } satisfies Claim);
  });
}

/**
 * Bring this host's copy of the project to the newest state the shared directory holds. Does
 * nothing when nothing is stored, when this host is already there, or when the directory holds a
 * tree this host did not build.
 */
export async function ensure(projectDir: string, sessionFile: string): Promise<void> {
  const tip = await readJson<Tip>(tipPath(sessionFile));
  if (!tip) return;
  if (!(await ours(projectDir, sessionFile, "restoring into"))) return;

  await withSessionLock(treeLockPath(projectDir), async () => {
    const held = await readJson<Held>(heldPath(projectDir));
    if (held?.tree === tip.tree) return;

    // What is on disk decides, not what a note claims. A directory holding anything this host has
    // not shipped is either somebody's working copy or a capture that never made it out, and a
    // reset would throw that away. Empty belongs to nobody, which is the ordinary shape of a
    // machine that has never seen this project.
    if (!(await isEmptyDir(projectDir))) {
      await ensureShadow(projectDir);
      await git(projectDir, ["add", "-A"]);
      const here = (await git(projectDir, ["write-tree"])).stdout.trim();
      if (here !== held?.tree) {
        console.warn(
          `not restoring ${projectDir} to ${tip.tree.slice(0, 8)}: it holds ${here.slice(0, 8)}, ` +
            `which nothing has shipped`,
        );
        return;
      }
    }

    await mkdir(projectDir, { recursive: true });
    await ensureShadow(projectDir);

    const names = (await readdir(shareDir(sessionFile)).catch(() => []))
      .filter((name) => name.endsWith(".bundle"))
      .sort();
    for (const name of names) {
      // In order, because each bundle after the first names the previous commit as a prerequisite.
      // Already having one is not an error worth stopping for.
      await git(projectDir, ["bundle", "unbundle", join(shareDir(sessionFile), name)]).catch(
        () => undefined,
      );
    }

    // `-u --reset` is what makes this a move rather than a merge: a file the newer tree dropped is
    // removed, which checking the paths out would leave behind.
    await git(projectDir, ["read-tree", "-u", "--reset", tip.tree]);
    await writeJson(heldPath(projectDir), { tree: tip.tree } satisfies Held);
    await writeJson(claimPath(projectDir), { session: sessionFile } satisfies Claim);
  });
}

/** Drop what a session's tree cost, once the session is over. */
export async function forget(sessionFile: string): Promise<void> {
  await rm(shareDir(sessionFile), { recursive: true, force: true });
}
