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
// Two things this deliberately does not do. It never touches the project's own `.git`: the shadow
// repository is host-local and points at the work tree from outside, so a project that is not a git
// repository works the same as one that is, and one that is keeps its own history untouched. And it
// never writes over a tree this host did not build, because that is somebody's working copy.

import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdir, readFile, readdir, rename, rm, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

// The ref the shadow repository keeps its newest snapshot on. A bundle has to name a ref, and this
// is the only one, so an incremental bundle is always this ref against the previous commit.
const SNAP = "refs/pi-temporal/snapshot";

// What a host knows about its own copy: the tree it last checked out, which doubles as the mark
// saying this directory is one we built rather than one a person is working in.
interface Held {
  readonly tree: string;
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
const bundleName = (seq: number) => `${String(seq).padStart(4, "0")}.bundle`;

// Host-local, and keyed by the path the tools run in. Two projects on one worker get a shadow
// repository each; the same project on two workers gets one on each of them.
function hostDir(projectDir: string) {
  const root = process.env.PI_TEMPORAL_DATA ?? join(homedir(), ".pi-temporal");
  return join(root, "trees", createHash("sha256").update(projectDir).digest("hex").slice(0, 16));
}

const gitDir = (projectDir: string) => join(hostDir(projectDir), "git");
const heldPath = (projectDir: string) => join(hostDir(projectDir), "held.json");

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

/**
 * Record the project's files against this session. Cheap when nothing changed: a tree id is
 * content-addressed, so an untouched directory produces the tree the tip already names and this
 * writes nothing.
 */
export async function capture(projectDir: string, sessionFile: string): Promise<void> {
  await ensureShadow(projectDir);
  // Everything git would track, which leaves out whatever the project ignores. A rebuilt tree may
  // still want an install step, the same as a fresh clone would.
  await git(projectDir, ["add", "-A"]);
  const tree = (await git(projectDir, ["write-tree"])).stdout.trim();

  const tip = await readJson<Tip>(tipPath(sessionFile));
  if (tip?.tree === tree) return;

  const parent = tip ? ["-p", tip.commit] : [];
  const commit = (
    await git(projectDir, ["commit-tree", tree, ...parent, "-m", `pi-temporal ${tree.slice(0, 8)}`])
  ).stdout.trim();
  await git(projectDir, ["update-ref", SNAP, commit]);

  const seq = (tip?.seq ?? 0) + 1;
  const dir = shareDir(sessionFile);
  await mkdir(dir, { recursive: true });
  // Only what the last capture did not already ship. Written to a scratch name and renamed, so a
  // host reading the directory never unbundles a file that is still being written.
  const scratch = join(dir, `${bundleName(seq)}.${process.pid}.writing`);
  await git(projectDir, [
    "bundle",
    "create",
    scratch,
    SNAP,
    ...(tip ? ["--not", tip.commit] : []),
  ]);
  await rename(scratch, join(dir, bundleName(seq)));

  await writeJson(tipPath(sessionFile), { tree, commit, seq } satisfies Tip);
  await writeJson(heldPath(projectDir), { tree } satisfies Held);
}

/**
 * Bring this host's copy of the project to the newest state the shared directory holds. Does
 * nothing when nothing is stored, when this host is already there, or when the directory holds a
 * tree this host did not build.
 */
export async function ensure(projectDir: string, sessionFile: string): Promise<void> {
  const tip = await readJson<Tip>(tipPath(sessionFile));
  if (!tip) return;

  const held = await readJson<Held>(heldPath(projectDir));
  if (held?.tree === tip.tree) return;

  // A directory with files in it that this host never built is a working copy, and checking a
  // stored tree out over it would rewrite somebody's files. An EMPTY directory is not a working
  // copy. It is the ordinary shape of a machine that has never seen this project, and refusing
  // there is how the tree ends up never travelling at all.
  if (!held && !(await isEmptyDir(projectDir))) return;

  await mkdir(projectDir, { recursive: true });
  await ensureShadow(projectDir);

  const names = (await readdir(shareDir(sessionFile)).catch(() => []))
    .filter((name) => name.endsWith(".bundle"))
    .sort();
  for (const name of names) {
    // In order, because each bundle after the first names the previous commit as a prerequisite.
    // Already having one is not an error worth stopping for, and neither is a bundle a newer
    // capture has since superseded.
    await git(projectDir, ["bundle", "unbundle", join(shareDir(sessionFile), name)]).catch(
      () => undefined,
    );
  }

  // `-u --reset` is what makes this a move rather than a merge: a file the newer tree dropped is
  // removed, which checking the paths out would leave behind.
  await git(projectDir, ["read-tree", "-u", "--reset", tip.tree]);
  await writeJson(heldPath(projectDir), { tree: tip.tree } satisfies Held);
}

/** Drop what a session's tree cost, once the session is over. */
export async function forget(sessionFile: string): Promise<void> {
  await rm(shareDir(sessionFile), { recursive: true, force: true });
}
