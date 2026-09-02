// Checks the part that moves a project between machines, with the machines faked as separate data
// directories and separate project directories over one shared session directory. Needs no Temporal
// server and no model key, because none of this involves either.
//
// Usage: npx tsx worktree-check.mts

import { execFileSync } from "node:child_process";
import { mkdir, mkdtemp, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import * as worktree from "./src/worktree.js";

const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

// A host is its own data directory: that is where the shadow repository and the note saying what
// this host holds both live. Nothing else about a host matters to this code.
const asHost = (root: string, name: string) => {
  process.env.PI_TEMPORAL_DATA = join(root, name, "data");
};

const read = (path: string) => readFile(path, "utf8").catch(() => "");

// The note a host keeps about what it holds. Its path is hashed from the project directory, so it
// is found rather than constructed: there is one tree directory per project on a host, and one
// note per session in it.
async function heldNote(root: string, host: string): Promise<{ seq?: number } | undefined> {
  const trees = join(root, host, "data", "trees");
  for (const dir of await readdir(trees).catch(() => [] as string[])) {
    const names = await readdir(join(trees, dir)).catch(() => [] as string[]);
    const note = names.find((n) => n.startsWith("held-"));
    if (note) return JSON.parse(await read(join(trees, dir, note))) as { seq?: number };
  }
  return undefined;
}

async function main() {
  const root = await mkdtemp(join(tmpdir(), "pi-worktree-"));
  const shared = join(root, "sessions");
  const sessionFile = join(shared, "s1.jsonl");
  const projectA = join(root, "a", "project");
  const projectB = join(root, "b", "project");
  await mkdir(shared, { recursive: true });
  await mkdir(projectA, { recursive: true });
  await mkdir(projectB, { recursive: true });

  asHost(root, "a");
  await writeFile(join(projectA, "note.txt"), "one\n");
  await mkdir(join(projectA, "sub"), { recursive: true });
  await writeFile(join(projectA, "sub", "deep.txt"), "deep\n");
  await worktree.capture(projectA, sessionFile, { seed: true });
  const shipped = await readdir(`${sessionFile}.tree`);
  check("a capture ships a bundle", shipped.some((n) => n.endsWith(".bundle")), shipped);

  // The case the whole thing exists for: a host that has never seen this project, with an empty
  // directory where the tools are about to run.
  asHost(root, "b");
  await worktree.ensure(projectB, sessionFile);
  check("a second host gets the files", (await read(join(projectB, "note.txt"))) === "one\n");
  check("nested files come too", (await read(join(projectB, "sub", "deep.txt"))) === "deep\n");

  // And back the other way, including a deletion, which a checkout of the paths alone would leave
  // behind on the host that did not do the deleting.
  await writeFile(join(projectB, "note.txt"), "two\n");
  await rm(join(projectB, "sub", "deep.txt"));
  await worktree.capture(projectB, sessionFile);
  asHost(root, "a");
  await worktree.ensure(projectA, sessionFile);
  check("the first host moves forward", (await read(join(projectA, "note.txt"))) === "two\n");
  const sub = await readdir(join(projectA, "sub")).catch(() => [] as string[]);
  check("a deleted file is deleted there too", !sub.includes("deep.txt"), sub);

  const before = (await readdir(`${sessionFile}.tree`)).length;
  await worktree.capture(projectA, sessionFile);
  const after = (await readdir(`${sessionFile}.tree`)).length;
  check("an unchanged capture ships nothing", after === before, { before, after });

  // The rule that keeps this out of a developer's checkout: files this host did not put there.
  const projectC = join(root, "c", "project");
  await mkdir(projectC, { recursive: true });
  await writeFile(join(projectC, "mine.txt"), "do not touch\n");
  asHost(root, "c");
  // Loudly, not quietly. A step that ran here would tell the model these files are the project.
  const refused = await worktree.ensure(projectC, sessionFile).then(() => false, () => true);
  const left = await readdir(projectC);
  check("a working copy is refused, not overwritten", refused && left.join() === "mine.txt", left);

  // A session that never shipped these files must not adopt them. Host A's directory holds s1's
  // work, so s2 gets neither a restore into it nor a capture out of it.
  asHost(root, "a");
  const other = join(root, "sessions", "s2.jsonl");
  const mine = await read(join(projectA, "note.txt"));
  const refusedCapture = await worktree
    .capture(projectA, other, { seed: true })
    .then(() => false, () => true);
  // Both directions, and both asserted. The restore returned before this check when the session had
  // shipped nothing, so a second session's tools ran in the first one's directory and edited its
  // files. The old shape computed this and then left it out of the assertion.
  const refusedEnsure = await worktree.ensure(projectA, other).then(() => false, () => true);
  const untouched = (await read(join(projectA, "note.txt"))) === mine;
  check("another session cannot adopt a directory", refusedCapture && refusedEnsure && untouched, {
    refusedCapture,
    refusedEnsure,
    untouched,
  });

  // The guard only bites when this host is behind, so move the tip first. Otherwise `ensure`
  // returns at "already at the tip" and never reaches the part being checked.
  asHost(root, "b");
  await writeFile(join(projectB, "note.txt"), "three\n");
  await worktree.capture(projectB, sessionFile);

  // A tool wrote and the worker died before the result got out, and the session moved on without
  // it. Both are real work and they cannot both stay. The tip is what every other host has agreed
  // on, so the odd one out is set aside and this host comes to the tip.
  asHost(root, "a");
  await writeFile(join(projectA, "unshipped.txt"), "not yours\n");
  await worktree.ensure(projectA, sessionFile);
  const moved = await read(join(projectA, "note.txt"));
  check("a host behind the tip is brought to it", moved === "three\n", moved);
  const salvaged = await readdir(join(`${sessionFile}.tree`, "salvage")).catch(() => [] as string[]);
  check("work a crash left behind is kept, not dropped", salvaged.some((n) => n.endsWith(".bundle")), salvaged);

  // The assertion the old shape did not make. That interleaving is a lost update, and reading only
  // the file that survives it leaves this green while everything shipped since reverts one line
  // away. It read `unshipped.txt` on both hosts and never re-read `note.txt`.
  asHost(root, "b");
  await worktree.ensure(projectB, sessionFile);
  const held = await read(join(projectB, "note.txt"));
  check("and nothing the other host shipped is reverted", held === "three\n", held);

  // Who may establish the project. A tool call lands on whichever worker is free, so a capture from
  // one must not be what decides: an empty directory on that host would become the project on every
  // host, and the host actually holding the files is then refused forever.
  const fresh = join(shared, "s3.jsonl");
  const projectD = join(root, "d", "project");
  const projectE = join(root, "e", "project");
  await mkdir(projectD, { recursive: true });
  await mkdir(projectE, { recursive: true });
  await writeFile(join(projectE, "real.txt"), "the project\n");
  asHost(root, "d");
  await worktree.capture(projectD, fresh);
  const seeded = await readdir(`${fresh}.tree`).catch(() => [] as string[]);
  check("a tool call cannot establish the project", seeded.length === 0, seeded);
  asHost(root, "e");
  await worktree.capture(projectE, fresh, { seed: true });
  asHost(root, "d");
  await worktree.ensure(projectD, fresh);
  const real = await read(join(projectD, "real.txt"));
  check("the host holding the files defines it", real === "the project\n", real);

  // What makes the next restore incremental. Read on a host whose note came from a restore rather
  // than from its own capture, which is the only place the reset path writes one. This pins that
  // the bookkeeping is written, not the number of subprocesses it saves, which nothing here
  // measures.
  const noted = await heldNote(root, "d");
  check("a restore records how far it got", typeof noted?.seq === "number", noted);

  // How a directory is handed back, so the next session can have it. Without this a worker with one
  // project directory serves one session for as long as it lives and refuses every later one.
  asHost(root, "b");
  await writeFile(join(projectB, "not-shipped.txt"), "mine\n");
  const refusedRelease = (await worktree.release(projectB, sessionFile)) === false;
  check("a directory holding unshipped work is not handed back", refusedRelease);
  await rm(join(projectB, "not-shipped.txt"));
  const released = await worktree.release(projectB, sessionFile);
  const emptied = (await readdir(projectB)).length === 0;
  check("and a clean one is", released && emptied, { released, emptied });
  const s4 = join(shared, "s4.jsonl");
  const reusable = await worktree
    .capture(projectB, s4, { seed: true })
    .then(() => true, () => false);
  check("which is what lets the next session use it", reusable);

  // The case the assertion above cannot see, because its fake project is a bare directory. A real
  // project is a git checkout, and a host that seeded the session from one is holding somebody
  // else's files: tracked, untracked, and the ones git ignores, which no bundle carries. Emptying
  // that is data loss, and the leftover `.git` would wedge the host for every later session too.
  const owned = join(root, "owned", "project");
  await mkdir(owned, { recursive: true });
  execFileSync("git", ["init", "-q", owned]);
  await writeFile(join(owned, "src.txt"), "code\n");
  await writeFile(join(owned, ".gitignore"), ".env\n");
  await writeFile(join(owned, ".env"), "SECRET=1\n");
  const s5 = join(shared, "s5.jsonl");
  asHost(root, "owned");
  await worktree.capture(owned, s5, { seed: true });
  const refusedOwned = (await worktree.release(owned, s5)) === false;
  const intact = (await read(join(owned, "src.txt"))) === "code\n";
  const ignoredKept = (await read(join(owned, ".env"))) === "SECRET=1\n";
  check("a directory this host did not build is never emptied", refusedOwned && intact && ignoredKept, {
    refusedOwned,
    intact,
    ignoredKept,
  });

  asHost(root, "a");
  await worktree.forget(sessionFile, projectA);
  const gone = await readdir(`${sessionFile}.tree`).then(() => false, () => true);
  check("forgetting a session drops what its tree cost", gone);

  // And hands the directory back. Otherwise a worker with one project directory serves exactly one
  // session for as long as it lives, which is not a fleet.
  const taken = await worktree
    .capture(projectA, other, { seed: true })
    .then(() => true, () => false);
  check("a finished session releases its directory", taken);

  console.log(failures.length === 0 ? "\nworktree-check: OK" : `\nworktree-check: ${failures.length} failed`);
  process.exit(failures.length === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
