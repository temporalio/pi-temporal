// Checks the part that moves a project between machines, with the machines faked as separate data
// directories and separate project directories over one shared session directory. Needs no Temporal
// server and no model key, because none of this involves either.
//
// Usage: npx tsx worktree-check.mts

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
  await worktree.capture(projectA, sessionFile);
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
  const refusedCapture = await worktree.capture(projectA, other).then(() => false, () => true);
  const refusedEnsure = await worktree.ensure(projectA, other).then(() => false, () => true);
  const untouched = (await read(join(projectA, "note.txt"))) === mine;
  check("another session cannot adopt a directory", refusedCapture && untouched, {
    refusedCapture,
    refusedEnsure,
    untouched,
  });

  // The guard only bites when this host is behind, so move the tip first. Otherwise `ensure`
  // returns at "already at the tip" and never reaches the part being checked.
  asHost(root, "b");
  await writeFile(join(projectB, "note.txt"), "three\n");
  await worktree.capture(projectB, sessionFile);

  // A tool wrote and the worker died before the result got out. Those files are real work, so the
  // move ships them rather than resetting over them.
  asHost(root, "a");
  await writeFile(join(projectA, "unshipped.txt"), "not yours\n");
  await worktree.ensure(projectA, sessionFile);
  const kept = await read(join(projectA, "unshipped.txt"));
  check("work a crash left behind is shipped, not dropped", kept === "not yours\n", kept);
  asHost(root, "b");
  await worktree.ensure(projectB, sessionFile);
  const reached = await read(join(projectB, "unshipped.txt"));
  check("and the other host then gets it", reached === "not yours\n", reached);

  asHost(root, "a");
  await worktree.forget(sessionFile, projectA);
  const gone = await readdir(`${sessionFile}.tree`).then(() => false, () => true);
  check("forgetting a session drops what its tree cost", gone);

  // And hands the directory back. Otherwise a worker with one project directory serves exactly one
  // session for as long as it lives, which is not a fleet.
  const taken = await worktree.capture(projectA, other).then(() => true, () => false);
  check("a finished session releases its directory", taken);

  console.log(failures.length === 0 ? "\nworktree-check: OK" : `\nworktree-check: ${failures.length} failed`);
  process.exit(failures.length === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
