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
  await worktree.ensure(projectC, sessionFile);
  const left = await readdir(projectC);
  check("a working copy is left alone", left.join() === "mine.txt", left);

  // A directory belongs to one session. A second one editing and resetting it would lose the
  // first one's files, and neither would report anything wrong. Back on host A, because a claim is
  // host-local: the case it covers is two sessions pointed at one directory on one machine.
  asHost(root, "a");
  const other = join(root, "sessions", "s2.jsonl");
  const mine = await read(join(projectA, "note.txt"));
  await worktree.capture(projectA, other);
  const untouched = (await read(join(projectA, "note.txt"))) === mine;
  const noTip = await readdir(`${other}.tree`).then(() => false, () => true);
  check("a second session cannot capture another's directory", untouched && noTip);

  // Local work nothing has shipped is what stops a reset, whether or not this host built the
  // directory. Otherwise a host that captured once may be moved over its own unshipped edits.
  await writeFile(join(projectA, "unshipped.txt"), "not yours\n");
  await worktree.ensure(projectA, sessionFile);
  const kept = await read(join(projectA, "unshipped.txt"));
  check("unshipped local work stops a reset", kept === "not yours\n", kept);
  await rm(join(projectA, "unshipped.txt"));

  await worktree.forget(sessionFile);
  const gone = await readdir(`${sessionFile}.tree`).then(() => false, () => true);
  check("forgetting a session drops what its tree cost", gone);

  console.log(failures.length === 0 ? "\nworktree-check: OK" : `\nworktree-check: ${failures.length} failed`);
  process.exit(failures.length === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
