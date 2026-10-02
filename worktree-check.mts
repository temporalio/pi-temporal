// Checks the part that moves a project between machines, with the machines faked as separate data
// directories and separate project directories over one shared session directory. Needs no Temporal
// server and no model key, because none of this involves either.
//
// Usage: npx tsx worktree-check.mts

import { execFileSync } from "node:child_process";
import { mkdir, mkdtemp, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";
import * as worktree from "./src/worktree.js";
import { withSessionLock } from "./src/session-lock.js";

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
async function heldNotePath(root: string, host: string): Promise<string | undefined> {
  const trees = join(root, host, "data", "trees");
  for (const dir of await readdir(trees).catch(() => [] as string[])) {
    const names = await readdir(join(trees, dir)).catch(() => [] as string[]);
    const note = names.find((n) => n.startsWith("held-"));
    if (note) return join(trees, dir, note);
  }
  return undefined;
}

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

  // The step gave up on a pinned host and ran the rest somewhere else. That host was never told,
  // so its tool can finish and try to publish long afterwards. This is the guard the workflow
  // rests on when it moves a step off a worker that stopped answering: without it, moving would
  // mean the stranded host quietly reverting the tree the rest of the step was written against.
  asHost(root, "b");
  await writeFile(join(projectB, "note.txt"), "four\n");
  await worktree.capture(projectB, sessionFile);
  asHost(root, "a");
  await writeFile(join(projectA, "late.txt"), "written after the step moved on\n");
  let lateRefused = false;
  await worktree.capture(projectA, sessionFile).catch(() => {
    lateRefused = true;
  });
  check("a stranded host cannot publish over the tip", lateRefused);
  asHost(root, "b");
  await worktree.ensure(projectB, sessionFile);
  check("so the host that carried on keeps its work", (await read(join(projectB, "note.txt"))) === "four\n");
  // And what it wrote is recoverable rather than gone, which is what makes the move survivable
  // rather than merely safe.
  asHost(root, "a");
  await worktree.setAside(projectA, sessionFile).catch(() => undefined);
  const kept = await readdir(join(`${sessionFile}.tree`, "salvage")).catch(() => [] as string[]);
  check("and the work it stranded is set aside", kept.length > 0, kept);

  // A writer that died between renaming its bundle into place and naming it as the tip leaves a
  // bundle nothing points at. Every host afterwards computes that same number, so refusing it wedges
  // the session everywhere rather than on the one host that crashed.
  const orphaned = join(shared, "s6.jsonl");
  const projectF = join(root, "f", "project");
  await mkdir(projectF, { recursive: true });
  asHost(root, "f");
  await writeFile(join(projectF, "one.txt"), "first\n");
  await worktree.capture(projectF, orphaned, { seed: true });
  const tipBefore = await read(join(`${orphaned}.tree`, "tip.json"));
  const notePath = (await heldNotePath(root, "f"))!;
  const noteBefore = await read(notePath);
  await writeFile(join(projectF, "two.txt"), "second\n");
  await worktree.capture(projectF, orphaned);
  // Roll both back, which is what a crash after the bundle rename and before either write leaves:
  // the bundle is on disk and nothing names it.
  await writeFile(join(`${orphaned}.tree`, "tip.json"), tipBefore);
  await writeFile(notePath, noteBefore);
  await writeFile(join(projectF, "three.txt"), "third\n");
  const carriedOn = await worktree.capture(projectF, orphaned).then(() => true, () => false);
  check("a bundle nothing names does not wedge the session", carriedOn);

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
  // And nothing else on a worker can either. A model call lands on whichever worker is free too, so
  // letting that one adopt its directory only moved the race rather than closing it. With nothing
  // shipped, every activity refuses until a client sends the project.
  const unestablished = await worktree.ensure(projectD, fresh).then(() => false, () => true);
  check("and no activity runs before a client sends it", unestablished);
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

  // The same directory after a restore, which is what a session with more than one host does to it.
  // An adopted directory's note carries no answer to "who built this", and reading that absence as
  // a value to fall back on promoted somebody's repository to one this host may empty.
  const helper = join(root, "s5-worker", "project");
  asHost(root, "s5-worker");
  await worktree.ensure(helper, s5);
  await writeFile(join(helper, "src.txt"), "edited\n");
  await worktree.capture(helper, s5);
  asHost(root, "owned");
  await worktree.ensure(owned, s5);
  const afterRestore = (await worktree.release(owned, s5)) === false;
  const stillThere = (await read(join(owned, "src.txt"))) === "edited\n";
  const stillIgnored = (await read(join(owned, ".env"))) === "SECRET=1\n";
  check(
    "and a restore does not turn it into one this host built",
    afterRestore && stillThere && stillIgnored,
    { afterRestore, stillThere, stillIgnored },
  );

  // Handing the directory back, from the two sides that are not the one host the retirement runs
  // on. A session leaves a note wherever it ran, so without this a directory serves one session and
  // refuses every later one, which is the failure the note was supposed to prevent for other
  // sessions and caused for itself.
  const s7 = join(shared, "s7.jsonl");
  await worktree.retire(owned, s5);
  const reseeded = await worktree
    .capture(owned, s7, { seed: true })
    .then(() => true, () => false);
  check("a checkout the session adopted can start the next one", reseeded);

  // And the worker that served the session but did not draw the retirement. Its directory is the
  // one kind that may be emptied, and nothing was emptying it.
  asHost(root, "s5-worker");
  const servedAgain = await worktree.ensure(helper, s7).then(() => true, () => false);
  const carried = (await read(join(helper, "src.txt"))) === "edited\n";
  check("and a worker that served it can serve the next one", servedAgain && carried, {
    servedAgain,
    carried,
  });

  // What stops a session's directory growing for as long as the session lives. Every so often a
  // capture carries the whole tree and stands on nothing, and the ones before it go. What has to
  // survive that is the restore, from both distances: a host that stopped early and one that has
  // never seen the session.
  const compacted = join(shared, "s8.jsonl");
  const projectLong = join(root, "long", "project");
  await mkdir(projectLong, { recursive: true });
  asHost(root, "long");
  await writeFile(join(projectLong, "count.txt"), "0\n");
  await worktree.capture(projectLong, compacted, { seed: true });

  const projectLate = join(root, "late", "project");
  asHost(root, "late");
  await worktree.ensure(projectLate, compacted);

  asHost(root, "long");
  for (let i = 1; i <= 45; i++) {
    await writeFile(join(projectLong, "count.txt"), `${i}\n`);
    await worktree.capture(projectLong, compacted);
  }
  const bundles = (await readdir(`${compacted}.tree`)).filter((n) => n.endsWith(".bundle"));
  check("the bundle chain restarts rather than growing for ever", bundles.length < 20, bundles.length);

  asHost(root, "late");
  await worktree.ensure(projectLate, compacted);
  const late = await read(join(projectLate, "count.txt"));
  check("a host that stopped early still catches up afterwards", late === "45\n", late);

  const projectNew = join(root, "new", "project");
  asHost(root, "new");
  await worktree.ensure(projectNew, compacted);
  const fresh2 = await read(join(projectNew, "count.txt"));
  check("and one that never saw the session gets there too", fresh2 === "45\n", fresh2);

  // The lock that crosses hosts, as opposed to the host-local one that keeps two sessions off one
  // directory. Held from outside, which is what another machine looks like from here, a capture
  // waits for it rather than writing a second bundle over the same number. Nothing used to hold
  // this: the activities happened to take the session's own lock, so the rule was true for them and
  // not for the client that seeds a project.
  const otherHostHolds = withSessionLock(join(`${compacted}.tree`, "writers"), async () => {
    await new Promise((resolve) => setTimeout(resolve, 700));
    return Date.now();
  });
  await new Promise((resolve) => setTimeout(resolve, 50));
  asHost(root, "long");
  await writeFile(join(projectLong, "count.txt"), "46\n");
  await worktree.capture(projectLong, compacted);
  const captureFinished = Date.now();
  const releasedAt = await otherHostHolds;
  check("a capture waits for the lock that crosses hosts", captureFinished >= releasedAt, {
    captureFinished,
    releasedAt,
  });

  // A session nobody sent a project to. Every firing of a schedule is its own session and nothing
  // is running at firing time to send one, so the client sends it once and each firing copies it.
  const templateFile = join(shared, "schedule-nightly.jsonl");
  const projectTemplate = join(root, "template", "project");
  await mkdir(projectTemplate, { recursive: true });
  asHost(root, "template");
  await writeFile(join(projectTemplate, "todo.txt"), "review the merges\n");
  await worktree.capture(projectTemplate, templateFile, { seed: true });
  await worktree.unclaim(projectTemplate, templateFile);

  const firing = join(shared, "fired-1.jsonl");
  const took = await worktree.adopt(templateFile, firing);
  const projectFiring = join(root, "firing", "project");
  await mkdir(projectFiring, { recursive: true });
  asHost(root, "firing");
  await worktree.ensure(projectFiring, firing);
  const arrived = await read(join(projectFiring, "todo.txt"));
  check("a firing takes the project the schedule was given", took && arrived === "review the merges\n", {
    took,
    arrived,
  });
  const again = await worktree.adopt(templateFile, firing);
  check("and a re-driven activity does not copy it twice", again === false);

  // The directory the template came from is not a directory a session is working in, so the next
  // real session started there is not refused. Nothing ever retires a template.
  asHost(root, "template");
  const stillUsable = await worktree
    .capture(projectTemplate, join(shared, "after-template.jsonl"), { seed: true })
    .then(() => true, () => false);
  check("and the directory it was sent from is free afterwards", stillUsable);

  // The directories a worker is holding for sessions that finished elsewhere. Nothing asks for them
  // again, so nothing frees them: the lazy path only acts when another session wants that same
  // directory. A worker that served fifty sessions holds fifty until this runs.
  const sweptSession = join(shared, "s9.jsonl");
  const projectSwept = join(root, "swept", "project");
  await mkdir(projectSwept, { recursive: true });
  const projectSeed9 = join(root, "seed9", "project");
  await mkdir(projectSeed9, { recursive: true });
  asHost(root, "seed9");
  await writeFile(join(projectSeed9, "swept.txt"), "yes\n");
  await worktree.capture(projectSeed9, sweptSession, { seed: true });
  asHost(root, "swept");
  await worktree.ensure(projectSwept, sweptSession);
  asHost(root, "seed9");
  await worktree.retire(projectSeed9, sweptSession);

  asHost(root, "swept");
  const heldBefore = (await readdir(projectSwept)).length;
  const freed = await worktree.sweep();
  const heldAfter = (await readdir(projectSwept)).length;
  check(
    "a sweep hands back what a finished session left on this host",
    freed === 1 && heldBefore > 0 && heldAfter === 0,
    { freed, heldBefore, heldAfter },
  );

  asHost(root, "a");
  await worktree.forget(sessionFile, projectA);
  const gone = (await readdir(`${sessionFile}.tree`)).length === 0;
  check("forgetting a session drops what its tree cost", gone);

  // And hands the directory back. Otherwise a worker with one project directory serves exactly one
  // session for as long as it lives, which is not a fleet.
  const taken = await worktree
    .capture(projectA, other, { seed: true })
    .then(() => true, () => false);
  check("a finished session releases its directory", taken);

  // A directory nobody named is not sent when it is a home directory or has nothing to keep
  // secrets out of what ships.
  const bare = await mkdtemp(join(tmpdir(), "pi-bare-"));
  const ignoring = await mkdtemp(join(tmpdir(), "pi-ignoring-"));
  await writeFile(join(ignoring, ".gitignore"), "node_modules\n");
  const refusal = worktree.projectRefusal;
  check("a home directory is refused", (await refusal(homedir())) !== undefined);
  check("a directory with no repository or ignore file is refused", !!(await refusal(bare)));
  check("a directory with an ignore file is sent", (await refusal(ignoring)) === undefined);
  await rm(bare, { recursive: true, force: true });
  await rm(ignoring, { recursive: true, force: true });

  console.log(failures.length === 0 ? "\nworktree-check: OK" : `\nworktree-check: ${failures.length} failed`);
  process.exit(failures.length === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
