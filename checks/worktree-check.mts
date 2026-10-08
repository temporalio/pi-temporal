// Checks how `worktree` ships a project between hosts, faked as separate data and project dirs
// over one shared session dir. Asserts files and deletions round-trip, stale or foreign hosts
// cannot publish or adopt, unknown checkouts are never emptied, and old bundles compact.
//
// Usage: npx tsx checks/worktree-check.mts

import { execFileSync } from "node:child_process";
import { mkdir, mkdtemp, readdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";
import * as worktree from "../src/tree/worktree.js";
import { withLease } from "../src/tree/lease.js";

const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

// A host is just its data directory, which holds its shadow repo and held-note.
const asHost = (root: string, name: string) => {
  process.env.PI_TEMPORAL_DATA = join(root, name, "data");
};

const read = (path: string) => readFile(path, "utf8").catch(() => "");

// The held-note path is hashed from the project directory, so search for it.
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

  // A host that has never seen this project, with an empty directory.
  asHost(root, "b");
  await worktree.ensure(projectB, sessionFile);
  check("a second host gets the files", (await read(join(projectB, "note.txt"))) === "one\n");
  check("nested files come too", (await read(join(projectB, "sub", "deep.txt"))) === "deep\n");

  // And back, including a deletion.
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

  // A directory holding files this host did not put there is someone's checkout.
  const projectC = join(root, "c", "project");
  await mkdir(projectC, { recursive: true });
  await writeFile(join(projectC, "mine.txt"), "do not touch\n");
  asHost(root, "c");
  const refused = await worktree.ensure(projectC, sessionFile).then(() => false, () => true);
  const left = await readdir(projectC);
  check("a working copy is refused, not overwritten", refused && left.join() === "mine.txt", left);

  // Host A's directory holds s1's work, so s2 can neither restore into it nor capture from it.
  asHost(root, "a");
  const other = join(root, "sessions", "s2.jsonl");
  const mine = await read(join(projectA, "note.txt"));
  const refusedCapture = await worktree
    .capture(projectA, other, { seed: true })
    .then(() => false, () => true);
  const refusedEnsure = await worktree.ensure(projectA, other).then(() => false, () => true);
  const untouched = (await read(join(projectA, "note.txt"))) === mine;
  check("another session cannot adopt a directory", refusedCapture && refusedEnsure && untouched, {
    refusedCapture,
    refusedEnsure,
    untouched,
  });

  // Move the tip first, or `ensure` returns early at "already at the tip".
  asHost(root, "b");
  await writeFile(join(projectB, "note.txt"), "three\n");
  await worktree.capture(projectB, sessionFile);

  // Unshipped work from a crashed tool conflicts with the tip. It is set aside and the host moves
  // to the tip.
  asHost(root, "a");
  await writeFile(join(projectA, "unshipped.txt"), "not yours\n");
  await worktree.ensure(projectA, sessionFile);
  const moved = await read(join(projectA, "note.txt"));
  check("a host behind the tip is brought to it", moved === "three\n", moved);
  const salvaged = await readdir(join(`${sessionFile}.tree`, "salvage")).catch(
    () => [] as string[],
  );
  check(
    "work a crash left behind is kept, not dropped",
    salvaged.some((n) => n.endsWith(".bundle")),
    salvaged,
  );

  // Re-read `note.txt` on the other host to catch a lost update.
  asHost(root, "b");
  await worktree.ensure(projectB, sessionFile);
  const held = await read(join(projectB, "note.txt"));
  check("and nothing the other host shipped is reverted", held === "three\n", held);

  // A host the step moved away from may still try to publish. It must not revert the tip.
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
  check(
    "so the host that carried on keeps its work",
    (await read(join(projectB, "note.txt"))) === "four\n",
  );
  asHost(root, "a");
  await worktree.setAside(projectA, sessionFile).catch(() => undefined);
  const kept = await readdir(join(`${sessionFile}.tree`, "salvage")).catch(() => [] as string[]);
  check("and the work it stranded is set aside", kept.length > 0, kept);

  // A crash after renaming a bundle but before claiming its tip leaves a bundle nothing names.
  // Every host computes the same next number, so it must not wedge the session.
  const orphaned = join(shared, "s6.jsonl");
  const projectF = join(root, "f", "project");
  const projectG = join(root, "g", "project");
  await mkdir(projectF, { recursive: true });
  await mkdir(projectG, { recursive: true });
  asHost(root, "f");
  await writeFile(join(projectF, "one.txt"), "first\n");
  await worktree.capture(projectF, orphaned, { seed: true });
  await writeFile(join(`${orphaned}.tree`, "00000002-dead.bundle"), "not a bundle");
  await writeFile(join(`${orphaned}.tree`, "00000002.bundle"), "not a bundle either");
  await writeFile(join(projectF, "two.txt"), "second\n");
  const carriedOn = await worktree.capture(projectF, orphaned).then(() => true, () => false);
  check("a bundle nothing names does not wedge the session", carriedOn);
  asHost(root, "g");
  const restored = await worktree.ensure(projectG, orphaned).then(
    async () => await read(join(projectG, "two.txt")),
    (err: unknown) => String(err),
  );
  check("and a new host restores past it", restored === "second\n", restored);

  // A store from before tips were one file each: a single `tip.json` and bundles named by number.
  const legacy = join(shared, "legacy.jsonl");
  const projectH = join(root, "h", "project");
  const projectI = join(root, "i", "project");
  await mkdir(projectH, { recursive: true });
  await mkdir(projectI, { recursive: true });
  asHost(root, "h");
  await writeFile(join(projectH, "one.txt"), "first\n");
  await worktree.capture(projectH, legacy, { seed: true });
  const store = `${legacy}.tree`;
  const { bundle, ...first } = JSON.parse(await read(join(store, "tips", "00000001.json")));
  await rename(join(store, bundle), join(store, "00000001.bundle"));
  await writeFile(join(store, "tip.json"), JSON.stringify(first));
  await rm(join(store, "tips"), { recursive: true });
  await writeFile(join(projectH, "two.txt"), "second\n");
  await worktree.capture(projectH, legacy);
  asHost(root, "i");
  const continued = await worktree.ensure(projectI, legacy).then(
    async () => (await read(join(projectI, "one.txt"))) + (await read(join(projectI, "two.txt"))),
    (err: unknown) => String(err),
  );
  check("an older store carries on in the new layout", continued === "first\nsecond\n", continued);

  // Only a client seed may establish the project. Otherwise a worker's empty directory could become
  // the project and the real one would be refused forever.
  const fresh = join(shared, "s3.jsonl");
  const projectD = join(root, "d", "project");
  const projectE = join(root, "e", "project");
  await mkdir(projectD, { recursive: true });
  await mkdir(projectE, { recursive: true });
  await writeFile(join(projectE, "real.txt"), "the project\n");
  asHost(root, "d");
  await worktree.capture(projectD, fresh);
  const seeded = (await readdir(`${fresh}.tree`).catch(() => [] as string[]))
    .filter((name) => name !== "writers.lock");
  check("a tool call cannot establish the project", seeded.length === 0, seeded);
  const unestablished = await worktree.ensure(projectD, fresh).then(() => false, () => true);
  check("and no activity runs before a client sends it", unestablished);
  asHost(root, "e");
  await worktree.capture(projectE, fresh, { seed: true });
  asHost(root, "d");
  await worktree.ensure(projectD, fresh);
  const real = await read(join(projectD, "real.txt"));
  check("the host holding the files defines it", real === "the project\n", real);

  // A restore must write `seq` to the note so the next restore is incremental.
  const noted = await heldNote(root, "d");
  check("a restore records how far it got", typeof noted?.seq === "number", noted);

  // Releasing a directory lets the next session use it.
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

  // A seeding host's git checkout holds ignored files no bundle carries. Emptying it loses data.
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
  check(
    "a directory this host did not build is never emptied",
    refusedOwned && intact && ignoredKept,
    {
      refusedOwned,
      intact,
      ignoredKept,
    },
  );

  // A restore must not make that checkout look like one this host built.
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

  // After retirement, the seeding checkout can start the next session.
  const s7 = join(shared, "s7.jsonl");
  await worktree.retire(owned, s5);
  const reseeded = await worktree
    .capture(owned, s7, { seed: true })
    .then(() => true, () => false);
  check("a checkout the session adopted can start the next one", reseeded);

  // And a worker that served the retired session can serve the next one.
  asHost(root, "s5-worker");
  const servedAgain = await worktree.ensure(helper, s7).then(() => true, () => false);
  const carried = (await read(join(helper, "src.txt"))) === "edited\n";
  check("and a worker that served it can serve the next one", servedAgain && carried, {
    servedAgain,
    carried,
  });

  // Periodically a capture ships the full tree and older bundles go. Restores must still work for
  // a host that fell behind and for a brand-new one.
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
  check(
    "the bundle chain restarts rather than growing for ever",
    bundles.length < 20,
    bundles.length,
  );

  asHost(root, "late");
  await worktree.ensure(projectLate, compacted);
  const late = await read(join(projectLate, "count.txt"));
  check("a host that stopped early still catches up afterwards", late === "45\n", late);

  const projectNew = join(root, "new", "project");
  asHost(root, "new");
  await worktree.ensure(projectNew, compacted);
  const fresh2 = await read(join(projectNew, "count.txt"));
  check("and one that never saw the session gets there too", fresh2 === "45\n", fresh2);

  // The cross-host `writers` lock. Held here as another machine would, a capture must wait for it.
  const otherHostHolds = withLease(join(`${compacted}.tree`, "writers"), async () => {
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

  // Each schedule firing is a new session. It copies the project from the schedule's template.
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
  check(
    "a firing takes the project the schedule was given",
    took && arrived === "review the merges\n",
    {
      took,
      arrived,
    },
  );
  const again = await worktree.adopt(templateFile, firing);
  check("and a re-driven activity does not copy it twice", again === false);

  // Templates are never retired, so their source directory must stay free.
  asHost(root, "template");
  const stillUsable = await worktree
    .capture(projectTemplate, join(shared, "after-template.jsonl"), { seed: true })
    .then(() => true, () => false);
  check("and the directory it was sent from is free afterwards", stillUsable);

  // `sweep` frees directories held for sessions that finished elsewhere. Nothing else would.
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
  const gone = (await readdir(`${sessionFile}.tree`))
    .every((name) => name === "writers.lock");
  check("forgetting a session drops what its tree cost", gone);

  const taken = await worktree
    .capture(projectA, other, { seed: true })
    .then(() => true, () => false);
  check("a finished session releases its directory", taken);

  // A file ignored after it was captured stops shipping, though the shadow index still tracks it.
  const ignoredLater = join(shared, "s10.jsonl");
  const projectIgnA = join(root, "ign-a", "project");
  const projectIgnB = join(root, "ign-b", "project");
  await mkdir(projectIgnA, { recursive: true });
  asHost(root, "ign-a");
  await writeFile(join(projectIgnA, ".gitignore"), "node_modules\n");
  await writeFile(join(projectIgnA, ".env"), "SECRET=old\n");
  await worktree.capture(projectIgnA, ignoredLater, { seed: true });
  asHost(root, "ign-b");
  await worktree.ensure(projectIgnB, ignoredLater);
  asHost(root, "ign-a");
  await writeFile(join(projectIgnA, ".gitignore"), "node_modules\n.env\n");
  await writeFile(join(projectIgnA, ".env"), "SECRET=new\n");
  await worktree.capture(projectIgnA, ignoredLater);
  asHost(root, "ign-b");
  await worktree.ensure(projectIgnB, ignoredLater);
  const leaked = await read(join(projectIgnB, ".env"));
  const rules = await read(join(projectIgnB, ".gitignore"));
  check(
    "a file ignored after it was captured stops shipping",
    leaked !== "SECRET=new\n" && rules.includes(".env"),
    { leaked, rules },
  );

  // A default project dir is refused if it is home or lacks a repo or ignore file.
  const bare = await mkdtemp(join(tmpdir(), "pi-bare-"));
  const ignoring = await mkdtemp(join(tmpdir(), "pi-ignoring-"));
  await writeFile(join(ignoring, ".gitignore"), "node_modules\n");
  const refusal = worktree.projectRefusal;
  check("a home directory is refused", (await refusal(homedir())) !== undefined);
  check("a directory with no repository or ignore file is refused", !!(await refusal(bare)));
  check("a directory with an ignore file is sent", (await refusal(ignoring)) === undefined);
  // A checkout alone has no ignore rules this capture can use. Its own `.git/info/exclude` does.
  const checkout = await mkdtemp(join(tmpdir(), "pi-checkout-"));
  await mkdir(join(checkout, ".git", "info"), { recursive: true });
  check("a checkout with no ignore rules is refused", !!(await refusal(checkout)));
  await writeFile(join(checkout, ".git", "info", "exclude"), "# local\n.env\n");
  check("a checkout with local ignore rules is sent", (await refusal(checkout)) === undefined);
  await writeFile(join(checkout, ".env"), "SECRET=local\n");
  await writeFile(join(checkout, "code.txt"), "code\n");
  asHost(root, "checkout");
  const excluded = join(shared, "excluded.jsonl");
  await worktree.capture(checkout, excluded, { seed: true });
  const reader = join(root, "checkout-reader", "project");
  await mkdir(reader, { recursive: true });
  asHost(root, "checkout-reader");
  await worktree.ensure(reader, excluded);
  check(
    "and what those rules exclude doesn't ship",
    (await read(join(reader, "code.txt"))) === "code\n" &&
      (await read(join(reader, ".env"))) === "",
  );
  await rm(checkout, { recursive: true, force: true });
  await rm(bare, { recursive: true, force: true });
  await rm(ignoring, { recursive: true, force: true });

  console.log(
    failures.length === 0
      ? "\nworktree-check: OK"
      : `\nworktree-check: ${failures.length} failed`,
  );
  process.exit(failures.length === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
