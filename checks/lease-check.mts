// Checks the one-writer lease on a tree store or project directory, which stops two activity
// attempts from writing it at once. Asserts no overlap (also under stale-lock races), dead holders
// are reclaimed, live ones are not, and a holder can tell, sync or async, when it lost the lock.
// No server needed.
//
// Usage: npx tsx checks/lease-check.mts

import { mkdir, mkdtemp, readdir, readFile, rm, stat, utimes, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { withLease } from "../src/tree/lease.js";

const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// The lock is a directory of claim files named by epoch, and the newest owns it. To pose as
// another holder, write a higher epoch.
const claimPath = (file: string, epoch = 1) =>
  join(`${file}.lock`, String(epoch).padStart(8, "0"));
async function claimAs(file: string, token: string, epoch = 1, ageMs = 0) {
  await mkdir(`${file}.lock`, { recursive: true });
  const path = claimPath(file, epoch);
  await writeFile(path, JSON.stringify({ token }), "utf8");
  if (ageMs > 0) {
    const when = new Date(Date.now() - ageMs);
    await utimes(path, when, when);
  }
  return path;
}
const claimsHeld = async (file: string) =>
  (await readdir(`${file}.lock`).catch(() => [] as string[])).filter((n) => /^\d+$/.test(n));

// Old enough that a refresh shows up as a fresh mtime.
const STALE_ENOUGH = 120_000;

// The lock's refresh interval.
const REFRESH_TICK = 3_000;

async function main() {
  const dir = await mkdtemp(join(tmpdir(), "pi-lock-"));
  const file = join(dir, "session.jsonl");

  const order: string[] = [];
  let inside = 0;
  let overlapped = false;
  const writer = (name: string) =>
    withLease(file, async () => {
      inside++;
      if (inside > 1) overlapped = true;
      order.push(`${name}:in`);
      await sleep(60);
      order.push(`${name}:out`);
      inside--;
    });

  await Promise.all([writer("a"), writer("b")]);
  check("two writers do not overlap", !overlapped, order);
  check("each one ran", order.length === 4, order);

  // A leaked lock would wedge the session.
  const expiredClaims = await claimsHeld(file);
  check("release retains one expired epoch", expiredClaims.length === 1, expiredClaims);
  check(
    "release permits the next holder without waiting",
    (await stat(join(`${file}.lock`, expiredClaims[0]!))).mtimeMs === 0,
  );

  // A dead holder's claim is reclaimed by age.
  await claimAs(file, "someone-else", 1, 120_000);
  let reclaimed = false;
  await withLease(file, async () => {
    reclaimed = true;
  });
  check("a dead holder's lock is reclaimed", reclaimed);

  // A live holder's is not, and the caller gets an error.
  let refused = false;
  await withLease(file, async () => {
    await withLease(file, async () => {}, 500).catch(() => {
      refused = true;
    });
  });
  check("a live holder's lock is not stolen", refused);

  // A contender stalls (via `withLease`'s last argument) between reading a stale claim and
  // reclaiming it, while another reclaims and holds it. The slow one must not get in.
  const late = join(dir, "late.jsonl");
  await claimAs(late, "dead", 1, STALE_ENOUGH);

  let insideLate = 0;
  let overlappedLate = false;
  const enterLate = async (hold: number) => {
    insideLate++;
    if (insideLate > 1) overlappedLate = true;
    await sleep(hold);
    insideLate--;
  };
  // Reads the stale claim, then stalls before it can take the next epoch.
  const slow = withLease(late, () => enterLate(300), 20_000, 1_500);
  await sleep(200);
  // Reclaims the same stale lock and still holds it when the slow one wakes.
  const quick = withLease(late, () => enterLate(2_000), 20_000);
  await Promise.all([slow, quick]);
  check("a reclaim that lost the race does not take a live lock", !overlappedLate);

  // Same, without the stall.
  let bothIn = false;
  for (let round = 0; round < 20 && !bothIn; round++) {
    const raced = join(dir, `raced-${round}.jsonl`);
    await claimAs(raced, "dead", 1, STALE_ENOUGH);

    let inside = 0;
    await Promise.all(
      [0, 1].map(() =>
        withLease(raced, async () => {
          inside++;
          if (inside > 1) bothIn = true;
          await new Promise((resolve) => setTimeout(resolve, 20));
          inside--;
        }).catch(() => undefined),
      ),
    );
  }
  check("contenders finding one stale lock together do not overlap", !bothIn);

  // A holder whose lock was taken must find out before it writes.
  const fenced = join(dir, "fenced.jsonl");
  let sawOwned: boolean | undefined;
  let sawLost: boolean | undefined;
  await withLease(fenced, async (owned) => {
    sawOwned = await owned();
    // Another holder takes it, as a slow holder would find on waking.
    await claimAs(fenced, "someone-else", 2);
    sawLost = await owned();
  });
  check("a holder can tell its lock is still its own", sawOwned === true);
  check("and can tell when it is not", sawLost === false);

  // Pi's append path is synchronous, so `ownedNow()` answers from the refresher's last tick. Hence
  // the wait of one tick below.
  const sync = join(dir, "sync.jsonl");
  let syncHeld: boolean | undefined;
  let syncLost: boolean | undefined;
  await withLease(sync, async (_owned, ownedNow) => {
    syncHeld = ownedNow();
    await claimAs(sync, "someone-else", 2);
    await sleep(REFRESH_TICK + 500);
    syncLost = ownedNow();
  });
  check("a writer that cannot await can still tell it holds the lock", syncHeld === true);
  // A holder with a stopped event loop is covered by `stall-check.mts`.
  check(
    "and a live one notices within a refresh when it does not",
    syncLost === false,
    { syncLost },
  );

  // The first activity locks before the session directory exists, so the lock must create it.
  const fresh = join(dir, "not-yet", "session.jsonl");
  const started = Date.now();
  let made = false;
  await withLease(fresh, async () => {
    made = true;
  }, 3000).catch(() => {});
  check("a session whose directory does not exist yet can be locked", made);
  check("and it does not wait to find that out", Date.now() - started < 1000, Date.now() - started);

  // A reclaimed holder must stop refreshing, or it keeps a dead successor's lock alive.
  const contested = join(dir, "contested.jsonl");
  let released = false;
  await withLease(contested, async () => {
    const taken = await claimAs(contested, "someone-else", 2);
    const stolen = new Date(Date.now() - STALE_ENOUGH);
    await utimes(taken, stolen, stolen);
    await sleep(4000);
    const info = await stat(claimPath(contested, 1)).catch(() => ({ mtimeMs: 0 }));
    released = Date.now() - info.mtimeMs > 3000;
  });
  check("a holder that lost the lock stops refreshing it", released);

  // One failed read is not a lost lock. Stopping on it would let a live holder's lock age out.
  const flaky = join(dir, "flaky.jsonl");
  let kept = false;
  await withLease(flaky, async () => {
    const lock = claimPath(flaky, 1);
    const saved = await readFile(lock, "utf8");
    // Overwrite in place for one tick. Removing it would mean reclaimed, which is a real stop.
    await writeFile(lock, "{ not json", "utf8");
    await sleep(REFRESH_TICK + 500);
    await writeFile(lock, saved, "utf8");
    const before = (await stat(lock)).mtimeMs;
    await sleep(REFRESH_TICK + 500);
    kept = (await stat(lock)).mtimeMs > before;
  });
  check("a refresher survives one bad read", kept);

  const bad = failures.length;
  console.log(bad === 0 ? "session-lock-check: OK" : `session-lock-check: ${bad} failed`);
  process.exit(bad === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
