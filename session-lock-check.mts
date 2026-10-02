// Checks the one-writer-at-a-time lock on a session file. Pi's session file is a tree, and two
// writers branch it rather than corrupt it, so this is what stops two attempts of the same
// activity from doing that. No Temporal server and no model key.
//
// Usage: npx tsx session-lock-check.mts

import { mkdir, mkdtemp, readdir, readFile, rm, stat, utimes, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { withSessionLock } from "./src/session-lock.js";

const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// What another holder looks like on disk: a claim file named for its epoch, holding its token. The
// lock is a directory of these and the newest one owns it, so a fixture that wants to be somebody
// else takes a higher epoch rather than overwriting a path.
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

// Older than the refresh interval, so a tick that did touch the file is visible as a fresh mtime.
const STALE_ENOUGH = 120_000;

// The lock refreshes every 3 seconds; waiting past one tick is how a refresh is observed.
const REFRESH_TICK = 3_000;

async function main() {
  const dir = await mkdtemp(join(tmpdir(), "pi-lock-"));
  const file = join(dir, "session.jsonl");

  // The case the lock exists for: two writers of one session file must not overlap.
  const order: string[] = [];
  let inside = 0;
  let overlapped = false;
  const writer = (name: string) =>
    withSessionLock(file, async () => {
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

  // The lock is not a leak: it has to be gone afterwards or the session is wedged for good.
  check("the lock is released", (await claimsHeld(file)).length === 0, await claimsHeld(file));

  // A holder that died leaves the file behind. Reclaiming it on age is what stops one crashed
  // worker from taking the session with it.
  await claimAs(file, "someone-else", 1, 120_000);
  let reclaimed = false;
  await withSessionLock(file, async () => {
    reclaimed = true;
  });
  check("a dead holder's lock is reclaimed", reclaimed);

  // A live holder's is not, and the caller is told rather than left to write anyway.
  let refused = false;
  await withSessionLock(file, async () => {
    await withSessionLock(file, async () => {}, 500).catch(() => {
      refused = true;
    });
  });
  check("a live holder's lock is not stolen", refused);

  // The interleaving the rename-based reclaim exists for, and the one nothing here could reach:
  // a contender stalled between measuring a stale lock's age and reclaiming it, for longer than
  // another takes to reclaim it and take it. The stall is the only thing injected, through
  // `withSessionLock`'s last argument. What is asserted is the real outcome: the slow one must not
  // move a lock the quick one is holding and then take it.
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
  const slow = withSessionLock(late, () => enterLate(300), 20_000, 1_500);
  await sleep(200);
  // Finds the same stale lock, reclaims it, and is still holding it when the slow one wakes up.
  const quick = withSessionLock(late, () => enterLate(2_000), 20_000);
  await Promise.all([slow, quick]);
  check("a reclaim that lost the race does not take a live lock", !overlappedLate);

  // Mutual exclusion when several contenders find one stale lock together, without the stall.
  let bothIn = false;
  for (let round = 0; round < 20 && !bothIn; round++) {
    const raced = join(dir, `raced-${round}.jsonl`);
    await claimAs(raced, "dead", 1, STALE_ENOUGH);

    let inside = 0;
    await Promise.all(
      [0, 1].map(() =>
        withSessionLock(raced, async () => {
          inside++;
          if (inside > 1) bothIn = true;
          await new Promise((resolve) => setTimeout(resolve, 20));
          inside--;
        }).catch(() => undefined),
      ),
    );
  }
  check("contenders finding one stale lock together do not overlap", !bothIn);

  // The late reclaim cannot be forced from here, so pin what protects against it instead. A holder
  // whose lock was taken has to be able to find that out before it writes, which is what every
  // writing activity asks on its way to the write.
  const fenced = join(dir, "fenced.jsonl");
  let sawOwned: boolean | undefined;
  let sawLost: boolean | undefined;
  await withSessionLock(fenced, async (owned) => {
    sawOwned = await owned();
    // Somebody else reclaims and takes it. This is the state a slow holder wakes up in.
    await claimAs(fenced, "someone-else", 2);
    sawLost = await owned();
  });
  check("a holder can tell its lock is still its own", sawOwned === true);
  check("and can tell when it is not", sawLost === false);

  // The same question, asked the way Pi's own append path has to ask it. That path is synchronous
  // all the way down, so the one place that is always immediately before a write cannot await a
  // read of the lock file. This answers from the refresher's last tick instead, which is why the
  // wait below is a tick and not nothing.
  const sync = join(dir, "sync.jsonl");
  let syncHeld: boolean | undefined;
  let syncLost: boolean | undefined;
  await withSessionLock(sync, async (_owned, ownedNow) => {
    syncHeld = ownedNow();
    await claimAs(sync, "someone-else", 2);
    await sleep(REFRESH_TICK + 500);
    syncLost = ownedNow();
  });
  check("a writer that cannot await can still tell it holds the lock", syncHeld === true);
  // A holder that is alive and ticking, which is the case a refresher can see. The other case, a
  // holder whose event loop stopped, is what `stall-check.mts` covers: it needs two processes,
  // because a stopped loop cannot run its own contender.
  check(
    "and a live one notices within a refresh when it does not",
    syncLost === false,
    { syncLost },
  );

  // The first activity of a session takes the lock before anything has created the directory the
  // session file lives in, so the lock has to make it rather than sit there failing.
  const fresh = join(dir, "not-yet", "session.jsonl");
  const started = Date.now();
  let made = false;
  await withSessionLock(fresh, async () => {
    made = true;
  }, 3000).catch(() => {});
  check("a session whose directory does not exist yet can be locked", made);
  check("and it does not wait to find that out", Date.now() - started < 1000, Date.now() - started);

  // A holder that was reclaimed while it was blocked must stop touching the lock. Otherwise it
  // keeps the next holder's lock alive long after that one is gone, and nobody can take it.
  const contested = join(dir, "contested.jsonl");
  let released = false;
  await withSessionLock(contested, async () => {
    // Reclaim it out from under the holder, the way a stalled holder is reclaimed on age.
    const taken = await claimAs(contested, "someone-else", 2);
    const stolen = new Date(Date.now() - STALE_ENOUGH);
    await utimes(taken, stolen, stolen);
    // Long enough for at least one refresh tick to notice.
    await sleep(4000);
    const info = await stat(claimPath(contested, 1)).catch(() => ({ mtimeMs: 0 }));
    released = Date.now() - info.mtimeMs > 3000;
  });
  check("a holder that lost the lock stops refreshing it", released);

  // One read that fails is not the lock being taken away. Stopping on it lets the lock age out
  // from under a holder that is still writing, which is the case the whole thing exists for, and
  // a transient read error on shared storage is likelier than a minute-long stall.
  const flaky = join(dir, "flaky.jsonl");
  let kept = false;
  await withSessionLock(flaky, async () => {
    const lock = claimPath(flaky, 1);
    const saved = await readFile(lock, "utf8");
    // Unreadable for one tick, then fine again. Overwritten in place rather than removed and
    // rewritten: absent means reclaimed, which the refresher is right to stop on, and a tick
    // landing in the gap between the two made this fail for the wrong reason.
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
