// Checks the one-writer-at-a-time lock on a session file. Pi's session file is a tree, and two
// writers branch it rather than corrupt it, so this is what stops two attempts of the same
// activity from doing that. No Temporal server and no model key.
//
// Usage: npx tsx session-lock-check.mts

import { mkdtemp, readdir, readFile, rm, stat, utimes, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { withSessionLock } from "./src/session-lock.js";

const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

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
  check("the lock is released", !(await readdir(dir)).includes("session.jsonl.lock"));

  // A holder that died leaves the file behind. Reclaiming it on age is what stops one crashed
  // worker from taking the session with it.
  await writeFile(`${file}.lock`, JSON.stringify({ token: "someone-else" }), "utf8");
  const stale = new Date(Date.now() - 120_000);
  await utimes(`${file}.lock`, stale, stale);
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

  // Mutual exclusion when several contenders find one stale lock together. It does NOT cover the
  // interleaving the rename-based reclaim exists for: that needs a contender stalled between
  // reading the lock's age and reclaiming it, for longer than another takes to reclaim and
  // acquire. Nothing here can hold the event loop at that point, so this passes either way.
  let bothIn = false;
  for (let round = 0; round < 20 && !bothIn; round++) {
    const raced = join(dir, `raced-${round}.jsonl`);
    await writeFile(`${raced}.lock`, JSON.stringify({ token: "dead" }), "utf8");
    const old = new Date(Date.now() - STALE_ENOUGH);
    await utimes(`${raced}.lock`, old, old);

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
    await rm(`${fenced}.lock`, { force: true });
    await writeFile(`${fenced}.lock`, JSON.stringify({ token: "someone-else" }), "utf8");
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
    await rm(`${sync}.lock`, { force: true });
    await writeFile(`${sync}.lock`, JSON.stringify({ token: "someone-else" }), "utf8");
    await sleep(REFRESH_TICK + 500);
    syncLost = ownedNow();
  });
  check("a writer that cannot await can still tell it holds the lock", syncHeld === true);
  check("and notices within a refresh when it does not", syncLost === false, { syncLost });

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
    await rm(`${contested}.lock`, { force: true });
    await writeFile(`${contested}.lock`, JSON.stringify({ token: "someone-else" }), "utf8");
    const stolen = new Date(Date.now() - STALE_ENOUGH);
    await utimes(`${contested}.lock`, stolen, stolen);
    // Long enough for at least one refresh tick to notice.
    await sleep(4000);
    const info = await stat(`${contested}.lock`);
    released = Date.now() - info.mtimeMs > 3000;
  });
  check("a holder that lost the lock stops refreshing it", released);

  // One read that fails is not the lock being taken away. Stopping on it lets the lock age out
  // from under a holder that is still writing, which is the case the whole thing exists for, and
  // a transient read error on shared storage is likelier than a minute-long stall.
  const flaky = join(dir, "flaky.jsonl");
  let kept = false;
  await withSessionLock(flaky, async () => {
    const lock = `${flaky}.lock`;
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
