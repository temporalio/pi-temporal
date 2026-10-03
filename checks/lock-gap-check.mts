// The takeover race a fifth review reproduced, against the protocol that replaced the one it
// broke. The story is theirs: a contender measures a stale lock and stalls; another takes the lock
// and holds it; the slow one wakes up and acts on what it measured. Under the old shape it moved
// the live holder's lock out of the way, which left the path free for a moment, and a third
// contender created one there. Two holders then passed the synchronous guard at the same time.
//
// Nothing here moves anything now. Taking over means creating the next epoch with an exclusive
// create, so a takeover decided from a stale reading fails outright: the epoch it wants is already
// taken. What is injected is the stall, through `withSessionLock`'s last argument. What is asserted
// is the outcome the review asserted, one holder at a time, and that the third contender was really
// racing rather than arriving after everything had settled.
//
// Usage: npx tsx checks/lock-gap-check.mts

import { mkdir, mkdtemp, writeFile, utimes } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { withSessionLock } from "../src/session-lock.js";

const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function main() {
  const root = await mkdtemp(join(tmpdir(), "pi-lock-gap-"));
  const file = join(root, "session.jsonl");

  // A holder that died: one claim, old enough that its age says nobody is refreshing it.
  await mkdir(`${file}.lock`, { recursive: true });
  const dead = join(`${file}.lock`, "00000001");
  await writeFile(dead, JSON.stringify({ token: "dead" }), "utf8");
  const cold = new Date(Date.now() - 120_000);
  await utimes(dead, cold, cold);

  let inside = 0;
  let overlapped = false;
  const guards: Array<() => boolean> = [];
  let bothGuarded = false;
  const hold = (ms: number) => async (_owned: () => Promise<boolean>, ownedNow: () => boolean) => {
    guards.push(ownedNow);
    // What the review measured: two live holders both answering the question a writer asks on its
    // way to a write. One `true` is the lock working; two at once is the failure.
    if (guards.filter((g) => g()).length > 1) bothGuarded = true;
    inside++;
    if (inside > 1) overlapped = true;
    await sleep(ms);
    inside--;
  };

  // Reads the dead claim, then stalls before it can take the next epoch.
  const slow = withSessionLock(file, hold(200), 30_000, 1_500);
  await sleep(200);
  // Finds the same dead claim, takes the next epoch, and is still holding it when the slow one
  // wakes up and tries to act on what it measured.
  const quick = withSessionLock(file, hold(2_500), 30_000);
  await sleep(400);
  // The third contender is the one that used to get in through the gap. It starts while the quick
  // one holds the lock and while the slow one is still stalled.
  const thirdStarted = Date.now();
  const third = withSessionLock(file, hold(200), 30_000);

  await Promise.all([slow, quick, third]);

  check("no two holders are inside at once", !overlapped);
  check("and no two answer the write guard at once", !bothGuarded);
  // Without this the check could pass by the third contender simply arriving late.
  check("the third contender really did race", Date.now() - thirdStarted > 2_000, {
    waited: Date.now() - thirdStarted,
  });
  check("every contender eventually held it", guards.length === 3, guards.length);

  console.log(
    failures.length === 0
      ? "lock-gap-check: OK"
      : `lock-gap-check: ${failures.length} failed`,
  );
  process.exit(failures.length === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
