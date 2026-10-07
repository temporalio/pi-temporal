// Checks that a stale lock takeover cannot let two holders in. One contender reads a dead claim
// and stalls (via `withSessionLock`'s last argument), a second takes the next epoch, a third races
// both. Asserts one holder at a time, including at the synchronous write guard.
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

  // A dead holder's claim, old enough to count as stale.
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
    // Two holders whose `ownedNow()` both answer true is the failure.
    if (guards.filter((g) => g()).length > 1) bothGuarded = true;
    inside++;
    if (inside > 1) overlapped = true;
    await sleep(ms);
    inside--;
  };

  // Reads the dead claim, then stalls before it can take the next epoch.
  const slow = withSessionLock(file, hold(200), 30_000, 1_500);
  await sleep(200);
  // Takes the next epoch and still holds it when the slow one wakes.
  const quick = withSessionLock(file, hold(2_500), 30_000);
  await sleep(400);
  // Starts while the quick one holds the lock and the slow one is still stalled.
  const thirdStarted = Date.now();
  const third = withSessionLock(file, hold(200), 30_000);

  await Promise.all([slow, quick, third]);

  check("no two holders are inside at once", !overlapped);
  check("and no two answer the write guard at once", !bothGuarded);
  // Without this the check could pass because the third contender arrived late.
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
