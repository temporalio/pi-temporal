// Checks the one-writer-at-a-time lock on a session file. Pi's session file is a tree, and two
// writers branch it rather than corrupt it, so this is what stops two attempts of the same
// activity from doing that. No Temporal server and no model key.
//
// Usage: npx tsx session-lock-check.mts

import { mkdtemp, readdir, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { withSessionLock } from "./src/session-lock.js";

const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

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
  const stale = new Date(Date.now() - 60_000);
  const { utimes } = await import("node:fs/promises");
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

  const bad = failures.length;
  console.log(bad === 0 ? "session-lock-check: OK" : `session-lock-check: ${bad} failed`);
  process.exit(bad === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
