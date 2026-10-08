// Checks that a lock holder whose event loop stopped can tell it lost the lock. The holder blocks
// in `Atomics.wait` (like SIGSTOP or a paused container) past `STALE_MS`, a second process
// reclaims the lock, and both `owned()` and `ownedNow()` must then answer false.
//
// Usage: npx tsx checks/stall-check.mts

import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { withLease } from "../src/tree/lease.js";

const execFileAsync = promisify(execFile);
const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

// STALE_MS is 60s, so the stall has to outlast it and the contender has to be waiting.
const STALL_MS = 66_000;

const role = process.argv[2];
const file = process.argv[3];

if (role === "holder") {
  await withLease(file!, async (owned, ownedNow) => {
    await writeFile(`${file}.holding`, "");
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, STALL_MS);
    // The write guard's question for a write that lands as soon as the loop resumes.
    console.log(JSON.stringify({ ownedNow: ownedNow(), owned: await owned() }));
  });
} else if (role === "contender") {
  const { access } = await import("node:fs/promises");
  for (;;) {
    try {
      await access(`${file}.holding`);
      break;
    } catch {
      await new Promise((r) => setTimeout(r, 100));
    }
  }
  await withLease(file!, async () => new Promise((r) => setTimeout(r, 20_000)), 120_000);
} else {
  const dir = await mkdtemp(join(tmpdir(), "pi-stall-"));
  const session = join(dir, "s.jsonl");
  const self = fileURLToPath(import.meta.url);
  const run = (which: string) => execFileAsync("npx", ["tsx", self, which, session]);
  const contender = run("contender");
  const holder = await run("holder");
  await contender.catch(() => undefined);

  const answers = JSON.parse(holder.stdout.trim().split("\n").pop()!) as {
    ownedNow: boolean;
    owned: boolean;
  };
  check("an awaited check sees a lock taken during a stall", answers.owned === false, answers);
  // `ownedNow` must not answer from a flag that a stopped event loop could not have updated.
  check("and so does the synchronous one", answers.ownedNow === false, answers);

  console.log(
    failures.length === 0
      ? "\nstall-check: OK"
      : `\nstall-check: ${failures.length} failed`,
  );
  process.exit(failures.length === 0 ? 0 : 1);
}
