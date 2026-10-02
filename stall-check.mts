// Whether a holder whose event loop stopped can tell it lost the lock, which is the only case the
// synchronous guard exists for. It needs two processes: the holder has to stop its own loop, and a
// stopped loop cannot run a contender.
//
// `Atomics.wait` on the main thread is what a paused container, a closed lid or SIGSTOP look like
// from inside the process. No timer fires and no I/O completes, so the lock's mtime ages past
// STALE_MS and the contender reclaims it.
//
// Usage: npx tsx stall-check.mts

import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { withSessionLock } from "./src/session-lock.js";

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
  await withSessionLock(file!, async (owned, ownedNow) => {
    await writeFile(`${file}.holding`, "");
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, STALL_MS);
    // What the session's write guard is asked, for a write landing the moment the loop resumes.
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
  await withSessionLock(file!, async () => new Promise((r) => setTimeout(r, 20_000)), 120_000);
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
  // The awaited check reads the file, so it has always been right about this.
  check("an awaited check sees a lock taken during a stall", answers.owned === false, answers);
  // The synchronous one used to answer from a flag a stopped event loop cannot have updated, so it
  // said "still mine" for exactly the case it was added to cover.
  check("and so does the synchronous one", answers.ownedNow === false, answers);

  console.log(
    failures.length === 0
      ? "\nstall-check: OK"
      : `\nstall-check: ${failures.length} failed`,
  );
  process.exit(failures.length === 0 ? 0 : 1);
}
