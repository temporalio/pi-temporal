import assert from "node:assert/strict";
import fs from "node:fs/promises";
import { syncBuiltinESMExports } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { withSessionLock } from "./src/session-lock.js";

const root = await fs.mkdtemp(join(tmpdir(), "pi-lock-gap-"));
const file = join(root, "session.jsonl");
const path = `${file}.lock`;
await fs.writeFile(path, JSON.stringify({ token: "dead" }));
const cold = new Date(Date.now() - 70_000);
await fs.utimes(path, cold, cold);
function barrier() {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => { release = resolve; });
  return { promise, release };
}
const gap = barrier();
const closeGap = barrier();
const quickStarted = barrier();
const thirdStarted = barrier();
const finishHolders = barrier();
let quickNow: (() => boolean) | undefined;
let thirdNow: (() => boolean) | undefined;
const originalRename = fs.rename;
let paused = false;
fs.rename = async (from, to) => {
  await originalRename(from, to);
  if (from === path && !paused) {
    const moved = JSON.parse(await fs.readFile(to, "utf8"));
    if (moved.token !== "dead") {
      paused = true;
      gap.release();
      await closeGap.promise;
    }
  }
};
syncBuiltinESMExports();
try {
  const slow = withSessionLock(file, async () => {}, 5_000, 700);
  await new Promise((resolve) => setTimeout(resolve, 50));
  const quick = withSessionLock(file, async (_owned, ownedNow) => {
    quickNow = ownedNow;
    quickStarted.release();
    await finishHolders.promise;
  }, 5_000);
  await quickStarted.promise;
  await gap.promise;
  const third = withSessionLock(file, async (_owned, ownedNow) => {
    thirdNow = ownedNow;
    thirdStarted.release();
    await finishHolders.promise;
  }, 5_000);
  await thirdStarted.promise;
  const result = { quickOwnedNow: quickNow!(), thirdOwnedNow: thirdNow!() };
  console.log(JSON.stringify(result));
  assert.equal(result.quickOwnedNow && result.thirdOwnedNow, false, "two live writers passed the synchronous guard");
  closeGap.release();
  finishHolders.release();
  await Promise.allSettled([slow, quick, third]);
} finally {
  closeGap.release();
  finishHolders.release();
  fs.rename = originalRename;
  syncBuiltinESMExports();
  await new Promise((resolve) => setTimeout(resolve, 1_000));
  await fs.rm(root, { recursive: true, force: true });
}
