import assert from "node:assert/strict";
import fs from "node:fs/promises";
import { syncBuiltinESMExports } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { withSessionLock } from "../src/session-lock.js";

const root = await fs.mkdtemp(join(tmpdir(), "pi-lease-recovery-"));
const originalWrite = fs.writeFile;
const originalUtimes = fs.utimes;
const originalStat = fs.stat;
const originalNow = Date.now;

function barrier() {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => { release = resolve; });
  return { promise, release };
}

async function releasedEpochRace() {
  const file = join(root, "epoch.jsonl");
  await fs.mkdir(`${file}.lock`);
  const dead = join(`${file}.lock`, "00000001");
  await fs.writeFile(dead, JSON.stringify({ token: "dead" }));
  await fs.utimes(dead, new Date(0), new Date(0));
  const paused = barrier();
  const resume = barrier();
  const active = barrier();
  const leave = barrier();
  let held = false;
  let overlapped = false;
  let delayed = false;
  fs.writeFile = (async (...args: Parameters<typeof fs.writeFile>) => {
    if (!delayed && args[0] === join(`${file}.lock`, "00000002")) {
      delayed = true;
      paused.release();
      await resume.promise;
    }
    return originalWrite(...args);
  }) as typeof fs.writeFile;
  syncBuiltinESMExports();

  const late = withSessionLock(file, async () => { overlapped ||= held; });
  let current: Promise<void> | undefined;
  try {
    await paused.promise;
    await withSessionLock(file, async () => {});
    current = withSessionLock(file, async () => {
      held = true;
      active.release();
      await leave.promise;
      held = false;
    });
    await active.promise;
    resume.release();
    await sleep(100);
    assert.equal(overlapped, false, "a paused contender cannot overtake a new holder");
  } finally {
    resume.release();
    leave.release();
    await Promise.allSettled([late, current]);
    fs.writeFile = originalWrite;
    syncBuiltinESMExports();
  }
  console.log("PASS a released epoch cannot admit a stale contender");
}

async function failedRenewal() {
  const file = join(root, "renewal.jsonl");
  await withSessionLock(file, async (_owned, ownedNow) => {
    let attempts = 0;
    fs.utimes = async (...args) => {
      if (String(args[0]).startsWith(`${file}.lock/`)) {
        attempts++;
        throw Object.assign(new Error("renewal refused"), { code: "EIO" });
      }
      return originalUtimes(...args);
    };
    syncBuiltinESMExports();
    Date.now = () => originalNow() + 51_000;
    try {
      // Wait for a refresh to reach the failure, not for one tick. A loaded machine runs late.
      const deadline = originalNow() + 15_000;
      while (attempts === 0 && originalNow() < deadline) await sleep(100);
      assert.ok(attempts > 0, "renewal must reach the injected storage failure");
      assert.equal(ownedNow(), false, "a readable token cannot extend a failed renewal");
    } finally {
      fs.utimes = originalUtimes;
      syncBuiltinESMExports();
      Date.now = originalNow;
    }
  });
  console.log("PASS failed renewal cannot extend write permission");
}

async function observedLoss() {
  const file = join(root, "loss.jsonl");
  await withSessionLock(file, async (owned, ownedNow) => {
    await fs.writeFile(join(`${file}.lock`, "00000002"), JSON.stringify({ token: "other" }));
    assert.equal(await owned(), false);
    assert.equal(ownedNow(), false, "an observed loss must stop synchronous writes at once");
    await fs.rm(join(`${file}.lock`, "00000002"));
    assert.equal(await owned(), false, "ownership cannot return after a confirmed loss");
  });
  console.log("PASS confirmed ownership loss stays lost");
}

async function unreadableClaim() {
  const file = join(root, "unreadable.jsonl");
  await fs.mkdir(`${file}.lock`);
  const owner = join(`${file}.lock`, "00000002");
  await fs.writeFile(owner, JSON.stringify({ token: "owner" }));
  let entered = false;
  fs.stat = (async (...args: Parameters<typeof fs.stat>) => {
    if (args[0] === owner) {
      throw Object.assign(new Error("metadata unavailable"), { code: "EIO" });
    }
    return originalStat(...args);
  }) as typeof fs.stat;
  syncBuiltinESMExports();
  try {
    await assert.rejects(withSessionLock(file, async () => { entered = true; }), { code: "EIO" });
    assert.equal(entered, false, "unreadable claims cannot admit another writer");
  } finally {
    fs.stat = originalStat;
    syncBuiltinESMExports();
  }
  console.log("PASS unreadable claims cannot admit another writer");
}

try {
  const checks = { releasedEpochRace, failedRenewal, observedLoss, unreadableClaim };
  const selected = process.argv[2];
  for (const [name, check] of Object.entries(checks)) {
    if (!selected || selected === name) await check();
  }
} finally {
  fs.writeFile = originalWrite;
  fs.utimes = originalUtimes;
  fs.stat = originalStat;
  Date.now = originalNow;
  syncBuiltinESMExports();
  await fs.rm(root, { recursive: true, force: true });
}
