// Checks how the tree lease recovers from races and storage faults. A paused contender can't
// reuse a released epoch, a failed or late renewal doesn't extend the lease, a lost lease stays
// lost, and an unreadable claim admits nobody. No server needed.
//
// Usage: npx tsx checks/lease-recovery-check.mts [scenario]

import assert from "node:assert/strict";
import fs from "node:fs/promises";
import { syncBuiltinESMExports } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { withLease } from "../src/tree/lease.js";

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

  const late = withLease(file, async () => { overlapped ||= held; });
  let current: Promise<void> | undefined;
  try {
    await paused.promise;
    await withLease(file, async () => {});
    current = withLease(file, async () => {
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
  await withLease(file, async (_owned, ownedNow) => {
    let attempts = 0;
    fs.utimes = async (...args) => {
      if (String(args[0]).startsWith(`${file}.lock/`)) {
        attempts++;
        throw Object.assign(new Error("renewal refused"), { code: "EIO" });
      }
      return originalUtimes(...args);
    };
    syncBuiltinESMExports();
    // Inside the window, so the refresh tries to renew and storage refuses it.
    Date.now = () => originalNow() + 45_000;
    try {
      // Wait for a refresh to reach the failure, not for one tick. A loaded machine runs late.
      const deadline = originalNow() + 15_000;
      while (attempts === 0 && originalNow() < deadline) await sleep(100);
      assert.ok(attempts > 0, "renewal must reach the injected storage failure");
      // Past the window measured from the last renewal that landed. A failed one doesn't count.
      Date.now = () => originalNow() + 51_000;
      assert.equal(ownedNow(), false, "a readable token cannot extend a failed renewal");
    } finally {
      fs.utimes = originalUtimes;
      syncBuiltinESMExports();
      Date.now = originalNow;
    }
  });
  console.log("PASS failed renewal cannot extend write permission");
}

// A renewal is decided when it starts but takes effect when storage answers. One that answers
// past the window can't extend the lease, because a contender may have taken over meanwhile.
async function lateRenewal() {
  const file = join(root, "late.jsonl");
  await withLease(file, async (owned, ownedNow) => {
    const stalled = barrier();
    const resume = barrier();
    let done = false;
    let stalling = true;
    fs.utimes = async (...args) => {
      if (stalling && String(args[0]).startsWith(`${file}.lock/`)) {
        stalling = false;
        stalled.release();
        await resume.promise;
        const result = await originalUtimes(...args);
        done = true;
        return result;
      }
      return originalUtimes(...args);
    };
    syncBuiltinESMExports();
    // Inside the window, so the refresh starts a renewal stamped 40s after the claim.
    Date.now = () => originalNow() + 40_000;
    try {
      await Promise.race([stalled.promise, sleep(15_000)]);
      assert.equal(stalling, false, "renewal must reach the stalled storage call");
      // Past the window from the claim while the renewal is stalled.
      Date.now = () => originalNow() + 55_000;
      resume.release();
      const deadline = originalNow() + 5_000;
      while (!done && originalNow() < deadline) await sleep(20);
      await sleep(50);
      assert.equal(ownedNow(), false, "a renewal that lands late cannot extend the lease");
      assert.equal(await owned(), false, "a renewal that lands late cannot confirm ownership");
    } finally {
      resume.release();
      fs.utimes = originalUtimes;
      syncBuiltinESMExports();
      Date.now = originalNow;
    }
  });
  console.log("PASS a late renewal cannot extend the lease");
}

async function observedLoss() {
  const file = join(root, "loss.jsonl");
  await withLease(file, async (owned, ownedNow) => {
    await fs.writeFile(join(`${file}.lock`, "00000002"), JSON.stringify({ token: "other" }));
    assert.equal(await owned(), false);
    assert.equal(ownedNow(), false, "an observed loss must stop synchronous writes at once");
    await fs.rm(join(`${file}.lock`, "00000002"));
    assert.equal(await owned(), false, "ownership cannot return after a confirmed loss");
  });
  console.log("PASS confirmed ownership loss stays lost");
}

// A holder that went unconfirmed past its window may already be overtaken. Storage still showing
// its claim doesn't give the lease back.
async function expiredStaysLost() {
  const file = join(root, "expired.jsonl");
  await withLease(file, async (owned, ownedNow) => {
    Date.now = () => originalNow() + 51_000;
    try {
      assert.equal(await owned(), false, "an expired holder cannot confirm ownership");
    } finally {
      Date.now = originalNow;
    }
    assert.equal(await owned(), false, "expiry is permanent even though the claim is still ours");
    assert.equal(ownedNow(), false);
  });
  console.log("PASS an expired lease stays lost");
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
    await assert.rejects(withLease(file, async () => { entered = true; }), { code: "EIO" });
    assert.equal(entered, false, "unreadable claims cannot admit another writer");
  } finally {
    fs.stat = originalStat;
    syncBuiltinESMExports();
  }
  console.log("PASS unreadable claims cannot admit another writer");
}

try {
  const checks = {
    releasedEpochRace,
    failedRenewal,
    lateRenewal,
    observedLoss,
    expiredStaysLost,
    unreadableClaim,
  };
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
