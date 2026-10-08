// One writer at a time for a project's tree store and for a host's project directory. Unlike the
// session file, these are also written by clients and by hosts outside any Workflow, so there is
// no Workflow to order them, and a lease stands in. Two attempts of the same activity can overlap
// after a stalled heartbeat.
//
// A lease, not a fence. Callers must re-check ownership right before each write. The lock is a
// directory of claims named by epoch. Taking over is an exclusive create of epoch N+1, and the
// claim counts only while no newer epoch exists. A released claim stays on disk, expired, so
// epochs only grow and a contender that paused can't reuse one.

import { mkdir, readdir, readFile, rm, stat, utimes, writeFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import { hostname } from "node:os";
import { dirname, join } from "node:path";

// Longer than the activity heartbeat timeout. A holder blocked in a synchronous write cannot
// refresh, and a shorter window would reclaim from a live holder.
const STALE_MS = 60_000;
const REFRESH_MS = 3_000;
const RETRY_MS = 250;
// The holder stops claiming ownership this much before `STALE_MS`, to allow for refresh delay and
// some clock drift between hosts. It is a margin, not a measured bound on skew.
const MARGIN_MS = 10_000;

// Needs an exclusive create that really excludes and coherent listings and mtimes. Checked on local
// disk and one NFSv4 mount. NFSv3 and SMB are untested.
const lockDir = (path: string) => `${path}.lock`;
// Epoch only, so two contenders for the same epoch compete on one path. The token goes inside.
const claimName = (epoch: number) => String(epoch).padStart(8, "0");

const held = (token: string) => JSON.stringify({ token, host: hostname(), pid: process.pid });

interface Claim {
  readonly epoch: number;
  readonly name: string;
  readonly mtimeMs: number;
}

// A stale handle doesn't prove a claim is gone, only that this read can't say. So the whole scan
// runs again, and a stale handle that stays is an error. Only a missing entry is absence.
async function claims(dir: string): Promise<Claim[]> {
  for (let attempt = 1; ; attempt++) {
    try {
      return await scanClaims(dir);
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== "ESTALE" || attempt >= 3) throw err;
    }
  }
}

async function scanClaims(dir: string): Promise<Claim[]> {
  const names = await readdir(dir);
  const found: Claim[] = [];
  for (const name of names) {
    const epoch = Number.parseInt(name, 10);
    if (!Number.isFinite(epoch)) continue;
    // Another client can delete a superseded claim between the listing and the stat.
    const info = await stat(join(dir, name)).catch((err: NodeJS.ErrnoException) => {
      if (err.code === "ENOENT") return undefined;
      throw err;
    });
    if (info) found.push({ epoch, name, mtimeMs: info.mtimeMs });
  }
  return found.sort((a, b) => a.epoch - b.epoch);
}

/**
 * Callers must check ownership before writing because an expired attempt can remain alive.
 * `ownedNow()` uses the last confirmed timestamp for synchronous append paths. Acquisition
 * timeout permits a later retry, not proof of liveness.
 */
export async function withLease<T>(
  path: string,
  body: (owned: () => Promise<boolean>, ownedNow: () => boolean) => Promise<T>,
  waitMs = 60_000,
  // Test hook: stall between reading claims and taking the next epoch, to reproduce the race.
  claimPauseMs = 0,
): Promise<T> {
  const dir = lockDir(path);
  const token = randomUUID();
  const deadline = Date.now() + waitMs;
  let mine!: { readonly epoch: number; readonly name: string; readonly confirmedAt: number };

  // The first activity of a session creates its directory, inside the lock.
  await mkdir(dirname(dir), { recursive: true });

  for (;;) {
    await mkdir(dir, { recursive: true });
    const existing = await claims(dir);
    const owner = existing[existing.length - 1];
    const contended = owner !== undefined && Date.now() - owner.mtimeMs < STALE_MS;
    if (!contended) {
      const epoch = (owner?.epoch ?? 0) + 1;
      const name = claimName(epoch);
      const path = join(dir, name);
      if (claimPauseMs > 0) await new Promise((resolve) => setTimeout(resolve, claimPauseMs));
      const confirmedAt = Date.now();
      let created = false;
      try {
        await writeFile(path, held(token), { encoding: "utf8", flag: "wx" });
        created = true;
      } catch (err) {
        const code = (err as NodeJS.ErrnoException).code;
        // Lost the race for this epoch, or the directory vanished. Look again.
        if (code !== "EEXIST" && code !== "ENOENT") throw err;
      }
      if (created) {
        try {
          // Lower epochs are superseded. Their holders notice because something newer exists.
          for (const stale of existing) {
            if (stale.epoch < epoch) await rm(join(dir, stale.name), { force: true });
          }
          // The create alone isn't a compare-and-set. A contender that paused before it can
          // recreate an epoch that was already superseded. So the claim counts only if it's
          // still the newest after the cleanup.
          if ((await claims(dir)).at(-1)?.epoch === epoch) {
            // The create stamps the server's clock. Stamp ours, so a client whose clock runs ahead
            // doesn't look stale to contenders until its first refresh.
            const stamp = new Date(confirmedAt);
            await utimes(path, stamp, stamp);
            mine = { epoch, name, confirmedAt };
            break;
          }
          await rm(path, { force: true });
          continue;
        } catch (err) {
          // Nobody can tell whether this claim holds, so expire it. Otherwise every contender,
          // the retry of this activity included, waits out the stale window.
          await utimes(path, new Date(0), new Date(0)).catch(() => {});
          throw err;
        }
      }
    }
    if (Date.now() >= deadline) {
      throw new Error(`another writer has held ${dir} for ${waitMs}ms`);
    }
    await new Promise((resolve) => setTimeout(resolve, RETRY_MS));
  }

  // Never unset. A lock taken away does not come back.
  let lost = false;
  // A paused event loop runs no timers, so the guard must also expire by elapsed time.
  let lastConfirmed = mine.confirmedAt;

  const holderOf = async (name: string) => {
    const text = await readFile(join(dir, name), "utf8").catch(() => undefined);
    if (text === undefined) return undefined;
    try {
      return (JSON.parse(text) as { token?: string }).token;
    } catch {
      return undefined;
    }
  };

  const markLost = () => {
    lost = true;
    clearInterval(refresh);
    return false;
  };

  // Past this, a contender may already have read the claim as stale and be taking the next
  // epoch. A late renewal can't win that back, because the contender doesn't read again before it
  // claims. So the lease is lost for good before storage is even asked.
  const expired = () => Date.now() - lastConfirmed >= STALE_MS - MARGIN_MS;

  let refreshing: Promise<void> | undefined;
  const refresh = setInterval(() => {
    if (lost || refreshing) return;
    if (expired()) {
      markLost();
      return;
    }
    refreshing = (async () => {
      const confirmedAt = Date.now();
      const list = await claims(dir).catch(() => undefined);
      if (!list || lost) return;
      const owner = list[list.length - 1];
      if (!owner || owner.epoch !== mine.epoch) {
        markLost();
        return;
      }
      const holder = await holderOf(owner.name);
      if (holder === undefined || lost) return;
      if (holder !== token) {
        markLost();
        return;
      }
      // The reads take time too. A renewal that lands past the window would hand back a lease a
      // contender may already be taking.
      if (expired()) {
        markLost();
        return;
      }
      const now = new Date(confirmedAt);
      const renewed = await utimes(join(dir, mine.name), now, now).then(
        () => true,
        () => false,
      );
      // Readers can still see our token when storage refuses renewal. That isn't a new lease.
      if (!renewed || lost) return;
      // A renewal that lands past the window is too late, since a contender may already hold the
      // next epoch.
      if (expired()) {
        markLost();
        return;
      }
      lastConfirmed = confirmedAt;
    })().finally(() => {
      refreshing = undefined;
    });
  }, REFRESH_MS);
  refresh.unref?.();

  // Reads storage instead of trusting the refresher, which can be up to `REFRESH_MS` stale.
  // An unreadable read is no confirmation, but no loss either, the same as for the refresher. A
  // newer epoch or another token is a loss, and a lock taken away does not come back.
  const owned = async () => {
    if (lost) return false;
    if (expired()) return markLost();
    const list = await claims(dir).catch(() => undefined);
    if (lost || list === undefined) return false;
    const owner = list[list.length - 1];
    if (!owner || owner.epoch !== mine.epoch) return markLost();
    const holder = await holderOf(owner.name);
    if (holder === undefined) return false;
    if (holder !== token) return markLost();
    return !lost;
  };

  // For synchronous appends that cannot await shared storage.
  const ownedNow = () => !lost && Date.now() - lastConfirmed < STALE_MS - MARGIN_MS;

  try {
    return await body(owned, ownedNow);
  } finally {
    clearInterval(refresh);
    // Pi keeps the `ownedNow` closure for the session's life, so it must answer no after release.
    lost = true;
    await refreshing;
    // Epoch reuse would let a paused contender overtake a new holder. Keep one expired claim.
    // Expiring it only makes the handoff faster, since it goes stale anyway. A failure here must
    // not turn the body's result into a failed activity.
    if ((await holderOf(mine.name)) === token) {
      const released = new Date(0);
      const path = join(dir, mine.name);
      await utimes(path, released, released).catch((err: NodeJS.ErrnoException) => {
        if (err.code !== "ENOENT") console.warn(`could not expire ${path}: ${err.message}`);
      });
    }
  }
}

