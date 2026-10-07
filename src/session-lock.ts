// One writer at a time for a session file. Pi's session file is a tree, so two writers branch it
// rather than corrupt it. Two attempts of the same activity can overlap after a stalled heartbeat.
//
// A lease, not a fence. Callers must re-check ownership right before each write. The lock is a
// directory of claims named by epoch. Taking over is an exclusive create of epoch N+1, so it is a
// compare-and-set on the state the contender read.

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
const lockDir = (sessionFile: string) => `${sessionFile}.lock`;
// Epoch only, so two contenders for the same epoch compete on one path. The token goes inside.
const claimName = (epoch: number) => String(epoch).padStart(8, "0");

const held = (token: string) => JSON.stringify({ token, host: hostname(), pid: process.pid });

interface Claim {
  readonly epoch: number;
  readonly name: string;
  readonly mtimeMs: number;
}

async function claims(dir: string): Promise<Claim[]> {
  const names = await readdir(dir);
  const found: Claim[] = [];
  for (const name of names) {
    const epoch = Number.parseInt(name, 10);
    if (!Number.isFinite(epoch)) continue;
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
export async function withSessionLock<T>(
  sessionFile: string,
  body: (owned: () => Promise<boolean>, ownedNow: () => boolean) => Promise<T>,
  waitMs = 60_000,
  // Test hook: stall between reading claims and taking the next epoch, to reproduce the race.
  claimPauseMs = 0,
): Promise<T> {
  const dir = lockDir(sessionFile);
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
      if (claimPauseMs > 0) await new Promise((resolve) => setTimeout(resolve, claimPauseMs));
      try {
        const confirmedAt = Date.now();
        await writeFile(join(dir, name), held(token), { encoding: "utf8", flag: "wx" });
        const latest = (await claims(dir)).at(-1);
        if (latest?.epoch !== epoch) {
          await rm(join(dir, name), { force: true });
          continue;
        }
        mine = { epoch, name, confirmedAt };
        // Lower epochs are superseded. Their holders notice because something newer exists.
        for (const stale of existing) {
          if (stale.epoch < epoch) await rm(join(dir, stale.name), { force: true });
        }
        if ((await claims(dir)).at(-1)?.epoch !== epoch) {
          await rm(join(dir, name), { force: true });
          continue;
        }
        break;
      } catch (err) {
        const code = (err as NodeJS.ErrnoException).code;
        // Lost the race for this epoch, or the directory vanished. Look again.
        if (code !== "EEXIST" && code !== "ENOENT") throw err;
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

  // Check the token too. An emptied directory restarts at epoch one.
  const stillOurs = async (list: Claim[]) => {
    const owner = list[list.length - 1];
    if (!owner || owner.epoch !== mine.epoch) return false;
    return (await holderOf(owner.name)) === token;
  };

  let refreshing: Promise<void> | undefined;
  const refresh = setInterval(() => {
    if (lost || refreshing) return;
    refreshing = (async () => {
      const confirmedAt = Date.now();
      const list = await claims(dir).catch(() => undefined);
      if (!list || lost) return;
      const owner = list[list.length - 1];
      if (!owner || owner.epoch !== mine.epoch) {
        lost = true;
        clearInterval(refresh);
        return;
      }
      const holder = await holderOf(owner.name);
      if (holder === undefined || lost) return;
      if (holder !== token) {
        lost = true;
        clearInterval(refresh);
        return;
      }
      const now = new Date(confirmedAt);
      const renewed = await utimes(join(dir, mine.name), now, now).then(
        () => true,
        () => false,
      );
      // Readers can still see our token when storage refuses renewal. That isn't a new lease.
      if (renewed && !lost) lastConfirmed = confirmedAt;
    })().finally(() => {
      refreshing = undefined;
    });
  }, REFRESH_MS);
  refresh.unref?.();

  // Reads storage instead of trusting the refresher, which can be up to `REFRESH_MS` stale.
  const owned = async () => {
    const list = await claims(dir).catch(() => undefined);
    if (lost || list === undefined) return false;
    const ours = await stillOurs(list);
    if (!ours) lost = true;
    return ours;
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
    if ((await holderOf(mine.name)) === token) {
      const released = new Date(0);
      await utimes(join(dir, mine.name), released, released).catch((err: NodeJS.ErrnoException) => {
        if (err.code !== "ENOENT") throw err;
      });
    }
  }
}

