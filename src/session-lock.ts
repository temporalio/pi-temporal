// One writer at a time for a session file. Pi's session file is a tree, and every entry takes its
// parent from the leaf its writer last saw, so two writers do not corrupt it, they branch it. Two
// attempts of the same activity are what does that: a heartbeat that stalls long enough for
// Temporal to start attempt 2 leaves attempt 1 alive and both appending. The tool calls take it
// too, because they move the project's files even though they do not write the transcript.
//
// It lives beside the session file, so it works wherever the session file works, and a holder that
// dies is reclaimed on age, which is why it is refreshed while it is held.
//
// A lease, not a fence. It narrows the overlap; it does not remove it. Neither the awaited
// ownership read nor the synchronous guard is atomic with the write it guards, and neither says
// anything about a tool still running outside the process.
//
// Taking it over is the part that has to be exact. A lock is a directory of claims, each named for
// the epoch it took, and the newest claim owns the lock. Taking over means creating the next epoch
// with an exclusive create, which is a compare and set: it says "I saw epoch N, and I claim N+1",
// and it fails if anybody else already claimed N+1 from the same reading. Competing takeovers
// compete on one path, so nothing moves a live claim out of the way to make room.
//
// The shape this replaced moved the stale claim aside and created a new one at its path. `rename`
// acts on the path rather than on the file whose age was measured, so a contender slow between the
// two moved whatever was there by then, and while it was moved the path was free for a third. A
// review reproduced both. Reading the file back caught the theft and could not undo it. The fault
// was that the takeover was not conditional on the state it measured; this one is.

import { mkdir, readdir, readFile, rm, rmdir, stat, utimes, writeFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import { hostname } from "node:os";
import { dirname, join } from "node:path";

// Longer than the activity heartbeat timeout, on purpose. A holder whose event loop is blocked
// cannot refresh, and the writes this lock protects are the synchronous ones most likely to block
// it, so a shorter window would reclaim from a holder that is alive and working.
//
// Expiry is not evidence that the holder stopped. A blocked event loop outlives any timeout here,
// so an attempt Temporal has given up on can still be inside the body when the next one claims the
// following epoch. That is what `owned()` is for: whoever is about to write asks whether the lock
// is still theirs, which narrows the hole to the gap between the question and the write.
//
// `ownedNow()` is the same question for a writer that cannot await one. Pi's append path is
// synchronous all the way down, and the write a model call ends with lands at the end of a stream
// that runs for minutes, so an awaited check before the call is not a check on that write at all.
const STALE_MS = 60_000;
const REFRESH_MS = 3_000;
const RETRY_MS = 250;
// How much earlier than that a holder stops saying the lock is its own. Both sides measuring the
// same threshold is wrong in the direction that costs something: the contender reads an mtime
// another host's clock wrote, so a clock ahead by `d` reclaims `d` early while the holder still
// answers yes to the question asked immediately before a write. A refresh interval plus room for
// drift. It is a margin, not a measured bound on skew.
const MARGIN_MS = 10_000;

// What the deployment has to provide: an exclusive create that really excludes, and directory and
// timestamp reads coherent enough that a listing does not miss a claim somebody just wrote. mtime
// comes from whichever host last touched the file, so a skewed clock reads a live lock as dead.
// Checked on local disk and on one NFSv4 mount. NFSv3 and SMB are untested here.
//
// A directory, holding one file per claim. The name is where the ordering lives, so deciding who
// owns the lock is a listing rather than a read: a claim that cannot be read yet still counts.
const lockDir = (sessionFile: string) => `${sessionFile}.lock`;
// The epoch alone, because the name is what the exclusive create competes on. Putting the holder's
// token in the name would give two contenders two different paths for the same epoch, and an
// exclusive create of two different paths excludes nothing. The token goes inside the file.
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
    const info = await stat(join(dir, name)).catch(() => undefined);
    if (info) found.push({ epoch, name, mtimeMs: info.mtimeMs });
  }
  return found.sort((a, b) => a.epoch - b.epoch);
}

/**
 * Callers must check ownership before writing because an expired attempt can remain alive.
 * `ownedNow()` uses the last confirmed timestamp for synchronous append paths; it cannot read
 * shared storage at the write. Acquisition timeout permits a later retry, not proof of liveness.
 */
export async function withSessionLock<T>(
  sessionFile: string,
  body: (owned: () => Promise<boolean>, ownedNow: () => boolean) => Promise<T>,
  waitMs = 60_000,
  // How long to stall between reading the claims and taking the next epoch. Zero everywhere but the
  // check that reproduces the takeover race: that interleaving needs a contender held at exactly
  // that point for longer than another takes to claim and hold, and nothing outside this module can
  // hold it there. Only the stall is injected; what the check asserts is the real outcome, one
  // holder or two.
  claimPauseMs = 0,
): Promise<T> {
  const dir = lockDir(sessionFile);
  const token = randomUUID();
  const deadline = Date.now() + waitMs;
  let mine!: { readonly epoch: number; readonly name: string };

  // The session directory may not exist yet. The first activity of a session is what creates it,
  // and it does that inside the lock.
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
        await writeFile(join(dir, name), held(token), { encoding: "utf8", flag: "wx" });
        mine = { epoch, name };
        // Everything below the epoch just taken is superseded by definition, and its holder finds
        // that out the same way anybody does: something newer exists.
        for (const stale of existing) {
          if (stale.epoch < epoch) await rm(join(dir, stale.name), { force: true });
        }
        break;
      } catch (err) {
        const code = (err as NodeJS.ErrnoException).code;
        // Somebody claimed this epoch from the same reading, or removed the directory under us.
        // Both mean look again; anything else is a real error and reporting it as contention would
        // send the next reader looking for a writer that does not exist.
        if (code !== "EEXIST" && code !== "ENOENT") throw err;
      }
    }
    if (Date.now() >= deadline) {
      throw new Error(`another writer has held ${dir} for ${waitMs}ms`);
    }
    await new Promise((resolve) => setTimeout(resolve, RETRY_MS));
  }

  // Set the moment the refresher finds the lock is somebody else's, and never unset: a lock taken
  // away does not come back.
  let lost = false;
  // A paused event loop runs no timer callbacks, so the guard must also expire by elapsed time.
  let lastConfirmed = Date.now();

  const holderOf = async (name: string) => {
    const text = await readFile(join(dir, name), "utf8").catch(() => undefined);
    if (text === undefined) return undefined;
    try {
      return (JSON.parse(text) as { token?: string }).token;
    } catch {
      return undefined;
    }
  };

  // By token as well as by epoch. A directory that empties completely starts again at one, so the
  // number on its own can name somebody else's claim.
  const stillOurs = async (list: Claim[]) => {
    const owner = list[list.length - 1];
    if (!owner || owner.epoch !== mine.epoch) return false;
    return (await holderOf(owner.name)) === token;
  };

  // Stops as soon as the lock is not ours. A holder superseded while it was blocked would otherwise
  // keep refreshing a claim nothing reads.
  const refresh = setInterval(() => {
    void (async () => {
      // One read that fails is not the lock being taken away, and the shared storage this exists
      // for is exactly where that happens. Stopping on it would let the claim age out from under a
      // holder that is still writing.
      const list = await claims(dir).catch(() => undefined);
      if (!list) return;
      const owner = list[list.length - 1];
      // Nothing there at all means this claim was removed, and something newer means it was
      // superseded. Both are the lock being gone, and it does not come back.
      if (!owner || owner.epoch > mine.epoch) {
        lost = true;
        clearInterval(refresh);
        return;
      }
      const holder = await holderOf(owner.name);
      // Unreadable is one bad read, not a lock taken away, and the shared storage this exists for
      // is exactly where that happens. It is not a confirmation either, so the synchronous guard
      // decays toward saying no while it lasts.
      if (holder === undefined) return;
      if (holder !== token) {
        lost = true;
        clearInterval(refresh);
        return;
      }
      lastConfirmed = Date.now();
      const now = new Date();
      await utimes(join(dir, mine.name), now, now).catch(() => {});
    })();
  }, REFRESH_MS);
  refresh.unref?.();

  // Read rather than trusting the refresher, which only notices on its next tick and so answers for
  // up to REFRESH_MS ago. A caller asking this is about to write.
  const owned = async () => {
    const list = await claims(dir).catch(() => undefined);
    return list !== undefined && (await stillOurs(list));
  };

  // Synchronous appends cannot await shared storage. The margin accounts for some delay between
  // this process's last confirmation and another host's reclaim decision, not every clock skew.
  const ownedNow = () => !lost && Date.now() - lastConfirmed < STALE_MS - MARGIN_MS;

  try {
    return await body(owned, ownedNow);
  } finally {
    clearInterval(refresh);
    // A holder that has given the lock back does not own it, and the guard it handed out is what a
    // writer asks. Pi's append path keeps that closure for the life of the session, so leaving it
    // answering yes would wave through exactly the write this exists to stop.
    lost = true;
    // Only while it is still ours. A superseder removes lower claims, and a directory that emptied
    // has started again at one, so removing by name alone can take a claim somebody else holds.
    if ((await holderOf(mine.name)) === token) await rm(join(dir, mine.name), { force: true });
    // And the directory, when nothing is left in it. A lock that leaves something behind is one
    // every reader of the shared directory has to know to ignore, and the acquire path already
    // treats a directory that vanished under it as a reason to look again.
    await rmdir(dir).catch(() => {});
  }
}

export const lockDirFor = lockDir;
