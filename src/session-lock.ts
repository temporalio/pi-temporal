// One writer at a time for a session file. Pi's session file is a tree, and every entry takes its
// parent from the leaf its writer last saw, so two writers do not corrupt it, they branch it. The
// step's own calls never write, but two attempts of the same activity can: a heartbeat that stalls
// long enough for Temporal to start attempt 2 leaves attempt 1 alive and both appending.
//
// Advisory, and beside the session file, so it works wherever the session file works. A holder
// that dies is reclaimed on age, which is why the lock is refreshed while it is held.

import { link, mkdir, readFile, rename, rm, stat, utimes, writeFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import { hostname } from "node:os";
import { dirname } from "node:path";

// Longer than the activity heartbeat timeout, on purpose. A holder whose event loop is blocked
// cannot refresh, and the writes this lock protects are the synchronous ones most likely to block
// it, so a shorter window would steal the lock from a holder that is alive and working.
//
// It does not make the lock safe on its own. Temporal giving up on an attempt stops it being
// waited for, not being run: an attempt blocked past this window is still inside the body when the
// next one reclaims. That is what `owned()` is for. Whoever is about to write asks whether the
// lock is still theirs, which narrows the hole from the whole body to the gap between that
// question and the write.
//
// `ownedNow()` is the same question for a writer that cannot await one. Pi's append path is
// synchronous all the way down, and the write a model call ends with lands at the end of a stream
// that runs for minutes, so an awaited check before the call is not a check on that write at all.
const STALE_MS = 60_000;
const REFRESH_MS = 3_000;
const RETRY_MS = 250;
// How much earlier than that a holder stops saying the lock is its own. Both sides measuring the
// same threshold is wrong in the direction that costs something: the contender reads an mtime
// another host's clock wrote, so a clock ahead by `d` reclaims `d` early, and the holder is still
// answering yes to the one question asked immediately before a write. A refresh interval plus room
// for the skew a machine drifts to without anybody noticing.
const MARGIN_MS = 10_000;

// mtime comes from whichever host last touched the file, so a skewed clock reads a live lock as
// dead. The exclusive create is what actually excludes, which needs a filesystem where O_EXCL is
// atomic: local disk, NFSv4, SMB. It is not reliable on NFSv3.

const lockPath = (sessionFile: string) => `${sessionFile}.lock`;

const held = (token: string) => JSON.stringify({ token, host: hostname(), pid: process.pid });

// Reclaims by renaming out of the way rather than removing. Two contenders deciding the same lock
// is stale at the same moment then move the same file, and only one of them wins.
//
// `rename` acts on the path, not on the file whose age was measured, so a contender slow between
// the two moves whatever is there by then, including a lock somebody has just taken. What the
// measurement can be pinned to is the content: a token changes only when a different holder writes
// one. So the file is read before the move and the moved file after it, and a reclaim that finds it
// changed puts it back and reports the lock as held rather than taking it. The restore is a `link`,
// which fails instead of overwriting, so a third contender that created one meanwhile keeps it and
// this one still loses. What is left is a lock briefly absent from its path, which is what `owned()`
// covers and why writers ask again on the way to the write.
async function taken(path: string, token: string, pauseMs = 0): Promise<boolean> {
  try {
    const info = await stat(path);
    if (Date.now() - info.mtimeMs < STALE_MS) return true;
    const before = await readFile(path, "utf8").catch(() => undefined);
    if (pauseMs > 0) await new Promise((resolve) => setTimeout(resolve, pauseMs));
    const corpse = `${path}.stale.${token}`;
    await rename(path, corpse);
    const moved = await readFile(corpse, "utf8").catch(() => undefined);
    if (moved !== before) {
      await link(corpse, path).catch(() => {});
      await rm(corpse, { force: true });
      return true;
    }
    await rm(corpse, { force: true });
    return false;
  } catch {
    return false;
  }
}

/**
 * Run `body` as the only writer of this session file. Throws if the lock cannot be taken in time,
 * which is a retryable condition: the holder is another attempt that is still working.
 *
 * `body` is handed an `owned()` it should call immediately before it writes, and an `ownedNow()`
 * for a writer that cannot await one. See STALE_MS.
 */
export async function withSessionLock<T>(
  sessionFile: string,
  body: (owned: () => Promise<boolean>, ownedNow: () => boolean) => Promise<T>,
  waitMs = 60_000,
  // How long to stall between measuring a stale lock's age and reclaiming it. Zero everywhere but
  // the check that reproduces the reclaim race: that interleaving needs a contender held at exactly
  // that point for longer than another takes to reclaim and acquire, and nothing outside this
  // module can hold it there. Only the stall is injected; what the check asserts is the real
  // outcome, one holder or two.
  reclaimPauseMs = 0,
): Promise<T> {
  const path = lockPath(sessionFile);
  const token = randomUUID();
  const deadline = Date.now() + waitMs;

  // The session directory may not exist yet. The first activity of a session is what creates it,
  // and it does that inside the lock.
  await mkdir(dirname(path), { recursive: true });

  for (;;) {
    try {
      await writeFile(path, held(token), { flag: "wx" });
      break;
    } catch (err) {
      const contended = await taken(path, token, reclaimPauseMs);
      if (Date.now() >= deadline) {
        // Only call it contention when it was. Anything else is the real error, and reporting a
        // writer that does not exist sends the next reader looking for one.
        throw contended ? new Error(`another writer has held ${path} for ${waitMs}ms`) : err;
      }
      // Always wait, including after clearing a stale lock. A failure that will not fix itself
      // would otherwise spin here for the whole deadline.
      await new Promise((resolve) => setTimeout(resolve, RETRY_MS));
    }
  }

  // Set the moment the refresher finds the lock is somebody else's, and never unset: a lock taken
  // away does not come back.
  let lost = false;
  // When this process last saw the lock file say the lock was ours. The flag above is not enough on
  // its own: it is set from inside a timer callback, and a process whose event loop stops does not
  // run timers, so a holder that was paused past STALE_MS and reclaimed comes back to a flag that
  // still says "mine". A clock read needs no event loop and no I/O, so it is the one thing a
  // synchronous caller can trust after a pause.
  let lastConfirmed = Date.now();

  // Stops as soon as the lock is not ours. A holder reclaimed while it was blocked would
  // otherwise keep the next holder's lock alive long after that one died.
  const refresh = setInterval(() => {
    void (async () => {
      let owner: { token?: string };
      try {
        owner = JSON.parse(await readFile(path, "utf8")) as { token?: string };
      } catch (err) {
        // Gone means reclaimed. Anything else is one read that failed, and the shared storage this
        // exists for is exactly where that happens. Stopping on it would let the lock age out from
        // under a holder that is still writing.
        if ((err as NodeJS.ErrnoException).code === "ENOENT") {
          lost = true;
          clearInterval(refresh);
        }
        return;
      }
      if (owner.token !== token) {
        lost = true;
        clearInterval(refresh);
        return;
      }
      lastConfirmed = Date.now();
      const now = new Date();
      await utimes(path, now, now).catch(() => {});
    })();
  }, REFRESH_MS);
  refresh.unref?.();

  // Read rather than trusting the refresher, which only notices on its next tick and so answers
  // for up to REFRESH_MS ago. A caller asking this is about to write.
  const owned = async () => {
    try {
      const mine = JSON.parse(await readFile(path, "utf8")) as { token?: string };
      return mine.token === token;
    } catch {
      return false;
    }
  };

  // For a writer that cannot await. Pi's append path is synchronous all the way down, so the one
  // place that is always immediately before a write cannot read the lock file.
  //
  // Two questions, both answered without I/O: has the refresher seen the lock taken, and has this
  // process been away long enough for it to have been. The second is what covers a stall, which is
  // the only way a live holder loses a lock it is still refreshing. It gives up before the age a
  // contender reclaims at rather than exactly at it, because the two are measured on two clocks.
  const ownedNow = () => !lost && Date.now() - lastConfirmed < STALE_MS - MARGIN_MS;

  try {
    return await body(owned, ownedNow);
  } finally {
    clearInterval(refresh);
    // Only our own. A lock reclaimed as stale belongs to whoever took it next.
    try {
      const mine = JSON.parse(await readFile(path, "utf8")) as { token?: string };
      if (mine.token === token) await rm(path, { force: true });
    } catch {
      // Gone, or unreadable. Either way it is not ours to remove.
    }
  }
}
