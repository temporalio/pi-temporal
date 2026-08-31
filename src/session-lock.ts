// One writer at a time for a session file. Pi's session file is a tree, and every entry takes its
// parent from the leaf its writer last saw, so two writers do not corrupt it, they branch it. The
// step's own calls never write, but two attempts of the same activity can: a heartbeat that stalls
// long enough for Temporal to start attempt 2 leaves attempt 1 alive and both appending.
//
// Advisory, and beside the session file, so it works wherever the session file works. A holder
// that dies is reclaimed on age, which is why the lock is refreshed while it is held.

import { mkdir, readFile, rename, rm, stat, utimes, writeFile } from "node:fs/promises";
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
const STALE_MS = 60_000;
const REFRESH_MS = 3_000;
const RETRY_MS = 250;

// mtime comes from whichever host last touched the file, so a skewed clock reads a live lock as
// dead. The exclusive create is what actually excludes, which needs a filesystem where O_EXCL is
// atomic: local disk, NFSv4, SMB. It is not reliable on NFSv3.

const lockPath = (sessionFile: string) => `${sessionFile}.lock`;

const held = (token: string) => JSON.stringify({ token, host: hostname(), pid: process.pid });

// Reclaims by renaming out of the way rather than removing. Two contenders deciding the same lock
// is stale at the same moment then move the same file, and only one of them wins.
//
// It does not make reclaiming safe. `rename` acts on the path, not on the file that was measured,
// so a contender slow between reading the age and reclaiming moves whatever is there by then,
// including a lock somebody else has just taken. Nothing a lock beside a file can do closes that.
// `owned()` is the answer, and it is why writers ask again on the way to the write.
async function taken(path: string, token: string): Promise<boolean> {
  try {
    const info = await stat(path);
    if (Date.now() - info.mtimeMs < STALE_MS) return true;
    const corpse = `${path}.stale.${token}`;
    await rename(path, corpse);
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
 * `body` is handed an `owned()` it should call immediately before it writes. See STALE_MS.
 */
export async function withSessionLock<T>(
  sessionFile: string,
  body: (owned: () => Promise<boolean>) => Promise<T>,
  waitMs = 60_000,
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
      const contended = await taken(path, token);
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
        if ((err as NodeJS.ErrnoException).code === "ENOENT") clearInterval(refresh);
        return;
      }
      if (owner.token !== token) {
        clearInterval(refresh);
        return;
      }
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

  try {
    return await body(owned);
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
