// One writer at a time for a session file. Pi's session file is a tree, and every entry takes its
// parent from the leaf its writer last saw, so two writers do not corrupt it, they branch it. The
// step's own calls never write, but two attempts of the same activity can: a heartbeat that stalls
// long enough for Temporal to start attempt 2 leaves attempt 1 alive and both appending.
//
// Advisory, and beside the session file, so it works wherever the session file works. A holder
// that dies is reclaimed on age, which is why the lock is refreshed while it is held.

import { mkdir, readFile, rm, stat, utimes, writeFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import { hostname } from "node:os";
import { dirname } from "node:path";

// Comfortably longer than the refresh, so a slow filesystem does not look like a dead holder.
const STALE_MS = 20_000;
const REFRESH_MS = 3_000;
const RETRY_MS = 250;

const lockPath = (sessionFile: string) => `${sessionFile}.lock`;

const held = (token: string) => JSON.stringify({ token, host: hostname(), pid: process.pid });

async function taken(path: string): Promise<boolean> {
  try {
    const info = await stat(path);
    if (Date.now() - info.mtimeMs < STALE_MS) return true;
    // The holder stopped refreshing, so it is gone. Reclaiming is safe: whatever it was writing
    // is a lost attempt Temporal has already given up on.
    await rm(path, { force: true });
    return false;
  } catch {
    return false;
  }
}

/**
 * Run `body` as the only writer of this session file. Throws if the lock cannot be taken in time,
 * which is a retryable condition: the holder is another attempt that is still working.
 */
export async function withSessionLock<T>(
  sessionFile: string,
  body: () => Promise<T>,
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
      const contended = await taken(path);
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

  const refresh = setInterval(() => {
    const now = new Date();
    void utimes(path, now, now).catch(() => {});
  }, REFRESH_MS);
  refresh.unref?.();

  try {
    return await body();
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
