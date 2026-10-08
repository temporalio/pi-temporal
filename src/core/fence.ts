// Orders the writers of one session file. Temporal already knows which attempt of which Activity
// is current, so the Workflow numbers every Activity it schedules, and each attempt adds its own
// number. A unit takes its token beside the file and stops writing once a higher one is there.
// A retry takes over without waiting for a lease to expire.
//
// The number is `<run start ms>.<Activity seq>.<attempt>`, so a later run, a later Activity of a
// run, and a later attempt all sort higher. A new run’s start time must be later than the old
// run’s, which the server’s clock gives unless it goes back by more than a whole run.
//
// The guard checks the fence before each append. A writer that stalls after the check
// can still land that one append. On NFS, mount with `actimeo=0` (at least `acdirmin=0` and
// `acdirmax=0`), or a cached listing can hide a newer number for up to a minute.

import { readdirSync } from "node:fs";
import { mkdir, readdir, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { ApplicationFailure } from "@temporalio/common";

/** The failure type of a unit that found a newer one already writing. Not retried. */
export const SUPERSEDED = "Superseded";

const dirOf = (sessionFile: string) => `${sessionFile}.fence`;
const TOKEN = /^\d{13}\.\d{8}\.\d{6}$/;
const pad = (n: number, width: number) => String(n).padStart(width, "0");

/** The whole fence, with the attempt that runs it. */
export const fenceToken = (prefix: string, attempt: number) => `${prefix}.${pad(attempt, 6)}`;

const newestOf = (names: readonly string[]) =>
  names.filter((name) => TOKEN.test(name)).sort().at(-1);

/**
 * Take `token` for writing `sessionFile`. Throws `SUPERSEDED` when a higher one is already there.
 * Returns the write guard, which throws once a higher one appears. Synchronous, since Pi asks it
 * right before each append.
 */
export async function takeFence(sessionFile: string, token: string): Promise<() => void> {
  if (!TOKEN.test(token)) throw new Error(`not a fence: ${token}`);
  const dir = dirOf(sessionFile);
  await mkdir(dir, { recursive: true });
  // Taking the same token twice is the same attempt, so it's not a conflict.
  await writeFile(join(dir, token), "", { flag: "wx" }).catch((err: NodeJS.ErrnoException) => {
    if (err.code !== "EEXIST") throw err;
  });
  const names = await readdir(dir);
  const newest = newestOf(names);
  if (newest !== undefined && newest > token) {
    throw ApplicationFailure.nonRetryable(
      `${sessionFile} is already written by ${newest}, which is newer than ${token}`,
      SUPERSEDED,
    );
  }
  // Only lower ones, which can no longer write. A higher one is never ours to remove.
  for (const name of names) {
    if (TOKEN.test(name) && name < token) await rm(join(dir, name), { force: true });
  }
  // A listing that fails refuses the write too. The retry costs a model call, which is cheaper than
  // a write nobody could order.
  return () => {
    const now = newestOf(readdirSync(dir));
    if (now !== undefined && now > token) {
      throw new Error(`${sessionFile} is now written by ${now}, which is newer than ${token}`);
    }
  };
}
