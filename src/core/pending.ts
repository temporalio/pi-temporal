// Per-step tool call state before the seal: which calls a dispatch started, and what finished ones
// produced. Kept beside the session file, one file per call, so concurrent calls don't branch the
// session tree and the seal can append results in the model's order.
//
// Dispatch claims outlive their results and their turn. A stalled attempt can come back after the
// seal and cleanup, and the claim is the only thing that stops it running the tool again. A claim
// says nothing about what the first dispatch did or whether it is still running.

import { createHash, randomUUID } from "node:crypto";
import { mkdir, readdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import type { ToolOutcome } from "./agent.js";

const RESULT = ".json";
const STARTED = ".started";

// Scoped by turn and step. Call ids are unique only per message, and steps restart at one per turn.
const rootFor = (sessionFile: string) => `${sessionFile}.pending`;
// Turn ids come from the submitter. Unsafe names are hashed, not rewritten, so two ids never share
// a directory. `~` isn't in the direct alphabet, so a hash can't be passed off as a direct id.
const keyFor = (turn: string) =>
  /^[A-Za-z0-9._-]{1,64}$/.test(turn) && turn !== "." && turn !== ".."
    ? turn
    : `~${createHash("sha256").update(turn).digest("hex").slice(0, 32)}`;
const turnDir = (sessionFile: string, turn: string) => join(rootFor(sessionFile), keyFor(turn));
const dirFor = (sessionFile: string, turn: string, step: number) =>
  join(turnDir(sessionFile, turn), String(step));
// Provider IDs must stay within one file, so a claim can't overwrite another call's state. An
// ID that can't be a file name is hashed, the same as a turn, so the call still gets a claim.
// `~` keeps a hash apart from any ID a provider sends as it is.
const callKey = (callId: string) =>
  callId && !/[/\\\0]/.test(callId) && Buffer.byteLength(callId) <= 200 && !callId.startsWith("~")
    ? callId
    : `~${createHash("sha256").update(callId).digest("hex").slice(0, 32)}`;
const resultPath = (sessionFile: string, turn: string, step: number, callId: string) =>
  join(dirFor(sessionFile, turn, step), `${callKey(callId)}${RESULT}`);
const dispatchPath = (sessionFile: string, turn: string, step: number, callId: string) =>
  join(dirFor(sessionFile, turn, step), `${callKey(callId)}${STARTED}`);

/** Exported for the checks, so they use the real layout instead of restating it. */
export const stepDirFor = dirFor;

/** Record that a dispatch is about to run the tool, before it can have any effect. */
export async function claimDispatch(
  sessionFile: string,
  turn: string,
  step: number,
  callId: string,
): Promise<boolean> {
  // Owner only. Kept results hold tool output until the seal writes it to the session.
  await mkdir(dirFor(sessionFile, turn, step), { recursive: true, mode: 0o700 });
  try {
    await writeFile(dispatchPath(sessionFile, turn, step, callId), "", {
      encoding: "utf8",
      flag: "wx",
    });
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "EEXIST") return false;
    throw error;
  }
}

/**
 * Whether a dispatch already started this call. If so, do not run the tool again. The first one
 * may have had side effects before it died.
 */
export async function dispatchClaimed(
  sessionFile: string,
  turn: string,
  step: number,
  callId: string,
): Promise<boolean> {
  try {
    await readFile(dispatchPath(sessionFile, turn, step, callId), "utf8");
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
    throw error;
  }
}

/**
 * Keep a call's result until the seal records it. Written then renamed, so a crash leaves no
 * half-file. A missing step directory means the session was deleted, so the result is dropped.
 */
export async function keepResult(
  sessionFile: string,
  turn: string,
  step: number,
  callId: string,
  outcome: ToolOutcome,
): Promise<void> {
  const target = resultPath(sessionFile, turn, step, callId);
  // Unique per writer. A timed-out attempt may still be writing while its retry writes too.
  const scratch = `${target}.${randomUUID()}.writing`;
  try {
    await writeFile(scratch, JSON.stringify(outcome), { encoding: "utf8", mode: 0o600 });
    await rename(scratch, target);
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code !== "ENOENT") throw err;
  } finally {
    // Don't let a cleanup error hide the real one.
    await rm(scratch, { force: true }).catch(() => {});
  }
}

/** What a call produced, or undefined when nothing kept a result for it. */
export async function readResult(
  sessionFile: string,
  turn: string,
  step: number,
  callId: string,
): Promise<ToolOutcome | undefined> {
  // Only a missing result is no result. Read as missing, an unreadable one would make the seal
  // record unknown over a real result.
  let kept: string;
  try {
    kept = await readFile(resultPath(sessionFile, turn, step, callId), "utf8");
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw err;
  }
  return JSON.parse(kept) as ToolOutcome;
}

/** Drop the results kept for the given calls of one step. Their claims stay: see `sweep`. */
export async function forgetResults(
  sessionFile: string,
  turn: string,
  step: number,
  callIds: readonly string[],
): Promise<void> {
  for (const callId of callIds) {
    await rm(resultPath(sessionFile, turn, step, callId), { force: true });
  }
}

/** Drop every result a session kept, from this turn and any before it. Their claims stay. */
export async function sweepResults(
  sessionFile: string,
  // Called before each step's delete. A caller under a lease passes a check that throws once the
  // lease is gone, so a stalled sweep can't delete a newer session's results.
  beforeDelete: () => Promise<void> = async () => {},
): Promise<void> {
  let turns: string[];
  try {
    turns = await readdir(rootFor(sessionFile));
  } catch {
    return;
  }
  for (const turn of turns) {
    const dir = join(rootFor(sessionFile), turn);
    for (const step of await readdir(dir).catch(() => [] as string[])) {
      await beforeDelete();
      await dropResults(join(dir, step));
    }
  }
}

// Everything in a step's directory except the dispatch claims.
async function dropResults(dir: string): Promise<void> {
  for (const entry of await readdir(dir).catch(() => [] as string[])) {
    if (entry.endsWith(STARTED)) continue;
    await rm(join(dir, entry), { recursive: true, force: true });
  }
}

/**
 * Drop what earlier steps kept. A step keeps its results until the next step, so a retried seal
 * still has them. Without them, a batch reads as wanting another step even when a tool asked the
 * turn to stop.
 */
export async function sweep(sessionFile: string, turn: string, before: number): Promise<void> {
  let steps: string[];
  try {
    steps = await readdir(turnDir(sessionFile, turn));
  } catch {
    return;
  }
  for (const name of steps) {
    const step = Number(name);
    if (!Number.isInteger(step) || step >= before) continue;
    // Results go, dispatch claims stay. A stalled attempt can return after the seal, and without
    // its claim the call looks fresh and the tool runs twice.
    await dropResults(join(turnDir(sessionFile, turn), name));
  }
}
