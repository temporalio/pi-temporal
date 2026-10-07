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
import type { TurnToolCallOutcome } from "@earendil-works/pi-coding-agent";

const RESULT = ".json";
const STARTED = ".started";

// Scoped by turn and step. Call ids are unique only per message, and steps restart at one per turn.
const rootFor = (sessionFile: string) => `${sessionFile}.pending`;
// Turn ids come from the submitter. Unsafe names are hashed, not rewritten, so two ids never share
// a directory.
const keyFor = (turn: string) =>
  /^[A-Za-z0-9._-]{1,64}$/.test(turn) && turn !== "." && turn !== ".."
    ? turn
    : createHash("sha256").update(turn).digest("hex").slice(0, 32);
const turnDir = (sessionFile: string, turn: string) => join(rootFor(sessionFile), keyFor(turn));
const dirFor = (sessionFile: string, turn: string, step: number) =>
  join(turnDir(sessionFile, turn), String(step));
const resultPath = (sessionFile: string, turn: string, step: number, callId: string) =>
  join(dirFor(sessionFile, turn, step), `${callId}${RESULT}`);
const dispatchPath = (sessionFile: string, turn: string, step: number, callId: string) =>
  join(dirFor(sessionFile, turn, step), `${callId}${STARTED}`);

/** Exported for the checks, so they use the real layout instead of restating it. */
export const stepDirFor = dirFor;

/** Record that a dispatch is about to run the tool, before it can have any effect. */
export async function noteDispatch(
  sessionFile: string,
  turn: string,
  step: number,
  callId: string,
): Promise<boolean> {
  await mkdir(dirFor(sessionFile, turn, step), { recursive: true });
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
export async function wasDispatched(
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
  outcome: TurnToolCallOutcome,
): Promise<void> {
  const target = resultPath(sessionFile, turn, step, callId);
  // Unique per writer. A timed-out attempt may still be writing while its retry writes too.
  const scratch = `${target}.${randomUUID()}.writing`;
  try {
    await writeFile(scratch, JSON.stringify(outcome), "utf8");
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
): Promise<TurnToolCallOutcome | undefined> {
  try {
    const kept = await readFile(resultPath(sessionFile, turn, step, callId), "utf8");
    return JSON.parse(kept) as TurnToolCallOutcome;
  } catch {
    return undefined;
  }
}

/** Drop the results kept for the given calls of one step. Their notes stay: see `sweep`. */
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

/** Drop every result a session kept, from this turn and any before it. Their notes stay. */
export async function sweepResults(sessionFile: string): Promise<void> {
  let turns: string[];
  try {
    turns = await readdir(rootFor(sessionFile));
  } catch {
    return;
  }
  for (const turn of turns) {
    const dir = join(rootFor(sessionFile), turn);
    for (const step of await readdir(dir).catch(() => [] as string[])) {
      await dropResults(join(dir, step));
    }
  }
}

// Everything in a step's directory except the dispatch notes.
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
    // Results go, dispatch notes stay. A stalled attempt can return after the seal, and without
    // its note the call looks fresh and the tool runs twice.
    await dropResults(join(turnDir(sessionFile, turn), name));
  }
}
