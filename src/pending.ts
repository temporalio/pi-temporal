// What a step knows about its tool calls before it is sealed: which ones a dispatch has started,
// and what the ones that finished produced. It lives beside the session file rather than in it,
// because a half-finished step has no place in the transcript the model reads, and because two
// calls settling at once would each parent their entry off the leaf they saw and branch the
// session tree.
//
// A dropped file only ever costs work, never correctness: a missing note makes a dispatch look
// fresh, and a missing result makes a call look unrun. Both are the safe direction for a caller
// that checks the transcript first.

import { randomUUID } from "node:crypto";
import { mkdir, readdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import type { TurnToolCallOutcome } from "@earendil-works/pi-coding-agent";

const RESULT = ".json";
const STARTED = ".started";

// Scoped by step, because a call id is only unique within the message that asked for it. A
// provider that reuses one across a session would otherwise have a later call find an earlier
// step's result and skip the tool.
const rootFor = (sessionFile: string) => `${sessionFile}.pending`;
const dirFor = (sessionFile: string, step: number) => join(rootFor(sessionFile), String(step));
const resultPath = (sessionFile: string, step: number, callId: string) =>
  join(dirFor(sessionFile, step), `${callId}${RESULT}`);
const dispatchPath = (sessionFile: string, step: number, callId: string) =>
  join(dirFor(sessionFile, step), `${callId}${STARTED}`);

/** Record that a dispatch is about to run the tool, before it can have any effect. */
export async function noteDispatch(
  sessionFile: string,
  step: number,
  callId: string,
): Promise<void> {
  await mkdir(dirFor(sessionFile, step), { recursive: true });
  await writeFile(dispatchPath(sessionFile, step, callId), "", "utf8");
}

/**
 * Whether a dispatch had already started this call. A second dispatch that finds this must not
 * run the tool again: the first one can have pushed, written or deleted before it died.
 */
export async function wasDispatched(
  sessionFile: string,
  step: number,
  callId: string,
): Promise<boolean> {
  try {
    await readFile(dispatchPath(sessionFile, step, callId), "utf8");
    return true;
  } catch {
    return false;
  }
}

/**
 * Keep what a call produced until the step records it. Written whole and then renamed, so a
 * crash part way through leaves no half-file for the seal to read as a result.
 */
export async function keepResult(
  sessionFile: string,
  step: number,
  callId: string,
  outcome: TurnToolCallOutcome,
): Promise<void> {
  await mkdir(dirFor(sessionFile, step), { recursive: true });
  const target = resultPath(sessionFile, step, callId);
  // Unique per writer. An attempt whose startToClose expired is still running while its retry
  // writes, and one scratch path between them publishes a document that is neither.
  const scratch = `${target}.${randomUUID()}.writing`;
  try {
    await writeFile(scratch, JSON.stringify(outcome), "utf8");
    await rename(scratch, target);
  } finally {
    // Whatever went wrong above is what the caller needs to see, not what went wrong tidying up.
    await rm(scratch, { force: true }).catch(() => {});
  }
}

/** What a call produced, or undefined when nothing kept a result for it. */
export async function readResult(
  sessionFile: string,
  step: number,
  callId: string,
): Promise<TurnToolCallOutcome | undefined> {
  try {
    const kept = await readFile(resultPath(sessionFile, step, callId), "utf8");
    return JSON.parse(kept) as TurnToolCallOutcome;
  } catch {
    return undefined;
  }
}

/** Drop what is kept for the given calls of one step. */
export async function forget(
  sessionFile: string,
  step: number,
  callIds: readonly string[],
): Promise<void> {
  for (const callId of callIds) {
    await rm(resultPath(sessionFile, step, callId), { force: true });
    await rm(dispatchPath(sessionFile, step, callId), { force: true });
  }
}

/**
 * Drop what earlier steps kept. A step keeps its own results until the step after it, because a
 * seal that dropped them as it recorded them would leave a retry of that seal with nothing to
 * read, and a batch with no results reads as one that wants another step even when a tool asked
 * the turn to stop.
 */
export async function sweep(sessionFile: string, before: number): Promise<void> {
  let steps: string[];
  try {
    steps = await readdir(rootFor(sessionFile));
  } catch {
    return;
  }
  for (const name of steps) {
    const step = Number(name);
    if (!Number.isInteger(step) || step >= before) continue;
    await rm(join(rootFor(sessionFile), name), { recursive: true, force: true });
  }
}
