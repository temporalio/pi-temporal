// What a step knows about its tool calls before it is sealed: which ones a dispatch has started,
// and what the ones that finished produced. It lives beside the session file rather than in it,
// because a half-finished step has no place in the transcript the model reads, and because two
// calls settling at once would each parent their entry off the leaf they saw and branch the
// session tree.
//
// A dropped file only ever costs work, never correctness: a missing note makes a dispatch look
// fresh, and a missing result makes a call look unrun. Both are the safe direction for a caller
// that checks the transcript first.

import { mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import type { TurnToolCallOutcome } from "@earendil-works/pi-coding-agent";

const dirFor = (sessionFile: string) => `${sessionFile}.pending`;
const resultPath = (sessionFile: string, callId: string) => join(dirFor(sessionFile), `${callId}.json`);
const dispatchPath = (sessionFile: string, callId: string) => join(dirFor(sessionFile), `${callId}.started`);

/** Record that a dispatch is about to run the tool, before it can have any effect. */
export async function noteDispatch(sessionFile: string, callId: string): Promise<void> {
  await mkdir(dirFor(sessionFile), { recursive: true });
  await writeFile(dispatchPath(sessionFile, callId), "", "utf8");
}

/**
 * Whether a dispatch had already started this call. A second dispatch that finds this must not
 * run the tool again: the first one can have pushed, written or deleted before it died.
 */
export async function wasDispatched(sessionFile: string, callId: string): Promise<boolean> {
  try {
    await readFile(dispatchPath(sessionFile, callId), "utf8");
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
  callId: string,
  outcome: TurnToolCallOutcome,
): Promise<void> {
  await mkdir(dirFor(sessionFile), { recursive: true });
  const target = resultPath(sessionFile, callId);
  const scratch = `${target}.writing`;
  await writeFile(scratch, JSON.stringify(outcome), "utf8");
  await rename(scratch, target);
}

/** What a call produced, or undefined when nothing kept a result for it. */
export async function readResult(
  sessionFile: string,
  callId: string,
): Promise<TurnToolCallOutcome | undefined> {
  try {
    return JSON.parse(await readFile(resultPath(sessionFile, callId), "utf8")) as TurnToolCallOutcome;
  } catch {
    return undefined;
  }
}

/** Drop what is kept for calls the step has recorded. */
export async function forget(sessionFile: string, callIds: readonly string[]): Promise<void> {
  for (const callId of callIds) {
    await rm(resultPath(sessionFile, callId), { force: true });
    await rm(dispatchPath(sessionFile, callId), { force: true });
  }
}
