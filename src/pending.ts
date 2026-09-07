// What a step knows about its tool calls before it is sealed: which ones a dispatch has started,
// and what the ones that finished produced. It lives beside the session file rather than in it,
// because a half-finished step has no place in the transcript the model reads, and because two
// calls settling at once would each parent their entry off the leaf they saw and branch the
// session tree.
//
// The dispatch note prevents a repeated side effect until the transcript settles the call.

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
): Promise<boolean> {
  await mkdir(dirFor(sessionFile, step), { recursive: true });
  try {
    await writeFile(dispatchPath(sessionFile, step, callId), "", { encoding: "utf8", flag: "wx" });
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "EEXIST") return false;
    throw error;
  }
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
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
    throw error;
  }
}

/**
 * Keep what a call produced until the step records it. Written whole and then renamed, so a crash
 * part way through leaves no half-file for the seal to read as a result.
 *
 * The dispatch note is what creates the step's directory, so this is only ever called after one.
 * A keep with no directory is a straggler whose turn is over, and it is dropped rather than
 * recreating a tree a later turn would then read.
 */
export async function keepResult(
  sessionFile: string,
  step: number,
  callId: string,
  outcome: TurnToolCallOutcome,
): Promise<void> {
  const target = resultPath(sessionFile, step, callId);
  // Unique per writer. An attempt whose startToClose expired is still running while its retry
  // writes, and one scratch path between them publishes a document that is neither.
  const scratch = `${target}.${randomUUID()}.writing`;
  try {
    await writeFile(scratch, JSON.stringify(outcome), "utf8");
    await rename(scratch, target);
  } catch (err) {
    // No directory means the turn this call belonged to is over and its files were swept. Writing
    // one back would leave it for a later turn to find, and a call id is only unique within the
    // message that asked for it. The dispatch note is what creates the directory.
    if ((err as NodeJS.ErrnoException).code !== "ENOENT") throw err;
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

/** Drop everything a session kept. A new turn numbers its steps from one again. */
export async function sweepAll(sessionFile: string): Promise<void> {
  await rm(rootFor(sessionFile), { recursive: true, force: true });
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
    const dir = join(rootFor(sessionFile), name);
    // The results go; the dispatch notes stay for the life of the session. A note is what says the
    // call was admitted, and admission has to outlive the result: an attempt that stalled before
    // taking its claim comes back long after the seal wrote the answer, and a swept directory made
    // its call look fresh, so it ran the tool a second time. A fifth review reproduced that. An
    // empty file per call is what keeping it costs, and `forget` takes them with the session.
    for (const entry of await readdir(dir).catch(() => [] as string[])) {
      if (entry.endsWith(STARTED)) continue;
      await rm(join(dir, entry), { recursive: true, force: true });
    }
  }
}
