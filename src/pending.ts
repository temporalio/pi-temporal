// What a step knows about its tool calls before it is sealed: which ones a dispatch has started,
// and what the ones that finished produced. It lives beside the session file rather than in it,
// because a half-finished step has no place in the transcript the model reads, and because two
// calls settling at once would each parent their entry off the leaf they saw and branch the
// session tree. Keeping the results in separate files is what lets the seal append them in the
// order the model asked for.
//
// A dispatch claim outlives the result it produced, and the turn it was written in, because the
// attempt it guards against is one that stalled: it comes back after the answer is recorded and
// after the cleanup that follows, and nothing else on disk can then tell its call from one nothing
// has run yet.
//
// What a claim answers is whether to admit a second dispatch under that identity. It says nothing
// about what the first one did, and nothing about whether it is still running.

import { createHash, randomUUID } from "node:crypto";
import { mkdir, readdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import type { TurnToolCallOutcome } from "@earendil-works/pi-coding-agent";

const RESULT = ".json";
const STARTED = ".started";

// Scoped by turn and step, because a call id is only unique within the message that asked for it
// and a turn numbers its steps from one again. Either scope alone would let a later call find an
// earlier one's files: without the step, within a turn; without the turn, across turns.
const rootFor = (sessionFile: string) => `${sessionFile}.pending`;
// The turn's id comes from whoever submitted the prompt, so it is not necessarily a name a
// filesystem takes. Anything but a plain one is used by its digest rather than rewritten, because
// rewriting maps two ids to one directory and that is the collision this scope exists to stop.
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

/**
 * Where one step of one turn keeps what it knows. Exported for the checks: a check that restates
 * the layout reports a change to it as a broken fixture rather than as a failed assertion, and the
 * scope is the thing under test.
 */
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
 * Whether a dispatch had already started this call. A second dispatch that finds this must not
 * run the tool again: the first one can have pushed, written or deleted before it died.
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
 * Keep what a call produced until the step records it. Written whole and then renamed, so a crash
 * part way through leaves no half-file for the seal to read as a result.
 *
 * The dispatch note is what creates the step's directory and it is not removed, so the directory
 * is normally there. A keep that finds none belongs to a session somebody deleted underneath it,
 * and recreating the tree for it would leave files nothing ever reads.
 */
export async function keepResult(
  sessionFile: string,
  turn: string,
  step: number,
  callId: string,
  outcome: TurnToolCallOutcome,
): Promise<void> {
  const target = resultPath(sessionFile, turn, step, callId);
  // Unique per writer. An attempt whose startToClose expired is still running while its retry
  // writes, and one scratch path between them publishes a document that is neither.
  const scratch = `${target}.${randomUUID()}.writing`;
  try {
    await writeFile(scratch, JSON.stringify(outcome), "utf8");
    await rename(scratch, target);
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code !== "ENOENT") throw err;
  } finally {
    // Whatever went wrong above is what the caller needs to see, not what went wrong tidying up.
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

// Everything in a step's directory except what says a call was admitted.
async function dropResults(dir: string): Promise<void> {
  for (const entry of await readdir(dir).catch(() => [] as string[])) {
    if (entry.endsWith(STARTED)) continue;
    await rm(join(dir, entry), { recursive: true, force: true });
  }
}

/**
 * Drop what earlier steps kept. A step keeps its own results until the step after it, because a
 * seal that dropped them as it recorded them would leave a retry of that seal with nothing to
 * read, and a batch with no results reads as one that wants another step even when a tool asked
 * the turn to stop.
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
    // The results go; the dispatch notes stay. A note is what says the call was admitted, and
    // admission has to outlive the result it produced: an attempt that stalled before taking its
    // claim comes back long after the seal wrote the answer, and a swept directory made its call
    // look fresh, so it ran the tool a second time. An empty file per call is what keeping it
    // costs, and the turn scope is what stops one being read as a later call's.
    await dropResults(join(turnDir(sessionFile, turn), name));
  }
}
