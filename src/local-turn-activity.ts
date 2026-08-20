// The activity behind piLocalTurn. It does not drive a session of its own: it finds the turn the
// pi process is already holding and runs it, so the transcript, the events and the streaming stay
// pi's own. Runs in the pi process, which is why the workflow pins it to that process's queue.

import { ApplicationFailure, Context } from "@temporalio/activity";
import type { LocalTurnInput } from "./protocol.js";

/** Turns this process is holding, waiting for their workflow to say run. */
export type LiveTurns = Map<string, { run: () => Promise<void> }>;

export function makeLocalTurnActivities(live: LiveTurns) {
  async function runLocalTurn(input: LocalTurnInput): Promise<void> {
    const turn = live.get(input.turnId);
    if (!turn) {
      // The process holding this turn is gone, or the turn already finished. Neither gets better
      // by trying again, and the turn is picked up when the session is opened next.
      throw ApplicationFailure.nonRetryable(`no live turn ${input.turnId}`, "TurnGone");
    }

    const beat = setInterval(() => {
      try {
        Context.current().heartbeat();
      } catch {
        // outside an activity context (a unit test); ignore
      }
    }, 3000);
    beat.unref?.();

    try {
      await turn.run();
    } finally {
      clearInterval(beat);
    }
  }

  return { runLocalTurn };
}
