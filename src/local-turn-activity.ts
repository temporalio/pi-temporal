// The activities behind piLocalTurn. None of them drives a session of its own: they find the turn
// the pi process is already holding and work on it, so the transcript, the events and the
// streaming stay pi's own. They run in the pi process, which is why the workflow pins them to that
// process's queue.
//
// A step's tool results are kept in memory here rather than beside the session file. The turn
// cannot outlive this process anyway (a workflow that comes back to a process that is gone gets
// TurnGone), so there is nothing a file would survive that the turn itself would.

import { ApplicationFailure, Context } from "@temporalio/activity";
import type { TurnSteps, TurnToolCallOutcome } from "@earendil-works/pi-coding-agent";
import { unknownToolCallOutcome } from "@earendil-works/pi-coding-agent";
import type {
  LocalModelCallResult,
  LocalSealInput,
  LocalStepInput,
  LocalToolCallInput,
  LocalTurnInput,
  ToolCallResult,
} from "./protocol.js";

/** A turn this process is holding, waiting for its workflow to say run. */
export interface LiveTurn {
  readonly run: () => Promise<void>;
  readonly steps: TurnSteps;
}

export type LiveTurns = Map<string, LiveTurn>;

/** What a stepped turn has done so far, so a retried activity does not do it twice. */
interface TurnProgress {
  recorded: boolean;
  readonly results: Map<string, TurnToolCallOutcome>;
}

export function makeLocalTurnActivities(live: LiveTurns) {
  const progress = new Map<string, TurnProgress>();

  const turnFor = (turnId: string): LiveTurn => {
    const turn = live.get(turnId);
    if (!turn) {
      // The process holding this turn is gone, or the turn already finished. Neither gets better
      // by trying again, and the turn is picked up when the session is opened next.
      throw ApplicationFailure.nonRetryable(`no live turn ${turnId}`, "TurnGone");
    }
    return turn;
  };

  const progressFor = (turnId: string): TurnProgress => {
    // A turn the process has let go of cannot come back, so what is remembered about it is dead
    // weight. An interrupted turn is the usual way one is let go of without sealing.
    for (const id of progress.keys()) {
      if (!live.has(id)) progress.delete(id);
    }
    const existing = progress.get(turnId);
    if (existing) return existing;
    const fresh: TurnProgress = { recorded: false, results: new Map() };
    progress.set(turnId, fresh);
    return fresh;
  };

  const heartbeating = async <T>(body: () => Promise<T>): Promise<T> => {
    const beat = setInterval(() => {
      try {
        Context.current().heartbeat();
      } catch {
        // outside an activity context (a unit test); ignore
      }
    }, 3000);
    beat.unref?.();
    try {
      return await body();
    } finally {
      clearInterval(beat);
    }
  };

  async function runLocalTurn(input: LocalTurnInput): Promise<void> {
    const turn = turnFor(input.turnId);
    await heartbeating(() => turn.run());
  }

  async function runLocalModelCall(input: LocalStepInput): Promise<LocalModelCallResult> {
    const turn = turnFor(input.turnId);
    const state = progressFor(input.turnId);
    return heartbeating(async () => {
      // Record before asking about the stop. A turn stopped between here and the executor being
      // handed it has a prompt the user typed and nothing holding it, and dropping it loses the
      // text with no error to show for it. Recorded and unanswered is a state resume handles.
      if (!state.recorded) {
        // Once. A retry that recorded again would put the prompt in the transcript twice.
        await turn.steps.record();
        state.recorded = true;
      }

      // An abort reaches the unit that was running and nothing else, so the loop has to stop
      // asking. Without this the next model call starts with a fresh signal and runs work the
      // user stopped.
      // What this turn kept is dropped when the process lets go of it, not here. Dropping it here
      // takes `recorded` with it, and a retry of this activity then records the prompt a second
      // time, which leaves the turn's own text sitting after all of its tool results.
      if (turn.steps.interrupted()) {
        return { calls: [], sequential: false, ended: true, interrupted: true };
      }

      const model = await turn.steps.modelCall();
      return {
        calls: model.toolCalls.map((call) => ({ id: call.id, name: call.name })),
        sequential: model.sequential,
        ended: model.ended,
      };
    });
  }

  async function runLocalToolCall(input: LocalToolCallInput): Promise<ToolCallResult> {
    const turn = turnFor(input.turnId);
    const state = progressFor(input.turnId);
    return heartbeating(async () => {
      if (state.results.has(input.call.id)) return { outcome: "already-settled" };

      const outcome = await turn.steps.runToolCall(input.call.id);
      if (!outcome) return { outcome: "already-settled" };

      state.results.set(input.call.id, outcome);
      return { outcome: "settled" };
    });
  }

  async function runLocalSeal(input: LocalSealInput): Promise<{ done: boolean }> {
    const turn = turnFor(input.turnId);
    const state = progressFor(input.turnId);
    return heartbeating(async () => {
      const results = input.calls.map(
        // A call with nothing kept for it is one whose dispatch never came back. Sealing without
        // it would leave a call no result answers, which the next model call cannot be made from.
        (call) => state.results.get(call.id) ?? unknownToolCallOutcome(call),
      );
      // Named here too. The live half is where an extension can append between the model call and
      // the seal, so it is the half with the exposure.
      const sealed = await turn.steps.sealStep(results, {
        expectCalls: input.calls.map((call) => call.id),
        postRun: !input.interrupted,
      });
      for (const call of input.calls) state.results.delete(call.id);
      if (sealed.done) progress.delete(input.turnId);
      return sealed;
    });
  }

  return { runLocalTurn, runLocalModelCall, runLocalToolCall, runLocalSeal };
}

export type LocalTurnActivities = ReturnType<typeof makeLocalTurnActivities>;
