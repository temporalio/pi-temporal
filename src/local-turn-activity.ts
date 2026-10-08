// Activities behind `piLocalTurn`. They run in the pi process and act on the turn it already
// holds, so transcript, events and streaming stay pi's own.
//
// Tool results are kept in memory, not on disk. A crash of this process loses results the seal
// had not recorded. Reopening the session resumes from the transcript.

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
import { TURN_STOPPED } from "./protocol.js";

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

// No Activity around it when a check calls it directly.
const cancellation = (): { signal?: AbortSignal } => {
  try {
    return { signal: Context.current().cancellationSignal };
  } catch {
    return {};
  }
};

export function makeLocalTurnActivities(live: LiveTurns) {
  const progress = new Map<string, TurnProgress>();

  const turnFor = (turnId: string): LiveTurn => {
    const turn = live.get(turnId);
    if (!turn) {
      // Process gone or turn finished. Retrying won't help. The next session open picks it up.
      throw ApplicationFailure.nonRetryable(`no live turn ${turnId}`, "TurnGone");
    }
    return turn;
  };

  const progressFor = (turnId: string): TurnProgress => {
    // Drop progress for turns the process let go of (usually interrupted ones).
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
      // Record before checking for a stop, or an early stop loses the user's prompt. Resume
      // handles a recorded, unanswered prompt.
      if (!state.recorded) {
        // Once. A retry that recorded again would duplicate the prompt.
        await turn.steps.record();
        state.recorded = true;
      }

      // An abort only reaches the running unit, so check here or the next model call runs anyway.
      // Keep `progress` here. Dropping it loses `recorded`, and a retry would record the prompt
      // again after the tool results.
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

      let outcome: TurnToolCallOutcome | undefined;
      try {
        // A cancelled Activity stops the tool like a user stop, so it reports what it did.
        outcome = await turn.steps.runToolCall(input.call.id, cancellation());
      } catch (err) {
        // Don't retry on a stopped turn. The seal reports the call as unknown.
        if (turn.steps.interrupted()) {
          throw ApplicationFailure.nonRetryable(`turn ${input.turnId} was stopped`, TURN_STOPPED);
        }
        throw err;
      }
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
        // Every call needs a result, or the next model call has an invalid transcript.
        (call) => state.results.get(call.id) ?? unknownToolCallOutcome(call),
      );
      // Extensions can append between the model call and the seal, so name the expected calls.
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
