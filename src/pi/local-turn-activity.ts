// A live turn stays in the Pi process so it can use Pi's transcript and streaming events.
//
// Unsealed tool results live in memory. A process crash loses them, and reopening the session
// resumes from the transcript.

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
} from "../core/protocol.js";
import { TURN_STOPPED } from "../core/protocol.js";

/** A turn this process is holding, waiting for its Workflow to say run. */
export interface LiveTurn {
  readonly run: () => Promise<void>;
  readonly steps: TurnSteps;
}

export type LiveTurns = Map<string, LiveTurn>;

/** What a stepped turn has done so far, so a retried Activity does not do it twice. */
interface TurnProgress {
  recorded: boolean;
  readonly results: Map<string, TurnToolCallOutcome>;
  // Retries join the same unit so they don't bill another model call or repeat a tool's effect.
  // Failed units are forgotten so another attempt can run them.
  readonly units: Map<string, Promise<unknown>>;
}

const cancellation = (): { signal: AbortSignal } => ({
  signal: Context.current().cancellationSignal,
});

export function makeLocalTurnActivities(live: LiveTurns) {
  const progress = new Map<string, TurnProgress>();

  const turnFor = (turnId: string): LiveTurn => {
    const turn = live.get(turnId);
    if (!turn) {
      // Retrying cannot recover a missing live turn. Reopening the session uses its transcript.
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
    const fresh: TurnProgress = { recorded: false, results: new Map(), units: new Map() };
    progress.set(turnId, fresh);
    return fresh;
  };

  // These Activities run only in the process that holds the turn, so an attempt's completion can
  // be lost while the work itself finished here. Temporal then retries, and this joins the
  // attempt that's already running or done.
  const once = <T>(state: TurnProgress, unit: string, body: () => Promise<T>): Promise<T> => {
    const known = state.units.get(unit);
    if (known) return known as Promise<T>;
    const attempt = body();
    state.units.set(unit, attempt);
    attempt.catch(() => {
      if (state.units.get(unit) === attempt) state.units.delete(unit);
    });
    return attempt;
  };

  const heartbeating = async <T>(body: () => Promise<T>): Promise<T> => {
    const beat = setInterval(() => Context.current().heartbeat(), 3000);
    beat.unref?.();
    try {
      return await body();
    } finally {
      clearInterval(beat);
    }
  };

  async function runLocalTurn(input: LocalTurnInput): Promise<void> {
    const turn = turnFor(input.turnId);
    const state = progressFor(input.turnId);
    await heartbeating(() => once(state, "turn", () => turn.run()));
  }

  async function runLocalModelCall(input: LocalStepInput): Promise<LocalModelCallResult> {
    const turn = turnFor(input.turnId);
    const state = progressFor(input.turnId);
    return heartbeating(() => once(state, `model:${input.step}`, async () => {
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

      // A cancelled Activity stops the provider request, so a stopped turn isn't billed on.
      const model = await turn.steps.modelCall(cancellation());
      return {
        calls: model.toolCalls.map((call) => ({ id: call.id, name: call.name })),
        sequential: model.sequential,
        ended: model.ended,
      };
    }));
  }

  async function runLocalToolCall(input: LocalToolCallInput): Promise<ToolCallResult> {
    const turn = turnFor(input.turnId);
    const state = progressFor(input.turnId);
    const unit = `tool:${input.step}:${input.call.id}`;
    return heartbeating(() => once(state, unit, async (): Promise<ToolCallResult> => {
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
    }));
  }

  async function runLocalSeal(input: LocalSealInput): Promise<{ done: boolean }> {
    const turn = turnFor(input.turnId);
    const state = progressFor(input.turnId);
    return heartbeating(() => once(state, `seal:${input.step}`, async () => {
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
    }));
  }

  return { runLocalTurn, runLocalModelCall, runLocalToolCall, runLocalSeal };
}

export type LocalTurnActivities = ReturnType<typeof makeLocalTurnActivities>;
