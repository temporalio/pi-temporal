// One step of a turn, driven as a model call, one activity per tool call, and a seal. The
// whole-step mode runs the same step inside a single activity; this one puts the workflow between
// the model asking for a tool and the tool running, which is where a per-tool retry policy, a
// per-tool timeout and a budget can live.
//
// The seal is the step's only writer. Two calls settling at once would each parent their entry off
// the leaf they saw and branch the session tree, so a call reports its result and the seal records
// them together, in the order the model asked.
//
// Sandbox-safe: no SDK imports and no Node builtins, so the workflow bundle can hold it. What it
// needs from the SDK is injected.

import type {
  DeferredToolCall,
  ModelCallResult,
  RunStepInput,
  RunStepResult,
  SealStepInput,
  ToolCallInput,
  ToolCallOutcome,
  ToolCallResult,
} from "./protocol.js";

export interface SteppedActivities {
  runModelCall(input: RunStepInput): Promise<ModelCallResult>;
  runToolCall(input: ToolCallInput): Promise<ToolCallResult>;
  sealStep(input: SealStepInput): Promise<RunStepResult>;
}

export interface SteppedStepDeps {
  readonly activities: SteppedActivities;
  readonly isCancellation: (err: unknown) => boolean;
  // The workflow's logger, so what a step could not settle is said where an operator reads about
  // the turn. History records an activity that succeeded; only the dispatch knows what it decided.
  readonly log?: (message: string, attributes: Record<string, unknown>) => void;
}

/** What one dispatch decided, or what stopped it. */
interface Dispatched {
  readonly call: DeferredToolCall;
  readonly outcome?: ToolCallOutcome;
  readonly error?: unknown;
}

/** What a model call reported, whichever activity made it. */
export interface StepCalls {
  readonly calls: readonly DeferredToolCall[];
  readonly sequential: boolean;
  readonly ended: boolean;
}

/**
 * Run the calls of one step, and report what could not be settled. Shared by the worker-owned
 * path and the turn a live pi process holds, because how a step's calls are fanned out and how an
 * interrupt reaches them is the same question in both.
 */
export async function dispatchStepCalls(
  step: number,
  model: StepCalls,
  runToolCall: (call: DeferredToolCall) => Promise<ToolCallResult>,
  deps: Pick<SteppedStepDeps, "isCancellation" | "log">,
): Promise<void> {
  // Errors are carried rather than thrown, so one call that ran out of retries does not leave its
  // siblings' promises rejecting with nobody to catch them.
  const dispatch = async (call: DeferredToolCall): Promise<Dispatched> => {
    try {
      const { outcome } = await runToolCall(call);
      return { call, outcome };
    } catch (error) {
      return { call, error };
    }
  };

  const dispatched: Dispatched[] = [];
  if (!model.ended) {
    if (model.sequential) {
      // A tool of this step says the batch runs in order, and an interrupt stops the rest of it.
      for (const call of model.calls) {
        const outcome = await dispatch(call);
        dispatched.push(outcome);
        if (outcome.error !== undefined && deps.isCancellation(outcome.error)) break;
      }
    } else {
      dispatched.push(...(await Promise.all(model.calls.map(dispatch))));
    }
  }

  // An interrupt is not a failed tool. Sealing after one would close a step the user stopped, and
  // settle calls whose dispatch is the thing that was cancelled.
  const stopped = dispatched.find((d) => d.error !== undefined && deps.isCancellation(d.error));
  if (stopped) throw stopped.error;

  const unsettled = dispatched.filter((d) => d.outcome !== "settled" && d.outcome !== "already-settled");
  if (unsettled.length > 0) {
    deps.log?.("step did not settle every call it dispatched", {
      step,
      calls: unsettled.map((d) => ({
        call: d.call.id,
        tool: d.call.name,
        outcome: d.outcome ?? "failed",
      })),
    });
  }
}

export function makeSteppedStep(deps: SteppedStepDeps): (input: RunStepInput) => Promise<RunStepResult> {
  const { runModelCall, runToolCall, sealStep } = deps.activities;

  return async (input: RunStepInput): Promise<RunStepResult> => {
    const model = await runModelCall(input);
    if (model.settled) {
      // A crashed step finalized from the transcript, or a retry landing after the turn's last
      // step: nothing to dispatch and nothing to close.
      return model.settled;
    }

    await dispatchStepCalls(
      input.step,
      model,
      (call) => {
        const toolInput: ToolCallInput = {
          sessionId: input.sessionId,
          sessionFile: input.sessionFile,
          step: input.step,
          call,
        };
        return runToolCall(toolInput);
      },
      deps,
    );

    // Every call of the step is sealed, including the ones no dispatch answered for. A step that
    // leaves one open leaves a transcript the next model call cannot be made from.
    return sealStep({
      sessionId: input.sessionId,
      sessionFile: input.sessionFile,
      step: input.step,
      calls: model.calls,
    });
  };
}
