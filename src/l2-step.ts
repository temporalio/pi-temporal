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
  // The same two activities, addressed to the queue one worker polls on its own. A step's tools
  // write the directory the model call's worker is standing in, so keeping them there is what lets
  // them run at once: they see each other through the filesystem rather than through the tree
  // store. Offered only the queue that worker reported.
  readonly pinnedTo?: (queue: string) => Pick<SteppedActivities, "runToolCall" | "sealStep">;
  // Whether a failure means nobody took the work. It is the one kind a pinned dispatch answers by
  // trying the shared queue, because it says the activity never started and so nothing ran.
  readonly isUnclaimed?: (err: unknown) => boolean;
  // Run the seal even though the turn was cancelled. Calls that finished have real results kept
  // for them, and abandoning the step tells the model they may have taken effect instead.
  readonly nonCancellable: <T>(fn: () => Promise<T>) => Promise<T>;
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
  /** The queue the worker that made this call polls on its own, when it has one. */
  readonly queue?: string;
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
): Promise<unknown> {
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

  const unsettled = dispatched.filter(
    (d) => d.outcome !== "settled" && d.outcome !== "already-settled",
  );
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

  // Handed back rather than thrown, and undefined when nothing stopped. The step still has to be
  // closed: the calls that finished before the stop have results and the seal records them.
  return dispatched.find((d) => d.error !== undefined && deps.isCancellation(d.error))?.error;
}

type SteppedStep = (input: RunStepInput) => Promise<RunStepResult>;

export function makeSteppedStep(deps: SteppedStepDeps): SteppedStep {
  const { runModelCall } = deps.activities;

  return async (input: RunStepInput): Promise<RunStepResult> => {
    const model = await runModelCall(input);
    if (model.settled) {
      // A crashed step finalized from the transcript, or a retry landing after the turn's last
      // step: nothing to dispatch and nothing to close.
      return model.settled;
    }

    // The worker that made the model call, when it offered a queue of its own. Everything else in
    // this step is addressed there first: it is the host holding the directory the tools write.
    const pinned = model.queue && deps.pinnedTo ? deps.pinnedTo(model.queue) : undefined;
    let unclaimed = false;
    const pinnedAttempts: Promise<void>[] = [];
    let unsafeFallback: { error: unknown } | undefined;
    // What is left of a step whose worker is gone goes to the shared queue one at a time. There it
    // can land on two hosts again, which is what the tree store cannot take.
    let shared: Promise<unknown> = Promise.resolve();
    const onShared = <T>(run: (on: SteppedActivities) => Promise<T>): Promise<T> => {
      const next = shared.then(async () => {
        // A queue timeout can leave sibling tools writing the pinned directory.
        await Promise.all(pinnedAttempts);
        if (unsafeFallback) throw unsafeFallback.error;
        return run(deps.activities);
      });
      shared = next.then(
        () => undefined,
        () => undefined,
      );
      return next;
    };
    const viaPinned = async <T>(
      run: (on: Pick<SteppedActivities, "runToolCall" | "sealStep">) => Promise<T>,
    ): Promise<T> => {
      if (unsafeFallback) throw unsafeFallback.error;
      if (!pinned || !deps.isUnclaimed) return run(deps.activities);
      if (unclaimed) return onShared(run);
      try {
        const attempt = run(pinned);
        pinnedAttempts.push(attempt.then(
          () => undefined,
          (error: unknown) => {
            // A failed started attempt can still have a live tool on that host.
            if (!deps.isUnclaimed!(error)) unsafeFallback = { error };
          },
        ));
        return await attempt;
      } catch (err) {
        if (!deps.isUnclaimed(err)) throw err;
        unclaimed = true;
        deps.log?.("pinned work was not taken; waiting for the step before using the shared queue", {
          step: input.step,
        });
        return onShared(run);
      }
    };

    const stopped = await dispatchStepCalls(
      input.step,
      model,
      (call) => {
        const toolInput: ToolCallInput = {
          sessionId: input.sessionId,
          sessionFile: input.sessionFile,
          step: input.step,
          call,
        };
        return viaPinned((on) => on.runToolCall(toolInput));
      },
      deps,
    );

    // Every call of the step is sealed, including the ones no dispatch answered for. A step that
    // leaves one open leaves a transcript the next model call cannot be made from.
    const seal = (interrupted: boolean): Promise<RunStepResult> => {
      const sealed: SealStepInput = {
        sessionId: input.sessionId,
        sessionFile: input.sessionFile,
        step: input.step,
        calls: model.calls,
        retryAttempt: input.retryAttempt,
        interrupted,
      };
      // An interrupted seal only appends the transcript; it must not move the project.
      if (interrupted) return deps.activities.sealStep(sealed);
      return viaPinned((on) => on.sealStep(sealed));
    };

    if (stopped) {
      // The user stopped the turn, so close the step and then let the stop through. Skipping the
      // seal would throw away the calls that finished, and the next prompt would be told their
      // outcome is unknown. The stop is what the caller hears about either way: a seal that fails
      // on the way out is not the thing worth reporting.
      await deps.nonCancellable(() => seal(true)).catch((err: unknown) => {
        // The stop is what the caller asked about, so it is what propagates. But the results of
        // every call that finished are lost with this, and the next prompt will tell the model
        // their outcome is unknown, so it does not go unsaid.
        deps.log?.("could not close a stopped step", { step: input.step, error: String(err) });
      });
      throw stopped;
    }
    return seal(false);
  };
}
