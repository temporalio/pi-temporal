// One step of a turn, driven as a model call, one Activity per tool call, and a seal. This puts
// the Workflow between the model asking for a tool and the tool running, so per-tool retry,
// timeout and budget can apply.
//
// The seal is the only writer of tool results. Concurrent writers would branch the session tree,
// so calls report results and the seal records them together, in the model's order.
//
// Sandbox-safe: no SDK or Node imports. SDK pieces are injected.

import type {
  DeferredToolCall,
  ModelCallResult,
  RunStepInput,
  RunStepResult,
  SealStepInput,
  Spend,
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
  // The same Activities on the model-call Worker's own queue. Tools write that host's project
  // directory, so they run there and can run concurrently.
  readonly onHost?: (queue: string) => Pick<SteppedActivities, "runToolCall" | "sealStep">;
  // Migration requires evidence that no attempt started on the host queue.
  readonly isUnclaimed?: (err: unknown) => boolean;
  // A fresh fence for each Activity scheduled. A seal sent again on another queue is a new
  // Activity, whose attempts count from one again.
  readonly fence?: () => string;
  // Run the seal even when the turn was cancelled, so finished calls keep their real results.
  readonly nonCancellable: <T>(fn: () => Promise<T>) => Promise<T>;
  // Checked between sequential calls. A started call is never stopped, since the transcript needs
  // its result. Given what this step's model call spent, which the Workflow has not counted yet.
  readonly outOfBudget?: (pending?: Pick<ModelCallResult, "spent" | "total">) => boolean;
  // The Workflow logger. History shows an Activity succeeded, but only the dispatch knows what it
  // decided.
  readonly log?: (message: string, attributes: Record<string, unknown>) => void;
}

/** What one dispatch decided, or what stopped it. */
interface Dispatched {
  readonly call: DeferredToolCall;
  readonly outcome?: ToolCallOutcome;
  readonly error?: unknown;
}

/** What a model call reported, whichever Activity made it. */
export interface StepCalls {
  readonly calls: readonly DeferredToolCall[];
  readonly sequential: boolean;
  readonly ended: boolean;
  /** The queue the Worker that made this call polls on its own, when it has one. */
  readonly queue?: string;
}

/**
 * Run the calls of one step and return the stop error, if any. Shared by the Worker path and the
 * live pi path.
 */
export async function dispatchStepCalls(
  step: number,
  model: StepCalls,
  runToolCall: (call: DeferredToolCall) => Promise<ToolCallResult>,
  deps: Pick<SteppedStepDeps, "isCancellation" | "log" | "outOfBudget">,
): Promise<unknown> {
  // Errors are returned, not thrown, so no sibling promise rejects unobserved.
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
      // An interrupt or an exhausted budget stops the rest of the batch. Nothing has started them.
      for (const call of model.calls) {
        const outcome = await dispatch(call);
        dispatched.push(outcome);
        if (outcome.error !== undefined && deps.isCancellation(outcome.error)) break;
        if (deps.outOfBudget?.()) {
          deps.log?.("stopped dispatching the rest of this step: the turn is out of budget", {
            step,
            left: model.calls.length - dispatched.length,
          });
          break;
        }
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

  // Returned, not thrown. The caller still seals so finished calls keep their results.
  return dispatched.find((d) => d.error !== undefined && deps.isCancellation(d.error))?.error;
}

type SteppedStep = (input: RunStepInput) => Promise<RunStepResult>;

// Step cost is the model call plus the seal, which may compact (another model call).
const together = (model: Spend | undefined, sealed: Spend | undefined): Spend | undefined => {
  if (!model && !sealed) return undefined;
  const cost = (model?.cost ?? 0) + (sealed?.cost ?? 0);
  return { tokens: (model?.tokens ?? 0) + (sealed?.tokens ?? 0), ...(cost ? { cost } : {}) };
};

export function makeSteppedStep(deps: SteppedStepDeps): SteppedStep {
  const { runModelCall } = deps.activities;

  return async (input: RunStepInput): Promise<RunStepResult> => {
    // The Workflow clock, so replay agrees. The seal adds this step's own time to the session's.
    const stepStartedAt = Date.now();
    const model = await runModelCall(input);
    const withSpend = (result: RunStepResult): RunStepResult => {
      const spent = together(model.spent, result.spent);
      // Prefer the seal's total. It read the session after the model call wrote to it.
      const total = result.total ?? model.total;
      const sessionSeconds = result.sessionSeconds ?? model.sessionSeconds;
      return {
        ...result,
        ...(model.queue ? { hostQueue: model.queue } : {}),
        ...(spent ? { spent } : {}),
        ...(total ? { total } : {}),
        ...(sessionSeconds === undefined ? {} : { sessionSeconds }),
      };
    };
    if (model.settled) {
      // Already finalized from the transcript. Nothing to dispatch or seal.
      return withSpend(model.settled);
    }

    // The model-call Worker's own queue, the host holding the project directory.
    const host = model.queue && deps.onHost ? deps.onHost(model.queue) : undefined;
    let unclaimed = false;
    let unsafeFailure: unknown;
    let stopFailure: unknown;
    const hostAttempts: Promise<void>[] = [];
    // After falling back, the shared queue runs one at a time. Two hosts at once would branch the
    // tree store.
    let shared: Promise<unknown> = Promise.resolve();
    const onShared = <T>(run: (on: SteppedActivities) => Promise<T>): Promise<T> => {
      const next = shared.then(async () => {
        // A sibling may have started even when this dispatch timed out in the queue.
        await Promise.all(hostAttempts);
        if (stopFailure !== undefined) throw stopFailure;
        if (unsafeFailure !== undefined) throw unsafeFailure;
        try {
          return await run(deps.activities);
        } catch (err) {
          if (deps.isCancellation(err)) stopFailure = err;
          throw err;
        }
      });
      shared = next.then(
        () => undefined,
        () => undefined,
      );
      return next;
    };
    // A timed-out body can keep writing. Only a dispatch that provably never started may move to
    // the shared queue, until workspaces are isolated.
    const viaHost = async <T>(
      run: (on: Pick<SteppedActivities, "runToolCall" | "sealStep">) => Promise<T>,
    ): Promise<T> => {
      if (stopFailure !== undefined) throw stopFailure;
      if (unsafeFailure !== undefined) throw unsafeFailure;
      if (!host) return run(deps.activities);
      if (unclaimed) return onShared(run);
      const attempt = run(host);
      // The barrier waits for it to be over, not to succeed.
      hostAttempts.push(attempt.then(() => undefined, () => undefined));
      try {
        return await attempt;
      } catch (err) {
        if (deps.isCancellation(err)) {
          stopFailure = err;
          throw err;
        }
        if (deps.isUnclaimed?.(err) !== true) {
          unsafeFailure = err;
          throw err;
        }
        unclaimed = true;
        deps.log?.(
          "the host queue did not take the work; the rest of the step goes to the shared queue",
          { step: input.step },
        );
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
          turn: input.promptId,
          step: input.step,
          call,
        };
        return viaHost((on) => on.runToolCall(toolInput));
      },
      {
        ...deps,
        outOfBudget: deps.outOfBudget && (() => deps.outOfBudget!(model)),
      },
    );

    // Seal every call, answered or not. An open call makes the transcript invalid for the model.
    const seal = (interrupted: boolean, lost = false): Promise<RunStepResult> => {
      const sealed: SealStepInput = {
        sessionId: input.sessionId,
        sessionFile: input.sessionFile,
        turn: input.promptId,
        step: input.step,
        calls: model.calls,
        agentState: input.agentState,
        sessionSeconds:
          input.sessionSeconds === undefined
            ? undefined
            : input.sessionSeconds + (Date.now() - stepStartedAt) / 1000,
        interrupted,
        ...(lost ? { lost: true } : {}),
      };
      const stamped = () => ({ ...sealed, ...(deps.fence ? { fence: deps.fence() } : {}) });
      // A recovery seal must not move a project that an abandoned tool may still write.
      if (interrupted) return deps.activities.sealStep(stamped());
      return viaHost((on) => on.sealStep(stamped()));
    };

    const recover = async (failure: unknown): Promise<RunStepResult> => {
      // Not a user stop, so the host is lost. The seal records that where every host reads it.
      const lost = !deps.isCancellation(failure);
      // Seal so completed results reach the transcript before the next prompt sweeps them. If
      // this fails, the original failure still decides the outcome.
      const sealed = await deps.nonCancellable(() => seal(true, lost)).catch((err: unknown) => {
        deps.log?.("could not record results before ending the step", {
          step: input.step,
          error: String(err),
        });
        return undefined;
      });
      // Sealed and fenced off from the lost host, so the turn can continue. The model sees which
      // calls have unknown outcomes.
      if (sealed && lost) {
        deps.log?.("the step lost its host; recorded what it had and carrying the turn on", {
          step: input.step,
        });
        // Always not done. If the transcript is already complete, the next model call settles it.
        return withSpend({
          done: false,
          ...(sealed.agentState ? { agentState: sealed.agentState } : {}),
          finalText: "",
        });
      }
      throw failure;
    };
    const failure = stopped ?? unsafeFailure;
    if (failure !== undefined) return recover(failure);
    try {
      return withSpend(await seal(false));
    } catch (err) {
      // A stop can land after every tool reported, while the seal is being scheduled. Once a stop
      // is asked, the server doesn't retry the seal, so a lost attempt comes back as a timeout.
      // Either way the tools' results still go in, through the recovery seal.
      return recover(stopFailure ?? unsafeFailure ?? err);
    }
  };
}
