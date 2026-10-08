// Checks the stepped step body (`src/core/stepped-step.ts`) against fake activities, with no
// server. Asserts every call reaches the seal, interrupts propagate, one failed tool does not end
// the turn, and host-queue work falls back to the shared queue only when it never started.
//
// Usage: npx tsx checks/stepped-step-check.mts

import { makeSteppedStep } from "../src/core/stepped-step.js";
import type {
  DeferredToolCall,
  ModelCallResult,
  RunStepInput,
  RunStepResult,
  SealStepInput,
  ToolCallInput,
  ToolCallResult,
} from "../src/core/protocol.js";

class FakeCancel extends Error {}
const isCancellation = (err: unknown) => err instanceof FakeCancel;

const INPUT: RunStepInput = {
  sessionId: "ses_1",
  sessionFile: "/tmp/ses_1.jsonl",
  step: 2,
  promptId: "p1",
  text: "go",
};
const SEALED: RunStepResult = { done: false, finalText: "" };
const call = (id: string, name = "bash"): DeferredToolCall => ({ id, name });

interface Recorded {
  readonly dispatched: ToolCallInput[];
  readonly seals: SealStepInput[];
  readonly overlapped: boolean;
  readonly lines: Array<{ message: string; attributes: Record<string, unknown> }>;
}

/** Drive one step with a scripted model call and a tool behaviour, and report what happened. */
async function drive(
  model: ModelCallResult,
  onTool: (input: ToolCallInput) => Promise<ToolCallResult> = async () => ({ outcome: "settled" }),
): Promise<{ result?: RunStepResult; error?: unknown } & Recorded> {
  const dispatched: ToolCallInput[] = [];
  const seals: SealStepInput[] = [];
  const lines: Array<{ message: string; attributes: Record<string, unknown> }> = [];
  let inFlight = 0;
  let overlapped = false;

  const step = makeSteppedStep({
    activities: {
      runModelCall: async () => model,
      runToolCall: async (input) => {
        dispatched.push(input);
        inFlight++;
        if (inFlight > 1) overlapped = true;
        try {
          return await onTool(input);
        } finally {
          inFlight--;
        }
      },
      sealStep: async (input) => {
        seals.push(input);
        return SEALED;
      },
    },
    isCancellation,
    nonCancellable: (fn) => fn(),
    log: (message, attributes) => lines.push({ message, attributes }),
  });

  try {
    return { result: await step(INPUT), dispatched, seals, overlapped, lines };
  } catch (error) {
    return { error, dispatched, seals, overlapped, lines };
  }
}

const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

async function main() {
  // A step the model call reports as over dispatches nothing and closes nothing.
  {
    const settled: RunStepResult = { done: true, finalText: "already answered" };
    const run = await drive({ settled, calls: [], sequential: false, ended: true });
    check("a settled step dispatches nothing", run.dispatched.length === 0, run.dispatched);
    check("a settled step is not sealed again", run.seals.length === 0, run.seals);
    const found = run.result?.finalText === "already answered";
    check("a settled step reports the answer it found", found, run.result);
  }

  // Every call reaches the seal in model order. Calls overlap by default.
  {
    // The tool waits, so overlap is observable.
    const batch = { calls: [call("c1"), call("c2")], sequential: false, ended: false };
    const run = await drive(batch, async () => {
      await new Promise((resolve) => setTimeout(resolve, 20));
      return { outcome: "settled" };
    });
    const ids = run.dispatched.map((d) => d.call.id).join(",");
    check("every call is dispatched", ids === "c1,c2", run.dispatched);
    check("calls overlap unless the batch says otherwise", run.overlapped, run.overlapped);
    const sealed = JSON.stringify(run.seals[0]?.calls.map((c) => c.id));
    check("the seal is handed the step's calls in order", sealed === '["c1","c2"]', run.seals[0]);
    check("nothing is said about a step that settled", run.lines.length === 0, run.lines);
  }

  // A sequential batch runs one call at a time.
  {
    const batch = { calls: [call("c1"), call("c2")], sequential: true, ended: false };
    const run = await drive(batch, async () => {
      await new Promise((resolve) => setTimeout(resolve, 20));
      return { outcome: "settled" };
    });
    check("a sequential batch does not overlap", !run.overlapped, run.overlapped);
    check("a sequential batch still reaches the seal", run.seals.length === 1, run.seals);
  }

  // A tool out of retries is sealed as unknown so the model can react. The turn goes on.
  {
    const batch = { calls: [call("c1"), call("c2")], sequential: false, ended: false };
    const run = await drive(batch, async (input) => {
      if (input.call.id === "c1") throw new Error("activity exhausted its retries");
      return { outcome: "settled" };
    });
    check("a failed call still lets the step close", run.seals.length === 1, run.seals);
    const sealed = JSON.stringify(run.seals[0]?.calls.map((c) => c.id));
    check("the failed call is still sealed", sealed === '["c1","c2"]', run.seals[0]);
    const said = JSON.stringify(run.lines[0]?.attributes);
    const failed = '{"step":2,"calls":[{"call":"c1","tool":"bash","outcome":"failed"}]}';
    const told = run.lines.length === 1 && said === failed;
    check("the failure is reported where the turn is read", told, run.lines);
  }

  // An interrupt is not a failed tool. It ends the turn.
  {
    const run = await drive({ calls: [call("c1")], sequential: false, ended: false }, async () => {
      throw new FakeCancel("interrupted");
    });
    check("an interrupt ends the turn", run.error instanceof FakeCancel, String(run.error));
    // Still sealed on the way out, so calls that finished before the stop keep their results.
    check("an interrupted step is still closed", run.seals.length === 1, run.seals);
  }

  // A sequential batch stops dispatching once one call is cancelled.
  {
    const batch = { calls: [call("c1"), call("c2")], sequential: true, ended: false };
    const run = await drive(batch, async () => {
      throw new FakeCancel("interrupted");
    });
    const stopped = run.dispatched.length === 1;
    check("an interrupt stops the rest of a sequential batch", stopped, run.dispatched);
  }

  // An ended model call still seals, since retry or compaction for a failed call happens there.
  {
    const run = await drive({ calls: [], sequential: false, ended: true });
    check("an ended model call dispatches nothing", run.dispatched.length === 0, run.dispatched);
    check("an ended model call is still sealed", run.seals.length === 1, run.seals);
  }

  // The agent's state must round-trip through the step. It is optional, so dropping it compiles
  // and silently resets what the agent counts, such as its retry budget, every step.
  {
    const seals: SealStepInput[] = [];
    const step = makeSteppedStep({
      activities: {
        runModelCall: async () => ({ calls: [], sequential: false, ended: true }),
        runToolCall: async () => ({ outcome: "settled" }),
        sealStep: async (input) => {
          seals.push(input);
          return { done: false, agentState: { retries: 2 }, finalText: "" };
        },
      },
      isCancellation,
      nonCancellable: (fn) => fn(),
    });
    const result = await step({ ...INPUT, agentState: { retries: 1 } });
    check("the seal is given the last step's state", seals[0]?.agentState?.retries === 1, seals[0]);
    check("and the state it returns comes back", result.agentState?.retries === 2, result);
  }

  // An unknown outcome is logged and still sealed.
  {
    const run = await drive({ calls: [call("c1")], sequential: false, ended: false }, async () => ({
      outcome: "unknown",
    }));
    const said = JSON.stringify(run.lines[0]?.attributes);
    const unknown = '{"step":2,"calls":[{"call":"c1","tool":"bash","outcome":"unknown"}]}';
    check("an unknown outcome is reported", run.lines.length === 1 && said === unknown, run.lines);
    check("an unknown outcome still seals", run.seals.length === 1, run.seals);
  }

  // A step's tools and seal go to the queue of the worker that made the model call, so they share
  // one directory. If nobody claims the host-queue work, it falls back to the shared queue.
  {
    const hostTools: ToolCallInput[] = [];
    const hostSeals: SealStepInput[] = [];
    const sharedTools: ToolCallInput[] = [];
    const sharedSeals: SealStepInput[] = [];
    let refuse = false;
    let refused = 0;
    // Shaped like Temporal's schedule-to-start timeout, which is what the workflow matches on.
    const unclaimed = () =>
      Object.assign(new Error("activity failed"), {
        name: "ActivityFailure",
        cause: { name: "TimeoutFailure", timeoutType: "SCHEDULE_TO_START" },
      });
    const isUnclaimed = (err: unknown) =>
      (err as { cause?: { timeoutType?: string } })?.cause?.timeoutType === "SCHEDULE_TO_START";

    const hostStep = (calls: DeferredToolCall[]) =>
      makeSteppedStep({
        activities: {
          runModelCall: async () => ({ calls, sequential: false, ended: false, queue: "mine" }),
          runToolCall: async (input) => {
            sharedTools.push(input);
            return { outcome: "settled" };
          },
          sealStep: async (input) => {
            sharedSeals.push(input);
            return SEALED;
          },
        },
        isCancellation,
        isUnclaimed,
        onHost: (queue) => {
          if (queue !== "mine") throw new Error(`routed to ${queue}`);
          return {
            runToolCall: async (input) => {
              if (refuse) {
                refused++;
                throw unclaimed();
              }
              hostTools.push(input);
              return { outcome: "settled" };
            },
            sealStep: async (input) => {
              if (refuse) {
                refused++;
                throw unclaimed();
              }
              hostSeals.push(input);
              return SEALED;
            },
          };
        },
        nonCancellable: (fn) => fn(),
      });

    await hostStep([call("a"), call("b")])(INPUT);
    check(
      "a step's tools and seal go back to the worker that made the model call",
      hostTools.length === 2 && hostSeals.length === 1 && sharedTools.length === 0,
      { hostTools: hostTools.length, hostSeals: hostSeals.length },
    );

    refuse = true;
    await hostStep([call("c")])(INPUT);
    // Only schedule-to-start proves the work never started, so moving it cannot run a tool twice.
    // After one refusal the seal skips the host queue.
    check(
      "and move to the shared queue when nobody takes them",
      sharedTools.length === 1 && sharedSeals.length === 1 && refused === 1,
      { sharedTools: sharedTools.length, sharedSeals: sharedSeals.length, refused },
    );
  }

  for (const outcome of ["completed", "failed", "cancelled"] as const) {
    const hostFails = outcome !== "completed";
    let finishHost!: () => void;
    const held = new Promise<void>((resolve) => { finishHost = resolve; });
    let hostStarted!: () => void;
    const started = new Promise<void>((resolve) => { hostStarted = resolve; });
    let sharedStarted = false;
    let hostActive = false;
    let crossedHosts = false;
    const unavailable = new Error("unclaimed");
    const failed =
      outcome === "cancelled" ? new FakeCancel() : new Error("started attempt timed out");
    const step = makeSteppedStep({
      activities: {
        runModelCall: async () => ({
          calls: [call("held"), call("waiting")],
          sequential: false,
          ended: false,
          queue: "host-a",
        }),
        runToolCall: async () => {
          sharedStarted = true;
          crossedHosts ||= hostActive;
          return { outcome: "settled" };
        },
        sealStep: async () => SEALED,
      },
      onHost: () => ({
        runToolCall: async (input) => {
          if (input.call.id === "waiting") throw unavailable;
          hostActive = true;
          hostStarted();
          try {
            await held;
            if (hostFails) throw failed;
            return { outcome: "settled" };
          } finally {
            // A real body may keep running past a server timeout. This fake one stops here.
            hostActive = false;
          }
        },
        sealStep: async () => SEALED,
      }),
      isCancellation,
      isUnclaimed: (error) => error === unavailable,
      nonCancellable: (fn) => fn(),
    });
    const result = step(INPUT).then(() => undefined, (error: unknown) => error);
    await started;
    await new Promise((resolve) => setTimeout(resolve, 0));
    check("an unclaimed sibling waits for the host-queue batch", !sharedStarted, { outcome });
    finishHost();
    const error = await result;
    check("fallback never overlaps a host-queue tool", !crossedHosts, { outcome });
    check(
      outcome === "cancelled"
        ? "a cancelled sibling blocks queued fallback"
        : hostFails
          ? "an uncertain host-queue attempt blocks migration, and the turn goes on"
          : "fallback resumes after the host-queue tool ships",
      // A stop reaches the turn. A lost host doesn't end it.
      hostFails
        ? !sharedStarted && error === (outcome === "cancelled" ? failed : undefined)
        : sharedStarted && error === undefined,
      { sharedStarted, error: String(error), outcome },
    );
  }

  // A timeout cannot distinguish a dead worker from one whose tool still writes.
  {
    const gone = new Error("the host-queue attempt did not come back");
    let sharedTools = 0;
    let sharedSeals = 0;
    let recoverySeal = false;
    const step = makeSteppedStep({
      activities: {
        runModelCall: async () => ({
          calls: [call("c1")],
          sequential: false,
          ended: false,
          queue: "w-1",
        }),
        runToolCall: async () => {
          sharedTools++;
          return { outcome: "unknown" };
        },
        sealStep: async (input) => {
          sharedSeals++;
          recoverySeal = input.interrupted === true;
          return SEALED;
        },
      },
      onHost: () => ({
        runToolCall: async () => {
          throw gone;
        },
        sealStep: async () => SEALED,
      }),
      isCancellation,
      isUnclaimed: () => false,
      nonCancellable: (fn) => fn(),
    });
    const result = await step(INPUT).then(() => undefined, (error: unknown) => error);
    check(
      "an uncertain host-queue attempt records results without shared tools",
      sharedTools === 0 && sharedSeals === 1 && recoverySeal,
      {
        sharedTools,
        sharedSeals,
      },
    );
    check("and the turn goes on past the lost host", result === undefined, String(result));
  }

  {
    const failed = new Error("host-queue attempt timed out");
    let normalSeals = 0;
    let recoverySeals = 0;
    const seal = async (input: SealStepInput) => {
      if (input.interrupted) recoverySeals++;
      else normalSeals++;
      return SEALED;
    };
    const step = makeSteppedStep({
      activities: {
        runModelCall: async () => ({
          calls: [call("held")],
          sequential: false,
          ended: false,
          queue: "host-a",
        }),
        runToolCall: async () => ({ outcome: "settled" }),
        sealStep: seal,
      },
      onHost: () => ({ runToolCall: async () => { throw failed; }, sealStep: seal }),
      isCancellation,
      isUnclaimed: () => false,
      nonCancellable: (fn) => fn(),
    });
    const error = await step(INPUT).then(() => undefined, (error: unknown) => error);
    check(
      "an uncertain host-queue tool permits only a recovery seal",
      normalSeals === 0 && recoverySeals === 1 && error === undefined,
      {
        normalSeals,
        recoverySeals,
        error: String(error),
      },
    );
  }

  {
    const cancelled = new FakeCancel();
    const unavailable = new Error("unclaimed");
    const sharedCalls: string[] = [];
    let recoverySeals = 0;
    const step = makeSteppedStep({
      activities: {
        runModelCall: async () => ({
          calls: [call("cancel"), call("must-not-run")],
          sequential: false,
          ended: false,
          queue: "host-a",
        }),
        runToolCall: async (input) => {
          sharedCalls.push(input.call.id);
          if (input.call.id === "cancel") throw cancelled;
          return { outcome: "settled" };
        },
        sealStep: async (input) => {
          if (input.interrupted) recoverySeals++;
          return SEALED;
        },
      },
      onHost: () => ({
        runToolCall: async () => { throw unavailable; },
        sealStep: async () => { throw unavailable; },
      }),
      isCancellation,
      isUnclaimed: (error) => error === unavailable,
      nonCancellable: (fn) => fn(),
    });
    const error = await step(INPUT).then(() => undefined, (failure: unknown) => failure);
    check(
      "a cancelled shared call stops the queued fallback",
      sharedCalls.join(",") === "cancel",
      sharedCalls,
    );
    check(
      "a fallback cancellation preserves the original stop and seals results",
      error === cancelled && recoverySeals === 1,
      {
        error: String(error), recoverySeals,
      },
    );
  }

  // After a stop the server doesn't retry the seal, so a lost attempt comes back as a timeout. On
  // the shared queue too, the step's results still go in through the recovery seal.
  {
    const timedOut = Object.assign(new Error("activity failed"), {
      name: "ActivityFailure",
      cause: { name: "TimeoutFailure", timeoutType: "START_TO_CLOSE" },
    });
    const seals: SealStepInput[] = [];
    const step = makeSteppedStep({
      activities: {
        runModelCall: async () => ({ calls: [call("c1")], sequential: false, ended: false }),
        runToolCall: async () => ({ outcome: "settled" }),
        sealStep: async (input) => {
          seals.push(input);
          if (!input.interrupted) throw timedOut;
          return SEALED;
        },
      },
      isCancellation,
      nonCancellable: (fn) => fn(),
    });
    const outcome = await step(INPUT).then(
      (result) => ({ result }),
      (error: unknown) => ({ error }),
    );
    const recovered = seals.length === 2 && seals[1]?.interrupted === true && seals[1]?.lost;
    check("a seal lost on the shared queue is recovered", recovered === true, seals);
    check("and the turn goes on past it", "result" in outcome, String(outcome));
  }

  const bad = failures.length;
  console.log(bad === 0 ? "stepped-step-check: OK" : `stepped-step-check: ${bad} failed`);
  process.exit(failures.length === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
