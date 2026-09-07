// Checks the stepped step body (src/l2-step.ts) against fake activities: no Temporal, no model
// key, no session file. What is pinned here is the orchestration, which is the part a live run
// cannot show you: every call reaches the seal, an interrupt is not swallowed, one broken tool
// does not take the turn with it, and a batch that says so runs in order.
//
// Usage: npx tsx l2-step-check.mts

import { makeSteppedStep } from "./src/l2-step.js";
import type {
  DeferredToolCall,
  ModelCallResult,
  RunStepInput,
  RunStepResult,
  SealStepInput,
  ToolCallInput,
  ToolCallResult,
} from "./src/protocol.js";

class FakeCancel extends Error {}
const isCancellation = (err: unknown) => err instanceof FakeCancel;

const INPUT: RunStepInput = {
  sessionId: "ses_1",
  sessionFile: "/tmp/ses_1.jsonl",
  step: 2,
  promptId: "p1",
  text: "go",
};
const SEALED: RunStepResult = { done: false, retryAttempt: 0, finalText: "" };
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
    // Straight through: what matters here is that the seal still runs after a stop.
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
    const settled: RunStepResult = { done: true, retryAttempt: 0, finalText: "already answered" };
    const run = await drive({ settled, calls: [], sequential: false, ended: true });
    check("a settled step dispatches nothing", run.dispatched.length === 0, run.dispatched);
    check("a settled step is not sealed again", run.seals.length === 0, run.seals);
    const found = run.result?.finalText === "already answered";
    check("a settled step reports the answer it found", found, run.result);
  }

  // Every call reaches the seal, in the order the model asked, and they overlap by default.
  {
    // The tool waits, so two of them running at once is observable.
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

  // A tool of the batch declares itself sequential, so the workflow runs them one at a time.
  {
    const batch = { calls: [call("c1"), call("c2")], sequential: true, ended: false };
    const run = await drive(batch, async () => {
      await new Promise((resolve) => setTimeout(resolve, 20));
      return { outcome: "settled" };
    });
    check("a sequential batch does not overlap", !run.overlapped, run.overlapped);
    check("a sequential batch still reaches the seal", run.seals.length === 1, run.seals);
  }

  // One tool that ran out of retries must not take the turn with it: the seal settles it as an
  // unknown outcome and the model gets to react.
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

  // An interrupt is not a failed tool. Sealing would close a step the user stopped.
  {
    const run = await drive({ calls: [call("c1")], sequential: false, ended: false }, async () => {
      throw new FakeCancel("interrupted");
    });
    check("an interrupt ends the turn", run.error instanceof FakeCancel, String(run.error));
    // Sealed on the way out, so a call that finished before the stop keeps its result. Skipping it
    // would have the next prompt told the outcome is unknown for work that is on disk.
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

  // A model call that ended the run dispatches nothing, and still seals: what answers for a
  // failed model call (a retry, a compaction) happens there.
  {
    const run = await drive({ calls: [], sequential: false, ended: true });
    check("an ended model call dispatches nothing", run.dispatched.length === 0, run.dispatched);
    check("an ended model call is still sealed", run.seals.length === 1, run.seals);
  }

  // The retry budget rides the workflow, so what the seal reports has to reach the next step.
  // Dropping it on the way through is silent: the field is optional to the compiler and zero
  // reads as "no failures yet", so a failing provider is asked again on every step.
  {
    const seals: SealStepInput[] = [];
    const step = makeSteppedStep({
      activities: {
        runModelCall: async () => ({ calls: [], sequential: false, ended: true }),
        runToolCall: async () => ({ outcome: "settled" }),
        sealStep: async (input) => {
          seals.push(input);
          return { done: false, retryAttempt: 2, finalText: "" };
        },
      },
      isCancellation,
      nonCancellable: (fn) => fn(),
    });
    const result = await step({ ...INPUT, retryAttempt: 1 });
    check("the seal is told what the last step spent", seals[0]?.retryAttempt === 1, seals[0]);
    check("and what it spends comes back", result.retryAttempt === 2, result);
  }

  // A call reported as unknown is one nothing ran twice, and it is said out loud.
  {
    const run = await drive({ calls: [call("c1")], sequential: false, ended: false }, async () => ({
      outcome: "unknown",
    }));
    const said = JSON.stringify(run.lines[0]?.attributes);
    const unknown = '{"step":2,"calls":[{"call":"c1","tool":"bash","outcome":"unknown"}]}';
    check("an unknown outcome is reported", run.lines.length === 1 && said === unknown, run.lines);
    check("an unknown outcome still seals", run.seals.length === 1, run.seals);
  }

  // Pinning the rest of a step to the worker that made its model call, and what happens when that
  // worker is gone. The pin is what lets a step's tools run together again while the tree travels:
  // they write one directory instead of shipping it to each other. The fallback is what keeps a
  // dead worker from holding the step for good.
  {
    const pinnedTools: ToolCallInput[] = [];
    const pinnedSeals: SealStepInput[] = [];
    const sharedTools: ToolCallInput[] = [];
    const sharedSeals: SealStepInput[] = [];
    let refuse = false;
    let refused = 0;
    // What Temporal raises when nobody took the work. Matched by shape, because that is what the
    // workflow's own predicate matches.
    const unclaimed = () =>
      Object.assign(new Error("activity failed"), {
        name: "ActivityFailure",
        cause: { name: "TimeoutFailure", timeoutType: "SCHEDULE_TO_START" },
      });
    const isUnclaimed = (err: unknown) =>
      (err as { cause?: { timeoutType?: string } })?.cause?.timeoutType === "SCHEDULE_TO_START";

    const pinnedStep = (calls: DeferredToolCall[]) =>
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
        pinnedTo: (queue) => {
          if (queue !== "mine") throw new Error(`pinned to ${queue}`);
          return {
            runToolCall: async (input) => {
              if (refuse) {
                refused++;
                throw unclaimed();
              }
              pinnedTools.push(input);
              return { outcome: "settled" };
            },
            sealStep: async (input) => {
              if (refuse) {
                refused++;
                throw unclaimed();
              }
              pinnedSeals.push(input);
              return SEALED;
            },
          };
        },
        nonCancellable: (fn) => fn(),
      });

    await pinnedStep([call("a"), call("b")])(INPUT);
    check(
      "a step's tools and seal go back to the worker that made the model call",
      pinnedTools.length === 2 && pinnedSeals.length === 1 && sharedTools.length === 0,
      { pinnedTools: pinnedTools.length, pinnedSeals: pinnedSeals.length },
    );

    refuse = true;
    await pinnedStep([call("c")])(INPUT);
    // Schedule-to-start is the one failure that says the activity never started, so moving the work
    // cannot run a tool twice. One refusal, not two: the seal that follows already knows.
    check(
      "and move to the shared queue when nobody takes them",
      sharedTools.length === 1 && sharedSeals.length === 1 && refused === 1,
      { sharedTools: sharedTools.length, sharedSeals: sharedSeals.length, refused },
    );
  }

  for (const pinnedFails of [false, true]) {
    let finishPinned!: () => void;
    const held = new Promise<void>((resolve) => { finishPinned = resolve; });
    let pinnedStarted!: () => void;
    const started = new Promise<void>((resolve) => { pinnedStarted = resolve; });
    let sharedStarted = false;
    let pinnedActive = false;
    let crossedHosts = false;
    const unavailable = new Error("unclaimed");
    const failed = new Error("started attempt timed out");
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
          crossedHosts ||= pinnedActive;
          return { outcome: "settled" };
        },
        sealStep: async () => SEALED,
      },
      pinnedTo: () => ({
        runToolCall: async (input) => {
          if (input.call.id === "waiting") throw unavailable;
          pinnedActive = true;
          pinnedStarted();
          await held;
          if (pinnedFails) throw failed;
          pinnedActive = false;
          return { outcome: "settled" };
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
    check("an unclaimed sibling waits for the pinned batch", !sharedStarted, { pinnedFails });
    finishPinned();
    const error = await result;
    check("fallback never overlaps a pinned tool", !crossedHosts, { pinnedFails });
    check(
      pinnedFails ? "an uncertain pinned attempt refuses migration" : "fallback resumes after the pinned tool ships",
      pinnedFails ? !sharedStarted && error === failed : sharedStarted && error === undefined,
      { sharedStarted, error: String(error) },
    );
  }

  {
    const failed = new Error("pinned attempt timed out");
    let seals = 0;
    const seal = async () => { seals++; return SEALED; };
    const step = makeSteppedStep({
      activities: {
        runModelCall: async () => ({ calls: [call("held")], sequential: false, ended: false, queue: "host-a" }),
        runToolCall: async () => ({ outcome: "settled" }),
        sealStep: seal,
      },
      pinnedTo: () => ({ runToolCall: async () => { throw failed; }, sealStep: seal }),
      isCancellation,
      isUnclaimed: () => false,
      nonCancellable: (fn) => fn(),
    });
    const error = await step(INPUT).then(() => undefined, (error: unknown) => error);
    check("an uncertain pinned tool prevents its seal", seals === 0 && error === failed, { seals });
  }

  const bad = failures.length;
  console.log(bad === 0 ? "l2-step-check: OK" : `l2-step-check: ${bad} failed`);
  process.exit(failures.length === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
