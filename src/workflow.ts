// The per-session durable executor. One workflow per Pi session. It owns the small control state
// (the pending-prompt queue and where a turn is up to); the large conversation state lives in Pi's
// session JSONL, which the runStep activity reads and writes. One step is one activity, so a
// worker crash re-drives that step alone, and every step before it stays done.
//
// Sandbox-safe: only @temporalio/workflow and type-only protocol imports. No Pi SDK, no Node.

import {
  proxyActivities,
  defineSignal,
  defineQuery,
  setHandler,
  condition,
  CancellationScope,
  isCancellation,
  log,
} from "@temporalio/workflow";
import { MAX_STEPS_PER_TURN, QUERIES, SIGNALS } from "./protocol.js";
import type { PromptInput, RunStepInput, RunStepResult, SessionTurnOptions, TurnState } from "./protocol.js";
import { makeSteppedStep, type SteppedActivities } from "./l2-step.js";

const activityOptions = {
  // One step is a single model call plus the tools it asks for, so minutes, not hours. The
  // heartbeat is the real liveness bound and re-drives within seconds of a worker death.
  startToCloseTimeout: "30 minutes",
  heartbeatTimeout: "30 seconds",
  retry: { maximumAttempts: 100 },
} as const;

const { runStep } = proxyActivities<{
  runStep(input: RunStepInput): Promise<RunStepResult>;
}>(activityOptions);

// Separate proxies are the point of the split, not an accident of it: a tool that hangs no longer
// holds the model call and every other tool of the same step under one shared timeout.
const { runModelCall } = proxyActivities<SteppedActivities>(activityOptions);
const { runToolCall } = proxyActivities<SteppedActivities>(activityOptions);
// Reading what the calls produced and writing them into the transcript. It should not inherit a
// step-sized backstop.
const { sealStep } = proxyActivities<SteppedActivities>({
  ...activityOptions,
  startToCloseTimeout: "5 minutes",
});

export const submitPrompt = defineSignal<[PromptInput]>(SIGNALS.submitPrompt);
export const interrupt = defineSignal<[]>(SIGNALS.interrupt);
export const turnState = defineQuery<TurnState>(QUERIES.turnState);

export async function piSession(
  sessionId: string,
  sessionFile: string,
  options?: SessionTurnOptions,
): Promise<void> {
  const idleTimeout = options?.idleTimeout ?? "5 minutes";
  // Same loop either way. Only what "one step" means differs, so wake, interrupt, the step
  // ceiling and idle retirement are unchanged.
  const runTurnStep: (input: RunStepInput) => Promise<RunStepResult> = options?.stepped
    ? makeSteppedStep({
        activities: { runModelCall, runToolCall, sealStep },
        isCancellation,
        // The SDK's logger, so a line carries its workflow and run id and is suppressed on replay.
        log: (message, attributes) => log.info(message, attributes),
      })
    : runStep;
  const queue: PromptInput[] = [];
  let current: CancellationScope | undefined;
  let running: TurnState["running"];
  let finished: TurnState["finished"];

  setHandler(submitPrompt, (p) => {
    queue.push(p);
  });
  setHandler(interrupt, () => {
    current?.cancel();
  });
  setHandler(turnState, () => ({ queued: queue.length, running, finished }));

  for (;;) {
    const woke = await condition(() => queue.length > 0, idleTimeout);
    if (!woke && queue.length === 0) return; // idle: retire; the next prompt starts a fresh run

    const prompt = queue.shift()!;
    let outcome: NonNullable<TurnState["finished"]>["outcome"] = "ceiling";
    let finalText = "";
    try {
      await CancellationScope.cancellable(async () => {
        current = CancellationScope.current();
        for (let step = 1; step <= MAX_STEPS_PER_TURN; step++) {
          running = { promptId: prompt.promptId, step };
          const input: RunStepInput = { sessionId, sessionFile, step, ...prompt };
          const result = await runTurnStep(input);
          if (result.done) {
            outcome = "answered";
            finalText = result.finalText;
            return;
          }
        }
        log.warn("turn hit the step ceiling and was left where it stopped", {
          sessionId,
          promptId: prompt.promptId,
          maxSteps: MAX_STEPS_PER_TURN,
        });
      });
    } catch (err) {
      // An interrupt cancels the in-flight step; the session keeps serving later prompts. A real
      // run error is already recorded in the session log, so we log-and-continue rather than fail
      // the whole session. (Surfacing typed errors to a resume caller is a follow-up.)
      outcome = "interrupted";
      if (!isCancellation(err)) {
        // TODO: classify and, for genuine failures, decide retry vs surface. For now, continue.
      }
    } finally {
      current = undefined;
      running = undefined;
      finished = { promptId: prompt.promptId, outcome, finalText };
    }
  }
}
