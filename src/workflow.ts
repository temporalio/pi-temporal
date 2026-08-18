// The per-session durable executor. One workflow per Pi session. It owns the small control state
// (the pending-prompt queue); the large conversation state lives in Pi's session JSONL, which the
// runPrompt activity reads and writes. A crash re-drives the in-flight turn on another worker.
//
// Sandbox-safe: only @temporalio/workflow and type-only protocol imports. No Pi SDK, no Node.

import {
  proxyActivities,
  defineSignal,
  setHandler,
  condition,
  CancellationScope,
  isCancellation,
} from "@temporalio/workflow";
import { SIGNALS } from "./protocol.js";
import type { PromptInput, RunPromptInput, RunPromptResult, SessionTurnOptions } from "./protocol.js";

const { runPrompt } = proxyActivities<{
  runPrompt(input: RunPromptInput): Promise<RunPromptResult>;
}>({
  // A turn is a full agent run (many model calls and tools), so give it room; the heartbeat is the
  // real liveness bound and re-drives within seconds of a worker death.
  startToCloseTimeout: "1 hour",
  heartbeatTimeout: "30 seconds",
  retry: { maximumAttempts: 100 },
});

export const submitPrompt = defineSignal<[PromptInput]>(SIGNALS.submitPrompt);
export const interrupt = defineSignal<[]>(SIGNALS.interrupt);

export async function piSession(
  sessionId: string,
  sessionFile: string,
  options?: SessionTurnOptions,
): Promise<void> {
  const idleTimeout = options?.idleTimeout ?? "5 minutes";
  const queue: PromptInput[] = [];
  let current: CancellationScope | undefined;

  setHandler(submitPrompt, (p) => {
    queue.push(p);
  });
  setHandler(interrupt, () => {
    current?.cancel();
  });

  for (;;) {
    const woke = await condition(() => queue.length > 0, idleTimeout);
    if (!woke && queue.length === 0) return; // idle: retire; the next prompt starts a fresh run

    const prompt = queue.shift()!;
    const input: RunPromptInput = { sessionId, sessionFile, ...prompt };
    try {
      await CancellationScope.cancellable(async () => {
        current = CancellationScope.current();
        await runPrompt(input);
      });
    } catch (err) {
      // An interrupt cancels the in-flight turn; the session keeps serving later prompts. A real
      // run error is already recorded in the session log, so we log-and-continue rather than fail
      // the whole session. (Surfacing typed errors to a resume caller is a follow-up.)
      if (!isCancellation(err)) {
        // TODO: classify and, for genuine failures, decide retry vs surface. For now, continue.
      }
    } finally {
      current = undefined;
    }
  }
}
