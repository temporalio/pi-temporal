// The durable step body: drive exactly one Pi turn (a prompt to agent_end) via Pi's SDK, with the
// session JSONL as the log. Runs inside a Temporal activity, so a worker crash re-runs this whole
// call; it must re-open the session file and not double-apply the prompt (see the idempotency
// guard). Node builtins and the Pi SDK are fine here (this is not workflow code).

import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import { Context } from "@temporalio/activity";
// Names per Pi's SDK docs (packages/coding-agent/docs/sdk.md). Verify against the installed
// version before trusting shapes; the SDK is young and may shift.
import {
  createAgentSession,
  SessionManager,
  ModelRuntime,
} from "@earendil-works/pi-coding-agent";
import type { RunPromptInput, RunPromptResult } from "./protocol.js";

const heartbeatEvery = (ms: number) => {
  const timer = setInterval(() => {
    try {
      Context.current().heartbeat();
    } catch {
      // outside an activity context (a unit test); ignore
    }
  }, ms);
  timer.unref?.();
  return () => clearInterval(timer);
};

// Best-effort idempotency: has this prompt already been applied and answered? We tag the prompt
// text with a hidden marker carrying the promptId, so a re-drive can find it in the recorded
// entries and skip re-running. TODO: verify Pi preserves the marker in the stored user message,
// and prefer a real side-channel (a promptId column) once the driver is proven.
const marker = (promptId: string) => `​[pi-temporal:${promptId}]`;

function alreadyApplied(sm: SessionManager, promptId: string): boolean {
  const entries = sm.getEntries() as Array<{ role?: string; text?: string; content?: string }>;
  const idx = entries.findIndex(
    (e) => (e.text ?? e.content ?? "").includes(marker(promptId)),
  );
  if (idx === -1) return false;
  // The prompt is recorded; treat it as done only if an assistant turn follows it.
  return entries.slice(idx + 1).some((e) => e.role === "assistant");
}

export function makeActivities(opts: { projectDir: string }) {
  async function runPrompt(input: RunPromptInput): Promise<RunPromptResult> {
    const stop = heartbeatEvery(3000);
    try {
      await mkdir(dirname(input.sessionFile), { recursive: true });
      const sessionManager = SessionManager.open(input.sessionFile);

      if (alreadyApplied(sessionManager, input.promptId)) {
        const entries = sessionManager.getEntries() as Array<{ role?: string; text?: string }>;
        const lastAssistant = [...entries].reverse().find((e) => e.role === "assistant");
        return { ran: false, finalText: lastAssistant?.text ?? "" };
      }

      const modelRuntime = await ModelRuntime.create();
      const { session } = await createAgentSession({
        sessionManager,
        modelRuntime,
        cwd: input.projectDir ?? opts.projectDir,
      });

      let finalText = "";
      const unsubscribe = session.subscribe((event: { type: string; message?: { text?: string } }) => {
        if (event.type === "turn_end" && event.message?.text) finalText = event.message.text;
      });

      try {
        // The marker rides the prompt so a re-drive can detect completion. It is zero-width, so it
        // does not change what the model reads in any meaningful way.
        await session.prompt(`${input.text}${marker(input.promptId)}`);
        await session.agent.waitForIdle();
        return { ran: true, finalText };
      } finally {
        unsubscribe?.();
        await session.dispose?.();
      }
    } finally {
      stop();
    }
  }

  return { runPrompt };
}

export type Activities = ReturnType<typeof makeActivities>;
