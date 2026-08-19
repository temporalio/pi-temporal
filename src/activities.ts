// The durable step body: drive exactly one Pi turn (a prompt to idle) via Pi's SDK, with the
// session JSONL as the log. Runs inside a Temporal activity, so a worker crash re-runs this whole
// call; it re-opens the session file. Node builtins and the Pi SDK are fine here (not workflow code).

import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import { Context } from "@temporalio/activity";
import { createAgentSession, SessionManager, ModelRuntime } from "@earendil-works/pi-coding-agent";
import type { RunPromptInput, RunPromptResult } from "./protocol.js";
import { textOf } from "./messages.js";

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

// Best-effort idempotency: tag the prompt with a zero-width marker carrying the promptId, so a
// re-drive can find it in the recorded messages. Returns the assistant's answer if this prompt was
// already applied AND answered with real text, else null (meaning: run it).
//
// VERIFIED LIMIT (crash test): this is coarse and cannot cleanly recover a crash MID-turn. If the
// worker dies after Pi recorded a tool-call assistant message but before the final answer, the
// turn has a dangling tool call. Pi's SDK exposes no way to resume an in-flight turn, so on retry
// we can only (a) re-prompt, which duplicates the prompt and re-runs any side effect, or (b) treat
// a non-empty final answer as the completion signal and otherwise re-run. We take (b): a tool-call
// message has empty text, so it does not count as done, and the turn is re-driven (accepting the
// duplicate-side-effect risk). Clean mid-turn recovery needs step-level control, which Pi does not
// expose today (see README). Between turns, recovery is clean.
const marker = (promptId: string) => `​[pi-temporal:${promptId}]`;

type Msg = { role?: string; content?: unknown };

function completedAnswer(messages: Msg[], promptId: string): string | null {
  const idx = messages.findIndex((m) => textOf(m.content).includes(marker(promptId)));
  if (idx === -1) return null;
  const answer = [...messages.slice(idx + 1)]
    .reverse()
    .find((m) => m.role === "assistant" && textOf(m.content).trim() !== "");
  return answer ? textOf(answer.content) : null;
}

export function makeActivities(opts: { projectDir: string; openaiKey?: string; modelHint?: string }) {
  async function runPrompt(input: RunPromptInput): Promise<RunPromptResult> {
    const stop = heartbeatEvery(3000);
    try {
      await mkdir(dirname(input.sessionFile), { recursive: true });
      const sessionManager = SessionManager.open(input.sessionFile);

      const modelRuntime = await ModelRuntime.create();
      if (opts.openaiKey) await modelRuntime.setRuntimeApiKey("openai", opts.openaiKey);
      const available = await modelRuntime.getAvailable("openai");
      const hint = opts.modelHint ?? "mini";
      const model = available.find((m) => m.id.includes(hint)) ?? available[0];
      if (!model) throw new Error("no OpenAI model available; check the key and provider support");

      const { session } = await createAgentSession({
        sessionManager,
        modelRuntime,
        model,
        cwd: opts.projectDir,
      });

      try {
        const already = completedAnswer(session.state.messages as Msg[], input.promptId);
        if (already !== null) return { ran: false, finalText: already };

        await session.prompt(`${input.text}${marker(input.promptId)}`);
        await session.waitForIdle();

        const answer = [...(session.state.messages as Msg[])].reverse().find((m) => m.role === "assistant");
        return { ran: true, finalText: answer ? textOf(answer.content) : "" };
      } finally {
        session.dispose();
      }
    } finally {
      stop();
    }
  }

  return { runPrompt };
}

export type Activities = ReturnType<typeof makeActivities>;
