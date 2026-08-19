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

// The prompt carries a zero-width marker with its promptId, so a re-driven activity can tell
// whether this exact prompt was already recorded on a prior attempt. Present means: do not prompt
// again; instead finish whatever the interrupted turn left behind.
const marker = (promptId: string) => `​[pi-temporal:${promptId}]`;

type Msg = { role?: string; content?: unknown };

const markerPresent = (messages: Msg[], promptId: string) =>
  messages.some((m) => textOf(m.content).includes(marker(promptId)));

const lastAssistantText = (messages: Msg[]) => {
  const answer = [...messages].reverse().find((m) => m.role === "assistant" && textOf(m.content).trim() !== "");
  return answer ? textOf(answer.content) : "";
};

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
        // A retry of an already-recorded prompt: finish the turn the crash interrupted (repair any
        // dangling tool call, or drive an unanswered prompt to completion) rather than re-prompting,
        // which would duplicate the prompt and re-run side effects.
        if (markerPresent(session.state.messages as Msg[], input.promptId)) {
          await session.resumeInterruptedTurn();
          await session.waitForIdle();
          return { ran: false, finalText: lastAssistantText(session.state.messages as Msg[]) };
        }

        await session.prompt(`${input.text}${marker(input.promptId)}`);
        await session.waitForIdle();
        return { ran: true, finalText: lastAssistantText(session.state.messages as Msg[]) };
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
