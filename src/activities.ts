// The durable step body: advance one Pi turn by exactly one step (one model call and the tools it
// asks for) via Pi's SDK, with the session JSONL as the log. Runs inside a Temporal activity, so a
// worker crash re-runs this one step; it re-opens the session file. Node builtins and the Pi SDK
// are fine here (not workflow code).

import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import { Context } from "@temporalio/activity";
import { createAgentSession, SessionManager, ModelRuntime } from "@earendil-works/pi-coding-agent";
import type { RunStepInput, RunStepResult } from "./protocol.js";
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
// whether this exact prompt was already recorded on a prior attempt. Present means: do not record
// it again; step whatever the transcript already holds.
const marker = (promptId: string) => `​[pi-temporal:${promptId}]`;

type Msg = { role?: string; content?: unknown };

const markerPresent = (messages: Msg[], promptId: string) =>
  messages.some((m) => textOf(m.content).includes(marker(promptId)));

const lastAssistantText = (messages: Msg[]) => {
  const answer = [...messages].reverse().find((m) => m.role === "assistant" && textOf(m.content).trim() !== "");
  return answer ? textOf(answer.content) : "";
};

export function makeActivities(opts: { projectDir: string; openaiKey?: string; modelHint?: string }) {
  async function runStep(input: RunStepInput): Promise<RunStepResult> {
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
        if (!markerPresent(session.state.messages as Msg[], input.promptId)) {
          // A fresh turn. Record the prompt without running it, so the first step is a step like
          // any other and the crash window before it is one Temporal already covers.
          if (!(await session.recordPrompt(`${input.text}${marker(input.promptId)}`))) {
            throw new Error("Pi did not record the prompt; an extension may have taken the text");
          }
        } else if (!session.prepareStep()) {
          // The prompt is recorded and the turn already has its answer. That is a retry landing
          // after the last step finished but before its result reached Temporal.
          return { done: true, finalText: lastAssistantText(session.state.messages as Msg[]) };
        }

        // prepareStep settled anything the crash left dangling, so this is a plain step: a tool
        // whose result never landed is reported as unknown, not re-run behind the model's back.
        const { done } = await session.step();
        await session.waitForIdle();
        const messages = session.state.messages as Msg[];
        return { done, finalText: done ? lastAssistantText(messages) : "" };
      } finally {
        session.dispose();
      }
    } finally {
      stop();
    }
  }

  return { runStep };
}

export type Activities = ReturnType<typeof makeActivities>;
