// The durable bodies of a turn, against Pi's SDK, with the session JSONL as the log. Two shapes:
//
// - runStep advances a turn by a whole step, one model call and the tools it asks for. One
//   activity, one step.
// - runModelCall, runToolCall and sealStep split that step three ways, so a tool call is its own
//   unit of work with its own retry policy, its own timeout, and its own place in history.
//
// Both re-open the session file, so either can land on any worker that can reach it. Node
// builtins and the Pi SDK are fine here (not workflow code).

import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import { Context } from "@temporalio/activity";
import {
  createAgentSession,
  findDanglingToolCalls,
  SessionManager,
  ModelRuntime,
  unknownToolCallOutcome,
  type AgentSession,
  type TurnToolCallOutcome,
} from "@earendil-works/pi-coding-agent";
import type {
  ModelCallResult,
  RunStepInput,
  RunStepResult,
  SealStepInput,
  ToolCallInput,
  ToolCallResult,
} from "./protocol.js";
import * as pending from "./pending.js";
import { withSessionLock } from "./session-lock.js";
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

type Msg = { role?: string; content?: unknown; toolCallId?: string };

const markerPresent = (messages: Msg[], promptId: string) =>
  messages.some((m) => textOf(m.content).includes(marker(promptId)));

// The turn's own last word, which is empty when it ended on a provider error. Reaching further
// back for something non-empty reports a previous turn's answer as this one's.
const lastAssistantText = (messages: Msg[]) => {
  const answer = [...messages].reverse().find((m) => m.role === "assistant");
  return answer ? textOf(answer.content) : "";
};

// Only what the step being run has answered. A call id is unique within the message that asked
// for it, so a provider that reuses one would otherwise find an earlier step's result.
const answeredInTranscript = (messages: Msg[], callId: string) => {
  let opened = -1;
  for (let i = messages.length - 1; i >= 0; i--) {
    if (messages[i].role === "assistant") {
      opened = i;
      break;
    }
  }
  return messages.slice(opened + 1).some((m) => m.role === "toolResult" && m.toolCallId === callId);
};

type Block = { type?: string; id?: string };

const blocksOf = (content: unknown) => (Array.isArray(content) ? (content as Block[]) : []);

const recordedInTranscript = (messages: Msg[], callId: string) =>
  messages.some(
    (m) =>
      m.role === "assistant" &&
      blocksOf(m.content).some((b) => b.type === "toolCall" && b.id === callId),
  );

/** The calls of the trailing assistant message, which is what prepareStep would settle. */
const danglingCallIds = (messages: Msg[]) =>
  findDanglingToolCalls(messages as unknown as Parameters<typeof findDanglingToolCalls>[0]).map(
    (call) => call.id,
  );

const noDispatchStarted = async (file: string, step: number, callIds: readonly string[]) => {
  if (callIds.length === 0) return false;
  for (const callId of callIds) {
    if (await pending.wasDispatched(file, step, callId)) return false;
  }
  return true;
};

export interface ActivityOptions {
  // Where the agent's tools run. The embedded worker points this at the pi session's own cwd.
  readonly projectDir: string;
  readonly provider?: string;
  // Matched as a substring of the model id, so "mini" picks the first mini the provider offers.
  readonly modelHint?: string;
  // Only needed when the key is not already in Pi's auth store.
  readonly apiKey?: string;
}

export function makeActivities(opts: ActivityOptions) {
  const provider = opts.provider ?? "openai";

  async function openSession(sessionFile: string): Promise<AgentSession> {
    await mkdir(dirname(sessionFile), { recursive: true });
    const sessionManager = SessionManager.open(sessionFile);

    const modelRuntime = await ModelRuntime.create();
    if (opts.apiKey) await modelRuntime.setRuntimeApiKey(provider, opts.apiKey);
    const available = await modelRuntime.getAvailable(provider);
    const hint = opts.modelHint ?? "mini";
    const model = available.find((m) => m.id.includes(hint)) ?? available[0];
    if (!model) {
      throw new Error(`no ${provider} model available; check the key and provider support`);
    }

    const { session } = await createAgentSession({
      sessionManager,
      modelRuntime,
      model,
      cwd: opts.projectDir,
    });
    return session;
  }

  /** Put the prompt in the transcript, or settle what an earlier attempt left behind. Returns the
   * turn's answer when there is nothing left to run. Only the stepped mode keeps dispatch notes,
   * so only it can tell an interrupted call from one nothing has run yet. */
  async function readyForStep(
    session: AgentSession,
    input: RunStepInput,
    stepped: boolean,
  ): Promise<RunStepResult | undefined> {
    const messages = () => session.state.messages as Msg[];
    // What the step before this one kept. Its own results stay until the step after it, so a
    // retried seal still reads real outcomes.
    if (stepped) await pending.sweep(input.sessionFile, input.step);

    if (!markerPresent(messages(), input.promptId)) {
      // Settle what the turn that stopped left behind first. A call with no result is a payload no
      // provider accepts, so a prompt recorded behind one makes every later turn of the session
      // fail rather than just the interrupted one.
      session.prepareStep();
      // Record the prompt without running it, so the first step is a step like any other and the
      // crash window before it is one Temporal already covers.
      if (!(await session.recordPrompt(`${input.text}${marker(input.promptId)}`))) {
        throw new Error("Pi did not record the prompt; an extension may have taken the text");
      }
      return undefined;
    }

    // A retry of this turn. Calls the model recorded but no dispatch ever started are not
    // interrupted work, so leave them for the model call to hand back. Settling them here would
    // tell the model that tools which never ran may have taken effect, and pay for a second
    // response on top.
    const dangling = danglingCallIds(messages());
    if (stepped && (await noDispatchStarted(input.sessionFile, input.step, dangling))) {
      return undefined;
    }

    if (!session.prepareStep()) {
      // The prompt is recorded and the turn already has its answer. That is a retry landing after
      // the last step finished but before its result reached Temporal.
      return { done: true, finalText: lastAssistantText(messages()) };
    }
    return undefined;
  }

  async function runStep(input: RunStepInput): Promise<RunStepResult> {
    const stop = heartbeatEvery(3000);
    try {
      return await withSessionLock(input.sessionFile, async () => {
        const session = await openSession(input.sessionFile);
        try {
          const settled = await readyForStep(session, input, false);
          if (settled) return settled;

          // prepareStep settled anything the crash left dangling, so this is a plain step: a tool
          // whose result never landed is reported as unknown, not re-run behind the model's back.
          const { done } = await session.step();
          await session.waitForIdle();
          const messages = session.state.messages as Msg[];
          return { done, finalText: done ? lastAssistantText(messages) : "" };
        } finally {
          session.dispose();
        }
      });
    } finally {
      stop();
    }
  }

  /** The model call of one step. The calls it reports are recorded and left for the workflow to
   * dispatch, one activity each. */
  async function runModelCall(input: RunStepInput): Promise<ModelCallResult> {
    const stop = heartbeatEvery(3000);
    try {
      return await withSessionLock(input.sessionFile, async () => {
        const session = await openSession(input.sessionFile);
        try {
          const settled = await readyForStep(session, input, true);
          if (settled) return { settled, calls: [], sequential: false, ended: true };

          const outcome = await session.modelCall();
          return {
            calls: outcome.toolCalls.map((call) => ({ id: call.id, name: call.name })),
            sequential: outcome.sequential,
            ended: outcome.ended,
          };
        } finally {
          session.dispose();
        }
      });
    } finally {
      stop();
    }
  }

  /** One recorded call of the current step. Its result is kept beside the session file until the
   * seal records it, so a tool that ran is not asked to run again. */
  async function runToolCall(input: ToolCallInput): Promise<ToolCallResult> {
    const stop = heartbeatEvery(3000);
    try {
      const session = await openSession(input.sessionFile);
      try {
        // The transcript first: a result in it means a whole dispatch and a seal already
        // happened, and the kept files for the call are what is left behind.
        if (answeredInTranscript(session.state.messages as Msg[], input.call.id)) {
          await pending.forget(input.sessionFile, input.step, [input.call.id]);
          return { outcome: "already-settled" };
        }

        if (await pending.readResult(input.sessionFile, input.step, input.call.id)) {
          return { outcome: "already-settled" };
        }

        // A call the transcript does not hold is one this attempt cannot run. Retryable on
        // purpose, and capped by the proxy: on shared storage the message that records it may not
        // have reached this host yet, and answering a stale read with a permanent failure tells
        // the model a tool that never ran may have taken effect.
        if (!recordedInTranscript(session.state.messages as Msg[], input.call.id)) {
          throw new Error(`no recorded tool call ${input.call.id} in ${input.sessionId}`);
        }

        if (await pending.wasDispatched(input.sessionFile, input.step, input.call.id)) {
          // A dispatch was inside this tool when it stopped, so the tool can have taken effect.
          // Re-running a push or a delete that already happened is the worse failure, so the
          // model is told the outcome instead of the tool being asked again.
          const unknown = unknownToolCallOutcome(input.call);
          await pending.keepResult(input.sessionFile, input.step, input.call.id, unknown);
          return { outcome: "unknown" };
        }

        // Before the tool can have any effect, so what follows reads as "a dispatch was inside
        // it". A stop in the moment between costs one call an unknown outcome, which is the
        // direction to be wrong in.
        await pending.noteDispatch(input.sessionFile, input.step, input.call.id);
        const outcome = await session.runToolCall(input.call.id);
        if (!outcome) return { outcome: "already-settled" };

        await pending.keepResult(input.sessionFile, input.step, input.call.id, outcome);
        return { outcome: "settled" };
      } finally {
        session.dispose();
      }
    } finally {
      stop();
    }
  }

  /** Close the step with what its calls produced, in the order the model asked for them. */
  async function sealStep(input: SealStepInput): Promise<RunStepResult> {
    const stop = heartbeatEvery(3000);
    try {
      return await withSessionLock(input.sessionFile, async () => {
        const session = await openSession(input.sessionFile);
        try {
          const results: TurnToolCallOutcome[] = [];
          for (const call of input.calls) {
            // A call with nothing kept for it is one whose dispatch never came back. Sealing
            // without it would leave the transcript holding a call no result answers, which is a
            // payload no provider accepts.
            const kept = await pending.readResult(input.sessionFile, input.step, call.id);
            results.push(kept ?? unknownToolCallOutcome(call));
          }

          // Named, so a message something else appended between the model call and here cannot
          // be the one these results are attributed to.
          const { done } = await session.sealStep(results, {
            expectCalls: input.calls.map((call) => call.id),
            retryAttempt: input.retryAttempt,
          });
          await session.waitForIdle();
          // What is kept stays until the next step sweeps it. A seal that dropped its own
          // results would leave a retry of that seal reading an empty batch, and an empty batch
          // reads as one that wants another step even when a tool asked the turn to stop.
          const answer = lastAssistantText(session.state.messages as Msg[]);
          return { done, finalText: done ? answer : "" };
        } finally {
          session.dispose();
        }
      });
    } finally {
      stop();
    }
  }

  return { runStep, runModelCall, runToolCall, sealStep };
}

export type Activities = ReturnType<typeof makeActivities>;
