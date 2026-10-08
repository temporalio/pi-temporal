// Pi's session format and retry state stay in this adapter so the core can serve other agents.

import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import { ApplicationFailure } from "@temporalio/activity";
import {
  createAgentSession,
  findDanglingToolCalls,
  ModelRuntime,
  SessionManager,
  unknownToolCallOutcome,
  type AgentSession as PiSession,
  type TurnToolCallOutcome,
} from "@earendil-works/pi-coding-agent";
import type { Agent, AgentSession, AgentState, ToolCallRef } from "../core/agent.js";
import type { Spend } from "../core/protocol.js";
import { textOf } from "./messages.js";

export interface PiOptions {
  // Where the agent's tools run. The embedded Worker points this at the pi session's own cwd.
  readonly projectDir: string;
  readonly provider?: string;
  // Matched as a substring of the model id, so "mini" picks the first mini the provider offers.
  readonly modelHint?: string;
  // Only needed when the key is not already in Pi's auth store.
  readonly apiKey?: string;
}

/** For checks: open a fake Pi session instead of a real one. */
export interface PiDependencies {
  readonly openSession?: (file: string, guard?: () => void) => Promise<PiSession>;
}

// A zero-width marker with the prompt id, so an Activity that runs again can tell whether this
// prompt was already recorded.
const marker = (promptId: string) => `​[pi-temporal:${promptId}]`;

type Msg = { role?: string; content?: unknown; toolCallId?: string };
type Block = { type?: string; id?: string };
type Entry = { type: string; customType?: string; data?: unknown; message?: unknown };
type Record = {
  getBranch(): Entry[];
  appendCustomEntry(customType: string, data: unknown): string;
};

const blocksOf = (content: unknown) => (Array.isArray(content) ? (content as Block[]) : []);
const latestResponse = (messages: Msg[]) =>
  [...messages].reverse().find((m) => m.role === "assistant");

// Optional, because the checks' fake sessions keep no record. A real session always has one.
const recordOf = (session: PiSession) =>
  (session as unknown as { sessionManager?: Record }).sessionManager;

const latestEntryOf = (
  record: Record | undefined,
  type: string,
  skip?: (data: unknown) => boolean,
) => {
  const branch = record?.getBranch() ?? [];
  for (let i = branch.length - 1; i >= 0; i--) {
    const entry = branch[i];
    if (entry.type !== "custom" || entry.customType !== type) continue;
    if (skip?.(entry.data)) continue;
    return entry.data;
  }
  return undefined;
};

/** Whether this turn's prompt is in the session file. A compaction can summarize the prompt out
 * of the context, but the branch keeps every raw message entry. */
const promptRecorded = (session: PiSession, promptId: string): boolean => {
  const record = recordOf(session);
  const messages =
    typeof record?.getBranch === "function"
      ? record
          .getBranch()
          .flatMap((entry) => (entry.type === "message" ? [entry.message as Msg] : []))
      : // Fake sessions in checks have no record. Their context is the whole transcript.
        (session.state.messages as Msg[]);
  return messages.some((m) => textOf(m.content).includes(marker(promptId)));
};

// Pi's retry count and its one compact-and-retry, carried by the Workflow between steps.
interface PiState {
  readonly retryAttempt?: number;
  readonly overflowRecoveryAttempted?: boolean;
}

function wrap(session: PiSession): AgentSession {
  const messages = () => session.state.messages as Msg[];
  return {
    prepareStep: () => session.prepareStep(),
    hasPrompt: (promptId) => promptRecorded(session, promptId),
    recordPrompt: (promptId, text) => session.recordPrompt(`${text}${marker(promptId)}`),
    // A stop aborts the call like a user stop, so Pi records an aborted response and the turn
    // ends as stopped. A unit already stopped asks the model nothing, and isn't billed.
    async modelCall(signal) {
      const outcome = await session.modelCall({ signal });
      return {
        toolCalls: outcome.toolCalls.map((call) => ({ id: call.id, name: call.name })),
        sequential: outcome.sequential,
        ended: outcome.ended,
      };
    },
    runToolCall: (callId, signal) => session.runToolCall(callId, { signal }),
    async sealStep(outcomes, { expectCalls, agentState, postRun }) {
      const state = (agentState ?? {}) as PiState;
      const sealed = await session.sealStep(outcomes as TurnToolCallOutcome[], {
        expectCalls,
        retryAttempt: state.retryAttempt ?? 0,
        overflowRecoveryAttempted: state.overflowRecoveryAttempted,
        postRun,
      });
      const next: PiState = {
        retryAttempt: sealed.retryAttempt,
        overflowRecoveryAttempted: sealed.overflowRecoveryAttempted,
      };
      return { done: sealed.done, agentState: next as AgentState };
    },
    waitForIdle: () => session.waitForIdle(),
    dispose: () => session.dispose(),

    // Only results after the latest response. Providers may reuse call ids across responses.
    answered(callId) {
      const all = messages();
      const opened = all.lastIndexOf(latestResponse(all) as Msg);
      return all.slice(opened + 1).some((m) => m.role === "toolResult" && m.toolCallId === callId);
    },
    asked: (callId) =>
      blocksOf(latestResponse(messages())?.content).some(
        (b) => b.type === "toolCall" && b.id === callId,
      ),
    unanswered: () =>
      findDanglingToolCalls(
        messages() as unknown as Parameters<typeof findDanglingToolCalls>[0],
      ).map((call) => call.id),
    endsWithResponse: () => messages().at(-1)?.role === "assistant",
    // The latest response only, even if empty. Looking further back would return an earlier
    // turn's answer.
    lastAnswer: () => textOf(latestResponse(messages())?.content),
    spend(): Spend | undefined {
      // Session-wide billing, compacted history included. Undefined for fake sessions in checks.
      type Stats = { tokens?: { total?: number }; cost?: number };
      const stats = (session as { getSessionStats?: () => Stats }).getSessionStats;
      if (typeof stats !== "function") return undefined;
      try {
        const now = stats.call(session);
        return { tokens: now.tokens?.total ?? 0, cost: now.cost ?? 0 };
      } catch {
        return undefined;
      }
    },
    latestEntry: (type, skip) => latestEntryOf(recordOf(session), type, skip),
    appendEntry: (type, data) => void recordOf(session)?.appendCustomEntry(type, data),
  };
}

export function piAgent(opts: PiOptions, dependencies: PiDependencies = {}): Agent {
  const provider = opts.provider ?? "openai";

  async function openSession(sessionFile: string, guard: () => void): Promise<PiSession> {
    if (dependencies.openSession) return dependencies.openSession(sessionFile, guard);
    await mkdir(dirname(sessionFile), { recursive: true });
    const sessionManager = SessionManager.open(sessionFile);
    sessionManager.setWriteGuard(guard);

    // A bad key or provider fails the same way on every attempt, so don't retry it.
    const modelRuntime = await ModelRuntime.create().catch((err: unknown) => {
      throw ApplicationFailure.nonRetryable(`could not set up the model runtime: ${String(err)}`);
    });
    const available = await (async () => {
      if (opts.apiKey) await modelRuntime.setRuntimeApiKey(provider, opts.apiKey);
      return await modelRuntime.getAvailable(provider);
    })().catch((err: unknown) => {
      throw ApplicationFailure.nonRetryable(`could not set up ${provider}: ${String(err)}`);
    });
    // Default to the provider's small, cheap model.
    const hint = opts.modelHint ?? (provider === "anthropic" ? "haiku" : "mini");
    const model = available.find((m) => m.id.includes(hint)) ?? available[0];
    if (!model) {
      throw ApplicationFailure.nonRetryable(
        `no ${provider} model available; check the key and provider support`,
      );
    }

    const { session } = await createAgentSession({
      sessionManager,
      modelRuntime,
      model,
      cwd: opts.projectDir,
    });
    return session;
  }

  return {
    open: async (sessionFile, guard) => wrap(await openSession(sessionFile, guard)),
    async openRecord(sessionFile, guard) {
      // A fake session in a check stands in for the record too.
      const record = dependencies.openSession
        ? recordOf(await dependencies.openSession(sessionFile, guard))
        : (() => {
            const manager = SessionManager.open(sessionFile);
            manager.setWriteGuard(guard);
            return manager as unknown as Record;
          })();
      if (!record) return undefined;
      return {
        latestEntry: (type, skip) => latestEntryOf(record, type, skip),
        appendEntry: (type, data) => void record.appendCustomEntry(type, data),
      };
    },
    unknownOutcome: (call: ToolCallRef) => unknownToolCallOutcome(call),
    notRunOutcome(call: ToolCallRef) {
      const outcome = unknownToolCallOutcome(call);
      outcome.message.content = [
        { type: "text", text: "This tool call did not run. The turn stopped before it started." },
      ];
      return outcome;
    },
  };
}
