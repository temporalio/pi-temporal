// Activity bodies of a turn, against Pi's SDK, with the session JSONL as the log.
// `runStep` runs a whole step in one activity. `runModelCall`, `runToolCall` and `sealStep` split
// it so each tool call is its own unit of work. All re-open the session file, so any worker that
// can reach it can run them.

import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import { ApplicationFailure, Context } from "@temporalio/activity";
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
  Spend,
  ToolCallInput,
  ToolCallResult,
} from "./protocol.js";
import { FAILED_BEFORE_CLAIM } from "./protocol.js";
import { SHIP_TREE_NEEDS_STEPS } from "./config.js";
import * as pending from "./pending.js";
import * as worktree from "./worktree.js";
import { withSessionLock } from "./session-lock.js";
import { textOf } from "./messages.js";

// Retry delay after a host refuses the project directory. Short, so the work finds a free host.
const REFUSAL_RETRY = "2 seconds";

// Whether Temporal asked this activity to stop. Only as fresh as the last heartbeat.
const stopRequested = () => {
  try {
    return Context.current().cancellationSignal.aborted;
  } catch {
    return false;
  }
};

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

// A zero-width marker with the promptId, so a re-driven activity can tell whether this prompt was
// already recorded.
const marker = (promptId: string) => `​[pi-temporal:${promptId}]`;

type Msg = { role?: string; content?: unknown; toolCallId?: string };

const markerPresent = (messages: Msg[], promptId: string) =>
  messages.some((m) => textOf(m.content).includes(marker(promptId)));

// The last assistant message only, even if empty. Looking further back would return an earlier
// turn's answer.
const lastAssistantText = (messages: Msg[]) => {
  const answer = [...messages].reverse().find((m) => m.role === "assistant");
  return answer ? textOf(answer.content) : "";
};

// Only results after the last assistant message. Providers may reuse call ids across messages.
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

/** The calls nothing answered yet, which is what prepareStep would settle. */
const danglingCallIds = (messages: Msg[]) =>
  findDanglingToolCalls(messages as unknown as Parameters<typeof findDanglingToolCalls>[0]).map(
    (call) => call.id,
  );

const noDispatchStarted = async (
  file: string,
  turn: string,
  step: number,
  callIds: readonly string[],
) => {
  if (callIds.length === 0) return false;
  for (const callId of callIds) {
    if (await pending.wasDispatched(file, turn, step, callId)) return false;
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
  // Ship the project's files with the session, so another worker sees the last one's changes.
  readonly shipTree?: boolean;
  // This worker's own queue. The model call reports it so the rest of the step runs on this host.
  readonly stepQueue?: string;
}

export function makeActivities(
  opts: ActivityOptions,
  dependencies: { openSession?: (file: string, guard?: () => void) => Promise<AgentSession> } = {},
) {
  const provider = opts.provider ?? "openai";

  // Restore the project tree before any tool runs. Errors propagate on purpose: a step must not
  // run against the wrong files. Failing sends the work to a host that can do it.
  const bringTree = async (sessionFile: string, current?: worktree.Writer) => {
    if (!opts.shipTree) return;
    await worktree.ensure(opts.projectDir, sessionFile, current).catch((err: unknown) => {
      // A refusal is this host saying no, not a failing unit. Use a fixed delay so the backoff
      // doesn't grow while a free host sits idle.
      if (err instanceof worktree.Quarantined) {
        throw ApplicationFailure.create({
          message: err.message,
          type: "WorktreeQuarantined",
          nextRetryDelay: REFUSAL_RETRY,
        });
      }
      throw err;
    });
  };
  // A lock can be reclaimed while its holder is blocked, so re-check before writing. On failure,
  // Temporal retries and the retry takes the lock.
  const stillOurs = async (owned: () => Promise<boolean>, what: string) => {
    if (!(await owned())) {
      throw new Error(`lost the session lock before ${what}; another attempt has it`);
    }
  };

  // Guards Pi's own appends. The assistant message lands at the end of a stream that can run for
  // minutes, long after any awaited check.
  const writeGuard = (ownedNow: () => boolean, what: string) => () => {
    if (!ownedNow()) {
      throw new Error(`lost the session lock during ${what}; another attempt has it`);
    }
  };

  const shipTree = async (
    sessionFile: string,
    of: { readonly current?: worktree.Writer; readonly fence?: worktree.Fence } = {},
  ) => {
    if (!opts.shipTree) return;
    // Don't fail the step. The result is recorded and a retry would not re-run the tool. Set the
    // files aside instead, since they are the only record of what the tool did.
    await worktree.capture(opts.projectDir, sessionFile, of).catch(async (err) => {
      console.error(`could not ship the project tree: ${String(err)}`);
      await worktree.setAside(opts.projectDir, sessionFile).catch((keepErr) => {
        console.error(`and could not set it aside either: ${String(keepErr)}`);
      });
    });
  };

  // Session-wide billing, including compacted history, so the delta across one activity is its
  // spend. Undefined for fake sessions in checks.
  const billed = (session: AgentSession) => {
    type Stats = { tokens?: { total?: number }; cost?: number };
    const stats = (session as { getSessionStats?: () => Stats }).getSessionStats;
    if (typeof stats !== "function") return undefined;
    try {
      const now = stats.call(session);
      return { tokens: now.tokens?.total ?? 0, cost: now.cost ?? 0 };
    } catch {
      return undefined;
    }
  };

  const spentSince = (
    before: ReturnType<typeof billed>,
    session: AgentSession,
  ): Spend | undefined => {
    const after = billed(session);
    if (!before || !after) return undefined;
    return { tokens: after.tokens - before.tokens, cost: after.cost - before.cost };
  };

  // Session total from the file. A run started after idle has an empty count, so the workflow
  // can't know this.
  const totalOf = (session: AgentSession): Spend | undefined => billed(session);

  async function openSession(sessionFile: string, guard?: () => void): Promise<AgentSession> {
    if (dependencies.openSession) return dependencies.openSession(sessionFile, guard);
    await mkdir(dirname(sessionFile), { recursive: true });
    const sessionManager = SessionManager.open(sessionFile);
    sessionManager.setWriteGuard(guard);

    const modelRuntime = await ModelRuntime.create();
    if (opts.apiKey) await modelRuntime.setRuntimeApiKey(provider, opts.apiKey);
    const available = await modelRuntime.getAvailable(provider);
    // Default to the provider's small, cheap model.
    const hint = opts.modelHint ?? (provider === "anthropic" ? "haiku" : "mini");
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

  /** Whether the session has work, after settling what a stopped turn left behind. A freshly
   * opened session reporting busy means something else is driving it. */
  const settleWhatStopped = (session: AgentSession): boolean => {
    const settled = session.prepareStep();
    if (settled === "busy") {
      throw new Error("the session is already running a unit of work; retrying");
    }
    return settled;
  };

  /** Record the prompt, or settle what an earlier attempt left. Returns the answer when nothing is
   * left to run. Only stepped mode has dispatch notes to tell interrupted calls from unstarted. */
  async function readyForStep(
    session: AgentSession,
    input: RunStepInput,
    stepped: boolean,
  ): Promise<RunStepResult | undefined> {
    const messages = () => session.state.messages as Msg[];
    // Drop earlier steps' results. This step's stay so a retried seal still reads them.
    if (stepped) await pending.sweep(input.sessionFile, input.promptId, input.step);

    if (!markerPresent(messages(), input.promptId)) {
      // New turn. Drop every earlier turn's results, which a per-step sweep may not reach.
      if (stepped) await pending.sweepResults(input.sessionFile);
      // Settle a stopped turn's open calls first. Providers reject unanswered calls, so a prompt
      // recorded after one breaks every later turn.
      settleWhatStopped(session);
      // Record without running, so the first step is a normal step that Temporal can retry.
      if (!(await session.recordPrompt(`${input.text}${marker(input.promptId)}`))) {
        throw new Error("Pi did not record the prompt; an extension may have taken the text");
      }
      return undefined;
    }

    // A retry. Recorded calls that no dispatch started are handed back by the model call, not
    // settled as unknown. Only when their assistant message is still last in the transcript.
    const dangling = danglingCallIds(messages());
    const last = messages()[messages().length - 1];
    const handBack =
      last?.role === "assistant" &&
      (await noDispatchStarted(input.sessionFile, input.promptId, input.step, dangling));
    if (stepped && handBack) {
      return undefined;
    }

    if (!settleWhatStopped(session)) {
      // Already answered. A retry that landed after the last step finished.
      return { done: true, retryAttempt: 0, finalText: lastAssistantText(messages()) };
    }
    return undefined;
  }

  async function runStep(input: RunStepInput): Promise<RunStepResult> {
    // Tree shipping needs stepped mode's fences. Retrying can't fix a config mismatch.
    if (opts.shipTree) {
      throw ApplicationFailure.nonRetryable(
        `a whole-step session reached a worker that ships the tree: ${SHIP_TREE_NEEDS_STEPS}`,
        "ConfigConflict",
      );
    }
    const stop = heartbeatEvery(3000);
    try {
      return await withSessionLock(input.sessionFile, async (owned, ownedNow) => {
        await bringTree(input.sessionFile);
        const session = await openSession(input.sessionFile, writeGuard(ownedNow, "the step"));
        try {
          await stillOurs(owned, "the step");
          const settled = await readyForStep(session, input, false);
          if (settled) return settled;

          // The stepped mode's three units in one activity. A stop is honoured between units. A
          // started unit runs to its end.
          if (stopRequested()) throw Context.current().cancellationSignal.reason;
          const before = billed(session);
          const model = await session.modelCall();
          const calls = model.ended ? [] : model.toolCalls;
          const results = new Map<string, TurnToolCallOutcome>();
          let stopped = false;
          const notStarted = new Set<string>();
          for (const call of calls) {
            if (stopped || stopRequested()) {
              stopped = true;
              notStarted.add(call.id);
              continue;
            }
            // One at a time. The session admits a single unit of work.
            const outcome = await session.runToolCall(call.id);
            if (outcome) results.set(call.id, outcome);
          }
          stopped ||= stopRequested();
          const sealed = await session.sealStep(
            // Calls with no outcome are already in the transcript. Calls the stop kept from
            // starting get unknown, so the step still closes.
            calls.flatMap((call) => {
              if (notStarted.has(call.id)) return [unknownToolCallOutcome(call)];
              return results.get(call.id) ?? [];
            }),
            {
              expectCalls: calls.map((call) => call.id),
              // Carried by the workflow, since this session is rebuilt every step.
              retryAttempt: input.retryAttempt,
              // A stopped turn records results only. No retry, no compaction.
              postRun: !stopped,
            },
          );
          const { done } = sealed;
          await session.waitForIdle();
          const messages = session.state.messages as Msg[];
          const spent = spentSince(before, session);
          await shipTree(input.sessionFile);
          if (stopped) throw Context.current().cancellationSignal.reason;
          const total = totalOf(session);
          return {
            done,
            retryAttempt: sealed.retryAttempt,
            finalText: done ? lastAssistantText(messages) : "",
            ...(spent ? { spent } : {}),
            ...(total ? { total } : {}),
          };
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
  // With tree shipping and no pinned queue, parallel tools on different hosts would overwrite each
  // other's captures, so they run in order. A pinned queue keeps them on one directory.
  const mustSerialize = (sequential: boolean) =>
    sequential || (opts.shipTree === true && opts.stepQueue === undefined);

  async function runModelCall(input: RunStepInput): Promise<ModelCallResult> {
    const stop = heartbeatEvery(3000);
    try {
      return await withSessionLock(input.sessionFile, async (owned, ownedNow) => {
        await bringTree(input.sessionFile);
        const session = await openSession(
          input.sessionFile,
          writeGuard(ownedNow, "the model call"),
        );
        try {
          // Before the first write (recording the prompt).
          await stillOurs(owned, "the model call");
          const settled = await readyForStep(session, input, true);
          if (settled) return { settled, calls: [], sequential: false, ended: true };

          const before = billed(session);
          const outcome = await session.modelCall();
          const spent = spentSince(before, session);
          const total = totalOf(session);
          return {
            calls: outcome.toolCalls.map((call) => ({ id: call.id, name: call.name })),
            sequential: mustSerialize(outcome.sequential),
            ended: outcome.ended,
            ...(opts.stepQueue === undefined ? {} : { queue: opts.stepQueue }),
            ...(spent ? { spent } : {}),
            ...(total ? { total } : {}),
          };
        } finally {
          session.dispose();
        }
      });
    } finally {
      stop();
    }
  }

  // Typed only when no attempt left a dispatch note. An earlier attempt may still run the tool.
  const beforeClaim = async (err: unknown, input: ToolCallInput): Promise<unknown> => {
    if (err instanceof ApplicationFailure) return err;
    const { sessionFile, turn, step, call } = input;
    const noted = await pending.wasDispatched(sessionFile, turn, step, call.id).catch(() => true);
    if (noted) return err;
    return ApplicationFailure.create({
      message: err instanceof Error ? err.message : String(err),
      type: FAILED_BEFORE_CLAIM,
      cause: err instanceof Error ? err : undefined,
    });
  };

  /** One recorded call of the current step. Its result is kept beside the session file until the
   * seal records it, so a tool that ran is not asked to run again. */
  async function runToolCall(input: ToolCallInput): Promise<ToolCallResult> {
    const stop = heartbeatEvery(3000);
    let claimed = false;
    try {
      // This host may not have run the model call, so restore the project files first. Under the
      // session lease to order it against transcript recovery. The tree store has its own leases.
      const writer: worktree.Writer = { turn: input.turn, step: input.step, callId: input.call.id };
      await withSessionLock(input.sessionFile, () => bringTree(input.sessionFile, writer));
      // Opening can append (a thinking-level entry), so the open is locked. The lock is released
      // before the tool runs so siblings stay parallel. After that, any append is refused, which
      // also catches an extension writing from `tool_execution_end`.
      let opening = true;
      const session = await withSessionLock(input.sessionFile, (_owned, ownedNow) =>
        openSession(input.sessionFile, () => {
          if (opening) return writeGuard(ownedNow, "opening the session")();
          throw new Error(
            "a tool activity must not write to the session: " +
              "the seal records what the step produced",
          );
        }),
      );
      opening = false;
      try {
        // Already sealed. Clean up the leftover kept result.
        if (answeredInTranscript(session.state.messages as Msg[], input.call.id)) {
          await pending.forgetResults(input.sessionFile, input.turn, input.step, [input.call.id]);
          return { outcome: "already-settled" };
        }

        if (await pending.readResult(input.sessionFile, input.turn, input.step, input.call.id)) {
          return { outcome: "already-settled" };
        }

        // Retryable on purpose. Shared storage may not show the recorded call on this host yet.
        if (!recordedInTranscript(session.state.messages as Msg[], input.call.id)) {
          throw new Error(`no recorded tool call ${input.call.id} in ${input.sessionId}`);
        }

        const { turn, step, call } = input;
        if (!(await pending.noteDispatch(input.sessionFile, turn, step, call.id))) {
          // An earlier dispatch started this tool, so it may have taken effect. Report unknown
          // rather than re-run a push or delete.
          // The first attempt may still return. The seal supplies unknown if no result arrives.
          return { outcome: "unknown" };
        }
        claimed = true;

        // Local marker that a tool is running here. A later turn checks it before reusing this
        // directory, since a timed-out attempt may still be running.
        if (opts.shipTree) await worktree.beginWrite(opts.projectDir, writer);
        const outcome = await session.runToolCall(input.call.id).finally(async () => {
          if (opts.shipTree) await worktree.endWrite(opts.projectDir, input.call.id);
        });
        if (!outcome) return { outcome: "already-settled" };

        await pending.keepResult(
          input.sessionFile,
          input.turn,
          input.step,
          input.call.id,
          outcome,
        );
        // Ship from here. Only this host has the tool's changes, and the seal may land elsewhere.
        await withSessionLock(input.sessionFile, () =>
          shipTree(input.sessionFile, { current: writer, fence: { turn, step } }),
        );
        return { outcome: "settled" };
      } finally {
        session.dispose();
      }
    } catch (err) {
      throw claimed ? err : await beforeClaim(err, input);
    } finally {
      stop();
    }
  }

  /** Close the step with what its calls produced, in the order the model asked for them. */
  async function sealStep(input: SealStepInput): Promise<RunStepResult> {
    const stop = heartbeatEvery(3000);
    try {
      return await withSessionLock(input.sessionFile, async (owned, ownedNow) => {
        // Close the step first. The lost host may still publish from a stale tip, and this lets
        // that capture be refused.
        if (input.lost && opts.shipTree) {
          await worktree.closeStep(input.sessionFile, { turn: input.turn, step: input.step });
        }
        // A cancelled tool may still be writing on another host.
        if (!input.interrupted) await bringTree(input.sessionFile);
        const session = await openSession(input.sessionFile, writeGuard(ownedNow, "the seal"));
        try {
          const results: TurnToolCallOutcome[] = [];
          for (const call of input.calls) {
            // No kept result means the dispatch never came back. Seal it as unknown, since
            // providers reject unanswered calls.
            const { turn, step } = input;
            const kept = await pending.readResult(input.sessionFile, turn, step, call.id);
            results.push(kept ?? unknownToolCallOutcome(call));
          }

          // `expectCalls` stops results being attached to a message appended since the model call.
          await stillOurs(owned, "the seal");
          const before = billed(session);
          const sealed = await session.sealStep(results, {
            expectCalls: input.calls.map((call) => call.id),
            retryAttempt: input.retryAttempt,
            // A stopped turn records results only. No retry, no compaction.
            postRun: !input.interrupted,
          });
          const { done } = sealed;
          await session.waitForIdle();
          // Kept results stay until the next step sweeps them, so a retried seal can read them.
          const answer = lastAssistantText(session.state.messages as Msg[]);
          // Ship after the seal, so the tree matches the transcript.
          if (!input.interrupted) {
            await shipTree(input.sessionFile, { fence: { turn: input.turn, step: input.step } });
          }
          const spent = spentSince(before, session);
          const total = totalOf(session);
          return {
            done,
            retryAttempt: sealed.retryAttempt,
            finalText: done ? answer : "",
            ...(spent ? { spent } : {}),
            ...(total ? { total } : {}),
          };
        } finally {
          session.dispose();
        }
      });
    } finally {
      stop();
    }
  }

  /** Release the project directory when the session goes idle, or this worker refuses every other
   * session. Also records the session as over, so other hosts can release theirs. */
  async function retireSession(input: { readonly sessionFile: string }): Promise<void> {
    if (!opts.shipTree) return;
    const freed = await worktree.retire(opts.projectDir, input.sessionFile).catch((err) => {
      console.warn(`could not hand back ${opts.projectDir}: ${String(err)}`);
      return false;
    });
    if (freed) console.log(`handed ${opts.projectDir} back: ${input.sessionFile} went idle`);
  }

  /** Copy a template project into a scheduled session before its first step. A no-op once the
   * session has its own tip, so a retry does not copy twice. */
  async function adoptProject(input: {
    readonly sessionFile: string;
    readonly template: string;
  }): Promise<void> {
    if (!opts.shipTree) return;
    const copied = await worktree.adopt(input.template, input.sessionFile);
    if (copied) console.log(`took the project from ${input.template} for ${input.sessionFile}`);
  }

  return { runStep, runModelCall, runToolCall, sealStep, retireSession, adoptProject };
}

export type Activities = ReturnType<typeof makeActivities>;
