// Activity bodies of a turn, against Pi's SDK, with the session JSONL as the log.
// `runStep` runs a whole step in one activity. `runModelCall`, `runToolCall` and `sealStep` split
// it so each tool call is its own unit of work. All re-open the session file, so any worker that
// can reach it can run them.

import { access, mkdir } from "node:fs/promises";
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
  DeferredToolCall,
  ModelCallResult,
  RetireInput,
  RunStepInput,
  RunStepResult,
  SealStepInput,
  Spend,
  ToolCallInput,
  ToolCallResult,
} from "./protocol.js";
import { FAILED_AFTER_CLAIM, FAILED_BEFORE_CLAIM } from "./protocol.js";
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

// The Activity logger, so each line carries its Workflow and Activity ids. Plain console when a
// check calls an activity directly, with no Activity around it.
const say = (level: "info" | "warn", message: string) => {
  try {
    Context.current().log[level](message);
  } catch {
    console[level](message);
  }
};

// Passed to a running tool, so a cancelled Activity stops its tool like a user stop. The tool
// reports what it did, and the seal records that instead of an unknown outcome. Cancellation
// arrives with a heartbeat, so it takes up to one heartbeat to reach the tool.
const cancelled = (): { signal?: AbortSignal } => {
  try {
    return { signal: Context.current().cancellationSignal };
  } catch {
    return {};
  }
};

// The model call, aborted like a user stop when the Activity is cancelled. Pi records an aborted
// response, and the turn ends as stopped instead of waiting out a slow provider.
const modelCallUntilCancelled = async (session: AgentSession) => {
  const { signal } = cancelled();
  const stop = () => void session.abort().catch(() => {});
  if (signal?.aborted) stop();
  else signal?.addEventListener("abort", stop, { once: true });
  try {
    return await session.modelCall();
  } finally {
    signal?.removeEventListener("abort", stop);
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

/** Whether this turn's prompt is in the session file. A compaction can summarize the prompt out
 * of the context, but the branch keeps every raw message entry. */
export const promptRecorded = (session: AgentSession, promptId: string): boolean => {
  const manager = (session as { sessionManager?: SessionManager }).sessionManager;
  // Fake sessions in checks have no manager. Their context is the whole transcript.
  if (typeof manager?.getBranch !== "function") {
    return markerPresent(session.state.messages as Msg[], promptId);
  }
  const branch = manager.getBranch().flatMap((entry) => (entry.type === "message" ? [entry] : []));
  return markerPresent(branch.map((entry) => entry.message as Msg), promptId);
};

/** For a call nothing started. Unknown would tell the model it may have taken effect. */
const notRunOutcome = (call: DeferredToolCall): TurnToolCallOutcome => {
  const outcome = unknownToolCallOutcome(call);
  outcome.message.content = [
    { type: "text", text: "This tool call did not run. The turn stopped before it started." },
  ];
  return outcome;
};

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

// Only the latest response. A provider can reuse a call id, and an older call with the same id is
// not this step's call.
const recordedInTranscript = (messages: Msg[], callId: string) => {
  const latest = [...messages].reverse().find((m) => m.role === "assistant");
  return blocksOf(latest?.content).some((b) => b.type === "toolCall" && b.id === callId);
};

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

  // The files are set aside either way, since they're the only record of what the tool did. A
  // refusal by design (behind the tip, step closed) doesn't fail the activity. Anything else does
  // when `retryable`, because the seal is the last capture before another worker reads the tip.
  // A tool call's capture never fails the call: its result is kept, and the seal ships again.
  const shipTree = async (
    sessionFile: string,
    of: { readonly current?: worktree.Writer; readonly fence?: worktree.Fence } = {},
    retryable = false,
  ) => {
    if (!opts.shipTree) return;
    await worktree.capture(opts.projectDir, sessionFile, of).catch(async (err) => {
      say("warn", `could not ship the project tree: ${String(err)}`);
      await worktree.setAside(opts.projectDir, sessionFile).catch((keepErr) => {
        say("warn", `and could not set it aside either: ${String(keepErr)}`);
      });
      if (retryable && !worktree.isRefusal(err)) throw err;
    });
  };

  // The session's turn time lives in its record, like its token count, so a run woken after an
  // idle exit still counts the turns before it. The Workflow's own count only spans one run.
  const SESSION_SECONDS_ENTRY = "pi-temporal.session-seconds";
  type Record = {
    getBranch(): { type: string; customType?: string; data?: unknown }[];
    appendCustomEntry(customType: string, data: unknown): string;
  };
  // Optional, because the checks' fake sessions keep no record. A real session always has one.
  const managerOf = (session: AgentSession) =>
    (session as unknown as { sessionManager?: Record }).sessionManager;
  // The session's time before `turn`, from the latest total another turn wrote. This turn's own
  // entries are skipped, so a step that runs again can't count the turn twice.
  const secondsBefore = (session: AgentSession, turn: string): number | undefined => {
    const manager = managerOf(session);
    return manager ? latestSeconds(manager, turn) : undefined;
  };
  const latestSeconds = (manager: Record, skipping?: string): number | undefined => {
    const branch = manager.getBranch();
    for (let i = branch.length - 1; i >= 0; i--) {
      const entry = branch[i];
      if (entry.type !== "custom" || entry.customType !== SESSION_SECONDS_ENTRY) continue;
      const data = entry.data as { turn?: unknown; seconds?: unknown } | undefined;
      if (skipping !== undefined && data?.turn === skipping) continue;
      return typeof data?.seconds === "number" ? data.seconds : undefined;
    }
    return undefined;
  };
  // Every step writes the total so far, so a turn that's stopped, runs out of budget, or fails
  // still counts up to its last step. Under the session lease, like any other append.
  const recordSeconds = (session: AgentSession, turn: string, seconds: number | undefined) => {
    if (seconds === undefined) return;
    managerOf(session)?.appendCustomEntry(SESSION_SECONDS_ENTRY, { turn, seconds });
  };
  const withSeconds = (session: AgentSession, turn: string) => {
    const seconds = secondsBefore(session, turn);
    return seconds === undefined ? {} : { sessionSeconds: seconds };
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

  async function openSession(sessionFile: string, guard?: () => void): Promise<AgentSession> {
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

    // Past the first step the prompt is in, whatever a lookup says. Recording it again would put
    // a second prompt in the middle of the turn.
    if (input.step <= 1 && !promptRecorded(session, input.promptId)) {
      // New turn. Drop every earlier turn's results, which a per-step sweep may not reach.
      if (stepped) await pending.sweepResults(input.sessionFile);
      // Settle a stopped turn's open calls first. Providers reject unanswered calls, so a prompt
      // recorded after one breaks every later turn.
      settleWhatStopped(session);
      // Record without running, so the first step is a normal step that Temporal can retry.
      if (!(await session.recordPrompt(`${input.text}${marker(input.promptId)}`))) {
        throw ApplicationFailure.nonRetryable(
          "Pi did not record the prompt; an extension may have taken the text",
        );
      }
      return undefined;
    }

    // A retry. Recorded calls that no dispatch started are handed back by the model call, not
    // settled as unknown. Only when their assistant message is still last in the transcript.
    if (stepped) {
      const dangling = danglingCallIds(messages());
      const last = messages()[messages().length - 1];
      const handBack =
        last?.role === "assistant" &&
        (await noDispatchStarted(input.sessionFile, input.promptId, input.step, dangling));
      if (handBack) return undefined;
    }

    if (!settleWhatStopped(session)) {
      // Already answered. A retry that landed after the last step finished.
      return { done: true, retryAttempt: 0, finalText: lastAssistantText(messages()) };
    }
    return undefined;
  }

  async function runStep(input: RunStepInput): Promise<RunStepResult> {
    // The step's own time goes into the session's, since the step that ends a turn records it.
    const stepStartedAt = Date.now();
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
          const model = await modelCallUntilCancelled(session);
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
            const outcome = await session.runToolCall(call.id, cancelled());
            if (outcome) results.set(call.id, outcome);
          }
          stopped ||= stopRequested();
          const sealed = await session.sealStep(
            // Calls with no outcome are already in the transcript. Calls the stop kept from
            // starting get a result saying so, so the step still closes.
            calls.flatMap((call) => {
              if (notStarted.has(call.id)) return [notRunOutcome(call)];
              return results.get(call.id) ?? [];
            }),
            {
              expectCalls: calls.map((call) => call.id),
              // Carried by the workflow, since this session is rebuilt every step.
              retryAttempt: input.retryAttempt,
              overflowRecoveryAttempted: input.overflowRecoveryAttempted,
              // A stopped turn records results only. No retry, no compaction.
              postRun: !stopped,
            },
          );
          const { done } = sealed;
          await session.waitForIdle();
          const secondsSoFar = withSeconds(session, input.promptId);
          if (input.sessionSeconds !== undefined) {
            const seconds = input.sessionSeconds + (Date.now() - stepStartedAt) / 1000;
            recordSeconds(session, input.promptId, seconds);
          }
          const messages = session.state.messages as Msg[];
          const spent = spentSince(before, session);
          await shipTree(input.sessionFile);
          if (stopped) throw Context.current().cancellationSignal.reason;
          const total = billed(session);
          return {
            done,
            retryAttempt: sealed.retryAttempt,
            overflowRecoveryAttempted: sealed.overflowRecoveryAttempted,
            finalText: done ? lastAssistantText(messages) : "",
            ...(spent ? { spent } : {}),
            ...(total ? { total } : {}),
            ...secondsSoFar,
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
  // With tree shipping and no host queue, parallel tools on different hosts would overwrite each
  // other's captures, so they run in order. A host queue keeps them on one directory.
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
          const outcome = await modelCallUntilCancelled(session);
          const spent = spentSince(before, session);
          const total = billed(session);
          return {
            calls: outcome.toolCalls.map((call) => ({ id: call.id, name: call.name })),
            sequential: mustSerialize(outcome.sequential),
            ended: outcome.ended,
            ...(opts.stepQueue === undefined ? {} : { queue: opts.stepQueue }),
            ...(spent ? { spent } : {}),
            ...(total ? { total } : {}),
            ...withSeconds(session, input.promptId),
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
    // A refusal comes from `bringTree`, which always runs before the claim. Typed like any other
    // failure before the claim, so a host-queue step moves to a free host.
    const refused = err instanceof ApplicationFailure && err.type === "WorktreeQuarantined";
    if (err instanceof ApplicationFailure && !refused) return err;
    const { sessionFile, turn, step, call } = input;
    const noted = await pending.wasDispatched(sessionFile, turn, step, call.id).catch(() => true);
    if (noted) return err;
    return ApplicationFailure.create({
      message: err instanceof Error ? err.message : String(err),
      type: FAILED_BEFORE_CLAIM,
      cause: err instanceof Error ? err : undefined,
      ...(refused ? { details: [err.type], nextRetryDelay: REFUSAL_RETRY } : {}),
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
        // A stopped turn's seal may be about to take this claim. Don't race it.
        if (stopRequested()) throw Context.current().cancellationSignal.reason;
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
        const outcome = await session.runToolCall(input.call.id, cancelled()).finally(async () => {
          if (opts.shipTree) await worktree.endWrite(opts.projectDir, writer);
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
        // The result is kept, so a failure here must not fail the call. The next step's restore
        // or a later capture can carry the files.
        await withSessionLock(input.sessionFile, () =>
          shipTree(input.sessionFile, { current: writer, fence: { turn, step } }),
        ).catch((err: unknown) => {
          say("warn", `could not ship the project tree after ${call.id}: ${String(err)}`);
        });
        return { outcome: "settled" };
      } finally {
        session.dispose();
      }
    } catch (err) {
      // A stop stays a stop. Typed as a failure, it would hide the cancellation.
      if (stopRequested()) throw err;
      if (!claimed) throw await beforeClaim(err, input);
      throw ApplicationFailure.nonRetryable(
        `tool call ${input.call.id} failed after it started: ${String(err)}`,
        FAILED_AFTER_CLAIM,
      );
    } finally {
      stop();
    }
  }

  /** Close the step with what its calls produced, in the order the model asked for them. */
  async function sealStep(input: SealStepInput): Promise<RunStepResult> {
    const stop = heartbeatEvery(3000);
    try {
      return await withSessionLock(input.sessionFile, async (owned, ownedNow) => {
        // Close the step first. The lost host, or a stopped tool still running there, may publish
        // from a stale tip later, and this lets that capture be refused.
        if (input.interrupted && opts.shipTree) {
          await worktree.closeStep(input.sessionFile, { turn: input.turn, step: input.step });
        }
        // A cancelled tool may still be writing on another host.
        if (!input.interrupted) await bringTree(input.sessionFile);
        const session = await openSession(input.sessionFile, writeGuard(ownedNow, "the seal"));
        try {
          const results: TurnToolCallOutcome[] = [];
          for (const call of input.calls) {
            const { turn, step } = input;
            const kept = await pending.readResult(input.sessionFile, turn, step, call.id);
            if (kept) {
              results.push(kept);
              continue;
            }
            // No kept result. Take the claim, so a late attempt can't run the tool after the
            // step closed. If the seal gets it, nothing started the call. Otherwise it's unknown,
            // since providers reject unanswered calls.
            const unstarted = await pending.noteDispatch(input.sessionFile, turn, step, call.id);
            results.push(unstarted ? notRunOutcome(call) : unknownToolCallOutcome(call));
          }

          // `expectCalls` stops results being attached to a message appended since the model call.
          await stillOurs(owned, "the seal");
          const before = billed(session);
          const sealed = await session.sealStep(results, {
            expectCalls: input.calls.map((call) => call.id),
            retryAttempt: input.retryAttempt,
            overflowRecoveryAttempted: input.overflowRecoveryAttempted,
            // A stopped turn records results only. No retry, no compaction.
            postRun: !input.interrupted,
          });
          const { done } = sealed;
          await session.waitForIdle();
          const secondsSoFar = withSeconds(session, input.turn);
          recordSeconds(session, input.turn, input.sessionSeconds);
          // Kept results stay until the next step sweeps them, so a retried seal can read them.
          const answer = lastAssistantText(session.state.messages as Msg[]);
          // Ship after the seal, so the tree matches the transcript.
          if (!input.interrupted) {
            await shipTree(
              input.sessionFile,
              { fence: { turn: input.turn, step: input.step } },
              true,
            );
          }
          const spent = spentSince(before, session);
          const total = billed(session);
          return {
            done,
            retryAttempt: sealed.retryAttempt,
            overflowRecoveryAttempted: sealed.overflowRecoveryAttempted,
            finalText: done ? answer : "",
            ...(spent ? { spent } : {}),
            ...(total ? { total } : {}),
            ...secondsSoFar,
          };
        } finally {
          session.dispose();
        }
      });
    } finally {
      stop();
    }
  }

  // The steps record the session's time as they go, but the last one of a turn writes before its
  // seal ends, and failed attempts never write. The Workflow counted all of it, and its run is
  // about to end, so its total goes in the record. Only ever raised, never lowered.
  async function keepSeconds(input: RetireInput): Promise<void> {
    const { sessionFile, turn, sessionSeconds } = input;
    if (turn === undefined || sessionSeconds === undefined) return;
    if (!(await access(sessionFile).then(() => true, () => false))) return;
    await withSessionLock(sessionFile, async (_owned, ownedNow) => {
      const guard = writeGuard(ownedNow, "recording the session's time");
      const manager = dependencies.openSession
        ? managerOf(await dependencies.openSession(sessionFile, guard))
        : SessionManager.open(sessionFile);
      if (!manager) return;
      if (manager instanceof SessionManager) manager.setWriteGuard(guard);
      const known = latestSeconds(manager);
      if (known !== undefined && known >= sessionSeconds) return;
      manager.appendCustomEntry(SESSION_SECONDS_ENTRY, { turn, seconds: sessionSeconds });
    });
  }

  /** Release the project directory when the session goes idle, or this worker refuses every other
   * session. Also records the session as over, so other hosts can release theirs. */
  async function retireSession(input: RetireInput): Promise<void> {
    await keepSeconds(input);
    if (!opts.shipTree) return;
    // Thrown, so Temporal retries. The Workflow gives up quietly once retries run out.
    const freed = await worktree.retire(opts.projectDir, input.sessionFile);
    if (freed) say("info", `handed ${opts.projectDir} back: ${input.sessionFile} went idle`);
  }

  /** Copy a template project into a scheduled session before its first step. A no-op once the
   * session has its own tip, so a retry does not copy twice. */
  async function adoptProject(input: {
    readonly sessionFile: string;
    readonly template: string;
  }): Promise<void> {
    if (!opts.shipTree) return;
    const copied = await worktree.adopt(input.template, input.sessionFile);
    if (copied) say("info", `took the project from ${input.template} for ${input.sessionFile}`);
  }

  return { runStep, runModelCall, runToolCall, sealStep, retireSession, adoptProject };
}

export type Activities = ReturnType<typeof makeActivities>;
