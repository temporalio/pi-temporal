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

// Long enough not to spin on a directory that stays refused, short enough that the work reaches a
// free host in about the time one dispatch takes.
const REFUSAL_RETRY = "2 seconds";

// Whether Temporal asked this activity to stop. Delivered through the heartbeats, so it is only
// ever as fresh as the last one. Outside an activity (a check driving the function directly)
// nothing can ask.
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
  // Ship the project's files with the session, so a worker on another machine finds the work the
  // last one did. Without it the transcript travels and the files do not.
  readonly shipTree?: boolean;
  // The queue this worker polls on its own. Reported by the model call so the rest of the step can
  // be sent back to this host, which is the one standing in the directory the tools will write.
  readonly stepQueue?: string;
}

export function makeActivities(
  opts: ActivityOptions,
  dependencies: { openSession?: (file: string, guard?: () => void) => Promise<AgentSession> } = {},
) {
  const provider = opts.provider ?? "openai";

  // Before anything the model asked for can run, and after the step that ran it is written down.
  // Both are no-ops when the tree is off, and the restore is a no-op on the host that captured,
  // because the tree it holds is already the one the shared directory names.
  // Deliberately not caught. A step that cannot get the project's files must not run against
  // whatever is in the directory: the model would be told those files are the project. Failing
  // sends the work to a host that can do it.
  const bringTree = async (sessionFile: string, current?: worktree.Writer) => {
    if (!opts.shipTree) return;
    await worktree.ensure(opts.projectDir, sessionFile, current).catch((err: unknown) => {
      // A refused directory is not a failing unit of work. It is this host saying no, and the same
      // dispatch on any other host would do fine, so it must not climb the retry backoff: the
      // interval doubles per attempt, and a host that answers first and refuses fastest is exactly
      // the one that would push the next attempt minutes out while a free host sits idle.
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
  // A lock can be reclaimed while its holder is blocked, so the holder asks again on the way to
  // the write. Failing here is the right answer: Temporal retries, and the retry takes the lock.
  const stillOurs = async (owned: () => Promise<boolean>, what: string) => {
    if (!(await owned())) {
      throw new Error(`lost the session lock before ${what}; another attempt has it`);
    }
  };

  // Handed to the session so every append asks before it lands. `stillOurs` guards the writes this
  // file makes; this guards the ones Pi makes inside a call, and the assistant message is the one
  // that matters: it is written at the end of a stream that can run for minutes, long after the
  // last thing anyone checked.
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
    // A capture that fails must not fail the step: the tool has already run and its result is
    // recorded, and a retry finds that result rather than running it again, so throwing here costs
    // an attempt and still ships nothing. What it must not do is lose the work. The files are the
    // only record of what the tool did, so they are kept where they can be recovered.
    await worktree.capture(opts.projectDir, sessionFile, of).catch(async (err) => {
      console.error(`could not ship the project tree: ${String(err)}`);
      await worktree.setAside(opts.projectDir, sessionFile).catch((keepErr) => {
        console.error(`and could not set it aside either: ${String(keepErr)}`);
      });
    });
  };

  // What the session has been billed for, as it reports it. Aggregated over every entry, including
  // history a compaction rewrote, so the difference across one activity is what that activity
  // spent. A faked session in a check has no such thing to say, and then nothing is reported and
  // the budget has nothing to add up.
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

  // What the record says the whole session has been billed, which is the number a session's own
  // bound is about. Nothing the workflow keeps can answer it: a session that went idle and was
  // woken again is a fresh run with an empty count, and the file is what remembers.
  const totalOf = (session: AgentSession): Spend | undefined => billed(session);

  async function openSession(sessionFile: string, guard?: () => void): Promise<AgentSession> {
    if (dependencies.openSession) return dependencies.openSession(sessionFile, guard);
    await mkdir(dirname(sessionFile), { recursive: true });
    const sessionManager = SessionManager.open(sessionFile);
    sessionManager.setWriteGuard(guard);

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

  /** Whether the session has work, after settling what a stopped turn left behind. A session
   * rebuilt for this activity has no run under way, so a busy one means something else is driving
   * it, and the answer it would give is not one to act on. */
  const settleWhatStopped = (session: AgentSession): boolean => {
    const settled = session.prepareStep();
    if (settled === "busy") {
      throw new Error("the session is already running a unit of work; retrying");
    }
    return settled;
  };

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
    if (stepped) await pending.sweep(input.sessionFile, input.promptId, input.step);

    if (!markerPresent(messages(), input.promptId)) {
      // Everything the last turn left. Steps are numbered per turn, so sweeping by step number
      // alone never reaches a turn that ran further than this one will.
      if (stepped) await pending.sweepResults(input.sessionFile);
      // Settle what the turn that stopped left behind first. A call with no result is a payload no
      // provider accepts, so a prompt recorded behind one makes every later turn of the session
      // fail rather than just the interrupted one.
      settleWhatStopped(session);
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
    // Only while the message that asked for them is the last one: a model call hands back the
    // calls of a trailing assistant message, and from behind results it would ask the provider
    // again with calls nothing answers.
    const dangling = danglingCallIds(messages());
    const last = messages()[messages().length - 1];
    const handBack =
      last?.role === "assistant" &&
      (await noDispatchStarted(input.sessionFile, input.promptId, input.step, dangling));
    if (stepped && handBack) {
      return undefined;
    }

    if (!settleWhatStopped(session)) {
      // The prompt is recorded and the turn already has its answer. That is a retry landing after
      // the last step finished but before its result reached Temporal.
      return { done: true, retryAttempt: 0, finalText: lastAssistantText(messages()) };
    }
    return undefined;
  }

  async function runStep(input: RunStepInput): Promise<RunStepResult> {
    // Whoever started the session chose whole-step mode, and this worker ships the tree, whose
    // fences only the stepped path keeps. Retrying cannot change either side, so it is said once.
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

          // prepareStep settled anything the crash left dangling, so this is a plain step: a tool
          // whose result never landed is reported as unknown, not re-run behind the model's back.
          // The same three units the stepped mode dispatches one by one, run here in one activity.
          // A stop is honoured between units, which is as fine as a whole step can be: a unit
          // that has started runs to its end, and nothing after it starts.
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
            // One at a time: the session admits a single unit of work, so calls cannot overlap.
            const outcome = await session.runToolCall(call.id);
            if (outcome) results.set(call.id, outcome);
          }
          stopped ||= stopRequested();
          const sealed = await session.sealStep(
            // A call with no outcome already has its result in the transcript, which the seal
            // finds, so only the ones this attempt ran are handed over. One the stop kept from
            // starting gets the outcome the stepped seal gives it, so the step still closes.
            calls.flatMap((call) => {
              if (notStarted.has(call.id)) return [unknownToolCallOutcome(call)];
              return results.get(call.id) ?? [];
            }),
            {
              expectCalls: calls.map((call) => call.id),
              // Carried by the workflow, because the seal reads no budget off the transcript and
              // this session is rebuilt for every step.
              retryAttempt: input.retryAttempt,
              // A stopped turn wants its results written down and nothing else, as in the stepped
              // seal: a retry or a compaction is work nobody asked for.
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
  // Tools of one step can be dispatched to different hosts, and each ships what its own directory
  // holds. Run two at once and the second to capture publishes a tree without the first one's
  // work. That is what serializing them costs, and what pinning the step to this worker buys back:
  // with a queue of our own they all land here, on one directory, and the tree never moves between
  // them.
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
          // Before the prompt is recorded, which is this activity's first write.
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

  // Typed only when no attempt of the call has a dispatch note: the workflow sees the last
  // attempt's failure, and an earlier one may have claimed the call and still be running the tool.
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
      // A tool call is its own dispatch, so it can land on a worker that ran neither the model
      // call nor any sibling tool. Without this it runs against whatever files that host happens
      // to have, and reports the answer as if it were the project's.
      //
      // Under the session lease, so this session's transcript recovery and its tree restore are
      // ordered against each other. It is not what keeps two hosts from publishing at once: the
      // tree store takes its own host-directory and shared-store leases for that.
      const writer: worktree.Writer = { turn: input.turn, step: input.step, callId: input.call.id };
      await withSessionLock(input.sessionFile, () => bringTree(input.sessionFile, writer));
      // Opening a session can append to it (a first thinking-level entry), and two of these run at
      // once. The lock covers the open and is given back before the tool runs, which is the part
      // that has to stay parallel.
      // The guard has to change when the lease does. Opening a session can append (a first
      // thinking-level entry), so it is allowed while the lease is held; once the lease is given
      // back, `ownedNow` can never go false again, so a guard closed over it would wave every
      // later write through. A tool activity has no business appending at all, and an extension
      // appending from `tool_execution_end` runs right here.
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
        // The transcript first: a result in it means a whole dispatch and a seal already
        // happened, and the kept files for the call are what is left behind.
        if (answeredInTranscript(session.state.messages as Msg[], input.call.id)) {
          await pending.forgetResults(input.sessionFile, input.turn, input.step, [input.call.id]);
          return { outcome: "already-settled" };
        }

        if (await pending.readResult(input.sessionFile, input.turn, input.step, input.call.id)) {
          return { outcome: "already-settled" };
        }

        // A call the transcript does not hold is one this attempt cannot run. Retryable on
        // purpose, and capped by the proxy: on shared storage the message that records it may not
        // have reached this host yet, and answering a stale read with a permanent failure tells
        // the model a tool that never ran may have taken effect.
        if (!recordedInTranscript(session.state.messages as Msg[], input.call.id)) {
          throw new Error(`no recorded tool call ${input.call.id} in ${input.sessionId}`);
        }

        const { turn, step, call } = input;
        if (!(await pending.noteDispatch(input.sessionFile, turn, step, call.id))) {
          // A dispatch was inside this tool when it stopped, so the tool can have taken effect.
          // Re-running a push or a delete that already happened is the worse failure, so the
          // model is told the outcome instead of the tool being asked again.
          const unknown = unknownToolCallOutcome(input.call);
          await pending.keepResult(input.sessionFile, turn, step, call.id, unknown);
          return { outcome: "unknown" };
        }
        claimed = true;

        // Said on this host before the tool can touch anything, and taken back below when the body
        // returns. It is the only local record of a tool that is still inside its own execution,
        // and what a later turn checks before it reuses this directory: Temporal giving up on this
        // attempt stops it being waited for, not being run.
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
        // Shipped from here, because this host ran the tool and is the only one holding what it
        // did. The seal can land anywhere, and capturing there would ship a directory that never
        // saw this tool. Under the shared lock, for the reason the restore above is.
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
        // Before the step is written down, and before anything else takes the project on: a host
        // the driver stopped waiting for is still standing on the tip it read, so what it publishes
        // afterwards reverts whatever replaced it. Saying the step is closed is what makes that
        // capture refusable on the host it comes from.
        if (input.lost && opts.shipTree) {
          await worktree.closeStep(input.sessionFile, { turn: input.turn, step: input.step });
        }
        // A cancelled tool may still be writing on another host.
        if (!input.interrupted) await bringTree(input.sessionFile);
        const session = await openSession(input.sessionFile, writeGuard(ownedNow, "the seal"));
        try {
          const results: TurnToolCallOutcome[] = [];
          for (const call of input.calls) {
            // A call with nothing kept for it is one whose dispatch never came back. Sealing
            // without it would leave the transcript holding a call no result answers, which is a
            // payload no provider accepts.
            const { turn, step } = input;
            const kept = await pending.readResult(input.sessionFile, turn, step, call.id);
            results.push(kept ?? unknownToolCallOutcome(call));
          }

          // Named, so a message something else appended between the model call and here cannot
          // be the one these results are attributed to.
          await stillOurs(owned, "the seal");
          const before = billed(session);
          const sealed = await session.sealStep(results, {
            expectCalls: input.calls.map((call) => call.id),
            retryAttempt: input.retryAttempt,
            // A stopped turn wants its results written down and nothing else. Deciding whether to
            // retry or compact is work nobody asked for, and a compaction is a whole model call.
            postRun: !input.interrupted,
          });
          const { done } = sealed;
          await session.waitForIdle();
          // What is kept stays until the next step sweeps it. A seal that dropped its own
          // results would leave a retry of that seal reading an empty batch, and an empty batch
          // reads as one that wants another step even when a tool asked the turn to stop.
          const answer = lastAssistantText(session.state.messages as Msg[]);
          // After the step is written down, so what ships is a tree whose transcript explains it.
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

  /** Give the project directory back when the session stops being driven. Without a caller for
   * this, a worker with one `PI_PROJECT_DIR` serves one session for as long as it lives and every
   * later one is refused. It also records that the session is over, which is the only way the hosts
   * that do not draw this activity can hand their own directories back. */
  async function retireSession(input: { readonly sessionFile: string }): Promise<void> {
    if (!opts.shipTree) return;
    const freed = await worktree.retire(opts.projectDir, input.sessionFile).catch((err) => {
      console.warn(`could not hand back ${opts.projectDir}: ${String(err)}`);
      return false;
    });
    if (freed) console.log(`handed ${opts.projectDir} back: ${input.sessionFile} went idle`);
  }

  /** Take the copy of the project a client left for a session that has nobody to send it. Runs
   * before the first step of a scheduled session, and does nothing once that session has a tip of
   * its own, so a re-driven activity does not copy twice. */
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
