// The Activities of a turn, for any agent that implements `Agent`. `runStep` runs a whole step in
// one Activity. `runModelCall`, `runToolCall` and `sealStep` split it so each tool call is its own
// unit of work. Each one opens the session again, so any Worker that can reach the file runs it.
//
// These guards keep adapter implementations independent of Temporal's retry rules.
// - Every unit that writes the session takes a fence token first (`fence.ts`), so a superseded
//   attempt can't write after a newer one took over.
// - A tool call takes a dispatch claim before the tool can act (`pending.ts`). A retry that finds
//   the claim reports an unknown outcome rather than run the tool twice.
// - A tool call keeps its result beside the session, and only the seal writes results in.

import { lstatSync, mkdirSync, realpathSync } from "node:fs";
import { access } from "node:fs/promises";
import { basename, dirname, join } from "node:path";
import { ApplicationFailure, Context } from "@temporalio/activity";
import type {
  Agent,
  AgentSession,
  ProjectStore,
  ToolCallRef,
  ToolOutcome,
  Writer,
} from "./agent.js";
import type {
  ModelCallResult,
  RetireInput,
  RunStepInput,
  RunStepResult,
  SealStepInput,
  Spend,
  ToolCallInput,
  ToolCallResult,
} from "./protocol.js";
import { FAILED_AFTER_CLAIM, FAILED_BEFORE_CLAIM, fencePrefix } from "./protocol.js";
import * as pending from "./pending.js";
import { takeFence, fenceToken } from "./fence.js";

// Retry delay after a host refuses the project directory. Short, so the work finds a free host.
const REFUSAL_RETRY = "2 seconds";
const QUARANTINED = "WorktreeQuarantined";
const OUTSIDE_SESSION_ROOT = "SessionOutsideRoot";

// The session's turn time lives in its record, like its token count, so a run woken after an idle
// exit still counts the turns before it. The Workflow's own count only spans one run.
const SESSION_SECONDS = "pi-temporal.session-seconds";

// A Worker shutdown cancels the attempt, but the step must go on here until the Worker exits. A
// lang-side shutdown sends no details, only the reason.
const shuttingDown = (context: Context) =>
  context.cancellationDetails?.workerShutdown === true ||
  (context.cancellationSignal.reason as Error | undefined)?.message === "WORKER_SHUTDOWN";

// Only a cancel the Workflow asked for is a stop. A timeout, a pause, a reset or an attempt the
// server no longer knows also cancels it, but the step must go on in a retry. Recording a stop
// would end the turn.
const stopAsked = (context: Context) => {
  const details = context.cancellationDetails;
  if (details) return details.cancelRequested;
  // No details, as from a server too old to send them. Only a shutdown says why.
  return !shuttingDown(context);
};

// Whether the Workflow asked this Activity to stop. Only as fresh as the last heartbeat.
const stopRequested = () => {
  const context = Context.current();
  return context.cancellationSignal.aborted && stopAsked(context);
};

// Cancelled, but not stopped and not shutting down: the attempt is no longer the step's. A retry
// may already hold the same fence token, since a reset rewinds the attempt number, so the fence
// alone can't keep this one out.
const abandoned = (context: Context) =>
  context.cancellationSignal.aborted && !shuttingDown(context) && !stopAsked(context);

// The Activity logger, so each line carries its Workflow and Activity ids.
const say = (level: "info" | "warn", message: string) => {
  Context.current().log[level](message);
};

// Passed to a running tool or model call, so a cancelled Activity stops it like a user stop. After
// a stop it reports what it did, and the seal records that instead of an unknown outcome. Any other
// cancel but a shutdown stops it too, and the fence guard keeps what it reports out. Cancellation
// arrives with a heartbeat, so it takes up to one heartbeat to get there.
const cancellation = (): AbortSignal => {
  const context = Context.current();
  const cancelled = context.cancellationSignal;
  const stop = new AbortController();
  // The abort fires outside the Activity's async context, so the context is captured here.
  const forward = () => {
    if (!shuttingDown(context)) stop.abort(cancelled.reason);
  };
  if (cancelled.aborted) forward();
  else cancelled.addEventListener("abort", forward, { once: true });
  return stop.signal;
};

const heartbeatEvery = (ms: number) => {
  const timer = setInterval(() => Context.current().heartbeat(), ms);
  timer.unref?.();
  return () => clearInterval(timer);
};

// The final answer goes into history, where every client reads it, and Continue-As-New carries the
// last one on. So it's capped. The whole answer stays in the session file.
const MAX_ANSWER_CHARS = 16 * 1024;
const answerOf = (session: AgentSession) => {
  const answer = session.lastAnswer();
  return answer.length <= MAX_ANSWER_CHARS
    ? answer
    : `${answer.slice(0, MAX_ANSWER_CHARS)}\n\n[cut short here; the session file has all of it]`;
};

export interface CoreActivityOptions {
  readonly agent: Agent;
  // Ships the project between hosts. Without one, every Worker must see the same directory.
  readonly store?: ProjectStore;
  // This Worker's host queue. The model call reports it, so the rest of the step runs here.
  readonly hostQueue?: string;
  // Every session file must be directly in this directory. Anyone who can start a Workflow in the
  // namespace picks its input, so a path from it is untrusted, and the Worker writes beside it.
  readonly sessionRoot: string;
}

// The session file, then the claim, fence and tree directories the core keeps beside it.
const SIBLINGS = ["", ".pending", ".fence", ".tree"];

export function makeCoreActivities(options: CoreActivityOptions) {
  const activities = makeUncheckedActivities(options);
  const { sessionRoot } = options;
  // Made here, once, so a fresh Worker takes its first session. Never a directory from input.
  mkdirSync(sessionRoot, { recursive: true });
  const root = realpathSync(sessionRoot);
  // A direct child of the root, compared by real path. A link under the root can't lead out, a
  // root reached through a link still takes its files, and a file can't hide in another
  // session's claim or fence directory. The parent is resolved the way the file system will
  // resolve it, so a `..` after a link goes where the link goes.
  const direct = (path: string) => {
    if (!/^[^/\\]+\.jsonl$/.test(basename(path))) return false;
    try {
      if (realpathSync(dirname(path)) !== root) return false;
    } catch {
      return false;
    }
    // The session and what the core keeps beside it must be real files in the root. A link
    // there would send its writes elsewhere. Input can't make one, but anything that writes the
    // root can, as a tool on any host of a shared session directory. The check runs before the
    // Activity does, so a link made after it still gets through. Only the agent's own open could
    // close that gap, and making one needs that write access already.
    // Any other error, as from a lost mount, goes up as is, so the attempt fails and is retried.
    const at = join(root, basename(path));
    return SIBLINGS.every(
      (suffix) => !lstatSync(at + suffix, { throwIfNoEntry: false })?.isSymbolicLink(),
    );
  };
  const inRoot = (path: string | undefined, what: string) => {
    if (path === undefined || direct(path)) return;
    // The same input fails the same way on every attempt, so a retry would only repeat this.
    throw ApplicationFailure.nonRetryable(
      `${what} ${path} is outside the session directory ${sessionRoot}`,
      OUTSIDE_SESSION_ROOT,
    );
  };
  const checked =
    <I extends { readonly sessionFile: string; readonly template?: string }, O>(
      activity: (input: I) => Promise<O>,
    ) =>
    (input: I): Promise<O> => {
      inRoot(input.sessionFile, "session file");
      inRoot(input.template, "template");
      return activity(input);
    };
  return {
    runStep: checked(activities.runStep),
    runModelCall: checked(activities.runModelCall),
    runToolCall: checked(activities.runToolCall),
    sealStep: checked(activities.sealStep),
    retireSession: checked(activities.retireSession),
    adoptProject: checked(activities.adoptProject),
  };
}

function makeUncheckedActivities({ agent, store, hostQueue }: CoreActivityOptions) {
  // Restore the project before work runs here. A step must not run against the wrong files, so
  // errors go up and send the work to a host that can do it.
  const bringTree = async (sessionFile: string, writer?: Writer) => {
    await store?.ensure(sessionFile, writer).catch((err: unknown) => {
      // This host saying no, not a failing unit. A fixed delay, so the backoff doesn't grow while
      // a free host sits idle.
      if (store.isQuarantine(err)) {
        throw ApplicationFailure.create({
          message: err instanceof Error ? err.message : String(err),
          type: QUARANTINED,
          nextRetryDelay: REFUSAL_RETRY,
        });
      }
      throw err;
    });
  };

  // The files are set aside either way, since they're the only record of what the tool did. A
  // refusal by design (behind the tip, step closed) doesn't fail the Activity. Anything else does
  // when `retryable`, because the seal is the last capture before another Worker reads the tip.
  const shipTree = async (
    sessionFile: string,
    of: { readonly current?: Writer; readonly fence?: { turn: string; step: number } } = {},
    retryable = false,
  ) => {
    if (!store) return;
    await store.capture(sessionFile, of).catch(async (err: unknown) => {
      say("warn", `could not ship the project tree: ${String(err)}`);
      await store.setAside(sessionFile).catch((keepErr: unknown) => {
        say("warn", `and could not set it aside either: ${String(keepErr)}`);
      });
      if (retryable && !store.isRefusal(err)) throw err;
    });
  };

  // Runs `body` as the session file's writer, with the guard the agent asks before each append.
  // The model's response lands at the end of a stream that can run for minutes, so the guard, not
  // a check up front, is what keeps a superseded attempt out. An abandoned one is kept out the same
  // way, so what its aborted call reports never lands.
  //
  // An Activity with no fence was scheduled by an older Workflow. It sorts below every fenced one,
  // so it can't block the run that follows.
  const withFence = async <T>(
    sessionFile: string,
    prefix: string | undefined,
    body: (guard: () => void) => Promise<T>,
  ) => {
    const context = Context.current();
    const token = fenceToken(prefix ?? fencePrefix(0, 0), context.info.attempt);
    const fence = await takeFence(sessionFile, token);
    return await body(() => {
      if (abandoned(context)) throw context.cancellationSignal.reason;
      fence();
    });
  };

  // The session's time before `turn`, from the latest total another turn wrote. This turn's own
  // entries are skipped, so a step that runs again can't count the turn twice.
  const secondsBefore = (session: AgentSession, turn: string) => {
    const latest = session.latestEntry(
      SESSION_SECONDS,
      (data) => (data as { turn?: unknown } | undefined)?.turn === turn,
    ) as { seconds?: unknown } | undefined;
    return typeof latest?.seconds === "number" ? { sessionSeconds: latest.seconds } : {};
  };
  // Every step writes the total so far, so a turn that's stopped, runs out of budget, or fails
  // still counts up to its last step. Fenced, like any other append.
  const recordSeconds = (session: AgentSession, turn: string, seconds: number | undefined) => {
    if (seconds !== undefined) session.appendEntry(SESSION_SECONDS, { turn, seconds });
  };

  // What one unit spent, from the session's own totals before and after.
  const spentSince = (before: Spend | undefined, session: AgentSession): Spend | undefined => {
    const after = session.spend();
    if (!before || !after) return undefined;
    return { tokens: after.tokens - before.tokens, cost: (after.cost ?? 0) - (before.cost ?? 0) };
  };
  const totals = (before: Spend | undefined, session: AgentSession) => {
    const spent = spentSince(before, session);
    const total = session.spend();
    return { ...(spent ? { spent } : {}), ...(total ? { total } : {}) };
  };

  /** Whether the session has work, after settling what a stopped turn left behind. */
  const settleWhatStopped = (session: AgentSession): boolean => {
    const settled = session.prepareStep();
    // A freshly opened session that's busy means something else is driving it.
    if (settled === "busy") throw new Error("the session is already running a unit of work");
    return settled;
  };

  const noDispatchStarted = async (input: RunStepInput, callIds: readonly string[]) => {
    if (callIds.length === 0) return false;
    for (const callId of callIds) {
      if (await pending.dispatchClaimed(input.sessionFile, input.promptId, input.step, callId)) {
        return false;
      }
    }
    return true;
  };

  /** Record the prompt, or settle what an earlier attempt left. Returns the answer when nothing is
   * left to run. Only stepped mode has dispatch claims to tell interrupted calls from unstarted. */
  async function readyForStep(
    session: AgentSession,
    input: RunStepInput,
    stepped: boolean,
  ): Promise<RunStepResult | undefined> {
    // Drop earlier steps' results. This step's stay so a retried seal still reads them.
    if (stepped) await pending.sweep(input.sessionFile, input.promptId, input.step);

    // Past the first step the prompt is in, whatever a lookup says. Recording it again would put
    // a second prompt in the middle of the turn.
    if (input.step <= 1 && !session.hasPrompt(input.promptId)) {
      // New turn. Drop every earlier turn's results, which a per-step sweep may not reach.
      if (stepped) await pending.sweepResults(input.sessionFile);
      // Settle a stopped turn's open calls first. Providers reject unanswered calls, so a prompt
      // recorded after one breaks every later turn.
      settleWhatStopped(session);
      // Record without running, so the first step is a normal step that Temporal can retry.
      if (input.text === undefined) {
        throw ApplicationFailure.nonRetryable("the first step of a turn came without its prompt");
      }
      if (!(await session.recordPrompt(input.promptId, input.text))) {
        throw ApplicationFailure.nonRetryable("the agent did not record the prompt");
      }
      return undefined;
    }

    // A retry. Recorded calls that no dispatch started are handed back by the model call, not
    // settled as unknown. Only when their response is still the latest entry.
    if (stepped && session.endsWithResponse()) {
      if (await noDispatchStarted(input, session.unanswered())) return undefined;
    }

    // Already answered. A retry that landed after the last step finished.
    if (!settleWhatStopped(session)) return { done: true, finalText: answerOf(session) };
    return undefined;
  }

  /** A model call, stopped like a user stop when the Activity is cancelled. After a stop the agent
   * records an aborted response, and the turn ends as stopped instead of waiting out a slow
   * provider. */
  const modelCall = (session: AgentSession) => session.modelCall(cancellation());

  async function runStep(input: RunStepInput): Promise<RunStepResult> {
    // The step's own time goes into the session's, since the step that ends a turn records it.
    const stepStartedAt = Date.now();
    // Shipping the project needs stepped mode's claims and fences. A retry can't fix that.
    if (store) {
      throw ApplicationFailure.nonRetryable(
        "a whole-step session reached a Worker that ships the project, which needs stepped mode",
        "ConfigConflict",
      );
    }
    const stop = heartbeatEvery(3000);
    try {
      return await withFence(input.sessionFile, input.fence, async (guard) => {
        const session = await agent.open(input.sessionFile, guard);
        try {
          guard();
          const settled = await readyForStep(session, input, false);
          if (settled) return settled;

          // The stepped mode's three units in one Activity. A stop is honoured between units.
          if (stopRequested()) throw Context.current().cancellationSignal.reason;
          const before = session.spend();
          const model = await modelCall(session);
          const calls = model.ended ? [] : model.toolCalls;
          const results = new Map<string, ToolOutcome>();
          let stopped = false;
          const notStarted = new Set<string>();
          for (const call of calls) {
            // An attempt the step no longer owns starts no more tools.
            guard();
            if (stopped || stopRequested()) {
              stopped = true;
              notStarted.add(call.id);
              continue;
            }
            // One at a time. The session admits a single unit of work.
            const outcome = await session.runToolCall(call.id, cancellation());
            if (outcome !== undefined) results.set(call.id, outcome);
          }
          stopped ||= stopRequested();
          const sealed = await session.sealStep(
            // Calls with no outcome are already in the session. Calls the stop kept from starting
            // get an outcome saying so, so the step still closes.
            calls.flatMap((call) =>
              notStarted.has(call.id)
                ? [agent.notRunOutcome(call)]
                : results.has(call.id)
                  ? [results.get(call.id)]
                  : [],
            ),
            {
              expectCalls: calls.map((call) => call.id),
              agentState: input.agentState,
              // A stopped turn records results only. No retry, no compaction.
              postRun: !stopped,
              // An aborted signal would refuse the seal, and a stopped one must still record.
              ...(stopped ? {} : { signal: cancellation() }),
            },
          );
          await session.waitForIdle();
          const seconds = secondsBefore(session, input.promptId);
          if (input.sessionSeconds !== undefined) {
            const total = input.sessionSeconds + (Date.now() - stepStartedAt) / 1000;
            recordSeconds(session, input.promptId, total);
          }
          if (stopped) throw Context.current().cancellationSignal.reason;
          return {
            done: sealed.done,
            ...(sealed.agentState ? { agentState: sealed.agentState } : {}),
            finalText: sealed.done ? answerOf(session) : "",
            ...totals(before, session),
            ...seconds,
          };
        } finally {
          session.dispose();
        }
      });
    } finally {
      stop();
    }
  }

  // With a project store and no host queue, parallel tools on different hosts would overwrite each
  // other's captures, so they run in order. A host queue keeps them on one directory.
  const mustSerialize = (sequential: boolean) =>
    sequential || (store !== undefined && hostQueue === undefined);

  /** The model call of one step. The calls it reports are recorded and left for the Workflow to
   * dispatch, one Activity each. */
  async function runModelCall(input: RunStepInput): Promise<ModelCallResult> {
    const stop = heartbeatEvery(3000);
    try {
      return await withFence(input.sessionFile, input.fence, async (guard) => {
        await bringTree(input.sessionFile);
        const session = await agent.open(input.sessionFile, guard);
        try {
          // Before the first write (recording the prompt).
          guard();
          const settled = await readyForStep(session, input, true);
          if (settled) return { settled, calls: [], sequential: false, ended: true };

          const before = session.spend();
          const outcome = await modelCall(session);
          return {
            calls: outcome.toolCalls.map((call) => ({ id: call.id, name: call.name })),
            sequential: mustSerialize(outcome.sequential),
            ended: outcome.ended,
            ...(hostQueue === undefined ? {} : { queue: hostQueue }),
            ...totals(before, session),
            ...secondsBefore(session, input.promptId),
          };
        } finally {
          session.dispose();
        }
      });
    } finally {
      stop();
    }
  }

  // Typed only when no attempt left a dispatch claim. An earlier attempt may still run the tool.
  const beforeClaim = async (err: unknown, input: ToolCallInput): Promise<unknown> => {
    // A refusal comes from `bringTree`, which always runs before the claim. Typed like any other
    // failure before the claim, so a host-queue step moves to a free host.
    const refused = err instanceof ApplicationFailure && err.type === QUARANTINED;
    if (err instanceof ApplicationFailure && !refused) return err;
    const { sessionFile, turn, step, call } = input;
    const claimed = await pending
      .dispatchClaimed(sessionFile, turn, step, call.id)
      .catch(() => true);
    if (claimed) return err;
    return ApplicationFailure.create({
      message: err instanceof Error ? err.message : String(err),
      type: FAILED_BEFORE_CLAIM,
      cause: err instanceof Error ? err : undefined,
      ...(refused ? { details: [QUARANTINED], nextRetryDelay: REFUSAL_RETRY } : {}),
    });
  };

  /** One recorded call of the current step. Its result is kept beside the session file until the
   * seal records it, so a tool that ran is not asked to run again. */
  async function runToolCall(input: ToolCallInput): Promise<ToolCallResult> {
    const stop = heartbeatEvery(3000);
    let claimed = false;
    try {
      // This host may not have run the model call, so restore the project first.
      const writer: Writer = { turn: input.turn, step: input.step, callId: input.call.id };
      await bringTree(input.sessionFile, writer);
      // A tool call never writes the session, so siblings need no order between them. What opening
      // could add, the model call already wrote. The seal records what the step produced.
      const session = await agent.open(input.sessionFile, () => {
        throw new Error(
          "a tool activity must not write to the session: the seal records what the step produced",
        );
      });
      try {
        const { turn, step, call } = input;
        // Already sealed. Clean up the leftover kept result.
        if (session.answered(call.id)) {
          await pending.forgetResults(input.sessionFile, turn, step, [call.id]);
          return { outcome: "already-settled" };
        }
        if ((await pending.readResult(input.sessionFile, turn, step, call.id)) !== undefined) {
          return { outcome: "already-settled" };
        }
        // Retryable on purpose. Shared storage may not show the recorded call on this host yet.
        if (!session.asked(call.id)) {
          throw new Error(`no recorded tool call ${call.id} in ${input.sessionId}`);
        }

        // A stopped turn's seal may be about to take this claim. Don't race it. An abandoned
        // attempt leaves the call to its retry.
        const context = Context.current();
        if (stopRequested() || abandoned(context)) throw context.cancellationSignal.reason;
        if (!(await pending.claimDispatch(input.sessionFile, turn, step, call.id))) {
          // An earlier dispatch started this tool, so it may have taken effect. Report unknown
          // rather than run a push or a delete again. The first attempt may still return, and
          // the seal supplies unknown if no result arrives.
          return { outcome: "unknown" };
        }
        claimed = true;

        // A marker that a tool is running here. A later turn checks it before reusing this
        // directory, since a timed-out attempt may still be running.
        await store?.beginWrite(writer);
        const outcome = await session.runToolCall(call.id, cancellation()).finally(async () => {
          // The outcome outranks the marker. A marker left behind only holds the directory until
          // its process is shown gone. A lost outcome turns a tool that worked into an unknown one.
          await store?.endWrite(writer).catch((err: unknown) => {
            say("warn", `could not clear the writer marker after ${call.id}: ${err}`);
          });
        });
        if (outcome === undefined) return { outcome: "already-settled" };
        // The claim stays, so the retry reports unknown. The aborted result says nothing true.
        if (abandoned(context)) throw context.cancellationSignal.reason;

        await pending.keepResult(input.sessionFile, turn, step, call.id, outcome);
        // Ship from here. Only this host has the tool's changes, and the seal may land elsewhere.
        // The result is kept, so a failure here must not fail the call. The next step's restore or
        // a later capture can carry the files.
        await shipTree(input.sessionFile, { current: writer, fence: { turn, step } }).catch(
          (err: unknown) => say("warn", `could not ship the project after ${call.id}: ${err}`),
        );
        return { outcome: "settled" };
      } finally {
        session.dispose();
      }
    } catch (err) {
      // A cancel stays a cancel. Typed as a failure, it would hide it, and a non-retryable one
      // would end a paused call for good.
      if (stopRequested() || abandoned(Context.current())) throw err;
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
      return await withFence(input.sessionFile, input.fence, async (guard) => {
        // Close the step first. The lost host, or a stopped tool still running there, may publish
        // from a stale tip later, and this lets that capture be refused.
        if (input.interrupted) await store?.closeStep(input.sessionFile, input);
        // A cancelled tool may still be writing on another host.
        if (!input.interrupted) await bringTree(input.sessionFile);
        const session = await agent.open(input.sessionFile, guard);
        try {
          const outcomes: ToolOutcome[] = [];
          for (const call of input.calls) {
            const { turn, step } = input;
            const kept = await pending.readResult(input.sessionFile, turn, step, call.id);
            if (kept !== undefined) {
              outcomes.push(kept);
              continue;
            }
            // No kept result. Take the claim, so a late attempt can't run the tool after the step
            // closed. If the seal gets it, nothing started the call. Otherwise it's unknown, since
            // providers reject unanswered calls.
            const unstarted = await pending.claimDispatch(input.sessionFile, turn, step, call.id);
            outcomes.push(unstarted ? agent.notRunOutcome(call) : agent.unknownOutcome(call));
          }

          // `expectCalls` stops results being attached to a response appended since the model call.
          guard();
          const before = session.spend();
          const sealed = await session.sealStep(outcomes, {
            expectCalls: input.calls.map((call) => call.id),
            agentState: input.agentState,
            // A stopped turn records results only. No retry, no compaction.
            postRun: !input.interrupted,
            // A stop that lands during a retry or compaction ends it, rather than waiting it out.
            signal: cancellation(),
          });
          await session.waitForIdle();
          const seconds = secondsBefore(session, input.turn);
          recordSeconds(session, input.turn, input.sessionSeconds);
          // Kept results stay until the next step sweeps them, so a retried seal can read them.
          const answer = answerOf(session);
          // Ship after the seal, so the project matches the session.
          if (!input.interrupted) {
            const fence = { turn: input.turn, step: input.step };
            await shipTree(input.sessionFile, { fence }, true);
          }
          return {
            done: sealed.done,
            ...(sealed.agentState ? { agentState: sealed.agentState } : {}),
            finalText: sealed.done ? answer : "",
            ...totals(before, session),
            ...seconds,
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
    // Only a missing file means nothing to add to. Any other error is thrown, so Temporal retries
    // and the record still gets the Workflow's total.
    const exists = await access(sessionFile).then(
      () => true,
      (err: NodeJS.ErrnoException) => {
        if (err.code === "ENOENT") return false;
        throw err;
      },
    );
    if (!exists) return;
    await withFence(sessionFile, input.fence, async (guard) => {
      const record = await agent.openRecord(sessionFile, guard);
      if (!record) return;
      const known = (record.latestEntry(SESSION_SECONDS) as { seconds?: unknown } | undefined)
        ?.seconds;
      if (typeof known === "number" && known >= sessionSeconds) return;
      record.appendEntry(SESSION_SECONDS, { turn, seconds: sessionSeconds });
    });
  }

  /** Release the project directory when the session goes idle, or this Worker refuses every
   * other session. Also records the session as over, so other hosts can release theirs. */
  async function retireSession(input: RetireInput): Promise<void> {
    await keepSeconds(input);
    // Thrown, so Temporal retries. The Workflow gives up quietly once retries run out.
    if (await store?.retire(input.sessionFile)) {
      say("info", `handed the project back: ${input.sessionFile} went idle`);
    }
  }

  /** Copy a template project into a scheduled session before its first step. A no-op once the
   * session has its own, so a retry does not copy twice. */
  async function adoptProject(input: {
    readonly sessionFile: string;
    readonly template: string;
  }): Promise<void> {
    if (await store?.adopt(input.template, input.sessionFile)) {
      say("info", `took the project from ${input.template} for ${input.sessionFile}`);
    }
  }

  return { runStep, runModelCall, runToolCall, sealStep, retireSession, adoptProject };
}

export type CoreActivities = ReturnType<typeof makeUncheckedActivities>;
export type { ToolCallRef };
