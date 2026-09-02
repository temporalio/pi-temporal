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
import * as worktree from "./worktree.js";
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
  // Ship the project's files with the session, so a worker on another machine finds the work the
  // last one did. Without it the transcript travels and the files do not.
  readonly shipTree?: boolean;
}

export function makeActivities(opts: ActivityOptions) {
  const provider = opts.provider ?? "openai";

  // Before anything the model asked for can run, and after the step that ran it is written down.
  // Both are no-ops when the tree is off, and the restore is a no-op on the host that captured,
  // because the tree it holds is already the one the shared directory names.
  // Deliberately not caught. A step that cannot get the project's files must not run against
  // whatever is in the directory: the model would be told those files are the project. Failing
  // sends the work to a host that can do it.
  const bringTree = async (sessionFile: string) => {
    if (!opts.shipTree) return;
    await worktree.ensure(opts.projectDir, sessionFile);
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

  const shipTree = async (sessionFile: string) => {
    if (!opts.shipTree) return;
    // A capture that fails must not fail the step: the tool has already run and its result is
    // recorded, and a retry finds that result rather than running it again, so throwing here costs
    // an attempt and still ships nothing. What it must not do is lose the work. The files are the
    // only record of what the tool did, so they are kept where they can be recovered.
    await worktree.capture(opts.projectDir, sessionFile).catch(async (err) => {
      console.error(`could not ship the project tree: ${String(err)}`);
      await worktree.setAside(opts.projectDir, sessionFile).catch((keepErr) => {
        console.error(`and could not set it aside either: ${String(keepErr)}`);
      });
    });
  };

  async function openSession(sessionFile: string, guard?: () => void): Promise<AgentSession> {
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
      // Everything the last turn left. Steps are numbered per turn, so sweeping by step number
      // alone never reaches a turn that ran further than this one will.
      if (stepped) await pending.sweepAll(input.sessionFile);
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
      return { done: true, retryAttempt: 0, finalText: lastAssistantText(messages()) };
    }
    return undefined;
  }

  async function runStep(input: RunStepInput): Promise<RunStepResult> {
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
          const { done } = await session.step();
          await session.waitForIdle();
          const messages = session.state.messages as Msg[];
          // Whole-step mode has no carry. Its session is rebuilt per activity too, so its retry
          // budget starts at zero on every step and only the step ceiling bounds it. That is how
          // it has always been; giving it the carry means giving step() the seal's post-run pass.
          await shipTree(input.sessionFile);
          return { done, retryAttempt: 0, finalText: done ? lastAssistantText(messages) : "" };
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
  // work. Sequential is what the moving files cost.
  const mustSerialize = (sequential: boolean) => sequential || opts.shipTree === true;

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

          const outcome = await session.modelCall();
          return {
            calls: outcome.toolCalls.map((call) => ({ id: call.id, name: call.name })),
            sequential: mustSerialize(outcome.sequential),
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
      // A tool call is its own dispatch, so it can land on a worker that ran neither the model
      // call nor any sibling tool. Without this it runs against whatever files that host happens
      // to have, and reports the answer as if it were the project's.
      //
      // Under the session lock, because the tip and the bundles live beside the session file and
      // the tree's own lock is host-local. Two hosts publishing at once is the case that has to be
      // excluded, and only the shared lock excludes it.
      await withSessionLock(input.sessionFile, () => bringTree(input.sessionFile));
      // Opening a session can append to it (a first thinking-level entry), and two of these run at
      // once. The lock covers the open and is given back before the tool runs, which is the part
      // that has to stay parallel.
      // The guard has to change when the lock does. While the lock is held it asks whether it is
      // still ours; once it is given back, `ownedNow` can never go false again, so a guard closed
      // over it would wave every later write through. A tool activity has no business writing at
      // all, and an extension appending from `tool_execution_end` runs right here.
      let opening = true;
      const session = await withSessionLock(input.sessionFile, (_owned, ownedNow) =>
        openSession(input.sessionFile, () => {
          if (opening) return writeGuard(ownedNow, "opening the session")();
          throw new Error(
            "a tool activity must not write to the session: the seal is the step's only writer",
          );
        }),
      );
      opening = false;
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
        // Shipped from here, because this host ran the tool and is the only one holding what it
        // did. The seal can land anywhere, and capturing there would ship a directory that never
        // saw this tool. Under the shared lock, for the reason the restore above is.
        await withSessionLock(input.sessionFile, () => shipTree(input.sessionFile));
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
      return await withSessionLock(input.sessionFile, async (owned, ownedNow) => {
        // The seal writes the step down, so it needs the tree the tools worked in. It can land on
        // a worker that ran none of them.
        await bringTree(input.sessionFile);
        const session = await openSession(input.sessionFile, writeGuard(ownedNow, "the seal"));
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
          await stillOurs(owned, "the seal");
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
          await shipTree(input.sessionFile);
          return { done, retryAttempt: sealed.retryAttempt, finalText: done ? answer : "" };
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
   * later one is refused. */
  async function retireSession(input: { readonly sessionFile: string }): Promise<void> {
    if (!opts.shipTree) return;
    const freed = await worktree.release(opts.projectDir, input.sessionFile).catch((err) => {
      console.warn(`could not hand back ${opts.projectDir}: ${String(err)}`);
      return false;
    });
    if (freed) console.log(`handed ${opts.projectDir} back: ${input.sessionFile} went idle`);
  }

  return { runStep, runModelCall, runToolCall, sealStep, retireSession };
}

export type Activities = ReturnType<typeof makeActivities>;
