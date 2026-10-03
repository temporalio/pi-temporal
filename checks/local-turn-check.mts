// Checks the workflow that wraps a turn of a live pi session, with the turn itself faked: the
// whole-turn mode hands the turn over once, and the stepped mode records the turn once and then
// drives a model call, its calls and a seal per step.
//
// Needs a Temporal server; no model key and no pi session. Usage: tsx checks/local-turn-check.mts

import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import { ApplicationFailure } from "@temporalio/activity";
import { Client, Connection } from "@temporalio/client";
import { NativeConnection, Worker } from "@temporalio/worker";
import { unknownToolCallOutcome } from "@earendil-works/pi-coding-agent";
import { fromEnv } from "../src/config.js";
import {
  type LiveTurn,
  type LiveTurns,
  makeLocalTurnActivities,
} from "../src/local-turn-activity.js";
import { LOCAL_TURN_WORKFLOW } from "../src/protocol.js";
import type { LocalTurnInput } from "../src/protocol.js";

const STEPS_TO_ANSWER = 3;
const cfg = fromEnv();

const failures: string[] = [];
const check = (what: string, ok: boolean, detail: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

/**
 * A turn that asks for two tools per step and answers on the last one. Two matters: the calls all
 * reach the one live agent this process holds, which admits a single unit of work at a time, so a
 * second call arriving while the first runs is refused.
 */
function fakeTurn(options: { interruptAfter?: number; stopInFirstTool?: boolean } = {}) {
  const seen: string[] = [];
  const postRuns: (boolean | undefined)[] = [];
  let step = 0;
  let stoppedInTool = false;
  let inFlight = 0;
  let overlapped = false;
  const turn: LiveTurn = {
    run: async () => {
      seen.push("run");
    },
    steps: {
      record: async () => {
        seen.push("record");
      },
      interrupted: () =>
        stoppedInTool || (options.interruptAfter !== undefined && step >= options.interruptAfter),
      modelCall: async () => {
        step++;
        seen.push(`model:${step}`);
        const asksForTools = step < STEPS_TO_ANSWER;
        return {
          toolCalls: asksForTools
            ? [`c${step}a`, `c${step}b`].map((id) => ({
                type: "toolCall" as const,
                id,
                name: "probe",
                arguments: {},
              }))
            : [],
          sequential: false,
          ended: false,
        };
      },
      runToolCall: async (toolCallId) => {
        seen.push(`tool:${toolCallId}`);
        // The user stops pi while this tool runs: the abort reaches it and it throws.
        if (options.stopInFirstTool && !stoppedInTool) {
          stoppedInTool = true;
          throw new Error("aborted");
        }
        inFlight++;
        if (inFlight > 1) overlapped = true;
        // Long enough that a second call dispatched at the same time would be seen here.
        await new Promise((resolve) => setTimeout(resolve, 30));
        inFlight--;
        return unknownToolCallOutcome({ id: toolCallId, name: "probe" });
      },
      sealStep: async (results, sealOptions) => {
        seen.push(`seal:${step}:${results.length}`);
        postRuns.push(sealOptions?.postRun);
        return { done: step === STEPS_TO_ANSWER, retryAttempt: 0 };
      },
    },
  };
  return { turn, seen, postRuns, didOverlap: () => overlapped };
}

async function main() {
  const taskQueue = `pi-local-turn-check-${randomUUID().slice(0, 8)}`;
  const live: LiveTurns = new Map();

  const nativeConnection = await NativeConnection.connect({ address: cfg.address });
  const worker = await Worker.create({
    connection: nativeConnection,
    namespace: cfg.namespace,
    taskQueue,
    workflowsPath: fileURLToPath(new URL("../src/workflows.ts", import.meta.url)),
    activities: makeLocalTurnActivities(live),
  });
  const running = worker.run();

  const connection = await Connection.connect({ address: cfg.address });
  const client = new Client({ connection, namespace: cfg.namespace });

  const drive = async (
    stepped: boolean,
    options: { interruptAfter?: number; stopInFirstTool?: boolean } = {},
  ) => {
    const turnId = randomUUID();
    const { turn, seen, postRuns, didOverlap } = fakeTurn(options);
    live.set(turnId, turn);
    const input: LocalTurnInput = { sessionId: "ses_1", turnId, taskQueue, stepped };
    try {
      await client.workflow.execute(LOCAL_TURN_WORKFLOW, {
        taskQueue,
        workflowId: `pi-turn-check-${turnId}`,
        args: [input],
      });
    } finally {
      live.delete(turnId);
    }
    return { seen, postRuns, didOverlap };
  };

  // The whole turn is handed over once, and pi runs it the way it always did.
  const whole = await drive(false);
  const handed = JSON.stringify(whole.seen) === '["run"]';
  check("whole-turn: the turn is handed over once", handed, whole.seen);

  // The stepped mode records the turn once, then one model call, its calls and a seal per step.
  const stepped = await drive(true);
  const expected = [
    "record",
    "model:1",
    "tool:c1a",
    "tool:c1b",
    "seal:1:2",
    "model:2",
    "tool:c2a",
    "tool:c2b",
    "seal:2:2",
    "model:3",
    "seal:3:0",
  ];
  const recorded = stepped.seen.filter((s) => s === "record").length === 1;
  check("stepped: the prompt is recorded once", recorded, stepped.seen);
  check(
    "stepped: a model call, its calls and a seal per step",
    JSON.stringify(stepped.seen) === JSON.stringify(expected),
    stepped.seen,
  );
  check("stepped: the turn was not also run whole", !stepped.seen.includes("run"), stepped.seen);
  // The live agent admits one unit of work at a time. A second call arriving while the first runs
  // is refused, and the step reports a tool that never ran as an unknown outcome.
  check("stepped: the calls of a step do not overlap", !stepped.didOverlap(), stepped.seen);

  // An abort reaches the unit that is running and nothing else, so the loop has to stop asking.
  const stopped = await drive(true, { interruptAfter: 1 });
  check("stepped: an interrupt stops the loop", !stopped.seen.includes("model:2"), stopped.seen);
  const resealed = stopped.seen.includes("seal:2:0");
  check("stepped: an interrupted turn is not sealed again", !resealed, stopped.seen);

  // A stop that lands inside a tool call. The call fails because of the stop, and that has to end
  // the step as a stop: its sibling does not run, the step is sealed without the post-run pass,
  // and the turn ends there rather than asking the model again.
  const midTool = await drive(true, { stopInFirstTool: true });
  check(
    "stepped: a stop inside a tool call does not run its sibling",
    !midTool.seen.includes("tool:c1b"),
    midTool.seen,
  );
  check(
    "stepped: and seals the step once, as stopped",
    midTool.seen.filter((s) => s.startsWith("seal:")).length === 1 &&
      JSON.stringify(midTool.postRuns) === "[false]",
    { seen: midTool.seen, postRuns: midTool.postRuns },
  );
  check(
    "stepped: and asks the model nothing more",
    !midTool.seen.includes("model:2"),
    midTool.seen,
  );

  // A stop between the executor being handed the turn and the first model call still has to leave
  // the prompt somewhere. Dropped, the user's text is gone with no error to show for it.
  const early = await drive(true, { interruptAfter: 0 });
  const kept = early.seen[0] === "record";
  check("stepped: a stop before the first call still records the prompt", kept, early.seen);
  check("stepped: and asks the model nothing", !early.seen.includes("model:1"), early.seen);

  // Straight at the activities, because a retry of one is not something the workflow script can
  // ask for. An interrupted model call whose answer never reached Temporal comes back, and what
  // it must not do is record the turn's prompt a second time.
  {
    const turnId = randomUUID();
    const { turn, seen } = fakeTurn({ interruptAfter: 0 });
    live.set(turnId, turn);
    const activities = makeLocalTurnActivities(live);
    try {
      await activities.runLocalModelCall({ turnId, step: 1 });
      await activities.runLocalModelCall({ turnId, step: 1 });
    } finally {
      live.delete(turnId);
    }
    const records = seen.filter((s) => s === "record").length;
    check("a retried model call does not record the prompt twice", records === 1, seen);
  }

  // A tool that throws once the user has stopped the turn must not be started again by a retry,
  // because nothing kept says it ran.
  {
    const turnId = randomUUID();
    const { turn } = fakeTurn({ interruptAfter: 0 });
    let runs = 0;
    turn.steps.runToolCall = async () => {
      runs++;
      throw new Error("aborted");
    };
    live.set(turnId, turn);
    const activities = makeLocalTurnActivities(live);
    let failure: unknown;
    try {
      await activities.runLocalToolCall({ turnId, step: 1, call: { id: "c1", name: "bash" } });
    } catch (err) {
      failure = err;
    } finally {
      live.delete(turnId);
    }
    const nonRetryable = failure instanceof ApplicationFailure && failure.nonRetryable === true;
    check("a tool that fails on a stopped turn is not retried", nonRetryable && runs === 1, {
      failure: String(failure),
      runs,
    });
  }

  worker.shutdown();
  await running;
  await connection.close();
  await nativeConnection.close();

  const bad = failures.length;
  console.log(bad === 0 ? "local-turn-check: OK" : `local-turn-check: ${bad} failed`);
  process.exit(failures.length === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
