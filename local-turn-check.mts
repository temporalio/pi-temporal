// Checks the workflow that wraps a turn of a live pi session, with the turn itself faked: the
// whole-turn mode hands the turn over once, and the stepped mode records the turn once and then
// drives a model call, its calls and a seal per step.
//
// Needs a Temporal server; no model key and no pi session. Usage: tsx local-turn-check.mts

import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import { Client, Connection } from "@temporalio/client";
import { NativeConnection, Worker } from "@temporalio/worker";
import { unknownToolCallOutcome } from "@earendil-works/pi-coding-agent";
import { fromEnv } from "./src/config.js";
import { type LiveTurn, type LiveTurns, makeLocalTurnActivities } from "./src/local-turn-activity.js";
import { LOCAL_TURN_WORKFLOW } from "./src/protocol.js";
import type { LocalTurnInput } from "./src/protocol.js";

const STEPS_TO_ANSWER = 3;
const cfg = fromEnv();

const failures: string[] = [];
const check = (what: string, ok: boolean, detail: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

/** A turn that asks for one tool per step and answers on the last one. */
function fakeTurn() {
  const seen: string[] = [];
  let step = 0;
  const turn: LiveTurn = {
    run: async () => {
      seen.push("run");
    },
    steps: {
      record: async () => {
        seen.push("record");
      },
      modelCall: async () => {
        step++;
        seen.push(`model:${step}`);
        const asksForTool = step < STEPS_TO_ANSWER;
        return {
          toolCalls: asksForTool
            ? [{ type: "toolCall" as const, id: `c${step}`, name: "probe", arguments: {} }]
            : [],
          sequential: false,
          ended: false,
          replayed: false,
        };
      },
      runToolCall: async (toolCallId) => {
        seen.push(`tool:${toolCallId}`);
        return unknownToolCallOutcome({ id: toolCallId, name: "probe" });
      },
      sealStep: async (results) => {
        seen.push(`seal:${step}:${results.length}`);
        return { done: step === STEPS_TO_ANSWER };
      },
    },
  };
  return { turn, seen };
}

async function main() {
  const taskQueue = `pi-local-turn-check-${randomUUID().slice(0, 8)}`;
  const live: LiveTurns = new Map();

  const nativeConnection = await NativeConnection.connect({ address: cfg.address });
  const worker = await Worker.create({
    connection: nativeConnection,
    namespace: cfg.namespace,
    taskQueue,
    workflowsPath: fileURLToPath(new URL("./src/workflows.ts", import.meta.url)),
    activities: makeLocalTurnActivities(live),
  });
  const running = worker.run();

  const connection = await Connection.connect({ address: cfg.address });
  const client = new Client({ connection, namespace: cfg.namespace });

  const drive = async (stepped: boolean) => {
    const turnId = randomUUID();
    const { turn, seen } = fakeTurn();
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
    return seen;
  };

  // The whole turn is handed over once, and pi runs it the way it always did.
  const whole = await drive(false);
  check("whole-turn: the turn is handed over once", JSON.stringify(whole) === '["run"]', whole);

  // The stepped mode records the turn once, then one model call, its calls and a seal per step.
  const stepped = await drive(true);
  const expected = [
    "record",
    "model:1",
    "tool:c1",
    "seal:1:1",
    "model:2",
    "tool:c2",
    "seal:2:1",
    "model:3",
    "seal:3:0",
  ];
  check("stepped: the prompt is recorded once", stepped.filter((s) => s === "record").length === 1, stepped);
  check("stepped: a model call, its calls and a seal per step", JSON.stringify(stepped) === JSON.stringify(expected), stepped);
  check("stepped: the turn was not also run whole", !stepped.includes("run"), stepped);

  worker.shutdown();
  await running;
  await connection.close();
  await nativeConnection.close();

  console.log(failures.length === 0 ? "local-turn-check: OK" : `local-turn-check: ${failures.length} failed`);
  process.exit(failures.length === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
