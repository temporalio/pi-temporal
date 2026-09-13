// A turn the operator has put a bound on, and what happens when it reaches it.
//
// This is the thing the workflow having the loop is for. The agent is not asked to keep to a
// budget and cannot be: the model decides what to ask for next, so whatever says no has to sit
// somewhere the model does not reach. Here that is between two steps, which is also the only place
// it can sit: a step that has started is left to finish, because stopping one leaves a call in the
// transcript that no result answers and the next turn of the session fails rather than this one.
//
// Both bounds are driven with stub activities that report what they spent, so what is checked is
// the workflow adding it up and stopping, not a provider's billing.
//
// Needs a Temporal server; no model key. Usage: npx tsx budget-check.mts

import { randomUUID } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { Client, Connection } from "@temporalio/client";
import { NativeConnection, Worker } from "@temporalio/worker";
import { QUERIES } from "./src/protocol.js";
import type { RunStepInput, RunStepResult, SessionTurnOptions, TurnState } from "./src/protocol.js";

const address = process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7233";
const queue = `pi-budget-${Date.now()}`;

const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

/** One step, as expensive and as slow as the case wants, and never finished unless it says so. */
const steps = new Map<string, number>();
let tokensPerStep = 0;
let secondsPerStep = 0;
let answerAfter = Number.POSITIVE_INFINITY;

const activities = {
  async runStep(input: RunStepInput): Promise<RunStepResult> {
    const taken = (steps.get(input.promptId) ?? 0) + 1;
    steps.set(input.promptId, taken);
    if (secondsPerStep) await sleep(secondsPerStep * 1000);
    return {
      done: taken >= answerAfter,
      retryAttempt: 0,
      finalText: taken >= answerAfter ? "answered" : "",
      spent: { tokens: tokensPerStep, cost: tokensPerStep / 1000 },
    };
  },
  async retireSession() {},
  async adoptProject() {},
};

async function main() {
  const connection = await Connection.connect({ address });
  const native = await NativeConnection.connect({ address });
  const client = new Client({ connection, namespace: "default" });
  const worker = await Worker.create({
    connection: native,
    namespace: "default",
    taskQueue: queue,
    workflowsPath: fileURLToPath(new URL("./src/workflows.ts", import.meta.url)),
    activities,
  });
  const running = worker.run();

  const turn = async (budget: SessionTurnOptions["budget"]) => {
    const promptId = randomUUID();
    const began = Date.now();
    const handle = await client.workflow.start("piSession", {
      workflowId: `${queue}-${promptId}`,
      taskQueue: queue,
      args: [promptId, `/unused/${promptId}.jsonl`, {
        idleTimeout: "100 milliseconds",
        initialPrompt: { promptId, text: "run" },
        budget,
      } satisfies SessionTurnOptions as never],
    });
    const deadline = Date.now() + 60_000;
    let finished: TurnState["finished"];
    while (Date.now() < deadline && !finished) {
      finished = (await handle.query<TurnState, []>(QUERIES.turnState).catch(() => undefined))?.finished;
      if (!finished) await sleep(200);
    }
    await handle.terminate().catch(() => undefined);
    return { finished, taken: steps.get(promptId) ?? 0, ran: Date.now() - began };
  };

  try {
    // Tokens. Three steps at 100 is 300, which is over 250, so the third step is the last one the
    // workflow drives: the bound is checked after a step, because that is when its cost is known.
    tokensPerStep = 100;
    const spent = await turn({ tokens: 250 });
    check("a turn that runs out of tokens is stopped where it got to", spent.finished?.outcome === "budget", spent.finished);
    check("after the step that crossed the bound, not before it", spent.taken === 3, spent.taken);

    // And a turn well inside its bound is not touched by any of it.
    answerAfter = 2;
    const inside = await turn({ tokens: 250 });
    check("a turn inside its bound answers as usual", inside.finished?.outcome === "answered", inside.finished);
    check("and runs every step it needed", inside.taken === 2, inside.taken);

    // Wall clock, with the same shape: the step that crosses it is the last one.
    answerAfter = Number.POSITIVE_INFINITY;
    tokensPerStep = 0;
    secondsPerStep = 1;
    const late = await turn({ seconds: 2 });
    check("a turn that runs out of time is stopped too", late.finished?.outcome === "budget", late.finished);
    // Not before it has spent it: more than one step, and the turn really did run for its bound.
    // How many steps that is depends on what each one costs to schedule, which is not this check's
    // business; that it did not stop early is.
    check("and not before it has spent it", late.taken >= 2 && late.ran >= 2_000, late);
  } finally {
    worker.shutdown();
    await running.catch(() => undefined);
    await connection.close();
    native.close();
  }

  console.log(failures.length === 0 ? "budget-check: OK" : `budget-check: ${failures.length} failed`);
  process.exit(failures.length === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
