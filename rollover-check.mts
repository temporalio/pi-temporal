// Checks that a long-lived session rolls over instead of growing its history until the server
// terminates it, and that the rollover is invisible: a prompt queued against the old run is
// answered by the new one.
//
// The rollover is driven by `maxHistory` here. In production it is the server's own
// `continueAsNewSuggested`, which no check can reach without writing tens of thousands of events.
//
// Needs a Temporal server; no model key. Usage: tsx rollover-check.mts

import { randomUUID } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { Client, Connection } from "@temporalio/client";
import { NativeConnection, Worker } from "@temporalio/worker";
import { fromEnv, sessionFileFor } from "./src/config.js";
import { WORKFLOW_TYPE, workflowId } from "./src/protocol.js";
import type {
  PromptInput,
  RunStepInput,
  RunStepResult,
  SessionTurnOptions,
} from "./src/protocol.js";

const cfg = fromEnv();
const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

// Every prompt this run was asked to answer, in order, so the check can tell whether the work
// queued against the old run survived the handover.
const answered: string[] = [];

async function main() {
  const taskQueue = `pi-rollover-${randomUUID().slice(0, 8)}`;
  const sessionId = `rollover-${randomUUID().slice(0, 8)}`;
  // Small enough that a couple of turns crosses it. One turn is a handful of events.
  const options: SessionTurnOptions = { idleTimeout: "10 seconds", maxHistory: 24 };

  const nativeConnection = await NativeConnection.connect({ address: cfg.address });
  const worker = await Worker.create({
    connection: nativeConnection,
    namespace: cfg.namespace,
    taskQueue,
    workflowsPath: fileURLToPath(new URL("./src/workflow.ts", import.meta.url)),
    activities: {
      async runStep(input: RunStepInput): Promise<RunStepResult> {
        answered.push(input.text);
        return { done: true, retryAttempt: 0, finalText: `answered ${input.text}` };
      },
    },
  });
  const running = worker.run();

  const connection = await Connection.connect({ address: cfg.address });
  const client = new Client({ connection, namespace: cfg.namespace });
  const handle = () => client.workflow.getHandle(workflowId(sessionId));
  const send = (text: string) =>
    client.workflow.signalWithStart(WORKFLOW_TYPE, {
      taskQueue,
      workflowId: workflowId(sessionId),
      args: [sessionId, sessionFileFor(cfg.sessionDir, sessionId), options],
      signal: "submitPrompt",
      signalArgs: [{ promptId: randomUUID(), text } satisfies PromptInput],
    });

  try {
    await send("one");
    const first = (await handle().describe()).runId;

    // Enough turns to cross the bound. Each one adds its own events, so the run that started this
    // is not the run that finishes it.
    for (const text of ["two", "three", "four", "five", "six"]) {
      await send(text);
      await sleep(400);
    }

    let rolled = "";
    for (let waited = 0; waited < 20_000 && !rolled; waited += 200) {
      const now = (await handle().describe()).runId;
      if (now !== first) rolled = now;
      else await sleep(200);
    }
    check("a session rolls over instead of growing without bound", rolled !== "", { first });

    // The point of carrying the queue, and the assertion that has to be about every prompt rather
    // than about one sent after the rollover was already visible. Whatever is in the queue when the
    // rollover fires is exactly what a dropped queue loses, and a client was told it was accepted.
    await send("after");
    const wanted = ["one", "two", "three", "four", "five", "six", "after"];
    for (let waited = 0; waited < 30_000; waited += 200) {
      if (wanted.every((t) => answered.includes(t))) break;
      await sleep(200);
    }
    const missing = wanted.filter((t) => !answered.includes(t));
    check(
      "and every prompt it accepted is still answered",
      missing.length === 0,
      { missing, answered },
    );
    check("each of them once", answered.length === wanted.length, answered);
  } finally {
    worker.shutdown();
    await running.catch(() => undefined);
    await connection.close();
    await nativeConnection.close();
  }

  console.log(
    failures.length === 0
      ? "\nrollover-check: OK"
      : `\nrollover-check: ${failures.length} failed`,
  );
  process.exit(failures.length === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
