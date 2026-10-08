// Checks that a long session continues as new and that no queued prompt is lost across it. Forces
// Continue-As-New with a small `maxHistory` (production uses `continueAsNewSuggested`) and asserts
// every accepted prompt is answered exactly once.
//
// Needs a Temporal server, no model key. Usage: tsx checks/continue-as-new-check.mts

import { randomUUID } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { Client, Connection } from "@temporalio/client";
import { NativeConnection, Worker } from "@temporalio/worker";
import { fromEnv, sessionFileFor } from "../src/config.js";
import { WORKFLOW_TYPE, workflowId } from "../src/core/protocol.js";
import type {
  PromptInput,
  RunStepInput,
  RunStepResult,
  SessionTurnOptions,
} from "../src/core/protocol.js";

const cfg = fromEnv();
const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

const answered: string[] = [];

async function main() {
  const taskQueue = `pi-continue-as-new-${randomUUID().slice(0, 8)}`;
  const sessionId = `continue-as-new-${randomUUID().slice(0, 8)}`;
  // A couple of turns cross this.
  const options: SessionTurnOptions = { idleTimeout: "10 seconds", maxHistory: 24 };

  const nativeConnection = await NativeConnection.connect({ address: cfg.address });
  const worker = await Worker.create({
    connection: nativeConnection,
    namespace: cfg.namespace,
    taskQueue,
    workflowsPath: fileURLToPath(new URL("../src/core/workflow.ts", import.meta.url)),
    activities: {
      async runStep(input: RunStepInput): Promise<RunStepResult> {
        answered.push(input.text ?? "");
        return { done: true, finalText: `answered ${input.text}` };
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
      args: [{ sessionId, sessionFile: sessionFileFor(cfg.sessionDir, sessionId), ...options }],
      signal: "submitPrompt",
      signalArgs: [{ promptId: randomUUID(), text } satisfies PromptInput],
    });

  try {
    await send("one");
    const first = (await handle().describe()).runId;

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
    check("a session continues as new instead of growing without bound", rolled !== "", { first });

    // Assert on every prompt, not just one sent after Continue-As-New. Prompts queued when it fires
    // are the ones a dropped queue would lose.
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
      ? "\ncontinue-as-new-check: OK"
      : `\ncontinue-as-new-check: ${failures.length} failed`,
  );
  process.exit(failures.length === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
