// Checks that a refused directory costs only that host, not the session. Two worker processes
// share a queue, and one has a live writer marker so its directory is refused. Asserts the work
// runs only on the other host and the turn is answered.
//
// Needs a Temporal server, no model key. Usage: npx tsx checks/quarantine-routing-check.mts

import { appendFileSync } from "node:fs";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { execFile, spawn } from "node:child_process";
import { promisify } from "node:util";
import { Client, Connection } from "@temporalio/client";
import { NativeConnection, Worker } from "@temporalio/worker";
import { makeActivities } from "../src/pi/activities.js";
import { QUERIES } from "../src/core/protocol.js";
import type { TurnState } from "../src/core/protocol.js";
import * as worktree from "../src/tree/worktree.js";
import type { AgentSession } from "@earendil-works/pi-coding-agent";

const run = promisify(execFile);
const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

const address = process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7233";
const queue = `pi-quarantine-routing-${Date.now()}`;

async function main() {
  const root = await mkdtemp(join(tmpdir(), "pi-q-routing-"));
  process.env.PI_TEMPORAL_DATA = join(root, "data");
  const sessions = join(root, "sessions");
  await mkdir(sessions, { recursive: true });
  const sessionFile = join(sessions, "s1.jsonl");

  const hosts = { refused: join(root, "host-refused"), free: join(root, "host-free") };
  await mkdir(hosts.refused, { recursive: true });
  // Empty, like a fresh worker. A non-empty unknown directory would be refused as a checkout.
  await mkdir(hosts.free, { recursive: true });
  await run("git", ["init", "-q", hosts.refused]);
  await writeFile(join(hosts.refused, "README.md"), "one\n");
  await worktree.capture(hosts.refused, sessionFile, { seed: true });

  // An earlier tool call that never returned, on the refused host only.
  await worktree.beginWrite(hosts.refused, { turn: "earlier", step: 1, callId: "never-returned" });

  const ranOn: string[] = [];
  // A minimal session for the real activity. The prompt must land in `messages`, since the activity
  // reads that to tell if the turn started. One transcript per process stands in for the file.
  const fakeSession = `() => {
    const messages = (globalThis.__fakeTranscript ??= []);
    return {
      state: { messages },
      prepareStep: () => true,
      recordPrompt: async (text) => {
        messages.push({ role: "user", content: text, timestamp: Date.now() });
        return true;
      },
      modelCall: async () => {
        messages.push({ role: "assistant", content: "answered", timestamp: Date.now() });
        return { toolCalls: [], sequential: false, ended: false };
      },
      runToolCall: async () => undefined,
      sealStep: async () => ({ done: true, retryAttempt: 0 }),
      waitForIdle: async () => {},
      dispose() {},
    };
  }`;

  const connection = await Connection.connect({ address });
  const native = await NativeConnection.connect({ address });
  const client = new Client({ connection, namespace: "default" });

  // This process is the refused host and also runs the workflows.
  const refused = await Worker.create({
    connection: native,
    namespace: "default",
    taskQueue: queue,
    // Eager dispatch would hand every activity back to this worker and hide the routing.
    maxEagerActivityReservationsPerWorkflowTask: 0,
    workflowsPath: fileURLToPath(new URL("../src/workflow-bundle.ts", import.meta.url)),
    activities: makeActivities(
      { projectDir: hosts.refused, shipTree: true },
      {
        openSession: async () => {
          ranOn.push("refused");
          // eslint-disable-next-line no-eval
          return (0, eval)(`(${fakeSession})`)() as AgentSession;
        },
      },
    ),
  });

  // The free host, an activity-only worker in a child process. Written beside this file, since a
  // module in /tmp cannot resolve the SDK.
  // Unique per run, so two runs from one checkout don't overwrite each other's helper.
  const helper = fileURLToPath(
    new URL(`./.free-host-worker-${process.pid}-${Date.now()}.tmp.mts`, import.meta.url),
  );
  await writeFile(
    helper,
    `import { NativeConnection, Worker } from "@temporalio/worker";\n` +
      `import { makeActivities } from ${JSON.stringify(
        fileURLToPath(new URL("../src/pi/activities.js", import.meta.url)),
      )};\n` +
      `const native = await NativeConnection.connect({ address: ${JSON.stringify(address)} });\n` +
      `const worker = await Worker.create({\n` +
      `  connection: native, namespace: "default", taskQueue: ${JSON.stringify(queue)},\n` +
      `  activities: makeActivities({ projectDir: ${JSON.stringify(hosts.free)}, ` +
      `shipTree: true }, {\n` +
      `    openSession: async () => {\n` +
      `      console.log("RAN_ON free");\n` +
      `      return (${fakeSession})();\n` +
      `    },\n` +
      `  }),\n` +
      `});\n` +
      `const running = worker.run();\n` +
      `console.log("FREE_HOST_READY");\n` +
      `await running;\n`,
    "utf8",
  );
  const child = spawn(process.execPath, ["--import", "tsx", helper], {
    cwd: fileURLToPath(new URL(".", import.meta.url)),
    env: { ...process.env, PI_TEMPORAL_DATA: process.env.PI_TEMPORAL_DATA },
    stdio: ["ignore", "pipe", "pipe"],
  });
  // The child has to be polling before the turn starts, or the refused host answers unopposed.
  let out = "";
  const ready = new Promise<boolean>((resolve) => {
    const timer = setTimeout(() => resolve(false), 60_000);
    child.stdout.on("data", (chunk: Buffer) => {
      out += chunk.toString();
      if (out.includes("FREE_HOST_READY")) {
        clearTimeout(timer);
        resolve(true);
      }
    });
    child.on("exit", () => {
      clearTimeout(timer);
      resolve(false);
    });
  });
  child.stdout.on("data", (chunk: Buffer) => {
    if (chunk.toString().includes("RAN_ON free")) ranOn.push("free");
  });
  child.stderr.on("data", (chunk: Buffer) => {
    const line = chunk.toString();
    if (process.env.FREE_HOST_LOG) appendFileSync(process.env.FREE_HOST_LOG, line);
    if (/Error|error:/.test(line)) console.error("[free host]", line.split("\n")[0].slice(0, 200));
  });
  const running = [refused.run()];

  try {
    if (!(await ready)) throw new Error("the free host never started polling");
    const handle = await client.workflow.start("piSession", {
      workflowId: `${queue}-session`,
      taskQueue: queue,
      // A worker that ships the tree refuses whole-step sessions.
      args: ["routing", sessionFile, {
        idleTimeout: "100 milliseconds",
        stepped: true,
        initialPrompt: { promptId: "routing", text: "run" },
      } as never],
    });
    // Long enough for the refusal, its backoff, and a redispatch.
    const started = Date.now();
    const deadline = started + 120_000;
    let finished: TurnState["finished"];
    while (Date.now() < deadline && !finished) {
      finished = (await handle.query<TurnState, []>(QUERIES.turnState).catch((err) => {
        console.error("[query]", String(err).slice(0, 200));
        return undefined;
      }))?.finished;
      if (!finished) await new Promise((r) => setTimeout(r, 500));
    }
    console.log(`the turn settled after ${Math.round((Date.now() - started) / 1000)}s`);
    await handle.terminate().catch(() => undefined);

    // The refusal is retryable, so any other worker can take the work.
    check("the work reaches a host that is not refused", ranOn.includes("free"), ranOn);
    check("and never runs on the refused one", !ranOn.includes("refused"), ranOn);
    // The session here is in memory. `detached-check.mts` covers a real transcript.
    check("and the turn is answered", finished?.outcome === "answered", finished);
    check("by the host that took the work", finished?.finalText === "answered", finished);
  } finally {
    child.kill("SIGKILL");
    await rm(helper, { force: true });
    refused.shutdown();
    await Promise.allSettled(running);
    await connection.close();
    native.close();
    await rm(root, { recursive: true, force: true });
  }

  console.log(
    failures.length === 0
      ? "quarantine-routing-check: OK"
      : `quarantine-routing-check: ${failures.length} failed`,
  );
  process.exit(failures.length === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
