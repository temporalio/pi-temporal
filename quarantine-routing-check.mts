// A refused directory must cost that directory, not the session. The refusal is persistent by
// design: nothing on the host can prove a tool that never came back has stopped, so the marker
// stays until it returns or an operator clears it. What must not follow is a session that cannot
// run anywhere.
//
// Two workers on one queue, each standing in its own project directory, and in their own processes
// because that is what two hosts are. One of them is refused. What is asserted is that the turn
// still finishes, on the other one.
//
// Needs a Temporal server; no model key. Usage: npx tsx quarantine-routing-check.mts

import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { execFile, spawn } from "node:child_process";
import { promisify } from "node:util";
import { Client, Connection } from "@temporalio/client";
import { NativeConnection, Worker } from "@temporalio/worker";
import { makeActivities } from "./src/activities.js";
import * as worktree from "./src/worktree.js";
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
  // The second host is an empty directory, which is what a worker that has never served this
  // session has. A directory holding files the session never shipped is somebody's checkout, and
  // the store refuses that on purpose.
  await mkdir(hosts.free, { recursive: true });
  await run("git", ["init", "-q", hosts.refused]);
  await writeFile(join(hosts.refused, "README.md"), "one\n");
  await worktree.capture(hosts.refused, sessionFile, { seed: true });

  // A tool call from an earlier turn that never came back, on the first host only.
  await worktree.beginWrite(hosts.refused, { turn: "earlier", step: 1, callId: "never-returned" });

  const ranOn: string[] = [];
  // Enough of a session for the real activity: recording a prompt has to leave it in the messages,
  // because that is what the activity reads to decide whether this turn has started.
  const fakeSession = `() => {
    const messages = [];
    return {
      state: { messages },
      prepareStep: () => true,
      recordPrompt: async (text) => {
        messages.push({ role: "user", content: text, timestamp: Date.now() });
        return true;
      },
      step: async () => {
        messages.push({ role: "assistant", content: "answered", timestamp: Date.now() });
        return { done: true, finalText: "answered" };
      },
      dispose() {},
    };
  }`;

  const connection = await Connection.connect({ address });
  const native = await NativeConnection.connect({ address });
  const client = new Client({ connection, namespace: "default" });

  // This process is the refused host, and it carries the workflows.
  const refused = await Worker.create({
    connection: native,
    namespace: "default",
    taskQueue: queue,
    // Off, or this worker hands every activity it schedules straight back to itself and the other
    // host never gets a look. A fleet has that on and more than one candidate; here it would only
    // hide what is being measured.
    maxEagerActivityReservationsPerWorkflowTask: 0,
    workflowsPath: fileURLToPath(new URL("./src/workflows.ts", import.meta.url)),
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

  // The other host is another process, polling the same queue for activities only.
  // Beside this file, not in the temporary root: Node resolves a module's imports from where the
  // file is, so a worker written into /tmp cannot find the SDK.
  const helper = fileURLToPath(new URL("./.free-host-worker.tmp.mts", import.meta.url));
  await writeFile(
    helper,
    `import { NativeConnection, Worker } from "@temporalio/worker";\n` +
      `import { makeActivities } from ${JSON.stringify(fileURLToPath(new URL("./src/activities.js", import.meta.url)))};\n` +
      `const native = await NativeConnection.connect({ address: ${JSON.stringify(address)} });\n` +
      `const worker = await Worker.create({\n` +
      `  connection: native, namespace: "default", taskQueue: ${JSON.stringify(queue)},\n` +
      `  activities: makeActivities({ projectDir: ${JSON.stringify(hosts.free)}, shipTree: true }, {\n` +
      `    openSession: async () => {\n` +
      `      console.log("RAN_ON free");\n` +
      `      return (${fakeSession})();\n` +
      `    },\n` +
      `  }),\n` +
      `});\n` +
      `await worker.run();\n`,
    "utf8",
  );
  const child = spawn(process.execPath, ["--import", "tsx", helper], {
    cwd: fileURLToPath(new URL(".", import.meta.url)),
    env: { ...process.env, PI_TEMPORAL_DATA: process.env.PI_TEMPORAL_DATA },
    stdio: ["ignore", "pipe", "pipe"],
  });
  child.stdout.on("data", (chunk: Buffer) => {
    if (chunk.toString().includes("RAN_ON free")) ranOn.push("free");
  });
  child.stderr.on("data", (chunk: Buffer) => {
    const line = chunk.toString();
    if (/Error|error:/.test(line)) console.error("[free host]", line.split("\n")[0].slice(0, 200));
  });
  const running = [refused.run()];
  // The child has to be polling before the turn starts, or the refused host answers unopposed.
  await new Promise((resolve) => setTimeout(resolve, 20_000));

  try {
    const handle = await client.workflow.start("piSession", {
      workflowId: `${queue}-session`,
      taskQueue: queue,
      args: ["routing", sessionFile, {
        idleTimeout: "100 milliseconds",
        initialPrompt: { promptId: "routing", text: "run" },
      } as never],
    });
    // Long enough for the refusal, its backoff, and a redispatch. What is being watched is where
    // the work goes, not how fast.
    const settled = handle.result().then(() => "done", () => "failed");
    await Promise.race([settled, new Promise((r) => setTimeout(r, 45_000))]);
    await handle.terminate().catch(() => undefined);

    // The refusal is an ordinary failure, so Temporal schedules the work again and any worker can
    // take it. A permanent one would have ended the turn with the directory.
    check("the work reaches a host that is not refused", ranOn.includes("free"), ranOn);
    check("and never runs on the refused one", !ranOn.includes("refused"), ranOn);
    // What this fixture cannot say: whether the turn then finishes. Its session is in memory, so
    // nothing it records survives the activity that recorded it. A turn finishing on a worker that
    // never saw the session is `detached-check.mts`, against a real transcript.
  } finally {
    child.kill("SIGKILL");
    await rm(helper, { force: true });
    refused.shutdown();
    await Promise.allSettled(running);
    await connection.close();
    native.close();
    await rm(root, { recursive: true, force: true });
  }

  console.log(failures.length === 0 ? "quarantine-routing-check: OK" : `quarantine-routing-check: ${failures.length} failed`);
  process.exit(failures.length === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
