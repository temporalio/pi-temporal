// Checks the echo example (`examples/echo/`), the template with no Pi in it. First its README's
// commands: the example Worker and `send.ts` answer a prompt. Then a stepped turn whose Worker is
// SIGKILLed while the echo tool runs. A new Worker must finish the turn and report the tool's
// outcome as unknown, from the dispatch claim, instead of running it a second time.
//
// Needs a Temporal server, no model key, and about a minute (one heartbeat timeout).
// Usage: npx tsx checks/echo-check.mts

import { type ChildProcess, execFile, spawn } from "node:child_process";
import { existsSync, mkdtempSync, openSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { appendFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { Client, Connection } from "@temporalio/client";
import { sendPrompt } from "../src/core/client.js";
import { type Quiet, UPDATES, workflowId } from "../src/core/protocol.js";
import { startEchoWorker } from "../examples/echo/worker.js";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const root = fileURLToPath(new URL("..", import.meta.url));
const lines = (path: string) =>
  existsSync(path) ? readFileSync(path, "utf8").split("\n").filter(Boolean) : [];

// As a child: an echo Worker whose tool logs each start and waits while the hold file exists.
if (process.argv.includes("--worker")) {
  const dir = process.env.ECHO_CHECK_DIR!;
  const worker = await startEchoWorker(async (text, signal) => {
    await appendFile(join(dir, "tool-runs.log"), `${process.pid}\n`);
    while (existsSync(join(dir, "hold"))) {
      if (signal?.aborted) throw new Error("stopped");
      await sleep(100);
    }
    return text;
  });
  await worker.run();
  process.exit(0);
}

const address = process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7233";
const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  const why = ok ? "" : ` (${JSON.stringify(detail)?.slice(0, 400)})`;
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${why}`);
  if (!ok) failures.push(what);
};

async function until(what: string, ok: () => boolean, ms: number) {
  const deadline = Date.now() + ms;
  while (!ok()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await sleep(100);
  }
}

const dir = mkdtempSync(join(tmpdir(), "echo-check-"));
const run = Date.now().toString(36);
const children: ChildProcess[] = [];
const spawnNode = (args: string[], env: Record<string, string>, log: string) => {
  const out = openSync(join(dir, log), "a");
  const child = spawn(process.execPath, ["--import", "tsx", ...args], {
    cwd: root,
    env: { ...process.env, ECHO_SESSION_DIR: dir, ECHO_CHECK_DIR: dir, ...env },
    stdio: ["ignore", out, out],
  });
  children.push(child);
  return child;
};

const connection = await Connection.connect({ address });
// The same namespace the spawned Worker and send.ts read, or the kill case talks past them.
const client = new Client({ connection, namespace: process.env.TEMPORAL_NAMESPACE ?? "default" });
const sessions: string[] = [];

try {
  // The README's commands, as a user runs them.
  const plainQueue = `echo-check-${run}`;
  const plainSession = `echo-check-plain-${run}`;
  sessions.push(plainSession);
  spawnNode(["examples/echo/worker.ts"], { ECHO_TASK_QUEUE: plainQueue }, "worker-plain.log");
  const sent = await promisify(execFile)(
    process.execPath,
    ["--import", "tsx", "examples/echo/send.ts", "hello from the echo check"],
    {
      cwd: root,
      timeout: 60_000,
      env: {
        ...process.env,
        ECHO_TASK_QUEUE: plainQueue,
        ECHO_SESSION: plainSession,
        ECHO_SESSION_DIR: dir,
      },
    },
  );
  check(
    "the example Worker and send.ts answer a prompt with its echo",
    sent.stdout.includes("answered: hello from the echo check"),
    sent.stdout,
  );

  // A stepped turn whose Worker dies inside the tool.
  const queue = `echo-check-kill-${run}`;
  const sessionId = `echo-check-kill-${run}`;
  sessions.push(sessionId);
  const sessionFile = join(dir, `${sessionId}.jsonl`);
  const runs = join(dir, "tool-runs.log");
  writeFileSync(join(dir, "hold"), "");
  const a = spawnNode(["checks/echo-check.mts", "--worker"], { ECHO_TASK_QUEUE: queue }, "a.log");
  const input = { sessionId, sessionFile, stepped: true, idleTimeout: "1 minute" };
  const text = "echo me once";
  await sendPrompt(client, { taskQueue: queue, sessionId, input }, { promptId: run, text });
  await until("the echo tool to start on worker A", () => lines(runs).length > 0, 60_000);
  a.kill("SIGKILL");
  rmSync(join(dir, "hold"));
  const b = spawnNode(["checks/echo-check.mts", "--worker"], { ECHO_TASK_QUEUE: queue }, "b.log");

  const handle = client.workflow.getHandle(workflowId(sessionId));
  const quiet = await Promise.race([
    handle.executeUpdate<Quiet, []>(UPDATES.waitForQuiet),
    sleep(180_000).then(() => undefined),
  ]);
  const finished = quiet?.finished;
  check("worker B finished the turn", finished?.outcome === "answered", finished);
  check(
    "and the answer says the echo's outcome is unknown",
    finished?.finalText === `the echo's outcome is unknown: ${text}`,
    finished?.finalText,
  );
  check("the tool ran once, on worker A", lines(runs).join() === String(a.pid), {
    runs: lines(runs),
    a: a.pid,
    b: b.pid,
  });
  const results = lines(sessionFile)
    .map((line) => JSON.parse(line) as { kind: string; status?: string })
    .filter((entry) => entry.kind === "result");
  check(
    "the session holds one result for the call, and it is unknown",
    results.length === 1 && results[0].status === "unknown",
    results,
  );
} catch (err) {
  check("the check ran to the end", false, String(err));
} finally {
  for (const child of children) child.kill("SIGKILL");
  for (const id of sessions) {
    await client.workflow
      .getHandle(workflowId(id))
      .terminate("echo-check done")
      .catch(() => {});
  }
  await connection.close();
  if (failures.length === 0) rmSync(dir, { recursive: true, force: true });
  else console.log(`  logs and session files are in ${dir}`);
}

const verdict = failures.length === 0 ? "OK" : `${failures.length} failed`;
console.log(`echo-check: ${verdict}`);
process.exit(failures.length === 0 ? 0 : 1);
