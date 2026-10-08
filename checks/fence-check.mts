// Checks that the session write guard fences off a writer that was superseded. SIGSTOPs worker A
// inside a model call, lets worker B take over with a later attempt and finish the turn, then
// SIGCONTs A. Asserts A's late append is refused and the file stays exactly as B left it.
//
// Uses `faux-worker.mts` processes. Needs a Temporal server, no model key, and a minute or two.

import { type ChildProcess, spawn } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Client, Connection } from "@temporalio/client";
import { files, SCENARIO } from "./faux-worker.mjs";
import { QUERIES, type TurnState } from "../src/core/protocol.js";

const address = process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7233";
const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  const why = ok ? "" : ` (${JSON.stringify(detail)?.slice(0, 400)})`;
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${why}`);
  if (!ok) failures.push(what);
};
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const workers: ChildProcess[] = [];
function startWorker(queue: string, dir: string): ChildProcess {
  const child = spawn(
    process.execPath,
    ["--import", "tsx", "faux-worker.mts", `--queue=${queue}`, `--dir=${dir}`],
    { cwd: fileURLToPath(new URL(".", import.meta.url)), stdio: "ignore" },
  );
  workers.push(child);
  return child;
}

async function until(what: string, ok: () => boolean | Promise<boolean>, ms: number) {
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    if (await ok()) return;
    await sleep(100);
  }
  throw new Error(`timed out waiting for ${what}`);
}

const lines = (path: string) =>
  existsSync(path) ? readFileSync(path, "utf8").trim().split("\n").filter(Boolean) : [];

interface Entry {
  type: string;
  id?: string;
  parentId?: string | null;
  message?: { role?: string; content?: unknown };
}
const entriesOf = (file: string): Entry[] => lines(file).map((line) => JSON.parse(line));
const role = (r: string) => (e: Entry) => e.type === "message" && e.message?.role === r;
/** One writer leaves a chain; two leave a branch, where two entries name the same parent. */
const linear = (entries: Entry[]) =>
  entries.slice(2).every((e, i) => e.parentId === entries[i + 1].id);

async function main() {
  const connection = await Connection.connect({ address });
  const client = new Client({ connection, namespace: "default" });
  const dir = mkdtempSync(join(tmpdir(), "pi-fence-check-"));
  const queue = `pi-fence-check-${Date.now()}`;
  const file = join(dir, "sessions", "fence.jsonl");

  try {
    const a = startWorker(queue, dir);
    const handle = await client.workflow.signalWithStart("piSession", {
      workflowId: queue,
      taskQueue: queue,
      args: [{ sessionId: queue, sessionFile: file, idleTimeout: "5 seconds", stepped: true }],
      signal: "submitPrompt",
      signalArgs: [{ promptId: `${queue}-prompt`, text: `run the probe ${SCENARIO.pauseModel}` }],
    });

    await until(
      "worker A to enter its model call",
      () => existsSync(files(dir).modelPausePoint),
      60_000,
    );
    a.kill("SIGSTOP");
    const atPause = entriesOf(file);
    check(
      "worker A held the session when it stopped, with the prompt written",
      atPause.some(role("user")) && !atPause.some(role("assistant")),
      atPause.map((e) => e.message?.role ?? e.type),
    );

    const b = startWorker(queue, dir);
    let finished: TurnState["finished"];
    await until(
      "worker B to answer the turn",
      async () => {
        const state = await handle.query<TurnState, []>(QUERIES.turnState).catch(() => undefined);
        finished = state?.finished;
        return finished !== undefined;
      },
      240_000,
    );
    check(
      "worker B took the turn over and answered it",
      finished?.outcome === "answered",
      finished,
    );
    const leftByB = readFileSync(file, "utf8");

    // A's model call now ends in an append, long after its lease.
    a.kill("SIGCONT");
    await until(
      "worker A's late write to be refused",
      () => lines(files(dir).refused).length > 0,
      30_000,
    ).catch(() => undefined);
    await sleep(2_000);

    const refused = lines(files(dir).refused);
    check(
      "the write guard refused worker A's late append",
      refused.some((line) => line.startsWith(`${a.pid} `)),
      { refused, a: a.pid, b: b.pid },
    );
    for (const line of refused) console.log(`  refused: ${line}`);
    check(
      "worker B was never refused",
      !refused.some((line) => line.startsWith(`${b.pid} `)),
      refused,
    );
    const after = readFileSync(file, "utf8");
    check("the session file is exactly as worker B left it", after === leftByB);
    const entries = entriesOf(file);
    const shape = entries.map((e) => e.message?.role ?? e.type);
    check("the session is one chain", linear(entries), shape);
    check(
      "it holds B's turn: one call, one result, one answer",
      entries.filter(role("assistant")).length === 2 &&
        entries.filter(role("toolResult")).length === 1 &&
        lines(files(dir).probes).length === 1,
      { shape, probes: lines(files(dir).probes) },
    );
  } finally {
    for (const child of workers) {
      child.kill("SIGCONT");
      child.kill("SIGKILL");
    }
    await connection.close();
    if (failures.length === 0) rmSync(dir, { recursive: true, force: true });
  }

  const verdict = failures.length === 0 ? "OK" : `${failures.length} failed`;
  console.log(`fence-check: ${verdict}`);
  process.exit(failures.length === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  for (const child of workers) {
    child.kill("SIGCONT");
    child.kill("SIGKILL");
  }
  process.exit(1);
});
