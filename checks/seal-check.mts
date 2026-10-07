// Checks the real `sealStep` over a real `AgentSession` under Temporal, with `faux-worker.mts` as
// the worker. Asserts turn_end and agent_before_settle run once, including after a SIGKILL mid-seal
// and a retry on another worker, and that the session file stays one chain.
//
// Needs a Temporal server and no model key. The crash case takes a minute or two.

import { type ChildProcess, spawn } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Client, Connection } from "@temporalio/client";
import { files, SCENARIO } from "./faux-worker.mjs";
import { QUERIES, SIGNALS, type TurnState } from "../src/protocol.js";

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
    { cwd: fileURLToPath(new URL(".", import.meta.url)), stdio: ["ignore", "ignore", "pipe"] },
  );
  let err = "";
  child.stderr?.on("data", (chunk) => {
    err = (err + String(chunk)).slice(-4_000);
  });
  child.on("exit", (code, signal) => {
    if (code && code !== 0 && signal === null) {
      console.error(`worker ${child.pid} exited ${code}\n${err}`);
    }
  });
  workers.push(child);
  return child;
}

async function until(what: string, ok: () => boolean | Promise<boolean>, ms: number) {
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    if (await ok()) return;
    await sleep(250);
  }
  throw new Error(`timed out waiting for ${what}`);
}

const lines = (path: string) =>
  existsSync(path) ? readFileSync(path, "utf8").trim().split("\n").filter(Boolean) : [];

interface Entry {
  type: string;
  id?: string;
  parentId?: string | null;
  customType?: string;
  message?: { role?: string };
}
const entriesOf = (file: string): Entry[] => lines(file).map((line) => JSON.parse(line));
const count = (entries: Entry[], match: (e: Entry) => boolean) => entries.filter(match).length;
const custom = (type: string) => (e: Entry) => e.type === "custom" && e.customType === type;
const role = (r: string) => (e: Entry) => e.type === "message" && e.message?.role === r;
/** One writer leaves a chain; two leave a branch, where two entries name the same parent. */
const linear = (entries: Entry[]) =>
  entries.slice(2).every((e, i) => e.parentId === entries[i + 1].id);

async function main() {
  const connection = await Connection.connect({ address });
  const client = new Client({ connection, namespace: "default" });

  const scenario = (name: string) => {
    const dir = mkdtempSync(join(tmpdir(), `pi-seal-check-${name}-`));
    return {
      dir,
      queue: `pi-seal-check-${name}-${Date.now()}`,
      file: join(dir, "sessions", `${name}.jsonl`),
    };
  };

  const turn = async (
    s: ReturnType<typeof scenario>,
    text: string,
    waitMs = 60_000,
    stepped = true,
  ) => {
    const handle = await client.workflow.signalWithStart("piSession", {
      workflowId: s.queue,
      taskQueue: s.queue,
      args: [s.queue, s.file, { idleTimeout: "5 seconds", stepped }],
      signal: "submitPrompt",
      signalArgs: [{ promptId: `${s.queue}-prompt`, text }],
    });
    return {
      handle,
      finished: async () => {
        let finished: TurnState["finished"];
        await until(
          "the turn to finish",
          async () => {
            const state = await handle
              .query<TurnState, []>(QUERIES.turnState)
              .catch(() => undefined);
            finished = state?.finished;
            return finished !== undefined;
          },
          waitMs,
        );
        return finished;
      },
    };
  };

  const dirs: string[] = [];
  try {
    // The last seal ends the turn. agent_before_settle runs inside it and its entry is kept.
    {
      const s = scenario("settle");
      dirs.push(s.dir);
      startWorker(s.queue, s.dir);
      const { finished } = await turn(s, "settle the turn");
      const answer = await finished();
      check("a stepped turn with a real seal answers", answer?.outcome === "answered", answer);
      const settled = lines(files(s.dir).settled);
      check(
        "agent_before_settle ran once, inside the seal activity",
        settled.length === 1 && settled[0].endsWith(" sealStep"),
        settled,
      );
      const entries = entriesOf(s.file);
      check(
        "and what it returned is in the session file",
        count(entries, custom("check-before-settle")) === 1,
        entries.map((e) => e.customType ?? e.message?.role ?? e.type),
      );
      check("the turn_end entry is there once", count(entries, custom("check-turn-end")) === 1);
      check("the session is one chain", linear(entries));
    }

    // A seal killed after writing its entries but before returning. The retry on another worker
    // must not rerun the tool or record anything twice.
    {
      const s = scenario("kill");
      dirs.push(s.dir);
      const first = startWorker(s.queue, s.dir);
      const { finished } = await turn(s, `run the probe ${SCENARIO.killSeal}`, 240_000);
      await until(
        "the seal to reach its kill point",
        () => existsSync(files(s.dir).sealKillPoint),
        60_000,
      );
      first.kill("SIGKILL");
      const before = entriesOf(s.file);
      check(
        "the killed seal had written its results and its turn_end entries",
        count(before, role("toolResult")) === 1 && count(before, custom("check-turn-end")) === 1,
        before.map((e) => e.customType ?? e.message?.role ?? e.type),
      );
      startWorker(s.queue, s.dir);
      const answer = await finished();
      check("the retried seal answers the turn", answer?.outcome === "answered", answer);
      const entries = entriesOf(s.file);
      const shape = entries.map((e) => e.customType ?? e.message?.role ?? e.type);
      const probes = lines(files(s.dir).probes);
      check("the tool ran once", probes.length === 1, probes);
      check("its result is recorded once", count(entries, role("toolResult")) === 1, shape);
      check(
        "turn_end is dispatched once across the crash",
        count(entries, custom("check-turn-end")) === 1,
        shape,
      );
      check(
        "agent_before_settle's entry is recorded once",
        count(entries, custom("check-before-settle")) === 1,
        shape,
      );
      check("the session is one chain", linear(entries), shape);
    }

    // turn_end commits a custom_message and asks to continue, so one more step runs from it.
    {
      const s = scenario("continue");
      dirs.push(s.dir);
      startWorker(s.queue, s.dir);
      const { finished } = await turn(s, `run the probe ${SCENARIO.continueOnce}`);
      const answer = await finished();
      check("a turn that turn_end continues answers", answer?.outcome === "answered", answer);
      const entries = entriesOf(s.file);
      const shape = entries.map((e) => e.customType ?? e.message?.role ?? e.type);
      check(
        "the continuation was one more model call",
        lines(files(s.dir).modelCalls).length === 3,
        lines(files(s.dir).modelCalls),
      );
      const next = entries.findIndex((e) => e.type === "custom_message");
      check(
        "made from the custom_message turn_end committed",
        next > 0 &&
          count(entries, (e) => e.type === "custom_message") === 1 &&
          entries.slice(next + 1).some(role("assistant")),
        shape,
      );
      check(
        "agent_before_settle ran only when the turn ended",
        lines(files(s.dir).settled).length === 1,
        lines(files(s.dir).settled),
      );
    }

    // Whole-step mode runs the same seal inside `runStep`. Its boundaries must behave the same.
    {
      const s = scenario("whole");
      dirs.push(s.dir);
      startWorker(s.queue, s.dir);
      const { handle, finished } = await turn(s, "run the probe", 60_000, false);
      const answer = await finished();
      check("a whole-step turn answers", answer?.outcome === "answered", answer);
      const history = await handle.fetchHistory();
      const activities = (history.events ?? []).flatMap((e) => {
        const name = e.activityTaskScheduledEventAttributes?.activityType?.name;
        return name ? [name] : [];
      });
      check(
        "as two runStep activities: the tool call, then the answer",
        activities.length === 2 && activities.every((name) => name === "runStep"),
        activities,
      );
      const probes = lines(files(s.dir).probes);
      check("the tool ran once", probes.length === 1, probes);
      const settled = lines(files(s.dir).settled);
      check(
        "agent_before_settle ran once, inside runStep",
        settled.length === 1 && settled[0].endsWith(" runStep"),
        settled,
      );
      const entries = entriesOf(s.file);
      const shape = entries.map((e) => e.customType ?? e.message?.role ?? e.type);
      check(
        "and the seal recorded that turn_end ran",
        count(entries, custom("pi.turn-end-dispatched")) > 0 &&
          count(entries, custom("check-turn-end")) === 1,
        shape,
      );
      check("the session is one chain", linear(entries), shape);
    }

    // A stop in whole-step mode lands inside `runStep`. The running call finishes, the next one
    // never starts, and the seal records only what there is.
    {
      const s = scenario("whole-stop");
      dirs.push(s.dir);
      writeFileSync(files(s.dir).slowProbe, "");
      startWorker(s.queue, s.dir);
      const { handle, finished } = await turn(s, `run two ${SCENARIO.twoCalls}`, 60_000, false);
      await until(
        "the first call to start",
        () => lines(files(s.dir).probesStarted).length > 0,
        60_000,
      );
      await handle.signal(SIGNALS.interrupt);
      const answer = await finished();
      check(
        "a stopped whole-step turn reports the stop",
        answer?.outcome === "interrupted",
        answer,
      );
      // The activity outlives the cancellation by one call, so its seal lands later.
      await until(
        "the stopped step to be sealed",
        () => entriesOf(s.file).filter(role("toolResult")).length === 2,
        30_000,
      ).catch(() => undefined);
      const entries = entriesOf(s.file);
      const shape = entries.map((e) => e.customType ?? e.message?.role ?? e.type);
      check(
        "the call that had started finished, and the next one never started",
        lines(files(s.dir).probesStarted).length === 1 && lines(files(s.dir).probes).length === 1,
        { started: lines(files(s.dir).probesStarted), finished: lines(files(s.dir).probes) },
      );
      check(
        "the seal recorded both calls, the one that never ran as an unknown outcome",
        count(entries, role("toolResult")) === 2,
        shape,
      );
      check(
        "and nothing ran after it: no second model call, no agent_before_settle",
        lines(files(s.dir).modelCalls).length === 1 && lines(files(s.dir).settled).length === 0,
        { modelCalls: lines(files(s.dir).modelCalls), settled: lines(files(s.dir).settled) },
      );
      check("the session is one chain", linear(entries), shape);
    }
  } finally {
    for (const child of workers) child.kill("SIGKILL");
    await connection.close();
    if (failures.length === 0) {
      for (const dir of dirs) rmSync(dir, { recursive: true, force: true });
    }
  }

  const verdict = failures.length === 0 ? "OK" : `${failures.length} failed`;
  console.log(`seal-check: ${verdict}`);
  process.exit(failures.length === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  for (const child of workers) child.kill("SIGKILL");
  process.exit(1);
});
