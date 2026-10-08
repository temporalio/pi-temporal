// Checks that the workflow enforces turn and session budgets between steps. Stub activities report
// spend, and asserts each bound stops the turn after the crossing step, `hardSeconds` stops it
// mid-step, and a finished turn's deadline never fires into the next turn.
//
// Needs a Temporal server, no model key. Usage: npx tsx checks/budget-check.mts

import { randomUUID } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { Client, Connection } from "@temporalio/client";
import { Context } from "@temporalio/activity";
import { NativeConnection, Worker } from "@temporalio/worker";
import { QUERIES } from "../src/core/protocol.js";
import type {
  ModelCallResult,
  RetireInput,
  RunStepInput,
  RunStepResult,
  ToolCallInput,
  ToolCallResult,
  SessionTurnOptions,
  TurnState,
} from "../src/core/protocol.js";

const address = process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7233";
const queue = `pi-budget-${Date.now()}`;

const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

/** One step, as expensive and as slow as the case wants, and never finished unless it says so. */
const steps = new Map<string, number>();
// Billed totals per session file. Like the session file, this outlives a workflow run.
const recorded = new Map<string, number>();
// Turn time per session file, as the activities keep it. Each step writes its turn's total, and a
// read skips the asking turn's own entries.
const recordedSeconds = new Map<string, { turn: string; seconds: number }[]>();
let tokensPerStep = 0;

// Waits like a real activity: it heartbeats, so a stop reaches it, and it ends when stopped.
async function stoppable(ms: number) {
  const ctx = Context.current();
  const beat = setInterval(() => ctx.heartbeat(), 200);
  try {
    await ctx.sleep(ms);
  } finally {
    clearInterval(beat);
  }
}

let stepsWriteLate = false;
const toolsRun: string[] = [];
let secondsPerStep = 0;
let answerAfter = Number.POSITIVE_INFINITY;
// Holds a step until this wall-clock time, for the stale-deadline case.
let gateUntil = 0;

const activities = {
  async runStep(input: RunStepInput): Promise<RunStepResult> {
    const taken = (steps.get(input.promptId) ?? 0) + 1;
    steps.set(input.promptId, taken);
    if (secondsPerStep) await stoppable(secondsPerStep * 1000);
    while (Date.now() < gateUntil) await sleep(100);
    const billed = (recorded.get(input.sessionFile) ?? 0) + tokensPerStep;
    recorded.set(input.sessionFile, billed);
    const entries = recordedSeconds.get(input.sessionFile) ?? [];
    const secondsBefore = entries.filter((e) => e.turn !== input.promptId).at(-1)?.seconds;
    if (input.sessionSeconds !== undefined) {
      // A step writes before its turn is over, so some of the turn's time is never in it.
      const seconds = input.sessionSeconds + (stepsWriteLate ? 0 : secondsPerStep);
      entries.push({ turn: input.promptId, seconds });
      recordedSeconds.set(input.sessionFile, entries);
    }
    return {
      ...(secondsBefore === undefined ? {} : { sessionSeconds: secondsBefore }),
      done: taken >= answerAfter,
      finalText: taken >= answerAfter ? "answered" : "",
      spent: { tokens: tokensPerStep, cost: tokensPerStep / 1000 },
      total: { tokens: billed, cost: billed / 1000 },
    };
  },
  // The stepped path. One model call asks for two tools that must run one after the other.
  async runModelCall(input: RunStepInput): Promise<ModelCallResult> {
    const billed = (recorded.get(input.sessionFile) ?? 0) + tokensPerStep;
    recorded.set(input.sessionFile, billed);
    return {
      calls: [{ id: "first", name: "bash" }, { id: "second", name: "bash" }],
      sequential: true,
      ended: false,
      spent: { tokens: tokensPerStep, cost: tokensPerStep / 1000 },
      total: { tokens: billed, cost: billed / 1000 },
    };
  },
  async runToolCall(input: ToolCallInput): Promise<ToolCallResult> {
    toolsRun.push(input.call.id);
    return { outcome: "settled" };
  },
  async sealStep(): Promise<RunStepResult> {
    return { done: true, finalText: "answered" };
  },
  // Like the real activity, keeps the Workflow's own total of the session's time.
  async retireSession(input: RetireInput) {
    if (input.turn === undefined || input.sessionSeconds === undefined) return;
    const entries = recordedSeconds.get(input.sessionFile) ?? [];
    entries.push({ turn: input.turn, seconds: input.sessionSeconds });
    recordedSeconds.set(input.sessionFile, entries);
  },
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
    workflowsPath: fileURLToPath(new URL("../src/workflow-bundle.ts", import.meta.url)),
    activities,
    maxHeartbeatThrottleInterval: "1 second",
  });
  const running = worker.run();

  // No `session` means a fresh session per call. `run` picks the workflow id, so two turns of one
  // session can share a run or use two.
  const turn = async (
    budget: SessionTurnOptions["budget"],
    session?: string,
    run = session,
    stepped = false,
    idle = session ? "60 seconds" : "100 milliseconds",
  ) => {
    const promptId = randomUUID();
    const began = Date.now();
    const handle = await client.workflow.signalWithStart("piSession", {
      workflowId: run ?? `${queue}-${promptId}`,
      taskQueue: queue,
      args: [session ?? promptId, session ?? `/unused/${promptId}.jsonl`, {
        idleTimeout: idle,
        budget,
        stepped,
      } satisfies SessionTurnOptions as never],
      signal: "submitPrompt",
      signalArgs: [{ promptId, text: "run" }],
    });
    const deadline = Date.now() + 60_000;
    let finished: TurnState["finished"];
    while (Date.now() < deadline && !finished) {
      finished = (
        await handle.query<TurnState, []>(QUERIES.turnState).catch(() => undefined)
      )?.finished;
      // Ignore the previous turn's answer until this one lands.
      if (finished?.promptId !== promptId) finished = undefined;
      if (!finished) await sleep(200);
    }
    if (!session) await handle.terminate().catch(() => undefined);
    return { finished, taken: steps.get(promptId) ?? 0, ran: Date.now() - began };
  };

  try {
    // The bound is checked after each step, so 3 x 100 crosses 250 on the third.
    tokensPerStep = 100;
    const spent = await turn({ tokens: 250 });
    check(
      "a turn that runs out of tokens is stopped where it got to",
      spent.finished?.outcome === "budget",
      spent.finished,
    );
    check("after the step that crossed the bound, not before it", spent.taken === 3, spent.taken);

    // A model call that spends past the bound stops the tools after the first. The workflow
    // counts the step only once it returns, so the check must see the model call's own spend.
    tokensPerStep = 300;
    const sequential = await turn({ tokens: 250 }, undefined, undefined, true);
    check(
      "a model call over the bound stops its sequential tools after the first",
      JSON.stringify(toolsRun) === '["first"]',
      { toolsRun, finished: sequential.finished },
    );
    tokensPerStep = 100;

    answerAfter = 2;
    const inside = await turn({ tokens: 250 });
    check(
      "a turn inside its bound answers as usual",
      inside.finished?.outcome === "answered",
      inside.finished,
    );
    check("and runs every step it needed", inside.taken === 2, inside.taken);

    // Cheap turns pass every per-turn bound, so the session needs its own.
    answerAfter = 2;
    tokensPerStep = 100;
    const session = `${queue}-session-budget`;
    const first = await turn({ tokens: 10_000, sessionTokens: 250 }, session);
    check(
      "a turn inside both bounds answers",
      first.finished?.outcome === "answered",
      first.finished,
    );
    check(
      "and the session's spend is reported with it",
      (first.finished?.spent?.tokens ?? 0) === 200,
      first.finished?.spent,
    );
    // 200 already spent, so this turn crosses 250 on its first step.
    const second = await turn({ tokens: 10_000, sessionTokens: 250 }, session);
    check(
      "a later turn of that session is stopped by what the session has spent",
      second.finished?.outcome === "budget",
      second.finished,
    );
    check(
      "and the session's total is what it was measured against",
      (second.finished?.spent?.tokens ?? 0) >= 250,
      second.finished?.spent,
    );

    // A new run of the same session starts with no count, so the reported total must bound it.
    answerAfter = 2;
    const woken = `${queue}-woken-session`;
    const firstRun = await turn({ sessionTokens: 250 }, woken, `${woken}-run-1`);
    check(
      "a turn of a fresh session answers",
      firstRun.finished?.outcome === "answered",
      firstRun.finished,
    );
    const secondRun = await turn({ sessionTokens: 250 }, woken, `${woken}-run-2`);
    check(
      "and a later run of that session is bounded by what the record says it spent",
      secondRun.finished?.outcome === "budget",
      secondRun.finished,
    );

    // The same for time. A run woken after an idle exit has no count of its own, so the earlier
    // turns' time must come back from the record.
    answerAfter = 2;
    tokensPerStep = 0;
    secondsPerStep = 1;
    const slow = `${queue}-woken-slow-session`;
    const slowFirst = await turn({ sessionSeconds: 2.5 }, slow, `${slow}-run-1`);
    check(
      "a turn of a fresh session inside its time answers",
      slowFirst.finished?.outcome === "answered",
      slowFirst.finished,
    );
    const slowSecond = await turn({ sessionSeconds: 2.5 }, slow, `${slow}-run-2`);
    check(
      "and a later run is bounded by the time the record says the session took",
      slowSecond.finished?.outcome === "budget" && slowSecond.taken === 1,
      slowSecond,
    );

    // A turn that never answered still took time. Its steps wrote it as they went.
    answerAfter = Number.POSITIVE_INFINITY;
    const stopped = `${queue}-woken-stopped-session`;
    const stoppedFirst = await turn({ seconds: 2, sessionSeconds: 100 }, stopped, `${stopped}-1`);
    check(
      "a turn stopped by its own time bound",
      stoppedFirst.finished?.outcome === "budget",
      stoppedFirst.finished,
    );
    answerAfter = 2;
    const stoppedSecond = await turn({ sessionSeconds: 2.5 }, stopped, `${stopped}-2`);
    check(
      "still counts toward the session in a later run",
      stoppedSecond.finished?.outcome === "budget" && stoppedSecond.taken === 1,
      stoppedSecond,
    );

    // The steps' own writes miss the end of a turn. The run that exits idle writes the Workflow's
    // total, so the next run still sees the whole turn.
    answerAfter = 1;
    secondsPerStep = 1;
    stepsWriteLate = true;
    const idled = `${queue}-idled-session`;
    await turn({ sessionSeconds: 100 }, idled, `${idled}-1`, false, "1 second");
    await client.workflow.getHandle(`${idled}-1`).result();
    answerAfter = 2;
    const afterIdle = await turn({ sessionSeconds: 1.5 }, idled, `${idled}-2`);
    check(
      "a run that exits idle leaves the session's whole time for the next run",
      afterIdle.finished?.outcome === "budget" && afterIdle.taken === 1,
      afterIdle,
    );
    stepsWriteLate = false;

    // `hardSeconds` is the only bound that interrupts a running step, like a user stop.
    answerAfter = Number.POSITIVE_INFINITY;
    tokensPerStep = 0;
    secondsPerStep = 5;
    const hard = await turn({ hardSeconds: 2 });
    check(
      "a turn past its deadline is stopped where it is",
      hard.finished?.outcome === "budget",
      hard.finished,
    );
    check(
      "inside the step that was running, not after it",
      hard.ran < 5_000,
      { ran: hard.ran, steps: hard.taken },
    );
    secondsPerStep = 1;

    // Soft wall clock: the step that crosses it is the last one.
    answerAfter = Number.POSITIVE_INFINITY;
    tokensPerStep = 0;
    secondsPerStep = 1;
    const late = await turn({ seconds: 2 });
    check(
      "a turn that runs out of time is stopped too",
      late.finished?.outcome === "budget",
      late.finished,
    );
    // The step count depends on scheduling cost. Only check it did not stop early.
    check("and not before it has spent it", late.taken >= 2 && late.ran >= 2_000, late);

    // A deadline must die with its turn. A scope's timers outlive its function returning, so the
    // second turn is held past the first one's deadline and must still answer.
    answerAfter = 1;
    tokensPerStep = 0;
    secondsPerStep = 0;
    const survivor = `${queue}-stale-deadline`;
    const t0 = Date.now();
    const early = await turn({ hardSeconds: 5 }, survivor);
    check(
      "a turn well inside its deadline answers",
      early.finished?.outcome === "answered",
      early.finished,
    );
    await sleep(2_000);
    gateUntil = t0 + 5_500;
    const next = await turn({ hardSeconds: 5 }, survivor);
    gateUntil = 0;
    check(
      "a deadline does not fire into the turn after it",
      next.finished?.outcome === "answered",
      next.finished,
    );
  } finally {
    const kept = [
      `${queue}-session-budget`,
      `${queue}-woken-session-run-1`,
      `${queue}-woken-session-run-2`,
      `${queue}-stale-deadline`,
    ];
    for (const id of kept) {
      await client.workflow.getHandle(id).terminate().catch(() => undefined);
    }
    worker.shutdown();
    await running.catch(() => undefined);
    await connection.close();
    native.close();
  }

  console.log(
    failures.length === 0
      ? "budget-check: OK"
      : `budget-check: ${failures.length} failed`,
  );
  process.exit(failures.length === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
