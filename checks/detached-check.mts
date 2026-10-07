// Checks that a session outlives both its client and its worker. A client starts a task and
// exits, worker A is SIGKILLed with a tool in flight, and worker B finishes the turn. Asserts the
// turn is answered, the tool is reported unknown and never rerun by the harness.
//
// Needs a Temporal dev server and a model key.
//
// Usage: OPENAI_API_KEY=... npx tsx checks/detached-check.mts

import { spawn, type ChildProcess } from "node:child_process";
import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const source = (path: string) => fileURLToPath(new URL(`../src/${path}`, import.meta.url));

const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  const why = ok ? "" : ` (${JSON.stringify(detail)?.slice(0, 300)})`;
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${why}`);
  if (!ok) failures.push(what);
};

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

const kids: ChildProcess[] = [];

// One process, not `npx` -> `tsx` -> node. Killing a wrapper would orphan the poller.
function worker(env: NodeJS.ProcessEnv) {
  const child = spawn(process.execPath, ["--import", "tsx", source("worker.ts")], {
    env: { ...process.env, ...env },
    stdio: "ignore",
    detached: true,
  });
  kids.push(child);
  return child;
}

function killGroup(child: ChildProcess) {
  if (child.pid === undefined) return;
  try {
    process.kill(-child.pid, "SIGKILL");
  } catch {
    // already gone
  }
}

// Every process whose command line names the worker. Run without a shell, which would match too.
async function workerPids(): Promise<number[]> {
  const found = await run("pgrep", ["-f", "worker\\.ts"], {});
  return found.out
    .split("\n")
    .map((l) => Number(l.trim()))
    .filter((n) => Number.isInteger(n) && n > 0);
}

const alive = (pid: number) => {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
};

// Kill a worker and wait until the OS agrees it is gone, so nothing of it can finish the turn.
async function killWorker(child: ChildProcess, pids: number[]) {
  killGroup(child);
  for (const pid of pids) {
    try {
      process.kill(pid, "SIGKILL");
    } catch {
      // already gone
    }
  }
  for (let i = 0; i < 40; i++) {
    if (!pids.some(alive)) return true;
    await sleep(250);
  }
  return false;
}

// The CLI as one process too, so a timeout kill actually ends it.
const cli = (args: string[]) => ({
  cmd: process.execPath,
  args: ["--import", "tsx", source("cli.ts"), ...args],
});

function run(command: string, args: string[], env: NodeJS.ProcessEnv, timeoutMs?: number) {
  return new Promise<{ code: number; out: string; err: string; timedOut: boolean }>((resolve) => {
    const child = spawn(command, args, { env: { ...process.env, ...env } });
    let out = "";
    let err = "";
    let timedOut = false;
    const timer = timeoutMs
      ? setTimeout(() => {
          timedOut = true;
          child.kill("SIGKILL");
        }, timeoutMs)
      : undefined;
    child.stdout.on("data", (d) => (out += d));
    child.stderr.on("data", (d) => (err += d));
    child.on("close", (code) => {
      clearTimeout(timer);
      resolve({ code: code ?? 0, out, err, timedOut });
    });
  });
}

async function main() {
  if (!process.env.OPENAI_API_KEY) throw new Error("set OPENAI_API_KEY");
  const sessions = await mkdtemp(join(tmpdir(), "pi-l3-"));
  // Counts tool executions, which the transcript cannot. The write comes before the sleep, so the
  // effect has happened when the worker dies and the takeover must not repeat it.
  const ranFile = join(sessions, "ran.txt");
  const env = {
    TEMPORAL_ADDRESS: process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7233",
    PI_SESSION_DIR: sessions,
    // The real model's tools run here, not in this checkout.
    PI_PROJECT_DIR: await mkdtemp(join(tmpdir(), "pi-l3-project-")),
    PI_TEMPORAL_TASK_QUEUE: `pi-l3-${Date.now()}`,
    PI_TEMPORAL_STEPPED: "1",
  };

  const idle = await workerPids();
  const workerA = worker(env);
  await sleep(12_000);
  check("worker A is polling", workerA.pid !== undefined && alive(workerA.pid), workerA.pid);

  // --- 1. hand it over and walk away
  const began = Date.now();
  const started = await run(
    process.execPath,
    [
      "--import",
      "tsx",
      source("cli.ts"),
      "start",
      `Run this exact command with the bash tool: echo HANDOVER >> ${ranFile} && sleep 45. ` +
        "Then report it.",
    ],
    env,
  );
  const sessionId = started.out.trim().split("\n")[0];
  check("start returned a session id", /^task-/.test(sessionId), started.out + started.err);
  check("start did not wait for the turn", Date.now() - began < 20_000, `${Date.now() - began}ms`);

  // Wait for the tool to be in flight, not for a fixed time, or the kill may land after the turn.
  const sessionLog = join(sessions, `${sessionId}.jsonl`);
  let toolInFlight = false;
  for (let i = 0; i < 60 && !toolInFlight; i++) {
    const log = await readFile(sessionLog, "utf8").catch(() => "");
    const dispatched = log.includes('"toolCall"') && !log.includes('"toolResult"');
    // Also wait for the effect, or the final count would pass for the wrong reason.
    const effected = (await readFile(ranFile, "utf8").catch(() => "")).includes("HANDOVER");
    toolInFlight = dispatched && effected;
    if (!toolInFlight) await sleep(1_000);
  }
  check("a tool is in flight before the kill", toolInFlight);

  // --- 2. kill the worker that is holding the turn
  const killedAt = Date.now();
  const died = await killWorker(workerA, workerA.pid ? [workerA.pid] : []);
  // Poll by pattern until reaped. A killed process stays listed until then.
  let left = await workerPids();
  for (let i = 0; i < 40 && left.some((pid) => !idle.includes(pid)); i++) {
    await sleep(250);
    left = await workerPids();
  }
  left = left.filter((pid) => !idle.includes(pid));
  check("worker A is really gone", died && left.length === 0, { died, left });

  // --- 3. a worker that never saw this session picks the step up
  worker(env);

  // After the kill, so its latency cannot push the kill past the end of the turn.
  const listed = await run(cli(["running"]).cmd, cli(["running"]).args, env, 90_000);
  check("running lists it", listed.out.includes(sessionId), listed.out + listed.err);

  // --- 4. follow it across the handover, from a process that is only ever a client
  const watched = await run(
    cli(["watch", sessionId]).cmd,
    cli(["watch", sessionId]).args,
    env,
    300_000,
  );
  check("watch returned rather than hanging", !watched.timedOut, watched.out.slice(-200));
  // The lost step is closed, not migrated (see `migration-rejoin-check.mts`). The next step is a
  // fresh model call on any free worker.
  check(
    "the turn is answered rather than dying with the worker",
    /answered/.test(watched.err),
    watched.err.slice(-300),
  );

  const entries = (await readFile(join(sessions, `${sessionId}.jsonl`), "utf8").catch(() => ""))
    .split("\n")
    .filter(Boolean)
    .map((l) => {
      try {
        return JSON.parse(l) as {
          timestamp?: string;
          message?: { role?: string; content?: unknown };
        };
      } catch {
        return undefined;
      }
    })
    .filter((e): e is NonNullable<typeof e> => e !== undefined);

  // The recovery seal closes the step on a worker that never ran it, keeping finished results.
  const sealedAfterKill = entries.some(
    (e) => e.message?.role === "toolResult" && Date.parse(e.timestamp ?? "") > killedAt,
  );
  check(
    "the results it already had are recorded anyway",
    sealedAfterKill,
    new Date(killedAt).toISOString(),
  );

  const text = (content: unknown) => JSON.stringify(content ?? "");
  const results = entries
    .filter((e) => e.message?.role === "toolResult")
    .map((e) => text(e.message?.content));
  // Every tool call that appends to the file. The model may ask again after an unknown outcome.
  const asked = entries
    .filter((e) => e.message?.role === "assistant" && Array.isArray(e.message.content))
    .flatMap((e) => e.message!.content as { type?: string }[])
    .filter((b) => b?.type === "toolCall" && text(b).includes("HANDOVER"));
  check("a tool that may have run is reported unknown", /unknown/i.test(results[0] ?? ""), results);
  const ran = (await readFile(ranFile, "utf8").catch(() => "")).split("\n").filter(Boolean);
  // One line per model request, never one per worker. The harness must not retry the dispatch.
  check("the tool ran once for each time the model asked", ran.length === asked.length, {
    ran,
    asked: asked.length,
  });
  console.log(
    `  (the model asked for it ${asked.length} time(s) across ${results.length} result(s))`,
  );

  const verdict = failures.length === 0 ? "OK" : `${failures.length} failed`;
  console.log(`\ndetached-check: ${verdict}`);
  process.exitCode = failures.length === 0 ? 0 : 1;
}

main()
  .catch((err) => {
    console.error(err);
    process.exitCode = 1;
  })
  .finally(() => {
    for (const kid of kids) killGroup(kid);
    setTimeout(() => process.exit(process.exitCode ?? 0), 500);
  });
