// Proves the claim a worker-owned session is supposed to make: it belongs to the deployment, not to
// the process that started it and not to the worker that happened to pick it up.
//
//   1. a client hands over a task and exits, holding nothing
//   2. worker A starts the turn, then A is killed with a tool still running
//   3. worker B, which never saw this session, finishes it
//   4. `running` and `watch` work from a process that is only ever a client, across the handover
//
// Needs a Temporal dev server, the fork build, and a model key. It is evidence rather than a unit
// test: none of this shows up inside one process.
//
// Usage: OPENAI_API_KEY=... npx tsx detached-check.mts

import { spawn, type ChildProcess } from "node:child_process";
import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)?.slice(0, 300)})`}`);
  if (!ok) failures.push(what);
};

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

const kids: ChildProcess[] = [];

// A worker as ONE process. Going through `npx` gives npx -> tsx -> node, where only the last one
// polls, so the handle points at a wrapper and killing it orphans the poller: the turn carries on
// and a check that means to prove a handover proves nothing instead. `node --import tsx` is the
// same worker with nothing in front of it.
function worker(env: NodeJS.ProcessEnv) {
  const child = spawn(process.execPath, ["--import", "tsx", "src/worker.ts"], {
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

// Every process in the worker's chain, because `npx` spawns `tsx` spawns node and only the last one
// polls. A narrower pattern matches the two wrappers and misses the poller, so killing what it
// returns orphans the very process the kill was for. Run without a shell, because a shell wrapper's
// own command line contains the pattern and would count itself.
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

// Kill a worker and wait for the OS to agree it is gone. Anything less and the next step races a
// process that is still holding the turn, which is the difference between proving a handover and
// proving nothing.
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

// Bounded, because the interesting failure of a follower is that it never returns, and a check that
// hangs reports nothing at all.
// The CLI as one process, for the same reason the worker is: a timeout that kills `npx` leaves the
// node process underneath it running and holding the pipes, so the bound never takes effect.
const cli = (args: string[]) => ({ cmd: process.execPath, args: ["--import", "tsx", "src/cli.ts", ...args] });

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
  // A side effect we can count. A second execution records no second result, because the retry
  // throws it away, so the transcript cannot tell "did not run again" from "ran again and the
  // result was dropped". This can. It is the `git push` case in one line.
  //
  // The write comes before the sleep, not after it. With the sleep first, the kill below takes the
  // whole process group and the tool never reaches the write, so the file is empty however the
  // handover goes and the count proves nothing. This way the effect has already happened when the
  // worker dies, which is the case worth asserting: the tool ran, nothing recorded it, and the
  // takeover must not run it again.
  const ranFile = join(sessions, "ran.txt");
  const env = {
    TEMPORAL_ADDRESS: process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7241",
    PI_SESSION_DIR: sessions,
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
      "src/cli.ts",
      "start",
      `Run this exact command with the bash tool: echo HANDOVER >> ${ranFile} && sleep 45. ` +
        "Then report it.",
    ],
    env,
  );
  const sessionId = started.out.trim().split("\n")[0];
  check("start returned a session id", /^task-/.test(sessionId), started.out + started.err);
  // The point of a detached start: the client is gone long before the turn is.
  check("start did not wait for the turn", Date.now() - began < 20_000, `${Date.now() - began}ms`);

  // Wait for the tool to actually be in flight, rather than for a duration. A fixed sleep plus a
  // command that takes its own time is how the kill ends up landing after the turn already
  // finished, and then the handover this is meant to prove never happens.
  const sessionLog = join(sessions, `${sessionId}.jsonl`);
  let toolInFlight = false;
  for (let i = 0; i < 60 && !toolInFlight; i++) {
    const log = await readFile(sessionLog, "utf8").catch(() => "");
    const dispatched = log.includes('"toolCall"') && !log.includes('"toolResult"');
    // And the effect has landed. Killing between the dispatch and the write leaves nothing to
    // count, and the assertion at the end would then pass for the wrong reason.
    const effected = (await readFile(ranFile, "utf8").catch(() => "")).includes("HANDOVER");
    toolInFlight = dispatched && effected;
    if (!toolInFlight) await sleep(1_000);
  }
  check("a tool is in flight before the kill", toolInFlight);

  // --- 2. kill the worker that is holding the turn
  const killedAt = Date.now();
  const died = await killWorker(workerA, workerA.pid ? [workerA.pid] : []);
  // Asked of the OS by pattern, not only of the pid we hold: the whole point of the next step is
  // that nothing of worker A is left that could finish this turn itself.
  const left = (await workerPids()).filter((pid) => !idle.includes(pid));
  check("worker A is really gone", died && left.length === 0, { died, left });

  // --- 3. a worker that never saw this session picks the step up
  worker(env);

  // Listing is not on the critical path, so it goes after the kill where its own latency cannot
  // push the kill past the end of the turn.
  const listed = await run(cli(["running"]).cmd, cli(["running"]).args, env, 90_000);
  check("running lists it", listed.out.includes(sessionId), listed.out + listed.err);

  // --- 4. follow it across the handover, from a process that is only ever a client
  const watched = await run(cli(["watch", sessionId]).cmd, cli(["watch", sessionId]).args, env, 120_000);
  check("watch returned rather than hanging", !watched.timedOut, watched.out.slice(-200));
  // Not "answered". A worker dying with a tool in flight ends the turn now, on purpose: the
  // attempt started, so nothing here can say the tool stopped, and a sixth review reproduced what
  // moving the rest of the step costs when it had not. `migration-rejoin-check.mts` is that
  // reproduction. What the turn owes the user instead is below: the results it already had, and no
  // second run of the tool.
  check("the turn ends rather than moving to another worker", /failed/.test(watched.err), watched.err.slice(-200));

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

  // The step still closes, on a worker that never ran it. That is the recovery seal: it records
  // what the step had and does not touch the project, so the results of a call that finished are
  // not thrown away with the host that ran it.
  const sealedAfterKill = entries.some(
    (e) => e.message?.role === "toolResult" && Date.parse(e.timestamp ?? "") > killedAt,
  );
  check("the results it already had are recorded anyway", sealedAfterKill, new Date(killedAt).toISOString());

  const text = (content: unknown) => JSON.stringify(content ?? "");
  const results = entries.filter((e) => e.message?.role === "toolResult").map((e) => text(e.message?.content));
  const calls = entries.filter(
    (e) =>
      e.message?.role === "assistant" &&
      Array.isArray(e.message.content) &&
      (e.message.content as { type?: string }[]).some((b) => b?.type === "toolCall"),
  );
  // The dispatch died between starting the tool and recording its result, which is exactly the case
  // a coding agent must not guess at: `sleep 45 && echo` is harmless, but `git push` is not.
  check(
    "a tool that may have run is reported unknown",
    results.length === 1 && /unknown/i.test(results[0]),
    results,
  );
  const ran = (await readFile(ranFile, "utf8").catch(() => "")).split("\n").filter(Boolean);
  // The file counts executions. One line is the effect that happened before the worker died; a
  // second is the `git push` running once per worker, which is the whole thing this level promises
  // not to do.
  check("the tool really ran once, not once per worker", ran.length === 1, ran);
  check("the model asked for the tool once", calls.length === 1, calls.length);

  console.log(failures.length === 0 ? "\ndetached-check: OK" : `\ndetached-check: ${failures.length} failed`);
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
