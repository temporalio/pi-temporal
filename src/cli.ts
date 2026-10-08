// The `pi-temporal` CLI starts, watches, stops and schedules Worker-owned sessions from anywhere.
// It follows a session without a running Pi process. The Workflow holds control state, and the
// session file holds the conversation. Queries and file reads expose both.
// Usage: tsx src/cli.ts <command> ... (no command prints the list).

import { randomUUID } from "node:crypto";
import { open, stat } from "node:fs/promises";
import { resolve } from "node:path";
import { StringDecoder } from "node:string_decoder";
import { connect, interrupt, sessionStart } from "./client.js";
import { sendPrompt, sessionExists } from "./core/client.js";
import { flushTracing } from "./core/tracing.js";
import {
  clientProblems,
  describe,
  fromEnv,
  notes,
  preflight,
  sessionFileFor,
} from "./config.js";
import * as worktree from "./tree/worktree.js";
import {
  SESSION_MEMO,
  SESSION_STATE_ATTRIBUTE,
  sessionIdProblem,
  UPDATES,
  WORKFLOW_TYPE,
  WORKFLOW_ID_PREFIX,
  workflowId,
} from "./core/protocol.js";
import {
  QueryRejectedError,
  ScheduleAlreadyRunning,
  ScheduleOverlapPolicy,
  WorkflowNotFoundError,
} from "@temporalio/client";
import type { Quiet, SessionInput, TurnState } from "./core/protocol.js";
import { textOf } from "./pi/messages.js";

const POLL_MS = 1_000;

const say = (line: string) => process.stderr.write(line + "\n");
// Machine-readable output goes to stdout, so `$(pi-temporal start ...)` captures only the id.
const emit = (line: string) => process.stdout.write(line + "\n");

const sleep = (ms: number) => new Promise((done) => setTimeout(done, ms));

// Queries are answered by Workers, so a session whose Workers are all down never answers. Bound it.
const QUERY_MS = 3_000;

// "Unreachable" is what a Worker restart looks like. Treating it as "gone" would end a follower
// mid-handover and report the turn as finished.
type Reached = { kind: "state"; state: TurnState } | { kind: "gone" } | { kind: "unreachable" };

async function turnStateOf(
  client: Awaited<ReturnType<typeof connect>>["client"],
  sessionId: string,
): Promise<Reached> {
  try {
    // Use the SDK deadline, not a timer race. A raced call keeps retrying, and closing the
    // connection under it throws from a grpc timer that no caller can catch.
    const state = await client.withDeadline(Date.now() + QUERY_MS, () =>
      client.workflow.getHandle(workflowId(sessionId)).query<TurnState, []>("turnState"),
    );
    return { kind: "state", state };
  } catch (err) {
    // Rejected because the run is closed (including terminated). The session is finished.
    if (err instanceof QueryRejectedError) return { kind: "gone" };
    // Retired or never started. Match on the error type, not its message text.
    if (err instanceof WorkflowNotFoundError) return { kind: "gone" };
    // Anything else (a deadline, a Worker that cannot answer) must not read as "finished".
    return { kind: "unreachable" };
  }
}

// Send the project with the task. A no-op unless the tree ships.
async function seedProject(sessionId: string, projectFlag: string | undefined) {
  const cfg = fromEnv();
  if (!cfg.shipTree) return;
  // Never fall back to cwd. From a home directory that would ship `~/.ssh` and `~/.aws`.
  const given = projectFlag ?? process.env.PI_PROJECT_DIR;
  // Absolute, since git runs in the directory and also names it as the work tree.
  const projectDir = given === undefined ? undefined : resolve(given);
  if (!projectDir) {
    throw new Error(
      'the tree is on, so this needs the project: pi-temporal start "..." --project=/path/to/repo',
    );
  }
  const file = sessionFileFor(cfg.sessionDir, sessionId);
  // Only the first prompt sends it. Later, the Workers' tip is ahead of this client's copy.
  if (await worktree.established(file)) {
    say("  the session already has its project");
    return;
  }
  await refuseProject(projectDir);
  await worktree.capture(projectDir, file, { seed: true });
  say(`  sent the project from ${projectDir}`);
  return { file, projectDir };
}

// The guard `/background` uses. From a home directory, `~/.ssh` and `~/.aws` would ship.
async function refuseProject(projectDir: string) {
  const refusal = await worktree.projectRefusal(projectDir);
  if (refusal) throw new Error(`not sending ${projectDir} as the project: ${refusal}`);
}

/**
 * Split a command's arguments into words and `--name=value` flags. An unknown flag or the
 * `--name value` form is refused, so a flag's value is never taken for the task text.
 */
function parse(command: string, args: string[], known: readonly string[] = []) {
  const flags = new Map<string, string>();
  const words: string[] = [];
  for (const arg of args) {
    if (!arg.startsWith("--")) {
      words.push(arg);
      continue;
    }
    const eq = arg.indexOf("=");
    const name = arg.slice(2, eq === -1 ? undefined : eq);
    if (!known.includes(name)) {
      const allowed = known.length ? known.map((k) => `--${k}=`).join(", ") : "none";
      throw new Error(`${command} has no flag --${name} (it takes: ${allowed})`);
    }
    if (eq === -1) throw new Error(`write --${name}=<value>, with the =`);
    flags.set(name, arg.slice(eq + 1));
  }
  return { word: words[0], flag: (name: string) => flags.get(name) };
}

/** Refuse to start a session the workers would refuse, before anything is written for it. */
function refuseConflicts() {
  const problems = clientProblems(fromEnv());
  if (problems.length > 0) throw new Error(`configuration: ${problems.join("; ")}`);
}

async function start(args: string[]) {
  const { word: text, flag } = parse("start", args, ["session", "project"]);
  refuseConflicts();
  if (!text) throw new Error('start wants a task: pi-temporal start "fix the failing test"');
  const sessionId = flag("session") ?? `task-${randomUUID().slice(0, 8)}`;
  // Reachable first. Seeding claims the project directory, and a server that's down would leave
  // that claim on a session that never starts, refusing every later one there.
  const { cfg, client, connection } = await connect();
  try {
    // Seed before the prompt, or the first worker to run an activity would supply the project.
    const seeded = await seedProject(sessionId, flag("project"));
    // Creates the session and delivers the prompt. Doesn't wait for the turn.
    const prompt = { promptId: randomUUID(), text };
    await sendPrompt(client, sessionStart(cfg, sessionId), prompt).catch(async (err: unknown) => {
      // Only a session the server says doesn't exist is safe to drop. Otherwise it may run.
      if (seeded && (await sessionExists(client, sessionId)) === false) {
        await worktree.forget(seeded.file, seeded.projectDir);
      } else {
        say(`  it may have started: pi-temporal watch ${sessionId}`);
      }
      throw err;
    });
  } finally {
    await connection.close();
  }
  emit(sessionId);
  say(`  follow it with: pi-temporal watch ${sessionId}`);
}

// A recurring task with no client. Each firing creates its own session.
async function schedule(args: string[]) {
  const { word: text, flag } = parse("schedule", args, ["every", "cron", "id", "project"]);
  refuseConflicts();
  const every = flag("every");
  const cron = flag("cron");
  const id = flag("id") ?? `pi-task-${randomUUID().slice(0, 8)}`;
  if (!text) throw new Error('schedule wants a task: pi-temporal schedule "..." --every=1h');
  if (!every && !cron) throw new Error("schedule wants --every=<duration> or --cron=<expression>");
  // Each firing's session id is this id plus the firing time, and the template is named after it
  // too. Refused here, before the schedule exists, since a firing would only fail later.
  // `unschedule` keeps the template for firings already queued, so each creation gets its own.
  // Otherwise the id could not be scheduled again.
  const templateId = `schedule-${id}-${randomUUID().slice(0, 8)}`;
  const firing = `${id}-0000-00-00T00:00:00Z`;
  const unsafe = sessionIdProblem(firing) ?? sessionIdProblem(templateId);
  if (unsafe) throw new Error(`schedule id ${JSON.stringify(id)} can't be used: ${unsafe}`);
  // No client runs at firing time, so capture the project once as a template and each firing
  // copies it. Workers never seed a project from their own directory.
  const scheduled = fromEnv();
  let projectDir: string | undefined;
  let template: string | undefined;
  if (scheduled.shipTree) {
    const given = flag("project") ?? process.env.PI_PROJECT_DIR;
    projectDir = given === undefined ? undefined : resolve(given);
    if (!projectDir) {
      throw new Error(
        "the tree is on, so a schedule needs the project: " +
          'pi-temporal schedule "..." --every=1h --project=/path/to/repo',
      );
    }
    await refuseProject(projectDir);
    template = sessionFileFor(scheduled.sessionDir, templateId);
  }

  const { cfg, client, connection } = await connect();
  try {
    // Created paused, before the template is captured. A taken id or a bad spec fails here, so
    // it can't overwrite another schedule's template or leave one behind.
    const handle = await client.schedule
      .create({
        scheduleId: id,
        spec: cron ? { cronExpressions: [cron] } : { intervals: [{ every: every! }] },
        // Skip a firing while the last run is still going. Two agents on one repo collide.
        policies: { overlap: ScheduleOverlapPolicy.SKIP },
        state: { paused: true, note: "waiting for its project" },
        action: {
          type: "startWorkflow",
          workflowType: WORKFLOW_TYPE,
          taskQueue: cfg.taskQueue,
          // Session-style id (Temporal appends the firing time), so `running` and `watch` see it.
          workflowId: workflowId(id),
          // No session id or file. Each firing derives its own from its Workflow ID.
          args: [
            {
              idleTimeout: cfg.idleTimeout,
              stepped: cfg.stepped,
              toolTimeoutMinutes: cfg.toolTimeoutMinutes,
              budget: cfg.budget,
              sessionDir: cfg.sessionDir,
              template,
              initialPrompt: { promptId: `scheduled-${id}`, text },
            } satisfies SessionInput,
          ],
        },
      })
      .catch((err) => {
        if (err instanceof ScheduleAlreadyRunning) {
          throw new Error(`a schedule ${id} already exists; pick another --id`);
        }
        throw err;
      });
    if (projectDir && template) {
      try {
        await worktree.capture(projectDir, template, { seed: true });
        // Nothing retires a template. Drop its claim, or later sessions in this directory are
        // refused.
        await worktree.unclaim(projectDir, template);
      } catch (err) {
        const deleted = await handle.delete().then(
          () => true,
          () => false,
        );
        // A template that kept its claim holds the directory for good, since nothing retires it.
        // So drop it, claim included. The schedule was still paused, so no firing needs it.
        if (deleted) await worktree.forget(template, projectDir).catch(() => {});
        throw err;
      }
      say(`  sent the project from ${projectDir}`);
    }
    await handle.unpause("project sent");
    emit(id);
    say(`  every firing starts its own session; see them with: pi-temporal running`);
  } finally {
    await connection.close();
  }
}

async function running() {
  const { client, connection } = await connect();
  try {
    // One List call. Each session keeps its state in its memo, so no session is asked directly.
    let any = false;
    for await (const wf of client.workflow.list({
      query: `WorkflowType = '${WORKFLOW_TYPE}' AND ExecutionStatus = 'Running'`,
    })) {
      if (!wf.workflowId.startsWith(WORKFLOW_ID_PREFIX)) continue;
      any = true;
      const id = wf.workflowId.slice(WORKFLOW_ID_PREFIX.length);
      const shown = wf.memo?.[SESSION_MEMO] as { state?: string; queued?: number } | undefined;
      const queued = shown?.queued ? `, ${shown.queued} queued` : "";
      emit(`${id}  ${shown?.state ?? "starting"}${queued}`);
    }
    if (!any) say("nothing running");
  } finally {
    await connection.close();
  }
}

// One line per log entry. A worker-owned session publishes nothing, so `watch` tails the file.
function render(entry: { message?: { role?: string; content?: unknown } }): string | undefined {
  const message = entry.message;
  if (!message?.role) return undefined;
  const text = textOf(message.content).trim();
  if (message.role === "user") return text ? `you: ${text.slice(0, 300)}` : undefined;
  if (message.role === "toolResult") {
    return `tool result: ${text.slice(0, 200).replace(/\n+/g, " ")}`;
  }
  if (message.role === "assistant") {
    const blocks = Array.isArray(message.content)
      ? (message.content as { type?: string; name?: string }[])
      : [];
    const calls = blocks.filter((b) => b?.type === "toolCall");
    if (calls.length) return `tool: ${calls.map((c) => c.name ?? "?").join(", ")}`;
    return text ? `said: ${text.slice(0, 400)}` : undefined;
  }
  return undefined;
}

async function watch(args: string[]) {
  const { word: sessionId, flag } = parse("watch", args, ["timeout"]);
  if (!sessionId) throw new Error("watch wants a session id");
  const limit = flag("timeout");
  const seconds = limit === undefined ? undefined : Number(limit);
  if (seconds !== undefined && !(seconds > 0)) {
    throw new Error("--timeout wants a number of seconds, e.g. --timeout=600");
  }
  const deadline = seconds === undefined ? undefined : Date.now() + seconds * 1_000;
  const { cfg, client, connection } = await connect();
  const file = sessionFileFor(cfg.sessionDir, sessionId);
  let offset = 0;
  let carry = "";
  // A read can end inside a multi-byte character. The decoder holds those bytes for the next one.
  let decoder = new StringDecoder("utf8");
  let inode: number | undefined;
  let seenFile = false;

  const restart = () => {
    offset = 0;
    carry = "";
    decoder = new StringDecoder("utf8");
  };

  const drain = async () => {
    let info: Awaited<ReturnType<typeof stat>>;
    try {
      info = await stat(file);
    } catch {
      return; // the first step has not written the file yet
    }
    seenFile = true;
    const size = info.size;
    // Compaction rewrites the file, maybe at the same size or bigger, so track the inode too.
    // `inode` is recorded from the open handle, so a swap between `stat` and `open` isn't missed.
    if (inode !== undefined && info.ino !== inode) {
      restart();
    } else if (size < offset) {
      // Shorter than what we read, so it's a different file.
      restart();
    }
    if (size === offset) return;
    const handle = await open(file, "r");
    try {
      const opened = await handle.stat();
      if (inode !== undefined && opened.ino !== inode) restart();
      inode = opened.ino;
      const readable = opened.size - offset;
      if (readable <= 0) return;
      const buffer = Buffer.alloc(readable);
      // A read can return less than asked, more so on shared storage. Advance by what came back,
      // and the next tick reads the rest.
      const { bytesRead } = await handle.read(buffer, 0, buffer.length, offset);
      offset += bytesRead;
      carry += decoder.write(buffer.subarray(0, bytesRead));
      const lines = carry.split("\n");
      // A read can end mid-line. Keep the partial line for the next tick.
      carry = lines.pop() ?? "";
      for (const line of lines) {
        if (!line.trim()) continue;
        try {
          const shown = render(JSON.parse(line));
          if (shown) emit(shown);
        } catch {
          // Not an entry we can render. The log format is pi's.
        }
      }
    } finally {
      await handle.close();
    }
  };

  // One Update waits for the session to go quiet, while the file is read for what it says. A run
  // that continues as new answers `moved`, and the next one is asked.
  let answer: { quiet?: Quiet; gone?: true } | undefined;
  let asking: Promise<void> | undefined;
  let waiting = false;
  const ask = () =>
    client.workflow
      .getHandle(workflowId(sessionId))
      .executeUpdate<Quiet, []>(UPDATES.waitForQuiet)
      .then(
        (quiet) => {
          if (!quiet.moved) answer = { quiet };
          asking = undefined;
        },
        (err: unknown) => {
          // Over, or never started. Anything else is a handover, so keep following.
          if (err instanceof WorkflowNotFoundError) answer = { gone: true };
          else if (!waiting) {
            say("  waiting for Temporal or a worker to answer");
            waiting = true;
          }
          asking = undefined;
        },
      );
  try {
    for (;;) {
      await drain();
      if (deadline !== undefined && Date.now() > deadline) {
        say(`  stopped watching after ${seconds}s; the session may still be going`);
        process.exitCode = 1;
        return;
      }
      if (answer) {
        await drain();
        const finished = answer.quiet?.finished;
        if (answer.gone && !seenFile) {
          say(`no such session: ${sessionId}`);
        } else if (finished) {
          say(`  ${finished.outcome}${finished.error ? `: ${finished.error}` : ""}`);
        } else if (answer.gone) {
          say("  the session is over; its outcome is no longer kept");
        }
        if (finished?.outcome !== "answered") process.exitCode = 1;
        return;
      }
      asking ??= ask();
      await sleep(POLL_MS);
    }
  } finally {
    await connection.close();
  }
}

async function main() {
  const [command, ...rest] = process.argv.slice(2);
  switch (command) {
    case "start":
      return start(rest);
    case "schedule":
      return schedule(rest);
    case "unschedule": {
      const id = parse(command, rest).word;
      if (!id) throw new Error("unschedule wants a schedule id");
      const { client, connection } = await connect();
      try {
        await client.schedule.getHandle(id).delete();
        say(`deleted ${id}`);
        // A firing can be queued before any worker copies its project.
        say("  kept the project template for firings already queued or running");
      } finally {
        await connection.close();
      }
      return;
    }
    case "running":
      parse(command, rest);
      return running();
    case "watch":
      return watch(rest);
    case "stop": {
      const sessionId = parse(command, rest).word;
      if (!sessionId) throw new Error("stop wants a session id");
      say(
        (await interrupt(sessionId))
          ? `interrupted ${sessionId}`
          : `${sessionId} has no turn running, so nothing was stopped`,
      );
      return;
    }
    // Print the resolved configuration and any problems with it.
    case "doctor": {
      parse(command, rest);
      const cfg = fromEnv();
      say("pi-temporal");
      for (const [name, value] of Object.entries(describe(cfg))) say(`  ${name}: ${value}`);
      for (const note of notes(cfg)) say(`  note: ${note}`);
      const problems = preflight(cfg);
      const reach = await connect()
        .then(async ({ client, connection }) => {
          try {
            await client.workflowService.getSystemInfo({});
            // An attribute the namespace doesn't have would fail every session's Workflow task.
            // A List that names it fails the same way, with no session needed.
            if (cfg.searchAttribute) {
              const query = `${SESSION_STATE_ATTRIBUTE} = 'idle'`;
              const missing = await client.workflow
                .list({ query, pageSize: 1 })
                [Symbol.asyncIterator]()
                .next()
                .then(
                  () => undefined,
                  (err: unknown) => (err instanceof Error ? err.message : String(err)),
                );
              if (missing) {
                const name = SESSION_STATE_ATTRIBUTE;
                problems.push(
                  `PI_TEMPORAL_SEARCH_ATTRIBUTE=1, but ${name} isn't usable in ${cfg.namespace}: ` +
                    `${missing}. Register it as a Keyword search attribute.`,
                );
              }
            }
            return "reached the server";
          } finally {
            await connection.close();
          }
        })
        .catch((err) => {
          const why = err instanceof Error ? err.message : String(err);
          return `could not reach ${cfg.address}: ${why}`;
        });
      say(`  server: ${reach}`);
      for (const problem of problems) say(`problem: ${problem}`);
      if (!reach.startsWith("reached")) process.exitCode = 1;
      if (problems.length > 0) process.exitCode = 1;
      else if (reach.startsWith("reached")) say("this deployment looks consistent");
      return;
    }
    // Drop a finished session's project bundles. Manual, since an idle session can be prompted
    // again and would restore from them. The transcript is kept.
    case "forget": {
      const sessionId = parse(command, rest).word;
      if (!sessionId) throw new Error("forget wants a session id");
      const { client, connection } = await connect();
      // Only when the session is known to be gone. Unreachable is not finished.
      const reached = await turnStateOf(client, sessionId).finally(() => connection.close());
      if (reached.kind !== "gone") {
        throw new Error(
          reached.kind === "state"
            ? `${sessionId} is still running; stop it first`
            : `cannot tell whether ${sessionId} is running; not touching its files`,
        );
      }
      const file = sessionFileFor(fromEnv().sessionDir, sessionId);
      if (!(await worktree.established(file))) {
        say(`${sessionId} kept no project files; nothing to drop`);
        return;
      }
      await worktree.forget(file);
      say(`dropped what ${sessionId} kept for its project files`);
      say("  the transcript is untouched; a new turn would start from an empty project");
      return;
    }
    case "release-tree": {
      // Clear the refusal a stranded tool left. We can't tell if that tool still runs, so the
      // operator decides.
      const dir = parse(command, rest).word;
      if (!dir) throw new Error("release-tree wants a project directory");
      const forgotten = await worktree.clearWriters(resolve(dir));
      if (forgotten === 0) {
        say(`${dir} was not refused; nothing to clear`);
        return;
      }
      say(`cleared ${forgotten} unaccounted writer(s) on ${dir}`);
      say("  stop anything still running in it first: this only forgets that they were there");
      return;
    }
    default:
      say("usage: pi-temporal <command> [args]");
      say('  start "<task>" [--session=<id>] [--project=<dir>]   hand a task to a worker');
      say("  running                          what this deployment is running");
      say("  watch <sessionId> [--timeout=<s>] follow one until its turn ends");
      say("  stop <sessionId>                 interrupt the turn in flight");
      say("  forget <sessionId>               drop the project files a finished session kept");
      say("  release-tree <projectDir>        clear writers this host cannot account for");
      say("  doctor                           what this deployment resolved, and what is wrong");
      say('  schedule "<task>" --every=1h     run it on a schedule, with no client at all');
      say("  unschedule <scheduleId>          stop that schedule");
      process.exitCode = command ? 1 : 0;
  }
}

main().catch(async (err) => {
  say(`error: ${err instanceof Error ? err.message : String(err)}`);
  await flushTracing();
  process.exit(1);
});
