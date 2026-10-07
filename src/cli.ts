// The `pi-temporal` CLI: start, watch, stop and schedule worker-owned sessions from any machine.
// It talks only to Temporal and the session file, never to a pi process. The workflow holds the
// control state and the file holds the conversation, so following a session is a query plus a tail.
// Usage: tsx src/cli.ts <command> ... (no command prints the list).

import { randomUUID } from "node:crypto";
import { open, stat } from "node:fs/promises";
import { connect, interrupt, submitPrompt } from "./client.js";
import {
  clientProblems,
  describe,
  fromEnv,
  notes,
  preflight,
  sessionFileFor,
} from "./config.js";
import * as worktree from "./worktree.js";
import { WORKFLOW_TYPE, WORKFLOW_ID_PREFIX, workflowId } from "./protocol.js";
import {
  QueryRejectedError,
  ScheduleOverlapPolicy,
  WorkflowNotFoundError,
} from "@temporalio/client";
import type { TurnState } from "./protocol.js";
import { textOf } from "./messages.js";

const POLL_MS = 1_000;

const say = (line: string) => process.stderr.write(line + "\n");
// Machine-readable output goes to stdout, so `$(pi-temporal start ...)` captures only the id.
const emit = (line: string) => process.stdout.write(line + "\n");

import { resolve } from "node:path";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// Queries are answered by workers, so a session whose workers are all down never answers. Bound it.
const QUERY_MS = 3_000;

// "Unreachable" is what a worker restart looks like. Treating it as "gone" would end a follower
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
    // Anything else (a deadline, a worker that cannot answer) must not read as "finished".
    return { kind: "unreachable" };
  }
}

// Send the project with the task. A no-op unless the tree ships.
async function seedProject(sessionId: string, projectFlag: string | undefined) {
  const cfg = fromEnv();
  if (!cfg.shipTree) return;
  // Never fall back to cwd. From a home directory that would ship `~/.ssh` and `~/.aws`.
  const projectDir = projectFlag ?? process.env.PI_PROJECT_DIR;
  if (!projectDir) {
    throw new Error(
      'the tree is on, so this needs the project: pi-temporal start "..." --project=/path/to/repo',
    );
  }
  const file = sessionFileFor(cfg.sessionDir, sessionId);
  // Only the first prompt sends it. Later, the workers' tip is ahead of this client's copy.
  if (await worktree.established(file)) {
    say("  the session already has its project");
    return;
  }
  await worktree.capture(projectDir, file, { seed: true });
  say(`  sent the project from ${projectDir}`);
}

// One flag's value, from `--name=value`. `slice(1).join("=")` keeps a value with its own `=` whole.
const flagOf = (args: string[], name: string) =>
  args.find((a) => a.startsWith(`--${name}=`))?.split("=").slice(1).join("=");

/** Refuse to start a session the workers would refuse, before anything is written for it. */
function refuseConflicts() {
  const problems = clientProblems(fromEnv());
  if (problems.length > 0) throw new Error(`configuration: ${problems.join("; ")}`);
}

async function start(args: string[]) {
  refuseConflicts();
  const text = args.find((a) => !a.startsWith("--"));
  if (!text) throw new Error('start wants a task: pi-temporal start "fix the failing test"');
  const sessionId = flagOf(args, "session") ?? `task-${randomUUID().slice(0, 8)}`;
  // Seed before the prompt, or the first worker to run an activity would supply the project.
  await seedProject(sessionId, flagOf(args, "project"));
  // Creates the session and delivers the prompt. Doesn't wait for the turn.
  await submitPrompt(sessionId, text);
  emit(sessionId);
  say(`  follow it with: pi-temporal watch ${sessionId}`);
}

// A recurring task with no client. Each firing creates its own session.
async function schedule(args: string[]) {
  refuseConflicts();
  const text = args.find((a) => !a.startsWith("--"));
  const every = flagOf(args, "every");
  const cron = flagOf(args, "cron");
  const id = flagOf(args, "id") ?? `pi-task-${randomUUID().slice(0, 8)}`;
  if (!text) throw new Error('schedule wants a task: pi-temporal schedule "..." --every=1h');
  if (!every && !cron) throw new Error("schedule wants --every=<duration> or --cron=<expression>");
  // No client runs at firing time, so capture the project once as a template and each firing
  // copies it. Workers never seed a project from their own directory.
  const scheduled = fromEnv();
  let template: string | undefined;
  if (scheduled.shipTree) {
    const projectDir = flagOf(args, "project") ?? process.env.PI_PROJECT_DIR;
    if (!projectDir) {
      throw new Error(
        "the tree is on, so a schedule needs the project: " +
          'pi-temporal schedule "..." --every=1h --project=/path/to/repo',
      );
    }
    template = sessionFileFor(scheduled.sessionDir, `schedule-${id}`);
    await worktree.capture(projectDir, template, { seed: true });
    // Nothing retires a template. Drop its claim, or later sessions in this directory are refused.
    await worktree.unclaim(projectDir, template);
    say(`  sent the project from ${projectDir}`);
  }

  const { cfg, client, connection } = await connect();
  try {
    await client.schedule.create({
      scheduleId: id,
      spec: cron ? { cronExpressions: [cron] } : { intervals: [{ every: every! }] },
      // Skip a firing while the last run is still going. Two agents on one repo collide.
      policies: { overlap: ScheduleOverlapPolicy.SKIP },
      action: {
        type: "startWorkflow",
        workflowType: WORKFLOW_TYPE,
        taskQueue: cfg.taskQueue,
        // Session-style id (Temporal appends the firing time), so `running` and `watch` see it.
        workflowId: workflowId(id),
        // Empty session id and file. Each firing derives its own from its workflow id.
        args: [
          "",
          "",
          {
            idleTimeout: cfg.idleTimeout,
            stepped: cfg.stepped,
            toolTimeoutMinutes: cfg.toolTimeoutMinutes,
            budget: cfg.budget,
            sessionDir: cfg.sessionDir,
            template,
            initialPrompt: { promptId: `scheduled-${id}`, text },
          },
        ],
      },
    });
    emit(id);
    say(`  every firing starts its own session; see them with: pi-temporal running`);
  } finally {
    await connection.close();
  }
}

async function running() {
  const { client, connection } = await connect();
  try {
    const ids: string[] = [];
    for await (const wf of client.workflow.list({
      query: `WorkflowType = '${WORKFLOW_TYPE}' AND ExecutionStatus = 'Running'`,
    })) {
      if (wf.workflowId.startsWith(WORKFLOW_ID_PREFIX)) {
        ids.push(wf.workflowId.slice(WORKFLOW_ID_PREFIX.length));
      }
    }
    if (ids.length === 0) {
      say("nothing running");
      return;
    }
    // Concurrent, so sessions with dead workers time out together, not one after another.
    const rows = await Promise.all(
      ids.map(async (id) => {
        const reached = await turnStateOf(client, id);
        if (reached.kind !== "state") return `${id}  ${reached.kind}`;
        const { state } = reached;
        if (state.running) return `${id}  step ${state.running.step}`;
        if (state.queued) return `${id}  ${state.queued} queued`;
        return `${id}  idle`;
      }),
    );
    for (const row of rows) emit(row);
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
  const sessionId = args.find((a) => !a.startsWith("--"));
  if (!sessionId) throw new Error("watch wants a session id");
  const { cfg, client, connection } = await connect();
  const file = sessionFileFor(cfg.sessionDir, sessionId);
  let offset = 0;
  let carry = "";
  let inode: number | undefined;

  const drain = async () => {
    let info: Awaited<ReturnType<typeof stat>>;
    try {
      info = await stat(file);
    } catch {
      return; // the first step has not written the file yet
    }
    const size = info.size;
    // Compaction rewrites the file, maybe at the same size or bigger, so track the inode too.
    // `inode` is recorded from the open handle, so a swap between `stat` and `open` isn't missed.
    if (inode !== undefined && info.ino !== inode) {
      offset = 0;
      carry = "";
    } else if (size < offset) {
      // Shorter than what we read, so it's a different file.
      offset = 0;
      carry = "";
    }
    if (size === offset) return;
    const handle = await open(file, "r");
    try {
      const opened = await handle.stat();
      if (inode !== undefined && opened.ino !== inode) {
        offset = 0;
        carry = "";
      }
      inode = opened.ino;
      const readable = opened.size - offset;
      if (readable <= 0) return;
      const buffer = Buffer.alloc(readable);
      await handle.read(buffer, 0, buffer.length, offset);
      offset = opened.size;
      carry += buffer.toString("utf8");
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

  try {
    for (;;) {
      await drain();
      const reached = await turnStateOf(client, sessionId);
      // Likely a worker handover. The turn is still going, so keep following.
      if (reached.kind === "unreachable") {
        await sleep(POLL_MS);
        continue;
      }
      // Exit when nothing is running or queued, without waiting out the workflow's idle timeout.
      const state = reached.kind === "state" ? reached.state : undefined;
      if (!state || (!state.running && state.queued === 0)) {
        await drain();
        if (state?.finished) {
          const { outcome, error } = state.finished;
          say(`  ${outcome}${error ? `: ${error}` : ""}`);
        }
        return;
      }
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
      const id = rest.find((a) => !a.startsWith("--"));
      if (!id) throw new Error("unschedule wants a schedule id");
      const { client, connection } = await connect();
      try {
        await client.schedule.getHandle(id).delete();
        // A firing can be queued before any worker copies its project.
        say(`deleted ${id}`);
        say("  kept the project template for firings already queued or running");
      } finally {
        await connection.close();
      }
      return;
    }
    case "running":
      return running();
    case "watch":
      return watch(rest);
    case "stop": {
      const sessionId = rest.find((a) => !a.startsWith("--"));
      if (!sessionId) throw new Error("stop wants a session id");
      await interrupt(sessionId);
      say(`interrupted ${sessionId}`);
      return;
    }
    // Print the resolved configuration and any problems with it.
    case "doctor": {
      const cfg = fromEnv();
      say("pi-temporal");
      for (const [name, value] of Object.entries(describe(cfg))) say(`  ${name}: ${value}`);
      for (const note of notes(cfg)) say(`  note: ${note}`);
      const problems = preflight(cfg);
      const reach = await connect()
        .then(async ({ client, connection }) => {
          try {
            await client.workflowService.getSystemInfo({});
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
      const sessionId = rest.find((a) => !a.startsWith("--"));
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
      await worktree.forget(sessionFileFor(fromEnv().sessionDir, sessionId));
      say(`dropped what ${sessionId} kept for its project files`);
      say("  the transcript is untouched; a new turn would start from an empty project");
      return;
    }
    case "release-tree": {
      // Clear the refusal a stranded tool left. We can't tell if that tool still runs, so the
      // operator decides.
      const dir = rest.find((a) => !a.startsWith("--"));
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
      say('  start "<task>" [--session=<id>]   hand a task to a worker and return');
      say("  running                          what this deployment is running");
      say("  watch <sessionId>                follow one until its turn ends");
      say("  stop <sessionId>                 interrupt the turn in flight");
      say("  forget <sessionId>               drop the project files a finished session kept");
      say("  release-tree <projectDir>        clear writers this host cannot account for");
      say("  doctor                           what this deployment resolved, and what is wrong");
      say('  schedule "<task>" --every=1h     run it on a schedule, with no client at all');
      say("  unschedule <scheduleId>          stop that schedule");
      process.exitCode = command ? 1 : 0;
  }
}

main().catch((err) => {
  say(`error: ${err instanceof Error ? err.message : String(err)}`);
  process.exit(1);
});
