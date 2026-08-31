// A client for a session nobody is sitting in front of. `/background` inside pi already sends a
// task to a worker, but the commands to reach one live inside a pi session, so a task could only be
// started, watched and stopped from the terminal that happened to start it.
//
// These talk to Temporal and to the session file, never to a pi process, because there is nothing
// to talk to: a worker-owned session has no server and no port. The workflow holds the control
// state (what is queued, which step a turn is on) and the session file holds the conversation, so
// following one is a query plus a tail. Both are reachable from any machine that can reach the
// cluster and the session directory, which is what makes the session outlive its client.
//
// Usage: tsx src/cli.ts <start|running|watch|stop> ...

import { randomUUID } from "node:crypto";
import { open, stat } from "node:fs/promises";
import { connect, interrupt, submitPrompt } from "./client.js";
import { sessionFileFor } from "./config.js";
import { WORKFLOW_TYPE, WORKFLOW_ID_PREFIX, workflowId } from "./protocol.js";
import { ScheduleOverlapPolicy } from "@temporalio/client";
import type { TurnState } from "./protocol.js";
import { textOf } from "./messages.js";

const POLL_MS = 1_000;

const say = (line: string) => process.stderr.write(line + "\n");
// Anything a script is meant to read. Kept off stderr so `$(pi-temporal start ...)` is an id and
// not an id plus whatever else was worth printing to a person.
const emit = (line: string) => process.stdout.write(line + "\n");

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// A session with no live workflow is idle, not an error: the supervisor retires after its idle
// timeout, and a session that finished an hour ago is the normal case for `watch` and `running`.
//
// A query is answered by a worker, so an open session whose workers are all down does not answer at
// all. Left unbounded that turns one dead worker into a listing that hangs, so give up and say so.
const QUERY_MS = 3_000;

// Three answers, not two. "Gone" means the session is over; "unreachable" means nobody could
// answer, which is what a worker restart looks like from here. A follower that treats the second
// as the first stops in the middle of a handover and reports the turn as finished.
type Reached = { kind: "state"; state: TurnState } | { kind: "gone" } | { kind: "unreachable" };

async function turnStateOf(
  client: Awaited<ReturnType<typeof connect>>["client"],
  sessionId: string,
): Promise<Reached> {
  try {
    // The SDK's own deadline, not a race against a timer. Racing leaves the call retrying in the
    // background after we stopped waiting for it, and closing the connection under one of those
    // throws from a grpc timer, where no caller can catch it.
    const state = await client.withDeadline(Date.now() + QUERY_MS, () =>
      client.workflow.getHandle(workflowId(sessionId)).query<TurnState, []>("turnState"),
    );
    return { kind: "state", state };
  } catch (err) {
    const message = String((err as Error)?.message ?? err);
    // A workflow that is not there, or already closed, is a session that has finished. Anything
    // else (a deadline, a worker that cannot answer) must not read as "finished".
    return /not found|NOT_FOUND|already completed|workflow execution already/i.test(message)
      ? { kind: "gone" }
      : { kind: "unreachable" };
  }
}

async function start(args: string[]) {
  const text = args.find((a) => !a.startsWith("--"));
  if (!text) throw new Error('start wants a task: pi-temporal start "fix the failing test"');
  const flag = (name: string) => args.find((a) => a.startsWith(`--${name}=`))?.split("=").slice(1).join("=");
  const sessionId = flag("session") ?? `task-${randomUUID().slice(0, 8)}`;
  // signal-with-start, so this both creates the session and hands it the prompt. Nothing waits for
  // the turn: whichever worker is polling the queue runs it.
  await submitPrompt(sessionId, text);
  emit(sessionId);
  say(`  follow it with: pi-temporal watch ${sessionId}`);
}

// A task with no client at all. `start` still needs something to run it; a schedule does not, and
// the session is created by the workflow rather than by whoever asked for it.
async function schedule(args: string[]) {
  const text = args.find((a) => !a.startsWith("--"));
  const flag = (name: string) => args.find((a) => a.startsWith(`--${name}=`))?.split("=").slice(1).join("=");
  const every = flag("every");
  const cron = flag("cron");
  const id = flag("id") ?? `pi-task-${randomUUID().slice(0, 8)}`;
  if (!text) throw new Error('schedule wants a task: pi-temporal schedule "..." --every=1h');
  if (!every && !cron) throw new Error("schedule wants --every=<duration> or --cron=<expression>");

  const { cfg, client, connection } = await connect();
  try {
    await client.schedule.create({
      scheduleId: id,
      spec: cron ? { cronExpressions: [cron] } : { intervals: [{ every: every! }] },
      // A run that is still going when the next one is due keeps going, and the next one is
      // skipped. An agent task is not a metrics scrape: two of them on one repo is a bad day.
      policies: { overlap: ScheduleOverlapPolicy.SKIP },
      action: {
        type: "startWorkflow",
        workflowType: WORKFLOW_TYPE,
        taskQueue: cfg.taskQueue,
        // Named the way a session's workflow is always named, because Temporal appends the firing
        // time to it. Without this a scheduled run lands on an id that `running` and `watch` do
        // not recognise as a session, and the only sessions you could see would be the ones a
        // client started.
        workflowId: workflowId(id),
        // No session id and no file: each firing derives its own from the workflow id it is given,
        // so two runs of the same schedule are two sessions rather than one confused one.
        args: [
          "",
          "",
          {
            idleTimeout: cfg.idleTimeout,
            stepped: cfg.stepped,
            sessionDir: cfg.sessionDir,
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
      if (wf.workflowId.startsWith(WORKFLOW_ID_PREFIX)) ids.push(wf.workflowId.slice(WORKFLOW_ID_PREFIX.length));
    }
    if (ids.length === 0) {
      say("nothing running");
      return;
    }
    // Concurrently, because the slow case is a session whose workers are gone and the cost of
    // those is otherwise paid one after another by whoever asked what is running.
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

// One line per thing that happened, read off the session's own log. A tail rather than a stream
// because a worker-owned session publishes nothing: the file is where the turn is written down.
function render(entry: { message?: { role?: string; content?: unknown } }): string | undefined {
  const message = entry.message;
  if (!message?.role) return undefined;
  const text = textOf(message.content).trim();
  if (message.role === "user") return text ? `you: ${text.slice(0, 300)}` : undefined;
  if (message.role === "toolResult") return `tool result: ${text.slice(0, 200).replace(/\n+/g, " ")}`;
  if (message.role === "assistant") {
    const calls = Array.isArray(message.content)
      ? (message.content as { type?: string; name?: string }[]).filter((b) => b?.type === "toolCall")
      : [];
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
    // A compaction rewrites the whole file, and the rewrite can be the same size or bigger, so
    // watching the length alone misses it. A new inode is the part that always changes.
    if (inode !== undefined && info.ino !== inode) {
      offset = 0;
      carry = "";
    } else if (size < offset) {
      // Shorter than what we read means it is not the file we were reading either way.
      offset = 0;
      carry = "";
    }
    inode = info.ino;
    if (size === offset) return;
    const handle = await open(file, "r");
    try {
      const buffer = Buffer.alloc(size - offset);
      await handle.read(buffer, 0, buffer.length, offset);
      offset = size;
      carry += buffer.toString("utf8");
      const lines = carry.split("\n");
      // A tail can land mid-line, and half a JSON document is not a parse error worth reporting.
      carry = lines.pop() ?? "";
      for (const line of lines) {
        if (!line.trim()) continue;
        try {
          const shown = render(JSON.parse(line));
          if (shown) emit(shown);
        } catch {
          // not an entry we can read; the log is pi's, not ours to police
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
      // Nobody could answer, so keep following. This is what the window between one worker dying
      // and the next one picking the turn up looks like, and the turn is still going.
      if (reached.kind === "unreachable") {
        await sleep(POLL_MS);
        continue;
      }
      // Idle is the exit: nothing running and nothing waiting. Turn-level, so unlike a workflow
      // still being open it does not sit through the supervisor's idle timeout.
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
        say(`deleted ${id}`);
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
    default:
      say("usage: pi-temporal <start|running|watch|stop> [args]");
      say('  start "<task>" [--session=<id>]   hand a task to a worker and return');
      say("  running                          what this deployment is running");
      say("  watch <sessionId>                follow one until its turn ends");
      say("  stop <sessionId>                 interrupt the turn in flight");
    say('  schedule "<task>" --every=1h     run it on a schedule, with no client at all');
    say("  unschedule <scheduleId>          stop that schedule");
      process.exitCode = command ? 1 : 0;
  }
}

main().catch((err) => {
  say(`error: ${err instanceof Error ? err.message : String(err)}`);
  process.exit(1);
});
