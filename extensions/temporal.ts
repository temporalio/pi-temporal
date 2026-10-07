// The pi extension. Each turn runs as a workflow on an in-process worker, so a turn cut by a crash
// finishes when the session reopens (PI_TEMPORAL_DURABLE_TURNS=0 turns this off).
// `/background` hands a task to a worker-owned session that keeps going after pi exits.
// The embedded worker needs the pi fork build. Set PI_TEMPORAL_EMBEDDED_WORKER=0 when a fleet
// worker owns the queue.

import type {
  ExtensionAPI,
  ExtensionContext,
  TurnExecutorContext,
} from "@earendil-works/pi-coding-agent";
import { randomUUID } from "node:crypto";
import {
  type Client,
  type Connection,
  QueryRejectedError,
  WorkflowNotFoundError,
} from "@temporalio/client";
import {
  LOCAL_TURN_WORKFLOW,
  QUERIES,
  SIGNALS,
  WORKFLOW_TYPE,
  workflowId,
} from "../src/protocol.js";
import type {
  LocalTurnInput,
  PromptInput,
  SessionTurnOptions,
  TurnState,
} from "../src/protocol.js";
import { type LiveTurns, makeLocalTurnActivities } from "../src/local-turn-activity.js";
import { createSessionWorker, type SessionWorker } from "../src/session-worker.js";
import * as worktree from "../src/worktree.js";
import { openClient } from "../src/client.js";
import {
  clientProblems,
  type Config,
  connectionOptions,
  fromEnv,
  modelApiKey,
  preflight,
} from "../src/config.js";

const STATUS_KEY = "pi-temporal";
const POLL_MS = 2000;
// Shorter than the poll interval, so a worker that never answers cannot stack polls behind it.
const QUERY_MS = 1500;

type Env = Config & {
  readonly embeddedWorker: boolean;
  readonly durableTurns: boolean;
  readonly provider?: string;
  readonly modelHint?: string;
};

// Shared settings come from `fromEnv`, so the extension and the worker agree on profile and
// session directory. The rest are extension-only.
const env = (): Env => ({
  ...fromEnv(),
  embeddedWorker: process.env.PI_TEMPORAL_EMBEDDED_WORKER !== "0",
  durableTurns: process.env.PI_TEMPORAL_DURABLE_TURNS !== "0",
  provider: process.env.PI_TEMPORAL_PROVIDER,
  modelHint: process.env.PI_MODEL,
});

interface Task {
  readonly sessionId: string;
  readonly promptId: string;
  readonly text: string;
}

export default function (pi: ExtensionAPI) {
  const cfg = env();
  // Connect lazily, so a pi that never runs a task opens no connection.
  let connecting: Promise<{ client: Client; connection: Connection }> | undefined;
  let embedding: Promise<SessionWorker> | undefined;
  const watching = new Map<string, Task>();
  let watcher: NodeJS.Timeout | undefined;
  // The turn executor is handed a turn, not a context, so it borrows the session's for messages.
  let uiCtx: ExtensionContext | undefined;
  let warnedNoTemporal = false;
  // The embedded worker hosts the same activities, so it gets the standalone worker's checks.
  const workerProblems = cfg.embeddedWorker ? preflight(cfg) : [];
  let polling = false;

  const connect = () => {
    connecting ??= openClient(fromEnv());
    return connecting;
  };

  // Building the workflow bundle takes a second, so start the worker on the first task.
  const startWorker = (ctx: ExtensionContext) => {
    embedding ??= (async () => {
      const worker = await createSessionWorker({
        address: cfg.address,
        connect: connectionOptions(fromEnv()),
        namespace: cfg.namespace,
        taskQueue: cfg.taskQueue,
        // Tools run where you are, so a background task sees the project you asked from.
        projectDir: ctx.cwd,
        provider: cfg.provider ?? ctx.model?.provider,
        modelHint: cfg.modelHint ?? ctx.model?.id,
        apiKey: modelApiKey(cfg.provider ?? ctx.model?.provider),
        shipTree: cfg.shipTree,
      });
      worker.run().catch((err) => {
        ctx.ui.notify(`background worker stopped: ${String(err)}`, "warning");
      });
      return worker;
    })();
    return embedding;
  };

  const describe = () =>
    [...watching.values()].map((t) => `${t.sessionId}: ${t.text.slice(0, 60)}`).join("\n");

  const showStatus = (ctx: ExtensionContext) => {
    if (watching.size === 0) {
      ctx.ui.setStatus(STATUS_KEY, "");
      return;
    }
    const plural = watching.size === 1 ? "" : "s";
    ctx.ui.setStatus(STATUS_KEY, `${watching.size} background task${plural} running`);
  };

  async function poll(ctx: ExtensionContext) {
    if (polling) return;
    polling = true;
    try {
      await pollOnce(ctx);
    } finally {
      polling = false;
    }
  }

  async function pollOnce(ctx: ExtensionContext) {
    const { client } = await connect();
    for (const [id, task] of [...watching]) {
      let state: TurnState;
      try {
        const handle = client.workflow.getHandle(workflowId(task.sessionId));
        state = await client.withDeadline(Date.now() + QUERY_MS, () =>
          handle.query<TurnState, []>(QUERIES.turnState),
        );
      } catch (err) {
        // Closed or gone means retired, so stop watching. Anything else is retried next tick.
        if (err instanceof QueryRejectedError || err instanceof WorkflowNotFoundError) {
          watching.delete(id);
        }
        continue;
      }
      if (state.finished?.promptId !== task.promptId) continue;

      watching.delete(id);
      const { outcome, finalText } = state.finished;
      if (outcome === "answered") {
        // Delivered as context for the next turn, so nothing is interrupted.
        await pi.sendMessage(
          {
            customType: "pi-temporal",
            content:
              `Background task "${task.text}" finished on a worker. ` +
              `It answered:\n\n${finalText}`,
            display: true,
            details: { sessionId: task.sessionId, promptId: task.promptId },
          },
          { deliverAs: "nextTurn" },
        );
        ctx.ui.notify(`background task done: ${task.sessionId}`, "info");
      } else {
        ctx.ui.notify(`background task ${outcome}: ${task.sessionId}`, "warning");
      }
    }

    showStatus(ctx);
    if (watching.size === 0 && watcher) {
      clearInterval(watcher);
      watcher = undefined;
    }
  }

  const watch = (ctx: ExtensionContext) => {
    showStatus(ctx);
    watcher ??= setInterval(() => {
      poll(ctx).catch(() => {
        // Don't bother the user. The next tick retries.
      });
    }, POLL_MS);
    watcher.unref?.();
  };

  // Each turn is wrapped in a workflow but runs here, so the queue is private to this process.
  const turnQueue = `${cfg.taskQueue}-local-${randomUUID().slice(0, 8)}`;
  const liveTurns: LiveTurns = new Map();
  let turnWorker: Promise<SessionWorker> | undefined;

  const startTurnWorker = () => {
    turnWorker ??= (async () => {
      const worker = await createSessionWorker({
        address: cfg.address,
        connect: connectionOptions(fromEnv()),
        namespace: cfg.namespace,
        taskQueue: turnQueue,
        projectDir: process.cwd(),
        activities: makeLocalTurnActivities(liveTurns),
      });
      worker.run().catch(() => {
        // The failing turn reports it. Later turns fall back to running locally.
      });
      return worker;
    })();
    return turnWorker;
  };

  const runTurnDurably = async (turn: TurnExecutorContext) => {
    const turnId = randomUUID();
    const turnWorkflow = `pi-turn-${turn.sessionId}-${turnId}`;
    const input: LocalTurnInput = {
      sessionId: turn.sessionId,
      turnId,
      taskQueue: turnQueue,
      stepped: cfg.stepped,
    };
    let ran = false;
    const ranHere = <T>(body: () => Promise<T>) => {
      ran = true;
      return body();
    };
    liveTurns.set(turnId, {
      run: () => ranHere(() => turn.run()),
      // Also marks `ran`, so the fallback below never reruns a turn that reached its model call.
      steps: {
        record: () => ranHere(() => turn.steps.record()),
        // Not wrapped. Checking for an interrupt doesn't run the turn.
        interrupted: () => turn.steps.interrupted(),
        modelCall: () => ranHere(() => turn.steps.modelCall()),
        runToolCall: (id) => ranHere(() => turn.steps.runToolCall(id)),
        sealStep: (results, options) => ranHere(() => turn.steps.sealStep(results, options)),
      },
    });

    try {
      await startTurnWorker();
      const { client } = await connect();
      await client.workflow.execute(LOCAL_TURN_WORKFLOW, {
        taskQueue: turnQueue,
        workflowId: turnWorkflow,
        args: [input],
      });
    } catch (err) {
      // The turn itself failed. That's pi's error to report, not ours to retry.
      if (ran) {
        // Stop the workflow from driving more steps of a turn that's over here.
        await connect()
          .then(({ client }) => client.workflow.getHandle(turnWorkflow).terminate("turn failed"))
          .catch(() => {});
        throw err;
      }
      // Temporal is unreachable. Run the turn as plain pi would, and warn, not fail.
      await turn.run();
      // Warn once per session, not on every turn.
      if (!warnedNoTemporal) {
        warnedNoTemporal = true;
        const where = `turns are not durable, Temporal is unreachable at ${cfg.address}`;
        uiCtx?.ui.notify(where, "warning");
      }
    } finally {
      liveTurns.delete(turnId);
    }
  };

  if (cfg.durableTurns) {
    pi.registerTurnExecutor(runTurnDurably, { resumeOnStart: true });
  }

  pi.registerCommand("background", {
    description: "Send a task to a worker that outlives pi, and bring its answer back",
    handler: async (args, ctx) => {
      const text = args.trim();
      if (!text) {
        ctx.ui.notify("usage: /background <task>", "warning");
        return;
      }
      // Refuse here, where the user sees it, not later in the worker.
      const problems = [...new Set([...clientProblems(cfg), ...workerProblems])];
      if (problems.length > 0) {
        ctx.ui.notify(`not sending the task: ${problems.join("; ")}`, "error");
        return;
      }

      const task: Task = {
        sessionId: `pi-${randomUUID().slice(0, 8)}`,
        promptId: randomUUID(),
        text,
      };
      const prompt: PromptInput = { promptId: task.promptId, text };
      const options: SessionTurnOptions = {
        idleTimeout: cfg.idleTimeout,
        stepped: cfg.stepped,
        toolTimeoutMinutes: cfg.toolTimeoutMinutes,
        budget: cfg.budget,
      };

      try {
        if (cfg.embeddedWorker) await startWorker(ctx);
        // Ship the project from this directory. Workers never seed it, since the first activity
        // could land on any of them.
        if (cfg.shipTree) {
          // Same guard as `start --project`. A home directory would ship `~/.ssh` and `~/.aws`.
          const refusal = await worktree.projectRefusal(ctx.cwd);
          if (refusal) {
            ctx.ui.notify(
              `not sending this directory as the project: ${refusal}. Run /background from it.`,
              "error",
            );
            return;
          }
          await worktree.capture(ctx.cwd, `${cfg.sessionDir}/${task.sessionId}.jsonl`, {
            seed: true,
          });
        }
        const { client } = await connect();
        await client.workflow.signalWithStart(WORKFLOW_TYPE, {
          taskQueue: cfg.taskQueue,
          workflowId: workflowId(task.sessionId),
          args: [task.sessionId, `${cfg.sessionDir}/${task.sessionId}.jsonl`, options],
          signal: SIGNALS.submitPrompt,
          signalArgs: [prompt],
        });
      } catch (err) {
        ctx.ui.notify(`could not reach Temporal at ${cfg.address}: ${String(err)}`, "error");
        return;
      }

      watching.set(task.promptId, task);
      watch(ctx);
      ctx.ui.notify(`background task started: ${task.sessionId}`, "info");
    },
  });

  pi.registerCommand("background-status", {
    description: "Show the background tasks this session is waiting on",
    handler: async (_args, ctx) => {
      const worker = embedding ? "worker: in this pi" : "worker: external";
      if (watching.size === 0) {
        ctx.ui.notify(`no background tasks running (${worker})`, "info");
        return;
      }
      ctx.ui.notify(`${describe()}\n${worker}`, "info");
    },
  });

  pi.registerCommand("background-stop", {
    description: "Interrupt the background tasks this session is waiting on",
    handler: async (_args, ctx) => {
      const { client } = await connect();
      for (const task of watching.values()) {
        try {
          await client.workflow.getHandle(workflowId(task.sessionId)).signal(SIGNALS.interrupt);
        } catch {
          // Already gone; poll() clears it.
        }
      }
      ctx.ui.notify(`interrupted ${watching.size} background task(s)`, "info");
    },
  });

  pi.on("session_start", async (_event, ctx) => {
    uiCtx = ctx;
    if (workerProblems.length > 0) {
      ctx.ui.notify(
        `pi-temporal will not run /background here: ${workerProblems.join("; ")}`,
        "warning",
      );
    }
  });

  pi.on("session_shutdown", async () => {
    if (turnWorker) {
      const worker = await turnWorker;
      turnWorker = undefined;
      await worker.stop();
    }
    if (watcher) {
      clearInterval(watcher);
      watcher = undefined;
    }
    if (embedding) {
      const worker = await embedding;
      embedding = undefined;
      // In-flight tasks aren't lost. The next worker to poll the queue picks them up.
      await worker.stop();
    }
    if (connecting) {
      const { connection } = await connecting;
      connecting = undefined;
      await connection.close();
    }
  });
}
