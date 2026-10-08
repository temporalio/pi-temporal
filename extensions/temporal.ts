// The pi extension. Each live turn runs as a workflow on an in-process worker, which gives it a
// record in Temporal and retries. The turn stays in this process, so it can't move to another one.
// A turn cut by a crash finishes when the session reopens. PI_TEMPORAL_LIVE_TURNS=0 turns this
// off.
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
  isGrpcServiceError,
  QueryRejectedError,
  ServiceError,
  WorkflowFailedError,
  WorkflowNotFoundError,
} from "@temporalio/client";
import { TransportError } from "@temporalio/worker";
import {
  LOCAL_TURN_WORKFLOW,
  QUERIES,
  SIGNALS,
  workflowId,
} from "../src/protocol.js";
import type {
  LocalTurnInput,
  PromptInput,
  TurnState,
} from "../src/protocol.js";
import { type LiveTurns, makeLocalTurnActivities } from "../src/local-turn-activity.js";
import { createSessionWorker, type SessionWorker } from "../src/session-worker.js";
import * as worktree from "../src/tree/worktree.js";
import { openClient, sendPrompt, sessionExists } from "../src/client.js";
import {
  clientProblems,
  type Config,
  connectionOptions,
  dropFromEnv,
  fromEnv,
  modelApiKey,
  preflight,
  sessionFileFor,
  TEMPORAL_CREDENTIAL_VARS,
} from "../src/config.js";

const STATUS_KEY = "pi-temporal";
const POLL_MS = 2000;
// Shorter than the poll interval, so a worker that never answers cannot stack polls behind it.
const QUERY_MS = 1500;
// How long quitting pi waits for an embedded worker's in-flight tool. Past it the tool is
// abandoned, and recovery reports its outcome unknown rather than running it again.
const EMBEDDED_STOP = "5s";
// A bound on a live turn's workflow, so a turn on a queue nobody polls can't wait forever. A day
// is far past any real turn, so it never cuts one short.
const TURN_TIMEOUT = "24h";

// A failure to talk to Temporal at all, as opposed to an error from what we asked it to do.
const unreachable = (err: unknown) =>
  err instanceof TransportError || err instanceof ServiceError || isGrpcServiceError(err);

type Env = Config & {
  readonly embeddedWorker: boolean;
  readonly liveTurns: boolean;
  readonly provider?: string;
  readonly modelHint?: string;
};

// Shared settings come from `fromEnv`, so the extension and the worker agree on profile and
// session directory. The rest are extension-only.
const env = (): Env => ({
  ...fromEnv(),
  embeddedWorker: process.env.PI_TEMPORAL_EMBEDDED_WORKER !== "0",
  liveTurns: process.env.PI_TEMPORAL_LIVE_TURNS !== "0",
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
  // pi's tools inherit this process's env. They must not see the Temporal credentials, which are
  // read once into `cfg` above. The model keys stay, since pi itself needs them.
  dropFromEnv(TEMPORAL_CREDENTIAL_VARS);
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
    if (connecting) return connecting;
    const opening = openClient(cfg);
    // Forget a failed attempt, so the next use tries again.
    opening.catch(() => {
      if (connecting === opening) connecting = undefined;
    });
    connecting = opening;
    return opening;
  };

  // Building the workflow bundle takes a second, so start the worker on the first task.
  const startWorker = (ctx: ExtensionContext) => {
    if (embedding) return embedding;
    const starting: Promise<SessionWorker> = (async () => {
      const worker = await createSessionWorker({
        address: cfg.address,
        connect: connectionOptions(cfg),
        namespace: cfg.namespace,
        taskQueue: cfg.taskQueue,
        // Tools run where you are, so a background task sees the project you asked from.
        projectDir: ctx.cwd,
        provider: cfg.provider ?? ctx.model?.provider,
        modelHint: cfg.modelHint ?? ctx.model?.id,
        apiKey: modelApiKey(cfg.provider ?? ctx.model?.provider),
        shipTree: cfg.shipTree,
        shutdownForceTime: EMBEDDED_STOP,
      });
      worker
        .run()
        .catch((err) => {
          ctx.ui.notify(`background worker stopped: ${String(err)}`, "warning");
        })
        .finally(() => {
          // Died on its own, not stopped by `session_shutdown`. A dead worker polls nothing, so
          // forget it and the next task starts a new one.
          if (embedding !== starting) return;
          embedding = undefined;
          worker.stop().catch(() => {});
        });
      return worker;
    })();
    starting.catch(() => {
      if (embedding === starting) embedding = undefined;
    });
    embedding = starting;
    return starting;
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
    if (turnWorker) return turnWorker;
    const starting: Promise<SessionWorker> = (async () => {
      const worker = await createSessionWorker({
        address: cfg.address,
        connect: connectionOptions(cfg),
        namespace: cfg.namespace,
        taskQueue: turnQueue,
        projectDir: process.cwd(),
        activities: makeLocalTurnActivities(liveTurns),
        shutdownForceTime: EMBEDDED_STOP,
      });
      worker
        .run()
        .catch(() => {
          // The failing turn reports it.
        })
        .finally(() => {
          // Forget a dead worker, so the next turn starts one instead of waiting on a queue
          // nobody polls.
          if (turnWorker !== starting) return;
          turnWorker = undefined;
          worker.stop().catch(() => {});
        });
      return worker;
    })();
    starting.catch(() => {
      if (turnWorker === starting) turnWorker = undefined;
    });
    turnWorker = starting;
    return starting;
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
    // Set once this process runs the turn itself. A late activity must not run it too.
    let local = false;
    // The step an activity is running now, so a lost client can wait for it to finish.
    let inFlight: Promise<unknown> | undefined;
    const ranHere = <T>(body: () => Promise<T>) => {
      if (local) return Promise.reject(new Error("this turn runs without Temporal"));
      ran = true;
      const step = body();
      inFlight = step;
      return step;
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

    const terminate = (reason: string) =>
      connect()
        .then(({ client }) => client.workflow.getHandle(turnWorkflow).terminate(reason))
        .catch(() => {});

    try {
      await startTurnWorker();
      const { client } = await connect();
      await client.workflow.execute(LOCAL_TURN_WORKFLOW, {
        taskQueue: turnQueue,
        workflowId: turnWorkflow,
        args: [input],
        workflowExecutionTimeout: TURN_TIMEOUT,
      });
    } catch (err) {
      if (ran) {
        // Don't let the workflow drive more steps of a turn that's over here.
        void terminate("turn ended in pi");
        // The workflow failed, so the turn did. That's pi's error to report, not ours to retry.
        if (err instanceof WorkflowFailedError) throw err;
        // Lost the client mid-turn. The step in flight runs in this process, so let it finish
        // rather than report a turn that may still be going as failed.
        await inFlight?.catch(() => {});
        throw new Error(
          `Temporal became unreachable at ${cfg.address} mid-turn. The steps already recorded ` +
            `are kept, but the turn did not finish.`,
          { cause: err },
        );
      }
      // No step ran. Make sure none can, then run the turn as plain pi would, and warn.
      local = true;
      liveTurns.delete(turnId);
      void terminate("turn ran without Temporal");
      await turn.run();
      // Warn once per session, not on every turn.
      if (!warnedNoTemporal) {
        warnedNoTemporal = true;
        const why = err instanceof Error ? err.message : String(err);
        const where = unreachable(err)
          ? `turns run without Temporal, it is unreachable at ${cfg.address}`
          : `turns run without Temporal here: ${why}`;
        uiCtx?.ui.notify(where, "warning");
      }
    } finally {
      liveTurns.delete(turnId);
    }
  };

  if (cfg.liveTurns) {
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

      const sessionFile = sessionFileFor(cfg.sessionDir, task.sessionId);
      let seeded = false;
      try {
        if (cfg.embeddedWorker) await startWorker(ctx);
        // Before the project is claimed. A server that's down would otherwise leave a claim on a
        // session that never starts, and this directory would refuse every later task.
        const { client } = await connect();
        // Ship the project from this directory. Workers never seed it, since the first activity
        // could land on any of them.
        if (cfg.shipTree) {
          // Same guard as `start` and `schedule`. A home directory would ship `~/.ssh`.
          const refusal = await worktree.projectRefusal(ctx.cwd);
          if (refusal) {
            ctx.ui.notify(
              `not sending this directory as the project: ${refusal}. Run /background from it.`,
              "error",
            );
            return;
          }
          await worktree.capture(ctx.cwd, sessionFile, { seed: true });
          seeded = true;
        }
        await sendPrompt(client, cfg, task.sessionId, prompt);
      } catch (err) {
        const why = err instanceof Error ? err.message : String(err);
        // Only a session the server says doesn't exist is safe to drop. Otherwise it may run.
        let mayRun = false;
        if (seeded) {
          const exists = await connect()
            .then(({ client }) => sessionExists(client, task.sessionId))
            .catch(() => undefined);
          if (exists === false) await worktree.forget(sessionFile, ctx.cwd).catch(() => {});
          else mayRun = true;
        }
        ctx.ui.notify(
          (unreachable(err)
            ? `could not reach Temporal at ${cfg.address}: ${why}`
            : `could not start the background task: ${why}`) +
            (mayRun ? `. It may have started as ${task.sessionId}.` : ""),
          "error",
        );
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
    if (watcher) {
      clearInterval(watcher);
      watcher = undefined;
    }
    const workers = [turnWorker, embedding];
    turnWorker = undefined;
    embedding = undefined;
    // In-flight background tasks aren't lost. The next worker to poll the queue picks them up.
    // A worker that failed to start or stop must not keep the rest from closing.
    await Promise.allSettled(workers.map((worker) => worker?.then((w) => w.stop())));
    const opened = connecting;
    connecting = undefined;
    await opened?.then(({ connection }) => connection.close()).catch(() => {});
  });
}
