// The in-process Worker gives live Pi turns a Temporal record and retries. A live turn stays
// in this process and resumes from its transcript when the session reopens after a crash.
// Set `PI_TEMPORAL_LIVE_TURNS=0` to disable live turns.
//
// `/background` tasks survive Pi exit because a Worker owns their sessions. The embedded Worker
// needs the Pi fork build. It's off by default in the `fleet` profile, where the fleet's Workers
// own the queue. `PI_TEMPORAL_EMBEDDED_WORKER` set on or off overrides that.

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
} from "../src/core/protocol.js";
import type {
  InterruptInput,
  LocalTurnInput,
  PromptInput,
  TurnState,
} from "../src/core/protocol.js";
import { type LiveTurns, makeLocalTurnActivities } from "../src/pi/local-turn-activity.js";
import { createSessionWorker, type SessionWorker } from "../src/core/session-worker.js";
import { makeActivities } from "../src/pi/activities.js";
import { dataConverterFor } from "../src/core/codec.js";
import { startTracing } from "../src/core/tracing.js";
import * as worktree from "../src/tree/worktree.js";
import { sendPrompt, sessionExists } from "../src/core/client.js";
import { openClient, sessionStart } from "../src/client.js";
import {
  clientProblems,
  type Config,
  connectionOptions,
  dropFromEnv,
  fromEnv,
  modelApiKey,
  onOff,
  preflight,
  sessionFileFor,
  TEMPORAL_CREDENTIAL_VARS,
} from "../src/config.js";
import { siblingModule } from "../src/core/paths.js";

const STATUS_KEY = "pi-temporal";
// Pi's live-turn Workflow beside the core session, for both Workers this extension runs.
const WORKFLOWS = siblingModule(import.meta.url, "../src/workflow-bundle");
const POLL_MS = 2000;
// Shorter than the poll interval, so a Worker that never answers cannot stack polls behind it.
const QUERY_MS = 1500;
// How long quitting pi waits for an embedded Worker's in-flight tool. Past it the tool is
// abandoned, and recovery reports its outcome unknown rather than running it again.
const EMBEDDED_STOP = "5s";
// A bound on a live turn's Workflow, so a turn on a queue nobody polls can't wait forever. A day
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

// Shared settings come from `fromEnv`, so the extension and the Worker agree on profile and
// session directory. The rest are extension-only.
const env = (): Env => {
  const shared = fromEnv();
  return {
    ...shared,
    // Off by default in the fleet profile. On the fleet's queue, this pi would take other
    // sessions' work and run it with the user's key in the user's directory. Read like every
    // other switch, so `false` means off and a typo is refused.
    embeddedWorker: onOff("PI_TEMPORAL_EMBEDDED_WORKER", shared.profile !== "fleet"),
    liveTurns: onOff("PI_TEMPORAL_LIVE_TURNS", true),
    provider: process.env.PI_TEMPORAL_PROVIDER,
    modelHint: process.env.PI_MODEL,
  };
};

interface Task {
  readonly sessionId: string;
  readonly promptId: string;
  readonly text: string;
}

export default function (pi: ExtensionAPI) {
  const cfg = env();
  // Keeps the Temporal credentials, read once into `cfg` above, out of every tool's environment.
  // The model keys stay, since pi itself needs them. Tools run as you, so they could read either
  // from this process anyway. This only stops them being handed over by default.
  dropFromEnv(TEMPORAL_CREDENTIAL_VARS);
  // Connect lazily, so a pi that never runs a task opens no connection.
  let connecting: Promise<{ client: Client; connection: Connection }> | undefined;
  let embedding: Promise<SessionWorker> | undefined;
  const watching = new Map<string, Task>();
  let watcher: NodeJS.Timeout | undefined;
  // The turn executor is handed a turn, not a context, so it borrows the session's for messages.
  let uiCtx: ExtensionContext | undefined;
  let warnedNoTemporal = false;
  // The embedded Worker hosts the same Activities, so it gets the standalone Worker's checks.
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

  // Deferring the bundle build keeps it out of Pi's startup path.
  const startWorker = (ctx: ExtensionContext) => {
    if (embedding) return embedding;
    const starting: Promise<SessionWorker> = (async () => {
      const worker = await createSessionWorker({
        address: cfg.address,
        connect: connectionOptions(cfg),
        namespace: cfg.namespace,
        taskQueue: cfg.taskQueue,
        // Only when Workers on other hosts have their own copy of the project.
        ...(cfg.shipTree ? { hostQueueFor: ctx.cwd } : {}),
        workflowsPath: WORKFLOWS,
        activities: (hostQueue) =>
          makeActivities({
            // Tools run where you are, so a background task sees the project you asked from.
            projectDir: ctx.cwd,
            provider: cfg.provider ?? ctx.model?.provider,
            modelHint: cfg.modelHint ?? ctx.model?.id,
            apiKey: modelApiKey(cfg.provider ?? ctx.model?.provider),
            shipTree: cfg.shipTree,
            hostQueue,
            sessionRoot: cfg.sessionDir,
          }),
        shutdownForceTime: EMBEDDED_STOP,
        maxConcurrentActivities: cfg.maxActivities,
        dataConverter: dataConverterFor(cfg),
        tracing: cfg.tracing ? startTracing("pi") : undefined,
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
        // Only this process's live turns. The queue is already this process's own.
        activities: () => makeLocalTurnActivities(liveTurns),
        workflowsPath: WORKFLOWS,
        shutdownForceTime: EMBEDDED_STOP,
        dataConverter: dataConverterFor(cfg),
        // The same tracing as the client that starts each turn, so a foreground prompt's trace
        // goes on into its Workflow and Activities.
        tracing: cfg.tracing ? startTracing("pi") : undefined,
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
        // The options carry the Activity's abort signal, so a stop reaches the running model call
        // or tool.
        modelCall: (options) => ranHere(() => turn.steps.modelCall(options)),
        runToolCall: (id, options) => ranHere(() => turn.steps.runToolCall(id, options)),
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
        await sendPrompt(client, sessionStart(cfg, task.sessionId), prompt);
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
          // Names the task's turn, so a stop can't reach a later prompt in the same session.
          const target: InterruptInput = { promptId: task.promptId };
          await client.workflow
            .getHandle(workflowId(task.sessionId))
            .signal(SIGNALS.interrupt, target);
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
