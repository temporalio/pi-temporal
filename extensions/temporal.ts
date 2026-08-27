// Puts the session's turns under Temporal, and adds a way to send a task off to a worker.
//
// Every turn is durable, with nothing to type and nothing to launch: the first turn registers an
// executor and starts a worker in this process, so each turn becomes a workflow, and a turn a
// crash cut in half is finished when the session is opened again. PI_TEMPORAL_DURABLE_TURNS=0
// turns that off.
//
// /background is the other half, and a different thing: it gives a task its own session that a
// worker owns, so it carries on after pi exits. That is offloading, not durability, which is why
// it is a command rather than the default.
//
// The worker calls step(), which only the fork build has, so this needs pi to be the fork. The
// Temporal side does not, which is why /background still works on stock pi against a worker
// running elsewhere. Set PI_TEMPORAL_EMBEDDED_WORKER=0 when a fleet worker owns the queue.

import type { ExtensionAPI, ExtensionContext, TurnExecutorContext } from "@earendil-works/pi-coding-agent";
import { randomUUID } from "node:crypto";
import { Client, Connection } from "@temporalio/client";
import { LOCAL_TURN_WORKFLOW, QUERIES, SIGNALS, WORKFLOW_TYPE, workflowId } from "../src/protocol.js";
import type { LocalTurnInput, PromptInput, SessionTurnOptions, TurnState } from "../src/protocol.js";
import { type LiveTurns, makeLocalTurnActivities } from "../src/local-turn-activity.js";
import { createSessionWorker, type SessionWorker } from "../src/session-worker.js";

const STATUS_KEY = "pi-temporal";
const POLL_MS = 2000;

interface Env {
  readonly address: string;
  readonly namespace: string;
  readonly taskQueue: string;
  readonly sessionDir: string;
  readonly idleTimeout: string;
  readonly stepped: boolean;
  readonly embeddedWorker: boolean;
  readonly durableTurns: boolean;
  readonly provider?: string;
  readonly modelHint?: string;
}

// Read here rather than importing src/config.ts: that one is the worker's, and an extension has
// no business inheriting the worker's defaults for the project directory.
const env = (): Env => ({
  address: process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7233",
  namespace: process.env.TEMPORAL_NAMESPACE ?? "default",
  taskQueue: process.env.PI_TEMPORAL_TASK_QUEUE ?? "pi-session",
  sessionDir: process.env.PI_SESSION_DIR ?? `${process.env.HOME}/.pi-temporal/sessions`,
  idleTimeout: process.env.PI_SESSION_IDLE_TIMEOUT ?? "5 minutes",
  stepped: process.env.PI_TEMPORAL_STEPPED === "1",
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
  // Started on first use, not in the factory: an invocation that never runs a task should not
  // open a connection.
  let connecting: Promise<{ client: Client; connection: Connection }> | undefined;
  let embedding: Promise<SessionWorker> | undefined;
  const watching = new Map<string, Task>();
  let watcher: NodeJS.Timeout | undefined;
  // The turn executor is handed a turn, not a context, so it borrows the session's for messages.
  let uiCtx: ExtensionContext | undefined;
  let warnedNoTemporal = false;

  const connect = () => {
    connecting ??= (async () => {
      const connection = await Connection.connect({ address: cfg.address });
      return { client: new Client({ connection, namespace: cfg.namespace }), connection };
    })();
    return connecting;
  };

  // The worker takes a second to build its workflow bundle, so start it with the first task
  // rather than at startup, and let it keep polling for the rest of the session.
  const startWorker = (ctx: ExtensionContext) => {
    embedding ??= (async () => {
      const worker = await createSessionWorker({
        address: cfg.address,
        namespace: cfg.namespace,
        taskQueue: cfg.taskQueue,
        // Tools run where you are, so a background task sees the project you asked from.
        projectDir: ctx.cwd,
        provider: cfg.provider ?? ctx.model?.provider,
        modelHint: cfg.modelHint ?? ctx.model?.id,
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
    ctx.ui.setStatus(STATUS_KEY, `${watching.size} background task${watching.size === 1 ? "" : "s"} running`);
  };

  async function poll(ctx: ExtensionContext) {
    const { client } = await connect();
    for (const [id, task] of [...watching]) {
      let state: TurnState;
      try {
        state = await client.workflow.getHandle(workflowId(task.sessionId)).query<TurnState, []>(QUERIES.turnState);
      } catch {
        // The session retires when it goes idle, so a missing workflow means the turn is over
        // and its answer is in the session file. Stop watching rather than reporting a failure.
        watching.delete(id);
        continue;
      }
      if (state.finished?.promptId !== task.promptId) continue;

      watching.delete(id);
      const { outcome, finalText } = state.finished;
      if (outcome === "answered") {
        // nextTurn, so the answer is context for whatever the user asks next and nothing is
        // interrupted to deliver it.
        await pi.sendMessage(
          {
            customType: "pi-temporal",
            content: `Background task "${task.text}" finished on a worker. It answered:\n\n${finalText}`,
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
        // A server that went away is not worth interrupting the user for; the next tick retries.
      });
    }, POLL_MS);
    watcher.unref?.();
  };

  // Every turn of this session, wrapped in a workflow. The turn still runs here, so the queue is
  // this process alone: no other worker could find the session it belongs to.
  const turnQueue = `${cfg.taskQueue}-local-${randomUUID().slice(0, 8)}`;
  const liveTurns: LiveTurns = new Map();
  let turnWorker: Promise<SessionWorker> | undefined;

  const startTurnWorker = () => {
    turnWorker ??= (async () => {
      const worker = await createSessionWorker({
        address: cfg.address,
        namespace: cfg.namespace,
        taskQueue: turnQueue,
        projectDir: process.cwd(),
        activities: makeLocalTurnActivities(liveTurns),
      });
      worker.run().catch(() => {
        // Reported by the turn that fails; a dead worker means turns run locally from here on.
      });
      return worker;
    })();
    return turnWorker;
  };

  const runTurnDurably = async (turn: TurnExecutorContext) => {
    const turnId = randomUUID();
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
      // The same turn, a step at a time. Wrapped the same way, because a turn that got as far as
      // its model call is one the fallback below must not run a second time.
      steps: {
        record: () => ranHere(() => turn.steps.record()),
        modelCall: () => ranHere(() => turn.steps.modelCall()),
        runToolCall: (id) => ranHere(() => turn.steps.runToolCall(id)),
        sealStep: (results) => ranHere(() => turn.steps.sealStep(results)),
      },
    });

    try {
      await startTurnWorker();
      const { client } = await connect();
      await client.workflow.execute(LOCAL_TURN_WORKFLOW, {
        taskQueue: turnQueue,
        workflowId: `pi-turn-${turn.sessionId}-${turnId}`,
        args: [input],
      });
    } catch (err) {
      // If the turn itself failed, that is pi's error to report, not ours to retry.
      if (ran) {
        throw err;
      }
      // Durability is not worth losing a turn over. Temporal being unreachable means no record of
      // this turn, so run it the way pi would have, and say so rather than failing the turn.
      await turn.run();
      // Once. A session with no Temporal to reach would otherwise say it on every turn.
      if (!warnedNoTemporal) {
        warnedNoTemporal = true;
        uiCtx?.ui.notify(`turns are not durable, Temporal is unreachable at ${cfg.address}`, "warning");
      }
    } finally {
      liveTurns.delete(turnId);
    }
  };

  if (cfg.durableTurns) {
    pi.registerTurnExecutor(runTurnDurably, { resumeOnStart: true });
  }

  pi.registerCommand("background", {
    description: "Send a task to a worker that keeps going after pi exits, and bring the answer back",
    handler: async (args, ctx) => {
      const text = args.trim();
      if (!text) {
        ctx.ui.notify("usage: /background <task>", "warning");
        return;
      }

      const task: Task = {
        sessionId: `pi-${randomUUID().slice(0, 8)}`,
        promptId: randomUUID(),
        text,
      };
      const prompt: PromptInput = { promptId: task.promptId, text };
      const options: SessionTurnOptions = { idleTimeout: cfg.idleTimeout, stepped: cfg.stepped };

      try {
        if (cfg.embeddedWorker) await startWorker(ctx);
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
      // A task in flight is not lost: the workflow keeps it, and the next worker to poll the
      // queue picks the step up, which may be the one this pi starts next time.
      await worker.stop();
    }
    if (connecting) {
      const { connection } = await connecting;
      connecting = undefined;
      await connection.close();
    }
  });
}
