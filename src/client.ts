// Client helpers to submit a prompt to a session and to interrupt one. A prompt is a
// signal-with-start: the first starts the per-session workflow, later ones join the running one.

import { randomUUID } from "node:crypto";
import { Client, Connection } from "@temporalio/client";
import { type Config, connectionOptions, fromEnv, sessionFileFor } from "./config.js";
import { WORKFLOW_TYPE, workflowId } from "./protocol.js";
import type { PromptInput, SessionTurnOptions } from "./protocol.js";

/** Every client here is built by this, so the CLI and the extension follow sessions alike. */
export async function openClient(cfg: Config = fromEnv()) {
  const connection = await Connection.connect(connectionOptions(cfg));
  const client = new Client({
    connection,
    namespace: cfg.namespace,
    // A closed workflow answers queries with its last state, so one terminated mid-turn looks busy
    // forever. Rejecting the query tells a follower the session is over.
    workflow: { queryRejectCondition: "NOT_OPEN" },
  });
  return { client, connection };
}

export async function connect() {
  const cfg = fromEnv();
  return { cfg, ...(await openClient(cfg)) };
}

export async function submitPrompt(sessionId: string, text: string, promptId = randomUUID()) {
  const { cfg, client, connection } = await connect();
  const prompt: PromptInput = { promptId, text };
  const options: SessionTurnOptions = {
    idleTimeout: cfg.idleTimeout,
    stepped: cfg.stepped,
    toolTimeoutMinutes: cfg.toolTimeoutMinutes,
    budget: cfg.budget,
  };
  try {
    await client.workflow.signalWithStart(WORKFLOW_TYPE, {
      taskQueue: cfg.taskQueue,
      workflowId: workflowId(sessionId),
      args: [sessionId, sessionFileFor(cfg.sessionDir, sessionId), options],
      signal: "submitPrompt",
      signalArgs: [prompt],
    });
  } finally {
    await connection.close();
  }
  return promptId;
}

export async function interrupt(sessionId: string) {
  const { client, connection } = await connect();
  try {
    await client.workflow.getHandle(workflowId(sessionId)).signal("interrupt");
  } finally {
    await connection.close();
  }
}
