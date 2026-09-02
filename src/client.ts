// Client helpers: submit a prompt to a session (start the workflow if idle, then signal), and
// interrupt a session. The prompt is written as a signal-with-start, so a first prompt starts the
// per-session workflow and later prompts coalesce into the running one.

import { randomUUID } from "node:crypto";
import { Client, Connection } from "@temporalio/client";
import { fromEnv, sessionFileFor } from "./config.js";
import { WORKFLOW_TYPE, workflowId } from "./protocol.js";
import type { PromptInput, SessionTurnOptions } from "./protocol.js";

export async function connect() {
  const cfg = fromEnv();
  const connection = await Connection.connect({ address: cfg.address });
  const client = new Client({
    connection,
    namespace: cfg.namespace,
    // A closed workflow answers a query by default, with the state it held when it closed. A run
    // that was terminated mid-turn then reports that turn as still running, and a follower polling
    // it never stops. Rejecting the query is what turns that into "the session is over".
    workflow: { queryRejectCondition: "NOT_OPEN" },
  });
  return { cfg, client, connection };
}

export async function submitPrompt(sessionId: string, text: string, promptId = randomUUID()) {
  const { cfg, client, connection } = await connect();
  const prompt: PromptInput = { promptId, text };
  const options: SessionTurnOptions = { idleTimeout: cfg.idleTimeout, stepped: cfg.stepped };
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
