// Pi's client side: a client and a session start built from the environment (`config.ts`), over
// the config-free helpers in `core/client.ts`.

import { randomUUID } from "node:crypto";
import { Client, Connection } from "@temporalio/client";
import { type Config, connectionOptions, fromEnv, sessionFileFor } from "./config.js";
import { interruptSession, type SessionStart, sendPrompt } from "./core/client.js";
import { dataConverterFor } from "./core/codec.js";
import type { SessionTurnOptions } from "./core/protocol.js";
import { clientTracing, startTracing } from "./core/tracing.js";

const withCodec = (cfg: Config) => {
  const dataConverter = dataConverterFor(cfg);
  return dataConverter ? { dataConverter } : {};
};

/** Every client here is built by this, so the CLI and the extension follow sessions alike. */
export async function openClient(cfg: Config = fromEnv()) {
  if (cfg.tracing) startTracing("pi-temporal-client");
  const connection = await Connection.connect(connectionOptions(cfg));
  const client = new Client({
    connection,
    namespace: cfg.namespace,
    // A closed Workflow answers queries with its last state, so one terminated mid-turn looks busy
    // forever. Rejecting the query tells a follower the session is over.
    workflow: { queryRejectCondition: "NOT_OPEN" },
    ...withCodec(cfg),
    // Starts each trace here, so a prompt's spans in the Worker hang under the call that sent it.
    ...(cfg.tracing ? { interceptors: clientTracing() } : {}),
  });
  return { client, connection };
}

export async function connect() {
  const cfg = fromEnv();
  return { cfg, ...(await openClient(cfg)) };
}

/** What a session started from this config runs with. */
export const sessionOptions = (cfg: Config): SessionTurnOptions => ({
  idleTimeout: cfg.idleTimeout,
  stepped: cfg.stepped,
  toolTimeoutMinutes: cfg.toolTimeoutMinutes,
  budget: cfg.budget,
  ...(cfg.searchAttribute ? { searchAttribute: true } : {}),
});

/** Where `sendPrompt` sends a prompt for this config, and how the session starts. */
export const sessionStart = (cfg: Config, sessionId: string): SessionStart => ({
  taskQueue: cfg.taskQueue,
  sessionId,
  input: {
    sessionId,
    sessionFile: sessionFileFor(cfg.sessionDir, sessionId),
    ...sessionOptions(cfg),
  },
});

export async function submitPrompt(sessionId: string, text: string, promptId = randomUUID()) {
  const { cfg, client, connection } = await connect();
  try {
    await sendPrompt(client, sessionStart(cfg, sessionId), { promptId, text });
  } finally {
    await connection.close();
  }
  return promptId;
}

/** `interruptSession` on a client of its own. */
export async function interrupt(sessionId: string): Promise<boolean> {
  const { client, connection } = await connect();
  try {
    return await interruptSession(client, sessionId);
  } finally {
    await connection.close();
  }
}
