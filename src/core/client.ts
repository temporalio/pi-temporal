// Client helpers to submit a prompt to a session and to interrupt one. A prompt is an
// Update-with-start: the first starts the per-session Workflow, later ones join the running one.
// The session checks the prompt and says where it sits in the queue.

import { randomUUID } from "node:crypto";
import {
  Client,
  Connection,
  WithStartWorkflowOperation,
  WorkflowNotFoundError,
  WorkflowUpdateFailedError,
  WorkflowUpdateRPCTimeoutOrCancelledError,
} from "@temporalio/client";
import { ApplicationFailure } from "@temporalio/common";
import { type Config, connectionOptions, fromEnv, sessionFileFor } from "../config.js";
import { dataConverterFor } from "./codec.js";
import {
  DUPLICATE_PROMPT,
  QUERIES,
  SIGNALS,
  UPDATES,
  WORKFLOW_TYPE,
  workflowId,
} from "./protocol.js";
import type {
  InterruptInput,
  PromptInput,
  SessionTurnOptions,
  Submitted,
  TurnState,
} from "./protocol.js";
import type { piSession } from "./workflow.js";

type Session = typeof piSession;

const withCodec = (cfg: Config) => {
  const dataConverter = dataConverterFor(cfg);
  return dataConverter ? { dataConverter } : {};
};

// An Update needs a Worker to accept it. Past this, the prompt goes as a Signal, which the server
// keeps until a Worker comes.
const ACCEPT_MS = 10_000;
// A Query needs a Worker to answer. Past this, a stop goes out without naming its turn.
const QUERY_MS = 3_000;

/** Every client here is built by this, so the CLI and the extension follow sessions alike. */
export async function openClient(cfg: Config = fromEnv()) {
  const connection = await Connection.connect(connectionOptions(cfg));
  const client = new Client({
    connection,
    namespace: cfg.namespace,
    // A closed Workflow answers queries with its last state, so one terminated mid-turn looks busy
    // forever. Rejecting the query tells a follower the session is over.
    workflow: { queryRejectCondition: "NOT_OPEN" },
    ...withCodec(cfg),
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

/**
 * Send a prompt, starting the session if it isn't running. Returns how many prompts are ahead of
 * it, or undefined when no Worker answered in time and it went as a Signal. A prompt the session
 * already has counts as sent, so a retry after a lost answer is safe.
 */
export async function sendPrompt(
  client: Client,
  cfg: Config,
  sessionId: string,
  prompt: PromptInput,
): Promise<number | undefined> {
  const file = sessionFileFor(cfg.sessionDir, sessionId);
  const args: Parameters<Session> = [{ sessionId, sessionFile: file, ...sessionOptions(cfg) }];
  const start = {
    taskQueue: cfg.taskQueue,
    workflowId: workflowId(sessionId),
    args,
    // Shown in the UI and CLI. The session id only, since a prompt may hold secrets.
    staticSummary: `pi session ${sessionId}`,
  };
  try {
    const submitted = await client.withDeadline(Date.now() + ACCEPT_MS, () =>
      client.workflow.executeUpdateWithStart<Session, Submitted, [PromptInput]>(UPDATES.submit, {
        args: [prompt],
        // The server keeps one answer per id, so a resent prompt gets the first one back.
        updateId: prompt.promptId,
        startWorkflowOperation: new WithStartWorkflowOperation<Session>(WORKFLOW_TYPE, {
          ...start,
          // A running session takes the prompt. A closed one, such as after an idle exit, starts
          // a new run under the same id. That's also the default, set here so it's on record.
          workflowIdConflictPolicy: "USE_EXISTING",
          workflowIdReusePolicy: "ALLOW_DUPLICATE",
        }),
      }),
    );
    return submitted.ahead;
  } catch (err) {
    if (err instanceof WorkflowUpdateFailedError) {
      const cause = err.cause;
      if (cause instanceof ApplicationFailure && cause.type === DUPLICATE_PROMPT) return undefined;
      throw cause ?? err;
    }
    if (!(err instanceof WorkflowUpdateRPCTimeoutOrCancelledError)) throw err;
  }
  // The session may already hold it from the Update, and drops a repeat.
  await client.workflow.signalWithStart(WORKFLOW_TYPE, {
    ...start,
    signal: SIGNALS.submitPrompt,
    signalArgs: [prompt],
  });
  return undefined;
}

export async function submitPrompt(sessionId: string, text: string, promptId = randomUUID()) {
  const { cfg, client, connection } = await connect();
  try {
    await sendPrompt(client, cfg, sessionId, { promptId, text });
  } finally {
    await connection.close();
  }
  return promptId;
}

/**
 * Whether a session's Workflow exists. Undefined when nobody can say, such as when the server is
 * unreachable. Only a definite false proves a start never landed.
 */
export async function sessionExists(
  client: Client,
  sessionId: string,
): Promise<boolean | undefined> {
  try {
    await client.workflow.getHandle(workflowId(sessionId)).describe();
    return true;
  } catch (err) {
    return err instanceof WorkflowNotFoundError ? false : undefined;
  }
}

/**
 * Stop the turn that's running now. Names it, so a stop that lands after that turn ended can't
 * stop the next one. Without a Worker to say which turn runs, the stop goes untargeted. Returns
 * false when the session said nothing was running, so nothing was sent.
 */
export async function interrupt(sessionId: string): Promise<boolean> {
  const { client, connection } = await connect();
  try {
    const handle = client.workflow.getHandle(workflowId(sessionId));
    const state = await client
      .withDeadline(Date.now() + QUERY_MS, () => handle.query<TurnState>(QUERIES.turnState))
      .catch(() => undefined);
    if (state && !state.running) return false;
    const target: InterruptInput = { promptId: state?.running?.promptId };
    await handle.signal(SIGNALS.interrupt, target);
    return true;
  } finally {
    await connection.close();
  }
}
