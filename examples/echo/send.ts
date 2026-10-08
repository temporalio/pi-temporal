// Sends one prompt to an echo session and prints the answer, using the core client only.
// Usage: npx tsx examples/echo/send.ts "hello" (ECHO_STEPPED=1 runs each tool call on its own)

import { randomUUID } from "node:crypto";
import { Client, Connection } from "@temporalio/client";
import { sendPrompt } from "../../src/core/client.js";
import { type Quiet, sessionFileFor, UPDATES, workflowId } from "../../src/core/protocol.js";
import { settings } from "./worker.js";

const text = process.argv[2];
if (!text) throw new Error('send.ts wants a prompt: npx tsx examples/echo/send.ts "hello"');
const { address, namespace, taskQueue, sessionDir } = settings();
const sessionId = process.env.ECHO_SESSION ?? `echo-${randomUUID().slice(0, 8)}`;
// The Worker opens this path and refuses one outside its directory, so both use the same one.
const sessionFile = sessionFileFor(sessionDir, sessionId);

const connection = await Connection.connect({ address });
try {
  const client = new Client({ connection, namespace });
  const input = { sessionId, sessionFile, stepped: process.env.ECHO_STEPPED === "1" };
  await sendPrompt(client, { taskQueue, sessionId, input }, { promptId: randomUUID(), text });
  // Resolves once nothing runs or waits. A run that moved on through Continue-As-New is asked
  // again.
  const handle = client.workflow.getHandle(workflowId(sessionId));
  let quiet: Quiet;
  do quiet = await handle.executeUpdate<Quiet, []>(UPDATES.waitForQuiet);
  while (quiet.moved);
  console.log(`session ${sessionId} (${sessionFile})`);
  console.log(`${quiet.finished?.outcome}: ${quiet.finished?.finalText}`);
} finally {
  await connection.close();
}
