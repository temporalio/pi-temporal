// Minimal end-to-end smoke: submit a prompt to a session, then poll the session file for the
// assistant's reply. Needs a running worker (npm run worker), a Temporal server, and a model key
// resolvable by Pi (auth.json or env). Usage: tsx src/demo.ts <sessionId> "<prompt>"

import { setTimeout as sleep } from "node:timers/promises";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import { fromEnv, sessionFileFor } from "./config.js";
import { submitPrompt } from "./client.js";

async function main() {
  const sessionId = process.argv[2] ?? `demo-${Date.now()}`;
  const text = process.argv[3] ?? "Reply with the single word PONG.";
  const cfg = fromEnv();
  const file = sessionFileFor(cfg.sessionDir, sessionId);

  const promptId = await submitPrompt(sessionId, text);
  console.log(`submitted prompt ${promptId} to session ${sessionId}`);
  console.log(`session file: ${file}`);

  for (let i = 0; i < 120; i++) {
    await sleep(2000);
    try {
      const sm = SessionManager.open(file);
      const entries = sm.getEntries() as Array<{ role?: string; text?: string }>;
      const lastAssistant = [...entries].reverse().find((e) => e.role === "assistant");
      if (lastAssistant?.text) {
        console.log(`reply: ${lastAssistant.text}`);
        return;
      }
    } catch {
      // file not created yet
    }
  }
  console.log("no reply within the wait window; check the worker log");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
