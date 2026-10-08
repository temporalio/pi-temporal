// Checks that a tool's result survives a failure to clear its writer marker. The marker only says
// a tool is running in this directory. Left behind, it holds the directory until its process is
// shown gone. The result is the record of what the tool did, so losing it would turn a tool that
// worked into an unknown outcome.
//
// The core Activities over the echo agent and a project store whose `endWrite` throws, under
// `MockActivityEnvironment`. No server and no model key. Usage:
// npx tsx checks/writer-marker-check.mts

import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { MockActivityEnvironment } from "@temporalio/testing";
import { echoAgent } from "../examples/echo/agent.js";
import { makeCoreActivities } from "../src/core/activities.js";
import type { ProjectStore } from "../src/core/agent.js";
import * as pending from "../src/core/pending.js";
import type { ModelCallResult, ToolCallResult } from "../src/core/protocol.js";

const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

const root = await mkdtemp(join(tmpdir(), "pi-writer-marker-"));
const sessionFile = join(root, "session.jsonl");

let endWrites = 0;
const store: ProjectStore = {
  async ensure() {},
  async capture() {},
  async setAside() {},
  async beginWrite() {},
  async endWrite() {
    endWrites++;
    throw new Error("the marker's directory went away");
  },
  async closeStep() {},
  async retire() {
    return true;
  },
  async adopt() {
    return false;
  },
  isRefusal: () => false,
  isQuarantine: () => false,
};

try {
  const activities = makeCoreActivities({
    agent: echoAgent(),
    store,
    hostQueue: "host",
    sessionRoot: root,
  });
  const env = () => new MockActivityEnvironment();
  const turn = "prompt-1";
  const model: ModelCallResult = await env().run(activities.runModelCall, {
    sessionId: "session",
    sessionFile,
    promptId: turn,
    text: "hello",
    step: 1,
  });
  const call = model.calls[0];
  check("the echo agent asks for one tool call", call !== undefined, model);

  const result = await env()
    .run(activities.runToolCall, { sessionId: "session", sessionFile, turn, step: 1, call })
    .then(
      (value) => ({ value: value as ToolCallResult, failure: undefined }),
      (failure: unknown) => ({ value: undefined, failure: String(failure) }),
    );
  check("clearing the marker was tried and failed", endWrites === 1, endWrites);
  check("the tool call still counts as settled", result.value?.outcome === "settled", result);
  const kept = await pending.readResult(sessionFile, turn, 1, call.id);
  check("and its result is kept for the seal", kept !== undefined, kept);
} finally {
  await rm(root, { recursive: true, force: true });
}

const bad = failures.length;
console.log(bad === 0 ? "writer-marker-check: OK" : `writer-marker-check: ${bad} failed`);
process.exit(bad === 0 ? 0 : 1);
