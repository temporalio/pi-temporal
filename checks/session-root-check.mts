// Checks that a Worker writes only under its session directory. Anyone who can start a Workflow in
// the namespace picks its input, session file included, and the Worker writes the session and its
// claims and fences beside that path. So a path outside the directory must be refused, without a
// retry, since the same input fails the same way every time.
//
// The core Activities over the echo agent, under `MockActivityEnvironment`. No server and no model
// key. Usage: npx tsx checks/session-root-check.mts

import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ApplicationFailure } from "@temporalio/common";
import { MockActivityEnvironment } from "@temporalio/testing";
import { echoAgent } from "../examples/echo/agent.js";
import { makeCoreActivities } from "../src/core/activities.js";

const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

const root = await mkdtemp(join(tmpdir(), "pi-session-root-"));
const sessions = join(root, "sessions");
const activities = makeCoreActivities({ agent: echoAgent(), sessionRoot: sessions });
// A Worker can't be built without the directory, so none runs with every path allowed.
// @ts-expect-error `sessionRoot` is required.
void (() => makeCoreActivities({ agent: echoAgent() }));
const modelCall = (sessionFile: string) =>
  new MockActivityEnvironment()
    .run(activities.runModelCall, {
      sessionId: "s",
      sessionFile,
      promptId: "p",
      text: "hello",
      step: 1,
    })
    .then(
      () => undefined,
      (err: unknown) => err,
    );
const refused = (err: unknown) =>
  err instanceof ApplicationFailure &&
  err.nonRetryable === true &&
  err.type === "SessionOutsideRoot";

try {
  const inside = await modelCall(join(sessions, "a.jsonl"));
  check("a session file in the directory is used", inside === undefined, String(inside));
  const outside = await modelCall(join(root, "elsewhere.jsonl"));
  check("one outside it is refused, and not retried", refused(outside), String(outside));
  const climbed = await modelCall(join(sessions, "..", "climbed.jsonl"));
  check("so is one that climbs out with ..", refused(climbed), String(climbed));
  const template = await new MockActivityEnvironment()
    .run(activities.adoptProject, {
      sessionFile: join(sessions, "b.jsonl"),
      template: join(root, "template.jsonl"),
    })
    .then(
      () => undefined,
      (err: unknown) => err,
    );
  check("and a template from outside it", refused(template), String(template));
} finally {
  await rm(root, { recursive: true, force: true });
}

const bad = failures.length;
console.log(bad === 0 ? "session-root-check: OK" : `session-root-check: ${bad} failed`);
process.exit(bad === 0 ? 0 : 1);
