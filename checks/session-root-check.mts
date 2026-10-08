// Checks that a Worker writes only under its session directory. Anyone who can start a Workflow in
// the namespace picks its input, session file included, and the Worker writes the session and its
// claims and fences beside that path. So a path outside the directory must be refused, without a
// retry, since the same input fails the same way every time.
//
// The core Activities over the echo agent, under `MockActivityEnvironment`. No server and no model
// key. Usage: npx tsx checks/session-root-check.mts

import { mkdir, mkdtemp, rm, stat, symlink } from "node:fs/promises";
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
  // A plain string, since `join` would take the `..` out before the Activity saw it.
  const climbed = await modelCall(`${sessions}/../climbed.jsonl`);
  check("so is one that climbs out with ..", refused(climbed), String(climbed));
  const away = join(root, "away");
  await mkdir(away);
  await symlink(away, join(sessions, "link"));
  const linked = await modelCall(join(sessions, "link", "escaped.jsonl"));
  check("and one under a link that leads out", refused(linked), String(linked));
  // Another session's own directories sit beside it, where a file could pre-empt its claims.
  await mkdir(join(sessions, "a.jsonl.fence", "x"), { recursive: true });
  const nested = await modelCall(join(sessions, "a.jsonl.fence", "x", "n.jsonl"));
  check("and one nested below the directory", refused(nested), String(nested));
  const named = await modelCall(join(sessions, "a.txt"));
  check("and one that isn't a .jsonl file", refused(named), String(named));

  // The directory reached through a link, as macOS's /var is /private/var, still takes its files
  // by either path.
  await symlink(sessions, join(root, "alias"));
  const viaAlias = makeCoreActivities({ agent: echoAgent(), sessionRoot: join(root, "alias") });
  const aliased = await new MockActivityEnvironment()
    .run(viaAlias.runModelCall, {
      sessionId: "s",
      sessionFile: join(sessions, "c.jsonl"),
      promptId: "p",
      text: "hello",
      step: 1,
    })
    .then(
      () => undefined,
      (err: unknown) => err,
    );
  check("a directory reached through a link takes its real path", !aliased, String(aliased));

  // The Worker makes the directory itself, so a fresh one doesn't refuse every session.
  makeCoreActivities({ agent: echoAgent(), sessionRoot: join(root, "fresh", "sessions") });
  const made = await stat(join(root, "fresh", "sessions")).then(
    (s) => s.isDirectory(),
    () => false,
  );
  check("a missing directory is made at setup", made);

  // Without a store, adopting writes nothing, so the root can be `/` here.
  const top = makeCoreActivities({ agent: echoAgent(), sessionRoot: "/" });
  const adopt = (sessionFile: string) =>
    new MockActivityEnvironment().run(top.adoptProject, { sessionFile, template: "/t.jsonl" }).then(
      () => undefined,
      (err: unknown) => err,
    );
  const atTop = await adopt("/s.jsonl");
  check("a root of / takes its own files", atTop === undefined, String(atTop));
  const belowTop = await adopt(join(sessions, "s.jsonl"));
  check("and only those", refused(belowTop), String(belowTop));
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
