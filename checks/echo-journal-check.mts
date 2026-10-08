// Checks that the echo agent recovers from a crash in the middle of an append. The cut last line
// must be gone after the next append, or it stops being the last line and every later open fails
// on it. Any agent's session format needs the same rule (docs/adapting.md).
//
// No server and no model key. Usage: npx tsx checks/echo-journal-check.mts

import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ApplicationFailure } from "@temporalio/common";
import { echoAgent } from "../examples/echo/agent.js";

const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

const root = await mkdtemp(join(tmpdir(), "pi-echo-journal-"));
const sessionFile = join(root, "session.jsonl");
const agent = echoAgent();
const guard = () => {};

try {
  const first = await agent.open(sessionFile, guard);
  first.appendEntry("count", 1);
  first.dispose();
  // A crash after half of the next entry was written.
  await writeFile(sessionFile, `${await readFile(sessionFile, "utf8")}{"kind":"note","ty`);

  const reopened = await agent.open(sessionFile, guard);
  check("a cut last line is read as if it weren't there", reopened.latestEntry("count") === 1);
  reopened.appendEntry("count", 2);
  reopened.dispose();

  const again = await agent.open(sessionFile, guard).then(
    (session) => session,
    (err: unknown) => err,
  );
  check("the session opens again after the next append", !(again instanceof Error), String(again));
  if (!(again instanceof Error)) {
    const session = again as Awaited<ReturnType<typeof agent.open>>;
    check("and holds the new entry", session.latestEntry("count") === 2);
    session.dispose();
  }
  const text = await readFile(sessionFile, "utf8");
  check("and no cut line is left in it", !text.includes('"ty\n') && text.endsWith("\n"), text);

  // A turn stopped mid-tool ends on its result, with no call-less response after it. The next
  // prompt must ask for its own echo, not answer from the stopped turn's result.
  const stoppedFile = join(root, "stopped.jsonl");
  const stopping = echoAgent({
    tool: async (text) => {
      if (text === "one") throw new Error("stopped");
      return text;
    },
  });
  const stopped = await stopping.open(stoppedFile, guard);
  await stopped.recordPrompt("p1", "one");
  const asked = await stopped.modelCall();
  const call = asked.toolCalls[0];
  const outcome = await stopped.runToolCall(call.id);
  await stopped.sealStep(outcome ? [outcome] : [], { expectCalls: [call.id], postRun: false });
  stopped.dispose();

  const second = await stopping.open(stoppedFile, guard);
  second.prepareStep();
  await second.recordPrompt("p2", "two");
  const next = await second.modelCall();
  check(
    "a prompt after a turn stopped mid-tool asks for its own echo",
    next.toolCalls.length === 1 && second.lastAnswer() === "",
    { calls: next.toolCalls.length, answer: second.lastAnswer() },
  );
  second.dispose();

  // A line that parses but isn't an entry fails at once, and isn't retried.
  const oddFile = join(root, "odd.jsonl");
  await writeFile(oddFile, "null\n");
  const odd = await agent.open(oddFile, guard).then(
    () => undefined,
    (err: unknown) => err,
  );
  check(
    "a line that isn't an entry fails without a retry",
    odd instanceof ApplicationFailure && odd.nonRetryable === true,
    String(odd),
  );
} finally {
  await rm(root, { recursive: true, force: true });
}

const bad = failures.length;
console.log(bad === 0 ? "echo-journal-check: OK" : `echo-journal-check: ${bad} failed`);
process.exit(bad === 0 ? 0 : 1);
