// Checks the `pending` dispatch claims and kept results that stop a started call from running
// twice. Asserts claims are exclusive, scoped by turn and step, survive sweeps that drop results,
// and that concurrent writers or odd prompt ids cannot corrupt them. No server needed.
//
// Usage: npx tsx checks/pending-check.mts

import { mkdtemp, readdir, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { unknownToolCallOutcome } from "@earendil-works/pi-coding-agent";
import * as pending from "../src/pending.js";

const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

const outcome = (id: string) => unknownToolCallOutcome({ id, name: "probe" });

async function main() {
  const dir = await mkdtemp(join(tmpdir(), "pi-pending-"));
  const file = join(dir, "session.jsonl");

  // Turn ids (prompt ids). Each turn numbers its steps from one.
  const t1 = "prompt-one";
  const t2 = "prompt-two";

  // A fresh call must look fresh, or the first attempt reports it as unknown and never runs it.
  check(
    "an untouched call has no note",
    (await pending.dispatchClaimed(file, t1, 1, "c1")) === false,
  );
  check(
    "an untouched call has no result",
    (await pending.readResult(file, t1, 1, "c1")) === undefined,
  );

  check("a first dispatch is admitted", (await pending.claimDispatch(file, t1, 1, "c1")) === true);
  check("a dispatch leaves a claim", (await pending.dispatchClaimed(file, t1, 1, "c1")) === true);
  check("the claim is not a result", (await pending.readResult(file, t1, 1, "c1")) === undefined);
  check("a second dispatch is refused", (await pending.claimDispatch(file, t1, 1, "c1")) === false);

  await pending.keepResult(file, t1, 1, "c1", outcome("c1"));
  const kept = await pending.readResult(file, t1, 1, "c1");
  check("a kept result round-trips", kept?.message.toolCallId === "c1", kept);
  check("a kept result keeps its terminate flag", kept?.terminate === false, kept);

  // Scratch from a dead writer must never read as a result.
  const step1 = pending.stepDirFor(file, t1, 1);
  await writeFile(join(step1, "c2.json.abandoned.writing"), "{ not json", "utf8");
  check("scratch is not a result", (await pending.readResult(file, t1, 1, "c2")) === undefined);

  // An attempt past its startToClose can still be writing while its retry writes. A shared
  // scratch path would publish a torn result that the seal reads as unknown.
  const big = (id: string, size: number) => {
    const kept = outcome(id);
    const content = [{ type: "text" as const, text: "x".repeat(size) }];
    return { ...kept, message: { ...kept.message, content } };
  };
  let overlapped: Awaited<ReturnType<typeof pending.readResult>>;
  let wrote = true;
  try {
    await Promise.all([
      pending.keepResult(file, t1, 1, "c3", big("c3", 400_000)),
      pending.keepResult(file, t1, 1, "c3", big("c3", 400_001)),
    ]);
    overlapped = await pending.readResult(file, t1, 1, "c3");
  } catch (err) {
    wrote = false;
    overlapped = undefined;
    console.log(`  (a writer threw: ${String(err)})`);
  }
  const whole = wrote && overlapped?.message.toolCallId === "c3";
  check("two writers for one call publish a whole result", whole, overlapped);

  // Only earlier steps are swept, so a seal retry can reread this step's results.
  await pending.claimDispatch(file, t1, 1, "c2");
  await pending.keepResult(file, t1, 1, "c2", outcome("c2"));
  await pending.claimDispatch(file, t1, 2, "c1");
  await pending.keepResult(file, t1, 2, "c1", outcome("c1"));
  await pending.sweep(file, t1, 2);
  check(
    "an earlier step's results are forgotten",
    (await pending.readResult(file, t1, 1, "c1")) === undefined,
  );
  // Notes must outlive results, or a stalled attempt reruns the tool (`stale-dispatch-check.mts`).
  check("its admission stays", (await pending.dispatchClaimed(file, t1, 1, "c1")) === true);
  const reused = (await pending.readResult(file, t1, 2, "c1"))?.message.toolCallId === "c1";
  check("the same id in this step is its own", reused);
  check("the whole earlier step goes", (await pending.readResult(file, t1, 1, "c2")) === undefined);

  const left = await readdir(pending.stepDirFor(file, t1, 2));
  check("no scratch is left behind", left.every((name) => !name.endsWith(".writing")), left);

  await pending.forgetResults(file, t1, 2, ["c1"]);
  check(
    "forgetting a result drops it",
    (await pending.readResult(file, t1, 2, "c1")) === undefined,
  );
  check("and leaves what admitted it", (await pending.dispatchClaimed(file, t1, 2, "c1")) === true);

  // A new turn drops every result and keeps every note.
  await pending.sweepResults(file);
  check(
    "a new turn drops the results",
    (await pending.readResult(file, t1, 2, "c1")) === undefined,
  );
  check("and keeps the admissions", (await pending.dispatchClaimed(file, t1, 1, "c1")) === true);

  // Without the turn in the scope, this step 1 would see the last turn's step 1 as dispatched.
  check(
    "a new turn's step 1 is its own",
    (await pending.dispatchClaimed(file, t2, 1, "c1")) === false,
  );
  check("and it is admitted", (await pending.claimDispatch(file, t2, 1, "c1")) === true);
  check(
    "without disturbing the turn before it",
    (await pending.dispatchClaimed(file, t1, 1, "c1")) === true,
  );

  // Prompt ids come from clients. They must not escape the path or collide on one directory.
  const escaping = "../../etc";
  await pending.claimDispatch(file, escaping, 1, "c1");
  check(
    "an id that is not a filename stays inside",
    (await readdir(`${file}.pending`)).length === 3,
  );
  check(
    "and is still its own turn",
    (await pending.dispatchClaimed(file, escaping, 1, "c1")) === true,
  );
  check(
    "and not somebody else's",
    (await pending.dispatchClaimed(file, "../../var", 1, "c1")) === false,
  );

  await pending.sweep(join(dir, "never-used.jsonl"), t1, 1);
  await pending.sweepResults(join(dir, "never-used.jsonl"));
  check("sweeping a session with no calls is quiet", true);

  const bad = failures.length;
  console.log(bad === 0 ? "pending-check: OK" : `pending-check: ${bad} failed`);
  process.exit(failures.length === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
