// Checks the files a step keeps about its calls before it is sealed. These are the durability
// primitives the whole "a call that already started is not silently repeated" claim rests on, and
// they need no Temporal server and no model key.
//
// Usage: npx tsx pending-check.mts

import { mkdtemp, readdir, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { unknownToolCallOutcome } from "@earendil-works/pi-coding-agent";
import * as pending from "./src/pending.js";

const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

const outcome = (id: string) => unknownToolCallOutcome({ id, name: "probe" });

async function main() {
  const dir = await mkdtemp(join(tmpdir(), "pi-pending-"));
  const file = join(dir, "session.jsonl");

  // The turn a call belongs to, which is the prompt's id. Two of them, because what a step keeps
  // is scoped by both, and a turn numbers its steps from one again.
  const t1 = "prompt-one";
  const t2 = "prompt-two";

  // Nothing kept yet: a fresh dispatch has to look fresh, or a first attempt reports its own
  // tool as an unknown outcome and never runs it.
  check("an untouched call has no note", (await pending.wasDispatched(file, t1, 1, "c1")) === false);
  check("an untouched call has no result", (await pending.readResult(file, t1, 1, "c1")) === undefined);

  check("a first dispatch is admitted", (await pending.noteDispatch(file, t1, 1, "c1")) === true);
  check("a dispatch leaves a note", (await pending.wasDispatched(file, t1, 1, "c1")) === true);
  check("the note is not a result", (await pending.readResult(file, t1, 1, "c1")) === undefined);
  // The claim is exclusive, and it is what a second attempt of the same call is refused by.
  check("a second dispatch is refused", (await pending.noteDispatch(file, t1, 1, "c1")) === false);

  await pending.keepResult(file, t1, 1, "c1", outcome("c1"));
  const kept = await pending.readResult(file, t1, 1, "c1");
  check("a kept result round-trips", kept?.message.toolCallId === "c1", kept);
  check("a kept result keeps its terminate flag", kept?.terminate === false, kept);

  // A writer that died leaves scratch behind. It must never read as a result.
  const step1 = pending.stepDirFor(file, t1, 1);
  await writeFile(join(step1, "c2.json.abandoned.writing"), "{ not json", "utf8");
  check("scratch is not a result", (await pending.readResult(file, t1, 1, "c2")) === undefined);

  // Two writers for one call is a real case: an attempt whose startToClose expired is still
  // running while its retry writes. One scratch path between them publishes a document that is
  // neither, and the seal then reads a tool that succeeded as an unknown outcome.
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

  // A step keeps its own results so a retry of its seal can read them again; only earlier steps
  // are swept. The scoping is what stops a reused call id finding a previous step's result.
  await pending.noteDispatch(file, t1, 1, "c2");
  await pending.keepResult(file, t1, 1, "c2", outcome("c2"));
  await pending.noteDispatch(file, t1, 2, "c1");
  await pending.keepResult(file, t1, 2, "c1", outcome("c1"));
  await pending.sweep(file, t1, 2);
  check("an earlier step's results are forgotten", (await pending.readResult(file, t1, 1, "c1")) === undefined);
  // And its notes are not. A note is what says the call was admitted, and an attempt that stalled
  // before taking its claim comes back after the results are gone: without the note its call looks
  // fresh and it runs the tool again. `stale-dispatch-check.mts` drives that interleaving.
  check("its admission stays", (await pending.wasDispatched(file, t1, 1, "c1")) === true);
  const reused = (await pending.readResult(file, t1, 2, "c1"))?.message.toolCallId === "c1";
  check("the same id in this step is its own", reused);
  check("the whole earlier step goes", (await pending.readResult(file, t1, 1, "c2")) === undefined);

  const left = await readdir(pending.stepDirFor(file, t1, 2));
  check("no scratch is left behind", left.every((name) => !name.endsWith(".writing")), left);

  await pending.forgetResults(file, t1, 2, ["c1"]);
  check("forgetting a result drops it", (await pending.readResult(file, t1, 2, "c1")) === undefined);
  check("and leaves what admitted it", (await pending.wasDispatched(file, t1, 2, "c1")) === true);

  // The next turn drops every result, its own and the turns before it, and keeps every note. The
  // results are recorded by then; the notes are what a stalled attempt is still measured against.
  await pending.sweepResults(file);
  check("a new turn drops the results", (await pending.readResult(file, t1, 2, "c1")) === undefined);
  check("and keeps the admissions", (await pending.wasDispatched(file, t1, 1, "c1")) === true);

  // The turn is half the scope, and it has to be: a turn numbers its steps from one again, so
  // without it the new turn's step 1 would read the last turn's step 1 as its own and report a
  // tool that never ran as already dispatched.
  check("a new turn's step 1 is its own", (await pending.wasDispatched(file, t2, 1, "c1")) === false);
  check("and it is admitted", (await pending.noteDispatch(file, t2, 1, "c1")) === true);
  check("without disturbing the turn before it", (await pending.wasDispatched(file, t1, 1, "c1")) === true);

  // A prompt id comes from whoever submitted it, so it is not necessarily a name a filesystem
  // takes. It must not reach the path, and two of them must not land on one directory.
  const escaping = "../../etc";
  await pending.noteDispatch(file, escaping, 1, "c1");
  check("an id that is not a filename stays inside", (await readdir(`${file}.pending`)).length === 3);
  check("and is still its own turn", (await pending.wasDispatched(file, escaping, 1, "c1")) === true);
  check("and not somebody else's", (await pending.wasDispatched(file, "../../var", 1, "c1")) === false);

  // A session that never dispatched anything has no directory, and sweeping it must not throw.
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
