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

  // Nothing kept yet: a fresh dispatch has to look fresh, or a first attempt reports its own
  // tool as an unknown outcome and never runs it.
  check("an untouched call has no note", (await pending.wasDispatched(file, 1, "c1")) === false);
  check("an untouched call has no result", (await pending.readResult(file, 1, "c1")) === undefined);

  await pending.noteDispatch(file, 1, "c1");
  check("a dispatch leaves a note", (await pending.wasDispatched(file, 1, "c1")) === true);
  check("the note is not a result", (await pending.readResult(file, 1, "c1")) === undefined);

  await pending.keepResult(file, 1, "c1", outcome("c1"));
  const kept = await pending.readResult(file, 1, "c1");
  check("a kept result round-trips", kept?.message.toolCallId === "c1", kept);
  check("a kept result keeps its terminate flag", kept?.terminate === false, kept);

  // A writer that died leaves scratch behind. It must never read as a result.
  await writeFile(`${file}.pending/1/c2.json.abandoned.writing`, "{ not json", "utf8");
  check("scratch is not a result", (await pending.readResult(file, 1, "c2")) === undefined);

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
      pending.keepResult(file, 1, "c3", big("c3", 400_000)),
      pending.keepResult(file, 1, "c3", big("c3", 400_001)),
    ]);
    overlapped = await pending.readResult(file, 1, "c3");
  } catch (err) {
    wrote = false;
    overlapped = undefined;
    console.log(`  (a writer threw: ${String(err)})`);
  }
  const whole = wrote && overlapped?.message.toolCallId === "c3";
  check("two writers for one call publish a whole result", whole, overlapped);

  // A step keeps its own results so a retry of its seal can read them again; only earlier steps
  // are swept. The scoping is what stops a reused call id finding a previous step's result.
  await pending.noteDispatch(file, 1, "c2");
  await pending.keepResult(file, 1, "c2", outcome("c2"));
  await pending.noteDispatch(file, 2, "c1");
  await pending.keepResult(file, 2, "c1", outcome("c1"));
  await pending.sweep(file, 2);
  check("an earlier step is forgotten", (await pending.readResult(file, 1, "c1")) === undefined);
  check("its note goes too", (await pending.wasDispatched(file, 1, "c1")) === false);
  const reused = (await pending.readResult(file, 2, "c1"))?.message.toolCallId === "c1";
  check("the same id in this step is its own", reused);
  check("the whole earlier step goes", (await pending.readResult(file, 1, "c2")) === undefined);

  const left = await readdir(`${file}.pending/2`);
  check("no scratch is left behind", left.every((name) => !name.endsWith(".writing")), left);

  await pending.forget(file, 2, ["c1"]);
  check("forget drops the result", (await pending.readResult(file, 2, "c1")) === undefined);
  check("forget drops the note", (await pending.wasDispatched(file, 2, "c1")) === false);

  // A turn that ended leaves stragglers behind. One writing its result back would put it where a
  // later turn looks, and a call id is only unique within the message that asked for it.
  await pending.sweepAll(file);
  await pending.keepResult(file, 2, "c9", outcome("c9"));
  const rebuilt = await pending.readResult(file, 2, "c9");
  check("a straggler cannot rebuild a swept turn", rebuilt === undefined);

  // A session that never dispatched anything has no directory, and sweeping it must not throw.
  await pending.sweep(join(dir, "never-used.jsonl"), 1);
  check("sweeping a session with no calls is quiet", true);

  const bad = failures.length;
  console.log(bad === 0 ? "pending-check: OK" : `pending-check: ${bad} failed`);
  process.exit(failures.length === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
