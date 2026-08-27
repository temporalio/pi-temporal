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
  check("an untouched call has no note", (await pending.wasDispatched(file, "c1")) === false);
  check("an untouched call has no result", (await pending.readResult(file, "c1")) === undefined);

  await pending.noteDispatch(file, "c1");
  check("a dispatch leaves a note", (await pending.wasDispatched(file, "c1")) === true);
  check("the note is not a result", (await pending.readResult(file, "c1")) === undefined);

  await pending.keepResult(file, "c1", outcome("c1"));
  const kept = await pending.readResult(file, "c1");
  check("a kept result round-trips", kept?.message.toolCallId === "c1", kept);
  check("a kept result keeps its terminate flag", kept?.terminate === false, kept);

  // A writer that died leaves scratch behind. It must never read as a result.
  await writeFile(`${file}.pending/c2.json.abandoned.writing`, "{ not json", "utf8");
  check("scratch is not a result", (await pending.readResult(file, "c2")) === undefined);

  // Two writers for one call is a real case: an attempt whose startToClose expired is still
  // running while its retry writes. One scratch path between them publishes a document that is
  // neither, and the seal then reads a tool that succeeded as an unknown outcome.
  const big = (id: string, size: number) => {
    const kept = outcome(id);
    const content = [{ type: "text", text: "x".repeat(size) }];
    return { ...kept, message: { ...kept.message, content } };
  };
  let overlapped: Awaited<ReturnType<typeof pending.readResult>>;
  let wrote = true;
  try {
    await Promise.all([
      pending.keepResult(file, "c3", big("c3", 400_000)),
      pending.keepResult(file, "c3", big("c3", 400_001)),
    ]);
    overlapped = await pending.readResult(file, "c3");
  } catch (err) {
    wrote = false;
    overlapped = undefined;
    console.log(`  (a writer threw: ${String(err)})`);
  }
  const whole = wrote && overlapped?.message.toolCallId === "c3";
  check("two writers for one call publish a whole result", whole, overlapped);

  // The seal keeps its results so a retry of the seal can read them again. The next step sweeps
  // what the transcript now answers, and nothing else.
  await pending.noteDispatch(file, "c2");
  await pending.keepResult(file, "c2", outcome("c2"));
  await pending.sweep(file, new Set(["c1"]));
  check("a swept call is forgotten", (await pending.readResult(file, "c1")) === undefined);
  check("its note goes too", (await pending.wasDispatched(file, "c1")) === false);
  const stillThere = (await pending.readResult(file, "c2"))?.message.toolCallId === "c2";
  check("an unanswered call is kept", stillThere);
  check("its note is kept", (await pending.wasDispatched(file, "c2")) === true);

  const left = await readdir(`${file}.pending`);
  check("scratch is swept", left.every((name) => !name.endsWith(".writing")), left);

  await pending.forget(file, ["c2"]);
  check("forget drops the result", (await pending.readResult(file, "c2")) === undefined);
  check("forget drops the note", (await pending.wasDispatched(file, "c2")) === false);

  // A session that never dispatched anything has no directory, and sweeping it must not throw.
  await pending.sweep(join(dir, "never-used.jsonl"), new Set(["c1"]));
  check("sweeping a session with no calls is quiet", true);

  const bad = failures.length;
  console.log(bad === 0 ? "pending-check: OK" : `pending-check: ${bad} failed`);
  process.exit(failures.length === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
