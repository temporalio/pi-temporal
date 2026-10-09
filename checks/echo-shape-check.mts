// A complete line that isn't an echo entry must fail on load without a retry, and an append
// must never write one. A retry can't fix a broken file, and a bad entry read as is crashes a
// later step on every attempt.
//
// No server, Pi fork, or model key. Usage: node --import tsx checks/echo-shape-check.mts

import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ApplicationFailure } from "@temporalio/common";
import { MockActivityEnvironment } from "@temporalio/testing";
import { echoAgent } from "../examples/echo/agent.js";
import { makeCoreActivities } from "../src/core/activities.js";
import type { ProjectStore } from "../src/core/agent.js";

const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

const prompt = { kind: "prompt", promptId: "p1", text: "hello" };
const call = { id: "c1", name: "echo", text: "hello" };
const response = { kind: "response", text: "", calls: [call], tokens: 5 };
const result = { kind: "result", callId: "c1", status: "ok", text: "hello" };
const note = { kind: "note", type: "marker", data: { count: 1 } };

const malformed: [string, unknown][] = [
  ["issue #47: response with no fields", { kind: "response" }],
  ["unknown kind", { kind: "other" }],
  ["non-string kind", { kind: 1 }],
  ["missing kind", {}],
  ["null", null],
  ["array", []],
  ["primitive", "response"],
  ["calls is null", { ...response, calls: null }],
  ["calls is an object", { ...response, calls: {} }],
  ["calls is a string", { ...response, calls: "c1" }],
  ["call is null", { ...response, calls: [null] }],
  ["call is an array", { ...response, calls: [[]] }],
  ["call is a primitive", { ...response, calls: ["c1"] }],
  ["invalid call after a valid call", { ...response, calls: [call, {}] }],
  ["invalid outcome status", { ...result, status: "other" }],
  ["aborted is a string", { ...response, aborted: "true" }],
  ["aborted is null", { ...response, aborted: null }],
];
// Every required field is checked for presence and type, including nested tool calls.
for (const [entry, fields] of [
  [prompt, { promptId: 1, text: null }],
  [response, { text: null, calls: null, tokens: "5" }],
  [result, { callId: 1, status: null, text: null }],
  [note, { type: 1 }],
] as [Record<string, unknown>, Record<string, unknown>][]) {
  for (const [field, wrong] of Object.entries(fields)) {
    const missing = { ...entry };
    delete missing[field];
    malformed.push([`${entry.kind}: missing ${field}`, missing]);
    malformed.push([`${entry.kind}: wrong ${field} type`, { ...entry, [field]: wrong }]);
  }
}
for (const field of ["id", "name", "text"]) {
  const missing: Record<string, unknown> = { ...call };
  delete missing[field];
  malformed.push([`call: missing ${field}`, { ...response, calls: [missing] }]);
  malformed.push([`call: wrong ${field} type`, { ...response, calls: [{ ...call, [field]: 1 }] }]);
}

const root = await mkdtemp(join(tmpdir(), "pi-echo-shape-"));
const file = join(root, "session.jsonl");
const agent = echoAgent();
let writes = 0;
// Set to stand in for a superseded or cancelled attempt, whose guard throws.
let fenced: Error | undefined;
const guard = () => {
  if (fenced) throw fenced;
  writes++;
};
const openers = {
  open: async () => {
    const session = await agent.open(file, guard);
    session.dispose();
  },
  openRecord: async () => { await agent.openRecord(file, guard); },
};
const rejection = (promise: Promise<unknown>) =>
  promise.then(() => undefined, (err: unknown) => err);
const thrown = (write: () => unknown) => rejection((async () => write())());
const nonRetryable = (err: unknown, path = file) =>
  err instanceof ApplicationFailure && err.nonRetryable === true && err.message.includes(path);
const jsonl = (...entries: unknown[]) => entries.map((e) => JSON.stringify(e)).join("\n") + "\n";
const circular: Record<string, unknown> = {};
circular.self = circular;

try {
  for (const [label, entry] of malformed) {
    const line = JSON.stringify(entry);
    // Complete JSON is invalid with or without its final newline. A valid preceding note must
    // not hide the malformed entry, and neither must a cut line after it. Only syntax errors from
    // cut objects can be skipped.
    for (const ending of ["\n", "", '\n{"kind":"prom']) {
      const contents = `${JSON.stringify(note)}\n${line}${ending}`;
      const how =
        ending === "\n" ? "with a newline" : ending ? "before a cut tail" : "without a newline";
      await writeFile(file, contents);
      for (const [name, open] of Object.entries(openers)) {
        const error: unknown = await open().then(() => undefined, (err: unknown) => err);
        check(
          `${name} rejects ${label} ${how} at load time`,
          nonRetryable(error) && (error as Error).message.includes(line.slice(0, 80)),
          String(error),
        );
      }
      check(
        `${label}: loading leaves the journal unchanged`,
        await readFile(file, "utf8") === contents,
      );
    }
  }

  // Positive controls exercise every supported kind/status, optional aborted, unknown note
  // payloads and extra fields. appendEntry(type, undefined) omits data when JSON is written.
  const valid = [
    prompt, response,
    ...["ok", "unknown", "not-run", "failed"].map((status) => ({ ...result, status })),
    { kind: "response", text: "hello", calls: [], tokens: 5 },
    { kind: "response", text: "", calls: [], tokens: 0, aborted: true },
    ...[null, 1, "text", [1], { count: 1 }, undefined].map((data, i) =>
      ({ kind: "note", type: `data-${i}`, data })),
    { ...note, extra: true },
  ];
  await writeFile(file, jsonl(...valid));
  const session = await agent.open(file, guard);
  check("valid entries can still prepare a step", session.prepareStep() === false);
  check("valid responses retain their token counts", session.spend()?.tokens === 10);
  check("valid prompts remain readable", session.hasPrompt("p1"));
  check(
    "note data remains unrestricted",
    JSON.stringify(session.latestEntry("marker")) === '{"count":1}',
  );
  session.dispose();
  const record = await agent.openRecord(file, guard);
  check(
    "valid notes also open through openRecord",
    JSON.stringify(record?.latestEntry("marker")) === '{"count":1}',
  );
  check("loading never invokes the write guard", writes === 0, writes);

  // Every reader asks for aborted === true, so false is as harmless as leaving it out.
  await writeFile(
    file,
    jsonl(prompt, { kind: "response", text: "", calls: [], tokens: 0, aborted: false }),
  );
  for (const [name, open] of Object.entries(openers)) {
    const error = await rejection(open());
    check(`${name} accepts a response with aborted false`, error === undefined, String(error));
  }
  await writeFile(file, jsonl(...valid));

  // A raw Signal/Update can supply a truthy numeric promptId despite the TypeScript signature.
  // It must fail before the append, keeping the session readable.
  const writing = await agent.open(file, guard);
  const before = await readFile(file, "utf8");
  const error = await rejection(writing.recordPrompt(1 as unknown as string, "hi"));
  check(
    "recordPrompt rejects a numeric promptId without a retry",
    nonRetryable(error),
    String(error),
  );
  check("an invalid append leaves the journal unchanged", await readFile(file, "utf8") === before);

  // The validation diagnostic must not throw a retryable TypeError when JSON.stringify cannot
  // serialize the rejected value. The error still needs to be non-retryable and leave no write.
  for (const [label, value] of [["BigInt", 1n], ["circular", circular]] as const) {
    const error = await rejection(writing.recordPrompt(value as unknown as string, "hi"));
    check(
      `recordPrompt rejects a ${label} promptId without a retry`,
      nonRetryable(error),
      String(error),
    );
    check(
      `a ${label} invalid append leaves the journal unchanged`,
      await readFile(file, "utf8") === before,
    );
    // Note data is free-form, but it still needs a line to be written as.
    const noted = await thrown(() => writing.appendEntry("marker", value));
    check(`appendEntry rejects ${label} data without a retry`, nonRetryable(noted), String(noted));
    check(
      `${label} note data leaves the journal unchanged`,
      await readFile(file, "utf8") === before,
    );
  }

  // A superseded or cancelled attempt reports the fence, not the entry it was asked to write.
  const superseded = new Error("superseded");
  fenced = superseded;
  const stopped = await rejection(writing.recordPrompt(1 as unknown as string, "hi"));
  fenced = undefined;
  check("the fence's error wins over an invalid entry's", stopped === superseded, String(stopped));
  check("and nothing is written", await readFile(file, "utf8") === before);

  // What an append keeps is what it wrote, so the session reads the same before and after a reopen.
  writing.appendEntry("dated", { at: new Date(0) });
  const kept = JSON.stringify(writing.latestEntry("dated"));
  check(
    "an append keeps the entry as written",
    (writing.latestEntry("dated") as { at?: unknown }).at === "1970-01-01T00:00:00.000Z",
    kept,
  );
  writing.dispose();
  const reopened = await agent.open(file, guard).then(
    (session) => {
      const read = JSON.stringify(session.latestEntry("dated"));
      session.dispose();
      return read;
    },
    (err: unknown) => String(err),
  );
  check(
    "the session still opens after rejecting invalid appends, as written",
    reopened === kept,
    reopened,
  );

  // A seal checks every outcome before it writes any, so a bad one can't leave a step half-sealed
  // with no retry to finish it.
  const sealFile = join(root, "seal.jsonl");
  const sealed = jsonl(prompt, { ...response, calls: [call, { ...call, id: "c2" }] });
  const ok = { callId: "c1", status: "ok", text: "hello" };
  await writeFile(sealFile, sealed);
  const sealing = await agent.open(sealFile, guard);
  const seal = (outcomes: unknown[]) =>
    sealing.sealStep(outcomes, { expectCalls: ["c1", "c2"], postRun: true });
  for (const [label, outcome] of [
    ["an outcome missing its fields", { callId: "c2" }],
    ["a null outcome", null],
    ["a primitive outcome", "c2"],
  ] as const) {
    const error = await rejection(seal([ok, outcome]));
    check(
      `sealStep rejects ${label} without a retry`,
      nonRetryable(error, sealFile),
      String(error),
    );
    check(
      `${label} writes none of the step's outcomes`,
      await readFile(sealFile, "utf8") === sealed,
    );
  }
  const done = await rejection(seal([ok, { ...ok, callId: "c2" }]));
  check(
    "a valid seal still writes every outcome",
    done === undefined && sealing.answered("c1") && sealing.answered("c2"),
    String(done),
  );
  sealing.dispose();

  // A JS tool can resolve to anything. What isn't text becomes a failed call, which the seal can
  // write, so the model can still answer.
  for (const [i, [label, value]] of ([
    ["undefined", undefined],
    ["a number", 1],
    ["an object", { text: "hello" }],
  ] as const).entries()) {
    const toolFile = join(root, `tool-${i}.jsonl`);
    await writeFile(toolFile, jsonl(prompt, response));
    const tool = async () => value as unknown as string;
    const tooled = await echoAgent({ tool }).open(toolFile, guard);
    // Through JSON, as the core keeps it in a file until the seal reads it.
    const outcome = JSON.parse(JSON.stringify(await tooled.runToolCall("c1")));
    check(`a tool that returns ${label} is a failed call`, outcome?.status === "failed", outcome);
    const error = await rejection(
      tooled.sealStep([outcome], { expectCalls: ["c1"], postRun: true }),
    );
    check(
      `and that failed call can be sealed`,
      error === undefined && tooled.answered("c1"),
      String(error),
    );
    tooled.dispose();
  }

  // A session that can't be read can't take the Workflow's seconds. Its project must still go
  // back, or no other host can take the session.
  const retired: string[] = [];
  const store = {
    retire: async (sessionFile: string) => { retired.push(sessionFile); return true; },
  } as unknown as ProjectStore;
  const core = makeCoreActivities({ agent, store, sessionRoot: root });
  const retire = (sessionFile: string) =>
    rejection(new MockActivityEnvironment().run(core.retireSession, {
      sessionFile,
      turn: "t1",
      sessionSeconds: 5,
    }));
  const idleFile = join(root, "idle.jsonl");
  await writeFile(idleFile, jsonl(prompt, { ...response, calls: [] }));
  const idle = await retire(idleFile);
  const counted = await agent.openRecord(idleFile, guard);
  const seconds = JSON.stringify(counted?.latestEntry("pi-temporal.session-seconds"));
  check(
    "an idle session is retired with the Workflow's seconds",
    idle === undefined && retired.includes(idleFile) && seconds === '{"turn":"t1","seconds":5}',
    String(idle),
  );
  const brokenFile = join(root, "broken.jsonl");
  const broken = jsonl(note, { kind: "response" });
  await writeFile(brokenFile, broken);
  const unreadable = await retire(brokenFile);
  check(
    "a session that can't be read is still retired",
    unreadable === undefined && retired.includes(brokenFile),
    String(unreadable),
  );
  check("and its file is left as it was", await readFile(brokenFile, "utf8") === broken);
} finally {
  await rm(root, { recursive: true, force: true });
}

const bad = failures.length;
console.log(bad === 0 ? "echo-shape-check: OK" : `echo-shape-check: ${bad} failed`);
process.exit(bad === 0 ? 0 : 1);
