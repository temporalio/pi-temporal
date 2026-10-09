// Reproduces #47: a complete {"kind":"response"} used to open successfully, then
// prepareStep() threw a retryable TypeError reading the missing calls.length.
// Both open() and openRecord() must reject malformed complete entries at load time with a
// non-retryable ApplicationFailure identifying the file and offending line. Retrying an
// unchanged broken journal cannot repair it. Loading must not write or invoke the fence.
// Valid entries (including arbitrary note data and extra fields) must remain readable;
// syntactically cut objects are still covered by echo-journal-check.mts.
// Invalid entries must also fail before append calls the write guard, so callers bypassing
// TypeScript cannot write a journal that the next open would reject.
//
// No server, Pi fork, or model key. Usage: node --import tsx checks/echo-shape-check.mts
// The repository's checks runner discovers this file automatically.

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
  ["aborted is false", { ...response, aborted: false }],
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
const guard = () => { writes++; };
const openers = {
  open: async () => {
    const session = await agent.open(file, guard);
    session.dispose();
  },
  openRecord: async () => { await agent.openRecord(file, guard); },
};

try {
  for (const [label, entry] of malformed) {
    const line = JSON.stringify(entry);
    // Complete JSON is invalid with or without its final newline. A valid preceding note must
    // not hide the malformed entry. Only syntax errors from cut objects can be skipped.
    for (const ending of ["\n", ""]) {
      const contents = `${JSON.stringify(note)}\n${line}${ending}`;
      await writeFile(file, contents);
      for (const [name, open] of Object.entries(openers)) {
        const error: unknown = await open().then(() => undefined, (err: unknown) => err);
        check(
          `${name} rejects ${label} ${ending ? "with" : "without"} a newline at load time`,
          error instanceof ApplicationFailure && error.nonRetryable === true &&
            error.message.includes(file) && error.message.includes(line.slice(0, 80)),
          String(error),
        );
      }
      check(`${label}: loading leaves the journal unchanged`, await readFile(file, "utf8") === contents);
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
  await writeFile(file, valid.map((entry) => JSON.stringify(entry)).join("\n") + "\n");
  const session = await agent.open(file, guard);
  check("valid entries can still prepare a step", session.prepareStep() === false);
  check("valid responses retain their token counts", session.spend()?.tokens === 10);
  check("valid prompts remain readable", session.hasPrompt("p1"));
  check("note data remains unrestricted", JSON.stringify(session.latestEntry("marker")) === '{"count":1}');
  session.dispose();
  const record = await agent.openRecord(file, guard);
  check("valid notes also open through openRecord", JSON.stringify(record?.latestEntry("marker")) === '{"count":1}');
  check("loading never invokes the write guard", writes === 0, writes);

  // A raw Signal/Update can supply a truthy numeric promptId despite the TypeScript signature.
  // It must fail before either the guard or the append, keeping the session readable.
  const writing = await agent.open(file, guard);
  const before = await readFile(file, "utf8");
  const writesBefore = writes;
  const error: unknown = await writing.recordPrompt(1 as unknown as string, "hi").then(
    () => undefined,
    (err: unknown) => err,
  );
  check(
    "recordPrompt rejects a numeric promptId without a retry",
    error instanceof ApplicationFailure && error.nonRetryable === true,
    String(error),
  );
  check("an invalid append never invokes the write guard", writes === writesBefore, writes);
  check("an invalid append leaves the journal unchanged", await readFile(file, "utf8") === before);
  writing.dispose();
  const readable = await agent.open(file, guard).then(
    (session) => { session.dispose(); return true; },
    () => false,
  );
  check("the session still opens after rejecting an invalid append", readable);
} finally {
  await rm(root, { recursive: true, force: true });
}

console.log(failures.length === 0 ? "echo-shape-check: OK" : `echo-shape-check: ${failures.length} failed`);
process.exit(failures.length === 0 ? 0 : 1);
