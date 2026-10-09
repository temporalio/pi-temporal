// A whole `Agent` with no model and no Pi: its "model" asks for one `echo` tool call with the
// prompt's text, then answers with what the tool gave back. The session is a JSONL file of this
// file's own entries. Each rule a comment cites is under "What must hold" in docs/adapting.md.

import { appendFileSync, existsSync, mkdirSync, readFileSync } from "node:fs";
import { dirname } from "node:path";
import { randomUUID } from "node:crypto";
import { ApplicationFailure } from "@temporalio/common";
import type {
  Agent,
  AgentSession,
  ModelCall,
  ToolCallRef,
} from "../../src/core/agent.js";

type Call = { id: string; name: string; text: string };
type Outcome = { callId: string; status: "ok" | "unknown" | "not-run" | "failed"; text: string };
type Prompt = { kind: "prompt"; promptId: string; text: string };
type Response = { kind: "response"; text: string; calls: Call[]; tokens: number; aborted?: true };
type Result = { kind: "result" } & Outcome;
// The core's bookkeeping. Not part of the conversation.
type Note = { kind: "note"; type: string; data: unknown };
type Entry = Prompt | Response | Result | Note;

/** The echo tool. A check can pass a slow one, to stop a Worker while it runs. */
export type EchoTool = (text: string, signal?: AbortSignal) => Promise<string>;

export interface EchoOptions {
  readonly tool?: EchoTool;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isEntry(value: unknown): value is Entry {
  if (!isObject(value)) return false;
  switch (value.kind) {
    case "prompt":
      return typeof value.promptId === "string" && typeof value.text === "string";
    case "response":
      return typeof value.text === "string" && typeof value.tokens === "number" &&
        Array.isArray(value.calls) && value.calls.every((call: unknown) =>
          isObject(call) && typeof call.id === "string" && typeof call.name === "string" &&
          typeof call.text === "string") &&
        (value.aborted === undefined || value.aborted === true);
    case "result":
      return typeof value.callId === "string" && typeof value.text === "string" &&
        (value.status === "ok" || value.status === "unknown" ||
          value.status === "not-run" || value.status === "failed");
    case "note":
      // data is deliberately unknown; JSON omits it when appendEntry receives undefined.
      return typeof value.type === "string";
    default:
      return false;
  }
}

function load(file: string): { entries: Entry[]; torn: boolean } {
  if (!existsSync(file)) return { entries: [], torn: false };
  const text = readFileSync(file, "utf8");
  // A crash mid-append can leave a cut last line. The next append ends it with a newline first.
  const torn = text.length > 0 && !text.endsWith("\n");
  const entries: Entry[] = [];
  for (const line of text.split("\n")) {
    if (!line) continue;
    let parsed: unknown;
    try {
      parsed = JSON.parse(line);
    } catch {
      // A cut line, now or from an earlier crash. Every entry is an object, and a cut object never
      // parses, so skipping it loses no whole entry.
      continue;
    }
    // A line that parses but isn't an entry is a broken file, not a crash. A broken file stays
    // broken, so a retry would burn every attempt on the same error.
    if (!isEntry(parsed)) {
      throw ApplicationFailure.nonRetryable(
        `the session ${file} can't be read: not a session entry: ${line.slice(0, 80)}`,
      );
    }
    entries.push(parsed);
  }
  return { entries, torn };
}

/** One file's entries, and the only way to add to them: through the guard. */
function journal(file: string, guard: () => void) {
  const { entries, torn } = load(file);
  let cut = torn;
  const append = (entry: Entry) => {
    // Every append calls the guard first, and stops if it throws. That's the fence.
    guard();
    // Only appends, never a rewrite. The fence lets a superseded writer through for one write
    // after its guard, and an append can't erase what a newer writer added. A newline ends a cut
    // last line, so it can't join this entry, and every later read skips it.
    appendFileSync(file, `${cut ? "\n" : ""}${JSON.stringify(entry)}\n`);
    cut = false;
    entries.push(entry);
  };
  const latestEntry = (type: string, skip?: (data: unknown) => boolean) => {
    for (let i = entries.length - 1; i >= 0; i--) {
      const e = entries[i];
      if (e.kind === "note" && e.type === type && !skip?.(e.data)) return e.data;
    }
    return undefined;
  };
  const appendEntry = (type: string, data: unknown) => append({ kind: "note", type, data });
  return { entries, append, latestEntry, appendEntry };
}

function openSession(file: string, guard: () => void, tool: EchoTool): AgentSession {
  const { entries, append, latestEntry, appendEntry } = journal(file, guard);
  let busy = false;

  const talk = (): Entry[] => entries.filter((e) => e.kind !== "note");
  const lastPrompt = () => talk().filter((e): e is Prompt => e.kind === "prompt").at(-1);
  const lastResponse = () => talk().filter((e): e is Response => e.kind === "response").at(-1);
  // Results after the latest response only. Another response may reuse a call id.
  const results = () => {
    const all = talk();
    const response = lastResponse();
    const from = response ? all.lastIndexOf(response) : -1;
    const after = from < 0 ? [] : all.slice(from + 1);
    return after.filter((e): e is Result => e.kind === "result");
  };
  const answered = (callId: string) => results().some((r) => r.callId === callId);
  const unanswered = () =>
    (lastResponse()?.calls ?? []).filter((c) => !answered(c.id)).map((c) => c.id);
  const endsWithResponse = () => talk().at(-1)?.kind === "response";
  const open = (): ModelCall => {
    const response = lastResponse();
    const calls = response?.calls ?? [];
    return {
      toolCalls: calls.map(({ id, name }) => ({ id, name })),
      sequential: false,
      ended: response?.aborted === true,
    };
  };
  const settle = (callId: string, status: Outcome["status"], text: string) =>
    append({ kind: "result", callId, status, text });

  return {
    prepareStep() {
      if (busy) return "busy";
      // A stopped or crashed step's open calls become unknown, never run again. A whole-step
      // retry has no dispatch claim, so this is all that keeps it from running a tool twice.
      for (const id of unanswered()) settle(id, "unknown", "this call may or may not have run");
      const last = talk().at(-1);
      return last !== undefined && !(last.kind === "response" && last.calls.length === 0);
    },
    // The prompt id is the marker. `hasPrompt` must find what `recordPrompt` wrote.
    hasPrompt: (promptId) => entries.some((e) => e.kind === "prompt" && e.promptId === promptId),
    async recordPrompt(promptId, text) {
      // A prompt after an unanswered call is a transcript no provider would take.
      if (unanswered().length > 0) return false;
      append({ kind: "prompt", promptId, text });
      return true;
    },
    async modelCall(signal) {
      // A retry reuses the recorded response. The completion can be lost after the response
      // reached the file, and asking again would add a second response and a second charge.
      if (endsWithResponse()) return open();
      // A step already stopped asks the model nothing, and ends the run.
      if (signal?.aborted) {
        append({ kind: "response", text: "", calls: [], tokens: 0, aborted: true });
        return open();
      }
      // The deterministic model. The real one retries its provider here, inside the step, since
      // the Activity's retry policy assumes it does.
      const prompt = lastPrompt();
      if (!prompt) throw ApplicationFailure.nonRetryable("a model call with no prompt");
      // This prompt's result only. A turn that ended without a call-less response, such as a stop
      // mid-tool, leaves its result after the latest response, and it doesn't answer this prompt.
      const all = talk();
      const result = results()
        .filter((r) => all.lastIndexOf(r) > all.lastIndexOf(prompt))
        .at(-1);
      const text = !result
        ? ""
        : result.status === "ok"
          ? result.text
          : `the echo's outcome is ${result.status}: ${prompt.text}`;
      const calls = result ? [] : [{ id: `echo-${randomUUID()}`, name: "echo", text: prompt.text }];
      append({ kind: "response", text, calls, tokens: prompt.text.length + text.length });
      return open();
    },
    async runToolCall(callId, signal) {
      // Writes nothing. Calls may run in parallel, so only the seal records outcomes.
      if (answered(callId)) return undefined;
      const call = lastResponse()?.calls.find((c) => c.id === callId);
      if (!call) throw new Error(`the latest response has no call ${callId}`);
      busy = true;
      try {
        const outcome: Outcome = { callId, status: "ok", text: await tool(call.text, signal) };
        return outcome;
      } catch (err) {
        // A stopped or failed tool still reports what happened. Plain JSON, since the core keeps
        // it in a file until the seal reads it.
        const outcome: Outcome = { callId, status: "failed", text: String(err) };
        return outcome;
      } finally {
        busy = false;
      }
    },
    async sealStep(outcomes, { expectCalls, agentState }) {
      const asked = (lastResponse()?.calls ?? []).map((c) => c.id);
      // Results go only on the response the model call recorded, never on a later one.
      if (asked.join() !== expectCalls.join()) {
        throw ApplicationFailure.nonRetryable(
          `the seal expected calls [${expectCalls}], and the session has [${asked}]`,
        );
      }
      // A retried seal skips what it already wrote, and so decides the same thing.
      for (const outcome of outcomes as Outcome[]) {
        if (!answered(outcome.callId)) settle(outcome.callId, outcome.status, outcome.text);
      }
      // No retry and no compaction here, so `postRun` changes nothing. The turn is over once the
      // latest response asked for no tool.
      return { done: asked.length === 0, ...(agentState ? { agentState } : {}) };
    },
    waitForIdle: async () => {},
    dispose: () => {},
    answered,
    asked: (callId) => (lastResponse()?.calls ?? []).some((c) => c.id === callId),
    unanswered,
    endsWithResponse,
    lastAnswer: () => lastResponse()?.text ?? "",
    spend: () => ({
      tokens: entries.reduce((sum, e) => sum + (e.kind === "response" ? e.tokens : 0), 0),
    }),
    latestEntry,
    appendEntry,
  };
}

export function echoAgent(options: EchoOptions = {}): Agent {
  const tool = options.tool ?? (async (text: string) => text);
  return {
    async open(sessionFile, guard) {
      // Claims, kept results and fence tokens go in `${sessionFile}.*`, so the directory must be
      // writable too.
      mkdirSync(dirname(sessionFile), { recursive: true });
      return openSession(sessionFile, guard, tool);
    },
    async openRecord(sessionFile, guard) {
      if (!existsSync(sessionFile)) return undefined;
      const { latestEntry, appendEntry } = journal(sessionFile, guard);
      return { latestEntry, appendEntry };
    },
    unknownOutcome: (call: ToolCallRef): Outcome => ({
      callId: call.id,
      status: "unknown",
      text: "this call may have run, and its result was lost",
    }),
    notRunOutcome: (call: ToolCallRef): Outcome => ({
      callId: call.id,
      status: "not-run",
      text: "this call did not run: the turn stopped before it started",
    }),
  };
}
