// Checks OpenTelemetry tracing. One trace must follow a prompt from the client call that sent it,
// through the session Workflow, to the Activity that ran its step. Also checks that a Worker with
// tracing off still runs a bundle that has the Workflow interceptors, as the prebuilt one does.
//
// Spans go to an in-memory exporter here. In production `startTracing` sends them over OTLP.
//
// Needs a Temporal server, no model key. Usage: npx tsx checks/tracing-check.mts

import { randomUUID } from "node:crypto";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Client, Connection } from "@temporalio/client";
import { Resource } from "@opentelemetry/resources";
import { InMemorySpanExporter, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base";
import { bundleWorkflowCode } from "@temporalio/worker";
import { createSessionWorker } from "../src/core/session-worker.js";
import {
  clientTracing,
  registerTracing,
  WORKFLOW_TRACING_MODULE,
  type Tracing,
} from "../src/core/tracing.js";
import { UPDATES, workflowId } from "../src/core/protocol.js";
import type { Quiet, RunStepInput, RunStepResult, SessionInput } from "../src/core/protocol.js";

const address = process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7233";
const namespace = "default";
const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

const exporter = new InMemorySpanExporter();
const spanProcessor = new SimpleSpanProcessor(exporter);
const resource = new Resource({ "service.name": "tracing-check" });
const provider = registerTracing(resource, spanProcessor);
const tracing: Tracing = { spanProcessor, resource, shutdown: () => provider.shutdown() };

const activities = () => ({
  async runStep(input: RunStepInput): Promise<RunStepResult> {
    return { done: true, finalText: `answered ${input.promptId}` };
  },
  async retireSession() {},
  async adoptProject() {},
});

/** Runs one prompt through a session on a fresh Worker, and returns when it's answered. */
async function oneTurn(
  label: string,
  workerTracing: Tracing | undefined,
  workflowBundlePath?: string,
) {
  const taskQueue = `pi-tracing-${label}-${randomUUID().slice(0, 8)}`;
  const worker = await createSessionWorker({
    address,
    namespace,
    taskQueue,
    activities,
    tracing: workerTracing,
    workflowBundlePath,
  });
  const running = worker.run();
  const connection = await Connection.connect({ address });
  const client = new Client({ connection, namespace, interceptors: clientTracing() });
  try {
    const sessionId = `tracing-${label}-${randomUUID().slice(0, 8)}`;
    const input: SessionInput = { sessionId, sessionFile: `/unused/${sessionId}.jsonl` };
    const handle = await client.workflow.signalWithStart("piSession", {
      taskQueue,
      workflowId: workflowId(sessionId),
      args: [input],
      signal: "submitPrompt",
      signalArgs: [{ promptId: "one", text: "one" }],
    });
    const quiet = await handle.executeUpdate<Quiet, []>(UPDATES.waitForQuiet);
    await handle.terminate("tracing-check done");
    return quiet.finished?.outcome;
  } finally {
    await worker.stop();
    await running.catch(() => undefined);
    await connection.close();
  }
}

const root = await mkdtemp(join(tmpdir(), "pi-tracing-"));
try {
  const outcome = await oneTurn("on", tracing);
  check("a traced session answers", outcome === "answered", outcome);
  await spanProcessor.forceFlush();
  const spans = exporter.getFinishedSpans();
  const named = (prefix: string) => spans.filter((span) => span.name.startsWith(prefix));
  const started = named("SignalWithStartWorkflow");
  // From inside the Workflow. The run's own span ends only when the run does.
  const workflow = named("StartActivity:runStep");
  const step = named("RunActivity:runStep");
  check(
    "the client, the Workflow and the step each have a span",
    [started, workflow, step].every((found) => found.length > 0),
    spans.map((span) => span.name),
  );
  const traceId = started[0]?.spanContext().traceId;
  check(
    "and all three are in the trace the client started",
    traceId !== undefined &&
      workflow.some((span) => span.spanContext().traceId === traceId) &&
      step.some((span) => span.spanContext().traceId === traceId),
    { traceId, workflow: workflow.map((s) => s.spanContext().traceId) },
  );

  // The prebuilt bundle always has the Workflow interceptors. Untraced, its sink drops them.
  const bundle = join(root, "workflow-bundle.js");
  const { code } = await bundleWorkflowCode({
    workflowsPath: fileURLToPath(new URL("../src/workflow-bundle.ts", import.meta.url)),
    workflowInterceptorModules: [WORKFLOW_TRACING_MODULE],
  });
  await writeFile(bundle, code);
  const before = exporter.getFinishedSpans().length;
  const untraced = await oneTurn("off", undefined, bundle);
  check("an untraced Worker runs the prebuilt bundle", untraced === "answered", untraced);
  await spanProcessor.forceFlush();
  const workflowSpans = exporter
    .getFinishedSpans()
    .slice(before)
    .filter((span) => span.name.startsWith("StartActivity"));
  check("and exports none of its Workflow spans", workflowSpans.length === 0, workflowSpans.length);
} finally {
  await provider.shutdown();
  await rm(root, { recursive: true, force: true });
}

const bad = failures.length;
console.log(bad === 0 ? "tracing-check: OK" : `tracing-check: ${bad} failed`);
process.exit(bad === 0 ? 0 : 1);
