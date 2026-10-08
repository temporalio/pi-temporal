// OpenTelemetry tracing, off unless `PI_TEMPORAL_TRACING=1`. A trace follows a prompt from the
// client through the session Workflow to each model call and tool call, so an operator can see
// where a slow or failed turn spent its time. Spans go to an OTLP endpoint that the standard
// `OTEL_EXPORTER_OTLP_*` variables name, and `OTEL_SERVICE_NAME` names the process.
//
// Not for Workflow code. Workflows get their spans from the interceptor module below, which runs
// in the sandbox and hands finished spans to the Worker through a sink.

import { createRequire } from "node:module";
import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";
import {
  CompositePropagator,
  W3CBaggagePropagator,
  W3CTraceContextPropagator,
} from "@opentelemetry/core";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { Resource } from "@opentelemetry/resources";
import {
  BasicTracerProvider,
  BatchSpanProcessor,
  NoopSpanProcessor,
  type SpanProcessor,
} from "@opentelemetry/sdk-trace-base";
import {
  makeWorkflowExporter,
  OpenTelemetryActivityInboundInterceptor,
  OpenTelemetryActivityOutboundInterceptor,
  OpenTelemetryWorkflowClientInterceptor,
} from "@temporalio/interceptors-opentelemetry";
import type { WorkerOptions } from "@temporalio/worker";

/** The Workflow interceptors, for `workflowInterceptorModules` when the bundle is built. */
export const WORKFLOW_TRACING_MODULE = createRequire(import.meta.url).resolve(
  "@temporalio/interceptors-opentelemetry/lib/workflow-interceptors",
);

export interface Tracing {
  readonly spanProcessor: SpanProcessor;
  readonly resource: Resource;
  // Sends what's buffered. Call it before the process exits, or the last spans are lost.
  readonly shutdown: () => Promise<void>;
}

let started: Tracing | undefined;

/**
 * Registers a provider for this process. The trace context follows async calls, and it crosses
 * process boundaries only in the W3C headers. The Node provider would also read Jaeger and B3
 * headers when `OTEL_PROPAGATORS` asks, and its Jaeger reader can crash on a malformed one.
 */
export function registerTracing(resource: Resource, spanProcessor: SpanProcessor) {
  // In the constructor. `addSpanProcessor` is deprecated from 1.30 on and gone in 2.x.
  const provider = new BasicTracerProvider({ resource, spanProcessors: [spanProcessor] });
  provider.register({
    contextManager: new AsyncLocalStorageContextManager().enable(),
    propagator: new CompositePropagator({
      propagators: [new W3CTraceContextPropagator(), new W3CBaggagePropagator()],
    }),
  });
  return provider;
}

/**
 * Starts tracing for this process, once. The tracer provider is global, so Activity and client
 * spans reach the same exporter as the Workflow spans the sink delivers. A process that ends on
 * its own flushes on the way out. One that calls `process.exit` calls `flushTracing` first.
 */
export function startTracing(serviceName: string): Tracing {
  if (started) return started;
  const resource = new Resource({
    "service.name": process.env.OTEL_SERVICE_NAME ?? serviceName,
  });
  const spanProcessor = new BatchSpanProcessor(new OTLPTraceExporter());
  const provider = registerTracing(resource, spanProcessor);
  process.once("beforeExit", () => void provider.shutdown());
  started = { spanProcessor, resource, shutdown: () => provider.shutdown() };
  return started;
}

// Long enough for a batch to reach a collector nearby. A collector that's down mustn't hold the
// exit, so a failed process still ends.
const FLUSH_MS = 5_000;

/**
 * Sends the spans still buffered, if this process traces. `beforeExit` doesn't fire for
 * `process.exit`, so a process that exits on an error calls this first, or it loses the spans
 * that show the error.
 */
export async function flushTracing(): Promise<void> {
  if (!started) return;
  await Promise.race([
    started.shutdown().catch(() => undefined),
    new Promise((resolve) => setTimeout(resolve, FLUSH_MS).unref()),
  ]);
}

/** Client interceptors that start a trace for each call and pass it on in the call's headers. */
export const clientTracing = () => ({ workflow: [new OpenTelemetryWorkflowClientInterceptor()] });

/**
 * Worker options for tracing. The `exporter` sink is always there, because the prebuilt bundle
 * has the Workflow interceptors and calls it whether or not this process traces. Without tracing
 * it drops what it gets, so one image serves both.
 */
export function workerTracing(
  tracing: Tracing | undefined,
  bundled: boolean,
): Pick<WorkerOptions, "interceptors" | "sinks"> {
  const sinks = {
    exporter: tracing
      ? makeWorkflowExporter(tracing.spanProcessor, tracing.resource)
      : makeWorkflowExporter(new NoopSpanProcessor(), new Resource({})),
  };
  if (!tracing) return { sinks };
  return {
    sinks,
    interceptors: {
      activity: [
        (ctx) => ({
          inbound: new OpenTelemetryActivityInboundInterceptor(ctx),
          outbound: new OpenTelemetryActivityOutboundInterceptor(ctx),
        }),
      ],
      // A prebuilt bundle already has them. `scripts/bundle.mts` always puts them in.
      ...(bundled ? {} : { workflowModules: [WORKFLOW_TRACING_MODULE] }),
    },
  };
}
