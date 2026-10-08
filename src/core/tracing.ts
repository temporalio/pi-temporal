// OpenTelemetry tracing, off unless `PI_TEMPORAL_TRACING=1`. A trace follows a prompt from the
// client through the session Workflow to each model call and tool call, so an operator can see
// where a slow or failed turn spent its time. Spans go to an OTLP endpoint that the standard
// `OTEL_EXPORTER_OTLP_*` variables name, and `OTEL_SERVICE_NAME` names the process.
//
// Not for Workflow code. Workflows get their spans from the interceptor module below, which runs
// in the sandbox and hands finished spans to the Worker through a sink.

import { createRequire } from "node:module";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { Resource } from "@opentelemetry/resources";
import {
  BatchSpanProcessor,
  NoopSpanProcessor,
  type SpanProcessor,
} from "@opentelemetry/sdk-trace-base";
import { NodeTracerProvider } from "@opentelemetry/sdk-trace-node";
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
 * Starts tracing for this process, once. The tracer provider is global, so Activity and client
 * spans reach the same exporter as the Workflow spans the sink delivers. A process that ends on
 * its own flushes on the way out. One that calls `process.exit` should call `shutdown` first.
 */
export function startTracing(serviceName: string): Tracing {
  if (started) return started;
  const resource = new Resource({
    "service.name": process.env.OTEL_SERVICE_NAME ?? serviceName,
  });
  const spanProcessor = new BatchSpanProcessor(new OTLPTraceExporter());
  const provider = new NodeTracerProvider({ resource, spanProcessors: [spanProcessor] });
  provider.register();
  process.once("beforeExit", () => void provider.shutdown());
  started = { spanProcessor, resource, shutdown: () => provider.shutdown() };
  return started;
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
