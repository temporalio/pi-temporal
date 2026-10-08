// Builds the Workflow bundle once, for production. A Worker given it skips bundling at start and
// runs exactly the code that was bundled and replay-tested.
//
// Usage: npm run bundle, then start Workers with
// PI_TEMPORAL_WORKFLOW_BUNDLE=dist/workflow-bundle.js

import { mkdir, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { bundleWorkflowCode } from "@temporalio/worker";
import { WORKFLOW_TRACING_MODULE } from "../src/core/tracing.js";

const out = fileURLToPath(new URL("../dist/workflow-bundle.js", import.meta.url));
const { code } = await bundleWorkflowCode({
  workflowsPath: fileURLToPath(new URL("../src/workflow-bundle.ts", import.meta.url)),
  // Always in, so `PI_TEMPORAL_TRACING` can turn tracing on or off without a new bundle. With
  // tracing off, the Worker's sink drops the Workflow spans.
  workflowInterceptorModules: [WORKFLOW_TRACING_MODULE],
});
await mkdir(fileURLToPath(new URL("../dist/", import.meta.url)), { recursive: true });
await writeFile(out, code);
console.log(`wrote ${out}`);
