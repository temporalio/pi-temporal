// The queue one worker polls on its own, next to the shared one. A step's tool calls and seal go
// back to the worker that made the model call, so they share its project directory on disk.
// Keyed by host and directory, since two containers can both serve `/project` with different files.
// Not for workflow code (uses `node:crypto`, `node:os`). Workflows get the name from an activity.

import { createHash } from "node:crypto";
import { hostname } from "node:os";
import { resolve } from "node:path";

/** Short enough to stay readable in the Temporal UI; long enough that a collision is
 * implausible. */
const DIGEST_LENGTH = 12;

export function queueForWorker(base: string, projectDir: string, host = hostname()): string {
  const canonical = resolve(projectDir).replace(/[/\\]+$/, "");
  const digest = createHash("sha256")
    .update(`${host} ${canonical}`)
    .digest("hex")
    .slice(0, DIGEST_LENGTH);
  return `${base}-w-${digest}`;
}
