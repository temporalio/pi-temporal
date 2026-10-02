// The queue one worker polls on its own, alongside the shared one.
//
// A step's tools write the project directory the worker that made the model call is standing in, so
// the tools and the seal are addressed back to it rather than to whichever worker is free. That is
// what lets them run at once: they see each other's writes through the filesystem instead of
// shipping the tree to each other, and shipping is what makes two hosts of one step a problem.
//
// Keyed by host as well as directory. Two containers both serve `/project` and share none of it, so
// a key on the path alone sends a step to a host whose directory is a different directory.
//
// Not imported by workflow code: this reaches for `node:crypto` and `node:os`, which the Temporal
// sandbox does not have. The queue name is chosen by the worker and carried to the workflow as an
// activity result.

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
