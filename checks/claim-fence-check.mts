// Checks the fence on a session file directly. A later Activity or attempt takes over at once, an
// earlier one is refused when it claims, and a holder's guard refuses once a later one claims.
// No server needed.
//
// Usage: npx tsx checks/claim-fence-check.mts

import assert from "node:assert/strict";
import { mkdtemp, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ApplicationFailure } from "@temporalio/common";
import { claimFence, fenceToken, SUPERSEDED } from "../src/fence.js";
import { fencePrefix } from "../src/protocol.js";

const root = await mkdtemp(join(tmpdir(), "pi-claim-fence-"));
const file = join(root, "s.jsonl");
const run = 1_791_000_000_000;
const token = (seq: number, attempt = 1) => fenceToken(fencePrefix(run, seq), attempt);
try {
  const first = await claimFence(file, token(1));
  first();
  // The same attempt claiming again is not a conflict.
  await claimFence(file, token(1));

  // A retry of the same Activity takes over at once.
  const retry = await claimFence(file, token(1, 2));
  retry();
  assert.throws(first, /newer/);
  console.log("PASS a retry takes over at once, and the attempt it replaced can't write");

  // A zombie that claims late is refused, and not retried.
  const late = await claimFence(file, token(1, 1)).then(
    () => undefined,
    (err: unknown) => err,
  );
  assert.ok(late instanceof ApplicationFailure && late.type === SUPERSEDED, String(late));
  console.log("PASS an earlier attempt that claims late is refused");

  // A later Activity of the run, and a later run, both sort higher than any attempt before.
  await claimFence(file, token(2));
  assert.throws(retry, /newer/);
  await claimFence(file, fenceToken(fencePrefix(run + 1, 1), 1));
  const names = (await readdir(`${file}.fence`)).sort();
  assert.deepEqual(names, [fenceToken(fencePrefix(run + 1, 1), 1)]);
  console.log("PASS a later Activity and a later run sort higher, and lower claims are dropped");
} finally {
  await rm(root, { recursive: true, force: true });
}
