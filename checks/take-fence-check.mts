// Checks the fence on a session file directly. A later Activity or attempt takes over at once. An
// earlier one is refused when it takes its token, and a holder's guard refuses once a later one
// takes one. No server needed.
//
// Usage: npx tsx checks/take-fence-check.mts

import assert from "node:assert/strict";
import { mkdtemp, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ApplicationFailure } from "@temporalio/common";
import { takeFence, fenceToken, SUPERSEDED } from "../src/core/fence.js";
import { fencePrefix, fenceStart } from "../src/core/protocol.js";

const root = await mkdtemp(join(tmpdir(), "pi-take-fence-"));
const file = join(root, "s.jsonl");
const run = 1_791_000_000_000;
const token = (seq: number, attempt = 1) => fenceToken(fencePrefix(run, seq), attempt);
try {
  const first = await takeFence(file, token(1));
  first();
  // The same attempt taking its token again is not a conflict.
  await takeFence(file, token(1));

  // A retry of the same Activity takes over at once.
  const retry = await takeFence(file, token(1, 2));
  retry();
  assert.throws(first, /newer/);
  console.log("PASS a retry takes over at once, and the attempt it replaced can't write");

  // A zombie that takes its token late is refused, and not retried.
  const late = await takeFence(file, token(1, 1)).then(
    () => undefined,
    (err: unknown) => err,
  );
  assert.ok(late instanceof ApplicationFailure && late.type === SUPERSEDED, String(late));
  console.log("PASS an earlier attempt that takes its token late is refused");

  // A later Activity of the run, and a later run, both sort higher than any attempt before.
  await takeFence(file, token(2));
  assert.throws(retry, /newer/);
  await takeFence(file, fenceToken(fencePrefix(run + 1, 1), 1));
  const names = (await readdir(`${file}.fence`)).sort();
  assert.deepEqual(names, [fenceToken(fencePrefix(run + 1, 1), 1)]);
  console.log("PASS a later Activity and a later run sort higher, and lower tokens are dropped");

  // A run that continues as new in the same millisecond counts on from the old run's last fence.
  const same = fenceStart(run, { ms: run, seq: 7 });
  assert.ok(fenceToken(fencePrefix(same.ms, same.seq + 1), 1) > token(7, 3));
  const later = fenceStart(run + 1, { ms: run, seq: 7 });
  assert.deepEqual(later, { ms: run + 1, seq: 0 });
  console.log("PASS a run that starts in the same millisecond still sorts after the one before");
} finally {
  await rm(root, { recursive: true, force: true });
}
