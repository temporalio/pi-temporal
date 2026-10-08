// Holds that a writer that stalls after its lease check can't take the session back to its older
// tree. Writer A stops right before it names its tip. B takes the lease over and publishes two
// steps. When A wakes up, the tip must stay B's, and a new host must restore B's newest files.
// Also when B's second step compacts the store and frees A's number, and when the session is
// forgotten while A waits.
//
// Usage: npx tsx checks/stale-tip-check.mts

import assert from "node:assert/strict";
import fs from "node:fs/promises";
import { syncBuiltinESMExports } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import * as worktree from "../src/tree/worktree.js";

const root = await fs.mkdtemp(join(tmpdir(), "pi-stale-tip-"));
const read = (path: string) => fs.readFile(path, "utf8").catch(() => undefined);

function barrier() {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });
  return { promise, release };
}

const originalWrite = fs.writeFile;
const originalMkdir = fs.mkdir;
const originalNow = Date.now;

/**
 * A stalls on its claim for the next bundle while B publishes two. With `steps` set, A first
 * takes the session that many bundles further, so B's second bundle is a restart that compacts.
 */
async function stalledWriter(name: string, steps: number) {
  const sessionFile = join(root, "sessions", `${name}.jsonl`);
  const project = (host: string) => join(root, name, host, "project");
  const asHost = (host: string) => {
    process.env.PI_TEMPORAL_DATA = join(root, name, host, "data");
  };
  for (const host of ["a", "b", "c"]) await fs.mkdir(project(host), { recursive: true });
  asHost("a");
  await fs.writeFile(join(project("a"), "checkout.ts"), "export const order = 1;\n");
  await worktree.capture(project("a"), sessionFile, { seed: true });
  for (let step = 0; step < steps; step++) {
    await fs.writeFile(join(project("a"), "step.txt"), `${step}\n`);
    await worktree.capture(project("a"), sessionFile);
  }
  asHost("b");
  await worktree.ensure(project("b"), sessionFile);

  // A pauses after its last lease check, while it writes the tip for its next bundle.
  const paused = barrier();
  const resume = barrier();
  let stalled = false;
  fs.writeFile = (async (...args: Parameters<typeof fs.writeFile>) => {
    const path = String(args[0]);
    if (!stalled && path.includes(`${join(".tree", "tips")}/`) && path.endsWith(".writing")) {
      stalled = true;
      paused.release();
      await resume.promise;
    }
    return originalWrite(...args);
  }) as typeof fs.writeFile;
  syncBuiltinESMExports();

  asHost("a");
  await fs.writeFile(join(project("a"), "checkout.ts"), "export const order = 2;\n");
  const late = worktree.capture(project("a"), sessionFile).then(
    () => undefined,
    (err: unknown) => err,
  );
  await paused.promise;

  // Past the lease's lifetime, so B may take it over from the stalled A.
  Date.now = () => originalNow() + 5 * 60_000;
  try {
    asHost("b");
    await fs.writeFile(join(project("b"), "checkout.ts"), "export const reject = true;\n");
    await worktree.capture(project("b"), sessionFile);
    await fs.writeFile(join(project("b"), "checkout.test.ts"), "test('rejects empty');\n");
    await worktree.capture(project("b"), sessionFile);
  } finally {
    Date.now = originalNow;
  }
  const newest = await worktree.tipOf(sessionFile);
  assert.equal(newest?.seq, steps + 3);

  asHost("a");
  resume.release();
  const failure = await late;
  fs.writeFile = originalWrite;
  syncBuiltinESMExports();
  // Across a compaction A's number is free again, so its claim can land. It must not matter.
  if (steps === 0) {
    assert.ok(failure instanceof Error, "the stalled publish must fail");
    console.log("PASS a stalled writer's late publish fails");
  }
  assert.deepEqual(await worktree.tipOf(sessionFile), newest);
  console.log(`PASS ${name}: the tip stays on the newest bundle`);

  asHost("c");
  await worktree.ensure(project("c"), sessionFile);
  assert.equal(await read(join(project("c"), "checkout.test.ts")), "test('rejects empty');\n");
  assert.equal(await read(join(project("c"), "checkout.ts")), "export const reject = true;\n");
  asHost("b");
  await worktree.ensure(project("b"), sessionFile);
  assert.equal(await read(join(project("b"), "checkout.test.ts")), "test('rejects empty');\n");
  console.log(`PASS ${name}: a new host and the new holder both keep the newest files`);
}

/**
 * A stalls on its claim while the session is forgotten and then sent again. A's late tip must not
 * bring back the forgotten tree, and must not get in the way of the new one.
 */
async function stalledAcrossForget(name: string, steps: number) {
  const sessionFile = join(root, "sessions", `${name}.jsonl`);
  const project = (host: string) => join(root, name, host, "project");
  const asHost = (host: string) => {
    process.env.PI_TEMPORAL_DATA = join(root, name, host, "data");
  };
  for (const host of ["a", "b", "c"]) await fs.mkdir(project(host), { recursive: true });
  asHost("a");
  await fs.writeFile(join(project("a"), "old.txt"), "one\n");
  await worktree.capture(project("a"), sessionFile, { seed: true });
  // With steps, A's next bundle is a restart, whose cleanup must not reach the new generation.
  for (let step = 0; step < steps; step++) {
    await fs.writeFile(join(project("a"), "step.txt"), `${step}\n`);
    await worktree.capture(project("a"), sessionFile);
  }

  const paused = barrier();
  const resume = barrier();
  let stalled = false;
  // Right after the lease check, before anything for the tip exists.
  fs.mkdir = (async (...args: Parameters<typeof fs.mkdir>) => {
    if (!stalled && String(args[0]).endsWith(join(".tree", "tips"))) {
      stalled = true;
      paused.release();
      await resume.promise;
    }
    return originalMkdir(...args);
  }) as typeof fs.mkdir;
  syncBuiltinESMExports();
  await fs.writeFile(join(project("a"), "old.txt"), "two\n");
  const late = worktree.capture(project("a"), sessionFile).then(
    () => undefined,
    (err: unknown) => err,
  );
  await paused.promise;

  const sendAgain = async () => {
    asHost("b");
    await fs.writeFile(join(project("b"), "new.txt"), "fresh\n");
    await worktree.capture(project("b"), sessionFile, { seed: true });
    await fs.writeFile(join(project("b"), "new.txt"), "fresh again\n");
    await worktree.capture(project("b"), sessionFile);
  };
  Date.now = () => originalNow() + 5 * 60_000;
  try {
    await worktree.forget(sessionFile);
    // Sent again before A wakes, so A's cleanup has the new generation's bundles to reach.
    if (steps > 0) await sendAgain();
  } finally {
    Date.now = originalNow;
  }
  asHost("a");
  resume.release();
  await late;
  fs.mkdir = originalMkdir;
  syncBuiltinESMExports();
  if (steps === 0) {
    assert.equal(await worktree.established(sessionFile), false);
    console.log("PASS a late tip does not bring back a forgotten session");
    await sendAgain();
  }
  asHost("c");
  await worktree.ensure(project("c"), sessionFile);
  assert.equal(await read(join(project("c"), "new.txt")), "fresh again\n");
  assert.equal(await read(join(project("c"), "old.txt")), undefined);
  console.log(`PASS ${name}: the project sent again restores on a new host`);
}

try {
  await fs.mkdir(join(root, "sessions"), { recursive: true });
  await stalledAcrossForget("forgotten", 0);
  // A ends at 39 and stalls on its restart at 40, then cleans up after the session was sent again.
  await stalledAcrossForget("forgotten-restart", 38);
  await stalledWriter("plain", 0);
  // A ends at 38 and stalls on 39. B publishes 39, then the restart at 40.
  await stalledWriter("compacted", 37);
} finally {
  fs.writeFile = originalWrite;
  fs.mkdir = originalMkdir;
  syncBuiltinESMExports();
  Date.now = originalNow;
  await fs.rm(root, { recursive: true, force: true });
}
