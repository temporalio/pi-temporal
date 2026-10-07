// Checks that `worktree.sweep` and `worktree.forget` never drop a project a live session still
// needs. Pauses or fails real fs calls at chosen points and asserts which directories survive.

import assert from "node:assert/strict";
import fs from "node:fs/promises";
import { syncBuiltinESMExports } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import * as worktree from "../src/worktree.js";
import { withSessionLock } from "../src/session-lock.js";

const root = await fs.mkdtemp(join(tmpdir(), "pi-storage-repair-"));
const originalData = process.env.PI_TEMPORAL_DATA;
const asHost = (name: string) => {
  process.env.PI_TEMPORAL_DATA = join(root, name, "data");
};
const project = (name: string) => join(root, name, "project");
const session = (name: string) => join(root, "sessions", `${name}.jsonl`);
const read = (path: string) => fs.readFile(path, "utf8").catch(() => "");

async function seed(name: string, file: string, text: string) {
  asHost(name);
  await fs.mkdir(project(name), { recursive: true });
  await fs.writeFile(join(project(name), "note.txt"), text);
  await fs.writeFile(join(project(name), ".gitignore"), "local-only.txt\n");
  await worktree.capture(project(name), file, { seed: true });
}

function barrier() {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => { release = resolve; });
  return { promise, release };
}

async function staleSweep() {
  const file = session("sweep");
  await seed("sweep-seed", file, "active work\n");
  asHost("sweep-worker");
  await worktree.ensure(project("sweep-worker"), file);
  asHost("sweep-seed");
  await worktree.retire(project("sweep-seed"), file);
  asHost("sweep-worker");

  const observed = barrier();
  const resumed = barrier();
  const originalRead = fs.readFile;
  let paused = false;
  // Pause a real storage read after it saw retirement, so a new turn can acquire the locks first.
  fs.readFile = (async (...args: Parameters<typeof fs.readFile>) => {
    const value = await originalRead(...args);
    if (!paused && args[0] === join(`${file}.tree`, "retired.json")) {
      paused = true;
      observed.release();
      await resumed.promise;
    }
    return value;
  }) as typeof fs.readFile;
  syncBuiltinESMExports();
  const sweeping = worktree.sweep();
  try {
    await observed.promise;
    await worktree.ensure(project("sweep-worker"), file);
    await fs.writeFile(join(project("sweep-worker"), "local-only.txt"), "not in a bundle\n");
    resumed.release();
    const freed = await sweeping;
    assert.equal(paused, true, "the storage interleaving must run");
    assert.equal(await read(join(project("sweep-worker"), "local-only.txt")), "not in a bundle\n");
    assert.equal(freed, 0, "a revived session must retain its directory");
    assert.equal(await read(join(project("sweep-worker"), "note.txt")), "active work\n");
  } finally {
    resumed.release();
    await sweeping;
    fs.readFile = originalRead;
    syncBuiltinESMExports();
  }
}

async function behindRetirement() {
  const file = session("behind");
  await seed("behind-seed", file, "first\n");
  asHost("behind-worker");
  await worktree.ensure(project("behind-worker"), file);
  asHost("behind-seed");
  await fs.writeFile(join(project("behind-seed"), "note.txt"), "second\n");
  await worktree.capture(project("behind-seed"), file);
  await worktree.retire(project("behind-seed"), file);
  asHost("behind-worker");
  await fs.writeFile(join(project("behind-worker"), "unshipped.txt"), "keep me\n");
  assert.equal(await worktree.sweep(), 0, "unshipped work must retain its directory");
  assert.equal(await read(join(project("behind-worker"), "unshipped.txt")), "keep me\n");
  await fs.rm(join(project("behind-worker"), "unshipped.txt"));
  assert.equal(await worktree.sweep(), 1, "a clean host behind the tip must be released");
  assert.deepEqual(await fs.readdir(project("behind-worker")), []);
}

async function forgottenRetirement() {
  const file = session("forgotten");
  await seed("forgotten-seed", file, "old session\n");
  asHost("forgotten-worker");
  await worktree.ensure(project("forgotten-worker"), file);
  asHost("forgotten-seed");
  await worktree.retire(project("forgotten-seed"), file);
  await worktree.forget(file);
  const next = session("next");
  await seed("next-seed", next, "next session\n");
  asHost("forgotten-worker");
  await worktree.ensure(project("forgotten-worker"), next);
  assert.equal(await read(join(project("forgotten-worker"), "note.txt")), "next session\n");
}

async function adoptedAfterForget() {
  const template = session("adopt-template");
  const file = session("adopted");
  await seed("adopt-seed", template, "live session\n");
  await worktree.forget(file);
  const marker = `${file}.tree.forgotten.json`;
  const originalRm = fs.rm;
  fs.rm = async (path, opts) => {
    if (path === marker) throw new Error("injected failure after establishment");
    return originalRm(path, opts);
  };
  syncBuiltinESMExports();
  try {
    await assert.rejects(worktree.adopt(template, file), /injected failure/);
  } finally {
    fs.rm = originalRm;
    syncBuiltinESMExports();
  }
  assert.equal(await worktree.established(file), true);
  asHost("adopt-worker");
  await worktree.ensure(project("adopt-worker"), file);
  assert.equal(await worktree.sweep(), 0, "a published project overrides an older deletion marker");
  assert.equal(await read(join(project("adopt-worker"), "note.txt")), "live session\n");
}

async function forgetWaits() {
  const file = session("forget-lock");
  await seed("forget-lock-seed", file, "serialized deletion\n");
  const acquired = barrier();
  const release = barrier();
  const held = withSessionLock(join(`${file}.tree`, "writers"), async () => {
    acquired.release();
    await release.promise;
  });
  await acquired.promise;
  let finished = false;
  const dropping = worktree.forget(file).then(() => { finished = true; });
  try {
    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.equal(finished, false, "deletion must wait for a tree writer");
    assert.equal(await worktree.established(file), true);
  } finally {
    release.release();
    await held;
    await dropping;
  }
  assert.deepEqual(await fs.readdir(`${file}.tree`), ["writers.lock"]);
}

async function liveWriterRetirement() {
  const file = session("live-writer");
  await seed("live-writer-seed", file, "keep this directory\n");
  asHost("live-writer-worker");
  const dir = project("live-writer-worker");
  await worktree.ensure(dir, file);
  await worktree.beginWrite(dir, { turn: "prompt", step: 1, callId: "live" });
  try {
    assert.equal(await worktree.retire(dir, file), false);
    assert.equal(await worktree.sweep(), 0);
    assert.equal(await read(join(dir, "note.txt")), "keep this directory\n");
  } finally {
    await worktree.endWrite(dir, "live");
  }
  assert.equal(await worktree.sweep(), 1, "retirement can release a directory after its tool ends");
}

async function oldClosure() {
  const file = session("old-closure");
  await seed("old-closure-seed", file, "original tree\n");
  for (let step = 1; step <= 501; step++) {
    await worktree.closeStep(file, { turn: "prompt", step });
  }
  const dir = project("old-closure-seed");
  await fs.writeFile(join(dir, "late.txt"), "late effect\n");
  await assert.rejects(
    worktree.capture(dir, file, { fence: { turn: "prompt", step: 1 } }),
    /was closed/,
  );
  asHost("old-closure-worker");
  await worktree.ensure(project("old-closure-worker"), file);
  assert.equal(await read(join(project("old-closure-worker"), "late.txt")), "");
}

async function unreadableTreeState() {
  const file = session("unreadable-tree");
  await seed("unreadable-tree-seed", file, "keep the tip\n");
  const dir = project("unreadable-tree-seed");
  const tip = join(`${file}.tree`, "tip.json");
  const saved = await fs.readFile(tip);
  await fs.writeFile(tip, "{ not json");
  await fs.writeFile(join(dir, "late.txt"), "not authorized\n");
  await assert.rejects(worktree.capture(dir, file, { seed: true }), SyntaxError);
  assert.equal(await fs.readFile(tip, "utf8"), "{ not json");
  await fs.writeFile(tip, saved);
  const closed = join(`${file}.tree`, "closed.json");
  await fs.mkdir(closed);
  await assert.rejects(
    worktree.capture(dir, file, { fence: { turn: "prompt", step: 1 } }),
    { code: "EISDIR" },
  );
}

// A step closed before closures became one marker each is still closed.
async function legacyClosure() {
  const file = session("legacy-closure");
  await seed("legacy-closure-seed", file, "original tree\n");
  await fs.writeFile(
    join(`${file}.tree`, "closed.json"),
    JSON.stringify([{ turn: "prompt", step: 1, at: new Date().toISOString() }]),
  );
  const dir = project("legacy-closure-seed");
  await fs.writeFile(join(dir, "late.txt"), "late effect\n");
  await assert.rejects(
    worktree.capture(dir, file, { fence: { turn: "prompt", step: 1 } }),
    /was closed/,
  );
}

// A writer that died between its scratch write and the rename leaves an empty `.writing` file.
async function leftoverScratch() {
  const file = session("scratch");
  await seed("scratch-seed", file, "scratch tree\n");
  asHost("scratch-worker");
  const dir = project("scratch-worker");
  await worktree.ensure(dir, file);
  const { createHash } = await import("node:crypto");
  const host = join(
    root, "scratch-worker", "data", "trees",
    createHash("sha256").update(dir).digest("hex").slice(0, 16),
  );
  await fs.mkdir(join(host, "writers"), { recursive: true });
  await fs.writeFile(join(host, "writers", "call.json.abc.writing"), "");
  await fs.writeFile(join(host, "held-0000000000000000.json.abc.writing"), "");

  await worktree.ensure(dir, file);
  await worktree.capture(dir, file);
  assert.equal(await worktree.clearWriters(dir), 0);
  await worktree.sweep();
}

// A late capture that is refused must leave the retirement in place, or no sweep frees the host.
async function refusedAfterRetirement() {
  const file = session("refused-late");
  await seed("refused-late-seed", file, "first\n");
  asHost("refused-late-worker");
  const dir = project("refused-late-worker");
  await worktree.ensure(dir, file);
  asHost("refused-late-seed");
  await fs.writeFile(join(project("refused-late-seed"), "note.txt"), "second\n");
  await worktree.capture(project("refused-late-seed"), file);
  await worktree.retire(project("refused-late-seed"), file);
  asHost("refused-late-worker");
  await assert.rejects(worktree.capture(dir, file), /the session is at/);
  assert.equal(await worktree.sweep(), 1, "a refused capture must not revive the session");
  assert.deepEqual(await fs.readdir(dir), []);
}

const checks = {
  staleSweep,
  behindRetirement,
  forgottenRetirement,
  adoptedAfterForget,
  forgetWaits,
  liveWriterRetirement,
  oldClosure,
  unreadableTreeState,
  leftoverScratch,
  legacyClosure,
  refusedAfterRetirement,
};
try {
  const selected = process.argv[2] as keyof typeof checks | undefined;
  for (const [name, check] of Object.entries(checks)) {
    if (selected && selected !== name) continue;
    await check();
    console.log(`PASS ${name}`);
  }
} finally {
  if (originalData === undefined) delete process.env.PI_TEMPORAL_DATA;
  else process.env.PI_TEMPORAL_DATA = originalData;
  await fs.rm(root, { recursive: true, force: true });
}
