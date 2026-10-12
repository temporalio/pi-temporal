// Regressions for https://github.com/temporalio/pi-temporal/issues/54.
// Assert the required behavior; the current data loss must fail this check.
// Drives SessionManager exported by the linked dist package. No server or model key.
// Needs the pinned fork (`npm run setup-fork`); PI_FORK_UNPINNED=1 permits a local build.
// Usage: node --import tsx checks/pi-session-open-check.mts

import { spawnSync } from "node:child_process";
import fs from "node:fs";
import { syncBuiltinESMExports } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { isDeepStrictEqual } from "node:util";

const repo = fileURLToPath(new URL("../", import.meta.url));
if (process.env.PI_FORK_UNPINNED === "1") {
  console.warn("pi-session-open-check: testing a local linked build (PI_FORK_UNPINNED=1)");
} else {
  const pin = spawnSync("bash", ["-c", ". ./scripts/pinned-fork.sh; pinned_fork"], {
    cwd: repo, stdio: "inherit",
  });
  if (pin.error) throw pin.error;
  if (pin.status !== 0) process.exit(pin.status ?? 1);
}
const { SessionManager } = await import("@earendil-works/pi-coding-agent");

const root = fs.mkdtempSync(join(tmpdir(), "pi-session-open-"));
const failures: string[] = [];
let assertions = 0;
const check = (label: string, ok: boolean, observed: string) => {
  assertions++;
  console.log(`${ok ? "PASS" : "FAIL"} ${label}: ${observed}`);
  if (!ok) failures.push(label);
};
const serialize = (entries: readonly unknown[]) =>
  entries.map((entry) => `${JSON.stringify(entry)}\n`).join("");
const originals = [
  {
    type: "message", id: "old-user", parentId: null,
    timestamp: "2026-10-12T00:00:00.000Z",
    message: { role: "user", content: "keep the original user message", timestamp: 1 },
  },
  {
    type: "custom", id: "old-custom", parentId: "old-user",
    timestamp: "2026-10-12T00:00:01.000Z", customType: "original",
    data: { keep: "the original custom entry" },
  },
];
const legacy = (version: 1 | 2) => serialize([
  {
    type: "session", version, id: `issue54-v${version}`,
    timestamp: "2026-10-12T00:00:00.000Z", cwd: root,
  },
  ...originals.map((entry) => {
    if (version === 2) return entry;
    const { id: _id, parentId: _parentId, ...old } = entry;
    return old;
  }),
]);

try {
  // Both adapter paths open before setWriteGuard, including the tool Activity's refusing guard.
  for (const [shape, before] of [
    ["empty", ""], ["v1", legacy(1)], ["v2", legacy(2)],
  ]) {
    const file = join(root, `${shape}.jsonl`);
    fs.writeFileSync(file, before);
    const manager = SessionManager.open(file);
    const afterOpen = fs.readFileSync(file, "utf8");
    check(`${shape} open is read-only`, afterOpen === before,
      `bytes equal=${afterOpen === before}, before=${Buffer.byteLength(before)} bytes, after=${Buffer.byteLength(afterOpen)} bytes`);

    for (const mode of ["single", "batch"]) {
      const entriesBefore = manager.getEntries();
      const leafBefore = manager.getLeafId();
      let refused = false;
      manager.setWriteGuard(() => { throw new Error("refuse transcript write"); });
      try {
        if (mode === "batch") {
          manager.batch(() => {
            manager.appendCustomEntry("refused-a", 1);
            manager.appendCustomEntry("refused-b", 2);
          });
        } else {
          manager.appendCustomEntry("refused", 1);
        }
      } catch (error) {
        refused = error instanceof Error && error.message === "refuse transcript write";
      }
      check(`${shape}/${mode} persistence is refused`, refused, `refused=${refused}`);
      // A refusal must not hide unauthorized initialization that already occurred during open.
      const unchanged = fs.readFileSync(file, "utf8") === before;
      check(`${shape}/${mode} refused persistence preserves original bytes`, unchanged,
        `unchanged since before open=${unchanged}`);
      check(`${shape}/${mode} refused persistence preserves entries and leaf`,
        isDeepStrictEqual(manager.getEntries(), entriesBefore) && manager.getLeafId() === leafBefore,
        `entries=${manager.getEntries().length}, leaf=${manager.getLeafId()}`);
    }
  }

  // Inject after the full loader reaches EOF, not at a particular write implementation. A
  // future read-only open still encounters this append before its first guarded persistence.
  const overlap = join(root, "overlap.jsonl");
  const newer = {
    type: "custom", id: "newer-complete", parentId: "old-custom",
    timestamp: "2026-10-12T00:00:02.000Z", customType: "newer-writer",
    data: { keep: "a complete append after the opening snapshot" },
  };
  fs.writeFileSync(overlap, legacy(2));
  const openBefore = fs.openSync;
  const readBefore = fs.readSync;
  const readers = new Set<number>();
  let injected = false;
  let newerExistedAtInjection = false;
  let opened: ReturnType<typeof SessionManager.open>;
  try {
    fs.openSync = (path, flags, mode) => {
      const fd = openBefore(path, flags, mode);
      if (path === overlap && flags === "r") readers.add(fd);
      return fd;
    };
    fs.readSync = (...args: unknown[]) => {
      const bytes = Reflect.apply(readBefore, fs, args) as number;
      if (bytes === 0 && typeof args[0] === "number" && readers.has(args[0]) && !injected) {
        injected = true;
        fs.appendFileSync(overlap, serialize([newer]));
        newerExistedAtInjection = fs.readFileSync(overlap, "utf8").includes('"id":"newer-complete"');
      }
      return bytes;
    };
    syncBuiltinESMExports();
    opened = SessionManager.open(overlap);
  } finally {
    fs.openSync = openBefore;
    fs.readSync = readBefore;
    syncBuiltinESMExports();
  }
  if (!injected || !newerExistedAtInjection) {
    throw new Error("overlap fixture did not inject its complete append after the load");
  }
  opened.setWriteGuard(() => {});
  opened.appendCustomEntry("current-write", { keep: "the guarded write" });
  const reloaded = SessionManager.open(overlap);
  const newerAfter = reloaded.getEntries().find((entry) => entry.id === newer.id);
  check("migration preserves the complete append after load", isDeepStrictEqual(newerAfter, newer),
    `complete before migration=${newerExistedAtInjection}, present after reload=${newerAfter !== undefined}`);
  check("migration preserves the original entries", isDeepStrictEqual(reloaded.getEntries().slice(0, 2), originals),
    `original IDs=${reloaded.getEntries().slice(0, 2).map((entry) => entry.id).join(",")}`);
  check("migration keeps the subsequent guarded entry",
    reloaded.getEntries().some((entry) => entry.type === "custom" && entry.customType === "current-write"),
    `reloaded entries=${reloaded.getEntries().length}`);

  // Kill a child at its first successful session write. Writing a replacement to a sibling
  // temporary file must leave the original entries intact, unlike in-place migration.
  const crash = join(root, "crash.jsonl");
  fs.writeFileSync(crash, legacy(2));
  const child = spawnSync(process.execPath, [
    "--import", "tsx", fileURLToPath(new URL("./pi-session-migration-child.mts", import.meta.url)), crash,
  ], { cwd: repo, encoding: "utf8", timeout: 30_000 });
  if (child.error) throw child.error;
  if (child.status !== 86 || !child.stdout.includes("CRASH_AFTER_FIRST_SESSION_WRITE")) {
    throw new Error(`crash fixture missed its write boundary: status=${child.status}, stderr=${child.stderr}, stdout=${child.stdout}`);
  }
  let afterCrash: unknown[] = [];
  let reopenError: unknown;
  try {
    afterCrash = SessionManager.open(crash).getEntries();
  } catch (error) {
    reopenError = error;
  }
  check("migration crash retains the exact original entries", reopenError === undefined &&
    isDeepStrictEqual(afterCrash.slice(0, originals.length), originals),
    `original=${originals.length} entries, after crash=${afterCrash.length}, reopen error=${String(reopenError)}`);
} finally {
  fs.rmSync(root, { recursive: true, force: true });
}

console.log(`pi-session-open-check: ${failures.length} failed / ${assertions} assertions`);
process.exitCode = failures.length > 0 ? 1 : 0;
