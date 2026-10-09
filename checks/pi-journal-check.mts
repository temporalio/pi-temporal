// Checks the SessionManager exported by setup-fork's linked dist build, rather than fork source.
// The fork's unit tests own its internal behavior; this checks the shipped package's recovery
// contract with single-entry and framed-batch cuts, single and batch appends, and reopening.
// Complete entries must survive while incomplete stale batches and refused writes stay absent.
// Opening a current-version session is read-only; recovery belongs to the next guarded append.
//
// No server or model key. Needs the pinned fork (`npm run setup-fork`).
// Usage: node --import tsx checks/pi-journal-check.mts

import { spawnSync } from "node:child_process";
import { appendFileSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { isDeepStrictEqual } from "node:util";
import type { SessionManager as PiSessionManager } from "@earendil-works/pi-coding-agent";

// Also check direct invocations, before importing a missing or stale linked build.
const pin = spawnSync("bash", ["-c", ". ./scripts/pinned-fork.sh; pinned_fork"], {
  cwd: fileURLToPath(new URL("../", import.meta.url)),
  stdio: "inherit",
});
if (pin.error) throw pin.error;
if (pin.status !== 0) process.exit(pin.status ?? 1);
const { SessionManager } = await import("@earendil-works/pi-coding-agent");

const failures: string[] = [];
const check = (label: string, ok: boolean) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${label}`);
  if (!ok) failures.push(label);
};
const root = mkdtempSync(join(tmpdir(), "pi-journal-"));

try {
  // Cut a batch emitted by the linked build: one complete framed line, then a partial second.
  const stale = SessionManager.create(root, join(root, "stale"));
  stale.appendMessage({ role: "user", content: "seed", timestamp: 1 });
  const staleFile = stale.getSessionFile()!;
  const beforeBatch = readFileSync(staleFile, "utf8");
  stale.batch(() => {
    stale.appendCustomEntry("stale-a", 1);
    stale.appendCustomEntry("stale-b", 2);
  });
  const batchLines = readFileSync(staleFile, "utf8")
    .slice(beforeBatch.length).split("\n").filter(Boolean);
  if (batchLines.length !== 2 || JSON.parse(batchLines[0]).batch?.size !== 2) {
    throw new Error("the linked build must frame a two-entry batch for the cut fixture");
  }
  const cuts = [
    ["single-cut", '{"type":"custom","customType":"stale","data":'],
    ["batch-cut", `${batchLines[0]}\n${batchLines[1].slice(0, -1)}`],
  ];

  for (const [shape, cut] of cuts) {
    for (const mode of ["single", "batch"]) {
      const label = `${shape}/${mode}`;
      const seed = SessionManager.create(root, join(root, label));
      seed.appendMessage({ role: "user", content: "seed", timestamp: 1 });
      const file = seed.getSessionFile()!;
      const current = SessionManager.open(file);
      const append = (writer: PiSessionManager, name: string) => {
        if (mode === "batch") {
          writer.batch(() => {
            writer.appendCustomEntry(`${name}-a`, 1);
            writer.appendCustomEntry(`${name}-b`, 2);
          });
        } else {
          writer.appendCustomEntry(name, 1);
        }
      };

      for (const n of [1, 2]) {
        const before = readFileSync(file, "utf8");
        let guards = 0;
        // Every guard injects a cut, including the last, without assuming a call count.
        current.setWriteGuard(() => {
          guards++;
          appendFileSync(file, cut);
        });
        append(current, `current-${n}`);
        const reopened = SessionManager.open(file);
        check(`${label} ${n}: exact complete entries reload`,
          isDeepStrictEqual(reopened.getEntries(), current.getEntries()));
        check(`${label} ${n}: leaf reloads`, reopened.getLeafId() === current.getLeafId());
        check(`${label} ${n}: earlier bytes and every injected cut remain`,
          guards > 0 && readFileSync(file, "utf8").startsWith(before + cut.repeat(guards)));
      }

      appendFileSync(file, cut);
      const beforeOpen = readFileSync(file, "utf8");
      const opened = SessionManager.open(file);
      check(`${label}: opening a torn session changes no bytes`,
        readFileSync(file, "utf8") === beforeOpen);
      check(`${label}: opening drops only incomplete entries`,
        isDeepStrictEqual(opened.getEntries(), current.getEntries()));
      opened.setWriteGuard(() => { throw new Error("superseded"); });
      let refused = false;
      try {
        append(opened, "refused");
      } catch (error) {
        refused = error instanceof Error && error.message === "superseded";
      }
      check(`${label}: a refused append changes no bytes or entries`,
        refused && readFileSync(file, "utf8") === beforeOpen &&
        isDeepStrictEqual(opened.getEntries(), current.getEntries()));
      let guards = 0;
      opened.setWriteGuard(() => { guards++; });
      append(opened, "kept");
      const recovered = SessionManager.open(file);
      check(`${label}: guarded recovery reloads exactly the complete entries`,
        guards > 0 && isDeepStrictEqual(recovered.getEntries(), opened.getEntries()));
      check(`${label}: recovery preserves earlier bytes`,
        readFileSync(file, "utf8").startsWith(beforeOpen));
    }
  }
} finally {
  rmSync(root, { recursive: true, force: true });
}

console.log(
  failures.length === 0 ? "pi-journal-check: OK" : `pi-journal-check: ${failures.length} failed`,
);
process.exitCode = failures.length === 0 ? 0 : 1;
