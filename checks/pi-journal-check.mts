// Reproduces #51 against the shipped Pi fork: a superseded writer can append a torn line
// after another SessionManager has opened the file, or after an earlier successful append.
// Every complete single entry and every entry of a complete batch must survive reopening.
// Opening a torn current-version session must not repair the file before its guard is set.
// The next guarded append must preserve earlier bytes and remain readable.
//
// No server or model key. Needs the pinned fork (`npm run setup-fork`).
// Usage: node --import tsx checks/pi-journal-check.mts

import { appendFileSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SessionManager } from "@earendil-works/pi-coding-agent";

const failures: string[] = [];
const check = (label: string, ok: boolean) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${label}`);
  if (!ok) failures.push(label);
};
const root = mkdtempSync(join(tmpdir(), "pi-journal-"));
const cut = '{"type":"custom","customType":"stale","data":';

try {
  for (const mode of ["single", "batch"]) {
    const seed = SessionManager.create(root, join(root, mode));
    seed.appendMessage({ role: "user", content: "seed", timestamp: 1 });
    const file = seed.getSessionFile()!;
    const current = SessionManager.open(file);
    const expected: string[] = [];
    for (const n of [1, 2]) {
      let guards = 0;
      // Inject the stale cut after the current writer's last guard, just before persistence.
      current.setWriteGuard(() => {
        if (++guards === (mode === "batch" ? 3 : 1)) appendFileSync(file, cut);
      });
      const before = readFileSync(file, "utf8");
      if (mode === "batch") {
        current.batch(() => {
          expected.push(current.appendCustomEntry(`current-${n}-a`, n));
          expected.push(current.appendCustomEntry(`current-${n}-b`, n));
        });
      } else {
        expected.push(current.appendCustomEntry(`current-${n}`, n));
      }
      check(`${mode} ${n}: entries visible in memory`,
        expected.every((id) => current.getEntries().some((entry) => entry.id === id)));
      const reopened = SessionManager.open(file);
      check(`${mode} ${n}: all complete entries survive reopening`,
        expected.every((id) => reopened.getEntries().some((entry) => entry.id === id)));
      check(`${mode} ${n}: earlier bytes unchanged`, readFileSync(file, "utf8").startsWith(before + cut));
    }

    appendFileSync(file, cut);
    const beforeOpen = readFileSync(file, "utf8");
    const opened = SessionManager.open(file);
    check(`${mode}: opening a torn session writes no repair`, readFileSync(file, "utf8") === beforeOpen);
    opened.setWriteGuard(() => { throw new Error("superseded"); });
    let refused = false;
    try {
      opened.appendCustomEntry("refused", 1);
    } catch (error) {
      refused = error instanceof Error && error.message === "superseded";
    }
    check(`${mode}: a refused append leaves the torn file unchanged`,
      refused && readFileSync(file, "utf8") === beforeOpen);
    let guards = 0;
    opened.setWriteGuard(() => { guards++; });
    const kept = opened.appendCustomEntry("kept", 2);
    check(`${mode}: the next guarded append survives reopening`,
      guards === 1 && SessionManager.open(file).getEntries().some((entry) => entry.id === kept));
    check(`${mode}: the next append preserves earlier bytes`, readFileSync(file, "utf8").startsWith(beforeOpen));
  }
} finally {
  rmSync(root, { recursive: true, force: true });
}

console.log(failures.length === 0 ? "pi-journal-check: OK" : `pi-journal-check: ${failures.length} failed`);
process.exitCode = failures.length === 0 ? 0 : 1;
