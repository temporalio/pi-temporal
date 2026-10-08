// Checks that opening a session makes its directory owner only, also one made before that rule.
// The directory holds the conversation and tool output, and `mkdir` sets a mode only on a
// directory it creates, so an older `0755` one would stay readable to other local users.
//
// Opens the session's record the way retiring a session does. No server and no model key.
// Usage: npx tsx checks/session-dir-check.mts

import { chmod, mkdir, mkdtemp, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { piAgent } from "../src/pi/agent.js";

const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

const root = await mkdtemp(join(tmpdir(), "pi-session-dir-"));
const mode = async (dir: string) => ((await stat(dir)).mode & 0o777).toString(8);
try {
  const agent = piAgent({ projectDir: root });

  const fresh = join(root, "fresh");
  await agent.openRecord(join(fresh, "a.jsonl"), () => {});
  check("a new session directory is owner only", (await mode(fresh)) === "700", await mode(fresh));

  const older = join(root, "older");
  await mkdir(older);
  await chmod(older, 0o755);
  await agent.openRecord(join(older, "b.jsonl"), () => {});
  check("one made before is tightened", (await mode(older)) === "700", await mode(older));
} finally {
  await rm(root, { recursive: true, force: true });
}

const bad = failures.length;
console.log(bad === 0 ? "session-dir-check: OK" : `session-dir-check: ${bad} failed`);
process.exit(bad === 0 ? 0 : 1);
