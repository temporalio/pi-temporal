// Replays every kept history in `histories/` against this code, with no server. A history that
// stops replaying means a change altered which commands a Workflow issues, and sessions still
// running on the old code would fail on upgrade. Gate the change with `patched()`, or ship it as a
// new Worker Deployment Version that old runs never reach.
//
// Keep each history once it's recorded. Each stands for runs that may still be open, so a history
// recorded again after every change would prove nothing. `record-histories.mts` adds new ones.
//
// No server and no model key. CI runs it as its own job.
// Usage: npx tsx checks/replay-kept-check.mts

import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { historyFromJSON } from "@temporalio/common/lib/proto-utils";
import { Worker } from "@temporalio/worker";

const workflowsPath = fileURLToPath(new URL("../src/workflow-bundle.ts", import.meta.url));
const kept = fileURLToPath(new URL("./histories/", import.meta.url));
const names = (await readdir(kept)).filter((name) => name.endsWith(".json")).sort();

const failures: string[] = [];
if (names.length === 0) failures.push("there are no kept histories to replay");
for (const name of names) {
  const workflowId = name.replace(/\.json$/, "");
  const history = historyFromJSON(JSON.parse(await readFile(join(kept, name), "utf8")));
  const failure = await Worker.runReplayHistory({ workflowsPath }, history, workflowId).then(
    () => undefined,
    (err: unknown) => err,
  );
  console.log(`${failure === undefined ? "PASS" : "FAIL"} the kept ${workflowId} history`);
  if (failure !== undefined) {
    console.log(`  ${String(failure)}`);
    failures.push(workflowId);
  }
}

const bad = failures.length;
console.log(bad === 0 ? "replay-kept-check: OK" : `replay-kept-check: ${bad} failed`);
process.exit(bad === 0 ? 0 : 1);
