import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { promisify } from "node:util";
import { Client, Connection } from "@temporalio/client";
import { makeActivities } from "./src/activities.js";
import * as worktree from "./src/worktree.js";

const root = await mkdtemp(join(tmpdir(), "pi-unschedule-"));
const project = join(root, "project");
const sessionDir = join(root, "sessions");
const id = `unschedule-${randomUUID()}`;
const template = join(sessionDir, `schedule-${id}.jsonl`);
const connection = await Connection.connect({
  address: process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7233",
});
const client = new Client({ connection, namespace: process.env.TEMPORAL_NAMESPACE ?? "default" });
const execute = promisify(execFile);
const cli = (args: string[]) =>
  execute(process.execPath, ["--import", "tsx", "src/cli.ts", ...args], {
    env: {
      ...process.env,
      PI_TEMPORAL_DATA: join(root, "host"),
      PI_SESSION_DIR: sessionDir,
      PI_TEMPORAL_SHIP_TREE: "1",
      PI_TEMPORAL_TASK_QUEUE: id,
    },
  });
let firing: string | undefined;
try {
  await mkdir(project);
  await writeFile(join(project, "seed.txt"), "the project\n");
  await cli(["schedule", "check the project", "--every=1h", `--id=${id}`, `--project=${project}`]);
  const scheduled = client.schedule.getHandle(id);
  await scheduled.trigger();
  for (let i = 0; i < 50 && !firing; i++) {
    firing = (await scheduled.describe()).info.recentActions[0]?.action.workflow.workflowId;
    if (!firing) await sleep(100);
  }
  assert.ok(firing, "the schedule must start a firing before deletion");
  const history = await client.workflow.getHandle(firing).fetchHistory();
  assert.ok(
    !history.events?.some((event) => event.workflowTaskStartedEventAttributes),
    "the firing must still be waiting for a worker",
  );
  await cli(["unschedule", id]);
  const target = join(sessionDir, "firing.jsonl");
  await makeActivities({ projectDir: project, shipTree: true }).adoptProject({
    sessionFile: target,
    template,
  });
  assert.equal(await worktree.established(target), true);
  console.log("PASS a queued firing can adopt after the schedule is deleted");
} finally {
  firing ??= await client.schedule.getHandle(id).describe()
    .then((schedule) => schedule.info.recentActions[0]?.action.workflow.workflowId)
    .catch(() => undefined);
  await client.schedule.getHandle(id).delete().catch(() => {});
  if (firing) await client.workflow.getHandle(firing).terminate("check cleanup").catch(() => {});
  await connection.close();
  await rm(root, { recursive: true, force: true });
}
