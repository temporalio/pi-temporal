// One half of docker/restart-check.sh, run as a container's main process, so it is pid 1 on every
// boot of that container. Not a check on its own, which is why it is not named like one: the
// question it answers only exists when the same container is started again.
//
// First boot: establish a project, say a tool call is inside it, and stay there until killed.
// Second boot: ask for the directory the way a later step would. The marker names pid 1, which is
// running again, on the same boot, in the same groups, with the same environment, and it is this
// process. The directory has to come back rather than stay refused.

import { existsSync } from "node:fs";
import { mkdir, readdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import * as worktree from "../src/worktree.js";

// The project is a volume, as in a fleet, so it cannot be moved aside: a directory left refused
// stays refused, which is the failure this exists to catch.
const work = "/work";
const project = "/project";
const sessionFile = join(work, "sessions", "restart.jsonl");
const phase = join(work, "phase");

async function firstBoot() {
  await mkdir(join(work, "sessions"), { recursive: true });
  await writeFile(join(project, "README.md"), "the project\n");
  await worktree.capture(project, sessionFile, { seed: true });
  await worktree.beginWrite(project, { turn: "turn-1", step: 1, callId: "in-flight" });
  await writeFile(phase, `marked by pid ${process.pid}\n`);
  console.log(`MARKED pid ${process.pid}`);
  // Inside the tool call, until the container is killed.
  setInterval(() => {}, 1_000);
}

async function secondBoot() {
  const marked = (await readFile(phase, "utf8")).trim();
  console.log(`booted again as pid ${process.pid}; the marker was ${marked}`);
  let failure = "";
  const warned: string[] = [];
  console.warn = (...args: unknown[]) => void warned.push(args.join(" "));
  await worktree
    .ensure(project, sessionFile, { turn: "turn-2", step: 1, callId: "later" })
    .catch((err) => {
      failure = String(err);
    });
  const writers = join(process.env.PI_TEMPORAL_DATA ?? "", "trees");
  let left = 0;
  for (const dir of await readdir(writers).catch(() => [] as string[])) {
    left += (await readdir(join(writers, dir, "writers")).catch(() => [] as string[])).length;
  }
  const kept = existsSync(join(project, "README.md"));
  const moved = warned.some((line) => line.startsWith("moved "));
  const ok = failure === "" && left === 0 && kept && !moved;
  console.log(ok ? "PASS the directory came back after a same-container restart" : "FAIL");
  if (!ok) console.log(JSON.stringify({ failure: failure.slice(0, 300), left, kept, moved }));
  process.exit(ok ? 0 : 1);
}

if (existsSync(phase)) await secondBoot();
else await firstBoot();
