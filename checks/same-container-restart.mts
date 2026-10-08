// Container entrypoint for `docker/restart-check.sh`, so it runs as pid 1 on every boot. Checks
// that a writer marker left by pid 1 before a kill does not refuse the project after a restart.
// First boot marks a tool call and hangs. Second boot runs `ensure` and asserts it gets the
// directory back without moving it aside.

import { existsSync } from "node:fs";
import { mkdir, readdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import * as worktree from "../src/tree/worktree.js";

// The project is a volume, so it cannot be moved aside. A refused directory stays refused.
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
  setInterval(() => {}, 1_000);
}

async function secondBoot() {
  const marked = (await readFile(phase, "utf8")).trim();
  console.log(`booted again as pid ${process.pid}; the marker was ${marked}`);
  let failure = "";
  const warned: string[] = [];
  console.warn = (...args: unknown[]) => void warned.push(args.join(" "));
  // The new pid 1 is in another pid namespace, so the marker clears only once it goes quiet.
  const began = Date.now();
  const until = began + 90_000;
  do {
    failure = "";
    await worktree
      .ensure(project, sessionFile, { turn: "turn-2", step: 1, callId: "later" })
      .catch((err) => {
        failure = String(err);
      });
    if (failure === "" || !failure.includes("never returned")) break;
    await new Promise((r) => setTimeout(r, 2_000));
  } while (Date.now() < until);
  console.log(`ensure settled after ${Math.round((Date.now() - began) / 1000)}s`);
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
