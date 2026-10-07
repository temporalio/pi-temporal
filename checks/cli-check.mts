// Checks the CLI's refusals. A flag value is never taken for the task, `start` and `schedule` keep
// the project guard `/background` has, `watch` and `forget` say when there's nothing there, and a
// taken or invalid schedule never touches a template.
//
// Needs a Temporal server, no model key. Usage: npx tsx checks/cli-check.mts

import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdir, mkdtemp, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Client, Connection } from "@temporalio/client";

const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

const address = process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7233";
const cliPath = fileURLToPath(new URL("../src/cli.ts", import.meta.url));
const root = await mkdtemp(join(tmpdir(), "pi-cli-check-"));
const sessions = join(root, "sessions");
const id = `cli-check-${randomUUID().slice(0, 8)}`;

const cli = (args: string[], env: NodeJS.ProcessEnv = {}) =>
  new Promise<{ code: number; out: string; err: string }>((done) => {
    execFile(
      process.execPath,
      ["--import", "tsx", cliPath, ...args],
      {
        env: {
          ...process.env,
          TEMPORAL_ADDRESS: address,
          PI_SESSION_DIR: sessions,
          PI_TEMPORAL_DATA: join(root, "data"),
          PI_TEMPORAL_TASK_QUEUE: id,
          ...env,
        },
        timeout: 60_000,
      },
      (err, out, stderr) => {
        const code = err ? (typeof err.code === "number" ? err.code : 1) : 0;
        done({ code, out, err: stderr });
      },
    );
  });

const connection = await Connection.connect({ address });
const client = new Client({ connection, namespace: "default" });
try {
  await mkdir(sessions, { recursive: true });
  const tree = { PI_TEMPORAL_SHIP_TREE: "1", PI_TEMPORAL_STEPPED: "1" };

  const spaced = await cli(["start", "--session", "the task"]);
  check("`--flag value` is refused", spaced.code !== 0 && /--session=/.test(spaced.err), spaced);
  const unknown = await cli(["start", "the task", "--sesion=x"]);
  check("an unknown flag is refused", unknown.code !== 0 && /no flag/.test(unknown.err), unknown);

  // A stand-in home, so a CLI without the guard can't ship the real one.
  const fakeHome = join(root, "home");
  await mkdir(fakeHome);
  const home = await cli(["start", "the task", `--project=${fakeHome}`], {
    ...tree,
    HOME: fakeHome,
  });
  check("start refuses the home directory", home.code !== 0 && /home/.test(home.err), home);
  const bare = join(root, "bare");
  await mkdir(bare);
  const scheduledBare = await cli(["schedule", "t", "--every=1h", `--id=${id}-bare`,
    `--project=${bare}`], tree);
  check("schedule refuses an unguarded directory", scheduledBare.code !== 0, scheduledBare);

  const watched = await cli(["watch", `${id}-nobody`]);
  check(
    "watch on an unknown id says so and fails",
    watched.code !== 0 && /no such session/.test(watched.err),
    watched,
  );
  const forgot = await cli(["forget", `${id}-nobody`]);
  check("forget with nothing stored says so", /nothing to drop/.test(forgot.err), forgot);

  const project = join(root, "project");
  await mkdir(project);
  await writeFile(join(project, ".gitignore"), "");
  await writeFile(join(project, "a.txt"), "first\n");
  const first = await cli(["schedule", "t", "--every=1h", `--id=${id}`, `--project=${project}`],
    tree);
  check("a schedule with a guarded project is created", first.code === 0, first);
  // The template's tip names the captured tree, so a second capture would change it.
  const tip = join(sessions, `schedule-${id}.jsonl.tree`, "tip.json");
  const tipBefore = await readFile(tip, "utf8").catch(() => "");
  const before = (await readdir(sessions)).sort();
  await writeFile(join(project, "a.txt"), "second\n");
  const taken = await cli(["schedule", "t", "--every=1h", `--id=${id}`, `--project=${project}`],
    tree);
  check("a taken schedule id is refused", taken.code !== 0 && /exists/.test(taken.err), taken);
  const invalid = await cli(["schedule", "t", "--every=soon", `--id=${id}-bad`,
    `--project=${project}`], tree);
  check("an invalid --every is refused", invalid.code !== 0, invalid);
  const after = (await readdir(sessions)).sort();
  check("neither wrote a template", JSON.stringify(after) === JSON.stringify(before), {
    before,
    after,
  });
  const tipAfter = await readFile(tip, "utf8").catch(() => "");
  check("the existing template is untouched", tipBefore !== "" && tipAfter === tipBefore, {
    tipBefore,
    tipAfter,
  });
} finally {
  for (const name of [id, `${id}-bad`, `${id}-bare`]) {
    await client.schedule.getHandle(name).delete().catch(() => {});
  }
  await connection.close();
  await rm(root, { recursive: true, force: true });
}

console.log(failures.length === 0 ? "cli-check: OK" : `cli-check: ${failures.length} failed`);
process.exit(failures.length === 0 ? 0 : 1);
