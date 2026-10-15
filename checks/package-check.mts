// Checks the npm package the way someone who installs it gets it. It packs the package, installs
// the tarball in a new project, and runs the echo example from it, whole-step and stepped. A file
// left out of `files`, an `exports` path that names nothing, or a module path that only works in
// source then fails here and not after a publish.
//
// Needs a Temporal server and the npm registry, for the package's dependencies. No model key.
// Usage: npx tsx checks/package-check.mts

import { execFile, spawn } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const run = promisify(execFile);
const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

const repo = fileURLToPath(new URL("..", import.meta.url));
const dir = await mkdtemp(join(tmpdir(), "pi-package-"));
const env = {
  ...process.env,
  ECHO_TASK_QUEUE: `package-check-${process.pid}`,
  ECHO_SESSION_DIR: join(dir, "sessions"),
};
let worker: ReturnType<typeof spawn> | undefined;

try {
  // `prepack` builds `lib`, so this is the tarball `npm publish` would upload.
  const packed = await run("npm", ["pack", "--json", "--pack-destination", dir], {
    cwd: repo,
    maxBuffer: 16 * 1024 * 1024,
  });
  const [{ filename, files }] = JSON.parse(packed.stdout) as {
    filename: string;
    files: { path: string }[];
  }[];
  const paths = files.map((file) => file.path);
  const tops = new Set(paths.map((path) => path.split("/")[0]));
  const expected = ["LICENSE", "README.md", "SECURITY.md", "extensions", "lib", "package.json"];
  const allowed = new Set([...expected, "src"]);
  check(
    "the tarball holds the build, the source Pi loads, and the docs",
    expected.every((top) => tops.has(top)) && [...tops].every((top) => allowed.has(top)),
    [...tops],
  );

  await writeFile(join(dir, "package.json"), '{ "type": "module", "private": true }\n');
  await run(
    "npm",
    ["install", "--no-audit", "--no-fund", join(dir, filename), "tsx"],
    { cwd: dir, maxBuffer: 16 * 1024 * 1024 },
  );

  // The echo example, importing the package instead of the source tree.
  const example = join(dir, "echo");
  await mkdir(example);
  for (const name of ["agent", "worker", "send"]) {
    const source = await readFile(join(repo, "examples", "echo", `${name}.ts`), "utf8");
    const rewritten = source.replaceAll(
      /"\.\.\/\.\.\/src\/core\/([a-z-]+)\.js"/g,
      '"@temporalio/pi-temporal/core/$1"',
    );
    await writeFile(join(example, `${name}.ts`), rewritten);
  }

  const tsx = join(dir, "node_modules", ".bin", "tsx");
  worker = spawn(tsx, [join(example, "worker.ts")], { cwd: dir, env, stdio: "ignore" });
  const send = (text: string, stepped: boolean) =>
    run(tsx, [join(example, "send.ts"), text], {
      cwd: dir,
      env: { ...env, ECHO_STEPPED: stepped ? "1" : "0" },
      timeout: 120_000,
    }).then(
      ({ stdout }) => stdout,
      (err: { stdout?: string; stderr?: string }) => `${err.stdout ?? ""}${err.stderr ?? ""}`,
    );
  const whole = await send("from the tarball", false);
  const answered = whole.includes("answered: from the tarball");
  check("a whole-step turn runs from the package", answered, whole);
  const stepped = await send("stepped, from the tarball", true);
  check(
    "and a stepped one",
    stepped.includes("answered: stepped, from the tarball"),
    stepped,
  );

  const help = await run(join(dir, "node_modules", ".bin", "pi-temporal"), ["--help"], {
    cwd: dir,
  }).then(
    ({ stdout, stderr }) => `${stdout}${stderr}`,
    (err: { stdout?: string; stderr?: string }) => `${err.stdout ?? ""}${err.stderr ?? ""}`,
  );
  check("the pi-temporal bin runs", help.includes("usage: pi-temporal"), help.slice(0, 200));
} finally {
  worker?.kill();
  await rm(dir, { recursive: true, force: true });
}

const bad = failures.length;
console.log(bad === 0 ? "package-check: OK" : `package-check: ${bad} failed`);
process.exit(bad === 0 ? 0 : 1);
