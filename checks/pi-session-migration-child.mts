// Fault-injection child for pi-session-open-check and issue #54. Only writes its fixture directory.
import fs from "node:fs";
import { syncBuiltinESMExports } from "node:module";
import { dirname, resolve, sep } from "node:path";
import { SessionManager } from "@earendil-works/pi-coding-agent";

const file = resolve(process.argv[2]);
const directory = `${dirname(file)}${sep}`;
const writers = new Set<number>();
const openBefore = fs.openSync;
const writeBefore = fs.writeFileSync;
const writeSyncBefore = fs.writeSync;
const appendBefore = fs.appendFileSync;
const fixturePath = (path: unknown) => typeof path === "string" && resolve(path).startsWith(directory);
// Do not stop at a lock/sidecar write: that would retain the original without exercising migration.
const sessionPayload = (data: unknown) => {
  const text = typeof data === "string" ? data : Buffer.isBuffer(data) ? data.toString("utf8") : "";
  return text.includes('"type":"session"');
};
const crash = () => {
  process.stdout.write("CRASH_AFTER_FIRST_SESSION_WRITE\n");
  process.exit(86);
};

fs.openSync = (path, flags, mode) => {
  const fd = openBefore(path, flags, mode);
  if (fixturePath(path) && typeof flags === "string" && /^[wa]/.test(flags)) writers.add(fd);
  return fd;
};
fs.writeFileSync = (path, data, options) => {
  writeBefore(path, data, options);
  if (sessionPayload(data) && ((typeof path === "number" && writers.has(path)) || fixturePath(path))) crash();
};
fs.writeSync = (...args: unknown[]) => {
  const bytes = Reflect.apply(writeSyncBefore, fs, args) as number;
  if (bytes > 0 && typeof args[0] === "number" && writers.has(args[0]) && sessionPayload(args[1])) crash();
  return bytes;
};
fs.appendFileSync = (path, data, options) => {
  appendBefore(path, data, options);
  if (sessionPayload(data) && ((typeof path === "number" && writers.has(path)) || fixturePath(path))) crash();
};
syncBuiltinESMExports();

const manager = SessionManager.open(file);
manager.setWriteGuard(() => {});
manager.appendCustomEntry("after-migration", { keep: "the triggering write" });
throw new Error("the fixture completed without reaching a session write");
