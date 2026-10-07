// Holds that a session id names one file inside the session directory. Ids reach the path from
// clients, the CLI, and schedules, so one that climbs out would write and delete elsewhere.
// Needs neither a server nor a model key.
import assert from "node:assert/strict";
import { sessionFileFor } from "../src/config.js";

for (const bad of ["", ".", "..", "../other", "a/b", "a\\b", "a\0b"]) {
  assert.throws(() => sessionFileFor("/sessions", bad), /can't be used/, JSON.stringify(bad));
}
assert.equal(sessionFileFor("/sessions", "task-1a2b"), "/sessions/task-1a2b.jsonl");
// A name past the file system's limit makes a session nobody can write. Bytes, not characters.
assert.throws(() => sessionFileFor("/sessions", "x".repeat(201)), /longer than 200 bytes/);
assert.throws(() => sessionFileFor("/sessions", "é".repeat(101)), /longer than 200 bytes/);
assert.equal(sessionFileFor("/sessions", "é".repeat(100)), `/sessions/${"é".repeat(100)}.jsonl`);
console.log("PASS a session id can only name one file in the session directory");
