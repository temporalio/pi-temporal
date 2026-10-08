// Holds that settings read from env mean the same in every process. Needs neither a server nor a
// model key.
import assert from "node:assert/strict";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { connectionOptions, dropFromEnv, fromEnv, modelApiKey, preflight } from "../src/config.js";

const dir = await mkdtemp(join(tmpdir(), "pi-config-"));
const keyFile = join(dir, "key");
await writeFile(keyFile, "from-file\n");

// A relative session dir travels in Workflow input, so it must not depend on who reads it.
process.env.PI_SESSION_DIR = "sessions";
assert.equal(fromEnv().sessionDir, join(process.cwd(), "sessions"));
console.log("PASS a relative session directory is made absolute");

// An optional secret declared empty must not hide the file next to it.
process.env.PI_TEMPORAL_API_KEY = "";
process.env.PI_TEMPORAL_API_KEY_FILE = keyFile;
assert.equal(fromEnv().apiKey, "from-file");
process.env.OPENAI_API_KEY = "";
process.env.OPENAI_API_KEY_FILE = keyFile;
assert.equal(modelApiKey("openai"), "from-file");
process.env.OPENAI_API_KEY = "direct";
assert.equal(modelApiKey("openai"), "direct");
console.log("PASS an empty key falls back to its file");

// Half an mTLS pair would connect without the certificate. The variables are dropped once read, so
// the refusal must not depend on them still being there.
process.env.PI_TEMPORAL_TLS_CERT = keyFile;
delete process.env.PI_TEMPORAL_TLS_KEY;
const half = fromEnv();
dropFromEnv(["PI_TEMPORAL_TLS_CERT"]);
assert.throws(() => connectionOptions(half), /come as a pair/);
assert.ok(preflight(half).some((problem) => /come as a pair/.test(problem)));
console.log("PASS half an mTLS pair never connects");

// The standard Temporal settings work too, and the `PI_TEMPORAL_*` ones win over them.
delete process.env.PI_TEMPORAL_API_KEY;
delete process.env.PI_TEMPORAL_API_KEY_FILE;
process.env.TEMPORAL_ADDRESS = "example.tmprl.cloud:7233";
process.env.TEMPORAL_NAMESPACE = "example.a1b2c";
process.env.TEMPORAL_API_KEY = "standard-key";
const standard = fromEnv();
assert.equal(standard.address, "example.tmprl.cloud:7233");
assert.equal(standard.namespace, "example.a1b2c");
assert.equal(connectionOptions(standard).apiKey, "standard-key");
process.env.PI_TEMPORAL_API_KEY = "ours";
assert.equal(connectionOptions(fromEnv()).apiKey, "ours");
console.log("PASS the standard Temporal settings apply, under the PI_TEMPORAL ones");

// A profile's TLS settings stay when an API key or our certificate pair is added on top.
delete process.env.PI_TEMPORAL_API_KEY;
process.env.TEMPORAL_TLS_SERVER_NAME = "tls.example";
process.env.PI_TEMPORAL_API_KEY = "ours";
const named = connectionOptions(fromEnv()).tls as { serverNameOverride?: string } | boolean;
assert.equal(typeof named === "object" && named.serverNameOverride, "tls.example");
delete process.env.TEMPORAL_TLS_SERVER_NAME;
console.log("PASS a profile's TLS settings stay under an API key");

// gRPC headers from the standard config can carry credentials, so tools don't get them either.
process.env.TEMPORAL_GRPC_META_AUTHORIZATION = "Bearer secret";
dropFromEnv([]);
assert.equal(process.env.TEMPORAL_GRPC_META_AUTHORIZATION, undefined);
console.log("PASS gRPC headers are dropped with the credentials");

// The standard config's certificate without its key is refused like ours.
process.env.TEMPORAL_TLS_CLIENT_CERT_DATA = "not a real certificate";
assert.ok(preflight(fromEnv()).some((problem) => /come as a pair/.test(problem)));
delete process.env.TEMPORAL_TLS_CLIENT_CERT_DATA;
console.log("PASS half the standard certificate pair is refused");
