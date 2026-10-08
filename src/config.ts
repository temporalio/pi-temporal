// Configuration from env, read by the client and Worker. Workflow code must never import this.
// `PI_TEMPORAL_PROFILE` (`local` or `fleet`) sets defaults that go together. A misconfigured fleet
// serves the wrong files instead of failing, so `preflight` refuses what one process can detect.
// Whether storage is really shared across hosts is for the operator to verify.

import { readFileSync } from "node:fs";
import { type Duration, msToNumber } from "@temporalio/common";
import { loadClientConnectConfig } from "@temporalio/envconfig";
import { homedir, userInfo } from "node:os";
import { resolve } from "node:path";
import type { TurnBudget } from "./core/protocol.js";

export type Profile = "local" | "fleet";

export interface Config {
  readonly profile: Profile;
  readonly address: string;
  readonly namespace: string;
  readonly taskQueue: string;
  // Where each session's JSONL log lives. Must be shared storage in a fleet.
  readonly sessionDir: string;
  readonly idleTimeout: string;
  // One Activity per model call, per tool call, and a seal, instead of one for the whole step.
  readonly stepped: boolean;
  // Ship the project's files with the session, so a Worker on another host sees the last one's
  // work. Not needed on a single machine.
  readonly shipTree: boolean;
  // Keep each session's state in the `PiSessionState` search attribute too. Needs it registered.
  readonly searchAttribute: boolean;
  // Encrypts payloads in history (`core/codec.ts`). 32 bytes, from base64.
  readonly codecKey?: Buffer;
  // OpenTelemetry tracing (`core/tracing.ts`). The standard `OTEL_*` variables configure it.
  readonly tracing: boolean;
  // Retired keys that still decrypt payloads sealed before a rotation. Never used to encrypt.
  readonly codecOldKeys?: readonly Buffer[];
  // API key for Temporal Cloud, or a certificate pair for mTLS. Never printed by `describe`.
  readonly apiKey?: string;
  readonly tls?: { readonly cert: string; readonly key: string; readonly ca?: string } | true;
  // Connection settings from the standard Temporal config: `TEMPORAL_*` variables and the profile
  // in `temporal.toml`. The `PI_TEMPORAL_*` settings above win over these.
  readonly standard?: StandardConnection;
  // Only one of the mTLS certificate and key was set. Kept here because the variables are dropped
  // from the environment once read, and no connection may go out with half a pair.
  readonly brokenTlsPair?: boolean;
  // How long one tool call may run in stepped mode. Unset keeps the Workflow's default.
  readonly toolTimeoutMinutes?: number;
  // How long a Worker's shutdown lets running Activities finish. A deploy should outlast most
  // model calls, so a step isn't cut off and paid for twice.
  readonly shutdownGrace: number;
  // Activity slots per Worker poller (`core/session-worker.ts`).
  readonly maxActivities: number;
  // Worker Versioning. Both `PI_TEMPORAL_DEPLOYMENT` and `PI_TEMPORAL_BUILD_ID`, or neither.
  readonly deployment?: { readonly name: string; readonly buildId: string };
  // Spend limits for a turn and a session. Unset means no limit.
  readonly budget?: TurnBudget;
}

const read = (path: string | undefined) => (path ? readFileSync(path, "utf8") : undefined);

// Set explicitly by the operator, not defaulted here. `preflight` needs the difference.
const given = (name: string) => process.env[name] !== undefined && process.env[name] !== "";

// A secret set directly, or else read from `<NAME>_FILE`. An empty direct value counts as unset,
// since deployments often declare an optional secret as empty.
const secret = (name: string) =>
  given(name) ? process.env[name] : read(process.env[`${name}_FILE`])?.trim();

// A typo must not quietly turn a switch off, so anything unrecognized is refused.
export function onOff(name: string, fallback: boolean): boolean {
  if (!given(name)) return fallback;
  const value = process.env[name]!.trim().toLowerCase();
  if (["1", "true", "on", "yes"].includes(value)) return true;
  if (["0", "false", "off", "no"].includes(value)) return false;
  throw new Error(
    `${name} must be 1/true/on/yes or 0/false/off/no, got ${JSON.stringify(process.env[name])}`,
  );
}

const localQueue = () => `pi-session-${userInfo().username.replace(/[^\w.-]/g, "_")}`;

function profileFromEnv(): Profile {
  const raw = process.env.PI_TEMPORAL_PROFILE;
  if (raw === undefined || raw === "" || raw === "local") return "local";
  if (raw === "fleet") return "fleet";
  throw new Error(`PI_TEMPORAL_PROFILE must be local or fleet, got ${JSON.stringify(raw)}`);
}

type StandardConnection = Omit<
  ReturnType<typeof loadClientConnectConfig>["connectionOptions"],
  "address"
>;

export function fromEnv(): Config {
  const profile = profileFromEnv();
  const fleet = profile === "fleet";
  const cert = process.env.PI_TEMPORAL_TLS_CERT;
  const key = process.env.PI_TEMPORAL_TLS_KEY;
  // The same settings every Temporal SDK and the `temporal` CLI read, so one profile serves all.
  const { connectionOptions: standardOptions, namespace } = loadClientConnectConfig();
  const { address, ...standard } = standardOptions;
  return {
    profile,
    address: address ?? "127.0.0.1:7233",
    namespace: namespace ?? "default",
    standard,
    // Per user on one machine, so two people sharing a dev server don't run each other's turns.
    // A fleet shares one queue on purpose.
    taskQueue: process.env.PI_TEMPORAL_TASK_QUEUE ?? (fleet ? "pi-session" : localQueue()),
    // Absolute, since it travels in Workflow input and the client and worker have different cwds.
    sessionDir: given("PI_SESSION_DIR")
      ? resolve(process.env.PI_SESSION_DIR!)
      : `${homedir()}/.pi-temporal/sessions`,
    idleTimeout: durationFromEnv("PI_SESSION_IDLE_TIMEOUT", "5 minutes"),
    // A fleet wants the smaller unit, so a dead worker loses one tool call, not a whole step.
    stepped: onOff("PI_TEMPORAL_STEPPED", fleet),
    searchAttribute: onOff("PI_TEMPORAL_SEARCH_ATTRIBUTE", false),
    tracing: onOff("PI_TEMPORAL_TRACING", false),
    ...codecKeyFromEnv(),
    toolTimeoutMinutes: minutesFromEnv("PI_TEMPORAL_TOOL_TIMEOUT_MINUTES"),
    shutdownGrace: (wholeFromEnv("PI_TEMPORAL_SHUTDOWN_GRACE_SECONDS", "seconds") ?? 60) * 1000,
    maxActivities: wholeFromEnv("PI_TEMPORAL_MAX_ACTIVITIES", "Activities") ?? 16,
    ...deploymentFromEnv(),
    budget: budgetFromEnv(),
    // In a fleet the files must travel, or tools run against the wrong directory.
    shipTree: onOff("PI_TEMPORAL_SHIP_TREE", fleet),
    apiKey: secret("PI_TEMPORAL_API_KEY"),
    tls:
      cert && key
        ? {
            cert: readFileSync(cert, "utf8"),
            key: readFileSync(key, "utf8"),
            ca: read(process.env.PI_TEMPORAL_TLS_CA),
          }
        : onOff("PI_TEMPORAL_TLS", false)
          ? true
          : undefined,
    // The standard config's pair too, which it otherwise skips without a word.
    brokenTlsPair: !cert !== !key || standardCert() !== standardKey(),
  };
}

// What reaches the Temporal control plane. Once `fromEnv` has read them, a process that runs
// agent tools drops them, so a tool doesn't inherit them. That's not isolation. A tool running as
// the same user can still read the process's original environment through `/proc`.
export const TEMPORAL_CREDENTIAL_VARS = [
  "PI_TEMPORAL_API_KEY",
  "PI_TEMPORAL_API_KEY_FILE",
  "PI_TEMPORAL_TLS_CERT",
  "PI_TEMPORAL_TLS_KEY",
  "PI_TEMPORAL_TLS_CA",
  "TEMPORAL_API_KEY",
  "TEMPORAL_TLS_CLIENT_CERT_DATA",
  "TEMPORAL_TLS_CLIENT_CERT_PATH",
  "TEMPORAL_TLS_CLIENT_KEY_DATA",
  "TEMPORAL_TLS_CLIENT_KEY_PATH",
  "TEMPORAL_CODEC_AUTH",
  "PI_TEMPORAL_CODEC_KEY",
  "PI_TEMPORAL_CODEC_KEY_FILE",
  "PI_TEMPORAL_CODEC_OLD_KEYS",
  "PI_TEMPORAL_CODEC_OLD_KEYS_FILE",
];

// A bad key must fail at start, not as payloads nobody can read.
function codecKeyFromEnv(): { codecKey?: Buffer; codecOldKeys?: Buffer[] } {
  const decoded = (name: string, encoded: string) => {
    const key = Buffer.from(encoded.trim(), "base64");
    if (key.length !== 32) throw new Error(`${name} must hold 32-byte keys, base64 encoded`);
    return key;
  };
  const encoded = secret("PI_TEMPORAL_CODEC_KEY");
  const old = secret("PI_TEMPORAL_CODEC_OLD_KEYS");
  // Old keys only decrypt. Without a current key nothing is encrypted, which a rotation never
  // means, so the pair is refused.
  if (encoded === undefined) {
    if (old !== undefined) {
      throw new Error("PI_TEMPORAL_CODEC_OLD_KEYS needs PI_TEMPORAL_CODEC_KEY");
    }
    return {};
  }
  return {
    codecKey: decoded("PI_TEMPORAL_CODEC_KEY", encoded),
    codecOldKeys: (old ?? "")
      .split(",")
      .filter((each) => each.trim() !== "")
      .map((each) => decoded("PI_TEMPORAL_CODEC_OLD_KEYS", each)),
  };
}

export function dropFromEnv(names: readonly string[]): void {
  for (const name of names) delete process.env[name];
  // gRPC headers from the standard config, which often carry an `authorization` header.
  for (const name of Object.keys(process.env)) {
    if (name.startsWith("TEMPORAL_GRPC_META_")) delete process.env[name];
  }
}

/** What the SDK's `Connection.connect` and `NativeConnection.connect` both take. */
export function connectionOptions(cfg: Config) {
  // Without the key, the certificate is ignored and the client connects as nobody.
  if (cfg.brokenTlsPair) throw new Error(TLS_PAIR);
  // Ours are added to the profile's TLS settings, so its CA or server name isn't lost.
  const profileTls = typeof cfg.standard?.tls === "object" ? cfg.standard.tls : {};
  const tls =
    cfg.tls === true || (cfg.apiKey && cfg.tls === undefined)
      ? (cfg.standard?.tls ?? true)
      : cfg.tls
        ? {
            ...profileTls,
            clientCertPair: { crt: Buffer.from(cfg.tls.cert), key: Buffer.from(cfg.tls.key) },
            ...(cfg.tls.ca ? { serverRootCACertificate: Buffer.from(cfg.tls.ca) } : {}),
          }
        : undefined;
  return {
    ...cfg.standard,
    address: cfg.address,
    ...(tls ? { tls } : {}),
    ...(cfg.apiKey ? { apiKey: cfg.apiKey } : {}),
  };
}

const standardCert = () =>
  given("TEMPORAL_TLS_CLIENT_CERT_PATH") || given("TEMPORAL_TLS_CLIENT_CERT_DATA");
const standardKey = () =>
  given("TEMPORAL_TLS_CLIENT_KEY_PATH") || given("TEMPORAL_TLS_CLIENT_KEY_DATA");

const TLS_PAIR =
  "a client certificate and its key come as a pair: PI_TEMPORAL_TLS_CERT with " +
  "PI_TEMPORAL_TLS_KEY, and TEMPORAL_TLS_CLIENT_CERT_* with TEMPORAL_TLS_CLIENT_KEY_*";

// From either source of settings.
const hasApiKey = (cfg: Config) => Boolean(cfg.apiKey ?? cfg.standard?.apiKey);

const LOOPBACK = /^(127\.0\.0\.1|localhost|\[::1\]|0\.0\.0\.0)(:|$)/;

/** Configuration checks cannot establish that a directory is shared or that hosts agree. */
export function preflight(cfg: Config): string[] {
  const problems: string[] = [];
  if (cfg.profile === "fleet") {
    if (!given("PI_SESSION_DIR")) {
      problems.push(
        "the fleet profile needs PI_SESSION_DIR on storage every worker can reach: the session " +
          "log is the record, and a worker that cannot read it cannot serve the session",
      );
    }
    if (!cfg.shipTree) {
      problems.push(
        "PI_TEMPORAL_SHIP_TREE=0 in a fleet: the transcript would travel and the project's files " +
          "would not, so a worker runs the tools in whatever directory it was pointed at",
      );
    }
  }
  problems.push(...clientProblems(cfg));
  if (hasApiKey(cfg) && LOOPBACK.test(cfg.address)) {
    problems.push(
      `an API key is set but TEMPORAL_ADDRESS is ${cfg.address}, which is a dev server`,
    );
  }
  if (hasApiKey(cfg) && cfg.namespace === "default") {
    problems.push(
      "an API key is set but TEMPORAL_NAMESPACE is `default`, which is not a Cloud namespace",
    );
  }
  if (cfg.brokenTlsPair) problems.push(TLS_PAIR);
  return problems;
}

// Only the stepped path fences off a timed-out tool that keeps writing. With whole steps, its late
// writes could be captured and shipped.
export const SHIP_TREE_NEEDS_STEPS =
  "PI_TEMPORAL_SHIP_TREE=1 needs PI_TEMPORAL_STEPPED=1: only the stepped path keeps the " +
  "writer markers and dispatch claims that fence a tool its activity stopped waiting for";

/** Client-side checks. `stepped` comes from whoever starts the session, so check it here. */
export function clientProblems(cfg: Config): string[] {
  return cfg.shipTree && !cfg.stepped ? [SHIP_TREE_NEEDS_STEPS] : [];
}

/**
 * The model provider's key, from `<PROVIDER>_API_KEY` or `<PROVIDER>_API_KEY_FILE`. Only `openai`
 * and `anthropic` are known, so any other provider gets no key rather than one of theirs.
 */
export function modelApiKey(
  provider = process.env.PI_TEMPORAL_PROVIDER ?? "openai",
): string | undefined {
  if (provider !== "openai" && provider !== "anthropic") return undefined;
  return secret(`${provider.toUpperCase()}_API_KEY`);
}

/** Plaintext can be intentional on a private network, so it is a note rather than a refusal. */
export function notes(cfg: Config): string[] {
  const said: string[] = [];
  if (!LOOPBACK.test(cfg.address) && !hasApiKey(cfg) && !cfg.tls && !cfg.standard?.tls) {
    said.push(
      `reaching ${cfg.address} in plaintext. For Temporal Cloud set PI_TEMPORAL_API_KEY; for a ` +
        "cluster with mTLS set PI_TEMPORAL_TLS_CERT and PI_TEMPORAL_TLS_KEY",
    );
  }
  return said;
}

/** Every setting that decides how this process behaves, and nothing that is a credential. */
export function describe(cfg: Config): Record<string, string> {
  return {
    profile: cfg.profile,
    address: cfg.address,
    namespace: cfg.namespace,
    taskQueue: cfg.taskQueue,
    sessionDir: cfg.sessionDir,
    idleTimeout: cfg.idleTimeout,
    stepped: String(cfg.stepped),
    toolTimeoutMinutes: cfg.toolTimeoutMinutes ? `${cfg.toolTimeoutMinutes} minutes` : "default",
    shipTree: String(cfg.shipTree),
    maxActivities: String(cfg.maxActivities),
    tracing: String(cfg.tracing),
    deployment: cfg.deployment
      ? `${cfg.deployment.name}.${cfg.deployment.buildId}`
      : "unversioned",
    budget: cfg.budget
      ? Object.entries(cfg.budget)
          .map(([name, value]) => `${name}=${value}`)
          .join(", ")
      : "none",
    payloads: cfg.codecKey ? "encrypted" : "plain",
    credentials: cfg.apiKey
      ? "api key"
      : cfg.tls
        ? "certificate pair"
        : cfg.standard?.apiKey
          ? "api key (standard config)"
          : cfg.standard?.tls
            ? "tls (standard config)"
            : "none (plaintext)",
  };
}

export const minutesFromEnv = (name: string) => wholeFromEnv(name, "minutes");

// Checked here, since the Workflow parses it. A typo found there fails every Workflow Task of the
// session, and its prompts wait as Signals until someone notices.
function durationFromEnv(name: string, fallback: string): string {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return fallback;
  let ms: number | undefined;
  try {
    ms = msToNumber(raw as Duration);
  } catch {
    ms = undefined;
  }
  if (ms === undefined || !Number.isFinite(ms) || ms <= 0) {
    throw new Error(
      `${name} must be a duration such as "5 minutes" or "30s", got ${JSON.stringify(raw)}`,
    );
  }
  return raw;
}

function wholeFromEnv(name: string, unit: string): number | undefined {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return undefined;
  const value = Number(raw);
  // Reject typos instead of silently setting the wrong bound.
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${name} must be a whole number of ${unit}, got ${JSON.stringify(raw)}`);
  }
  return value;
}

function deploymentFromEnv(): Pick<Config, "deployment"> {
  const name = process.env.PI_TEMPORAL_DEPLOYMENT;
  const buildId = process.env.PI_TEMPORAL_BUILD_ID;
  if (!name && !buildId) return {};
  // Half a version would start an unversioned Worker that the deployment never routes to.
  if (!name || !buildId) {
    throw new Error("PI_TEMPORAL_DEPLOYMENT and PI_TEMPORAL_BUILD_ID must be set together");
  }
  return { deployment: { name, buildId } };
}

// Read where a session starts and passed in its options, since workflows can't read env.
function budgetFromEnv(): TurnBudget | undefined {
  const budget: TurnBudget = {
    tokens: wholeFromEnv("PI_TEMPORAL_BUDGET_TOKENS", "tokens"),
    seconds: wholeFromEnv("PI_TEMPORAL_BUDGET_SECONDS", "seconds"),
    hardSeconds: wholeFromEnv("PI_TEMPORAL_BUDGET_HARD_SECONDS", "seconds"),
    sessionTokens: wholeFromEnv("PI_TEMPORAL_BUDGET_SESSION_TOKENS", "tokens"),
    sessionSeconds: wholeFromEnv("PI_TEMPORAL_BUDGET_SESSION_SECONDS", "seconds"),
  };
  const set = Object.entries(budget).filter(([, value]) => value !== undefined);
  return set.length > 0 ? (Object.fromEntries(set) as TurnBudget) : undefined;
}

export { sessionFileFor } from "./core/protocol.js";
