// Temporal + Pi wiring, read from env. Nothing here is read at module load by the workflow
// (workflow code must stay deterministic); the client and worker read it.
//
// Two deployments, not a dozen knobs. `PI_TEMPORAL_PROFILE` picks one and the settings that go
// with it follow, because they are not independent: a fleet whose session directory is not shared,
// or whose project files do not travel, answers with the wrong files rather than failing.
//
// What a profile cannot do is check the fleet. `preflight` reads this process's own configuration
// and refuses what one process can be refused for, before it accepts work rather than after, and
// `pi-temporal doctor` prints what this process resolved. That the storage is really shared, that
// placement is what you think, and that the other hosts agree are the operator's to verify.

import { readFileSync } from "node:fs";
import type { TurnBudget } from "./protocol.js";

export type Profile = "local" | "fleet";

export interface Config {
  readonly profile: Profile;
  readonly address: string;
  readonly namespace: string;
  readonly taskQueue: string;
  // Directory where each session's JSONL log lives. Shared storage in a fleet: it is the record,
  // and a worker that cannot reach it cannot serve the session at all.
  readonly sessionDir: string;
  readonly idleTimeout: string;
  // One activity per model call, per tool call, and a seal, instead of one for the whole step.
  readonly stepped: boolean;
  // Ship the project's files with the session, so a worker on another machine finds the work the
  // last one did. On a laptop the tools already run in the directory you meant, and shipping it
  // there is disk spent on a problem that host does not have.
  readonly shipTree: boolean;
  // How a server that is not the dev server is reached. An API key is what Temporal Cloud takes; a
  // certificate pair is what a self-hosted cluster with mTLS takes. Both are read from files rather
  // than carried as values, so what `describe` prints is the path and never the secret.
  readonly apiKey?: string;
  readonly tls?: { readonly cert: string; readonly key: string; readonly ca?: string } | true;
  // How long one tool call may run in stepped mode. Unset keeps the workflow's default.
  readonly toolTimeoutMinutes?: number;
  // What a turn and a session may spend before the workflow stops driving them. Unset is no bound.
  readonly budget?: TurnBudget;
}

const read = (path: string | undefined) => (path ? readFileSync(path, "utf8") : undefined);

// Set explicitly by an operator, as opposed to a default this file invented. The difference decides
// whether a fleet is configured or only looks configured.
const given = (name: string) => process.env[name] !== undefined && process.env[name] !== "";

const onOff = (name: string, fallback: boolean) =>
  given(name) ? process.env[name] === "1" : fallback;

export function fromEnv(): Config {
  const profile: Profile = process.env.PI_TEMPORAL_PROFILE === "fleet" ? "fleet" : "local";
  const fleet = profile === "fleet";
  const cert = process.env.PI_TEMPORAL_TLS_CERT;
  const key = process.env.PI_TEMPORAL_TLS_KEY;
  return {
    profile,
    address: process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7233",
    namespace: process.env.TEMPORAL_NAMESPACE ?? "default",
    taskQueue: process.env.PI_TEMPORAL_TASK_QUEUE ?? "pi-session",
    sessionDir: process.env.PI_SESSION_DIR ?? `${process.env.HOME}/.pi-temporal/sessions`,
    idleTimeout: process.env.PI_SESSION_IDLE_TIMEOUT ?? "5 minutes",
    // The unit of work a fleet wants is the smaller one: a worker dying takes one tool call with it
    // rather than a whole step, and a tool call is where the retry policy and the approval belong.
    stepped: onOff("PI_TEMPORAL_STEPPED", fleet),
    toolTimeoutMinutes: minutesFromEnv("PI_TEMPORAL_TOOL_TIMEOUT_MINUTES"),
    budget: budgetFromEnv(),
    // In a fleet the files have to travel or a worker runs the tools against a directory that is
    // not the project and tells the model those files are it.
    shipTree: onOff("PI_TEMPORAL_SHIP_TREE", fleet),
    apiKey: process.env.PI_TEMPORAL_API_KEY ?? read(process.env.PI_TEMPORAL_API_KEY_FILE),
    tls:
      cert && key
        ? {
            cert: readFileSync(cert, "utf8"),
            key: readFileSync(key, "utf8"),
            ca: read(process.env.PI_TEMPORAL_TLS_CA),
          }
        : process.env.PI_TEMPORAL_TLS === "1"
          ? true
          : undefined,
  };
}

/** What the SDK's `Connection.connect` and `NativeConnection.connect` both take. */
export function connectionOptions(cfg: Config) {
  const tls =
    cfg.tls === true || (cfg.apiKey && cfg.tls === undefined)
      ? true
      : cfg.tls
        ? {
            clientCertPair: { crt: Buffer.from(cfg.tls.cert), key: Buffer.from(cfg.tls.key) },
            ...(cfg.tls.ca ? { serverRootCACertificate: Buffer.from(cfg.tls.ca) } : {}),
          }
        : undefined;
  return {
    address: cfg.address,
    ...(tls ? { tls } : {}),
    ...(cfg.apiKey ? { apiKey: cfg.apiKey } : {}),
  };
}

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
  if (cfg.apiKey && LOOPBACK.test(cfg.address)) {
    problems.push(
      `an API key is set but TEMPORAL_ADDRESS is ${cfg.address}, which is a dev server`,
    );
  }
  if (cfg.apiKey && cfg.namespace === "default") {
    problems.push(
      "an API key is set but TEMPORAL_NAMESPACE is `default`, which is not a Cloud namespace",
    );
  }
  if (!!process.env.PI_TEMPORAL_TLS_CERT !== !!process.env.PI_TEMPORAL_TLS_KEY) {
    problems.push("PI_TEMPORAL_TLS_CERT and PI_TEMPORAL_TLS_KEY come as a pair");
  }
  return problems;
}

// The writer markers and the closed-step fence exist only on the stepped path: a whole step never
// says which calls are inside their own execution, so a timed-out attempt's tool can keep writing a
// directory a later restore brings back to the tip, and the next capture ships what it wrote with
// nothing to refuse it.
export const SHIP_TREE_NEEDS_STEPS =
  "PI_TEMPORAL_SHIP_TREE=1 needs PI_TEMPORAL_STEPPED=1: only the stepped path keeps the " +
  "writer markers and dispatch claims that fence a tool its activity stopped waiting for";

/**
 * What a client can be refused for from its own environment. `stepped` rides the session's input
 * from whoever starts it, so a client that would start a whole-step session for shipping workers
 * finds out here rather than on the first activity.
 */
export function clientProblems(cfg: Config): string[] {
  return cfg.shipTree && !cfg.stepped ? [SHIP_TREE_NEEDS_STEPS] : [];
}

/** The model provider's key, from the environment or from a file, the way the scripts take it. */
export function modelApiKey(): string | undefined {
  return process.env.OPENAI_API_KEY ?? read(process.env.OPENAI_API_KEY_FILE)?.trim();
}

/** Plaintext can be intentional on a private network, so it is a note rather than a refusal. */
export function notes(cfg: Config): string[] {
  const said: string[] = [];
  if (!LOOPBACK.test(cfg.address) && !cfg.apiKey && !cfg.tls) {
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
    budget: cfg.budget
      ? Object.entries(cfg.budget)
          .map(([name, value]) => `${name}=${value}`)
          .join(", ")
      : "none",
    credentials: cfg.apiKey ? "api key" : cfg.tls ? "certificate pair" : "none (plaintext)",
  };
}

export const minutesFromEnv = (name: string) => wholeFromEnv(name, "minutes");

function wholeFromEnv(name: string, unit: string): number | undefined {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return undefined;
  const value = Number(raw);
  // A typo here would otherwise become a bound nobody asked for, or none at all.
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${name} must be a whole number of ${unit}, got ${JSON.stringify(raw)}`);
  }
  return value;
}

// Read where a session starts and carried in its options, because the workflow may not read the
// environment. Each bound is separate, and none is set unless an operator sets it.
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

export const sessionFileFor = (sessionDir: string, sessionId: string) =>
  `${sessionDir}/${sessionId}.jsonl`;
