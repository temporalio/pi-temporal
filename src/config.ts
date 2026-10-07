// Configuration from env, read by the client and worker. Workflow code must never import this.
// `PI_TEMPORAL_PROFILE` (`local` or `fleet`) sets defaults that go together. A misconfigured fleet
// serves the wrong files instead of failing, so `preflight` refuses what one process can detect.
// Whether storage is really shared across hosts is for the operator to verify.

import { readFileSync } from "node:fs";
import type { TurnBudget } from "./protocol.js";

export type Profile = "local" | "fleet";

export interface Config {
  readonly profile: Profile;
  readonly address: string;
  readonly namespace: string;
  readonly taskQueue: string;
  // Where each session's JSONL log lives. Must be shared storage in a fleet.
  readonly sessionDir: string;
  readonly idleTimeout: string;
  // One activity per model call, per tool call, and a seal, instead of one for the whole step.
  readonly stepped: boolean;
  // Ship the project's files with the session, so a worker on another host sees the last one's
  // work. Not needed on a single machine.
  readonly shipTree: boolean;
  // API key for Temporal Cloud, or a certificate pair for mTLS. Never printed by `describe`.
  readonly apiKey?: string;
  readonly tls?: { readonly cert: string; readonly key: string; readonly ca?: string } | true;
  // How long one tool call may run in stepped mode. Unset keeps the workflow's default.
  readonly toolTimeoutMinutes?: number;
  // Spend limits for a turn and a session. Unset means no limit.
  readonly budget?: TurnBudget;
}

const read = (path: string | undefined) => (path ? readFileSync(path, "utf8") : undefined);

// Set explicitly by the operator, not defaulted here. `preflight` needs the difference.
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
    // A fleet wants the smaller unit, so a dead worker loses one tool call, not a whole step.
    stepped: onOff("PI_TEMPORAL_STEPPED", fleet),
    toolTimeoutMinutes: minutesFromEnv("PI_TEMPORAL_TOOL_TIMEOUT_MINUTES"),
    budget: budgetFromEnv(),
    // In a fleet the files must travel, or tools run against the wrong directory.
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

// Only the stepped path fences off a timed-out tool that keeps writing. With whole steps, its late
// writes could be captured and shipped.
export const SHIP_TREE_NEEDS_STEPS =
  "PI_TEMPORAL_SHIP_TREE=1 needs PI_TEMPORAL_STEPPED=1: only the stepped path keeps the " +
  "writer markers and dispatch claims that fence a tool its activity stopped waiting for";

/** Client-side checks. `stepped` comes from whoever starts the session, so check it here. */
export function clientProblems(cfg: Config): string[] {
  return cfg.shipTree && !cfg.stepped ? [SHIP_TREE_NEEDS_STEPS] : [];
}

/** The model provider's key, from `<PROVIDER>_API_KEY` or `<PROVIDER>_API_KEY_FILE`. */
export function modelApiKey(
  provider = process.env.PI_TEMPORAL_PROVIDER ?? "openai",
): string | undefined {
  const prefix = provider === "anthropic" ? "ANTHROPIC" : "OPENAI";
  return process.env[`${prefix}_API_KEY`] ?? read(process.env[`${prefix}_API_KEY_FILE`])?.trim();
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
  // Reject typos instead of silently setting the wrong bound.
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${name} must be a whole number of ${unit}, got ${JSON.stringify(raw)}`);
  }
  return value;
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

export const sessionFileFor = (sessionDir: string, sessionId: string) =>
  `${sessionDir}/${sessionId}.jsonl`;
