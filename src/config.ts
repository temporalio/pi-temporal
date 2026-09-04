// Temporal + Pi wiring, read from env. Nothing here is read at module load by the workflow
// (workflow code must stay deterministic); the client and worker read it.
//
// Two deployments, not a dozen knobs. `PI_TEMPORAL_PROFILE` picks one and everything that has to
// agree follows from it, because the settings are not independent: a fleet whose session directory
// is not shared, or whose project files do not travel, is not a fleet that works. It is one that
// answers with the wrong files. `preflight` says so before any work is accepted rather than after,
// and `pi-temporal doctor` prints what a process resolved.

import { readFileSync } from "node:fs";

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
  // certificate pair is what a self-hosted cluster with mTLS takes. Neither is a file path in the
  // process's argv or logs, because both are credentials.
  readonly apiKey?: string;
  readonly tls?: { readonly cert: string; readonly key: string; readonly ca?: string } | true;
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
    // In a fleet the files have to travel or a worker runs the tools against a directory that is
    // not the project and tells the model those files are it.
    shipTree: onOff("PI_TEMPORAL_SHIP_TREE", fleet),
    apiKey: process.env.PI_TEMPORAL_API_KEY ?? read(process.env.PI_TEMPORAL_API_KEY_FILE),
    tls:
      cert && key
        ? { cert: readFileSync(cert, "utf8"), key: readFileSync(key, "utf8"), ca: read(process.env.PI_TEMPORAL_TLS_CA) }
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

/**
 * What is wrong with this deployment, in the operator's words rather than in the failure it would
 * otherwise produce hours later. Every one of these has a failure that reads as something else: a
 * session directory nobody else can see reads as a worker that never picks anything up, and a
 * fleet with the tree off reads as a model that keeps being told the project is empty.
 */
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
  if (cfg.apiKey && LOOPBACK.test(cfg.address)) {
    problems.push(`an API key is set but TEMPORAL_ADDRESS is ${cfg.address}, which is a dev server`);
  }
  if (cfg.apiKey && cfg.namespace === "default") {
    problems.push("an API key is set but TEMPORAL_NAMESPACE is `default`, which is not a Cloud namespace");
  }
  if (!!process.env.PI_TEMPORAL_TLS_CERT !== !!process.env.PI_TEMPORAL_TLS_KEY) {
    problems.push("PI_TEMPORAL_TLS_CERT and PI_TEMPORAL_TLS_KEY come as a pair");
  }
  return problems;
}

/**
 * Worth saying, not worth refusing. The difference matters: a process that exits takes a deployment
 * with it, so only what cannot work belongs in `preflight`. Plaintext to an address that is not
 * loopback is a private network in most deployments and a mistake in some, and nothing here can
 * tell which.
 */
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
    shipTree: String(cfg.shipTree),
    credentials: cfg.apiKey ? "api key" : cfg.tls ? "certificate pair" : "none (plaintext)",
  };
}

export const sessionFileFor = (sessionDir: string, sessionId: string) =>
  `${sessionDir}/${sessionId}.jsonl`;
