// Temporal + Pi wiring, read from env. Nothing here is read at module load by the workflow
// (workflow code must stay deterministic); the client and worker read it.

export interface Config {
  readonly address: string;
  readonly namespace: string;
  readonly taskQueue: string;
  // Directory where each session's JSONL log lives. Point this at shared storage for a fleet.
  readonly sessionDir: string;
  readonly idleTimeout: string;
  // One activity per model call, per tool call, and a seal, instead of one for the whole step.
  readonly stepped: boolean;
  // How long one tool call may run in stepped mode. Unset keeps the workflow's default.
  readonly toolTimeoutMinutes?: number;
}

export function fromEnv(): Config {
  return {
    address: process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7233",
    namespace: process.env.TEMPORAL_NAMESPACE ?? "default",
    taskQueue: process.env.PI_TEMPORAL_TASK_QUEUE ?? "pi-session",
    sessionDir: process.env.PI_SESSION_DIR ?? `${process.env.HOME}/.pi-temporal/sessions`,
    idleTimeout: process.env.PI_SESSION_IDLE_TIMEOUT ?? "5 minutes",
    stepped: process.env.PI_TEMPORAL_STEPPED === "1",
    toolTimeoutMinutes: minutesFromEnv("PI_TEMPORAL_TOOL_TIMEOUT_MINUTES"),
  };
}

export function minutesFromEnv(name: string): number | undefined {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return undefined;
  const minutes = Number(raw);
  // A typo here would otherwise become a timeout nobody asked for.
  if (!Number.isInteger(minutes) || minutes <= 0) {
    throw new Error(`${name} must be a whole number of minutes, got ${JSON.stringify(raw)}`);
  }
  return minutes;
}

export const sessionFileFor = (sessionDir: string, sessionId: string) =>
  `${sessionDir}/${sessionId}.jsonl`;
