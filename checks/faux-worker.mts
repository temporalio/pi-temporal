// Not a check. A worker with the real activities over a real `AgentSession` and a scripted model,
// run as a child process so checks can kill or pause it mid-activity. Every choice it makes comes
// from the transcript and marker files, never memory, so a retry on a fresh process repeats it.
//
// Run as a worker: npx tsx checks/faux-worker.mts --queue=<task queue> --dir=<scratch dir>

import { appendFileSync, existsSync, mkdirSync, realpathSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { Context } from "@temporalio/activity";
import { NativeConnection, Worker } from "@temporalio/worker";
import {
  type AgentSession,
  createAgentSession,
  DefaultResourceLoader,
  ModelRuntime,
  SessionManager,
  SettingsManager,
  type ToolDefinition,
} from "@earendil-works/pi-coding-agent";
import { makeActivities } from "../src/activities.js";

// Load the agent's own copies by path. The provider registry and stream class are module state.
const codingAgent = new URL("../node_modules/@earendil-works/pi-coding-agent", import.meta.url);
const forkPackages = join(realpathSync(fileURLToPath(codingAgent)), "..");
const piAi = await import(pathToFileURL(join(forkPackages, "ai", "dist", "index.js")).href);
const typebox = await import(
  pathToFileURL(join(forkPackages, "..", "node_modules", "typebox", "build", "index.mjs")).href
);

export const PROVIDER = "faux-check";
const MODEL_ID = "faux-check-mini";

/** Scenario tokens a prompt carries. The worker reads them back out of the transcript. */
export const SCENARIO = {
  killSeal: "[kill-seal]",
  pauseModel: "[pause-model]",
  continueOnce: "[continue-once]",
  twoCalls: "[two-calls]",
} as const;

/** Files a scenario leaves in the scratch directory, for the check to read. */
export const files = (dir: string) => ({
  probes: join(dir, "probe.log"),
  probesStarted: join(dir, "probe-started.log"),
  // While present, the probe is slow enough to stop the turn inside it.
  slowProbe: join(dir, "slow-probe"),
  modelCalls: join(dir, "model.log"),
  settled: join(dir, "settle.log"),
  refused: join(dir, "refused.log"),
  sealKillPoint: join(dir, "seal-kill-point"),
  modelPausePoint: join(dir, "model-pause-point"),
});

/** True the first time it is asked across every process sharing `path`. */
function firstTime(path: string): boolean {
  try {
    writeFileSync(path, String(process.pid), { flag: "wx" });
    return true;
  } catch {
    return false;
  }
}

type Message = { role: string; content?: unknown };
const textOf = (content: unknown): string =>
  typeof content === "string"
    ? content
    : Array.isArray(content)
      ? content.map((b: { text?: string }) => b?.text ?? "").join("")
      : "";
const promptOf = (messages: readonly Message[]) =>
  textOf(messages.find((m) => m.role === "user")?.content);

/** One answer from the transcript alone: call the probe until a result is in, then answer. */
function respond(messages: readonly Message[]) {
  const answered = messages.some((m) => m.role === "toolResult");
  if (!answered) {
    const calls = promptOf(messages).includes(SCENARIO.twoCalls) ? ["first", "second"] : ["once"];
    return piAi.fauxAssistantMessage(
      calls.map((note) => piAi.fauxToolCall("probe", { note })),
      { stopReason: "toolUse" },
    );
  }
  return piAi.fauxAssistantMessage("all done");
}

function scriptedStream(dir: string) {
  return (
    model: { api: string; provider: string; id: string },
    context: { messages: Message[] },
  ) => {
    const stream = piAi.createAssistantMessageEventStream();
    void (async () => {
      appendFileSync(files(dir).modelCalls, `${process.pid}\n`);
      // Gives the check time to SIGSTOP this process. The append after it must then be refused.
      if (
        promptOf(context.messages).includes(SCENARIO.pauseModel) &&
        firstTime(files(dir).modelPausePoint)
      ) {
        await new Promise((resolve) => setTimeout(resolve, 4_000));
      }
      const message = {
        ...respond(context.messages),
        api: model.api,
        provider: model.provider,
        model: model.id,
      };
      stream.push({ type: "start", partial: { ...message, content: [] } });
      stream.push({ type: "done", reason: message.stopReason, message });
    })();
    return stream;
  };
}

function probeTool(dir: string): ToolDefinition {
  return {
    name: "probe",
    label: "Probe",
    description: "Records that it ran",
    parameters: typebox.Type.Object({ note: typebox.Type.String() }),
    async execute(toolCallId: string) {
      appendFileSync(files(dir).probesStarted, `${toolCallId}\n`);
      if (existsSync(files(dir).slowProbe)) {
        await new Promise((resolve) => setTimeout(resolve, 8_000));
      }
      appendFileSync(files(dir).probes, `${toolCallId}\n`);
      return { content: [{ type: "text", text: "probed" }], details: {} };
    },
  } as unknown as ToolDefinition;
}

const branchMessages = (ctx: { sessionManager: { getBranch(): unknown[] } }) =>
  ctx.sessionManager
    .getBranch()
    .flatMap((e) => {
      const entry = e as { type: string; message?: Message; customType?: string };
      if (entry.type === "message" && entry.message) return [entry.message];
      if (entry.type === "custom_message") return [{ role: `custom:${entry.customType}` }];
      return [];
    });

// Each boundary leaves a marker entry in the session file, so one that ran twice shows as two.
function scenarioExtension(dir: string) {
  return (pi: {
    on(event: string, handler: (event: unknown, ctx: never) => unknown): void;
  }) => {
    pi.on("turn_end", (event, ctx) => {
      const { message } = event as { message: { content?: { type?: string }[] } };
      const asked = (message.content ?? []).some((b) => b?.type === "toolCall");
      if (asked) return undefined;
      const seen = branchMessages(ctx as never);
      const entries: unknown[] = [{ type: "custom", customType: "check-turn-end" }];
      if (
        promptOf(seen).includes(SCENARIO.continueOnce) &&
        !seen.some((m) => m.role === "custom:check-next-work")
      ) {
        entries.push({
          type: "custom_message",
          customType: "check-next-work",
          content: "one more step",
          display: false,
        });
        return { entries, continue: true };
      }
      return { entries };
    });
    pi.on("agent_before_settle", async (_event, ctx) => {
      let activity = "outside an activity";
      try {
        activity = Context.current().info.activityType;
      } catch {
        // not inside an activity
      }
      appendFileSync(files(dir).settled, `${process.pid} ${activity}\n`);
      const seen = branchMessages(ctx as never);
      // Entries are written but the activity has not returned. The check kills this process here.
      if (promptOf(seen).includes(SCENARIO.killSeal) && firstTime(files(dir).sealKillPoint)) {
        await new Promise(() => {});
      }
      return { entries: [{ type: "custom", customType: "check-before-settle" }] };
    });
  };
}

/** The real session the production activity would open, with the scripted model in it. */
export function fauxOpenSession(dir: string, projectDir: string) {
  return async (sessionFile: string, guard?: () => void): Promise<AgentSession> => {
    mkdirSync(dirname(sessionFile), { recursive: true });
    const sessionManager = SessionManager.open(sessionFile);
    // Wrap the guard to log each refusal for `fence-check.mts`, then rethrow.
    sessionManager.setWriteGuard(
      guard &&
        (() => {
          try {
            guard();
          } catch (err) {
            appendFileSync(files(dir).refused, `${process.pid} ${String(err)}\n`);
            throw err;
          }
        }),
    );
    const agentDir = join(dir, "agent");
    mkdirSync(agentDir, { recursive: true });
    const modelRuntime = await ModelRuntime.create({
      authPath: join(agentDir, "auth.json"),
      modelsPath: null,
    });
    modelRuntime.registerProvider(PROVIDER, {
      baseUrl: "http://faux.invalid",
      apiKey: "faux-key",
      api: "faux-check-api",
      streamSimple: scriptedStream(dir) as never,
      models: [
        {
          id: MODEL_ID,
          name: "Faux check model",
          reasoning: false,
          input: ["text"],
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
          contextWindow: 200_000,
          maxTokens: 8_192,
        },
      ],
    });
    const model = (await modelRuntime.getAvailable(PROVIDER)).find((m) => m.id === MODEL_ID);
    if (!model) throw new Error("the scripted provider registered no model");
    const resourceLoader = new DefaultResourceLoader({
      cwd: projectDir,
      agentDir,
      extensionFactories: [scenarioExtension(dir) as never],
      noSkills: true,
      noPromptTemplates: true,
      noThemes: true,
      noContextFiles: true,
    });
    await resourceLoader.reload();
    const { session } = await createAgentSession({
      sessionManager,
      modelRuntime,
      model,
      cwd: projectDir,
      agentDir,
      resourceLoader,
      settingsManager: SettingsManager.inMemory(),
      customTools: [probeTool(dir)],
    });
    return session;
  };
}

export async function runFauxWorker(queue: string, dir: string) {
  const projectDir = join(dir, "project");
  mkdirSync(projectDir, { recursive: true });
  const connection = await NativeConnection.connect({
    address: process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7233",
  });
  const worker = await Worker.create({
    connection,
    namespace: "default",
    taskQueue: queue,
    workflowsPath: fileURLToPath(new URL("../src/workflows.ts", import.meta.url)),
    activities: makeActivities(
      { projectDir, provider: PROVIDER },
      { openSession: fauxOpenSession(dir, projectDir) },
    ),
  });
  return { worker, connection };
}

const arg = (name: string) =>
  process.argv.find((a) => a.startsWith(`--${name}=`))?.slice(name.length + 3);

if (process.argv[1] && realpathSync(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const queue = arg("queue");
  const dir = arg("dir");
  if (!queue || !dir) throw new Error("faux-worker wants --queue=<queue> --dir=<scratch dir>");
  const { worker } = await runFauxWorker(queue, dir);
  console.log(`faux worker ${process.pid} polling ${queue}`);
  await worker.run();
}
