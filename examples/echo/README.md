# Echo: an agent on the core alone

A whole `Agent` with no Pi, no fork, no Docker and no model key. Its "model" is scripted. For a
prompt, the first step asks for one `echo` tool call with the prompt's text, and the second step
answers with what the tool returned. It keeps its session in a JSONL file of its own format.

Copy this directory to put your own agent on Temporal. Replace the scripted model in `agent.ts`
with yours, and keep every rule its comments cite from [docs/adapting.md](../../docs/adapting.md).

| file | what it is |
|---|---|
| `agent.ts` | `echoAgent()`, the whole `Agent` over its session file |
| `worker.ts` | a Worker from `createSessionWorker` and `makeCoreActivities`, nothing else |
| `send.ts` | sends one prompt with `sendPrompt` and waits for the answer with `waitForQuiet` |

## Run it

From the repo root, after `npm ci`, each in its own terminal:

```
./scripts/temporal-dev.sh
npx tsx examples/echo/worker.ts
npx tsx examples/echo/send.ts "hello"
```

`send.ts` prints `answered: hello`. Session files go to `$TMPDIR/echo-sessions`, or to
`ECHO_SESSION_DIR`. `ECHO_STEPPED=1` runs the model call, the tool call and the seal as separate
Activities. `ECHO_TASK_QUEUE` and `TEMPORAL_ADDRESS` work for both scripts.

`checks/echo-check.mts` runs these commands, then kills a Worker while the echo tool runs and
checks that the new Worker reports the outcome as unknown instead of running the tool again.
