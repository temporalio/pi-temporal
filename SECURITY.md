# Security

Do not open public GitHub issues for suspected security vulnerabilities. Report them to
security@temporal.io instead.

## What this repo doesn't protect against

- Agent tools run as the Worker's user. They can read what that user can, including the Worker's
  environment and key files. Run Workers under a separate user, or in a sandbox, if tools must not
  see the model key. [docs/guarantees.md](docs/guarantees.md) has the details.
- Anyone who can start a Workflow in the namespace picks its input and can make a Worker run
  tools. Give namespace access only to people you would let run the agent.
- The local dev server (`scripts/temporal-dev.sh`, `docker/compose.yml`, `demo/run.sh`) has no
  auth. It listens on loopback only. Don't publish it on a shared network.
