# A worker (or a client) as its own machine. The point of running this in a container is that two
# workers then share nothing except the Temporal cluster and the session directory, which is the
# claim the README makes and could not otherwise show: on one host, "another worker" is another
# process reading the same disk.
#
# The fork is copied in already fetched rather than cloned here. It lives in a private repo, and a
# build argument holding a token is a worse answer than a build context that already has it.

FROM node:22-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends git ca-certificates procps \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci

# The fork's own dependencies, resolved for this platform. The tree is copied with its build output
# already in it, so nothing is compiled here; `@temporalio/*` carry native bindings, which is why
# they cannot come from the host's node_modules.
COPY .fork/pi /app/.fork/pi
RUN cd /app/.fork/pi && npm ci --ignore-scripts

# Same link setup-fork.sh makes, so the fork resolves its own siblings from its workspace.
RUN mkdir -p /app/node_modules/@earendil-works \
  && rm -rf /app/node_modules/@earendil-works/pi-coding-agent \
  && ln -s ../../.fork/pi/packages/coding-agent /app/node_modules/@earendil-works/pi-coding-agent

COPY tsconfig.json ./
COPY src ./src
COPY extensions ./extensions
COPY detached-check.mts ./

# Where the session log lives. A volume mounted here in more than one container is the shared
# storage the config comments point at.
ENV PI_SESSION_DIR=/sessions
RUN mkdir -p /sessions

# One process, not npx -> tsx -> node. A container stops by signalling PID 1, and a wrapper would
# swallow that the same way it swallows a kill.
CMD ["node", "--import", "tsx", "src/worker.ts"]
