# A worker (or client) image. In containers, workers share only the Temporal cluster and the
# session directory. The fork is copied from `.fork/pi`, so the image has the pinned build.

FROM node:22-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends git ca-certificates procps \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci

# Install the fork's dependencies for this platform (native bindings). Its build output is copied.
COPY .fork/pi /app/.fork/pi
RUN cd /app/.fork/pi && npm ci --ignore-scripts

# Same link setup-fork.sh makes.
RUN mkdir -p /app/node_modules/@earendil-works \
  && rm -rf /app/node_modules/@earendil-works/pi-coding-agent \
  && ln -s ../../.fork/pi/packages/coding-agent /app/node_modules/@earendil-works/pi-coding-agent

COPY tsconfig.json ./
COPY src ./src
COPY extensions ./extensions
COPY checks/detached-check.mts ./checks/

# Session logs. Mount one shared volume here in every container.
ENV PI_SESSION_DIR=/sessions
# Where tools run. Not shared: a project reaches another container only with the session.
ENV PI_PROJECT_DIR=/project
RUN mkdir -p /sessions /project

# Node as PID 1, with no npx wrapper to swallow the stop signal.
CMD ["node", "--import", "tsx", "src/worker.ts"]
