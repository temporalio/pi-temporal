// Binding Pi and the optional tree store here keeps the core Activities independent of Pi.

import { makeCoreActivities } from "../core/activities.js";
import { treeStore } from "../tree/store.js";
import { piAgent, type PiDependencies, type PiOptions } from "./agent.js";

export interface ActivityOptions extends PiOptions {
  // Ship the project's files with the session, so another Worker sees the last one's changes.
  readonly shipTree?: boolean;
  // This Worker's host queue. The model call reports it, so the rest of the step runs here.
  readonly hostQueue?: string;
  // The directory every session file must be under. See `CoreActivityOptions`.
  readonly sessionRoot: string;
}

export function makeActivities(opts: ActivityOptions, dependencies: PiDependencies = {}) {
  return makeCoreActivities({
    agent: piAgent(opts, dependencies),
    ...(opts.shipTree ? { store: treeStore(opts.projectDir) } : {}),
    ...(opts.hostQueue === undefined ? {} : { hostQueue: opts.hostQueue }),
    sessionRoot: opts.sessionRoot,
  });
}

export type Activities = ReturnType<typeof makeActivities>;
