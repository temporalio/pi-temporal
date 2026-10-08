// The project directory as a `ProjectStore`, shipped between hosts as git bundles (`worktree.ts`).
// Optional: needed only when Workers on different hosts take turns on one session's files.

import type { ProjectStore } from "../core/agent.js";
import * as worktree from "./worktree.js";

export function treeStore(projectDir: string): ProjectStore {
  return {
    ensure: (sessionFile, writer) => worktree.ensure(projectDir, sessionFile, writer),
    capture: (sessionFile, of) => worktree.capture(projectDir, sessionFile, of),
    setAside: (sessionFile) => worktree.setAside(projectDir, sessionFile),
    beginWrite: (writer) => worktree.beginWrite(projectDir, writer),
    endWrite: (writer) => worktree.endWrite(projectDir, writer),
    closeStep: (sessionFile, step) =>
      worktree.closeStep(sessionFile, { turn: step.turn, step: step.step }),
    retire: (sessionFile) => worktree.retire(projectDir, sessionFile),
    adopt: (template, sessionFile) => worktree.adopt(template, sessionFile),
    isRefusal: (err) => worktree.isRefusal(err),
    isQuarantine: (err) => err instanceof worktree.Quarantined,
  };
}
