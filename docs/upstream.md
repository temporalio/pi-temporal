# Upstreaming

The fork adds a step-level API to Pi's `AgentSession`. Against `earendil-works/pi` main it's about
+1350 / -210 production lines in 9 files, plus tests. We'd propose it in this order, smallest
first:

1. **Session write guard.** `SessionManager.setWriteGuard` rejects appends from a writer that no
   longer owns the file. Small, no Temporal in it, and useful to any host with a stale writer.
2. **Share the model and tool phases of a turn.** An extraction with no new behavior. The normal
   `prompt()` path is the acceptance test.
3. **Expose the step cursor.** Pause after the model call, run the tools, then close the step.
   This is what an external driver needs.
4. **Let an extension drive a turn.** `pi.registerTurnExecutor` and `recordPrompt`, on top of 3.
5. **Resume an interrupted local turn.** Pi's own crash recovery, if the maintainers want it.

Dispatch claims, durable retry counts, and Temporal's retry classification stay in this repo.
