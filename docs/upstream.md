# Upstreaming

The fork adds a step API to Pi’s `AgentSession`. Compared with `earendil-works/pi` main, it adds
about 1,350 production lines and removes 210 across nine files, plus tests. We’d propose the
changes in the order below, starting with the smallest.

1. **Session write guard.** `SessionManager.setWriteGuard` rejects appends from a writer that no
   longer owns the file. It has no Temporal dependency and can stop a stale writer on any host.
2. **Share the model and tool phases of a turn.** An extraction with no new behavior. The normal
   `prompt()` path is the acceptance test.
3. **Expose the step cursor.** Pause after the model call, run the tools, then close the step.
   An external driver needs these boundaries to dispatch each unit of work.
4. **Let an extension drive a turn.** `pi.registerTurnExecutor` and `recordPrompt`, on top of 3.
5. **Resume an interrupted local turn.** Pi’s own crash recovery, if the maintainers want it.

Dispatch claims, durable retry counts, and Temporal’s retry classification stay in this repo.
