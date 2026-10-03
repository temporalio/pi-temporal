# Upstreaming

## What upstream would have to take

A sixth review measured this against the harness it forks rather than against
itself, which nobody had done. The seam is three methods; the patch is not.

Pi's fork, against a `main` level with `earendil-works/pi`:

| | files | lines |
|---|---|---|
| production | 9 | +1356 / -213 |
| tests it adds | 8 | +2088 |
| documentation | 1 | +49 |

That production figure moved twice this round. The review's reduction took an
unused stream adapter and seven internal exports out, from +1324 to +1258. The
preparation fix put +98 back, because carrying the completed turn and what its
preparation returned through the model call, the tools and the seal is state the
host has to hold. Net it is 32 lines above where the round started, and correct
where it was not.

The order to ask for it in, smallest first:

1. **`feat(coding-agent): reject session appends through a write guard`**, on
   branch `moe/session-write-guard`. Two files, 85 lines, no Temporal anywhere in
   it. Useful to anyone with a stale or read-only writer of a session file.
   Built on `main`, tested there, and its three tests were checked by removing
   the guard call to watch two of them fail.
2. **Share the model and tool phases of a turn.** The extraction, with the
   completed turn travelling through it. No replay, no executor registration.
   The ordinary `prompt()` path is the acceptance test.
3. **Expose the step cursor.** What 2 makes possible: pause after the model,
   settle tools, then close the step. This is the contract the preparation fix
   defines, and the one an external driver actually needs.
4. **Let an extension drive a turn.** Executor registration and `recordPrompt`,
   on top of 3.
5. **Resume an interrupted local turn.** The host's own crash recovery, if the
   maintainers want it. Dispatch claims, durable retry counts and Temporal's
   retry classification stay here, in the driver.

Only the first is built and tested in isolation. The rest is an order of
dependency, not four more branches.

