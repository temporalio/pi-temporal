// Loaded by path, in the Worker and in the Workflow sandbox, so it can't take arguments. A
// failure's message and stack trace are plain fields unless moved into a payload. They carry tool
// and provider errors, which can quote the task, so the codec seals them like any payload.

import { DefaultFailureConverter } from "@temporalio/common";

export const failureConverter = new DefaultFailureConverter({ encodeCommonAttributes: true });
