// Loaded by path, in the Worker and in the Workflow sandbox, so it can't take arguments. A
// failure's message and stack trace are plain fields unless moved into a payload. They carry tool
// and provider errors, which can quote the task, so the codec seals them like any payload.
//
// Plain CommonJS on purpose. The SDK loads this with `require`, outside any TypeScript loader,
// and Node won't strip types from a file under a `node_modules` directory.

const { DefaultFailureConverter } = require("@temporalio/common");

exports.failureConverter = new DefaultFailureConverter({ encodeCommonAttributes: true });
