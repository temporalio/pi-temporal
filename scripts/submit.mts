import { submitPrompt } from "../src/core/client.js";
const [, , sid, text] = process.argv;
const id = await submitPrompt(sid, text);
console.log("promptId", id);
process.exit(0);
