import { readFileSync } from "node:fs";
const file = process.argv[2];
const lines = readFileSync(file, "utf8").trim().split("\n").filter(Boolean);
let userMarker = 0, assistant = 0, toolCalls = 0, toolResults = 0, lastAssistant = "";
for (const l of lines) {
  try {
    const e = JSON.parse(l);
    const m = e.message;
    if (!m) continue;
    const text = typeof m.content === "string" ? m.content : Array.isArray(m.content) ? m.content.map((b: any) => b?.text ?? "").join("") : "";
    if (m.role === "user" && text.includes("pi-temporal:")) userMarker++;
    if (m.role === "assistant") {
      assistant++;
      if (Array.isArray(m.content)) toolCalls += m.content.filter((b: any) => b?.type === "toolCall").length;
      if (text.trim()) lastAssistant = text.trim().slice(-80);
    }
    if (m.role === "toolResult") toolResults++;
  } catch {}
}
console.log(JSON.stringify({ userMarkerMsgs: userMarker, assistantMsgs: assistant, toolCalls, toolResults, lastAssistant }));
