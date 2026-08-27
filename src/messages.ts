// Defensive text extraction from a Pi AgentMessage's content, which is either a string or an
// array of content blocks (TextContent | ImageContent). Node-side only.

export function textOf(content: unknown): string {
  if (typeof content === "string") return content;
  if (Array.isArray(content)) {
    return content
      .map((b) =>
        b && typeof b === "object" && "text" in b
          ? String((b as { text?: unknown }).text ?? "")
          : "",
      )
      .join("");
  }
  return "";
}
