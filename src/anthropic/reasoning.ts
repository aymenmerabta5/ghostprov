const PREFIX = "ghostprov-r1.";
const MAX_ENCODED_LENGTH = 8 * 1024 * 1024;
const BASE64URL = /^[A-Za-z0-9_-]+$/;

export type ResponsesReasoningItem = {
  type: "reasoning";
  id: string;
  encrypted_content: string;
  summary: readonly unknown[];
};

export function encodeReasoningItem(item: ResponsesReasoningItem): string {
  return PREFIX + Buffer.from(JSON.stringify(item), "utf8").toString("base64url");
}

export function decodeReasoningItem(value: string): ResponsesReasoningItem | null {
  if (!value.startsWith(PREFIX) || value.length > MAX_ENCODED_LENGTH) return null;

  const encoded = value.slice(PREFIX.length);
  if (!encoded || !BASE64URL.test(encoded)) return null;

  try {
    const item: unknown = JSON.parse(Buffer.from(encoded, "base64url").toString("utf8"));
    if (item === null || typeof item !== "object" || Array.isArray(item)) return null;

    const candidate = item as Record<string, unknown>;
    if (
      candidate.type !== "reasoning" ||
      typeof candidate.id !== "string" ||
      typeof candidate.encrypted_content !== "string" ||
      !Array.isArray(candidate.summary)
    ) {
      return null;
    }

    return {
      type: "reasoning",
      id: candidate.id,
      encrypted_content: candidate.encrypted_content,
      summary: candidate.summary,
    };
  } catch {
    return null;
  }
}
