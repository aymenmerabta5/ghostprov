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
  if (item.type !== "reasoning" || typeof item.id !== "string" || !item.id ||
    typeof item.encrypted_content !== "string" || !item.encrypted_content) {
    throw new TypeError("reasoning continuity requires nonempty id and encrypted content");
  }
  const encoded = Buffer.from(JSON.stringify({
    id: item.id,
    encrypted_content: item.encrypted_content,
  }), "utf8").toString("base64url");
  const envelope = PREFIX + encoded;
  if (envelope.length > MAX_ENCODED_LENGTH) {
    throw new RangeError("reasoning continuity envelope is too large");
  }
  return envelope;
}

export function decodeReasoningItem(value: string): ResponsesReasoningItem | null {
  if (!value.startsWith(PREFIX) || value.length > MAX_ENCODED_LENGTH) return null;

  const encoded = value.slice(PREFIX.length);
  if (!encoded || !BASE64URL.test(encoded)) return null;

  try {
    const bytes = Buffer.from(encoded, "base64url");
    if (bytes.toString("base64url") !== encoded) return null;
    const json = bytes.toString("utf8");
    const item: unknown = JSON.parse(json);
    if (item === null || typeof item !== "object" || Array.isArray(item)) return null;

    const candidate = item as Record<string, unknown>;
    if (
      Object.keys(candidate).length !== 2 ||
      typeof candidate.id !== "string" || !candidate.id ||
      typeof candidate.encrypted_content !== "string" || !candidate.encrypted_content
    ) {
      return null;
    }
    if (json !== JSON.stringify({
      id: candidate.id,
      encrypted_content: candidate.encrypted_content,
    })) return null;

    return {
      type: "reasoning",
      id: candidate.id,
      encrypted_content: candidate.encrypted_content,
      summary: [],
    };
  } catch {
    return null;
  }
}
