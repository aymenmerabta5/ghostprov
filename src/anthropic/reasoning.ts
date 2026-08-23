const PREFIX = "ghostprov-r1.";
const MAX_DECODED_LENGTH = 8 * 1024 * 1024;
const BASE64URL = /^[A-Za-z0-9_-]+$/;

function decodedBase64UrlLength(encodedLength: number): number | null {
  const remainder = encodedLength % 4;
  if (remainder === 1) return null;
  return Math.floor(encodedLength / 4) * 3 + (remainder === 2 ? 1 : remainder === 3 ? 2 : 0);
}

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
  const json = JSON.stringify({
    id: item.id,
    encrypted_content: item.encrypted_content,
  });
  if (Buffer.byteLength(json, "utf8") > MAX_DECODED_LENGTH) {
    throw new RangeError("reasoning continuity envelope is too large");
  }
  return PREFIX + Buffer.from(json, "utf8").toString("base64url");
}

export function decodeReasoningItem(value: string): ResponsesReasoningItem | null {
  if (!value.startsWith(PREFIX)) return null;

  const encodedLength = value.length - PREFIX.length;
  const decodedLength = decodedBase64UrlLength(encodedLength);
  if (decodedLength === null || decodedLength > MAX_DECODED_LENGTH) return null;

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
