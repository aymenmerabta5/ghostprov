import {
  DEFAULT_ANTHROPIC_REQUEST_LIMITS,
  measureBoundedJson,
} from "./request-validation";

const V1_PREFIX = "ghostprov-r1.";
const V2_PREFIX = "ghostprov-r2.";
const MAX_DECODED_LENGTH = 8 * 1024 * 1024;
const BASE64URL = /^[A-Za-z0-9_-]+$/;
const UTF8_FATAL = new TextDecoder("utf-8", { fatal: true });

function base64UrlValue(value: string): number {
  const unit = value.charCodeAt(0);
  if (unit >= 0x41 && unit <= 0x5a) return unit - 0x41;
  if (unit >= 0x61 && unit <= 0x7a) return unit - 0x61 + 26;
  if (unit >= 0x30 && unit <= 0x39) return unit - 0x30 + 52;
  return unit === 0x2d ? 62 : 63;
}

function canonicalBase64Url(value: string): boolean {
  if (!value || !BASE64URL.test(value)) return false;
  const remainder = value.length % 4;
  if (remainder === 1) return false;
  if (remainder === 0) return true;
  const tail = base64UrlValue(value.at(-1)!);
  return remainder === 2 ? (tail & 0x0f) === 0 : (tail & 0x03) === 0;
}

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

const MAX_SUMMARY_SEGMENTS_PER_ITEM = 4_096;
const MAX_SUMMARY_SEGMENTS_PER_REQUEST = 8_192;

export type ReasoningSignatureBudget = {
  reserve(bytes: number): boolean;
};

export class ReasoningDecodeBudget {
  #segments = 0;
  #units = 0;

  reserve(segments: number, units: number): boolean {
    if (!Number.isSafeInteger(segments) || segments < 0 ||
      !Number.isSafeInteger(units) || units < 0 ||
      this.#segments + segments > MAX_SUMMARY_SEGMENTS_PER_REQUEST ||
      this.#units + units > MAX_DECODED_LENGTH) return false;
    this.#segments += segments;
    this.#units += units;
    return true;
  }
}

function reasoningSummaryLayout(summary: readonly unknown[]): string {
  if (summary.length > MAX_SUMMARY_SEGMENTS_PER_ITEM) {
    throw new RangeError("reasoning summary contains too many items");
  }
  return summary.map(value => {
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      throw new TypeError("reasoning summary item is invalid");
    }
    const item = value as Record<string, unknown>;
    if (item.type !== "summary_text" || typeof item.text !== "string") {
      throw new TypeError("reasoning summary item is invalid");
    }
    return String(item.text.length);
  }).join(",");
}

/**
 * V2 preserves summary segmentation without embedding hidden summary plaintext.
 * The visible thinking text supplied by the client reconstructs those segments.
 */
export function encodeReasoningItem(
  item: ResponsesReasoningItem,
  signatureBudget?: ReasoningSignatureBudget,
): string {
  if (item.type !== "reasoning" || typeof item.id !== "string" || !item.id ||
    typeof item.encrypted_content !== "string" || !item.encrypted_content ||
    !Array.isArray(item.summary)) {
    throw new TypeError("reasoning continuity requires nonempty id and encrypted content");
  }
  const source = {
    id: item.id,
    encrypted_content: item.encrypted_content,
    summary_layout: reasoningSummaryLayout(item.summary),
  };
  let decodedBytes: number;
  try {
    decodedBytes = measureBoundedJson(
      source,
      "reasoning continuity envelope",
      MAX_DECODED_LENGTH,
      { ...DEFAULT_ANTHROPIC_REQUEST_LIMITS },
    );
  } catch {
    throw new RangeError("reasoning continuity envelope is too large");
  }
  const remainder = decodedBytes % 3;
  const payloadBytes = Math.ceil(decodedBytes / 3) * 4 -
    (remainder === 1 ? 2 : remainder === 2 ? 1 : 0);
  const signatureBytes = V2_PREFIX.length + payloadBytes;
  if (signatureBudget && !signatureBudget.reserve(signatureBytes)) {
    throw new RangeError("reasoning continuity signatures exceed response capacity");
  }
  const json = JSON.stringify(source);
  return V2_PREFIX + Buffer.from(json, "utf8").toString("base64url");
}

type CanonicalString = {
  readonly value: string;
  readonly next: number;
};

type DecodedEnvelope = {
  readonly id: string;
  readonly encryptedContent: string;
  readonly summaryLayout?: string;
};

function decodeEnvelopeJson(value: string, prefix: string): string | null {
  if (!value.startsWith(prefix)) return null;
  const encodedLength = value.length - prefix.length;
  const decodedLength = decodedBase64UrlLength(encodedLength);
  if (decodedLength === null || decodedLength > MAX_DECODED_LENGTH) return null;
  const encoded = value.slice(prefix.length);
  if (!canonicalBase64Url(encoded)) return null;

  try {
    return UTF8_FATAL.decode(Buffer.from(encoded, "base64url"));
  } catch {
    return null;
  }
}

/** Parses one canonical JSON string without materializing an untrusted object graph. */
function canonicalString(json: string, start: number): CanonicalString | null {
  if (json.charCodeAt(start) !== 0x22) return null;
  let escaped = false;
  for (let cursor = start + 1; cursor < json.length; cursor += 1) {
    const unit = json.charCodeAt(cursor);
    if (unit < 0x20) return null;
    if (unit === 0x5c) {
      escaped = true;
      cursor += 1;
      if (cursor >= json.length) return null;
      if (json.charCodeAt(cursor) === 0x75) {
        if (cursor + 4 >= json.length ||
          !/^[0-9a-fA-F]{4}$/.test(json.slice(cursor + 1, cursor + 5))) return null;
        cursor += 4;
      }
      continue;
    }
    if (unit !== 0x22) continue;
    if (!escaped) {
      return { value: json.slice(start + 1, cursor), next: cursor + 1 };
    }
    const raw = json.slice(start, cursor + 1);
    try {
      const parsed: unknown = JSON.parse(raw);
      if (typeof parsed !== "string" || JSON.stringify(parsed) !== raw) return null;
      return { value: parsed, next: cursor + 1 };
    } catch {
      return null;
    }
  }
  return null;
}

function parseEnvelope(value: string, prefix: string, withSummary: boolean): DecodedEnvelope | null {
  const json = decodeEnvelopeJson(value, prefix);
  if (json === null || !json.startsWith('{"id":')) return null;
  const id = canonicalString(json, 6);
  if (!id || !id.value || !json.startsWith(',"encrypted_content":', id.next)) return null;
  const encryptedStart = id.next + ',"encrypted_content":'.length;
  const encrypted = canonicalString(json, encryptedStart);
  if (!encrypted || !encrypted.value) return null;

  if (!withSummary) {
    return encrypted.next === json.length - 1 && json.endsWith("}")
      ? { id: id.value, encryptedContent: encrypted.value }
      : null;
  }
  if (!json.startsWith(',"summary_layout":', encrypted.next)) return null;
  const summaryStart = encrypted.next + ',"summary_layout":'.length;
  const summary = canonicalString(json, summaryStart);
  if (!summary || summary.next !== json.length - 1 || !json.endsWith("}")) return null;
  return {
    id: id.value,
    encryptedContent: encrypted.value,
    summaryLayout: summary.value,
  };
}

function scanSummaryLayout(
  layout: string,
  onLength?: (length: number) => void,
): { segments: number; total: number } | null {
  if (layout === "") return { segments: 0, total: 0 };
  let segments = 0;
  let total = 0;
  let value = 0;
  let digits = 0;
  let first = 0;
  for (let cursor = 0; cursor <= layout.length; cursor += 1) {
    const unit = cursor < layout.length ? layout.charCodeAt(cursor) : 0x2c;
    if (unit === 0x2c) {
      if (digits === 0 || digits > 1 && first === 0x30) return null;
      total += value;
      segments += 1;
      if (!Number.isSafeInteger(total) || total > MAX_DECODED_LENGTH ||
        segments > MAX_SUMMARY_SEGMENTS_PER_ITEM) return null;
      onLength?.(value);
      value = 0;
      digits = 0;
      continue;
    }
    if (unit < 0x30 || unit > 0x39) return null;
    if (digits === 0) first = unit;
    const digit = unit - 0x30;
    if (value > Math.floor((MAX_DECODED_LENGTH - digit) / 10)) return null;
    value = value * 10 + digit;
    digits += 1;
  }
  return { segments, total };
}

function decodeV2Summary(
  layout: string,
  visibleThinking: string | undefined,
  budget: ReasoningDecodeBudget | undefined,
): readonly unknown[] | null {
  const decoded = scanSummaryLayout(layout);
  if (!decoded) return null;

  // Omitted thinking deliberately keeps the old encrypted-only replay behavior.
  // Count its declared segments against the request budget even though no summary
  // objects are materialized, so repeated layouts cannot bypass aggregate work.
  if (visibleThinking === undefined || (visibleThinking === "" && decoded.total > 0)) {
    return budget && !budget.reserve(decoded.segments, 0) ? null : [];
  }
  if (visibleThinking.length !== decoded.total ||
    budget && !budget.reserve(decoded.segments, decoded.total)) return null;
  const summary: Array<{ type: "summary_text"; text: string }> = [];
  let cursor = 0;
  scanSummaryLayout(layout, length => {
    summary.push({
      type: "summary_text",
      text: visibleThinking.slice(cursor, cursor + length),
    });
    cursor += length;
  });
  return summary;
}

export function isGhostProvReasoningSignature(value: string): boolean {
  return value.startsWith(V1_PREFIX) || value.startsWith(V2_PREFIX);
}

export function decodeReasoningItem(
  value: string,
  visibleThinking?: string,
  budget?: ReasoningDecodeBudget,
): ResponsesReasoningItem | null {
  const v2 = parseEnvelope(value, V2_PREFIX, true);
  if (v2) {
    const summary = decodeV2Summary(v2.summaryLayout!, visibleThinking, budget);
    if (!summary) return null;
    return {
      type: "reasoning",
      id: v2.id,
      encrypted_content: v2.encryptedContent,
      summary,
    };
  }

  const v1 = parseEnvelope(value, V1_PREFIX, false);
  if (!v1) return null;
  return {
    type: "reasoning",
    id: v1.id,
    encrypted_content: v1.encryptedContent,
    summary: [],
  };
}
