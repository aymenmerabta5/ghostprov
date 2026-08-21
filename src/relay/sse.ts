/**
 * SSE helpers — parse relay event-stream payloads.
 *
 * Protocol v2 (runner.py): progressive response.json carries a bounded TAIL
 *   {"streaming":true,"total":<abs len>,"start":<tail abs offset>,"tail":"..."}
 * so per-poll cost is O(window) regardless of response size — huge reasoning
 * outputs never degrade throughput.
 * Legacy v1 ({"sse":"<full>"}) still supported.
 */

export interface SseFrame {
  event: string;
  data: string;
}

/** Split an SSE buffer region into complete frames; trailing partial frame is left unconsumed. */
export function parseFrames(region: string, emitFrom: number): { frames: SseFrame[]; nextOffset: number } {
  const frames: SseFrame[] = [];
  let start = emitFrom;
  for (;;) {
    const lf = region.indexOf("\n\n", start);
    const crlf = region.indexOf("\r\n\r\n", start);
    let idx = -1;
    let delimiterLength = 0;
    if (lf !== -1 && (crlf === -1 || lf < crlf)) {
      idx = lf;
      delimiterLength = 2;
    } else if (crlf !== -1) {
      idx = crlf;
      delimiterLength = 4;
    }
    if (idx === -1) break;
    const chunk = region.slice(start, idx);
    start = idx + delimiterLength;
    let event = "message";
    const dataLines: string[] = [];
    for (const line of chunk.split(/\r?\n/)) {
      if (line.startsWith("event:")) event = line.slice(6).trim();
      else if (line.startsWith("data:")) dataLines.push(line.slice(5).trimStart());
    }
    if (dataLines.length) frames.push({ event, data: dataLines.join("\n") });
  }
  return { frames, nextOffset: start };
}

export function sseFrame(event: string, data: unknown): string {
  return `event: ${event}\ndata: ${typeof data === "string" ? data : JSON.stringify(data)}\n\n`;
}

/**
 * Strip ghostprov-internal protocol keys before a final Responses object
 * reaches any client. gp_sse/gp_ack/gp_total are our private disk protocol —
 * Codex and every other Responses-API consumer must never see them.
 */
export function sanitizeFinal(obj: any): any {
  if (!obj || typeof obj !== "object" || Array.isArray(obj)) return obj;
  const clone = { ...obj };
  delete clone.gp_sse;
  delete clone.gp_ack;
  delete clone.gp_total;
  delete clone.gp_frames;
  delete clone.gp_slot;
  delete clone.gp_request;
  delete clone.gp_transport;
  delete clone.transport;
  return clone;
}

export const SSE_HEADERS = {
  "content-type": "text/event-stream; charset=utf-8",
  "cache-control": "no-cache",
} as const;

export class SseProtocolGapError extends Error {
  readonly code = "sse_window_gap";

  constructor(readonly cursor: number, readonly windowStart: number) {
    super(`SSE window starts at ${windowStart}, past unconsumed cursor ${cursor}`);
    this.name = "SseProtocolGapError";
  }
}

function codePointLength(value: string): number {
  return Array.from(value).length;
}

function sliceCodePoints(value: string, start: number, end?: number): string {
  return Array.from(value).slice(start, end).join("");
}

/**
 * Incremental extractor over both protocols.
 * Feed it each poll's raw response.json text; it returns only NEW complete frames.
 */
export class SseDeltaExtractor {
  /** Absolute daemon code-point offset immediately after the last complete frame. */
  private cursor = 0;

  feed(raw: string): SseFrame[] {
    let j: any;
    try {
      j = JSON.parse(raw);
    } catch {
      // mid-write partial JSON — retry next poll
      return [];
    }
    if (j?.streaming !== true) return [];
    if (typeof j.sse === "string") {
      // legacy v1: full accumulated stream
      const total = codePointLength(j.sse);
      if (total <= this.cursor) return [];
      const region = sliceCodePoints(j.sse, this.cursor);
      const { frames, nextOffset } = parseFrames(region, 0);
      this.cursor += codePointLength(region.slice(0, nextOffset));
      return frames;
    }
    if (typeof j.tail === "string" && Number.isFinite(j.total) && Number.isFinite(j.start)) {
      const total = Math.max(0, Math.trunc(j.total));
      const start = Math.max(0, Math.trunc(j.start));
      if (total <= this.cursor) return [];
      if (start > this.cursor) throw new SseProtocolGapError(this.cursor, start);

      // Python's daemon reports code-point offsets. Slice via Array.from so
      // astral characters consume one position on both sides of the wire.
      const tailLength = codePointLength(j.tail);
      const visibleEnd = start + tailLength;
      const boundedEnd = Math.min(total, visibleEnd);
      const region = sliceCodePoints(j.tail, this.cursor - start, boundedEnd - start);
      const { frames, nextOffset } = parseFrames(region, 0);
      this.cursor += codePointLength(region.slice(0, nextOffset));
      return frames;
    }
    return [];
  }
}
