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

function nextSseLine(region: string, start: number): {
  line: string;
  nextOffset: number;
} | null {
  for (let index = start; index < region.length; index += 1) {
    const code = region.charCodeAt(index);
    if (code === 0x0a) {
      return { line: region.slice(start, index), nextOffset: index + 1 };
    }
    if (code === 0x0d) {
      const crlf = region.charCodeAt(index + 1) === 0x0a;
      return {
        line: region.slice(start, index),
        nextOffset: index + (crlf ? 2 : 1),
      };
    }
  }
  return null;
}

/** Split an SSE buffer region into complete frames; trailing partial frame is left unconsumed. */
export function parseFrames(region: string, emitFrom: number): { frames: SseFrame[]; nextOffset: number } {
  const frames: SseFrame[] = [];
  let cursor = emitFrom;
  let nextOffset = emitFrom;
  let event = "";
  let dataLines: string[] = [];
  for (;;) {
    const parsed = nextSseLine(region, cursor);
    if (!parsed) break;
    cursor = parsed.nextOffset;
    const line = parsed.line;
    if (line.length === 0) {
      if (dataLines.length > 0) {
        frames.push({ event: event || "message", data: dataLines.join("\n") });
      }
      event = "";
      dataLines = [];
      nextOffset = cursor;
      continue;
    }
    if (line.startsWith(":")) continue;
    const colon = line.indexOf(":");
    const field = colon < 0 ? line : line.slice(0, colon);
    let value = colon < 0 ? "" : line.slice(colon + 1);
    if (value.startsWith(" ")) value = value.slice(1);
    if (field === "event") event = value;
    else if (field === "data") dataLines.push(value);
  }
  return { frames, nextOffset };
}

export function sseFrame(event: string, data: unknown): string {
  const encoded = typeof data === "string" ? data : JSON.stringify(data);
  const lines = String(encoded).split(/\r\n|\r|\n/);
  return `event: ${event}\n${lines.map(line => `data: ${line}\n`).join("")}\n`;
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

/**
 * Slot streams are backed by an authoritative terminal transcript (`gp_sse`).
 * If polling misses the beginning of the daemon's bounded window, preserve the
 * strict extractor invariant but defer delivery until that transcript arrives.
 * This never fabricates or skips frames: terminal replay starts at offset zero
 * and the wrapped extractor's cursor makes already-delivered frames idempotent.
 */
export class TerminalBackedSseExtractor {
  private readonly extractor = new SseDeltaExtractor();
  deferredGap: SseProtocolGapError | null = null;

  feedProgress(raw: string): SseFrame[] {
    if (this.deferredGap) return [];
    try {
      return this.extractor.feed(raw);
    } catch (error) {
      if (!(error instanceof SseProtocolGapError)) throw error;
      this.deferredGap = error;
      return [];
    }
  }

  replayTerminal(fullSse: string, total = codePointLength(fullSse)): SseFrame[] {
    const frames = this.extractor.feed(JSON.stringify({
      streaming: true,
      total,
      start: 0,
      tail: fullSse,
    }));
    this.deferredGap = null;
    return frames;
  }
}
