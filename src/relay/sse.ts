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
    const idx = region.indexOf("\n\n", start);
    if (idx === -1) break;
    const chunk = region.slice(start, idx);
    start = idx + 2;
    let event = "message";
    const dataLines: string[] = [];
    for (const line of chunk.split("\n")) {
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

export const SSE_HEADERS = {
  "content-type": "text/event-stream; charset=utf-8",
  "cache-control": "no-cache",
} as const;

/**
 * Incremental extractor over both protocols.
 * Feed it each poll's raw response.json text; it returns only NEW complete frames.
 */
export class SseDeltaExtractor {
  /** absolute chars of the stream consumed so far */
  private consumed = 0;
  /** carry-over for a frame split across polls */
  private pending = "";
  private pendingBase = 0; // absolute offset of pending[0]

  feed(raw: string): SseFrame[] {
    try {
      const j = JSON.parse(raw);
      if (j?.streaming !== true) return [];
      if (typeof j.sse === "string") {
        // legacy v1: full accumulated stream
        if (j.sse.length <= this.consumed) return [];
        const region = j.sse;
        const from = this.consumed;
        const { frames, nextOffset } = parseFrames(region, from);
        this.consumed = nextOffset;
        return frames;
      }
      if (typeof j.tail === "string" && Number.isFinite(j.total) && Number.isFinite(j.start)) {
        const total: number = j.total;
        const start: number = j.start;
        if (total <= this.consumed) return [];
        if (start > this.consumed) {
          // window slid past our cursor (client stalled) — drop missed frames
          this.consumed = start;
          this.pending = "";
          this.pendingBase = start;
        }
        // region of new bytes we can see: [max(consumed,start), total)
        const tailFrom = Math.max(this.consumed, start) - start;
        const region = this.pending + j.tail.slice(tailFrom);
        const regionBase = this.pendingBase;
        const { frames, nextOffset } = parseFrames(region, this.consumed - regionBase);
        // keep unconsumed remainder as pending
        this.pending = region.slice(nextOffset);
        this.pendingBase = regionBase + nextOffset;
        this.consumed = total;
        return frames;
      }
    } catch {
      // mid-write partial JSON — retry next poll
    }
    return [];
  }
}
