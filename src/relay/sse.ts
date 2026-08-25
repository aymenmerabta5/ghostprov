/** SSE helpers shared by the public API and the V2 sidecar transport. */

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

export const SSE_HEADERS = {
  "content-type": "text/event-stream; charset=utf-8",
  "cache-control": "no-cache",
} as const;
