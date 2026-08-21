import { describe, expect, it } from "vitest";
import { SseDeltaExtractor, SseProtocolGapError, parseFrames } from "../src/relay/sse";

const FRAME = [
  "event: response.output_text.delta",
  'data: {"type":"response.output_text.delta","delta":"hello"}',
  "",
  "",
].join("\n");

function codePoints(value: string): string[] {
  return Array.from(value);
}

function progress(full: string, visibleCodePoints: number, startCodePoints = 0): string {
  const points = codePoints(full);
  return JSON.stringify({
    streaming: true,
    total: visibleCodePoints,
    start: startCodePoints,
    tail: points.slice(startCodePoints, visibleCodePoints).join(""),
  });
}

describe("SseDeltaExtractor windowed protocol", () => {
  it("emits a frame split at every character boundary", () => {
    const total = codePoints(FRAME).length;
    for (let cut = 1; cut < total; cut++) {
      const extractor = new SseDeltaExtractor();
      expect(extractor.feed(progress(FRAME, cut))).toEqual([]);
      expect(extractor.feed(progress(FRAME, total))).toEqual([
        {
          event: "response.output_text.delta",
          data: '{"type":"response.output_text.delta","delta":"hello"}',
        },
      ]);
      expect(extractor.feed(progress(FRAME, total))).toEqual([]);
    }
  });

  it("parses CRLF-delimited events", () => {
    const frame = FRAME.replaceAll("\n", "\r\n");
    const extractor = new SseDeltaExtractor();
    const frames = extractor.feed(progress(frame, codePoints(frame).length));
    expect(frames).toHaveLength(1);
    expect(frames[0].event).toBe("response.output_text.delta");
  });

  it("uses daemon code-point offsets for non-BMP text", () => {
    const first = FRAME.replace("hello", "🚀");
    const second = FRAME.replace("hello", "done");
    const stream = first + second;
    const firstLength = codePoints(first).length;
    const total = codePoints(stream).length;
    const extractor = new SseDeltaExtractor();

    expect(extractor.feed(progress(stream, firstLength))).toEqual([
      {
        event: "response.output_text.delta",
        data: '{"type":"response.output_text.delta","delta":"🚀"}',
      },
    ]);

    const overlapStart = Math.max(0, firstLength - 12);
    expect(extractor.feed(progress(stream, total, overlapStart))).toEqual([
      {
        event: "response.output_text.delta",
        data: '{"type":"response.output_text.delta","delta":"done"}',
      },
    ]);
  });

  it("fails loudly when the sliding window moves beyond an incomplete frame", () => {
    const extractor = new SseDeltaExtractor();
    expect(extractor.feed(progress(FRAME, 10))).toEqual([]);
    expect(() => extractor.feed(progress(FRAME, codePoints(FRAME).length, 11))).toThrow(
      SseProtocolGapError,
    );
  });
});

describe("parseFrames", () => {
  it("leaves an incomplete CRLF frame unconsumed", () => {
    const complete = "event: a\r\ndata: 1\r\n\r\n";
    const partial = "event: b\r\ndata";
    const parsed = parseFrames(complete + partial, 0);
    expect(parsed.frames).toEqual([{ event: "a", data: "1" }]);
    expect(parsed.nextOffset).toBe(complete.length);
  });
});
