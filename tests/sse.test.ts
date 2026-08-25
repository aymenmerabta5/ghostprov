import { describe, expect, it } from "bun:test";
import {
  parseFrames,
  sseFrame,
} from "../src/relay/sse";

describe("parseFrames", () => {
  it("leaves an incomplete CRLF frame unconsumed", () => {
    const complete = "event: a\r\ndata: 1\r\n\r\n";
    const partial = "event: b\r\ndata";
    const parsed = parseFrames(complete + partial, 0);
    expect(parsed.frames).toEqual([{ event: "a", data: "1" }]);
    expect(parsed.nextOffset).toBe(complete.length);
  });

  it("parses CR-only and mixed endings with comments and multiline data", () => {
    const first = ": ignored\revent: alpha\rdata: first\rdata:  second\r\r";
    const second = "event: beta\r\ndata: one\ndata: two\r\n\r";
    const parsed = parseFrames(first + second, 0);

    expect(parsed.frames).toEqual([
      { event: "alpha", data: "first\n second" },
      { event: "beta", data: "one\ntwo" },
    ]);
    expect(parsed.nextOffset).toBe((first + second).length);
  });

  it("serializes every logical payload line as its own data field", () => {
    const wire = sseFrame("multi", "alpha\nbeta\r\ngamma\rdelta\n");

    expect(wire).toBe(
      "event: multi\n" +
      "data: alpha\n" +
      "data: beta\n" +
      "data: gamma\n" +
      "data: delta\n" +
      "data: \n\n",
    );
    expect(parseFrames(wire, 0).frames).toEqual([{
      event: "multi",
      data: "alpha\nbeta\ngamma\ndelta\n",
    }]);
  });
});
