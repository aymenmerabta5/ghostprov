import { describe, expect, it } from "bun:test";
import {
  assertUnicodeWellFormed,
  UnicodeStopFilter,
} from "../src/anthropic/stream-stop-filter";

function combined<T>(chunks: Array<{ owner: T; text: string }>): string {
  return chunks.map(chunk => chunk.text).join("");
}

function filtered(stopSequences: readonly string[], chunks: readonly string[]): {
  text: string;
  matchedSequence: string | null;
} {
  const filter = new UnicodeStopFilter<number>(stopSequences);
  const maxSuffix = Math.max(0, ...stopSequences.map(stop => Array.from(stop).length - 1));
  let text = "";
  for (const [index, value] of chunks.entries()) {
    text += combined(filter.append(index, value).chunks);
    // One extra unit is permitted only for a dangling high surrogate.
    expect(filter.retainedCodePoints()).toBeLessThanOrEqual(maxSuffix + 1);
  }
  const terminal = filter.endSegment();
  text += combined(terminal.chunks);
  expect(filter.retainedCodePoints()).toBe(0);
  return { text, matchedSequence: terminal.matchedSequence };
}

describe("UnicodeStopFilter", () => {
  it("validates and emits a large no-stop delta as one contiguous chunk", () => {
    const filter = new UnicodeStopFilter<string>([]);
    const value = "x".repeat(1024 * 1024);

    const result = filter.append("owner", value);

    expect(result).toEqual({
      chunks: [{ owner: "owner", text: value }],
      matchedSequence: null,
    });
    expect(filter.retainedCodePoints()).toBe(0);
  });

  it("preserves the first chunk owner when an astral scalar spans appends", () => {
    const filter = new UnicodeStopFilter<string>([]);

    expect(filter.append("first", "before\ud83d").chunks).toEqual([
      { owner: "first", text: "before" },
    ]);
    const second = filter.append("second", "\ude80after");

    expect(second.chunks).toEqual([
      { owner: "first", text: "🚀" },
      { owner: "second", text: "after" },
    ]);
    expect(combined(second.chunks)).toBe("🚀after");
  });

  it("preserves the data-validation error contract for malformed scalars", () => {
    expect(() => assertUnicodeWellFormed("\udc00")).toThrow(
      "Responses data contains an invalid Unicode scalar value",
    );
  });

  it("selects the earliest stop start and preserves configured order for ties", () => {
    const filter = new UnicodeStopFilter<string>(["bc", "abcd"]);
    const result = filter.append("owner", "zabcdtail");

    expect(result).toEqual({
      chunks: [{ owner: "owner", text: "z" }],
      matchedSequence: "abcd",
    });

    const tied = new UnicodeStopFilter<string>(["ab", "abc"]);
    expect(tied.append("owner", "abc").matchedSequence).toBe("ab");
  });

  it.each([
    [["bc", "abcd"], "zabcdtail", "z", "abcd"],
    [["abcd", "bc"], "zabcdtail", "z", "abcd"],
    [["abc", "ab"], "zabctail", "z", "abc"],
    [["ab", "abc"], "zabctail", "z", "ab"],
    [["abcd", "bc"], "zabcXtail", "za", "bc"],
    [["abcd", "bc"], "zabc", "za", "bc"],
    [["🚀b", "a🚀bc"], "za🚀bctail", "z", "a🚀bc"],
    [["b", "aaab"], "zaaaabtail", "za", "aaab"],
    [["abab", "bab"], "zababbab", "z", "abab"],
    [["bc", "abcc", "zabc"], "yzabcc", "y", "zabc"],
  ] as const)("matches %j independently of every split in %s", (stops, value, text, stop) => {
    const expected = { text, matchedSequence: stop };
    expect(filtered(stops, [value])).toEqual(expected);
    // UTF-16 offsets deliberately include splits inside astral scalars.
    for (let split = 0; split <= value.length; split += 1) {
      expect(filtered(stops, [value.slice(0, split), value.slice(split)])).toEqual(expected);
    }
    expect(filtered(stops, value.split(""))).toEqual(expected);
  });

  it("holds only the undecided suffix and preserves owners across candidate resolution", () => {
    const filter = new UnicodeStopFilter<string>(["bc", "abcd"]);
    expect(filter.append("first", "za")).toEqual({ chunks: [], matchedSequence: null });
    expect(filter.append("second", "b")).toEqual({ chunks: [], matchedSequence: null });
    expect(filter.append("third", "c")).toEqual({
      chunks: [{ owner: "first", text: "z" }],
      matchedSequence: null,
    });
    expect(filter.retainedCodePoints()).toBe(3);
    expect(filter.append("fourth", "X")).toEqual({
      chunks: [{ owner: "first", text: "a" }],
      matchedSequence: "bc",
    });
    expect(filter.retainedCodePoints()).toBe(0);
    for (const owner of ["first", "second", "third", "fourth"]) {
      expect(filter.hasPending(owner)).toBe(false);
    }
  });

  it("resolves a completed candidate at a segment boundary and suppresses later text", () => {
    const filter = new UnicodeStopFilter<string>(["abcd", "bc"]);
    expect(filter.append("before", "zabc")).toEqual({
      chunks: [{ owner: "before", text: "z" }],
      matchedSequence: null,
    });
    expect(filter.endSegment()).toEqual({
      chunks: [{ owner: "before", text: "a" }],
      matchedSequence: "bc",
    });
    expect(filter.append("after", "d visible")).toEqual({ chunks: [], matchedSequence: "bc" });
    expect(filter.endSegment()).toEqual({ chunks: [], matchedSequence: "bc" });
  });

  it("resets unfinished prefixes at a segment boundary without inventing a match", () => {
    const filter = new UnicodeStopFilter<string>(["abcd"]);
    expect(filter.append("before", "zab")).toEqual({ chunks: [], matchedSequence: null });
    expect(filter.endSegment()).toEqual({
      chunks: [{ owner: "before", text: "zab" }], matchedSequence: null,
    });
    expect(filter.append("after", "cd")).toEqual({ chunks: [], matchedSequence: null });
    expect(filter.endSegment()).toEqual({
      chunks: [{ owner: "after", text: "cd" }], matchedSequence: null,
    });
  });

  it("does not delay an unambiguous stop or a winning configured tie", () => {
    const ordinary = new UnicodeStopFilter<string>(["STOP", "unrelated-long-stop"]);
    expect(ordinary.append("owner", "before STOP")).toEqual({
      chunks: [{ owner: "owner", text: "before " }], matchedSequence: "STOP",
    });
    const tie = new UnicodeStopFilter<string>(["ab", "abcd"]);
    expect(tie.append("owner", "zab")).toEqual({
      chunks: [{ owner: "owner", text: "z" }], matchedSequence: "ab",
    });
  });

  it("clears a pending candidate and still rejects a dangling scalar at a boundary", () => {
    const filter = new UnicodeStopFilter<string>(["abcd", "bc"]);
    filter.append("old", "zabc");
    filter.clear();
    expect(filter.append("new", "d")).toEqual({ chunks: [], matchedSequence: null });
    expect(filter.endSegment()).toEqual({
      chunks: [{ owner: "new", text: "d" }], matchedSequence: null,
    });
    filter.append("invalid", "zabc\ud83d");
    expect(() => filter.endSegment()).toThrow("within an astral character");
  });

  it("keeps maximum-length stop sets bounded on large near-matching input", () => {
    const prefix = "a".repeat(8_190);
    const stops = ["b", "c", "d", "e"].map(end => prefix + end);
    const filter = new UnicodeStopFilter<string>(stops);
    const value = "a".repeat(1024 * 1024);
    const started = performance.now();
    const first = filter.append("large", value);
    expect(first.matchedSequence).toBeNull();
    expect(combined(first.chunks)).toBe(value.slice(0, -8_190));
    expect(filter.retainedCodePoints()).toBe(8_190);
    const second = filter.append("last", "c");
    expect(second).toEqual({ chunks: [], matchedSequence: stops[1] });
    expect(filter.retainedCodePoints()).toBe(0);
    expect(performance.now() - started).toBeLessThan(2_000);
  });

  it("updates long stop matches incrementally instead of rescanning the suffix", () => {
    const stop = `${"a".repeat(4_095)}b`;
    const filter = new UnicodeStopFilter<string>([stop]);
    const started = performance.now();

    for (let index = 0; index < 100_000; index += 1) {
      const result = filter.append("owner", "a");
      expect(result.matchedSequence).toBeNull();
    }

    const elapsedMs = performance.now() - started;
    expect(elapsedMs).toBeLessThan(1_000);
    expect(filter.retainedCodePoints()).toBe(4_095);
  });
});
