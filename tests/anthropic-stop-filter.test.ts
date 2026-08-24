import { describe, expect, it } from "bun:test";
import {
  assertUnicodeWellFormed,
  UnicodeStopFilter,
} from "../src/anthropic/stream-stop-filter";

function combined<T>(chunks: Array<{ owner: T; text: string }>): string {
  return chunks.map(chunk => chunk.text).join("");
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
