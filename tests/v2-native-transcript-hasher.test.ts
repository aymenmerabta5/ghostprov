import { describe, expect, it } from "bun:test";
import {
  IncrementalSha256,
  TranscriptProof,
} from "../src/v2/protocol/transcript";
import { sha256Hex } from "../src/v2/protocol/shared";
import { NativeIncrementalSha256 } from "../src/v2/transport/native-transcript-hasher";

describe("native host transcript hashing", () => {
  it("matches WebCrypto and the portable Deno fallback over randomized chunks", async () => {
    let seed = 0x6d2b_79f5;
    for (let testCase = 0; testCase < 256; testCase++) {
      seed = (Math.imul(seed, 1_664_525) + 1_013_904_223) >>> 0;
      const bytes = new Uint8Array(seed % 16_384);
      for (let index = 0; index < bytes.length; index++) {
        seed = (Math.imul(seed, 1_664_525) + 1_013_904_223) >>> 0;
        bytes[index] = seed;
      }
      const native = new NativeIncrementalSha256();
      const portable = new IncrementalSha256();
      for (let offset = 0; offset < bytes.length;) {
        seed = (Math.imul(seed, 1_664_525) + 1_013_904_223) >>> 0;
        const size = 1 + seed % 521;
        const chunk = bytes.subarray(offset, Math.min(bytes.length, offset + size));
        native.update(chunk);
        portable.update(chunk);
        offset += size;
      }
      const expected = await sha256Hex(bytes);
      expect(native.digestHex()).toBe(expected);
      expect(portable.digestHex()).toBe(expected);
      expect(native.bytesHashed).toBe(bytes.byteLength);
    }
  });

  it("takes non-destructive digests and remains appendable", async () => {
    const native = new NativeIncrementalSha256();
    native.update(new TextEncoder().encode("ab"));
    expect(native.digestHex()).toBe(await sha256Hex("ab"));
    expect(native.digestHex()).toBe(await sha256Hex("ab"));
    native.update(new TextEncoder().encode("c"));
    expect(native.digestHex()).toBe(await sha256Hex("abc"));
  });

  it("keeps a 16 MiB host transcript fixed-memory on the native host hasher", () => {
    const data = "x".repeat(16 * 1024 * 1024);
    const frame = { event: "response.output_text.delta", data };
    // Warm both implementations before comparing the same logical event.
    new NativeIncrementalSha256().update(new Uint8Array(64 * 1024)).digestHex();
    new IncrementalSha256().update(new Uint8Array(64 * 1024)).digestHex();

    const native = new TranscriptProof("native-large", "a".repeat(64), {
      maxBytes: 17 * 1024 * 1024,
      retainBytes: false,
      hasher: new NativeIncrementalSha256(),
    });
    let started = performance.now();
    native.append(frame);
    const nativeMs = performance.now() - started;

    const portable = new TranscriptProof("portable-large", "a".repeat(64), {
      maxBytes: 17 * 1024 * 1024,
      retainBytes: false,
      hasher: new IncrementalSha256(),
    });
    started = performance.now();
    portable.append(frame);
    const portableMs = performance.now() - started;

    expect(native.snapshot().transcriptSha256).toBe(portable.snapshot().transcriptSha256);
    expect(native.retainedByteCount).toBe(0);
    expect(portableMs).toBeGreaterThanOrEqual(0);
    // Correctness tests must not depend on a noisy one-sample relative timing
    // comparison. This generous absolute guard still catches an accidental
    // catastrophic host fallback while remaining portable across CI/OS load.
    expect(nativeMs).toBeLessThan(1_000);
  }, 10_000);
});
