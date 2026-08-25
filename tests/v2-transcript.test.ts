import { describe, expect, it } from "bun:test";
import type { SseFrame } from "../src/relay/sse";
import {
  IncrementalSha256,
  canonicalJsonSha256,
  TranscriptProof,
  V2TerminalProofError,
  validateTerminalProof,
} from "../src/v2/protocol/transcript";
import { canonicalJson,sha256Hex, utf8Bytes } from "../src/v2/protocol/shared";

const REQUEST_ID = "req-terminal-1";
const REQUEST_DIGEST = "a".repeat(64);
const FINAL = {
  id: "resp_1",
  object: "response",
  status: "completed",
  output: [{ type: "message", content: [{ type: "output_text", text: "hello 🌍" }] }],
};

function event(event: string, data: unknown): SseFrame {
  return { event, data: JSON.stringify(data) };
}

function completedFrames(): SseFrame[] {
  return [
    event("response.created", { type: "response.created", response: { id: "resp_1" } }),
    event("response.output_text.delta", { type: "response.output_text.delta", delta: "hello 🌍" }),
    event("response.completed", { type: "response.completed", response: FINAL }),
  ];
}

describe("V2 transcript proof", () => {
  it("matches WebCrypto across block/padding boundaries and incremental chunking", async () => {
    for (const length of [0, 1, 55, 56, 63, 64, 65, 127, 128, 129, 1_025]) {
      const bytes = Uint8Array.from({ length }, (_, index) => (
        (index * 131 + length * 17) & 0xff
      ));
      const incremental = new IncrementalSha256();
      for (let offset = 0, step = 1; offset < bytes.byteLength; step++) {
        const size = [1, 7, 31, 64, 257][step % 5]!;
        incremental.update(bytes.subarray(offset, Math.min(bytes.byteLength, offset + size)));
        offset += size;
      }
      expect(incremental.bytesHashed).toBe(bytes.byteLength);
      expect(incremental.digestHex()).toBe(await sha256Hex(bytes));
      // Digest is non-destructive so terminal validation can snapshot safely.
      expect(incremental.digestHex()).toBe(await sha256Hex(bytes));
    }
    expect(new IncrementalSha256().update(utf8Bytes("abc")).digestHex()).toBe(
      "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
    );
    const appendAfterDigest = new IncrementalSha256().update(utf8Bytes("ab"));
    expect(appendAfterDigest.digestHex()).toBe(await sha256Hex("ab"));
    appendAfterDigest.update(utf8Bytes("c"));
    expect(appendAfterDigest.digestHex()).toBe(await sha256Hex("abc"));

    const mutable = utf8Bytes("mutable input view");
    const copied = mutable.slice();
    const mutationSafe = new IncrementalSha256().update(mutable.subarray(1, -1));
    mutable.fill(0);
    expect(mutationSafe.digestHex()).toBe(await sha256Hex(copied.subarray(1, -1)));

    const millionA = new Uint8Array(1_000_000).fill("a".charCodeAt(0));
    expect(new IncrementalSha256().update(millionA).digestHex()).toBe(
      "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0",
    );

    let seed = 0x1234_5678;
    for (let testCase = 0; testCase < 256; testCase++) {
      seed = (Math.imul(seed, 1_664_525) + 1_013_904_223) >>> 0;
      const bytes = new Uint8Array(seed % 8_192);
      for (let index = 0; index < bytes.length; index++) {
        seed = (Math.imul(seed, 1_664_525) + 1_013_904_223) >>> 0;
        bytes[index] = seed;
      }
      const incremental = new IncrementalSha256();
      for (let offset = 0; offset < bytes.length;) {
        seed = (Math.imul(seed, 1_664_525) + 1_013_904_223) >>> 0;
        const size = 1 + seed % 257;
        incremental.update(bytes.subarray(offset, Math.min(bytes.length, offset + size)));
        offset += size;
      }
      expect(incremental.digestHex()).toBe(await sha256Hex(bytes));
    }
  });

  it("yields while hashing one large event without changing its canonical proof", async () => {
    const frame = event("response.output_text.delta", {
      type: "response.output_text.delta",
      delta: "x".repeat(2 * 1024 * 1024),
    });
    const ordinary = new TranscriptProof(REQUEST_ID, REQUEST_DIGEST, {
      maxBytes: 3 * 1024 * 1024,
      retainBytes: false,
    });
    ordinary.append(frame);
    const chunked = new TranscriptProof(REQUEST_ID, REQUEST_DIGEST, {
      maxBytes: 3 * 1024 * 1024,
      retainBytes: false,
    });
    let yields = 0;
    await chunked.appendCanonicalChunked(frame, {
      chunkBytes: 64 * 1024,
      yieldTask: async () => { yields++; },
    });
    expect(yields).toBeGreaterThan(20);
    expect(chunked.retainedByteCount).toBe(0);
    expect(chunked.snapshot().transcriptSha256)
      .toBe(ordinary.snapshot().transcriptSha256);
  });

  it("bounds UTF-16 input slices for multi-byte jumbo data", async () => {
    const transcript = new TranscriptProof(REQUEST_ID, REQUEST_DIGEST, {
      maxBytes: 2 * 1024 * 1024,
      retainBytes: false,
    });
    let maxSliceUnits = 0;
    await transcript.appendCanonicalStream({
      event: "response.output_text.delta",
      data: "€".repeat(256 * 1024),
    }, {
      yieldTask: async () => {},
      onInputSliceUnits: units => { maxSliceUnits = Math.max(maxSliceUnits, units); },
    });
    expect(maxSliceUnits * 2).toBeLessThanOrEqual(64 * 1024);
  });

  it("fails closed instead of allocating a jumbo synchronous canonical buffer", () => {
    const transcript = new TranscriptProof(REQUEST_ID, REQUEST_DIGEST, {
      maxBytes: 2 * 1024 * 1024,
      retainBytes: false,
    });
    expect(() => transcript.appendCanonical({
      event: "response.output_text.delta",
      data: "x".repeat(128 * 1024),
    })).toThrow(expect.objectContaining({ code: "streaming_required" }));
    expect(transcript.snapshot()).toMatchObject({ eventCount: 0, byteCount: 0 });
    expect(() => transcript.appendCanonical({ event: 1, data: "x" } as any))
      .toThrow(expect.objectContaining({ code: "frame" }));
  });

  it("hashes canonical final JSON incrementally with frozen semantics", async () => {
    const value = {
      z: `quote\" slash\\ control\n lone\ud800 ${"🌍".repeat(100_000)}`,
      a: [true, null, -0, { y: "last", x: "first" }],
    };
    let yields = 0;
    let scratch = 0;
    const digest = await canonicalJsonSha256(value, {
      yieldTask: async () => { yields++; },
      onScratchBytes: value => { scratch = Math.max(scratch, value); },
    });
    expect(digest).toBe(await sha256Hex(canonicalJson(value)));
    expect(yields).toBeGreaterThan(1);
    expect(scratch).toBeLessThanOrEqual(64 * 1024);
  });

  it("buffers escape-heavy canonical strings instead of awaiting per character", async () => {
    const value = { text: "\"".repeat(200_000) };
    let yields = 0;
    const digest = await canonicalJsonSha256(value, {
      yieldTask: async () => { yields++; },
    });
    expect(digest).toBe(await sha256Hex(canonicalJson(value)));
    expect(yields).toBeGreaterThan(1);
    expect(yields).toBeLessThan(100);
  });

  it("accepts only a terminal matching request, response, counts, and transcript digest", async () => {
    const transcript = new TranscriptProof(REQUEST_ID, REQUEST_DIGEST);
    for (const frame of completedFrames()) transcript.append(frame);
    const terminal = await transcript.createTerminal(FINAL);

    // Frozen R1 wire proof: incremental validation must not alter canonical SSE.
    expect(terminal).toMatchObject({
      byteCount: 402,
      transcriptSha256: "09b8c880089afbe49789660be7814d8c3529a019e60437ba822a957b6c8316d4",
    });

    await expect(validateTerminalProof(terminal, transcript.snapshot())).resolves.toEqual(FINAL);
    await expect(validateTerminalProof({
      ...terminal,
      transcriptSha256: "0".repeat(64),
    }, transcript.snapshot())).rejects.toThrow("transcript digest");
    await expect(validateTerminalProof({
      ...terminal,
      responseId: "resp_wrong",
    }, transcript.snapshot())).rejects.toThrow("response ID");
    await expect(validateTerminalProof({
      ...terminal,
      eventCount: terminal.eventCount + 1,
    }, transcript.snapshot())).rejects.toThrow("event count");
    const mutatedSnapshot = transcript.snapshot();
    mutatedSnapshot.canonicalBytes[0] ^= 1;
    await expect(validateTerminalProof(terminal, mutatedSnapshot))
      .rejects.toThrow("transcript digest");
  });

  it("preserves arbitrary provider extension fields in the proven final", async () => {
    const extended = {
      ...FINAL,
      transport: { provider: "future-transport", region: "test" },
      provider_extension: { nested: [1, "two", true] },
      gp_sse: "provider-owned-extension",
    };
    const transcript = new TranscriptProof(REQUEST_ID, REQUEST_DIGEST);
    transcript.append(event("response.completed", {
      type: "response.completed",
      response: extended,
    }));
    const terminal = await transcript.createTerminal(extended);

    await expect(validateTerminalProof(terminal, transcript.snapshot())).resolves.toEqual(extended);
  });

  it("uses UTF-8 byte counts rather than JavaScript string length", async () => {
    const transcript = new TranscriptProof(REQUEST_ID, REQUEST_DIGEST);
    let returnedBytes = 0;
    for (const frame of completedFrames()) returnedBytes += transcript.append(frame);
    const terminal = await transcript.createTerminal();
    const canonical = completedFrames()
      .map(frame => `event: ${frame.event}\ndata: ${frame.data}\n\n`)
      .join("");

    expect(terminal.byteCount).toBe(new TextEncoder().encode(canonical).byteLength);
    expect(terminal.byteCount).toBeGreaterThan(canonical.length);
    expect(returnedBytes).toBe(terminal.byteCount);
    expect(transcript.eventCount).toBe(terminal.eventCount);
    expect(transcript.byteCount).toBe(terminal.byteCount);
  });

  it("rejects duplicate or missing terminal events", async () => {
    const duplicate = new TranscriptProof(REQUEST_ID, REQUEST_DIGEST);
    for (const frame of completedFrames()) duplicate.append(frame);
    expect(() => duplicate.append(completedFrames().at(-1)!)).toThrow("duplicate terminal");

    const missing = new TranscriptProof(REQUEST_ID, REQUEST_DIGEST);
    missing.append(event("response.created", { type: "response.created", response: { id: "resp_1" } }));
    await expect(missing.createTerminal()).rejects.toBeInstanceOf(V2TerminalProofError);
  });

  it("rejects mismatched final object and final-event digest", async () => {
    const transcript = new TranscriptProof(REQUEST_ID, REQUEST_DIGEST);
    for (const frame of completedFrames()) transcript.append(frame);
    await expect(transcript.createTerminal({ ...FINAL, id: "resp_other" })).rejects.toThrow("final object");
    const terminal = await transcript.createTerminal();
    await expect(validateTerminalProof({
      ...terminal,
      finalSha256: "f".repeat(64),
    }, transcript.snapshot())).rejects.toThrow("final digest");
  });

  it("enforces a reservation-backed transcript byte ceiling", () => {
    const transcript = new TranscriptProof(REQUEST_ID, REQUEST_DIGEST, { maxBytes: 32 });
    expect(() => transcript.append(event("response.output_text.delta", {
      type: "response.output_text.delta",
      delta: "x".repeat(64),
    }))).toThrow("byte limit");
  });

  it("proves a multi-MiB 4096-event transcript without retaining its bytes", async () => {
    const retained = new TranscriptProof(REQUEST_ID, REQUEST_DIGEST, {
      maxBytes: 8 * 1024 * 1024,
    });
    const incremental = new TranscriptProof(REQUEST_ID, REQUEST_DIGEST, {
      maxBytes: 8 * 1024 * 1024,
      retainBytes: false,
    });
    const frames: SseFrame[] = [];
    for (let index = 0; index < 4_095; index++) {
      frames.push({
        event: "response.output_text.delta",
        data: JSON.stringify({
          type: "response.output_text.delta",
          delta: `${String(index).padStart(4, "0")}:${"x".repeat(1_000)}`,
        }),
      });
    }
    frames.push(event("response.completed", {
      type: "response.completed",
      response: FINAL,
    }));
    for (const frame of frames) {
      retained.append(frame);
      incremental.append(frame);
    }

    const [retainedTerminal, incrementalTerminal] = await Promise.all([
      retained.createTerminal(),
      incremental.createTerminal(),
    ]);
    const retainedSnapshot = retained.snapshot();
    const incrementalSnapshot = incremental.snapshot();
    expect(incrementalTerminal).toEqual(retainedTerminal);
    expect(retainedTerminal.transcriptSha256).toBe(
      await sha256Hex(retainedSnapshot.canonicalBytes),
    );
    expect(retainedSnapshot.canonicalBytes.byteLength).toBeGreaterThan(4 * 1024 * 1024);
    expect(incremental.retainedByteCount).toBe(0);
    expect(incrementalSnapshot).toMatchObject({
      eventCount: 4_096,
      byteCount: retainedSnapshot.byteCount,
      canonicalBytes: null,
      retainsBytes: false,
      transcriptSha256: retainedTerminal.transcriptSha256,
    });
    await expect(validateTerminalProof(incrementalTerminal, incrementalSnapshot))
      .resolves.toEqual(FINAL);
  }, 30_000);

  it("keeps a 64 MiB-equivalent transcript at zero retained transcript bytes", async () => {
    const transcript = new TranscriptProof(REQUEST_ID, REQUEST_DIGEST, {
      maxBytes: 70 * 1024 * 1024,
      retainBytes: false,
    });
    const delta = "x".repeat(64 * 1024);
    let largestCanonicalChunk = 0;
    for (let index = 0; index < 1_024; index++) {
      await transcript.appendCanonicalStream({
        event: "response.output_text.delta",
        data: JSON.stringify({ type: "response.output_text.delta", index, delta }),
      }, {
        yieldTask: async () => {},
        onChunk: bytes => {
          largestCanonicalChunk = Math.max(largestCanonicalChunk, bytes.byteLength);
        },
      });
    }
    transcript.append(event("response.completed", {
      type: "response.completed",
      response: FINAL,
    }));

    const terminal = await transcript.createTerminal();
    expect(terminal.byteCount).toBeGreaterThan(64 * 1024 * 1024);
    expect(transcript.retainedByteCount).toBe(0);
    expect(transcript.snapshot().canonicalBytes).toBeNull();
    expect(largestCanonicalChunk).toBeLessThan(66 * 1024);
  }, 30_000);

  it("bundles the shared incremental proof for Deno without Node crypto", async () => {
    const result = await Bun.build({
      entrypoints: [`${import.meta.dir}/../sandbox/v2/main.ts`],
      target: "browser",
      format: "esm",
      minify: true,
    });
    expect(result.success).toBe(true);
    expect(result.logs).toEqual([]);
    const bundled = await result.outputs[0]!.text();
    expect(bundled).toContain("Incremental SHA-256 input");
    expect(bundled).not.toMatch(/node:crypto|from\s*["']crypto["']/);
  });
});
