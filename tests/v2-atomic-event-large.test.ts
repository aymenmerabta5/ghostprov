import { describe, expect, it } from "bun:test";
import {
  MAX_TRANSCRIPT_CHUNK_BYTES,
  TranscriptProof,
} from "../src/v2/protocol/transcript";
import { AtomicBytePermitPool } from "../src/v2/memory/atomic-byte-permits";
import {
  estimateAtomicEventPermitBytes,
  IncrementalSseEventParser,
} from "../src/v2/sidecar/upstream";

const MiB = 1024 * 1024;

describe("64 MiB atomic SSE event", () => {
  it("hashes the exact cap with <=64 KiB scratch and rejects cap + 1 before mutation", async () => {
    const cap = 64 * MiB;
    const eventName = "response.output_text.delta";
    const canonicalOverhead = 7 + eventName.length + 7 + 2;
    const data = "x".repeat(cap - canonicalOverhead);
    const transcript = new TranscriptProof("large-event", "a".repeat(64), {
      maxBytes: cap,
      retainBytes: false,
    });
    let chunks = 0;
    let streamedBytes = 0;
    let largestChunk = 0;
    let largestScratch = 0;
    const written = await transcript.appendCanonicalStream({ event: eventName, data }, {
      chunkBytes: MAX_TRANSCRIPT_CHUNK_BYTES,
      yieldTask: async () => {},
      onScratchBytes: value => { largestScratch = Math.max(largestScratch, value); },
      onChunk: value => {
        chunks++;
        streamedBytes += value.byteLength;
        largestChunk = Math.max(largestChunk, value.byteLength);
      },
    });

    expect(written).toBe(cap);
    expect(streamedBytes).toBe(cap);
    expect(chunks).toBe(1024);
    expect(largestChunk).toBeLessThanOrEqual(MAX_TRANSCRIPT_CHUNK_BYTES);
    expect(largestScratch).toBeLessThanOrEqual(MAX_TRANSCRIPT_CHUNK_BYTES);
    expect(transcript.snapshot()).toMatchObject({
      byteCount: cap,
      eventCount: 1,
      retainsBytes: false,
      canonicalBytes: null,
    });

    const tooLarge = new TranscriptProof("large-event", "a".repeat(64), {
      maxBytes: cap,
      retainBytes: false,
    });
    await expect(tooLarge.appendCanonicalStream({
      event: eventName,
      data: data + "x",
    }, {
      yieldTask: async () => {},
    })).rejects.toMatchObject({ code: "byte_limit" });
    expect(tooLarge.snapshot()).toMatchObject({ eventCount: 0, byteCount: 0 });
  }, 30_000);

  it("charges the 64 MiB logical cap as a 257 MiB physical jumbo event", async () => {
    const cap = 64 * MiB;
    const charged = estimateAtomicEventPermitBytes(cap);
    expect(charged).toBe(257 * MiB);
    expect(() => estimateAtomicEventPermitBytes(Number.MAX_SAFE_INTEGER)).toThrow(RangeError);
    const pool = new AtomicBytePermitPool({
      ordinaryBytes: 2 * MiB,
      jumboBytes: charged,
    });
    const parser = new IncrementalSseEventParser({
      maxEventBytes: cap,
      permits: pool,
    });
    const chunk = new Uint8Array(64 * 1024).fill("x".charCodeAt(0));
    for (let index = 0; index < 1024; index++) {
      await parser.push(chunk, () => { throw new Error("unexpected event"); });
    }
    expect(parser.snapshot()).toMatchObject({
      currentEventBytes: cap,
      currentLogicalReservedBytes: cap,
      currentReservedBytes: charged,
      currentRegion: "jumbo",
      maxScratchBytes: 64 * 1024,
    });
    expect(pool.snapshot()).toMatchObject({
      ordinaryUsedBytes: 0,
      jumboUsedBytes: charged,
      activePermits: 1,
    });
    await expect(parser.push(Uint8Array.of(0x78), () => {})).rejects.toMatchObject({
      code: "event_too_large",
    });
    expect(pool.snapshot().jumboUsedBytes).toBe(charged);
    parser.abort();
    expect(pool.snapshot()).toMatchObject({ activePermits: 0, jumboUsedBytes: 0 });
  }, 30_000);
});
