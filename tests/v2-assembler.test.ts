import { describe, expect, it } from "bun:test";
import { gzipSync } from "fflate";
import {
  GP_V2_REQUEST_MAX_BYTES,
  base64UrlDecode,
  base64UrlEncode,
  sha256Hex,
  type HostFrame,
  type ReliableFrame,
  type RequestEncoding,
} from "../src/v2/protocol/shared";
import {
  RequestAssembler,
  RequestAssemblyError,
} from "../src/v2/sidecar/assembler";
import { AtomicBytePermitPool } from "../src/v2/memory/atomic-byte-permits";
import {
  atomicBytePermitOf,
  releaseAtomicBytePermit,
} from "../src/v2/memory/permit-carrier";

const REQUEST = "req-assembly-1";

async function frames(
  decoded: Uint8Array,
  options: { encoding?: RequestEncoding; partBytes?: number } = {},
): Promise<ReliableFrame<HostFrame>[]> {
  const encoding = options.encoding ?? "identity";
  const encoded = encoding === "identity" ? decoded : gzipSync(decoded);
  const partBytes = options.partBytes ?? 11;
  const partCount = Math.ceil(encoded.byteLength / partBytes);
  const result: ReliableFrame<HostFrame>[] = [{
    kind: "request_open",
    requestId: REQUEST,
    requestDigest: await sha256Hex(decoded),
    decodedBytes: decoded.byteLength,
    encodedBytes: encoded.byteLength,
    encoding,
    partCount,
    epoch: 1,
    sequence: 0,
  }];
  for (let offset = 0, partIndex = 0; offset < encoded.length; offset += partBytes, partIndex++) {
    result.push({
      kind: "request_data",
      requestId: REQUEST,
      partIndex,
      offset,
      dataBase64Url: base64UrlEncode(encoded.subarray(offset, offset + partBytes)),
      epoch: 1,
      sequence: result.length,
    });
  }
  return result;
}

describe("RequestAssembler", () => {
  it("transfers one global request-memory permit into the committed body", async () => {
    const decoded = new TextEncoder().encode(JSON.stringify({ input: "owned request" }));
    const requestFrames = await frames(decoded, { partBytes: 7 });
    const declared = requestFrames[0] as Extract<ReliableFrame<HostFrame>, { kind: "request_open" }>;
    const reservation = declared.decodedBytes + declared.encodedBytes +
      declared.partCount * RequestAssembler.PART_METADATA_BYTES;
    const pool = new AtomicBytePermitPool({ ordinaryBytes: reservation, jumboBytes: 1 });
    const assembler = new RequestAssembler({
      maxReservedBytes: reservation,
      atomicBytePermits: pool,
    });
    for (const frame of requestFrames) assembler.accept(frame);
    expect(pool.snapshot().ordinaryUsedBytes).toBe(reservation);

    const body = await assembler.commit(REQUEST);
    expect(atomicBytePermitOf(body)).toMatchObject({ active: true, bytes: reservation });
    expect(assembler.snapshot()).toEqual({ requests: 0, reservedBytes: 0 });
    releaseAtomicBytePermit(body);
    expect(pool.snapshot()).toMatchObject({ ordinaryUsedBytes: 0, activePermits: 0 });
  });

  it("reassembles reordered identity parts after exact integrity proof", async () => {
    const decoded = new TextEncoder().encode(JSON.stringify({ model: "gpt-5.6-sol", input: "hello 🌍" }));
    const requestFrames = await frames(decoded, { partBytes: 7 });
    const assembler = new RequestAssembler();
    assembler.accept(requestFrames[0]);
    for (const frame of requestFrames.slice(1).reverse()) assembler.accept(frame);

    await expect(assembler.commit(REQUEST)).resolves.toEqual(decoded);
    expect(assembler.snapshot()).toEqual({ requests: 0, reservedBytes: 0 });
  });

  it("decompresses gzip only after all parts and enforces decoded length", async () => {
    const decoded = new TextEncoder().encode(JSON.stringify({ input: "compressible ".repeat(1_000) }));
    const requestFrames = await frames(decoded, { encoding: "gzip+base64", partBytes: 13 });
    const assembler = new RequestAssembler();
    for (const frame of requestFrames) assembler.accept(frame);

    await expect(assembler.commit(REQUEST)).resolves.toEqual(decoded);
  });

  it("maps a corrupt native gzip stream to a bounded encoding failure", async () => {
    const decoded = new TextEncoder().encode(JSON.stringify({ input: "gzip integrity".repeat(100) }));
    const requestFrames = await frames(decoded, { encoding: "gzip+base64", partBytes: 13 });
    const finalIndex = requestFrames.length - 1;
    const finalPart = requestFrames[finalIndex] as Extract<
      ReliableFrame<HostFrame>,
      { kind: "request_data" }
    >;
    const corrupt = base64UrlDecode(finalPart.dataBase64Url);
    corrupt[corrupt.length - 1] ^= 0xff;
    requestFrames[finalIndex] = {
      ...finalPart,
      dataBase64Url: base64UrlEncode(corrupt),
    };
    const assembler = new RequestAssembler();
    for (const frame of requestFrames) assembler.accept(frame);

    await expect(assembler.commit(REQUEST)).rejects.toMatchObject({ code: "encoding" });
    expect(assembler.snapshot()).toEqual({ requests: 0, reservedBytes: 0 });
  });

  it("accepts identical duplicates but rejects conflicting duplicate parts", async () => {
    const decoded = new TextEncoder().encode("duplicate-check");
    const requestFrames = await frames(decoded, { partBytes: 8 });
    const firstPart = requestFrames[1] as Extract<ReliableFrame<HostFrame>, { kind: "request_data" }>;
    const assembler = new RequestAssembler();
    assembler.accept(requestFrames[0]);
    assembler.accept(firstPart);
    assembler.accept(firstPart);
    expect(() => assembler.accept({
      ...firstPart,
      dataBase64Url: base64UrlEncode(new TextEncoder().encode("conflict")),
    })).toThrow("conflicting duplicate");
  });

  it("rejects gaps, overlaps, wrong digests, and declared-size mismatches", async () => {
    const decoded = new TextEncoder().encode("0123456789abcdef");
    const requestFrames = await frames(decoded, { partBytes: 8 });
    const open = requestFrames[0] as Extract<ReliableFrame<HostFrame>, { kind: "request_open" }>;
    const firstPart = requestFrames[1] as Extract<ReliableFrame<HostFrame>, { kind: "request_data" }>;
    const secondPart = requestFrames[2] as Extract<ReliableFrame<HostFrame>, { kind: "request_data" }>;

    const gap = new RequestAssembler();
    gap.accept(open);
    gap.accept(firstPart);
    await expect(gap.commit(REQUEST)).rejects.toMatchObject({ code: "gap" });

    const overlap = new RequestAssembler();
    overlap.accept(open);
    overlap.accept(firstPart);
    expect(() => overlap.accept({ ...secondPart, offset: 4 })).toThrow("overlap");

    const reverseOverlap = new RequestAssembler();
    reverseOverlap.accept(open);
    reverseOverlap.accept({ ...secondPart, offset: 4 });
    expect(() => reverseOverlap.accept(firstPart)).toThrow("overlap");

    const digest = new RequestAssembler();
    digest.accept({ ...open, requestDigest: "0".repeat(64) });
    for (const frame of requestFrames.slice(1)) digest.accept(frame);
    await expect(digest.commit(REQUEST)).rejects.toMatchObject({ code: "digest" });
  });

  it("rejects oversized declarations before reserving memory", () => {
    const assembler = new RequestAssembler();
    expect(() => assembler.accept({
      kind: "request_open",
      requestId: REQUEST,
      requestDigest: "a".repeat(64),
      decodedBytes: GP_V2_REQUEST_MAX_BYTES + 1,
      encodedBytes: 1,
      encoding: "identity",
      partCount: 1,
      epoch: 1,
      sequence: 0,
    })).toThrow(RequestAssemblyError);
    expect(assembler.snapshot().reservedBytes).toBe(0);
  });

  it("reserves encoded and decoded representations for hostile compressed declarations", () => {
    const assembler = new RequestAssembler({ maxReservedBytes: 32 });
    expect(() => assembler.accept({
      kind: "request_open",
      requestId: REQUEST,
      requestDigest: "a".repeat(64),
      decodedBytes: 1,
      encodedBytes: 64,
      encoding: "gzip+base64",
      partCount: 1,
      epoch: 1,
      sequence: 0,
    })).toThrow("reservation budget");
    expect(assembler.snapshot().reservedBytes).toBe(0);
  });

  it("rejects part-count metadata amplification before allocating the index", () => {
    const assembler = new RequestAssembler();
    expect(() => assembler.accept({
      kind: "request_open",
      requestId: REQUEST,
      requestDigest: "a".repeat(64),
      decodedBytes: 1,
      encodedBytes: 1,
      encoding: "identity",
      partCount: 65_536,
      epoch: 1,
      sequence: 0,
    })).toThrow("exceeds bounds");
    expect(assembler.snapshot()).toEqual({ requests: 0, reservedBytes: 0 });
  });

  it("bounds tiny abandoned requests by count as well as declared bytes", async () => {
    const decoded = new TextEncoder().encode("x");
    const [first] = await frames(decoded);
    const open = first as Extract<ReliableFrame<HostFrame>, { kind: "request_open" }>;
    const assembler = new RequestAssembler({ maxRequests: 1 });
    assembler.accept({ ...open, requestId: "req-a" });
    expect(() => assembler.accept({ ...open, requestId: "req-b" })).toThrow(
      "request capacity",
    );
    assembler.cancel("req-a");
    expect(() => assembler.accept({ ...open, requestId: "req-b" })).not.toThrow();
  });

  it("accounts for dense part metadata and enforces the configured part bound", async () => {
    const decoded = new TextEncoder().encode("ab");
    const [first] = await frames(decoded, { partBytes: 1 });
    const open = first as Extract<ReliableFrame<HostFrame>, { kind: "request_open" }>;
    const assembler = new RequestAssembler({ maxPartsPerRequest: 2 });
    assembler.accept(open);
    expect(assembler.snapshot().reservedBytes).toBe(
      decoded.byteLength * 2 + 2 * RequestAssembler.PART_METADATA_BYTES,
    );
    assembler.cancel(REQUEST);
    expect(() => assembler.accept({ ...open, partCount: 3, encodedBytes: 3 })).toThrow(
      "exceeds bounds",
    );
  });

  it("sweeps incomplete assemblies after their bounded inactivity TTL", async () => {
    let now = 1_000;
    const decoded = new TextEncoder().encode("stale");
    const [open] = await frames(decoded);
    const assembler = new RequestAssembler({
      incompleteTtlMs: 10,
      now: () => now,
    });
    assembler.accept(open!);
    now = 1_009;
    expect(assembler.sweepExpired()).toEqual([]);
    now = 1_010;
    expect(assembler.sweepExpired()).toEqual([REQUEST]);
    expect(assembler.snapshot()).toEqual({ requests: 0, reservedBytes: 0 });
  });

  it("rejects a decompression bomb when output exceeds its declaration", async () => {
    const actual = new TextEncoder().encode("x".repeat(1_000_000));
    const requestFrames = await frames(actual, { encoding: "gzip+base64", partBytes: 1024 });
    const open = requestFrames[0] as Extract<ReliableFrame<HostFrame>, { kind: "request_open" }>;
    const assembler = new RequestAssembler();
    assembler.accept({ ...open, decodedBytes: 32 });
    for (const frame of requestFrames.slice(1)) assembler.accept(frame);

    await expect(assembler.commit(REQUEST)).rejects.toMatchObject({ code: "bounds" });
    expect(assembler.snapshot().reservedBytes).toBe(0);
  });

  it("releases reservations on cancellation", async () => {
    const decoded = new TextEncoder().encode("cancel");
    const requestFrames = await frames(decoded);
    const assembler = new RequestAssembler();
    assembler.accept(requestFrames[0]);
    expect(assembler.snapshot()).toEqual({
      requests: 1,
      reservedBytes: decoded.byteLength * 2 + RequestAssembler.PART_METADATA_BYTES,
    });
    assembler.cancel(REQUEST);
    expect(assembler.snapshot()).toEqual({ requests: 0, reservedBytes: 0 });
  });
});
