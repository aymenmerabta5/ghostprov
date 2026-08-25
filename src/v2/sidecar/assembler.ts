import { Gunzip } from "fflate";
import {
  GP_V2_REQUEST_MAX_BYTES,
  base64UrlDecode,
  sha256Hex,
  type HostFrame,
  type RequestCommitFrame,
  type ReliableFrame,
  type RequestEncoding,
} from "../protocol/shared";

export type RequestAssemblyErrorCode =
  | "bounds"
  | "gap"
  | "overlap"
  | "conflict"
  | "digest"
  | "encoding";

export class RequestAssemblyError extends Error {
  readonly name = "RequestAssemblyError";

  constructor(message: string, readonly code: RequestAssemblyErrorCode) {
    super(message);
  }
}

interface RequestPart {
  offset: number;
  bytes: Uint8Array;
}

interface RequestState {
  requestId: string;
  requestDigest: string;
  decodedBytes: number;
  encodedBytes: number;
  encoding: RequestEncoding;
  partCount: number;
  reservationBytes: number;
  lastActivityAtMs: number;
  receivedParts: number;
  parts: Array<RequestPart | undefined>;
}

function bytesEqual(left: Uint8Array, right: Uint8Array): boolean {
  if (left.byteLength !== right.byteLength) return false;
  let difference = 0;
  for (let index = 0; index < left.length; index++) difference |= left[index] ^ right[index];
  return difference === 0;
}

function validPositive(value: number, maximum: number): boolean {
  return Number.isSafeInteger(value) && value >= 1 && value <= maximum;
}

export class RequestAssembler {
  // Dense array slot + RequestPart object + Uint8Array view/backing metadata.
  // Payload bytes are charged separately; this intentionally rounds upward.
  static readonly PART_METADATA_BYTES = 128;

  readonly #maxReservedBytes: number;
  readonly #maxRequests: number;
  readonly #maxPartsPerRequest: number;
  readonly #incompleteTtlMs: number;
  readonly #now: () => number;
  readonly #requests = new Map<string, RequestState>();
  #reservedBytes = 0;

  constructor(options: {
    maxReservedBytes?: number;
    maxRequests?: number;
    maxPartsPerRequest?: number;
    incompleteTtlMs?: number;
    now?: () => number;
  } = {}) {
    this.#maxReservedBytes = options.maxReservedBytes ?? 256 * 1024 * 1024;
    this.#maxRequests = options.maxRequests ?? 256;
    this.#maxPartsPerRequest = options.maxPartsPerRequest ?? 8_192;
    this.#incompleteTtlMs = options.incompleteTtlMs ?? 15 * 60_000;
    this.#now = options.now ?? (() => Date.now());
    if (!validPositive(this.#maxReservedBytes, Number.MAX_SAFE_INTEGER)) {
      throw new RangeError("maxReservedBytes must be a positive safe integer");
    }
    if (!validPositive(this.#maxRequests, 65_536)) {
      throw new RangeError("maxRequests must be an integer from 1 through 65536");
    }
    if (!validPositive(this.#maxPartsPerRequest, 65_536)) {
      throw new RangeError("maxPartsPerRequest must be an integer from 1 through 65536");
    }
    if (!Number.isFinite(this.#incompleteTtlMs) || this.#incompleteTtlMs < 1) {
      throw new RangeError("incompleteTtlMs must be positive and finite");
    }
  }

  accept(frame: ReliableFrame<HostFrame>): void {
    if (frame.kind === "request_open") {
      this.#open(frame);
      return;
    }
    if (frame.kind === "request_data") {
      this.#data(frame);
      return;
    }
    throw new RequestAssemblyError(
      `Assembler does not accept ${frame.kind} frames`,
      "encoding",
    );
  }

  async commit(commitment: string | RequestCommitFrame): Promise<Uint8Array> {
    const requestId = typeof commitment === "string" ? commitment : commitment.requestId;
    const state = this.#requests.get(requestId);
    if (!state) throw new RequestAssemblyError("Request was not opened", "gap");
    if (this.#isExpired(state)) {
      this.cancel(requestId);
      throw new RequestAssemblyError("Request assembly expired", "gap");
    }
    try {
      if (
        typeof commitment !== "string" &&
        "protocolRevision" in commitment &&
        commitment.protocolRevision === "r2" &&
        (
          commitment.requestDigest !== state.requestDigest ||
          commitment.decodedBytes !== state.decodedBytes ||
          commitment.encodedBytes !== state.encodedBytes ||
          commitment.encoding !== state.encoding ||
          commitment.partCount !== state.partCount
        )
      ) {
        throw new RequestAssemblyError(
          "Request commit manifest conflicts with request open",
          "conflict",
        );
      }
      if (state.receivedParts !== state.partCount) {
        throw new RequestAssemblyError("Request has missing parts", "gap");
      }
      let expectedOffset = 0;
      for (let partIndex = 0; partIndex < state.partCount; partIndex++) {
        const part = state.parts[partIndex];
        if (!part || part.offset !== expectedOffset) {
          throw new RequestAssemblyError("Request has a byte gap", "gap");
        }
        expectedOffset += part.bytes.byteLength;
      }
      if (expectedOffset !== state.encodedBytes) {
        throw new RequestAssemblyError("Request encoded size does not match", "bounds");
      }
      const decoded = state.encoding === "identity"
        ? this.#identity(state)
        : this.#gunzip(state);
      if (await sha256Hex(decoded) !== state.requestDigest) {
        decoded.fill(0);
        throw new RequestAssemblyError("Request digest does not match", "digest");
      }
      return decoded;
    } finally {
      this.cancel(requestId);
    }
  }

  cancel(requestId: string): void {
    const state = this.#requests.get(requestId);
    if (!state) return;
    for (const part of state.parts) part?.bytes.fill(0);
    this.#requests.delete(requestId);
    this.#reservedBytes -= state.reservationBytes;
  }

  snapshot(): { requests: number; reservedBytes: number } {
    return {
      requests: this.#requests.size,
      reservedBytes: this.#reservedBytes,
    };
  }

  sweepExpired(nowMs = this.#now()): readonly string[] {
    if (!Number.isFinite(nowMs) || nowMs < 0) {
      throw new RangeError("sweep time must be finite and non-negative");
    }
    const cutoff = nowMs - this.#incompleteTtlMs;
    const expired: string[] = [];
    for (const [requestId, state] of this.#requests) {
      if (state.lastActivityAtMs > cutoff) continue;
      expired.push(requestId);
      this.cancel(requestId);
    }
    return Object.freeze(expired);
  }

  #open(frame: Extract<ReliableFrame<HostFrame>, { kind: "request_open" }>): void {
    if (
      !validPositive(frame.decodedBytes, GP_V2_REQUEST_MAX_BYTES) ||
      !validPositive(frame.encodedBytes, GP_V2_REQUEST_MAX_BYTES) ||
      !validPositive(frame.partCount, this.#maxPartsPerRequest) ||
      frame.partCount > frame.encodedBytes ||
      !/^[a-f0-9]{64}$/.test(frame.requestDigest)
    ) {
      throw new RequestAssemblyError("Request declaration exceeds bounds", "bounds");
    }
    let existing = this.#requests.get(frame.requestId);
    if (existing && this.#isExpired(existing)) {
      this.cancel(frame.requestId);
      existing = undefined;
    }
    if (existing) {
      if (
        existing.requestDigest === frame.requestDigest &&
        existing.decodedBytes === frame.decodedBytes &&
        existing.encodedBytes === frame.encodedBytes &&
        existing.encoding === frame.encoding &&
        existing.partCount === frame.partCount
      ) {
        existing.lastActivityAtMs = this.#now();
        return;
      }
      throw new RequestAssemblyError("Request open has a conflicting duplicate", "conflict");
    }
    // Parts remain resident until commit. Commit/decompression materializes the
    // decoded body alongside them, so reserve both representations rather than
    // claiming only the larger one and exceeding the configured memory bound.
    const reservationBytes = frame.decodedBytes + frame.encodedBytes +
      frame.partCount * RequestAssembler.PART_METADATA_BYTES;
    if (this.#requests.size >= this.#maxRequests) {
      throw new RequestAssemblyError("Assembler request capacity is exhausted", "bounds");
    }
    if (this.#reservedBytes + reservationBytes > this.#maxReservedBytes) {
      throw new RequestAssemblyError("Assembler reservation budget is exhausted", "bounds");
    }
    this.#requests.set(frame.requestId, {
      requestId: frame.requestId,
      requestDigest: frame.requestDigest,
      decodedBytes: frame.decodedBytes,
      encodedBytes: frame.encodedBytes,
      encoding: frame.encoding,
      partCount: frame.partCount,
      reservationBytes,
      lastActivityAtMs: this.#now(),
      receivedParts: 0,
      parts: new Array<RequestPart | undefined>(frame.partCount),
    });
    this.#reservedBytes += reservationBytes;
  }

  #data(frame: Extract<ReliableFrame<HostFrame>, { kind: "request_data" }>): void {
    const state = this.#requests.get(frame.requestId);
    if (!state) throw new RequestAssemblyError("Request data arrived before open", "gap");
    if (this.#isExpired(state)) {
      this.cancel(frame.requestId);
      throw new RequestAssemblyError("Request assembly expired", "gap");
    }
    if (
      !Number.isSafeInteger(frame.partIndex) ||
      frame.partIndex < 0 ||
      frame.partIndex >= state.partCount ||
      !Number.isSafeInteger(frame.offset) ||
      frame.offset < 0
    ) {
      throw new RequestAssemblyError("Request part index or offset is invalid", "bounds");
    }
    let bytes: Uint8Array;
    try {
      bytes = base64UrlDecode(frame.dataBase64Url);
    } catch {
      throw new RequestAssemblyError("Request part encoding is invalid", "encoding");
    }
    if (
      bytes.byteLength < 1 ||
      frame.offset + bytes.byteLength > state.encodedBytes
    ) {
      bytes.fill(0);
      throw new RequestAssemblyError("Request part exceeds declared bounds", "bounds");
    }
    const existing = state.parts[frame.partIndex];
    if (existing) {
      const identical = existing.offset === frame.offset && bytesEqual(existing.bytes, bytes);
      bytes.fill(0);
      if (identical) {
        state.lastActivityAtMs = this.#now();
        return;
      }
      throw new RequestAssemblyError("Request part has a conflicting duplicate", "conflict");
    }
    const end = frame.offset + bytes.byteLength;
    // Logical part indexes define byte order. Checking the adjacent received
    // indexes makes normal ingestion O(1) per part; any overlap whose neighbor
    // has not arrived yet is still caught by the contiguous O(P) commit pass.
    const previous = state.parts[frame.partIndex - 1];
    const next = state.parts[frame.partIndex + 1];
    if (
      (previous && previous.offset + previous.bytes.byteLength > frame.offset) ||
      (next && end > next.offset)
    ) {
      bytes.fill(0);
      throw new RequestAssemblyError("Request parts overlap", "overlap");
    }
    state.parts[frame.partIndex] = {
      offset: frame.offset,
      bytes,
    };
    state.receivedParts++;
    state.lastActivityAtMs = this.#now();
  }

  #identity(state: RequestState): Uint8Array {
    if (state.encodedBytes !== state.decodedBytes) {
      throw new RequestAssemblyError("Identity request sizes do not match", "bounds");
    }
    const only = state.partCount === 1 ? state.parts[0] : undefined;
    if (only) {
      // Transfer ownership of a single decoded part instead of copying it. The
      // finally/cancel path must not erase the returned request body.
      state.parts[0] = undefined;
      state.receivedParts = 0;
      return only.bytes;
    }
    const output = new Uint8Array(state.decodedBytes);
    for (const part of state.parts) output.set(part!.bytes, part!.offset);
    return output;
  }

  #gunzip(state: RequestState): Uint8Array {
    const output = new Uint8Array(state.decodedBytes);
    let total = 0;
    const gunzip = new Gunzip((chunk, _final) => {
      const next = total + chunk.byteLength;
      if (next > state.decodedBytes || next > GP_V2_REQUEST_MAX_BYTES) {
        throw new RequestAssemblyError("Decompressed request exceeds declared bounds", "bounds");
      }
      output.set(chunk, total);
      total = next;
    });
    try {
      for (let partIndex = 0; partIndex < state.partCount; partIndex++) {
        gunzip.push(state.parts[partIndex]!.bytes, partIndex === state.partCount - 1);
      }
    } catch (error) {
      output.fill(0);
      if (error instanceof RequestAssemblyError) throw error;
      throw new RequestAssemblyError("Request gzip stream is invalid", "encoding");
    }
    if (total !== state.decodedBytes) {
      output.fill(0);
      throw new RequestAssemblyError("Decompressed request size does not match", "bounds");
    }
    return output;
  }

  #isExpired(state: RequestState): boolean {
    return state.lastActivityAtMs <= this.#now() - this.#incompleteTtlMs;
  }
}
