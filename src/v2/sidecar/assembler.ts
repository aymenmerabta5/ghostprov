import { Gunzip } from "fflate";
import {
  GP_V2_REQUEST_MAX_BYTES,
  base64UrlDecode,
  sha256Hex,
  type HostFrame,
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
  partIndex: number;
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
  parts: Map<number, RequestPart>;
}

function bytesEqual(left: Uint8Array, right: Uint8Array): boolean {
  if (left.byteLength !== right.byteLength) return false;
  let difference = 0;
  for (let index = 0; index < left.length; index++) difference |= left[index] ^ right[index];
  return difference === 0;
}

function concat(chunks: readonly Uint8Array[], total: number): Uint8Array {
  const output = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    output.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return output;
}

function validPositive(value: number, maximum: number): boolean {
  return Number.isSafeInteger(value) && value >= 1 && value <= maximum;
}

export class RequestAssembler {
  readonly #maxReservedBytes: number;
  readonly #requests = new Map<string, RequestState>();
  #reservedBytes = 0;

  constructor(options: { maxReservedBytes?: number } = {}) {
    this.#maxReservedBytes = options.maxReservedBytes ?? 256 * 1024 * 1024;
    if (!validPositive(this.#maxReservedBytes, Number.MAX_SAFE_INTEGER)) {
      throw new RangeError("maxReservedBytes must be a positive safe integer");
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

  async commit(requestId: string): Promise<Uint8Array> {
    const state = this.#requests.get(requestId);
    if (!state) throw new RequestAssemblyError("Request was not opened", "gap");
    try {
      if (state.parts.size !== state.partCount) {
        throw new RequestAssemblyError("Request has missing parts", "gap");
      }
      const ordered: RequestPart[] = [];
      let expectedOffset = 0;
      for (let partIndex = 0; partIndex < state.partCount; partIndex++) {
        const part = state.parts.get(partIndex);
        if (!part || part.offset !== expectedOffset) {
          throw new RequestAssemblyError("Request has a byte gap", "gap");
        }
        ordered.push(part);
        expectedOffset += part.bytes.byteLength;
      }
      if (expectedOffset !== state.encodedBytes) {
        throw new RequestAssemblyError("Request encoded size does not match", "bounds");
      }
      const encoded = concat(ordered.map(part => part.bytes), state.encodedBytes);
      const decoded = state.encoding === "identity"
        ? this.#identity(encoded, state)
        : this.#gunzip(encoded, state);
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
    for (const part of state.parts.values()) part.bytes.fill(0);
    this.#requests.delete(requestId);
    this.#reservedBytes -= state.reservationBytes;
  }

  snapshot(): { requests: number; reservedBytes: number } {
    return {
      requests: this.#requests.size,
      reservedBytes: this.#reservedBytes,
    };
  }

  #open(frame: Extract<ReliableFrame<HostFrame>, { kind: "request_open" }>): void {
    if (
      !validPositive(frame.decodedBytes, GP_V2_REQUEST_MAX_BYTES) ||
      !validPositive(frame.encodedBytes, GP_V2_REQUEST_MAX_BYTES) ||
      !validPositive(frame.partCount, 65_536) ||
      !/^[a-f0-9]{64}$/.test(frame.requestDigest)
    ) {
      throw new RequestAssemblyError("Request declaration exceeds bounds", "bounds");
    }
    const existing = this.#requests.get(frame.requestId);
    if (existing) {
      if (
        existing.requestDigest === frame.requestDigest &&
        existing.decodedBytes === frame.decodedBytes &&
        existing.encodedBytes === frame.encodedBytes &&
        existing.encoding === frame.encoding &&
        existing.partCount === frame.partCount
      ) return;
      throw new RequestAssemblyError("Request open has a conflicting duplicate", "conflict");
    }
    const reservationBytes = Math.max(frame.decodedBytes, frame.encodedBytes);
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
      parts: new Map(),
    });
    this.#reservedBytes += reservationBytes;
  }

  #data(frame: Extract<ReliableFrame<HostFrame>, { kind: "request_data" }>): void {
    const state = this.#requests.get(frame.requestId);
    if (!state) throw new RequestAssemblyError("Request data arrived before open", "gap");
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
    const existing = state.parts.get(frame.partIndex);
    if (existing) {
      const identical = existing.offset === frame.offset && bytesEqual(existing.bytes, bytes);
      bytes.fill(0);
      if (identical) return;
      throw new RequestAssemblyError("Request part has a conflicting duplicate", "conflict");
    }
    const end = frame.offset + bytes.byteLength;
    for (const part of state.parts.values()) {
      const partEnd = part.offset + part.bytes.byteLength;
      if (frame.offset < partEnd && end > part.offset) {
        bytes.fill(0);
        throw new RequestAssemblyError("Request parts overlap", "overlap");
      }
    }
    state.parts.set(frame.partIndex, {
      partIndex: frame.partIndex,
      offset: frame.offset,
      bytes,
    });
  }

  #identity(encoded: Uint8Array, state: RequestState): Uint8Array {
    if (state.encodedBytes !== state.decodedBytes) {
      encoded.fill(0);
      throw new RequestAssemblyError("Identity request sizes do not match", "bounds");
    }
    return encoded;
  }

  #gunzip(encoded: Uint8Array, state: RequestState): Uint8Array {
    const chunks: Uint8Array[] = [];
    let total = 0;
    const gunzip = new Gunzip((chunk, _final) => {
      total += chunk.byteLength;
      if (total > state.decodedBytes || total > GP_V2_REQUEST_MAX_BYTES) {
        throw new RequestAssemblyError("Decompressed request exceeds declared bounds", "bounds");
      }
      chunks.push(chunk.slice());
    });
    try {
      gunzip.push(encoded, true);
    } catch (error) {
      for (const chunk of chunks) chunk.fill(0);
      if (error instanceof RequestAssemblyError) throw error;
      throw new RequestAssemblyError("Request gzip stream is invalid", "encoding");
    } finally {
      encoded.fill(0);
    }
    if (total !== state.decodedBytes) {
      for (const chunk of chunks) chunk.fill(0);
      throw new RequestAssemblyError("Decompressed request size does not match", "bounds");
    }
    const output = concat(chunks, total);
    for (const chunk of chunks) chunk.fill(0);
    return output;
  }
}
