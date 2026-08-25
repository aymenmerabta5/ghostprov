import type { SseFrame } from "../../relay/sse";
import {
  bytesToHex,
  sha256Hex,
  utf8Bytes,
  type SidecarFrame,
  type TerminalStatus,
} from "./shared";

export class V2TerminalProofError extends Error {
  readonly name = "V2TerminalProofError";

  constructor(message: string, readonly code: string) {
    super(message);
  }
}

export type ResponseTerminalProof = Extract<SidecarFrame, {
  kind: "response_terminal";
}>;

interface TranscriptSnapshotBase {
  requestId: string;
  requestDigest: string;
  eventCount: number;
  byteCount: number;
  final: Record<string, unknown> | null;
  finalEventIndex: number | null;
  terminalStatus: TerminalStatus | null;
}

export type TranscriptSnapshot<RetainsBytes extends boolean = true> =
  TranscriptSnapshotBase & (
    RetainsBytes extends true
      ? {
          retainsBytes: true;
          canonicalBytes: Uint8Array;
          transcriptSha256: null;
        }
      : {
          retainsBytes: false;
          canonicalBytes: null;
          transcriptSha256: string;
        }
  );

const SHA256_INITIAL = new Uint32Array([
  0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
  0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
]);

const SHA256_K = new Uint32Array([
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5,
  0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
  0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
  0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
  0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc,
  0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
  0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
  0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
  0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
  0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3,
  0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5,
  0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
  0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
  0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
]);

function rotateRight(value: number, bits: number): number {
  return (value >>> bits) | (value << (32 - bits));
}

export interface IncrementalSha256Hasher {
  readonly bytesHashed: number;
  update(value: Uint8Array): this;
  /** Must not finalize or otherwise mutate the live hash state. */
  digestHex(): string;
}

export interface ChunkedTranscriptAppendOptions {
  /** Maximum synchronous hashing work before yielding back to the runtime. */
  chunkBytes?: number;
  /** Injectable for deterministic tests. Production uses a zero-delay task. */
  yieldTask?: () => Promise<void>;
}

export interface StreamingTranscriptAppendOptions extends ChunkedTranscriptAppendOptions {
  /** Called in canonical byte order. Each view is at most `chunkBytes` long. */
  onChunk?(bytes: Uint8Array): void | Promise<void>;
  /** Test/diagnostic hook; never receives a value above 64 KiB. */
  onScratchBytes?(bytes: number): void;
  /** UTF-16 input slice size; multiply by two for its conservative byte size. */
  onInputSliceUnits?(units: number): void;
}

interface PreparedTranscriptAppend {
  event: string;
  data: string;
  byteLength: number;
  terminalFinal: Record<string, unknown> | null;
  terminalStatus: TerminalStatus | null;
}

export const MAX_TRANSCRIPT_CHUNK_BYTES = 64 * 1024;
const MAX_TRANSCRIPT_INPUT_SLICE_UNITS = 32 * 1024;

/** Exact TextEncoder-compatible UTF-8 length without allocating encoded bytes. */
export function utf8ByteLength(value: string): number {
  let total = 0;
  for (let index = 0; index < value.length; index++) {
    const code = value.charCodeAt(index);
    if (code <= 0x7f) {
      total++;
    } else if (code <= 0x7ff) {
      total += 2;
    } else if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (next >= 0xdc00 && next <= 0xdfff) {
        total += 4;
        index++;
      } else {
        // TextEncoder replaces an unpaired surrogate with U+FFFD.
        total += 3;
      }
    } else {
      // Includes unpaired low surrogates, also encoded as U+FFFD.
      total += 3;
    }
    if (total > Number.MAX_SAFE_INTEGER) {
      throw new RangeError("UTF-8 input is too large");
    }
  }
  return total;
}

export interface ChunkedStringWorkOptions {
  /** Maximum UTF-16 input units scanned before a real task yield. */
  chunkUnits?: number;
  yieldTask?: () => Promise<void>;
}

/** Yielding equivalent of utf8ByteLength for jumbo strings. */
export async function utf8ByteLengthChunked(
  value: string,
  options: ChunkedStringWorkOptions = {},
): Promise<number> {
  const chunkUnits = options.chunkUnits ?? MAX_TRANSCRIPT_INPUT_SLICE_UNITS;
  if (!Number.isSafeInteger(chunkUnits) || chunkUnits < 1 || chunkUnits > 32 * 1024) {
    throw new RangeError("chunkUnits must be between 1 and 32 Ki UTF-16 units");
  }
  const yieldTask = options.yieldTask ?? yieldTranscriptTask;
  let total = 0;
  let index = 0;
  while (index < value.length) {
    const end = Math.min(value.length, index + chunkUnits);
    while (index < end) {
      const code = value.charCodeAt(index++);
      if (code <= 0x7f) total++;
      else if (code <= 0x7ff) total += 2;
      else if (code >= 0xd800 && code <= 0xdbff) {
        const next = value.charCodeAt(index);
        if (next >= 0xdc00 && next <= 0xdfff) {
          total += 4;
          index++;
        } else total += 3;
      } else total += 3;
    }
    if (total > Number.MAX_SAFE_INTEGER) throw new RangeError("UTF-8 input is too large");
    if (index < value.length) await yieldTask();
  }
  return total;
}

function yieldTranscriptTask(): Promise<void> {
  return new Promise(resolve => {
    const timer = setTimeout(resolve, 0);
    timer.unref?.();
  });
}

/** Fixed-memory SHA-256 used by both Bun host code and bundled Deno code. */
export class IncrementalSha256 implements IncrementalSha256Hasher {
  readonly #state = new Uint32Array(SHA256_INITIAL);
  readonly #buffer = new Uint8Array(64);
  readonly #words = new Uint32Array(64);
  #bufferLength = 0;
  #bytesHashed = 0;

  get bytesHashed(): number {
    return this.#bytesHashed;
  }

  update(value: Uint8Array): this {
    if (!(value instanceof Uint8Array)) {
      throw new TypeError("Incremental SHA-256 input must be Uint8Array");
    }
    if (value.byteLength === 0) return this;
    if (this.#bytesHashed + value.byteLength > Number.MAX_SAFE_INTEGER) {
      throw new RangeError("Incremental SHA-256 input is too large");
    }
    this.#bytesHashed += value.byteLength;
    let offset = 0;
    if (this.#bufferLength > 0) {
      const take = Math.min(64 - this.#bufferLength, value.byteLength);
      this.#buffer.set(value.subarray(0, take), this.#bufferLength);
      this.#bufferLength += take;
      offset = take;
      if (this.#bufferLength === 64) {
        this.#compress(this.#buffer, 0);
        this.#bufferLength = 0;
      }
    }
    while (offset + 64 <= value.byteLength) {
      this.#compress(value, offset);
      offset += 64;
    }
    if (offset < value.byteLength) {
      this.#buffer.set(value.subarray(offset), 0);
      this.#bufferLength = value.byteLength - offset;
    }
    return this;
  }

  digest(): Uint8Array {
    const copy = this.#clone();
    return copy.#finish();
  }

  digestHex(): string {
    return bytesToHex(this.digest());
  }

  #clone(): IncrementalSha256 {
    const clone = new IncrementalSha256();
    clone.#state.set(this.#state);
    clone.#buffer.set(this.#buffer);
    clone.#bufferLength = this.#bufferLength;
    clone.#bytesHashed = this.#bytesHashed;
    return clone;
  }

  #finish(): Uint8Array {
    let offset = this.#bufferLength;
    this.#buffer[offset++] = 0x80;
    if (offset > 56) {
      this.#buffer.fill(0, offset, 64);
      this.#compress(this.#buffer, 0);
      offset = 0;
    }
    this.#buffer.fill(0, offset, 56);
    const high = Math.floor(this.#bytesHashed / 0x2000_0000);
    const low = ((this.#bytesHashed % 0x2000_0000) * 8) >>> 0;
    this.#buffer[56] = high >>> 24;
    this.#buffer[57] = high >>> 16;
    this.#buffer[58] = high >>> 8;
    this.#buffer[59] = high;
    this.#buffer[60] = low >>> 24;
    this.#buffer[61] = low >>> 16;
    this.#buffer[62] = low >>> 8;
    this.#buffer[63] = low;
    this.#compress(this.#buffer, 0);

    const output = new Uint8Array(32);
    for (let index = 0; index < this.#state.length; index++) {
      const value = this.#state[index]!;
      const outputOffset = index * 4;
      output[outputOffset] = value >>> 24;
      output[outputOffset + 1] = value >>> 16;
      output[outputOffset + 2] = value >>> 8;
      output[outputOffset + 3] = value;
    }
    return output;
  }

  #compress(input: Uint8Array, offset: number): void {
    const words = this.#words;
    for (let index = 0; index < 16; index++) {
      const start = offset + index * 4;
      words[index] = (
        input[start]! << 24 |
        input[start + 1]! << 16 |
        input[start + 2]! << 8 |
        input[start + 3]!
      ) >>> 0;
    }
    for (let index = 16; index < 64; index++) {
      const previous15 = words[index - 15]!;
      const previous2 = words[index - 2]!;
      const sigma0 = rotateRight(previous15, 7) ^
        rotateRight(previous15, 18) ^
        (previous15 >>> 3);
      const sigma1 = rotateRight(previous2, 17) ^
        rotateRight(previous2, 19) ^
        (previous2 >>> 10);
      words[index] = (
        words[index - 16]! + sigma0 + words[index - 7]! + sigma1
      ) >>> 0;
    }

    let a = this.#state[0]!;
    let b = this.#state[1]!;
    let c = this.#state[2]!;
    let d = this.#state[3]!;
    let e = this.#state[4]!;
    let f = this.#state[5]!;
    let g = this.#state[6]!;
    let h = this.#state[7]!;
    for (let index = 0; index < 64; index++) {
      const sum1 = rotateRight(e, 6) ^ rotateRight(e, 11) ^ rotateRight(e, 25);
      const choice = (e & f) ^ (~e & g);
      const temporary1 = (h + sum1 + choice + SHA256_K[index]! + words[index]!) >>> 0;
      const sum0 = rotateRight(a, 2) ^ rotateRight(a, 13) ^ rotateRight(a, 22);
      const majority = (a & b) ^ (a & c) ^ (b & c);
      const temporary2 = (sum0 + majority) >>> 0;
      h = g;
      g = f;
      f = e;
      e = (d + temporary1) >>> 0;
      d = c;
      c = b;
      b = a;
      a = (temporary1 + temporary2) >>> 0;
    }
    this.#state[0] = (this.#state[0]! + a) >>> 0;
    this.#state[1] = (this.#state[1]! + b) >>> 0;
    this.#state[2] = (this.#state[2]! + c) >>> 0;
    this.#state[3] = (this.#state[3]! + d) >>> 0;
    this.#state[4] = (this.#state[4]! + e) >>> 0;
    this.#state[5] = (this.#state[5]! + f) >>> 0;
    this.#state[6] = (this.#state[6]! + g) >>> 0;
    this.#state[7] = (this.#state[7]! + h) >>> 0;
  }
}

export interface CanonicalJsonHashOptions extends ChunkedStringWorkOptions {
  chunkBytes?: number;
  onScratchBytes?(bytes: number): void;
}

class CanonicalJsonHashSink {
  readonly #scratch: Uint8Array;
  readonly #encoder = new TextEncoder();
  readonly #hasher = new IncrementalSha256();
  readonly #yieldTask: () => Promise<void>;
  #scratchLength = 0;
  #yieldBeforeMoreWork = false;

  constructor(options: CanonicalJsonHashOptions) {
    const chunkBytes = options.chunkBytes ?? MAX_TRANSCRIPT_CHUNK_BYTES;
    if (!Number.isSafeInteger(chunkBytes) || chunkBytes < 4 || chunkBytes > 64 * 1024) {
      throw new RangeError("chunkBytes must be between 4 bytes and 64 KiB");
    }
    this.#scratch = new Uint8Array(chunkBytes);
    options.onScratchBytes?.(chunkBytes);
    this.#yieldTask = options.yieldTask ?? yieldTranscriptTask;
  }

  async write(value: string): Promise<void> {
    let offset = 0;
    while (offset < value.length) {
      if (this.#yieldBeforeMoreWork) {
        this.#yieldBeforeMoreWork = false;
        await this.#yieldTask();
      }
      const available = this.#scratch.byteLength - this.#scratchLength;
      let end = Math.min(
        value.length,
        offset + Math.min(available, MAX_TRANSCRIPT_INPUT_SLICE_UNITS),
      );
      if (
        end < value.length &&
        end > offset &&
        value.charCodeAt(end - 1) >= 0xd800 &&
        value.charCodeAt(end - 1) <= 0xdbff &&
        value.charCodeAt(end) >= 0xdc00 &&
        value.charCodeAt(end) <= 0xdfff
      ) end--;
      const result = this.#encoder.encodeInto(
        value.slice(offset, end),
        this.#scratch.subarray(this.#scratchLength),
      );
      if (result.read === 0 && result.written === 0) {
        this.#flush();
        continue;
      }
      offset += result.read;
      this.#scratchLength += result.written;
      if (this.#scratchLength === this.#scratch.byteLength) this.#flush();
    }
  }

  writeAscii(value: string): void {
    for (let index = 0; index < value.length; index++) {
      const code = value.charCodeAt(index);
      if (code > 0x7f) throw new TypeError("Canonical JSON ASCII fragment is invalid");
      this.#writeByte(code);
    }
  }

  writeCodePoint(codePoint: number): void {
    if (codePoint <= 0x7f) {
      this.#writeByte(codePoint);
    } else if (codePoint <= 0x7ff) {
      this.#writeByte(0xc0 | (codePoint >>> 6));
      this.#writeByte(0x80 | (codePoint & 0x3f));
    } else if (codePoint <= 0xffff) {
      this.#writeByte(0xe0 | (codePoint >>> 12));
      this.#writeByte(0x80 | ((codePoint >>> 6) & 0x3f));
      this.#writeByte(0x80 | (codePoint & 0x3f));
    } else {
      this.#writeByte(0xf0 | (codePoint >>> 18));
      this.#writeByte(0x80 | ((codePoint >>> 12) & 0x3f));
      this.#writeByte(0x80 | ((codePoint >>> 6) & 0x3f));
      this.#writeByte(0x80 | (codePoint & 0x3f));
    }
  }

  writeUnicodeEscape(code: number): void {
    const hex = "0123456789abcdef";
    this.writeAscii("\\u");
    this.#writeByte(hex.charCodeAt(code >>> 12 & 0xf));
    this.#writeByte(hex.charCodeAt(code >>> 8 & 0xf));
    this.#writeByte(hex.charCodeAt(code >>> 4 & 0xf));
    this.#writeByte(hex.charCodeAt(code & 0xf));
  }

  finish(): string {
    if (this.#scratchLength > 0) this.#flush();
    return this.#hasher.digestHex();
  }

  async checkpoint(): Promise<void> {
    if (this.#scratchLength > 0) this.#flush();
    if (this.#yieldBeforeMoreWork) {
      this.#yieldBeforeMoreWork = false;
      await this.#yieldTask();
    }
  }

  #flush(): void {
    if (this.#scratchLength === 0) return;
    this.#hasher.update(this.#scratch.subarray(0, this.#scratchLength));
    this.#scratchLength = 0;
    this.#yieldBeforeMoreWork = true;
  }

  #writeByte(value: number): void {
    this.#scratch[this.#scratchLength++] = value;
    if (this.#scratchLength === this.#scratch.byteLength) this.#flush();
  }
}

const JSON_SHORT_ESCAPES = new Map<number, string>([
  [0x08, "\\b"],
  [0x09, "\\t"],
  [0x0a, "\\n"],
  [0x0c, "\\f"],
  [0x0d, "\\r"],
  [0x22, "\\\""],
  [0x5c, "\\\\"],
]);

async function writeCanonicalString(
  value: string,
  sink: CanonicalJsonHashSink,
): Promise<void> {
  sink.writeAscii("\"");
  let scanned = 0;
  for (let index = 0; index < value.length;) {
    const code = value.charCodeAt(index);
    const shortEscape = JSON_SHORT_ESCAPES.get(code);
    if (shortEscape) {
      sink.writeAscii(shortEscape);
      index++;
    } else if (code <= 0x1f) {
      sink.writeUnicodeEscape(code);
      index++;
    } else if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (next >= 0xdc00 && next <= 0xdfff) {
        sink.writeCodePoint(0x1_0000 + ((code - 0xd800) << 10) + next - 0xdc00);
        index += 2;
      } else {
        sink.writeUnicodeEscape(code);
        index++;
      }
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      sink.writeUnicodeEscape(code);
      index++;
    } else {
      sink.writeCodePoint(code);
      index++;
    }
    if (++scanned >= MAX_TRANSCRIPT_INPUT_SLICE_UNITS) {
      scanned = 0;
      await sink.checkpoint();
    }
  }
  sink.writeAscii("\"");
}

async function writeCanonicalValue(
  value: unknown,
  sink: CanonicalJsonHashSink,
  seen: WeakSet<object>,
): Promise<void> {
  if (value === null) {
    await sink.write("null");
    return;
  }
  if (typeof value === "string") {
    await writeCanonicalString(value, sink);
    return;
  }
  if (typeof value === "boolean") {
    await sink.write(value ? "true" : "false");
    return;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new TypeError("Canonical JSON numbers must be finite");
    await sink.write(JSON.stringify(value));
    return;
  }
  if (typeof value !== "object") {
    throw new TypeError(`Canonical JSON does not support ${typeof value}`);
  }
  if (seen.has(value)) throw new TypeError("Canonical JSON does not support cycles");
  seen.add(value);
  try {
    if (Array.isArray(value)) {
      await sink.write("[");
      for (let index = 0; index < value.length; index++) {
        if (index > 0) await sink.write(",");
        // Preserve the existing Array.map(...).join(",") sparse-hole result.
        if (index in value) await writeCanonicalValue(value[index], sink, seen);
      }
      await sink.write("]");
      return;
    }
    await sink.write("{");
    const object = value as Record<string, unknown>;
    const keys = Object.keys(object).sort();
    for (let index = 0; index < keys.length; index++) {
      if (index > 0) await sink.write(",");
      const key = keys[index]!;
      await writeCanonicalString(key, sink);
      await sink.write(":");
      await writeCanonicalValue(object[key], sink, seen);
    }
    await sink.write("}");
  } finally {
    seen.delete(value);
  }
}

/** Hash canonical JSON without materializing its full string or UTF-8 bytes. */
export async function canonicalJsonSha256(
  value: unknown,
  options: CanonicalJsonHashOptions = {},
): Promise<string> {
  const sink = new CanonicalJsonHashSink(options);
  await writeCanonicalValue(value, sink, new WeakSet());
  return sink.finish();
}

const TERMINALS = new Map<string, TerminalStatus>([
  ["response.completed", "completed"],
  ["response.failed", "failed"],
  ["response.incomplete", "incomplete"],
]);

function object(value: unknown, label: string): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new V2TerminalProofError(`${label} must be an object`, "shape");
  }
  return value as Record<string, unknown>;
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

function responseIdentity(final: Record<string, unknown>): {
  id: string;
  status: TerminalStatus;
} {
  if (typeof final.id !== "string" || final.id.length === 0) {
    throw new V2TerminalProofError("Final response ID is missing", "response_id");
  }
  if (
    final.status !== "completed" &&
    final.status !== "failed" &&
    final.status !== "incomplete"
  ) {
    throw new V2TerminalProofError("Final response status is invalid", "terminal_status");
  }
  return { id: final.id, status: final.status };
}

export class TranscriptProof<RetainsBytes extends boolean = true> {
  readonly #requestId: string;
  readonly #requestDigest: string;
  readonly #maxBytes: number;
  readonly #chunks: Uint8Array[] | null;
  readonly #hasher: IncrementalSha256Hasher | null;
  #eventCount = 0;
  #byteCount = 0;
  #final: Record<string, unknown> | null = null;
  #finalEventIndex: number | null = null;
  #terminalStatus: TerminalStatus | null = null;

  constructor(
    requestId: string,
    requestDigest: string,
    options: {
      maxBytes?: number;
      retainBytes?: RetainsBytes;
      hasher?: IncrementalSha256Hasher;
    } = {},
  ) {
    if (!requestId) throw new TypeError("requestId must not be empty");
    if (!/^[a-f0-9]{64}$/.test(requestDigest)) {
      throw new TypeError("requestDigest must be a lowercase SHA-256 digest");
    }
    const maxBytes = options.maxBytes ?? 64 * 1024 * 1024;
    if (!Number.isSafeInteger(maxBytes) || maxBytes < 1) {
      throw new RangeError("maxBytes must be a positive safe integer");
    }
    this.#requestId = requestId;
    this.#requestDigest = requestDigest;
    this.#maxBytes = maxBytes;
    this.#chunks = options.retainBytes === false ? null : [];
    if (this.#chunks !== null && options.hasher) {
      throw new TypeError("A transcript hasher requires retainBytes=false");
    }
    this.#hasher = this.#chunks === null
      ? options.hasher ?? new IncrementalSha256()
      : null;
  }

  get eventCount(): number {
    return this.#eventCount;
  }

  get byteCount(): number {
    return this.#byteCount;
  }

  get retainedByteCount(): number {
    return this.#chunks ? this.#byteCount : 0;
  }

  append(frame: SseFrame): number {
    const prepared = this.#prepareAppend(frame);
    const scratch = new Uint8Array(MAX_TRANSCRIPT_CHUNK_BYTES);
    const encoder = new TextEncoder();
    let scratchLength = 0;
    const flush = (): void => {
      if (scratchLength === 0) return;
      const chunk = scratch.subarray(0, scratchLength);
      this.#hasher?.update(chunk);
      this.#chunks?.push(chunk.slice());
      scratchLength = 0;
    };
    for (const part of ["event: ", prepared.event, "\ndata: ", prepared.data, "\n\n"]) {
      let offset = 0;
      while (offset < part.length) {
        const available = scratch.byteLength - scratchLength;
        let end = Math.min(
          part.length,
          offset + Math.min(available, MAX_TRANSCRIPT_INPUT_SLICE_UNITS),
        );
        if (
          end < part.length &&
          end > offset &&
          part.charCodeAt(end - 1) >= 0xd800 &&
          part.charCodeAt(end - 1) <= 0xdbff &&
          part.charCodeAt(end) >= 0xdc00 &&
          part.charCodeAt(end) <= 0xdfff
        ) end--;
        const result = encoder.encodeInto(
          part.slice(offset, end),
          scratch.subarray(scratchLength),
        );
        if (result.read === 0 && result.written === 0) {
          flush();
          continue;
        }
        offset += result.read;
        scratchLength += result.written;
        if (scratchLength === scratch.byteLength) flush();
      }
    }
    flush();
    this.#finishAppend(prepared);
    return prepared.byteLength;
  }

  /** Append once and return the exact canonical SSE bytes used by the proof. */
  appendCanonical(frame: SseFrame): Uint8Array {
    if (
      typeof frame.event === "string" &&
      typeof frame.data === "string" &&
      16 + frame.event.length + frame.data.length > MAX_TRANSCRIPT_CHUNK_BYTES
    ) {
      throw new V2TerminalProofError(
        "Jumbo canonical events require appendCanonicalStream",
        "streaming_required",
      );
    }
    const prepared = this.#prepareAppend(frame);
    if (prepared.byteLength > MAX_TRANSCRIPT_CHUNK_BYTES) {
      throw new V2TerminalProofError(
        "Jumbo canonical events require appendCanonicalStream",
        "streaming_required",
      );
    }
    const encoded = utf8Bytes(`event: ${prepared.event}\ndata: ${prepared.data}\n\n`);
    this.#hasher?.update(encoded);
    this.#chunks?.push(encoded);
    this.#finishAppend(prepared);
    return encoded;
  }

  /**
   * Sidecar-safe append for a potentially multi-megabyte SSE event. The
   * transcript remains fixed-memory while pure-JS hashing yields between
   * bounded chunks instead of monopolizing the Deno event loop.
   */
  async appendCanonicalChunked(
    frame: SseFrame,
    options: ChunkedTranscriptAppendOptions = {},
  ): Promise<number> {
    return this.appendCanonicalStream(frame, options);
  }

  /**
   * Append/hash one canonical event using fixed scratch and an optional ordered
   * sink. Unlike `appendCanonicalChunked`, this production path never creates a
   * full-event canonical Uint8Array.
   */
  async appendCanonicalStream(
    frame: SseFrame,
    options: StreamingTranscriptAppendOptions = {},
  ): Promise<number> {
    const chunkBytes = options.chunkBytes ?? MAX_TRANSCRIPT_CHUNK_BYTES;
    if (
      !Number.isSafeInteger(chunkBytes) ||
      chunkBytes < 4 ||
      chunkBytes > MAX_TRANSCRIPT_CHUNK_BYTES
    ) {
      throw new RangeError("chunkBytes must be between 4 bytes and 64 KiB");
    }
    if (typeof frame.event !== "string" || !frame.event || typeof frame.data !== "string") {
      throw new V2TerminalProofError("SSE frame is invalid", "frame");
    }
    const yieldTask = options.yieldTask ?? yieldTranscriptTask;
    const [eventBytes, dataBytes] = await Promise.all([
      utf8ByteLengthChunked(frame.event, { yieldTask }),
      utf8ByteLengthChunked(frame.data, { yieldTask }),
    ]);
    const prepared = this.#prepareAppend(frame, 7 + eventBytes + 7 + dataBytes + 2);
    const scratch = new Uint8Array(chunkBytes);
    options.onScratchBytes?.(scratch.byteLength);
    const encoder = new TextEncoder();
    let scratchLength = 0;
    let written = 0;
    let emittedChunks = 0;
    let yieldBeforeMoreWork = false;
    const flush = async (): Promise<void> => {
      if (scratchLength === 0) return;
      const chunk = scratch.subarray(0, scratchLength);
      this.#hasher?.update(chunk);
      this.#chunks?.push(chunk.slice());
      await options.onChunk?.(chunk);
      written += chunk.byteLength;
      scratchLength = 0;
      emittedChunks++;
      yieldBeforeMoreWork = true;
    };
    const encodePart = async (part: string): Promise<void> => {
      let offset = 0;
      while (offset < part.length) {
        if (yieldBeforeMoreWork) {
          // A sink is allowed to resolve synchronously, so it is not itself a
          // fairness boundary. Do not pay a timer after the final chunk.
          yieldBeforeMoreWork = false;
          await yieldTask();
        }
        const available = scratch.byteLength - scratchLength;
        let end = Math.min(
          part.length,
          offset + Math.min(available, MAX_TRANSCRIPT_INPUT_SLICE_UNITS),
        );
        if (
          end < part.length &&
          end > offset &&
          part.charCodeAt(end - 1) >= 0xd800 &&
          part.charCodeAt(end - 1) <= 0xdbff &&
          part.charCodeAt(end) >= 0xdc00 &&
          part.charCodeAt(end) <= 0xdfff
        ) end--;
        options.onInputSliceUnits?.(end - offset);
        const result = encoder.encodeInto(
          part.slice(offset, end),
          scratch.subarray(scratchLength),
        );
        if (result.read === 0 && result.written === 0) {
          await flush();
          continue;
        }
        offset += result.read;
        scratchLength += result.written;
        if (scratchLength === scratch.byteLength) await flush();
      }
    };
    try {
      await encodePart("event: ");
      await encodePart(prepared.event);
      await encodePart("\ndata: ");
      await encodePart(prepared.data);
      await encodePart("\n\n");
      await flush();
    } catch (error) {
      // The transcript is deliberately unusable after a sink failure: hashing
      // may already have crossed an irreversible durable-spool boundary.
      throw error;
    }
    if (written !== prepared.byteLength || emittedChunks < 1) {
      throw new V2TerminalProofError("Canonical transcript encoding is inconsistent", "encoding");
    }
    this.#finishAppend(prepared);
    return written;
  }

  #prepareAppend(frame: SseFrame, knownByteLength?: number): PreparedTranscriptAppend {
    if (typeof frame.event !== "string" || !frame.event || typeof frame.data !== "string") {
      throw new V2TerminalProofError("SSE frame is invalid", "frame");
    }
    const byteLength = knownByteLength ??
      7 + utf8ByteLength(frame.event) + 7 + utf8ByteLength(frame.data) + 2;
    if (this.#byteCount + byteLength > this.#maxBytes) {
      throw new V2TerminalProofError("Transcript exceeds its byte limit", "byte_limit");
    }
    const terminalStatus = TERMINALS.get(frame.event) ?? null;
    let terminalFinal: Record<string, unknown> | null = null;
    if (terminalStatus) {
      if (this.#final) {
        throw new V2TerminalProofError("Transcript contains a duplicate terminal event", "duplicate_terminal");
      }
      let decoded: unknown;
      try {
        decoded = JSON.parse(frame.data);
      } catch {
        throw new V2TerminalProofError("Terminal event data is invalid JSON", "terminal_json");
      }
      const event = object(decoded, "Terminal event");
      if (event.type !== frame.event) {
        throw new V2TerminalProofError("Terminal event type does not match its SSE name", "terminal_type");
      }
      terminalFinal = object(event.response, "Final response");
      const identity = responseIdentity(terminalFinal);
      if (identity.status !== terminalStatus) {
        throw new V2TerminalProofError("Final response status does not match terminal event", "terminal_status");
      }
    }

    return {
      event: frame.event,
      data: frame.data,
      byteLength,
      terminalFinal,
      terminalStatus,
    };
  }

  #finishAppend(prepared: PreparedTranscriptAppend): void {
    const { byteLength, terminalFinal, terminalStatus } = prepared;
    if (terminalFinal && terminalStatus) {
      this.#final = terminalFinal;
      this.#finalEventIndex = this.#eventCount;
      this.#terminalStatus = terminalStatus;
    }
    this.#eventCount++;
    this.#byteCount += byteLength;
  }

  async createTerminal(
    expectedFinal?: Record<string, unknown>,
  ): Promise<ResponseTerminalProof> {
    if (
      !this.#final ||
      this.#finalEventIndex === null ||
      this.#terminalStatus === null
    ) {
      throw new V2TerminalProofError("Transcript does not contain a terminal event", "missing_terminal");
    }
    // Capture one point-in-time proof before either hashing await below.
    const final = this.#final;
    const identity = responseIdentity(final);
    const requestId = this.#requestId;
    const requestDigest = this.#requestDigest;
    const eventCount = this.#eventCount;
    const byteCount = this.#byteCount;
    const terminalStatus = this.#terminalStatus;
    const finalEventIndex = this.#finalEventIndex;
    const incrementalDigest = this.#hasher?.digestHex() ?? null;
    const retainedBytes = this.#chunks ? concat(this.#chunks, byteCount) : null;
    const transcriptSha256 = incrementalDigest ?? await sha256Hex(retainedBytes!);
    const finalSha256 = await canonicalJsonSha256(final);
    if (expectedFinal && await canonicalJsonSha256(expectedFinal) !== finalSha256) {
      throw new V2TerminalProofError("Expected final object does not match terminal event", "final_object");
    }
    return {
      kind: "response_terminal",
      requestId,
      requestDigest,
      responseId: identity.id,
      eventCount,
      byteCount,
      transcriptSha256,
      terminalStatus,
      finalEventIndex,
      finalSha256,
    };
  }

  snapshot(): TranscriptSnapshot<RetainsBytes> {
    const common: TranscriptSnapshotBase = {
      requestId: this.#requestId,
      requestDigest: this.#requestDigest,
      eventCount: this.#eventCount,
      byteCount: this.#byteCount,
      final: this.#final,
      finalEventIndex: this.#finalEventIndex,
      terminalStatus: this.#terminalStatus,
    };
    return (this.#chunks
      ? {
          ...common,
          retainsBytes: true,
          canonicalBytes: concat(this.#chunks, this.#byteCount),
          transcriptSha256: null,
        }
      : {
          ...common,
          retainsBytes: false,
          canonicalBytes: null,
          transcriptSha256: this.#hasher!.digestHex(),
        }) as unknown as TranscriptSnapshot<RetainsBytes>;
  }
}

export async function validateTerminalProof(
  terminal: ResponseTerminalProof,
  snapshot: TranscriptSnapshot<boolean>,
): Promise<Record<string, unknown>> {
  if (terminal.requestId !== snapshot.requestId) {
    throw new V2TerminalProofError("Terminal request ID does not match", "request_id");
  }
  if (terminal.requestDigest !== snapshot.requestDigest) {
    throw new V2TerminalProofError("Terminal request digest does not match", "request_digest");
  }
  if (terminal.eventCount !== snapshot.eventCount) {
    throw new V2TerminalProofError("Terminal event count does not match", "event_count");
  }
  if (terminal.byteCount !== snapshot.byteCount) {
    throw new V2TerminalProofError("Terminal byte count does not match", "byte_count");
  }
  if (!snapshot.final || snapshot.finalEventIndex === null || !snapshot.terminalStatus) {
    throw new V2TerminalProofError("Local transcript has no terminal event", "missing_terminal");
  }
  const identity = responseIdentity(snapshot.final);
  if (terminal.responseId !== identity.id) {
    throw new V2TerminalProofError("Terminal response ID does not match", "response_id");
  }
  if (
    terminal.terminalStatus !== identity.status ||
    terminal.terminalStatus !== snapshot.terminalStatus
  ) {
    throw new V2TerminalProofError("Terminal response status does not match", "terminal_status");
  }
  if (terminal.finalEventIndex !== snapshot.finalEventIndex) {
    throw new V2TerminalProofError("Terminal final event index does not match", "final_event_index");
  }
  const finalDigest = await canonicalJsonSha256(snapshot.final);
  if (terminal.finalSha256 !== finalDigest) {
    throw new V2TerminalProofError("Terminal final digest does not match", "final_digest");
  }
  let transcriptDigest: string;
  if (snapshot.retainsBytes) {
    if (
      !(snapshot.canonicalBytes instanceof Uint8Array) ||
      snapshot.canonicalBytes.byteLength !== snapshot.byteCount ||
      snapshot.transcriptSha256 !== null
    ) {
      throw new V2TerminalProofError("Retained transcript snapshot is inconsistent", "transcript_snapshot");
    }
    transcriptDigest = await sha256Hex(snapshot.canonicalBytes);
  } else {
    if (
      snapshot.canonicalBytes !== null ||
      !/^[a-f0-9]{64}$/.test(snapshot.transcriptSha256)
    ) {
      throw new V2TerminalProofError("Incremental transcript snapshot is inconsistent", "transcript_snapshot");
    }
    transcriptDigest = snapshot.transcriptSha256;
  }
  if (terminal.transcriptSha256 !== transcriptDigest) {
    throw new V2TerminalProofError("Terminal transcript digest does not match", "transcript_digest");
  }
  return snapshot.final;
}
