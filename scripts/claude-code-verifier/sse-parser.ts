import { parseFrames, type SseFrame } from "../../src/relay/sse";
import { ForwardingProtocolError } from "./shared";

export type VerifierFrameAcceptor = (
  frame: SseFrame,
) => Promise<Record<string, unknown> | null>;

export class BoundedSseParser {
  readonly #maxEventBytes: number;
  readonly #maxReadChunkBytes: number;
  readonly #maxSegments: number;
  #parts: Uint8Array[] = [];
  #eventBytes = 0;
  #lineHasContent = false;
  #pendingCr = false;
  #pendingDispatch = false;
  #swallowLeadingLf = false;

  constructor(maxEventBytes: number, maxReadChunkBytes: number, maxSegments: number) {
    this.#maxEventBytes = maxEventBytes;
    this.#maxReadChunkBytes = maxReadChunkBytes;
    this.#maxSegments = maxSegments;
  }

  async push(
    chunk: Uint8Array,
    accept: VerifierFrameAcceptor,
  ): Promise<Record<string, unknown> | null> {
    if (chunk.byteLength > this.#maxReadChunkBytes) {
      throw new ForwardingProtocolError("Upstream SSE read chunk is too large");
    }
    let segmentStart = 0;
    if (this.#swallowLeadingLf && chunk.byteLength > 0) {
      this.#swallowLeadingLf = false;
      if (chunk[0] === 0x0a) segmentStart = 1;
    }
    for (let index = segmentStart; index < chunk.byteLength; index += 1) {
      const byte = chunk[index]!;
      if (this.#pendingCr) {
        this.#pendingCr = false;
        if (byte === 0x0a) {
          if (this.#pendingDispatch) {
            this.#append(chunk.slice(segmentStart, index + 1));
            segmentStart = index + 1;
            const final = await this.#dispatch(accept);
            if (final) return final;
          }
          continue;
        }
        if (this.#pendingDispatch) {
          this.#append(chunk.slice(segmentStart, index));
          segmentStart = index;
          const final = await this.#dispatch(accept);
          if (final) return final;
        }
      }
      if (byte === 0x0d) {
        this.#pendingDispatch = !this.#lineHasContent;
        this.#lineHasContent = false;
        this.#pendingCr = true;
        continue;
      }
      if (byte === 0x0a) {
        const dispatch = !this.#lineHasContent;
        this.#lineHasContent = false;
        if (dispatch) {
          this.#append(chunk.slice(segmentStart, index + 1));
          segmentStart = index + 1;
          const final = await this.#dispatch(accept);
          if (final) return final;
        }
        continue;
      }
      this.#lineHasContent = true;
    }
    this.#append(chunk.slice(segmentStart));
    if (this.#pendingCr && this.#pendingDispatch) {
      this.#pendingCr = false;
      const final = await this.#dispatch(accept);
      this.#swallowLeadingLf = true;
      return final;
    }
    return null;
  }

  async finish(accept: VerifierFrameAcceptor): Promise<Record<string, unknown> | null> {
    if (this.#pendingCr && this.#pendingDispatch) {
      this.#pendingCr = false;
      return this.#dispatch(accept);
    }
    if (this.#eventBytes === 0) return null;
    throw new ForwardingProtocolError("Upstream event stream ended mid-frame");
  }

  #append(bytes: Uint8Array): void {
    if (bytes.byteLength === 0) return;
    const nextBytes = this.#eventBytes + bytes.byteLength;
    if (nextBytes > this.#maxEventBytes) {
      throw new ForwardingProtocolError("Upstream SSE event is too large");
    }
    if (this.#parts.length >= this.#maxSegments) {
      throw new ForwardingProtocolError("Upstream SSE event has too many segments");
    }
    this.#parts.push(bytes);
    this.#eventBytes = nextBytes;
  }

  #takeFrame(): SseFrame | null {
    let raw: string;
    try {
      raw = new TextDecoder("utf-8", { fatal: true }).decode(this.#takeBytes());
    } catch {
      throw new ForwardingProtocolError("Upstream SSE event is not valid UTF-8");
    }
    const parsed = parseFrames(raw, 0);
    if (parsed.nextOffset !== raw.length || parsed.frames.length > 1) {
      throw new ForwardingProtocolError("Upstream SSE event is malformed");
    }
    return parsed.frames[0] ?? null;
  }

  #takeBytes(): Uint8Array {
    const bytes = new Uint8Array(this.#eventBytes);
    let offset = 0;
    for (const part of this.#parts) {
      bytes.set(part, offset);
      offset += part.byteLength;
    }
    this.#parts = [];
    this.#eventBytes = 0;
    return bytes;
  }

  async #dispatch(accept: VerifierFrameAcceptor): Promise<Record<string, unknown> | null> {
    const frame = this.#takeFrame();
    this.#lineHasContent = false;
    this.#pendingCr = false;
    this.#pendingDispatch = false;
    return frame ? accept(frame) : null;
  }
}
