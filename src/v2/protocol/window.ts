import {
  GP_V2_FRAME_MAX_BYTES,
  GP_V2_WINDOW_MAX_BYTES,
  GP_V2_WINDOW_MAX_FRAMES,
  canonicalJson,
  utf8Bytes,
  type AckCursor,
  type ReliableFrame,
} from "./shared";

export class V2WindowError extends Error {
  readonly name = "V2WindowError";

  constructor(message: string, readonly code: string) {
    super(message);
  }
}

interface OutboundStoredFrame<T> {
  frame: ReliableFrame<T>;
  encodedBytes: number;
}

interface InboundStoredFrame<T> extends OutboundStoredFrame<T> {
  canonical: string;
}

function validEpoch(value: number): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new V2WindowError("epoch must be a non-negative safe integer", "epoch");
  }
}

function encodedFrameBytes(value: unknown): number {
  let json: string;
  try {
    json = JSON.stringify(value);
  } catch {
    throw new V2WindowError("frame must be JSON serializable", "frame_size");
  }
  const bytes = utf8Bytes(json).byteLength;
  if (bytes > GP_V2_FRAME_MAX_BYTES) {
    throw new V2WindowError("frame exceeds the hard 16 KiB limit", "frame_size");
  }
  return bytes;
}

export class OutboundFrameWindow<T extends object> {
  #epoch: number;
  #nextSequence = 0;
  #acknowledgedSequence = -1;
  #encodedBytes = 0;
  #frames: OutboundStoredFrame<T>[] = [];
  #snapshot: {
    epoch: number;
    nextSequence: number;
    encodedBytes: number;
    frames: readonly ReliableFrame<T>[];
  } | null = null;

  constructor(options: { epoch: number }) {
    validEpoch(options.epoch);
    this.#epoch = options.epoch;
  }

  push(frame: T): ReliableFrame<T> {
    if (this.#frames.length >= GP_V2_WINDOW_MAX_FRAMES) {
      throw new V2WindowError(
        `Outbound state exceeds the ${GP_V2_WINDOW_MAX_FRAMES}-frame window`,
        "window_frames",
      );
    }
    const reliable = Object.freeze({
      ...frame,
      epoch: this.#epoch,
      sequence: this.#nextSequence,
    }) as ReliableFrame<T>;
    const encodedBytes = encodedFrameBytes(reliable);
    if (this.#encodedBytes + encodedBytes > GP_V2_WINDOW_MAX_BYTES) {
      throw new V2WindowError(
        `Outbound state exceeds the ${GP_V2_WINDOW_MAX_BYTES}-byte window`,
        "window_bytes",
      );
    }
    this.#frames.push({
      frame: reliable,
      encodedBytes,
    });
    this.#nextSequence++;
    this.#encodedBytes += encodedBytes;
    this.#snapshot = null;
    return reliable;
  }

  acknowledge(cursor: AckCursor): void {
    validEpoch(cursor.epoch);
    if (!Number.isSafeInteger(cursor.sequence) || cursor.sequence < -1) {
      throw new V2WindowError("ACK sequence must be a safe integer at least -1", "ack");
    }
    if (cursor.epoch < this.#epoch) return;
    if (cursor.epoch > this.#epoch) {
      throw new V2WindowError("ACK belongs to a future epoch", "ack_epoch");
    }
    const lastPublished = this.#nextSequence - 1;
    if (cursor.sequence > lastPublished) {
      throw new V2WindowError("ACK sequence is beyond published frames", "ack_future");
    }
    if (cursor.sequence <= this.#acknowledgedSequence) return;
    this.#acknowledgedSequence = cursor.sequence;
    const retained: OutboundStoredFrame<T>[] = [];
    let retainedBytes = 0;
    for (const stored of this.#frames) {
      if (stored.frame.sequence <= cursor.sequence) continue;
      retained.push(stored);
      retainedBytes += stored.encodedBytes;
    }
    this.#frames = retained;
    this.#encodedBytes = retainedBytes;
    this.#snapshot = null;
  }

  snapshot(): {
    epoch: number;
    nextSequence: number;
    encodedBytes: number;
    frames: readonly ReliableFrame<T>[];
  } {
    if (this.#snapshot) return this.#snapshot;
    this.#snapshot = Object.freeze({
      epoch: this.#epoch,
      nextSequence: this.#nextSequence,
      encodedBytes: this.#encodedBytes,
      frames: Object.freeze(this.#frames.map(stored => stored.frame)),
    });
    return this.#snapshot;
  }

  reset(epoch: number): void {
    validEpoch(epoch);
    if (epoch <= this.#epoch) {
      throw new V2WindowError("reset epoch must advance", "epoch");
    }
    this.#epoch = epoch;
    this.#nextSequence = 0;
    this.#acknowledgedSequence = -1;
    this.#encodedBytes = 0;
    this.#frames = [];
    this.#snapshot = null;
  }
}

export class InboundFrameCursor<T extends object> {
  #epoch: number;
  #nextSequence = 0;
  #bufferBytes = 0;
  readonly #buffer = new Map<number, InboundStoredFrame<T>>();

  constructor(options: { epoch: number }) {
    validEpoch(options.epoch);
    this.#epoch = options.epoch;
  }

  accept(frames: readonly ReliableFrame<T>[]): ReliableFrame<T>[] {
    for (const frame of frames) {
      validEpoch(frame.epoch);
      if (!Number.isSafeInteger(frame.sequence) || frame.sequence < 0) {
        throw new V2WindowError("frame sequence must be non-negative", "sequence");
      }
      if (frame.epoch < this.#epoch) continue;
      if (frame.epoch > this.#epoch) {
        throw new V2WindowError("frame belongs to a future epoch", "epoch");
      }
      if (frame.sequence < this.#nextSequence) continue;
      const canonical = canonicalJson(frame);
      const existing = this.#buffer.get(frame.sequence);
      if (existing) {
        if (existing.canonical !== canonical) {
          throw new V2WindowError("frame sequence has a conflicting duplicate", "duplicate");
        }
        continue;
      }
      if (
        frame.sequence - this.#nextSequence > GP_V2_WINDOW_MAX_FRAMES ||
        this.#buffer.size >= GP_V2_WINDOW_MAX_FRAMES
      ) {
        throw new V2WindowError(
          `Inbound gap exceeds the ${GP_V2_WINDOW_MAX_FRAMES}-frame window`,
          "window_frames",
        );
      }
      const encodedBytes = encodedFrameBytes(frame);
      if (this.#bufferBytes + encodedBytes > GP_V2_WINDOW_MAX_BYTES) {
        throw new V2WindowError(
          `Inbound state exceeds the ${GP_V2_WINDOW_MAX_BYTES}-byte window`,
          "window_bytes",
        );
      }
      this.#buffer.set(frame.sequence, { frame, encodedBytes, canonical });
      this.#bufferBytes += encodedBytes;
    }

    const delivered: ReliableFrame<T>[] = [];
    for (;;) {
      const stored = this.#buffer.get(this.#nextSequence);
      if (!stored) break;
      this.#buffer.delete(this.#nextSequence);
      this.#bufferBytes -= stored.encodedBytes;
      delivered.push(stored.frame);
      this.#nextSequence++;
    }
    return delivered;
  }

  ack(): AckCursor {
    return { epoch: this.#epoch, sequence: this.#nextSequence - 1 };
  }

  reset(epoch: number): void {
    validEpoch(epoch);
    if (epoch <= this.#epoch) {
      throw new V2WindowError("reset epoch must advance", "epoch");
    }
    this.#epoch = epoch;
    this.#nextSequence = 0;
    this.#bufferBytes = 0;
    this.#buffer.clear();
  }
}
