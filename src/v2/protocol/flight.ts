import type { AckCursor, FrameIdentity } from "./shared";

function assertEpoch(value: number): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new RangeError("flight epoch must be a non-negative safe integer");
  }
}

function assertSequence(value: number, allowEmpty = false): void {
  if (!Number.isSafeInteger(value) || value < (allowEmpty ? -1 : 0)) {
    throw new RangeError("flight sequence is invalid");
  }
}

/** Tracks exactly one immutable reliable flight and its cumulative ACK. */
export class AckClockedFlight {
  #epoch: number;
  #highSequence: number | null = null;
  readonly #sequences = new Set<number>();

  constructor(options: { epoch: number }) {
    assertEpoch(options.epoch);
    this.#epoch = options.epoch;
  }

  get active(): boolean {
    return this.#highSequence !== null;
  }

  get highSequence(): number | null {
    return this.#highSequence;
  }

  get frameCount(): number {
    return this.#sequences.size;
  }

  published(frames: readonly FrameIdentity[]): void {
    if (frames.length === 0) {
      if (this.active) throw new Error("cannot publish an empty active flight");
      return;
    }
    let previous = -1;
    for (const frame of frames) {
      assertEpoch(frame.epoch);
      assertSequence(frame.sequence);
      if (frame.epoch !== this.#epoch) {
        throw new RangeError("published frame belongs to another flight epoch");
      }
      if (frame.sequence <= previous) {
        throw new RangeError("flight sequences must be strictly increasing");
      }
      previous = frame.sequence;
    }

    if (!this.active) {
      this.#highSequence = frames.at(-1)!.sequence;
      for (const frame of frames) this.#sequences.add(frame.sequence);
      return;
    }

    if (
      frames.at(-1)!.sequence !== this.#highSequence ||
      frames.length !== this.#sequences.size ||
      frames.some(frame => !this.#sequences.has(frame.sequence))
    ) {
      throw new Error("active flight is immutable until its high ACK");
    }
  }

  acknowledged(cursor: AckCursor): boolean {
    assertEpoch(cursor.epoch);
    assertSequence(cursor.sequence, true);
    if (cursor.epoch < this.#epoch || !this.active) return false;
    if (cursor.epoch > this.#epoch) {
      throw new RangeError("ACK belongs to a future flight epoch");
    }
    for (const sequence of this.#sequences) {
      if (sequence <= cursor.sequence) this.#sequences.delete(sequence);
    }
    if (cursor.sequence < this.#highSequence!) return false;
    this.#highSequence = null;
    this.#sequences.clear();
    return true;
  }

  reset(epoch: number): void {
    assertEpoch(epoch);
    if (epoch <= this.#epoch) throw new RangeError("flight epoch must advance");
    this.#epoch = epoch;
    this.#highSequence = null;
    this.#sequences.clear();
  }
}
