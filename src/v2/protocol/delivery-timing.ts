import { computeV2RetransmitDelayMs } from "./retransmit";
import {
  GP_V2_WINDOW_MAX_FRAMES,
  type AckCursor,
  type FrameIdentity,
} from "./shared";

function assertEpoch(value: number): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new RangeError("delivery epoch must be a non-negative safe integer");
  }
}

function assertSequence(value: number, allowEmpty = false): void {
  const minimum = allowEmpty ? -1 : 0;
  if (!Number.isSafeInteger(value) || value < minimum) {
    throw new RangeError(`delivery sequence must be a safe integer at least ${minimum}`);
  }
}

function assertTime(value: number): void {
  if (!Number.isFinite(value) || value < 0) {
    throw new RangeError("delivery time must be finite and non-negative");
  }
}

/**
 * Owns bounded first-send timestamps and the retransmission deadline for one
 * reliable epoch. Ordinary cumulative republishes never move the deadline.
 */
export class ReliableDeliveryTiming {
  #epoch: number;
  readonly #minimumMs: number;
  readonly #maxTracked: number;
  readonly #firstSentAt = new Map<number, number>();
  #lastAcknowledged = -1;
  #roundTripEwmaMs: number | null = null;
  #attempt = 0;
  #deadlineBaseMs: number | null = null;

  constructor(options: {
    epoch: number;
    minimumMs?: number;
    maxTracked?: number;
  }) {
    assertEpoch(options.epoch);
    const minimumMs = options.minimumMs ?? 250;
    if (!Number.isFinite(minimumMs) || minimumMs < 1) {
      throw new RangeError("minimumMs must be positive");
    }
    const maxTracked = options.maxTracked ?? GP_V2_WINDOW_MAX_FRAMES;
    if (!Number.isSafeInteger(maxTracked) || maxTracked < 1) {
      throw new RangeError("maxTracked must be a positive safe integer");
    }
    this.#epoch = options.epoch;
    this.#minimumMs = minimumMs;
    this.#maxTracked = maxTracked;
  }

  get roundTripEwmaMs(): number | null {
    return this.#roundTripEwmaMs;
  }

  get attempt(): number {
    return this.#attempt;
  }

  get trackedSequences(): number {
    return this.#firstSentAt.size;
  }

  published(frames: readonly FrameIdentity[], nowMs: number): void {
    assertTime(nowMs);
    const unseen = new Set<number>();
    for (const frame of frames) {
      assertEpoch(frame.epoch);
      assertSequence(frame.sequence);
      if (frame.epoch !== this.#epoch) {
        throw new RangeError("published frame belongs to another delivery epoch");
      }
      if (
        frame.sequence > this.#lastAcknowledged &&
        !this.#firstSentAt.has(frame.sequence)
      ) unseen.add(frame.sequence);
    }
    if (this.#firstSentAt.size + unseen.size > this.#maxTracked) {
      throw new RangeError("reliable RTT tracker exceeds its sequence bound");
    }
    for (const sequence of unseen) this.#firstSentAt.set(sequence, nowMs);
    if (this.#firstSentAt.size > 0 && this.#deadlineBaseMs === null) {
      this.#deadlineBaseMs = nowMs;
    }
  }

  acknowledged(
    cursor: AckCursor,
    hasOutstanding: boolean,
    nowMs: number,
  ): boolean {
    assertEpoch(cursor.epoch);
    assertSequence(cursor.sequence, true);
    assertTime(nowMs);
    if (cursor.epoch < this.#epoch || cursor.sequence <= this.#lastAcknowledged) {
      return false;
    }
    if (cursor.epoch > this.#epoch) {
      throw new RangeError("ACK belongs to a future delivery epoch");
    }

    let sampledSequence = -1;
    let sampledAt: number | null = null;
    for (const [sequence, firstSentAt] of this.#firstSentAt) {
      if (sequence > cursor.sequence) continue;
      this.#firstSentAt.delete(sequence);
      if (sequence > sampledSequence) {
        sampledSequence = sequence;
        sampledAt = firstSentAt;
      }
    }
    this.#lastAcknowledged = cursor.sequence;
    if (sampledAt !== null) {
      const sampleMs = Math.max(0, nowMs - sampledAt);
      this.#roundTripEwmaMs = this.#roundTripEwmaMs === null
        ? sampleMs
        : this.#roundTripEwmaMs * 0.8 + sampleMs * 0.2;
    }
    this.#attempt = 0;
    this.#deadlineBaseMs = hasOutstanding ? nowMs : null;
    return true;
  }

  retransmitted(nowMs: number): void {
    assertTime(nowMs);
    if (this.#deadlineBaseMs === null) {
      throw new Error("cannot retransmit an empty delivery window");
    }
    this.#attempt++;
    this.#deadlineBaseMs = nowMs;
  }

  nextDelayMs(nowMs: number, jitterSeed: number): number | null {
    assertTime(nowMs);
    if (this.#deadlineBaseMs === null) return null;
    const delayMs = computeV2RetransmitDelayMs(
      this.#roundTripEwmaMs,
      jitterSeed,
      this.#minimumMs,
      this.#attempt,
    );
    return Math.max(0, this.#deadlineBaseMs + delayMs - nowMs);
  }

  reset(epoch: number): void {
    assertEpoch(epoch);
    if (epoch <= this.#epoch) {
      throw new RangeError("delivery epoch must advance");
    }
    this.#epoch = epoch;
    this.#firstSentAt.clear();
    this.#lastAcknowledged = -1;
    this.#roundTripEwmaMs = null;
    this.#attempt = 0;
    this.#deadlineBaseMs = null;
  }
}
