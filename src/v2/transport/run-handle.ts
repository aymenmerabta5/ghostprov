import { SYSTEM_CLOCK, type Clock } from "../../core/ports";
import type { RunHandle } from "../../relay/run-handle";
import { sseFrame, type SseFrame } from "../../relay/sse";
import { utf8Bytes, type UpstreamPrelude } from "../protocol/shared";
import type { RateLimitHint } from "../rate-limit";
import type { ProviderErrorEnvelope } from "../provider-error";
import type { AtomicBytePermit } from "../memory/atomic-byte-permits";
import {
  V2_EVENT_MAX_BUFFERED_EVENTS,
  V2_EVENT_SOFT_BUFFER_BYTES,
  v2RetainedEventPermitBytes,
} from "../memory/event-memory-sizing";
import {
  attachAtomicBytePermit,
  releaseAtomicBytePermit,
  shareAtomicBytePermit,
  transferAtomicBytePermit,
} from "../memory/permit-carrier";

const RESPONSE_TERMINAL_EVENTS = new Set([
  "response.completed",
  "response.failed",
  "response.incomplete",
]);

export class V2PreCommitError extends Error {
  readonly name = "V2PreCommitError";
  readonly committed = false;

  constructor(message: string, readonly code: string) {
    super(message);
  }
}

export class V2CommittedError extends Error {
  readonly name = "V2CommittedError";
  readonly committed = true;

  constructor(
    message: string,
    readonly code: string,
    readonly ambiguous: boolean,
    readonly rateLimit?: RateLimitHint,
    readonly providerError?: ProviderErrorEnvelope,
  ) {
    super(message);
  }
}

interface BufferedEvent {
  frame: SseFrame;
  bytes: number;
}

export interface V2RunHandleOptions {
  maxBufferedBytes?: number;
  /** Hard metadata/count fence in addition to the byte bound. */
  maxBufferedEvents?: number;
  clock?: Clock;
  expectUpstreamPrelude?: boolean;
  /** Narrow opt-in used once V2TransportSession transfers every event permit. */
  requireTransferredEventMemory?: boolean;
}

export class V2EventMemoryError extends Error {
  readonly name = "V2EventMemoryError";

  constructor(
    message: string,
    readonly code: "coverage" | "jumbo_required",
  ) {
    super(message);
  }
}

export class V2RunHandle implements RunHandle {
  readonly final: Promise<Record<string, unknown>>;
  readonly upstreamPrelude?: Promise<UpstreamPrelude>;
  rateLimit?: RateLimitHint;
  readonly stats: RunHandle["stats"] = {
    bytes: 0,
    firstByteMs: null as number | null,
    frames: 0,
  };
  readonly #clock: Clock;
  readonly #startedAtMs: number;
  readonly #maxBufferedBytes: number;
  readonly #maxBufferedEvents: number;
  readonly #requireTransferredEventMemory: boolean;
  // Keep a head cursor rather than shifting the array for every event. Array
  // shift is O(n), which made draining a long response quadratic and retained
  // all later entries while V8 moved them down one slot at a time.
  readonly #buffer: Array<BufferedEvent | undefined> = [];
  #bufferHead = 0;
  readonly #eventWaiters = new Set<() => void>();
  readonly #spaceWaiters = new Set<() => void>();
  readonly #resolveFinal: (value: Record<string, unknown>) => void;
  readonly #rejectFinal: (error: unknown) => void;
  #resolveUpstreamPrelude?: (value: UpstreamPrelude) => void;
  #rejectUpstreamPrelude?: (error: unknown) => void;
  #upstreamPreludeSettled = false;
  #bufferedBytes = 0;
  #bufferedEvents = 0;
  #closed = false;
  #failure: Error | null = null;
  #eventsClaimed = false;
  #eventStreamClosed = false;
  #discardEvents = false;
  /** Shared terminal allocation retained independently from event delivery. */
  #finalPermitAnchor: object | null = null;

  constructor(options: V2RunHandleOptions) {
    const maxBufferedBytes = options.maxBufferedBytes ?? V2_EVENT_SOFT_BUFFER_BYTES;
    if (
      !Number.isSafeInteger(maxBufferedBytes) ||
      maxBufferedBytes < 1 ||
      maxBufferedBytes > V2_EVENT_SOFT_BUFFER_BYTES
    ) {
      throw new RangeError(
        `maxBufferedBytes must be a positive safe integer no greater than ${V2_EVENT_SOFT_BUFFER_BYTES}`,
      );
    }
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#startedAtMs = this.#clock.now();
    this.#maxBufferedBytes = maxBufferedBytes;
    const maxBufferedEvents = options.maxBufferedEvents ?? V2_EVENT_MAX_BUFFERED_EVENTS;
    if (
      !Number.isSafeInteger(maxBufferedEvents) ||
      maxBufferedEvents < 1 ||
      maxBufferedEvents > V2_EVENT_MAX_BUFFERED_EVENTS
    ) {
      throw new RangeError(
        `maxBufferedEvents must be a positive safe integer no greater than ${V2_EVENT_MAX_BUFFERED_EVENTS}`,
      );
    }
    this.#maxBufferedEvents = maxBufferedEvents;
    this.#requireTransferredEventMemory = options.requireTransferredEventMemory ?? false;
    let resolveFinal!: (value: Record<string, unknown>) => void;
    let rejectFinal!: (error: unknown) => void;
    this.final = new Promise<Record<string, unknown>>((resolve, reject) => {
      resolveFinal = resolve;
      rejectFinal = reject;
    });
    this.#resolveFinal = resolveFinal;
    this.#rejectFinal = rejectFinal;
    void this.final.catch(() => undefined);
    if (options.expectUpstreamPrelude) {
      this.upstreamPrelude = new Promise<UpstreamPrelude>((resolve, reject) => {
        this.#resolveUpstreamPrelude = resolve;
        this.#rejectUpstreamPrelude = reject;
      });
      void this.upstreamPrelude.catch(() => undefined);
    }
  }

  acceptUpstreamPrelude(prelude: UpstreamPrelude): void {
    if (!this.upstreamPrelude || this.#upstreamPreludeSettled || this.#closed) return;
    if (
      !Number.isSafeInteger(prelude.status) || prelude.status < 200 || prelude.status > 299 ||
      typeof prelude.contentType !== "string" || prelude.contentType.length < 1 ||
      prelude.contentType.length > 256 ||
      !(prelude.upstreamRequestId === null || typeof prelude.upstreamRequestId === "string")
    ) throw new TypeError("Upstream prelude is invalid");
    this.#upstreamPreludeSettled = true;
    this.#resolveUpstreamPrelude?.({ ...prelude });
  }

  async acceptEvent(
    frame: SseFrame,
    knownWireBytes?: number,
    transferredMemoryPermit?: AtomicBytePermit,
  ): Promise<void> {
    let createdFinalAnchor = false;
    try {
      if (!frame || typeof frame.event !== "string" || !frame.event || typeof frame.data !== "string") {
        throw new TypeError("SSE frame is invalid");
      }
      if (
        knownWireBytes !== undefined &&
        (!Number.isSafeInteger(knownWireBytes) || knownWireBytes < 1)
      ) {
        throw new RangeError("knownWireBytes must be a positive safe integer");
      }
      const bytes = knownWireBytes ??
        utf8Bytes(sseFrame(frame.event, frame.data)).byteLength;
      this.#assertPermitCoverage(bytes, transferredMemoryPermit);
      if (this.#discardEvents) {
        this.#recordAccepted(bytes);
        if (
          transferredMemoryPermit &&
          RESPONSE_TERMINAL_EVENTS.has(frame.event) &&
          !this.#finalPermitAnchor
        ) {
          const anchor = {};
          attachAtomicBytePermit(anchor, transferredMemoryPermit);
          this.#finalPermitAnchor = anchor;
          transferredMemoryPermit = undefined;
        }
        transferredMemoryPermit?.release();
        return;
      }
      while (
        !this.#closed &&
        !this.#eventStreamClosed &&
        (
          this.#bufferedEvents >= this.#maxBufferedEvents ||
          (this.#bufferedBytes > 0 && this.#bufferedBytes + bytes > this.#maxBufferedBytes)
        )
      ) {
        await new Promise<void>(resolve => this.#spaceWaiters.add(resolve));
      }
      if (this.#closed || this.#eventStreamClosed) {
        throw this.#failure ?? new Error("V2 run handle event stream is closed");
      }
      if (transferredMemoryPermit) {
        attachAtomicBytePermit(frame, transferredMemoryPermit);
      }
      if (
        transferredMemoryPermit &&
        RESPONSE_TERMINAL_EVENTS.has(frame.event) &&
        !this.#finalPermitAnchor
      ) {
        const anchor = {};
        shareAtomicBytePermit(frame, anchor, "host-run-handle-final");
        this.#finalPermitAnchor = anchor;
        createdFinalAnchor = true;
      }
      this.#buffer.push({
        frame,
        bytes,
      });
      this.#bufferedBytes += bytes;
      this.#bufferedEvents++;
      this.#recordAccepted(bytes);
      this.#wake(this.#eventWaiters);
    } catch (error) {
      releaseAtomicBytePermit(frame);
      if (createdFinalAnchor) this.#releaseFinalPermitAnchor();
      transferredMemoryPermit?.release();
      throw error;
    }
  }

  /**
   * Immediate delivery used by the cumulative transport receive chain. False
   * leaves the transferred permit owned by the caller and never queues/waits.
   */
  tryAcceptEvent(
    frame: SseFrame,
    knownWireBytes?: number,
    transferredMemoryPermit?: AtomicBytePermit,
  ): boolean {
    let createdFinalAnchor = false;
    try {
      if (!frame || typeof frame.event !== "string" || !frame.event || typeof frame.data !== "string") {
        throw new TypeError("SSE frame is invalid");
      }
      if (
        knownWireBytes !== undefined &&
        (!Number.isSafeInteger(knownWireBytes) || knownWireBytes < 1)
      ) throw new RangeError("knownWireBytes must be a positive safe integer");
      const bytes = knownWireBytes ?? utf8Bytes(sseFrame(frame.event, frame.data)).byteLength;
      this.#assertPermitCoverage(bytes, transferredMemoryPermit);
      if (this.#discardEvents) {
        this.#recordAccepted(bytes);
        if (
          transferredMemoryPermit &&
          RESPONSE_TERMINAL_EVENTS.has(frame.event) &&
          !this.#finalPermitAnchor
        ) {
          const anchor = {};
          attachAtomicBytePermit(anchor, transferredMemoryPermit);
          this.#finalPermitAnchor = anchor;
          transferredMemoryPermit = undefined;
        }
        transferredMemoryPermit?.release();
        return true;
      }
      if (this.#closed || this.#eventStreamClosed) {
        throw this.#failure ?? new Error("V2 run handle event stream is closed");
      }
      if (
        this.#bufferedEvents >= this.#maxBufferedEvents ||
        (this.#bufferedBytes > 0 && this.#bufferedBytes + bytes > this.#maxBufferedBytes)
      ) return false;
      if (transferredMemoryPermit) attachAtomicBytePermit(frame, transferredMemoryPermit);
      if (
        transferredMemoryPermit &&
        RESPONSE_TERMINAL_EVENTS.has(frame.event) &&
        !this.#finalPermitAnchor
      ) {
        const anchor = {};
        shareAtomicBytePermit(frame, anchor, "host-run-handle-final");
        this.#finalPermitAnchor = anchor;
        createdFinalAnchor = true;
      }
      this.#buffer.push({ frame, bytes });
      this.#bufferedBytes += bytes;
      this.#bufferedEvents++;
      this.#recordAccepted(bytes);
      this.#wake(this.#eventWaiters);
      return true;
    } catch (error) {
      releaseAtomicBytePermit(frame);
      if (createdFinalAnchor) this.#releaseFinalPermitAnchor();
      transferredMemoryPermit?.release();
      throw error;
    }
  }

  #recordAccepted(bytes: number): void {
    if (this.stats.firstByteMs === null) {
      this.stats.firstByteMs = Math.max(0, this.#clock.now() - this.#startedAtMs);
    }
    this.stats.bytes += bytes;
    this.stats.frames++;
  }

  async *events(): AsyncGenerator<SseFrame> {
    if (this.#eventsClaimed) throw new Error("V2 run events may only be consumed once");
    this.#eventsClaimed = true;
    try {
      for (;;) {
        const entry = this.#takeBufferedEvent();
        if (entry) {
          this.#bufferedBytes -= entry.bytes;
          this.#bufferedEvents = Math.max(0, this.#bufferedEvents - 1);
          this.#wake(this.#spaceWaiters);
          try {
            // The permit remains charged while the consumer owns the yielded
            // frame and is released only when it resumes or closes the iterator.
            yield entry.frame;
          } finally {
            releaseAtomicBytePermit(entry.frame);
          }
          continue;
        }
        if (this.#closed || this.#eventStreamClosed) return;
        await new Promise<void>(resolve => this.#eventWaiters.add(resolve));
      }
    } finally {
      this.#eventStreamClosed = true;
      this.#releaseBufferedEvents();
      this.#wakeAll();
    }
  }

  /** Explicit hook for final-only callers which will never consume `events()`. */
  discardUnconsumedEvents(): void {
    if (this.#eventsClaimed) return;
    this.#discardEvents = true;
    this.#eventStreamClosed = true;
    this.#releaseBufferedEvents();
    this.#wakeAll();
  }

  settle(final: Record<string, unknown>, rateLimit?: RateLimitHint): void {
    if (this.#closed) return;
    if (this.#finalPermitAnchor) {
      try {
        transferAtomicBytePermit(
          this.#finalPermitAnchor,
          final,
          "host-run-handle-final",
        );
        this.#finalPermitAnchor = null;
      } catch (cause) {
        this.#releaseFinalPermitAnchor();
        this.fail(cause instanceof Error
          ? cause
          : new Error("Terminal response memory ownership transfer failed"));
        return;
      }
    }
    if (this.upstreamPrelude && !this.#upstreamPreludeSettled) {
      const error = new Error("V2 response completed without an upstream prelude");
      this.#upstreamPreludeSettled = true;
      this.#rejectUpstreamPrelude?.(error);
    }
    this.rateLimit = rateLimit;
    this.#closed = true;
    this.#resolveFinal(final);
    this.#wakeAll();
  }

  fail(error: Error): void {
    this.#finishFailure(
      error,
      error instanceof V2CommittedError && error.ambiguous,
    );
  }

  cancel(error: Error): void {
    this.#finishFailure(error, true);
  }

  #finishFailure(error: Error, discardAcceptedEvents: boolean): void {
    if (this.#closed) return;
    if (this.upstreamPrelude && !this.#upstreamPreludeSettled) {
      this.#upstreamPreludeSettled = true;
      this.#rejectUpstreamPrelude?.(error);
    }
    this.#closed = true;
    this.#failure = error;
    this.#eventStreamClosed = true;
    if (discardAcceptedEvents) {
      this.#discardEvents = true;
      this.#releaseBufferedEvents();
    }
    this.#releaseFinalPermitAnchor();
    this.#rejectFinal(error);
    this.#wakeAll();
  }

  #wake(waiters: Set<() => void>): void {
    for (const resolve of waiters) resolve();
    waiters.clear();
  }

  #wakeAll(): void {
    this.#wake(this.#eventWaiters);
    this.#wake(this.#spaceWaiters);
  }

  #assertPermitCoverage(bytes: number, permit?: AtomicBytePermit): void {
    if (!permit) {
      if (this.#requireTransferredEventMemory) {
        throw new V2EventMemoryError(
          bytes > this.#maxBufferedBytes
            ? "An event above the soft buffer requires a transferred jumbo permit"
            : "Atomic event delivery requires a transferred memory permit",
          bytes > this.#maxBufferedBytes ? "jumbo_required" : "coverage",
        );
      }
      // Temporary compatibility seam until V2TransportSession transfers every
      // permit and opts into `requireTransferredEventMemory`.
      return;
    }
    const requiredBytes = v2RetainedEventPermitBytes(bytes);
    if (!permit.active || permit.bytes < requiredBytes) {
      throw new V2EventMemoryError(
        "Transferred event-memory permit does not cover the physical working set",
        "coverage",
      );
    }
    if (bytes > this.#maxBufferedBytes && permit.region !== "jumbo") {
      throw new V2EventMemoryError(
        "An event above the soft buffer requires the shared jumbo lane",
        "jumbo_required",
      );
    }
  }

  #releaseBufferedEvents(): void {
    for (let index = this.#bufferHead; index < this.#buffer.length; index++) {
      const entry = this.#buffer[index];
      if (!entry) continue;
      releaseAtomicBytePermit(entry.frame);
      this.#buffer[index] = undefined;
    }
    this.#buffer.length = 0;
    this.#bufferHead = 0;
    this.#bufferedBytes = 0;
    this.#bufferedEvents = 0;
  }

  #releaseFinalPermitAnchor(): void {
    if (!this.#finalPermitAnchor) return;
    releaseAtomicBytePermit(this.#finalPermitAnchor);
    this.#finalPermitAnchor = null;
  }

  #takeBufferedEvent(): BufferedEvent | undefined {
    if (this.#bufferHead >= this.#buffer.length) return undefined;
    const entry = this.#buffer[this.#bufferHead];
    // Clear the consumed slot immediately so the frame can be collected even
    // when the backing array remains allocated for later events.
    this.#buffer[this.#bufferHead] = undefined;
    this.#bufferHead++;
    if (this.#bufferHead === this.#buffer.length) {
      this.#buffer.length = 0;
      this.#bufferHead = 0;
    } else if (this.#bufferHead >= 1_024 && this.#bufferHead * 2 >= this.#buffer.length) {
      // Compaction is infrequent and amortized; the common dequeue is O(1).
      this.#buffer.splice(0, this.#bufferHead);
      this.#bufferHead = 0;
    }
    return entry;
  }
}
