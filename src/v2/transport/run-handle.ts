import { SYSTEM_CLOCK, type Clock } from "../../core/ports";
import type { RunHandle } from "../../relay/run-handle";
import type { SseFrame } from "../../relay/sse";
import { utf8Bytes, type UpstreamPrelude } from "../protocol/shared";
import type { RateLimitHint } from "../rate-limit";
import type { ProviderErrorEnvelope } from "../provider-error";

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
  maxBufferedBytes: number;
  clock?: Clock;
  expectUpstreamPrelude?: boolean;
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
  #closed = false;
  #failure: Error | null = null;
  #eventsClaimed = false;

  constructor(options: V2RunHandleOptions) {
    if (!Number.isSafeInteger(options.maxBufferedBytes) || options.maxBufferedBytes < 1) {
      throw new RangeError("maxBufferedBytes must be a positive safe integer");
    }
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#startedAtMs = this.#clock.now();
    this.#maxBufferedBytes = options.maxBufferedBytes;
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

  async acceptEvent(frame: SseFrame, knownWireBytes?: number): Promise<void> {
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
      utf8Bytes(`event: ${frame.event}\ndata: ${frame.data}\n\n`).byteLength;
    while (
      !this.#closed &&
      this.#bufferHead < this.#buffer.length &&
      this.#bufferedBytes + bytes > this.#maxBufferedBytes
    ) {
      await new Promise<void>(resolve => this.#spaceWaiters.add(resolve));
    }
    if (this.#closed) {
      throw this.#failure ?? new Error("V2 run handle is closed");
    }
    this.#buffer.push({ frame, bytes });
    this.#bufferedBytes += bytes;
    if (this.stats.firstByteMs === null) {
      this.stats.firstByteMs = Math.max(0, this.#clock.now() - this.#startedAtMs);
    }
    this.stats.bytes += bytes;
    this.stats.frames++;
    this.#wake(this.#eventWaiters);
  }

  async *events(): AsyncGenerator<SseFrame> {
    if (this.#eventsClaimed) throw new Error("V2 run events may only be consumed once");
    this.#eventsClaimed = true;
    for (;;) {
      const entry = this.#takeBufferedEvent();
      if (entry) {
        this.#bufferedBytes -= entry.bytes;
        this.#wake(this.#spaceWaiters);
        yield entry.frame;
        continue;
      }
      if (this.#closed) return;
      await new Promise<void>(resolve => this.#eventWaiters.add(resolve));
    }
  }

  settle(final: Record<string, unknown>, rateLimit?: RateLimitHint): void {
    if (this.#closed) return;
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
    if (this.#closed) return;
    if (this.upstreamPrelude && !this.#upstreamPreludeSettled) {
      this.#upstreamPreludeSettled = true;
      this.#rejectUpstreamPrelude?.(error);
    }
    this.#closed = true;
    this.#failure = error;
    this.#rejectFinal(error);
    this.#wakeAll();
  }

  cancel(error: Error): void {
    this.fail(error);
  }

  #wake(waiters: Set<() => void>): void {
    for (const resolve of waiters) resolve();
    waiters.clear();
  }

  #wakeAll(): void {
    this.#wake(this.#eventWaiters);
    this.#wake(this.#spaceWaiters);
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
