import type { RunHandle } from "../../src/relay/run-handle";
import { sseFrame, type SseFrame } from "../../src/relay/sse";
import {
  abortError,
  ForwardingProtocolError,
  positiveSafeInteger,
} from "./shared";

const HARD_MAX_QUEUED_EVENT_BYTES = 1024 * 1024;

interface BufferedVerifierFrame {
  frame: SseFrame;
  bytes: number;
}

export class VerifierRunHandle implements RunHandle {
  readonly final: Promise<Record<string, unknown>>;
  readonly stats: RunHandle["stats"] = {
    bytes: 0,
    firstByteMs: null,
    frames: 0,
  };
  readonly #maxBufferedBytes: number;
  readonly #onIteratorCancel: () => void;
  readonly #startedAt = performance.now();
  readonly #buffer: BufferedVerifierFrame[] = [];
  readonly #eventWaiters = new Set<() => void>();
  readonly #spaceWaiters = new Set<() => void>();
  readonly #resolveFinal: (value: Record<string, unknown>) => void;
  readonly #rejectFinal: (error: unknown) => void;
  #bufferedBytes = 0;
  #closed = false;
  #failure: Error | null = null;
  #eventsClaimed = false;

  constructor(maxBufferedBytes: number, onIteratorCancel: () => void) {
    this.#maxBufferedBytes = Math.min(
      positiveSafeInteger(maxBufferedBytes, "maxBufferedBytes"),
      HARD_MAX_QUEUED_EVENT_BYTES,
    );
    this.#onIteratorCancel = onIteratorCancel;
    let resolveFinal!: (value: Record<string, unknown>) => void;
    let rejectFinal!: (error: unknown) => void;
    this.final = new Promise<Record<string, unknown>>((resolve, reject) => {
      resolveFinal = resolve;
      rejectFinal = reject;
    });
    this.#resolveFinal = resolveFinal;
    this.#rejectFinal = rejectFinal;
    void this.final.catch(() => undefined);
  }

  canAcceptEvent(frame: SseFrame): boolean {
    const bytes = this.#frameBytes(frame);
    return bytes <= HARD_MAX_QUEUED_EVENT_BYTES && bytes <= this.#maxBufferedBytes;
  }

  async acceptEvent(frame: SseFrame): Promise<void> {
    if (!frame || typeof frame.event !== "string" || !frame.event ||
        typeof frame.data !== "string") {
      throw new ForwardingProtocolError("Upstream SSE event is malformed");
    }
    const bytes = this.#frameBytes(frame);
    if (bytes > HARD_MAX_QUEUED_EVENT_BYTES || bytes > this.#maxBufferedBytes) {
      throw new ForwardingProtocolError("Upstream SSE event exceeds verifier queue limit");
    }
    while (!this.#closed && this.#bufferedBytes + bytes > this.#maxBufferedBytes) {
      await new Promise<void>(resolve => this.#spaceWaiters.add(resolve));
    }
    if (this.#closed) throw this.#failure ?? abortError();
    this.#buffer.push({ frame, bytes });
    this.#bufferedBytes += bytes;
    if (this.stats.firstByteMs === null) {
      this.stats.firstByteMs = Math.max(0, performance.now() - this.#startedAt);
    }
    this.stats.bytes += bytes;
    this.stats.frames += 1;
    this.#wake(this.#eventWaiters);
  }

  events(): AsyncGenerator<SseFrame> {
    if (this.#eventsClaimed) throw new Error("Verifier run events may only be consumed once");
    this.#eventsClaimed = true;
    let done = false;
    let cancelled = false;
    const cancel = (): void => {
      if (cancelled) return;
      cancelled = true;
      if (!this.#closed) this.#onIteratorCancel();
    };
    const iterator = {
      next: async (): Promise<IteratorResult<SseFrame>> => {
        if (done) return { value: undefined, done: true };
        const result = await this.#nextEvent();
        if (result.done) done = true;
        return result;
      },
      return: async (_value?: unknown): Promise<IteratorResult<SseFrame>> => {
        cancel();
        done = true;
        return { value: undefined, done: true };
      },
      throw: async (_error?: unknown): Promise<IteratorResult<SseFrame>> => {
        cancel();
        done = true;
        throw abortError();
      },
      [Symbol.asyncIterator]() {
        return this;
      },
    };
    return iterator as unknown as AsyncGenerator<SseFrame>;
  }

  settle(final: Record<string, unknown>): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#resolveFinal(final);
    this.#wakeAll();
  }

  fail(error: Error): void {
    if (this.#closed) return;
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

  #frameBytes(frame: SseFrame): number {
    return new TextEncoder().encode(sseFrame(frame.event, frame.data)).byteLength;
  }

  async #nextEvent(): Promise<IteratorResult<SseFrame>> {
    for (;;) {
      const entry = this.#buffer.shift();
      if (entry) {
        this.#bufferedBytes -= entry.bytes;
        this.#wake(this.#spaceWaiters);
        return { value: entry.frame, done: false };
      }
      if (this.#closed) return { value: undefined, done: true };
      await new Promise<void>(resolve => this.#eventWaiters.add(resolve));
    }
  }
}
