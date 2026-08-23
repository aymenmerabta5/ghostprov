import { SYSTEM_CLOCK, type Clock } from "../../core/ports";
import type { RunHandle } from "../../relay/run-handle";
import type { SseFrame } from "../../relay/sse";
import { utf8Bytes } from "../protocol/shared";

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
}

export class V2RunHandle implements RunHandle {
  readonly final: Promise<Record<string, unknown>>;
  readonly stats: RunHandle["stats"] = {
    bytes: 0,
    firstByteMs: null as number | null,
    frames: 0,
  };
  readonly #clock: Clock;
  readonly #startedAtMs: number;
  readonly #maxBufferedBytes: number;
  readonly #buffer: BufferedEvent[] = [];
  readonly #eventWaiters = new Set<() => void>();
  readonly #spaceWaiters = new Set<() => void>();
  readonly #resolveFinal: (value: Record<string, unknown>) => void;
  readonly #rejectFinal: (error: unknown) => void;
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
  }

  async acceptEvent(frame: SseFrame): Promise<void> {
    if (!frame || typeof frame.event !== "string" || !frame.event || typeof frame.data !== "string") {
      throw new TypeError("SSE frame is invalid");
    }
    const bytes = utf8Bytes(`event: ${frame.event}\ndata: ${frame.data}\n\n`).byteLength;
    while (
      !this.#closed &&
      this.#buffer.length > 0 &&
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
      const entry = this.#buffer.shift();
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
}
