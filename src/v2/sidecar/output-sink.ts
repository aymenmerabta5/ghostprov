interface Waiter {
  resolve(): void;
  reject(error: unknown): void;
  signal?: AbortSignal;
  abort?: () => void;
}

interface OutputWatermarks {
  highFrames: number;
  highBytes: number;
  lowFrames: number;
  lowBytes: number;
  hardFrames: number;
  hardBytes: number;
}

function aborted(signal: AbortSignal): unknown {
  return signal.reason ?? new DOMException("aborted", "AbortError");
}

export class OutputBackpressure {
  readonly #limits: OutputWatermarks;
  readonly #waiters: Waiter[] = [];
  #paused = false;
  #closed = false;
  #frames = 0;
  #bytes = 0;

  constructor(limits: OutputWatermarks) {
    if (
      limits.lowFrames >= limits.highFrames ||
      limits.highFrames > limits.hardFrames ||
      limits.lowBytes >= limits.highBytes ||
      limits.highBytes > limits.hardBytes
    ) throw new RangeError("invalid output watermarks");
    this.#limits = limits;
  }

  update(value: { frames: number; bytes: number }): void {
    if (this.#closed) return;
    this.#frames = Math.max(0, value.frames);
    this.#bytes = Math.max(0, value.bytes);
    if (
      this.#frames >= this.#limits.highFrames ||
      this.#bytes >= this.#limits.highBytes
    ) this.#paused = true;
    if (
      this.#paused &&
      this.#frames <= this.#limits.lowFrames &&
      this.#bytes <= this.#limits.lowBytes
    ) {
      this.#paused = false;
      for (const waiter of this.#waiters.splice(0)) {
        this.#detach(waiter);
        waiter.resolve();
      }
    }
  }

  wait(signal?: AbortSignal): Promise<void> {
    if (this.#closed) return Promise.reject(new Error("output backpressure closed"));
    if (signal?.aborted) return Promise.reject(aborted(signal));
    if (!this.#paused) return Promise.resolve();
    return new Promise((resolve, reject) => {
      const waiter: Waiter = { resolve, reject, signal };
      waiter.abort = () => {
        const index = this.#waiters.indexOf(waiter);
        if (index < 0) return;
        this.#waiters.splice(index, 1);
        this.#detach(waiter);
        reject(aborted(signal!));
      };
      signal?.addEventListener("abort", waiter.abort, { once: true });
      this.#waiters.push(waiter);
    });
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#paused = false;
    const error = new Error("output backpressure closed");
    for (const waiter of this.#waiters.splice(0)) {
      this.#detach(waiter);
      waiter.reject(error);
    }
  }

  snapshot(): {
    paused: boolean;
    closed: boolean;
    frames: number;
    bytes: number;
    waiters: number;
    hardExceeded: boolean;
  } {
    return {
      paused: this.#paused,
      closed: this.#closed,
      frames: this.#frames,
      bytes: this.#bytes,
      waiters: this.#waiters.length,
      hardExceeded:
        this.#frames > this.#limits.hardFrames ||
        this.#bytes > this.#limits.hardBytes,
    };
  }

  #detach(waiter: Waiter): void {
    if (waiter.abort) waiter.signal?.removeEventListener("abort", waiter.abort);
  }
}
