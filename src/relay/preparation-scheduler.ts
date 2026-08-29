type PreparationJob<T> = {
  readonly task: () => Promise<T | null>;
  readonly costBytes: number;
  readonly signal: AbortSignal;
  readonly resolve: (value: T | null) => void;
  onAbort?: () => void;
};

export type BoundedPreparationSchedulerOptions = Readonly<{
  maxQueued: number;
  maxQueuedBytes: number;
}>;

/**
 * Serializes cooperative preprocessing on the main thread while bounding every
 * queued closure by count and retained bytes. Overflow skips the optimization;
 * callers continue through their ordinary full-payload path.
 */
export class BoundedPreparationScheduler<T> {
  readonly #queue: Array<PreparationJob<T>> = [];
  readonly #maxQueued: number;
  readonly #maxQueuedBytes: number;
  #queuedBytes = 0;
  #scheduled = false;
  #running = false;
  #closed = false;

  constructor(options: BoundedPreparationSchedulerOptions) {
    if (!Number.isSafeInteger(options.maxQueued) || options.maxQueued < 1) {
      throw new RangeError("maxQueued must be a positive safe integer");
    }
    if (!Number.isSafeInteger(options.maxQueuedBytes) || options.maxQueuedBytes < 1) {
      throw new RangeError("maxQueuedBytes must be a positive safe integer");
    }
    this.#maxQueued = options.maxQueued;
    this.#maxQueuedBytes = options.maxQueuedBytes;
  }

  prepare(
    task: () => Promise<T | null>,
    costBytes: number,
    signal: AbortSignal,
  ): Promise<T | null> {
    if (this.#closed || signal.aborted) return Promise.resolve(null);
    if (!Number.isSafeInteger(costBytes) || costBytes < 1) return Promise.resolve(null);
    const cost = costBytes;
    if (
      this.#queue.length >= this.#maxQueued ||
      this.#queuedBytes + cost > this.#maxQueuedBytes
    ) return Promise.resolve(null);

    return new Promise(resolve => {
      const job: PreparationJob<T> = { task, costBytes: cost, signal, resolve };
      job.onAbort = () => {
        const index = this.#queue.indexOf(job);
        if (index < 0) return;
        this.#queue.splice(index, 1);
        this.#queuedBytes = Math.max(0, this.#queuedBytes - job.costBytes);
        resolve(null);
      };
      signal.addEventListener("abort", job.onAbort, { once: true });
      this.#queue.push(job);
      this.#queuedBytes += cost;
      this.#schedule();
    });
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    for (const job of this.#queue.splice(0)) {
      job.signal.removeEventListener("abort", job.onAbort!);
      job.resolve(null);
    }
    this.#queuedBytes = 0;
  }

  #schedule(): void {
    if (this.#closed || this.#scheduled || this.#running || this.#queue.length === 0) return;
    this.#scheduled = true;
    setTimeout(() => {
      this.#scheduled = false;
      void this.#drainOne();
    }, 0);
  }

  async #drainOne(): Promise<void> {
    if (this.#running) return;
    const job = this.#queue.shift();
    if (!job) return;
    this.#queuedBytes = Math.max(0, this.#queuedBytes - job.costBytes);
    job.signal.removeEventListener("abort", job.onAbort!);
    if (job.signal.aborted) {
      job.resolve(null);
      this.#schedule();
      return;
    }

    this.#running = true;
    let result: T | null = null;
    try {
      result = await job.task();
    } catch {
      // Preprocessing is optional; the full-payload path remains authoritative.
    } finally {
      this.#running = false;
    }
    job.resolve(result);
    this.#schedule();
  }
}
