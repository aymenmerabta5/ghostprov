const DEFAULT_MAX_QUEUED = 1_024;
const DEFAULT_MAX_QUEUED_BYTES = 512 * 1024 * 1024;
const DEFAULT_JOB_TIMEOUT_MS = 120_000;
const MAX_TIMER_MS = 2_147_483_647;

export interface CpuTransformPoolSnapshot {
  workers: number;
  readyWorkers?: number;
  restartingWorkers?: number;
  retiringWorkers?: number;
  liveWorkers?: number;
  active: number;
  queued: number;
  queuedBytes?: number;
  maxQueued?: number;
  maxQueuedBytes?: number;
  maxActive: number;
  restarts?: number;
  closed: boolean;
}

export interface CpuTransformJobOptions {
  /**
   * Transfer the input buffer into the pool instead of copying it. The view must
   * cover a complete ArrayBuffer. On acceptance the caller's buffer is detached.
   */
  transferInput?: boolean;
  signal?: AbortSignal;
  /** Total time in the queue and worker. */
  timeoutMs?: number;
}

export interface CpuTransformPoolOptions {
  workers: number;
  maxQueued?: number;
  maxQueuedBytes?: number;
  jobTimeoutMs?: number;
  /** Test/embedding hook. Production callers should use the default worker. */
  workerFactory?: () => Worker;
}

export class CpuTransformQueueFullError extends Error {
  readonly name = "CpuTransformQueueFullError";
  readonly status = 503;
  readonly code = "cpu_transform_overloaded";

  constructor() {
    super("CPU transform queue is full");
  }
}

export class CpuTransformTimeoutError extends Error {
  readonly name = "CpuTransformTimeoutError";
  readonly status = 503;
  readonly code = "cpu_transform_timeout";

  constructor(timeoutMs: number) {
    super(`CPU transform timed out after ${timeoutMs}ms`);
  }
}

interface Job {
  id: number;
  data: Uint8Array;
  resolve: (value: Uint8Array) => void;
  reject: (error: unknown) => void;
  signal?: AbortSignal;
  onAbort?: () => void;
  timer?: ReturnType<typeof setTimeout>;
  settled: boolean;
}

interface WorkerState {
  worker: Worker | null;
  job: Job | null;
  retiring: boolean;
  onClose: ((event: Event) => void) | null;
  closePromise: Promise<void> | null;
  resolveClose: (() => void) | null;
  restartTimer: ReturnType<typeof setTimeout> | null;
  restartAttempts: number;
}

function abortError(signal: AbortSignal): unknown {
  void signal;
  return new DOMException("CPU transform aborted", "AbortError");
}

function positiveInteger(name: string, value: number): number {
  if (!Number.isSafeInteger(value) || value < 1) throw new RangeError(`${name} must be positive`);
  return value;
}

function nonNegativeInteger(name: string, value: number): number {
  if (!Number.isSafeInteger(value) || value < 0) throw new RangeError(`${name} must be non-negative`);
  return value;
}

function timeout(name: string, value: number): number {
  if (!Number.isFinite(value) || value < 1 || value > MAX_TIMER_MS) {
    throw new RangeError(`${name} must be between 1 and ${MAX_TIMER_MS}`);
  }
  return value;
}

function copyInput(data: Uint8Array): Uint8Array {
  const copy = new Uint8Array(data.byteLength);
  copy.set(data);
  return copy;
}

function takeInput(data: Uint8Array): Uint8Array {
  if (!(data.buffer instanceof ArrayBuffer)) {
    throw new TypeError("transferInput requires an ArrayBuffer-backed Uint8Array");
  }
  if (data.byteOffset !== 0 || data.byteLength !== data.buffer.byteLength) {
    throw new RangeError("transferInput requires a view covering its complete ArrayBuffer");
  }
  return structuredClone(data, { transfer: [data.buffer] });
}

export class CpuTransformPool {
  readonly #states: WorkerState[];
  readonly #queue: Job[] = [];
  readonly #maxQueued: number;
  readonly #maxQueuedBytes: number;
  readonly #jobTimeoutMs: number;
  readonly #workerFactory: () => Worker;
  #nextId = 1;
  #queuedBytes = 0;
  #maxActive = 0;
  #restarts = 0;
  #closed = false;
  #closePromise: Promise<void> | null = null;
  #dispatching = false;
  #dispatchRequested = false;

  constructor(options: CpuTransformPoolOptions) {
    positiveInteger("workers", options.workers);
    this.#maxQueued = nonNegativeInteger("maxQueued", options.maxQueued ?? DEFAULT_MAX_QUEUED);
    this.#maxQueuedBytes = nonNegativeInteger(
      "maxQueuedBytes",
      options.maxQueuedBytes ?? DEFAULT_MAX_QUEUED_BYTES,
    );
    this.#jobTimeoutMs = timeout("jobTimeoutMs", options.jobTimeoutMs ?? DEFAULT_JOB_TIMEOUT_MS);
    this.#workerFactory = options.workerFactory ?? (() => new Worker(
      new URL("./cpu-transform-worker.ts", import.meta.url).href,
      { type: "module" },
    ));
    this.#states = Array.from({ length: options.workers }, () => ({
      worker: null,
      job: null,
      retiring: false,
      onClose: null,
      closePromise: null,
      resolveClose: null,
      restartTimer: null,
      restartAttempts: 0,
    }));

    try {
      for (const state of this.#states) this.#startWorker(state);
    } catch (error) {
      this.#closed = true;
      for (const state of this.#states) {
        if (state.worker) this.#retireWorker(state, state.worker, error);
      }
      throw error;
    }
  }

  gzip(data: Uint8Array, options: CpuTransformJobOptions = {}): Promise<Uint8Array> {
    if (this.#closed) return Promise.reject(new Error("CPU transform pool is closed"));
    if (options.signal?.aborted) return Promise.reject(abortError(options.signal));

    let timeoutMs: number;
    try {
      timeoutMs = timeout("timeoutMs", options.timeoutMs ?? this.#jobTimeoutMs);
    } catch (error) {
      return Promise.reject(error);
    }

    const willQueue = !this.#states.some(
      state => state.worker !== null && !state.retiring && state.job === null,
    );
    if (willQueue && (
      this.#queue.length >= this.#maxQueued ||
      this.#queuedBytes + data.byteLength > this.#maxQueuedBytes
    )) {
      return Promise.reject(new CpuTransformQueueFullError());
    }

    let ownedData: Uint8Array;
    try {
      ownedData = options.transferInput ? takeInput(data) : copyInput(data);
    } catch (error) {
      return Promise.reject(error);
    }

    return new Promise((resolve, reject) => {
      const job: Job = {
        id: this.#nextId++,
        data: ownedData,
        resolve,
        reject,
        signal: options.signal,
        settled: false,
      };
      job.onAbort = () => this.#cancel(job, abortError(options.signal!));
      options.signal?.addEventListener("abort", job.onAbort, { once: true });
      job.timer = setTimeout(
        () => this.#cancel(job, new CpuTransformTimeoutError(timeoutMs)),
        timeoutMs,
      );
      this.#queue.push(job);
      this.#queuedBytes += ownedData.byteLength;
      this.#dispatch();
    });
  }

  snapshot(): CpuTransformPoolSnapshot {
    const liveWorkers = this.#states.filter(state => state.worker !== null).length;
    const retiringWorkers = this.#states.filter(state => state.retiring).length;
    const readyWorkers = this.#states.filter(state => state.worker !== null && !state.retiring).length;
    const active = this.#states.filter(state => state.job !== null).length;
    return {
      workers: this.#states.length,
      readyWorkers,
      restartingWorkers: this.#states.length - readyWorkers,
      retiringWorkers,
      liveWorkers,
      active,
      queued: this.#queue.length,
      queuedBytes: this.#queuedBytes,
      maxQueued: this.#maxQueued,
      maxQueuedBytes: this.#maxQueuedBytes,
      maxActive: this.#maxActive,
      restarts: this.#restarts,
      closed: this.#closed,
    };
  }

  close(): Promise<void> {
    if (this.#closePromise) return this.#closePromise;
    this.#closed = true;
    const error = new Error("CPU transform pool closed");

    for (const job of this.#queue.splice(0)) {
      this.#queuedBytes -= job.data.byteLength;
      this.#reject(job, error);
    }
    this.#queuedBytes = 0;

    const closePromises: Promise<void>[] = [];
    for (const state of this.#states) {
      if (state.restartTimer) {
        clearTimeout(state.restartTimer);
        state.restartTimer = null;
      }
      if (state.closePromise) closePromises.push(state.closePromise);
    }
    this.#closePromise = Promise.all(closePromises).then(() => undefined);
    for (const state of this.#states) {
      if (state.worker) this.#retireWorker(state, state.worker, error);
    }
    return this.#closePromise;
  }

  #startWorker(state: WorkerState): void {
    if (this.#closed || state.worker || state.retiring) return;
    const worker = this.#workerFactory();
    state.worker = worker;
    state.retiring = false;
    state.closePromise = new Promise(resolve => { state.resolveClose = resolve; });
    const onClose = (event: Event) => {
      const close = event as CloseEvent;
      this.#workerClosed(
        state,
        worker,
        new Error(close.reason || `CPU transform worker closed${close.code ? ` (${close.code})` : ""}`),
      );
    };
    state.onClose = onClose;
    worker.onmessage = event => this.#complete(state, worker, event.data);
    worker.onerror = event => {
      event.preventDefault();
      this.#failWorker(state, worker, new Error(event.message || "CPU transform worker failed"));
    };
    worker.onmessageerror = () => this.#failWorker(
      state,
      worker,
      new Error("CPU transform worker returned an unreadable message"),
    );
    worker.addEventListener("close", onClose);
  }

  #detachWorkerHandlers(state: WorkerState, worker: Worker, includeClose: boolean): void {
    worker.onmessage = null;
    worker.onerror = null;
    worker.onmessageerror = null;
    if (includeClose && state.onClose) {
      worker.removeEventListener("close", state.onClose);
      state.onClose = null;
    }
  }

  #retireWorker(state: WorkerState, worker: Worker, error: unknown): void {
    if (state.worker !== worker || state.retiring) return;
    state.retiring = true;
    const job = state.job;
    state.job = null;
    if (job) this.#reject(job, error);
    // Keep the close listener attached: Bun's close event is the proof that the
    // synchronous gzip worker exited. A replacement must not start before it.
    this.#detachWorkerHandlers(state, worker, false);
    try {
      worker.terminate();
    } catch {
      // Preserve the retiring slot if termination cannot be requested. Starting
      // a replacement without an exit acknowledgement could exceed the CPU cap.
    }
    this.#dispatch();
  }

  #workerClosed(state: WorkerState, worker: Worker, error: unknown): void {
    if (state.worker !== worker) return;
    const job = state.job;
    state.job = null;
    if (job) this.#reject(job, error);
    this.#detachWorkerHandlers(state, worker, true);
    state.worker = null;
    state.retiring = false;
    state.resolveClose?.();
    state.resolveClose = null;
    state.closePromise = null;
    if (!this.#closed) {
      this.#restarts++;
      this.#scheduleRestart(state);
      this.#dispatch();
    }
  }

  #scheduleRestart(state: WorkerState): void {
    if (this.#closed || state.worker || state.retiring || state.restartTimer) return;
    const attempt = state.restartAttempts++;
    const delayMs = attempt === 0 ? 0 : Math.min(1_000, 25 * (2 ** Math.min(6, attempt - 1)));
    state.restartTimer = setTimeout(() => {
      state.restartTimer = null;
      if (this.#closed) return;
      try {
        this.#startWorker(state);
      } catch {
        this.#scheduleRestart(state);
        return;
      }
      this.#dispatch();
    }, delayMs);
  }

  #dispatch(): void {
    if (this.#closed) return;
    if (this.#dispatching) {
      this.#dispatchRequested = true;
      return;
    }
    this.#dispatching = true;
    try {
      do {
        this.#dispatchRequested = false;
        for (const state of this.#states) {
          const worker = state.worker;
          if (!worker || state.retiring || state.job) continue;
          const job = this.#queue.shift();
          if (!job) break;
          this.#queuedBytes -= job.data.byteLength;
          state.job = job;
          const active = this.#states.reduce((count, value) => count + (value.job ? 1 : 0), 0);
          this.#maxActive = Math.max(this.#maxActive, active);
          try {
            if (!(job.data.buffer instanceof ArrayBuffer)) {
              throw new TypeError("CPU transform input is not transferable");
            }
            worker.postMessage(
              { id: job.id, operation: "gzip", data: job.data },
              [job.data.buffer],
            );
          } catch (error) {
            this.#failWorker(
              state,
              worker,
              error instanceof Error ? error : new Error(String(error)),
            );
          }
        }
      } while (this.#dispatchRequested && !this.#closed);
    } finally {
      this.#dispatching = false;
    }
  }

  #complete(state: WorkerState, worker: Worker, response: unknown): void {
    if (this.#closed || state.worker !== worker) return;
    const job = state.job;
    if (!job) {
      this.#failWorker(state, worker, new Error("CPU transform worker returned an unsolicited response"));
      return;
    }
    if (!response || typeof response !== "object" || (response as { id?: unknown }).id !== job.id) {
      this.#failWorker(state, worker, new Error("CPU transform worker response ID mismatch"));
      return;
    }

    state.job = null;
    state.restartAttempts = 0;
    const result = response as { data?: unknown; error?: unknown };
    if (typeof result.error === "string") {
      this.#reject(job, new Error(result.error));
    } else if (result.data instanceof Uint8Array) {
      // The worker transferred this buffer; returning the view avoids another full copy.
      this.#resolve(job, result.data);
    } else {
      this.#reject(job, new Error("CPU transform worker returned no data"));
    }
    this.#dispatch();
  }

  #failWorker(state: WorkerState, worker: Worker, error: unknown): void {
    this.#retireWorker(state, worker, error);
  }

  #cancel(job: Job, error: unknown): void {
    if (job.settled) return;
    const queuedIndex = this.#queue.indexOf(job);
    if (queuedIndex >= 0) {
      this.#queue.splice(queuedIndex, 1);
      this.#queuedBytes -= job.data.byteLength;
      this.#reject(job, error);
      return;
    }
    const state = this.#states.find(value => value.job === job);
    if (!state?.worker) {
      this.#reject(job, error);
      return;
    }
    // gzipSync cannot be interrupted; replace the worker to release this slot now.
    this.#failWorker(state, state.worker, error);
  }

  #resolve(job: Job, value: Uint8Array): void {
    if (job.settled) return;
    job.settled = true;
    if (job.timer) clearTimeout(job.timer);
    job.signal?.removeEventListener("abort", job.onAbort!);
    job.resolve(value);
  }

  #reject(job: Job, error: unknown): void {
    if (job.settled) return;
    job.settled = true;
    if (job.timer) clearTimeout(job.timer);
    job.signal?.removeEventListener("abort", job.onAbort!);
    job.reject(error);
  }
}
