export interface CpuTransformPoolSnapshot {
  workers: number;
  active: number;
  queued: number;
  maxActive: number;
  closed: boolean;
}

interface Job {
  id: number;
  data: Uint8Array;
  resolve: (value: Uint8Array) => void;
  reject: (error: unknown) => void;
}

interface WorkerState {
  worker: Worker;
  job: Job | null;
}

export class CpuTransformPool {
  readonly #states: WorkerState[];
  readonly #queue: Job[] = [];
  #nextId = 1;
  #maxActive = 0;
  #closed = false;

  constructor(options: { workers: number }) {
    if (!Number.isInteger(options.workers) || options.workers < 1) throw new RangeError("workers must be positive");
    this.#states = Array.from({ length: options.workers }, () => {
      const worker = new Worker(new URL("./cpu-transform-worker.ts", import.meta.url).href, { type: "module" });
      const state: WorkerState = { worker, job: null };
      worker.onmessage = event => this.#complete(state, event.data);
      worker.onerror = event => this.#fail(state, new Error(event.message));
      return state;
    });
  }

  gzip(data: Uint8Array): Promise<Uint8Array> {
    if (this.#closed) return Promise.reject(new Error("CPU transform pool is closed"));
    return new Promise((resolve, reject) => {
      this.#queue.push({ id: this.#nextId++, data: data.slice(), resolve, reject });
      this.#dispatch();
    });
  }

  snapshot(): CpuTransformPoolSnapshot {
    const active = this.#states.filter(state => state.job !== null).length;
    return {
      workers: this.#states.length,
      active,
      queued: this.#queue.length,
      maxActive: this.#maxActive,
      closed: this.#closed,
    };
  }

  async close(): Promise<void> {
    this.#closed = true;
    const error = new Error("CPU transform pool closed");
    for (const job of this.#queue.splice(0)) job.reject(error);
    for (const state of this.#states) {
      state.job?.reject(error);
      state.job = null;
      state.worker.terminate();
    }
  }

  #dispatch(): void {
    for (const state of this.#states) {
      if (state.job) continue;
      const job = this.#queue.shift();
      if (!job) break;
      state.job = job;
      this.#maxActive = Math.max(this.#maxActive, this.#states.filter(value => value.job !== null).length);
      state.worker.postMessage(
        { id: job.id, operation: "gzip", data: job.data },
        [job.data.buffer],
      );
    }
  }

  #complete(state: WorkerState, response: { id: number; data?: Uint8Array; error?: string }): void {
    const job = state.job;
    if (!job || response.id !== job.id) return;
    state.job = null;
    if (response.error) job.reject(new Error(response.error));
    else if (response.data) job.resolve(new Uint8Array(response.data));
    else job.reject(new Error("CPU worker returned no data"));
    this.#dispatch();
  }

  #fail(state: WorkerState, error: Error): void {
    const job = state.job;
    state.job = null;
    job?.reject(error);
    this.#dispatch();
  }
}
