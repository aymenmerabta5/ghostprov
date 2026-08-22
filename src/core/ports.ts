export type FetchLike = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;

export interface Clock {
  now(): number;
  sleep(ms: number, signal?: AbortSignal): Promise<void>;
  waitUntil(deadlineMs: number, signal?: AbortSignal): Promise<void>;
}

export interface RandomSource {
  next(): number;
  uuid(): string;
}

export interface MachineSample {
  totalBytes: number;
  freeBytes: number;
  rssBytes: number;
  heapBytes: number;
  externalBytes: number;
  logicalProcessors: number;
}

export interface MachineProbe {
  sample(): MachineSample;
}

export type MetricAttributes = Record<string, string | number | boolean>;

export interface MetricsSink {
  count(name: string, value?: number, attributes?: MetricAttributes): void;
  observe(name: string, value: number, attributes?: MetricAttributes): void;
}

export const NOOP_METRICS: MetricsSink = {
  count() {},
  observe() {},
};

function abortError(): DOMException {
  return new DOMException("Operation aborted", "AbortError");
}

export const SYSTEM_CLOCK: Clock = {
  now: () => performance.timeOrigin + performance.now(),
  sleep(ms, signal) {
    if (signal?.aborted) return Promise.reject(abortError());
    return new Promise<void>((resolve, reject) => {
      const timer = setTimeout(done, ms);
      const onAbort = () => {
        clearTimeout(timer);
        signal?.removeEventListener("abort", onAbort);
        reject(abortError());
      };
      function done() {
        signal?.removeEventListener("abort", onAbort);
        resolve();
      }
      signal?.addEventListener("abort", onAbort, { once: true });
    });
  },
  async waitUntil(deadlineMs, signal) {
    const remaining = deadlineMs - (performance.timeOrigin + performance.now());
    if (remaining > 0) await this.sleep(remaining, signal);
  },
};

export const SYSTEM_RANDOM: RandomSource = {
  next: () => Math.random(),
  uuid: () => crypto.randomUUID(),
};
