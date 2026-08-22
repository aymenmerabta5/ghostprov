import { SYSTEM_CLOCK, type Clock } from "../../core/ports";

export class AdmissionError extends Error {
  readonly name = "AdmissionError";

  constructor(
    message: string,
    readonly code: "bounds" | "capacity" | "closed",
  ) {
    super(message);
  }
}

interface Waiter {
  reservedBytes: number;
  resolve: (lease: { release(): void }) => void;
  reject: (error: unknown) => void;
  signal?: AbortSignal;
  onAbort?: () => void;
}

function abortError(): DOMException {
  return new DOMException("Admission acquisition aborted", "AbortError");
}

export class AdmissionController {
  readonly #physicalMax: number;
  readonly #acceptedMax: number;
  readonly #memoryBudgetBytes: number;
  readonly #clock: Clock;
  readonly #waiters: Waiter[] = [];
  readonly #resetTimes: number[] = [];
  #hostCeiling: number;
  #adaptiveLimit: number;
  #lastRecoveryAt: number;
  #active = 0;
  #reservedBytes = 0;
  #closed = false;

  constructor(options: {
    physicalMax: number;
    acceptedMax: number;
    hostCeiling: number;
    memoryBudgetBytes: number;
    clock?: Clock;
  }) {
    if (!Number.isSafeInteger(options.physicalMax) || options.physicalMax < 1 || options.physicalMax > 128) {
      throw new RangeError("physicalMax must be an integer from 1 through 128");
    }
    if (!Number.isSafeInteger(options.acceptedMax) || options.acceptedMax < options.physicalMax) {
      throw new RangeError("acceptedMax must be at least physicalMax");
    }
    if (!Number.isSafeInteger(options.memoryBudgetBytes) || options.memoryBudgetBytes < 1) {
      throw new RangeError("memoryBudgetBytes must be positive");
    }
    if (!Number.isSafeInteger(options.hostCeiling) || options.hostCeiling < 0) {
      throw new RangeError("hostCeiling must be non-negative");
    }
    this.#physicalMax = options.physicalMax;
    this.#acceptedMax = options.acceptedMax;
    this.#memoryBudgetBytes = options.memoryBudgetBytes;
    this.#hostCeiling = Math.min(options.hostCeiling, options.physicalMax);
    this.#adaptiveLimit = options.physicalMax;
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#lastRecoveryAt = this.#clock.now();
  }

  acquire(
    reservedBytes: number,
    signal?: AbortSignal,
  ): Promise<{ release(): void }> {
    if (signal?.aborted) return Promise.reject(abortError());
    this.#validateReservation(reservedBytes);
    if (this.#closed) {
      return Promise.reject(new AdmissionError("Admission is closed", "closed"));
    }
    const immediate = this.tryAcquire(reservedBytes);
    if (immediate) return Promise.resolve(immediate);
    if (this.#active + this.#waiters.length >= this.#acceptedMax) {
      return Promise.reject(new AdmissionError("Accepted request queue is full", "capacity"));
    }
    return new Promise((resolve, reject) => {
      const waiter: Waiter = { reservedBytes, resolve, reject, signal };
      waiter.onAbort = () => {
        const index = this.#waiters.indexOf(waiter);
        if (index < 0) return;
        this.#waiters.splice(index, 1);
        reject(abortError());
      };
      signal?.addEventListener("abort", waiter.onAbort, { once: true });
      this.#waiters.push(waiter);
    });
  }

  tryAcquire(reservedBytes: number): { release(): void } | null {
    this.#validateReservation(reservedBytes);
    if (this.#closed) throw new AdmissionError("Admission is closed", "closed");
    this.refresh();
    if (
      this.#active >= this.#effectiveLimit() ||
      this.#reservedBytes + reservedBytes > this.#memoryBudgetBytes
    ) return null;
    this.#active++;
    this.#reservedBytes += reservedBytes;
    let released = false;
    return {
      release: () => {
        if (released) return;
        released = true;
        this.#active--;
        this.#reservedBytes -= reservedBytes;
        this.#drain();
      },
    };
  }

  setHostCeiling(value: number): void {
    if (!Number.isSafeInteger(value) || value < 0) {
      throw new AdmissionError("Host ceiling must be non-negative", "bounds");
    }
    this.#hostCeiling = Math.min(value, this.#physicalMax);
    this.#drain();
  }

  recordProxyReset(): void {
    const now = this.#clock.now();
    while (this.#resetTimes.length && this.#resetTimes[0] < now - 30_000) {
      this.#resetTimes.shift();
    }
    this.#resetTimes.push(now);
    if (this.#resetTimes.length < 3) return;
    this.#adaptiveLimit = Math.max(1, Math.floor(this.#adaptiveLimit / 2));
    this.#resetTimes.length = 0;
    this.#lastRecoveryAt = now;
  }

  refresh(): void {
    if (this.#adaptiveLimit >= this.#physicalMax) return;
    const now = this.#clock.now();
    const steps = Math.floor((now - this.#lastRecoveryAt) / 30_000);
    if (steps < 1) return;
    this.#adaptiveLimit = Math.min(this.#physicalMax, this.#adaptiveLimit + steps);
    this.#lastRecoveryAt += steps * 30_000;
    this.#drain();
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    for (const waiter of this.#waiters.splice(0)) {
      waiter.signal?.removeEventListener("abort", waiter.onAbort!);
      waiter.reject(new AdmissionError("Admission is closed", "closed"));
    }
  }

  snapshot(): {
    active: number;
    queued: number;
    effectiveLimit: number;
    physicalMax: number;
    reservedBytes: number;
    memoryBudgetBytes: number;
  } {
    return {
      active: this.#active,
      queued: this.#waiters.length,
      effectiveLimit: this.#effectiveLimit(),
      physicalMax: this.#physicalMax,
      reservedBytes: this.#reservedBytes,
      memoryBudgetBytes: this.#memoryBudgetBytes,
    };
  }

  #effectiveLimit(): number {
    return Math.min(this.#physicalMax, this.#hostCeiling, this.#adaptiveLimit);
  }

  #validateReservation(value: number): void {
    if (!Number.isSafeInteger(value) || value < 1 || value > this.#memoryBudgetBytes) {
      throw new AdmissionError("Reservation exceeds the sidecar memory budget", "bounds");
    }
  }

  #drain(): void {
    if (this.#closed) return;
    this.refresh();
    for (;;) {
      const waiter = this.#waiters[0];
      if (!waiter) return;
      if (waiter.signal?.aborted) {
        this.#waiters.shift();
        waiter.reject(abortError());
        continue;
      }
      if (
        this.#active >= this.#effectiveLimit() ||
        this.#reservedBytes + waiter.reservedBytes > this.#memoryBudgetBytes
      ) return;
      this.#waiters.shift();
      waiter.signal?.removeEventListener("abort", waiter.onAbort!);
      const lease = this.tryAcquire(waiter.reservedBytes);
      if (!lease) {
        this.#waiters.unshift(waiter);
        return;
      }
      waiter.resolve(lease);
    }
  }
}
