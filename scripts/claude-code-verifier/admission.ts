import { ApiError } from "../../src/http/errors";
import {
  abortError,
  positiveSafeInteger,
  verifierClosingError,
} from "./shared";

interface ForwardWaiter {
  resolve: (lease: { release(): void }) => void;
  reject: (error: unknown) => void;
  signal?: AbortSignal;
  onAbort?: () => void;
}

export class ForwardSemaphore {
  readonly #capacity: number;
  readonly #waiters: ForwardWaiter[] = [];
  #active = 0;
  #closed = false;

  constructor(capacity: number) {
    this.#capacity = positiveSafeInteger(capacity, "forward capacity");
  }

  acquire(signal?: AbortSignal): Promise<{ release(): void }> {
    if (signal?.aborted) return Promise.reject(abortError());
    if (this.#closed) return Promise.reject(verifierClosingError());
    if (this.#active < this.#capacity && this.#waiters.length === 0) {
      return Promise.resolve(this.#grant());
    }
    return new Promise((resolve, reject) => {
      const waiter: ForwardWaiter = { resolve, reject, signal };
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

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    for (const waiter of this.#waiters.splice(0)) {
      waiter.signal?.removeEventListener("abort", waiter.onAbort!);
      waiter.reject(verifierClosingError());
    }
  }

  snapshot(): { active: number; queued: number } {
    return { active: this.#active, queued: this.#waiters.length };
  }

  #grant(): { release(): void } {
    this.#active += 1;
    let released = false;
    return {
      release: () => {
        if (released) return;
        released = true;
        this.#active = Math.max(0, this.#active - 1);
        this.#drain();
      },
    };
  }

  #drain(): void {
    if (this.#closed) return;
    while (this.#active < this.#capacity) {
      const waiter = this.#waiters.shift();
      if (!waiter) return;
      if (waiter.signal?.aborted) {
        waiter.reject(abortError());
        continue;
      }
      waiter.signal?.removeEventListener("abort", waiter.onAbort!);
      waiter.resolve(this.#grant());
    }
  }
}

interface BodyWaiter {
  bytes: number;
  resolve: (lease: { release(): void }) => void;
  reject: (error: unknown) => void;
  signal?: AbortSignal;
  onAbort?: () => void;
}

export class IncomingBodyBudget {
  readonly #capacityBytes: number;
  readonly #waiters: BodyWaiter[] = [];
  #reservedBytes = 0;
  #closed = false;

  constructor(capacityBytes: number) {
    this.#capacityBytes = positiveSafeInteger(capacityBytes, "incoming body budget");
  }

  acquire(bytes: number, signal?: AbortSignal): Promise<{ release(): void }> {
    if (!Number.isSafeInteger(bytes) || bytes < 0) {
      return Promise.reject(new ApiError(400, "Incoming body reservation is invalid", {
        type: "invalid_request_error",
        code: "invalid_body_reservation",
      }));
    }
    if (bytes > this.#capacityBytes) {
      return Promise.reject(new ApiError(413, "Incoming body exceeds verifier memory budget", {
        type: "invalid_request_error",
        code: "request_too_large",
      }));
    }
    if (signal?.aborted) return Promise.reject(abortError());
    if (this.#closed) return Promise.reject(verifierClosingError());
    if (bytes === 0 || (this.#waiters.length === 0 && this.#fits(bytes))) {
      return Promise.resolve(this.#grant(bytes));
    }
    return new Promise((resolve, reject) => {
      const waiter: BodyWaiter = { bytes, resolve, reject, signal };
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

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    for (const waiter of this.#waiters.splice(0)) {
      waiter.signal?.removeEventListener("abort", waiter.onAbort!);
      waiter.reject(verifierClosingError());
    }
  }

  snapshot(): { capacityBytes: number; reservedBytes: number; queued: number } {
    return {
      capacityBytes: this.#capacityBytes,
      reservedBytes: this.#reservedBytes,
      queued: this.#waiters.length,
    };
  }

  #fits(bytes: number): boolean {
    return this.#reservedBytes + bytes <= this.#capacityBytes;
  }

  #grant(bytes: number): { release(): void } {
    this.#reservedBytes += bytes;
    let released = false;
    return {
      release: () => {
        if (released) return;
        released = true;
        this.#reservedBytes = Math.max(0, this.#reservedBytes - bytes);
        this.#drain();
      },
    };
  }

  #drain(): void {
    if (this.#closed) return;
    for (;;) {
      const waiter = this.#waiters[0];
      if (!waiter) return;
      if (waiter.signal?.aborted) {
        this.#waiters.shift();
        waiter.reject(abortError());
        continue;
      }
      if (!this.#fits(waiter.bytes)) return;
      this.#waiters.shift();
      waiter.signal?.removeEventListener("abort", waiter.onAbort!);
      waiter.resolve(this.#grant(waiter.bytes));
    }
  }
}
