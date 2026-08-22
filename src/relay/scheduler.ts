import type { RunHandle } from "./run-handle";

export interface SchedulerStats {
  capacity: number;
  active: number;
  queued: number;
  completed: number;
  failed: number;
  cancelled: number;
}

interface Waiter {
  resolve: (lease: SchedulerLease) => void;
  reject: (error: unknown) => void;
  signal?: AbortSignal;
  onAbort?: () => void;
}

class SchedulerLease {
  private released = false;

  constructor(private readonly owner: ResponseScheduler) {}

  complete(): void {
    if (this.released) return;
    this.released = true;
    this.owner.release("completed");
  }

  fail(): void {
    if (this.released) return;
    this.released = true;
    this.owner.release("failed");
  }
}

function abortError(): DOMException {
  return new DOMException("Response request aborted while queued", "AbortError");
}

/** FIFO admission gate whose leases are explicitly held through final output. */
export class ResponseScheduler {
  readonly capacity: number;
  private active = 0;
  private readonly waiters: Waiter[] = [];
  private completed = 0;
  private failed = 0;
  private cancelled = 0;

  constructor(capacity: number) {
    if (!Number.isInteger(capacity) || capacity < 1) throw new RangeError("scheduler capacity must be a positive integer");
    this.capacity = capacity;
  }

  acquire(signal?: AbortSignal): Promise<SchedulerLease> {
    if (signal?.aborted) {
      this.cancelled++;
      return Promise.reject(abortError());
    }
    return new Promise((resolve, reject) => {
      const waiter: Waiter = { resolve, reject, signal };
      if (signal) {
        waiter.onAbort = () => {
          const index = this.waiters.indexOf(waiter);
          if (index === -1) return;
          this.waiters.splice(index, 1);
          this.cancelled++;
          reject(abortError());
        };
        signal.addEventListener("abort", waiter.onAbort, { once: true });
      }
      this.waiters.push(waiter);
      this.drain();
    });
  }

  stats(): SchedulerStats {
    return {
      capacity: this.capacity,
      active: this.active,
      queued: this.waiters.length,
      completed: this.completed,
      failed: this.failed,
      cancelled: this.cancelled,
    };
  }

  release(outcome: "completed" | "failed"): void {
    if (this.active <= 0) throw new Error("scheduler lease released without an active operation");
    this.active--;
    if (outcome === "completed") this.completed++;
    else this.failed++;
    this.drain();
  }

  private drain(): void {
    while (this.active < this.capacity && this.waiters.length) {
      const waiter = this.waiters.shift()!;
      if (waiter.onAbort) waiter.signal?.removeEventListener("abort", waiter.onAbort);
      if (waiter.signal?.aborted) {
        this.cancelled++;
        waiter.reject(abortError());
        continue;
      }
      this.active++;
      waiter.resolve(new SchedulerLease(this));
    }
  }
}

export type RelayStarter = (body: Record<string, unknown>) => Promise<RunHandle>;

/** Acquire capacity, start a relay, and retain the lease until final settles. */
export async function scheduleRelay(
  scheduler: ResponseScheduler,
  relay: RelayStarter,
  body: Record<string, unknown>,
  options?: { signal?: AbortSignal },
): Promise<RunHandle> {
  const lease = await scheduler.acquire(options?.signal);
  let handle: RunHandle;
  try {
    handle = await relay(body);
  } catch (error) {
    lease.fail();
    throw error;
  }
  void handle.final.then(
    () => lease.complete(),
    () => lease.fail(),
  );
  return handle;
}
