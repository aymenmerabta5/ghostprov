import { slotId, type SlotId } from "./types";

interface SlotState {
  id: SlotId;
  busy: boolean;
  quarantined: boolean;
}

interface Waiter {
  resolve: (lease: SlotLease) => void;
  reject: (error: unknown) => void;
  signal?: AbortSignal;
  onAbort?: () => void;
}

export interface SlotPoolSnapshot {
  capacity: number;
  active: number;
  available: number;
  quarantined: number;
  queued: number;
}

function abortError(): DOMException {
  return new DOMException("Slot acquisition aborted", "AbortError");
}

export class SlotLease {
  readonly slot: SlotId;
  readonly #pool: SlotPool;
  #released = false;

  constructor(pool: SlotPool, slot: SlotId) {
    this.#pool = pool;
    this.slot = slot;
  }

  quarantine(): void {
    this.#pool.quarantine(this.slot);
  }

  recover(): void {
    this.#pool.recover(this.slot);
  }

  release(): void {
    if (this.#released) return;
    this.#released = true;
    this.#pool.release(this.slot);
  }
}

export class SlotPool {
  readonly #slots: SlotState[];
  readonly #waiters: Waiter[] = [];

  constructor(capacity: number) {
    if (!Number.isInteger(capacity) || capacity < 1 || capacity > 4096) {
      throw new RangeError("SlotPool capacity must be between 1 and 4096");
    }
    this.#slots = Array.from({ length: capacity }, (_, index) => ({
      id: slotId(index + 1),
      busy: false,
      quarantined: false,
    }));
  }

  acquire(signal?: AbortSignal): Promise<SlotLease> {
    if (signal?.aborted) return Promise.reject(abortError());
    const state = this.#slots.find(value => !value.busy && !value.quarantined);
    if (state) {
      state.busy = true;
      return Promise.resolve(new SlotLease(this, state.id));
    }
    return new Promise((resolve, reject) => {
      const waiter: Waiter = { resolve, reject, signal };
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

  quarantine(id: SlotId): void {
    this.#state(id).quarantined = true;
  }

  recover(id: SlotId): void {
    this.#state(id).quarantined = false;
    this.#drain();
  }

  release(id: SlotId): void {
    const state = this.#state(id);
    if (!state.busy) return;
    state.busy = false;
    this.#drain();
  }

  snapshot(): SlotPoolSnapshot {
    return {
      capacity: this.#slots.length,
      active: this.#slots.filter(value => value.busy).length,
      available: this.#slots.filter(value => !value.busy && !value.quarantined).length,
      quarantined: this.#slots.filter(value => value.quarantined).length,
      queued: this.#waiters.length,
    };
  }

  #state(id: SlotId): SlotState {
    const state = this.#slots[id - 1];
    if (!state || state.id !== id) throw new RangeError(`Unknown slot ${id}`);
    return state;
  }

  #drain(): void {
    while (this.#waiters.length) {
      const state = this.#slots.find(value => !value.busy && !value.quarantined);
      if (!state) return;
      const waiter = this.#waiters.shift()!;
      if (waiter.signal?.aborted) {
        waiter.reject(abortError());
        continue;
      }
      waiter.signal?.removeEventListener("abort", waiter.onAbort!);
      state.busy = true;
      waiter.resolve(new SlotLease(this, state.id));
    }
  }
}
