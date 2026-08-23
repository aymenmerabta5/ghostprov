export type PublicationReason = "initial" | "paced" | "urgent" | "retransmission";

export type PublicationTimerHandle = number | object;

export interface PublicationTimer {
  now(): number;
  set(callback: () => void, delayMs: number): PublicationTimerHandle;
  clear(handle: PublicationTimerHandle): void;
  defer(callback: () => void): void;
}

const SYSTEM_PUBLICATION_TIMER: PublicationTimer = {
  now: () => performance.now(),
  set(callback, delayMs) {
    const handle = setTimeout(callback, delayMs);
    handle.unref?.();
    return handle;
  },
  clear: handle => clearTimeout(handle as ReturnType<typeof setTimeout>),
  defer: callback => queueMicrotask(callback),
};

export interface PublicationPacerOptions {
  intervalMs?: number;
  timer?: PublicationTimer;
  publish(reason: PublicationReason): void;
}

/**
 * Coalesces cumulative-state publications across every logical stream owned by
 * one transport peer. Data observes a maximum publication rate while ACK and
 * control state preempt the cadence once per microtask.
 */
export class PublicationPacer {
  readonly #intervalMs: number;
  readonly #timer: PublicationTimer;
  readonly #publish: (reason: PublicationReason) => void;
  #cadence: PublicationTimerHandle | null = null;
  #lastPublishedAt: number | null = null;
  #urgentScheduled = false;
  #generation = 0;
  #dirty = false;
  #closed = false;

  constructor(options: PublicationPacerOptions) {
    const intervalMs = options.intervalMs ?? 25;
    if (!Number.isFinite(intervalMs) || intervalMs < 1) {
      throw new RangeError("publication interval must be positive");
    }
    this.#intervalMs = intervalMs;
    this.#timer = options.timer ?? SYSTEM_PUBLICATION_TIMER;
    this.#publish = options.publish;
  }

  requestPaced(): void {
    if (this.#closed) return;
    this.#dirty = true;
    if (this.#cadence !== null || this.#urgentScheduled) return;
    const now = this.#timer.now();
    const deadline = (this.#lastPublishedAt ?? now) + this.#intervalMs;
    const handle = this.#timer.set(() => {
      if (this.#cadence !== handle) return;
      this.#cadence = null;
      if (!this.#closed && this.#dirty) this.#emit("paced");
    }, Math.max(0, deadline - now));
    this.#cadence = handle;
  }

  requestUrgent(): void {
    if (this.#closed) return;
    this.#dirty = true;
    if (this.#cadence !== null) {
      this.#timer.clear(this.#cadence);
      this.#cadence = null;
    }
    if (this.#urgentScheduled) return;
    this.#urgentScheduled = true;
    const generation = ++this.#generation;
    this.#timer.defer(() => {
      if (generation !== this.#generation) return;
      this.#urgentScheduled = false;
      if (!this.#closed && this.#dirty) this.#emit("urgent");
    });
  }

  flushNow(reason: PublicationReason): void {
    if (this.#closed) return;
    this.#cancelPending();
    this.#emit(reason);
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#cancelPending();
  }

  #cancelPending(): void {
    if (this.#cadence !== null) this.#timer.clear(this.#cadence);
    this.#cadence = null;
    this.#dirty = false;
    this.#urgentScheduled = false;
    this.#generation++;
  }

  #emit(reason: PublicationReason): void {
    this.#dirty = false;
    this.#lastPublishedAt = this.#timer.now();
    this.#publish(reason);
  }
}
