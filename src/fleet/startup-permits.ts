import { RecoveryPermitPool } from "../sandbox/supervised-runtime";

export interface FleetStartupPermitSnapshot {
  readonly total: ReturnType<RecoveryPermitPool["snapshot"]>;
  readonly cold: ReturnType<RecoveryPermitPool["snapshot"]>;
}

export interface FleetStartupPermitTransition {
  readonly action: "acquired" | "released";
  readonly coldStart: boolean;
  readonly snapshot: FleetStartupPermitSnapshot;
}

/**
 * Bounds expensive first-ever Y-Sweet/bootstrap work more tightly than normal
 * recovery. Cold admission is acquired first so cold waiters never occupy all
 * general recovery slots while previously healthy projects need one.
 */
export class FleetStartupPermitPool {
  readonly #total: RecoveryPermitPool;
  readonly #cold: RecoveryPermitPool;
  readonly #onTransition?: (transition: FleetStartupPermitTransition) => void;

  constructor(
    totalLimit: number,
    coldLimit: number,
    options: { onTransition?: (transition: FleetStartupPermitTransition) => void } = {},
  ) {
    if (!Number.isSafeInteger(totalLimit) || totalLimit < 1) {
      throw new RangeError("totalLimit must be a positive safe integer");
    }
    if (!Number.isSafeInteger(coldLimit) || coldLimit < 1 || coldLimit > totalLimit) {
      throw new RangeError("coldLimit must be a positive safe integer no greater than totalLimit");
    }
    this.#total = new RecoveryPermitPool(totalLimit);
    this.#cold = new RecoveryPermitPool(coldLimit);
    this.#onTransition = options.onTransition;
  }

  async acquire(cold: boolean, signal?: AbortSignal): Promise<() => void> {
    let releaseCold: (() => void) | null = null;
    let releaseTotal: (() => void) | null = null;
    try {
      if (cold) releaseCold = await this.#cold.acquire(signal);
      releaseTotal = await this.#total.acquire(signal);
    } catch (error) {
      releaseTotal?.();
      releaseCold?.();
      throw error;
    }
    this.#emit("acquired", cold);
    let released = false;
    return () => {
      if (released) return;
      released = true;
      releaseTotal?.();
      releaseCold?.();
      this.#emit("released", cold);
    };
  }

  snapshot(): FleetStartupPermitSnapshot {
    return Object.freeze({
      total: Object.freeze(this.#total.snapshot()),
      cold: Object.freeze(this.#cold.snapshot()),
    });
  }

  #emit(action: FleetStartupPermitTransition["action"], coldStart: boolean): void {
    if (!this.#onTransition) return;
    try {
      this.#onTransition(Object.freeze({
        action,
        coldStart,
        snapshot: this.snapshot(),
      }));
    } catch {}
  }
}
