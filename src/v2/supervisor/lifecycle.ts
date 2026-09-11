import type { V2BootstrapSupervisor } from "./host";

type SupervisorCleanupOwner = Pick<V2BootstrapSupervisor, "shutdown" | "close">;

/** Retains bounded supervisor cleanup ownership beyond a runtime's stop waiter. */
export class SupervisorLifecycle {
  readonly #owners = new Set<SupervisorCleanupOwner>();
  readonly #limit: number;
  #closing = false;
  #shutdown: Promise<void> | null = null;

  constructor(limit: number) {
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 256) throw new RangeError("Supervisor lifecycle limit is invalid");
    this.#limit = limit;
  }

  assertAvailable(): void {
    if (this.#closing) throw new Error("Supervisor lifecycle is closing");
    if (this.#owners.size >= this.#limit) throw new Error("Supervisor cleanup capacity is exhausted");
  }

  register(owner: SupervisorCleanupOwner): () => void {
    this.assertAvailable();
    if (this.#owners.has(owner)) throw new Error("Supervisor cleanup owner is already registered");
    this.#owners.add(owner);
    return () => { this.#owners.delete(owner); };
  }

  beginShutdown(): void { this.#closing = true; }

  shutdown(signal: AbortSignal): Promise<void> {
    if (this.#shutdown) return this.#shutdown;
    this.beginShutdown();
    if (this.#owners.size === 0) return this.#shutdown = Promise.resolve();
    const cleanup = Promise.allSettled([...this.#owners].map(async owner => {
      try { await owner.shutdown("system_shutdown", signal); }
      finally { await owner.close(); }
    })).then(results => {
      const failures = results.flatMap(result => result.status === "rejected" ? [result.reason] : []);
      if (failures.length) throw new AggregateError(failures, "Supervisor cleanup was incomplete");
    });
    this.#shutdown = new Promise<void>((resolve, reject) => {
      const abort = () => reject(signal.reason ?? new Error("Supervisor cleanup deadline expired"));
      signal.addEventListener("abort", abort, { once: true });
      if (signal.aborted) abort();
      cleanup.then(resolve, reject).finally(() => signal.removeEventListener("abort", abort));
    });
    return this.#shutdown;
  }
}
