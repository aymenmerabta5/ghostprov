/** One active verification/handler and one pending, size-validated control message. */
export class SupervisorMailbox<T extends { readonly sequence: number }> {
  readonly #handle: (value: T) => Promise<void>;
  #pending: T | null = null;
  #active: Promise<void> | null = null;
  #closed = false;

  constructor(handle: (value: T) => Promise<void>) {
    this.#handle = handle;
  }

  offer(value: T): void {
    if (this.#closed || (this.#pending && this.#pending.sequence > value.sequence)) return;
    this.#pending = value;
    if (!this.#active) this.#start();
  }

  snapshot(): { active: number; queued: number } {
    return { active: this.#active ? 1 : 0, queued: this.#pending ? 1 : 0 };
  }

  async close(): Promise<void> {
    this.#closed = true;
    this.#pending = null;
    await this.#active;
  }

  #start(): void {
    const active = Promise.resolve().then(async () => {
      while (!this.#closed && this.#pending) {
        const value = this.#pending;
        this.#pending = null;
        try { await this.#handle(value); } catch { /* Invalid messages do not poison later verification. */ }
      }
    }).finally(() => {
      if (this.#active === active) this.#active = null;
      if (!this.#closed && this.#pending) this.#start();
    });
    this.#active = active;
  }
}
