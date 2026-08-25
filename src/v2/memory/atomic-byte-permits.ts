/**
 * A runtime-neutral byte allocator for memory that must remain atomic while it
 * moves between pipeline stages.  The ordinary region is shared.  The jumbo
 * region is a single, deliberately reserved lane so an in-progress object can
 * leave the ordinary region before waiting for its remaining bytes.  That
 * transfer is what prevents N partially-built objects from deadlocking while
 * all of them try to grow.
 *
 * This module intentionally uses only web/ECMAScript APIs shared by Bun and
 * Deno.  It is bundled into the sidecar release.
 */

export type AtomicBytePermitRegion = "ordinary" | "jumbo";

export const ATOMIC_EVENT_MEMORY_MULTIPLIER = 4;
/** Parser/hash/segment scratch, string metadata, and bounded overlap margin. */
export const ATOMIC_EVENT_FIXED_OVERHEAD_BYTES = 1024 * 1024;

/** Shared checked estimator for host and sidecar governor wiring. */
export function estimateAtomicEventPermitBytes(logicalBytes: number): number {
  if (!Number.isSafeInteger(logicalBytes) || logicalBytes < 0) {
    throw new RangeError("logicalBytes must be a non-negative safe integer");
  }
  if (logicalBytes === 0) return 0;
  const maximum = Math.floor(
    (Number.MAX_SAFE_INTEGER - ATOMIC_EVENT_FIXED_OVERHEAD_BYTES) /
      ATOMIC_EVENT_MEMORY_MULTIPLIER,
  );
  if (logicalBytes > maximum) {
    throw new RangeError("Atomic event memory estimate exceeds the safe integer range");
  }
  return logicalBytes * ATOMIC_EVENT_MEMORY_MULTIPLIER +
    ATOMIC_EVENT_FIXED_OVERHEAD_BYTES;
}

export interface AtomicBytePermitPoolOptions {
  ordinaryBytes: number;
  /** Capacity of the one reserved jumbo lane. Set to zero to disable it. */
  jumboBytes: number;
  /** Total queued acquisitions and resizes. */
  maxWaiters?: number;
}

export interface AtomicBytePermitAcquireOptions {
  signal?: AbortSignal;
  /** `auto` uses ordinary space when the request can fit there. */
  region?: AtomicBytePermitRegion | "auto";
  label?: string;
}

export interface AtomicBytePermitResizeOptions {
  signal?: AbortSignal;
  /**
   * When ordinary growth cannot complete immediately, atomically move to the
   * reserved lane instead of waiting while retaining ordinary bytes.
   */
  allowJumbo?: boolean;
}

export type AtomicBytePermitTryAcquireOptions = Omit<
  AtomicBytePermitAcquireOptions,
  "signal"
>;

export interface AtomicBytePermitPoolSnapshot {
  ordinaryCapacityBytes: number;
  ordinaryUsedBytes: number;
  ordinaryAvailableBytes: number;
  jumboCapacityBytes: number;
  jumboUsedBytes: number;
  jumboAvailableBytes: number;
  jumboActive: boolean;
  activePermits: number;
  waiters: number;
  ordinaryWaiters: number;
  jumboWaiters: number;
  promotionWaiters: number;
  closed: boolean;
}

export class AtomicBytePermitError extends Error {
  readonly name = "AtomicBytePermitError";

  constructor(
    message: string,
    readonly code: "capacity" | "closed" | "released" | "waiters" | "pending",
  ) {
    super(message);
  }
}

interface Allocation {
  readonly id: number;
  bytes: number;
  region: AtomicBytePermitRegion;
  label: string | null;
  token: object;
  released: boolean;
  pending: Waiter | null;
}

interface Waiter {
  readonly kind: "acquire" | "resize" | "promote";
  readonly region: AtomicBytePermitRegion;
  readonly bytes: number;
  readonly allocation: Allocation | null;
  readonly label: string | null;
  readonly signal?: AbortSignal;
  readonly resolve: (permit: AtomicBytePermit) => void;
  readonly reject: (error: unknown) => void;
  cancelled: boolean;
  detachAbort?: () => void;
}

const DEFAULT_MAX_WAITERS = 4_096;

function bytes(value: number, name: string, allowZero = false): number {
  if (!Number.isSafeInteger(value) || value < (allowZero ? 0 : 1)) {
    throw new RangeError(`${name} must be ${allowZero ? "a non-negative" : "a positive"} safe integer`);
  }
  return value;
}

function abortReason(signal: AbortSignal): unknown {
  return signal.reason ?? new DOMException("The operation was aborted", "AbortError");
}

function safeLabel(value: string | undefined): string | null {
  if (value === undefined || value.length === 0) return null;
  return value.slice(0, 128);
}

/** One ownership handle. `transfer()` invalidates only the old handle. */
export class AtomicBytePermit {
  readonly #pool: AtomicBytePermitPool;
  readonly #allocation: Allocation;
  readonly #token: object;
  #inactive = false;
  #operationInFlight = false;

  /** @internal Constructed only by AtomicBytePermitPool. */
  constructor(pool: AtomicBytePermitPool, allocation: Allocation, token: object) {
    this.#pool = pool;
    this.#allocation = allocation;
    this.#token = token;
  }

  get bytes(): number {
    return this.#ownsAllocation() ? this.#allocation.bytes : 0;
  }

  get region(): AtomicBytePermitRegion {
    this.#assertOwned();
    return this.#allocation.region;
  }

  get label(): string | null {
    return this.#ownsAllocation() ? this.#allocation.label : null;
  }

  get active(): boolean {
    return this.#ownsAllocation();
  }

  /** Grow or shrink without ever exposing an uncharged intermediate state. */
  async resize(
    targetBytes: number,
    options: AtomicBytePermitResizeOptions = {},
  ): Promise<this> {
    this.#assertOwned();
    this.#beginOperation();
    try {
      await this.#pool.resize(this.#allocation, targetBytes, options);
    } finally {
      this.#operationInFlight = false;
    }
    this.#assertOwned();
    return this;
  }

  /** Immediate resize; false guarantees no allocation/queue mutation. */
  tryResize(
    targetBytes: number,
    options: Omit<AtomicBytePermitResizeOptions, "signal"> = {},
  ): boolean {
    this.#assertOwned();
    this.#beginOperation();
    try {
      return this.#pool.tryResize(this.#allocation, targetBytes, options);
    } finally {
      this.#operationInFlight = false;
    }
  }

  /** Force an atomic ordinary -> reserved-lane promotion. */
  async promote(targetBytes = this.bytes, signal?: AbortSignal): Promise<this> {
    this.#assertOwned();
    this.#beginOperation();
    try {
      await this.#pool.promote(this.#allocation, targetBytes, signal);
    } finally {
      this.#operationInFlight = false;
    }
    this.#assertOwned();
    return this;
  }

  /** Immediate reserved-lane promotion; false leaves the permit untouched. */
  tryPromote(targetBytes = this.bytes): boolean {
    this.#assertOwned();
    this.#beginOperation();
    try {
      return this.#pool.tryPromote(this.#allocation, targetBytes);
    } finally {
      this.#operationInFlight = false;
    }
  }

  /** Synchronous shrink. A target of zero is equivalent to release. */
  shrink(targetBytes: number): this {
    this.#assertOwned();
    if (this.#operationInFlight) {
      throw new AtomicBytePermitError("Cannot shrink a permit during an in-flight operation", "pending");
    }
    this.#pool.shrink(this.#allocation, targetBytes);
    if (targetBytes === 0) this.#inactive = true;
    return this;
  }

  /**
   * Atomically hand the existing allocation to a new stage/owner. The returned
   * handle is the only handle able to mutate or release it. Calling release on
   * this old handle afterwards is intentionally an idempotent no-op.
   */
  transfer(label?: string): AtomicBytePermit {
    this.#assertOwned();
    if (this.#operationInFlight) {
      throw new AtomicBytePermitError("Cannot transfer a permit during an in-flight operation", "pending");
    }
    if (this.#allocation.pending) {
      throw new AtomicBytePermitError("Cannot transfer a permit with a pending resize", "pending");
    }
    const token = {};
    this.#allocation.token = token;
    this.#allocation.label = safeLabel(label);
    this.#inactive = true;
    return new AtomicBytePermit(this.#pool, this.#allocation, token);
  }

  /** Idempotent. A transferred/released handle cannot release its successor. */
  release(): void {
    if (!this.#ownsAllocation()) {
      this.#inactive = true;
      return;
    }
    this.#inactive = true;
    this.#pool.release(this.#allocation);
  }

  #ownsAllocation(): boolean {
    return !this.#inactive &&
      !this.#allocation.released &&
      this.#allocation.token === this.#token;
  }

  #assertOwned(): void {
    if (!this.#ownsAllocation()) {
      throw new AtomicBytePermitError("Byte permit is released or transferred", "released");
    }
  }

  #beginOperation(): void {
    if (this.#operationInFlight) {
      throw new AtomicBytePermitError("Byte permit already has an in-flight operation", "pending");
    }
    this.#operationInFlight = true;
  }
}

/**
 * Bounded byte permits with FIFO ordering inside each independent region.
 * Promotions share the jumbo FIFO with direct jumbo acquisitions. Keeping the
 * ordinary and jumbo queues independent is essential: an ordinary acquisition
 * cannot sit in front of a promotion whose completion would free ordinary
 * bytes.
 */
export class AtomicBytePermitPool {
  readonly #ordinaryCapacity: number;
  readonly #jumboCapacity: number;
  readonly #maxWaiters: number;
  readonly #ordinaryQueue: Waiter[] = [];
  readonly #jumboQueue: Waiter[] = [];
  readonly #allocations = new Map<number, Allocation>();
  #ordinaryHead = 0;
  #jumboHead = 0;
  #ordinaryUsed = 0;
  #jumboUsed = 0;
  #nextId = 1;
  #waiterCount = 0;
  #closed = false;
  #closeReason: unknown = null;

  constructor(options: AtomicBytePermitPoolOptions) {
    this.#ordinaryCapacity = bytes(options.ordinaryBytes, "ordinaryBytes", true);
    this.#jumboCapacity = bytes(options.jumboBytes, "jumboBytes", true);
    if (this.#ordinaryCapacity === 0 && this.#jumboCapacity === 0) {
      throw new RangeError("At least one byte-permit region must have capacity");
    }
    this.#maxWaiters = bytes(options.maxWaiters ?? DEFAULT_MAX_WAITERS, "maxWaiters");
  }

  async acquire(
    requestedBytes: number,
    options: AtomicBytePermitAcquireOptions = {},
  ): Promise<AtomicBytePermit> {
    requestedBytes = bytes(requestedBytes, "requestedBytes");
    this.#ensureOpen();
    if (options.signal?.aborted) return Promise.reject(abortReason(options.signal));
    const requestedRegion = options.region ?? "auto";
    const region = requestedRegion === "auto"
      ? requestedBytes <= this.#ordinaryCapacity ? "ordinary" : "jumbo"
      : requestedRegion;
    this.#ensureFits(region, requestedBytes);
    if (this.#canAcquireImmediately(region, requestedBytes)) {
      return this.#allocate(region, requestedBytes, safeLabel(options.label));
    }
    return this.#enqueue({
      kind: "acquire",
      region,
      bytes: requestedBytes,
      allocation: null,
      label: safeLabel(options.label),
      signal: options.signal,
    });
  }

  acquireJumbo(
    requestedBytes: number,
    options: Omit<AtomicBytePermitAcquireOptions, "region"> = {},
  ): Promise<AtomicBytePermit> {
    return this.acquire(requestedBytes, { ...options, region: "jumbo" });
  }

  /** Immediate FIFO-preserving acquisition. Null never queues or mutates. */
  tryAcquire(
    requestedBytes: number,
    options: AtomicBytePermitTryAcquireOptions = {},
  ): AtomicBytePermit | null {
    requestedBytes = bytes(requestedBytes, "requestedBytes");
    this.#ensureOpen();
    const requestedRegion = options.region ?? "auto";
    const region = requestedRegion === "auto"
      ? requestedBytes <= this.#ordinaryCapacity ? "ordinary" : "jumbo"
      : requestedRegion;
    this.#ensureFits(region, requestedBytes);
    if (!this.#canAcquireImmediately(region, requestedBytes)) return null;
    return this.#allocate(region, requestedBytes, safeLabel(options.label));
  }

  snapshot(): AtomicBytePermitPoolSnapshot {
    let ordinaryWaiters = 0;
    let jumboWaiters = 0;
    let promotionWaiters = 0;
    for (let index = this.#ordinaryHead; index < this.#ordinaryQueue.length; index++) {
      const waiter = this.#ordinaryQueue[index]!;
      if (!waiter.cancelled) ordinaryWaiters++;
    }
    for (let index = this.#jumboHead; index < this.#jumboQueue.length; index++) {
      const waiter = this.#jumboQueue[index]!;
      if (waiter.cancelled) continue;
      jumboWaiters++;
      if (waiter.kind === "promote") promotionWaiters++;
    }
    return {
      ordinaryCapacityBytes: this.#ordinaryCapacity,
      ordinaryUsedBytes: this.#ordinaryUsed,
      ordinaryAvailableBytes: this.#ordinaryCapacity - this.#ordinaryUsed,
      jumboCapacityBytes: this.#jumboCapacity,
      jumboUsedBytes: this.#jumboUsed,
      jumboAvailableBytes: this.#jumboCapacity - this.#jumboUsed,
      jumboActive: this.#jumboUsed > 0,
      activePermits: this.#allocations.size,
      waiters: ordinaryWaiters + jumboWaiters,
      ordinaryWaiters,
      jumboWaiters,
      promotionWaiters,
      closed: this.#closed,
    };
  }

  /** Reject all queued work. Existing allocations remain valid until release. */
  close(reason: unknown = new AtomicBytePermitError("Byte permit pool is closed", "closed")): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#closeReason = reason;
    for (const queue of [this.#ordinaryQueue, this.#jumboQueue]) {
      for (const waiter of queue) this.#rejectWaiter(waiter, reason);
    }
    this.#compactQueues(true);
  }

  /** @internal */
  async resize(
    allocation: Allocation,
    targetBytes: number,
    options: AtomicBytePermitResizeOptions,
  ): Promise<void> {
    this.#assertAllocation(allocation);
    targetBytes = bytes(targetBytes, "targetBytes");
    if (targetBytes === allocation.bytes) return;
    if (targetBytes < allocation.bytes) {
      this.shrink(allocation, targetBytes);
      return;
    }
    this.#ensureOpen();
    if (options.signal?.aborted) throw abortReason(options.signal);
    if (allocation.pending) {
      throw new AtomicBytePermitError("Byte permit already has a pending resize", "pending");
    }
    if (allocation.region === "jumbo") {
      this.#ensureFits("jumbo", targetBytes);
      // The sole jumbo allocation owns the lane, so growing within its fixed
      // capacity never waits on another allocation.
      this.#jumboUsed += targetBytes - allocation.bytes;
      allocation.bytes = targetBytes;
      return;
    }

    this.#ensureFitsForResize(targetBytes, options.allowJumbo !== false);
    const delta = targetBytes - allocation.bytes;
    if (
      !this.#ordinaryQueueHasLiveWaiter() &&
      this.#ordinaryUsed + delta <= this.#ordinaryCapacity
    ) {
      this.#ordinaryUsed += delta;
      allocation.bytes = targetBytes;
      return;
    }

    // Never queue a grow while retaining ordinary bytes when the reserved lane
    // can hold the finished object. That is the all-partial-grow deadlock.
    if (options.allowJumbo !== false && targetBytes <= this.#jumboCapacity) {
      await this.#enqueue({
        kind: "promote",
        region: "jumbo",
        bytes: targetBytes,
        allocation,
        label: allocation.label,
        signal: options.signal,
      });
      return;
    }

    // Waiting in the ordinary acquisition FIFO while retaining ordinary bytes
    // can self-deadlock with the waiter in front of us. If the reserved lane
    // cannot accept this object, fail atomically and leave the old charge
    // untouched; callers may shed work instead of hanging the pool.
    throw new AtomicBytePermitError(
      "Byte permit cannot grow atomically without the reserved jumbo lane",
      "capacity",
    );
  }

  /** @internal */
  tryResize(
    allocation: Allocation,
    targetBytes: number,
    options: Omit<AtomicBytePermitResizeOptions, "signal">,
  ): boolean {
    this.#assertAllocation(allocation);
    targetBytes = bytes(targetBytes, "targetBytes");
    if (allocation.pending) {
      throw new AtomicBytePermitError("Byte permit already has a pending resize", "pending");
    }
    if (targetBytes === allocation.bytes) return true;
    if (targetBytes < allocation.bytes) {
      this.shrink(allocation, targetBytes);
      return true;
    }
    this.#ensureOpen();
    if (allocation.region === "jumbo") {
      this.#ensureFits("jumbo", targetBytes);
      this.#jumboUsed += targetBytes - allocation.bytes;
      allocation.bytes = targetBytes;
      return true;
    }
    this.#ensureFitsForResize(targetBytes, options.allowJumbo !== false);
    const delta = targetBytes - allocation.bytes;
    if (
      !this.#ordinaryQueueHasLiveWaiter() &&
      this.#ordinaryUsed + delta <= this.#ordinaryCapacity
    ) {
      this.#ordinaryUsed += delta;
      allocation.bytes = targetBytes;
      return true;
    }
    if (
      options.allowJumbo !== false &&
      targetBytes <= this.#jumboCapacity &&
      this.#jumboUsed === 0 &&
      !this.#jumboQueueHasLiveWaiter()
    ) {
      this.#ordinaryUsed -= allocation.bytes;
      allocation.region = "jumbo";
      allocation.bytes = targetBytes;
      this.#jumboUsed = targetBytes;
      this.#drainOrdinary();
      this.#compactQueues(false);
      return true;
    }
    return false;
  }

  /** @internal */
  tryPromote(allocation: Allocation, targetBytes: number): boolean {
    this.#assertAllocation(allocation);
    targetBytes = bytes(targetBytes, "targetBytes");
    if (allocation.pending) {
      throw new AtomicBytePermitError("Byte permit already has a pending resize", "pending");
    }
    if (allocation.region === "jumbo") {
      return this.tryResize(allocation, targetBytes, {});
    }
    this.#ensureOpen();
    this.#ensureFits("jumbo", targetBytes);
    if (this.#jumboUsed !== 0 || this.#jumboQueueHasLiveWaiter()) return false;
    this.#ordinaryUsed -= allocation.bytes;
    allocation.region = "jumbo";
    allocation.bytes = targetBytes;
    this.#jumboUsed = targetBytes;
    this.#drainOrdinary();
    this.#compactQueues(false);
    return true;
  }

  /** @internal */
  async promote(allocation: Allocation, targetBytes: number, signal?: AbortSignal): Promise<void> {
    this.#assertAllocation(allocation);
    targetBytes = bytes(targetBytes, "targetBytes");
    if (allocation.region === "jumbo") {
      await this.resize(allocation, targetBytes, { signal });
      return;
    }
    this.#ensureOpen();
    this.#ensureFits("jumbo", targetBytes);
    if (signal?.aborted) throw abortReason(signal);
    if (allocation.pending) {
      throw new AtomicBytePermitError("Byte permit already has a pending resize", "pending");
    }
    await this.#enqueue({
      kind: "promote",
      region: "jumbo",
      bytes: targetBytes,
      allocation,
      label: allocation.label,
      signal,
    });
  }

  /** @internal */
  shrink(allocation: Allocation, targetBytes: number): void {
    this.#assertAllocation(allocation);
    targetBytes = bytes(targetBytes, "targetBytes", true);
    if (targetBytes > allocation.bytes) {
      throw new RangeError("shrink target exceeds the current permit size");
    }
    if (allocation.pending) {
      throw new AtomicBytePermitError("Cannot shrink a permit with a pending resize", "pending");
    }
    if (targetBytes === allocation.bytes) return;
    if (targetBytes === 0) {
      this.release(allocation);
      return;
    }
    const released = allocation.bytes - targetBytes;
    allocation.bytes = targetBytes;
    if (allocation.region === "ordinary") this.#ordinaryUsed -= released;
    else this.#jumboUsed -= released;
    this.#drain();
  }

  /** @internal */
  release(allocation: Allocation): void {
    if (allocation.released) return;
    if (allocation.pending) {
      this.#rejectWaiter(
        allocation.pending,
        new AtomicBytePermitError("Byte permit was released while resizing", "released"),
      );
    }
    allocation.released = true;
    allocation.pending = null;
    this.#allocations.delete(allocation.id);
    if (allocation.region === "ordinary") this.#ordinaryUsed -= allocation.bytes;
    else this.#jumboUsed -= allocation.bytes;
    allocation.bytes = 0;
    this.#drain();
  }

  #enqueue(input: Omit<Waiter, "resolve" | "reject" | "cancelled">): Promise<AtomicBytePermit> {
    if (this.#waiterCount >= this.#maxWaiters) {
      return Promise.reject(new AtomicBytePermitError("Byte permit waiter capacity is exhausted", "waiters"));
    }
    return new Promise<AtomicBytePermit>((resolve, reject) => {
      const waiter: Waiter = { ...input, resolve, reject, cancelled: false };
      this.#waiterCount++;
      if (waiter.allocation) waiter.allocation.pending = waiter;
      const onAbort = () => {
        this.#rejectWaiter(waiter, abortReason(waiter.signal!));
        this.#drain();
      };
      if (waiter.signal) {
        waiter.signal.addEventListener("abort", onAbort, { once: true });
        waiter.detachAbort = () => waiter.signal!.removeEventListener("abort", onAbort);
      }
      (waiter.region === "ordinary" ? this.#ordinaryQueue : this.#jumboQueue).push(waiter);
      this.#drain();
    });
  }

  #drain(): void {
    if (this.#closed) return;
    // Jumbo first: a promotion releases ordinary bytes and may unblock its FIFO.
    this.#drainJumbo();
    this.#drainOrdinary();
    this.#compactQueues(false);
  }

  #drainJumbo(): void {
    this.#skipCancelled(this.#jumboQueue, "jumbo");
    if (this.#jumboUsed !== 0) return;
    const waiter = this.#jumboQueue[this.#jumboHead];
    if (!waiter) return;
    if (!this.#waiterStillValid(waiter)) {
      this.#rejectWaiter(waiter, new AtomicBytePermitError("Byte permit is no longer active", "released"));
      this.#jumboHead++;
      this.#drainJumbo();
      return;
    }
    this.#jumboHead++;
    let permit: AtomicBytePermit;
    if (waiter.kind === "acquire") {
      permit = this.#allocate("jumbo", waiter.bytes, waiter.label);
    } else {
      const allocation = waiter.allocation!;
      this.#ordinaryUsed -= allocation.bytes;
      allocation.region = "jumbo";
      allocation.bytes = waiter.bytes;
      this.#jumboUsed = waiter.bytes;
      permit = new AtomicBytePermit(this, allocation, allocation.token);
    }
    this.#resolveWaiter(waiter, permit);
  }

  #drainOrdinary(): void {
    for (;;) {
      this.#skipCancelled(this.#ordinaryQueue, "ordinary");
      const waiter = this.#ordinaryQueue[this.#ordinaryHead];
      if (!waiter) return;
      if (!this.#waiterStillValid(waiter)) {
        this.#rejectWaiter(waiter, new AtomicBytePermitError("Byte permit is no longer active", "released"));
        this.#ordinaryHead++;
        continue;
      }
      const additional = waiter.kind === "acquire"
        ? waiter.bytes
        : waiter.bytes - waiter.allocation!.bytes;
      if (this.#ordinaryUsed + additional > this.#ordinaryCapacity) return;
      this.#ordinaryHead++;
      let permit: AtomicBytePermit;
      if (waiter.kind === "acquire") {
        permit = this.#allocate("ordinary", waiter.bytes, waiter.label);
      } else {
        const allocation = waiter.allocation!;
        this.#ordinaryUsed += additional;
        allocation.bytes = waiter.bytes;
        permit = new AtomicBytePermit(this, allocation, allocation.token);
      }
      this.#resolveWaiter(waiter, permit);
    }
  }

  #allocate(
    region: AtomicBytePermitRegion,
    allocationBytes: number,
    label: string | null,
  ): AtomicBytePermit {
    const token = {};
    const allocation: Allocation = {
      id: this.#nextId++,
      bytes: allocationBytes,
      region,
      label,
      token,
      released: false,
      pending: null,
    };
    this.#allocations.set(allocation.id, allocation);
    if (region === "ordinary") this.#ordinaryUsed += allocationBytes;
    else this.#jumboUsed += allocationBytes;
    return new AtomicBytePermit(this, allocation, token);
  }

  #resolveWaiter(waiter: Waiter, permit: AtomicBytePermit): void {
    if (waiter.cancelled) return;
    waiter.cancelled = true;
    this.#waiterCount--;
    waiter.detachAbort?.();
    if (waiter.allocation?.pending === waiter) waiter.allocation.pending = null;
    waiter.resolve(permit);
  }

  #rejectWaiter(waiter: Waiter, error: unknown): void {
    if (waiter.cancelled) return;
    waiter.cancelled = true;
    this.#waiterCount--;
    waiter.detachAbort?.();
    if (waiter.allocation?.pending === waiter) waiter.allocation.pending = null;
    waiter.reject(error);
  }

  #skipCancelled(queue: Waiter[], region: AtomicBytePermitRegion): void {
    if (region === "ordinary") {
      while (queue[this.#ordinaryHead]?.cancelled) this.#ordinaryHead++;
    } else {
      while (queue[this.#jumboHead]?.cancelled) this.#jumboHead++;
    }
  }

  #compactQueues(force: boolean): void {
    if (force) {
      this.#ordinaryQueue.length = 0;
      this.#jumboQueue.length = 0;
      this.#ordinaryHead = 0;
      this.#jumboHead = 0;
      return;
    }
    if (this.#ordinaryHead > 1_024 && this.#ordinaryHead * 2 > this.#ordinaryQueue.length) {
      this.#ordinaryQueue.splice(0, this.#ordinaryHead);
      this.#ordinaryHead = 0;
    }
    if (this.#jumboHead > 1_024 && this.#jumboHead * 2 > this.#jumboQueue.length) {
      this.#jumboQueue.splice(0, this.#jumboHead);
      this.#jumboHead = 0;
    }
  }

  #waiterStillValid(waiter: Waiter): boolean {
    return waiter.kind === "acquire" || (
      waiter.allocation !== null &&
      !waiter.allocation.released &&
      waiter.allocation.pending === waiter
    );
  }

  #ordinaryQueueHasLiveWaiter(): boolean {
    for (let index = this.#ordinaryHead; index < this.#ordinaryQueue.length; index++) {
      if (!this.#ordinaryQueue[index]!.cancelled) return true;
    }
    return false;
  }

  #jumboQueueHasLiveWaiter(): boolean {
    for (let index = this.#jumboHead; index < this.#jumboQueue.length; index++) {
      if (!this.#jumboQueue[index]!.cancelled) return true;
    }
    return false;
  }

  #canAcquireImmediately(region: AtomicBytePermitRegion, requestedBytes: number): boolean {
    if (region === "ordinary") {
      return !this.#ordinaryQueueHasLiveWaiter() &&
        this.#ordinaryUsed + requestedBytes <= this.#ordinaryCapacity;
    }
    return this.#jumboUsed === 0 && !this.#jumboQueueHasLiveWaiter();
  }

  #ensureFits(region: AtomicBytePermitRegion, requestedBytes: number): void {
    const capacity = region === "ordinary" ? this.#ordinaryCapacity : this.#jumboCapacity;
    if (requestedBytes > capacity || capacity === 0) {
      throw new AtomicBytePermitError(
        `Requested bytes cannot fit in the ${region} permit region`,
        "capacity",
      );
    }
  }

  #ensureFitsForResize(targetBytes: number, allowJumbo: boolean): void {
    if (
      targetBytes > this.#ordinaryCapacity &&
      (!allowJumbo || targetBytes > this.#jumboCapacity)
    ) {
      throw new AtomicBytePermitError("Requested bytes cannot fit in the permit pool", "capacity");
    }
  }

  #ensureOpen(): void {
    if (this.#closed) {
      throw this.#closeReason ?? new AtomicBytePermitError("Byte permit pool is closed", "closed");
    }
  }

  #assertAllocation(allocation: Allocation): void {
    if (allocation.released || !this.#allocations.has(allocation.id)) {
      throw new AtomicBytePermitError("Byte permit is released", "released");
    }
  }
}
