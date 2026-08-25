import * as os from "node:os";
import type { MachineProbe, MachineSample } from "../core/ports";
import type { WorkloadDescriptor } from "./workload";
import type {
  ReleasableLease,
  ResourceGovernorDetailedSnapshot,
  ResourceSizing,
} from "./types";

const MiB = 1024 ** 2;
const GiB = 1024 ** 3;
const DEFAULT_MAX_BYTE_WAITERS = 256;
const DEFAULT_MAX_CPU_WAITERS = 256;
const DEFAULT_MAX_QUEUED_RESERVATION_BYTES = 512 * MiB;
const DEFAULT_WAITER_TIMEOUT_MS = 30_000;
const DEFAULT_PRESSURE_POLL_MS = 250;
const MAX_WAITER_BYPASSES = 8;

export interface EffectiveMemoryInput {
  hostTotalBytes: number;
  hostFreeBytes: number;
  rssBytes: number;
  constrainedBytes?: number;
  availableBytes?: number;
}

export function resolveEffectiveMemory(input: EffectiveMemoryInput): {
  totalBytes: number;
  freeBytes: number;
} {
  const hostTotalBytes = Math.max(1, Math.floor(input.hostTotalBytes));
  const rssBytes = Math.max(0, Math.floor(input.rssBytes));
  const constrainedBytes = Number.isFinite(input.constrainedBytes) &&
      input.constrainedBytes! > 0
    ? Math.floor(input.constrainedBytes!)
    : null;
  const totalBytes = constrainedBytes === null
    ? hostTotalBytes
    : Math.max(1, Math.min(hostTotalBytes, constrainedBytes));
  const allocationHeadroom = Math.max(0, totalBytes - rssBytes);
  let freeBytes = Math.min(
    Math.max(0, Math.floor(input.hostFreeBytes)),
    allocationHeadroom,
  );
  if (Number.isFinite(input.availableBytes) && input.availableBytes! >= 0) {
    freeBytes = Math.min(freeBytes, Math.floor(input.availableBytes!));
  }
  return { totalBytes, freeBytes };
}

function optionalProcessMemory(method: "constrainedMemory" | "availableMemory"): number | undefined {
  try {
    const value = process[method]?.();
    return typeof value === "number" && Number.isFinite(value) ? value : undefined;
  } catch {
    return undefined;
  }
}

export class SystemMachineProbe implements MachineProbe {
  sample(): MachineSample {
    const memory = process.memoryUsage();
    const effective = resolveEffectiveMemory({
      hostTotalBytes: os.totalmem(),
      hostFreeBytes: os.freemem(),
      rssBytes: memory.rss,
      constrainedBytes: optionalProcessMemory("constrainedMemory"),
      availableBytes: optionalProcessMemory("availableMemory"),
    });
    return {
      totalBytes: effective.totalBytes,
      freeBytes: effective.freeBytes,
      rssBytes: memory.rss,
      heapBytes: memory.heapUsed,
      externalBytes: memory.external + memory.arrayBuffers,
      logicalProcessors: typeof os.availableParallelism === "function"
        ? os.availableParallelism()
        : os.cpus().length,
    };
  }
}

function clamp(minimum: number, maximum: number, value: number): number {
  return Math.max(minimum, Math.min(maximum, value));
}

export interface DerivedResourceSizing extends ResourceSizing {
  maximumReservationCapacityBytes: number;
}

export function deriveResourceSizing(
  sample: ReturnType<MachineProbe["sample"]>,
): DerivedResourceSizing {
  const systemReserveBytes = Math.floor(Math.min(
    sample.totalBytes * 0.50,
    Math.max(4 * GiB, sample.totalBytes * 0.20),
  ));
  const maximumProcessCeilingBytes = Math.floor(Math.max(
    sample.rssBytes,
    sample.totalBytes * 0.60,
  ));
  const processCeilingBytes = Math.floor(Math.max(
    sample.rssBytes,
    Math.min(
      maximumProcessCeilingBytes,
      sample.rssBytes + Math.max(0, sample.freeBytes - systemReserveBytes),
    ),
  ));
  const pauseFreeBytes = Math.floor(Math.min(
    sample.totalBytes * 0.25,
    Math.max(2 * GiB, sample.totalBytes * 0.10),
  ));
  const resumeFreeBytes = Math.floor(Math.min(
    sample.totalBytes * 0.40,
    Math.max(3 * GiB, sample.totalBytes * 0.15),
  ));
  return {
    totalBytes: sample.totalBytes,
    startupFreeBytes: sample.freeBytes,
    startupRssBytes: sample.rssBytes,
    systemReserveBytes,
    processCeilingBytes,
    reservationCapacityBytes: Math.max(0, processCeilingBytes - sample.rssBytes),
    // Structural per-request limit. Live RSS is deliberately excluded; it
    // affects current admission headroom, not whether a request is valid.
    maximumReservationCapacityBytes: Math.floor(sample.totalBytes * 0.60),
    pauseFreeBytes,
    resumeFreeBytes: Math.max(pauseFreeBytes, resumeFreeBytes),
    cpuTransformWorkers: clamp(1, 8, Math.floor(sample.logicalProcessors / 2)),
  };
}

interface ByteWaiter {
  reservedBytes: number;
  bypasses: number;
  resolve: (lease: ResourceLease) => void;
  reject: (error: unknown) => void;
  deadlineMs: number;
  timer?: ReturnType<typeof setTimeout>;
  signal?: AbortSignal;
  onAbort?: () => void;
}

interface CpuWaiter {
  resolve: (lease: ReleasableLease) => void;
  reject: (error: unknown) => void;
  deadlineMs: number;
  timer?: ReturnType<typeof setTimeout>;
  signal?: AbortSignal;
  onAbort?: () => void;
}

export interface ResourceAcquireOptions {
  /** Absolute wall-clock deadline. The governor default applies when omitted. */
  deadlineMs?: number;
}

/**
 * A byte reservation which can be handed from one request phase to another.
 * `replace()` is an immediate in-place accounting swap. It never queues: if
 * growth cannot fit, it rejects while this lease remains fully active.
 */
export interface ResourceLease extends ReleasableLease {
  readonly reservedBytes: number;
  replace(
    workload: WorkloadDescriptor,
    signal?: AbortSignal,
    options?: ResourceAcquireOptions,
  ): Promise<ResourceLease>;
}

export class ResourceReservationTooLargeError extends RangeError {
  readonly name = "ResourceReservationTooLargeError";
  readonly status = 413;
  readonly code = "resource_reservation_too_large";

  constructor() {
    super("Resource reservation exceeds the maximum host capacity");
  }
}

export class ResourceGovernorQueueFullError extends Error {
  readonly name = "ResourceGovernorQueueFullError";
  readonly status = 503;
  readonly code = "resource_admission_overloaded";

  constructor() {
    super("Resource admission queue is full");
  }
}

export class ResourceGovernorReplacementOverloadError extends Error {
  readonly name = "ResourceGovernorReplacementOverloadError";
  readonly status = 503;
  readonly code = "resource_replacement_overloaded";

  constructor() {
    super("Replacement reservation cannot fit current host headroom");
  }
}

export class ResourceGovernorTimeoutError extends Error {
  readonly name = "ResourceGovernorTimeoutError";
  readonly status = 503;
  readonly code = "resource_admission_timeout";

  constructor() {
    super("Resource admission deadline expired");
  }
}

export class ResourceGovernorClosedError extends Error {
  readonly name = "ResourceGovernorClosedError";
  readonly status = 503;
  readonly code = "resource_governor_closed";

  constructor() {
    super("Resource governor is closed");
  }
}

function abortError(): DOMException {
  return new DOMException("Resource acquisition aborted", "AbortError");
}

export class ResourceGovernor {
  readonly #probe: MachineProbe;
  readonly #sizing: DerivedResourceSizing;
  readonly #maximumReservationCapacityBytes: number;
  readonly #maxByteWaiters: number;
  readonly #maxCpuWaiters: number;
  readonly #maxQueuedReservationBytes: number;
  readonly #waiterTimeoutMs: number;
  readonly #pressurePollMs: number;
  readonly #now: () => number;
  readonly #byteWaiters: ByteWaiter[] = [];
  readonly #cpuWaiters: CpuWaiter[] = [];
  #unreservedRssBaselineBytes: number;
  #resetRssBaselineOnNextSample = false;
  #activeByteLeases = 0;
  #reservedBytes = 0;
  #queuedReservationBytes = 0;
  #cpuActive = 0;
  #paused = false;
  #closed = false;
  #pressureTimer?: ReturnType<typeof setTimeout>;
  #drainingBytes = false;

  constructor(options: {
    probe: MachineProbe;
    maxByteWaiters?: number;
    maxCpuWaiters?: number;
    maxQueuedReservationBytes?: number;
    waiterTimeoutMs?: number;
    pressurePollMs?: number;
    now?: () => number;
  }) {
    this.#probe = options.probe;
    const initialSample = options.probe.sample();
    this.#sizing = deriveResourceSizing(initialSample);
    this.#maximumReservationCapacityBytes = this.#sizing.maximumReservationCapacityBytes;
    this.#unreservedRssBaselineBytes = initialSample.rssBytes;
    this.#maxByteWaiters = positiveSafeInteger(
      options.maxByteWaiters ?? DEFAULT_MAX_BYTE_WAITERS,
      "maxByteWaiters",
    );
    this.#maxCpuWaiters = positiveSafeInteger(
      options.maxCpuWaiters ?? DEFAULT_MAX_CPU_WAITERS,
      "maxCpuWaiters",
    );
    this.#maxQueuedReservationBytes = nonNegativeSafeInteger(
      options.maxQueuedReservationBytes ?? DEFAULT_MAX_QUEUED_RESERVATION_BYTES,
      "maxQueuedReservationBytes",
    );
    this.#waiterTimeoutMs = positiveSafeInteger(
      options.waiterTimeoutMs ?? DEFAULT_WAITER_TIMEOUT_MS,
      "waiterTimeoutMs",
    );
    this.#pressurePollMs = positiveSafeInteger(
      options.pressurePollMs ?? DEFAULT_PRESSURE_POLL_MS,
      "pressurePollMs",
    );
    this.#now = options.now ?? Date.now;
    this.refreshPressure();
  }

  refreshPressure(): void {
    if (this.#closed) return;
    this.#refreshPressureWithoutDrain();
    this.#drainBytes(false);
  }

  tryAcquire(workload: WorkloadDescriptor): ResourceLease | null {
    if (this.#closed) return null;
    this.#refreshPressureWithoutDrain();
    try {
      this.#assertWorkload(workload);
    } catch {
      return null;
    }
    this.#rejectStaleByteWaiters();
    if (this.#byteWaiters.length > 0) return null;
    return this.#tryGrant(workload.reservedBytes);
  }

  #tryGrant(reservedBytes: number): ResourceLease | null {
    if (this.#paused) return null;
    if (this.#reservedBytes + reservedBytes > this.#sizing.reservationCapacityBytes) return null;
    this.#reservedBytes += reservedBytes;
    this.#activeByteLeases++;
    return this.#createLease(reservedBytes);
  }

  #createLease(reservedBytes: number): ResourceLease {
    let released = false;
    let replacing = false;
    return {
      get reservedBytes() {
        return released ? 0 : reservedBytes;
      },
      release: () => {
        if (released) return;
        released = true;
        this.#reservedBytes = Math.max(0, this.#reservedBytes - reservedBytes);
        this.#activeByteLeases = Math.max(0, this.#activeByteLeases - 1);
        if (this.#activeByteLeases === 0) this.#resetRssBaselineOnNextSample = true;
        this.#drainBytes();
      },
      replace: (next, signal, options) => {
        if (released || replacing) {
          return Promise.reject(new Error("Resource lease is no longer active"));
        }
        replacing = true;
        return this.#replaceLease(reservedBytes, next, signal, options, () => {
          released = true;
        }).catch(error => {
          if (!released) replacing = false;
          throw error;
        });
      },
    };
  }

  acquire(
    workload: WorkloadDescriptor,
    signal?: AbortSignal,
    options: ResourceAcquireOptions = {},
  ): Promise<ResourceLease> {
    if (this.#closed) return Promise.reject(new ResourceGovernorClosedError());
    if (signal?.aborted) return Promise.reject(abortError());
    this.#refreshPressureWithoutDrain();
    try {
      this.#assertWorkload(workload);
    } catch (error) {
      return Promise.reject(error);
    }
    let deadlineMs: number;
    try {
      deadlineMs = this.#deadline(options);
    } catch (error) {
      return Promise.reject(error);
    }
    if (deadlineMs <= this.#now()) return Promise.reject(new ResourceGovernorTimeoutError());
    this.#rejectStaleByteWaiters();
    const immediate = this.#byteWaiters.length === 0
      ? this.#tryGrant(workload.reservedBytes)
      : null;
    if (immediate) return Promise.resolve(immediate);
    return this.#enqueueByteWaiter(workload.reservedBytes, signal, deadlineMs);
  }

  #enqueueByteWaiter(
    reservedBytes: number,
    signal: AbortSignal | undefined,
    deadlineMs: number,
  ): Promise<ResourceLease> {
    this.#rejectStaleByteWaiters();
    if (
      this.#byteWaiters.length >= this.#maxByteWaiters ||
      this.#queuedReservationBytes + reservedBytes > this.#maxQueuedReservationBytes
    ) return Promise.reject(new ResourceGovernorQueueFullError());
    return new Promise((resolve, reject) => {
      const waiter: ByteWaiter = {
        reservedBytes,
        bypasses: 0,
        resolve,
        reject,
        signal,
        deadlineMs,
      };
      waiter.onAbort = () => {
        if (!this.#removeByteWaiter(waiter)) return;
        reject(abortError());
        this.#drainBytes();
      };
      signal?.addEventListener("abort", waiter.onAbort, { once: true });
      this.#byteWaiters.push(waiter);
      this.#queuedReservationBytes += reservedBytes;
      this.#armByteWaiter(waiter);
      this.#schedulePressurePoll();
      this.#drainBytes(false);
    });
  }

  capacityHint(perActiveRequestBytes: number, physicalMaximum: number): number {
    if (!Number.isFinite(perActiveRequestBytes) || perActiveRequestBytes <= 0) {
      throw new RangeError("perActiveRequestBytes must be positive");
    }
    if (!Number.isSafeInteger(physicalMaximum) || physicalMaximum < 0) {
      throw new RangeError("physicalMaximum must be a non-negative safe integer");
    }
    if (this.#closed) return 0;
    this.refreshPressure();
    const snapshot = this.snapshot();
    if (snapshot.paused) return 0;
    const remaining = Math.max(
      0,
      snapshot.reservationCapacityBytes - snapshot.reservedBytes,
    );
    return Math.max(
      0,
      Math.min(physicalMaximum, Math.floor(remaining / perActiveRequestBytes)),
    );
  }

  tryAcquireCpu(): ReleasableLease | null {
    if (this.#closed || this.#cpuWaiters.length > 0) return null;
    return this.#tryGrantCpu();
  }

  #tryGrantCpu(): ReleasableLease | null {
    if (this.#cpuActive >= this.#sizing.cpuTransformWorkers) return null;
    this.#cpuActive++;
    let released = false;
    return {
      release: () => {
        if (released) return;
        released = true;
        this.#cpuActive--;
        this.#drainCpu();
      },
    };
  }

  acquireCpu(
    signal?: AbortSignal,
    options: ResourceAcquireOptions = {},
  ): Promise<ReleasableLease> {
    if (this.#closed) return Promise.reject(new ResourceGovernorClosedError());
    if (signal?.aborted) return Promise.reject(abortError());
    let deadlineMs: number;
    try {
      deadlineMs = this.#deadline(options);
    } catch (error) {
      return Promise.reject(error);
    }
    if (deadlineMs <= this.#now()) return Promise.reject(new ResourceGovernorTimeoutError());
    this.#rejectStaleCpuWaiters();
    const immediate = this.#cpuWaiters.length === 0 ? this.#tryGrantCpu() : null;
    if (immediate) return Promise.resolve(immediate);
    if (this.#cpuWaiters.length >= this.#maxCpuWaiters) {
      return Promise.reject(new ResourceGovernorQueueFullError());
    }
    return new Promise((resolve, reject) => {
      const waiter: CpuWaiter = { resolve, reject, signal, deadlineMs };
      waiter.onAbort = () => {
        if (!this.#removeCpuWaiter(waiter)) return;
        reject(abortError());
        this.#drainCpu();
      };
      signal?.addEventListener("abort", waiter.onAbort, { once: true });
      this.#cpuWaiters.push(waiter);
      this.#armCpuWaiter(waiter);
    });
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#pressureTimer) clearTimeout(this.#pressureTimer);
    this.#pressureTimer = undefined;
    const error = new ResourceGovernorClosedError();
    for (const waiter of this.#byteWaiters.splice(0)) {
      this.#cleanupByteWaiter(waiter);
      waiter.reject(error);
    }
    this.#queuedReservationBytes = 0;
    for (const waiter of this.#cpuWaiters.splice(0)) {
      this.#cleanupCpuWaiter(waiter);
      waiter.reject(error);
    }
  }

  snapshot(): ResourceGovernorDetailedSnapshot {
    return {
      ...this.#sizing,
      paused: this.#paused,
      closed: this.#closed,
      reservedBytes: this.#reservedBytes,
      queued: this.#byteWaiters.length,
      queuedReservationBytes: this.#queuedReservationBytes,
      queueCapacity: this.#maxByteWaiters,
      queueReservationCapacityBytes: this.#maxQueuedReservationBytes,
      cpuActive: this.#cpuActive,
      cpuQueued: this.#cpuWaiters.length,
      cpuQueueCapacity: this.#maxCpuWaiters,
    };
  }

  #refreshPressureWithoutDrain(): void {
    const sample = this.#probe.sample();
    this.#refreshSizing(sample);
    const free = sample.freeBytes;
    if (this.#paused) {
      if (free >= this.#sizing.resumeFreeBytes) this.#paused = false;
    } else if (free <= this.#sizing.pauseFreeBytes) {
      this.#paused = true;
    }
  }

  #refreshSizing(sample: MachineSample): void {
    const next = deriveResourceSizing(sample);
    if (this.#activeByteLeases === 0) {
      if (this.#resetRssBaselineOnNextSample) {
        this.#unreservedRssBaselineBytes = sample.rssBytes;
        this.#resetRssBaselineOnNextSample = false;
      } else if (sample.rssBytes < this.#unreservedRssBaselineBytes) {
        this.#unreservedRssBaselineBytes = sample.rssBytes;
      }
    }
    const materializedReservationBytes = Math.min(
      this.#reservedBytes,
      Math.max(0, sample.rssBytes - this.#unreservedRssBaselineBytes),
    );
    const unmaterializedReservationBytes = Math.max(
      0,
      this.#reservedBytes - materializedReservationBytes,
    );
    const physicalHeadroomBytes = Math.max(
      0,
      next.processCeilingBytes - sample.rssBytes,
    );
    const additionalReservationBytes = Math.max(
      0,
      physicalHeadroomBytes - unmaterializedReservationBytes,
    );
    this.#sizing.totalBytes = next.totalBytes;
    this.#sizing.systemReserveBytes = next.systemReserveBytes;
    this.#sizing.processCeilingBytes = next.processCeilingBytes;
    this.#sizing.reservationCapacityBytes =
      this.#reservedBytes + additionalReservationBytes;
    // This is a structural per-request limit, not live headroom. Keep the
    // constructor-proven maximum immutable so transient RSS pressure queues a
    // valid request instead of permanently misclassifying it as oversized.
    this.#sizing.maximumReservationCapacityBytes = this.#maximumReservationCapacityBytes;
    this.#sizing.pauseFreeBytes = next.pauseFreeBytes;
    this.#sizing.resumeFreeBytes = next.resumeFreeBytes;
  }

  #drainBytes(refresh = true): void {
    if (this.#closed || this.#drainingBytes) return;
    this.#drainingBytes = true;
    try {
      if (refresh) this.#refreshPressureWithoutDrain();
      this.#rejectStaleByteWaiters();
      if (this.#paused) return;
      for (;;) {
        const available = Math.max(
          0,
          this.#sizing.reservationCapacityBytes - this.#reservedBytes,
        );
        let index = -1;
        for (let candidate = 0; candidate < this.#byteWaiters.length; candidate++) {
          const waiter = this.#byteWaiters[candidate];
          if (waiter.reservedBytes <= available) {
            index = candidate;
            break;
          }
          // A large request may be bypassed briefly to keep the queue useful,
          // but after a bounded number of skips it reserves the next headroom.
          if (waiter.bypasses >= MAX_WAITER_BYPASSES) return;
        }
        if (index < 0) return;
        for (let skipped = 0; skipped < index; skipped++) {
          this.#byteWaiters[skipped].bypasses++;
        }
        const waiter = this.#removeByteWaiterAt(index);
        const lease = this.#tryGrant(waiter.reservedBytes);
        if (!lease) return;
        waiter.resolve(lease);
      }
    } finally {
      this.#drainingBytes = false;
      if (this.#byteWaiters.length > 0) this.#schedulePressurePoll();
      else this.#clearPressurePoll();
    }
  }

  #drainCpu(): void {
    if (this.#closed) return;
    this.#rejectStaleCpuWaiters();
    while (this.#cpuActive < this.#sizing.cpuTransformWorkers) {
      const waiter = this.#removeCpuWaiterAt(0);
      if (!waiter) return;
      waiter.resolve(this.#tryGrantCpu()!);
    }
  }

  #replaceLease(
    currentReservedBytes: number,
    next: WorkloadDescriptor,
    signal: AbortSignal | undefined,
    options: ResourceAcquireOptions | undefined,
    invalidate: () => void,
  ): Promise<ResourceLease> {
    if (this.#closed) return Promise.reject(new ResourceGovernorClosedError());
    if (signal?.aborted) return Promise.reject(abortError());
    this.#refreshPressureWithoutDrain();
    try {
      this.#assertWorkload(next);
    } catch (error) {
      return Promise.reject(error);
    }
    let deadlineMs: number;
    try {
      deadlineMs = this.#deadline(options ?? {});
    } catch (error) {
      return Promise.reject(error);
    }
    if (deadlineMs <= this.#now()) return Promise.reject(new ResourceGovernorTimeoutError());
    const projectedReservedBytes = Math.max(
      0,
      this.#reservedBytes - currentReservedBytes + next.reservedBytes,
    );
    const grows = next.reservedBytes > currentReservedBytes;
    if (
      (grows && this.#paused) ||
      projectedReservedBytes > this.#sizing.reservationCapacityBytes
    ) {
      return Promise.reject(new ResourceGovernorReplacementOverloadError());
    }

    // Swap the accounting in place. On any failure above, the old lease and
    // all of its bytes remain active until the caller unwinds and releases it.
    this.#reservedBytes = projectedReservedBytes;
    invalidate();
    const replacement = this.#createLease(next.reservedBytes);
    if (next.reservedBytes < currentReservedBytes) this.#drainBytes();
    return Promise.resolve(replacement);
  }

  #assertWorkload(workload: WorkloadDescriptor): void {
    if (!Number.isSafeInteger(workload.reservedBytes) || workload.reservedBytes < 0) {
      throw new RangeError("workload.reservedBytes must be a non-negative safe integer");
    }
    if (workload.reservedBytes > this.#maximumReservationCapacityBytes) {
      throw new ResourceReservationTooLargeError();
    }
  }

  #deadline(options: ResourceAcquireOptions): number {
    const deadlineMs = options.deadlineMs ?? this.#now() + this.#waiterTimeoutMs;
    if (!Number.isFinite(deadlineMs)) throw new RangeError("deadlineMs must be finite");
    return deadlineMs;
  }

  #armByteWaiter(waiter: ByteWaiter): void {
    const remaining = Math.max(0, waiter.deadlineMs - this.#now());
    waiter.timer = setTimeout(() => {
      waiter.timer = undefined;
      if (this.#now() < waiter.deadlineMs) {
        this.#armByteWaiter(waiter);
        return;
      }
      if (!this.#removeByteWaiter(waiter)) return;
      waiter.reject(new ResourceGovernorTimeoutError());
      this.#drainBytes();
    }, Math.min(remaining, 2_147_483_647));
    waiter.timer.unref?.();
  }

  #armCpuWaiter(waiter: CpuWaiter): void {
    const remaining = Math.max(0, waiter.deadlineMs - this.#now());
    waiter.timer = setTimeout(() => {
      waiter.timer = undefined;
      if (this.#now() < waiter.deadlineMs) {
        this.#armCpuWaiter(waiter);
        return;
      }
      if (!this.#removeCpuWaiter(waiter)) return;
      waiter.reject(new ResourceGovernorTimeoutError());
      this.#drainCpu();
    }, Math.min(remaining, 2_147_483_647));
    waiter.timer.unref?.();
  }

  #removeByteWaiter(waiter: ByteWaiter): boolean {
    const index = this.#byteWaiters.indexOf(waiter);
    if (index < 0) return false;
    this.#removeByteWaiterAt(index);
    return true;
  }

  #removeByteWaiterAt(index: number): ByteWaiter {
    const [waiter] = this.#byteWaiters.splice(index, 1);
    this.#queuedReservationBytes = Math.max(
      0,
      this.#queuedReservationBytes - waiter.reservedBytes,
    );
    this.#cleanupByteWaiter(waiter);
    return waiter;
  }

  #cleanupByteWaiter(waiter: ByteWaiter): void {
    if (waiter.timer) clearTimeout(waiter.timer);
    waiter.timer = undefined;
    waiter.signal?.removeEventListener("abort", waiter.onAbort!);
  }

  #removeCpuWaiter(waiter: CpuWaiter): boolean {
    const index = this.#cpuWaiters.indexOf(waiter);
    if (index < 0) return false;
    this.#removeCpuWaiterAt(index);
    return true;
  }

  #removeCpuWaiterAt(index: number): CpuWaiter | undefined {
    if (index < 0 || index >= this.#cpuWaiters.length) return undefined;
    const [waiter] = this.#cpuWaiters.splice(index, 1);
    this.#cleanupCpuWaiter(waiter);
    return waiter;
  }

  #cleanupCpuWaiter(waiter: CpuWaiter): void {
    if (waiter.timer) clearTimeout(waiter.timer);
    waiter.timer = undefined;
    waiter.signal?.removeEventListener("abort", waiter.onAbort!);
  }

  #rejectStaleByteWaiters(): void {
    const now = this.#now();
    for (let index = this.#byteWaiters.length - 1; index >= 0; index--) {
      const waiter = this.#byteWaiters[index];
      const error = waiter.signal?.aborted
        ? abortError()
        : now >= waiter.deadlineMs
          ? new ResourceGovernorTimeoutError()
          : null;
      if (!error) continue;
      this.#removeByteWaiterAt(index).reject(error);
    }
  }

  #rejectStaleCpuWaiters(): void {
    const now = this.#now();
    for (let index = this.#cpuWaiters.length - 1; index >= 0; index--) {
      const waiter = this.#cpuWaiters[index];
      const error = waiter.signal?.aborted
        ? abortError()
        : now >= waiter.deadlineMs
          ? new ResourceGovernorTimeoutError()
          : null;
      if (!error) continue;
      this.#removeCpuWaiterAt(index)?.reject(error);
    }
  }

  #schedulePressurePoll(): void {
    if (this.#closed || this.#pressureTimer || this.#byteWaiters.length === 0) return;
    this.#pressureTimer = setTimeout(() => {
      this.#pressureTimer = undefined;
      this.refreshPressure();
    }, this.#pressurePollMs);
    this.#pressureTimer.unref?.();
  }

  #clearPressurePoll(): void {
    if (this.#pressureTimer) clearTimeout(this.#pressureTimer);
    this.#pressureTimer = undefined;
  }
}

function positiveSafeInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new RangeError(`${name} must be a positive safe integer`);
  }
  return value;
}

function nonNegativeSafeInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new RangeError(`${name} must be a non-negative safe integer`);
  }
  return value;
}
