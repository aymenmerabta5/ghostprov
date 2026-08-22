import * as os from "node:os";
import type { MachineProbe, MachineSample } from "../core/ports";
import type { WorkloadDescriptor } from "./workload";
import type {
  ReleasableLease,
  ResourceGovernorSnapshot,
  ResourceSizing,
} from "./types";

const GiB = 1024 ** 3;

export class SystemMachineProbe implements MachineProbe {
  sample(): MachineSample {
    const memory = process.memoryUsage();
    return {
      totalBytes: os.totalmem(),
      freeBytes: os.freemem(),
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

export function deriveResourceSizing(sample: ReturnType<MachineProbe["sample"]>): ResourceSizing {
  const systemReserveBytes = Math.max(4 * GiB, sample.totalBytes * 0.20);
  const processCeilingBytes = Math.max(
    sample.rssBytes,
    Math.min(
      sample.totalBytes * 0.60,
      sample.rssBytes + Math.max(0, sample.freeBytes - systemReserveBytes),
    ),
  );
  return {
    totalBytes: sample.totalBytes,
    startupFreeBytes: sample.freeBytes,
    startupRssBytes: sample.rssBytes,
    systemReserveBytes,
    processCeilingBytes,
    reservationCapacityBytes: Math.max(0, processCeilingBytes - sample.rssBytes),
    pauseFreeBytes: Math.max(2 * GiB, sample.totalBytes * 0.10),
    resumeFreeBytes: Math.max(3 * GiB, sample.totalBytes * 0.15),
    cpuTransformWorkers: clamp(2, 8, Math.floor(sample.logicalProcessors / 2)),
  };
}

interface ByteWaiter {
  workload: WorkloadDescriptor;
  resolve: (lease: ReleasableLease) => void;
  reject: (error: unknown) => void;
  signal?: AbortSignal;
  onAbort?: () => void;
}

interface CpuWaiter {
  resolve: (lease: ReleasableLease) => void;
  reject: (error: unknown) => void;
  signal?: AbortSignal;
  onAbort?: () => void;
}

function abortError(): DOMException {
  return new DOMException("Resource acquisition aborted", "AbortError");
}

export class ResourceGovernor {
  readonly #probe: MachineProbe;
  readonly #sizing: ResourceSizing;
  readonly #byteWaiters: ByteWaiter[] = [];
  readonly #cpuWaiters: CpuWaiter[] = [];
  #reservedBytes = 0;
  #cpuActive = 0;
  #paused = false;

  constructor(options: { probe: MachineProbe }) {
    this.#probe = options.probe;
    this.#sizing = deriveResourceSizing(options.probe.sample());
    this.refreshPressure();
  }

  refreshPressure(): void {
    const free = this.#probe.sample().freeBytes;
    if (this.#paused) {
      if (free >= this.#sizing.resumeFreeBytes) this.#paused = false;
    } else if (free <= this.#sizing.pauseFreeBytes) {
      this.#paused = true;
    }
    this.#drainBytes();
  }

  tryAcquire(workload: WorkloadDescriptor): ReleasableLease | null {
    this.#refreshPressureWithoutDrain();
    if (this.#paused) return null;
    if (this.#reservedBytes + workload.reservedBytes > this.#sizing.reservationCapacityBytes) return null;
    this.#reservedBytes += workload.reservedBytes;
    let released = false;
    return {
      release: () => {
        if (released) return;
        released = true;
        this.#reservedBytes -= workload.reservedBytes;
        this.#drainBytes();
      },
    };
  }

  acquire(workload: WorkloadDescriptor, signal?: AbortSignal): Promise<ReleasableLease> {
    if (signal?.aborted) return Promise.reject(abortError());
    const immediate = this.tryAcquire(workload);
    if (immediate) return Promise.resolve(immediate);
    return new Promise((resolve, reject) => {
      const waiter: ByteWaiter = { workload, resolve, reject, signal };
      waiter.onAbort = () => {
        const index = this.#byteWaiters.indexOf(waiter);
        if (index < 0) return;
        this.#byteWaiters.splice(index, 1);
        reject(abortError());
      };
      signal?.addEventListener("abort", waiter.onAbort, { once: true });
      this.#byteWaiters.push(waiter);
    });
  }

  tryAcquireCpu(): ReleasableLease | null {
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

  acquireCpu(signal?: AbortSignal): Promise<ReleasableLease> {
    if (signal?.aborted) return Promise.reject(abortError());
    const immediate = this.tryAcquireCpu();
    if (immediate) return Promise.resolve(immediate);
    return new Promise((resolve, reject) => {
      const waiter: CpuWaiter = { resolve, reject, signal };
      waiter.onAbort = () => {
        const index = this.#cpuWaiters.indexOf(waiter);
        if (index < 0) return;
        this.#cpuWaiters.splice(index, 1);
        reject(abortError());
      };
      signal?.addEventListener("abort", waiter.onAbort, { once: true });
      this.#cpuWaiters.push(waiter);
    });
  }

  snapshot(): ResourceGovernorSnapshot {
    return {
      ...this.#sizing,
      paused: this.#paused,
      reservedBytes: this.#reservedBytes,
      queued: this.#byteWaiters.length,
      cpuActive: this.#cpuActive,
      cpuQueued: this.#cpuWaiters.length,
    };
  }

  #refreshPressureWithoutDrain(): void {
    const free = this.#probe.sample().freeBytes;
    if (this.#paused) {
      if (free >= this.#sizing.resumeFreeBytes) this.#paused = false;
    } else if (free <= this.#sizing.pauseFreeBytes) {
      this.#paused = true;
    }
  }

  #drainBytes(): void {
    this.#refreshPressureWithoutDrain();
    if (this.#paused) return;
    for (;;) {
      const waiter = this.#byteWaiters[0];
      if (!waiter) return;
      if (waiter.signal?.aborted) {
        this.#byteWaiters.shift();
        waiter.reject(abortError());
        continue;
      }
      const lease = this.tryAcquire(waiter.workload);
      if (!lease) return;
      this.#byteWaiters.shift();
      waiter.signal?.removeEventListener("abort", waiter.onAbort!);
      waiter.resolve(lease);
    }
  }

  #drainCpu(): void {
    while (this.#cpuActive < this.#sizing.cpuTransformWorkers) {
      const waiter = this.#cpuWaiters.shift();
      if (!waiter) return;
      if (waiter.signal?.aborted) {
        waiter.reject(abortError());
        continue;
      }
      waiter.signal?.removeEventListener("abort", waiter.onAbort!);
      waiter.resolve(this.tryAcquireCpu()!);
    }
  }
}
