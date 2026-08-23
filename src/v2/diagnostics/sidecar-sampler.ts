import { BoundedDistribution } from "./statistics";
import type { V2SidecarResourceSnapshot } from "./types";

export interface SidecarRuntimeProbe {
  memory(): { rss: number; heapUsed: number; heapTotal: number; external: number };
  systemMemory(): { total: number; free: number } | null;
  processCpuPercent(): number | null;
  normalizedLoad1(): number | null;
}

export interface SidecarResourceSamplerOptions {
  enabled: boolean;
  intervalMs: number;
  now?: () => number;
  probe?: SidecarRuntimeProbe;
}

const finite = (value: unknown): number | null => typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : null;

function productionProbe(): SidecarRuntimeProbe {
  const deno = (globalThis as any).Deno;
  return {
    memory: () => {
      const value = deno?.memoryUsage?.() ?? {};
      return { rss: finite(value.rss) ?? 0, heapUsed: finite(value.heapUsed) ?? 0, heapTotal: finite(value.heapTotal) ?? 0, external: finite(value.external) ?? 0 };
    },
    systemMemory: () => {
      try {
        const value = deno?.systemMemoryInfo?.();
        return value ? { total: finite(value.total) ?? 0, free: finite(value.free ?? value.available) ?? 0 } : null;
      } catch { return null; }
    },
    processCpuPercent: () => null,
    normalizedLoad1: () => {
      try {
        const load = deno?.loadavg?.()?.[0];
        const processors = Math.max(1, Number((globalThis as any).navigator?.hardwareConcurrency) || 1);
        return finite(load) === null ? null : load / processors;
      } catch { return null; }
    },
  };
}

export class SidecarResourceSampler {
  readonly #enabled: boolean;
  readonly #intervalMs: number;
  readonly #now: () => number;
  readonly #probe: SidecarRuntimeProbe;
  readonly #eventLoop = new BoundedDistribution(256);
  #timer: ReturnType<typeof setTimeout> | null = null;
  #expected = 0;
  #closed = false;

  constructor(options: SidecarResourceSamplerOptions) {
    if (!Number.isSafeInteger(options.intervalMs) || options.intervalMs < 100 || options.intervalMs > 60_000) throw new RangeError("diagnostic interval must be 100 through 60000");
    this.#enabled = options.enabled; this.#intervalMs = options.intervalMs;
    this.#now = options.now ?? (() => performance.now());
    this.#probe = options.probe ?? productionProbe();
  }

  get estimatedBytes(): number { return this.#eventLoop.estimatedBytes + 256; }

  start(): void {
    if (!this.#enabled || this.#closed || this.#timer) return;
    this.#expected = this.#now() + this.#intervalMs;
    const tick = () => {
      this.#timer = null;
      if (this.#closed) return;
      const now = this.#now();
      this.recordEventLoopDelay(Math.max(0, now - this.#expected));
      this.#expected = now + this.#intervalMs;
      this.#timer = setTimeout(tick, this.#intervalMs);
      (this.#timer as any).unref?.();
    };
    this.#timer = setTimeout(tick, this.#intervalMs);
    (this.#timer as any).unref?.();
  }

  recordEventLoopDelay(delayMs: number): void { if (this.#enabled) this.#eventLoop.record(delayMs); }

  snapshot(): V2SidecarResourceSnapshot {
    if (!this.#enabled) return { processRssBytes: 0, heapUsedBytes: 0, heapTotalBytes: 0, externalBytes: 0, systemTotalBytes: null, systemFreeBytes: null, processCpuPercent: null, normalizedLoad1: null, eventLoopP50Ms: 0, eventLoopP95Ms: 0, eventLoopMaxMs: 0 };
    const memory = this.#probe.memory(); const system = this.#probe.systemMemory(); const delays = this.#eventLoop.snapshotAndReset();
    return { processRssBytes: finite(memory.rss) ?? 0, heapUsedBytes: finite(memory.heapUsed) ?? 0, heapTotalBytes: finite(memory.heapTotal) ?? 0, externalBytes: finite(memory.external) ?? 0, systemTotalBytes: system ? finite(system.total) : null, systemFreeBytes: system ? finite(system.free) : null, processCpuPercent: finite(this.#probe.processCpuPercent()), normalizedLoad1: finite(this.#probe.normalizedLoad1()), eventLoopP50Ms: delays.p50, eventLoopP95Ms: delays.p95, eventLoopMaxMs: delays.max };
  }

  async close(): Promise<void> { this.#closed = true; if (this.#timer) clearTimeout(this.#timer); this.#timer = null; }
}
