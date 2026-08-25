import { BoundedDistribution } from "./statistics";
import type { V2SidecarResourceSnapshot } from "./types";

export interface SidecarRuntimeProbe {
  memory(): {
    rss: number;
    heapUsed: number;
    heapTotal: number;
    external: number;
    arrayBuffers?: number | null;
  };
  systemMemory(): { total: number; free: number } | null;
  /** A process-local CPU delta over the probe's previous observation. */
  processCpuPercent(): number | null;
  normalizedLoad1(): number | null;
}

export interface SidecarResourceSamplerOptions {
  enabled: boolean;
  intervalMs: number;
  now?: () => number;
  probe?: SidecarRuntimeProbe;
}

const finite = (value: unknown): number | null => (
  typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : null
);

function emptySnapshot(): V2SidecarResourceSnapshot {
  return {
    processRssBytes: 0,
    heapUsedBytes: 0,
    heapTotalBytes: 0,
    externalBytes: 0,
    arrayBufferBytes: null,
    systemTotalBytes: null,
    systemFreeBytes: null,
    processCpuPercent: null,
    normalizedLoad1: null,
    eventLoopSamples: 0,
    eventLoopP50Ms: 0,
    eventLoopP95Ms: 0,
    eventLoopMaxMs: 0,
    gcSamples: 0,
    gcDurationP95Ms: 0,
    gcDurationMaxMs: 0,
  };
}

/**
 * Use only runtime APIs already present in the sidecar process. In particular,
 * this probe does not read /proc, spawn a helper, or broaden Deno permissions.
 */
function productionProbe(): SidecarRuntimeProbe {
  const deno = (globalThis as any).Deno;
  const runtimeProcess = (globalThis as any).process;
  let previousCpu: { user: number; system: number } | null = null;
  let previousCpuAtMs: number | null = null;

  return {
    memory: () => {
      const value = deno?.memoryUsage?.() ?? {};
      let processMemory: Record<string, unknown> = {};
      try { processMemory = runtimeProcess?.memoryUsage?.() ?? {}; } catch {}
      return {
        rss: finite(value.rss) ?? 0,
        heapUsed: finite(value.heapUsed) ?? 0,
        heapTotal: finite(value.heapTotal) ?? 0,
        external: finite(value.external) ?? 0,
        // Deno currently omits this field. Node-compatible runtimes may expose
        // it, so report it only when directly measured rather than estimating.
        arrayBuffers: finite(value.arrayBuffers ?? processMemory.arrayBuffers),
      };
    },
    systemMemory: () => {
      try {
        const value = deno?.systemMemoryInfo?.();
        return value
          ? { total: finite(value.total) ?? 0, free: finite(value.free ?? value.available) ?? 0 }
          : null;
      } catch { return null; }
    },
    processCpuPercent: () => {
      try {
        if (typeof runtimeProcess?.cpuUsage !== "function") return null;
        const current = runtimeProcess.cpuUsage();
        const now = performance.now();
        const parsed = {
          user: finite(current?.user),
          system: finite(current?.system),
        };
        if (parsed.user === null || parsed.system === null) return null;
        const prior = previousCpu;
        const priorAt = previousCpuAtMs;
        previousCpu = { user: parsed.user, system: parsed.system };
        previousCpuAtMs = now;
        if (!prior || priorAt === null || now <= priorAt) return null;
        const usedMicros = (parsed.user - prior.user) + (parsed.system - prior.system);
        if (usedMicros < 0) return null;
        return (usedMicros / ((now - priorAt) * 1_000)) * 100;
      } catch { return null; }
    },
    normalizedLoad1: () => {
      try {
        const load = deno?.loadavg?.()?.[0];
        const processors = Math.max(
          1,
          Number((globalThis as any).navigator?.hardwareConcurrency) || 1,
        );
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
  readonly #gc = new BoundedDistribution(128);
  #timer: ReturnType<typeof setTimeout> | null = null;
  #expected = 0;
  #latest = emptySnapshot();
  #started = false;
  #gcObserver: { disconnect(): void } | null = null;
  #closed = false;

  constructor(options: SidecarResourceSamplerOptions) {
    if (
      !Number.isSafeInteger(options.intervalMs) ||
      options.intervalMs < 100 ||
      options.intervalMs > 60_000
    ) throw new RangeError("diagnostic interval must be 100 through 60000");
    this.#enabled = options.enabled;
    this.#intervalMs = options.intervalMs;
    this.#now = options.now ?? (() => performance.now());
    this.#probe = options.probe ?? productionProbe();
  }

  get estimatedBytes(): number {
    return this.#eventLoop.estimatedBytes + this.#gc.estimatedBytes + 512;
  }

  start(): void {
    if (!this.#enabled || this.#closed || this.#started) return;
    this.#started = true;
    this.#installGcObserver();
    this.#refresh();
    this.#expected = this.#now() + this.#intervalMs;
    const tick = () => {
      this.#timer = null;
      if (this.#closed) return;
      const now = this.#now();
      this.recordEventLoopDelay(Math.max(0, now - this.#expected));
      this.#refresh();
      this.#expected = now + this.#intervalMs;
      this.#timer = setTimeout(tick, this.#intervalMs);
      (this.#timer as any).unref?.();
    };
    this.#timer = setTimeout(tick, this.#intervalMs);
    (this.#timer as any).unref?.();
  }

  recordEventLoopDelay(delayMs: number): void {
    if (this.#enabled) this.#eventLoop.record(delayMs);
  }

  /** Public for deterministic probes; production calls this from PerformanceObserver. */
  recordGcDuration(durationMs: number): void {
    if (this.#enabled) this.#gc.record(durationMs);
  }

  /**
   * Production reads a cached interval sample. This keeps every Awareness
   * control publication O(1) and, critically, does not clear either histogram.
   */
  snapshot(): V2SidecarResourceSnapshot {
    if (!this.#enabled) return emptySnapshot();
    // Unit probes and callers which deliberately do not start the timer still
    // receive a current measurement. The live sidecar always calls start().
    if (!this.#started && !this.#closed) this.#refresh();
    return { ...this.#latest };
  }

  async close(): Promise<void> {
    this.#closed = true;
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = null;
    try { this.#gcObserver?.disconnect(); } catch {}
    this.#gcObserver = null;
  }

  #refresh(): void {
    let memory: ReturnType<SidecarRuntimeProbe["memory"]> = {
      rss: 0,
      heapUsed: 0,
      heapTotal: 0,
      external: 0,
    };
    let system: ReturnType<SidecarRuntimeProbe["systemMemory"]> = null;
    let processCpuPercent: number | null = null;
    let normalizedLoad1: number | null = null;
    try { memory = this.#probe.memory(); } catch {}
    try { system = this.#probe.systemMemory(); } catch {}
    try { processCpuPercent = finite(this.#probe.processCpuPercent()); } catch {}
    try { normalizedLoad1 = finite(this.#probe.normalizedLoad1()); } catch {}
    const delays = this.#eventLoop.snapshot();
    const gc = this.#gc.snapshot();
    this.#latest = {
      processRssBytes: finite(memory.rss) ?? 0,
      heapUsedBytes: finite(memory.heapUsed) ?? 0,
      heapTotalBytes: finite(memory.heapTotal) ?? 0,
      externalBytes: finite(memory.external) ?? 0,
      arrayBufferBytes: finite(memory.arrayBuffers),
      systemTotalBytes: system ? finite(system.total) : null,
      systemFreeBytes: system ? finite(system.free) : null,
      processCpuPercent,
      normalizedLoad1,
      eventLoopSamples: delays.count,
      eventLoopP50Ms: delays.p50,
      eventLoopP95Ms: delays.p95,
      eventLoopMaxMs: delays.max,
      gcSamples: gc.count,
      gcDurationP95Ms: gc.p95,
      gcDurationMaxMs: gc.max,
    };
  }

  #installGcObserver(): void {
    try {
      const Observer = (globalThis as any).PerformanceObserver;
      if (
        typeof Observer !== "function" ||
        !Array.isArray(Observer.supportedEntryTypes) ||
        !Observer.supportedEntryTypes.includes("gc")
      ) return;
      const observer = new Observer((list: { getEntries(): Array<{ duration?: unknown }> }) => {
        for (const entry of list.getEntries()) {
          const duration = finite(entry.duration);
          if (duration !== null) this.recordGcDuration(duration);
        }
      });
      observer.observe({ entryTypes: ["gc"], buffered: false });
      this.#gcObserver = observer;
    } catch {
      this.#gcObserver = null;
    }
  }
}
