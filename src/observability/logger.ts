import type { ObservabilityConfig } from "./config";
import {
  observabilityEvent,
  type ObservabilityEvent,
  type ObservabilityEventName,
  type ObservabilityPriority,
} from "./events";
import { assertSafeObservabilityEvent } from "./redaction";
import { RotatingJsonlSink } from "./rotating-jsonl";

export interface ObservabilitySink {
  writeBatch(lines: readonly string[]): Promise<void>;
  close(): Promise<void>;
}

export interface LoggerSnapshot {
  queuedEvents: number;
  /** Conservative queue reservations; serialization happens off the emit path. */
  queuedBytes: number;
  droppedSamples: number;
  droppedProgress: number;
  droppedTerminals: number;
  sinkErrors: number;
}

export interface LoggerLike {
  emit(event: ObservabilityEvent, priority?: ObservabilityPriority): void;
  snapshot(): LoggerSnapshot;
  close(deadlineMs?: number): Promise<void>;
}

interface QueuedEvent {
  event: ObservabilityEvent;
  reservedBytes: number;
}

/** O(1) head removal with infrequent amortized compaction. */
class HeadIndexedQueue<T> {
  #items: Array<T | undefined> = [];
  #head = 0;

  get length(): number { return this.#items.length - this.#head; }

  push(value: T): void { this.#items.push(value); }

  peek(): T | undefined { return this.#items[this.#head]; }

  shift(): T | undefined {
    if (this.#head >= this.#items.length) return undefined;
    const value = this.#items[this.#head];
    this.#items[this.#head++] = undefined;
    if (this.#head === this.#items.length) {
      this.#items.length = 0;
      this.#head = 0;
    } else if (this.#head >= 1_024 && this.#head * 2 >= this.#items.length) {
      this.#items = this.#items.slice(this.#head);
      this.#head = 0;
    }
    return value;
  }
}

function record(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function compactRelease(value: unknown): unknown {
  const release = record(value);
  if (!release || !Array.isArray(release.selections)) return null;
  const selections = release.selections.slice(0, 8);
  return {
    knownRuntimes: release.knownRuntimes,
    unknownRuntimes: release.unknownRuntimes,
    selections,
    selectionCount: release.selections.length,
    truncated: release.selections.length > selections.length,
  };
}

/** Full JSONL remains on disk; this only compacts the console representation. */
export function formatConsoleObservabilityLine(line: string): string {
  const trimmed = line.trimEnd();
  if (!trimmed.includes('"event":"fleet.sample"')) return trimmed;
  try {
    const event = record(JSON.parse(trimmed));
    if (!event || event.event !== "fleet.sample") return trimmed;
    const runtimeStates: Record<string, number> = {};
    if (Array.isArray(event.runtimes)) {
      for (const candidate of event.runtimes) {
        const runtime = record(candidate);
        const state = typeof runtime?.state === "string" ? runtime.state : "unknown";
        runtimeStates[state] = (runtimeStates[state] ?? 0) + 1;
      }
    }
    const resources = record(event.resources);
    const traffic = record(event.traffic);
    const trafficTotal = record(traffic?.total);
    const websocket = record(event.websocket);
    return JSON.stringify({
      schema: event.schema,
      event: event.event,
      ts: event.ts,
      capacity: event.capacity,
      active: event.active,
      available: event.available,
      queued: event.queued,
      queued_bytes: event.queued_bytes,
      desired: event.desired,
      ready: event.ready,
      unavailable: event.unavailable,
      cpu_percent: event.cpu_percent,
      rss_bytes: event.rss_bytes,
      event_loop_p95_ms: event.event_loop_p95_ms,
      runtime_states: runtimeStates,
      pressure: resources ? {
        memory_paused: resources.memory_paused,
        governor_queued: resources.governor_queued,
        cpu_queued: resources.cpu_queued,
        transform_queued: resources.transform_queued,
      } : null,
      logger: event.logger,
      traffic: traffic ? {
        requests_per_second: traffic.requests_per_second,
        success_rate: traffic.success_rate,
        failed: traffic.failed,
        total_p95_ms: trafficTotal?.p95_ms,
      } : null,
      websocket: websocket ? {
        active_sessions: websocket.activeSessions,
        pending_requests: websocket.pendingRequests,
        ingress_reserved_bytes: websocket.ingressReservedBytes,
        ingress_reserved_peak_bytes: websocket.ingressReservedBytesPeak,
        ingress_capacity_bytes: websocket.ingressCapacityBytes,
        ingress_promotions: websocket.ingressPromotions,
        ingress_rejections: websocket.ingressRejections,
        ingress_parser_rejections: websocket.ingressParserRejections,
        ingress_upgrade_rejections: websocket.ingressUpgradeRejections,
        response_graph_rejections: websocket.responseGraphRejections,
      } : null,
      release: compactRelease(event.release),
    });
  } catch {
    return trimmed;
  }
}

const CONSOLE_ALWAYS = new Set<ObservabilityEventName>([
  "server.started",
  "server.stopping",
  "request.completed",
  "request.failed",
  "monitor.error",
  "logger.dropped",
  "logger.error",
  "sandbox.recovery.started",
  "sandbox.recovery.completed",
  "sandbox.recovery.aborted",
  "prism.circuit.changed",
]);

function lineEvent(line: string): ObservabilityEventName | null {
  const match = /"event":"([a-z._]+)"/.exec(line);
  return match?.[1] as ObservabilityEventName ?? null;
}

export interface ConsoleSinkOptions {
  now?: () => number;
  write?: (batch: string) => void;
  progressIntervalMs?: number;
}

/**
 * At most one sampled/progress record per interval. Terminal/error records are
 * never sampled, but a complete sink batch uses one console write rather than
 * one synchronous write per request.
 */
export class ConsoleSink implements ObservabilitySink {
  readonly #now: () => number;
  readonly #write: (batch: string) => void;
  readonly #progressIntervalMs: number;
  #nextProgressAtMs = 0;

  constructor(options: ConsoleSinkOptions = {}) {
    this.#now = options.now ?? Date.now;
    this.#write = options.write ?? (batch => console.log(batch));
    this.#progressIntervalMs = options.progressIntervalMs ?? 1_000;
    if (!Number.isSafeInteger(this.#progressIntervalMs) || this.#progressIntervalMs < 100) {
      throw new RangeError("console progress interval must be at least 100ms");
    }
  }

  async writeBatch(lines: readonly string[]): Promise<void> {
    const output: string[] = [];
    let sampled = false;
    const now = this.#now();
    for (const line of lines) {
      const event = lineEvent(line);
      if (event && CONSOLE_ALWAYS.has(event)) {
        output.push(formatConsoleObservabilityLine(line));
      } else if (!sampled && now >= this.#nextProgressAtMs) {
        sampled = true;
        this.#nextProgressAtMs = now + this.#progressIntervalMs;
        output.push(formatConsoleObservabilityLine(line));
      }
    }
    if (output.length) this.#write(output.join("\n"));
  }

  async close(): Promise<void> {}
}

const RESERVATION_BYTES: Readonly<Record<ObservabilityPriority, number>> = {
  terminal: 2 * 1024,
  progress: 1024,
  sample: 64 * 1024,
};
const BATCH_MAX_EVENTS = 256;
const BATCH_MAX_RESERVED_BYTES = 256 * 1024;

export class ObservabilityLogger implements LoggerLike {
  readonly #config: ObservabilityConfig;
  readonly #sinks: readonly ObservabilitySink[];
  readonly #queues: Record<ObservabilityPriority, HeadIndexedQueue<QueuedEvent>> = {
    terminal: new HeadIndexedQueue(),
    progress: new HeadIndexedQueue(),
    sample: new HeadIndexedQueue(),
  };
  #queuedBytes = 0;
  #droppedSamples = 0;
  #droppedProgress = 0;
  #droppedTerminals = 0;
  #sinkErrors = 0;
  #timer: ReturnType<typeof setTimeout> | null = null;
  #flushChain: Promise<void> = Promise.resolve();
  #closed = false;

  constructor(options: {
    config: ObservabilityConfig;
    sinks: readonly ObservabilitySink[];
  }) {
    this.#config = options.config;
    this.#sinks = Object.freeze([...options.sinks]);
  }

  emit(event: ObservabilityEvent, priority: ObservabilityPriority = "progress"): void {
    if (this.#closed || this.#config.level === "off") return;
    let eventName: unknown;
    try { eventName = event?.event; } catch {
      this.#sinkErrors++;
      return;
    }
    if (typeof eventName !== "string") {
      this.#sinkErrors++;
      return;
    }
    if (this.#config.level === "error" && ![
      "request.failed",
      "monitor.error",
      "logger.error",
      "sandbox.recovery.started",
      "sandbox.recovery.completed",
      "sandbox.recovery.aborted",
      "prism.circuit.changed",
    ].includes(eventName)) return;
    const normalizedPriority: ObservabilityPriority = priority in this.#queues
      ? priority
      : "progress";
    const reservedBytes = RESERVATION_BYTES[normalizedPriority];
    const item = { event, reservedBytes };
    if (!this.#makeRoom(item, normalizedPriority)) {
      this.#recordDrop(normalizedPriority);
      return;
    }
    this.#queues[normalizedPriority].push(item);
    this.#queuedBytes += reservedBytes;
    this.#scheduleFlush();
  }

  snapshot(): LoggerSnapshot {
    return {
      queuedEvents: this.#eventCount(),
      queuedBytes: this.#queuedBytes,
      droppedSamples: this.#droppedSamples,
      droppedProgress: this.#droppedProgress,
      droppedTerminals: this.#droppedTerminals,
      sinkErrors: this.#sinkErrors,
    };
  }

  async close(deadlineMs = 5_000): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = null;
    const draining = this.#flushChain.then(() => this.#drainAll());
    await Promise.race([
      draining,
      new Promise<void>(resolve => setTimeout(resolve, deadlineMs)),
    ]);
    await Promise.allSettled(this.#sinks.map(sink => sink.close()));
  }

  #eventCount(): number {
    return this.#queues.terminal.length +
      this.#queues.progress.length +
      this.#queues.sample.length;
  }

  #fits(item: QueuedEvent): boolean {
    return this.#eventCount() + 1 <= this.#config.queueMaxEvents &&
      this.#queuedBytes + item.reservedBytes <= this.#config.queueMaxBytes;
  }

  /** sample -> progress -> reject; queued terminals are never evicted. */
  #makeRoom(item: QueuedEvent, priority: ObservabilityPriority): boolean {
    while (!this.#fits(item)) {
      if (this.#evict("sample")) continue;
      if (priority === "terminal" && this.#evict("progress")) continue;
      return false;
    }
    return true;
  }

  #evict(priority: "sample" | "progress"): boolean {
    const item = this.#queues[priority].shift();
    if (!item) return false;
    this.#queuedBytes = Math.max(0, this.#queuedBytes - item.reservedBytes);
    this.#recordDrop(priority);
    return true;
  }

  #recordDrop(priority: ObservabilityPriority): void {
    if (priority === "sample") this.#droppedSamples++;
    else if (priority === "progress") this.#droppedProgress++;
    else this.#droppedTerminals++;
  }

  #scheduleFlush(): void {
    if (this.#timer || this.#closed) return;
    this.#timer = setTimeout(() => {
      this.#timer = null;
      this.#flushChain = this.#flushChain
        .then(() => this.#drainOnce())
        .catch(() => { this.#sinkErrors++; });
    }, this.#config.flushMs);
    this.#timer.unref?.();
  }

  #takeBatch(): QueuedEvent[] {
    const events: QueuedEvent[] = [];
    let reservedBytes = 0;
    for (const priority of ["terminal", "progress", "sample"] as const) {
      for (;;) {
        const next = this.#queues[priority].peek();
        if (!next) break;
        if (events.length >= BATCH_MAX_EVENTS) return events;
        if (events.length && reservedBytes + next.reservedBytes > BATCH_MAX_RESERVED_BYTES) {
          return events;
        }
        this.#queues[priority].shift();
        this.#queuedBytes = Math.max(0, this.#queuedBytes - next.reservedBytes);
        events.push(next);
        reservedBytes += next.reservedBytes;
      }
    }
    return events;
  }

  #dropReport(): string | null {
    if (!this.#droppedSamples && !this.#droppedProgress && !this.#droppedTerminals) return null;
    const report = observabilityEvent("logger.dropped", {
      samples: this.#droppedSamples,
      progress: this.#droppedProgress,
      terminals: this.#droppedTerminals,
      queued_events: this.#eventCount(),
      queued_bytes: this.#queuedBytes,
    });
    this.#droppedSamples = 0;
    this.#droppedProgress = 0;
    this.#droppedTerminals = 0;
    return `${JSON.stringify(report)}\n`;
  }

  #serialize(events: readonly QueuedEvent[]): string[] {
    const lines: string[] = [];
    for (const item of events) {
      try {
        // Privacy validation and payload serialization stay off request emit().
        assertSafeObservabilityEvent(item.event);
        lines.push(`${JSON.stringify(item.event)}\n`);
      } catch {
        this.#sinkErrors++;
      }
    }
    return lines;
  }

  async #write(events: readonly QueuedEvent[]): Promise<void> {
    const lines = this.#serialize(events);
    const report = this.#dropReport();
    if (report) lines.push(report);
    if (!lines.length) return;
    const results = await Promise.allSettled(
      this.#sinks.map(sink => sink.writeBatch(lines)),
    );
    const failures = results.filter(result => result.status === "rejected").length;
    if (failures) {
      this.#sinkErrors += failures;
      console.error(`[observability] ${failures} logging sink(s) failed`);
    }
  }

  async #drainOnce(): Promise<void> {
    await this.#write(this.#takeBatch());
    if (this.#eventCount() > 0) this.#scheduleFlush();
  }

  async #drainAll(): Promise<void> {
    while (this.#eventCount() > 0) await this.#write(this.#takeBatch());
    await this.#write([]);
  }
}

export function createObservabilityLogger(
  config: ObservabilityConfig,
  prefix = "ghostprov-v2",
): ObservabilityLogger {
  const sinks: ObservabilitySink[] = [new ConsoleSink()];
  if (config.level !== "off") {
    sinks.push(new RotatingJsonlSink({
      directory: config.directory,
      prefix,
      rotateBytes: config.rotateBytes,
      retainFiles: config.retainFiles,
    }));
  }
  return new ObservabilityLogger({ config, sinks });
}
