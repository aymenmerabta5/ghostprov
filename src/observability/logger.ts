import type { ObservabilityConfig } from "./config";
import {
  observabilityEvent,
  SANDBOX_AVAILABILITY_REASONS,
  type ObservabilityEvent,
  type ObservabilityEventName,
  type ObservabilityPriority,
} from "./events";
import { assertSafeObservabilityEvent } from "./redaction";
import { RotatingJsonlSink } from "./rotating-jsonl";
import { WEBSOCKET_CLOSE_CAUSES } from "../ws/telemetry";

const AVAILABILITY_REASONS = new Set<string>(SANDBOX_AVAILABILITY_REASONS);

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
  maxBytes: number;
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

function compactWebSocketCloseCauses(value: unknown): Record<string, number> | undefined {
  const source = record(value);
  if (!source) return undefined;
  const compact: Record<string, number> = {};
  for (const cause of WEBSOCKET_CLOSE_CAUSES) {
    const count = source[cause];
    if (typeof count === "number" && Number.isSafeInteger(count) && count > 0) {
      compact[cause] = count;
    }
  }
  return Object.keys(compact).length > 0 ? compact : undefined;
}

/** Full JSONL remains on disk; this only compacts the console representation. */
export function formatConsoleObservabilityLine(line: string): string {
  const trimmed = line.trimEnd();
  if (!trimmed.includes('"event":"fleet.sample"')) return trimmed;
  try {
    const event = record(JSON.parse(trimmed));
    if (!event || event.event !== "fleet.sample") return trimmed;
    const runtimeStates: Record<string, number> = {};
    const availabilityReasons: Record<string, number> = {};
    if (Array.isArray(event.runtimes)) {
      for (const candidate of event.runtimes) {
        const runtime = record(candidate);
        const state = typeof runtime?.state === "string" ? runtime.state : "unknown";
        runtimeStates[state] = (runtimeStates[state] ?? 0) + 1;
        const reason = runtime?.availability_reason;
        if (typeof reason === "string" && AVAILABILITY_REASONS.has(reason)) {
          availabilityReasons[reason] = (availabilityReasons[reason] ?? 0) + 1;
        }
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
      routable: event.routable,
      unavailable: event.unavailable,
      cpu_percent: event.cpu_percent,
      rss_bytes: event.rss_bytes,
      event_loop_p95_ms: event.event_loop_p95_ms,
      runtime_states: runtimeStates,
      ...(Object.keys(availabilityReasons).length ? { availability_reasons: availabilityReasons } : {}),
      pressure: resources ? {
        memory_paused: resources.memory_paused,
        governor_queued: resources.governor_queued,
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
        closed_sessions: websocket.closedSessions,
        closed_active_responses: websocket.closedActiveResponses,
        close_causes: compactWebSocketCloseCauses(websocket.closeCauses),
        pending_requests: websocket.pendingRequests,
        ingress_reserved_bytes: websocket.ingressReservedBytes,
        ingress_reserved_peak_bytes: websocket.ingressReservedBytesPeak,
        ingress_capacity_bytes: websocket.ingressCapacityBytes,
        ingress_guaranteed_sessions: websocket.ingressGuaranteedSessions,
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
  "request.model_fallback",
  "websocket.payload_rejected",
  "websocket.http_fallback",
  "monitor.error",
  "logger.dropped",
  "logger.error",
  "sandbox.recovery.started",
  "sandbox.recovery.retry_scheduled",
  "sandbox.recovery.completed",
  "sandbox.recovery.aborted",
  "sandbox.liveness",
  "prism.circuit.changed",
  "fleet.replacement.qualified",
  "fleet.replacement.started",
  "fleet.replacement.successor_reserved",
  "fleet.replacement.successor_created",
  "fleet.replacement.successor_activated",
  "fleet.replacement.old_retired",
  "fleet.mutation.blocked",
  "fleet.startup_permits.changed",
  "fleet.bootstrap_cleanup.started",
  "fleet.bootstrap_cleanup.succeeded",
  "fleet.bootstrap_cleanup.deferred",
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
      // Detailed records live in JSONL. Keep the console's sampled slot for
      // the aggregate which describes the complete fleet.
      if (event === "fleet.runtime_sample") continue;
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

/** Hard encoded-size ceiling for one event at each priority. */
const EVENT_MAX_BYTES: Readonly<Record<ObservabilityPriority, number>> = {
  terminal: 4 * 1024,
  progress: 4 * 1024,
  sample: 64 * 1024,
};
const BATCH_MAX_EVENTS = 256;
const BATCH_MAX_RESERVED_BYTES = 256 * 1024;
const EVENT_MAX_DEPTH = 32;
const EVENT_MAX_NODES = 8_192;

type JsonMeasure = { bytes: number; unknownAccessor: boolean };

class DeferredObservabilityAccessorError extends TypeError {}

/**
 * Measures retained data without serializing the complete event. String work
 * is bounded by maxBytes, and traversal stops at fixed depth/node ceilings.
 */
function boundedRetainedJsonBytes(value: unknown, maxBytes: number): JsonMeasure | null {
  let bytes = 0;
  let nodes = 0;
  let unknownAccessor = false;
  const active = new WeakSet<object>();
  const add = (amount: number): boolean => {
    bytes += amount;
    return Number.isSafeInteger(bytes) && bytes <= maxBytes;
  };
  const stringBytes = (text: string): number | null => {
    if (text.length > maxBytes) return null;
    const encoded = JSON.stringify(text);
    const length = Buffer.byteLength(encoded);
    return length <= maxBytes ? length : null;
  };
  const visit = (candidate: unknown, depth: number, arrayValue = false): boolean => {
    if (++nodes > EVENT_MAX_NODES || depth > EVENT_MAX_DEPTH) return false;
    if (candidate === null) return add(4);
    if (typeof candidate === "string") {
      const length = stringBytes(candidate);
      return length !== null && add(length);
    }
    if (typeof candidate === "boolean") return add(candidate ? 4 : 5);
    if (typeof candidate === "number") {
      return Number.isFinite(candidate) && add(Buffer.byteLength(String(candidate)));
    }
    if (candidate === undefined && !arrayValue) return true;
    if (typeof candidate !== "object") return false;
    if (active.has(candidate)) return false;
    active.add(candidate);
    try {
      if (Array.isArray(candidate)) {
        if (candidate.length > EVENT_MAX_NODES || !add(2)) return false;
        for (let index = 0; index < candidate.length; index++) {
          if (index > 0 && !add(1)) return false;
          const descriptor = Object.getOwnPropertyDescriptor(candidate, String(index));
          if (!descriptor) {
            if (!add(4)) return false;
          } else if (!("value" in descriptor)) {
            unknownAccessor = true;
          } else if (!visit(descriptor.value, depth + 1, true)) {
            return false;
          }
        }
        return true;
      }
      if (!add(2)) return false;
      let entries = 0;
      let emitted = 0;
      for (const key in candidate as Record<string, unknown>) {
        if (!Object.prototype.hasOwnProperty.call(candidate, key)) continue;
        if (++entries > EVENT_MAX_NODES) return false;
        const descriptor = Object.getOwnPropertyDescriptor(candidate, key);
        if (!descriptor || descriptor.enumerable === false) continue;
        if (emitted++ > 0 && !add(1)) return false;
        const keyBytes = stringBytes(key);
        if (keyBytes === null || !add(keyBytes + 1)) return false;
        if (!("value" in descriptor)) {
          unknownAccessor = true;
          continue;
        }
        if (!visit(descriptor.value, depth + 1)) return false;
      }
      return true;
    } finally {
      active.delete(candidate);
    }
  };
  return visit(value, 0)
    ? { bytes: unknownAccessor ? maxBytes : Math.max(1, bytes), unknownAccessor }
    : null;
}

function boundedJsonClone(value: unknown, maxBytes: number): ObservabilityEvent {
  let nodes = 0;
  let stringUnits = 0;
  const active = new WeakSet<object>();
  const clone = (candidate: unknown, depth: number): unknown => {
    if (++nodes > EVENT_MAX_NODES || depth > EVENT_MAX_DEPTH) {
      throw new RangeError("Observability event structure exceeds its bound");
    }
    if (typeof candidate === "string") {
      stringUnits += candidate.length;
      if (stringUnits > maxBytes || candidate.length > maxBytes) {
        throw new RangeError("Observability event string exceeds its bound");
      }
      return candidate;
    }
    if (candidate === null || typeof candidate === "boolean") return candidate;
    if (typeof candidate === "number") {
      if (!Number.isFinite(candidate)) throw new TypeError("Observability number is invalid");
      return candidate;
    }
    if (!candidate || typeof candidate !== "object" || active.has(candidate)) {
      throw new TypeError("Observability event is not bounded JSON");
    }
    active.add(candidate);
    try {
      if (Array.isArray(candidate)) {
        if (candidate.length > EVENT_MAX_NODES) {
          throw new RangeError("Observability array exceeds its bound");
        }
        const output = new Array<unknown>(candidate.length);
        for (let index = 0; index < candidate.length; index++) {
          const descriptor = Object.getOwnPropertyDescriptor(candidate, String(index));
          if (!descriptor) {
            output[index] = null;
          } else if (!("value" in descriptor)) {
            throw new DeferredObservabilityAccessorError(
              "Observability accessors are validated during drain",
            );
          } else {
            output[index] = clone(descriptor.value, depth + 1);
          }
        }
        return output;
      }
      const output: Record<string, unknown> = {};
      let entries = 0;
      for (const key in candidate as Record<string, unknown>) {
        if (!Object.prototype.hasOwnProperty.call(candidate, key)) continue;
        if (++entries > EVENT_MAX_NODES) {
          throw new RangeError("Observability object exceeds its bound");
        }
        stringUnits += key.length;
        if (stringUnits > maxBytes || key.length > maxBytes) {
          throw new RangeError("Observability key exceeds its bound");
        }
        const descriptor = Object.getOwnPropertyDescriptor(candidate, key);
        if (!descriptor || !("value" in descriptor)) {
          throw new DeferredObservabilityAccessorError(
            "Observability accessors are validated during drain",
          );
        }
        const child = descriptor.value;
        if (child === undefined || typeof child === "function" || typeof child === "symbol") {
          continue;
        }
        if (typeof child === "bigint") {
          throw new TypeError("Observability event contains a non-JSON value");
        }
        output[key] = clone(child, depth + 1);
      }
      return output;
    } finally {
      active.delete(candidate);
    }
  };
  const cloned = clone(value, 0) as ObservabilityEvent;
  const measured = boundedRetainedJsonBytes(cloned, maxBytes);
  if (!measured || measured.bytes > maxBytes) {
    throw new RangeError("Observability event exceeds its encoded byte limit");
  }
  return cloned;
}

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
      "request.model_fallback",
      "websocket.payload_rejected",
      "websocket.http_fallback",
      "monitor.error",
      "logger.error",
      "sandbox.recovery.started",
      "sandbox.recovery.retry_scheduled",
      "sandbox.recovery.completed",
      "sandbox.recovery.aborted",
      "sandbox.liveness",
      "prism.circuit.changed",
      "fleet.replacement.qualified",
      "fleet.replacement.started",
      "fleet.replacement.successor_reserved",
      "fleet.replacement.successor_created",
      "fleet.replacement.successor_activated",
      "fleet.replacement.old_retired",
      "fleet.mutation.blocked",
      "fleet.bootstrap_cleanup.started",
      "fleet.bootstrap_cleanup.succeeded",
      "fleet.bootstrap_cleanup.deferred",
    ].includes(eventName)) return;
    const normalizedPriority: ObservabilityPriority = priority in this.#queues
      ? priority
      : "progress";
    const maxBytes = Math.min(
      EVENT_MAX_BYTES[normalizedPriority],
      this.#config.rotateBytes,
    );
    let queuedEvent = event;
    let measured: JsonMeasure | null;
    try {
      queuedEvent = boundedJsonClone(event, maxBytes);
      measured = boundedRetainedJsonBytes(queuedEvent, maxBytes);
    } catch (error) {
      if (error instanceof DeferredObservabilityAccessorError) {
        // Accessors are not invoked on the request path. Charge the full event
        // ceiling, then validate/drop the original object during batched drain.
        measured = { bytes: maxBytes, unknownAccessor: true };
      } else {
        measured = null;
      }
    }
    if (!measured) {
      this.#recordDrop(normalizedPriority);
      return;
    }
    const item = { event: queuedEvent, reservedBytes: measured.bytes, maxBytes };
    if (!this.#makeRoom(item, normalizedPriority)) {
      this.#recordDrop(normalizedPriority);
      return;
    }
    this.#queues[normalizedPriority].push(item);
    this.#queuedBytes += item.reservedBytes;
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
        const bounded = boundedJsonClone(item.event, item.maxBytes);
        assertSafeObservabilityEvent(bounded);
        const line = `${JSON.stringify(bounded)}\n`;
        if (Buffer.byteLength(line) > item.maxBytes) {
          throw new RangeError("Observability event exceeds its encoded byte limit");
        }
        lines.push(line);
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
