import type { ObservabilityConfig } from "./config";
import {
  observabilityEvent,
  type ObservabilityEvent,
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

interface QueuedLine {
  line: string;
  bytes: number;
}

class ConsoleSink implements ObservabilitySink {
  async writeBatch(lines: readonly string[]): Promise<void> {
    for (const line of lines) console.log(line.trimEnd());
  }
  async close(): Promise<void> {}
}

export class ObservabilityLogger implements LoggerLike {
  readonly #config: ObservabilityConfig;
  readonly #sinks: readonly ObservabilitySink[];
  readonly #queues: Record<ObservabilityPriority, QueuedLine[]> = {
    terminal: [],
    progress: [],
    sample: [],
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
    if (this.#config.level === "error" && ![
      "request.failed",
      "monitor.error",
      "logger.error",
    ].includes(event.event)) return;
    try {
      assertSafeObservabilityEvent(event);
      const line = `${JSON.stringify(event)}\n`;
      const item = { line, bytes: Buffer.byteLength(line) };
      if (!this.#makeRoom(item, priority)) {
        this.#recordDrop(priority);
        return;
      }
      this.#queues[priority].push(item);
      this.#queuedBytes += item.bytes;
      this.#scheduleFlush();
    } catch {
      this.#sinkErrors++;
    }
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

  #fits(item: QueuedLine): boolean {
    return this.#eventCount() + 1 <= this.#config.queueMaxEvents &&
      this.#queuedBytes + item.bytes <= this.#config.queueMaxBytes;
  }

  #makeRoom(item: QueuedLine, priority: ObservabilityPriority): boolean {
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
    this.#queuedBytes -= item.bytes;
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

  #takeBatch(): string[] {
    const lines: string[] = [];
    let bytes = 0;
    for (const priority of ["terminal", "progress", "sample"] as const) {
      while (this.#queues[priority].length && lines.length < 256) {
        const next = this.#queues[priority][0]!;
        if (lines.length && bytes + next.bytes > 256 * 1024) return lines;
        this.#queues[priority].shift();
        this.#queuedBytes -= next.bytes;
        lines.push(next.line);
        bytes += next.bytes;
      }
    }
    return lines;
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

  async #write(lines: string[]): Promise<void> {
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
