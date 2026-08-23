import { utf8Bytes } from "./shared";

export type FramePriority = "terminal" | "control" | "commit" | "data";

export class V2SchedulerError extends Error {
  readonly name = "V2SchedulerError";

  constructor(message: string, readonly code: string) {
    super(message);
  }
}

interface StreamQueue<T> {
  streamId: string;
  weight: number;
  current: number;
  frames: T[];
  bytes: number[];
  enqueuedAt: number[];
}

const PRIORITIES: readonly FramePriority[] = [
  "terminal",
  "control",
  "commit",
  "data",
];

function encodedBytes(value: unknown): number {
  try {
    return utf8Bytes(JSON.stringify(value)).byteLength;
  } catch {
    throw new V2SchedulerError("Scheduled frame must be JSON serializable", "frame");
  }
}

export class WeightedFrameScheduler<T extends object> {
  readonly #now: () => number;
  readonly #queues = new Map<FramePriority, Map<string, StreamQueue<T>>>(
    PRIORITIES.map(priority => [priority, new Map()]),
  );

  constructor(options: { now?: () => number } = {}) { this.#now = options.now ?? (() => performance.now()); }

  enqueue(
    priority: FramePriority,
    streamId: string,
    frame: T,
    weight = 1,
  ): void {
    const queues = this.#queues.get(priority);
    if (!queues) throw new V2SchedulerError("Unknown frame priority", "priority");
    if (!streamId || streamId.length > 128) {
      throw new V2SchedulerError("streamId must be 1 through 128 characters", "stream");
    }
    if (!Number.isSafeInteger(weight) || weight < 1 || weight > 1024) {
      throw new V2SchedulerError("weight must be an integer from 1 through 1024", "weight");
    }
    let queue = queues.get(streamId);
    if (!queue) {
      queue = { streamId, weight, current: 0, frames: [], bytes: [], enqueuedAt: [] };
      queues.set(streamId, queue);
    } else if (queue.weight !== weight) {
      throw new V2SchedulerError("stream weight cannot change while frames are queued", "weight");
    }
    queue.frames.push(frame); queue.bytes.push(encodedBytes(frame)); queue.enqueuedAt.push(this.#now());
  }

  take(
    maxFrames: number,
    maxEncodedBytes = Number.POSITIVE_INFINITY,
  ): T[] {
    if (!Number.isSafeInteger(maxFrames) || maxFrames < 1) {
      throw new V2SchedulerError("maxFrames must be a positive safe integer", "budget");
    }
    if (
      maxEncodedBytes !== Number.POSITIVE_INFINITY &&
      (!Number.isSafeInteger(maxEncodedBytes) || maxEncodedBytes < 1)
    ) {
      throw new V2SchedulerError("maxEncodedBytes must be positive", "budget");
    }
    const result: T[] = [];
    let totalBytes = 0;
    while (result.length < maxFrames) {
      const selected = this.#selectHighestPriority();
      if (!selected) break;
      const frame = selected.frames[0];
      const bytes = selected.bytes[0]!;
      if (totalBytes + bytes > maxEncodedBytes) break;
      selected.frames.shift();
      selected.bytes.shift(); selected.enqueuedAt.shift();
      result.push(frame);
      totalBytes += bytes;
      if (selected.frames.length === 0) {
        for (const queues of this.#queues.values()) {
          if (queues.get(selected.streamId) === selected) queues.delete(selected.streamId);
        }
      }
    }
    return result;
  }

  removeStream(streamId: string): void {
    for (const queues of this.#queues.values()) queues.delete(streamId);
  }

  snapshot(): { queuedFrames: number; streams: number; queuedEncodedBytes: number; oldestQueuedFrameMs: number } {
    let queuedFrames = 0;
    let queuedEncodedBytes = 0; let oldest = Number.POSITIVE_INFINITY;
    const streams = new Set<string>();
    for (const queues of this.#queues.values()) {
      for (const queue of queues.values()) {
        queuedFrames += queue.frames.length;
        queuedEncodedBytes += queue.bytes.reduce((sum, value) => sum + value, 0);
        if (queue.enqueuedAt.length) oldest = Math.min(oldest, queue.enqueuedAt[0]!);
        streams.add(queue.streamId);
      }
    }
    return { queuedFrames, streams: streams.size, queuedEncodedBytes, oldestQueuedFrameMs: Number.isFinite(oldest) ? Math.max(0, this.#now() - oldest) : 0 };
  }

  #selectHighestPriority(): StreamQueue<T> | null {
    for (const priority of PRIORITIES) {
      const queues = this.#queues.get(priority)!;
      if (queues.size === 0) continue;
      let selected: StreamQueue<T> | null = null;
      let totalWeight = 0;
      for (const queue of queues.values()) {
        totalWeight += queue.weight;
        queue.current += queue.weight;
        if (!selected || queue.current > selected.current) selected = queue;
      }
      selected!.current -= totalWeight;
      return selected;
    }
    return null;
  }
}
