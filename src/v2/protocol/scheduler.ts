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
  readonly #queues = new Map<FramePriority, Map<string, StreamQueue<T>>>(
    PRIORITIES.map(priority => [priority, new Map()]),
  );

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
      queue = { streamId, weight, current: 0, frames: [] };
      queues.set(streamId, queue);
    } else if (queue.weight !== weight) {
      throw new V2SchedulerError("stream weight cannot change while frames are queued", "weight");
    }
    queue.frames.push(frame);
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
      const bytes = encodedBytes(frame);
      if (totalBytes + bytes > maxEncodedBytes) break;
      selected.frames.shift();
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

  snapshot(): { queuedFrames: number; streams: number } {
    let queuedFrames = 0;
    const streams = new Set<string>();
    for (const queues of this.#queues.values()) {
      for (const queue of queues.values()) {
        queuedFrames += queue.frames.length;
        streams.add(queue.streamId);
      }
    }
    return { queuedFrames, streams: streams.size };
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
