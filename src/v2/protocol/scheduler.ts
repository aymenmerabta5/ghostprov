import { utf8Bytes } from "./shared";

export type FramePriority = "terminal" | "control" | "commit" | "data";

export class V2SchedulerError extends Error {
  readonly name: string = "V2SchedulerError";

  constructor(message: string, readonly code: string) {
    super(message);
  }
}

export type V2SchedulerOverflowDimension = "frames" | "bytes" | "streams";

/** A local queue bound was reached before ownership of the frame transferred. */
export class V2SchedulerOverflowError extends V2SchedulerError {
  override readonly name = "V2SchedulerOverflowError";

  constructor(
    readonly dimension: V2SchedulerOverflowDimension,
    readonly limit: number,
    readonly queued: number,
    readonly attempted: number,
  ) {
    super(
      `V2 scheduler ${dimension} limit ${limit} would be exceeded ` +
        `(queued ${queued}, attempted ${attempted})`,
      `overflow_${dimension}`,
    );
  }
}

export interface WeightedFrameSchedulerOptions {
  now?: () => number;
  maxQueuedFrames?: number;
  maxQueuedEncodedBytes?: number;
  maxStreams?: number;
  quantumBytes?: number;
}

const PRIORITIES: readonly FramePriority[] = [
  "terminal",
  "control",
  "commit",
  "data",
];

// These are last-resort local safety bounds, not provider throttles. Normal
// sidecar backpressure engages well before them.
const DEFAULT_MAX_QUEUED_FRAMES = 4_096;
const DEFAULT_MAX_QUEUED_ENCODED_BYTES = 16 * 1024 * 1024;
const DEFAULT_MAX_STREAMS = 1_024;
const DEFAULT_QUANTUM_BYTES = 16 * 1024;

interface QueueEntry<T> {
  readonly frame: T;
  readonly bytes: number;
  readonly enqueuedAt: number;
  readonly queue: StreamQueue<T>;
  prevQueue: QueueEntry<T> | null;
  nextQueue: QueueEntry<T> | null;
  prevStream: QueueEntry<T> | null;
  nextStream: QueueEntry<T> | null;
  prevGlobal: QueueEntry<T> | null;
  nextGlobal: QueueEntry<T> | null;
}

interface StreamState<T> {
  readonly streamId: string;
  head: QueueEntry<T> | null;
  tail: QueueEntry<T> | null;
}

interface StreamQueue<T> {
  readonly streamId: string;
  readonly priority: FramePriority;
  readonly weight: number;
  readonly owner: PriorityState<T>;
  head: QueueEntry<T> | null;
  tail: QueueEntry<T> | null;
  frameCount: number;
  deficitBytes: number;
  turnActive: boolean;
  previous: StreamQueue<T>;
  next: StreamQueue<T>;
}

interface PriorityState<T> {
  readonly queues: Map<string, StreamQueue<T>>;
  cursor: StreamQueue<T> | null;
  queueCount: number;
}

interface SchedulerLimits {
  readonly maxQueuedFrames: number;
  readonly maxQueuedEncodedBytes: number;
  readonly maxStreams: number;
  readonly quantumBytes: number;
}

function positiveSafeInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new V2SchedulerError(`${name} must be a positive safe integer`, "limit");
  }
  return value;
}

function encodedBytes(value: unknown): number {
  try {
    return utf8Bytes(JSON.stringify(value)).byteLength;
  } catch {
    throw new V2SchedulerError("Scheduled frame must be JSON serializable", "frame");
  }
}

type PrioritySelection<T> =
  | { readonly entry: QueueEntry<T>; readonly budgetBlocked: false }
  | { readonly entry: null; readonly budgetBlocked: boolean };

/**
 * A bounded strict-priority scheduler with byte-based weighted deficit round
 * robin inside each priority.
 *
 * All accounting and snapshots are O(1). Entries use intrusive FIFO deques,
 * avoiding Array.shift() and repeated byte/age scans under deep backlog.
 */
export class WeightedFrameScheduler<T extends object> {
  readonly #now: () => number;
  readonly #limits: SchedulerLimits;
  readonly #priorities = new Map<FramePriority, PriorityState<T>>(
    PRIORITIES.map(priority => [priority, {
      queues: new Map<string, StreamQueue<T>>(),
      cursor: null,
      queueCount: 0,
    }]),
  );
  readonly #streams = new Map<string, StreamState<T>>();

  #queuedFrames = 0;
  #queuedEncodedBytes = 0;
  #globalHead: QueueEntry<T> | null = null;
  #globalTail: QueueEntry<T> | null = null;

  constructor(options: WeightedFrameSchedulerOptions = {}) {
    this.#now = options.now ?? (() => performance.now());
    const maxQueuedFrames = positiveSafeInteger(
      options.maxQueuedFrames ?? DEFAULT_MAX_QUEUED_FRAMES,
      "maxQueuedFrames",
    );
    const maxQueuedEncodedBytes = positiveSafeInteger(
      options.maxQueuedEncodedBytes ?? DEFAULT_MAX_QUEUED_ENCODED_BYTES,
      "maxQueuedEncodedBytes",
    );
    const maxStreams = positiveSafeInteger(
      options.maxStreams ?? DEFAULT_MAX_STREAMS,
      "maxStreams",
    );
    const quantumBytes = positiveSafeInteger(
      options.quantumBytes ?? DEFAULT_QUANTUM_BYTES,
      "quantumBytes",
    );
    if (quantumBytes > Math.floor(Number.MAX_SAFE_INTEGER / 1_024)) {
      throw new V2SchedulerError("quantumBytes is too large for weighted accounting", "limit");
    }
    this.#limits = {
      maxQueuedFrames,
      maxQueuedEncodedBytes,
      maxStreams,
      quantumBytes,
    };
  }

  enqueue(
    priority: FramePriority,
    streamId: string,
    frame: T,
    weight = 1,
  ): void {
    const owner = this.#priorities.get(priority);
    if (!owner) throw new V2SchedulerError("Unknown frame priority", "priority");
    if (!streamId || streamId.length > 128) {
      throw new V2SchedulerError("streamId must be 1 through 128 characters", "stream");
    }
    if (!Number.isSafeInteger(weight) || weight < 1 || weight > 1_024) {
      throw new V2SchedulerError("weight must be an integer from 1 through 1024", "weight");
    }

    const existingQueue = owner.queues.get(streamId);
    if (existingQueue && existingQueue.weight !== weight) {
      throw new V2SchedulerError("stream weight cannot change while frames are queued", "weight");
    }

    // Serialize before any queue mutation. An overflow/error therefore leaves
    // both the frame and all accounting with the caller.
    const bytes = encodedBytes(frame);
    if (this.#queuedFrames >= this.#limits.maxQueuedFrames) {
      throw new V2SchedulerOverflowError(
        "frames",
        this.#limits.maxQueuedFrames,
        this.#queuedFrames,
        1,
      );
    }
    if (bytes > this.#limits.maxQueuedEncodedBytes - this.#queuedEncodedBytes) {
      throw new V2SchedulerOverflowError(
        "bytes",
        this.#limits.maxQueuedEncodedBytes,
        this.#queuedEncodedBytes,
        bytes,
      );
    }

    let stream = this.#streams.get(streamId);
    if (!stream) {
      if (this.#streams.size >= this.#limits.maxStreams) {
        throw new V2SchedulerOverflowError(
          "streams",
          this.#limits.maxStreams,
          this.#streams.size,
          1,
        );
      }
      stream = { streamId, head: null, tail: null };
      this.#streams.set(streamId, stream);
    }

    const queue = existingQueue ?? this.#createQueue(owner, priority, streamId, weight);
    const entry: QueueEntry<T> = {
      frame,
      bytes,
      enqueuedAt: this.#now(),
      queue,
      prevQueue: queue.tail,
      nextQueue: null,
      prevStream: stream.tail,
      nextStream: null,
      prevGlobal: this.#globalTail,
      nextGlobal: null,
    };

    if (queue.tail) queue.tail.nextQueue = entry;
    else queue.head = entry;
    queue.tail = entry;
    queue.frameCount++;

    if (stream.tail) stream.tail.nextStream = entry;
    else stream.head = entry;
    stream.tail = entry;

    if (this.#globalTail) this.#globalTail.nextGlobal = entry;
    else this.#globalHead = entry;
    this.#globalTail = entry;

    this.#queuedFrames++;
    this.#queuedEncodedBytes += bytes;
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
    while (result.length < maxFrames && this.#queuedFrames > 0) {
      const remainingBytes = maxEncodedBytes - totalBytes;
      const selected = this.#selectHighestPriority(remainingBytes);
      if (!selected.entry) {
        // A higher-priority frame exists but none can fit in the remaining
        // caller budget. Preserve strict priority rather than bypassing it.
        if (selected.budgetBlocked) break;
        // All remaining terminals may be fenced behind older data in their
        // own streams. Lower priorities are considered by the selector; this
        // branch therefore only represents an internally empty scheduler.
        break;
      }

      const { frame, bytes } = selected.entry;
      this.#dequeue(selected.entry);
      result.push(frame);
      totalBytes += bytes;
    }
    return result;
  }

  removeStream(streamId: string): void {
    const stream = this.#streams.get(streamId);
    if (!stream) return;
    let entry: QueueEntry<T> | null = stream.head;
    while (entry) {
      const next: QueueEntry<T> | null = entry.nextStream;
      this.#removeEntry(entry);
      entry = next;
    }
  }

  snapshot(): {
    queuedFrames: number;
    streams: number;
    queuedEncodedBytes: number;
    oldestQueuedFrameMs: number;
  } {
    return {
      queuedFrames: this.#queuedFrames,
      streams: this.#streams.size,
      queuedEncodedBytes: this.#queuedEncodedBytes,
      oldestQueuedFrameMs: this.#globalHead
        ? Math.max(0, this.#now() - this.#globalHead.enqueuedAt)
        : 0,
    };
  }

  #createQueue(
    owner: PriorityState<T>,
    priority: FramePriority,
    streamId: string,
    weight: number,
  ): StreamQueue<T> {
    const queue = {
      streamId,
      priority,
      weight,
      owner,
      head: null,
      tail: null,
      frameCount: 0,
      deficitBytes: 0,
      turnActive: false,
    } as StreamQueue<T>;
    if (!owner.cursor) {
      queue.previous = queue;
      queue.next = queue;
      owner.cursor = queue;
    } else {
      const tail = owner.cursor.previous;
      queue.previous = tail;
      queue.next = owner.cursor;
      tail.next = queue;
      owner.cursor.previous = queue;
    }
    owner.queues.set(streamId, queue);
    owner.queueCount++;
    return queue;
  }

  #selectHighestPriority(remainingBytes: number): PrioritySelection<T> {
    for (const priority of PRIORITIES) {
      const owner = this.#priorities.get(priority)!;
      if (!owner.cursor) continue;
      const selected = this.#selectFromPriority(owner, remainingBytes);
      if (selected.entry || selected.budgetBlocked) return selected;
    }
    return { entry: null, budgetBlocked: false };
  }

  #selectFromPriority(
    owner: PriorityState<T>,
    remainingBytes: number,
  ): PrioritySelection<T> {
    let visited = 0;
    let sawBudgetBlocked = false;
    let sawCandidateWithinBudget = false;
    let roundMultiplier = 1;
    let nextRoundMultiplier = Number.POSITIVE_INFINITY;

    while (owner.cursor) {
      const queue = owner.cursor;
      const entry = queue.head!;

      // Terminal is urgent across streams, but it must never pass an older
      // queued frame belonging to its own stream.
      if (this.#terminalIsFenced(entry)) {
        this.#endTurn(queue);
      } else if (entry.bytes > remainingBytes) {
        sawBudgetBlocked = true;
        this.#endTurn(queue);
      } else {
        sawCandidateWithinBudget = true;
        if (!queue.turnActive) {
          const entitlement = Math.min(
            this.#limits.maxQueuedEncodedBytes,
            this.#limits.quantumBytes * queue.weight,
          );
          const addition = roundMultiplier >
              Math.floor(this.#limits.maxQueuedEncodedBytes / entitlement)
            ? this.#limits.maxQueuedEncodedBytes
            : entitlement * roundMultiplier;
          queue.deficitBytes = Math.min(
            this.#limits.maxQueuedEncodedBytes,
            queue.deficitBytes + addition,
          );
          queue.turnActive = true;
        }
        if (entry.bytes <= queue.deficitBytes) {
          return { entry, budgetBlocked: false };
        }
        const entitlement = Math.min(
          this.#limits.maxQueuedEncodedBytes,
          this.#limits.quantumBytes * queue.weight,
        );
        nextRoundMultiplier = Math.min(
          nextRoundMultiplier,
          Math.ceil((entry.bytes - queue.deficitBytes) / entitlement),
        );
        this.#endTurn(queue);
      }

      visited++;
      if (visited < owner.queueCount) continue;
      if (!sawCandidateWithinBudget) {
        return { entry: null, budgetBlocked: sawBudgetBlocked };
      }

      // All fitting heads need more deficit. Start another bounded DRR round;
      // fast-forwarding empty rounds prevents a pathological tiny custom
      // quantum from turning one large frame into millions of scheduler loops.
      visited = 0;
      sawBudgetBlocked = false;
      sawCandidateWithinBudget = false;
      roundMultiplier = Math.max(1, nextRoundMultiplier);
      nextRoundMultiplier = Number.POSITIVE_INFINITY;
    }
    return { entry: null, budgetBlocked: false };
  }

  #terminalIsFenced(entry: QueueEntry<T>): boolean {
    if (entry.queue.priority !== "terminal") return false;
    return this.#streams.get(entry.queue.streamId)?.head !== entry;
  }

  #endTurn(queue: StreamQueue<T>): void {
    queue.turnActive = false;
    queue.owner.cursor = queue.next;
  }

  #dequeue(entry: QueueEntry<T>): void {
    const queue = entry.queue;
    queue.deficitBytes = Math.max(0, queue.deficitBytes - entry.bytes);
    this.#removeEntry(entry);
    if (queue.frameCount === 0) return;

    const next = queue.head!;
    if (!this.#terminalIsFenced(next) && next.bytes <= queue.deficitBytes) {
      queue.turnActive = true;
      queue.owner.cursor = queue;
      return;
    }
    this.#endTurn(queue);
  }

  #removeEntry(entry: QueueEntry<T>): void {
    const queue = entry.queue;
    const stream = this.#streams.get(queue.streamId)!;

    if (entry.prevQueue) entry.prevQueue.nextQueue = entry.nextQueue;
    else queue.head = entry.nextQueue;
    if (entry.nextQueue) entry.nextQueue.prevQueue = entry.prevQueue;
    else queue.tail = entry.prevQueue;
    queue.frameCount--;

    if (entry.prevStream) entry.prevStream.nextStream = entry.nextStream;
    else stream.head = entry.nextStream;
    if (entry.nextStream) entry.nextStream.prevStream = entry.prevStream;
    else stream.tail = entry.prevStream;

    if (entry.prevGlobal) entry.prevGlobal.nextGlobal = entry.nextGlobal;
    else this.#globalHead = entry.nextGlobal;
    if (entry.nextGlobal) entry.nextGlobal.prevGlobal = entry.prevGlobal;
    else this.#globalTail = entry.prevGlobal;

    this.#queuedFrames--;
    this.#queuedEncodedBytes -= entry.bytes;

    if (queue.frameCount === 0) this.#removeQueue(queue);
    if (!stream.head) this.#streams.delete(stream.streamId);
  }

  #removeQueue(queue: StreamQueue<T>): void {
    const owner = queue.owner;
    if (owner.queueCount === 1) {
      owner.cursor = null;
    } else {
      queue.previous.next = queue.next;
      queue.next.previous = queue.previous;
      if (owner.cursor === queue) owner.cursor = queue.next;
    }
    owner.queues.delete(queue.streamId);
    owner.queueCount--;
    queue.deficitBytes = 0;
    queue.turnActive = false;
  }
}
