import type { SseFrame } from "../../relay/sse";
import {
  base64UrlEncode,
  utf8Bytes,
  type SidecarFrame,
} from "../protocol/shared";
import type { FramePriority } from "../protocol/scheduler";

type ResponseBatchFrame = Extract<SidecarFrame, { kind: "response_batch" }>;
type ResponseBatchItem = ResponseBatchFrame["items"][number];

interface BatchState {
  firstEventIndex: number;
  items: ResponseBatchItem[];
  timer: ReturnType<typeof setTimeout> | null;
}

export interface ResponseBatcherOptions {
  emit(frame: ResponseBatchFrame, priority: FramePriority): void;
  flushMs?: number;
  targetBytes?: number;
}

const MAX_BATCH_ITEMS = 32;

function batchFrame(
  requestId: string,
  firstEventIndex: number,
  items: readonly ResponseBatchItem[],
): ResponseBatchFrame {
  return {
    kind: "response_batch",
    requestId,
    firstEventIndex,
    items,
  };
}

function encodedBytes(frame: ResponseBatchFrame): number {
  return utf8Bytes(JSON.stringify(frame)).byteLength;
}

export class ResponseBatcher {
  readonly #emit: ResponseBatcherOptions["emit"];
  readonly #flushMs: number;
  readonly #targetBytes: number;
  readonly #states = new Map<string, BatchState>();

  constructor(options: ResponseBatcherOptions) {
    this.#emit = options.emit;
    this.#flushMs = options.flushMs ?? 25;
    this.#targetBytes = options.targetBytes ?? 14 * 1024;
    if (
      !Number.isFinite(this.#flushMs) ||
      this.#flushMs < 1 ||
      !Number.isSafeInteger(this.#targetBytes) ||
      this.#targetBytes < 512 ||
      this.#targetBytes >= 16 * 1024
    ) {
      throw new RangeError("invalid response batch limits");
    }
  }

  push(requestId: string, eventIndex: number, event: SseFrame): boolean {
    const item: ResponseBatchItem = {
      event: event.event,
      dataBase64Url: base64UrlEncode(utf8Bytes(event.data)),
    };
    let state = this.#states.get(requestId);

    if (state && state.firstEventIndex + state.items.length !== eventIndex) {
      this.flush(requestId);
      state = undefined;
    }
    if (state && encodedBytes(batchFrame(
      requestId,
      state.firstEventIndex,
      [...state.items, item],
    )) > this.#targetBytes) {
      this.flush(requestId);
      state = undefined;
    }
    if (!state) {
      if (encodedBytes(batchFrame(requestId, eventIndex, [item])) > this.#targetBytes) {
        return false;
      }
      state = {
        firstEventIndex: eventIndex,
        items: [],
        timer: null,
      };
      this.#states.set(requestId, state);
      state.timer = setTimeout(() => this.flush(requestId), this.#flushMs);
      state.timer.unref?.();
    }

    state.items.push(item);
    if (state.items.length >= MAX_BATCH_ITEMS) this.flush(requestId);
    return true;
  }

  flush(requestId: string, priority: FramePriority = "data"): void {
    const state = this.#states.get(requestId);
    if (!state) return;
    this.#states.delete(requestId);
    if (state.timer) clearTimeout(state.timer);
    if (state.items.length === 0) return;
    this.#emit(batchFrame(
      requestId,
      state.firstEventIndex,
      Object.freeze([...state.items]),
    ), priority);
  }

  cancel(requestId: string): void {
    const state = this.#states.get(requestId);
    if (!state) return;
    this.#states.delete(requestId);
    if (state.timer) clearTimeout(state.timer);
  }

  flushAll(): void {
    for (const requestId of [...this.#states.keys()]) this.flush(requestId);
  }
}
