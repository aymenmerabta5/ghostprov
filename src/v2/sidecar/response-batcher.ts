import type { SseFrame } from "../../relay/sse";
import {
  base64UrlEncode,
  utf8Bytes,
  type SidecarFrame,
} from "../protocol/shared";
import type { FramePriority } from "../protocol/scheduler";
import { utf8ByteLength, utf8ByteLengthChunked } from "../protocol/transcript";

type ResponseBatchFrame = Extract<SidecarFrame, { kind: "response_batch" }>;
type ResponseBatchItem = ResponseBatchFrame["items"][number];

interface BatchState {
  firstEventIndex: number;
  items: ResponseBatchItem[];
  encodedBytes: number;
  timer: ReturnType<typeof setTimeout> | null;
}

export interface ResponseBatcherOptions {
  emit(frame: ResponseBatchFrame, priority: FramePriority): void;
  flushMs?: number;
  targetBytes?: number;
}

export interface EncodedResponseEventPart {
  partIndex: number;
  partCount: number;
  offset: number;
  dataBase64Url: string;
}

export interface StreamResponseEventPartsOptions {
  partBytes?: number;
  /** Known parser byte count avoids a second full-string length pass. */
  byteCount?: number;
  yieldTask?: () => Promise<void>;
  yieldEveryBytes?: number;
  onScratchBytes?(bytes: number): void;
}

/** 8 KiB raw stays safely below the 16 KiB JSON/base64 physical frame cap. */
export const MAX_RESPONSE_EVENT_PART_BYTES = 8 * 1024;
const MAX_RESPONSE_EVENT_PART_COUNT = 65_536;

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
  return utf8ByteLength(JSON.stringify(frame));
}

function base64UrlLength(bytes: number): number {
  const complete = Math.floor(bytes / 3) * 4;
  const remainder = bytes % 3;
  return complete + (remainder === 0 ? 0 : remainder + 1);
}

interface PreparedBatchItem {
  item: ResponseBatchItem;
  encodedBytes: number;
}

const EMPTY_ITEM_BYTES = utf8ByteLength(JSON.stringify({
  event: "",
  dataBase64Url: "",
}));
const SAFE_BATCH_EVENT = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;

/** JSON.stringify-compatible UTF-8 size for one string, including quotes. */
function jsonStringBytes(value: string): number {
  let total = 2;
  for (let index = 0; index < value.length; index++) {
    const code = value.charCodeAt(index);
    if (code === 0x22 || code === 0x5c || code === 0x08 || code === 0x09 ||
      code === 0x0a || code === 0x0c || code === 0x0d) {
      total += 2;
    } else if (code <= 0x1f) {
      total += 6;
    } else if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (next >= 0xdc00 && next <= 0xdfff) {
        total += 4;
        index++;
      } else {
        // Well-formed JSON.stringify escapes lone surrogates as \udxxx.
        total += 6;
      }
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      total += 6;
    } else if (code <= 0x7f) {
      total++;
    } else if (code <= 0x7ff) {
      total += 2;
    } else {
      total += 3;
    }
  }
  return total;
}

function predictedItemBytes(
  event: SseFrame,
  maximumItemBytes: number,
): { dataBytes: number; itemBytes: number } | null {
  if (!SAFE_BATCH_EVENT.test(event.event)) return null;
  const fixedBytes = EMPTY_ITEM_BYTES + jsonStringBytes(event.event) - 2;
  // UTF-8 is never shorter than the number of UTF-16 code units. This O(1)
  // lower bound rejects jumbo events without a full synchronous string scan.
  if (fixedBytes + base64UrlLength(event.data.length) > maximumItemBytes) return null;
  const dataBytes = utf8ByteLength(event.data);
  return {
    dataBytes,
    itemBytes: fixedBytes + base64UrlLength(dataBytes),
  };
}

function yieldResponseTask(): Promise<void> {
  return new Promise(resolve => {
    const timer = setTimeout(resolve, 0);
    timer.unref?.();
  });
}

/**
 * Incrementally encode/base64-segment one event without a full UTF-8 buffer.
 * Byte boundaries are identical to slicing TextEncoder output, including when
 * a multi-byte code point crosses a physical part boundary.
 */
export async function streamResponseEventParts(
  data: string,
  onPart: (part: EncodedResponseEventPart) => void | Promise<void>,
  options: StreamResponseEventPartsOptions = {},
): Promise<{ byteCount: number; partCount: number }> {
  const partBytes = options.partBytes ?? 8 * 1024;
  if (
    !Number.isSafeInteger(partBytes) ||
    partBytes < 1 ||
    partBytes > MAX_RESPONSE_EVENT_PART_BYTES
  ) throw new RangeError("partBytes must be between 1 and 8 KiB");
  const yieldTask = options.yieldTask ?? yieldResponseTask;
  const yieldEveryBytes = options.yieldEveryBytes ?? 64 * 1024;
  if (!Number.isSafeInteger(yieldEveryBytes) || yieldEveryBytes < 1) {
    throw new RangeError("yieldEveryBytes must be a positive safe integer");
  }
  const byteCount = options.byteCount === undefined
    ? await utf8ByteLengthChunked(data, { yieldTask })
    : options.byteCount;
  if (!Number.isSafeInteger(byteCount) || byteCount < 0) {
    throw new RangeError("byteCount must be a non-negative safe integer");
  }
  const partCount = Math.max(1, Math.ceil(byteCount / partBytes));
  if (partCount > MAX_RESPONSE_EVENT_PART_COUNT) {
    throw new RangeError("response event exceeds the protocol part-count limit");
  }
  const scratch = new Uint8Array(partBytes);
  options.onScratchBytes?.(scratch.byteLength);
  let scratchLength = 0;
  let partIndex = 0;
  let offset = 0;
  let workSinceYield = 0;
  let yieldBeforeMoreWork = false;
  const flush = async (allowEmpty = false): Promise<void> => {
    if (scratchLength === 0 && !allowEmpty) return;
    const length = scratchLength;
    await onPart({
      partIndex,
      partCount,
      offset,
      dataBase64Url: base64UrlEncode(scratch.subarray(0, length)),
    });
    partIndex++;
    offset += length;
    workSinceYield += length;
    if (workSinceYield >= yieldEveryBytes) yieldBeforeMoreWork = true;
    scratchLength = 0;
  };
  for (let index = 0; index < data.length; index++) {
    if (yieldBeforeMoreWork) {
      yieldBeforeMoreWork = false;
      workSinceYield = 0;
      await yieldTask();
    }
    const first = data.charCodeAt(index);
    let codePoint: number;
    if (first >= 0xd800 && first <= 0xdbff) {
      const second = data.charCodeAt(index + 1);
      if (second >= 0xdc00 && second <= 0xdfff) {
        codePoint = 0x1_0000 + ((first - 0xd800) << 10) + second - 0xdc00;
        index++;
      } else {
        codePoint = 0xfffd;
      }
    } else if (first >= 0xdc00 && first <= 0xdfff) {
      codePoint = 0xfffd;
    } else {
      codePoint = first;
    }

    let encodedLength: number;
    let byte0 = 0;
    let byte1 = 0;
    let byte2 = 0;
    let byte3 = 0;
    if (codePoint <= 0x7f) {
      byte0 = codePoint;
      encodedLength = 1;
    } else if (codePoint <= 0x7ff) {
      byte0 = 0xc0 | (codePoint >>> 6);
      byte1 = 0x80 | (codePoint & 0x3f);
      encodedLength = 2;
    } else if (codePoint <= 0xffff) {
      byte0 = 0xe0 | (codePoint >>> 12);
      byte1 = 0x80 | ((codePoint >>> 6) & 0x3f);
      byte2 = 0x80 | (codePoint & 0x3f);
      encodedLength = 3;
    } else {
      byte0 = 0xf0 | (codePoint >>> 18);
      byte1 = 0x80 | ((codePoint >>> 12) & 0x3f);
      byte2 = 0x80 | ((codePoint >>> 6) & 0x3f);
      byte3 = 0x80 | (codePoint & 0x3f);
      encodedLength = 4;
    }
    for (let byteIndex = 0; byteIndex < encodedLength; byteIndex++) {
      scratch[scratchLength++] = byteIndex === 0
        ? byte0
        : byteIndex === 1
        ? byte1
        : byteIndex === 2
        ? byte2
        : byte3;
      if (scratchLength === scratch.byteLength) await flush();
    }
  }
  if (scratchLength > 0) await flush();
  else if (byteCount === 0) await flush(true);
  if (partIndex !== partCount || offset !== byteCount) {
    throw new Error("Response event segmentation is inconsistent");
  }
  return { byteCount, partCount };
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
      this.#targetBytes > 14 * 1024
    ) {
      throw new RangeError("invalid response batch limits");
    }
  }

  /**
   * Allocation-free preflight used by event emitters before considering the
   * batching path. In particular, a jumbo event never gets a speculative full
   * UTF-8 buffer or base64 string merely to discover that it needs segmentation.
   */
  canBatch(requestId: string, eventIndex: number, event: SseFrame): boolean {
    const emptyBytes = encodedBytes(batchFrame(requestId, eventIndex, []));
    const predicted = predictedItemBytes(event, this.#targetBytes - emptyBytes);
    return predicted !== null && emptyBytes + predicted.itemBytes <= this.#targetBytes;
  }

  push(requestId: string, eventIndex: number, event: SseFrame): boolean {
    let state = this.#states.get(requestId);

    if (state && state.firstEventIndex + state.items.length !== eventIndex) {
      this.flush(requestId);
      state = undefined;
    }
    const emptyBytes = encodedBytes(batchFrame(requestId, eventIndex, []));
    const maximumItemBytes = state
      ? this.#targetBytes - state.encodedBytes - (state.items.length > 0 ? 1 : 0)
      : this.#targetBytes - emptyBytes;
    let predicted = predictedItemBytes(event, maximumItemBytes);
    if (!predicted && state) {
      this.flush(requestId);
      state = undefined;
      predicted = predictedItemBytes(event, this.#targetBytes - emptyBytes);
    }
    if (!predicted) return false;
    const commaBytes = state && state.items.length > 0 ? 1 : 0;
    if (state && state.encodedBytes + commaBytes + predicted.itemBytes > this.#targetBytes) {
      this.flush(requestId);
      state = undefined;
    }
    if (!state) {
      if (emptyBytes + predicted.itemBytes > this.#targetBytes) {
        return false;
      }
      state = {
        firstEventIndex: eventIndex,
        items: [],
        encodedBytes: emptyBytes,
        timer: null,
      };
      this.#states.set(requestId, state);
      state.timer = setTimeout(() => this.flush(requestId), this.#flushMs);
      state.timer.unref?.();
    }

    // This conversion is reached only for an event proven to fit in a small
    // physical batch. It is therefore bounded by targetBytes (<16 KiB).
    const prepared: PreparedBatchItem = {
      item: {
        event: event.event,
        dataBase64Url: base64UrlEncode(utf8Bytes(event.data)),
      },
      encodedBytes: predicted.itemBytes,
    };
    if (state.items.length > 0) state.encodedBytes++;
    state.items.push(prepared.item);
    state.encodedBytes += prepared.encodedBytes;
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
