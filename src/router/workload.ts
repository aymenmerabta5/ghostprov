import {
  MAX_RESPONSES_JSON_DEPTH,
  MAX_RESPONSES_JSON_ITEMS,
} from "../relay/responses-validation";

export type WorkloadClass =
  | "text-small"
  | "text-large"
  | "image-generation"
  | "image-edit";

export interface WorkloadDescriptor {
  class: WorkloadClass;
  requestBytes: number;
  reservedBytes: number;
  stream: boolean;
  model?: string;
}

export interface WorkloadOptions {
  encodedBytes?: number;
  /** Exact JSON structural bytes already measured by the caller. */
  structuralBytes?: number;
}

const MiB = 1024 ** 2;
export const MAX_WORKLOAD_JSON_ITEMS = MAX_RESPONSES_JSON_ITEMS;

export class WorkloadTraversalLimitError extends Error {
  readonly name = "WorkloadTraversalLimitError";
  readonly status = 400;
  readonly code = "invalid_json_structure";
  readonly param = "body";

  constructor(readonly reason: "depth" | "items") {
    super(reason === "depth"
      ? `Request body exceeds the maximum JSON nesting depth of ${MAX_RESPONSES_JSON_DEPTH}`
      : `Request body contains more than ${MAX_WORKLOAD_JSON_ITEMS} JSON values`);
  }
}

/**
 * Counts a JSON string without materializing its escaped representation.
 * Request bodies can contain multi-megabyte prompts/base64 images, so keeping
 * a second serialized copy solely for admission accounting is prohibitively
 * expensive under concurrency.
 */
function jsonStringUnitBytes(value: string, index: number): { bytes: number; width: number } {
  const unit = value.charCodeAt(index);
    if (unit <= 0x7f) {
    return {
      bytes: unit === 0x22 || unit === 0x5c || unit === 0x08 || unit === 0x09 ||
          unit === 0x0a || unit === 0x0c || unit === 0x0d
        ? 2
        : unit <= 0x1f ? 6 : 1,
      width: 1,
    };
    }
    if (unit <= 0x7ff) {
    return { bytes: 2, width: 1 };
    }
    if (unit >= 0xd800 && unit <= 0xdbff) {
      const low = value.charCodeAt(index + 1);
      if (low >= 0xdc00 && low <= 0xdfff) {
      return { bytes: 4, width: 2 };
      }
    return { bytes: 6, width: 1 };
    }
  return { bytes: unit >= 0xdc00 && unit <= 0xdfff ? 6 : 3, width: 1 };
}

function jsonStringBytes(value: string): number {
  // Native UTF-8 sizing covers the overwhelmingly common prompt/base64 path
  // without a JavaScript loop. Adjust only JSON escapes and lone surrogates;
  // paired surrogates already have their exact four-byte UTF-8 width.
  let bytes = Buffer.byteLength(value, "utf8") + 2;
  const special = /["\\\u0000-\u001f\uD800-\uDFFF]/g;
  for (let match = special.exec(value); match; match = special.exec(value)) {
    const index = match.index;
    const unit = value.charCodeAt(index);
    if (unit === 0x22 || unit === 0x5c || unit === 0x08 || unit === 0x09 ||
      unit === 0x0a || unit === 0x0c || unit === 0x0d) {
      bytes += 1;
      continue;
    }
    if (unit <= 0x1f) {
      bytes += 5;
      continue;
    }
    const pairedHigh = unit >= 0xd800 && unit <= 0xdbff &&
      value.charCodeAt(index + 1) >= 0xdc00 && value.charCodeAt(index + 1) <= 0xdfff;
    const pairedLow = unit >= 0xdc00 && unit <= 0xdfff &&
      value.charCodeAt(index - 1) >= 0xd800 && value.charCodeAt(index - 1) <= 0xdbff;
    if (!pairedHigh && !pairedLow) bytes += 3;
  }
  return bytes;
}

interface JsonValueFrame {
  kind: "value";
  value: unknown;
  depth: number;
  arrayValue: boolean;
}

interface JsonArrayFrame {
  kind: "array";
  value: unknown[];
  depth: number;
  index: number;
}

interface JsonObjectFrame {
  kind: "object";
  value: Record<string, unknown>;
  depth: number;
  keys: string[];
  index: number;
  entries: number;
}

type JsonMeasureFrame = JsonValueFrame | JsonArrayFrame | JsonObjectFrame;

function unsupportedJsonObjectValue(value: unknown): boolean {
  return typeof value === "undefined" || typeof value === "function" || typeof value === "symbol";
}

function jsonEncodedBytes(value: unknown): number | null {
  const active = new Set<object>();
  const stack: JsonMeasureFrame[] = [{
    kind: "value",
    value,
    depth: -1,
    arrayValue: false,
  }];
  let bytes = 0;
  let items = 0;

  const addBytes = (amount: number): void => {
    const next = bytes + amount;
    if (!Number.isSafeInteger(next)) {
      throw new RangeError("JSON byte length exceeds safe integer bounds");
    }
    bytes = next;
  };
  const countItem = (): void => {
    items++;
    if (items > MAX_WORKLOAD_JSON_ITEMS) {
      throw new WorkloadTraversalLimitError("items");
    }
  };

  while (stack.length > 0) {
    const frame = stack.pop()!;
    if (frame.kind === "array") {
      if (frame.index >= frame.value.length) {
        active.delete(frame.value);
        continue;
      }
      if (frame.index > 0) addBytes(1);
      const child = frame.value[frame.index];
      frame.index++;
      stack.push(frame, {
        kind: "value",
        value: child,
        depth: frame.depth + 1,
        arrayValue: true,
      });
      continue;
    }
    if (frame.kind === "object") {
      let scheduled = false;
      while (frame.index < frame.keys.length) {
        const key = frame.keys[frame.index++]!;
        const child = frame.value[key];
        if (unsupportedJsonObjectValue(child)) {
          countItem();
          continue;
        }
        if (frame.entries++ > 0) addBytes(1);
        addBytes(jsonStringBytes(key) + 1);
        stack.push(frame, {
          kind: "value",
          value: child,
          depth: frame.depth === -1 && key === "input" && Array.isArray(child)
            ? -1
            : frame.depth + 1,
          arrayValue: false,
        });
        scheduled = true;
        break;
      }
      if (!scheduled) active.delete(frame.value);
      continue;
    }

    countItem();
    const current = frame.value;
    if (current === null) {
      addBytes(4);
      continue;
    }
    switch (typeof current) {
      case "string":
        addBytes(jsonStringBytes(current));
        continue;
      case "boolean":
        addBytes(current ? 4 : 5);
        continue;
      case "number":
        addBytes(Number.isFinite(current) ? Buffer.byteLength(String(current), "utf8") : 4);
        continue;
      case "bigint":
        throw new TypeError("Do not know how to serialize a BigInt");
      case "undefined":
      case "function":
      case "symbol":
        if (frame.arrayValue) addBytes(4);
        continue;
      case "object":
        break;
    }

    if (frame.depth > MAX_RESPONSES_JSON_DEPTH) {
      throw new WorkloadTraversalLimitError("depth");
    }
    const object = current as Record<string, unknown>;
    if (active.has(object)) {
      throw new TypeError("Converting circular structure to JSON");
    }
    active.add(object);
    addBytes(2); // brackets/braces
    if (Array.isArray(object)) {
      if (object.length > MAX_WORKLOAD_JSON_ITEMS - items) {
        throw new WorkloadTraversalLimitError("items");
      }
      stack.push({ kind: "array", value: object, depth: frame.depth, index: 0 });
    } else {
      stack.push({
        kind: "object",
        value: object,
        depth: frame.depth,
        keys: Object.keys(object),
        index: 0,
        entries: 0,
      });
    }
  }
  return unsupportedJsonObjectValue(value) ? null : bytes;
}

export function jsonEncodedByteLength(value: unknown): number {
  return jsonEncodedBytes(value) ?? 0;
}

export interface JsonEncodedByteLengthAsyncOptions {
  signal?: AbortSignal;
  /** Deterministic test seam; production yields to the next task turn. */
  yieldTask?: () => Promise<void>;
}

const JSON_MEASURE_UNITS_PER_YIELD = 64 * 1024;
const JSON_MEASURE_NODES_PER_YIELD = 256;

function jsonMeasureAbort(signal: AbortSignal): unknown {
  return signal.reason ?? new DOMException("JSON measurement aborted", "AbortError");
}

/** Exact, allocation-bounded JSON sizing for untrusted large request graphs. */
export async function jsonEncodedByteLengthAsync(
  value: unknown,
  options: JsonEncodedByteLengthAsyncOptions = {},
): Promise<number> {
  const signal = options.signal;
  const yieldTask = options.yieldTask ?? (() => new Promise<void>(resolve => setImmediate(resolve)));
  if (signal?.aborted) throw jsonMeasureAbort(signal);
  const active = new Set<object>();
  const stack: JsonMeasureFrame[] = [{
    kind: "value",
    value,
    depth: -1,
    arrayValue: false,
  }];
  let bytes = 0;
  let items = 0;
  let unitsSinceYield = 0;
  let nodesSinceYield = 0;

  const addBytes = (amount: number): void => {
    const next = bytes + amount;
    if (!Number.isSafeInteger(next)) throw new RangeError("JSON byte length exceeds safe integer bounds");
    bytes = next;
  };
  const checkpoint = (units = 0, nodes = 0): Promise<void> | null => {
    if (signal?.aborted) throw jsonMeasureAbort(signal);
    unitsSinceYield += units;
    nodesSinceYield += nodes;
    if (unitsSinceYield < JSON_MEASURE_UNITS_PER_YIELD &&
      nodesSinceYield < JSON_MEASURE_NODES_PER_YIELD) return null;
    unitsSinceYield = 0;
    nodesSinceYield = 0;
    return yieldTask().then(() => {
      if (signal?.aborted) throw jsonMeasureAbort(signal);
    });
  };
  const addString = async (text: string): Promise<void> => {
    addBytes(2);
    let chunkUnits = 0;
    for (let index = 0; index < text.length;) {
      const encoded = jsonStringUnitBytes(text, index);
      addBytes(encoded.bytes);
      index += encoded.width;
      chunkUnits += encoded.width;
      if (chunkUnits >= JSON_MEASURE_UNITS_PER_YIELD) {
        const pending = checkpoint(chunkUnits);
        if (pending) await pending;
        chunkUnits = 0;
      }
    }
    if (chunkUnits > 0) {
      const pending = checkpoint(chunkUnits);
      if (pending) await pending;
    }
  };
  const countItem = (): Promise<void> | null => {
    items++;
    if (items > MAX_WORKLOAD_JSON_ITEMS) throw new WorkloadTraversalLimitError("items");
    return checkpoint(0, 1);
  };

  while (stack.length > 0) {
    const frame = stack.pop()!;
    if (frame.kind === "array") {
      if (frame.index >= frame.value.length) {
        active.delete(frame.value);
        continue;
      }
      if (frame.index > 0) addBytes(1);
      const child = frame.value[frame.index];
      frame.index++;
      stack.push(frame, {
        kind: "value",
        value: child,
        depth: frame.depth + 1,
        arrayValue: true,
      });
      continue;
    }
    if (frame.kind === "object") {
      let scheduled = false;
      while (frame.index < frame.keys.length) {
        const key = frame.keys[frame.index++]!;
        const child = frame.value[key];
        if (unsupportedJsonObjectValue(child)) {
          const pending = countItem();
          if (pending) await pending;
          continue;
        }
        if (frame.entries++ > 0) addBytes(1);
        await addString(key);
        addBytes(1);
        stack.push(frame, {
          kind: "value",
          value: child,
          depth: frame.depth === -1 && key === "input" && Array.isArray(child)
            ? -1
            : frame.depth + 1,
          arrayValue: false,
        });
        scheduled = true;
        break;
      }
      if (!scheduled) active.delete(frame.value);
      continue;
    }

    const itemCheckpoint = countItem();
    if (itemCheckpoint) await itemCheckpoint;
    const current = frame.value;
    if (current === null) {
      addBytes(4);
      continue;
    }
    switch (typeof current) {
      case "string":
        await addString(current);
        continue;
      case "boolean":
        addBytes(current ? 4 : 5);
        continue;
      case "number":
        addBytes(Number.isFinite(current) ? Buffer.byteLength(String(current), "utf8") : 4);
        continue;
      case "bigint":
        throw new TypeError("Do not know how to serialize a BigInt");
      case "undefined":
      case "function":
      case "symbol":
        if (frame.arrayValue) addBytes(4);
        continue;
      case "object":
        break;
    }

    if (frame.depth > MAX_RESPONSES_JSON_DEPTH) throw new WorkloadTraversalLimitError("depth");
    const object = current as Record<string, unknown>;
    if (active.has(object)) throw new TypeError("Converting circular structure to JSON");
    active.add(object);
    addBytes(2);
    if (Array.isArray(object)) {
      if (object.length > MAX_WORKLOAD_JSON_ITEMS - items) {
        throw new WorkloadTraversalLimitError("items");
      }
      stack.push({ kind: "array", value: object, depth: frame.depth, index: 0 });
    } else {
      stack.push({
        kind: "object",
        value: object,
        depth: frame.depth,
        keys: Object.keys(object),
        index: 0,
        entries: 0,
      });
    }
  }
  return unsupportedJsonObjectValue(value) ? 0 : bytes;
}

interface ScanValueFrame {
  kind: "value";
  value: unknown;
  depth: number;
}

interface ScanArrayFrame {
  kind: "array";
  value: unknown[];
  depth: number;
  index: number;
}

interface ScanObjectFrame {
  kind: "object";
  value: Record<string, unknown>;
  depth: number;
  keys: string[];
  index: number;
}

type ImageScanFrame = ScanValueFrame | ScanArrayFrame | ScanObjectFrame;

function containsInputImage(value: unknown): boolean {
  const active = new Set<object>();
  const stack: ImageScanFrame[] = [{
    kind: "value",
    value,
    depth: Array.isArray(value) ? -1 : 0,
  }];
  let items = 0;

  while (stack.length > 0) {
    const frame = stack.pop()!;
    if (frame.kind === "array") {
      if (frame.index >= frame.value.length) {
        active.delete(frame.value);
        continue;
      }
      const child = frame.value[frame.index++];
      stack.push(frame, { kind: "value", value: child, depth: frame.depth + 1 });
      continue;
    }
    if (frame.kind === "object") {
      if (frame.index >= frame.keys.length) {
        active.delete(frame.value);
        continue;
      }
      const child = frame.value[frame.keys[frame.index++]!];
      stack.push(frame, { kind: "value", value: child, depth: frame.depth + 1 });
      continue;
    }

    items++;
    if (items > MAX_WORKLOAD_JSON_ITEMS) {
      throw new WorkloadTraversalLimitError("items");
    }
    const current = frame.value;
    if (!current || typeof current !== "object") continue;
    if (frame.depth > MAX_RESPONSES_JSON_DEPTH) {
      throw new WorkloadTraversalLimitError("depth");
    }
    const object = current as Record<string, unknown>;
    if (active.has(object)) {
      throw new TypeError("Converting circular structure to JSON");
    }
    if (!Array.isArray(object) && object.type === "input_image") return true;
    active.add(object);
    if (Array.isArray(object)) {
      if (object.length > MAX_WORKLOAD_JSON_ITEMS - items) {
        throw new WorkloadTraversalLimitError("items");
      }
      stack.push({ kind: "array", value: object, depth: frame.depth, index: 0 });
    } else {
      stack.push({
        kind: "object",
        value: object,
        depth: frame.depth,
        keys: Object.keys(object),
        index: 0,
      });
    }
  }
  return false;
}

export function classifyWorkload(
  body: Record<string, unknown>,
  options: WorkloadOptions = {},
): WorkloadDescriptor {
  for (const [name, value] of [
    ["encodedBytes", options.encodedBytes],
    ["structuralBytes", options.structuralBytes],
  ] as const) {
    if (value !== undefined && (!Number.isSafeInteger(value) || value < 0)) {
      throw new RangeError(`${name} must be a non-negative safe integer`);
    }
  }
  const structuralBytes = options.structuralBytes ?? jsonEncodedByteLength(body);
  const requestBytes = Math.max(options.encodedBytes ?? 0, structuralBytes);
  const tools = Array.isArray(body.tools) ? body.tools : [];
  const imageTool = tools.find(tool => tool && typeof tool === "object" && (tool as any).type === "image_generation") as Record<string, unknown> | undefined;
  const stream = body.stream === true;
  let workloadClass: WorkloadClass;
  let reservedBytes: number;

  if (imageTool && (imageTool.action === "edit" || containsInputImage(body.input))) {
    workloadClass = "image-edit";
    reservedBytes = Math.max(64 * MiB, requestBytes * 3 + 32 * MiB);
  } else if (imageTool) {
    workloadClass = "image-generation";
    reservedBytes = Math.max(24 * MiB, requestBytes * 3 + 16 * MiB);
  } else if (requestBytes > 512 * 1024) {
    workloadClass = "text-large";
    reservedBytes = requestBytes * 6 + (stream ? 4 * MiB : 2 * MiB);
  } else {
    workloadClass = "text-small";
    reservedBytes = Math.max(8 * MiB, requestBytes * 4 + (stream ? 2 * MiB : MiB));
  }

  return {
    class: workloadClass,
    requestBytes,
    reservedBytes: Math.ceil(reservedBytes),
    stream,
    model: imageTool && typeof imageTool.model === "string"
      ? imageTool.model
      : typeof body.model === "string"
        ? body.model
        : undefined,
  };
}
