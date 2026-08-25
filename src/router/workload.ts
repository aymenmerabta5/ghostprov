export type WorkloadClass =
  | "text-small"
  | "text-large"
  | "image-generation"
  | "image-edit";

export interface WorkloadDescriptor {
  class: WorkloadClass;
  requestBytes: number;
  reservedBytes: number;
  cpuPermits: 0 | 1;
  stream: boolean;
  model?: string;
}

export interface WorkloadOptions {
  encodedBytes?: number;
}

const MiB = 1024 ** 2;

/**
 * Counts a JSON string without materializing its escaped representation.
 * Request bodies can contain multi-megabyte prompts/base64 images, so keeping
 * a second serialized copy solely for admission accounting is prohibitively
 * expensive under concurrency.
 */
function jsonStringBytes(value: string): number {
  let bytes = Buffer.byteLength(value, "utf8") + 2;
  const escaped = /["\\\u0000-\u001f\ud800-\udfff]/g;
  for (let match = escaped.exec(value); match; match = escaped.exec(value)) {
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
    if (unit >= 0xd800 && unit <= 0xdbff) {
      const low = value.charCodeAt(index + 1);
      if (low >= 0xdc00 && low <= 0xdfff) {
        escaped.lastIndex = index + 2;
      } else {
        // Buffer encodes a lone surrogate as a three-byte replacement while
        // well-formed JSON.stringify emits a six-byte \udxxx escape.
        bytes += 3;
      }
      continue;
    }
    if (unit >= 0xdc00 && unit <= 0xdfff) {
      bytes += 3;
    }
  }
  return bytes;
}

function jsonEncodedBytes(value: unknown, seen = new Set<object>()): number | null {
  if (value === null) return 4;
  switch (typeof value) {
    case "string":
      return jsonStringBytes(value);
    case "boolean":
      return value ? 4 : 5;
    case "number":
      return Number.isFinite(value) ? Buffer.byteLength(String(value), "utf8") : 4;
    case "bigint":
      throw new TypeError("Do not know how to serialize a BigInt");
    case "undefined":
    case "function":
    case "symbol":
      return null;
    case "object":
      break;
  }

  const object = value as Record<string, unknown>;
  if (seen.has(object)) {
    throw new TypeError("Converting circular structure to JSON");
  }
  seen.add(object);
  try {
    let bytes = 2; // brackets/braces
    let entries = 0;
    if (Array.isArray(object)) {
      for (let index = 0; index < object.length; index++) {
        const item = jsonEncodedBytes(object[index], seen);
        bytes += (entries++ > 0 ? 1 : 0) + (item ?? 4);
      }
      return bytes;
    }
    for (const key of Object.keys(object)) {
      const item = jsonEncodedBytes(object[key], seen);
      if (item === null) continue;
      bytes += (entries++ > 0 ? 1 : 0) + jsonStringBytes(key) + 1 + item;
    }
    return bytes;
  } finally {
    seen.delete(object);
  }
}

export function jsonEncodedByteLength(value: unknown): number {
  return jsonEncodedBytes(value) ?? 0;
}

function containsInputImage(value: unknown): boolean {
  if (!value || typeof value !== "object") return false;
  if (Array.isArray(value)) return value.some(containsInputImage);
  const record = value as Record<string, unknown>;
  if (record.type === "input_image") return true;
  return Object.values(record).some(containsInputImage);
}

export function classifyWorkload(
  body: Record<string, unknown>,
  options: WorkloadOptions = {},
): WorkloadDescriptor {
  if (
    options.encodedBytes !== undefined &&
    (!Number.isSafeInteger(options.encodedBytes) || options.encodedBytes < 0)
  ) {
    throw new RangeError("encodedBytes must be a non-negative safe integer");
  }
  const structuralBytes = jsonEncodedByteLength(body);
  const requestBytes = Math.max(options.encodedBytes ?? 0, structuralBytes);
  const tools = Array.isArray(body.tools) ? body.tools : [];
  const imageTool = tools.find(tool => tool && typeof tool === "object" && (tool as any).type === "image_generation") as Record<string, unknown> | undefined;
  const stream = body.stream === true;
  let workloadClass: WorkloadClass;
  let reservedBytes: number;
  let cpuPermits: 0 | 1;

  if (imageTool && (imageTool.action === "edit" || containsInputImage(body.input))) {
    workloadClass = "image-edit";
    reservedBytes = Math.max(64 * MiB, requestBytes * 3 + 32 * MiB);
    cpuPermits = 1;
  } else if (imageTool) {
    workloadClass = "image-generation";
    reservedBytes = Math.max(24 * MiB, requestBytes * 3 + 16 * MiB);
    cpuPermits = requestBytes > 512 * 1024 ? 1 : 0;
  } else if (requestBytes > 512 * 1024) {
    workloadClass = "text-large";
    reservedBytes = requestBytes * 6 + (stream ? 4 * MiB : 2 * MiB);
    cpuPermits = 1;
  } else {
    workloadClass = "text-small";
    reservedBytes = Math.max(8 * MiB, requestBytes * 4 + (stream ? 2 * MiB : MiB));
    cpuPermits = 0;
  }

  return {
    class: workloadClass,
    requestBytes,
    reservedBytes: Math.ceil(reservedBytes),
    cpuPermits,
    stream,
    model: typeof body.model === "string" ? body.model : undefined,
  };
}
