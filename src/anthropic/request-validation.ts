import { AnthropicRequestError } from "./errors";

export type AnthropicRequestLimits = {
  inboundBytes: number;
  relayBytes: number;
  mediaBytes: number;
  maxJsonDepth: number;
  maxArrayItems: number;
  maxTotalJsonItems: number;
};

export const DEFAULT_ANTHROPIC_REQUEST_LIMITS: Readonly<AnthropicRequestLimits> = Object.freeze({
  inboundBytes: 32 * 1024 * 1024,
  relayBytes: 64 * 1024 * 1024,
  mediaBytes: 32 * 1024 * 1024,
  maxJsonDepth: 64,
  maxArrayItems: 100_000,
  maxTotalJsonItems: 1_000_000,
});

export function resolveAnthropicRequestLimits(
  overrides: Partial<AnthropicRequestLimits> | undefined,
): AnthropicRequestLimits {
  const resolved = { ...DEFAULT_ANTHROPIC_REQUEST_LIMITS, ...overrides };
  for (const key of [
    "inboundBytes",
    "relayBytes",
    "mediaBytes",
    "maxArrayItems",
    "maxTotalJsonItems",
  ] as const) {
    if (!Number.isSafeInteger(resolved[key]) || resolved[key] < 1) {
      throw new RangeError(`${key} must be a positive safe integer`);
    }
  }
  if (!Number.isSafeInteger(resolved.maxJsonDepth) || resolved.maxJsonDepth < 0) {
    throw new RangeError("maxJsonDepth must be a nonnegative safe integer");
  }
  return resolved;
}

export function hasOwn(record: Record<string, unknown>, key: string): boolean {
  return Object.prototype.hasOwnProperty.call(record, key);
}

export function requireRecord(value: unknown, name: string): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new AnthropicRequestError(400, `${name} must be an object`);
  }
  return value as Record<string, unknown>;
}

export function requireArray(
  value: unknown,
  name: string,
  limits: AnthropicRequestLimits,
): unknown[] {
  if (!Array.isArray(value)) {
    throw new AnthropicRequestError(400, `${name} must be an array`);
  }
  if (value.length > limits.maxArrayItems) {
    throw new AnthropicRequestError(
      400,
      `${name} contains more than ${limits.maxArrayItems} items`,
    );
  }
  return value;
}

export function requireString(
  record: Record<string, unknown>,
  key: string,
  name: string,
  allowEmpty = false,
): string {
  const value = record[key];
  if (typeof value !== "string" || (!allowEmpty && !value.trim())) {
    throw new AnthropicRequestError(400, `${name} is required`);
  }
  return value;
}

export function requireOptionalBoolean(
  record: Record<string, unknown>,
  key: string,
  name: string,
): boolean | undefined {
  if (!hasOwn(record, key)) return undefined;
  if (typeof record[key] !== "boolean") {
    throw new AnthropicRequestError(400, `${name} must be a boolean`);
  }
  return record[key] as boolean;
}

export function requireHttpUrl(value: string, name: string): string {
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    throw new AnthropicRequestError(400, `${name} must be an http(s) URL`);
  }
  if ((parsed.protocol !== "http:" && parsed.protocol !== "https:") || !parsed.hostname) {
    throw new AnthropicRequestError(400, `${name} must be an http(s) URL`);
  }
  return value;
}

function requestTooLarge(name: string, maxBytes: number): AnthropicRequestError {
  return new AnthropicRequestError(
    413,
    `${name} exceeds its ${maxBytes}-byte limit`,
    "request_too_large",
  );
}

export class RequestMediaBudget {
  #decodedBytes = 0;

  constructor(private readonly maxBytes: number) {}

  add(decodedBytes: number, name: string): void {
    this.#decodedBytes += decodedBytes;
    if (this.#decodedBytes > this.maxBytes) throw requestTooLarge(`${name} media`, this.maxBytes);
  }
}

const BASE64 = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;

export function validateBase64(
  data: string,
  name: string,
  budget: RequestMediaBudget,
): void {
  if (!data || data.length % 4 !== 0 || !BASE64.test(data)) {
    throw new AnthropicRequestError(400, `${name} must be canonical base64`);
  }
  const padding = data.endsWith("==") ? 2 : data.endsWith("=") ? 1 : 0;
  budget.add((data.length / 4) * 3 - padding, name);
}

type VisitFrame = {
  kind: "visit";
  value: unknown;
  depth: number;
} | {
  kind: "exit";
  value: object;
};

function jsonCompatibilityError(name: string): AnthropicRequestError {
  return new AnthropicRequestError(400, `${name} must contain only JSON-compatible values`);
}

function jsonStringBytes(value: string): number {
  let bytes = 2;
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code === 0x22 || code === 0x5c || code === 0x08 || code === 0x0c ||
      code === 0x0a || code === 0x0d || code === 0x09) {
      bytes += 2;
    } else if (code <= 0x1f) {
      bytes += 6;
    } else if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (next >= 0xdc00 && next <= 0xdfff) {
        bytes += 4;
        index += 1;
      } else {
        bytes += 6;
      }
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      bytes += 6;
    } else if (code <= 0x7f) {
      bytes += 1;
    } else if (code <= 0x7ff) {
      bytes += 2;
    } else {
      bytes += 3;
    }
  }
  return bytes;
}

function numberBytes(value: number): number {
  return Object.is(value, -0) ? 1 : String(value).length;
}

export function measureBoundedJson(
  value: unknown,
  name: string,
  maxBytes: number,
  limits: AnthropicRequestLimits,
): number {
  const active = new Set<object>();
  const stack: VisitFrame[] = [{ kind: "visit", value, depth: 0 }];
  let items = 0;
  let bytes = 0;
  const addBytes = (amount: number): void => {
    bytes += amount;
    if (bytes > maxBytes) throw requestTooLarge(name, maxBytes);
  };

  while (stack.length) {
    const frame = stack.pop()!;
    if (frame.kind === "exit") {
      active.delete(frame.value);
      continue;
    }

    items += 1;
    if (items > limits.maxTotalJsonItems) {
      throw new AnthropicRequestError(400, `${name} contains too many items`);
    }
    if (frame.depth > limits.maxJsonDepth) {
      throw new AnthropicRequestError(
        400,
        `${name} exceeds the maximum JSON depth of ${limits.maxJsonDepth}`,
      );
    }

    const current = frame.value;
    if (current === null) {
      addBytes(4);
      continue;
    }
    if (typeof current === "string") {
      addBytes(jsonStringBytes(current));
      continue;
    }
    if (typeof current === "boolean") {
      addBytes(current ? 4 : 5);
      continue;
    }
    if (typeof current === "number") {
      if (!Number.isFinite(current)) throw jsonCompatibilityError(name);
      addBytes(numberBytes(current));
      continue;
    }
    if (typeof current !== "object") throw jsonCompatibilityError(name);
    if (active.has(current)) {
      throw new AnthropicRequestError(400, `${name} must not contain cyclic values`);
    }

    let children: unknown[];
    try {
      if (Array.isArray(current)) {
        if (current.length > limits.maxArrayItems) {
          throw new AnthropicRequestError(
            400,
            `${name} contains more than ${limits.maxArrayItems} items`,
          );
        }
        addBytes(2 + Math.max(0, current.length - 1));
        children = current;
      } else {
        const prototype = Object.getPrototypeOf(current);
        if (prototype !== Object.prototype && prototype !== null) {
          throw jsonCompatibilityError(name);
        }
        if (Object.getOwnPropertySymbols(current).length ||
          typeof (current as Record<string, unknown>).toJSON === "function") {
          throw jsonCompatibilityError(name);
        }
        const keys = Object.keys(current);
        addBytes(2 + Math.max(0, keys.length - 1));
        children = [];
        for (const key of keys) {
          addBytes(jsonStringBytes(key) + 1);
          children.push((current as Record<string, unknown>)[key]);
        }
      }
    } catch (error) {
      if (error instanceof AnthropicRequestError) throw error;
      throw jsonCompatibilityError(name);
    }

    active.add(current);
    stack.push({ kind: "exit", value: current });
    for (let index = children.length - 1; index >= 0; index -= 1) {
      stack.push({ kind: "visit", value: children[index], depth: frame.depth + 1 });
    }
  }
  return bytes;
}

export function serializeBoundedJson(
  value: unknown,
  name: string,
  maxBytes: number,
  limits: AnthropicRequestLimits,
): string {
  measureBoundedJson(value, name, maxBytes, limits);
  try {
    const serialized = JSON.stringify(value);
    if (serialized === undefined) throw jsonCompatibilityError(name);
    return serialized;
  } catch (error) {
    if (error instanceof AnthropicRequestError) throw error;
    throw jsonCompatibilityError(name);
  }
}
