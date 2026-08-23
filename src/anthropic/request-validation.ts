import { AnthropicRequestError } from "./errors";

export const MAX_REQUEST_BYTES = 64 * 1024 * 1024;
export const MAX_JSON_DEPTH = 64;
export const MAX_ARRAY_ITEMS = 100_000;
const MAX_TOTAL_JSON_ITEMS = 1_000_000;

export function hasOwn(record: Record<string, unknown>, key: string): boolean {
  return Object.prototype.hasOwnProperty.call(record, key);
}

export function requireRecord(value: unknown, name: string): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new AnthropicRequestError(400, `${name} must be an object`);
  }
  return value as Record<string, unknown>;
}

export function requireArray(value: unknown, name: string): unknown[] {
  if (!Array.isArray(value)) {
    throw new AnthropicRequestError(400, `${name} must be an array`);
  }
  if (value.length > MAX_ARRAY_ITEMS) {
    throw new AnthropicRequestError(400, `${name} contains more than ${MAX_ARRAY_ITEMS} items`);
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

export class RequestMediaBudget {
  #decodedBytes = 0;

  add(decodedBytes: number, name: string): void {
    this.#decodedBytes += decodedBytes;
    if (this.#decodedBytes > MAX_REQUEST_BYTES) {
      throw new AnthropicRequestError(400, `${name} exceeds the 64 MiB media limit`);
    }
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

function validateJsonValue(value: unknown, name: string): void {
  const active = new Set<object>();
  const stack: VisitFrame[] = [{ kind: "visit", value, depth: 0 }];
  let items = 0;

  while (stack.length) {
    const frame = stack.pop()!;
    if (frame.kind === "exit") {
      active.delete(frame.value);
      continue;
    }

    items += 1;
    if (items > MAX_TOTAL_JSON_ITEMS) {
      throw new AnthropicRequestError(400, `${name} contains too many items`);
    }
    if (frame.depth > MAX_JSON_DEPTH) {
      throw new AnthropicRequestError(400, `${name} exceeds the maximum JSON depth of ${MAX_JSON_DEPTH}`);
    }

    const current = frame.value;
    if (current === null || typeof current === "string" || typeof current === "boolean") continue;
    if (typeof current === "number") {
      if (!Number.isFinite(current)) throw jsonCompatibilityError(name);
      continue;
    }
    if (typeof current !== "object") throw jsonCompatibilityError(name);
    if (active.has(current)) {
      throw new AnthropicRequestError(400, `${name} must not contain cyclic values`);
    }

    let children: unknown[];
    try {
      if (Array.isArray(current)) {
        if (current.length > MAX_ARRAY_ITEMS) {
          throw new AnthropicRequestError(
            400,
            `${name} contains more than ${MAX_ARRAY_ITEMS} items`,
          );
        }
        children = current;
      } else {
        const prototype = Object.getPrototypeOf(current);
        if (prototype !== Object.prototype && prototype !== null) {
          throw jsonCompatibilityError(name);
        }
        if (Object.getOwnPropertySymbols(current).length) throw jsonCompatibilityError(name);
        const keys = Object.keys(current);
        if (keys.length > MAX_ARRAY_ITEMS) {
          throw new AnthropicRequestError(
            400,
            `${name} contains more than ${MAX_ARRAY_ITEMS} items`,
          );
        }
        children = keys.map(key => (current as Record<string, unknown>)[key]);
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
}

export function serializeBoundedJson(value: unknown, name: string): string {
  validateJsonValue(value, name);
  let serialized: string | undefined;
  try {
    serialized = JSON.stringify(value);
  } catch {
    throw jsonCompatibilityError(name);
  }
  if (serialized === undefined) throw jsonCompatibilityError(name);
  if (Buffer.byteLength(serialized, "utf8") > MAX_REQUEST_BYTES) {
    throw new AnthropicRequestError(400, `${name} exceeds the 64 MiB limit`);
  }
  return serialized;
}
