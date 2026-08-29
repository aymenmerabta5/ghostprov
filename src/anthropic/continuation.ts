import { createHmac, randomBytes, timingSafeEqual } from "node:crypto";
import {
  authenticatedTenantScopeHmac,
  type AuthenticatedTenantScope,
} from "../http/api-key";
import { translateAnthropicAssistantReplay } from "./request-content";
import { decodeReasoningItem } from "./reasoning";
import type { ResponsesInputItem, ResponsesRequestBody } from "./request-types";
import {
  resolveAnthropicRequestLimits,
  type AnthropicRequestLimits,
} from "./request-validation";
import {
  record,
  validateTerminalOutputItemProjection,
  validateTerminalResponseProjection,
} from "./response-validation";
import type { AnthropicAdapterMessage } from "./types";
import { MAX_RESPONSES_JSON_DEPTH } from "../relay/responses-validation";

export const ANTHROPIC_CONTINUATION_MAX_RECORDS = 4_096;
export const ANTHROPIC_CONTINUATION_TTL_MS = 60 * 60 * 1_000;
/** Conservative per-record heap estimate, including map keys and maximum IDs. */
export const ANTHROPIC_CONTINUATION_RECORD_METADATA_BYTES = 1_536;
export const ANTHROPIC_CONTINUATION_METADATA_BYTES =
  ANTHROPIC_CONTINUATION_MAX_RECORDS * ANTHROPIC_CONTINUATION_RECORD_METADATA_BYTES;
export const ANTHROPIC_CONTINUATION_MAX_CANONICAL_BYTES = 64 * 1024 * 1024;
export const ANTHROPIC_CONTINUATION_MAX_CANONICAL_NODES = 1_000_000;

/** @deprecated Use the record names: ambiguity tombstones also consume capacity. */
export const ANTHROPIC_CONTINUATION_MAX_ENTRIES = ANTHROPIC_CONTINUATION_MAX_RECORDS;
/** @deprecated Use the record names: ambiguity tombstones also consume capacity. */
export const ANTHROPIC_CONTINUATION_ENTRY_METADATA_BYTES =
  ANTHROPIC_CONTINUATION_RECORD_METADATA_BYTES;

const HMAC_KEY_BYTES = 32;
const DIGEST_BYTES = 32;
const MAX_PROVIDER_RESPONSE_ID_BYTES = 256;
const MAX_REPLAY_ITEMS = 4_096;
// Replay inference is advisory. Keep its remaining synchronous projection work
// below one interactive turn; larger outputs simply use their explicit full
// history on the next request and can still be committed by other lanes.
const MAX_SYNCHRONOUS_REPLAY_STRING_UNITS = 1024 * 1024;
const MAX_SYNCHRONOUS_REPLAY_NODES = 8_192;
const MAX_SYNCHRONOUS_REPLAY_TERMINAL_BYTES = 1024 * 1024;
const SCOPE_DOMAIN = "ghostprov/anthropic-continuation/v1/scope\0";
const SETTINGS_DOMAIN = "ghostprov/anthropic-continuation/v1/settings\0";
const INPUT_SEED_DOMAIN = "ghostprov/anthropic-continuation/v1/input-seed\0";
const INPUT_ITEM_DOMAIN = "ghostprov/anthropic-continuation/v1/input-item\0";
const EXCLUDED_SETTINGS_FIELDS = new Set([
  "input",
  "stream",
  "store",
  "previous_response_id",
]);

/** Opaque application authority created only by the server authentication layer. */
export type AnthropicContinuationScope = AuthenticatedTenantScope;

export type AnthropicContinuationDecision =
  | "hit"
  | "miss"
  | "ambiguous"
  | "empty_suffix"
  | "explicit";

export type AnthropicContinuationDigest = Readonly<{
  digest: string;
  itemCount: number;
  canonicalBytes: number;
  canonicalNodes: number;
}>;

export type AnthropicContinuationMatch = Readonly<{
  generation: number;
  providerResponseId: string;
  prefixItemCount: number;
  prefixCanonicalBytes: number;
  historyDigest: string;
  settingsDigest: string;
  scopeDigest: string;
}>;

export type PreparedAnthropicContinuation = Readonly<{
  /** Exact full request snapshot whose bytes produced inputDigest. */
  fullRelayBody: ResponsesRequestBody;
  relayBody: ResponsesRequestBody;
  decision: AnthropicContinuationDecision;
  match: AnthropicContinuationMatch | null;
  inputDigest: AnthropicContinuationDigest;
  settingsDigest: string;
  settingsCanonicalBytes: number;
  settingsCanonicalNodes: number;
  scopeDigest: string;
}>;

export type AnthropicContinuationSnapshot = Readonly<{
  records: number;
  uniqueEntries: number;
  ambiguousBuckets: number;
  logicalMetadataBytes: number;
  maxRecords: number;
  metadataBudgetBytes: number;
  ttlMs: number;
  nextGeneration: number;
}>;

type CanonicalSink = {
  update(value: string | Uint8Array): unknown;
};

type MutableDigest = {
  digest: string;
  itemCount: number;
  canonicalBytes: number;
  canonicalNodes: number;
};

type ContinuationEntry = {
  scopeDigest: string;
  settingsDigest: string;
  historyDigest: string;
  itemCount: number;
  canonicalBytes: number;
  canonicalNodes: number;
  providerResponseId: string;
  generation: number;
  expiresAt: number;
};

type UniqueContinuationBucket = {
  readonly kind: "unique";
  readonly generation: number;
};

type AmbiguousContinuationBucket = {
  readonly kind: "ambiguous";
  readonly generation: number;
  expiresAt: number;
};

type ContinuationBucket = UniqueContinuationBucket | AmbiguousContinuationBucket;

type ContinuationPreparationState = {
  readonly mutationEpoch: number;
  readonly scopeDigest: string;
  readonly settingsDigest: string;
  readonly settingsCanonicalBytes: number;
  readonly settingsCanonicalNodes: number;
  digest: MutableDigest;
  longest: {
    readonly key: string;
    readonly bucket: ContinuationBucket;
    readonly entry: ContinuationEntry | null;
  } | null;
};

const CANONICAL_BYTES_PER_YIELD = 64 * 1024;
const CANONICAL_NODES_PER_YIELD = 256;
const CANONICAL_STRING_CHUNK_UNITS = 16 * 1024;
const MAX_ADVISORY_SNAPSHOT_BYTES = 8 * 1024 * 1024;
const MAX_ADVISORY_SNAPSHOT_NODES = 65_536;
const MAX_CANONICAL_OBJECT_KEYS = 8_192;
const MAX_SHUTDOWN_HOOKS = 4;

export type AnthropicContinuationIndexOptions = {
  /** Test seam only. Production callers must rely on a random process-local key. */
  key?: Uint8Array;
  /** Monotonic wall-clock test seam. */
  now?: () => number;
  /** Bounded cooperative canonicalization test seam. */
  maxCanonicalBytes?: number;
  /** Bounded cooperative canonicalization test seam. */
  maxCanonicalNodes?: number;
};

type CooperativeCanonicalWork = {
  readonly signal?: AbortSignal;
  readonly maxBytes: number;
  readonly maxNodes: number;
  bytesSinceYield: number;
  nodesSinceYield: number;
  totalBytes: number;
  totalNodes: number;
};

async function cloneCanonicalJsonAsync(
  value: unknown,
  work: CooperativeCanonicalWork,
  seen = new Set<object>(),
): Promise<unknown> {
  const entryCheckpoint = canonicalCheckpoint(work, 0, 1);
  if (entryCheckpoint) await entryCheckpoint;
  if (value === null || typeof value === "boolean" || typeof value === "number") return value;
  if (typeof value === "string") {
    const stringCheckpoint = canonicalCheckpoint(work, byteLength(value));
    if (stringCheckpoint) await stringCheckpoint;
    return value;
  }
  if (typeof value === "bigint") throw new TypeError("canonical JSON does not support bigint");
  if (typeof value !== "object") return null;
  if (seen.has(value)) throw new TypeError("canonical JSON does not support cycles");
  seen.add(value);
  try {
    if (Array.isArray(value)) {
      if (value.length > work.maxNodes - work.totalNodes) {
        throw new RangeError("continuation canonical nodes exceed the configured limit");
      }
      const snapshot = new Array<unknown>(value.length);
      for (let index = 0; index < value.length; index += 1) {
        const child = value[index];
        snapshot[index] = await cloneCanonicalJsonAsync(
          child === undefined || typeof child === "function" || typeof child === "symbol"
            ? null
            : child,
          work,
          seen,
        );
      }
      return snapshot;
    }

    const source = value as Record<string, unknown>;
    const keys = await boundedOwnEnumerableKeysAsync(
      source,
      Math.min(
        MAX_CANONICAL_OBJECT_KEYS,
        Math.max(0, work.maxNodes - work.totalNodes),
      ),
      work,
    );
    const keysCheckpoint = canonicalCheckpoint(work, 0, keys.length, keys.length);
    if (keysCheckpoint) await keysCheckpoint;
    const snapshot: Record<string, unknown> = Object.create(null);
    for (const key of keys) {
      const child = source[key];
      if (child === undefined || typeof child === "function" || typeof child === "symbol") {
        continue;
      }
      const keyCheckpoint = canonicalCheckpoint(work, byteLength(key), 0, 1);
      if (keyCheckpoint) await keyCheckpoint;
      snapshot[key] = await cloneCanonicalJsonAsync(child, work, seen);
    }
    return snapshot;
  } finally {
    seen.delete(value);
  }
}

function addSafe(left: number, right: number, name: string): number {
  const value = left + right;
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new RangeError(`${name} exceeds the safe integer range`);
  }
  return value;
}

function byteLength(value: string): number {
  return Buffer.byteLength(value, "utf8");
}

async function boundedOwnEnumerableKeysAsync(
  value: Record<string, unknown>,
  maximumEnumerated: number,
  work: CooperativeCanonicalWork,
): Promise<string[]> {
  const keys: string[] = [];
  let enumerated = 0;
  for (const key in value) {
    enumerated++;
    if (enumerated > maximumEnumerated) {
      throw new RangeError("continuation canonical nodes exceed the configured limit");
    }
    if (Object.prototype.hasOwnProperty.call(value, key)) keys.push(key);
    const checkpoint = canonicalCheckpoint(work, 0, 0, 1);
    if (checkpoint) await checkpoint;
  }
  return keys;
}

function ownJsonKeys(value: Record<string, unknown>): string[] {
  return Object.keys(value)
    .filter(key => {
      const child = value[key];
      return child !== undefined && typeof child !== "function" && typeof child !== "symbol";
    })
    .sort();
}

function updateCanonicalString(
  emit: (text: string) => void,
  value: string,
): void {
  emit('"');
  let cursor = 0;
  const escaped = /["\\\x00-\x1f\ud800-\udfff]/g;
  for (let match = escaped.exec(value); match; match = escaped.exec(value)) {
    const index = match.index;
    if (index > cursor) emit(value.slice(cursor, index));
    const unit = value.charCodeAt(index);
    switch (unit) {
      case 0x22: emit('\\"'); cursor = index + 1; break;
      case 0x5c: emit("\\\\"); cursor = index + 1; break;
      case 0x08: emit("\\b"); cursor = index + 1; break;
      case 0x09: emit("\\t"); cursor = index + 1; break;
      case 0x0a: emit("\\n"); cursor = index + 1; break;
      case 0x0c: emit("\\f"); cursor = index + 1; break;
      case 0x0d: emit("\\r"); cursor = index + 1; break;
      default: {
        if (unit >= 0xd800 && unit <= 0xdbff) {
          const low = value.charCodeAt(index + 1);
          if (low >= 0xdc00 && low <= 0xdfff) {
            emit(value.slice(index, index + 2));
            cursor = index + 2;
            escaped.lastIndex = cursor;
            break;
          }
        }
        emit(`\\u${unit.toString(16).padStart(4, "0")}`);
        cursor = index + 1;
      }
    }
  }
  if (cursor < value.length) emit(value.slice(cursor));
  emit('"');
}

function updateCanonical(
  sink: CanonicalSink,
  value: unknown,
  onBytes: (bytes: number) => void,
  seen: Set<object>,
  onNode: () => void = () => undefined,
): void {
  onNode();
  const emit = (text: string): void => {
    sink.update(text);
    onBytes(byteLength(text));
  };

  if (value === null) {
    emit("null");
    return;
  }
  if (typeof value === "string") {
    updateCanonicalString(emit, value);
    return;
  }
  if (typeof value === "boolean") {
    emit(value ? "true" : "false");
    return;
  }
  if (typeof value === "number") {
    emit(Number.isFinite(value) ? JSON.stringify(value) : "null");
    return;
  }
  if (typeof value === "bigint") throw new TypeError("canonical JSON does not support bigint");
  if (typeof value !== "object") {
    emit("null");
    return;
  }
  if (seen.has(value)) throw new TypeError("canonical JSON does not support cycles");
  seen.add(value);
  try {
    if (Array.isArray(value)) {
      emit("[");
      for (let index = 0; index < value.length; index += 1) {
        if (index > 0) emit(",");
        const child = value[index];
        updateCanonical(
          sink,
          child === undefined || typeof child === "function" || typeof child === "symbol"
            ? null
            : child,
          onBytes,
          seen,
          onNode,
        );
      }
      emit("]");
      return;
    }

    const object = value as Record<string, unknown>;
    const keys = ownJsonKeys(object);
    emit("{");
    for (let index = 0; index < keys.length; index += 1) {
      if (index > 0) emit(",");
      const key = keys[index];
      updateCanonicalString(emit, key);
      emit(":");
      updateCanonical(sink, object[key], onBytes, seen, onNode);
    }
    emit("}");
  } finally {
    seen.delete(value);
  }
}

function canonicalAbortReason(signal: AbortSignal): unknown {
  return signal.reason ?? new DOMException("Request aborted", "AbortError");
}

function linkedAbortSignal(
  request: AbortSignal | undefined,
  shutdown: AbortSignal,
): { signal: AbortSignal; dispose(): void } {
  if (!request) return { signal: shutdown, dispose() {} };
  const controller = new AbortController();
  const abortRequest = () => controller.abort(request.reason);
  const abortShutdown = () => controller.abort(shutdown.reason);
  if (request.aborted) abortRequest();
  else request.addEventListener("abort", abortRequest, { once: true });
  if (shutdown.aborted) abortShutdown();
  else shutdown.addEventListener("abort", abortShutdown, { once: true });
  return {
    signal: controller.signal,
    dispose() {
      request.removeEventListener("abort", abortRequest);
      shutdown.removeEventListener("abort", abortShutdown);
    },
  };
}

function createCanonicalWork(
  maxBytes: number,
  maxNodes: number,
  signal?: AbortSignal,
): CooperativeCanonicalWork {
  if (signal?.aborted) throw canonicalAbortReason(signal);
  return {
    signal,
    maxBytes,
    maxNodes,
    bytesSinceYield: 0,
    nodesSinceYield: 0,
    totalBytes: 0,
    totalNodes: 0,
  };
}

function canonicalCheckpoint(
  work: CooperativeCanonicalWork,
  bytes = 0,
  nodes = 0,
  workUnits = nodes,
): Promise<void> | null {
  if (work.signal?.aborted) throw canonicalAbortReason(work.signal);
  work.totalBytes = addSafe(work.totalBytes, bytes, "cooperative canonical bytes");
  work.totalNodes = addSafe(work.totalNodes, nodes, "cooperative canonical nodes");
  if (work.totalBytes > work.maxBytes) {
    throw new RangeError("continuation canonical bytes exceed the configured limit");
  }
  if (work.totalNodes > work.maxNodes) {
    throw new RangeError("continuation canonical nodes exceed the configured limit");
  }
  work.bytesSinceYield = addSafe(work.bytesSinceYield, bytes, "canonical yield bytes");
  work.nodesSinceYield = addSafe(
    work.nodesSinceYield,
    workUnits,
    "canonical yield nodes",
  );
  if (work.bytesSinceYield < CANONICAL_BYTES_PER_YIELD &&
    work.nodesSinceYield < CANONICAL_NODES_PER_YIELD) return null;

  work.bytesSinceYield = 0;
  work.nodesSinceYield = 0;
  return new Promise<void>(resolve => setImmediate(resolve)).then(() => {
    if (work.signal?.aborted) throw canonicalAbortReason(work.signal);
  });
}

async function updateCanonicalText(
  sink: CanonicalSink,
  text: string,
  onBytes: (bytes: number) => void,
  work: CooperativeCanonicalWork,
): Promise<void> {
  sink.update(text);
  const bytes = byteLength(text);
  onBytes(bytes);
  const checkpoint = canonicalCheckpoint(work, bytes);
  if (checkpoint) await checkpoint;
}

async function updateCanonicalStringAsync(
  sink: CanonicalSink,
  value: string,
  onBytes: (bytes: number) => void,
  work: CooperativeCanonicalWork,
): Promise<void> {
  let parts: string[] = ['"'];
  let bufferedUnits = 1;
  let rawStart = 0;
  const append = (part: string): void => {
    if (!part) return;
    parts.push(part);
    bufferedUnits = addSafe(bufferedUnits, part.length, "canonical string chunk units");
  };
  const flush = async (): Promise<void> => {
    if (parts.length === 0) return;
    const chunk = parts.join("");
    parts = [];
    bufferedUnits = 0;
    await updateCanonicalText(sink, chunk, onBytes, work);
  };

  for (let index = 0; index < value.length;) {
    const unit = value.charCodeAt(index);
    let escaped: string | null = null;
    let width = 1;
    switch (unit) {
      case 0x22: escaped = '\\"'; break;
      case 0x5c: escaped = "\\\\"; break;
      case 0x08: escaped = "\\b"; break;
      case 0x09: escaped = "\\t"; break;
      case 0x0a: escaped = "\\n"; break;
      case 0x0c: escaped = "\\f"; break;
      case 0x0d: escaped = "\\r"; break;
      default: {
        if (unit < 0x20 || (unit >= 0xd800 && unit <= 0xdfff)) {
          if (unit >= 0xd800 && unit <= 0xdbff) {
            const low = value.charCodeAt(index + 1);
            if (low >= 0xdc00 && low <= 0xdfff) width = 2;
            else escaped = `\\u${unit.toString(16).padStart(4, "0")}`;
          } else {
            escaped = `\\u${unit.toString(16).padStart(4, "0")}`;
          }
        }
      }
    }

    if (escaped !== null) {
      if (index > rawStart) append(value.slice(rawStart, index));
      append(escaped);
      index += 1;
      rawStart = index;
    } else {
      index += width;
    }

    if (bufferedUnits + index - rawStart >= CANONICAL_STRING_CHUNK_UNITS) {
      if (index > rawStart) append(value.slice(rawStart, index));
      rawStart = index;
      await flush();
    }
  }
  if (rawStart < value.length) append(value.slice(rawStart));
  append('"');
  await flush();
}

async function cooperativeSortKeys(
  keys: string[],
  work: CooperativeCanonicalWork,
): Promise<string[]> {
  if (keys.length < 2) return keys;
  let source = keys;
  let target = new Array<string>(keys.length);
  for (let width = 1; width < keys.length; width *= 2) {
    for (let start = 0; start < keys.length; start += width * 2) {
      const middle = Math.min(start + width, keys.length);
      const end = Math.min(start + width * 2, keys.length);
      let left = start;
      let right = middle;
      for (let cursor = start; cursor < end; cursor += 1) {
        if (right >= end || (left < middle && source[left]! <= source[right]!)) {
          target[cursor] = source[left++]!;
        } else {
          target[cursor] = source[right++]!;
        }
        const checkpoint = canonicalCheckpoint(work, 0, 0, 1);
        if (checkpoint) await checkpoint;
      }
    }
    [source, target] = [target, source];
    if (width > Math.floor(keys.length / 2)) break;
  }
  return source;
}

async function ownJsonKeysAsync(
  value: Record<string, unknown>,
  work: CooperativeCanonicalWork,
): Promise<string[]> {
  // Object.keys matches JSON's own-enumerable contract and does not walk a
  // hostile prototype chain. Charge the first (and only) complete key array
  // before cooperative merge sort allocates its equally-sized scratch array.
  const keys = await boundedOwnEnumerableKeysAsync(
    value,
    Math.min(
      MAX_CANONICAL_OBJECT_KEYS,
      Math.max(0, work.maxNodes - work.totalNodes),
    ),
    work,
  );
  // Key enumeration/sort work contributes to cooperative yield accounting,
  // but keys are not separate canonical JSON value nodes. Semantic node
  // metadata must match the synchronous digest path exactly.
  const keysCheckpoint = canonicalCheckpoint(work, 0, 0, keys.length);
  if (keysCheckpoint) await keysCheckpoint;
  let retained = 0;
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index]!;
    const child = value[key];
    if (child !== undefined && typeof child !== "function" && typeof child !== "symbol") {
      keys[retained++] = key;
    }
    const checkpoint = canonicalCheckpoint(work, 0, 0, 1);
    if (checkpoint) await checkpoint;
  }
  keys.length = retained;
  return cooperativeSortKeys(keys, work);
}

async function updateCanonicalAsync(
  sink: CanonicalSink,
  value: unknown,
  onBytes: (bytes: number) => void,
  seen: Set<object>,
  work: CooperativeCanonicalWork,
): Promise<void> {
  const checkpoint = canonicalCheckpoint(work, 0, 1);
  if (checkpoint) await checkpoint;
  const emit = (text: string): Promise<void> =>
    updateCanonicalText(sink, text, onBytes, work);

  if (value === null) return emit("null");
  if (typeof value === "string") {
    await updateCanonicalStringAsync(sink, value, onBytes, work);
    return;
  }
  if (typeof value === "boolean") return emit(value ? "true" : "false");
  if (typeof value === "number") return emit(Number.isFinite(value) ? JSON.stringify(value) : "null");
  if (typeof value === "bigint") throw new TypeError("canonical JSON does not support bigint");
  if (typeof value !== "object") return emit("null");
  if (seen.has(value)) throw new TypeError("canonical JSON does not support cycles");
  seen.add(value);
  try {
    if (Array.isArray(value)) {
      await emit("[");
      for (let index = 0; index < value.length; index += 1) {
        if (index > 0) await emit(",");
        const child = value[index];
        await updateCanonicalAsync(
          sink,
          child === undefined || typeof child === "function" || typeof child === "symbol"
            ? null
            : child,
          onBytes,
          seen,
          work,
        );
      }
      await emit("]");
      return;
    }

    const object = value as Record<string, unknown>;
    const keys = await ownJsonKeysAsync(object, work);
    await emit("{");
    for (let index = 0; index < keys.length; index += 1) {
      if (index > 0) await emit(",");
      const key = keys[index]!;
      await updateCanonicalStringAsync(sink, key, onBytes, work);
      await emit(":");
      await updateCanonicalAsync(sink, object[key], onBytes, seen, work);
    }
    await emit("}");
  } finally {
    seen.delete(value);
  }
}

function updateCanonicalSettings(
  sink: CanonicalSink,
  body: Record<string, unknown>,
): { bytes: number; nodes: number } {
  let ignoredBytes = 0;
  let nodes = 1;
  const count = (bytes: number): void => { ignoredBytes = addSafe(ignoredBytes, bytes, "canonical settings bytes"); };
  const emit = (text: string): void => {
    sink.update(text);
    count(byteLength(text));
  };
  const keys = ownJsonKeys(body).filter(key => !EXCLUDED_SETTINGS_FIELDS.has(key));
  emit("{");
  for (let index = 0; index < keys.length; index += 1) {
    if (index > 0) emit(",");
    const key = keys[index];
    updateCanonicalString(emit, key);
    emit(":");
    updateCanonical(sink, body[key], count, new Set(), () => { nodes++; });
  }
  emit("}");
  return { bytes: ignoredBytes, nodes };
}

async function updateCanonicalSettingsAsync(
  sink: CanonicalSink,
  body: Record<string, unknown>,
  work: CooperativeCanonicalWork,
): Promise<void> {
  let ignoredBytes = 0;
  const rootCheckpoint = canonicalCheckpoint(work, 0, 1);
  if (rootCheckpoint) await rootCheckpoint;
  const count = (bytes: number): void => {
    ignoredBytes = addSafe(ignoredBytes, bytes, "canonical settings bytes");
  };
  const emit = (text: string): Promise<void> => updateCanonicalText(sink, text, count, work);
  const keys = (await ownJsonKeysAsync(body, work))
    .filter(key => !EXCLUDED_SETTINGS_FIELDS.has(key));
  await emit("{");
  for (let index = 0; index < keys.length; index += 1) {
    if (index > 0) await emit(",");
    const key = keys[index]!;
    await updateCanonicalStringAsync(sink, key, count, work);
    await emit(":");
    await updateCanonicalAsync(sink, body[key], count, new Set(), work);
  }
  await emit("}");
}

function digestBuffer(value: string): Buffer {
  const decoded = Buffer.from(value, "base64url");
  if (decoded.length !== DIGEST_BYTES || decoded.toString("base64url") !== value) {
    throw new TypeError("continuation digest is malformed");
  }
  return decoded;
}

function initialInputDigest(key: Buffer): MutableDigest {
  return {
    digest: createHmac("sha256", key).update(INPUT_SEED_DOMAIN, "utf8").digest("base64url"),
    itemCount: 0,
    canonicalBytes: 0,
    canonicalNodes: 0,
  };
}

function appendInputItem(
  key: Buffer,
  previous: AnthropicContinuationDigest,
  item: ResponsesInputItem,
): MutableDigest {
  const hmac = createHmac("sha256", key);
  hmac.update(INPUT_ITEM_DOMAIN, "utf8");
  hmac.update(digestBuffer(previous.digest));
  let itemBytes = 0;
  let itemNodes = 0;
  updateCanonical(
    hmac,
    item,
    bytes => { itemBytes = addSafe(itemBytes, bytes, "canonical input item bytes"); },
    new Set(),
    () => { itemNodes++; },
  );
  return {
    digest: hmac.digest("base64url"),
    itemCount: addSafe(previous.itemCount, 1, "continuation input item count"),
    canonicalBytes: addSafe(previous.canonicalBytes, itemBytes, "continuation canonical bytes"),
    canonicalNodes: addSafe(previous.canonicalNodes, itemNodes, "continuation canonical nodes"),
  };
}

async function appendInputItemAsync(
  key: Buffer,
  previous: AnthropicContinuationDigest,
  item: ResponsesInputItem,
  work: CooperativeCanonicalWork,
): Promise<MutableDigest> {
  const hmac = createHmac("sha256", key);
  hmac.update(INPUT_ITEM_DOMAIN, "utf8");
  hmac.update(digestBuffer(previous.digest));
  let itemBytes = 0;
  const nodesBefore = work.totalNodes;
  await updateCanonicalAsync(
    hmac,
    item,
    bytes => { itemBytes = addSafe(itemBytes, bytes, "canonical input item bytes"); },
    new Set(),
    work,
  );
  return {
    digest: hmac.digest("base64url"),
    itemCount: addSafe(previous.itemCount, 1, "continuation input item count"),
    canonicalBytes: addSafe(
      previous.canonicalBytes,
      itemBytes,
      "continuation canonical bytes",
    ),
    canonicalNodes: addSafe(
      previous.canonicalNodes,
      work.totalNodes - nodesBefore,
      "continuation canonical nodes",
    ),
  };
}

function settingsDigest(
  key: Buffer,
  body: Record<string, unknown>,
): { digest: string; bytes: number; nodes: number } {
  const hmac = createHmac("sha256", key);
  hmac.update(SETTINGS_DOMAIN, "utf8");
  const measured = updateCanonicalSettings(hmac, body);
  return { digest: hmac.digest("base64url"), ...measured };
}

async function settingsDigestAsync(
  key: Buffer,
  body: Record<string, unknown>,
  work: CooperativeCanonicalWork,
): Promise<{ digest: string; bytes: number; nodes: number }> {
  const hmac = createHmac("sha256", key);
  hmac.update(SETTINGS_DOMAIN, "utf8");
  const bytesBefore = work.totalBytes;
  const nodesBefore = work.totalNodes;
  await updateCanonicalSettingsAsync(hmac, body, work);
  return {
    digest: hmac.digest("base64url"),
    bytes: work.totalBytes - bytesBefore,
    nodes: work.totalNodes - nodesBefore,
  };
}

function opaqueScopeDigest(key: Buffer, scope: AnthropicContinuationScope): string {
  return authenticatedTenantScopeHmac(scope, key, SCOPE_DOMAIN);
}

function entryKey(
  scope: string,
  settings: string,
  input: AnthropicContinuationDigest,
): string {
  return `${scope}.${settings}.${input.itemCount}.${input.canonicalBytes}.${input.digest}`;
}

function boundedReference(value: string | null | undefined, maxBytes: number): string | null {
  if (value === undefined || value === null) return null;
  if (!value || byteLength(value) > maxBytes) return null;
  return value;
}

function jsonEquivalent(left: unknown, right: unknown, seen = new Map<object, object>()): boolean {
  if (Object.is(left, right)) return true;
  if (typeof left === "number" && typeof right === "number" &&
    !Number.isFinite(left) && !Number.isFinite(right)) return true;
  if (!left || !right || typeof left !== "object" || typeof right !== "object") return false;
  if (Array.isArray(left) !== Array.isArray(right)) return false;
  const paired = seen.get(left);
  if (paired) return paired === right;
  seen.set(left, right);
  if (Array.isArray(left) && Array.isArray(right)) {
    if (left.length !== right.length) return false;
    for (let index = 0; index < left.length; index += 1) {
      if (!jsonEquivalent(left[index], right[index], seen)) return false;
    }
    return true;
  }
  const leftRecord = left as Record<string, unknown>;
  const rightRecord = right as Record<string, unknown>;
  const leftKeys = ownJsonKeys(leftRecord);
  const rightKeys = ownJsonKeys(rightRecord);
  if (leftKeys.length !== rightKeys.length) return false;
  for (let index = 0; index < leftKeys.length; index += 1) {
    const key = leftKeys[index];
    if (key !== rightKeys[index] || !jsonEquivalent(leftRecord[key], rightRecord[key], seen)) {
      return false;
    }
  }
  return true;
}

function replayProjectionEquivalent(
  expected: readonly ResponsesInputItem[],
  replayed: readonly ResponsesInputItem[],
  projected: AnthropicAdapterMessage,
): boolean {
  if (expected.length !== replayed.length) return false;
  const thinkingBlocks = projected.content.filter(block => block.type === "thinking");
  let thinkingIndex = 0;
  for (let index = 0; index < expected.length; index += 1) {
    const expectedItem = expected[index];
    const replayedItem = replayed[index];
    const expectedRecord = record(expectedItem);
    const replayedRecord = record(replayedItem);
    if (expectedRecord?.type !== "reasoning") {
      if (!jsonEquivalent(expectedItem, replayedItem)) return false;
      continue;
    }

    const thinking = thinkingBlocks[thinkingIndex++];
    if (jsonEquivalent(expectedItem, replayedItem)) continue;
    if (!thinking || thinking.thinking !== "" || replayedRecord?.type !== "reasoning" ||
      !Array.isArray(replayedRecord.summary) || replayedRecord.summary.length !== 0 ||
      !Array.isArray(expectedRecord.summary)) {
      return false;
    }
    const summaryText = expectedRecord.summary.map(value => {
      const part = record(value);
      return part?.type === "summary_text" && typeof part.text === "string"
        ? part.text
        : null;
    });
    if (summaryText.some(value => value === null)) return false;
    const reconstructed = decodeReasoningItem(
      thinking.signature,
      (summaryText as string[]).join(""),
    );
    if (!reconstructed || !jsonEquivalent(expectedItem, reconstructed)) return false;
  }
  return thinkingIndex === thinkingBlocks.length;
}

function replaySafeJsonNumbers(
  value: unknown,
  budget: { remaining: number },
): boolean {
  const pending: Array<{ value: unknown; depth: number }> = [{ value, depth: 0 }];
  const seen = new WeakSet<object>();
  while (pending.length > 0) {
    const current = pending.pop()!;
    if (--budget.remaining < 0) return false;
    if (typeof current.value === "number") {
      if (!Number.isFinite(current.value) ||
        (Number.isInteger(current.value) && !Number.isSafeInteger(current.value))) return false;
      continue;
    }
    if (!current.value || typeof current.value !== "object") continue;
    if (current.depth > MAX_RESPONSES_JSON_DEPTH || seen.has(current.value)) return false;
    seen.add(current.value);
    if (Array.isArray(current.value)) {
      if (current.value.length > budget.remaining) return false;
      for (let index = current.value.length - 1; index >= 0; index--) {
        pending.push({ value: current.value[index], depth: current.depth + 1 });
      }
      continue;
    }
    const keys = Object.keys(current.value);
    if (keys.length > budget.remaining) return false;
    for (let index = keys.length - 1; index >= 0; index--) {
      pending.push({
        value: (current.value as Record<string, unknown>)[keys[index]!],
        depth: current.depth + 1,
      });
    }
  }
  return true;
}

const MESSAGE_REPLAY_KEYS = new Set([
  "type", "id", "status", "role", "content", "phase",
]);
const OUTPUT_TEXT_REPLAY_KEYS = new Set([
  "type", "text", "annotations", "logprobs",
]);
const REASONING_REPLAY_KEYS = new Set([
  "type", "id", "status", "encrypted_content", "summary", "content",
]);
const DIRECT_CALLER_REPLAY_KEYS = new Set(["type"]);
const FUNCTION_CALL_REPLAY_KEYS = new Set([
  "type", "id", "status", "call_id", "name", "arguments", "namespace", "caller",
]);

function hasOnlyReplayKeys(
  value: Record<string, unknown>,
  allowed: ReadonlySet<string>,
): boolean {
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) return false;
  let count = 0;
  for (const key in value) {
    if (!Object.prototype.hasOwnProperty.call(value, key)) continue;
    // A replay-safe object cannot contain more distinct keys than its allowlist.
    // Reject at the first additive provider field without allocating/scanning a
    // potentially million-entry Object.keys array.
    if (++count > allowed.size || !allowed.has(key)) return false;
  }
  return true;
}

function expectedReplayItems(
  terminal: ReturnType<typeof validateTerminalResponseProjection>,
  projected: AnthropicAdapterMessage,
  validatedFunctionArguments?: (
    outputIndex: number,
    item: Readonly<Record<string, unknown>>,
  ) => boolean,
  replayBudget: { remaining: number } = { remaining: MAX_SYNCHRONOUS_REPLAY_NODES },
): { items: ResponsesInputItem[]; meaningful: boolean } | null {
  if (terminal.output.length > MAX_REPLAY_ITEMS) return null;
  const items: ResponsesInputItem[] = [];
  const text: string[] = [];
  const projectedTools = projected.content.filter(block => block.type === "tool_use");
  let projectedToolIndex = 0;
  let meaningful = false;
  const flushText = (): void => {
    if (text.length === 0) return;
    const content = text.splice(0).join("");
    if (content.length > 0) meaningful = true;
    items.push({ type: "message", role: "assistant", content });
  };

  for (let outputIndex = 0; outputIndex < terminal.output.length; outputIndex += 1) {
    const value = terminal.output[outputIndex];
    const item = validateTerminalOutputItemProjection(value, terminal.status);
    if (item.type === "message") {
      if (!hasOnlyReplayKeys(item, MESSAGE_REPLAY_KEYS) ||
        item.status !== "completed") {
        return null;
      }
      // `phase` is provider-only metadata that Anthropic clients cannot replay.
      // The exact value remains in the persisted provider parent. Digest only
      // the client-visible projection; a second parent with the same projection
      // becomes an ambiguity tombstone rather than selecting either lineage.
      for (const part of item.content) {
        if (!hasOnlyReplayKeys(part, OUTPUT_TEXT_REPLAY_KEYS) ||
          part.type !== "output_text" || part.annotations.length > 0 || part.logprobs.length > 0) {
          return null;
        }
        text.push(part.text);
      }
      continue;
    }
    flushText();
    if (item.type === "reasoning") {
      if (!hasOnlyReplayKeys(item, REASONING_REPLAY_KEYS) ||
        item.status === "incomplete" || typeof item.encrypted_content !== "string" ||
        item.encrypted_content.length === 0 || (item.content?.length ?? 0) > 0) return null;
      meaningful = true;
      items.push({
        type: "reasoning",
        id: item.id,
        encrypted_content: item.encrypted_content,
        summary: item.summary,
      });
      continue;
    }

    if (!hasOnlyReplayKeys(item, FUNCTION_CALL_REPLAY_KEYS) ||
      item.status !== "completed" || item.namespace !== undefined ||
      (item.caller !== undefined && item.caller !== null &&
        (item.caller.type !== "direct" ||
          !hasOnlyReplayKeys(item.caller, DIRECT_CALLER_REPLAY_KEYS)))) {
      return null;
    }
    let input: Record<string, unknown>;
    const projectedTool = projectedTools[projectedToolIndex++];
    if (validatedFunctionArguments?.(outputIndex, item) && projectedTool &&
      projectedTool.id === item.call_id && projectedTool.name === item.name) {
      input = projectedTool.input;
    } else if (item.arguments.trim().length > 0) {
      let parsed: unknown;
      try {
        parsed = JSON.parse(item.arguments);
      } catch {
        return null;
      }
      const parsedRecord = record(parsed);
      if (!parsedRecord) return null;
      input = parsedRecord;
    } else {
      input = {};
    }
    if (!replaySafeJsonNumbers(input, replayBudget)) return null;
    let argumentsJson: string;
    try {
      argumentsJson = JSON.stringify(input);
    } catch {
      return null;
    }
    meaningful = true;
    items.push({
      type: "function_call",
      call_id: item.call_id,
      name: item.name,
      arguments: argumentsJson,
    });
  }
  flushText();
  return { items, meaningful };
}

/**
 * Reprojects a successfully translated Anthropic message into the exact ordered
 * Responses input items that a Claude client will send on its next turn.
 * Returning null means that replay would discard or reinterpret provider state.
 */
export function projectAnthropicReplay(
  finalValue: unknown,
  projected: AnthropicAdapterMessage,
  options: {
    limits?: Partial<AnthropicRequestLimits>;
    /** Trusted transport byte count; absent/large terminals are not replayed synchronously. */
    terminalBytes?: number;
    /** Internal SSE proof that the exact argument string was already validated. */
    validatedFunctionArguments?(
      outputIndex: number,
      item: Readonly<Record<string, unknown>>,
    ): boolean;
  } = {},
): ResponsesInputItem[] | null {
  try {
    if (!Number.isSafeInteger(options.terminalBytes) ||
      options.terminalBytes! < 1 ||
      options.terminalBytes! > MAX_SYNCHRONOUS_REPLAY_TERMINAL_BYTES) {
      return null;
    }
    const terminal = validateTerminalResponseProjection(finalValue);
    if (terminal.status !== "completed" || terminal.incompleteReason !== null ||
      terminal.output.length === 0 || projected.stop_reason === "refusal" ||
      projected.stop_reason === "max_tokens" || projected.stop_reason === "stop_sequence" ||
      projected.stop_sequence !== null) {
      return null;
    }
    if (terminal.output.length > MAX_REPLAY_ITEMS) return null;
    let replayStringUnits = 0;
    let replayNodes = 0;
    const add = (candidate: unknown): boolean => {
      if (typeof candidate !== "string") return true;
      replayStringUnits = addSafe(
        replayStringUnits,
        candidate.length,
        "replay projection string units",
      );
      return replayStringUnits <= MAX_SYNCHRONOUS_REPLAY_STRING_UNITS;
    };
    for (const value of terminal.output) {
      if (++replayNodes > MAX_SYNCHRONOUS_REPLAY_NODES) return null;
      const item = record(value);
      if (!item) return null;
      if (!add(item.type) || !add(item.id) || !add(item.status) || !add(item.role) ||
        !add(item.phase) || !add(item.call_id) || !add(item.name) ||
        !add(item.arguments) || !add(item.encrypted_content)) return null;
      if (Array.isArray(item.content)) {
        for (const part of item.content) {
          if (++replayNodes > MAX_SYNCHRONOUS_REPLAY_NODES) return null;
          const child = record(part);
          if (child && (!add(child.type) || !add(child.text) || !add(child.refusal))) return null;
          if ((Array.isArray(child?.annotations) && child.annotations.length > 0) ||
            (Array.isArray(child?.logprobs) && child.logprobs.length > 0)) return null;
        }
      }
      if (Array.isArray(item.summary)) {
        for (const part of item.summary) {
          if (++replayNodes > MAX_SYNCHRONOUS_REPLAY_NODES) return null;
          const child = record(part);
          if (child && (!add(child.type) || !add(child.text))) return null;
        }
      }
    }
    if (projected.content.length > MAX_SYNCHRONOUS_REPLAY_NODES - replayNodes) return null;
    for (const block of projected.content) {
      replayNodes++;
      if (block.type === "text") {
        if (!add(block.text)) return null;
      } else if (block.type === "thinking") {
        if (!add(block.thinking) || !add(block.signature)) return null;
      } else if (!add(block.id) || !add(block.name)) return null;
    }
    const expected = expectedReplayItems(
      terminal,
      projected,
      options.validatedFunctionArguments,
      { remaining: MAX_SYNCHRONOUS_REPLAY_NODES - replayNodes },
    );
    if (!expected || !expected.meaningful || expected.items.length === 0) return null;
    const limits = resolveAnthropicRequestLimits(options.limits);
    const replayed = translateAnthropicAssistantReplay(projected.content, limits);
    if (replayed.length === 0 ||
      !replayProjectionEquivalent(expected.items, replayed, projected)) return null;
    return replayed;
  } catch {
    return null;
  }
}

/**
 * Process-local, bounded metadata index. It retains no input/output values: all
 * history and settings are represented by keyed fixed-size digests.
 */
export class AnthropicContinuationIndex {
  readonly #key: Buffer;
  readonly #now: () => number;
  readonly #maxCanonicalBytes: number;
  readonly #maxCanonicalNodes: number;
  readonly #entries = new Map<number, ContinuationEntry>();
  readonly #buckets = new Map<string, ContinuationBucket>();
  /** Absolute-expiry order is independent from the entry LRU order. */
  readonly #entryExpiry = new Map<number, ContinuationEntry>();
  readonly #ambiguityExpiry = new Map<string, AmbiguousContinuationBucket>();
  readonly #shutdownHooks = new Set<() => void>();
  readonly #shutdown = new AbortController();
  #nextGeneration = 1;
  #mutationEpoch = 0;
  #lastNow = 0;
  #acceptingCommits = true;

  constructor(options: AnthropicContinuationIndexOptions = {}) {
    const key = options.key === undefined ? randomBytes(HMAC_KEY_BYTES) : Buffer.from(options.key);
    if (key.length !== HMAC_KEY_BYTES) throw new RangeError(`continuation HMAC key must be ${HMAC_KEY_BYTES} bytes`);
    this.#key = Buffer.from(key);
    this.#now = options.now ?? Date.now;
    this.#maxCanonicalBytes = this.#canonicalLimit(
      options.maxCanonicalBytes,
      ANTHROPIC_CONTINUATION_MAX_CANONICAL_BYTES,
      "maxCanonicalBytes",
    );
    this.#maxCanonicalNodes = this.#canonicalLimit(
      options.maxCanonicalNodes,
      ANTHROPIC_CONTINUATION_MAX_CANONICAL_NODES,
      "maxCanonicalNodes",
    );
  }

  digestInput(items: readonly ResponsesInputItem[]): AnthropicContinuationDigest {
    let digest = initialInputDigest(this.#key);
    for (const item of items) digest = appendInputItem(this.#key, digest, item);
    return Object.freeze({ ...digest });
  }

  fingerprintSettings(body: Record<string, unknown>): string {
    return settingsDigest(this.#key, body).digest;
  }

  prepare(
    body: ResponsesRequestBody,
    scope: AnthropicContinuationScope,
  ): PreparedAnthropicContinuation {
    if (Object.hasOwn(body, "previous_response_id")) return this.#explicitPreparation(body);
    const state = this.#beginPreparation(body, scope);
    for (const item of body.input) this.#appendPreparationItem(state, item);
    return this.#finishPreparation(body, state);
  }

  async prepareAsync(
    body: ResponsesRequestBody,
    scope: AnthropicContinuationScope,
    signal?: AbortSignal,
  ): Promise<PreparedAnthropicContinuation> {
    const linked = linkedAbortSignal(signal, this.#shutdown.signal);
    try {
      if (linked.signal.aborted) throw canonicalAbortReason(linked.signal);
      if (Object.hasOwn(body, "previous_response_id")) return this.#explicitPreparation(body);
      const snapshotWork = createCanonicalWork(
        Math.min(this.#maxCanonicalBytes, MAX_ADVISORY_SNAPSHOT_BYTES),
        Math.min(this.#maxCanonicalNodes, MAX_ADVISORY_SNAPSHOT_NODES),
        linked.signal,
      );
      const snapshotValue = await cloneCanonicalJsonAsync(body, snapshotWork);
      const snapshotRecord = record(snapshotValue);
      if (!snapshotRecord || !Array.isArray(snapshotRecord.input)) {
        throw new TypeError("Continuation request snapshot is malformed");
      }
      // Strings remain shared immutable primitives; only containers are copied.
      // Hashing and both initial/fallback relay attempts now own this exact
      // bounded snapshot, so caller mutation cannot change executed lineage.
      const stableBody = snapshotRecord as unknown as ResponsesRequestBody;
      let work = createCanonicalWork(
        this.#maxCanonicalBytes,
        this.#maxCanonicalNodes,
        linked.signal,
      );
      let state = await this.#beginPreparationAsync(stableBody, scope, work);
      await this.#scanPreparationAsync(state, stableBody.input, work);
      if (state.mutationEpoch !== this.#mutationEpoch) {
        // Metadata changed while cooperative hashing yielded. Re-scan once from
        // the immutable request so a new longer/replaced relevant prefix is
        // observed without retaining one digest key per input item. Continuous
        // churn remains a conservative miss at #finishPreparation.
        work = createCanonicalWork(
          this.#maxCanonicalBytes,
          this.#maxCanonicalNodes,
          linked.signal,
        );
        state = await this.#beginPreparationAsync(stableBody, scope, work);
        await this.#scanPreparationAsync(state, stableBody.input, work);
      }
      if (linked.signal.aborted) throw canonicalAbortReason(linked.signal);
      return this.#finishPreparation(stableBody, state);
    } finally {
      linked.dispose();
    }
  }

  commit(
    prepared: Pick<
      PreparedAnthropicContinuation,
      "decision" | "inputDigest" | "settingsDigest" |
        "settingsCanonicalBytes" | "settingsCanonicalNodes" | "scopeDigest"
    >,
    scope: AnthropicContinuationScope,
    providerResponseId: string,
    replayItems: readonly ResponsesInputItem[],
  ): boolean {
    const responseId = this.#validateCommit(prepared, scope, providerResponseId, replayItems);
    if (responseId === null) return false;

    let digest: AnthropicContinuationDigest = prepared.inputDigest;
    for (const item of replayItems) digest = appendInputItem(this.#key, digest, item);
    if (digest.itemCount <= prepared.inputDigest.itemCount) return false;
    if (!this.#withinCanonicalBounds(prepared, digest)) return false;
    return this.#commitDigest(prepared, responseId, digest);
  }

  async commitAsync(
    prepared: Pick<
      PreparedAnthropicContinuation,
      "decision" | "inputDigest" | "settingsDigest" |
        "settingsCanonicalBytes" | "settingsCanonicalNodes" | "scopeDigest"
    >,
    scope: AnthropicContinuationScope,
    providerResponseId: string,
    replayItems: readonly ResponsesInputItem[],
    signal?: AbortSignal,
  ): Promise<boolean> {
    const linked = linkedAbortSignal(signal, this.#shutdown.signal);
    try {
      if (linked.signal.aborted) throw canonicalAbortReason(linked.signal);
      const responseId = this.#validateCommit(prepared, scope, providerResponseId, replayItems);
      if (responseId === null) return false;
      const work = createCanonicalWork(
        this.#maxCanonicalBytes,
        this.#maxCanonicalNodes,
        linked.signal,
      );
      let digest: AnthropicContinuationDigest = prepared.inputDigest;
      for (const item of replayItems) {
        digest = await appendInputItemAsync(this.#key, digest, item, work);
      }
      if (digest.itemCount <= prepared.inputDigest.itemCount) return false;
      if (linked.signal.aborted) throw canonicalAbortReason(linked.signal);
      if (!this.#withinCanonicalBounds(prepared, digest)) return false;
      return this.#commitDigest(prepared, responseId, digest);
    } catch (error) {
      if (this.#shutdown.signal.aborted) return false;
      throw error;
    } finally {
      linked.dispose();
    }
  }

  #validateCommit(
    prepared: Pick<
      PreparedAnthropicContinuation,
      "decision" | "inputDigest" | "settingsDigest" |
        "settingsCanonicalBytes" | "settingsCanonicalNodes" | "scopeDigest"
    >,
    scope: AnthropicContinuationScope,
    providerResponseId: string,
    replayItems: readonly ResponsesInputItem[],
  ): string | null {
    if (!this.#acceptingCommits || prepared.decision === "empty_suffix" ||
      prepared.decision === "explicit" || replayItems.length === 0 ||
      replayItems.length > MAX_REPLAY_ITEMS) return null;
    const commitScopeDigest = opaqueScopeDigest(this.#key, scope);
    if (!timingSafeEqual(
      digestBuffer(commitScopeDigest),
      digestBuffer(prepared.scopeDigest),
    )) return null;
    return boundedReference(providerResponseId, MAX_PROVIDER_RESPONSE_ID_BYTES);
  }

  #withinCanonicalBounds(
    prepared: Pick<
      PreparedAnthropicContinuation,
      "settingsCanonicalBytes" | "settingsCanonicalNodes"
    >,
    digest: AnthropicContinuationDigest,
  ): boolean {
    return prepared.settingsCanonicalBytes + digest.canonicalBytes <=
        this.#maxCanonicalBytes &&
      prepared.settingsCanonicalNodes + digest.canonicalNodes <=
        this.#maxCanonicalNodes;
  }

  #commitDigest(
    prepared: Pick<
      PreparedAnthropicContinuation,
      "settingsDigest" | "scopeDigest"
    >,
    responseId: string,
    digest: AnthropicContinuationDigest,
  ): boolean {
    // Cooperative commits can race shutdown while hashing. Never repopulate the
    // index after the lifecycle barrier has fired.
    if (!this.#acceptingCommits) return false;
    const now = this.#time();
    this.#pruneExpired(now);
    const expiresAt = addSafe(now, ANTHROPIC_CONTINUATION_TTL_MS, "continuation expiry");
    const key = entryKey(prepared.scopeDigest, prepared.settingsDigest, digest);
    const existingBucket = this.#buckets.get(key);
    if (existingBucket?.kind === "ambiguous") {
      if (expiresAt > existingBucket.expiresAt) {
        existingBucket.expiresAt = expiresAt;
        this.#ambiguityExpiry.delete(key);
        this.#ambiguityExpiry.set(key, existingBucket);
      }
      return true;
    }
    if (existingBucket?.kind === "unique") {
      const existing = this.#entries.get(existingBucket.generation);
      if (existing?.providerResponseId === responseId) {
        this.#touch(existing);
        return true;
      }
      if (existing) {
        const ambiguityExpiry = Math.max(existing.expiresAt, expiresAt);
        this.#delete(existing);
        const generation = this.#allocateGeneration();
        const ambiguous: AmbiguousContinuationBucket = {
          kind: "ambiguous",
          generation,
          expiresAt: ambiguityExpiry,
        };
        this.#buckets.set(key, ambiguous);
        this.#markMutation();
        this.#ambiguityExpiry.set(key, ambiguous);
        return true;
      }
      if (this.#buckets.delete(key)) this.#markMutation();
    }

    if (!this.#makeRoomForRecord()) return false;
    const generation = this.#allocateGeneration();
    const entry: ContinuationEntry = {
      scopeDigest: prepared.scopeDigest,
      settingsDigest: prepared.settingsDigest,
      historyDigest: digest.digest,
      itemCount: digest.itemCount,
      canonicalBytes: digest.canonicalBytes,
      canonicalNodes: digest.canonicalNodes,
      providerResponseId: responseId,
      generation,
      expiresAt,
    };
    this.#entries.set(generation, entry);
    this.#entryExpiry.set(generation, entry);
    this.#buckets.set(key, { kind: "unique", generation });
    this.#markMutation();
    return true;
  }

  compareDelete(match: AnthropicContinuationMatch): boolean {
    const entry = this.#entries.get(match.generation);
    if (!entry || entry.historyDigest !== match.historyDigest ||
      entry.settingsDigest !== match.settingsDigest || entry.scopeDigest !== match.scopeDigest ||
      entry.providerResponseId !== match.providerResponseId) {
      return false;
    }
    this.#delete(entry);
    return true;
  }

  /** Prevents in-flight HTTP/SSE completions from repopulating shutdown state. */
  beginShutdown(): void {
    if (!this.#acceptingCommits) return;
    this.#acceptingCommits = false;
    this.#shutdown.abort(new DOMException("Continuation index is shutting down", "AbortError"));
    for (const hook of this.#shutdownHooks) {
      try {
        hook();
      } catch {
        // Shutdown remains authoritative even if an optional optimizer hook fails.
      }
    }
    this.#shutdownHooks.clear();
  }

  /** Links bounded optimizer state to the index lifecycle. */
  onShutdown(hook: () => void): () => void {
    if (!this.#acceptingCommits) {
      hook();
      return () => undefined;
    }
    if (this.#shutdownHooks.size >= MAX_SHUTDOWN_HOOKS) {
      throw new RangeError("continuation shutdown hook limit exceeded");
    }
    this.#shutdownHooks.add(hook);
    let active = true;
    return () => {
      if (!active) return;
      active = false;
      this.#shutdownHooks.delete(hook);
    };
  }

  clear(): void {
    if (this.#buckets.size > 0) this.#markMutation();
    this.#entries.clear();
    this.#buckets.clear();
    this.#entryExpiry.clear();
    this.#ambiguityExpiry.clear();
  }

  snapshot(): AnthropicContinuationSnapshot {
    this.#pruneExpired(this.#time());
    const records = this.#recordCount();
    return Object.freeze({
      records,
      uniqueEntries: this.#entries.size,
      ambiguousBuckets: this.#ambiguityExpiry.size,
      logicalMetadataBytes: records * ANTHROPIC_CONTINUATION_RECORD_METADATA_BYTES,
      maxRecords: ANTHROPIC_CONTINUATION_MAX_RECORDS,
      metadataBudgetBytes: ANTHROPIC_CONTINUATION_METADATA_BYTES,
      ttlMs: ANTHROPIC_CONTINUATION_TTL_MS,
      nextGeneration: this.#nextGeneration,
    });
  }

  #beginPreparation(
    body: ResponsesRequestBody,
    scope: AnthropicContinuationScope,
  ): ContinuationPreparationState {
    const now = this.#time();
    this.#pruneExpired(now);
    const settings = settingsDigest(this.#key, body);
    return {
      mutationEpoch: this.#mutationEpoch,
      scopeDigest: opaqueScopeDigest(this.#key, scope),
      settingsDigest: settings.digest,
      settingsCanonicalBytes: settings.bytes,
      settingsCanonicalNodes: settings.nodes,
      digest: initialInputDigest(this.#key),
      longest: null,
    };
  }

  async #beginPreparationAsync(
    body: ResponsesRequestBody,
    scope: AnthropicContinuationScope,
    work: CooperativeCanonicalWork,
  ): Promise<ContinuationPreparationState> {
    const now = this.#time();
    this.#pruneExpired(now);
    const settings = await settingsDigestAsync(this.#key, body, work);
    return {
      mutationEpoch: this.#mutationEpoch,
      scopeDigest: opaqueScopeDigest(this.#key, scope),
      settingsDigest: settings.digest,
      settingsCanonicalBytes: settings.bytes,
      settingsCanonicalNodes: settings.nodes,
      digest: initialInputDigest(this.#key),
      longest: null,
    };
  }

  #appendPreparationItem(
    state: ContinuationPreparationState,
    item: ResponsesInputItem,
  ): void {
    state.digest = appendInputItem(this.#key, state.digest, item);
    this.#observePreparationBucket(state);
  }

  async #appendPreparationItemAsync(
    state: ContinuationPreparationState,
    item: ResponsesInputItem,
    work: CooperativeCanonicalWork,
  ): Promise<void> {
    state.digest = await appendInputItemAsync(this.#key, state.digest, item, work);
    this.#observePreparationBucket(state);
  }

  async #scanPreparationAsync(
    state: ContinuationPreparationState,
    input: readonly ResponsesInputItem[],
    work: CooperativeCanonicalWork,
  ): Promise<void> {
    for (let index = 0; index < input.length; index += 1) {
      await this.#appendPreparationItemAsync(state, input[index]!, work);
    }
  }

  #observePreparationBucket(state: ContinuationPreparationState): void {
    const key = entryKey(
      state.scopeDigest,
      state.settingsDigest,
      state.digest,
    );
    const bucket = this.#buckets.get(key);
    if (!bucket) return;
    let entry: ContinuationEntry | null = null;
    if (bucket.kind === "ambiguous") {
      entry = null;
    } else {
      entry = this.#entries.get(bucket.generation) ?? null;
      if (!entry) return;
    }
    state.longest = { key, bucket, entry };
  }

  #finishPreparation(
    body: ResponsesRequestBody,
    state: ContinuationPreparationState,
  ): PreparedAnthropicContinuation {
    const inputDigest = Object.freeze({ ...state.digest });
    const finishNow = this.#time();
    this.#pruneExpired(finishNow);
    if (state.mutationEpoch !== this.#mutationEpoch) {
      return Object.freeze({
        fullRelayBody: body,
        relayBody: body,
        decision: "miss",
        match: null,
        inputDigest,
        settingsDigest: state.settingsDigest,
        settingsCanonicalBytes: state.settingsCanonicalBytes,
        settingsCanonicalNodes: state.settingsCanonicalNodes,
        scopeDigest: state.scopeDigest,
      });
    }
    if (
      state.longest &&
      this.#buckets.get(state.longest.key) !== state.longest.bucket
    ) {
      return Object.freeze({
        fullRelayBody: body,
        relayBody: body,
        decision: "miss",
        match: null,
        inputDigest,
        settingsDigest: state.settingsDigest,
        settingsCanonicalBytes: state.settingsCanonicalBytes,
        settingsCanonicalNodes: state.settingsCanonicalNodes,
        scopeDigest: state.scopeDigest,
      });
    }
    if (!state.longest) {
      return Object.freeze({
        fullRelayBody: body,
        relayBody: body,
        decision: "miss",
        match: null,
        inputDigest,
        settingsDigest: state.settingsDigest,
        settingsCanonicalBytes: state.settingsCanonicalBytes,
        settingsCanonicalNodes: state.settingsCanonicalNodes,
        scopeDigest: state.scopeDigest,
      });
    }
    if (state.longest.bucket.kind === "ambiguous" || !state.longest.entry) {
      return Object.freeze({
        fullRelayBody: body,
        relayBody: body,
        decision: "ambiguous",
        match: null,
        inputDigest,
        settingsDigest: state.settingsDigest,
        settingsCanonicalBytes: state.settingsCanonicalBytes,
        settingsCanonicalNodes: state.settingsCanonicalNodes,
        scopeDigest: state.scopeDigest,
      });
    }

    const entry = state.longest.entry;
    if (this.#entries.get(entry.generation) !== entry) {
      return Object.freeze({
        fullRelayBody: body,
        relayBody: body,
        decision: "miss",
        match: null,
        inputDigest,
        settingsDigest: state.settingsDigest,
        settingsCanonicalBytes: state.settingsCanonicalBytes,
        settingsCanonicalNodes: state.settingsCanonicalNodes,
        scopeDigest: state.scopeDigest,
      });
    }
    if (entry.itemCount >= body.input.length) {
      return Object.freeze({
        fullRelayBody: body,
        relayBody: body,
        decision: "empty_suffix",
        match: null,
        inputDigest,
        settingsDigest: state.settingsDigest,
        settingsCanonicalBytes: state.settingsCanonicalBytes,
        settingsCanonicalNodes: state.settingsCanonicalNodes,
        scopeDigest: state.scopeDigest,
      });
    }
    this.#touch(entry);
    const match: AnthropicContinuationMatch = Object.freeze({
      generation: entry.generation,
      providerResponseId: entry.providerResponseId,
      prefixItemCount: entry.itemCount,
      prefixCanonicalBytes: entry.canonicalBytes,
      historyDigest: entry.historyDigest,
      settingsDigest: entry.settingsDigest,
      scopeDigest: entry.scopeDigest,
    });
    return Object.freeze({
      fullRelayBody: body,
      relayBody: {
        ...body,
        store: true as const,
        input: body.input.slice(entry.itemCount),
        previous_response_id: entry.providerResponseId,
      },
      decision: "hit",
      match,
      inputDigest,
      settingsDigest: state.settingsDigest,
      settingsCanonicalBytes: state.settingsCanonicalBytes,
      settingsCanonicalNodes: state.settingsCanonicalNodes,
      scopeDigest: state.scopeDigest,
    });
  }

  #time(): number {
    const value = this.#now();
    if (!Number.isSafeInteger(value) || value < 0) throw new RangeError("continuation clock is invalid");
    // Absolute expiries share insertion order. Clamp a rare wall-clock rollback
    // so that order stays monotonic without extending any already-issued expiry.
    this.#lastNow = Math.max(this.#lastNow, value);
    return this.#lastNow;
  }

  #explicitPreparation(body: ResponsesRequestBody): PreparedAnthropicContinuation {
    const inputDigest = Object.freeze({ ...initialInputDigest(this.#key) });
    return Object.freeze({
      fullRelayBody: body,
      relayBody: body,
      decision: "explicit",
      match: null,
      inputDigest,
      // Explicit lineage never reaches commit. Content-free sentinels keep the
      // common result shape without canonicalizing ignored history/settings.
      settingsDigest: inputDigest.digest,
      settingsCanonicalBytes: 0,
      settingsCanonicalNodes: 0,
      scopeDigest: inputDigest.digest,
    });
  }

  #canonicalLimit(value: number | undefined, ceiling: number, name: string): number {
    const resolved = value ?? ceiling;
    if (!Number.isSafeInteger(resolved) || resolved < 1 || resolved > ceiling) {
      throw new RangeError(`${name} must be a positive safe integer no greater than ${ceiling}`);
    }
    return resolved;
  }

  #allocateGeneration(): number {
    const generation = this.#nextGeneration;
    if (!Number.isSafeInteger(generation) || generation < 1) {
      throw new RangeError("continuation generation is exhausted");
    }
    this.#nextGeneration = generation + 1;
    return generation;
  }

  #markMutation(): void {
    if (this.#mutationEpoch >= Number.MAX_SAFE_INTEGER) {
      throw new RangeError("continuation mutation epoch is exhausted");
    }
    this.#mutationEpoch++;
  }

  #touch(entry: ContinuationEntry): void {
    this.#entries.delete(entry.generation);
    this.#entries.set(entry.generation, entry);
  }

  #pruneExpired(now: number): void {
    for (;;) {
      const entry = this.#entryExpiry.values().next().value as ContinuationEntry | undefined;
      if (!entry || now < entry.expiresAt) break;
      this.#delete(entry);
    }
    for (;;) {
      const pair = this.#ambiguityExpiry.entries().next().value as
        | [string, AmbiguousContinuationBucket]
        | undefined;
      if (!pair || now < pair[1].expiresAt) break;
      this.#deleteAmbiguity(pair[0], pair[1]);
    }
  }

  #recordCount(): number {
    return this.#entries.size + this.#ambiguityExpiry.size;
  }

  #makeRoomForRecord(): boolean {
    while (
      this.#recordCount() >= ANTHROPIC_CONTINUATION_MAX_RECORDS ||
      (this.#recordCount() + 1) * ANTHROPIC_CONTINUATION_RECORD_METADATA_BYTES >
        ANTHROPIC_CONTINUATION_METADATA_BYTES
    ) {
      // Ambiguity tombstones cannot be evicted early: doing so could turn an
      // uncertain lineage into an apparently unique parent. Unique entries are
      // only an optimization and are safe to discard by LRU order.
      const oldest = this.#entries.values().next().value as ContinuationEntry | undefined;
      if (!oldest) return false;
      this.#delete(oldest);
    }
    return true;
  }

  #delete(entry: ContinuationEntry): void {
    if (!this.#entries.delete(entry.generation)) return;
    this.#entryExpiry.delete(entry.generation);
    const key = entryKey(entry.scopeDigest, entry.settingsDigest, {
      digest: entry.historyDigest,
      itemCount: entry.itemCount,
      canonicalBytes: entry.canonicalBytes,
      canonicalNodes: entry.canonicalNodes,
    });
    const bucket = this.#buckets.get(key);
    if (bucket?.kind === "unique" && bucket.generation === entry.generation) {
      this.#buckets.delete(key);
      this.#markMutation();
    }
  }

  #deleteAmbiguity(key: string, bucket: AmbiguousContinuationBucket): void {
    if (this.#buckets.get(key) !== bucket) return;
    this.#buckets.delete(key);
    this.#markMutation();
    this.#ambiguityExpiry.delete(key);
  }
}

export function terminalProviderResponseId(finalValue: unknown): string | null {
  try {
    const validated = validateTerminalResponseProjection(finalValue);
    return typeof validated.record.id === "string" ? validated.record.id : null;
  } catch {
    return null;
  }
}
