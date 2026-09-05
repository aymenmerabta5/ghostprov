import { SYSTEM_CLOCK, type Clock } from "../../core/ports";
import type { RunHandle } from "../../relay/run-handle";
import {
  MAX_RESPONSES_JSON_DEPTH,
  MAX_RESPONSES_JSON_ITEMS,
  ResponsesInputStructureError,
} from "../../relay/responses-validation";
import { normalizeHostUpstreamResponsesRequest as normalizeUpstreamResponsesRequest } from "../../relay/host-upstream-policy";
import type { SandboxAdmissionProgress } from "../../sandbox/registry";
import { jsonEncodedByteLength } from "../../router/workload";
import type { SseFrame } from "../../relay/sse";
import type {
  AwarenessChannel,
  AwarenessPeerUpdate,
} from "../awareness/channel";
import {
  GP_V2_R1_SCHEMA_VERSION,
  GP_V2_R2_SCHEMA_VERSION,
  GP_V2_R2_TRANSPORT_LIMITS,
  awarenessEnvelopeEncodedBytes,
  base64UrlDecode,
  base64UrlEncode,
  protocolTransportLimits,
  sha256Hex,
  utf8Bytes,
  utf8Text,
  type AwarenessEnvelope,
  type AwarenessLiveness,
  type AwarenessLane,
  type HostFrame,
  type ProtocolBinding,
  type ReliableFrame,
  type RequestEncoding,
  type R2AwarenessEnvelope,
  type R2RequestPreparedFrame,
  type R2RequestStartFrame,
  type SidecarFrame,
  type UpstreamPrelude,
  type V2ProtocolTransportLimits,
  type V2TransportProfile,
} from "../protocol/shared";
import { AckClockedFlight } from "../protocol/flight";
import { WeightedFrameScheduler } from "../protocol/scheduler";
import { ReliableDeliveryTiming } from "../protocol/delivery-timing";
import {
  PublicationPacer,
  type PublicationReason,
  type PublicationTimer,
} from "../protocol/publication-pacer";
import {
  HostRequestDiagnosticsObserver,
  type VersionedHostTransportDiagnostics,
} from "../diagnostics/host-observer";
import type { V2EnvelopeDiagnostics, V2TransportDiagnostics } from "../diagnostics/types";
import {
  MAX_TRANSCRIPT_CHUNK_BYTES,
  TranscriptProof,
  validateTerminalProof,
  type IncrementalSha256Hasher,
} from "../protocol/transcript";
import {
  encodedFrameBytes,
  InboundFrameCursor,
  OutboundFrameWindow,
} from "../protocol/window";
import { TransportCongestion } from "./congestion";
import {
  V2CommittedError,
  V2PreCommitError,
  V2RunHandle,
} from "./run-handle";
import { createNativeTranscriptHasher } from "./native-transcript-hasher";
import {
  v2HostTransferBudget,
  v2RequestTransferCost,
} from "./admission-cost";
import {
  AtomicBytePermit,
  AtomicBytePermitPool,
  estimateAtomicEventPermitBytes,
} from "../memory/atomic-byte-permits";
import { v2RetainedEventPermitBytes } from "../memory/event-memory-sizing";
import { releaseAtomicBytePermit } from "../memory/permit-carrier";

export { computeV2RetransmitDelayMs } from "../protocol/retransmit";
import {
  DEFAULT_V2_INITIAL_RETRANSMIT_MS,
  deterministicV2JitterSeed,
} from "../protocol/retransmit";

const R1_REQUEST_PART_BYTES = 8 * 1024;
/** Dense host-generated R2 part with >256 bytes of worst-case frame headroom. */
export const V2_R2_REQUEST_PART_BYTES = 11_900;
const MIN_GZIP_SAVINGS_RATIO = 0.05;
const LARGE_INLINE_BASE64_BYTES = 64 * 1024;
const BULK_GZIP_MIN_BYTES = 2 * 1024 * 1024;
const GZIP_SAMPLE_BYTES = 64 * 1024;
export const V2_LATENCY_CRITICAL_REQUEST_BYTES = 256 * 1024;
const MAX_PRECOMMIT_EXECUTION_RESERVATIONS = 4;
const MAX_BULK_BODY_PREFETCH_RESERVATIONS = 6;
const ACCELERATION_RECOVERY_CLEAN_FLIGHTS = 2;
const DEFAULT_TRANSPORT_STALL_MS = 2_000;
const GOODPUT_IDLE_RESET_MS = 60_000;
// Keep sub-MiB interactive requests on the original single-turn fast path;
// larger canonicalization yields often enough to bound host event-loop stalls.
const CANONICAL_CHUNK_CODE_UNITS = 1024 * 1024;
const CANONICAL_ITEMS_PER_YIELD = 256;
const MAX_CANONICAL_JSON_ITEMS = 1_000_000;
const MAX_PREPARATION_WAITERS = 128;
const MAX_FAST_PREPARED_PAYLOADS = 128;
const MAX_FAST_ACTIVE_PREPARATIONS = 16;
const PREPARATION_CANCELLATION_FENCE_MS = 25;
const FAST_PREPARATION_MAX_ITEMS = 4_096;
const FAST_PREPARATION_HEADROOM_BYTES = 64;
export const MAX_TERMINAL_OBJECT_KEYS = 4_096;
const MAX_TERMINAL_CANONICAL_KEY_UNITS = 256 * 1024;
const MAX_TERMINAL_CANONICAL_KEY_LENGTH = 4 * 1024;
const MAX_REQUEST_CANONICAL_KEY_UNITS = 256 * 1024;
const MAX_REQUEST_CANONICAL_KEY_LENGTH = 4 * 1024;
const TERMINAL_RESPONSE_EVENTS = new Set([
  "response.completed",
  "response.failed",
  "response.incomplete",
]);

type TerminalStructureFrame =
  | {
      readonly kind: "value";
      readonly value: unknown;
      readonly depth: number;
    }
  | {
      readonly kind: "array";
      readonly value: unknown[];
      readonly depth: number;
      readonly length: number;
      index: number;
    }
  | {
      readonly kind: "object";
      readonly value: Record<string, unknown>;
      readonly depth: number;
      readonly keys: Iterator<string>;
    };

function* terminalOwnEnumerableKeys(value: Record<string, unknown>): Generator<string> {
  for (const key in value) {
    if (Object.prototype.hasOwnProperty.call(value, key)) yield key;
  }
}

/** @internal Host-only structural fence before frozen terminal proof hashing. */
export function validateHostTerminalFinalStructure(root: Record<string, unknown>): void {
  const stack: TerminalStructureFrame[] = [{ kind: "value", value: root, depth: 0 }];
  const seen = new WeakSet<object>();
  let scheduledValues = 1;
  let keyUnits = 0;

  const scheduleChildren = (count: number): void => {
    if (count > MAX_RESPONSES_JSON_ITEMS - scheduledValues) {
      throw new Error("Terminal final contains too many JSON values");
    }
    scheduledValues += count;
  };

  while (stack.length > 0) {
    const frame = stack.pop()!;
    if (frame.kind === "array") {
      if (frame.index >= frame.length) continue;
      const index = frame.index++;
      if (!Object.hasOwn(frame.value, index)) {
        throw new Error("Terminal final contains a sparse array");
      }
      stack.push(frame, {
        kind: "value",
        value: frame.value[index],
        depth: frame.depth + 1,
      });
      continue;
    }
    if (frame.kind === "object") {
      const next = frame.keys.next();
      if (next.done) continue;
      stack.push(frame, {
        kind: "value",
        value: frame.value[next.value],
        depth: frame.depth + 1,
      });
      continue;
    }

    const value = frame.value;
    if (!value || typeof value !== "object") continue;
    if (frame.depth > MAX_RESPONSES_JSON_DEPTH) {
      throw new Error("Terminal final exceeds the maximum JSON nesting depth");
    }
    if (seen.has(value)) throw new Error("Terminal final contains a JSON cycle");
    seen.add(value);

    if (Array.isArray(value)) {
      scheduleChildren(value.length);
      stack.push({
        kind: "array",
        value,
        depth: frame.depth,
        length: value.length,
        index: 0,
      });
      continue;
    }

    const object = value as Record<string, unknown>;
    let keyCount = 0;
    for (const key in object) {
      if (!Object.prototype.hasOwnProperty.call(object, key)) continue;
      if (++keyCount > MAX_TERMINAL_OBJECT_KEYS) {
        throw new Error("Terminal final object contains too many keys");
      }
      keyUnits += key.length;
      if (
        key.length > MAX_TERMINAL_CANONICAL_KEY_LENGTH ||
        keyUnits > MAX_TERMINAL_CANONICAL_KEY_UNITS
      ) {
        throw new Error("Terminal final canonical keys exceed the host work limit");
      }
    }
    scheduleChildren(keyCount);
    stack.push({
      kind: "object",
      value: object,
      depth: frame.depth,
      keys: terminalOwnEnumerableKeys(object),
    });
  }
}

function hostFrameCarriesRequest(frame: ReliableFrame<HostFrame>): boolean {
  // Route payloads by request-body goodput only. Tiny request_start/commit
  // control flights would otherwise drag the EWMA toward a few KiB/s after
  // every successful handshake and make healthy sandboxes look unusable.
  return frame.kind === "request_data";
}

function snapshotFastPreparationBody(
  body: Record<string, unknown>,
): Readonly<Record<string, unknown>> | null {
  try {
    // Preserve the allocation-light hot path used by Codex's common flat
    // 128 KiB body. Nested JSON falls through to the bounded deep snapshot.
    const flat: Record<string, unknown> = Object.create(null);
    let flatKeys = 0;
    let flatUnits = 0;
    let flatCandidate = true;
    for (const key in body) {
      if (!Object.prototype.hasOwnProperty.call(body, key) || ++flatKeys > 32) {
        flatCandidate = false;
        break;
      }
      const descriptor = Object.getOwnPropertyDescriptor(body, key);
      if (!descriptor || !("value" in descriptor)) {
        flatCandidate = false;
        break;
      }
      const value = descriptor.value;
      if (value !== null && typeof value === "object" ||
        typeof value === "undefined" || typeof value === "function" ||
        typeof value === "symbol" || typeof value === "bigint" ||
        (typeof value === "number" && !Number.isFinite(value))) {
        flatCandidate = false;
        break;
      }
      flatUnits += key.length + (typeof value === "string" ? value.length : 0);
      if (flatUnits > V2_LATENCY_CRITICAL_REQUEST_BYTES) {
        flatCandidate = false;
        break;
      }
      flat[key] = value;
    }
    if (flatCandidate &&
      jsonEncodedByteLength(flat) + FAST_PREPARATION_HEADROOM_BYTES <=
        V2_LATENCY_CRITICAL_REQUEST_BYTES) return Object.freeze(flat);

    const stack: unknown[] = [body];
    const seen = new WeakSet<object>();
    let items = 0;
    let stringUnits = 0;
    while (stack.length > 0) {
      const value = stack.pop();
      if (++items > FAST_PREPARATION_MAX_ITEMS) return null;
      if (typeof value === "string") {
        stringUnits += value.length;
        if (stringUnits > V2_LATENCY_CRITICAL_REQUEST_BYTES) return null;
        continue;
      }
      if (value === null || typeof value === "boolean") continue;
      if (typeof value === "number") {
        if (!Number.isFinite(value)) return null;
        continue;
      }
      if (!value || typeof value !== "object" || seen.has(value)) return null;
      seen.add(value);
      if (Array.isArray(value)) {
        if (Object.getPrototypeOf(value) !== Array.prototype ||
          stack.length + value.length > FAST_PREPARATION_MAX_ITEMS - items) return null;
        for (let index = value.length - 1; index >= 0; index--) {
          const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
          if (!descriptor || !("value" in descriptor)) return null;
          stack.push(descriptor.value);
        }
        continue;
      }
      const object = value as Record<string, unknown>;
      const prototype = Object.getPrototypeOf(object);
      if (prototype !== Object.prototype && prototype !== null) return null;
      let ownKeys = 0;
      for (const key in object) {
        if (!Object.prototype.hasOwnProperty.call(object, key)) return null;
        if (++ownKeys + stack.length > FAST_PREPARATION_MAX_ITEMS - items) return null;
        const descriptor = Object.getOwnPropertyDescriptor(object, key);
        if (!descriptor || !("value" in descriptor)) return null;
        const child = descriptor.value;
        if (typeof child === "undefined" || typeof child === "function" ||
          typeof child === "symbol" || typeof child === "bigint") return null;
        stringUnits += key.length;
        if (stringUnits > V2_LATENCY_CRITICAL_REQUEST_BYTES) return null;
        stack.push(child);
      }
    }
    const encoded = JSON.stringify(body);
    if (typeof encoded !== "string") return null;
    const snapshot = JSON.parse(encoded) as Record<string, unknown>;
    if (
      jsonEncodedByteLength(snapshot) + FAST_PREPARATION_HEADROOM_BYTES >
        V2_LATENCY_CRITICAL_REQUEST_BYTES
    ) return null;
    return Object.freeze(snapshot);
  } catch {
    return null;
  }
}

type CanonicalContainer =
  | {
      kind: "array";
      value: unknown[];
      depth: number;
      index: number;
      length: number;
    }
  | {
      kind: "object";
      value: Record<string, unknown>;
      depth: number;
      index: number;
      keys: string[];
    };

/**
 * Host-only stack-safe equivalent of protocol canonicalJson. The checked-in
 * sidecar serializer is frozen; direct transport callers still need a bounded
 * serialization path before their bytes enter the R8 request digest.
 */
function yieldCanonicalWork(signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) {
    return Promise.reject(abortError("V2 request aborted during canonical serialization"));
  }
  return new Promise<void>((resolve, reject) => {
    let settled = false;
    const finish = (operation: () => void): void => {
      if (settled) return;
      settled = true;
      clearImmediate(immediate);
      signal?.removeEventListener("abort", onAbort);
      operation();
    };
    const onAbort = () => finish(() => reject(
      abortError("V2 request aborted during canonical serialization"),
    ));
    const immediate = setImmediate(() => finish(resolve));
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

async function canonicalHostRequestBytes(
  value: unknown,
  maxBytes: number,
  signal?: AbortSignal,
): Promise<{
  bytes: Uint8Array;
  containsLargeInlineBase64: boolean;
}> {
  const chunks: Uint8Array[] = [];
  let buffered = "";
  let byteCount = 0;
  let workSinceYield = 0;
  let itemsSinceYield = 0;
  let canonicalKeyUnits = 0;
  let containsLargeInlineBase64 = false;
  let inlineBase64Units = 0;
  const flush = (): void => {
    if (!buffered) return;
    const encoded = utf8Bytes(buffered);
    buffered = "";
    if (byteCount + encoded.byteLength > maxBytes) {
      throw new V2PreCommitError("V2 request exceeds the 64 MiB limit", "request_too_large");
    }
    chunks.push(encoded);
    byteCount += encoded.byteLength;
  };
  const append = (part: string): void => {
    buffered += part;
    workSinceYield += part.length;
    if (buffered.length < CANONICAL_CHUNK_CODE_UNITS) return;
    flush();
  };
  const checkpoint = (): Promise<void> | null => {
    if (signal?.aborted) {
      throw abortError("V2 request aborted during canonical serialization");
    }
    if (
      workSinceYield < CANONICAL_CHUNK_CODE_UNITS &&
      itemsSinceYield < CANONICAL_ITEMS_PER_YIELD
    ) return null;
    workSinceYield = 0;
    itemsSinceYield = 0;
    return yieldCanonicalWork(signal);
  };
  let canonicalItems = 0;
  const countCanonicalItem = (): void => {
    canonicalItems++;
    itemsSinceYield++;
    if (canonicalItems > MAX_CANONICAL_JSON_ITEMS) {
      throw new ResponsesInputStructureError("body");
    }
  };
  const chargeCanonicalKey = (key: string): void => {
    canonicalKeyUnits += key.length;
    if (key.length > MAX_REQUEST_CANONICAL_KEY_LENGTH ||
      canonicalKeyUnits > MAX_REQUEST_CANONICAL_KEY_UNITS) {
      throw new ResponsesInputStructureError("body", "items");
    }
  };
  const sortedCanonicalKeysAsync = async (
    input: Record<string, unknown>,
  ): Promise<string[]> => {
    const keys: string[] = [];
    for (const key in input) {
      workSinceYield += Math.max(1, key.length);
      itemsSinceYield++;
      const pendingYield = checkpoint();
      if (pendingYield) await pendingYield;
      if (!Object.prototype.hasOwnProperty.call(input, key)) continue;
      if (keys.length >= MAX_CANONICAL_JSON_ITEMS - canonicalItems) {
        throw new ResponsesInputStructureError("body");
      }
      chargeCanonicalKey(key);
      keys.push(key);
    }
    if (keys.length < 2) return keys;

    // Native Array#sort is one uninterruptible O(n log n) turn. A bottom-up
    // merge keeps the same UTF-16 lexical order while yielding cooperatively.
    let source = keys;
    let target = new Array<string>(keys.length);
    for (let width = 1; width < keys.length; width *= 2) {
      for (let start = 0; start < keys.length; start += width * 2) {
        const middle = Math.min(start + width, keys.length);
        const end = Math.min(start + width * 2, keys.length);
        let left = start;
        let right = middle;
        for (let output = start; output < end; output++) {
          if (
            left < middle &&
            (right >= end || source[left]! <= source[right]!)
          ) target[output] = source[left++]!;
          else target[output] = source[right++]!;
          workSinceYield++;
          itemsSinceYield++;
          const pendingYield = checkpoint();
          if (pendingYield) await pendingYield;
        }
      }
      [source, target] = [target, source];
    }
    return source;
  };
  const sortedCanonicalKeys = (
    input: Record<string, unknown>,
  ): string[] | Promise<string[]> => {
    const small: string[] = [];
    let chargedUnits = 0;
    for (const key in input) {
      if (!Object.prototype.hasOwnProperty.call(input, key)) continue;
      chargeCanonicalKey(key);
      chargedUnits += key.length;
      small.push(key);
      if (small.length > 256) {
        canonicalKeyUnits -= chargedUnits;
        return sortedCanonicalKeysAsync(input);
      }
    }
    return small.sort();
  };
  const appendLargeJsonString = async (input: string): Promise<void> => {
    append('"');
    for (let offset = 0; offset < input.length;) {
      let end = Math.min(input.length, offset + CANONICAL_CHUNK_CODE_UNITS);
      if (
        end < input.length &&
        input.charCodeAt(end - 1) >= 0xd800 &&
        input.charCodeAt(end - 1) <= 0xdbff &&
        input.charCodeAt(end) >= 0xdc00 &&
        input.charCodeAt(end) <= 0xdfff
      ) end--;
      const encoded = JSON.stringify(input.slice(offset, end));
      append(encoded.slice(1, -1));
      offset = end;
      const pendingYield = checkpoint();
      if (pendingYield) await pendingYield;
    }
    append('"');
  };
  const appendJsonString = (input: string): Promise<void> | null => {
    if (input.length >= CANONICAL_CHUNK_CODE_UNITS) {
      return appendLargeJsonString(input);
    }
    append(JSON.stringify(input));
    return null;
  };
  const ancestors = new WeakSet<object>();
  const containers: CanonicalContainer[] = [];
  // The shared Responses validator defines each top-level field (and each
  // element of the top-level input array) as depth zero. Keep the transport
  // serializer on that same acceptance boundary while leaving the body shell
  // and input collection themselves uncounted.
  let pending: { value: unknown; depth: number } | null = { value, depth: -1 };

  for (;;) {
    if (pending) {
      const current = pending;
      pending = null;
      const candidate = current.value;
      countCanonicalItem();
      if (candidate === null) {
        append("null");
      } else if (typeof candidate === "string") {
        if (
          !containsLargeInlineBase64 &&
          /^data:[^;,]{1,127};base64,/i.test(candidate)
        ) {
          inlineBase64Units = Math.min(
            LARGE_INLINE_BASE64_BYTES,
            inlineBase64Units + candidate.length,
          );
          containsLargeInlineBase64 = inlineBase64Units >= LARGE_INLINE_BASE64_BYTES;
        }
        const pendingString = appendJsonString(candidate);
        if (pendingString) await pendingString;
      } else if (typeof candidate === "boolean") {
        append(JSON.stringify(candidate));
      } else if (typeof candidate === "number") {
        if (!Number.isFinite(candidate)) {
          throw new TypeError("Canonical JSON numbers must be finite");
        }
        append(JSON.stringify(candidate));
      } else {
        if (typeof candidate !== "object") {
          throw new TypeError(`Canonical JSON does not support ${typeof candidate}`);
        }
        if (current.depth > MAX_RESPONSES_JSON_DEPTH) {
          throw new ResponsesInputStructureError("body");
        }
        if (ancestors.has(candidate)) {
          throw new TypeError("Canonical JSON does not support cycles");
        }
        ancestors.add(candidate);
        if (Array.isArray(candidate)) {
          append("[");
          containers.push({
            kind: "array",
            value: candidate,
            depth: current.depth,
            index: 0,
            length: candidate.length,
          });
        } else {
          const sortedKeys = sortedCanonicalKeys(candidate as Record<string, unknown>);
          append("{");
          containers.push({
            kind: "object",
            value: candidate as Record<string, unknown>,
            depth: current.depth,
            index: 0,
            keys: sortedKeys instanceof Promise ? await sortedKeys : sortedKeys,
          });
        }
      }
      const pendingYield = checkpoint();
      if (pendingYield) await pendingYield;
      continue;
    }

    const container = containers.at(-1);
    if (!container) break;
    if (container.kind === "array") {
      if (container.index >= container.length) {
        append("]");
        ancestors.delete(container.value);
        containers.pop();
        continue;
      }
      const index = container.index++;
      if (index > 0) append(",");
      const descriptor = Object.getOwnPropertyDescriptor(container.value, String(index));
      if (!descriptor || !("value" in descriptor)) {
        throw new TypeError("Canonical JSON arrays must be dense data values");
      }
      pending = { value: descriptor.value, depth: container.depth + 1 };
      continue;
    }

    if (container.index >= container.keys.length) {
      append("}");
      ancestors.delete(container.value);
      containers.pop();
      continue;
    }
    const index = container.index++;
    if (index > 0) append(",");
    const key = container.keys[index]!;
    const pendingKey = appendJsonString(key);
    if (pendingKey) await pendingKey;
    append(":");
    const child = container.value[key];
    pending = {
      value: child,
      depth: container.depth === -1 && key === "input" && Array.isArray(child)
        ? -1
        : container.depth + 1,
    };
    const pendingYield = checkpoint();
    if (pendingYield) await pendingYield;
  }

  flush();
  const output = new Uint8Array(byteCount);
  let offset = 0;
  workSinceYield = 0;
  itemsSinceYield = 0;
  for (const chunk of chunks) {
    output.set(chunk, offset);
    offset += chunk.byteLength;
    workSinceYield += chunk.byteLength;
    const pendingYield = checkpoint();
    if (pendingYield) await pendingYield;
  }
  return { bytes: output, containsLargeInlineBase64 };
}

export interface V2TransportSnapshot {
  connected: boolean;
  committed: number;
  active: number;
  /** Active host request preparations. Always bounded by local preparation limits. */
  preparing?: number;
  /** Body-only R2 reservations waiting behind the four-start safety gate. */
  prefetched?: number;
  queued: number;
  quarantined: number;
  ambiguousQuarantined?: number;
  ambiguityLimit?: number;
  credits: number;
  admissionCredits?: number;
  /** Safe host run calls, including bounded body-only bulk-profile prefetch capacity. */
  prefetchCredits?: number;
  /** Remaining weighted request-body transfer units after reflected reservations. */
  transferCredits?: number;
  admissionBlockReason?: V2AdmissionBlockReason;
  retransmissions: number;
  awarenessRttMs: number | null;
  /** Age of the newest valid envelope from the bootstrap-bound sidecar instance. */
  sidecarPeerAgeMs?: number | null;
  diagnostics?: { host: V2TransportDiagnostics; sidecar: V2EnvelopeDiagnostics | null };
}

export type V2AdmissionBlockReason =
  | "available"
  | "transport_unavailable"
  | "transport_recovery"
  | "draining"
  | "transport_congested"
  | "zero_remote_credits"
  | "host_ceiling"
  | "execution_window"
  | "transfer_budget";

export type V2TransportLivenessErrorCode =
  | "unavailable"
  | "probe_in_flight"
  | "timeout"
  | "publish"
  | "instance_changed";

export class V2TransportLivenessError extends Error {
  readonly name = "V2TransportLivenessError";

  constructor(
    message: string,
    readonly code: V2TransportLivenessErrorCode,
    options?: ErrorOptions,
  ) {
    super(message, options);
  }
}

export interface V2TransportLivenessResult {
  rttMs: number;
  sentAtMs: number;
}

export interface V2GzipOptions {
  signal?: AbortSignal;
  /** The supplied view is a disposable, complete buffer owned by the caller. */
  inputOwnership?: "owned";
  priority?: "latency" | "bulk";
}

export interface V2TransportRunOptions {
  /** Ownership handoff after this session charges its admission credit. */
  onAdmissionReflected?(): void;
  /** Safe pre-execution progress; observer exceptions are isolated. */
  onAdmissionProgress?(progress: SandboxAdmissionProgress): void;
  /** Irreversible execution handoff after the protocol-specific start fence. */
  onExecutionRiskReflected?(): void;
}

export interface V2TransportSessionOptions {
  /** Bounded piggybacking of verified receipts on an immediate follow-up flight. */
  terminalReceiptCoalescing?: boolean;
  binding: ProtocolBinding;
  transportProfile?: V2TransportProfile;
  channel: AwarenessChannel<HostFrame, SidecarFrame>;
  clock?: Clock;
  gzip?: (bytes: Uint8Array, options?: V2GzipOptions) => Promise<Uint8Array>;
  maxResponseBytes: number;
  maxBufferedResponseBytes?: number;
  hostCeiling?: number;
  retransmitMs?: number;
  rotationTimeoutMs?: number;
  publicationIntervalMs?: number;
  /** Primarily injectable for deterministic component tests. */
  publicationTimer?: PublicationTimer;
  unadmittedTimeoutMs?: number;
  /** Sidecar instance proven by the bootstrap handshake. Required for liveness probes. */
  expectedSidecarInstanceId?: string;
  /** Deterministic tests may inject this; production uses a fresh UUID. */
  hostInstanceId?: string;
  /** Primarily injectable for deterministic tests. Production defaults to five seconds. */
  livenessTimeoutMs?: number;
  /** One process-wide pool shared by every V2 transport session. */
  eventMemoryPool?: AtomicBytePermitPool;
  /** Coalesced notification when admission or transport-health state changes. */
  onStateChange?(): void;
  /** Deterministic tests may shorten the production two-second stall fence. */
  transportStallMs?: number;
}

interface EventPart {
  offset: number;
  bytes: Uint8Array;
}

interface EventAssembly {
  event: string;
  partCount: number;
  parts: Map<number, EventPart>;
  rawBytes: number;
  permit: AtomicBytePermit | null;
}

interface CompletedEvent {
  frame: SseFrame;
  wireBytes: number;
  permit: AtomicBytePermit | null;
}

interface RecoveryPrefix {
  readonly expectedEventCount: number;
  readonly expectedByteCount: number;
  readonly expectedDigest: string;
  readonly hasher: IncrementalSha256Hasher;
  hashedEventCount: number;
  hashedByteCount: number;
  nextEventIndex: number;
  segmented: {
    event: string;
    partCount: number;
    nextPartIndex: number;
    nextOffset: number;
  } | null;
}

interface PreparedRequestPlan {
  requestDigest: string;
  decodedBytes: number;
  encodedBytes: number;
  encoding: RequestEncoding;
  partCount: number;
  partBytes: number;
  encodedPayload: Uint8Array | null;
  nextPartIndex: number;
  stage: "open" | "data" | "commit" | "done";
}

interface RequestManifest {
  requestDigest: string;
  decodedBytes: number;
  encodedBytes: number;
  encoding: RequestEncoding;
  partCount: number;
}

interface RequestState {
  requestId: string;
  requestDigest: string;
  manifest: RequestManifest;
  /** Lazily consumed so large base64 frame arrays are never retained. */
  preparedRequest: PreparedRequestPlan | null;
  preparationRetained: boolean;
  preparationFast: boolean;
  handle: V2RunHandle;
  transcript: TranscriptProof<false>;
  resolveRun: (handle: RunHandle) => void;
  rejectRun: (error: unknown) => void;
  signal?: AbortSignal;
  onAbort?: () => void;
  onAdmissionReflected?: () => void;
  onAdmissionProgress?: (progress: SandboxAdmissionProgress) => void;
  acknowledgedEncodedBytes: number;
  onExecutionRiskReflected?: () => void;
  admitted: boolean;
  transferCost: number;
  latencyCriticalPreCommitTracked: boolean;
  latencyCriticalRequestTracked: boolean;
  published: boolean;
  commitStaged: boolean;
  commitPublished: boolean;
  /** Irreversible R2 boundary after request_start enters the cumulative window. */
  startRisk: boolean;
  startQueued: boolean;
  startReserved: boolean;
  preparedStart: R2RequestStartFrame | null;
  committed: boolean;
  cancelled: boolean;
  quarantineKind: "cancellation" | "ambiguity" | null;
  responseBytes: number;
  nextEventIndex: number;
  assemblies: Map<number, EventAssembly>;
  completedEvents: Map<number, CompletedEvent>;
  terminal: Extract<ReliableFrame<SidecarFrame>, { kind: "response_terminal" }> | null;
  cancelledTerminalPermit: AtomicBytePermit | null;
  terminalFinalizationAnchor: object | null;
  upstreamPrelude: UpstreamPrelude | null;
  diagnostics: HostRequestDiagnosticsObserver;
  firstResponseSeen: boolean;
  admissionTimer?: ReturnType<typeof setTimeout>;
  recoveryPrefix?: RecoveryPrefix;
}

export interface V2SidecarRecoveryToken {
  readonly id: string;
  readonly binding: ProtocolBinding;
  readonly previousInstanceId: string;
}

interface SidecarRecoveryState {
  token: V2SidecarRecoveryToken;
  markerSeen: boolean;
  /** IDs imported from the retired host and therefore never orphan candidates. */
  ownedRequestIds: ReadonlySet<string>;
  resolve(): void;
  reject(error: unknown): void;
  promise: Promise<void>;
}

interface TerminalReceipt {
  terminal: Extract<SidecarFrame, { kind: "response_terminal" }>;
  queued: boolean;
  replayed: boolean;
  cursor: { epoch: number; sequence: number } | null;
}

interface OrphanEventAssembly extends EventAssembly {
  eventIndex: number;
  nextPartIndex: number;
  nextOffset: number;
}

interface OrphanRecoveryCandidate {
  requestId: string;
  requestDigest: string;
  transcript: TranscriptProof<false> | null;
  terminalProof: Extract<SidecarFrame, { kind: "response_terminal" }> | null;
  responseBytes: number;
  nextEventIndex: number;
  assembly: OrphanEventAssembly | null;
}

interface SidecarRecoveryCapsulePayload {
  states: RequestState[];
  receipts: Map<string, TerminalReceipt>;
  ambiguityTombstones: Map<string, string>;
  consumed: boolean;
}

const SIDECAR_RECOVERY_CAPSULES = new WeakMap<object, SidecarRecoveryCapsulePayload>();

interface RotationWaiter {
  resolve: () => void;
  reject: (error: unknown) => void;
  signal?: AbortSignal;
  onAbort?: () => void;
  timer?: ReturnType<typeof setTimeout>;
}

interface PreparationWaiter {
  readonly fast: boolean;
  resolve(controller: AbortController): void;
  reject(error: unknown): void;
  signal?: AbortSignal;
  onAbort?: () => void;
  timer?: ReturnType<typeof setTimeout>;
}

interface LivenessProbe {
  sentAtMs: number;
  startedAtMs: number;
  resolve: (result: V2TransportLivenessResult) => void;
  reject: (error: unknown) => void;
  signal?: AbortSignal;
  onAbort?: () => void;
  timer?: ReturnType<typeof setTimeout>;
}

const SAFE_INSTANCE_ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
const DEFAULT_LIVENESS_TIMEOUT_MS = 5_000;
const MAX_TERMINAL_RECEIPTS = 4_096;
const TERMINAL_RECEIPT_COALESCE_MS = 10;

function abortError(message: string): DOMException {
  return new DOMException(message, "AbortError");
}

function preCommitCancellationError(): V2PreCommitError {
  return new V2PreCommitError(
    "V2 request was cancelled before provider execution",
    "cancelled_precommit",
  );
}

function bytesEqual(left: Uint8Array, right: Uint8Array): boolean {
  if (left.byteLength !== right.byteLength) return false;
  let difference = 0;
  for (let index = 0; index < left.byteLength; index++) {
    difference |= left[index]! ^ right[index]!;
  }
  return difference === 0;
}

function concat(parts: readonly Uint8Array[], total: number): Uint8Array {
  const output = new Uint8Array(total);
  let offset = 0;
  for (const part of parts) {
    output.set(part, offset);
    offset += part.byteLength;
  }
  return output;
}

function base64UrlDecodedBytes(value: string): number | null {
  if (!/^[A-Za-z0-9_-]*$/.test(value) || value.length % 4 === 1) return null;
  const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
  const remainder = value.length % 4;
  if (remainder === 2 && (alphabet.indexOf(value.at(-1)!) & 0x0f) !== 0) return null;
  if (remainder === 3 && (alphabet.indexOf(value.at(-1)!) & 0x03) !== 0) return null;
  return Math.floor(value.length * 3 / 4);
}

/**
 * Host-only fast path after base64UrlDecodedBytes has already proved the
 * alphabet, padding shape, trailing bits, and exact decoded length. The shared
 * browser/Deno decoder must materialize an atob binary string and copy it one
 * byte at a time; Bun's native Buffer decoder avoids that transient string.
 */
function decodeCanonicalBase64Url(value: string, decodedBytes: number): Uint8Array {
  const output = Buffer.from(value, "base64url");
  if (output.byteLength !== decodedBytes) {
    throw new TypeError("Value must be canonical unpadded base64url");
  }
  return output;
}

function partialEventLogicalBytes(event: string, rawBytes: number): number {
  return utf8Bytes(`event: ${event}\ndata: \n\n`).byteLength + rawBytes;
}

function canonicalEventWireBytes(event: string, parts: readonly Uint8Array[]): number {
  let rawBytes = 0;
  for (const part of parts) {
    rawBytes += part.byteLength;
  }
  // TranscriptProof's frozen canonical contract is one data prefix followed by
  // the exact event-data string, including any embedded CR/LF bytes.
  return partialEventLogicalBytes(event, rawBytes);
}

function equivalentEventStreamContentType(left: string, right: string): boolean {
  const normalize = (value: string): string | null => {
    const parts = value.split(";").map(part => part.trim().toLowerCase());
    if (parts.shift() !== "text/event-stream") return null;
    const retained: string[] = [];
    for (const part of parts) {
      if (!part) continue;
      const separator = part.indexOf("=");
      const key = separator < 0 ? part : part.slice(0, separator).trim();
      const raw = separator < 0 ? "" : part.slice(separator + 1).trim().replace(/^"|"$/g, "");
      if (key === "charset" && (raw === "utf-8" || raw === "utf8")) continue;
      retained.push(`${key}=${raw}`);
    }
    retained.sort();
    return `text/event-stream;${retained.join(";")}`;
  };
  const normalizedLeft = normalize(left);
  return normalizedLeft !== null && normalizedLeft === normalize(right);
}

function validBinding(
  envelope: AwarenessEnvelope<SidecarFrame>,
  binding: ProtocolBinding,
): boolean {
  const revisionMatches = binding.protocolRevision === "r2"
    ? envelope.schemaVersion === GP_V2_R2_SCHEMA_VERSION &&
      envelope.protocolRevision === "r2" &&
      envelope.launchNonce === binding.launchNonce
    : envelope.schemaVersion === GP_V2_R1_SCHEMA_VERSION;
  return envelope.protocol === "gp-awareness-v2" &&
    revisionMatches &&
    envelope.projectId === binding.projectId &&
    envelope.sessionId === binding.sessionId &&
    envelope.leaderGeneration === binding.leaderGeneration &&
    envelope.buildHash === binding.buildHash &&
    envelope.schemaHash === binding.schemaHash &&
    envelope.peerId === "sidecar";
}

function sameRecoveryScope(left: ProtocolBinding, right: ProtocolBinding): boolean {
  return left.projectId === right.projectId &&
    left.sessionId === right.sessionId &&
    left.leaderGeneration === right.leaderGeneration &&
    left.buildHash === right.buildHash &&
    left.schemaHash === right.schemaHash &&
    left.protocolRevision === right.protocolRevision;
}

type PublicationUrgency = "paced" | "urgent";

function hostFrameUrgency(frame: HostFrame): PublicationUrgency {
  switch (frame.kind) {
    case "request_open":
    case "request_data":
    case "request_commit":
      return "paced";
    case "request_start":
      return "urgent";
    default:
      return "urgent";
  }
}

function strongestUrgency(
  left: PublicationUrgency | null,
  right: PublicationUrgency | null,
): PublicationUrgency | null {
  if (left === "urgent" || right === "urgent") return "urgent";
  return left ?? right;
}

export class V2TransportSession {
  readonly #binding: ProtocolBinding;
  readonly #limits: V2ProtocolTransportLimits;
  readonly #transportProfile: V2TransportProfile;
  readonly #transferBudget: number;
  readonly #channel: AwarenessChannel<HostFrame, SidecarFrame>;
  readonly #clock: Clock;
  readonly #gzip?: (bytes: Uint8Array, options?: V2GzipOptions) => Promise<Uint8Array>;
  readonly #maxResponseBytes: number;
  readonly #maxBufferedResponseBytes: number;
  readonly #eventMemoryPool?: AtomicBytePermitPool;
  readonly #onStateChange?: () => void;
  readonly #transportStallMs: number;
  readonly #fastRetransmitFloorMs: number;
  #hostCeiling: number;
  readonly #rotationTimeoutMs: number;
  readonly #unadmittedTimeoutMs: number;
  readonly #expectedSidecarInstanceId: string | null;
  readonly #livenessTimeoutMs: number;
  readonly #hostInstanceId: string;
  readonly #outbound: OutboundFrameWindow<HostFrame>;
  readonly #inbound: InboundFrameCursor<SidecarFrame>;
  #processedInboundAck = { epoch: 1, sequence: -1 };
  readonly #scheduler = new WeightedFrameScheduler<HostFrame>();
  readonly #delivery: ReliableDeliveryTiming;
  readonly #flight = new AckClockedFlight({ epoch: 1 });
  readonly #pacer: PublicationPacer;
  readonly #requests = new Map<string, RequestState>();
  readonly #preparations = new Set<AbortController>();
  readonly #fastPreparations = new Set<AbortController>();
  readonly #preparationWaiters: PreparationWaiter[] = [];
  #retainedPreparedPayloads = 0;
  #retainedFastPreparedPayloads = 0;
  #committedRequestCount = 0;
  #admittedUncommittedRequestCount = 0;
  #unadmittedRequestCount = 0;
  #admissionProgressObservers = 0;
  readonly #rotations = new Map<string, RotationWaiter>();
  readonly #rotationStreams = new Set<string>();
  readonly #pending: RequestState[] = [];
  readonly #preparedStarts: RequestState[] = [];
  readonly #idleWaiters = new Set<() => void>();
  readonly #retiredSidecarInstances = new Set<string>();
  readonly #terminalReceipts = new Map<string, TerminalReceipt>();
  readonly #terminalReceiptCoalescing: boolean;
  // Shares the receipt registry's count bound; never holds response content.
  readonly #deferredTerminalReceipts = new Set<string>();
  #terminalReceiptTimer: AbortController | null = null;
  readonly #ambiguityTombstones = new Map<string, string>();
  readonly #orphanRecoveryCandidates = new Map<string, OrphanRecoveryCandidate>();
  readonly #startupRecoveryOwnedRequestIds = new Set<string>();
  #sidecarRecovery: SidecarRecoveryState | null = null;
  #startupRecoveryOpen = true;
  #connected = false;
  #closed = false;
  #draining = false;
  #protocolFailed = false;
  #connecting: Promise<void> | null = null;
  #unsubscribe: (() => void) | null = null;
  #timer: ReturnType<typeof setTimeout> | null = null;
  #stallTimer: ReturnType<typeof setTimeout> | null = null;
  /** Serializes envelope metadata/cursor admission, never per-request delivery. */
  #processing: Promise<void> = Promise.resolve();
  /** At most one accepted reliable-window worth of bounded frame tasks. */
  readonly #inboundFrameTasks = new Set<Promise<void>>();
  /** Per-request tails preserve response order without coupling unrelated requests. */
  readonly #inboundFrameTails = new Map<string, Promise<void>>();
  /** Successfully processed sequences waiting behind an earlier unfinished frame. */
  readonly #completedInboundSequences = new Set<number>();
  /** Global protocol markers form a barrier across every earlier request tail. */
  #inboundFrameBarrier: Promise<void> = Promise.resolve();
  /**
   * Event-memory waits deliberately backpressure the reliable data lane. A
   * terminal close or same-session recovery must abort those waits before it
   * joins the processing tail, otherwise an occupied jumbo lane could make
   * teardown wait on downstream delivery owned by another session.
   */
  readonly #eventMemoryWaitAbort = new AbortController();
  #lifecycleEpoch = 0;
  #teardownPromise: Promise<void> | null = null;
  #dataRevision = 0;
  #controlRevision = 0;
  #dataDirty = false;
  #sidecarInstanceId: string | null = null;
  readonly #sidecarRevision: Record<AwarenessLane, number> = {
    data: -1,
    control: -1,
  };
  readonly #observedPeerRevision: Record<AwarenessLane, number> = {
    data: -1,
    control: -1,
  };
  #lastValidPeerAtMs: number | null = null;
  #lastLivenessSentAtMs = -1;
  #livenessProbe: LivenessProbe | null = null;
  #remoteCredits = 0;
  #creditReservations = 0;
  #transferReservations = 0;
  #startReservations = 0;
  #latencyCriticalPreCommitTransfers = 0;
  #latencyCriticalRequests = 0;
  #quarantined = 0;
  #ambiguousQuarantined = 0;
  #retransmissions = 0;
  readonly #drainWaiters = new Set<{
    resolve(): void;
    reject(error: unknown): void;
  }>();
  #drainFrameQueued = false;
  #remoteDrained = false;
  #sidecarDiagnostics: V2EnvelopeDiagnostics | null = null;
  #publications = 0;
  #pacedPublications = 0;
  #urgentPublications = 0;
  #publishedBytes = 0;
  #hostDiagnosticsRevision = 0;
  #publishedHostDiagnostics: VersionedHostTransportDiagnostics | null = null;
  readonly #hostDiagnosticsSource = (): VersionedHostTransportDiagnostics | null =>
    this.#publishedHostDiagnostics;
  #dataPublications = 0;
  #controlPublications = 0;
  #dataPublishedBytes = 0;
  #controlPublishedBytes = 0;
  #flightCompletions = 0;
  #peakRetransmissionAttempt = 0;
  #unadmittedTimeouts = 0;
  readonly #congestion: TransportCongestion;
  #stallEvents = 0;
  #stateChangeScheduled = false;
  #flightStartedAtMs: number | null = null;
  #flightRequestBytes = 0;
  #currentFlightRetransmitted = false;
  #acceleratedFlightsHealthy = true;
  #cleanRequestFlightsSinceTrouble = 0;
  #acknowledgedGoodputBytesPerSecond: number | null = null;
  #goodputUpdatedAtMs: number | null = null;
  #uniqueAcknowledgedBytes = 0;
  #retransmittedBytes = 0;
  #lastFastRetransmitAtMs: number | null = null;
  #stallTimerDueAtMs: number | null = null;

  constructor(options: V2TransportSessionOptions) {
    if (options.channel.projectId !== options.binding.projectId) {
      throw new TypeError("Awareness channel belongs to another V2 project");
    }
    if (!Number.isSafeInteger(options.maxResponseBytes) || options.maxResponseBytes < 1) {
      throw new RangeError("maxResponseBytes must be a positive safe integer");
    }
    const maxBufferedResponseBytes = options.maxBufferedResponseBytes ?? Math.min(
      options.maxResponseBytes,
      512 * 1024,
    );
    if (
      !Number.isSafeInteger(maxBufferedResponseBytes) ||
      maxBufferedResponseBytes < 1 ||
      maxBufferedResponseBytes > options.maxResponseBytes
    ) {
      throw new RangeError("maxBufferedResponseBytes must be between 1 and maxResponseBytes");
    }
    const hostCeiling = options.hostCeiling ?? 128;
    if (!Number.isSafeInteger(hostCeiling) || hostCeiling < 0 || hostCeiling > 128) {
      throw new RangeError("hostCeiling must be an integer from 0 through 128");
    }
    const retransmitMs = options.retransmitMs ?? 250;
    if (!Number.isFinite(retransmitMs) || retransmitMs < 1) {
      throw new RangeError("retransmitMs must be positive");
    }
    const rotationTimeoutMs = options.rotationTimeoutMs ?? 30_000;
    if (!Number.isSafeInteger(rotationTimeoutMs) || rotationTimeoutMs < 1) {
      throw new RangeError("rotationTimeoutMs must be a positive safe integer");
    }
    const unadmittedTimeoutMs = options.unadmittedTimeoutMs ?? 1_000;
    if (!Number.isSafeInteger(unadmittedTimeoutMs) || unadmittedTimeoutMs < 1) {
      throw new RangeError("unadmittedTimeoutMs must be a positive safe integer");
    }
    if (
      options.expectedSidecarInstanceId !== undefined &&
      !SAFE_INSTANCE_ID.test(options.expectedSidecarInstanceId)
    ) {
      throw new TypeError("expectedSidecarInstanceId is invalid");
    }
    if (options.hostInstanceId !== undefined && !SAFE_INSTANCE_ID.test(options.hostInstanceId)) {
      throw new TypeError("hostInstanceId is invalid");
    }
    if (!SAFE_INSTANCE_ID.test(options.binding.launchNonce)) {
      throw new TypeError("binding launchNonce is invalid");
    }
    if (options.binding.protocolRevision === "r2" && !options.expectedSidecarInstanceId) {
      throw new TypeError("R2 transport requires expectedSidecarInstanceId");
    }
    const livenessTimeoutMs = options.livenessTimeoutMs ?? DEFAULT_LIVENESS_TIMEOUT_MS;
    if (!Number.isSafeInteger(livenessTimeoutMs) || livenessTimeoutMs < 1) {
      throw new RangeError("livenessTimeoutMs must be a positive safe integer");
    }
    this.#binding = options.binding;
    this.#transportProfile = options.transportProfile ?? "standard";
    this.#limits = protocolTransportLimits(
      options.binding.protocolRevision,
      this.#transportProfile,
    );
    this.#transferBudget = v2HostTransferBudget(this.#transportProfile);
    this.#outbound = new OutboundFrameWindow<HostFrame>({
      epoch: 1,
      limits: this.#limits,
    });
    this.#inbound = new InboundFrameCursor<SidecarFrame>({
      epoch: 1,
      limits: this.#limits,
    });
    this.#channel = options.channel;
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#gzip = options.gzip;
    this.#maxResponseBytes = options.maxResponseBytes;
    this.#maxBufferedResponseBytes = maxBufferedResponseBytes;
    this.#eventMemoryPool = options.eventMemoryPool;
    this.#onStateChange = options.onStateChange;
    this.#terminalReceiptCoalescing = options.terminalReceiptCoalescing ?? true;
    this.#transportStallMs = options.transportStallMs ?? DEFAULT_TRANSPORT_STALL_MS;
    if (!Number.isFinite(this.#transportStallMs) || this.#transportStallMs < 1) {
      throw new RangeError("transportStallMs must be positive");
    }
    this.#fastRetransmitFloorMs = Math.min(1_000, this.#transportStallMs);
    this.#congestion = new TransportCongestion({ highAgeMs: this.#transportStallMs });
    this.#hostCeiling = hostCeiling;
    this.#rotationTimeoutMs = rotationTimeoutMs;
    this.#unadmittedTimeoutMs = unadmittedTimeoutMs;
    this.#expectedSidecarInstanceId = options.expectedSidecarInstanceId ?? null;
    this.#hostInstanceId = options.hostInstanceId ?? `host-${crypto.randomUUID()}`;
    this.#livenessTimeoutMs = livenessTimeoutMs;
    this.#delivery = new ReliableDeliveryTiming({
      epoch: 1,
      minimumMs: retransmitMs,
      initialRtoMs: options.retransmitMs === undefined
        ? Math.max(retransmitMs, DEFAULT_V2_INITIAL_RETRANSMIT_MS)
        : retransmitMs,
    });
    this.#pacer = new PublicationPacer({
      intervalMs: options.publicationIntervalMs,
      timer: options.publicationTimer,
      publish: reason => this.#publish(reason),
    });
  }

  connect(signal?: AbortSignal): Promise<void> {
    if (this.#closed) return Promise.reject(new Error("V2 transport session is closed"));
    if (this.#protocolFailed) {
      return Promise.reject(new Error("V2 transport session failed its protocol"));
    }
    if (this.#connected) return Promise.resolve();
    if (this.#connecting) return this.#connecting;
    const connecting = (async () => {
      if (signal?.aborted) throw abortError("V2 transport connection aborted");
      this.#unsubscribe ??= this.#channel.subscribe(this.#onAwarenessUpdate);
      await this.#channel.connect(signal);
      if (this.#closed) throw new Error("V2 transport session closed while connecting");
      this.#connected = true;
      this.#pacer.flushNow("initial");
    })().finally(() => {
      if (this.#connecting === connecting) this.#connecting = null;
    });
    this.#connecting = connecting;
    return connecting;
  }

  async resumeAwareness(signal?: AbortSignal): Promise<void> {
    if (this.#closed || this.#draining || this.#protocolFailed || !this.#connected) {
      throw new V2TransportLivenessError(
        "V2 transport is unavailable for Awareness reconnect",
        "unavailable",
      );
    }
    await this.#channel.connect(signal);
    if (this.#closed || this.#draining || this.#protocolFailed) {
      throw new V2TransportLivenessError(
        "V2 transport closed during Awareness reconnect",
        "unavailable",
      );
    }
    this.#pacer.flushNow("urgent");
  }

  /**
   * Proves that the exact sidecar instance accepted during bootstrap can still
   * execute its engine loop. The ping lives in the control Awareness state and
   * never enters the reliable request/response window.
   */
  probeLiveness(signal?: AbortSignal): Promise<V2TransportLivenessResult> {
    if (signal?.aborted) {
      return Promise.reject(abortError("V2 sidecar liveness probe aborted"));
    }
    if (!this.#connected || this.#closed || this.#draining) {
      return Promise.reject(new V2TransportLivenessError(
        "V2 transport is unavailable for a liveness probe",
        "unavailable",
      ));
    }
    if (!this.#expectedSidecarInstanceId) {
      return Promise.reject(new V2TransportLivenessError(
        "V2 transport has no bootstrap-bound sidecar instance",
        "unavailable",
      ));
    }
    if (
      (this.#sidecarInstanceId &&
        this.#sidecarInstanceId !== this.#expectedSidecarInstanceId) ||
      this.#retiredSidecarInstances.has(this.#expectedSidecarInstanceId)
    ) {
      return Promise.reject(new V2TransportLivenessError(
        "V2 sidecar instance changed after bootstrap",
        "instance_changed",
      ));
    }
    if (this.#livenessProbe) {
      return Promise.reject(new V2TransportLivenessError(
        "A V2 sidecar liveness probe is already in flight",
        "probe_in_flight",
      ));
    }

    const startedAtMs = this.#clock.now();
    const clockSentAtMs = Math.floor(startedAtMs);
    const sentAtMs = Math.max(clockSentAtMs, this.#lastLivenessSentAtMs + 1);
    if (!Number.isSafeInteger(sentAtMs) || sentAtMs < 0) {
      return Promise.reject(new V2TransportLivenessError(
        "V2 liveness clock is outside the supported range",
        "unavailable",
      ));
    }
    this.#lastLivenessSentAtMs = sentAtMs;

    let resolve!: (result: V2TransportLivenessResult) => void;
    let reject!: (error: unknown) => void;
    const pending = new Promise<V2TransportLivenessResult>((resolveValue, rejectValue) => {
      resolve = resolveValue;
      reject = rejectValue;
    });
    const probe: LivenessProbe = {
      sentAtMs,
      startedAtMs,
      resolve,
      reject,
      signal,
    };
    probe.onAbort = () => {
      this.#rejectLivenessProbe(
        probe,
        abortError("V2 sidecar liveness probe aborted"),
      );
    };
    signal?.addEventListener("abort", probe.onAbort, { once: true });
    probe.timer = setTimeout(() => {
      this.#rejectLivenessProbe(
        probe,
        new V2TransportLivenessError(
          "V2 sidecar liveness probe timed out",
          "timeout",
        ),
      );
    }, this.#livenessTimeoutMs);
    probe.timer.unref?.();
    this.#livenessProbe = probe;

    try {
      this.#publishLane("control", [], "urgent");
    } catch (cause) {
      this.#rejectLivenessProbe(
        probe,
        new V2TransportLivenessError(
          "V2 sidecar liveness probe could not be published",
          "publish",
          { cause },
        ),
      );
    }
    return pending;
  }

  async run(
    body: Record<string, unknown>,
    signal?: AbortSignal,
    options?: V2TransportRunOptions,
  ): Promise<RunHandle> {
    if (signal?.aborted) throw preCommitCancellationError();
    if (!this.#connected || this.#closed) {
      throw new V2PreCommitError("V2 transport is not connected", "not_connected");
    }
    if (this.#sidecarRecovery) {
      throw new V2PreCommitError(
        "V2 sidecar recovery is in progress",
        "sidecar_recovering",
      );
    }
    if (this.#draining) {
      throw new V2PreCommitError("V2 transport is draining", "draining");
    }
    const fastBody = snapshotFastPreparationBody(body);
    const fastPreparation = fastBody !== null;
    const preparationLease = this.#acquirePreparation(signal, fastPreparation);
    // Preserve the original scheduling contract for the bounded fast wave: an
    // immediately available slot must not add a microtask before
    // canonicalization. The immutable size/item probe above caps its aggregate
    // turn and memory cost; larger work remains on the four-slot lane.
    const preparation = preparationLease instanceof AbortController
      ? preparationLease
      : await preparationLease;
    const abortPreparation = () => preparation.abort(
      signal?.reason ?? abortError("V2 request aborted during preparation"),
    );
    if (signal?.aborted) abortPreparation();
    signal?.addEventListener("abort", abortPreparation, { once: true });
    const preparationTimeoutError = new V2PreCommitError(
      "V2 request preparation timed out before admission",
      "capacity_changed",
    );
    let preparationWorkSettled = false;
    let preparationCancellationFence: ReturnType<typeof setTimeout> | undefined;
    const preparationTimer = setTimeout(() => {
      preparation.abort(preparationTimeoutError);
      preparationCancellationFence = setTimeout(() => {
        preparationCancellationFence = undefined;
        if (!preparationWorkSettled) {
          this.#failProtocol(new Error(
            "V2 request preparation ignored its cancellation deadline",
          ));
        }
      }, PREPARATION_CANCELLATION_FENCE_MS);
      preparationCancellationFence.unref?.();
    }, this.#unadmittedTimeoutMs);
    preparationTimer.unref?.();
    let rejectPreparationAbort!: (error: unknown) => void;
    const preparationAborted = new Promise<never>((_resolve, reject) => {
      rejectPreparationAbort = reject;
    });
    void preparationAborted.catch(() => undefined);
    const onPreparationAbort = () => {
      const reason = preparation.signal.reason;
      rejectPreparationAbort(
        reason instanceof Error
          ? reason
          : abortError("V2 request aborted during preparation"),
      );
    };
    preparation.signal.addEventListener("abort", onPreparationAbort, { once: true });
    if (preparation.signal.aborted) onPreparationAbort();
    let releasePreparationWhenSettled = false;
    const preparationWork = this.#prepareRequest(
      fastBody ?? body,
      preparation.signal,
    );
    const onPreparationWorkSettled = () => {
      preparationWorkSettled = true;
      if (preparationCancellationFence) clearTimeout(preparationCancellationFence);
      preparationCancellationFence = undefined;
      if (releasePreparationWhenSettled) this.#releasePreparation(preparation);
    };
    void preparationWork.then(onPreparationWorkSettled, onPreparationWorkSettled);
    let requestId!: string;
    let retainStartupOwnership = false;
    const diagnostics = new HostRequestDiagnosticsObserver(
      () => this.#clock.now(),
      this.#hostDiagnosticsSource,
    );
    let state: RequestState | undefined;
    let committed: Promise<RunHandle> | undefined;
    try {
      requestId = this.#allocateRequestId();
      retainStartupOwnership = this.#binding.protocolRevision === "r2" &&
        this.#startupRecoveryOpen;
      if (
        retainStartupOwnership &&
        this.#startupRecoveryOwnedRequestIds.size >= MAX_TERMINAL_RECEIPTS
      ) {
        throw new V2PreCommitError(
          "V2 startup recovery request identity capacity is exhausted",
          "startup_recovery_capacity",
        );
      }
      if (retainStartupOwnership) this.#startupRecoveryOwnedRequestIds.add(requestId);
      const prepared = await Promise.race([preparationWork, preparationAborted]);
      diagnostics.prepared(prepared);
      if (signal?.aborted) throw preCommitCancellationError();
      if (this.#sidecarRecovery || !this.#connected) {
        throw new V2PreCommitError(
          "V2 sidecar recovery started during request preparation",
          "sidecar_recovering",
        );
      }
      if (this.#draining || this.#closed) {
        throw new V2PreCommitError("V2 transport stopped during request preparation", "draining");
      }
      const handle = new V2RunHandle({
        clock: this.#clock,
        maxBufferedBytes: this.#maxBufferedResponseBytes,
        expectUpstreamPrelude: this.#binding.protocolRevision === "r2",
        requireTransferredEventMemory: this.#eventMemoryPool !== undefined,
      });
      handle.stats.diagnostics = diagnostics.value();
      let resolveRun!: (value: RunHandle) => void;
      let rejectRun!: (error: unknown) => void;
      committed = new Promise<RunHandle>((resolve, reject) => {
        resolveRun = resolve;
        rejectRun = reject;
      });
      void committed.catch(() => undefined);
      const preparedIsFast = prepared.decodedBytes <= V2_LATENCY_CRITICAL_REQUEST_BYTES;
      if (fastPreparation && !preparedIsFast) {
        throw new V2PreCommitError(
          "V2 fast preparation exceeded its immutable size reservation",
          "preparation_capacity",
        );
      }
      state = {
        requestId,
        requestDigest: prepared.requestDigest,
        manifest: {
          requestDigest: prepared.requestDigest,
          decodedBytes: prepared.decodedBytes,
          encodedBytes: prepared.encodedBytes,
          encoding: prepared.encoding,
          partCount: prepared.partCount,
        },
        preparedRequest: prepared,
        preparationRetained: false,
        preparationFast: fastPreparation,
        handle,
        transcript: new TranscriptProof(requestId, prepared.requestDigest, {
          maxBytes: this.#maxResponseBytes,
          retainBytes: false,
          hasher: createNativeTranscriptHasher(),
        }),
        resolveRun,
        rejectRun,
        signal,
        onAdmissionReflected: options?.onAdmissionReflected,
        onAdmissionProgress: options?.onAdmissionProgress,
        acknowledgedEncodedBytes: 0,
        onExecutionRiskReflected: options?.onExecutionRiskReflected,
        admitted: false,
        transferCost: v2RequestTransferCost(prepared.encodedBytes, this.#transportProfile),
        latencyCriticalPreCommitTracked: false,
        latencyCriticalRequestTracked: false,
        published: false,
        commitStaged: false,
        commitPublished: false,
        startRisk: false,
        startQueued: false,
        startReserved: false,
        preparedStart: null,
        committed: false,
        cancelled: false,
        quarantineKind: null,
        responseBytes: 0,
        nextEventIndex: 0,
        assemblies: new Map(),
        completedEvents: new Map(),
        terminal: null,
        cancelledTerminalPermit: null,
        terminalFinalizationAnchor: null,
        upstreamPrelude: null,
        diagnostics,
        firstResponseSeen: false,
      };
      // Classification uses the immutable canonical result, never a mutable or
      // accessor-backed caller object. This makes the 128-slot fast bound an
      // actual <=32 MiB canonical-payload bound.
      if (fastPreparation) this.#retainedFastPreparedPayloads++;
      else this.#retainedPreparedPayloads++;
      state.preparationRetained = true;
      if (state.onAdmissionProgress) this.#admissionProgressObservers++;
      state.onAbort = () => this.#abortRequest(state!);
      this.#requests.set(requestId, state);
      this.#pending.push(state);
      this.#trackRequestState(state);
      signal?.addEventListener("abort", state.onAbort, { once: true });
      if (signal?.aborted) state.onAbort();
    } catch (error) {
      if (state) {
        if (this.#requests.has(state.requestId)) {
          this.#failPreCommit(
            state,
            error instanceof Error ? error : new Error("V2 request registration failed"),
          );
        } else {
          this.#releasePreparedPayload(state);
        }
      }
      if (retainStartupOwnership) this.#startupRecoveryOwnedRequestIds.delete(requestId);
      if (
        preparation.signal.aborted &&
        !signal?.aborted &&
        (this.#closed || this.#draining || !this.#connected)
      ) {
        throw new V2PreCommitError(
          "V2 transport stopped during request preparation",
          "draining",
        );
      }
      if (signal?.aborted && !(error instanceof V2PreCommitError)) {
        throw preCommitCancellationError();
      }
      throw error;
    } finally {
      clearTimeout(preparationTimer);
      signal?.removeEventListener("abort", abortPreparation);
      preparation.signal.removeEventListener("abort", onPreparationAbort);
      releasePreparationWhenSettled = true;
      if (preparationWorkSettled) this.#releasePreparation(preparation);
    }
    if (!state || !committed) {
      throw new Error("V2 request preparation completed without registered state");
    }
    if (!this.#requests.has(state.requestId)) return committed;
    state.admissionTimer = setTimeout(() => {
      if (!this.#requests.has(state.requestId) || state.admitted) return;
      this.#unadmittedTimeouts++;
      this.#failPreCommit(state, new V2PreCommitError(
        "Sandbox capacity changed before admission",
        "capacity_changed",
      ));
    }, this.#unadmittedTimeoutMs);
    state.admissionTimer.unref?.();
    this.#admitPending();
    this.#requestPublication(this.#pump());
    return committed;
  }

  /**
   * R2-only in-place recovery boundary. It revokes the elected sidecar and all
   * old publications while retaining only requests for which execution may
   * already have started. The returned token is process-local and single use.
   */
  async suspendSidecarRecovery(
    _reason: string,
    signal?: AbortSignal,
  ): Promise<V2SidecarRecoveryToken> {
    if (this.#binding.protocolRevision !== "r2") {
      throw new V2TransportLivenessError(
        "In-place sidecar recovery requires R2",
        "unavailable",
      );
    }
    if (signal?.aborted) throw abortError("V2 sidecar recovery was aborted");
    if (this.#closed || this.#draining || !this.#connected || this.#sidecarRecovery) {
      throw new V2TransportLivenessError(
        "V2 transport is unavailable for sidecar recovery",
        "unavailable",
      );
    }
    const previousInstanceId = this.#expectedSidecarInstanceId ?? this.#sidecarInstanceId;
    if (!previousInstanceId) {
      throw new V2TransportLivenessError(
        "V2 transport has no elected sidecar to recover",
        "unavailable",
      );
    }
    const token = Object.freeze({
      id: crypto.randomUUID(),
      binding: Object.freeze({ ...this.#binding }),
      previousInstanceId,
    });
    const payload: SidecarRecoveryCapsulePayload = {
      states: [],
      receipts: new Map(),
      ambiguityTombstones: new Map(),
      consumed: false,
    };

    // This is the irreversible suspension boundary. Stop every future
    // publication/update source before clearing Awareness, then drain the
    // stable processing tail. Caller cancellation is intentionally ignored
    // beyond this point: we must produce a capsule or fail closed, never leave
    // a half-revoked transport with live handles.
    this.#draining = true;
    this.#remoteCredits = 0;
    this.#connected = false;
    this.#abortEventMemoryWaits("V2 sidecar recovery interrupted event-memory backpressure");
    for (const preparation of this.#preparations) {
      preparation.abort(abortError("V2 sidecar recovery interrupted request preparation"));
    }
    this.#preparations.clear();
    this.#fastPreparations.clear();
    this.#rejectPreparationWaiters(new V2PreCommitError(
      "V2 sidecar recovery interrupted request preparation",
      "sidecar_recovery",
    ));
    this.#pacer.close();
    this.#cancelDeferredTerminalReceipts();
    this.#unsubscribe?.();
    this.#unsubscribe = null;
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = null;
    if (this.#stallTimer) clearTimeout(this.#stallTimer);
    this.#stallTimer = null;
    this.#stallTimerDueAtMs = null;
    if (this.#livenessProbe) {
      this.#rejectLivenessProbe(
        this.#livenessProbe,
        new V2TransportLivenessError(
          "V2 sidecar liveness probe was superseded by recovery",
          "instance_changed",
        ),
        false,
      );
    }
    try {
      this.#channel.clear();
    } catch (cause) {
      const message = cause instanceof Error
        ? cause.message
        : "V2 sidecar recovery could not revoke its Awareness state";
      await this.#beginTeardown(
        new V2PreCommitError(message, "sidecar_recovery_suspend_failed"),
        new V2CommittedError(message, "sidecar_recovery_suspend_failed", true),
      ).catch(() => undefined);
      throw cause;
    }
    await this.#joinProcessingTail();
    if (this.#teardownPromise || this.#protocolFailed) {
      throw new V2TransportLivenessError(
        "V2 transport failed while suspending sidecar recovery",
        "unavailable",
      );
    }
    // Frame processing can classify a request as ambiguous while the
    // suspension tail is draining. Snapshot only after that tail is stable so
    // the replacement cannot lose any safety-budget tombstone.
    payload.ambiguityTombstones = new Map(this.#ambiguityTombstones);

    const rotationIds = new Set([
      ...this.#rotationStreams,
      ...this.#rotations.keys(),
    ]);
    for (const rotation of this.#rotations.values()) {
      rotation.signal?.removeEventListener("abort", rotation.onAbort!);
      if (rotation.timer) clearTimeout(rotation.timer);
      rotation.reject(new V2TransportLivenessError(
        "Credential rotation was interrupted by sidecar recovery",
        "instance_changed",
      ));
    }
    this.#rotations.clear();
    this.#scheduler.removeStream("control");
    for (const rotationId of rotationIds) {
      this.#removeRotationStream(rotationId);
    }
    this.#rotationStreams.clear();

    for (const state of [...this.#requests.values()]) {
      this.#scheduler.removeStream(state.requestId);
      if (!this.#hasExecutionRisk(state)) {
        this.#failPreCommit(state, new V2PreCommitError(
          "V2 sidecar stopped before execution during recovery",
          "sidecar_recovery",
        ));
        continue;
      }
      this.#releasePartialResponseState(state);
      state.preparedRequest = null;
      state.terminal = null;
      state.startQueued = false;
      state.startReserved = false;
      state.preparedStart = null;
      state.onAdmissionReflected = undefined;
      this.#clearAdmissionProgress(state);
      state.onExecutionRiskReflected = undefined;
      const prefix = state.transcript.snapshot();
      state.responseBytes = prefix.byteCount;
      state.recoveryPrefix = {
        expectedEventCount: prefix.eventCount,
        expectedByteCount: prefix.byteCount,
        expectedDigest: prefix.transcriptSha256,
        hasher: createNativeTranscriptHasher(),
        hashedEventCount: 0,
        hashedByteCount: 0,
        nextEventIndex: 0,
        segmented: null,
      };
      this.#detachStateForRecovery(state);
      payload.states.push(state);
    }
    this.#creditReservations = 0;
    this.#transferReservations = 0;
    this.#startReservations = 0;
    this.#preparedStarts.length = 0;
    this.#latencyCriticalPreCommitTransfers = 0;
    this.#latencyCriticalRequests = 0;
    for (const [requestId, receipt] of this.#terminalReceipts) {
      this.#scheduler.removeStream(requestId);
      receipt.queued = false;
      receipt.replayed = false;
      receipt.cursor = null;
      payload.receipts.set(requestId, receipt);
    }
    this.#terminalReceipts.clear();
    this.#clearOrphanRecoveryCandidates();
    this.#startupRecoveryOwnedRequestIds.clear();
    this.#ambiguityTombstones.clear();
    this.#ambiguousQuarantined = 0;
    this.#quarantined = 0;
    // The retired session is intentionally never reconnectable. Drop every
    // cumulative window/cursor after ownership has moved so prompt parts and
    // sealed credential frames cannot remain retained behind close()'s
    // already-closed fast path.
    const nextOutboundEpoch = this.#outbound.snapshot().epoch + 1;
    this.#outbound.reset(nextOutboundEpoch);
    this.#delivery.reset(nextOutboundEpoch);
    this.#flight.reset(nextOutboundEpoch);
    this.#resetFlightMeasurement();
    const nextInboundEpoch = this.#inbound.ack().epoch + 1;
    this.#inbound.reset(nextInboundEpoch);
    this.#processedInboundAck = { epoch: nextInboundEpoch, sequence: -1 };
    this.#completedInboundSequences.clear();
    this.#inboundFrameTails.clear();
    this.#inboundFrameBarrier = Promise.resolve();
    this.#dataDirty = false;
    this.#sidecarDiagnostics = null;

    this.#closed = true;
    SIDECAR_RECOVERY_CAPSULES.set(token, payload);
    try {
      await this.#channel.close();
    } catch (cause) {
      this.failSidecarRecovery(token, new V2CommittedError(
        cause instanceof Error ? cause.message : "Old sidecar channel could not close",
        "sidecar_recovery_suspend_failed",
        true,
      ));
      throw cause;
    }
    return token;
  }

  adoptRecoveredSidecar(token: V2SidecarRecoveryToken): Promise<void> {
    if (this.#binding.protocolRevision !== "r2" || this.#connected || this.#closed) {
      throw new V2TransportLivenessError(
        "Recovery capsule requires a fresh, disconnected R2 transport",
        "unavailable",
      );
    }
    const payload = SIDECAR_RECOVERY_CAPSULES.get(token);
    if (!payload || payload.consumed) {
      throw new V2TransportLivenessError(
        "V2 sidecar recovery capsule is stale or already consumed",
        "instance_changed",
      );
    }
    if (
      !sameRecoveryScope(this.#binding, token.binding) ||
      this.#binding.launchNonce === token.binding.launchNonce ||
      this.#expectedSidecarInstanceId === token.previousInstanceId
    ) {
      throw new V2TransportLivenessError(
        "Recovery target does not match the same-sandbox R2 scope",
        "instance_changed",
      );
    }
    payload.consumed = true;
    SIDECAR_RECOVERY_CAPSULES.delete(token);
    let resolve!: () => void;
    let reject!: (error: unknown) => void;
    const promise = new Promise<void>((resolveValue, rejectValue) => {
      resolve = resolveValue;
      reject = rejectValue;
    });
    void promise.catch(() => undefined);
    const ownedRequestIds = new Set<string>([
      ...payload.states.map(state => state.requestId),
      ...payload.receipts.keys(),
      ...payload.ambiguityTombstones.keys(),
    ]);
    this.#sidecarRecovery = {
      token,
      markerSeen: false,
      ownedRequestIds,
      resolve,
      reject,
      promise,
    };
    this.#startupRecoveryOpen = false;
    this.#startupRecoveryOwnedRequestIds.clear();
    this.#remoteCredits = 0;
    for (const [requestId, requestDigest] of payload.ambiguityTombstones) {
      if (this.#ambiguityTombstones.has(requestId)) continue;
      this.#ambiguityTombstones.set(requestId, requestDigest);
      this.#ambiguousQuarantined++;
      this.#quarantined++;
    }
    for (const [requestId, receipt] of payload.receipts) {
      this.#terminalReceipts.set(requestId, receipt);
    }
    for (const state of payload.states) {
      state.signal?.removeEventListener("abort", state.onAbort!);
      state.onAbort = () => this.#abortRequest(state);
      state.signal?.addEventListener("abort", state.onAbort, { once: true });
      state.diagnostics.followPeak(this.#hostDiagnosticsSource);
      this.#requests.set(state.requestId, state);
      this.#trackRequestState(state);
      if (state.quarantineKind) this.#quarantined++;
      if (state.quarantineKind === "ambiguity") this.#ambiguousQuarantined++;
      if (state.signal?.aborted) this.#abortRequest(state);
    }
    return promise;
  }

  failSidecarRecovery(token: V2SidecarRecoveryToken, cause: unknown): void {
    const error = cause instanceof V2CommittedError
      ? cause
      : new V2CommittedError(
          cause instanceof Error ? cause.message : "V2 sidecar recovery failed",
          "sidecar_recovery_failed",
          true,
        );
    const recovery = this.#sidecarRecovery;
    if (recovery?.token === token) {
      void this.#beginTeardown(
        new V2PreCommitError(error.message, error.code),
        error,
      ).catch(() => undefined);
      return;
    }
    const payload = SIDECAR_RECOVERY_CAPSULES.get(token);
    if (!payload || payload.consumed) return;
    payload.consumed = true;
    SIDECAR_RECOVERY_CAPSULES.delete(token);
    for (const state of payload.states) {
      if (!state.committed) state.rejectRun(error);
      state.handle.fail(error);
    }
    payload.receipts.clear();
    payload.ambiguityTombstones.clear();
  }
  async drain(reason: string, signal?: AbortSignal): Promise<void> {
    if (this.#closed) return this.#teardownPromise ?? Promise.resolve();
    this.#draining = true;
    this.#abortPreparations(new V2PreCommitError(
      "V2 transport drained during request preparation",
      "draining",
    ));
    for (const state of [...this.#pending]) {
      if (state.admitted) continue;
      this.#failPreCommit(
        state,
        new V2PreCommitError("V2 transport drained before admission", "draining"),
      );
    }
    while (this.#requests.size > 0) {
      await this.#waitForIdle(signal);
    }
    this.#queueDrain(reason);
    await this.#waitForDrained(signal);
    await this.close();
  }

  /**
   * Recovery-only stop: cancel every local request, publish one fenced drain
   * immediately, continue ACKing sidecar output until drained/deadline, then
   * close locally. No request is exposed as safely retryable after start-risk.
   */
  async forceStop(reason: string, signal?: AbortSignal): Promise<void> {
    if (this.#closed) return this.#teardownPromise ?? Promise.resolve();
    if (!this.#connected) {
      await this.close();
      return;
    }
    this.#draining = true;
    this.#abortEventMemoryWaits("V2 sandbox recovery interrupted event-memory backpressure");
    this.#abortPreparations(new V2PreCommitError(
      "V2 sandbox recovery interrupted request preparation",
      "sandbox_recovery",
    ));
    for (const state of [...this.#requests.values()]) {
      const requestId = state.requestId;
      const published = state.published;
      if (this.#hasExecutionRisk(state)) {
        this.#failCommitted(state, new V2CommittedError(
          "V2 request was interrupted by sandbox recovery",
          "sandbox_recovery",
          true,
        ));
      } else {
        this.#failPreCommit(state, new V2PreCommitError(
          "V2 request stopped before execution during sandbox recovery",
          "sandbox_recovery",
        ));
      }
      if (published) {
        try {
          this.#scheduler.enqueue("control", requestId, {
            kind: "request_cancel",
            requestId,
            reason: "sandbox recovery",
          });
        } catch {
          // The drain frame remains the authoritative process-wide stop.
        }
      }
    }
    try {
      this.#queueDrain(reason);
      await this.#waitForDrained(signal);
    } finally {
      await this.close();
    }
  }

  #queueDrain(reason: string): void {
    if (this.#drainFrameQueued || this.#closed) return;
    this.#scheduler.enqueue("control", "control", {
      kind: "drain",
      reason: reason.replace(/[\r\n\0]/g, " ").trim().slice(0, 256) || "shutdown",
    });
    this.#drainFrameQueued = true;
    this.#requestPublication(this.#pump());
  }

  #waitForIdle(signal?: AbortSignal): Promise<void> {
    if (this.#requests.size === 0) return Promise.resolve();
    if (signal?.aborted) return Promise.reject(abortError("V2 transport drain aborted"));
    return new Promise<void>((resolve, reject) => {
      let settled = false;
      const finish = (operation: () => void) => {
        if (settled) return;
        settled = true;
        this.#idleWaiters.delete(onIdle);
        signal?.removeEventListener("abort", onAbort);
        operation();
      };
      const onIdle = () => finish(resolve);
      const onAbort = () => finish(() => reject(abortError("V2 transport drain aborted")));
      this.#idleWaiters.add(onIdle);
      signal?.addEventListener("abort", onAbort, { once: true });
      if (this.#requests.size === 0) onIdle();
    });
  }

  #waitForDrained(signal?: AbortSignal): Promise<void> {
    if (this.#remoteDrained && this.#terminalReceipts.size === 0) return Promise.resolve();
    if (signal?.aborted) return Promise.reject(abortError("V2 transport drain aborted"));
    return new Promise<void>((resolve, reject) => {
      let settled = false;
      const finish = (operation: () => void) => {
        if (settled) return;
        settled = true;
        this.#drainWaiters.delete(waiter);
        signal?.removeEventListener("abort", onAbort);
        operation();
      };
      const waiter = {
        resolve: () => finish(resolve),
        reject: (error: unknown) => finish(() => reject(error)),
      };
      const onAbort = () => waiter.reject(abortError("V2 transport drain aborted"));
      this.#drainWaiters.add(waiter);
      signal?.addEventListener("abort", onAbort, { once: true });
    });
  }

  #maybeResolveDrainWaiters(): void {
    if (!this.#remoteDrained || this.#terminalReceipts.size > 0) return;
    for (const waiter of [...this.#drainWaiters]) waiter.resolve();
  }

  #removeRotationStream(rotationId: string): void {
    this.#scheduler.removeStream(`rotation:${rotationId}`);
    this.#rotationStreams.delete(rotationId);
  }

  rotateCredential(
    rotationId: string,
    sealedJson: string,
    signal?: AbortSignal,
  ): Promise<void> {
    if (!this.#connected || this.#closed || this.#draining || this.#sidecarRecovery) {
      return Promise.reject(new Error("V2 transport is unavailable for credential rotation"));
    }
    if (!/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(rotationId)) {
      return Promise.reject(new TypeError("rotationId is invalid"));
    }
    if (typeof sealedJson !== "string" || sealedJson.length < 1 || sealedJson.length > 10_000) {
      return Promise.reject(new RangeError("sealed credential rotation is invalid"));
    }
    if (signal?.aborted) {
      return Promise.reject(abortError("Credential rotation aborted"));
    }
    if (this.#rotations.has(rotationId)) {
      return Promise.reject(new Error(`Credential rotation is already pending: ${rotationId}`));
    }
    const pending = new Promise<void>((resolve, reject) => {
      const waiter: RotationWaiter = { resolve, reject, signal };
      waiter.onAbort = () => {
        if (this.#rotations.get(rotationId) !== waiter) return;
        this.#rotations.delete(rotationId);
        this.#removeRotationStream(rotationId);
        if (waiter.timer) clearTimeout(waiter.timer);
        reject(abortError("Credential rotation aborted"));
      };
      signal?.addEventListener("abort", waiter.onAbort, { once: true });
      this.#rotations.set(rotationId, waiter);
      waiter.timer = setTimeout(() => {
        if (this.#rotations.get(rotationId) !== waiter) return;
        this.#rotations.delete(rotationId);
        this.#removeRotationStream(rotationId);
        signal?.removeEventListener("abort", waiter.onAbort!);
        reject(new Error(`Credential rotation ACK timed out: ${rotationId}`));
      }, this.#rotationTimeoutMs);
    });
    void pending.catch(() => undefined);
    this.#rotationStreams.add(rotationId);
    try {
      this.#scheduler.enqueue("control", `rotation:${rotationId}`, {
        kind: "credential_rotate",
        rotationId,
        sealedJson,
      });
    } catch (cause) {
      const waiter = this.#rotations.get(rotationId);
      if (waiter) {
        this.#rotations.delete(rotationId);
        waiter.signal?.removeEventListener("abort", waiter.onAbort!);
        if (waiter.timer) clearTimeout(waiter.timer);
        this.#removeRotationStream(rotationId);
        waiter.reject(cause);
      }
      return pending;
    }
    this.#requestPublication(this.#pump());
    return pending;
  }

  close(): Promise<void> {
    if (this.#closed && !this.#teardownPromise) return Promise.resolve();
    return this.#beginTeardown(
      new V2PreCommitError("V2 transport session closed", "closed"),
      new V2CommittedError(
        "V2 transport closed after request commitment may have been published",
        "closed",
        true,
      ),
    );
  }

  #beginTeardown(
    preCommitError: Error,
    committedError: V2CommittedError,
  ): Promise<void> {
    if (this.#teardownPromise) return this.#teardownPromise;
    this.#closed = true;
    this.#connected = false;
    this.#draining = true;
    this.#remoteCredits = 0;
    this.#startupRecoveryOpen = false;
    this.#abortEventMemoryWaits("V2 transport closed during event-memory backpressure");
    for (const preparation of this.#preparations) {
      preparation.abort(abortError("V2 transport closed during request preparation"));
    }
    this.#preparations.clear();
    this.#fastPreparations.clear();
    this.#rejectPreparationWaiters(new V2PreCommitError(
      "V2 transport closed before request preparation",
      "draining",
    ));
    this.#lifecycleEpoch++;
    if (this.#sidecarRecovery) {
      const recovery = this.#sidecarRecovery;
      this.#sidecarRecovery = null;
      recovery.reject(committedError);
    }
    if (this.#livenessProbe) {
      this.#rejectLivenessProbe(
        this.#livenessProbe,
        new V2TransportLivenessError(
          "V2 transport closed during its liveness probe",
          "unavailable",
        ),
        false,
      );
    }
    this.#pacer.close();
    this.#cancelDeferredTerminalReceipts();
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = null;
    if (this.#stallTimer) clearTimeout(this.#stallTimer);
    this.#stallTimer = null;
    this.#stallTimerDueAtMs = null;
    this.#unsubscribe?.();
    this.#unsubscribe = null;
    for (const [rotationId, waiter] of this.#rotations) {
      waiter.signal?.removeEventListener("abort", waiter.onAbort!);
      if (waiter.timer) clearTimeout(waiter.timer);
      waiter.reject(preCommitError);
      this.#rotations.delete(rotationId);
      this.#removeRotationStream(rotationId);
    }
    for (const rotationId of [...this.#rotationStreams]) {
      this.#removeRotationStream(rotationId);
    }
    this.#rotationStreams.clear();
    for (const waiter of [...this.#drainWaiters]) waiter.reject(preCommitError);
    // Wake any reliable-data delivery currently waiting for client buffer
    // space before joining the serialized processing tail. Teardown is already
    // irreversible, so retaining a blocked producer here would deadlock close
    // and prevent the state below from being classified and released.
    for (const state of this.#requests.values()) {
      if (this.#hasExecutionRisk(state)) state.handle.cancel(committedError);
      else state.handle.fail(preCommitError);
    }
    const teardown = Promise.resolve().then(async () => {
      await this.#joinProcessingTail();
      for (const state of [...this.#requests.values()]) {
        if (this.#hasExecutionRisk(state)) this.#failCommitted(state, committedError);
        else this.#failPreCommit(state, preCommitError);
      }
      for (const requestId of this.#terminalReceipts.keys()) {
        this.#scheduler.removeStream(requestId);
      }
      this.#scheduler.removeStream("control");
      this.#terminalReceipts.clear();
      this.#clearOrphanRecoveryCandidates();
      this.#startupRecoveryOwnedRequestIds.clear();
      this.#creditReservations = 0;
      this.#transferReservations = 0;
      this.#startReservations = 0;
      this.#preparedStarts.length = 0;
      const nextEpoch = this.#outbound.snapshot().epoch + 1;
      this.#outbound.reset(nextEpoch);
      this.#delivery.reset(nextEpoch);
      this.#flight.reset(nextEpoch);
      this.#resetFlightMeasurement();
      this.#dataDirty = false;
      this.#completedInboundSequences.clear();
      this.#inboundFrameTails.clear();
      this.#inboundFrameBarrier = Promise.resolve();
      try {
        this.#channel.clear();
      } finally {
        await this.#channel.close();
      }
    });
    this.#teardownPromise = teardown;
    return teardown;
  }

  snapshot(): V2TransportSnapshot {
    const executionReservations = Math.min(
      this.#admittedUncommittedRequestCount,
      Math.max(
        0,
        MAX_PRECOMMIT_EXECUTION_RESERVATIONS - this.#ambiguousQuarantined,
      ),
    );
    const prefetched = this.#admittedUncommittedRequestCount - executionReservations;
    return {
      connected: this.#connected && !this.#closed && !this.#sidecarRecovery,
      committed: this.#committedRequestCount,
      active: this.#committedRequestCount + executionReservations,
      preparing: this.#preparations.size,
      prefetched,
      queued: this.#unadmittedRequestCount + prefetched +
        this.#preparations.size + this.#preparationWaiters.length,
      quarantined: this.#quarantined,
      ambiguousQuarantined: this.#ambiguousQuarantined,
      ambiguityLimit: Math.max(1, Math.min(4, this.#hostCeiling)),
      credits: Math.max(0, this.#remoteCredits - this.#creditReservations),
      admissionCredits: this.#availableCredits(),
      prefetchCredits: this.#availablePrefetchCredits(),
      transferCredits: Math.max(0, this.#transferBudget - this.#transferReservations),
      admissionBlockReason: this.#admissionBlockReason(),
      retransmissions: this.#retransmissions,
      awarenessRttMs: this.#delivery.roundTripEwmaMs,
      sidecarPeerAgeMs: this.#lastValidPeerAtMs === null
        ? null
        : Math.max(0, this.#clock.now() - this.#lastValidPeerAtMs),
      diagnostics: { host: this.#hostTransportDiagnostics(), sidecar: this.#sidecarDiagnostics },
    };
  }

  setHostCeiling(value: number): void {
    if (!Number.isSafeInteger(value) || value < 0 || value > 128) {
      throw new RangeError("host ceiling must be an integer from 0 through 128");
    }
    if (value === this.#hostCeiling) return;
    this.#hostCeiling = value;
    if (this.#connected && !this.#closed) this.#pacer.requestUrgent();
  }

  #preparationLimit(): number {
    // bulk-v1 may retain the same six body-only prefetches that transport
    // admission already bounds. CPU-heavy canonicalization/compression remains
    // independently capped at four below, so this does not turn six retained
    // payload slots into six concurrent preparation jobs.
    return this.#transportProfile === "bulk-v1" &&
        this.#binding.protocolRevision === "r2"
      ? MAX_BULK_BODY_PREFETCH_RESERVATIONS
      : MAX_PRECOMMIT_EXECUTION_RESERVATIONS;
  }

  #boundedPreparationCount(): number {
    return this.#preparations.size - this.#fastPreparations.size;
  }

  #acquirePreparation(
    signal: AbortSignal | undefined,
    fast: boolean,
  ): AbortController | Promise<AbortController> {
    if (signal?.aborted) {
      return Promise.reject(preCommitCancellationError());
    }
    if (this.#canAcquirePreparation(fast)) {
      const controller = new AbortController();
      this.#preparations.add(controller);
      if (fast) this.#fastPreparations.add(controller);
      this.#notifyStateChange();
      return controller;
    }
    if (this.#preparationWaiters.length >= MAX_PREPARATION_WAITERS) {
      return Promise.reject(new V2PreCommitError(
        "V2 request preparation queue is full",
        "preparation_capacity",
      ));
    }
    return new Promise<AbortController>((resolve, reject) => {
      const waiter: PreparationWaiter = { fast, resolve, reject, signal };
      waiter.onAbort = () => {
        const index = this.#preparationWaiters.indexOf(waiter);
        if (index < 0) return;
        this.#preparationWaiters.splice(index, 1);
        signal?.removeEventListener("abort", waiter.onAbort!);
        if (waiter.timer) clearTimeout(waiter.timer);
        this.#notifyStateChange();
        reject(preCommitCancellationError());
      };
      signal?.addEventListener("abort", waiter.onAbort, { once: true });
      waiter.timer = setTimeout(() => {
        const index = this.#preparationWaiters.indexOf(waiter);
        if (index < 0) return;
        this.#preparationWaiters.splice(index, 1);
        signal?.removeEventListener("abort", waiter.onAbort!);
        this.#notifyStateChange();
        reject(new V2PreCommitError(
          "V2 request preparation capacity changed before admission",
          "capacity_changed",
        ));
      }, this.#unadmittedTimeoutMs);
      waiter.timer.unref?.();
      this.#preparationWaiters.push(waiter);
      this.#notifyStateChange();
    });
  }

  #releasePreparation(controller: AbortController): void {
    if (!this.#preparations.delete(controller)) return;
    this.#fastPreparations.delete(controller);
    this.#notifyStateChange();
    this.#drainPreparationWaiters();
  }

  #releasePreparedPayload(state: RequestState): void {
    if (!state.preparationRetained) return;
    state.preparationRetained = false;
    if (state.preparationFast) {
      state.preparationFast = false;
      this.#retainedFastPreparedPayloads = Math.max(
        0,
        this.#retainedFastPreparedPayloads - 1,
      );
    } else {
      this.#retainedPreparedPayloads = Math.max(0, this.#retainedPreparedPayloads - 1);
    }
    this.#drainPreparationWaiters();
  }

  #drainPreparationWaiters(): void {
    while (this.#preparationWaiters.length > 0) {
      let index = this.#preparationWaiters.findIndex(waiter => (
        waiter.fast && this.#canAcquirePreparation(true)
      ));
      if (index < 0) {
        index = this.#preparationWaiters.findIndex(waiter => (
          !waiter.fast && this.#canAcquirePreparation(false)
        ));
      }
      if (index < 0) return;
      const [waiter] = this.#preparationWaiters.splice(index, 1);
      waiter.signal?.removeEventListener("abort", waiter.onAbort!);
      if (waiter.timer) clearTimeout(waiter.timer);
      if (waiter.signal?.aborted) {
        waiter.reject(preCommitCancellationError());
        continue;
      }
      if (this.#closed || this.#draining || this.#sidecarRecovery || !this.#connected) {
        waiter.reject(new V2PreCommitError(
          "V2 transport stopped before request preparation",
          "draining",
        ));
        continue;
      }
      const next = new AbortController();
      this.#preparations.add(next);
      if (waiter.fast) this.#fastPreparations.add(next);
      this.#notifyStateChange();
      waiter.resolve(next);
    }
  }

  #canAcquirePreparation(fast: boolean): boolean {
    if (fast) {
      return this.#fastPreparations.size < MAX_FAST_ACTIVE_PREPARATIONS &&
        this.#fastPreparations.size + this.#retainedFastPreparedPayloads <
          MAX_FAST_PREPARED_PAYLOADS;
    }
    if (this.#boundedPreparationCount() >= MAX_PRECOMMIT_EXECUTION_RESERVATIONS) return false;
    return this.#boundedPreparationCount() + this.#retainedPreparedPayloads <
      this.#preparationLimit();
  }

  #rejectPreparationWaiters(error: Error): void {
    for (const waiter of this.#preparationWaiters.splice(0)) {
      waiter.signal?.removeEventListener("abort", waiter.onAbort!);
      if (waiter.timer) clearTimeout(waiter.timer);
      waiter.reject(error);
    }
    this.#notifyStateChange();
  }

  #abortPreparations(error: Error): void {
    for (const preparation of this.#preparations) preparation.abort(error);
    this.#preparations.clear();
    this.#fastPreparations.clear();
    this.#rejectPreparationWaiters(error);
  }

  #allocateRequestId(): string {
    for (let attempt = 0; attempt < 8; attempt++) {
      const requestId = `req-${crypto.randomUUID()}`;
      if (
        !this.#requests.has(requestId) &&
        !this.#terminalReceipts.has(requestId) &&
        !this.#ambiguityTombstones.has(requestId) &&
        !this.#orphanRecoveryCandidates.has(requestId) &&
        !this.#startupRecoveryOwnedRequestIds.has(requestId) &&
        !this.#sidecarRecovery?.ownedRequestIds.has(requestId)
      ) return requestId;
    }
    throw new V2PreCommitError(
      "V2 transport could not allocate a unique request identity",
      "request_id_collision",
    );
  }

  #notifyStateChange(): void {
    if (!this.#onStateChange || this.#stateChangeScheduled || this.#closed) return;
    this.#stateChangeScheduled = true;
    queueMicrotask(() => {
      this.#stateChangeScheduled = false;
      if (!this.#closed) this.#onStateChange?.();
    });
  }

  #observeSidecarLiveness(envelope: AwarenessEnvelope<SidecarFrame>): void {
    if (this.#closed || this.#protocolFailed || !validBinding(envelope, this.#binding)) return;
    if (
      this.#binding.protocolRevision === "r2" &&
      (envelope as R2AwarenessEnvelope<SidecarFrame>).targetInstanceId !== this.#hostInstanceId
    ) return;
    const lane: AwarenessLane = envelope.lane ?? "data";
    if (lane === "control" && envelope.frames.length > 0) return;
    const expectedInstanceId = this.#expectedSidecarInstanceId;
    if (!expectedInstanceId) return;
    if (envelope.instanceId !== expectedInstanceId) {
      if (
        this.#sidecarInstanceId &&
        lane === "data" &&
        envelope.epoch <= this.#inbound.ack().epoch
      ) return;
      const probe = this.#livenessProbe;
      if (probe && !this.#retiredSidecarInstances.has(envelope.instanceId)) {
        this.#rejectLivenessProbe(
          probe,
          new V2TransportLivenessError(
            "V2 sidecar instance changed during its liveness probe",
            "instance_changed",
          ),
        );
      }
      return;
    }
    if (this.#retiredSidecarInstances.has(envelope.instanceId)) return;
    if (envelope.revision <= this.#observedPeerRevision[lane]) return;
    this.#observedPeerRevision[lane] = envelope.revision;
    this.#lastValidPeerAtMs = this.#clock.now();
    if (lane !== "control") return;

    const pong = envelope.liveness;
    const probe = this.#livenessProbe;
    if (!probe || pong?.kind !== "pong" || pong.sentAtMs !== probe.sentAtMs) return;
    this.#resolveLivenessProbe(probe, {
      rttMs: Math.max(0, this.#clock.now() - probe.startedAtMs),
      sentAtMs: probe.sentAtMs,
    });
  }

  #resolveLivenessProbe(
    probe: LivenessProbe,
    result: V2TransportLivenessResult,
  ): void {
    if (!this.#finishLivenessProbe(probe, true)) return;
    // Idle links may have no credit/ACK delta to refresh the router cache.
    // Publish the successful exact-instance proof so its peer age can expire
    // accurately instead of appearing fresh forever or aging out while alive.
    this.#notifyStateChange();
    probe.resolve(result);
  }

  #rejectLivenessProbe(
    probe: LivenessProbe,
    error: unknown,
    clearState = true,
  ): void {
    if (!this.#finishLivenessProbe(probe, clearState)) return;
    probe.reject(error);
  }

  #finishLivenessProbe(probe: LivenessProbe, clearState: boolean): boolean {
    if (this.#livenessProbe !== probe) return false;
    this.#livenessProbe = null;
    if (probe.timer) clearTimeout(probe.timer);
    probe.signal?.removeEventListener("abort", probe.onAbort!);
    if (clearState && this.#connected && !this.#closed) {
      try {
        // Replace the persistent control state so a late pong cannot heal a
        // later probe and reconnecting peers do not replay a stale ping.
        this.#publishLane("control", [], "urgent");
      } catch {
        // The probe result is already known. Cleanup publication is best effort.
      }
    }
    return true;
  }

  #ownsLifecycle(epoch: number): boolean {
    return epoch === this.#lifecycleEpoch && !this.#closed && !this.#protocolFailed;
  }

  async #joinProcessingTail(): Promise<void> {
    for (;;) {
      const tail = this.#processing;
      const frameTasks = [...this.#inboundFrameTasks];
      await Promise.all([
        tail.catch(() => undefined),
        ...frameTasks.map(task => task.catch(() => undefined)),
      ]);
      if (tail === this.#processing && this.#inboundFrameTasks.size === 0) return;
    }
  }

  #inboundFrameStream(frame: ReliableFrame<SidecarFrame>): string {
    if ("requestId" in frame) return `request:${frame.requestId}`;
    if (frame.kind === "credential_ack") return `rotation:${frame.rotationId}`;
    return `control:${frame.kind}`;
  }

  #inboundFrameIsBarrier(frame: ReliableFrame<SidecarFrame>): boolean {
    return frame.kind === "recovery_complete" || frame.kind === "drained";
  }

  #scheduleInboundFrame(
    frame: ReliableFrame<SidecarFrame>,
    lifecycleEpoch: number,
    orphanRecoveryEnvelope: boolean,
  ): void {
    if (this.#inboundFrameTasks.size >= this.#limits.windowMaxFrames) {
      throw new Error("Accepted sidecar frame processing exceeds the reliable window bound");
    }
    const barrier = this.#inboundFrameIsBarrier(frame);
    const stream = this.#inboundFrameStream(frame);
    const predecessors = new Set<Promise<void>>([this.#inboundFrameBarrier]);
    if (barrier) {
      for (const tail of this.#inboundFrameTails.values()) predecessors.add(tail);
    } else {
      const streamTail = this.#inboundFrameTails.get(stream);
      if (streamTail) predecessors.add(streamTail);
    }

    let task!: Promise<void>;
    const processing = Promise.all([...predecessors])
      .then(async () => {
        if (!this.#ownsLifecycle(lifecycleEpoch)) return;
        await this.#processFrame(frame, lifecycleEpoch, orphanRecoveryEnvelope);
        if (!this.#ownsLifecycle(lifecycleEpoch)) return;
        this.#completeInboundFrame(frame, lifecycleEpoch);
      })
      .catch(error => {
        if (this.#ownsLifecycle(lifecycleEpoch)) this.#failProtocol(error);
      });
    task = processing.finally(() => {
      this.#inboundFrameTasks.delete(task);
      if (!barrier && this.#inboundFrameTails.get(stream) === task) {
        this.#inboundFrameTails.delete(stream);
      }
    });
    this.#inboundFrameTasks.add(task);
    if (barrier) this.#inboundFrameBarrier = task;
    else this.#inboundFrameTails.set(stream, task);
  }

  #completeInboundFrame(
    frame: ReliableFrame<SidecarFrame>,
    lifecycleEpoch: number,
  ): void {
    if (!this.#ownsLifecycle(lifecycleEpoch)) return;
    if (frame.epoch !== this.#processedInboundAck.epoch) {
      throw new Error("Processed sidecar frame belongs to a stale reliable epoch");
    }
    if (
      frame.sequence <= this.#processedInboundAck.sequence ||
      this.#completedInboundSequences.has(frame.sequence)
    ) {
      throw new Error("Sidecar frame completion sequence was repeated");
    }
    const accepted = this.#inbound.ack();
    if (accepted.epoch !== frame.epoch || frame.sequence > accepted.sequence) {
      throw new Error("Sidecar frame completed beyond the accepted reliable cursor");
    }
    this.#completedInboundSequences.add(frame.sequence);
    if (this.#completedInboundSequences.size > this.#limits.windowMaxFrames) {
      throw new Error("Completed sidecar frames exceed the reliable window bound");
    }

    let nextSequence = this.#processedInboundAck.sequence + 1;
    let advanced = false;
    while (this.#completedInboundSequences.delete(nextSequence)) {
      this.#processedInboundAck = { epoch: frame.epoch, sequence: nextSequence };
      nextSequence++;
      advanced = true;
    }
    this.#maybeCompleteSidecarRecovery();
    if (!this.#ownsLifecycle(lifecycleEpoch)) return;
    this.#admitPending();
    const urgency = this.#pump();
    this.#requestPublication(advanced ? "urgent" : urgency);
    this.#armStallWatchdog();
  }

  readonly #onAwarenessUpdate = (
    update: AwarenessPeerUpdate<SidecarFrame>,
  ): void => {
    if (this.#closed || this.#protocolFailed) return;
    const lifecycleEpoch = this.#lifecycleEpoch;
    // Liveness must not queue behind a response frame waiting for client-side
    // buffer space. Only the reliable data path remains serialized below.
    this.#observeSidecarLiveness(update.envelope);
    this.#processing = this.#processing
      .then(() => this.#ownsLifecycle(lifecycleEpoch)
        ? this.#processEnvelope(update.envelope, lifecycleEpoch)
        : undefined)
      .catch(error => {
        if (this.#ownsLifecycle(lifecycleEpoch)) this.#failProtocol(error);
      });
  };

  async #processEnvelope(
    envelope: AwarenessEnvelope<SidecarFrame>,
    lifecycleEpoch: number,
  ): Promise<void> {
    if (!this.#ownsLifecycle(lifecycleEpoch)) return;
    // A recovery/drain marker is globally ordered. Later envelope metadata may
    // pass ordinary request-local work, but never a still-running global fence.
    await this.#inboundFrameBarrier;
    if (!this.#ownsLifecycle(lifecycleEpoch)) return;
    if (!validBinding(envelope, this.#binding)) {
      throw new Error("Sidecar Awareness binding is invalid");
    }
    if (
      this.#binding.protocolRevision === "r2" &&
      (envelope as R2AwarenessEnvelope<SidecarFrame>).targetInstanceId !== this.#hostInstanceId
    ) return;
    const lane: AwarenessLane = envelope.lane ?? "data";
    if (
      this.#expectedSidecarInstanceId &&
      envelope.instanceId !== this.#expectedSidecarInstanceId
    ) {
      if (this.#binding.protocolRevision === "r2") return;
      // An old data-state replay cannot take over the transport and may arrive
      // after the bootstrap-bound peer is already live. Ignore that stale
      // epoch; a foreign current/new epoch or any control state fails closed.
      if (
        this.#sidecarInstanceId &&
        lane === "data" &&
        envelope.epoch <= this.#inbound.ack().epoch
      ) return;
      this.#retiredSidecarInstances.add(envelope.instanceId);
      throw new V2TransportLivenessError(
        "Sidecar instance does not match the bootstrap-bound instance",
        "instance_changed",
      );
    }
    const legacy = envelope.lane === undefined;
    if (lane === "control" && envelope.frames.length > 0) {
      throw new Error("Sidecar control Awareness state contains reliable frames");
    }
    let replaced = false;
    if (this.#sidecarInstanceId && envelope.instanceId !== this.#sidecarInstanceId) {
      if (this.#retiredSidecarInstances.has(envelope.instanceId)) return;
      if (lane === "data" && envelope.epoch <= this.#inbound.ack().epoch) return;
      this.#retiredSidecarInstances.add(this.#sidecarInstanceId);
      const message = "Sidecar instance changed while transport state was active";
      for (const state of [...this.#requests.values()]) {
        if (this.#hasExecutionRisk(state)) {
          this.#failCommitted(state, new V2CommittedError(message, "instance_changed", true));
        } else {
          this.#failPreCommit(state, new V2PreCommitError(message, "instance_changed"));
        }
      }
      const nextEpoch = this.#outbound.snapshot().epoch + 1;
      this.#outbound.reset(nextEpoch);
      this.#delivery.reset(nextEpoch);
      this.#flight.reset(nextEpoch);
      this.#resetFlightMeasurement();
      this.#channel.clear("data");
      this.#dataDirty = false;
      this.#sidecarRevision.data = -1;
      this.#sidecarRevision.control = -1;
      this.#clearOrphanRecoveryCandidates();
      this.#startupRecoveryOwnedRequestIds.clear();
      this.#startupRecoveryOpen = false;
      replaced = true;
    }
    if (envelope.revision <= this.#sidecarRevision[lane]) return;
    this.#sidecarInstanceId = envelope.instanceId;
    this.#sidecarRevision[lane] = envelope.revision;
    const orphanRecoveryEnvelope = this.#binding.protocolRevision === "r2" && (
      this.#sidecarRecovery !== null || (
        this.#startupRecoveryOpen && envelope.credits === 0 && !envelope.draining
      )
    );
    if (
      this.#binding.protocolRevision === "r2" &&
      !this.#sidecarRecovery &&
      envelope.credits > 0
    ) {
      this.#startupRecoveryOpen = false;
      this.#startupRecoveryOwnedRequestIds.clear();
      this.#clearOrphanRecoveryCandidates();
    }
    let creditsChanged = false;
    if (lane === "control" || legacy) {
      this.#sidecarDiagnostics = envelope.diagnostics ?? null;
      const nextCredits = Math.max(0, Math.min(128, envelope.credits));
      creditsChanged = nextCredits !== this.#remoteCredits;
      this.#remoteCredits = nextCredits;
    }

    this.#acknowledgeOutbound(envelope.ack);
    const flightCompleted = this.#flight.acknowledged(envelope.ack);
    const now = this.#clock.now();
    let goodputChanged = false;
    if (flightCompleted) {
      const requestBytes = this.#flightRequestBytes;
      const retransmitted = this.#currentFlightRetransmitted;
      this.#flightCompletions++;
      goodputChanged = this.#recordAcknowledgedFlight(now);
      this.#observeAccelerationFlight(requestBytes, retransmitted);
    }
    const acknowledgementAdvanced = this.#delivery.acknowledged(
      envelope.ack,
      this.#outbound.snapshot().frames.length > 0,
      now,
    );
    if (acknowledgementAdvanced) {
      this.#lastFastRetransmitAtMs = null;
      this.#armRetransmission();
    }
    const congestionChanged = this.#updateCongestion();
    if (creditsChanged || congestionChanged || goodputChanged) this.#notifyStateChange();
    if (!acknowledgementAdvanced && lane === "control") {
      this.#maybeFastRetransmit(now);
    }
    let delivered: readonly ReliableFrame<SidecarFrame>[] = [];
    let replayProcessedAck = false;
    if (lane === "data") {
      const currentEpoch = this.#inbound.ack().epoch;
      if (envelope.epoch > currentEpoch) {
        if (
          this.#processedInboundAck.epoch !== currentEpoch ||
          this.#processedInboundAck.sequence !== this.#inbound.ack().sequence ||
          this.#inboundFrameTasks.size > 0 ||
          this.#completedInboundSequences.size > 0
        ) {
          throw new Error("Sidecar epoch changed while accepted frames were still processing");
        }
        this.#clearOrphanRecoveryCandidates();
        this.#startupRecoveryOwnedRequestIds.clear();
        this.#startupRecoveryOpen = false;
        this.#inbound.reset(envelope.epoch);
        this.#processedInboundAck = { epoch: envelope.epoch, sequence: -1 };
        this.#completedInboundSequences.clear();
        this.#inboundFrameTails.clear();
        this.#inboundFrameBarrier = Promise.resolve();
      }
      const envelopeHigh = envelope.frames.reduce(
        (high, frame) => frame.epoch === this.#processedInboundAck.epoch
          ? Math.max(high, frame.sequence)
          : high,
        -1,
      );
      delivered = this.#inbound.accept(envelope.frames);
      if (
        this.#inbound.ack().epoch !== this.#processedInboundAck.epoch ||
        this.#inbound.ack().sequence - this.#processedInboundAck.sequence >
          this.#limits.windowMaxFrames
      ) {
        throw new Error("Unprocessed sidecar frames exceed the reliable window bound");
      }
      for (const frame of delivered) {
        this.#scheduleInboundFrame(frame, lifecycleEpoch, orphanRecoveryEnvelope);
      }
      if (
        delivered.length === 0 &&
        envelopeHigh >= 0 &&
        envelopeHigh <= this.#processedInboundAck.sequence
      ) {
        // A newer publication carrying an already-processed immutable flight
        // proves the peer missed our ACK-only state. Replay the current ACK
        // immediately instead of making it climb the retransmission backoff.
        replayProcessedAck = true;
      }
    }
    if (!this.#ownsLifecycle(lifecycleEpoch)) return;
    this.#maybeCompleteSidecarRecovery();
    this.#admitPending();
    const urgency = this.#pump();
    if (flightCompleted && urgency === null) this.#channel.clear("data");
    this.#requestPublication(
      replaced || replayProcessedAck ? "urgent" : urgency,
    );
    this.#armStallWatchdog();
  }

  async #processFrame(
    frame: ReliableFrame<SidecarFrame>,
    lifecycleEpoch: number,
    orphanRecoveryEnvelope: boolean,
  ): Promise<void> {
    switch (frame.kind) {
      case "request_prepared": {
        const state = this.#requests.get(frame.requestId);
        if (!state) return;
        if (this.#binding.protocolRevision !== "r2") {
          this.#failPreCommit(state, new V2PreCommitError(
            "R1 sidecar emitted an R2 preparation proof",
            "protocol",
          ));
          return;
        }
        if (!this.#preparedProofMatches(state, frame) || !state.commitPublished) {
          const error = "Sidecar preparation proof does not match the committed request";
          if (this.#hasExecutionRisk(state)) {
            this.#failCommitted(state, new V2CommittedError(error, "prepared_fence", true));
          } else {
            this.#failPreCommit(state, new V2PreCommitError(error, "prepared_fence"));
          }
          return;
        }
        if (state.startQueued || state.startRisk || state.preparedStart) return;
        state.acknowledgedEncodedBytes = state.manifest.encodedBytes;
        this.#reflectAdmissionProgress(state, {
          stage: "request_prepared",
          acknowledgedEncodedBytes: state.manifest.encodedBytes,
          totalEncodedBytes: state.manifest.encodedBytes,
        });
        if (!this.#requests.has(state.requestId)) return;
        state.preparedStart = this.#requestStartFromPrepared(frame);
        this.#preparedStarts.push(state);
        this.#stagePreparedStarts();
        return;
      }
      case "upstream_prelude": {
        const state = this.#requests.get(frame.requestId);
        if (!state) return;
        if (!state.committed) {
          const message = "Sidecar emitted upstream headers before commitment";
          if (this.#hasExecutionRisk(state)) {
            this.#failCommitted(state, new V2CommittedError(message, "protocol", true));
          } else {
            this.#failPreCommit(state, new V2PreCommitError(message, "protocol"));
          }
          return;
        }
        const prelude = {
          status: frame.status,
          contentType: frame.contentType,
          upstreamRequestId: frame.upstreamRequestId,
        };
        if (state.upstreamPrelude) {
          if (
            state.upstreamPrelude.status !== prelude.status ||
            !equivalentEventStreamContentType(
              state.upstreamPrelude.contentType,
              prelude.contentType,
            ) ||
            (
              state.upstreamPrelude.upstreamRequestId !== prelude.upstreamRequestId &&
              !(state.recoveryPrefix && prelude.upstreamRequestId === null)
            )
          ) {
            this.#failCommitted(state, new V2CommittedError(
              "Sidecar emitted conflicting upstream headers",
              "prelude_conflict",
              true,
            ));
          }
          return;
        }
        state.upstreamPrelude = prelude;
        state.handle.acceptUpstreamPrelude(prelude);
        return;
      }
      case "request_ack": {
        const state = this.#requests.get(frame.requestId);
        if (!state) {
          this.#trackOrphanRecoveryCandidate(frame, orphanRecoveryEnvelope);
          return;
        }
        if (frame.requestDigest !== state.requestDigest) {
          if (this.#binding.protocolRevision === "r2") {
            state.startRisk = true;
            this.#reflectExecutionRisk(state);
          }
          if (this.#hasExecutionRisk(state)) {
            this.#failCommitted(state, new V2CommittedError(
              "Committed ACK digest does not match the request",
              "ack_digest",
              true,
            ));
          } else {
            this.#failPreCommit(state, new V2PreCommitError(
              "Committed ACK digest does not match the request",
              "ack_digest",
            ));
          }
          return;
        }
        if (this.#binding.protocolRevision === "r2" && !state.startRisk) {
          state.startRisk = true;
          this.#reflectExecutionRisk(state);
          this.#failCommitted(state, new V2CommittedError(
            "Sidecar claimed fetch-started commitment before request_start",
            "ack_before_start",
            true,
          ));
          return;
        }
        if (state.committed) return;
        this.#releaseStartReservation(state);
        this.#stopTrackingLatencyCriticalPreCommit(state);
        this.#markRequestCommitted(state);
        state.diagnostics.committed(frame.diagnostics);
        this.#creditReservations = Math.max(0, this.#creditReservations - 1);
        this.#transferReservations = Math.max(
          0,
          this.#transferReservations - state.transferCost,
        );
        state.resolveRun(state.handle);
        return;
      }
      case "request_reject": {
        const state = this.#requests.get(frame.requestId);
        if (!state) {
          this.#deleteOrphanRecoveryCandidate(frame.requestId);
          return;
        }
        if (state.committed || (
          this.#binding.protocolRevision === "r2" && state.startRisk
        )) {
          this.#failCommitted(state, new V2CommittedError(
            frame.message,
            frame.code,
            true,
          ));
        } else {
          this.#failPreCommit(state, new V2PreCommitError(frame.message, frame.code));
        }
        return;
      }
      case "response_event": {
        const state = this.#requests.get(frame.requestId);
        if (!state) {
          const candidate = this.#orphanRecoveryCandidates.get(frame.requestId);
          if (candidate) {
            await this.#acceptOrphanRecoveryEventPart(candidate, frame, lifecycleEpoch);
          }
          return;
        }
        if (!state.committed) {
          const message = "Sidecar emitted response data before commitment";
          if (this.#hasExecutionRisk(state)) {
            this.#failCommitted(state, new V2CommittedError(message, "protocol", true));
          } else {
            this.#failPreCommit(state, new V2PreCommitError(message, "protocol"));
          }
          return;
        }
        if (this.#binding.protocolRevision === "r2" && !state.upstreamPrelude) {
          this.#failCommitted(state, new V2CommittedError(
            "Sidecar emitted response data before upstream headers",
            "missing_prelude",
            true,
          ));
          return;
        }
        if (this.#consumeRecoveryPrefixPart(state, frame)) return;
        if (!state.firstResponseSeen) { state.firstResponseSeen = true; state.diagnostics.firstFrame(frame.diagnostics); }
        await this.#acceptResponsePart(state, frame, lifecycleEpoch);
        return;
      }
      case "response_batch": {
        const state = this.#requests.get(frame.requestId);
        if (!state) {
          const candidate = this.#orphanRecoveryCandidates.get(frame.requestId);
          if (!candidate) return;
          for (let offset = 0; offset < frame.items.length; offset++) {
            const item = frame.items[offset]!;
            if (!await this.#acceptOrphanRecoveryWholeEvent(
              candidate,
              frame.firstEventIndex + offset,
              item.event,
              item.dataBase64Url,
              lifecycleEpoch,
            )) return;
          }
          return;
        }
        if (!state.committed) {
          const message = "Sidecar emitted response data before commitment";
          if (this.#hasExecutionRisk(state)) {
            this.#failCommitted(state, new V2CommittedError(message, "protocol", true));
          } else {
            this.#failPreCommit(state, new V2PreCommitError(message, "protocol"));
          }
          return;
        }
        if (this.#binding.protocolRevision === "r2" && !state.upstreamPrelude) {
          this.#failCommitted(state, new V2CommittedError(
            "Sidecar emitted response data before upstream headers",
            "missing_prelude",
            true,
          ));
          return;
        }
        for (let offset = 0; offset < frame.items.length; offset++) {
          const eventIndex = frame.firstEventIndex + offset;
          const item = frame.items[offset]!;
          if (this.#consumeRecoveryPrefixWhole(
            state,
            eventIndex,
            item.event,
            item.dataBase64Url,
          )) {
            if (!this.#requests.has(state.requestId)) return;
            continue;
          }
          const acceptedWhole = await this.#acceptWholeEncodedEvent(
            state,
            eventIndex,
            item.event,
            item.dataBase64Url,
            lifecycleEpoch,
          );
          if (!acceptedWhole) return;
        }
        await this.#drainCompletedEvents(state, lifecycleEpoch);
        return;
      }
      case "response_terminal": {
        const state = this.#requests.get(frame.requestId);
        if (!state) {
          const receipt = this.#terminalReceipts.get(frame.requestId);
          if (receipt && (this.#sidecarRecovery || receipt.replayed)) {
            if (!this.#terminalMatchesReceipt(frame, receipt.terminal)) {
              this.#failRecoveryProtocol(
                "Recovered terminal conflicts with its retained receipt",
                "terminal_receipt_conflict",
              );
              return;
            }
            receipt.replayed = true;
            this.#queueTerminalReceipt(frame.requestId, receipt);
            return;
          }
          if (receipt) return;
          if (
            this.#sidecarRecovery?.ownedRequestIds.has(frame.requestId) ||
            this.#startupRecoveryOwnedRequestIds.has(frame.requestId)
          ) {
            this.#failRecoveryProtocol(
              "Recovered terminal conflicts with a locally owned definitive outcome",
              "owned_terminal_conflict",
            );
            return;
          }
          const candidate = this.#orphanRecoveryCandidates.get(frame.requestId);
          if (!candidate) return;
          await this.#validateOrphanRecoveryTerminal(candidate, frame, lifecycleEpoch);
          return;
        }
        if (!state.committed) {
          const message = "Sidecar emitted a terminal before commitment";
          if (this.#hasExecutionRisk(state)) {
            this.#failCommitted(state, new V2CommittedError(message, "protocol", true));
          } else {
            this.#failPreCommit(state, new V2PreCommitError(message, "protocol"));
          }
          return;
        }
        if (this.#binding.protocolRevision === "r2" && !state.upstreamPrelude) {
          this.#failCommitted(state, new V2CommittedError(
            "Sidecar emitted terminal proof before upstream headers",
            "missing_prelude",
            true,
          ));
          return;
        }
        if (state.terminal && state.terminal.transcriptSha256 !== frame.transcriptSha256) {
          this.#failCommitted(state, new V2CommittedError(
            "Sidecar emitted conflicting terminal proofs",
            "terminal_conflict",
            true,
          ));
          return;
        }
        state.terminal = frame;
        await this.#tryFinalize(state, lifecycleEpoch);
        return;
      }
      case "response_error": {
        const state = this.#requests.get(frame.requestId);
        if (!state) {
          this.#deleteOrphanRecoveryCandidate(frame.requestId);
          return;
        }
        if (state.committed || (
          this.#binding.protocolRevision === "r2" && state.startRisk
        )) {
          this.#failCommitted(state, new V2CommittedError(
            frame.message,
            frame.code,
            frame.ambiguous,
            frame.rateLimit,
            frame.providerError,
          ));
        } else {
          this.#failPreCommit(state, new V2PreCommitError(frame.message, frame.code));
        }
        return;
      }
      case "request_cancelled": {
        const state = this.#requests.get(frame.requestId);
        if (!state) {
          this.#deleteOrphanRecoveryCandidate(frame.requestId);
          return;
        }
        if (this.#sidecarRecovery && !state.cancelled) {
          this.#failCommitted(state, new V2CommittedError(
            "Recovered request was durably cancelled",
            "cancelled",
            false,
          ));
          this.#maybeCompleteSidecarRecovery();
          return;
        }
        if (!state.cancelled) return;
        const releaseQuarantine = state.quarantineKind === "cancellation";
        this.#removeState(state);
        if (releaseQuarantine) {
          this.#quarantined = Math.max(0, this.#quarantined - 1);
          state.quarantineKind = null;
        }
        return;
      }
      case "recovery_complete":
        if (this.#orphanRecoveryCandidates.size > 0) {
          const candidate = this.#orphanRecoveryCandidates.values().next().value!;
          this.#failOrphanRecoveryCandidate(
            candidate,
            "Recovered orphan replay ended without a complete terminal proof",
            "orphan_terminal_truncated",
          );
          return;
        }
        this.#startupRecoveryOpen = false;
        this.#startupRecoveryOwnedRequestIds.clear();
        if (this.#sidecarRecovery) {
          this.#sidecarRecovery.markerSeen = true;
          this.#maybeCompleteSidecarRecovery();
        }
        return;
      case "drained":
        if (frame.active === 0 && frame.queued === 0) {
          this.#remoteDrained = true;
          this.#maybeResolveDrainWaiters();
        }
        return;
      case "credential_ack": {
        const waiter = this.#rotations.get(frame.rotationId);
        if (!waiter) return;
        this.#rotations.delete(frame.rotationId);
        this.#removeRotationStream(frame.rotationId);
        waiter.signal?.removeEventListener("abort", waiter.onAbort!);
        if (waiter.timer) clearTimeout(waiter.timer);
        waiter.resolve();
        return;
      }
      case "pong":
        return;
    }
  }

  #hasExecutionRisk(state: RequestState): boolean {
    if (state.committed) return true;
    return this.#binding.protocolRevision === "r2"
      ? state.startRisk
      : state.commitPublished;
  }

  #reflectExecutionRisk(state: RequestState): void {
    if (!this.#hasExecutionRisk(state)) return;
    this.#clearAdmissionProgress(state);
    const onExecutionRiskReflected = state.onExecutionRiskReflected;
    state.onExecutionRiskReflected = undefined;
    // Risk is already recorded in authoritative transport state. An observer
    // failure must never make an irreversible request look retryable.
    try { onExecutionRiskReflected?.(); } catch {}
  }

  #consumeRecoveryPrefixWhole(
    state: RequestState,
    eventIndex: number,
    event: string,
    encoded: string,
  ): boolean {
    const prefix = state.recoveryPrefix;
    if (!prefix || eventIndex >= prefix.expectedEventCount) return false;
    if (prefix.segmented || eventIndex !== prefix.nextEventIndex) {
      this.#failRecoveryPrefix(state, "Recovered response prefix order conflicts");
      return true;
    }
    let bytes: Uint8Array;
    try { bytes = base64UrlDecode(encoded); } catch {
      this.#failRecoveryPrefix(state, "Recovered response prefix encoding is invalid");
      return true;
    }
    this.#appendRecoveryPrefixBytes(prefix, event, bytes);
    this.#finishRecoveryPrefixIfReady(state, false);
    return true;
  }

  #consumeRecoveryPrefixPart(
    state: RequestState,
    frame: Extract<ReliableFrame<SidecarFrame>, { kind: "response_event" }>,
  ): boolean {
    const prefix = state.recoveryPrefix;
    if (!prefix || frame.eventIndex >= prefix.expectedEventCount) return false;
    if (frame.eventIndex !== prefix.nextEventIndex) {
      this.#failRecoveryPrefix(state, "Recovered response prefix order conflicts");
      return true;
    }
    let segmented = prefix.segmented;
    if (!segmented) {
      if (frame.partIndex !== 0 || frame.offset !== 0) {
        this.#failRecoveryPrefix(state, "Recovered response prefix begins with a gap");
        return true;
      }
      segmented = prefix.segmented = {
        event: frame.event,
        partCount: frame.partCount,
        nextPartIndex: 0,
        nextOffset: 0,
      };
      const header = utf8Bytes(`event: ${frame.event}\ndata: `);
      prefix.hasher.update(header);
      prefix.hashedByteCount += header.byteLength;
    }
    if (
      segmented.event !== frame.event ||
      segmented.partCount !== frame.partCount ||
      frame.partIndex !== segmented.nextPartIndex ||
      frame.offset !== segmented.nextOffset
    ) {
      this.#failRecoveryPrefix(state, "Recovered response prefix parts conflict");
      return true;
    }
    let bytes: Uint8Array;
    try { bytes = base64UrlDecode(frame.dataBase64Url); } catch {
      this.#failRecoveryPrefix(state, "Recovered response prefix encoding is invalid");
      return true;
    }
    prefix.hasher.update(bytes);
    prefix.hashedByteCount += bytes.byteLength;
    segmented.nextPartIndex++;
    segmented.nextOffset += bytes.byteLength;
    if (segmented.nextPartIndex === segmented.partCount) {
      const footer = utf8Bytes("\n\n");
      prefix.hasher.update(footer);
      prefix.hashedByteCount += footer.byteLength;
      prefix.hashedEventCount++;
      prefix.nextEventIndex++;
      prefix.segmented = null;
      this.#finishRecoveryPrefixIfReady(state, false);
    }
    return true;
  }

  #appendRecoveryPrefixBytes(
    prefix: RecoveryPrefix,
    event: string,
    bytes: Uint8Array,
  ): void {
    const header = utf8Bytes(`event: ${event}\ndata: `);
    const footer = utf8Bytes("\n\n");
    prefix.hasher.update(header).update(bytes).update(footer);
    prefix.hashedByteCount += header.byteLength + bytes.byteLength + footer.byteLength;
    prefix.hashedEventCount++;
    prefix.nextEventIndex++;
  }

  #failRecoveryPrefix(state: RequestState, message: string): void {
    this.#failCommitted(state, new V2CommittedError(
      message,
      "recovery_prefix_mismatch",
      true,
    ));
  }

  #preparedProofMatches(
    state: RequestState,
    frame: ReliableFrame<R2RequestPreparedFrame>,
  ): boolean {
    const manifest = state.manifest;
    return frame.protocolRevision === "r2" &&
      frame.schemaVersion === GP_V2_R2_SCHEMA_VERSION &&
      frame.projectId === this.#binding.projectId &&
      frame.sessionId === this.#binding.sessionId &&
      frame.leaderGeneration === this.#binding.leaderGeneration &&
      frame.buildHash === this.#binding.buildHash &&
      frame.schemaHash === this.#binding.schemaHash &&
      frame.launchNonce === this.#binding.launchNonce &&
      frame.hostInstanceId === this.#hostInstanceId &&
      frame.targetInstanceId === this.#expectedSidecarInstanceId &&
      frame.requestDigest === manifest.requestDigest &&
      frame.decodedBytes === manifest.decodedBytes &&
      frame.encodedBytes === manifest.encodedBytes &&
      frame.encoding === manifest.encoding &&
      frame.partCount === manifest.partCount;
  }

  #requestStartFromPrepared(
    frame: ReliableFrame<R2RequestPreparedFrame>,
  ): R2RequestStartFrame {
    return {
      kind: "request_start",
      requestId: frame.requestId,
      protocolRevision: "r2",
      schemaVersion: GP_V2_R2_SCHEMA_VERSION,
      projectId: frame.projectId,
      sessionId: frame.sessionId,
      leaderGeneration: frame.leaderGeneration,
      buildHash: frame.buildHash,
      schemaHash: frame.schemaHash,
      launchNonce: frame.launchNonce,
      hostInstanceId: frame.hostInstanceId,
      targetInstanceId: frame.targetInstanceId,
      requestDigest: frame.requestDigest,
      decodedBytes: frame.decodedBytes,
      encodedBytes: frame.encodedBytes,
      encoding: frame.encoding,
      partCount: frame.partCount,
    };
  }

  #stagePreparedStarts(): void {
    if (
      this.#binding.protocolRevision !== "r2" ||
      this.#closed || this.#draining || this.#sidecarRecovery
    ) return;
    while (
      this.#preparedStarts.length > 0 &&
      this.#startReservations + this.#ambiguousQuarantined <
        MAX_PRECOMMIT_EXECUTION_RESERVATIONS
    ) {
      const latencyIndex = this.#preparedStarts.findIndex(state => (
        this.#transportProfile === "bulk-v1" &&
            state.manifest.decodedBytes <= V2_LATENCY_CRITICAL_REQUEST_BYTES
      ));
      const index = latencyIndex >= 0 ? latencyIndex : 0;
      const state = this.#preparedStarts.splice(index, 1)[0]!;
      const start = state.preparedStart;
      if (!start || !this.#requests.has(state.requestId) || state.startQueued || state.startRisk) {
        continue;
      }
      state.preparedStart = null;
      state.startQueued = true;
      state.startReserved = true;
      this.#startReservations++;
      try {
        this.#scheduler.enqueue("commit", state.requestId, start);
      } catch (cause) {
        state.startQueued = false;
        state.startReserved = false;
        this.#startReservations = Math.max(0, this.#startReservations - 1);
        this.#failRequestStaging(state, new V2PreCommitError(
          cause instanceof Error ? cause.message : "R2 request start could not be staged",
          "start_stage",
        ));
      }
    }
  }

  #discardPreparedStart(state: RequestState): void {
    state.preparedStart = null;
    const index = this.#preparedStarts.indexOf(state);
    if (index >= 0) this.#preparedStarts.splice(index, 1);
  }

  #releaseStartReservation(state: RequestState): void {
    if (!state.startReserved) return;
    state.startReserved = false;
    this.#startReservations = Math.max(0, this.#startReservations - 1);
    this.#stagePreparedStarts();
  }

  #abortEventMemoryWaits(message: string): void {
    if (this.#eventMemoryWaitAbort.signal.aborted) return;
    this.#eventMemoryWaitAbort.abort(abortError(message));
  }

  #eventMemoryWaitInterrupted(lifecycleEpoch: number): boolean {
    return this.#eventMemoryWaitAbort.signal.aborted || !this.#ownsLifecycle(lifecycleEpoch);
  }

  async #acquireEventPermit(
    logicalBytes: number,
    label: string,
    wait = true,
  ): Promise<AtomicBytePermit | null | undefined> {
    const signal = this.#eventMemoryWaitAbort.signal;
    if (signal.aborted) return null;
    if (!this.#eventMemoryPool) return undefined;
    try {
      const requestedBytes = estimateAtomicEventPermitBytes(logicalBytes);
      if (!wait) {
        return this.#eventMemoryPool.tryAcquire(requestedBytes, {
          region: logicalBytes > this.#maxBufferedResponseBytes ? "jumbo" : "auto",
          label,
        });
      }
      return await this.#eventMemoryPool.acquire(
        requestedBytes,
        {
          region: logicalBytes > this.#maxBufferedResponseBytes ? "jumbo" : "auto",
          label,
          signal,
        },
      );
    } catch {
      return null;
    }
  }

  async #resizeEventPermit(
    permit: AtomicBytePermit | null,
    logicalBytes: number,
    wait = true,
  ): Promise<boolean> {
    const signal = this.#eventMemoryWaitAbort.signal;
    if (signal.aborted) return false;
    if (!this.#eventMemoryPool) return true;
    if (!permit) return false;
    try {
      const targetBytes = estimateAtomicEventPermitBytes(logicalBytes);
      if (!wait) {
        if (
          logicalBytes > this.#maxBufferedResponseBytes &&
          permit.region !== "jumbo"
        ) return permit.tryPromote(targetBytes);
        return permit.tryResize(targetBytes, { allowJumbo: true });
      }
      // RunHandle deliberately admits an event above its soft queue bound only
      // while that event owns the single jumbo lane. A normal resize prefers
      // spare ordinary capacity, so force the region transition before the
      // permit is transferred even when ordinary growth would still fit.
      if (
        logicalBytes > this.#maxBufferedResponseBytes &&
        permit.region !== "jumbo"
      ) {
        await permit.promote(targetBytes, signal);
        return true;
      }
      await permit.resize(targetBytes, {
        allowJumbo: true,
        signal,
      });
      return true;
    } catch {
      return false;
    }
  }

  #tryRetainEventPermit(
    permit: AtomicBytePermit | null,
    wireBytes: number,
  ): boolean {
    if (!this.#eventMemoryPool) return true;
    if (!permit) return false;
    try {
      // Jumbo ownership is a delivery invariant, not merely a capacity
      // fallback. Shrinking must never demote an above-soft event.
      if (
        wireBytes > this.#maxBufferedResponseBytes &&
        permit.region !== "jumbo"
      ) return false;
      permit.shrink(v2RetainedEventPermitBytes(wireBytes));
      return true;
    } catch {
      return false;
    }
  }

  #retainsOtherJumboEvent(state: RequestState, eventIndex: number): boolean {
    for (const [index, completed] of state.completedEvents) {
      if (index === eventIndex || !completed.permit?.active) continue;
      if (completed.permit.region === "jumbo") return true;
    }
    for (const [index, assembly] of state.assemblies) {
      if (index === eventIndex || !assembly.permit?.active) continue;
      if (assembly.permit.region === "jumbo") return true;
    }
    return false;
  }

  #retainsOtherEvent(state: RequestState, eventIndex: number): boolean {
    for (const index of state.completedEvents.keys()) {
      if (index !== eventIndex) return true;
    }
    for (const index of state.assemblies.keys()) {
      if (index !== eventIndex) return true;
    }
    return false;
  }

  #rejectConflictingJumboReorder(
    state: RequestState,
    eventIndex: number,
    logicalBytes: number,
  ): boolean {
    if (
      logicalBytes <= this.#maxBufferedResponseBytes ||
      !this.#retainsOtherJumboEvent(state, eventIndex)
    ) return false;
    // Reliable priority may place a terminal ahead of an earlier logical
    // event. One withheld jumbo is safe, but waiting for a second jumbo on the
    // same serialized request tail can never make progress: the first cannot
    // drain until the missing index is processed behind this task.
    this.#failEventMemory(
      state,
      "Host event memory cannot reorder multiple jumbo response events",
    );
    return true;
  }

  #failEventMemory(state: RequestState, message: string): void {
    if (!this.#requests.has(state.requestId)) return;
    this.#failCommitted(state, new V2CommittedError(
      message,
      "event_memory_capacity",
      true,
    ));
    this.#scheduler.enqueue("control", state.requestId, {
      kind: "request_cancel",
      requestId: state.requestId,
      reason: "host event memory unavailable",
    });
    this.#requestPublication(this.#pump());
  }

  async #acceptWholeEncodedEvent(
    state: RequestState,
    eventIndex: number,
    event: string,
    encoded: string,
    lifecycleEpoch: number,
  ): Promise<boolean> {
    const recoveryPrefix = state.recoveryPrefix;
    const nextEventIndex = recoveryPrefix
      ? recoveryPrefix.nextEventIndex
      : state.nextEventIndex;
    if (
      eventIndex < nextEventIndex ||
      state.completedEvents.has(eventIndex) ||
      state.assemblies.has(eventIndex)
    ) {
      this.#failCommitted(state, new V2CommittedError(
        "Response batch event indexes conflict",
        "response_conflict",
        true,
      ));
      return false;
    }
    const decodedLength = base64UrlDecodedBytes(encoded);
    if (decodedLength === null) {
      this.#failCommitted(state, new V2CommittedError(
        "Response batch encoding is invalid",
        "response_encoding",
        true,
      ));
      return false;
    }
    const initialLogicalBytes = partialEventLogicalBytes(event, decodedLength);
    if (this.#rejectConflictingJumboReorder(state, eventIndex, initialLogicalBytes)) {
      return false;
    }
    const canWaitForMemory = !this.#retainsOtherEvent(state, eventIndex);
    const permit = await this.#acquireEventPermit(
      initialLogicalBytes,
      `host-response:${state.requestId}`,
      canWaitForMemory,
    );
    if (permit === null) {
      if (
        this.#eventMemoryWaitInterrupted(lifecycleEpoch) ||
        this.#requests.get(state.requestId) !== state
      ) return false;
      this.#failEventMemory(state, "Host event memory is unavailable");
      return false;
    }
    if (
      this.#eventMemoryWaitInterrupted(lifecycleEpoch) ||
      this.#requests.get(state.requestId) !== state
    ) {
      permit?.release();
      return false;
    }
    let bytes: Uint8Array;
    try {
      bytes = decodeCanonicalBase64Url(encoded, decodedLength);
    } catch {
      permit?.release();
      this.#failCommitted(state, new V2CommittedError(
        "Response batch encoding is invalid",
        "response_encoding",
        true,
      ));
      return false;
    }
    const wireBytes = canonicalEventWireBytes(event, [bytes]);
    const overLimit = (!recoveryPrefix || eventIndex >= recoveryPrefix.expectedEventCount) &&
      state.responseBytes + wireBytes > this.#maxResponseBytes;
    if (overLimit) {
      permit?.release();
      this.#failCommitted(state, new V2CommittedError(
        "Response exceeds the configured canonical byte limit",
        "response_too_large",
        true,
      ));
      return false;
    }
    if (this.#rejectConflictingJumboReorder(state, eventIndex, wireBytes)) {
      permit?.release();
      return false;
    }
    if (!await this.#resizeEventPermit(
      permit ?? null,
      wireBytes,
      canWaitForMemory,
    )) {
      permit?.release();
      if (
        this.#eventMemoryWaitInterrupted(lifecycleEpoch) ||
        this.#requests.get(state.requestId) !== state
      ) return false;
      this.#failEventMemory(state, "Host event memory cannot grow without blocking");
      return false;
    }
    if (
      this.#eventMemoryWaitInterrupted(lifecycleEpoch) ||
      this.#requests.get(state.requestId) !== state
    ) {
      permit?.release();
      return false;
    }
    let data: string;
    try {
      data = utf8Text(bytes);
    } catch {
      permit?.release();
      this.#failCommitted(state, new V2CommittedError(
        "Response batch event is not valid UTF-8",
        "response_encoding",
        true,
      ));
      return false;
    }
    if (
      eventIndex < nextEventIndex ||
      state.completedEvents.has(eventIndex) ||
      state.assemblies.has(eventIndex)
    ) {
      permit?.release();
      this.#failCommitted(state, new V2CommittedError(
        "Response batch event indexes conflict",
        "response_conflict",
        true,
      ));
      return false;
    }
    if (!this.#tryRetainEventPermit(permit ?? null, wireBytes)) {
      permit?.release();
      this.#failEventMemory(state, "Host event memory cannot retain the completed event");
      return false;
    }
    // A response_batch is one reliable frame and may contain multiple events.
    // Hand a jumbo event to the consumer before the batch loop acquires the
    // next item, otherwise this task could wait behind the jumbo permit it
    // still owns itself.
    const drainBeforeNextBatchItem = permit?.region === "jumbo";
    if (!recoveryPrefix || eventIndex >= recoveryPrefix.expectedEventCount) {
      state.responseBytes += wireBytes;
    }
    state.completedEvents.set(eventIndex, {
      frame: { event, data },
      wireBytes,
      permit: permit ?? null,
    });
    if (drainBeforeNextBatchItem) {
      await this.#drainCompletedEvents(state, lifecycleEpoch);
      return this.#ownsLifecycle(lifecycleEpoch) &&
        this.#requests.get(state.requestId) === state;
    }
    return true;
  }

  async #acceptResponsePart(
    state: RequestState,
    frame: Extract<ReliableFrame<SidecarFrame>, { kind: "response_event" }>,
    lifecycleEpoch: number,
  ): Promise<void> {
    if (!this.#ownsLifecycle(lifecycleEpoch)) return;
    const recoveryPrefix = state.recoveryPrefix;
    const nextEventIndex = recoveryPrefix
      ? recoveryPrefix.nextEventIndex
      : state.nextEventIndex;
    if (
      frame.eventIndex < nextEventIndex ||
      state.completedEvents.has(frame.eventIndex)
    ) {
      this.#failCommitted(state, new V2CommittedError(
        "Response event index conflicts with completed output",
        "response_conflict",
        true,
      ));
      return;
    }
    const decodedLength = base64UrlDecodedBytes(frame.dataBase64Url);
    if (decodedLength === null) {
      this.#failCommitted(state, new V2CommittedError(
        "Response event part is not valid base64url",
        "response_encoding",
        true,
      ));
      return;
    }
    let assembly = state.assemblies.get(frame.eventIndex);
    if (!assembly) {
      const initialLogicalBytes = partialEventLogicalBytes(frame.event, decodedLength);
      if (this.#rejectConflictingJumboReorder(
        state,
        frame.eventIndex,
        initialLogicalBytes,
      )) return;
      const canWaitForMemory = !this.#retainsOtherEvent(state, frame.eventIndex);
      const permit = await this.#acquireEventPermit(
        initialLogicalBytes,
        `host-response:${state.requestId}`,
        canWaitForMemory,
      );
      if (permit === null) {
        if (
          this.#eventMemoryWaitInterrupted(lifecycleEpoch) ||
          this.#requests.get(state.requestId) !== state
        ) return;
        this.#failEventMemory(state, "Host event memory is unavailable");
        return;
      }
      if (
        this.#eventMemoryWaitInterrupted(lifecycleEpoch) ||
        this.#requests.get(state.requestId) !== state
      ) {
        permit?.release();
        return;
      }
      assembly = {
        event: frame.event,
        partCount: frame.partCount,
        parts: new Map(),
        rawBytes: 0,
        permit: permit ?? null,
      };
      state.assemblies.set(frame.eventIndex, assembly);
    }
    if (assembly.event !== frame.event || assembly.partCount !== frame.partCount) {
      this.#failCommitted(state, new V2CommittedError(
        "Response event parts conflict",
        "response_conflict",
        true,
      ));
      return;
    }
    const existing = assembly.parts.get(frame.partIndex);
    if (!existing) {
      const resizedLogicalBytes = partialEventLogicalBytes(
        assembly.event,
        assembly.rawBytes + decodedLength,
      );
      if (this.#rejectConflictingJumboReorder(
        state,
        frame.eventIndex,
        resizedLogicalBytes,
      )) return;
      const canWaitForMemory = !this.#retainsOtherEvent(state, frame.eventIndex);
      if (!await this.#resizeEventPermit(
        assembly.permit,
        resizedLogicalBytes,
        canWaitForMemory,
      )) {
        if (
          this.#eventMemoryWaitInterrupted(lifecycleEpoch) ||
          this.#requests.get(state.requestId) !== state
        ) return;
        this.#failEventMemory(state, "Host event memory cannot grow without blocking");
        return;
      }
      if (
        this.#eventMemoryWaitInterrupted(lifecycleEpoch) ||
        this.#requests.get(state.requestId) !== state ||
        state.assemblies.get(frame.eventIndex) !== assembly
      ) return;
    }
    let bytes: Uint8Array;
    try {
      bytes = decodeCanonicalBase64Url(frame.dataBase64Url, decodedLength);
    } catch {
      this.#failCommitted(state, new V2CommittedError(
        "Response event part is not valid base64url",
        "response_encoding",
        true,
      ));
      return;
    }
    if (existing) {
      if (existing.offset !== frame.offset || !bytesEqual(existing.bytes, bytes)) {
        this.#failCommitted(state, new V2CommittedError(
          "Response event contains a conflicting duplicate part",
          "response_conflict",
          true,
        ));
      }
      return;
    }
    assembly.parts.set(frame.partIndex, { offset: frame.offset, bytes });
    assembly.rawBytes += bytes.byteLength;
    if (assembly.parts.size !== assembly.partCount) return;

    const parts: Uint8Array[] = [];
    let expectedOffset = 0;
    for (let index = 0; index < assembly.partCount; index++) {
      const part = assembly.parts.get(index);
      if (!part || part.offset !== expectedOffset) {
        this.#failCommitted(state, new V2CommittedError(
          "Response event has a byte gap",
          "response_gap",
          true,
        ));
        return;
      }
      parts.push(part.bytes);
      expectedOffset += part.bytes.byteLength;
    }
    const wireBytes = canonicalEventWireBytes(assembly.event, parts);
    if (
      (!recoveryPrefix || frame.eventIndex >= recoveryPrefix.expectedEventCount) &&
      state.responseBytes + wireBytes > this.#maxResponseBytes
    ) {
      this.#failCommitted(state, new V2CommittedError(
        "Response exceeds the configured canonical byte limit",
        "response_too_large",
        true,
      ));
      return;
    }
    if (this.#rejectConflictingJumboReorder(state, frame.eventIndex, wireBytes)) {
      return;
    }
    const canWaitForMemory = !this.#retainsOtherEvent(state, frame.eventIndex);
    if (!await this.#resizeEventPermit(
      assembly.permit,
      wireBytes,
      canWaitForMemory,
    )) {
      if (
        this.#eventMemoryWaitInterrupted(lifecycleEpoch) ||
        this.#requests.get(state.requestId) !== state
      ) return;
      this.#failEventMemory(state, "Host event memory cannot materialize the complete event");
      return;
    }
    if (
      this.#eventMemoryWaitInterrupted(lifecycleEpoch) ||
      this.#requests.get(state.requestId) !== state ||
      state.assemblies.get(frame.eventIndex) !== assembly
    ) return;
    let data: string;
    try {
      data = utf8Text(concat(parts, expectedOffset));
    } catch {
      this.#failCommitted(state, new V2CommittedError(
        "Response event is not valid UTF-8",
        "response_encoding",
        true,
      ));
      return;
    }
    assembly.parts.clear();
    parts.length = 0;
    if (!this.#tryRetainEventPermit(assembly.permit, wireBytes)) {
      this.#failEventMemory(state, "Host event memory cannot retain the completed event");
      return;
    }
    state.assemblies.delete(frame.eventIndex);
    if (!recoveryPrefix || frame.eventIndex >= recoveryPrefix.expectedEventCount) {
      state.responseBytes += wireBytes;
    }
    state.completedEvents.set(frame.eventIndex, {
      frame: { event: frame.event, data },
      wireBytes,
      permit: assembly.permit,
    });
    assembly.permit = null;
    await this.#drainCompletedEvents(state, lifecycleEpoch);
  }

  async #drainCompletedEvents(
    state: RequestState,
    lifecycleEpoch: number,
  ): Promise<void> {
    if (!this.#ownsLifecycle(lifecycleEpoch) || !this.#requests.has(state.requestId)) return;
    if (!this.#finishRecoveryPrefixIfReady(state, false)) return;
    for (;;) {
      const completed = state.completedEvents.get(state.nextEventIndex);
      if (!completed) break;
      let wireBytes: number;
      try {
        // Batched response events are normally small. Hash their exact-size
        // canonical buffer instead of allocating the general-purpose scratch.
        // Jumbo events use the existing fixed-memory cooperative path. Its
        // referenced task yield keeps Bun alive, and this frame is not marked
        // complete (therefore cannot advance the cumulative ACK) until every
        // canonical byte has entered the proof.
        wireBytes = completed.wireBytes <= MAX_TRANSCRIPT_CHUNK_BYTES
          ? state.transcript.appendCanonical(completed.frame).byteLength
          : await state.transcript.appendCanonicalStream(completed.frame, {
              chunkBytes: MAX_TRANSCRIPT_CHUNK_BYTES,
              yieldTask: async () => {
                await yieldCanonicalWork();
                if (!this.#ownsLifecycle(lifecycleEpoch)) {
                  throw abortError("Host response proof was interrupted");
                }
              },
            });
      } catch (cause) {
        if (
          !this.#ownsLifecycle(lifecycleEpoch) ||
          this.#requests.get(state.requestId) !== state
        ) return;
        // Keep the entry map-owned until failure cleanup releases its permit.
        // Transcript validation (notably terminal JSON parsing) may throw
        // after the event has been fully materialized.
        this.#failCommitted(state, new V2CommittedError(
          cause instanceof Error ? cause.message : "Response transcript validation failed",
          "response_transcript",
          true,
        ));
        return;
      }
      if (wireBytes !== completed.wireBytes) {
        this.#failCommitted(state, new V2CommittedError(
          "Canonical response byte accounting diverged",
          "response_size_mismatch",
          true,
        ));
        return;
      }
      if (state.cancelled) {
        state.completedEvents.delete(state.nextEventIndex);
        state.nextEventIndex++;
        if (TERMINAL_RESPONSE_EVENTS.has(completed.frame.event)) {
          state.cancelledTerminalPermit?.release();
          state.cancelledTerminalPermit = completed.permit;
        } else {
          completed.permit?.release();
        }
        completed.permit = null;
        continue;
      }
      let permit: AtomicBytePermit | undefined;
      let terminalFinalizationAnchor: object | undefined;
      try {
        permit = completed.permit?.transfer("host-run-handle-event");
      } catch (cause) {
        this.#failCommitted(state, new V2CommittedError(
          cause instanceof Error ? cause.message : "Host event memory transfer failed",
          "event_memory_delivery",
          true,
        ));
        return;
      }
      completed.permit = null;
      state.completedEvents.delete(state.nextEventIndex);
      state.nextEventIndex++;
      const started = this.#clock.now();
      try {
        if (
          TERMINAL_RESPONSE_EVENTS.has(completed.frame.event) &&
          !state.terminalFinalizationAnchor
        ) {
          terminalFinalizationAnchor = {};
          // Store before acceptEvent can suspend on client backpressure. A
          // concurrent abort/remove path can then release the proof reference.
          state.terminalFinalizationAnchor = terminalFinalizationAnchor;
        }
        // Backpressure the reliable data lane instead of converting a short
        // consumer pause into an ambiguous committed failure. While this
        // awaits, the inbound cumulative ACK intentionally does not advance,
        // so the sidecar's bounded reliable window and output backpressure
        // stop upstream consumption. Control-lane liveness is observed before
        // this serialized data path and therefore remains responsive.
        await state.handle.acceptEvent(
          completed.frame,
          wireBytes,
          permit,
          terminalFinalizationAnchor,
          this.#eventMemoryWaitAbort.signal,
        );
        permit = undefined;
      } catch (error) {
        // V2RunHandle releases a transferred permit on rejection. A client
        // abort may close the stream while delivery is waiting for space; the
        // cancellation path owns that outcome and must not become ambiguity.
        permit = undefined;
        if (
          !this.#ownsLifecycle(lifecycleEpoch) ||
          !this.#requests.has(state.requestId) ||
          state.cancelled
        ) return;
        if (this.#eventMemoryWaitAbort.signal.aborted) {
          this.#failCommitted(state, new V2CommittedError(
            "V2 sidecar recovery interrupted pending response delivery",
            "sidecar_recovery",
            true,
          ));
          return;
        }
        this.#failCommitted(state, new V2CommittedError(
          error instanceof Error ? error.message : "Host event delivery failed",
          "event_memory_delivery",
          true,
        ));
        return;
      }
      state.diagnostics.assemblyDelivered(this.#clock.now() - started);
    }
    await this.#tryFinalize(state, lifecycleEpoch);
  }

  #finishRecoveryPrefixIfReady(state: RequestState, terminalSeen: boolean): boolean {
    const prefix = state.recoveryPrefix;
    if (!prefix) return true;
    if (prefix.nextEventIndex < prefix.expectedEventCount) {
      if (terminalSeen) {
        this.#failCommitted(state, new V2CommittedError(
          "Recovered terminal omitted part of the previously delivered response prefix",
          "recovery_prefix_missing",
          true,
        ));
      }
      return false;
    }
    if (
      prefix.hashedEventCount !== prefix.expectedEventCount ||
      prefix.hashedByteCount !== prefix.expectedByteCount ||
      prefix.hasher.digestHex() !== prefix.expectedDigest ||
      prefix.segmented !== null
    ) {
      this.#failCommitted(state, new V2CommittedError(
        "Recovered response prefix does not match the original transcript",
        "recovery_prefix_mismatch",
        true,
      ));
      return false;
    }
    state.recoveryPrefix = undefined;
    return true;
  }

  async #tryFinalize(state: RequestState, lifecycleEpoch: number): Promise<void> {
    if (!this.#ownsLifecycle(lifecycleEpoch) || !this.#requests.has(state.requestId)) return;
    const terminal = state.terminal;
    if (!terminal) return;
    // A terminal may arrive before one or more response frames. Check the
    // cheap counters first: snapshot() materializes the entire transcript and
    // doing that after every newly completed event is quadratic.
    const eventCount = state.transcript.eventCount;
    if (eventCount > terminal.eventCount) {
      this.#failCommitted(state, new V2CommittedError(
        "Response contains more events than its terminal proof",
        "terminal_count",
        true,
      ));
      return;
    }
    if (eventCount < terminal.eventCount) return;
    if (state.assemblies.size > 0 || state.completedEvents.size > 0) return;
    const snapshot = state.transcript.snapshot();
    const cancelledPermit = state.cancelledTerminalPermit;
    const finalizationAnchor = state.terminalFinalizationAnchor;
    state.cancelledTerminalPermit = null;
    state.terminalFinalizationAnchor = null;
    let final: Record<string, unknown>;
    try {
      if (!snapshot.final) throw new Error("Terminal proof has no final response");
      validateHostTerminalFinalStructure(snapshot.final);
      final = await validateTerminalProof(terminal, snapshot);
    } catch (cause) {
      if (
        !this.#ownsLifecycle(lifecycleEpoch) ||
        this.#requests.get(state.requestId) !== state
      ) return;
      this.#failCommitted(state, new V2CommittedError(
        cause instanceof Error ? cause.message : "Terminal proof validation failed",
        "terminal_proof",
        true,
      ));
      return;
    } finally {
      cancelledPermit?.release();
      releaseAtomicBytePermit(finalizationAnchor);
    }
    if (
      !this.#ownsLifecycle(lifecycleEpoch) ||
      this.#requests.get(state.requestId) !== state ||
      state.terminal !== terminal
    ) return;
    if (
      !this.#terminalReceipts.has(state.requestId) &&
      this.#terminalReceipts.size >= MAX_TERMINAL_RECEIPTS
    ) {
      this.#failCommitted(state, new V2CommittedError(
        "Host terminal receipt capacity is exhausted",
        "terminal_receipt_capacity",
        true,
      ));
      return;
    }
    const wasCancelled = state.cancelled;
    const wasQuarantined = state.quarantineKind === "cancellation";
    // Even a client-cancelled request owns a durable terminal spool. Suppress
    // client delivery, but retain and ACK its verified terminal so recovery
    // cannot replay that spool forever.
    const receipt: TerminalReceipt = {
      terminal: { ...terminal },
      queued: false,
      replayed: this.#sidecarRecovery !== null,
      cursor: null,
    };
    this.#terminalReceipts.set(state.requestId, receipt);
    this.#removeState(state);
    if (!wasCancelled) state.handle.settle(final, terminal.rateLimit);
    if (wasQuarantined) {
      this.#quarantined = Math.max(0, this.#quarantined - 1);
      state.quarantineKind = null;
    }
    if (!wasCancelled && this.#deferTerminalReceipt(state.requestId)) return;
    this.#queueTerminalReceipt(state.requestId, receipt);
    this.#requestPublication(this.#pump());
  }

  #deferTerminalReceipt(requestId: string): boolean {
    if (!this.#terminalReceiptCoalescing || this.#binding.protocolRevision !== "r2" || this.#draining || this.#sidecarRecovery ||
      !this.#connected || this.#closed || this.#flight.active || this.#dataDirty ||
      this.#scheduler.snapshot().queuedFrames > 0 ||
      this.#deferredTerminalReceipts.size >= MAX_TERMINAL_RECEIPTS) return false;
    this.#deferredTerminalReceipts.add(requestId);
    if (this.#terminalReceiptTimer !== null) return true;
    const epoch = this.#lifecycleEpoch;
    const timer = new AbortController();
    this.#terminalReceiptTimer = timer;
    void this.#clock.sleep(TERMINAL_RECEIPT_COALESCE_MS, timer.signal).then(() => {
      if (this.#terminalReceiptTimer !== timer) return;
      this.#terminalReceiptTimer = null;
      if (!this.#ownsLifecycle(epoch)) return;
      try {
        this.#flushDeferredTerminalReceipts();
        this.#requestPublication(this.#pump());
      } catch (cause) {
        this.#failProtocol(cause);
      }
    }, cause => {
      if (this.#terminalReceiptTimer !== timer) return;
      this.#terminalReceiptTimer = null;
      if (!timer.signal.aborted && this.#ownsLifecycle(epoch)) this.#failProtocol(cause);
    });
    return true;
  }

  #flushDeferredTerminalReceipts(): void {
    this.#terminalReceiptTimer?.abort();
    this.#terminalReceiptTimer = null;
    for (const requestId of this.#deferredTerminalReceipts) {
      this.#deferredTerminalReceipts.delete(requestId);
      const receipt = this.#terminalReceipts.get(requestId);
      if (receipt) this.#queueTerminalReceipt(requestId, receipt);
    }
  }

  #cancelDeferredTerminalReceipts(): void {
    this.#terminalReceiptTimer?.abort();
    this.#terminalReceiptTimer = null;
    this.#deferredTerminalReceipts.clear();
  }

  #queueTerminalReceipt(requestId: string, receipt: TerminalReceipt): void {
    if (receipt.queued) return;
    this.#scheduler.enqueue("control", requestId, {
      kind: "response_ack",
      requestId,
      terminalDigest: receipt.terminal.transcriptSha256,
    });
    receipt.queued = true;
  }

  #releaseOrphanRecoveryCandidate(candidate: OrphanRecoveryCandidate): void {
    const assembly = candidate.assembly;
    if (assembly) {
      for (const part of assembly.parts.values()) part.bytes.fill(0);
      assembly.parts.clear();
      assembly.permit?.release();
      assembly.permit = null;
      candidate.assembly = null;
    }
  }

  #deleteOrphanRecoveryCandidate(requestId: string): void {
    const candidate = this.#orphanRecoveryCandidates.get(requestId);
    if (!candidate) return;
    this.#orphanRecoveryCandidates.delete(requestId);
    this.#releaseOrphanRecoveryCandidate(candidate);
  }

  #clearOrphanRecoveryCandidates(): void {
    for (const candidate of this.#orphanRecoveryCandidates.values()) {
      this.#releaseOrphanRecoveryCandidate(candidate);
    }
    this.#orphanRecoveryCandidates.clear();
  }

  #failOrphanRecoveryCandidate(
    candidate: OrphanRecoveryCandidate,
    message: string,
    code: string,
  ): void {
    if (this.#orphanRecoveryCandidates.get(candidate.requestId) === candidate) {
      this.#orphanRecoveryCandidates.delete(candidate.requestId);
    }
    this.#releaseOrphanRecoveryCandidate(candidate);
    this.#failRecoveryProtocol(message, code);
  }

  async #appendOrphanRecoveryEvent(
    candidate: OrphanRecoveryCandidate,
    eventIndex: number,
    frame: SseFrame,
    wireBytes: number,
    permit: AtomicBytePermit | null | undefined,
    lifecycleEpoch: number,
  ): Promise<boolean> {
    if (
      eventIndex !== candidate.nextEventIndex ||
      candidate.responseBytes + wireBytes > this.#maxResponseBytes
    ) {
      permit?.release();
      this.#failOrphanRecoveryCandidate(
        candidate,
        eventIndex !== candidate.nextEventIndex
          ? "Recovered orphan response event order is invalid"
          : "Recovered orphan response exceeds the configured byte limit",
        eventIndex !== candidate.nextEventIndex ? "orphan_event_order" : "response_too_large",
      );
      return false;
    }
    try {
      let transcript = candidate.transcript;
      if (!transcript || candidate.terminalProof) {
        throw new Error("Recovered orphan emitted output after its terminal event");
      }
      const appended = await transcript.appendCanonicalStream(frame, {
        chunkBytes: MAX_TRANSCRIPT_CHUNK_BYTES,
        yieldTask: async () => {
          await yieldCanonicalWork();
          if (!this.#ownsLifecycle(lifecycleEpoch)) {
            throw abortError("Recovered orphan verification was interrupted");
          }
        },
      });
      if (appended !== wireBytes) {
        throw new Error("Canonical orphan response byte accounting diverged");
      }
      if (
        frame.event === "response.completed" ||
        frame.event === "response.failed" ||
        frame.event === "response.incomplete"
      ) {
        candidate.terminalProof = await transcript.createTerminal();
        // Retain only bounded proof metadata between the terminal SSE event and
        // response_terminal; the parsed final object is no longer reachable.
        candidate.transcript = null;
        transcript = null;
      }
    } catch (cause) {
      permit?.release();
      if (!this.#ownsLifecycle(lifecycleEpoch)) return false;
      this.#failOrphanRecoveryCandidate(
        candidate,
        cause instanceof Error ? cause.message : "Recovered orphan transcript is invalid",
        "orphan_transcript",
      );
      return false;
    }
    permit?.release();
    if (
      !this.#ownsLifecycle(lifecycleEpoch) ||
      this.#orphanRecoveryCandidates.get(candidate.requestId) !== candidate
    ) return false;
    candidate.responseBytes += wireBytes;
    candidate.nextEventIndex++;
    return true;
  }

  async #acceptOrphanRecoveryWholeEvent(
    candidate: OrphanRecoveryCandidate,
    eventIndex: number,
    event: string,
    encoded: string,
    lifecycleEpoch: number,
  ): Promise<boolean> {
    if (candidate.assembly || eventIndex !== candidate.nextEventIndex) {
      this.#failOrphanRecoveryCandidate(
        candidate,
        "Recovered orphan response batch order conflicts",
        "orphan_event_order",
      );
      return false;
    }
    const decodedLength = base64UrlDecodedBytes(encoded);
    if (decodedLength === null) {
      this.#failOrphanRecoveryCandidate(
        candidate,
        "Recovered orphan response batch encoding is invalid",
        "response_encoding",
      );
      return false;
    }
    const wireBytes = partialEventLogicalBytes(event, decodedLength);
    if (candidate.responseBytes + wireBytes > this.#maxResponseBytes) {
      this.#failOrphanRecoveryCandidate(
        candidate,
        "Recovered orphan response exceeds the configured byte limit",
        "response_too_large",
      );
      return false;
    }
    const permit = await this.#acquireEventPermit(
      wireBytes,
      `host-orphan-recovery:${candidate.requestId}`,
    );
    if (permit === null) {
      if (
        this.#eventMemoryWaitInterrupted(lifecycleEpoch) ||
        this.#orphanRecoveryCandidates.get(candidate.requestId) !== candidate
      ) return false;
      this.#failOrphanRecoveryCandidate(
        candidate,
        "Host event memory is unavailable for orphan recovery",
        "event_memory_capacity",
      );
      return false;
    }
    if (
      this.#eventMemoryWaitInterrupted(lifecycleEpoch) ||
      this.#orphanRecoveryCandidates.get(candidate.requestId) !== candidate
    ) {
      permit?.release();
      return false;
    }
    let bytes: Uint8Array;
    try {
      bytes = decodeCanonicalBase64Url(encoded, decodedLength);
    } catch {
      permit?.release();
      this.#failOrphanRecoveryCandidate(
        candidate,
        "Recovered orphan response batch encoding is invalid",
        "response_encoding",
      );
      return false;
    }
    if (!await this.#resizeEventPermit(permit ?? null, wireBytes)) {
      bytes.fill(0);
      permit?.release();
      if (
        this.#eventMemoryWaitInterrupted(lifecycleEpoch) ||
        this.#orphanRecoveryCandidates.get(candidate.requestId) !== candidate
      ) return false;
      this.#failOrphanRecoveryCandidate(
        candidate,
        "Host event memory cannot verify the recovered orphan event",
        "event_memory_capacity",
      );
      return false;
    }
    if (
      this.#eventMemoryWaitInterrupted(lifecycleEpoch) ||
      this.#orphanRecoveryCandidates.get(candidate.requestId) !== candidate
    ) {
      bytes.fill(0);
      permit?.release();
      return false;
    }
    let data: string;
    try {
      data = utf8Text(bytes);
    } catch {
      bytes.fill(0);
      permit?.release();
      this.#failOrphanRecoveryCandidate(
        candidate,
        "Recovered orphan response event is not valid UTF-8",
        "response_encoding",
      );
      return false;
    }
    bytes.fill(0);
    return this.#appendOrphanRecoveryEvent(
      candidate,
      eventIndex,
      { event, data },
      wireBytes,
      permit,
      lifecycleEpoch,
    );
  }

  async #acceptOrphanRecoveryEventPart(
    candidate: OrphanRecoveryCandidate,
    frame: Extract<ReliableFrame<SidecarFrame>, { kind: "response_event" }>,
    lifecycleEpoch: number,
  ): Promise<void> {
    let assembly = candidate.assembly;
    if (!assembly) {
      if (
        frame.eventIndex !== candidate.nextEventIndex ||
        frame.partIndex !== 0 ||
        frame.offset !== 0
      ) {
        this.#failOrphanRecoveryCandidate(
          candidate,
          "Recovered orphan response begins with a gap",
          "orphan_event_order",
        );
        return;
      }
      const decodedLength = base64UrlDecodedBytes(frame.dataBase64Url);
      if (decodedLength === null) {
        this.#failOrphanRecoveryCandidate(
          candidate,
          "Recovered orphan response part is not valid base64url",
          "response_encoding",
        );
        return;
      }
      const partialBytes = partialEventLogicalBytes(frame.event, decodedLength);
      if (candidate.responseBytes + partialBytes > this.#maxResponseBytes) {
        this.#failOrphanRecoveryCandidate(
          candidate,
          "Recovered orphan response exceeds the configured byte limit",
          "response_too_large",
        );
        return;
      }
      const permit = await this.#acquireEventPermit(
        partialBytes,
        `host-orphan-recovery:${candidate.requestId}`,
      );
      if (permit === null) {
        if (
          this.#eventMemoryWaitInterrupted(lifecycleEpoch) ||
          this.#orphanRecoveryCandidates.get(candidate.requestId) !== candidate
        ) return;
        this.#failOrphanRecoveryCandidate(
          candidate,
          "Host event memory is unavailable for orphan recovery",
          "event_memory_capacity",
        );
        return;
      }
      if (
        this.#eventMemoryWaitInterrupted(lifecycleEpoch) ||
        this.#orphanRecoveryCandidates.get(candidate.requestId) !== candidate
      ) {
        permit?.release();
        return;
      }
      assembly = {
        eventIndex: frame.eventIndex,
        event: frame.event,
        partCount: frame.partCount,
        parts: new Map(),
        rawBytes: 0,
        permit: permit ?? null,
        nextPartIndex: 0,
        nextOffset: 0,
      };
      candidate.assembly = assembly;
    }
    if (
      assembly.eventIndex !== frame.eventIndex ||
      assembly.event !== frame.event ||
      assembly.partCount !== frame.partCount ||
      assembly.nextPartIndex !== frame.partIndex ||
      assembly.nextOffset !== frame.offset
    ) {
      this.#failOrphanRecoveryCandidate(
        candidate,
        "Recovered orphan response parts conflict",
        "orphan_event_order",
      );
      return;
    }
    const decodedLength = base64UrlDecodedBytes(frame.dataBase64Url);
    const partialBytes = decodedLength === null
      ? null
      : partialEventLogicalBytes(assembly.event, assembly.rawBytes + decodedLength);
    if (
      decodedLength === null ||
      partialBytes === null ||
      candidate.responseBytes + partialBytes > this.#maxResponseBytes
    ) {
      this.#failOrphanRecoveryCandidate(
        candidate,
        decodedLength === null
          ? "Recovered orphan response part is not valid base64url"
          : partialBytes !== null && candidate.responseBytes + partialBytes > this.#maxResponseBytes
            ? "Recovered orphan response exceeds the configured byte limit"
          : "Host event memory cannot grow the recovered orphan event",
        decodedLength === null
          ? "response_encoding"
          : partialBytes !== null && candidate.responseBytes + partialBytes > this.#maxResponseBytes
            ? "response_too_large"
            : "event_memory_capacity",
      );
      return;
    }
    if (!await this.#resizeEventPermit(assembly.permit, partialBytes)) {
      if (
        this.#eventMemoryWaitInterrupted(lifecycleEpoch) ||
        this.#orphanRecoveryCandidates.get(candidate.requestId) !== candidate
      ) return;
      this.#failOrphanRecoveryCandidate(
        candidate,
        "Host event memory cannot grow the recovered orphan event",
        "event_memory_capacity",
      );
      return;
    }
    if (
      this.#eventMemoryWaitInterrupted(lifecycleEpoch) ||
      this.#orphanRecoveryCandidates.get(candidate.requestId) !== candidate ||
      candidate.assembly !== assembly
    ) return;
    let bytes: Uint8Array;
    try {
      bytes = decodeCanonicalBase64Url(frame.dataBase64Url, decodedLength);
    } catch {
      this.#failOrphanRecoveryCandidate(
        candidate,
        "Recovered orphan response part is not valid base64url",
        "response_encoding",
      );
      return;
    }
    assembly.parts.set(frame.partIndex, { offset: frame.offset, bytes });
    assembly.rawBytes += bytes.byteLength;
    assembly.nextPartIndex++;
    assembly.nextOffset += bytes.byteLength;
    if (assembly.nextPartIndex !== assembly.partCount) return;

    const parts = [...assembly.parts.values()].map(part => part.bytes);
    const wireBytes = canonicalEventWireBytes(assembly.event, parts);
    if (candidate.responseBytes + wireBytes > this.#maxResponseBytes) {
      this.#failOrphanRecoveryCandidate(
        candidate,
        "Recovered orphan response exceeds the configured byte limit",
        "response_too_large",
      );
      return;
    }
    if (!await this.#resizeEventPermit(assembly.permit, wireBytes)) {
      if (
        this.#eventMemoryWaitInterrupted(lifecycleEpoch) ||
        this.#orphanRecoveryCandidates.get(candidate.requestId) !== candidate
      ) return;
      this.#failOrphanRecoveryCandidate(
        candidate,
        "Host event memory cannot verify the recovered orphan event",
        "event_memory_capacity",
      );
      return;
    }
    if (
      this.#eventMemoryWaitInterrupted(lifecycleEpoch) ||
      this.#orphanRecoveryCandidates.get(candidate.requestId) !== candidate ||
      candidate.assembly !== assembly
    ) return;
    let data: string;
    try {
      data = utf8Text(concat(parts, assembly.rawBytes));
    } catch {
      this.#failOrphanRecoveryCandidate(
        candidate,
        "Recovered orphan response event is not valid UTF-8",
        "response_encoding",
      );
      return;
    }
    for (const part of parts) part.fill(0);
    assembly.parts.clear();
    const permit = assembly.permit;
    assembly.permit = null;
    candidate.assembly = null;
    await this.#appendOrphanRecoveryEvent(
      candidate,
      frame.eventIndex,
      { event: frame.event, data },
      wireBytes,
      permit,
      lifecycleEpoch,
    );
  }

  async #validateOrphanRecoveryTerminal(
    candidate: OrphanRecoveryCandidate,
    terminal: Extract<ReliableFrame<SidecarFrame>, { kind: "response_terminal" }>,
    lifecycleEpoch: number,
  ): Promise<void> {
    if (candidate.assembly) {
      this.#failOrphanRecoveryCandidate(
        candidate,
        "Recovered orphan terminal arrived before its event completed",
        "orphan_terminal_truncated",
      );
      return;
    }
    if (!this.#ownsLifecycle(lifecycleEpoch)) return;
    const derived = candidate.terminalProof;
    if (!derived || !this.#terminalMatchesReceipt(terminal, derived)) {
      this.#failOrphanRecoveryCandidate(
        candidate,
        derived
          ? "Recovered orphan terminal does not match its fully verified replay"
          : "Recovered orphan terminal arrived without a verified terminal event",
        "orphan_terminal_proof",
      );
      return;
    }
    if (
      !this.#ownsLifecycle(lifecycleEpoch) ||
      this.#orphanRecoveryCandidates.get(candidate.requestId) !== candidate
    ) return;
    if (this.#terminalReceipts.size >= MAX_TERMINAL_RECEIPTS) {
      this.#failOrphanRecoveryCandidate(
        candidate,
        "Host terminal receipt capacity is exhausted during recovery",
        "terminal_receipt_capacity",
      );
      return;
    }
    this.#orphanRecoveryCandidates.delete(candidate.requestId);
    const receipt: TerminalReceipt = {
      terminal: { ...terminal },
      queued: false,
      replayed: true,
      cursor: null,
    };
    this.#terminalReceipts.set(candidate.requestId, receipt);
    this.#queueTerminalReceipt(candidate.requestId, receipt);
  }

  #trackOrphanRecoveryCandidate(
    frame: Extract<ReliableFrame<SidecarFrame>, { kind: "request_ack" }>,
    recoveryEnvelope: boolean,
  ): void {
    if (!recoveryEnvelope || this.#binding.protocolRevision !== "r2") return;
    const recovery = this.#sidecarRecovery;
    // Startup candidates are accepted only inside the initial zero-credit
    // recovery window. Locally allocated/imported IDs remain owned even after
    // an earlier frame in the same flight removes their live RequestState.
    if ((!recovery && !this.#startupRecoveryOpen) || recovery?.ownedRequestIds.has(frame.requestId)) {
      return;
    }
    if (
      this.#terminalReceipts.has(frame.requestId) ||
      this.#ambiguityTombstones.has(frame.requestId) ||
      this.#startupRecoveryOwnedRequestIds.has(frame.requestId)
    ) return;
    const existing = this.#orphanRecoveryCandidates.get(frame.requestId);
    if (existing) {
      if (existing.requestDigest !== frame.requestDigest) {
        this.#failRecoveryProtocol(
          "Recovered orphan commitment changed its request digest",
          "orphan_commit_conflict",
        );
      }
      return;
    }
    if (this.#orphanRecoveryCandidates.size >= MAX_TERMINAL_RECEIPTS) {
      this.#failRecoveryProtocol(
        "Host orphan recovery candidate capacity is exhausted",
        "terminal_receipt_capacity",
      );
      return;
    }
    this.#orphanRecoveryCandidates.set(frame.requestId, {
      requestId: frame.requestId,
      requestDigest: frame.requestDigest,
      transcript: new TranscriptProof(frame.requestId, frame.requestDigest, {
        maxBytes: this.#maxResponseBytes,
        retainBytes: false,
        hasher: createNativeTranscriptHasher(),
      }),
      terminalProof: null,
      responseBytes: 0,
      nextEventIndex: 0,
      assembly: null,
    });
  }

  #failRecoveryProtocol(message: string, code: string): void {
    const error = new V2CommittedError(message, code, true);
    const recovery = this.#sidecarRecovery;
    if (recovery) {
      this.failSidecarRecovery(recovery.token, error);
      return;
    }
    throw error;
  }

  #terminalMatchesReceipt(
    frame: Extract<ReliableFrame<SidecarFrame>, { kind: "response_terminal" }>,
    expected: Extract<SidecarFrame, { kind: "response_terminal" }>,
  ): boolean {
    return frame.requestId === expected.requestId &&
      frame.requestDigest === expected.requestDigest &&
      frame.responseId === expected.responseId &&
      frame.eventCount === expected.eventCount &&
      frame.byteCount === expected.byteCount &&
      frame.transcriptSha256 === expected.transcriptSha256 &&
      frame.terminalStatus === expected.terminalStatus &&
      frame.finalEventIndex === expected.finalEventIndex &&
      frame.finalSha256 === expected.finalSha256;
  }

  #acknowledgeOutbound(cursor: { epoch: number; sequence: number }): void {
    let progressed: Map<RequestState, number> | null = null;
    if (this.#admissionProgressObservers > 0) {
      for (const frame of this.#outbound.snapshot().frames) {
        if (
          frame.epoch !== cursor.epoch || frame.sequence > cursor.sequence ||
          frame.kind !== "request_data"
        ) continue;
        const state = this.#requests.get(frame.requestId);
        if (!state?.onAdmissionProgress) continue;
        const acknowledgedBytes = base64UrlDecodedBytes(frame.dataBase64Url);
        if (acknowledgedBytes === null) continue;
        const values = progressed ??= new Map();
        values.set(state, (values.get(state) ?? 0) + acknowledgedBytes);
      }
    }
    this.#outbound.acknowledge(cursor);
    // Validate and apply the cumulative ACK before notifying observers. A
    // forged future cursor must never look like transfer progress.
    if (progressed) {
      for (const [state, acknowledgedBytes] of progressed) {
        state.acknowledgedEncodedBytes = Math.min(
          state.manifest.encodedBytes,
          state.acknowledgedEncodedBytes + acknowledgedBytes,
        );
        this.#reflectAdmissionProgress(state, {
          stage: "request_data",
          acknowledgedEncodedBytes: state.acknowledgedEncodedBytes,
          totalEncodedBytes: state.manifest.encodedBytes,
        });
      }
    }
    for (const [requestId, receipt] of this.#terminalReceipts) {
      if (
        receipt.cursor?.epoch === cursor.epoch &&
        receipt.cursor.sequence <= cursor.sequence
      ) this.#terminalReceipts.delete(requestId);
    }
    this.#maybeResolveDrainWaiters();
    this.#maybeCompleteSidecarRecovery();
  }

  async #prepareRequest(
    body: Record<string, unknown>,
    signal?: AbortSignal,
  ): Promise<PreparedRequestPlan> {
    // The sidecar forwards these proven canonical bytes directly to fetch to
    // avoid a decoded-string + parsed-object + reserialized-string peak. Apply
    // both upstream policy fields before canonicalization so byte accounting,
    // digest, compression, frames, and the immutable R8 fetch body all describe
    // exactly the same bytes. Outer HTTP streaming remains a separate contract.
    const upstreamBody = normalizeUpstreamResponsesRequest(body);
    const canonical = await canonicalHostRequestBytes(
      upstreamBody,
      this.#limits.requestMaxBytes,
      signal,
    );
    const decoded = canonical.bytes;
    if (decoded.byteLength < 1) {
      throw new V2PreCommitError("V2 request exceeds the 64 MiB limit", "request_too_large");
    }
    const requestDigest = await sha256Hex(decoded);
    if (signal?.aborted) throw signal.reason ?? abortError("V2 request aborted after hashing");
    const partBytes = this.#binding.protocolRevision === "r2"
      ? V2_R2_REQUEST_PART_BYTES
      : R1_REQUEST_PART_BYTES;
    let encoded = decoded;
    let encoding: RequestEncoding = "identity";
    const inlineBase64 = canonical.containsLargeInlineBase64;
    // Non-tiny inline data URLs recover roughly one quarter of their base64
    // expansion only with balanced gzip on Bun. Keep latency-critical inline
    // data URLs on the existing zero-compression path and ordinary text unchanged.
    const gzipPriority = decoded.byteLength >= BULK_GZIP_MIN_BYTES ||
        (inlineBase64 && decoded.byteLength > V2_LATENCY_CRITICAL_REQUEST_BYTES)
      ? "bulk"
      : "latency";
    if (
      this.#gzip &&
      decoded.byteLength > partBytes &&
      (!inlineBase64 || gzipPriority === "bulk")
    ) {
      const sampleLength = Math.min(decoded.byteLength, GZIP_SAMPLE_BYTES);
      const sample = new Uint8Array(sampleLength);
      const firstLength = Math.ceil(sampleLength / 2);
      sample.set(decoded.subarray(0, firstLength), 0);
      sample.set(
        decoded.subarray(decoded.byteLength - (sampleLength - firstLength)),
        firstLength,
      );
      const sampleCompressed = await this.#gzip(sample, {
        signal,
        inputOwnership: "owned",
        priority: gzipPriority,
      });
      if (signal?.aborted) {
        throw signal.reason ?? abortError("V2 request aborted after compression sampling");
      }
      const sampleSavings = sampleLength - sampleCompressed.byteLength;
      const sampleWorthwhile = sampleCompressed.byteLength > 0 &&
        sampleSavings >= Math.ceil(sampleLength * MIN_GZIP_SAVINGS_RATIO);
      // Compression owns this scratch copy, so a worker pool may transfer its
      // complete ArrayBuffer without detaching the canonical request bytes.
      const compressed = !sampleWorthwhile
        ? null
        : sampleLength === decoded.byteLength
          ? sampleCompressed
          : await this.#gzip(decoded.slice(), {
              signal,
              inputOwnership: "owned",
              priority: gzipPriority,
            });
      if (signal?.aborted) {
        throw signal.reason ?? abortError("V2 request aborted after compression");
      }
      const identityParts = Math.ceil(decoded.byteLength / partBytes);
      const compressedParts = compressed && compressed.byteLength > 0
        ? Math.ceil(compressed.byteLength / partBytes)
        : Number.POSITIVE_INFINITY;
      const savedBytes = compressed ? decoded.byteLength - compressed.byteLength : 0;
      if (
        compressedParts < identityParts &&
        savedBytes >= partBytes &&
        savedBytes >= Math.ceil(decoded.byteLength * MIN_GZIP_SAVINGS_RATIO)
      ) {
        encoded = compressed!;
        encoding = "gzip+base64";
      }
    }
    if (encoded.byteLength < 1 || encoded.byteLength > this.#limits.requestMaxBytes) {
      throw new V2PreCommitError("Encoded V2 request exceeds the 64 MiB limit", "request_too_large");
    }
    const partCount = Math.ceil(encoded.byteLength / partBytes);
    return {
      requestDigest,
      decodedBytes: decoded.byteLength,
      encodedBytes: encoded.byteLength,
      encoding,
      partCount,
      partBytes,
      encodedPayload: encoded,
      nextPartIndex: 0,
      stage: "open",
    };
  }

  #takePreparedFrame(
    requestId: string,
    plan: PreparedRequestPlan,
  ): HostFrame | null {
    if (plan.stage === "done") return null;
    if (plan.stage === "open") {
      plan.stage = "data";
      return {
        kind: "request_open",
        requestId,
        requestDigest: plan.requestDigest,
        decodedBytes: plan.decodedBytes,
        encodedBytes: plan.encodedBytes,
        encoding: plan.encoding,
        partCount: plan.partCount,
      };
    }
    if (plan.stage === "data") {
      const encoded = plan.encodedPayload;
      if (!encoded || plan.nextPartIndex >= plan.partCount) {
        throw new Error("Prepared V2 request data state is invalid");
      }
      const partIndex = plan.nextPartIndex++;
      const offset = partIndex * plan.partBytes;
      const frame: HostFrame = {
        kind: "request_data",
        requestId,
        partIndex,
        offset,
        dataBase64Url: base64UrlEncode(
          encoded.subarray(offset, Math.min(encoded.byteLength, offset + plan.partBytes)),
        ),
      };
      if (plan.nextPartIndex === plan.partCount) {
        // Every byte now lives only in a scheduled/outbound physical frame.
        // Release the compact source before the upstream response starts.
        plan.encodedPayload = null;
        plan.stage = "commit";
      }
      return frame;
    }
    plan.stage = "done";
    return this.#binding.protocolRevision === "r2"
      ? {
          kind: "request_commit",
          requestId,
          protocolRevision: "r2",
          schemaVersion: GP_V2_R2_SCHEMA_VERSION,
          projectId: this.#binding.projectId,
          sessionId: this.#binding.sessionId,
          leaderGeneration: this.#binding.leaderGeneration,
          buildHash: this.#binding.buildHash,
          schemaHash: this.#binding.schemaHash,
          launchNonce: this.#binding.launchNonce,
          hostInstanceId: this.#hostInstanceId,
          targetInstanceId: this.#expectedSidecarInstanceId!,
          requestDigest: plan.requestDigest,
          decodedBytes: plan.decodedBytes,
          encodedBytes: plan.encodedBytes,
          encoding: plan.encoding,
          partCount: plan.partCount,
        }
      : { kind: "request_commit", requestId };
  }

  #stageNextRequestFrame(state: RequestState): void {
    const plan = state.preparedRequest;
    if (!plan) return;
    let frame: HostFrame | null;
    try {
      frame = this.#takePreparedFrame(state.requestId, plan);
    } catch (cause) {
      this.#failRequestStaging(state, new V2PreCommitError(
        cause instanceof Error ? cause.message : "Prepared V2 request frame is invalid",
        "request_stage",
      ));
      return;
    }
    if (!frame) {
      state.preparedRequest = null;
      this.#releasePreparedPayload(state);
      return;
    }
    // One frame per request is enough for weighted fairness; #pump replenishes
    // it only after the scheduler transfers ownership of the previous frame.
    try {
      // Request-body scheduling is profile-neutral. Only the R2 request_start
      // fence uses commit priority; promoting tiny bulk bodies would change
      // otherwise identical standard/bulk flight membership.
      this.#scheduler.enqueue("data", state.requestId, frame);
    } catch (cause) {
      // Frame extraction mutates the lazy cursor, so enqueue failure cannot be
      // retried transactionally. Fail and release the entire pre-execution
      // state rather than leaking a reservation or sending a body with a hole.
      this.#failRequestStaging(state, new V2PreCommitError(
        cause instanceof Error ? cause.message : "Prepared V2 request could not be staged",
        "request_stage",
      ));
      return;
    }
    if (plan.encodedPayload === null) this.#releasePreparedPayload(state);
    if (plan.stage === "done") state.preparedRequest = null;
  }

  #failRequestStaging(state: RequestState, error: V2PreCommitError): void {
    const published = state.published;
    this.#failPreCommit(state, error);
    if (!published || this.#closed) return;
    try {
      this.#scheduler.enqueue("control", state.requestId, {
        kind: "request_cancel",
        requestId: state.requestId,
        reason: "host request staging failed",
      });
    } catch (cause) {
      this.#failProtocol(cause);
    }
  }

  #admitPending(): void {
    while (this.#availablePrefetchCredits() > 0) {
      const transferAvailable = this.#transferBudget - this.#transferReservations;
      const eligible = (candidate: RequestState) => (
        !candidate.admitted && candidate.transferCost <= transferAvailable
      );
      // A queued interactive request always claims the next body slot before
      // medium prefetch. Once one is admitted the bulk-profile precommit depth returns to
      // the established four-request contract.
      const latencyCandidate = this.#availableCredits() > 0
        ? this.#pending.find(candidate => (
            eligible(candidate) &&
            candidate.manifest.decodedBytes <= V2_LATENCY_CRITICAL_REQUEST_BYTES
          ))
        : undefined;
      const state = latencyCandidate ?? this.#pending.find(candidate => (
        eligible(candidate) &&
        candidate.manifest.decodedBytes > V2_LATENCY_CRITICAL_REQUEST_BYTES
      ));
      if (!state) return;
      this.#markRequestAdmitted(state);
      if (state.admissionTimer) clearTimeout(state.admissionTimer);
      state.admissionTimer = undefined;
      this.#creditReservations++;
      this.#transferReservations += state.transferCost;
      if (state.manifest.decodedBytes <= V2_LATENCY_CRITICAL_REQUEST_BYTES) {
        state.latencyCriticalPreCommitTracked = true;
        this.#latencyCriticalPreCommitTransfers++;
        state.latencyCriticalRequestTracked = true;
        this.#latencyCriticalRequests++;
      }
      const onAdmissionReflected = state.onAdmissionReflected;
      state.onAdmissionReflected = undefined;
      // The credit is owned by the transport before the router is notified.
      // An observer bug must retain the conservative router reservation rather
      // than unwind an otherwise valid transport admission.
      try { onAdmissionReflected?.(); } catch {}
      this.#stageNextRequestFrame(state);
    }
  }

  #availableCredits(): number {
    if (
      this.#sidecarRecovery || !this.#connected || this.#closed || this.#draining ||
      this.#congestion.congested
    ) return 0;
    return Math.max(0, Math.min(
      this.#remoteCredits - this.#creditReservations,
      this.#hostCeiling - this.#creditReservations - this.#ambiguousQuarantined,
      MAX_PRECOMMIT_EXECUTION_RESERVATIONS -
        this.#creditReservations - this.#ambiguousQuarantined,
      this.#transferBudget - this.#transferReservations,
    ));
  }

  #availablePrefetchCredits(): number {
    if (
      this.#sidecarRecovery || !this.#connected || this.#closed || this.#draining ||
      this.#congestion.congested
    ) return 0;
    const limit = this.#bulkBodyPrefetchEnabled()
      ? MAX_BULK_BODY_PREFETCH_RESERVATIONS
      : MAX_PRECOMMIT_EXECUTION_RESERVATIONS;
    return Math.max(0, Math.min(
      this.#remoteCredits - this.#creditReservations,
      this.#hostCeiling - this.#creditReservations - this.#ambiguousQuarantined,
      limit - this.#creditReservations - this.#ambiguousQuarantined,
      this.#transferBudget - this.#transferReservations,
    ));
  }

  #admissionBlockReason(): V2AdmissionBlockReason {
    if (this.#sidecarRecovery) return "transport_recovery";
    if (!this.#connected || this.#closed) return "transport_unavailable";
    if (this.#draining) return "draining";
    if (this.#congestion.congested) return "transport_congested";
    if (this.#remoteCredits - this.#creditReservations <= 0) {
      return "zero_remote_credits";
    }
    if (
      this.#hostCeiling - this.#creditReservations - this.#ambiguousQuarantined <= 0
    ) return "host_ceiling";
    const prefetchLimit = this.#bulkBodyPrefetchEnabled()
      ? MAX_BULK_BODY_PREFETCH_RESERVATIONS
      : MAX_PRECOMMIT_EXECUTION_RESERVATIONS;
    if (
      prefetchLimit - this.#creditReservations - this.#ambiguousQuarantined <= 0
    ) return "execution_window";
    if (this.#transferBudget - this.#transferReservations <= 0) {
      return "transfer_budget";
    }
    return "available";
  }

  #bulkBodyPrefetchEnabled(): boolean {
    // An idle transport has not classified the next body yet, so advertise the
    // common four-request admission window to the router. Once exact canonical
    // sizing proves at least one live request is non-latency-critical, bulk R2
    // may use its two additional body-only prefetch slots. A live tiny request
    // keeps the shared four-request contract until its terminal cleanup.
    return this.#transportProfile === "bulk-v1" &&
      this.#binding.protocolRevision === "r2" &&
      this.#latencyCriticalRequests === 0 &&
      (this.#creditReservations > 0 || this.#committedRequestCount > 0);
  }

  #hostFlightLimits(): Pick<
    V2ProtocolTransportLimits,
    "hostFlightMaxFrames" | "hostFlightMaxBytes"
  > {
    if (this.#transportProfile !== "bulk-v1") return this.#limits;
    // Requests already small enough to fit one standard flight never pay for
    // medium/bulk sizing. A loss or stall also returns future flights to the
    // proven standard bound until two clean request flights restore confidence.
    // Both decisions are O(1) on the publication hot path.
    return this.#latencyCriticalPreCommitTransfers > 0 ||
        this.#congestion.congested ||
        !this.#acceleratedFlightsHealthy
      ? GP_V2_R2_TRANSPORT_LIMITS
      : this.#limits;
  }

  #pump(): PublicationUrgency | null {
    if (!this.#connected || this.#closed) return null;
    if (this.#flight.active) return null;
    // A completed response must not occupy an otherwise empty immutable flight
    // just before a fast tool result arrives. Piggyback its verified receipt on
    // that next flight, or send it alone when the bounded timer expires. The
    // cumulative receive ACK and every request execution fence are unchanged.
    if (this.#deferredTerminalReceipts.size > 0 &&
      (this.#draining || this.#scheduler.snapshot().queuedFrames > 0)) {
      this.#flushDeferredTerminalReceipts();
    }
    const flightLimits = this.#hostFlightLimits();
    let urgency: PublicationUrgency | null = null;
    for (;;) {
      const snapshot = this.#outbound.snapshot();
      const frameBudget = Math.min(
        flightLimits.hostFlightMaxFrames - snapshot.frames.length,
        this.#limits.windowMaxFrames - snapshot.frames.length,
      );
      const byteBudget = Math.min(
        flightLimits.hostFlightMaxBytes - snapshot.encodedBytes,
        this.#limits.windowMaxBytes - snapshot.encodedBytes,
      );
      if (frameBudget < 1 || byteBudget < 512) break;
      let next: HostFrame[];
      try {
        next = this.#scheduler.take(1, Math.max(1, byteBudget - 256));
      } catch (cause) {
        this.#failProtocol(cause);
        return urgency;
      }
      if (next.length === 0) break;
      let reliable: ReliableFrame<HostFrame>;
      try {
        reliable = this.#outbound.push(next[0]!);
      } catch (cause) {
        // take() already transferred this frame out of the scheduler. A window
        // invariant failure is therefore transport-wide and cannot be repaired
        // by retrying one request without risking an execution-order hole.
        this.#failProtocol(cause);
        return urgency;
      }
      if (reliable.kind === "response_ack") {
        const receipt = this.#terminalReceipts.get(reliable.requestId);
        if (receipt && receipt.terminal.transcriptSha256 === reliable.terminalDigest) {
          receipt.cursor = { epoch: reliable.epoch, sequence: reliable.sequence };
        }
      }
      this.#dataDirty = true;
      urgency = strongestUrgency(urgency, hostFrameUrgency(reliable));
      if ("requestId" in reliable && typeof reliable.requestId === "string") {
        const state = this.#requests.get(reliable.requestId);
        if (state) state.published = true;
        if (state && reliable.kind === "request_commit") state.commitStaged = true;
        if (state && reliable.kind === "request_start") {
          state.startRisk = true;
          this.#reflectExecutionRisk(state);
        }
        if (state) state.diagnostics.published();
        if (state && reliable.kind !== "request_commit") {
          this.#stageNextRequestFrame(state);
        }
      }
    }
    return urgency;
  }

  #requestPublication(urgency: PublicationUrgency | null): void {
    if (urgency === "urgent") this.#pacer.requestUrgent();
    else if (urgency === "paced") this.#pacer.requestPaced();
    this.#armStallWatchdog();
  }

  #publish(reason: PublicationReason): void {
    if (!this.#connected || this.#closed) return;
    try {
      const outbound = this.#outbound.snapshot();
      const publishData = reason === "retransmission" || this.#dataDirty;
      if (publishData) {
        const newFlight = !this.#flight.active;
        if (newFlight) {
          this.#currentFlightRetransmitted = false;
          this.#flightStartedAtMs = this.#clock.now();
          this.#flightRequestBytes = outbound.frames
            .filter(hostFrameCarriesRequest)
            .reduce((total, frame) => total + encodedFrameBytes(frame), 0);
        }
        if (reason === "retransmission") {
          this.#retransmittedBytes += outbound.encodedBytes;
        }
        this.#flight.published(outbound.frames);
        this.#delivery.published(outbound.frames, this.#clock.now());
        this.#publishLane("data", outbound.frames, reason);
        this.#dataDirty = false;
      }
      if (reason === "initial" || reason === "urgent" || !publishData) {
        this.#publishLane("control", [], reason);
      }
      const diagnostics = this.#hostTransportDiagnostics();
      this.#publishedHostDiagnostics = {
        revision: ++this.#hostDiagnosticsRevision,
        value: diagnostics,
      };
      this.#armRetransmission();
      this.#armStallWatchdog();
    } catch (cause) {
      if (this.#isReconnectableAwarenessFailure(cause)) {
        this.#currentFlightRetransmitted = true;
        this.#markAccelerationTrouble();
        this.#remoteCredits = 0;
        return;
      }
      this.#failProtocol(cause);
    }
  }

  #isReconnectableAwarenessFailure(value: unknown): boolean {
    if (!value || typeof value !== "object") return false;
    const error = value as { name?: unknown; kind?: unknown };
    return error.name === "AwarenessChannelError" &&
      (error.kind === "auth" || error.kind === "disconnected");
  }

  #publishLane(
    lane: AwarenessLane,
    frames: readonly ReliableFrame<HostFrame>[],
    reason: PublicationReason,
  ): void {
    const outbound = this.#outbound.snapshot();
    let recoveryActive = 0;
    if (this.#sidecarRecovery) {
      for (const state of this.#requests.values()) {
        if (this.#hasExecutionRisk(state)) recoveryActive++;
      }
    }
    const requiredOwned = Math.min(
      128,
      this.#committedRequestCount +
        this.#creditReservations +
        this.#ambiguousQuarantined,
    );
    const common = {
      protocol: "gp-awareness-v2",
      schemaHash: this.#binding.schemaHash,
      buildHash: this.#binding.buildHash,
      projectId: this.#binding.projectId,
      sessionId: this.#binding.sessionId,
      peerId: "host",
      instanceId: this.#hostInstanceId,
      leaderGeneration: this.#binding.leaderGeneration,
      epoch: outbound.epoch,
      revision: lane === "data" ? this.#dataRevision++ : this.#controlRevision++,
      ack: this.#processedInboundAck,
      // Congestion closes new local admission, but requests already owned by
      // this host must retain exactly enough sidecar admission. Publishing zero
      // can overtake their data-lane commit and deadlock its ACK; advertising
      // the full ceiling would instead erase congestion backpressure.
      credits: this.#sidecarRecovery
        ? 0
        : this.#congestion.congested
          ? requiredOwned
          : Math.max(this.#hostCeiling, requiredOwned),
      active: this.#sidecarRecovery
        ? Math.max(1, recoveryActive)
        : this.#committedRequestCount,
      queued: this.#unadmittedRequestCount +
        this.#preparations.size + this.#preparationWaiters.length,
      draining: this.#draining,
      frames,
      lane,
      ...(lane === "control" && this.#livenessProbe
        ? { liveness: {
            kind: "ping",
            sentAtMs: this.#livenessProbe.sentAtMs,
          } satisfies AwarenessLiveness }
        : {}),
    } as const;
    const envelope: AwarenessEnvelope<HostFrame> =
      this.#binding.protocolRevision === "r2"
        ? {
            ...common,
            protocolRevision: "r2",
            schemaVersion: GP_V2_R2_SCHEMA_VERSION,
            launchNonce: this.#binding.launchNonce,
            targetInstanceId: this.#expectedSidecarInstanceId!,
          }
        : {
            ...common,
            schemaVersion: GP_V2_R1_SCHEMA_VERSION,
          };
    this.#publications++;
    if (reason === "paced") this.#pacedPublications++;
    else if (reason === "urgent") this.#urgentPublications++;
    const encodedBytes = awarenessEnvelopeEncodedBytes(
      envelope,
      frames.length > 0 ? outbound.encodedBytes : 0,
    );
    this.#publishedBytes += encodedBytes;
    if (lane === "data") {
      this.#dataPublications++;
      this.#dataPublishedBytes += encodedBytes;
    } else {
      this.#controlPublications++;
      this.#controlPublishedBytes += encodedBytes;
    }
    if (lane === "data") {
      // R1 crosses its irreversible boundary at request_commit. R2 records its
      // later start-risk before request_start is even staged.
      for (const frame of frames) {
        if (frame.kind !== "request_commit") continue;
        const state = this.#requests.get(frame.requestId);
        if (state) {
          state.commitPublished = true;
          this.#reflectExecutionRisk(state);
        }
      }
    }
    this.#channel.publish(envelope, lane);
  }

  #hostTransportDiagnostics(): V2TransportDiagnostics {
    const window = this.#outbound.snapshot();
    const scheduler = this.#scheduler.snapshot();
    return {
      windowFrames: window.frames.length,
      windowBytes: window.encodedBytes,
      schedulerFrames: scheduler.queuedFrames,
      schedulerBytes: scheduler.queuedEncodedBytes,
      schedulerStreams: scheduler.streams,
      oldestQueuedFrameMs: scheduler.oldestQueuedFrameMs,
      ackProgressAgeMs: this.#delivery.ackProgressAgeMs(this.#clock.now()),
      retransmissionAttempt: this.#delivery.attempt,
      peakRetransmissionAttempt: this.#peakRetransmissionAttempt,
      retransmissionsTotal: this.#retransmissions,
      publicationsTotal: this.#publications,
      publishedBytesTotal: this.#publishedBytes,
      admittedUncommitted: this.#admittedUncommittedRequestCount,
      roundTripEwmaMs: this.#delivery.roundTripEwmaMs,
      roundTripVariationMs: this.#delivery.roundTripVariationMs,
      pacedPublicationsTotal: this.#pacedPublications,
      urgentPublicationsTotal: this.#urgentPublications,
      logicalEventsTotal: 0,
      responseBatchFramesTotal: 0,
      batchedEventsTotal: 0,
      dataPublicationsTotal: this.#dataPublications,
      controlPublicationsTotal: this.#controlPublications,
      dataPublishedBytesTotal: this.#dataPublishedBytes,
      controlPublishedBytesTotal: this.#controlPublishedBytes,
      flightCompletionsTotal: this.#flightCompletions,
      adaptiveCapacity: 0,
      capacityIncreasesTotal: 0,
      capacityReductionsTotal: 0,
      unadmittedTimeoutsTotal: this.#unadmittedTimeouts,
      acknowledgedGoodputBytesPerSecond: this.#currentGoodput(),
      uniqueAcknowledgedBytesTotal: this.#uniqueAcknowledgedBytes,
      retransmittedBytesTotal: this.#retransmittedBytes,
      stallEventsTotal: this.#stallEvents,
    };
  }

  #currentGoodput(): number | null {
    if (
      this.#acknowledgedGoodputBytesPerSecond === null ||
      this.#goodputUpdatedAtMs === null ||
      this.#clock.now() - this.#goodputUpdatedAtMs >= GOODPUT_IDLE_RESET_MS
    ) return null;
    return this.#acknowledgedGoodputBytesPerSecond;
  }

  #recordAcknowledgedFlight(now: number): boolean {
    const started = this.#flightStartedAtMs;
    const bytes = this.#flightRequestBytes;
    this.#flightStartedAtMs = null;
    this.#flightRequestBytes = 0;
    if (bytes > 0) this.#uniqueAcknowledgedBytes += bytes;
    if (started === null || bytes <= 0 || now <= started) return false;
    const sample = bytes * 1_000 / (now - started);
    this.#acknowledgedGoodputBytesPerSecond =
      this.#acknowledgedGoodputBytesPerSecond === null
        ? sample
        : this.#acknowledgedGoodputBytesPerSecond * 0.75 + sample * 0.25;
    this.#goodputUpdatedAtMs = now;
    return true;
  }

  #resetFlightMeasurement(): void {
    this.#flightStartedAtMs = null;
    this.#flightRequestBytes = 0;
    this.#currentFlightRetransmitted = false;
    this.#acceleratedFlightsHealthy = true;
    this.#cleanRequestFlightsSinceTrouble = 0;
    this.#lastFastRetransmitAtMs = null;
  }

  #markAccelerationTrouble(): void {
    this.#acceleratedFlightsHealthy = false;
    this.#cleanRequestFlightsSinceTrouble = 0;
  }

  #observeAccelerationFlight(requestBytes: number, retransmitted: boolean): void {
    this.#currentFlightRetransmitted = false;
    if (requestBytes <= 0) return;
    if (retransmitted) {
      this.#markAccelerationTrouble();
      return;
    }
    if (this.#acceleratedFlightsHealthy) return;
    this.#cleanRequestFlightsSinceTrouble++;
    if (this.#cleanRequestFlightsSinceTrouble < ACCELERATION_RECOVERY_CLEAN_FLIGHTS) return;
    this.#acceleratedFlightsHealthy = true;
    this.#cleanRequestFlightsSinceTrouble = 0;
  }

  #updateCongestion(): boolean {
    const before = this.#congestion.congested;
    const scheduled = this.#scheduler.snapshot();
    const outbound = this.#outbound.snapshot();
    const ackProgressAgeMs = this.#delivery.ackProgressAgeMs(this.#clock.now());
    this.#congestion.update({
      schedulerFrames: scheduled.queuedFrames,
      schedulerBytes: scheduled.queuedEncodedBytes,
      oldestQueuedFrameMs: scheduled.oldestQueuedFrameMs,
      ackProgressAgeMs,
      windowFrames: outbound.frames.length,
    });
    if (
      !before && this.#congestion.congested && outbound.frames.length > 0 &&
      ackProgressAgeMs >= this.#transportStallMs
    ) {
      this.#stallEvents++;
      this.#markAccelerationTrouble();
    }
    return before !== this.#congestion.congested;
  }

  #armStallWatchdog(): void {
    if (!this.#connected || this.#closed) return;
    const scheduled = this.#scheduler.snapshot();
    const outbound = this.#outbound.snapshot();
    if (scheduled.queuedFrames === 0 && outbound.frames.length === 0) {
      if (this.#stallTimer) clearTimeout(this.#stallTimer);
      this.#stallTimer = null;
      this.#stallTimerDueAtMs = null;
      if (this.#updateCongestion()) this.#notifyStateChange();
      return;
    }
    const now = this.#clock.now();
    const age = Math.max(
      scheduled.oldestQueuedFrameMs,
      outbound.frames.length > 0
        ? this.#delivery.ackProgressAgeMs(now)
        : 0,
    );
    const delay = this.#congestion.congested
      ? Math.max(100, Math.min(500, this.#transportStallMs / 2))
      : Math.max(1, this.#transportStallMs - age);
    const dueAt = now + delay;
    if (
      this.#stallTimer && this.#stallTimerDueAtMs !== null &&
      this.#stallTimerDueAtMs <= dueAt
    ) return;
    if (this.#stallTimer) clearTimeout(this.#stallTimer);
    this.#stallTimerDueAtMs = dueAt;
    this.#stallTimer = setTimeout(() => {
      this.#stallTimer = null;
      this.#stallTimerDueAtMs = null;
      if (this.#closed) return;
      if (this.#updateCongestion()) {
        this.#notifyStateChange();
        this.#pacer.requestUrgent();
      }
      this.#armStallWatchdog();
    }, delay);
    this.#stallTimer.unref?.();
  }

  #maybeFastRetransmit(now: number): boolean {
    if (!this.#flight.active || this.#outbound.snapshot().frames.length === 0) return false;
    const rtt = this.#delivery.roundTripEwmaMs;
    const threshold = Math.max(
      this.#fastRetransmitFloorMs,
      (rtt ?? this.#fastRetransmitFloorMs) * 1.5,
    );
    if (this.#delivery.ackProgressAgeMs(now) < threshold) return false;
    if (
      this.#lastFastRetransmitAtMs !== null &&
      now - this.#lastFastRetransmitAtMs < Math.max(
        this.#fastRetransmitFloorMs,
        (rtt ?? this.#fastRetransmitFloorMs) * 2,
      )
    ) return false;
    this.#lastFastRetransmitAtMs = now;
    this.#currentFlightRetransmitted = true;
    this.#markAccelerationTrouble();
    this.#delivery.retransmitted(now);
    this.#retransmissions++;
    this.#peakRetransmissionAttempt = Math.max(
      this.#peakRetransmissionAttempt,
      this.#delivery.attempt,
    );
    this.#pacer.flushNow("retransmission");
    return true;
  }

  #armRetransmission(): void {
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = null;
    if (
      !this.#connected ||
      this.#closed ||
      this.#outbound.snapshot().frames.length === 0
    ) return;
    const revision = this.#dataRevision + this.#controlRevision;
    const jitterSeed = deterministicV2JitterSeed(this.#hostInstanceId, revision);
    const delay = this.#delivery.nextDelayMs(this.#clock.now(), jitterSeed);
    if (delay === null) return;
    this.#timer = setTimeout(() => {
      this.#timer = null;
      if (!this.#connected || this.#closed) return;
      if (this.#outbound.snapshot().frames.length === 0) return;
      this.#currentFlightRetransmitted = true;
      this.#markAccelerationTrouble();
      this.#delivery.retransmitted(this.#clock.now());
      this.#retransmissions++;
      this.#peakRetransmissionAttempt = Math.max(
        this.#peakRetransmissionAttempt,
        this.#delivery.attempt,
      );
      this.#pacer.flushNow("retransmission");
    }, delay);
  }

  #releasePartialResponseState(state: RequestState): void {
    for (const assembly of state.assemblies.values()) assembly.permit?.release();
    state.assemblies.clear();
    for (const completed of state.completedEvents.values()) completed.permit?.release();
    state.completedEvents.clear();
  }

  #trackRequestState(state: RequestState): void {
    if (state.committed) this.#committedRequestCount++;
    else if (state.admitted) this.#admittedUncommittedRequestCount++;
    else this.#unadmittedRequestCount++;
  }

  #untrackRequestState(state: RequestState): void {
    if (state.committed) this.#committedRequestCount--;
    else if (state.admitted) this.#admittedUncommittedRequestCount--;
    else this.#unadmittedRequestCount--;
  }

  #markRequestAdmitted(state: RequestState): void {
    if (state.admitted) return;
    state.admitted = true;
    this.#unadmittedRequestCount--;
    this.#admittedUncommittedRequestCount++;
  }

  #markRequestCommitted(state: RequestState): void {
    if (state.committed) return;
    if (state.admitted) this.#admittedUncommittedRequestCount--;
    else this.#unadmittedRequestCount--;
    state.committed = true;
    this.#committedRequestCount++;
  }

  #detachStateForRecovery(state: RequestState): void {
    state.diagnostics.freezePeak();
    state.cancelledTerminalPermit?.release();
    state.cancelledTerminalPermit = null;
    releaseAtomicBytePermit(state.terminalFinalizationAnchor);
    state.terminalFinalizationAnchor = null;
    this.#releasePreparedPayload(state);
    this.#discardPreparedStart(state);
    this.#releaseStartReservation(state);
    this.#stopTrackingLatencyCriticalPreCommit(state);
    this.#stopTrackingLatencyCriticalRequest(state);
    this.#untrackRequestState(state);
    this.#requests.delete(state.requestId);
    const pendingIndex = this.#pending.indexOf(state);
    if (pendingIndex >= 0) this.#pending.splice(pendingIndex, 1);
    this.#scheduler.removeStream(state.requestId);
    state.signal?.removeEventListener("abort", state.onAbort!);
    this.#clearAdmissionProgress(state);
    if (state.admissionTimer) clearTimeout(state.admissionTimer);
    state.admissionTimer = undefined;
    if (this.#requests.size === 0) {
      for (const resolve of this.#idleWaiters) resolve();
      this.#idleWaiters.clear();
    }
  }

  #maybeCompleteSidecarRecovery(): void {
    const recovery = this.#sidecarRecovery;
    if (!recovery || !recovery.markerSeen) return;
    for (const state of [...this.#requests.values()]) {
      if (!this.#finishRecoveryPrefixIfReady(state, true)) continue;
      if (!this.#requests.has(state.requestId)) continue;
      this.#failCommitted(state, new V2CommittedError(
        "Recovered sidecar has no durable outcome for the retained request",
        "sidecar_recovery_unresolved",
        true,
      ));
    }
    for (const [requestId, receipt] of this.#terminalReceipts) {
      if (!receipt.replayed) this.#terminalReceipts.delete(requestId);
    }
    if (this.#requests.size > 0 || this.#terminalReceipts.size > 0) return;
    this.#sidecarRecovery = null;
    recovery.resolve();
    this.#pacer.requestUrgent();
  }

  #abortRequest(state: RequestState): void {
    if (!this.#requests.has(state.requestId)) return;
    this.#scheduler.removeStream(state.requestId);
    if (
      this.#hasExecutionRisk(state) ||
      (this.#binding.protocolRevision === "r1" && state.commitStaged)
    ) {
      const acknowledged = state.committed;
      // A staged commit cannot be retracted from the cumulative window. The
      // already-scheduled publication may still send it after this callback.
      if (this.#binding.protocolRevision === "r1" && state.commitStaged) {
        state.commitPublished = true;
        this.#reflectExecutionRisk(state);
      }
      state.cancelled = true;
      if (!state.quarantineKind) {
        state.quarantineKind = "cancellation";
        this.#quarantined++;
      }
      const error = new V2CommittedError(
        "V2 request was cancelled after commitment",
        "cancelled",
        true,
      );
      if (!acknowledged) state.rejectRun(error);
      state.handle.cancel(error);
      this.#enqueueCancellation(state.requestId, "client aborted");
    } else {
      const published = state.published;
      const error = preCommitCancellationError();
      this.#removeState(state);
      state.rejectRun(error);
      state.handle.cancel(error);
      if (published) {
        this.#enqueueCancellation(state.requestId, "client aborted");
      }
    }
    this.#requestPublication(this.#pump());
  }

  #enqueueCancellation(requestId: string, reason: string): void {
    try {
      this.#scheduler.enqueue("control", requestId, {
        kind: "request_cancel",
        requestId,
        reason,
      });
    } catch (cause) {
      // A partial request without its cancellation would retain sidecar
      // assembly state while this session falsely remained healthy.
      this.#failProtocol(cause);
    }
  }

  #failPreCommit(state: RequestState, error: Error): void {
    if (!this.#requests.has(state.requestId)) return;
    this.#removeState(state);
    state.rejectRun(error);
    state.handle.fail(error);
  }

  #failCommitted(state: RequestState, error: V2CommittedError): void {
    if (!this.#requests.has(state.requestId)) return;
    const acknowledged = state.committed;
    if (error.ambiguous) {
      this.#ambiguityTombstones.set(state.requestId, state.requestDigest);
      if (!state.quarantineKind) {
        state.quarantineKind = "ambiguity";
        this.#quarantined++;
        this.#ambiguousQuarantined++;
      } else if (state.quarantineKind === "cancellation") {
        state.quarantineKind = "ambiguity";
        this.#ambiguousQuarantined++;
      }
    } else {
      // A durable definitive outcome can win a cancellation race before the
      // fetch-started ACK. Release both the cancellation quarantine and the
      // still-charged admission reservation exactly once.
      if (state.quarantineKind === "cancellation") {
        state.quarantineKind = null;
        this.#quarantined = Math.max(0, this.#quarantined - 1);
      }
      if (state.admitted && !state.committed) {
        this.#creditReservations = Math.max(0, this.#creditReservations - 1);
        this.#transferReservations = Math.max(
          0,
          this.#transferReservations - state.transferCost,
        );
      }
    }
    this.#stopTrackingLatencyCriticalPreCommit(state);
    // commitPublished is deliberately promoted to committed-risk here so
    // removal cannot release capacity for work that may still be executing.
    this.#markRequestCommitted(state);
    this.#removeState(state);
    if (!acknowledged) state.rejectRun(error);
    state.handle.fail(error);
  }

  #removeState(state: RequestState): void {
    state.diagnostics.freezePeak();
    state.cancelledTerminalPermit?.release();
    state.cancelledTerminalPermit = null;
    releaseAtomicBytePermit(state.terminalFinalizationAnchor);
    state.terminalFinalizationAnchor = null;
    this.#releasePreparedPayload(state);
    this.#discardPreparedStart(state);
    this.#releaseStartReservation(state);
    this.#stopTrackingLatencyCriticalPreCommit(state);
    this.#stopTrackingLatencyCriticalRequest(state);
    this.#untrackRequestState(state);
    this.#requests.delete(state.requestId);
    state.preparedRequest = null;
    state.recoveryPrefix = undefined;
    for (const assembly of state.assemblies.values()) assembly.permit?.release();
    state.assemblies.clear();
    for (const completed of state.completedEvents.values()) completed.permit?.release();
    state.completedEvents.clear();
    const pendingIndex = this.#pending.indexOf(state);
    if (pendingIndex >= 0) this.#pending.splice(pendingIndex, 1);
    this.#scheduler.removeStream(state.requestId);
    if (state.admitted && !state.committed) {
      this.#creditReservations = Math.max(0, this.#creditReservations - 1);
      this.#transferReservations = Math.max(
        0,
        this.#transferReservations - state.transferCost,
      );
    }
    state.signal?.removeEventListener("abort", state.onAbort!);
    state.onAdmissionReflected = undefined;
    this.#clearAdmissionProgress(state);
    state.onExecutionRiskReflected = undefined;
    if (state.admissionTimer) clearTimeout(state.admissionTimer);
    state.admissionTimer = undefined;
    if (this.#requests.size === 0) {
      for (const resolve of this.#idleWaiters) resolve();
      this.#idleWaiters.clear();
    }
    this.#admitPending();
  }

  #reflectAdmissionProgress(
    state: RequestState,
    progress: SandboxAdmissionProgress,
  ): void {
    const onAdmissionProgress = state.onAdmissionProgress;
    // Progress is already proven in authoritative protocol state. Observers
    // are advisory and can neither roll it back nor poison frame processing.
    try { onAdmissionProgress?.(progress); } catch {}
  }

  #clearAdmissionProgress(state: RequestState): void {
    if (!state.onAdmissionProgress) return;
    state.onAdmissionProgress = undefined;
    this.#admissionProgressObservers = Math.max(0, this.#admissionProgressObservers - 1);
  }

  #stopTrackingLatencyCriticalPreCommit(state: RequestState): void {
    if (!state.latencyCriticalPreCommitTracked) return;
    state.latencyCriticalPreCommitTracked = false;
    this.#latencyCriticalPreCommitTransfers = Math.max(
      0,
      this.#latencyCriticalPreCommitTransfers - 1,
    );
  }

  #stopTrackingLatencyCriticalRequest(state: RequestState): void {
    if (!state.latencyCriticalRequestTracked) return;
    state.latencyCriticalRequestTracked = false;
    this.#latencyCriticalRequests = Math.max(0, this.#latencyCriticalRequests - 1);
  }

  #failProtocol(cause: unknown): void {
    if (this.#protocolFailed || this.#teardownPromise) return;
    const message = cause instanceof Error ? cause.message : "V2 transport protocol failed";
    this.#protocolFailed = true;
    void this.#beginTeardown(
      new V2PreCommitError(message, "protocol"),
      new V2CommittedError(message, "protocol", true),
    ).catch(() => undefined);
  }
}
