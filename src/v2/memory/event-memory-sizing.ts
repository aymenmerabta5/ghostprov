import {
  ATOMIC_EVENT_FIXED_OVERHEAD_BYTES,
  ATOMIC_EVENT_MEMORY_MULTIPLIER,
  estimateAtomicEventPermitBytes,
} from "./atomic-byte-permits";

const KiB = 1024;

/** The normal per-handle event queue remains deliberately small. */
export const V2_EVENT_SOFT_BUFFER_BYTES = 512 * KiB;
/** Conservative peak: decoded parts + concat + a worst-case UTF-16 string. */
export const V2_EVENT_PHYSICAL_FACTOR = ATOMIC_EVENT_MEMORY_MULTIPLIER;
/** Fixed scratch/metadata allowance reserved only by the single jumbo lane. */
export const V2_EVENT_JUMBO_OVERHEAD_BYTES = ATOMIC_EVENT_FIXED_OVERHEAD_BYTES;
/** Conservative retained JS string/object/output copies after assembly ends. */
export const V2_EVENT_RETAINED_FACTOR = 4;
/** Per buffered event references, queue entry, carrier, and allocator metadata. */
export const V2_EVENT_RETAINED_OVERHEAD_BYTES = 4 * KiB;
/** A byte bound alone does not constrain arbitrarily many empty/tiny events. */
export const V2_EVENT_MAX_BUFFERED_EVENTS = 128;

function checkedNonNegative(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new RangeError(`${name} must be a non-negative safe integer`);
  }
  return value;
}

export function checkedMemoryMultiply(
  left: number,
  right: number,
  name = "memory product",
): number {
  checkedNonNegative(left, `${name} left operand`);
  checkedNonNegative(right, `${name} right operand`);
  const result = left * right;
  if (!Number.isSafeInteger(result)) throw new RangeError(`${name} exceeds safe integer capacity`);
  return result;
}

export function checkedMemoryAdd(
  left: number,
  right: number,
  name = "memory sum",
): number {
  checkedNonNegative(left, `${name} left operand`);
  checkedNonNegative(right, `${name} right operand`);
  const result = left + right;
  if (!Number.isSafeInteger(result)) throw new RangeError(`${name} exceeds safe integer capacity`);
  return result;
}

/** Exact permit coverage required before materializing one host SSE event. */
export function v2EventPhysicalWorkingSetBytes(
  wireBytes: number,
  _softBufferBytes = V2_EVENT_SOFT_BUFFER_BYTES,
): number {
  checkedNonNegative(wireBytes, "wireBytes");
  checkedNonNegative(_softBufferBytes, "softBufferBytes");
  return estimateAtomicEventPermitBytes(wireBytes);
}

/** Permit charge after decoded/concat/hash scratch has been released. */
export function v2RetainedEventPermitBytes(wireBytes: number): number {
  checkedNonNegative(wireBytes, "wireBytes");
  return checkedMemoryAdd(
    checkedMemoryMultiply(wireBytes, V2_EVENT_RETAINED_FACTOR, "retained event bytes"),
    V2_EVENT_RETAINED_OVERHEAD_BYTES,
    "retained event permit",
  );
}

export interface V2HostEventMemorySizing {
  softBufferBytes: number;
  physicalFactor: typeof V2_EVENT_PHYSICAL_FACTOR;
  jumboOverheadBytes: number;
  retainedFactor: typeof V2_EVENT_RETAINED_FACTOR;
  retainedOverheadBytes: number;
  maxBufferedEvents: number;
  ordinaryBytes: number;
  jumboBytes: number;
  totalBytes: number;
}

export interface V2SidecarEventMemorySizing {
  ordinaryBytes: number;
  jumboBytes: number;
  totalBytes: number;
}

/** One sidecar-global pool: fixed legal jumbo lane plus the ordinary remainder. */
export function deriveV2SidecarEventMemorySizing(input: {
  totalBytes: number;
  responseMaxBytes: number;
}): V2SidecarEventMemorySizing {
  checkedNonNegative(input.totalBytes, "sidecar totalBytes");
  checkedNonNegative(input.responseMaxBytes, "sidecar responseMaxBytes");
  const jumboBytes = estimateAtomicEventPermitBytes(input.responseMaxBytes);
  const ordinaryBytes = input.totalBytes - jumboBytes;
  if (!Number.isSafeInteger(ordinaryBytes) || ordinaryBytes < 1) {
    throw new RangeError("Sidecar memory budget must exceed one legal jumbo event reservation");
  }
  return Object.freeze({ ordinaryBytes, jumboBytes, totalBytes: input.totalBytes });
}

/**
 * One ordinary soft-buffer working set per configured transport-active slot,
 * plus one process-wide jumbo lane able to materialize the largest legal event.
 */
export function deriveV2HostEventMemorySizing(input: {
  responseMaxBytes: number;
  runtimes: number;
  maxActivePerRuntime: number;
  softBufferBytes?: number;
}): V2HostEventMemorySizing {
  checkedNonNegative(input.responseMaxBytes, "responseMaxBytes");
  checkedNonNegative(input.runtimes, "runtimes");
  checkedNonNegative(input.maxActivePerRuntime, "maxActivePerRuntime");
  const softBufferBytes = input.softBufferBytes ?? V2_EVENT_SOFT_BUFFER_BYTES;
  checkedNonNegative(softBufferBytes, "softBufferBytes");
  if (
    input.responseMaxBytes < 1 ||
    input.runtimes < 1 ||
    input.maxActivePerRuntime < 1 ||
    softBufferBytes < 1
  ) throw new RangeError("V2 host event memory sizing inputs must be positive");

  const assemblyPerActive = estimateAtomicEventPermitBytes(softBufferBytes);
  const retainedPayloadPerActive = checkedMemoryMultiply(
    softBufferBytes,
    V2_EVENT_RETAINED_FACTOR,
    "V2 retained buffered payload",
  );
  const retainedMetadataPerActive = checkedMemoryMultiply(
    V2_EVENT_MAX_BUFFERED_EVENTS,
    V2_EVENT_RETAINED_OVERHEAD_BYTES,
    "V2 retained buffered metadata",
  );
  const ordinaryPerActive = checkedMemoryAdd(
    assemblyPerActive,
    checkedMemoryAdd(
      retainedPayloadPerActive,
      retainedMetadataPerActive,
      "V2 retained event queue",
    ),
    "V2 assembly and retained overlap",
  );
  const activeSlots = checkedMemoryMultiply(
    input.runtimes,
    input.maxActivePerRuntime,
    "V2 configured active slots",
  );
  const ordinaryBytes = checkedMemoryMultiply(
    ordinaryPerActive,
    activeSlots,
    "V2 ordinary event pool",
  );
  const jumboBytes = estimateAtomicEventPermitBytes(input.responseMaxBytes);
  return Object.freeze({
    softBufferBytes,
    physicalFactor: V2_EVENT_PHYSICAL_FACTOR,
    jumboOverheadBytes: V2_EVENT_JUMBO_OVERHEAD_BYTES,
    retainedFactor: V2_EVENT_RETAINED_FACTOR,
    retainedOverheadBytes: V2_EVENT_RETAINED_OVERHEAD_BYTES,
    maxBufferedEvents: V2_EVENT_MAX_BUFFERED_EVENTS,
    ordinaryBytes,
    jumboBytes,
    totalBytes: checkedMemoryAdd(ordinaryBytes, jumboBytes, "V2 host event pool"),
  });
}
