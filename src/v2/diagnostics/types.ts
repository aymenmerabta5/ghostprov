const MAX_DIAGNOSTIC_BYTES = 2_048;
const FORBIDDEN_KEY = /(?:prompt|instructions|output|content|arguments|headers?|cookie|authorization|token|secret|encrypted|body|url|client|stream_id)/i;
const encoder = new TextEncoder();

export interface V2SidecarResourceSnapshot {
  processRssBytes: number;
  heapUsedBytes: number;
  heapTotalBytes: number;
  externalBytes: number;
  /** V8 ArrayBuffer backing stores when the runtime exposes them separately. */
  arrayBufferBytes: number | null;
  systemTotalBytes: number | null;
  systemFreeBytes: number | null;
  processCpuPercent: number | null;
  normalizedLoad1: number | null;
  eventLoopSamples: number;
  eventLoopP50Ms: number;
  eventLoopP95Ms: number;
  eventLoopMaxMs: number;
  /** Rolling bounded GC observations; zero when this runtime cannot expose GC entries. */
  gcSamples: number;
  gcDurationP95Ms: number;
  gcDurationMaxMs: number;
}

export interface V2TransportDiagnostics {
  windowFrames: number;
  windowBytes: number;
  schedulerFrames: number;
  schedulerBytes: number;
  schedulerStreams: number;
  oldestQueuedFrameMs: number;
  ackProgressAgeMs: number;
  retransmissionAttempt: number;
  peakRetransmissionAttempt: number;
  retransmissionsTotal: number;
  publicationsTotal: number;
  publishedBytesTotal: number;
  admittedUncommitted: number;
  roundTripEwmaMs: number | null;
  /** RFC-style RTT variation used by the bounded retransmission clock. */
  roundTripVariationMs?: number | null;
  pacedPublicationsTotal: number;
  urgentPublicationsTotal: number;
  logicalEventsTotal: number;
  responseBatchFramesTotal: number;
  batchedEventsTotal: number;
  dataPublicationsTotal: number;
  controlPublicationsTotal: number;
  dataPublishedBytesTotal: number;
  controlPublishedBytesTotal: number;
  flightCompletionsTotal: number;
  adaptiveCapacity: number;
  capacityIncreasesTotal: number;
  capacityReductionsTotal: number;
  unadmittedTimeoutsTotal: number;
  /** Effective unique directional data bytes acknowledged per second, excluding replay. */
  acknowledgedGoodputBytesPerSecond?: number | null;
  /** Unique directional data bytes cumulatively acknowledged by the peer. */
  uniqueAcknowledgedBytesTotal?: number;
  /** Bytes republished specifically because a reliable flight timed out. */
  retransmittedBytesTotal?: number;
  /** Number of transitions into a no-progress transport stall. */
  stallEventsTotal?: number;
}

export interface V2EnvelopeDiagnostics {
  version: 1;
  resource: V2SidecarResourceSnapshot | null;
  transport: V2TransportDiagnostics;
  malformedCount: number;
}

export interface V2RequestAckDiagnostics {
  firstFrameToCommitMs: number;
  commitToAssemblyMs: number;
  claimSyncMs: number;
  admissionWaitMs: number;
  commitProcessingMs: number;
  preparedToFetchStartMs?: number;
  fetchStartSyncMs?: number;
}

export interface V2FirstEventDiagnostics {
  upstreamFirstEventMs: number;
  schedulerWaitMs: number;
}

export interface V2TerminalDiagnostics {
  upstreamTotalMs: number;
  schedulerPeakFrames: number;
  schedulerPeakBytes: number;
  ackProgressAgePeakMs: number;
}

function parseDurations<T extends object>(value: unknown, keys: readonly string[]): T | null {
  if (encodedDiagnosticBytes(value) > MAX_DIAGNOSTIC_BYTES) return null;
  const raw = object(value);
  if (!raw || !exact(raw, keys)) return null;
  const entries = keys.map(key => [key, number(raw[key])] as const);
  if (entries.some(([, value]) => value === null)) return null;
  return Object.fromEntries(entries) as T;
}

export function parseRequestAckDiagnostics(value: unknown): V2RequestAckDiagnostics | null {
  const raw = object(value);
  const required = [
    "firstFrameToCommitMs", "commitToAssemblyMs", "claimSyncMs",
    "admissionWaitMs", "commitProcessingMs",
  ] as const;
  const optional = ["preparedToFetchStartMs", "fetchStartSyncMs"] as const;
  if (!raw || encodedDiagnosticBytes(value) > MAX_DIAGNOSTIC_BYTES ||
      !exactWithOptional(raw, required, optional)) return null;
  const parsed = Object.fromEntries([...required, ...optional]
    .filter(key => raw[key] !== undefined)
    .map(key => [key, number(raw[key])]));
  if (Object.values(parsed).some(item => item === null)) return null;
  return parsed as unknown as V2RequestAckDiagnostics;
}
export const parseFirstEventDiagnostics = (value: unknown): V2FirstEventDiagnostics | null => parseDurations<V2FirstEventDiagnostics>(value, ["upstreamFirstEventMs", "schedulerWaitMs"]);
export const parseTerminalDiagnostics = (value: unknown): V2TerminalDiagnostics | null => parseDurations<V2TerminalDiagnostics>(value, ["upstreamTotalMs", "schedulerPeakFrames", "schedulerPeakBytes", "ackProgressAgePeakMs"]);

function object(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function exact(value: Record<string, unknown>, keys: readonly string[]): boolean {
  const allowed = new Set(keys);
  return Object.keys(value).every(key => allowed.has(key) && !FORBIDDEN_KEY.test(key)) &&
    keys.every(key => key in value);
}

function exactWithOptional(
  value: Record<string, unknown>,
  required: readonly string[],
  optional: readonly string[],
): boolean {
  const allowed = new Set([...required, ...optional]);
  return Object.keys(value).every(key => allowed.has(key) && !FORBIDDEN_KEY.test(key)) &&
    required.every(key => key in value);
}

function number(value: unknown, integer = false): number | null {
  return typeof value === "number" && Number.isFinite(value) && value >= 0 &&
      (!integer || Number.isSafeInteger(value))
    ? value
    : null;
}

function nullableNumber(value: unknown): number | null | undefined {
  if (value === null) return null;
  const parsed = number(value);
  return parsed === null ? undefined : parsed;
}

function parseResource(value: unknown): V2SidecarResourceSnapshot | null {
  const raw = object(value);
  const requiredKeys = [
    "processRssBytes", "heapUsedBytes", "heapTotalBytes", "externalBytes",
    "systemTotalBytes", "systemFreeBytes", "processCpuPercent", "normalizedLoad1",
    "eventLoopP50Ms", "eventLoopP95Ms", "eventLoopMaxMs",
  ] as const;
  // Resource diagnostics are intentionally extensible within envelope v1.
  // Accept old sidecars without the optional metrics while rejecting unknown
  // fields, then normalize absent values to explicit null/zero evidence.
  const optionalKeys = [
    "arrayBufferBytes", "eventLoopSamples", "gcSamples",
    "gcDurationP95Ms", "gcDurationMaxMs",
  ] as const;
  if (!raw || !exactWithOptional(raw, requiredKeys, optionalKeys)) return null;
  const systemTotalBytes = nullableNumber(raw.systemTotalBytes);
  const systemFreeBytes = nullableNumber(raw.systemFreeBytes);
  const processCpuPercent = nullableNumber(raw.processCpuPercent);
  const normalizedLoad1 = nullableNumber(raw.normalizedLoad1);
  const arrayBufferBytes = raw.arrayBufferBytes === undefined
    ? null
    : nullableNumber(raw.arrayBufferBytes);
  if (
    [systemTotalBytes, systemFreeBytes, processCpuPercent, normalizedLoad1, arrayBufferBytes]
      .includes(undefined)
  ) return null;
  const numericKeys = [
    "processRssBytes", "heapUsedBytes", "heapTotalBytes", "externalBytes",
    "eventLoopP50Ms", "eventLoopP95Ms", "eventLoopMaxMs",
  ] as const;
  const required = numericKeys.map(key => number(raw[key]));
  if (required.some(value => value === null)) return null;
  const eventLoopSamples = raw.eventLoopSamples === undefined
    ? 0
    : number(raw.eventLoopSamples, true);
  const gcSamples = raw.gcSamples === undefined ? 0 : number(raw.gcSamples, true);
  const gcDurationP95Ms = raw.gcDurationP95Ms === undefined
    ? 0
    : number(raw.gcDurationP95Ms);
  const gcDurationMaxMs = raw.gcDurationMaxMs === undefined
    ? 0
    : number(raw.gcDurationMaxMs);
  if (
    eventLoopSamples === null || gcSamples === null ||
    gcDurationP95Ms === null || gcDurationMaxMs === null
  ) return null;
  return {
    processRssBytes: required[0]!, heapUsedBytes: required[1]!,
    heapTotalBytes: required[2]!, externalBytes: required[3]!,
    arrayBufferBytes: arrayBufferBytes!,
    systemTotalBytes: systemTotalBytes!, systemFreeBytes: systemFreeBytes!,
    processCpuPercent: processCpuPercent!, normalizedLoad1: normalizedLoad1!,
    eventLoopSamples,
    eventLoopP50Ms: required[4]!, eventLoopP95Ms: required[5]!, eventLoopMaxMs: required[6]!,
    gcSamples, gcDurationP95Ms, gcDurationMaxMs,
  };
}

function parseTransport(value: unknown): V2TransportDiagnostics | null {
  const raw = object(value);
  const requiredKeys = [
    "windowFrames", "windowBytes", "schedulerFrames", "schedulerBytes", "schedulerStreams",
    "oldestQueuedFrameMs", "ackProgressAgeMs", "retransmissionAttempt",
    "peakRetransmissionAttempt", "retransmissionsTotal", "publicationsTotal",
    "publishedBytesTotal", "admittedUncommitted", "roundTripEwmaMs",
    "pacedPublicationsTotal", "urgentPublicationsTotal", "logicalEventsTotal",
    "responseBatchFramesTotal", "batchedEventsTotal",
    "dataPublicationsTotal", "controlPublicationsTotal", "dataPublishedBytesTotal",
    "controlPublishedBytesTotal", "flightCompletionsTotal", "adaptiveCapacity",
    "capacityIncreasesTotal", "capacityReductionsTotal", "unadmittedTimeoutsTotal",
  ] as const;
  const optionalKeys = [
    "roundTripVariationMs", "acknowledgedGoodputBytesPerSecond",
    "uniqueAcknowledgedBytesTotal", "retransmittedBytesTotal", "stallEventsTotal",
  ] as const;
  if (!raw || !exactWithOptional(raw, requiredKeys, optionalKeys)) return null;
  const roundTripEwmaMs = nullableNumber(raw.roundTripEwmaMs);
  if (roundTripEwmaMs === undefined) return null;
  const counters = new Set([
    "windowFrames", "windowBytes", "schedulerFrames", "schedulerBytes", "schedulerStreams",
    "retransmissionAttempt", "peakRetransmissionAttempt", "retransmissionsTotal",
    "publicationsTotal", "publishedBytesTotal", "admittedUncommitted",
    "pacedPublicationsTotal", "urgentPublicationsTotal", "logicalEventsTotal",
    "responseBatchFramesTotal", "batchedEventsTotal",
    "dataPublicationsTotal", "controlPublicationsTotal", "dataPublishedBytesTotal",
    "controlPublishedBytesTotal", "flightCompletionsTotal", "adaptiveCapacity",
    "capacityIncreasesTotal", "capacityReductionsTotal", "unadmittedTimeoutsTotal",
  ]);
  const parsed = Object.fromEntries(requiredKeys
    .filter(key => key !== "roundTripEwmaMs")
    .map(key => [key, number(raw[key], counters.has(key))]));
  if (Object.values(parsed).some(value => value === null)) return null;
  const roundTripVariationMs = raw.roundTripVariationMs === undefined
    ? undefined
    : nullableNumber(raw.roundTripVariationMs);
  const acknowledgedGoodputBytesPerSecond =
    raw.acknowledgedGoodputBytesPerSecond === undefined
      ? undefined
      : nullableNumber(raw.acknowledgedGoodputBytesPerSecond);
  const optionalCounters = Object.fromEntries(
    ["uniqueAcknowledgedBytesTotal", "retransmittedBytesTotal", "stallEventsTotal"]
      .filter(key => raw[key] !== undefined)
      .map(key => [key, number(raw[key], true)]),
  );
  if (
    roundTripVariationMs === undefined && raw.roundTripVariationMs !== undefined ||
    acknowledgedGoodputBytesPerSecond === undefined &&
      raw.acknowledgedGoodputBytesPerSecond !== undefined ||
    Object.values(optionalCounters).some(value => value === null)
  ) return null;
  return {
    ...(parsed as unknown as Omit<V2TransportDiagnostics, "roundTripEwmaMs">),
    roundTripEwmaMs,
    ...(raw.roundTripVariationMs === undefined ? {} : { roundTripVariationMs }),
    ...(raw.acknowledgedGoodputBytesPerSecond === undefined
      ? {}
      : { acknowledgedGoodputBytesPerSecond }),
    ...(optionalCounters as Partial<V2TransportDiagnostics>),
  };
}

export function encodedDiagnosticBytes(value: unknown): number {
  try { return encoder.encode(JSON.stringify(value)).byteLength; } catch { return Number.POSITIVE_INFINITY; }
}

export function parseDiagnosticObject(value: unknown): V2EnvelopeDiagnostics | null {
  if (encodedDiagnosticBytes(value) > MAX_DIAGNOSTIC_BYTES) return null;
  const raw = object(value);
  if (!raw || !exact(raw, ["version", "resource", "transport", "malformedCount"])) return null;
  if (raw.version !== 1) return null;
  const resource = raw.resource === null ? null : parseResource(raw.resource);
  const transport = parseTransport(raw.transport);
  const malformedCount = number(raw.malformedCount, true);
  if ((raw.resource !== null && !resource) || !transport || malformedCount === null) return null;
  return { version: 1, resource, transport, malformedCount };
}
