const MAX_DIAGNOSTIC_BYTES = 2_048;
const FORBIDDEN_KEY = /(?:prompt|instructions|output|content|arguments|headers?|cookie|authorization|token|secret|encrypted|body|url|client|stream_id)/i;
const encoder = new TextEncoder();

export interface V2SidecarResourceSnapshot {
  processRssBytes: number;
  heapUsedBytes: number;
  heapTotalBytes: number;
  externalBytes: number;
  systemTotalBytes: number | null;
  systemFreeBytes: number | null;
  processCpuPercent: number | null;
  normalizedLoad1: number | null;
  eventLoopP50Ms: number;
  eventLoopP95Ms: number;
  eventLoopMaxMs: number;
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
  pacedPublicationsTotal: number;
  urgentPublicationsTotal: number;
  logicalEventsTotal: number;
  responseBatchFramesTotal: number;
  batchedEventsTotal: number;
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

export const parseRequestAckDiagnostics = (value: unknown): V2RequestAckDiagnostics | null => parseDurations<V2RequestAckDiagnostics>(value, ["firstFrameToCommitMs", "commitToAssemblyMs", "claimSyncMs", "admissionWaitMs", "commitProcessingMs"]);
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
  const keys = [
    "processRssBytes", "heapUsedBytes", "heapTotalBytes", "externalBytes",
    "systemTotalBytes", "systemFreeBytes", "processCpuPercent", "normalizedLoad1",
    "eventLoopP50Ms", "eventLoopP95Ms", "eventLoopMaxMs",
  ] as const;
  if (!raw || !exact(raw, keys)) return null;
  const systemTotalBytes = nullableNumber(raw.systemTotalBytes);
  const systemFreeBytes = nullableNumber(raw.systemFreeBytes);
  const processCpuPercent = nullableNumber(raw.processCpuPercent);
  const normalizedLoad1 = nullableNumber(raw.normalizedLoad1);
  if ([systemTotalBytes, systemFreeBytes, processCpuPercent, normalizedLoad1].includes(undefined)) return null;
  const required = keys.slice(0, 4).concat(keys.slice(8)).map(key => number(raw[key]));
  if (required.some(value => value === null)) return null;
  return {
    processRssBytes: required[0]!, heapUsedBytes: required[1]!,
    heapTotalBytes: required[2]!, externalBytes: required[3]!,
    systemTotalBytes: systemTotalBytes!, systemFreeBytes: systemFreeBytes!,
    processCpuPercent: processCpuPercent!, normalizedLoad1: normalizedLoad1!,
    eventLoopP50Ms: required[4]!, eventLoopP95Ms: required[5]!, eventLoopMaxMs: required[6]!,
  };
}

function parseTransport(value: unknown): V2TransportDiagnostics | null {
  const raw = object(value);
  const keys = [
    "windowFrames", "windowBytes", "schedulerFrames", "schedulerBytes", "schedulerStreams",
    "oldestQueuedFrameMs", "ackProgressAgeMs", "retransmissionAttempt",
    "peakRetransmissionAttempt", "retransmissionsTotal", "publicationsTotal",
    "publishedBytesTotal", "admittedUncommitted", "roundTripEwmaMs",
    "pacedPublicationsTotal", "urgentPublicationsTotal", "logicalEventsTotal",
    "responseBatchFramesTotal", "batchedEventsTotal",
  ] as const;
  if (!raw || !exact(raw, keys)) return null;
  const roundTripEwmaMs = nullableNumber(raw.roundTripEwmaMs);
  if (roundTripEwmaMs === undefined) return null;
  const counters = new Set([
    "windowFrames", "windowBytes", "schedulerFrames", "schedulerBytes", "schedulerStreams",
    "retransmissionAttempt", "peakRetransmissionAttempt", "retransmissionsTotal",
    "publicationsTotal", "publishedBytesTotal", "admittedUncommitted",
    "pacedPublicationsTotal", "urgentPublicationsTotal", "logicalEventsTotal",
    "responseBatchFramesTotal", "batchedEventsTotal",
  ]);
  const parsed = Object.fromEntries(keys
    .filter(key => key !== "roundTripEwmaMs")
    .map(key => [key, number(raw[key], counters.has(key))]));
  if (Object.values(parsed).some(value => value === null)) return null;
  return { ...(parsed as unknown as Omit<V2TransportDiagnostics, "roundTripEwmaMs">), roundTripEwmaMs };
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
