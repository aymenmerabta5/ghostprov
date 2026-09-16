export const REQUEST_CANCELLATION_ORIGINS = Object.freeze([
  "websocket_remote_close",
  "websocket_heartbeat_timeout",
  "websocket_delivery_error",
  "websocket_protocol_error",
  "websocket_server_shutdown",
  "websocket_idle_timeout",
  "websocket_ingress_capacity",
  "websocket_socket_error",
] as const);

export type RequestCancellationOrigin = typeof REQUEST_CANCELLATION_ORIGINS[number];

export interface RequestCancellationMetadata {
  readonly origin: RequestCancellationOrigin;
  /** An observed WebSocket close event code; omitted when none was observed. */
  readonly websocketCloseCode?: number;
}

// The already-bounded request signal owns this fixed-size diagnostic record.
// Weak keys add no independent retention, and no public abort reason is changed.
const cancellationMetadata = new WeakMap<AbortSignal, RequestCancellationMetadata>();
const allowedOrigins: ReadonlySet<string> = new Set(REQUEST_CANCELLATION_ORIGINS);

/** Record host-observed cause before aborting; the first cancellation owns it. */
export function recordRequestCancellation(
  signal: AbortSignal,
  origin: RequestCancellationOrigin,
  websocketCloseCode?: number,
): void {
  if (signal.aborted || cancellationMetadata.has(signal) || !allowedOrigins.has(origin)) return;
  const code = typeof websocketCloseCode === "number" && Number.isInteger(websocketCloseCode) &&
      websocketCloseCode >= 1_000 && websocketCloseCode <= 4_999
    ? websocketCloseCode
    : undefined;
  cancellationMetadata.set(signal, Object.freeze({
    origin,
    ...(code === undefined ? {} : { websocketCloseCode: code }),
  }));
}

/** Signals without actual host-recorded cancellation evidence remain unknown. */
export function requestCancellationMetadata(
  signal: AbortSignal | undefined,
): RequestCancellationMetadata | null {
  return signal?.aborted ? cancellationMetadata.get(signal) ?? null : null;
}
