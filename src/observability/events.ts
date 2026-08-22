export type ObservabilityPriority = "terminal" | "progress" | "sample";

export type ObservabilityEventName =
  | "server.started"
  | "server.stopping"
  | "request.accepted"
  | "admission.acquired"
  | "request.queued"
  | "route.selected"
  | "transport.committed"
  | "response.first_event"
  | "request.completed"
  | "request.failed"
  | "fleet.sample"
  | "monitor.sample"
  | "monitor.error"
  | "logger.dropped"
  | "logger.error";

export type JsonScalar = string | number | boolean | null;
export type JsonValue = JsonScalar | JsonValue[] | { [key: string]: JsonValue };

export interface ObservabilityEvent {
  schema: 1;
  event: ObservabilityEventName;
  ts: string;
  mono_ms: number;
  [key: string]: JsonValue;
}

export function observabilityEvent(
  event: ObservabilityEventName,
  fields: Record<string, JsonValue> = {},
  now = Date.now(),
  monoMs = performance.now(),
): ObservabilityEvent {
  return Object.freeze({
    schema: 1 as const,
    event,
    ts: new Date(now).toISOString(),
    mono_ms: Math.round(monoMs * 1000) / 1000,
    ...fields,
  });
}
