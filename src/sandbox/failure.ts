import { BOOTSTRAP_FAILURE_CODES, BOOTSTRAP_FAILURE_PHASES, type SandboxFailureSnapshot } from "./types";

const SAFE_DETAIL = /^[a-zA-Z][a-zA-Z0-9._-]{0,63}$/;

function safeDetail(value: unknown): string | null {
  return typeof value === "string" && SAFE_DETAIL.test(value) ? value : null;
}

function redactFailureMessage(value: string): string {
  return value
    .replace(/\b(cookie|authorization|x-crixet-[a-z0-9_-]+)\s*[:=]\s*[^,\r\n]*/gi, "$1=[REDACTED]")
    .replace(/\b(token|api[_-]?key|secret|password)\s*[:=]\s*[^;\s,]+/gi, "$1=[REDACTED]")
    .replace(/[a-zA-Z0-9+/_=-]{64,}/g, "[REDACTED]")
    .slice(0, 240);
}

/**
 * Lifecycle failures wrap bounded bootstrap/retirement errors in `cause` and
 * occasionally `AggregateError.errors`. Walk only a small, cycle-safe set of
 * data properties so structured sidecar diagnostics survive that wrapping
 * without retaining arbitrary error graphs or reading dynamic accessors.
 */
function failureRecords(error: unknown): Record<string, unknown>[] {
  const records: Record<string, unknown>[] = [];
  const pending: unknown[] = [error];
  const seen = new WeakSet<object>();
  while (pending.length > 0 && records.length < 8) {
    const value = pending.shift();
    if (!value || typeof value !== "object" || seen.has(value)) continue;
    seen.add(value);
    const record = value as Record<string, unknown>;
    records.push(record);
    const cause = Object.getOwnPropertyDescriptor(value, "cause")?.value;
    if (cause !== undefined) pending.push(cause);
    const errors = Object.getOwnPropertyDescriptor(value, "errors")?.value;
    if (Array.isArray(errors)) {
      for (const nested of errors.slice(0, 8 - records.length)) pending.push(nested);
    }
  }
  return records;
}

export function describeSandboxFailure(error: unknown): SandboxFailureSnapshot {
  const records = failureRecords(error);
  const record = records[0] ?? {};
  const supervisor = records.find(value => Object.getOwnPropertyDescriptor(value, "name")?.value === "V2SupervisorBootstrapError");
  const phase = supervisor && Object.getOwnPropertyDescriptor(supervisor, "kind")?.value;
  const code = supervisor && Object.getOwnPropertyDescriptor(supervisor, "remoteCode")?.value;
  const rawSidecar = records
    .map(value => Object.getOwnPropertyDescriptor(value, "sidecarDiagnostic")?.value)
    .find(value => value && typeof value === "object" && !Array.isArray(value));
  const sidecar = rawSidecar && typeof rawSidecar === "object" && !Array.isArray(rawSidecar)
    ? rawSidecar as Record<string, unknown>
    : null;
  const sidecarErrorName = safeDetail(sidecar?.errorName);
  const sidecarErrorCode = safeDetail(sidecar?.errorCode);
  const sidecarCauseName = sidecar?.causeName === null
    ? null
    : safeDetail(sidecar?.causeName);
  const sidecarPhase = safeDetail(sidecar?.phase);
  const sidecarPermission = sidecar?.permission === null
    ? null
    : safeDetail(sidecar?.permission);
  const rawMessage = error instanceof Error ? error.message : String(error ?? "Sandbox runtime failed");
  const rawName = error instanceof Error ? error.name : "Error";
  const status = typeof record.status === "number" && Number.isInteger(record.status) &&
    record.status >= 100 && record.status <= 599
    ? record.status
    : null;
  return {
    name: safeDetail(rawName) ?? "Error",
    kind: safeDetail(record.kind),
    status,
    operation: safeDetail(record.operation),
    message: redactFailureMessage(rawMessage) || "Sandbox runtime failed",
    ...(BOOTSTRAP_FAILURE_PHASES.includes(phase) ? { bootstrapPhase: phase } : {}),
    ...(BOOTSTRAP_FAILURE_CODES.includes(code) ? { bootstrapCode: code } : {}),
    ...(sidecarErrorName && sidecarErrorCode
      ? {
          sidecarErrorName,
          sidecarErrorCode,
          sidecarCauseName,
          ...(sidecarPhase ? { sidecarPhase } : {}),
          ...(sidecarPermission !== undefined ? { sidecarPermission } : {}),
        }
      : {}),
  };
}
