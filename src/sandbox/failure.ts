import type { SandboxFailureSnapshot } from "./types";

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

export function describeSandboxFailure(error: unknown): SandboxFailureSnapshot {
  const record = error && typeof error === "object" ? error as Record<string, unknown> : {};
  const rawSidecar = record.sidecarDiagnostic;
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
