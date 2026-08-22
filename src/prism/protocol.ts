import {
  SecretValue,
  projectId,
  sandboxId,
  sandboxSessionId,
  type ProjectId,
  type ResourceToken,
  type SandboxId,
  type SandboxSessionId,
  type SandboxToken,
  type YSweetToken,
} from "../sandbox/types";

export interface ProvisionedSandbox {
  url: string;
  token: SandboxToken;
  sandboxId?: SandboxId;
  sessionId?: SandboxSessionId;
  sessionFirstSeenAtMs?: number;
}

export interface YSweetEnvelope {
  url: string;
  baseUrl: string;
  docId: ProjectId;
  token: YSweetToken;
  authorization: string;
}

export interface ResourceCredential {
  token: ResourceToken;
  expiresAtMs: number;
  scopes: readonly string[];
  baseUrl: string;
}

function record(value: unknown, label: string): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new TypeError(`${label} must be an object`);
  return value as Record<string, unknown>;
}

function stringField(value: Record<string, unknown>, field: string): string {
  const result = value[field];
  if (typeof result !== "string" || result.length === 0) throw new TypeError(`${field} must be a non-empty string`);
  return result;
}

function trailingSlash(value: string): string {
  return value.endsWith("/") ? value : `${value}/`;
}

export function parseProvisionedSandbox(value: unknown): ProvisionedSandbox {
  const body = record(value, "ProvisionedSandbox");
  const parsed: ProvisionedSandbox = {
    url: trailingSlash(stringField(body, "url")),
    token: new SecretValue("SandboxToken", stringField(body, "token")),
  };
  if (typeof body.sandbox_id === "string" && body.sandbox_id) parsed.sandboxId = sandboxId(body.sandbox_id);
  if (typeof body.sandbox_session_id === "string" && body.sandbox_session_id) parsed.sessionId = sandboxSessionId(body.sandbox_session_id);
  if (typeof body.sandbox_session_first_seen_at_ms === "number" && Number.isFinite(body.sandbox_session_first_seen_at_ms)) {
    parsed.sessionFirstSeenAtMs = body.sandbox_session_first_seen_at_ms;
  }
  return parsed;
}

export function parseYSweetEnvelope(value: unknown): YSweetEnvelope {
  const body = record(value, "YSweetEnvelope");
  return {
    url: stringField(body, "url"),
    baseUrl: trailingSlash(stringField(body, "baseUrl")),
    docId: projectId(stringField(body, "docId")),
    token: new SecretValue("YSweetToken", stringField(body, "token")),
    authorization: stringField(body, "authorization"),
  };
}

export function parseResourceCredential(value: unknown, nowMs: number): ResourceCredential {
  const body = record(value, "ResourceCredential");
  const absolute = typeof body.expires_at === "number" && Number.isFinite(body.expires_at) && body.expires_at > 0
    ? body.expires_at * 1000
    : null;
  const relative = typeof body.max_age_seconds === "number" && Number.isFinite(body.max_age_seconds) && body.max_age_seconds > 0
    ? nowMs + body.max_age_seconds * 1000
    : null;
  const expiresAtMs = absolute ?? relative;
  if (expiresAtMs === null) throw new TypeError("ResourceCredential expiry is missing");
  const scopes = Array.isArray(body.scopes) && body.scopes.every(scope => typeof scope === "string")
    ? body.scopes as string[]
    : [];
  return {
    token: new SecretValue("ResourceToken", stringField(body, "access_token")),
    expiresAtMs,
    scopes: Object.freeze([...scopes]),
    baseUrl: trailingSlash(stringField(body, "resources_base_url")),
  };
}
