export type Brand<T, Name extends string> = T & { readonly __brand: Name };

export type ProjectId = Brand<string, "ProjectId">;
export type SandboxId = Brand<string, "SandboxId">;
export type SandboxSessionId = Brand<string, "SandboxSessionId">;

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export function projectId(value: string): ProjectId {
  if (!UUID.test(value)) throw new TypeError(`Invalid ProjectId: ${value}`);
  return value as ProjectId;
}

function nonEmptyId<Name extends string>(value: string, name: Name): Brand<string, Name> {
  if (typeof value !== "string" || value.trim().length === 0) throw new TypeError(`Invalid ${name}`);
  return value.trim() as Brand<string, Name>;
}

export function sandboxId(value: string): SandboxId {
  return nonEmptyId(value, "SandboxId");
}

export function sandboxSessionId(value: string): SandboxSessionId {
  return nonEmptyId(value, "SandboxSessionId");
}

export class SecretValue<Name extends string> {
  readonly #value: string;
  readonly #name: Name;

  constructor(name: Name, value: string) {
    if (typeof value !== "string" || value.length === 0) throw new TypeError(`${name} must not be empty`);
    this.#name = name;
    this.#value = value;
  }

  reveal(): string {
    return this.#value;
  }

  kind(): Name {
    return this.#name;
  }

  toString(): string {
    return "[REDACTED]";
  }

  toJSON(): string {
    return "[REDACTED]";
  }

  [Symbol.toPrimitive](): string {
    return "[REDACTED]";
  }
}

export type SandboxToken = SecretValue<"SandboxToken">;
export type YSweetToken = SecretValue<"YSweetToken">;
export type ResourceToken = SecretValue<"ResourceToken">;

export const BOOTSTRAP_FAILURE_PHASES = ["deploy", "exec", "hello", "readiness", "shutdown"] as const;
export const BOOTSTRAP_FAILURE_CODES = ["bootstrap_ownership_conflict", "bootstrap_cleanup_unproven", "bootstrap_stage_lost", "bootstrap_cleanup_not_persisted"] as const;
export const BOOTSTRAP_FAILURE_REASONS = ["missing_receipt", "release_mismatch", "generation_conflict", "file_mismatch", "ownership_changed", "transport", "bootstrap", "sequence", "challenge", "command"] as const;

export interface SandboxFailureSnapshot {
  name: string;
  kind: string | null;
  status: number | null;
  operation: string | null;
  message: string;
  bootstrapPhase?: typeof BOOTSTRAP_FAILURE_PHASES[number];
  bootstrapCode?: typeof BOOTSTRAP_FAILURE_CODES[number];
  bootstrapReason?: typeof BOOTSTRAP_FAILURE_REASONS[number];
  /** Bounded atoms emitted by the trusted sidecar fatal record, when present. */
  sidecarErrorName?: string;
  sidecarErrorCode?: string;
  sidecarCauseName?: string | null;
  sidecarPhase?: string;
  sidecarPermission?: string | null;
}
