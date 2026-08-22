export type Brand<T, Name extends string> = T & { readonly __brand: Name };

export type ProjectId = Brand<string, "ProjectId">;
export type SandboxId = Brand<string, "SandboxId">;
export type SandboxSessionId = Brand<string, "SandboxSessionId">;
export type RelayRequestId = Brand<string, "RelayRequestId">;
export type SlotId = Brand<number, "SlotId">;

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

export function relayRequestId(value: string): RelayRequestId {
  return nonEmptyId(value, "RelayRequestId");
}

export function slotId(value: number): SlotId {
  if (!Number.isInteger(value) || value < 1 || value > 4096) throw new RangeError(`Invalid SlotId: ${value}`);
  return value as SlotId;
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

export type SandboxLifecycleState =
  | { type: "configured"; projectId: ProjectId }
  | { type: "provisioning"; projectId: ProjectId; generation: number }
  | { type: "syncing_credentials"; projectId: ProjectId; generation: number }
  | { type: "syncing_workspace"; projectId: ProjectId; generation: number }
  | { type: "deploying_daemon"; projectId: ProjectId; generation: number }
  | { type: "ready"; projectId: ProjectId; generation: number; capacity: number }
  | { type: "draining"; projectId: ProjectId; generation: number; reason: string }
  | { type: "recovering"; projectId: ProjectId; generation: number; attempt: number }
  | { type: "auth_required"; projectId: ProjectId; generation: number }
  | { type: "failed"; projectId: ProjectId; generation: number; error: Error }
  | { type: "stopped"; projectId: ProjectId; generation: number };

export interface SandboxSnapshot {
  projectId: ProjectId;
  state: SandboxLifecycleState;
  capacity: number;
  active: number;
  available: number;
  quarantined: number;
  heartbeatAgeMs: number | null;
  resourceExpiresInMs: number | null;
}
