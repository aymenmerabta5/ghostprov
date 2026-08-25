import type {
  AtomicBytePermit,
  AtomicBytePermitResizeOptions,
  AtomicBytePermitRegion,
} from "./atomic-byte-permits";

/**
 * Public read-only view used by diagnostics/tests. Mutation remains private to
 * this module so a shared allocation cannot be resized through one reference
 * while another pipeline stage still owns it.
 */
export interface AtomicBytePermitView {
  readonly active: boolean;
  readonly bytes: number;
  readonly region: AtomicBytePermitRegion;
  readonly label: string | null;
}

interface SharedPermitState {
  readonly permit: AtomicBytePermit;
  references: number;
}

/** One independently releasable reference to a single physical allocation. */
class PermitReference implements AtomicBytePermitView {
  #active = true;

  constructor(
    readonly state: SharedPermitState,
    readonly referenceLabel: string | null,
  ) {}

  get active(): boolean {
    return this.#active && this.state.permit.active;
  }

  get bytes(): number {
    return this.active ? this.state.permit.bytes : 0;
  }

  get region(): AtomicBytePermitRegion {
    if (!this.active) throw new Error("Atomic byte permit reference is inactive");
    return this.state.permit.region;
  }

  get label(): string | null {
    return this.active ? this.referenceLabel : null;
  }

  /** Move this reference without changing the shared reference count. */
  transfer(label?: string): PermitReference {
    this.#assertActive();
    this.#active = false;
    return new PermitReference(this.state, safeLabel(label));
  }

  /** Add one independently releasable owner of the same physical charge. */
  share(label?: string): PermitReference {
    this.#assertActive();
    this.state.references++;
    return new PermitReference(this.state, safeLabel(label));
  }

  release(): void {
    if (!this.#active) return;
    this.#active = false;
    this.state.references = Math.max(0, this.state.references - 1);
    if (this.state.references === 0) this.state.permit.release();
  }

  #assertActive(): void {
    if (!this.active) throw new Error("Atomic byte permit reference is inactive");
  }
}

const PERMITS = new WeakMap<object, PermitReference>();

function carrier(value: unknown): object | null {
  return value !== null && (typeof value === "object" || typeof value === "function")
    ? value as object
    : null;
}

function safeLabel(value: string | undefined): string | null {
  return value && value.length > 0 ? value.slice(0, 128) : null;
}

function attachReference<T extends object>(target: T, reference: PermitReference): T {
  const existing = PERMITS.get(target);
  if (existing?.active) throw new Error("Target already owns an atomic byte permit");
  PERMITS.set(target, reference);
  return target;
}

/** Attach exclusive ownership without making it visible to JSON/stringification. */
export function attachAtomicBytePermit<T extends object>(
  target: T,
  permit: AtomicBytePermit,
): T {
  if (!permit.active) throw new Error("Cannot attach an inactive atomic byte permit");
  const existing = PERMITS.get(target);
  if (existing?.active) throw new Error("Target already owns an atomic byte permit");
  return attachReference(target, new PermitReference({ permit, references: 1 }, permit.label));
}

export function atomicBytePermitOf(value: unknown): AtomicBytePermitView | null {
  const valueCarrier = carrier(value);
  if (!valueCarrier) return null;
  const reference = PERMITS.get(valueCarrier);
  return reference?.active ? reference : null;
}

/**
 * Atomically detach the source reference and attach its successor to target.
 * A missing carrier is a no-op for compatibility with non-V2/fake handles.
 */
export function transferAtomicBytePermit<T extends object>(
  source: unknown,
  target: T,
  label?: string,
): T {
  const sourceCarrier = carrier(source);
  const reference = sourceCarrier ? PERMITS.get(sourceCarrier) : undefined;
  if (!reference) return target;
  const existing = PERMITS.get(target);
  if (existing?.active) throw new Error("Target already owns an atomic byte permit");
  PERMITS.delete(sourceCarrier!);
  if (!reference.active) return target;
  return attachReference(target, reference.transfer(label));
}

/**
 * Give target an independent reference to source's one physical allocation.
 * Releasing either carrier leaves the allocation charged until every shared
 * owner releases. Missing source ownership is a compatibility no-op.
 */
export function shareAtomicBytePermit<T extends object>(
  source: unknown,
  target: T,
  label?: string,
): T {
  const sourceCarrier = carrier(source);
  const reference = sourceCarrier ? PERMITS.get(sourceCarrier) : undefined;
  if (!reference?.active) return target;
  const existing = PERMITS.get(target);
  if (existing?.active) throw new Error("Target already owns an atomic byte permit");
  return attachReference(target, reference.share(label));
}

/**
 * Grow/shrink an exclusively owned carrier in place. Shared terminal/output
 * allocations deliberately refuse mutation because every reference relies on
 * one stable physical charge.
 */
export function tryResizeAtomicBytePermit(
  value: unknown,
  targetBytes: number,
  options: Omit<AtomicBytePermitResizeOptions, "signal"> = {},
): boolean {
  const valueCarrier = carrier(value);
  const reference = valueCarrier ? PERMITS.get(valueCarrier) : undefined;
  if (!reference?.active || reference.state.references !== 1) return false;
  return reference.state.permit.tryResize(targetBytes, options);
}

/** Idempotently release and detach ownership from any carrier object. */
export function releaseAtomicBytePermit(value: unknown): void {
  const valueCarrier = carrier(value);
  if (!valueCarrier) return;
  const reference = PERMITS.get(valueCarrier);
  if (!reference) return;
  PERMITS.delete(valueCarrier);
  reference.release();
}
