import { SYSTEM_CLOCK, type Clock } from "../core/ports";
import {
  rateLimitCooldownMs,
  type RateLimitHint,
  type RateLimitProblem,
} from "../v2/rate-limit";

export type RateLimitGateState = "cold" | "open" | "cooldown" | "probe";

export interface RateLimitGateSnapshot {
  state: RateLimitGateState;
  retryAfterMs: number | null;
  tokenLimit: number | null;
  remainingTokens: number | null;
  active: number;
  coldMaxActive: number;
  lastRateLimit: RateLimitProblem | null;
}

export interface RateLimitLease {
  readonly estimatedTokens: number;
  complete(snapshot?: RateLimitHint): void;
  rateLimited(hint?: RateLimitHint, problem?: RateLimitProblem): void;
  release(): void;
}

const MIN_HEADROOM_TOKENS = 10_000;

/**
 * One host-level controller shared by every sandbox. It starts with bounded
 * parallel admission until upstream headers or a canonical 429 teach it the
 * active TPM budget. Cold saturation is queue pressure, not a synthetic 429.
 * Thereafter it maintains a conservative token bucket and, after any 429,
 * permits only a single half-open probe before reopening the fleet.
 */
export class FleetRateLimitGate {
  readonly #clock: Clock;
  readonly #coldMaxActive: number;
  readonly #listeners = new Set<() => void>();
  #state: RateLimitGateState = "cold";
  #active = 0;
  #generation = 0;
  #cooldownUntilMs = 0;
  #tokenLimit: number | null = null;
  #remainingTokens: number | null = null;
  #lastProblem: RateLimitProblem | null = null;
  #refilledAtMs: number;
  #timer: ReturnType<typeof setTimeout> | null = null;

  constructor(options: { clock?: Clock; coldMaxActive?: number } = {}) {
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#coldMaxActive = options.coldMaxActive ?? 16;
    if (
      !Number.isSafeInteger(this.#coldMaxActive) ||
      this.#coldMaxActive < 1 ||
      this.#coldMaxActive > 128
    ) {
      throw new RangeError("coldMaxActive must be an integer from 1 through 128");
    }
    this.#refilledAtMs = this.#clock.now();
  }

  subscribe(listener: () => void): () => void {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }

  snapshot(): RateLimitGateSnapshot {
    this.#refill();
    const retryAfterMs = this.#rateLimitDelay(0);
    return {
      state: this.#state,
      retryAfterMs,
      tokenLimit: this.#tokenLimit,
      remainingTokens: this.#remainingTokens === null
        ? null
        : this.#state === "cooldown"
          ? 0
          : Math.floor(this.#remainingTokens),
      active: this.#active,
      coldMaxActive: this.#coldMaxActive,
      lastRateLimit: this.#lastProblem,
    };
  }

  latestProblem(): RateLimitProblem | null {
    return this.#lastProblem;
  }

  /** Returns a local retry delay without submitting an upstream request. */
  retryAfterMs(estimatedTokens = 0): number | null {
    if (!Number.isFinite(estimatedTokens) || estimatedTokens < 0) {
      throw new RangeError("estimatedTokens must be non-negative");
    }
    this.#refill();
    return this.#rateLimitDelay(Math.ceil(estimatedTokens));
  }

  tryAcquire(estimatedTokens: number): RateLimitLease | null {
    if (!Number.isFinite(estimatedTokens) || estimatedTokens < 1) {
      throw new RangeError("estimatedTokens must be positive");
    }
    const tokens = Math.ceil(estimatedTokens);
    this.#refill();
    if (this.#rateLimitDelay(tokens) !== null) return null;
    const stateAtAcquire = this.#state;
    if (
      (stateAtAcquire === "cold" && this.#active >= this.#coldMaxActive) ||
      (stateAtAcquire === "probe" && this.#active > 0)
    ) {
      return null;
    }
    if (this.#remainingTokens !== null) this.#remainingTokens -= tokens;
    this.#active++;
    let settled = false;
    const generation = this.#generation;
    const finish = (
      kind: "complete" | "rate_limited" | "release",
      hint?: RateLimitHint,
      problem?: RateLimitProblem,
    ): void => {
      if (settled) return;
      settled = true;
      this.#active = Math.max(0, this.#active - 1);
      if (kind === "rate_limited") {
        this.rateLimited(hint, problem);
        return;
      }
      if (hint) this.observe(hint);
      if (
        (stateAtAcquire === "cold" || stateAtAcquire === "probe") &&
        generation === this.#generation &&
        this.#state === stateAtAcquire
      ) {
        // With no usable headers, continue with bounded cold admission. A
        // successful header lets the conservative token bucket open normally.
        this.#state = this.#remainingTokens === null ? "cold" : "open";
      }
      this.#emit();
    };
    return {
      estimatedTokens: tokens,
      complete: hint => finish("complete", hint),
      rateLimited: (hint, problem) => finish("rate_limited", hint, problem),
      release: () => finish("release"),
    };
  }

  observe(hint: RateLimitHint): void {
    this.#refill();
    const scope = effectiveTokenScope(hint);
    if (scope) {
      const previousLimit = this.#tokenLimit;
      this.#tokenLimit = scope.limit;
      if (this.#remainingTokens === null || previousLimit !== scope.limit) {
        this.#remainingTokens = scope.remaining;
      } else {
        // Header snapshots include work already admitted by any sidecar. Never
        // increase from a late/stale response; it would reintroduce a stampede.
        this.#remainingTokens = Math.min(this.#remainingTokens, scope.remaining);
      }
      this.#refilledAtMs = this.#clock.now();
    }
    this.#emit();
  }

  rateLimited(hint?: RateLimitHint, problem?: RateLimitProblem): void {
    if (problem) this.#lastProblem = problem;
    if (hint) this.observe(hint);
    const now = this.#clock.now();
    const delay = rateLimitCooldownMs(hint);
    this.#cooldownUntilMs = Math.max(this.#cooldownUntilMs, now + delay);
    this.#state = "cooldown";
    this.#generation++;
    // A 429 means our local prediction was optimistic. Treat the bucket as
    // empty until the upstream headers or elapsed refill prove otherwise.
    if (this.#remainingTokens !== null) this.#remainingTokens = 0;
    this.#refilledAtMs = now;
    this.#scheduleWake();
    this.#emit();
  }

  close(): void {
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = null;
    this.#listeners.clear();
  }

  #refill(): void {
    const now = this.#clock.now();
    if (this.#remainingTokens !== null && this.#tokenLimit !== null) {
      const elapsed = Math.max(0, now - this.#refilledAtMs);
      this.#remainingTokens = Math.min(
        this.#tokenLimit,
        this.#remainingTokens + elapsed * (this.#tokenLimit / 60_000),
      );
    }
    this.#refilledAtMs = now;
  }

  #rateLimitDelay(estimatedTokens: number): number | null {
    const now = this.#clock.now();
    if (this.#state === "cooldown") {
      const remaining = Math.max(0, this.#cooldownUntilMs - now);
      if (remaining > 0) return Math.max(1, Math.ceil(remaining));
      this.#state = "probe";
    }
    if (this.#remainingTokens === null || this.#tokenLimit === null) return null;
    const headroom = Math.max(MIN_HEADROOM_TOKENS, Math.ceil(this.#tokenLimit * 0.01));
    const needed = estimatedTokens + headroom - this.#remainingTokens;
    if (needed <= 0) return null;
    const wait = Math.max(1, Math.ceil(needed / (this.#tokenLimit / 60_000)));
    this.#cooldownUntilMs = Math.max(this.#cooldownUntilMs, now + wait);
    this.#state = "cooldown";
    this.#scheduleWake();
    return wait;
  }

  #scheduleWake(): void {
    if (this.#timer) clearTimeout(this.#timer);
    const delay = Math.max(1, Math.ceil(this.#cooldownUntilMs - this.#clock.now()));
    this.#timer = setTimeout(() => {
      this.#timer = null;
      this.#emit();
    }, delay);
    this.#timer.unref?.();
  }

  #emit(): void {
    for (const listener of this.#listeners) listener();
  }
}

/**
 * OpenAI rate limits vary by model, so production routing owns one controller
 * per model ID. A caller can explicitly map dashboard-declared shared model
 * families to one scope; the registry never guesses that unrelated models
 * share a TPM bucket.
 */
export class ModelRateLimitGates {
  readonly #clock?: Clock;
  readonly #coldMaxActive: number;
  readonly #scopeForModel: (model: string) => string;
  readonly #gates = new Map<string, FleetRateLimitGate>();
  readonly #modelScopes = new Map<string, string>();
  readonly #unsubscribes = new Map<string, () => void>();
  readonly #listeners = new Set<() => void>();

  constructor(options: {
    clock?: Clock;
    coldMaxActive?: number;
    scopeForModel?: (model: string) => string;
  } = {}) {
    this.#clock = options.clock;
    this.#coldMaxActive = options.coldMaxActive ?? 16;
    if (
      !Number.isSafeInteger(this.#coldMaxActive) ||
      this.#coldMaxActive < 1 ||
      this.#coldMaxActive > 128
    ) {
      throw new RangeError("coldMaxActive must be an integer from 1 through 128");
    }
    this.#scopeForModel = options.scopeForModel ?? (model => model);
  }

  forModel(model: unknown): FleetRateLimitGate {
    const modelScope = normalizeModelScope(model);
    const priorScope = this.#modelScopes.get(modelScope);
    if (priorScope) return this.#gates.get(priorScope)!;
    const scope = normalizeModelScope(this.#scopeForModel(modelScope));
    const existing = this.#gates.get(scope);
    if (existing) {
      this.#modelScopes.set(modelScope, scope);
      return existing;
    }
    // Bound hostile/accidental model-name cardinality without weakening the
    // known-model isolation guarantee.
    const key = this.#gates.size < 64 ? scope : "other";
    const fallback = this.#gates.get(key);
    if (fallback) {
      this.#modelScopes.set(modelScope, key);
      return fallback;
    }
    const gate = new FleetRateLimitGate({
      ...(this.#clock ? { clock: this.#clock } : {}),
      coldMaxActive: this.#coldMaxActive,
    });
    this.#gates.set(key, gate);
    this.#modelScopes.set(modelScope, key);
    this.#unsubscribes.set(key, gate.subscribe(() => this.#emit()));
    this.#emit();
    return gate;
  }

  scopeForModel(model: unknown): string {
    const modelScope = normalizeModelScope(model);
    return this.#modelScopes.get(modelScope) ??
      normalizeModelScope(this.#scopeForModel(modelScope));
  }

  snapshot(): Readonly<Record<string, RateLimitGateSnapshot>> {
    return Object.freeze(Object.fromEntries(
      [...this.#gates.entries()].map(([model, gate]) => [model, gate.snapshot()]),
    ));
  }

  subscribe(listener: () => void): () => void {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }

  close(): void {
    for (const unsubscribe of this.#unsubscribes.values()) unsubscribe();
    this.#unsubscribes.clear();
    for (const gate of this.#gates.values()) gate.close();
    this.#gates.clear();
    this.#modelScopes.clear();
    this.#listeners.clear();
  }

  #emit(): void {
    for (const listener of this.#listeners) listener();
  }
}

export function normalizeModelScope(model: unknown): string {
  if (typeof model !== "string") return "default";
  const normalized = model.trim().toLowerCase();
  if (!normalized) return "default";
  return /^[a-z0-9][a-z0-9._:-]{0,127}$/.test(normalized) ? normalized : "default";
}

function effectiveTokenScope(hint: RateLimitHint): { limit: number; remaining: number } | null {
  const scopes: Array<{ limit: number; remaining: number }> = [];
  if (
    Number.isSafeInteger(hint.limitTokens) && hint.limitTokens! > 0 &&
    Number.isSafeInteger(hint.remainingTokens) && hint.remainingTokens! >= 0
  ) {
    scopes.push({ limit: hint.limitTokens!, remaining: Math.min(hint.limitTokens!, hint.remainingTokens!) });
  }
  if (
    Number.isSafeInteger(hint.limitProjectTokens) && hint.limitProjectTokens! > 0 &&
    Number.isSafeInteger(hint.remainingProjectTokens) && hint.remainingProjectTokens! >= 0
  ) {
    scopes.push({
      limit: hint.limitProjectTokens!,
      remaining: Math.min(hint.limitProjectTokens!, hint.remainingProjectTokens!),
    });
  }
  if (!scopes.length) return null;
  return scopes.reduce((lowest, candidate) => (
    candidate.remaining / candidate.limit < lowest.remaining / lowest.limit ? candidate : lowest
  ));
}

/**
 * A deliberately conservative preflight estimate. OpenAI may reserve output
 * and reasoning tokens we cannot tokenize exactly in the router, so this is a
 * safety budget rather than an accounting or billing value.
 */
export function estimateRateLimitTokens(body: Record<string, unknown>): number {
  let bytes = 0;
  try {
    bytes = new TextEncoder().encode(JSON.stringify(body)).byteLength;
  } catch {
    bytes = 0;
  }
  const configuredOutput = [
    body.max_output_tokens,
    body.max_tokens,
    body.max_completion_tokens,
  ].find((value): value is number => (
    typeof value === "number" && Number.isFinite(value) && value >= 0
  ));
  const output = configuredOutput ?? 128_000;
  const input = Math.ceil(bytes / 2);
  return Math.max(128_000, Math.ceil((input + output) * 1.25));
}
