import { SYSTEM_CLOCK, type Clock } from "../core/ports";

export type RuntimeHealthOutcome = "healthy" | "neutral" | "unhealthy";

const NEUTRAL_CODES = new Set([
  "cancelled",
  "capacity",
  "capacity_changed",
  "draining",
  "request_too_large",
  "response_too_large",
]);

function errorRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object"
    ? value as Record<string, unknown>
    : {};
}

export function classifyRuntimeError(
  error: unknown,
): Exclude<RuntimeHealthOutcome, "healthy"> {
  const value = errorRecord(error);
  if (value.name === "AbortError") return "neutral";
  if (typeof value.code === "string" && NEUTRAL_CODES.has(value.code)) {
    return "neutral";
  }
  if (value.committed === true && value.ambiguous === false) return "neutral";
  return "unhealthy";
}

export class DecayingRuntimeFailureScore {
  readonly #clock: Clock;
  readonly #weight: number;
  readonly #halfLifeMs: number;
  #score = 0;
  #updatedAtMs: number;

  constructor(options: {
    clock?: Clock;
    weight?: number;
    halfLifeMs?: number;
  } = {}) {
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#weight = options.weight ?? 0.20;
    this.#halfLifeMs = options.halfLifeMs ?? 60_000;
    if (!Number.isFinite(this.#weight) || this.#weight <= 0 || this.#weight > 1) {
      throw new RangeError("runtime health weight must be in (0, 1]");
    }
    if (!Number.isFinite(this.#halfLifeMs) || this.#halfLifeMs <= 0) {
      throw new RangeError("runtime health half-life must be positive");
    }
    this.#updatedAtMs = this.#now();
  }

  record(outcome: RuntimeHealthOutcome): void {
    this.#advance();
    if (outcome === "unhealthy") {
      this.#score += this.#weight * (1 - this.#score);
    } else if (outcome === "healthy") {
      this.#score *= 1 - this.#weight;
    }
    this.#score = Math.max(0, Math.min(1, this.#score));
  }

  value(): number {
    this.#advance();
    return this.#score;
  }

  #advance(): void {
    const now = this.#now();
    if (now < this.#updatedAtMs) {
      throw new RangeError("runtime health clock moved backwards");
    }
    this.#score *= Math.pow(0.5, (now - this.#updatedAtMs) / this.#halfLifeMs);
    this.#updatedAtMs = now;
  }

  #now(): number {
    const value = this.#clock.now();
    if (!Number.isFinite(value)) {
      throw new RangeError("runtime health clock must be finite");
    }
    return value;
  }
}
