import { describe, expect, it } from "bun:test";
import type { Clock } from "../src/core/ports";
import {
  estimateRateLimitTokens,
  FleetRateLimitGate,
  ModelRateLimitGates,
} from "../src/router/rate-limit-gate";
import {
  rateLimitHintFromError,
  rateLimitProblemFromError,
} from "../src/v2/rate-limit";

const FULL_TPM_MESSAGE = "Rate limit reached for gpt-5.6-sol in organization " +
  "org-ExampleOrganization123 on tokens per min (TPM): Limit 40000000, " +
  "Used 40000000, Requested 609930. Please try again in 914ms. Visit " +
  "https://platform.openai.com/account/rate-limits";

class TestClock implements Clock {
  value = 0;
  now(): number { return this.value; }
  async sleep(ms: number): Promise<void> { this.value += ms; }
  async waitUntil(deadlineMs: number): Promise<void> { this.value = Math.max(this.value, deadlineMs); }
  advance(ms: number): void { this.value += ms; }
}

describe("FleetRateLimitGate", () => {
  it("serializes a cold fleet until an upstream token header establishes its budget", () => {
    const gate = new FleetRateLimitGate({ clock: new TestClock() });
    const first = gate.tryAcquire(128_000);
    expect(first).not.toBeNull();
    expect(gate.tryAcquire(128_000)).toBeNull();

    first!.complete({
      limitTokens: 40_000_000,
      remainingTokens: 39_000_000,
      resetTokensMs: 60_000,
    });
    expect(gate.snapshot()).toMatchObject({ state: "open", tokenLimit: 40_000_000 });
    expect(gate.tryAcquire(128_000)).not.toBeNull();
    gate.close();
  });

  it("holds the fleet after a TPM hit and lets only one half-open probe through", () => {
    const clock = new TestClock();
    const gate = new FleetRateLimitGate({ clock });
    const first = gate.tryAcquire(128_000)!;
    first.complete({ limitTokens: 40_000_000, remainingTokens: 39_000_000 });
    const limited = gate.tryAcquire(128_000)!;
    limited.rateLimited({ retryAfterMs: 109, resetTokensMs: 1_000 });

    expect(gate.retryAfterMs(128_000)).toBe(1_000);
    expect(gate.tryAcquire(128_000)).toBeNull();
    clock.advance(1_000);
    const probe = gate.tryAcquire(128_000);
    expect(probe).not.toBeNull();
    expect(gate.tryAcquire(128_000)).toBeNull();

    probe!.complete({ limitTokens: 40_000_000, remainingTokens: 39_000_000 });
    expect(gate.snapshot().state).toBe("open");
    expect(gate.tryAcquire(128_000)).not.toBeNull();
    gate.close();
  });

  it("paces normal admission from remaining-token headers instead of re-saturating TPM", () => {
    const clock = new TestClock();
    const gate = new FleetRateLimitGate({ clock });
    const initial = gate.tryAcquire(10_000)!;
    initial.complete({ limitTokens: 100_000, remainingTokens: 20_000 });

    const first = gate.tryAcquire(10_000);
    expect(first).not.toBeNull();
    first!.complete();
    expect(gate.retryAfterMs(10_000)).toBe(6_000);
    expect(gate.tryAcquire(10_000)).toBeNull();
    clock.advance(6_000);
    expect(gate.tryAcquire(10_000)).not.toBeNull();
    gate.close();
  });

  it("reserves a conservative allowance for long contexts and output budgets", () => {
    expect(estimateRateLimitTokens({ input: "x" })).toBeGreaterThanOrEqual(128_000);
    expect(estimateRateLimitTokens({ input: "x", max_output_tokens: 500_000 })).toBeGreaterThanOrEqual(625_000);
  });

  it("falls back to the canonical TPM error text when a proxy strips headers", () => {
    expect(rateLimitHintFromError({
      code: "rate_limit_exceeded",
      message: "Rate limit reached for gpt-5.6-sol on tokens per min (TPM): Limit 40000000, Used 40000000, Requested 72794. Please try again in 109ms.",
    })).toEqual({
      limitTokens: 40_000_000,
      remainingTokens: 0,
      retryAfterMs: 109,
    });
  });

  it("retains the complete organization TPM problem and exposes real viewer values", () => {
    const clock = new TestClock();
    const gate = new FleetRateLimitGate({ clock });
    const problem = rateLimitProblemFromError(
      new Error(`stream disconnected before completion: ${FULL_TPM_MESSAGE}`),
    );
    const hint = rateLimitHintFromError(new Error(FULL_TPM_MESSAGE));
    expect(problem).toMatchObject({
      message: `stream disconnected before completion: ${FULL_TPM_MESSAGE}`,
      model: "gpt-5.6-sol",
      organization: "org-ExampleOrganization123",
      limitKind: "tokens_per_min",
      tokenLimit: 40_000_000,
      usedTokens: 40_000_000,
      requestedTokens: 609_930,
      retryAfterMs: 914,
    });

    gate.tryAcquire(128_000)!.rateLimited(hint, problem);
    expect(gate.snapshot()).toMatchObject({
      state: "cooldown",
      tokenLimit: 40_000_000,
      remainingTokens: 0,
      active: 0,
      lastRateLimit: problem,
    });
    gate.close();
  });

  it("extracts the same TPM signal from a V1 daemon envelope", () => {
    const daemonError = Object.assign(new Error("Upstream request failed"), {
      kind: "upstream",
      details: {
        code: "rate_limited_429",
        message: JSON.stringify({
          error: {
            message: FULL_TPM_MESSAGE,
            type: "rate_limit_error",
            code: "rate_limit_exceeded",
          },
        }),
      },
    });
    expect(rateLimitHintFromError(daemonError)).toEqual({
      limitTokens: 40_000_000,
      remainingTokens: 0,
      retryAfterMs: 914,
    });
    expect(rateLimitProblemFromError(daemonError)).toMatchObject({
      message: FULL_TPM_MESSAGE,
      type: "rate_limit_error",
      code: "rate_limit_exceeded",
    });
  });

  it("keeps different models isolated while allowing an explicit shared-model scope", () => {
    const gates = new ModelRateLimitGates();
    const sol = gates.forModel("gpt-5.6-sol");
    const terra = gates.forModel("gpt-5.6-terra");
    expect(sol).not.toBe(terra);
    sol.rateLimited({ retryAfterMs: 60_000 });
    expect(sol.retryAfterMs(128_000)).not.toBeNull();
    expect(terra.retryAfterMs(128_000)).toBeNull();
    gates.close();

    const shared = new ModelRateLimitGates({
      scopeForModel: model => (
        model === "gpt-5.6-sol" || model === "gpt-5.6-terra" ? "shared:sol-terra" : model
      ),
    });
    expect(shared.forModel("gpt-5.6-sol")).toBe(shared.forModel("gpt-5.6-terra"));
    shared.close();
  });
});
