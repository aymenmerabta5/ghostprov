import { describe, expect, it } from "bun:test";
import type { RunHandle } from "../src/relay/run-handle";
import { FleetRateLimitGate, ModelRateLimitGates } from "../src/router/rate-limit-gate";
import { MultiSandboxRouter, RouterRateLimitedError } from "../src/router/router";
import { ResourceGovernor } from "../src/router/resource-governor";
import { SandboxRegistry, type RoutableSandboxRuntime } from "../src/sandbox/registry";
import { projectId, type ProjectId } from "../src/sandbox/types";
import { apiErrorResponse } from "../src/http/errors";

const FULL_TPM_MESSAGE = "Rate limit reached for gpt-5.6-sol in organization " +
  "org-ExampleOrganization123 on tokens per min (TPM): Limit 40000000, " +
  "Used 40000000, Requested 609930. Please try again in 914ms. Visit " +
  "https://platform.openai.com/account/rate-limits";

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((resolveValue, rejectValue) => {
    resolve = resolveValue;
    reject = rejectValue;
  });
  return { promise, resolve, reject };
}

class Runtime implements RoutableSandboxRuntime {
  readonly projectId: ProjectId;
  calls = 0;
  readonly finals: Array<ReturnType<typeof deferred<Record<string, unknown>>>> = [];
  readonly rateLimits: Array<RunHandle["rateLimit"]> = [];

  constructor(id = "00000000-0000-4000-8000-000000000001") {
    this.projectId = projectId(id);
  }

  async start(): Promise<void> {}
  async drain(): Promise<void> {}
  async stop(): Promise<void> {}
  snapshot() {
    return {
      projectId: this.projectId,
      state: "ready",
      capacity: 8,
      active: 0,
      available: 8,
      quarantined: 0,
      heartbeatAgeMs: 1,
      credentialExpiresInMs: 60_000,
      ackEwmaMs: 1,
      completionEwmaMs: 1,
      recentFailureRate: 0,
      queueDepth: 0,
      memoryPressure: 0,
      failure: null,
    } as const;
  }
  async run(): Promise<RunHandle> {
    this.calls++;
    const final = deferred<Record<string, unknown>>();
    this.finals.push(final);
    return {
      async *events() {},
      final: final.promise,
      rateLimit: this.rateLimits.shift(),
      stats: { bytes: 0, firstByteMs: null, frames: 0 },
    };
  }
}

function router(runtime: Runtime | readonly Runtime[], rateLimit: {
  rateLimitGate?: FleetRateLimitGate;
  rateLimitGates?: ModelRateLimitGates;
} = {}): MultiSandboxRouter {
  return new MultiSandboxRouter({
    registry: new SandboxRegistry(Array.isArray(runtime) ? runtime : [runtime]),
    governor: new ResourceGovernor({
      probe: { sample: () => ({
        totalBytes: 8 * 1024 ** 3,
        freeBytes: 6 * 1024 ** 3,
        rssBytes: 64 * 1024 ** 2,
        heapBytes: 32 * 1024 ** 2,
        externalBytes: 8 * 1024 ** 2,
        logicalProcessors: 4,
      }) },
    }),
    rateLimitGate: rateLimit.rateLimitGate,
    rateLimitGates: rateLimit.rateLimitGates,
    random: { next: () => 0, uuid: () => "trace-rate-limit" },
  });
}

describe("V2 fleet TPM admission", () => {
  it("turns a streamed rate-limit terminal into a fleet-wide local 429 before another sandbox call", async () => {
    const runtime = new Runtime();
    runtime.rateLimits.push({
      limitTokens: 40_000_000,
      remainingTokens: 0,
      resetTokensMs: 60_000,
    });
    const value = router(runtime, { rateLimitGate: new FleetRateLimitGate() });
    const handle = await value.startRelay({ model: "gpt-5.6-sol", input: "first" });
    runtime.finals[0]!.resolve({
      id: "resp_limited",
      status: "failed",
      error: {
        code: "rate_limit_exceeded",
        type: "rate_limit_error",
        message: "Rate limit reached. Please try again in 109ms.",
      },
    });
    await handle.final;
    await Promise.resolve();

    await expect(value.startRelay({ model: "gpt-5.6-sol", input: "second" }))
      .rejects.toMatchObject({ status: 429, code: "rate_limit_exceeded" });
    expect(runtime.calls).toBe(1);
    await value.close();
  });

  it("blocks only the rate-limited model across every sandbox", async () => {
    const runtime = new Runtime();
    runtime.rateLimits.push(
      { limitTokens: 40_000_000, remainingTokens: 0, resetTokensMs: 60_000 },
      { limitTokens: 40_000_000, remainingTokens: 39_000_000 },
    );
    const value = router(runtime, { rateLimitGates: new ModelRateLimitGates() });
    const sol = await value.startRelay({ model: "gpt-5.6-sol", input: "first" });
    runtime.finals[0]!.resolve({
      id: "resp_limited",
      status: "failed",
      error: {
        code: "rate_limit_exceeded",
        type: "rate_limit_error",
        message: "Rate limit reached. Please try again in 109ms.",
      },
    });
    await sol.final;
    await Promise.resolve();

    await expect(value.startRelay({ model: "gpt-5.6-sol", input: "second" }))
      .rejects.toMatchObject({ status: 429 });
    const terra = await value.startRelay({ model: "gpt-5.6-terra", input: "third" });
    expect(runtime.calls).toBe(2);
    runtime.finals[1]!.resolve({ id: "resp_terra", status: "completed" });
    await terra.final;
    await value.close();
  });

  it("shares one model limit across distinct sandboxes and returns the original problem", async () => {
    const first = new Runtime("00000000-0000-4000-8000-000000000001");
    const second = new Runtime("00000000-0000-4000-8000-000000000002");
    first.rateLimits.push({
      limitTokens: 40_000_000,
      remainingTokens: 0,
      retryAfterMs: 914,
    });
    const gates = new ModelRateLimitGates();
    const value = router([first, second], { rateLimitGates: gates });

    const handle = await value.startRelay({ model: "gpt-5.6-sol", input: "first" });
    first.finals[0]!.resolve({
      id: "resp_limited",
      status: "failed",
      error: {
        message: FULL_TPM_MESSAGE,
        type: "rate_limit_error",
        code: "rate_limit_exceeded",
      },
    });
    await handle.final;
    await Promise.resolve();

    expect(value.snapshot().rateLimit).toMatchObject({
      "gpt-5.6-sol": {
        state: "cooldown",
        tokenLimit: 40_000_000,
        remainingTokens: 0,
        lastRateLimit: {
          message: FULL_TPM_MESSAGE,
          organization: "org-ExampleOrganization123",
          requestedTokens: 609_930,
        },
      },
    });
    expect(value.snapshot().runtimes).toEqual(expect.arrayContaining([
      expect.objectContaining({
        projectId: first.projectId,
        upstreamOrganization: {
          id: "org-ExampleOrganization123",
          model: "gpt-5.6-sol",
          source: "rate_limit",
          observedAgoMs: expect.any(Number),
        },
      }),
      expect.objectContaining({
        projectId: second.projectId,
        upstreamOrganization: null,
      }),
    ]));

    let blocked: unknown;
    try {
      await value.startRelay({ model: "gpt-5.6-sol", input: "second" });
    } catch (error) {
      blocked = error;
    }
    expect(blocked).toBeInstanceOf(RouterRateLimitedError);
    expect((blocked as Error).message).toBe(FULL_TPM_MESSAGE);
    const response = apiErrorResponse(blocked);
    expect(response.status).toBe(429);
    expect(await response.json()).toEqual({
      error: {
        message: FULL_TPM_MESSAGE,
        type: "rate_limit_error",
        param: null,
        code: "rate_limit_exceeded",
      },
    });
    expect(first.calls).toBe(1);
    expect(second.calls).toBe(0);
    await value.close();
  });

  it("returns retry metadata on the local 429 response", async () => {
    const response = apiErrorResponse(new RouterRateLimitedError({
      retryAfterMs: 109,
      resetTokensMs: 360_000,
    }));
    expect(response.status).toBe(429);
    expect(response.headers.get("retry-after")).toBe("360");
    expect(response.headers.get("x-ratelimit-reset-tokens")).toBe("360000ms");
  });

  it("maps a V2 committed upstream 429 to the same public error contract", async () => {
    const upstream = Object.assign(
      new Error("Rate limit reached. Please try again in 109ms."),
      {
        code: "rate_limit_exceeded",
        rateLimit: { retryAfterMs: 109, resetTokensMs: 360_000 },
      },
    );
    const response = apiErrorResponse(upstream);
    expect(response.status).toBe(429);
    expect(response.headers.get("retry-after")).toBe("1");
    expect(response.headers.get("x-ratelimit-reset-tokens")).toBe("360000ms");
    await expect(response.json()).resolves.toMatchObject({
      error: { type: "rate_limit_error", code: "rate_limit_exceeded" },
    });
  });
});
