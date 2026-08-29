import { describe, expect, it } from "bun:test";
import type { RunHandle } from "../src/relay/run-handle";
import { MultiSandboxRouter } from "../src/router/router";
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
  active = 0;
  incarnation = 1;
  readonly listeners = new Set<() => void>();
  readonly finals: Array<ReturnType<typeof deferred<Record<string, unknown>>>> = [];
  readonly rateLimits: Array<RunHandle["rateLimit"]> = [];
  readonly startErrors: unknown[] = [];

  constructor(
    id = "00000000-0000-4000-8000-000000000001",
    readonly capacity = 64,
  ) {
    this.projectId = projectId(id);
  }
  async start(): Promise<void> {}
  async drain(): Promise<void> {}
  async stop(): Promise<void> {}
  subscribe(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }
  replaceIncarnation(): void {
    this.incarnation++;
    for (const listener of this.listeners) listener();
  }
  snapshot() {
    return {
      projectId: this.projectId,
      incarnation: this.incarnation,
      state: "ready",
      capacity: this.capacity,
      active: this.active,
      available: Math.max(0, this.capacity - this.active),
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
    if (this.startErrors.length) throw this.startErrors.shift();
    const final = deferred<Record<string, unknown>>();
    this.finals.push(final);
    this.active++;
    return {
      async *events() {},
      final: final.promise.finally(() => { this.active--; }),
      rateLimit: this.rateLimits.shift(),
      stats: { bytes: 0, firstByteMs: null, frames: 0 },
    };
  }
}

function router(runtime: Runtime | readonly Runtime[]): MultiSandboxRouter {
  return new MultiSandboxRouter({
    registry: new SandboxRegistry(Array.isArray(runtime) ? runtime : [runtime]),
    governor: new ResourceGovernor({
      probe: { sample: () => ({
        totalBytes: 64 * 1024 ** 3,
        freeBytes: 52 * 1024 ** 3,
        rssBytes: 64 * 1024 ** 2,
        heapBytes: 32 * 1024 ** 2,
        externalBytes: 8 * 1024 ** 2,
        logicalProcessors: 16,
      }) },
    }),
    random: { next: () => 0, uuid: () => crypto.randomUUID() },
  });
}

describe("V2 upstream rate-limit observation", () => {
  it("dispatches more than sixteen same-model requests without a provider gate", async () => {
    const runtime = new Runtime();
    const value = router(runtime);
    const handles = await Promise.all(Array.from({ length: 40 }, (_, index) => (
      value.startRelay({ model: "gpt-5.6-sol", input: `ungated-${index}` })
    )));
    expect(runtime.calls).toBe(40);
    expect(value.snapshot().queued).toBe(0);
    expect(value.snapshot()).not.toHaveProperty("rateLimit");
    for (const [index, final] of runtime.finals.entries()) {
      final.resolve({ id: `resp_${index}`, status: "completed" });
    }
    await Promise.all(handles.map(handle => handle.final));
    expect(value.trafficSnapshotAndReset()).toMatchObject({
      accepted: 40,
      committed: 40,
      succeeded: 40,
      rateLimited: 0,
    });
    await value.close();
  });

  it("records a full streamed TPM problem but never blocks the next request", async () => {
    const runtime = new Runtime();
    runtime.rateLimits.push({
      limitTokens: 40_000_000,
      remainingTokens: 0,
      retryAfterMs: 914,
    });
    const value = router(runtime);
    const first = await value.startRelay({ model: "gpt-5.6-sol", input: "first" });
    runtime.finals[0]!.resolve({
      id: "resp_limited",
      status: "failed",
      error: {
        message: FULL_TPM_MESSAGE,
        type: "rate_limit_error",
        code: "rate_limit_exceeded",
      },
    });
    await first.final;
    await Promise.resolve();
    expect(value.snapshot().runtimes[0]).toMatchObject({
      upstreamOrganization: {
        id: "org-ExampleOrganization123",
        model: "gpt-5.6-sol",
        source: "rate_limit",
      },
      upstreamRateLimit: {
        hint: { limitTokens: 40_000_000, remainingTokens: 0, retryAfterMs: 914 },
        problem: {
          message: FULL_TPM_MESSAGE,
          organization: "org-ExampleOrganization123",
          requestedTokens: 609_930,
        },
        requestedModel: "gpt-5.6-sol",
      },
      upstreamRateLimits: {
        "gpt-5.6-sol": {
          problem: { organization: "org-ExampleOrganization123" },
        },
      },
    });
    const second = await value.startRelay({ model: "gpt-5.6-sol", input: "second" });
    expect(runtime.calls).toBe(2);
    runtime.finals[1]!.resolve({ id: "resp_second", status: "completed" });
    await second.final;
    await value.close();
  });

  it("does not flush or reject locally queued capacity work after a 429", async () => {
    const runtime = new Runtime("00000000-0000-4000-8000-000000000001", 1);
    runtime.rateLimits.push({ remainingTokens: 0, retryAfterMs: 914 });
    const value = router(runtime);
    const first = await value.startRelay({ model: "gpt-5.6-sol", input: "first" });
    const secondPending = value.startRelay({ model: "gpt-5.6-sol", input: "second" });
    for (let turn = 0; turn < 20 && value.snapshot().queued === 0; turn++) {
      await Promise.resolve();
    }
    expect(runtime.calls).toBe(1);
    expect(value.snapshot().queued).toBe(1);

    runtime.finals[0]!.resolve({
      status: "failed",
      error: { message: FULL_TPM_MESSAGE, code: "rate_limit_exceeded" },
    });
    await first.final;
    const second = await secondPending;
    expect(runtime.calls).toBe(2);
    runtime.finals[1]!.resolve({ id: "resp_second", status: "completed" });
    await second.final;
    await value.close();
  });

  it("returns the original pre-commit 429 and does not reroute it", async () => {
    const first = new Runtime("00000000-0000-4000-8000-000000000001");
    const second = new Runtime("00000000-0000-4000-8000-000000000002");
    const upstream = Object.assign(new Error(FULL_TPM_MESSAGE), {
      status: 429,
      code: "rate_limit_exceeded",
      type: "rate_limit_error",
      rateLimit: { retryAfterMs: 914, limitTokens: 40_000_000, remainingTokens: 0 },
    });
    first.startErrors.push(upstream);
    second.startErrors.push(upstream);
    const value = router([first, second]);
    let observed: unknown;
    try {
      await value.startRelay({ model: "gpt-5.6-sol", input: "limited" });
    } catch (error) {
      observed = error;
    }
    expect(observed).toBe(upstream);
    expect(first.calls + second.calls).toBe(1);
    const response = apiErrorResponse(observed);
    expect(response.status).toBe(429);
    expect(response.headers.get("retry-after")).toBe("1");
    expect(await response.json()).toEqual({
      error: {
        message: FULL_TPM_MESSAGE,
        type: "rate_limit_error",
        param: null,
        code: "rate_limit_exceeded",
      },
    });
    await value.close();
  });

  it("keeps observations scoped to the sandbox that produced the evidence", async () => {
    const first = new Runtime("00000000-0000-4000-8000-000000000001");
    const second = new Runtime("00000000-0000-4000-8000-000000000002");
    first.rateLimits.push({ remainingTokens: 0, retryAfterMs: 914 });
    second.rateLimits.push({ remainingTokens: 0, retryAfterMs: 914 });
    const value = router([first, second]);
    const handle = await value.startRelay({ model: "gpt-5.6-sol", input: "first" });
    const selected = first.calls ? first : second;
    const other = first.calls ? second : first;
    selected.finals[0]!.resolve({
      status: "failed",
      error: { message: FULL_TPM_MESSAGE, code: "rate_limit_exceeded" },
    });
    await handle.final;
    await Promise.resolve();
    const snapshots = value.snapshot().runtimes;
    expect(snapshots.find(runtime => runtime.projectId === selected.projectId)?.upstreamRateLimit)
      .not.toBeNull();
    expect(snapshots.find(runtime => runtime.projectId === other.projectId)?.upstreamRateLimit)
      .toBeNull();
    await value.close();
  });

  it("retains bounded observation evidence independently for each model", async () => {
    const runtime = new Runtime();
    runtime.rateLimits.push(
      { remainingTokens: 0, retryAfterMs: 100 },
      { remainingTokens: 0, retryAfterMs: 200 },
    );
    const value = router(runtime);
    const sol = await value.startRelay({ model: "gpt-5.6-sol", input: "sol" });
    runtime.finals[0]!.resolve({
      status: "failed",
      error: { message: "Rate limit reached. Please try again in 100ms.", code: "rate_limit_exceeded" },
    });
    await sol.final;
    const terra = await value.startRelay({ model: "gpt-5.6-terra", input: "terra" });
    runtime.finals[1]!.resolve({
      status: "failed",
      error: { message: "Rate limit reached. Please try again in 200ms.", code: "rate_limit_exceeded" },
    });
    await terra.final;
    await Promise.resolve();

    expect(Object.keys(value.snapshot().runtimes[0]!.upstreamRateLimits ?? {}).sort())
      .toEqual(["gpt-5.6-sol", "gpt-5.6-terra"]);
    expect(runtime.calls).toBe(2);
    await value.close();
  });

  it("clears observations on revival and drops late evidence from the old incarnation", async () => {
    const runtime = new Runtime();
    runtime.rateLimits.push(
      { remainingTokens: 0, retryAfterMs: 100 },
      { remainingTokens: 0, retryAfterMs: 200 },
    );
    const value = router(runtime);
    const first = await value.startRelay({ model: "gpt-5.6-sol", input: "first-old" });
    const late = await value.startRelay({ model: "gpt-5.6-sol", input: "late-old" });
    runtime.finals[0]!.resolve({
      status: "failed",
      error: { message: FULL_TPM_MESSAGE, code: "rate_limit_exceeded" },
    });
    await first.final;
    await Promise.resolve();
    expect(value.snapshot().runtimes[0]!.upstreamRateLimit).not.toBeNull();

    runtime.replaceIncarnation();
    expect(value.snapshot().runtimes[0]!.upstreamRateLimit).toBeNull();
    runtime.finals[1]!.resolve({
      status: "failed",
      error: { message: FULL_TPM_MESSAGE, code: "rate_limit_exceeded" },
    });
    await late.final;
    await Promise.resolve();
    expect(value.snapshot().runtimes[0]!.upstreamRateLimit).toBeNull();
    expect(value.snapshot().runtimes[0]!.upstreamOrganization).toBeNull();
    await value.close();
  });

  it("maps a committed upstream 429 without any local cooldown wrapper", async () => {
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
