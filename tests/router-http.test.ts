import { afterEach, describe, expect, it } from "bun:test";
import { WebSocket } from "ws";
import { Agent, request as httpRequest, type Server } from "node:http";
import type { RunHandle } from "../src/relay/run-handle";
import { parseFrames, sseFrame } from "../src/relay/sse";
import {
  MultiSandboxRouter,
  type IncomingResourceLease,
} from "../src/router/router";
import { ResourceGovernor } from "../src/router/resource-governor";
import { classifyWorkload } from "../src/router/workload";
import {
  SandboxRegistry,
  type RoutableSandboxRuntime,
  type RoutableSandboxSnapshot,
} from "../src/sandbox/registry";
import { projectId } from "../src/sandbox/types";
import { createGhostProvApp } from "../src/server/app";
import { createGhostProvServer } from "../src/server/runtime";
import { closeResponsesWebSockets } from "../src/ws/server";
import { V2CommittedError, V2RunHandle } from "../src/v2/transport/run-handle";
import { createProviderErrorEnvelope } from "../src/v2/provider-error";
import { resolveHttpClientId } from "../src/http/request-lease";
import { AtomicBytePermitPool } from "../src/v2/memory/atomic-byte-permits";
import { v2EventPhysicalWorkingSetBytes } from "../src/v2/memory/event-memory-sizing";

const servers: Server[] = [];

afterEach(async () => {
  await Promise.all(servers.splice(0).map(server => new Promise<void>(resolve => server.close(() => resolve()))));
});

function final(id: string, output: unknown[] = []) {
  return { id, object: "response", status: "completed", model: "gpt-5.6-sol", output };
}

function handle(value: any, frames: Array<{ event: string; data: string }> = []): RunHandle {
  return {
    async *events() { for (const frame of frames) yield frame; },
    final: Promise.resolve(value),
    stats: { bytes: 0, firstByteMs: null, frames: frames.length },
  };
}

function healthRouter(overrides: Array<Partial<RoutableSandboxSnapshot>>): MultiSandboxRouter {
  const runtimes = overrides.map((override, index): RoutableSandboxRuntime => {
    const snapshot: RoutableSandboxSnapshot = {
      projectId: projectId(`00000000-0000-4000-8000-${String(index + 1).padStart(12, "0")}`),
      state: "ready",
      capacity: 30,
      active: 0,
      available: 30,
      quarantined: 0,
      heartbeatAgeMs: 1_000,
      credentialExpiresInMs: 3_600_000,
      ackEwmaMs: 1_000,
      completionEwmaMs: 10_000,
      recentFailureRate: 0,
      queueDepth: 0,
      memoryPressure: 0,
      failure: null,
      ...override,
    };
    return {
      projectId: snapshot.projectId,
      async start() {},
      async drain() {},
      async stop() {},
      snapshot: () => snapshot,
      async run() { return handle(final(`resp_${index}`)); },
    };
  });
  return new MultiSandboxRouter({
    registry: new SandboxRegistry(runtimes),
    governor: new ResourceGovernor({
      probe: {
        sample: () => ({
          totalBytes: 16 * 1024 ** 3,
          freeBytes: 12 * 1024 ** 3,
          rssBytes: 256 * 1024 ** 2,
          heapBytes: 64 * 1024 ** 2,
          externalBytes: 8 * 1024 ** 2,
          logicalProcessors: 8,
        }),
      },
    }),
  });
}

class FakeRouter {
  workloads: Array<{
    body: Record<string, unknown>;
    source?: string;
    clientId?: string;
    incomingLease?: IncomingResourceLease;
  }> = [];
  bodyReservations: number[] = [];
  bodyReservationGrowths: number[] = [];
  bodyLeases: IncomingResourceLease[] = [];
  bodyReservationActive = 0;
  bodyReservationReleases = 0;
  async acquireIncomingBody(bytes: number) {
    this.bodyReservations.push(bytes);
    this.bodyReservationActive++;
    let released = false;
    const lease = {
      ensureBytes: async (nextBytes: number) => {
        this.bodyReservationGrowths.push(nextBytes);
      },
      release: () => {
        if (released) return;
        released = true;
        this.bodyReservationActive--;
        this.bodyReservationReleases++;
      },
    };
    this.bodyLeases.push(lease);
    return lease;
  }
  async startRelay(body: Record<string, unknown>, options: {
    source?: string;
    clientId?: string;
    incomingLease?: IncomingResourceLease;
  } = {}): Promise<RunHandle> {
    expect(this.bodyReservationActive).toBeGreaterThanOrEqual(0);
    this.workloads.push({ body, ...options });
    if (Array.isArray(body.tools) && (body.tools as any[])[0]?.type === "image_generation") {
      return handle(final(`resp_image_${this.workloads.length}`, [{
        type: "image_generation_call",
        status: "completed",
        result: `PNG-${this.workloads.length}`,
        background: "opaque",
        output_format: "png",
        quality: "low",
        size: "1024x1024",
      }]));
    }
    const response = final(`resp_${this.workloads.length}`);
    return handle(response, [{
      event: "response.completed",
      data: JSON.stringify({ type: "response.completed", response }),
    }]);
  }
  snapshot() {
    return { capacity: 120, active: 7, available: 113, queued: 2, runtimes: [], resources: {} };
  }
}

describe("router HTTP composition", () => {
  it("uses bounded privacy-safe HTTP fairness identities", () => {
    const connection = { httpConnectionClientId: "http-connection-42" };
    expect(resolveHttpClientId(connection)).toBe("http-connection-42");
    expect(resolveHttpClientId(connection)).toBe("http-connection-42");

    const explicit = resolveHttpClientId(connection, "customer-session-alpha");
    expect(explicit).toMatch(/^http-explicit-\d{4}$/);
    expect(explicit).toBe(resolveHttpClientId({}, "customer-session-alpha"));
    expect(explicit).not.toContain("customer-session-alpha");
    expect(resolveHttpClientId(connection, "x".repeat(129))).toBe("http-connection-42");

    const buckets = new Set(Array.from({ length: 5_000 }, (_, index) => (
      resolveHttpClientId({}, `client-${index}`)
    )));
    expect(buckets.size).toBeLessThanOrEqual(1_024);
    expect(Math.max(...[...buckets].map(value => value.length))).toBeLessThanOrEqual(32);
  });

  it("rejects keepalive intervals that cannot be scheduled safely", () => {
    const router = new FakeRouter();
    for (const keepAliveMs of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, 2_147_483_648]) {
      expect(() => createGhostProvApp({ router, keepAliveMs })).toThrow(RangeError);
    }
    expect(() => createGhostProvApp({ router, keepAliveMs: 1 })).not.toThrow();
    expect(() => createGhostProvApp({ router, keepAliveMs: 2_147_483_647 })).not.toThrow();
  });

  it("drains bounded relay events for non-streaming responses", async () => {
    const responseFinal = final("resp_nonstream_bounded");
    const bounded = new V2RunHandle({ maxBufferedBytes: 128 });
    const producer = (async () => {
      for (let sequence = 0; sequence < 32; sequence++) {
        await bounded.acceptEvent({
          event: "response.output_text.delta",
          data: JSON.stringify({
            type: "response.output_text.delta",
            sequence_number: sequence,
            delta: "bounded-event",
          }),
        });
      }
      bounded.settle(responseFinal);
    })();
    const router = {
      snapshot: () => ({ capacity: 1, active: 0, available: 1, queued: 0, runtimes: [], resources: {} }),
      startRelay: async () => bounded,
    };

    const pending = createGhostProvApp({ router }).request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "gpt-5.6-sol", input: "x" }),
    });
    const response = await Promise.race([
      pending,
      new Promise<null>(resolve => setTimeout(() => resolve(null), 500)),
    ]);

    expect(response).not.toBeNull();
    if (!response) return;
    await producer;
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual(responseFinal);
  });

  it("transfers event memory into the HTTP chunk until the reader advances", async () => {
    const frame = {
      event: "response.completed",
      data: JSON.stringify({ type: "response.completed", response: final("resp_owned_http") }),
    };
    const wireBytes = new TextEncoder().encode(sseFrame(frame.event, frame.data)).byteLength;
    const coverage = v2EventPhysicalWorkingSetBytes(wireBytes);
    const permits = new AtomicBytePermitPool({ ordinaryBytes: coverage, jumboBytes: 1 });
    const owned = new V2RunHandle({});
    await owned.acceptEvent(
      frame,
      wireBytes,
      (await permits.acquire(coverage)).transfer("http-run-handle-test"),
    );
    owned.settle(final("resp_owned_http"));
    const router = {
      snapshot: () => ({ capacity: 1, active: 0, available: 1, queued: 0, runtimes: [], resources: {} }),
      startRelay: async () => owned,
    };
    const response = await createGhostProvApp({ router }).request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "gpt-5.6-sol", input: "x", stream: true }),
    });
    const reader = response.body!.getReader();
    const first = await reader.read();
    expect(first.done).toBe(false);
    expect(permits.snapshot().ordinaryUsedBytes).toBe(coverage);
    expect((await reader.read()).done).toBe(true);
    await Bun.sleep(0);
    expect(permits.snapshot().ordinaryUsedBytes).toBe(0);
    reader.releaseLock();
    permits.close();
  });

  it("waits for the R2 upstream prelude and preserves an initial provider 429", async () => {
    const providerError = await createProviderErrorEnvelope({
      origin: "http",
      status: 429,
      model: "gpt-5.6-sol",
      headers: new Headers({
        "retry-after": "0.136",
        "x-ratelimit-limit-tokens": "40000000",
        "x-ratelimit-remaining-tokens": "0",
        "x-request-id": "req_prelude_429",
      }),
      value: {
        error: {
          message: "Rate limit reached for gpt-5.6-sol in organization org-Test123",
          type: "rate_limit_error",
          param: null,
          code: "rate_limit_exceeded",
        },
      },
    });
    const upstream = Object.assign(new Error(providerError.error.message), {
      providerError,
    });
    const router = {
      snapshot: () => ({ capacity: 1, active: 0, available: 1, queued: 0, runtimes: [], resources: {} }),
      startRelay: async (): Promise<RunHandle> => ({
        async *events() {},
        final: new Promise(() => undefined),
        upstreamPrelude: Promise.reject(upstream),
        stats: { bytes: 0, firstByteMs: null, frames: 0 },
      }),
    };

    const response = await createGhostProvApp({ router }).request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "gpt-5.6-sol", input: "x", stream: true }),
    });

    expect(response.status).toBe(429);
    expect(response.headers.get("retry-after")).toBe("0.136");
    expect(response.headers.get("x-ratelimit-limit-tokens")).toBe("40000000");
    expect(response.headers.get("x-request-id")).toBe("req_prelude_429");
    expect(await response.json()).toEqual({ error: providerError.error });
  });

  it("does not expose streaming response headers before the R2 success prelude", async () => {
    let resolvePrelude!: (value: {
      status: number;
      contentType: string;
      upstreamRequestId: string | null;
    }) => void;
    const prelude = new Promise<{
      status: number;
      contentType: string;
      upstreamRequestId: string | null;
    }>(resolve => { resolvePrelude = resolve; });
    const responseFinal = final("resp_prelude_success");
    const router = {
      snapshot: () => ({ capacity: 1, active: 0, available: 1, queued: 0, runtimes: [], resources: {} }),
      startRelay: async (): Promise<RunHandle> => ({
        async *events() {
          yield {
            event: "response.completed",
            data: JSON.stringify({ type: "response.completed", response: responseFinal }),
          };
        },
        final: Promise.resolve(responseFinal),
        upstreamPrelude: prelude,
        stats: { bytes: 0, firstByteMs: null, frames: 1 },
      }),
    };
    const pending = Promise.resolve(createGhostProvApp({ router }).request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "gpt-5.6-sol", input: "x", stream: true }),
    }));
    const early = await Promise.race([
      pending.then(() => "response" as const),
      new Promise<"waiting">(resolve => setTimeout(() => resolve("waiting"), 20)),
    ]);
    expect(early).toBe("waiting");

    resolvePrelude({
      status: 200,
      contentType: "text/event-stream",
      upstreamRequestId: "req_prelude_success",
    });
    const response = await pending;
    expect(response.status).toBe(200);
    expect(response.headers.get("x-request-id")).toBe("req_prelude_success");
    expect((await response.text())).toContain("event: response.completed");
  });

  it("keeps a provider SSE error exact after the successful prelude committed HTTP 200", async () => {
    const providerError = await createProviderErrorEnvelope({
      origin: "sse",
      status: 429,
      model: "gpt-5.6-sol",
      value: {
        type: "error",
        error: {
          message: "Rate limit reached in organization org-Late123",
          type: "rate_limit_error",
          param: null,
          code: "rate_limit_exceeded",
        },
      },
    });
    const realHandle = new V2RunHandle({
      maxBufferedBytes: 4_096,
      expectUpstreamPrelude: true,
    });
    const router = {
      snapshot: () => ({ capacity: 1, active: 0, available: 1, queued: 0, runtimes: [], resources: {} }),
      startRelay: async () => realHandle,
    };
    const pending = Promise.resolve(createGhostProvApp({ router }).request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "gpt-5.6-sol", input: "x", stream: true }),
    }));
    realHandle.acceptUpstreamPrelude({
      status: 200,
      contentType: "text/event-stream",
      upstreamRequestId: "req_late_error",
    });
    const response = await pending;
    await realHandle.acceptEvent({
      event: "response.output_text.delta",
      data: JSON.stringify({
        type: "response.output_text.delta",
        delta: "valid prefix",
      }),
    });
    realHandle.fail(new V2CommittedError(
      providerError.error.message,
      providerError.error.code ?? "upstream_error",
      false,
      { retryAfterMs: 136, remainingTokens: 0 },
      providerError,
    ));

    expect(response.status).toBe(200);
    const frames = parseFrames(await response.text(), 0).frames;
    expect(frames).toHaveLength(2);
    expect(frames[0]).toMatchObject({
      event: "response.output_text.delta",
      data: JSON.stringify({
        type: "response.output_text.delta",
        delta: "valid prefix",
      }),
    });
    expect(frames[1]!.event).toBe("error");
    expect(JSON.parse(frames[1]!.data)).toEqual({
      type: "error",
      error: providerError.error,
    });
  });

  it("aborts non-streaming relay work when event draining fails before final settles", async () => {
    let relaySignal: AbortSignal | undefined;
    let iteratorClosed = false;
    const streamHandle: RunHandle = {
      async *events() {
        try {
          throw new DOMException("event drain failed", "AbortError");
        } finally {
          iteratorClosed = true;
        }
      },
      final: new Promise(() => undefined),
      stats: { bytes: 0, firstByteMs: null, frames: 0 },
    };
    const router = {
      snapshot: () => ({ capacity: 1, active: 0, available: 1, queued: 0, runtimes: [], resources: {} }),
      startRelay: async (
        _body: Record<string, unknown>,
        options: { signal?: AbortSignal } = {},
      ) => {
        relaySignal = options.signal;
        return streamHandle;
      },
    };

    const pending = createGhostProvApp({ router }).request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "gpt-5.6-sol", input: "x" }),
    });
    const response = await Promise.race([
      pending,
      new Promise<null>(resolve => setTimeout(() => resolve(null), 500)),
    ]);

    expect(response).not.toBeNull();
    if (!response) return;
    expect(response.status).toBe(499);
    expect(relaySignal?.aborted).toBe(true);
    expect(iteratorClosed).toBe(true);
  });

  it("aborts and returns a waiting non-stream iterator when final rejects", async () => {
    let relaySignal: AbortSignal | undefined;
    let returnCalled = false;
    let rejectFinal!: (error: unknown) => void;
    const finalPromise = new Promise<never>((_resolve, reject) => { rejectFinal = reject; });
    const waitingIterator = {
      next: () => new Promise<IteratorResult<{ event: string; data: string }, void>>(() => undefined),
      return: async () => {
        returnCalled = true;
        return { done: true as const, value: undefined };
      },
      throw: async (error?: unknown) => { throw error; },
      [Symbol.asyncIterator]() { return this; },
    } as unknown as AsyncGenerator<{ event: string; data: string }, void>;
    const streamHandle: RunHandle = {
      events: () => waitingIterator,
      final: finalPromise,
      stats: { bytes: 0, firstByteMs: null, frames: 0 },
    };
    const router = {
      snapshot: () => ({ capacity: 1, active: 0, available: 1, queued: 0, runtimes: [], resources: {} }),
      startRelay: async (
        _body: Record<string, unknown>,
        options: { signal?: AbortSignal } = {},
      ) => {
        relaySignal = options.signal;
        setTimeout(() => rejectFinal(new DOMException("final failed", "AbortError")), 0);
        return streamHandle;
      },
    };

    const pending = createGhostProvApp({ router }).request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "gpt-5.6-sol", input: "x" }),
    });
    const response = await Promise.race([
      pending,
      new Promise<null>(resolve => setTimeout(() => resolve(null), 500)),
    ]);

    expect(response).not.toBeNull();
    if (!response) return;
    expect(response.status).toBe(499);
    expect(relaySignal?.aborted).toBe(true);
    expect(returnCalled).toBe(true);
  });

  it("bounds SSE pull-ahead and aborts the relay when the response body is cancelled", async () => {
    let produced = 0;
    let iteratorClosed = false;
    let relaySignal: AbortSignal | undefined;
    const streamHandle: RunHandle = {
      async *events() {
        try {
          for (let sequence = 0; sequence < 2_000; sequence++) {
            produced++;
            yield {
              event: "response.output_text.delta",
              data: JSON.stringify({
                type: "response.output_text.delta",
                sequence_number: sequence,
                delta: "x",
              }),
            };
          }
        } finally {
          iteratorClosed = true;
        }
      },
      final: new Promise(() => undefined),
      stats: { bytes: 0, firstByteMs: null, frames: 0 },
    };
    const router = {
      snapshot: () => ({ capacity: 1, active: 0, available: 1, queued: 0, runtimes: [], resources: {} }),
      startRelay: async (
        _body: Record<string, unknown>,
        options: { signal?: AbortSignal } = {},
      ) => {
        relaySignal = options.signal;
        return streamHandle;
      },
    };
    const response = await createGhostProvApp({ router }).request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "gpt-5.6-sol", input: "x", stream: true }),
    });

    await new Promise(resolve => setTimeout(resolve, 10));
    expect(produced).toBe(0);
    await response.body!.cancel("test cancellation");
    await new Promise(resolve => setTimeout(resolve, 0));
    expect(relaySignal?.aborted).toBe(true);
    // With a zero stream high-water mark the async generator never starts, so
    // its body/finally correctly never runs; cancellation still aborts relay.
    expect(iteratorClosed).toBe(false);
  });

  it("aborts and returns the SSE iterator when streaming fails", async () => {
    let relaySignal: AbortSignal | undefined;
    let returnCalled = false;
    const failedIterator = {
      next: async () => { throw new DOMException("stream failed", "AbortError"); },
      return: async () => {
        returnCalled = true;
        return { done: true as const, value: undefined };
      },
      throw: async (error?: unknown) => { throw error; },
      [Symbol.asyncIterator]() { return this; },
    } as unknown as AsyncGenerator<{ event: string; data: string }, void>;
    const streamHandle: RunHandle = {
      events: () => failedIterator,
      final: new Promise(() => undefined),
      stats: { bytes: 0, firstByteMs: null, frames: 0 },
    };
    const router = {
      snapshot: () => ({ capacity: 1, active: 0, available: 1, queued: 0, runtimes: [], resources: {} }),
      startRelay: async (
        _body: Record<string, unknown>,
        options: { signal?: AbortSignal } = {},
      ) => {
        relaySignal = options.signal;
        return streamHandle;
      },
    };
    const response = await createGhostProvApp({ router }).request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "gpt-5.6-sol", input: "x", stream: true }),
    });

    const text = await response.text();
    expect(text).toContain("event: error");
    expect(relaySignal?.aborted).toBe(true);
    expect(returnCalled).toBe(true);
  });

  it("routes HTTP and image fan-out through one router with source metadata", async () => {
    const router = new FakeRouter();
    const app = createGhostProvApp({
      router,
      imageOptions: { responsesModel: "gpt-5.6-sol", maxEditInputBytes: 40 * 1024 ** 2 },
    });

    const response = await app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "gpt-5.6-sol", input: "hello" }),
    });
    const image = await app.request("/v1/images/generations", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "gpt-image-2", prompt: "circle", n: 2, quality: "low" }),
    });

    expect(response.status).toBe(200);
    expect(image.status).toBe(200);
    expect(router.workloads.map(workload => workload.source)).toEqual(["http", "image", "image"]);
    expect(router.workloads[0]!.clientId).toMatch(/^http-request-\d{4}$/);
    expect(router.workloads[1]!.clientId).toMatch(/^http-request-\d{4}$/);
    expect(router.workloads[2]!.clientId).toBe(router.workloads[1]!.clientId);
    expect(router.workloads.map(workload => workload.clientId)).not.toContain("http");
    expect(router.workloads.map(workload => workload.clientId)).not.toContain("images");
  });

  it.each(["completed", "incomplete", "failed"] as const)(
    "forwards one response.%s terminal without fabricating completed",
    async status => {
      const responseFinal = { ...final(`resp_${status}`), status };
      const terminal = `response.${status}`;
      const router = {
        snapshot: () => ({ capacity: 1, active: 0, available: 1, queued: 0, runtimes: [], resources: {} }),
        startRelay: async () => handle(responseFinal, [{
          event: terminal,
          data: JSON.stringify({
            type: terminal,
            sequence_number: 4,
            response: responseFinal,
          }),
        }]),
      };
      const appResponse = await createGhostProvApp({ router }).request("/v1/responses", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ model: "gpt-5.6-sol", input: "x", stream: true }),
      });
      const frames = parseFrames(await appResponse.text(), 0).frames;

      expect(frames.map(frame => frame.event)).toEqual([terminal]);
    },
  );

  it.each(["completed", "incomplete", "failed"] as const)(
    "synthesizes a missing response.%s terminal with the next sequence",
    async status => {
      const responseFinal = { ...final(`resp_missing_${status}`), status };
      const terminal = `response.${status}`;
      const router = {
        snapshot: () => ({ capacity: 1, active: 0, available: 1, queued: 0, runtimes: [], resources: {} }),
        startRelay: async () => handle(responseFinal, [{
          event: "response.output_text.delta",
          data: JSON.stringify({
            type: "response.output_text.delta",
            sequence_number: 7,
            delta: "x",
          }),
        }]),
      };
      const appResponse = await createGhostProvApp({ router }).request("/v1/responses", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ model: "gpt-5.6-sol", input: "x", stream: true }),
      });
      const frames = parseFrames(await appResponse.text(), 0).frames;
      const synthesized = JSON.parse(frames.at(-1)!.data);

      expect(frames.map(frame => frame.event)).toEqual([
        "response.output_text.delta",
        terminal,
      ]);
      expect(synthesized).toEqual({
        type: terminal,
        sequence_number: 8,
        response: responseFinal,
      });
    },
  );

  it("reports aggregate capacity and sanitized runtime snapshots", async () => {
    const router = new FakeRouter();
    const app = createGhostProvApp({ router });
    const health = await (await app.request("/health")).json();
    const sandboxes = await (await app.request("/__sandboxes")).json();
    expect(health).toMatchObject({ ok: true, capacity: 120, active: 7, available: 113, queued: 2 });
    expect((health as any).release).toEqual({
      knownRuntimes: 0,
      unknownRuntimes: 0,
      selections: [],
    });
    expect(JSON.stringify(sandboxes)).not.toMatch(/token|cookie/i);
  });

  it("reports exact verified release identities and explicit unknown runtimes", async () => {
    const buildHash = "a".repeat(64);
    const schemaHash = "b".repeat(64);
    const router = healthRouter([
      { releaseRevision: "r3", protocolRevision: "r2", buildHash, schemaHash },
      {
        token: "credential-canary",
        sandboxSessionId: "session-canary",
        instanceId: "instance-canary",
      } as any,
    ]);
    try {
      const app = createGhostProvApp({ router });
      const health = await (await app.request("/health")).json() as any;
      const sandboxes = await (await app.request("/__sandboxes")).json() as any;
      expect(health.release).toEqual({
        knownRuntimes: 1,
        unknownRuntimes: 1,
        selections: [{ releaseRevision: "r3", protocolRevision: "r2", buildHash, schemaHash, runtimes: 1 }],
      });
      expect(sandboxes.release).toEqual(health.release);
      expect(sandboxes.runtimes).toEqual(expect.arrayContaining([
        expect.objectContaining({ releaseRevision: "r3", protocolRevision: "r2", buildHash, schemaHash }),
        expect.objectContaining({ releaseRevision: null, protocolRevision: null, buildHash: null, schemaHash: null }),
      ]));
      expect(JSON.stringify({ health, sandboxes })).not.toMatch(
        /sandboxSessionId|instanceId|authorization|ySweetToken/i,
      );
      expect(JSON.stringify(sandboxes)).not.toContain("credential-canary");
    } finally {
      await router.close();
    }
  });

  it("reports partial fleet unavailability and background recovery truth", async () => {
    const unavailableProject = projectId("00000000-0000-4000-8000-000000000099");
    const readyProject = projectId("00000000-0000-4000-8000-000000000098");
    let recovered = false;
    const router = healthRouter([{}]);
    const fleetSnapshot = () => ({
      desired: 2,
      ready: recovered ? 2 : 1,
      provisioning: 0,
      draining: 0,
      failed: recovered ? 0 : 1,
      unavailable: recovered ? 0 : 1,
      unavailableProjects: recovered ? [] : [unavailableProject],
      projects: [unavailableProject, readyProject],
    });
    try {
      const app = createGhostProvApp({ router, fleetSnapshot });
      expect(await (await app.request("/health")).json()).toMatchObject({
        ok: true,
        desired: 2,
        ready: 1,
        unavailable: 1,
        unavailableProjects: [unavailableProject],
        unavailableProjectsTruncated: false,
      });
      expect(await (await app.request("/__sandboxes")).json()).toMatchObject({
        desired: 2,
        ready: 1,
        unavailable: 1,
        unavailableProjects: [unavailableProject],
      });
      recovered = true;
      expect(await (await app.request("/health")).json()).toMatchObject({
        desired: 2,
        ready: 2,
        unavailable: 0,
        unavailableProjects: [],
      });
    } finally {
      await router.close();
    }
  });

  it("reports 503 and zero capacity when every ready runtime heartbeat is stale", async () => {
    const router = healthRouter([
      { heartbeatAgeMs: null },
      { heartbeatAgeMs: 20_001 },
    ]);
    try {
      const response = await createGhostProvApp({ router }).request("/health");
      expect(response.status).toBe(503);
      expect(await response.json()).toMatchObject({ ok: false, capacity: 0 });
    } finally {
      await router.close();
    }
  });

  it("reports a healthy full runtime as capacity even without an immediately free slot", async () => {
    const router = healthRouter([{ active: 30, available: 0 }]);
    try {
      const response = await createGhostProvApp({ router }).request("/health");
      expect(response.status).toBe(200);
      expect(await response.json()).toMatchObject({
        ok: true,
        capacity: 30,
        active: 30,
        available: 0,
      });
    } finally {
      await router.close();
    }
  });

  it("serves a Codex-compatible models list without contacting a sandbox", async () => {
    const router = new FakeRouter();
    const response = await createGhostProvApp({ router }).request(
      "/v1/models?client_version=0.149.0",
    );
    const body = await response.json() as any;

    expect(response.status).toBe(200);
    expect(body).toMatchObject({ object: "list" });
    expect(body.data.map((model: any) => model.id)).toEqual(expect.arrayContaining([
      "gpt-5.6-sol",
      "gpt-5.6-terra",
      "gpt-5.6-luna",
      "gpt-5.5",
      "gpt-image-2",
    ]));
    expect(body.models.map((model: any) => model.slug)).toEqual([
      "gpt-5.6-sol",
      "gpt-5.6-terra",
      "gpt-5.6-luna",
      "gpt-5.5",
    ]);
    expect(body.models.every((model: any) => (
      typeof model.base_instructions === "string" &&
      model.base_instructions.length > 0 &&
      model.service_tiers.some((tier: any) => tier.id === "priority")
    ))).toBe(true);
    expect(router.workloads).toHaveLength(0);
  });

  it("reports an unhealthy status when no sandbox capacity is routable", async () => {
    const router = new FakeRouter();
    router.snapshot = () => ({ capacity: 0, active: 0, available: 0, queued: 0, runtimes: [], resources: {} });
    const response = await createGhostProvApp({ router }).request("/health");
    expect(response.status).toBe(503);
    expect(await response.json()).toMatchObject({ ok: false, capacity: 0 });
  });

  it("refuses candidate mode on protected port 8787", () => {
    const router = new FakeRouter();
    const app = createGhostProvApp({ router });
    expect(() => createGhostProvServer({ app, router, candidate: true, port: 8787 })).toThrow("protected");
  });

  it("routes Codex WebSocket work through the same router", async () => {
    const router = new FakeRouter();
    const app = createGhostProvApp({ router });
    const server = createGhostProvServer({ app, router, candidate: true, port: 0 });
    servers.push(server);
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("missing address");
    const socket = new WebSocket(`ws://127.0.0.1:${address.port}/v1/responses`);
    await new Promise<void>((resolve, reject) => {
      socket.once("open", resolve);
      socket.once("error", reject);
    });
    const completed = new Promise<any>((resolve, reject) => {
      socket.on("message", data => {
        const event = JSON.parse(data.toString());
        if (event.type === "response.completed") resolve(event);
        if (event.type === "error") reject(new Error(JSON.stringify(event)));
      });
    });
    socket.send(JSON.stringify({
      type: "response.create",
      stream_id: "candidate",
      model: "gpt-5.6-sol",
      store: false,
      input: "hello websocket",
    }));
    await completed;
    expect(router.workloads.at(-1)?.source).toBe("websocket");
    expect(router.workloads.at(-1)?.clientId).toMatch(/^ws-/);
    expect(router.workloads.at(-1)?.clientId).not.toBe("websocket");
    socket.close();
  });

  it("atomically hands the HTTP body reservation to workload admission", async () => {
    const MiB = 1024 ** 2;
    const governor = new ResourceGovernor({
      probe: {
        sample: () => ({
          totalBytes: 16 * 1024 ** 3,
          freeBytes: 12 * 1024 ** 3,
          rssBytes: 256 * MiB,
          heapBytes: 64 * MiB,
          externalBytes: 8 * MiB,
          logicalProcessors: 8,
        }),
      },
    });
    const reservationCapacity = governor.snapshot().reservationCapacityBytes;
    const blocker = await governor.acquire({
      class: "text-large",
      requestBytes: 0,
      reservedBytes: reservationCapacity - 8 * MiB,
      cpuPermits: 0,
      stream: false,
    });
    const id = projectId("00000000-0000-4000-8000-000000000099");
    const runtime: RoutableSandboxRuntime = {
      projectId: id,
      async start() {},
      async drain() {},
      async stop() {},
      snapshot: () => ({
        projectId: id,
        state: "ready",
        capacity: 1,
        active: 0,
        available: 1,
        quarantined: 0,
        heartbeatAgeMs: 0,
        credentialExpiresInMs: 60_000,
        ackEwmaMs: 1,
        completionEwmaMs: 1,
        recentFailureRate: 0,
        queueDepth: 0,
        memoryPressure: 0,
        failure: null,
      }),
      async run() { return handle(final("resp_atomic_handoff")); },
    };
    const router = new MultiSandboxRouter({
      registry: new SandboxRegistry([runtime]),
      governor,
    });
    const app = createGhostProvApp({ router });
    const server = createGhostProvServer({ app, router, candidate: true, port: 0 });
    servers.push(server);
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("missing address");

    try {
      const body = JSON.stringify({ model: "gpt-5.6-sol", input: "handoff" });
      const response = await Promise.race([
        fetch(`http://127.0.0.1:${address.port}/v1/responses`, {
          method: "POST",
          headers: {
            "content-type": "application/json",
            "content-length": String(Buffer.byteLength(body)),
          },
          body,
        }),
        new Promise<never>((_, reject) => setTimeout(
          () => reject(new Error("HTTP reservation was nested instead of replaced")),
          500,
        )),
      ]);
      expect(response.status).toBe(200);
      expect(await response.json()).toMatchObject({ id: "resp_atomic_handoff" });
      await new Promise(resolve => setTimeout(resolve, 0));
      expect(governor.snapshot().reservedBytes).toBe(blocker.reservedBytes);
    } finally {
      await router.close();
      blocker.release();
      governor.close();
    }
  });

  it("grows unknown ingress accounting from the small floor under one deadline", async () => {
    const MiB = 1024 ** 2;
    const router = healthRouter([]);
    const ingress = await router.acquireIncomingBody(0, undefined, { maxBytes: 64 * MiB });
    try {
      expect(ingress.reservedBytes).toBe(8 * MiB);
      expect(ingress.deadlineMs! - ingress.acceptedAtMs!).toBe(30_000);
      await ingress.ensureBytes!(4 * MiB);
      expect(ingress.reservedBytes).toBe(16 * MiB);
      expect(router.snapshot().resources.reservedBytes).toBe(16 * MiB);
      await ingress.ensureBytes!(6 * MiB);
      expect(ingress.reservedBytes).toBe(16 * MiB);
      await ingress.ensureBytes!(8 * MiB);
      expect(ingress.reservedBytes).toBe(32 * MiB);
    } finally {
      ingress.release();
      await router.close();
    }
    expect(router.snapshot().resources.reservedBytes).toBe(0);
  });

  it("hands one image batch ingress lease to exactly one concurrent fan-out job", async () => {
    const router = new FakeRouter();
    const app = createGhostProvApp({ router });
    const server = createGhostProvServer({ app, router, candidate: true, port: 0 });
    servers.push(server);
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("missing address");
    const body = JSON.stringify({
      model: "gpt-image-2",
      prompt: "two circles",
      n: 2,
    });

    const response = await fetch(`http://127.0.0.1:${address.port}/v1/images/generations`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "content-length": String(Buffer.byteLength(body)),
      },
      body,
    });

    expect(response.status).toBe(200);
    expect((await response.json() as any).data).toHaveLength(2);
    const imageCalls = router.workloads.filter(call => call.source === "image");
    expect(imageCalls).toHaveLength(2);
    expect(imageCalls.filter(call => call.incomingLease !== undefined)).toHaveLength(1);
    expect(imageCalls[0].incomingLease).toBe(router.bodyLeases[0]);
    expect(imageCalls[1].incomingLease).toBeUndefined();
    expect(router.bodyReservationReleases).toBe(1);
    expect(router.bodyReservationActive).toBe(0);
    expect(router.workloads.at(-1)?.clientId).toMatch(/^http-connection-\d{1,4}$/);
  });

  it("keeps retained image ingress memory inside the first execution reservation", async () => {
    const MiB = 1024 ** 2;
    const governor = new ResourceGovernor({
      probe: {
        sample: () => ({
          totalBytes: 16 * 1024 ** 3,
          freeBytes: 12 * 1024 ** 3,
          rssBytes: 256 * MiB,
          heapBytes: 64 * MiB,
          externalBytes: 8 * MiB,
          logicalProcessors: 8,
        }),
      },
    });
    const id = projectId("00000000-0000-4000-8000-000000000100");
    let settleFinal!: (value: ReturnType<typeof final>) => void;
    const pendingFinal = new Promise<ReturnType<typeof final>>(resolve => {
      settleFinal = resolve;
    });
    const runtime: RoutableSandboxRuntime = {
      projectId: id,
      async start() {},
      async drain() {},
      async stop() {},
      snapshot: () => ({
        projectId: id,
        state: "ready",
        capacity: 1,
        active: 0,
        available: 1,
        quarantined: 0,
        heartbeatAgeMs: 0,
        credentialExpiresInMs: 60_000,
        ackEwmaMs: 1,
        completionEwmaMs: 1,
        recentFailureRate: 0,
        queueDepth: 0,
        memoryPressure: 0,
        failure: null,
      }),
      async run() { return handle(pendingFinal); },
    };
    const router = new MultiSandboxRouter({
      registry: new SandboxRegistry([runtime]),
      governor,
    });
    const body = {
      model: "gpt-5.6-sol",
      input: "make an image",
      tools: [{ type: "image_generation" }],
    };
    const incoming = await router.acquireIncomingBody(1024);
    const execution = classifyWorkload(body);

    try {
      const relay = await router.startRelay(body, {
        source: "image",
        incomingLease: incoming,
      });
      expect(governor.snapshot().reservedBytes).toBe(
        execution.reservedBytes + 8 * MiB,
      );
      settleFinal(final("resp_image_memory"));
      await relay.final;
      await new Promise(resolve => setTimeout(resolve, 0));
      expect(governor.snapshot().reservedBytes).toBe(0);
    } finally {
      settleFinal(final("resp_image_memory_cleanup"));
      incoming.release();
      await router.close();
      governor.close();
    }
  });

  it("releases an unconsumed ingress lease when image parsing fails", async () => {
    const router = new FakeRouter();
    const app = createGhostProvApp({ router });
    const server = createGhostProvServer({ app, router, candidate: true, port: 0 });
    servers.push(server);
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("missing address");

    const response = await fetch(`http://127.0.0.1:${address.port}/v1/images/generations`, {
      method: "POST",
      headers: { "content-type": "application/json", "content-length": "1" },
      body: "{",
    });

    expect(response.status).toBe(400);
    expect(router.workloads).toHaveLength(0);
    expect(router.bodyReservationReleases).toBe(1);
    expect(router.bodyReservationActive).toBe(0);
  });

  it("closes owned WebSockets with a restart code after graceful drain", async () => {
    const router = new FakeRouter();
    const app = createGhostProvApp({ router });
    const server = createGhostProvServer({ app, router, candidate: true, port: 0 });
    servers.push(server);
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("missing address");
    const socket = new WebSocket(`ws://127.0.0.1:${address.port}/v1/responses`);
    await new Promise<void>((resolve, reject) => {
      socket.once("open", resolve);
      socket.once("error", reject);
    });
    const closed = new Promise<number>(resolve => socket.once("close", code => resolve(code)));

    await closeResponsesWebSockets(server, { timeoutMs: 1_000 });
    expect(await closed).toBe(1012);
  });

  it("reserves HTTP body memory before buffering and releases it after parsing", async () => {
    const router = new FakeRouter();
    const app = createGhostProvApp({ router });
    const server = createGhostProvServer({ app, router, candidate: true, port: 0, maxBodyBytes: 64 * 1024 ** 2 });
    servers.push(server);
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("missing address");
    const body = JSON.stringify({ model: "gpt-5.6-sol", input: "large-enough".repeat(1000) });

    const response = await fetch(`http://127.0.0.1:${address.port}/v1/responses`, {
      method: "POST",
      headers: { "content-type": "application/json", "content-length": String(Buffer.byteLength(body)) },
      body,
    });
    expect(response.status).toBe(200);
    expect(router.bodyReservations).toEqual([Buffer.byteLength(body)]);
    expect(router.bodyReservationReleases).toBe(1);
    expect(router.bodyReservationActive).toBe(0);
  });

  it("starts chunked ingress small and grows it only to observed bytes", async () => {
    const router = new FakeRouter();
    const app = createGhostProvApp({ router });
    const server = createGhostProvServer({ app, router, candidate: true, port: 0 });
    servers.push(server);
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("missing address");
    const body = JSON.stringify({ model: "gpt-5.6-sol", input: "tiny chunked body" });

    const response = await new Promise<{ status: number; body: string }>((resolve, reject) => {
      const request = httpRequest({
        hostname: "127.0.0.1",
        port: address.port,
        path: "/v1/responses",
        method: "POST",
        headers: {
          "content-type": "application/json",
          "transfer-encoding": "chunked",
        },
      }, incoming => {
        let responseBody = "";
        incoming.setEncoding("utf8");
        incoming.on("data", chunk => { responseBody += chunk; });
        incoming.once("end", () => resolve({
          status: incoming.statusCode ?? 0,
          body: responseBody,
        }));
      });
      request.once("error", reject);
      request.write(body.slice(0, 7));
      request.end(body.slice(7));
    });

    expect(response.status).toBe(200);
    expect(JSON.parse(response.body)).toMatchObject({ status: "completed" });
    expect(router.bodyReservations).toEqual([0]);
    expect(router.bodyReservationGrowths.at(-1)).toBe(Buffer.byteLength(body));
    expect(Math.max(...router.bodyReservationGrowths)).toBeLessThan(1024);
  });

  it("interrupts a stalled HTTP upload so server shutdown cannot hang", async () => {
    const router = healthRouter([]);
    const app = createGhostProvApp({ router });
    const server = createGhostProvServer({ app, router, candidate: true, port: 0 });
    servers.push(server);
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("missing address");

    const request = httpRequest({
      hostname: "127.0.0.1",
      port: address.port,
      path: "/v1/responses",
      method: "POST",
      headers: {
        connection: "close",
        "content-type": "application/json",
        "content-length": "1024",
      },
    });
    request.on("error", () => undefined);
    request.write("{");
    try {
      for (let turn = 0; router.snapshot().resources.reservedBytes === 0 && turn < 100; turn++) {
        await Bun.sleep(1);
      }
      expect(router.snapshot().resources.reservedBytes).toBeGreaterThan(0);
      const serverClosed = new Promise<void>((resolve, reject) => {
        server.close(error => error ? reject(error) : resolve());
      });
      router.beginShutdown();
      await Promise.race([
        serverClosed,
        Bun.sleep(500).then(() => { throw new Error("stalled upload blocked server shutdown"); }),
      ]);
      await router.awaitIdle(500);
    } finally {
      request.destroy();
      await router.close();
    }
  });

  it("keeps one bounded fairness identity across a persistent HTTP connection", async () => {
    const router = new FakeRouter();
    const app = createGhostProvApp({ router });
    const server = createGhostProvServer({ app, router, candidate: true, port: 0 });
    servers.push(server);
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("missing address");
    const agent = new Agent({ keepAlive: true, maxSockets: 1 });
    const body = JSON.stringify({ model: "gpt-5.6-sol", input: "identity" });
    const send = (explicit?: string) => new Promise<void>((resolve, reject) => {
      const request = httpRequest({
        hostname: "127.0.0.1",
        port: address.port,
        path: "/v1/responses",
        method: "POST",
        agent,
        headers: {
          "content-type": "application/json",
          "content-length": Buffer.byteLength(body),
          ...(explicit ? { "x-ghostprov-client-id": explicit } : {}),
        },
      }, response => {
        response.resume();
        response.once("end", resolve);
      });
      request.once("error", reject);
      request.end(body);
    });

    try {
      await send();
      await send();
      expect(router.workloads[0]!.clientId).toMatch(/^http-connection-\d{1,4}$/);
      expect(router.workloads[1]!.clientId).toBe(router.workloads[0]!.clientId);
      await send("caller-session-must-not-escape");
      expect(router.workloads[2]!.clientId).toBe(
        resolveHttpClientId({}, "caller-session-must-not-escape"),
      );
      expect(router.workloads[2]!.clientId).not.toContain("caller-session-must-not-escape");
    } finally {
      agent.destroy();
    }
  });

  it("propagates a downstream HTTP disconnect to the relay signal", async () => {
    let markStarted!: () => void;
    let markAborted!: () => void;
    const started = new Promise<void>(resolve => { markStarted = resolve; });
    const aborted = new Promise<void>(resolve => { markAborted = resolve; });
    const router = {
      snapshot: () => ({ capacity: 1, active: 0, available: 1, queued: 0, runtimes: [], resources: {} }),
      startRelay: (_body: Record<string, unknown>, options: { signal?: AbortSignal } = {}) => {
        markStarted();
        return new Promise<RunHandle>((_resolve, reject) => {
          const fallback = setTimeout(() => reject(new Error("disconnect signal not propagated")), 1_000);
          options.signal?.addEventListener("abort", () => {
            clearTimeout(fallback);
            markAborted();
            reject(new DOMException("aborted", "AbortError"));
          }, { once: true });
        });
      },
    };
    const app = createGhostProvApp({ router });
    const server = createGhostProvServer({ app, router, candidate: true, port: 0 });
    servers.push(server);
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("missing address");
    const body = JSON.stringify({ model: "gpt-5.6-sol", input: "disconnect" });
    const client = httpRequest({
      hostname: "127.0.0.1",
      port: address.port,
      path: "/v1/responses",
      method: "POST",
      headers: { "content-type": "application/json", "content-length": Buffer.byteLength(body) },
    });
    client.on("error", () => undefined);
    client.end(body);
    await started;
    client.destroy();

    await Promise.race([
      aborted,
      new Promise((_, reject) => setTimeout(() => reject(new Error("abort not observed")), 300)),
    ]);
  });
});
