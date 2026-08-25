import { afterEach, describe, expect, it } from "bun:test";
import { WebSocket } from "ws";
import { request as httpRequest, type Server } from "node:http";
import type { RunHandle } from "../src/relay/run-handle";
import { parseFrames } from "../src/relay/sse";
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
  bodyLeases: IncomingResourceLease[] = [];
  bodyReservationActive = 0;
  bodyReservationReleases = 0;
  async acquireIncomingBody(bytes: number) {
    this.bodyReservations.push(bytes);
    this.bodyReservationActive++;
    let released = false;
    const lease = {
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
    realHandle.fail(new V2CommittedError(
      providerError.error.message,
      providerError.error.code ?? "upstream_error",
      false,
      { retryAfterMs: 136, remainingTokens: 0 },
      providerError,
    ));

    expect(response.status).toBe(200);
    const frames = parseFrames(await response.text(), 0).frames;
    expect(frames).toHaveLength(1);
    expect(frames[0]!.event).toBe("error");
    expect(JSON.parse(frames[0]!.data)).toEqual({
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
    expect(produced).toBeLessThanOrEqual(2);
    await response.body!.cancel("test cancellation");
    await new Promise(resolve => setTimeout(resolve, 0));
    expect(relaySignal?.aborted).toBe(true);
    expect(iteratorClosed).toBe(true);
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
    expect(JSON.stringify(sandboxes)).not.toMatch(/token|cookie/i);
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
      serializedBody: "",
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
    const execution = classifyWorkload({ ...body, service_tier: "fast" });

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
