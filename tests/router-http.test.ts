import { afterEach, describe, expect, it } from "bun:test";
import { WebSocket } from "ws";
import { Agent, request as httpRequest, type Server } from "node:http";
import { createConnection } from "node:net";
import type { RunHandle } from "../src/relay/run-handle";
import { parseFrames, sseFrame, type SseFrame } from "../src/relay/sse";
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
import { ResponsesHttpContinuationIndex } from "../src/http/responses-continuation";
import { ResponseProvenanceObsoleteError } from "../src/router/provenance";
import {
  createAuthenticatedTenantScope,
  type AuthenticatedTenantScope,
} from "../src/http/api-key";
import { AtomicBytePermitPool } from "../src/v2/memory/atomic-byte-permits";
import { v2EventPhysicalWorkingSetBytes } from "../src/v2/memory/event-memory-sizing";
import { V2_R3_RELEASE } from "../src/v2/sidecar/assets";

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
    authorizationScope: createAuthenticatedTenantScope(),
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
  authorizationScope = createAuthenticatedTenantScope();
  workloads: Array<{
    body: Record<string, unknown>;
    source?: string;
    clientId?: string;
    incomingLease?: IncomingResourceLease;
    authorizationScope?: AuthenticatedTenantScope;
    encodedBodyBytes?: number;
  }> = [];
  bodyReservations: number[] = [];
  bodyReservationGrowths: number[] = [];
  bodyLeases: IncomingResourceLease[] = [];
  bodyReservationActive = 0;
  bodyReservationReleases = 0;
  responseProvenanceAuthorizationScope() {
    return this.authorizationScope;
  }
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
    authorizationScope?: AuthenticatedTenantScope;
    encodedBodyBytes?: number;
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
    expect(() => createGhostProvApp({
      router,
      authorizationScope: {} as AuthenticatedTenantScope,
    })).toThrow(TypeError);
    const foreignScope = createAuthenticatedTenantScope();
    expect(() => createGhostProvApp({
      router,
      authorizationScope: foreignScope,
    })).toThrow("router's exact AuthenticatedTenantScope");
    const app = createGhostProvApp({ router });
    expect(() => createGhostProvServer({
      app,
      router,
      authorizationScope: foreignScope,
      candidate: true,
      port: 0,
    })).toThrow("router's exact AuthenticatedTenantScope");
    const otherRouter = new FakeRouter();
    expect(() => createGhostProvServer({
      app,
      router: otherRouter,
      authorizationScope: otherRouter.authorizationScope,
      candidate: true,
      port: 0,
    })).toThrow("app, HTTP router, and WebSocket router");
  });

  it.each([[null], [[]], ["text"], [42], [true]])(
    "rejects non-object Responses JSON roots locally: %p",
    async root => {
      const router = new FakeRouter();
      const app = createGhostProvApp({
        router,
        authorizationScope: router.authorizationScope,
      });

      const response = await app.fetch(new Request("http://localhost/v1/responses", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(root),
      }));

      expect(response.status).toBe(400);
      expect(await response.json()).toMatchObject({
        error: {
          type: "invalid_request_error",
          param: "body",
          code: "invalid_type",
        },
      });
      expect(router.workloads).toHaveLength(0);
    },
  );

  for (const regression of [
    {
      name: "malformed nested function arguments",
      invalidOutput: {
        type: "function_call",
        id: "fc_invalid_prefix",
        status: "completed",
        call_id: "call_invalid_prefix",
        name: "read",
        arguments: "{\"path\":",
      },
      expectedCode: "invalid_function_call_arguments",
      expectedParam: "input[1].arguments",
    },
    {
      name: "a lone Unicode surrogate",
      invalidOutput: {
        type: "message",
        id: "msg_invalid_prefix",
        status: "completed",
        role: "assistant",
        content: [{
          type: "output_text",
          text: "\ud800",
          annotations: [],
          logprobs: [],
        }],
      },
      expectedCode: "invalid_unicode",
      expectedParam: "input[1]",
    },
  ]) {
    it(`validates native full history before prefix inference can strip ${regression.name}`, async () => {
      const authorizationScope = createAuthenticatedTenantScope();
      const continuationIndex = new ResponsesHttpContinuationIndex({
        key: new Uint8Array(32).fill(0x43),
      });
      const calls: Record<string, unknown>[] = [];
      const root = {
        type: "message",
        role: "user",
        content: [{ type: "input_text", text: "root" }],
      };
      const next = {
        type: "message",
        role: "user",
        content: [{ type: "input_text", text: "next" }],
      };
      const router = {
        responseProvenanceAuthorizationScope: () => authorizationScope,
        snapshot: () => ({
          capacity: 1,
          active: 0,
          available: 1,
          queued: 0,
          runtimes: [],
          resources: {},
        }),
        startRelay: async (body: Record<string, unknown>) => {
          calls.push(structuredClone(body));
          return handle(final("resp_invalid_prefix", calls.length === 1
            ? [regression.invalidOutput]
            : []));
        },
      };
      const app = createGhostProvApp({
        router,
        authorizationScope,
        responsesContinuationIndex: continuationIndex,
      });

      const seeded = await app.request("/v1/responses", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ model: "gpt-5.6-sol", input: [root] }),
      });
      expect(seeded.status).toBe(200);
      await seeded.json();
      expect(continuationIndex.snapshot().records).toBe(1);

      const rejected = await app.request("/v1/responses", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          model: "gpt-5.6-sol",
          input: [root, regression.invalidOutput, next],
        }),
      });

      expect(rejected.status).toBe(400);
      expect(await rejected.json()).toMatchObject({
        error: {
          type: "invalid_request_error",
          param: regression.expectedParam,
          code: regression.expectedCode,
        },
      });
      expect(calls).toHaveLength(1);
    });
  }

  it.each(["gpt-5.6-sol", "gpt-6-astra"])("forces upstream persistence and streaming while preserving %s and the public nonstream contract", async model => {
    const router = new FakeRouter();
    const authorizationScope = createAuthenticatedTenantScope();
    router.authorizationScope = authorizationScope;
    const app = createGhostProvApp({
      router,
      authorizationScope,
    });

    const publicBody = {
      model,
      input: "persist this upstream",
      stream: false,
      store: false,
      previous_response_id: "resp_parent_exact",
    };
    const publicJson = JSON.stringify(publicBody);
    const incomingLease: IncomingResourceLease = {
      bodyBytes: Buffer.byteLength(publicJson),
      release() {},
    };
    const response = await app.fetch(new Request("http://localhost/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: publicJson,
    }), { incomingBodyLease: incomingLease });

    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toContain("application/json");
    expect(await response.json()).toMatchObject({ status: "completed" });
    expect(router.workloads[0]!.authorizationScope).toBe(authorizationScope);
    const expectedUpstreamBody = {
      model,
      input: "persist this upstream",
      stream: true,
      store: true,
      previous_response_id: "resp_parent_exact",
    };
    expect(router.workloads[0]!.body).toEqual(expectedUpstreamBody);
    expect(router.workloads[0]!.encodedBodyBytes).toBe(
      Buffer.byteLength(JSON.stringify(expectedUpstreamBody)),
    );
  });

  it("converts an exact native Responses full history into a stored suffix", async () => {
    const authorizationScope = createAuthenticatedTenantScope();
    const calls: Array<{
      body: Record<string, unknown>;
      options: Record<string, unknown>;
    }> = [];
    const firstUser = {
      type: "message",
      role: "user",
      content: [{ type: "input_text", text: "first" }],
    };
    const answer = {
      type: "message",
      id: "msg_parent",
      status: "completed",
      role: "assistant",
      content: [{
        type: "output_text",
        text: "answer",
        annotations: [],
        logprobs: [],
      }],
    };
    const nextUser = {
      type: "message",
      role: "user",
      content: [{ type: "input_text", text: "next" }],
    };
    const router = {
      responseProvenanceAuthorizationScope: () => authorizationScope,
      snapshot: () => ({
        capacity: 1,
        active: 0,
        available: 1,
        queued: 0,
        runtimes: [],
        resources: {},
      }),
      startRelay: async (
        body: Record<string, unknown>,
        options: Record<string, unknown> = {},
      ) => {
        calls.push({ body: structuredClone(body), options });
        return handle(final(
          calls.length === 1 ? "resp_full_parent" : "resp_full_child",
          calls.length === 1 ? [answer] : [],
        ));
      },
    };
    const app = createGhostProvApp({ router, authorizationScope });

    const first = await app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "gpt-5.6-sol",
        stream: false,
        input: [firstUser],
      }),
    });
    expect(first.status).toBe(200);
    await first.json();

    const second = await app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "gpt-5.6-sol",
        stream: false,
        input: [firstUser, answer, nextUser],
      }),
    });
    expect(second.status).toBe(200);
    await second.json();

    expect(calls).toHaveLength(2);
    expect(calls[1]!.body).toMatchObject({
      stream: true,
      store: true,
      previous_response_id: "resp_full_parent",
      input: [nextUser],
    });
    expect(calls[1]!.options.authorizationScope).toBe(authorizationScope);
    expect(Number(calls[1]!.options.fairnessRequestBytes)).toBeGreaterThan(
      Buffer.byteLength(JSON.stringify(nextUser)),
    );
  });

  it("commits a native Responses prefix before exposing an SSE terminal", async () => {
    const authorizationScope = createAuthenticatedTenantScope();
    const continuationIndex = new ResponsesHttpContinuationIndex({
      key: new Uint8Array(32).fill(0x41),
    });
    const commit = continuationIndex.commit.bind(continuationIndex);
    let releaseCommit!: () => void;
    let commitStarted = false;
    const commitGate = new Promise<void>(resolve => { releaseCommit = resolve; });
    continuationIndex.commit = async (...args) => {
      commitStarted = true;
      await commitGate;
      return commit(...args);
    };
    const calls: Record<string, unknown>[] = [];
    const root = {
      type: "message",
      role: "user",
      content: [{ type: "input_text", text: "root" }],
    };
    const answer = {
      type: "message",
      id: "msg_sse_parent",
      status: "completed",
      role: "assistant",
      content: [{
        type: "output_text",
        text: "answer",
        annotations: [],
        logprobs: [],
      }],
    };
    const parent = final("resp_sse_parent", [answer]);
    let resolveParent!: (value: Record<string, unknown>) => void;
    const parentFinal = new Promise<Record<string, unknown>>(resolve => {
      resolveParent = resolve;
    });
    const router = {
      responseProvenanceAuthorizationScope: () => authorizationScope,
      snapshot: () => ({
        capacity: 1,
        active: 0,
        available: 1,
        queued: 0,
        runtimes: [],
        resources: {},
      }),
      startRelay: async (body: Record<string, unknown>) => {
        calls.push(structuredClone(body));
        if (calls.length === 1) {
          return handle(parentFinal, [{
            event: "response.completed",
            data: JSON.stringify({ type: "response.completed", response: parent }),
          }]);
        }
        return handle(final("resp_sse_child"));
      },
    };
    const app = createGhostProvApp({
      router,
      authorizationScope,
      responsesContinuationIndex: continuationIndex,
    });
    const streamed = await app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "gpt-5.6-sol", stream: true, input: [root] }),
    });
    const reader = streamed.body!.getReader();
    let terminalVisible = false;
    const terminal = reader.read().then(value => {
      terminalVisible = true;
      return value;
    });
    await Bun.sleep(10);
    expect(terminalVisible).toBe(false);

    resolveParent(parent);
    for (let turn = 0; turn < 50 && !commitStarted; turn++) await Bun.sleep(1);
    expect(commitStarted).toBe(true);
    await Bun.sleep(10);
    expect(terminalVisible).toBe(false);
    releaseCommit();
    expect(new TextDecoder().decode((await terminal).value)).toContain("response.completed");
    await reader.cancel();

    const next = {
      type: "message",
      role: "user",
      content: [{ type: "input_text", text: "next" }],
    };
    const continued = await app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "gpt-5.6-sol",
        stream: false,
        input: [root, answer, next],
      }),
    });
    expect(continued.status).toBe(200);
    await continued.json();
    expect(calls[1]).toMatchObject({
      previous_response_id: "resp_sse_parent",
      input: [next],
    });
  });

  it("commits native Responses metadata before exposing a JSON final", async () => {
    const authorizationScope = createAuthenticatedTenantScope();
    const continuationIndex = new ResponsesHttpContinuationIndex({
      key: new Uint8Array(32).fill(0x42),
    });
    const commit = continuationIndex.commit.bind(continuationIndex);
    let releaseCommit!: () => void;
    let commitStarted = false;
    const commitGate = new Promise<void>(resolve => { releaseCommit = resolve; });
    continuationIndex.commit = async (...args) => {
      commitStarted = true;
      await commitGate;
      return commit(...args);
    };
    const router = {
      responseProvenanceAuthorizationScope: () => authorizationScope,
      snapshot: () => ({
        capacity: 1,
        active: 0,
        available: 1,
        queued: 0,
        runtimes: [],
        resources: {},
      }),
      startRelay: async () => handle(final("resp_json_commit", [{
        type: "message",
        id: "msg_json_commit",
        status: "completed",
        role: "assistant",
        content: [{ type: "output_text", text: "done", annotations: [], logprobs: [] }],
      }])),
    };
    const app = createGhostProvApp({
      router,
      authorizationScope,
      responsesContinuationIndex: continuationIndex,
    });

    let responseSettled = false;
    const response = Promise.resolve(app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "gpt-5.6-sol", input: [{
        type: "message",
        role: "user",
        content: [{ type: "input_text", text: "wait for commit" }],
      }] }),
    })).then((value: Response) => {
      responseSettled = true;
      return value;
    });

    for (let turn = 0; turn < 50 && !commitStarted; turn++) await Bun.sleep(1);
    expect(commitStarted).toBe(true);
    expect(responseSettled).toBe(false);
    releaseCommit();
    expect((await response).status).toBe(200);
  });

  it("retries one definitive stale auto-inferred Responses parent with full history", async () => {
    const authorizationScope = createAuthenticatedTenantScope();
    const providerError = await createProviderErrorEnvelope({
      origin: "http",
      status: 400,
      value: {
        error: {
          message: "Previous response was not found",
          type: "invalid_request_error",
          param: "previous_response_id",
          code: "previous_response_not_found",
        },
      },
    });
    const stale = new V2CommittedError(
      providerError.error.message,
      providerError.error.code!,
      false,
      undefined,
      providerError,
    );
    const root = {
      type: "message",
      role: "user",
      content: [{ type: "input_text", text: "root" }],
    };
    const answer = {
      type: "message",
      id: "msg_stale_parent",
      status: "completed",
      role: "assistant",
      content: [{
        type: "output_text",
        text: "answer",
        annotations: [],
        logprobs: [],
      }],
    };
    const next = {
      type: "message",
      role: "user",
      content: [{ type: "input_text", text: "next" }],
    };
    const calls: Record<string, unknown>[] = [];
    const router = {
      responseProvenanceAuthorizationScope: () => authorizationScope,
      snapshot: () => ({
        capacity: 1,
        active: 0,
        available: 1,
        queued: 0,
        runtimes: [],
        resources: {},
      }),
      startRelay: async (body: Record<string, unknown>) => {
        calls.push(structuredClone(body));
        if (calls.length === 1) return handle(final("resp_stale_parent", [answer]));
        if (calls.length === 2) throw stale;
        return handle(final("resp_rehydrated"));
      },
    };
    const app = createGhostProvApp({ router, authorizationScope });
    await (await app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "gpt-5.6-sol", stream: false, input: [root] }),
    })).json();

    const response = await app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "gpt-5.6-sol",
        stream: false,
        input: [root, answer, next],
      }),
    });
    expect(response.status).toBe(200);
    await response.json();

    expect(calls).toHaveLength(3);
    expect(calls[1]).toMatchObject({
      previous_response_id: "resp_stale_parent",
      input: [next],
    });
    expect(calls[2]).not.toHaveProperty("previous_response_id");
    expect(calls[2]!.input).toEqual([root, answer, next]);
  });

  it("retries one locally obsolete inferred parent but preserves an explicit caller ID", async () => {
    const authorizationScope = createAuthenticatedTenantScope();
    const root = { type: "message", role: "user", content: "root" };
    const answer = {
      type: "message",
      id: "msg_locally_obsolete_parent",
      status: "completed",
      role: "assistant",
      content: [{ type: "output_text", text: "answer", annotations: [], logprobs: [] }],
    };
    const next = { type: "message", role: "user", content: "next" };
    const calls: Record<string, unknown>[] = [];
    const router = {
      responseProvenanceAuthorizationScope: () => authorizationScope,
      snapshot: () => ({
        capacity: 1,
        active: 0,
        available: 1,
        queued: 0,
        runtimes: [],
        resources: {},
      }),
      startRelay: async (body: Record<string, unknown>) => {
        calls.push(structuredClone(body));
        if (calls.length === 1) {
          return handle(final("resp_locally_obsolete_parent", [answer]));
        }
        if (body.previous_response_id === "resp_locally_obsolete_parent") {
          throw new ResponseProvenanceObsoleteError();
        }
        if (body.previous_response_id === "resp_explicit_caller") {
          throw new ResponseProvenanceObsoleteError();
        }
        return handle(final("resp_locally_rehydrated"));
      },
    };
    const app = createGhostProvApp({ router, authorizationScope });
    await (await app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "gpt-5.6-sol", input: [root] }),
    })).json();

    const response = await app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "gpt-5.6-sol",
        input: [root, answer, next],
      }),
    });
    expect(response.status).toBe(200);
    await response.json();
    expect(calls[1]).toMatchObject({
      previous_response_id: "resp_locally_obsolete_parent",
      input: [next],
    });
    expect(calls[2]).not.toHaveProperty("previous_response_id");
    expect(calls[2]!.input).toEqual([root, answer, next]);

    const explicit = await app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "gpt-5.6-sol",
        previous_response_id: "resp_explicit_caller",
        input: [next],
      }),
    });
    expect(explicit.status).toBe(409);
    expect(calls).toHaveLength(4);
    expect(calls[3]).toMatchObject({
      previous_response_id: "resp_explicit_caller",
      input: [next],
    });
  });

  it("lets every concurrent definitive stale inferred Responses hit retry its own immutable full history once", async () => {
    const authorizationScope = createAuthenticatedTenantScope();
    const providerError = await createProviderErrorEnvelope({
      origin: "http",
      status: 400,
      value: { error: {
        message: "Previous response was not found",
        type: "invalid_request_error",
        param: "previous_response_id",
        code: "previous_response_not_found",
      } },
    });
    const stale = new V2CommittedError(
      providerError.error.message,
      providerError.error.code!,
      false,
      undefined,
      providerError,
    );
    const root = { type: "message", role: "user", content: "root" };
    const answer = {
      type: "message",
      id: "msg_concurrent_stale_parent",
      status: "completed",
      role: "assistant",
      content: [{ type: "output_text", text: "answer", annotations: [], logprobs: [] }],
    };
    let staleArrivals = 0;
    let releaseStale!: () => void;
    const bothStale = new Promise<void>(resolve => { releaseStale = resolve; });
    const calls: Record<string, unknown>[] = [];
    const router = {
      responseProvenanceAuthorizationScope: () => authorizationScope,
      snapshot: () => ({
        capacity: 2,
        active: 0,
        available: 2,
        queued: 0,
        runtimes: [],
        resources: {},
      }),
      startRelay: async (body: Record<string, unknown>) => {
        calls.push(structuredClone(body));
        if (calls.length === 1) {
          return handle(final("resp_concurrent_stale_parent", [answer]));
        }
        if (Object.hasOwn(body, "previous_response_id")) {
          staleArrivals += 1;
          if (staleArrivals === 2) releaseStale();
          await bothStale;
          throw stale;
        }
        return handle(final(`resp_concurrent_fallback_${calls.length}`));
      },
    };
    const app = createGhostProvApp({ router, authorizationScope });
    await (await app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "gpt-5.6-sol", input: [root] }),
    })).json();

    const histories = ["next-a", "next-b"].map(text => [
      root,
      answer,
      { type: "message", role: "user", content: text },
    ]);
    const responses = await Promise.all(histories.map(input => app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "gpt-5.6-sol", stream: false, input }),
    })));

    expect(responses.map(response => response.status)).toEqual([200, 200]);
    await Promise.all(responses.map(response => response.json()));
    const inferred = calls.filter(body => Object.hasOwn(body, "previous_response_id"));
    const fullFallbacks = calls.slice(1)
      .filter(body => !Object.hasOwn(body, "previous_response_id"));
    expect(inferred).toHaveLength(2);
    expect(fullFallbacks).toHaveLength(2);
    expect(fullFallbacks.map(body => body.input)).toEqual(expect.arrayContaining(histories));
    expect(calls).toHaveLength(5);
  });

  it("never retries a stale inferred parent after any accepted relay event", async () => {
    const authorizationScope = createAuthenticatedTenantScope();
    const providerError = await createProviderErrorEnvelope({
      origin: "http",
      status: 400,
      value: { error: {
        message: "Previous response was not found",
        type: "invalid_request_error",
        param: "previous_response_id",
        code: "previous_response_not_found",
      } },
    });
    const stale = new V2CommittedError(
      providerError.error.message,
      providerError.error.code!,
      false,
      undefined,
      providerError,
    );
    const root = { type: "message", role: "user", content: "root" };
    const answer = {
      type: "message",
      id: "msg_no_replay",
      status: "completed",
      role: "assistant",
      content: [{ type: "output_text", text: "answer", annotations: [], logprobs: [] }],
    };
    const next = { type: "message", role: "user", content: "next" };
    const calls: Record<string, unknown>[] = [];
    const router = {
      responseProvenanceAuthorizationScope: () => authorizationScope,
      snapshot: () => ({
        capacity: 1,
        active: 0,
        available: 1,
        queued: 0,
        runtimes: [],
        resources: {},
      }),
      startRelay: async (body: Record<string, unknown>) => {
        calls.push(structuredClone(body));
        if (calls.length === 1) return handle(final("resp_no_replay_parent", [answer]));
        return handle(Promise.reject(stale), [{
          event: "response.created",
          data: JSON.stringify({ type: "response.created" }),
        }]);
      },
    };
    const app = createGhostProvApp({ router, authorizationScope });
    await (await app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "gpt-5.6-sol", input: [root] }),
    })).json();

    const response = await app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "gpt-5.6-sol", input: [root, answer, next] }),
    });
    expect(response.status).toBe(400);
    expect(await response.json()).toMatchObject({
      error: { code: "previous_response_not_found" },
    });
    expect(calls).toHaveLength(2);
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

  it("adapts a definitive provider rate limit to a retryable Responses SSE terminal", async () => {
    const message = "Rate limit reached in organization org-Late123. " +
      "Please try again in 136ms.";
    const providerError = await createProviderErrorEnvelope({
      origin: "sse",
      status: 429,
      model: "gpt-5.6-sol",
      value: {
        type: "error",
        error: {
          message,
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
    expect(frames[1]!.event).toBe("response.failed");
    expect(JSON.parse(frames[1]!.data)).toEqual({
      type: "response.failed",
      status: 429,
      response: {
        status: "failed",
        error: providerError.error,
      },
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

  it("settles native non-stream cancellation when final and events ignore abort", async () => {
    let returnCalled = false;
    const waitingIterator = {
      next: () => new Promise<IteratorResult<SseFrame, void>>(() => undefined),
      return: async () => {
        returnCalled = true;
        return { done: true as const, value: undefined };
      },
      [Symbol.asyncIterator]() { return this; },
    } as unknown as AsyncGenerator<SseFrame, void>;
    const router = {
      snapshot: () => ({
        capacity: 1,
        active: 0,
        available: 1,
        queued: 0,
        runtimes: [],
        resources: {},
      }),
      startRelay: async (): Promise<RunHandle> => ({
        events: () => waitingIterator,
        final: new Promise(() => undefined),
        stats: { bytes: 0, firstByteMs: null, frames: 0 },
      }),
    };
    const app = createGhostProvApp({ router });
    const controller = new AbortController();
    const pending = app.fetch(new Request("http://localhost/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "gpt-5.6-sol", input: "cancel" }),
      signal: controller.signal,
    }));

    await Bun.sleep(0);
    controller.abort(new DOMException("cancelled", "AbortError"));
    const response = await Promise.race([
      pending,
      Bun.sleep(250).then(() => null),
    ]);
    expect(response).not.toBeNull();
    expect(response?.status).toBe(499);
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

  for (const wait of ["iterator", "final"] as const) {
    it(`wakes a pending native SSE ${wait} wait when the request aborts`, async () => {
      let returnCalled = false;
      const iterator = {
        next: () => wait === "iterator"
          ? new Promise<IteratorResult<SseFrame, void>>(() => undefined)
          : Promise.resolve({ done: true as const, value: undefined }),
        return() {
          returnCalled = true;
          return new Promise<IteratorResult<SseFrame, void>>(() => undefined);
        },
        [Symbol.asyncIterator]() { return this; },
      } as unknown as AsyncGenerator<SseFrame, void>;
      const router = {
        snapshot: () => ({
          capacity: 1,
          active: 0,
          available: 1,
          queued: 0,
          runtimes: [],
          resources: {},
        }),
        startRelay: async (): Promise<RunHandle> => ({
          events: () => iterator,
          final: new Promise(() => undefined),
          stats: { bytes: 0, firstByteMs: null, frames: 0 },
        }),
      };
      const caller = new AbortController();
      const response = await createGhostProvApp({ router, keepAliveMs: 60_000 }).fetch(
        new Request("http://localhost/v1/responses", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ model: "gpt-5.6-sol", input: "x", stream: true }),
          signal: caller.signal,
        }),
      );
      const reader = response.body!.getReader();
      const pending = reader.read();
      await Bun.sleep(0);
      caller.abort(new DOMException("client disconnected", "AbortError"));
      const result = await Promise.race([
        pending,
        Bun.sleep(100).then(() => { throw new Error(`${wait} wait did not wake`); }),
      ]);
      expect(result.done).toBe(true);
      expect(returnCalled).toBe(true);
    });
  }

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
    expect(router.workloads.every(workload => (
      workload.body.stream === true && workload.body.store === true
    ))).toBe(true);
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

  it("withholds an SSE terminal until the authoritative final has settled", async () => {
    const responseFinal = final("resp_terminal_commit_gate");
    let settleFinal!: (value: typeof responseFinal) => void;
    const authoritative = new Promise<typeof responseFinal>(resolve => {
      settleFinal = resolve;
    });
    const router = {
      snapshot: () => ({
        capacity: 1,
        active: 0,
        available: 1,
        queued: 0,
        runtimes: [],
        resources: {},
      }),
      startRelay: async (): Promise<RunHandle> => ({
        async *events() {
          yield {
            event: "response.completed",
            data: JSON.stringify({
              type: "response.completed",
              sequence_number: 3,
              response: responseFinal,
            }),
          };
        },
        final: authoritative,
        stats: { bytes: 0, firstByteMs: 0, frames: 1 },
      }),
    };
    const appResponse = await createGhostProvApp({ router }).request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "gpt-5.6-sol", input: "x", stream: true }),
    });
    const reader = appResponse.body!.getReader();
    const firstRead = reader.read();
    await Promise.resolve();
    expect(await Promise.race([
      firstRead.then(() => "settled" as const),
      Promise.resolve("pending" as const),
    ])).toBe("pending");

    settleFinal(responseFinal);
    const firstChunk = await firstRead;
    expect(new TextDecoder().decode(firstChunk.value)).toContain("event: response.completed");
    await reader.cancel();
  });

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

  it("advances synthesized terminal sequence after an oversized event", async () => {
    const responseFinal = final("resp_missing_after_large_event");
    const router = {
      snapshot: () => ({ capacity: 1, active: 0, available: 1, queued: 0, runtimes: [], resources: {} }),
      startRelay: async () => handle(responseFinal, [{
        event: "response.output_text.delta",
        data: JSON.stringify({
          type: "response.output_text.delta",
          sequence_number: 0,
          delta: "small",
        }),
      }, {
        event: "response.output_text.delta",
        data: JSON.stringify({
          type: "response.output_text.delta",
          sequence_number: 1,
          delta: "x".repeat(70 * 1024),
        }),
      }]),
    };
    const appResponse = await createGhostProvApp({ router }).request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "gpt-5.6-sol", input: "x", stream: true }),
    });
    const frames = parseFrames(await appResponse.text(), 0).frames;

    expect(frames.map(frame => JSON.parse(frame.data).sequence_number)).toEqual([0, 1, 2]);
  });

  it("reports aggregate capacity and sanitized runtime snapshots", async () => {
    const router = new FakeRouter();
    const app = createGhostProvApp({ router });
    const health = await (await app.request("/health")).json();
    const sandboxes = await (await app.request("/__sandboxes")).json();
    expect(health).toMatchObject({ ok: true, capacity: 120, active: 7, available: 113, queued: 2 });
    expect((health as any).release).toBeUndefined();
    expect((sandboxes as any).release).toEqual({
      knownRuntimes: 0,
      unknownRuntimes: 0,
      selections: [],
    });
    expect(JSON.stringify(sandboxes)).not.toMatch(/token|cookie/i);
  });

  it("reports exact verified release identities and explicit unknown runtimes", async () => {
    const buildHash = V2_R3_RELEASE.buildHash;
    const schemaHash = V2_R3_RELEASE.schemaHash;
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
      expect(health.release).toBeUndefined();
      expect(sandboxes.release).toEqual({
        knownRuntimes: 1,
        unknownRuntimes: 1,
        selections: [{ releaseRevision: "r3", protocolRevision: "r2", buildHash, schemaHash, runtimes: 1 }],
      });
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
      pendingRetirements: recovered ? 0 : 1,
      projectMutation: {
        state: recovered ? "closed" as const : "open" as const,
        consecutiveFailures: recovered ? 0 : 2,
        retryInMs: recovered ? null : 5_000,
      },
    });
    try {
      const app = createGhostProvApp({ router, fleetSnapshot });
      expect(await (await app.request("/health")).json()).toMatchObject({
        ok: true,
        desired: 2,
        ready: 1,
        unavailable: 1,
        pendingRetirements: 1,
        projectMutation: { state: "open", consecutiveFailures: 2, retryInMs: 5_000 },
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
      "gpt-6-astra",
      "gpt-5.5",
      "gpt-image-2",
    ]));
    expect(body.models.map((model: any) => model.slug)).toEqual([
      "gpt-5.6-sol",
      "gpt-5.6-terra",
      "gpt-5.6-luna",
      "gpt-6-astra",
      "gpt-5.5",
    ]);
    const astra = body.models.find((model: any) => model.slug === "gpt-6-astra");
    expect(astra.supported_reasoning_levels.map((level: any) => level.effort))
      .toEqual(["low", "medium", "high", "xhigh", "max"]);
    expect(astra).toMatchObject({
      default_reasoning_level: "medium",
      max_context_window: 1_050_000,
      use_responses_lite: false,
    });
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

  it("refuses candidate mode on protected port 18787", () => {
    const router = new FakeRouter();
    const app = createGhostProvApp({ router });
    expect(() => createGhostProvServer({ app, router, candidate: true, port: 18787 })).toThrow("protected");
  });

  it("rejects hostile or missing HTTP authorities before auth and body admission", async () => {
    const apiKey = "test-ghostprov-key-0123456789abcdef";
    const router = new FakeRouter();
    const app = createGhostProvApp({ router, apiKey });
    const server = createGhostProvServer({ app, router, apiKey, candidate: true, port: 0 });
    servers.push(server);
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("missing address");

    const postStatus = (headers: Record<string, string>): Promise<number> => new Promise(
      (resolve, reject) => {
        const body = "{}";
        const request = httpRequest({
          host: "127.0.0.1",
          port: address.port,
          path: "/v1/responses",
          method: "POST",
          headers: {
            authorization: `Bearer ${apiKey}`,
            "content-type": "application/json",
            "content-length": Buffer.byteLength(body),
            ...headers,
          },
        }, response => {
          response.resume();
          response.once("end", () => resolve(response.statusCode ?? 0));
        });
        request.once("error", reject);
        request.end(body);
      },
    );
    const hostileStatus = await postStatus({ host: "attacker.example" });
    const forwardedStatus = await postStatus({ "x-forwarded-host": "attacker.example" });
    const originStatus = await postStatus({ origin: "https://attacker.example" });
    const missingHost = await new Promise<string>((resolve, reject) => {
      const socket = createConnection({ host: "127.0.0.1", port: address.port });
      let response = "";
      socket.setEncoding("utf8");
      socket.once("connect", () => socket.end(
        "POST /v1/responses HTTP/1.0\r\nContent-Length: 2\r\n\r\n{}",
      ));
      socket.on("data", chunk => { response += chunk; });
      socket.once("end", () => resolve(response));
      socket.once("error", reject);
    });

    expect(hostileStatus).toBe(421);
    expect(forwardedStatus).toBe(421);
    expect(originStatus).toBe(403);
    expect(missingHost).toMatch(/^HTTP\/1\.1 421 /);
    expect(router.bodyReservations).toEqual([]);
    expect(router.workloads).toEqual([]);
  });

  it("authenticates sandbox snapshots before body admission", async () => {
    const apiKey = "test-ghostprov-key-0123456789abcdef";
    const router = new FakeRouter();
    const app = createGhostProvApp({ router, apiKey });
    const server = createGhostProvServer({ app, router, apiKey, candidate: true, port: 0 });
    servers.push(server);
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("missing address");

    const response = await fetch(`http://127.0.0.1:${address.port}/__sandboxes`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "{}",
    });

    expect(response.status).toBe(401);
    expect(router.bodyReservations).toEqual([]);
    expect(router.workloads).toEqual([]);
  });

  it("shares the explicit app host allowlist with WebSocket upgrades", async () => {
    const router = new FakeRouter();
    const allowedHosts = ["ghostprov.example"];
    const app = createGhostProvApp({ router, allowedHosts });
    const server = createGhostProvServer({
      app,
      router,
      candidate: true,
      port: 0,
      allowedHosts,
    });
    servers.push(server);
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("missing address");
    const socket = new WebSocket(`ws://127.0.0.1:${address.port}/v1/responses`, {
      headers: {
        host: "ghostprov.example",
        origin: "https://ghostprov.example",
      },
    });
    await new Promise<void>((resolve, reject) => {
      socket.once("open", resolve);
      socket.once("error", reject);
    });
    socket.close();
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
      authorizationScope: createAuthenticatedTenantScope(),
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

  it("retains a streaming request graph while upstream headers are pending", async () => {
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
    const id = projectId("00000000-0000-4000-8000-000000000098");
    const responseFinal = final("resp_stream_request_lifetime");
    let active = 0;
    let markPreludeAccessed!: () => void;
    const preludeAccessed = new Promise<void>(resolve => { markPreludeAccessed = resolve; });
    let resolvePrelude!: (value: {
      status: number;
      contentType: string;
      upstreamRequestId: null;
    }) => void;
    const upstreamPrelude = new Promise<{
      status: number;
      contentType: string;
      upstreamRequestId: null;
    }>(resolve => { resolvePrelude = resolve; });
    let resolveFinal!: (value: typeof responseFinal) => void;
    const terminal = new Promise<typeof responseFinal>(resolve => { resolveFinal = resolve; });
    const runtime: RoutableSandboxRuntime = {
      projectId: id,
      async start() {},
      async drain() {},
      async stop() {},
      snapshot: () => ({
        projectId: id,
        state: "ready",
        capacity: 1,
        active,
        available: Math.max(0, 1 - active),
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
      async run(_body, _signal, options) {
        active = 1;
        options?.onAdmissionReflected?.();
        options?.onExecutionRiskReflected?.();
        return {
          async *events() {},
          get upstreamPrelude() {
            markPreludeAccessed();
            return upstreamPrelude;
          },
          final: terminal.finally(() => { active = 0; }),
          stats: { bytes: 0, firstByteMs: null, frames: 0 },
        };
      },
    };
    const authorizationScope = createAuthenticatedTenantScope();
    const router = new MultiSandboxRouter({
      registry: new SandboxRegistry([runtime]),
      authorizationScope,
      governor,
    });
    const app = createGhostProvApp({ router, authorizationScope });
    const requestBody = JSON.stringify({
      model: "gpt-5.6-sol",
      input: "x".repeat(768 * 1024),
      stream: true,
    });
    const incomingLease = await router.acquireIncomingBody(Buffer.byteLength(requestBody));
    let response: Response | null = null;
    const responsePromise = app.fetch(new Request("http://localhost/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: requestBody,
    }), { incomingBodyLease: incomingLease });

    try {
      let preludeTimer: ReturnType<typeof setTimeout> | undefined;
      try {
        await Promise.race([
          preludeAccessed,
          new Promise<never>((_, reject) => {
            preludeTimer = setTimeout(
              () => reject(new Error("HTTP handler did not await upstream headers")),
              2_000,
            );
          }),
        ]);
      } finally {
        if (preludeTimer !== undefined) clearTimeout(preludeTimer);
      }
      // Once transport commitment returns the handle, the router normally
      // shrinks its request reservation to 4 MiB. The HTTP frame still owns the
      // parsed request while awaiting headers, so its graph lifetime must keep
      // the full reservation instead.
      expect(governor.snapshot().reservedBytes).toBeGreaterThan(4 * MiB);

      resolvePrelude({
        status: 200,
        contentType: "text/event-stream",
        upstreamRequestId: null,
      });
      response = await responsePromise;
      expect(response.status).toBe(200);
      resolveFinal(responseFinal);
      for (let turn = 0; turn < 100 && governor.snapshot().reservedBytes > 0; turn++) {
        await Bun.sleep(1);
      }
      expect(governor.snapshot().reservedBytes).toBe(0);
      await response.body?.cancel();
    } finally {
      resolvePrelude({
        status: 200,
        contentType: "text/event-stream",
        upstreamRequestId: null,
      });
      resolveFinal(responseFinal);
      response ??= await Promise.resolve(responsePromise).catch(() => null);
      await response?.body?.cancel().catch(() => undefined);
      await router.close();
      governor.close();
    }
  });

  it("detaches ingress cancellation while retaining byte ownership", async () => {
    const caller = new AbortController();
    const router = healthRouter([]);
    const ingress = await router.acquireIncomingBody(1024, caller.signal);
    try {
      ingress.completeAdmission?.();
      caller.abort(new DOMException("late caller abort", "AbortError"));
      expect(ingress.signal?.aborted).toBe(false);
      expect(ingress.reservedBytes).toBeGreaterThan(0);
    } finally {
      ingress.release();
      await router.close();
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
    const authorizationScope = createAuthenticatedTenantScope();
    const router = {
      responseProvenanceAuthorizationScope: () => authorizationScope,
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
    const app = createGhostProvApp({ router, authorizationScope });
    const server = createGhostProvServer({
      app,
      router,
      authorizationScope,
      candidate: true,
      port: 0,
    });
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
