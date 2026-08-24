import { afterEach, describe, expect, it } from "bun:test";
import { WebSocket } from "ws";
import { request as httpRequest, type Server } from "node:http";
import type { RunHandle } from "../src/relay/run-handle";
import { parseFrames } from "../src/relay/sse";
import { MultiSandboxRouter } from "../src/router/router";
import { ResourceGovernor } from "../src/router/resource-governor";
import {
  SandboxRegistry,
  type RoutableSandboxRuntime,
  type RoutableSandboxSnapshot,
} from "../src/sandbox/registry";
import { projectId } from "../src/sandbox/types";
import { createGhostProvApp } from "../src/server/app";
import { createGhostProvServer } from "../src/server/runtime";

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
  workloads: Array<{ body: Record<string, unknown>; source?: string; clientId?: string }> = [];
  bodyReservations: number[] = [];
  bodyReservationActive = 0;
  bodyReservationReleases = 0;
  async acquireIncomingBody(bytes: number) {
    this.bodyReservations.push(bytes);
    this.bodyReservationActive++;
    let released = false;
    return {
      release: () => {
        if (released) return;
        released = true;
        this.bodyReservationActive--;
        this.bodyReservationReleases++;
      },
    };
  }
  async startRelay(body: Record<string, unknown>, options: { source?: string; clientId?: string } = {}): Promise<RunHandle> {
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
    socket.close();
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
