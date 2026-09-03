import { describe, expect, it } from "bun:test";
import { ResourceGovernor } from "../src/router/resource-governor";
import {
  reserveHostEventMemoryPool,
  reserveResponseGraphMemory,
  reserveWebSocketIngressMemory,
} from "../src/server/system";
import { AtomicBytePermitPool } from "../src/v2/memory/atomic-byte-permits";
import {
  attachAtomicBytePermit,
  releaseAtomicBytePermit,
} from "../src/v2/memory/permit-carrier";
import {
  ResponseGraph,
  SharedByteBudget,
  SYNTHETIC_WARMUP_ID_PREFIX,
} from "../src/ws/response-graph";
import { createAuthenticatedTenantScope } from "../src/http/api-key";
import { deriveV2HostEventMemorySizing } from "../src/v2/memory/event-memory-sizing";
import { WebSocketProcessBudget } from "../src/ws/session";

function governor() {
  return new ResourceGovernor({
    probe: {
      sample: () => ({
        totalBytes: 16 * 1024 ** 3,
        freeBytes: 12 * 1024 ** 3,
        rssBytes: 256 * 1024 ** 2,
        heapBytes: 64 * 1024 ** 2,
        externalBytes: 16 * 1024 ** 2,
        logicalProcessors: 8,
      }),
    },
  });
}

function resolved(graph: ResponseGraph, input: string) {
  return graph.resolve({
    type: "response.create",
    store: false,
    input,
  }, { authorizationScope: createAuthenticatedTenantScope() });
}

describe("elastic process-budget governor memory", () => {
  it("charges WebSocket ingress and queued bytes only while retained", () => {
    const value = governor();
    const budget = new WebSocketProcessBudget({
      maxSessions: 2,
      maxQueuedBytes: 1024,
      maxIngressBytes: 4096,
    });
    const memory = reserveWebSocketIngressMemory(value, budget, { quantumBytes: 1 });
    expect(memory.maximumBytes).toBe(5120);
    expect(memory.reservedBytes).toBe(0);
    expect(value.snapshot().reservedBytes).toBe(0);

    const session = budget.openSession(3072)!;
    expect(memory.reservedBytes).toBe(3072);
    expect(value.snapshot().reservedBytes).toBe(3072);
    expect(session.tryReserve(1024)).toBe(true);
    expect(memory.reservedBytes).toBe(4096);
    session.release(1024);
    expect(memory.reservedBytes).toBe(3072);
    expect(() => memory.close()).toThrow("1 sessions");
    expect(value.snapshot().reservedBytes).toBe(3072);

    session.close();
    expect(memory.reservedBytes).toBe(0);
    memory.close();
    memory.close();
    expect(value.snapshot().reservedBytes).toBe(0);
    value.close();
  });

  it("backs one shared automatic-promotion guarantee without charging it twice", () => {
    const value = governor();
    const budget = new WebSocketProcessBudget({
      maxSessions: 2,
      maxQueuedBytes: 4096,
      maxIngressBytes: 8192,
    });
    const memory = reserveWebSocketIngressMemory(value, budget, { quantumBytes: 1 });
    const first = budget.openSession(1024, 2048)!;
    const second = budget.openSession(1024, 2048)!;
    expect(memory.reservedBytes).toBe(4096);

    expect(first.tryEnsureIngress(2048)).toBe(true);
    expect(memory.reservedBytes).toBe(4096);
    first.releaseIngress();
    expect(memory.reservedBytes).toBe(4096);

    first.close();
    expect(memory.reservedBytes).toBe(3072);
    second.close();
    expect(memory.reservedBytes).toBe(0);
    memory.close();
    value.close();
  });

  it("preserves the forty-two-session default ingress guarantee", () => {
    const MiB = 1024 ** 2;
    const value = governor();
    const budget = new WebSocketProcessBudget({
      maxSessions: 256,
      maxQueuedBytes: 512 * MiB,
      maxIngressBytes: 768 * MiB,
    });
    const memory = reserveWebSocketIngressMemory(value, budget);
    const sessions = Array.from({ length: 42 }, () => budget.openSession(17 * MiB, 48 * MiB));
    expect(sessions.every(Boolean)).toBe(true);
    expect(memory.reservedBytes).toBe((42 * 17 + 48) * MiB);
    expect(budget.openSession(17 * MiB, 48 * MiB)).toBeNull();

    sessions.forEach(session => session!.close());
    expect(memory.reservedBytes).toBe(0);
    memory.close();
    value.close();
  });

  it("does not poll host memory for every bounded WebSocket frame", () => {
    const MiB = 1024 ** 2;
    let samples = 0;
    const value = new ResourceGovernor({
      probe: {
        sample: () => {
          samples++;
          return {
            totalBytes: 16 * 1024 ** 3,
            freeBytes: 12 * 1024 ** 3,
            rssBytes: 256 * MiB,
            heapBytes: 64 * MiB,
            externalBytes: 16 * MiB,
            logicalProcessors: 8,
          };
        },
      },
    });
    const budget = new WebSocketProcessBudget({
      maxSessions: 256,
      maxQueuedBytes: 512 * MiB,
      maxIngressBytes: 768 * MiB,
    });
    const memory = reserveWebSocketIngressMemory(value, budget);
    const session = budget.openSession(17 * MiB, 48 * MiB)!;
    expect(session.tryReserve(1024)).toBe(true);
    session.release(1024);
    const afterWarmup = samples;

    for (let frame = 0; frame < 10_000; frame++) {
      if (!session.tryReserve(1024)) throw new Error("Frame reservation unexpectedly failed");
      session.release(1024);
    }
    expect(samples).toBe(afterWarmup);
    expect(memory.reservedBytes).toBe(66 * MiB);

    session.close();
    expect(memory.reservedBytes).toBe(0);
    memory.close();
    value.close();
  });

  it("rejects WebSocket growth before retaining bytes when host headroom disappears", () => {
    const MiB = 1024 ** 2;
    let freeBytes = 12 * 1024 ** 3;
    const value = new ResourceGovernor({
      probe: {
        sample: () => ({
          totalBytes: 16 * 1024 ** 3,
          freeBytes,
          rssBytes: 256 * MiB,
          heapBytes: 64 * MiB,
          externalBytes: 16 * MiB,
          logicalProcessors: 8,
        }),
      },
    });
    const budget = new WebSocketProcessBudget({
      maxSessions: 1,
      maxQueuedBytes: 4 * MiB,
      maxIngressBytes: 4 * MiB,
    });
    const memory = reserveWebSocketIngressMemory(value, budget);
    const session = budget.openSession(MiB)!;
    expect(memory.reservedBytes).toBe(MiB);

    freeBytes = 4 * 1024 ** 3;
    expect(session.tryReserve(MiB)).toBe(false);
    expect(session.tryEnsureIngress(MiB)).toBe(false);
    expect(budget.snapshot()).toMatchObject({
      queuedBytes: 0,
      ingressReservedBytes: MiB,
    });
    expect(memory.reservedBytes).toBe(MiB);

    session.close();
    expect(memory.reservedBytes).toBe(0);
    memory.close();
    value.close();
  });

  it("keeps the host event fixed lease charged until deferred permits release", async () => {
    const value = governor();
    const memory = reserveHostEventMemoryPool(value, deriveV2HostEventMemorySizing({
      responseMaxBytes: 1024,
      runtimes: 1,
      maxActivePerRuntime: 1,
      softBufferBytes: 128,
    }));
    const permit = await memory.permits.acquire(1024);
    expect(value.snapshot().reservedBytes).toBe(memory.reservedBytes);

    expect(() => memory.close()).toThrow("1 active permits");
    expect(memory.snapshot()).toMatchObject({ closed: true, activePermits: 1 });
    expect(value.snapshot().reservedBytes).toBe(memory.reservedBytes);
    expect(() => memory.permits.tryAcquire(1)).toThrow("closed");

    permit.release();
    memory.close();
    memory.close();
    expect(memory.snapshot()).toMatchObject({ closed: true, activePermits: 0 });
    expect(value.snapshot().reservedBytes).toBe(0);
    value.close();
  });

  it("charges graph backing for retained synthetic bytes and releases it on clear", () => {
    const value = governor();
    const budget = new SharedByteBudget(1024 * 1024);
    const memory = reserveResponseGraphMemory(value, budget, { quantumBytes: 1 });
    expect(memory.maximumBytes).toBe(budget.limit);
    expect(memory.reservedBytes).toBe(0);
    expect(value.snapshot().reservedBytes).toBe(0);

    const graph = new ResponseGraph({ sharedBudget: budget });
    graph.recordWarmup(`${SYNTHETIC_WARMUP_ID_PREFIX}governor`, {
      type: "response.create",
      generate: false,
      store: false,
      input: "one",
    }, { authorizationScope: createAuthenticatedTenantScope() });
    expect(budget.used).toBeGreaterThan(0);
    expect(memory.reservedBytes).toBe(budget.used);
    expect(value.snapshot().reservedBytes).toBe(budget.used);
    expect(() => memory.close()).toThrow("retained bytes");
    expect(value.snapshot().reservedBytes).toBe(budget.used);

    graph.clear();
    expect(memory.reservedBytes).toBe(0);
    memory.close();
    expect(value.snapshot().reservedBytes).toBe(0);
    value.close();
  });

  it("keeps graph accounting unchanged when elastic backing rejects growth", () => {
    const MiB = 1024 ** 2;
    let freeBytes = 12 * 1024 ** 3;
    const value = new ResourceGovernor({
      probe: {
        sample: () => ({
          totalBytes: 16 * 1024 ** 3,
          freeBytes,
          rssBytes: 256 * MiB,
          heapBytes: 64 * MiB,
          externalBytes: 16 * MiB,
          logicalProcessors: 8,
        }),
      },
    });
    const budget = new SharedByteBudget(4 * MiB);
    const memory = reserveResponseGraphMemory(value, budget);
    expect(budget.reserve(MiB)).toBe(true);
    freeBytes = 4 * 1024 ** 3;
    expect(budget.reserve(MiB)).toBe(false);
    expect(budget.used).toBe(MiB);
    expect(memory.reservedBytes).toBe(MiB);

    budget.release(MiB);
    memory.close();
    value.close();
  });

  it("does not let idle WebSocket and graph ceilings starve a fresh request", () => {
    const MiB = 1024 ** 2;
    const value = new ResourceGovernor({
      probe: {
        sample: () => ({
          totalBytes: 32 * 1024 ** 3,
          freeBytes: Math.floor(8.1 * 1024 ** 3),
          rssBytes: 175 * MiB,
          heapBytes: 64 * MiB,
          externalBytes: 16 * MiB,
          logicalProcessors: 8,
        }),
      },
    });
    const host = reserveHostEventMemoryPool(value, deriveV2HostEventMemorySizing({
      responseMaxBytes: 64 * MiB,
      runtimes: 8,
      maxActivePerRuntime: 8,
    }));
    const graph = reserveResponseGraphMemory(value, new SharedByteBudget(512 * MiB));
    const websocketBudget = new WebSocketProcessBudget({
      maxSessions: 256,
      maxQueuedBytes: 512 * MiB,
      maxIngressBytes: 768 * MiB,
    });
    const websocket = reserveWebSocketIngressMemory(value, websocketBudget);

    expect(host.reservedBytes).toBe(705 * MiB);
    expect(graph.maximumBytes + websocket.maximumBytes).toBe(1792 * MiB);
    expect(graph.reservedBytes).toBe(0);
    expect(websocket.reservedBytes).toBe(0);
    expect(value.snapshot().reservedBytes).toBe(host.reservedBytes);
    const session = websocketBudget.openSession(17 * MiB, 48 * MiB)!;
    expect(websocket.reservedBytes).toBe(65 * MiB);
    expect(value.snapshot().reservedBytes).toBe(770 * MiB);
    expect(value.snapshot().reservedBytes).toBeLessThan(1024 * MiB);
    const request = value.tryAcquire({
      class: "text-small",
      requestBytes: 1024,
      reservedBytes: 8 * MiB,
      stream: true,
    });
    expect(request).not.toBeNull();

    request!.release();
    session.close();
    websocket.close();
    graph.close();
    host.close();
    value.close();
  });

  it("does not pin transport permits or graph bytes across generated terminals", async () => {
    const budget = new SharedByteBudget(4 * 1024 * 1024);
    const graph = new ResponseGraph({
      maxNodes: 128,
      maxBytes: budget.limit,
      sharedBudget: budget,
    });
    const permits = new AtomicBytePermitPool({ ordinaryBytes: 64 * 1024, jumboBytes: 1 });

    for (let index = 0; index < 100; index++) {
      const final = attachAtomicBytePermit({
        id: `resp_${index}`,
        status: "completed",
        output: [{ type: "message", content: `cached-${index}` }],
      }, await permits.acquire(64 * 1024));
      resolved(graph, String(index));
      releaseAtomicBytePermit(final);
      expect(permits.snapshot().ordinaryUsedBytes).toBe(0);
    }
    expect(graph.stats()).toEqual({ nodes: 0, bytes: 0 });
    expect(budget.used).toBe(0);

    // Generated histories are provider-backed, so unrelated event traffic can
    // still use the complete transport allocation domain.
    const unrelated = permits.tryAcquire(64 * 1024);
    expect(unrelated).not.toBeNull();
    unrelated!.release();
    graph.clear();
    expect(budget.used).toBe(0);
    permits.close();
  });
});
