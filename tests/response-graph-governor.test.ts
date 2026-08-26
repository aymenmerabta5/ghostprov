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
import { ResponseGraph, SharedByteBudget } from "../src/ws/response-graph";
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
  });
}

describe("ResponseGraph fixed governor memory", () => {
  it("charges independent WebSocket ingress and queued-memory ceilings", () => {
    const value = governor();
    const budget = new WebSocketProcessBudget({
      maxSessions: 2,
      maxQueuedBytes: 1024,
      maxIngressBytes: 4096,
    });
    const memory = reserveWebSocketIngressMemory(value, budget);
    const session = budget.openSession(3072)!;
    expect(memory.reservedBytes).toBe(5120);
    expect(value.snapshot().reservedBytes).toBe(5120);
    expect(() => memory.close()).toThrow("1 sessions");
    expect(value.snapshot().reservedBytes).toBe(5120);

    session.close();
    memory.close();
    memory.close();
    expect(value.snapshot().reservedBytes).toBe(0);
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

  it("keeps its fixed lease until every graph byte is released", () => {
    const value = governor();
    const budget = new SharedByteBudget(1024 * 1024);
    const memory = reserveResponseGraphMemory(value, budget);
    expect(value.snapshot().reservedBytes).toBe(budget.limit);

    const graph = new ResponseGraph({ sharedBudget: budget });
    graph.record(resolved(graph, "one"), {
      id: "resp_one",
      status: "completed",
      output: [{ type: "message", content: "cached" }],
    });
    expect(budget.used).toBeGreaterThan(0);
    expect(() => memory.close()).toThrow("retained bytes");
    expect(value.snapshot().reservedBytes).toBe(budget.limit);

    graph.clear();
    memory.close();
    expect(value.snapshot().reservedBytes).toBe(0);
    value.close();
  });

  it("does not pin transport permits across many cached terminals", async () => {
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
      graph.record(resolved(graph, String(index)), final);
      releaseAtomicBytePermit(final);
      expect(permits.snapshot().ordinaryUsedBytes).toBe(0);
    }
    expect(graph.stats()).toMatchObject({ nodes: 100 });
    expect(budget.used).toBeGreaterThan(0);

    // Cached history remains live while unrelated event traffic can still use
    // the complete transport allocation domain.
    const unrelated = permits.tryAcquire(64 * 1024);
    expect(unrelated).not.toBeNull();
    unrelated!.release();
    graph.clear();
    expect(budget.used).toBe(0);
    permits.close();
  });
});
