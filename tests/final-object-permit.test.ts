import { describe, expect, it } from "bun:test";
import type { WebSocket } from "ws";
import { createGhostProvApp, type RouterLike } from "../src/server/app";
import type { RunHandle } from "../src/relay/run-handle";
import { sseFrame } from "../src/relay/sse";
import { AtomicBytePermitPool } from "../src/v2/memory/atomic-byte-permits";
import { v2RetainedEventPermitBytes } from "../src/v2/memory/event-memory-sizing";
import { V2RunHandle } from "../src/v2/transport/run-handle";
import {
  atomicBytePermitOf,
  attachAtomicBytePermit,
} from "../src/v2/memory/permit-carrier";
import { ResponseGraph } from "../src/ws/response-graph";
import { ResponsesWebSocketSession } from "../src/ws/session";
import { createAuthenticatedTenantScope } from "../src/http/api-key";

function terminalHarness(
  payloadBytes = 1024 * 1024,
  sharedPool?: AtomicBytePermitPool,
) {
  const final = {
    id: "resp_permitted",
    object: "response",
    model: "gpt-5.6-sol",
    status: "completed",
    output: [{ type: "message", content: "x".repeat(payloadBytes) }],
  };
  const frame = {
    event: "response.completed",
    data: JSON.stringify({ type: "response.completed", response: final }),
  };
  const wireBytes = new TextEncoder().encode(sseFrame(frame.event, frame.data)).byteLength;
  const retainedBytes = v2RetainedEventPermitBytes(wireBytes);
  const pool = sharedPool ?? new AtomicBytePermitPool({ ordinaryBytes: 1, jumboBytes: retainedBytes });
  const handle = new V2RunHandle({ requireTransferredEventMemory: true });
  const permit = pool.tryAcquire(retainedBytes, { region: "jumbo" });
  if (!permit || !handle.tryAcceptEvent(frame, wireBytes, permit)) {
    throw new Error("test terminal could not enter the run handle");
  }
  handle.settle(final);
  return { final, frame, handle, pool, retainedBytes };
}

function appFor(handle: RunHandle) {
  const router: RouterLike = {
    async startRelay() { return handle; },
    snapshot: () => ({
      capacity: 1,
      active: 0,
      available: 1,
      queued: 0,
      runtimes: [],
      resources: {},
    }),
  };
  return createGhostProvApp({ router });
}

async function responseFrom(handle: RunHandle, stream: boolean): Promise<Response> {
  return appFor(handle).request("/v1/responses", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ model: "gpt-5.6-sol", input: "test", stream }),
  });
}

async function waitFor(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("timed out waiting for permit state");
    await new Promise(resolve => setTimeout(resolve, 5));
  }
}

class SlowSocket {
  readonly CONNECTING = 0;
  readonly OPEN = 1;
  readonly CLOSED = 3;
  readyState = this.OPEN;
  bufferedAmount = 0;
  readonly sent: string[] = [];
  readonly callbacks: Array<(error?: Error) => void> = [];

  send(data: string, callback: (error?: Error) => void): void {
    this.sent.push(data);
    this.callbacks.push(callback);
  }

  close(): void { this.readyState = this.CLOSED; }
  terminate(): void { this.readyState = this.CLOSED; }
  flush(): void {
    for (const callback of this.callbacks.splice(0)) callback();
  }
}

describe("terminal final-object permit ownership", () => {
  it("holds a 1 MiB nonstream final through JSON delivery and then releases", async () => {
    const value = terminalHarness();
    const response = await responseFrom(value.handle, false);
    expect(response.status).toBe(200);
    expect(value.pool.snapshot().jumboUsedBytes).toBe(value.retainedBytes);

    const reader = response.body!.getReader();
    const first = await reader.read();
    expect(first.done).toBe(false);
    expect(first.value!.byteLength).toBeGreaterThan(1024 * 1024);
    expect(value.pool.snapshot().jumboUsedBytes).toBe(value.retainedBytes);
    await expect(reader.read()).resolves.toEqual({ value: undefined, done: true });
    expect(value.pool.snapshot()).toMatchObject({ activePermits: 0, jumboUsedBytes: 0 });
    value.pool.close();
  });

  it("releases an unread nonstream final when the response body is cancelled", async () => {
    const value = terminalHarness();
    const response = await responseFrom(value.handle, false);
    expect(value.pool.snapshot().jumboUsedBytes).toBe(value.retainedBytes);
    await response.body!.cancel("client disconnected");
    expect(value.pool.snapshot()).toMatchObject({ activePermits: 0, jumboUsedBytes: 0 });
    value.pool.close();
  });

  it("holds independent event and final references through a slow SSE drain", async () => {
    const value = terminalHarness();
    const response = await responseFrom(value.handle, true);
    const reader = response.body!.getReader();
    const first = await reader.read();
    expect(first.done).toBe(false);
    expect(new TextDecoder().decode(first.value)).toContain("response.completed");
    expect(value.pool.snapshot().jumboUsedBytes).toBe(value.retainedBytes);
    await expect(reader.read()).resolves.toEqual({ value: undefined, done: true });
    expect(value.pool.snapshot()).toMatchObject({ activePermits: 0, jumboUsedBytes: 0 });
    value.pool.close();
  });

  it("decouples provider-persisted finals so sequential jumbo sockets share one live lane", async () => {
    const first = terminalHarness();
    const firstSocket = new SlowSocket();
    const firstGraph = new ResponseGraph();
    const firstSession = new ResponsesWebSocketSession(firstSocket as unknown as WebSocket, {
      authorizationScope: createAuthenticatedTenantScope(),
      graph: firstGraph,
      sendTimeoutMs: 10_000,
      startRelay: async () => first.handle,
    });
    firstSession.receive(JSON.stringify({
      type: "response.create",
      stream_id: "large-one",
      model: "gpt-5.6-sol",
      store: false,
      input: "cache this",
    }));

    await waitFor(() => firstSocket.sent.length === 1);
    expect(first.pool.snapshot()).toMatchObject({ activePermits: 1, jumboUsedBytes: first.retainedBytes });
    firstSocket.flush();
    await waitFor(() => first.pool.snapshot().activePermits === 0);
    expect(firstGraph.stats()).toEqual({ nodes: 0, bytes: 0 });
    expect(first.pool.snapshot()).toMatchObject({ activePermits: 0, jumboUsedBytes: 0 });

    // Keep the first socket open without retaining a generated-history copy. A
    // second above-soft terminal must still acquire the sole process jumbo lane.
    const second = terminalHarness(1024 * 1024, first.pool);
    const secondSocket = new SlowSocket();
    const secondGraph = new ResponseGraph();
    const secondSession = new ResponsesWebSocketSession(secondSocket as unknown as WebSocket, {
      authorizationScope: createAuthenticatedTenantScope(),
      graph: secondGraph,
      sendTimeoutMs: 10_000,
      startRelay: async () => second.handle,
    });
    secondSession.receive(JSON.stringify({
      type: "response.create",
      stream_id: "large-two",
      model: "gpt-5.6-sol",
      store: false,
      input: "cache this too",
    }));
    await waitFor(() => secondSocket.sent.length === 1);
    secondSocket.flush();
    await waitFor(() => first.pool.snapshot().activePermits === 0);
    expect(secondGraph.stats()).toEqual({ nodes: 0, bytes: 0 });
    expect(first.pool.snapshot()).toMatchObject({ activePermits: 0, jumboUsedBytes: 0 });

    firstSession.close();
    secondSession.close();
    first.pool.close();
  });

  it("releases a dequeued terminal permit when HTTP SSE cancels during final proof", async () => {
    const pool = new AtomicBytePermitPool({ ordinaryBytes: 8_192, jumboBytes: 1 });
    const permit = await pool.acquire(8_192);
    const frame = attachAtomicBytePermit({
      event: "response.completed",
      data: JSON.stringify({
        type: "response.completed",
        response: { id: "resp_waiting", status: "completed", output: [] },
      }),
    }, permit);
    let yielded = false;
    let resolveFinal!: (value: Record<string, unknown>) => void;
    const final = new Promise<Record<string, unknown>>(resolve => { resolveFinal = resolve; });
    const handle: RunHandle = {
      async *events() {
        yielded = true;
        yield frame;
      },
      final,
      stats: { bytes: 0, firstByteMs: null, frames: 1 },
    };
    const response = await responseFrom(handle, true);
    const reader = response.body!.getReader();
    const pendingRead = reader.read();
    await waitFor(() => yielded && pool.snapshot().activePermits === 1);

    await reader.cancel("client disconnected during terminal proof");
    await waitFor(() => pool.snapshot().activePermits === 0);
    resolveFinal({ id: "resp_waiting", status: "completed", output: [] });
    await pendingRead;

    expect(pool.snapshot()).toMatchObject({
      activePermits: 0,
      ordinaryUsedBytes: 0,
    });
    pool.close();
  });

  it("releases a definitive final that settles after WebSocket cancellation", async () => {
    const pool = new AtomicBytePermitPool({ ordinaryBytes: 8_192, jumboBytes: 1 });
    let resolveFinal!: (value: Record<string, unknown>) => void;
    const final = new Promise<Record<string, unknown>>(resolve => { resolveFinal = resolve; });
    const handle: RunHandle = {
      async *events() {},
      final,
      stats: { bytes: 0, firstByteMs: null, frames: 0 },
    };
    const socket = new SlowSocket();
    let started = false;
    const session = new ResponsesWebSocketSession(socket as unknown as WebSocket, {
      authorizationScope: createAuthenticatedTenantScope(),
      startRelay: async () => {
        started = true;
        return handle;
      },
    });
    session.receive(JSON.stringify({
      type: "response.create",
      model: "gpt-5.6-sol",
      store: false,
      input: "cancel",
    }));
    await waitFor(() => started);
    session.close();
    resolveFinal(attachAtomicBytePermit(
      { id: "resp_late", status: "completed", output: [] },
      await pool.acquire(8_192),
    ));
    await waitFor(() => pool.snapshot().ordinaryUsedBytes === 0);
    pool.close();
  });

  it("discards a late V2 handle without closing its committed event producer", async () => {
    const pool = new AtomicBytePermitPool({ ordinaryBytes: 64 * 1024, jumboBytes: 1 });
    const v2Handle = new V2RunHandle({
      expectUpstreamPrelude: true,
      requireTransferredEventMemory: true,
    });
    let discardCalls = 0;
    let eventsCalls = 0;
    const lateHandle: RunHandle = {
      events: () => {
        eventsCalls++;
        return v2Handle.events();
      },
      discardUnconsumedEvents: () => {
        discardCalls++;
        v2Handle.discardUnconsumedEvents();
      },
      final: v2Handle.final,
      upstreamPrelude: v2Handle.upstreamPrelude,
      stats: v2Handle.stats,
    };
    let started = false;
    let resolveStart!: (handle: RunHandle) => void;
    const delayedStart = new Promise<RunHandle>(resolve => { resolveStart = resolve; });
    const socket = new SlowSocket();
    const session = new ResponsesWebSocketSession(socket as unknown as WebSocket, {
      authorizationScope: createAuthenticatedTenantScope(),
      startRelay: async () => {
        started = true;
        return delayedStart;
      },
    });
    session.receive(JSON.stringify({ type: "response.create", input: "late handle" }));
    await waitFor(() => started);
    session.close();
    resolveStart(lateHandle);
    await waitFor(() => discardCalls === 1);

    const accept = async (event: string, data: string): Promise<void> => {
      const frame = { event, data };
      const wireBytes = new TextEncoder().encode(sseFrame(event, data)).byteLength;
      const retainedBytes = v2RetainedEventPermitBytes(wireBytes);
      await v2Handle.acceptEvent(frame, wireBytes, await pool.acquire(retainedBytes));
    };
    const final = { id: "resp_late_v2", status: "completed", output: [] };
    let producerError: unknown;
    try {
      await accept("response.created", JSON.stringify({
        type: "response.created",
        response: { id: final.id, status: "in_progress", output: [] },
      }));
      await accept("response.completed", JSON.stringify({
        type: "response.completed",
        response: final,
      }));
      v2Handle.settle(final);
    } catch (error) {
      producerError = error;
      v2Handle.cancel(error instanceof Error ? error : new Error("late producer failed"));
    }

    await waitFor(() => pool.snapshot().activePermits === 0);
    expect(producerError).toBeUndefined();
    expect(discardCalls).toBe(1);
    expect(eventsCalls).toBe(0);
    expect(v2Handle.stats.frames).toBe(2);
    expect(pool.snapshot()).toMatchObject({ activePermits: 0, ordinaryUsedBytes: 0 });
    expect(socket.sent).toEqual([]);
    pool.close();
  });

  it("releases a buffered terminal when the next event read ignores cancellation", async () => {
    const pool = new AtomicBytePermitPool({ ordinaryBytes: 8_192, jumboBytes: 1 });
    const response = { id: "resp_pending_read", status: "completed", output: [] };
    const frame = attachAtomicBytePermit({
      event: "response.completed",
      data: JSON.stringify({ type: "response.completed", response }),
    }, await pool.acquire(4_096));
    const handle: RunHandle = {
      async *events() {
        yield frame;
        await new Promise<void>(() => undefined);
      },
      final: new Promise(() => undefined),
      stats: { bytes: 0, firstByteMs: null, frames: 1 },
    };
    const socket = new SlowSocket();
    const session = new ResponsesWebSocketSession(socket as unknown as WebSocket, {
      authorizationScope: createAuthenticatedTenantScope(),
      startRelay: async () => handle,
    });
    session.receive(JSON.stringify({
      type: "response.create",
      model: "gpt-5.6-sol",
      input: "cancel pending read",
    }));
    await waitFor(() => atomicBytePermitOf(frame) === null);
    session.close();
    await waitFor(() => pool.snapshot().ordinaryUsedBytes === 0);
    pool.close();
  });

  it("releases a buffered terminal when the authoritative final never settles", async () => {
    const pool = new AtomicBytePermitPool({ ordinaryBytes: 8_192, jumboBytes: 1 });
    const response = { id: "resp_pending_final", status: "completed", output: [] };
    const frame = attachAtomicBytePermit({
      event: "response.completed",
      data: JSON.stringify({ type: "response.completed", response }),
    }, await pool.acquire(4_096));
    const handle: RunHandle = {
      async *events() {
        yield frame;
      },
      final: new Promise(() => undefined),
      stats: { bytes: 0, firstByteMs: null, frames: 1 },
    };
    const session = new ResponsesWebSocketSession(new SlowSocket() as unknown as WebSocket, {
      authorizationScope: createAuthenticatedTenantScope(),
      startRelay: async () => handle,
    });
    session.receive(JSON.stringify({ type: "response.create", input: "cancel final" }));
    await waitFor(() => atomicBytePermitOf(frame) === null);
    session.close();
    await waitFor(() => pool.snapshot().ordinaryUsedBytes === 0);
    pool.close();
  });
});
