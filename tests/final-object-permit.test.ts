import { describe, expect, it } from "bun:test";
import type { WebSocket } from "ws";
import { createGhostProvApp, type RouterLike } from "../src/server/app";
import type { RunHandle } from "../src/relay/run-handle";
import { sseFrame } from "../src/relay/sse";
import { AtomicBytePermitPool } from "../src/v2/memory/atomic-byte-permits";
import { v2RetainedEventPermitBytes } from "../src/v2/memory/event-memory-sizing";
import { V2RunHandle } from "../src/v2/transport/run-handle";
import { attachAtomicBytePermit } from "../src/v2/memory/permit-carrier";
import { ResponseGraph } from "../src/ws/response-graph";
import { ResponsesWebSocketSession } from "../src/ws/session";

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

function appFor(handle: V2RunHandle) {
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

async function responseFrom(handle: V2RunHandle, stream: boolean): Promise<Response> {
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

  it("decouples cached finals so sequential jumbo sockets share one live lane", async () => {
    const first = terminalHarness();
    const firstSocket = new SlowSocket();
    const firstGraph = new ResponseGraph();
    const firstSession = new ResponsesWebSocketSession(firstSocket as unknown as WebSocket, {
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
    expect(firstGraph.stats().nodes).toBe(1);
    expect(first.pool.snapshot()).toMatchObject({ activePermits: 0, jumboUsedBytes: 0 });

    // Keep the first socket and its 1 MiB continuation cache open. A second
    // above-soft terminal must still acquire the sole process jumbo lane.
    const second = terminalHarness(1024 * 1024, first.pool);
    const secondSocket = new SlowSocket();
    const secondGraph = new ResponseGraph();
    const secondSession = new ResponsesWebSocketSession(secondSocket as unknown as WebSocket, {
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
    expect(secondGraph.stats().nodes).toBe(1);
    expect(first.pool.snapshot()).toMatchObject({ activePermits: 0, jumboUsedBytes: 0 });

    firstSession.close();
    secondSession.close();
    first.pool.close();
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
});
