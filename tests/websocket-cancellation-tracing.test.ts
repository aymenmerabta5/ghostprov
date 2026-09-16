import { describe, expect, it } from "bun:test";
import type { WebSocket } from "ws";
import {
  REQUEST_CANCELLATION_ORIGINS,
  recordRequestCancellation,
  requestCancellationMetadata,
  type RequestCancellationOrigin,
} from "../src/core/cancellation-metadata";
import { createAuthenticatedTenantScope } from "../src/http/api-key";
import type { ObservabilityEvent } from "../src/observability/events";
import type { LoggerLike } from "../src/observability/logger";
import { assertSafeObservabilityEvent } from "../src/observability/redaction";
import { ResourceGovernor } from "../src/router/resource-governor";
import { MultiSandboxRouter } from "../src/router/router";
import {
  SandboxRegistry,
  type RoutableSandboxRuntime,
  type SandboxRunOptions,
} from "../src/sandbox/registry";
import { projectId } from "../src/sandbox/types";
import { V2CommittedError, V2RunHandle } from "../src/v2/transport/run-handle";
import { ResponsesWebSocketSession } from "../src/ws/session";

class FakeSocket {
  readonly OPEN = 1;
  readonly CONNECTING = 0;
  readyState = this.OPEN;
  bufferedAmount = 0;
  send(_data: string, callback: (error?: Error) => void) { callback(); }
  close() { this.readyState = 3; }
  terminate() { this.readyState = 3; }
}

async function flush() {
  await new Promise<void>(resolve => setImmediate(resolve));
  for (let index = 0; index < 100; index++) await Promise.resolve();
}

function fixture() {
  const events: ObservabilityEvent[] = [];
  const logger: LoggerLike = {
    emit(event) { assertSafeObservabilityEvent(event); events.push(event); },
    snapshot: () => ({ queuedEvents: 0, queuedBytes: 0, droppedSamples: 0,
      droppedProgress: 0, droppedTerminals: 0, sinkErrors: 0 }),
    async close() {},
  };
  const signals: AbortSignal[] = [];
  const handles: V2RunHandle[] = [];
  const runtime: RoutableSandboxRuntime = {
    projectId: projectId("00000000-0000-4000-8000-000000000032"),
    async start() {}, async drain() {}, async stop() {},
    snapshot() {
      return {
        projectId: this.projectId, incarnation: 1, state: "ready", capacity: 4,
        active: 0, available: 4, quarantined: 0, heartbeatAgeMs: 0,
        credentialExpiresInMs: 3_600_000, ackEwmaMs: 10, completionEwmaMs: 100,
        recentFailureRate: 0, queueDepth: 0, memoryPressure: 0, failure: null,
      };
    },
    async run(_body, signal, options?: SandboxRunOptions) {
      const handle = new V2RunHandle({});
      handles.push(handle);
      signals.push(signal!);
      signal?.addEventListener("abort", () => {
        handle.fail(new V2CommittedError("Request cancelled", "cancelled", false));
      }, { once: true });
      options?.onAdmissionReflected?.();
      options?.onExecutionRiskReflected?.();
      return handle;
    },
  };
  const governor = new ResourceGovernor({ probe: { sample: () => ({
    totalBytes: 32 * 1024 ** 3, freeBytes: 20 * 1024 ** 3,
    rssBytes: 320 * 1024 ** 2, heapBytes: 100 * 1024 ** 2,
    externalBytes: 20 * 1024 ** 2, logicalProcessors: 12,
  }) } });
  const authorizationScope = createAuthenticatedTenantScope();
  let trace = 0;
  const router = new MultiSandboxRouter({
    registry: new SandboxRegistry([runtime]), governor, logger, authorizationScope,
    random: { next: () => 0.5, uuid: () => `cancel-trace-${++trace}` },
  });
  const sessions: ResponsesWebSocketSession[] = [];
  const createSession = () => {
    const session = new ResponsesWebSocketSession(new FakeSocket() as unknown as WebSocket, {
      authorizationScope,
      startRelay: (body, options) => router.startRelay(body, { ...options, source: "websocket" }),
    });
    sessions.push(session);
    return session;
  };
  return {
    events, signals, handles, router, governor, createSession,
    async close() {
      sessions.forEach(session => session.close("server_shutdown"));
      await flush();
      await router.close();
      governor.close();
    },
  };
}

describe("host cancellation metadata", () => {
  it("keeps the first bounded observation without changing the abort reason", () => {
    for (const origin of REQUEST_CANCELLATION_ORIGINS) {
      const controller = new AbortController();
      recordRequestCancellation(controller.signal, origin, 1000);
      recordRequestCancellation(controller.signal, "websocket_server_shutdown", 1012);
      expect(requestCancellationMetadata(controller.signal)).toBeNull();
      controller.abort();
      expect(controller.signal.reason).toBeInstanceOf(DOMException);
      expect(controller.signal.reason.name).toBe("AbortError");
      expect(requestCancellationMetadata(controller.signal)).toEqual({ origin, websocketCloseCode: 1000 });
      expect(Object.isFrozen(requestCancellationMetadata(controller.signal))).toBe(true);
    }
  });

  it("leaves unknown causes unknown and omits malformed observed close codes", () => {
    for (const code of [undefined, NaN, Infinity, 999, 5000, 1000.5]) {
      const controller = new AbortController();
      recordRequestCancellation(controller.signal, "websocket_remote_close", code);
      controller.abort();
      expect(requestCancellationMetadata(controller.signal)).toEqual({ origin: "websocket_remote_close" });
    }
    const invalid = new AbortController();
    recordRequestCancellation(invalid.signal, "private close reason" as RequestCancellationOrigin, 1000);
    invalid.abort();
    expect(requestCancellationMetadata(invalid.signal)).toBeNull();
    const previouslyAborted = new AbortController();
    const originalReason = { origin: "websocket_remote_close", websocketCloseCode: 1000, private: "plain private canary" };
    previouslyAborted.abort(originalReason);
    recordRequestCancellation(previouslyAborted.signal, "websocket_remote_close", 1000);
    expect(previouslyAborted.signal.reason).toBe(originalReason);
    expect(requestCancellationMetadata(previouslyAborted.signal)).toBeNull();
  });
});

describe("WebSocket cancellation trace correlation", () => {
  it("records simultaneous session closures on their own traces without content or fabricated codes", async () => {
    const system = fixture();
    try {
      const first = system.createSession();
      const second = system.createSession();
      first.receive(JSON.stringify({ type: "response.create", model: "gpt-5.6-terra", input: "private first prompt" }));
      await flush();
      second.receive(JSON.stringify({ type: "response.create", model: "gpt-5.6-terra", input: "private second prompt" }));
      await flush();
      expect(system.handles).toHaveLength(2);
      first.close("remote_close", 1001);
      second.close("heartbeat_timeout");
      await flush();
      const failures = system.events.filter(event => event.event === "request.failed");
      expect(failures).toHaveLength(2);
      expect(failures.find(event => event.trace_id === "cancel-trace-1")).toMatchObject({
        error_kind: "cancelled", cancellation_origin: "websocket_remote_close", websocket_close_code: 1001,
      });
      const heartbeat = failures.find(event => event.trace_id === "cancel-trace-2");
      expect(heartbeat).toMatchObject({ error_kind: "cancelled", cancellation_origin: "websocket_heartbeat_timeout" });
      expect(heartbeat).not.toHaveProperty("websocket_close_code");
      expect(JSON.stringify(system.events)).not.toContain("private first prompt");
      expect(JSON.stringify(system.events)).not.toContain("private second prompt");
      expect(system.signals.every(signal => signal.aborted && signal.reason.name === "AbortError")).toBe(true);
      expect(system.governor.snapshot().reservedBytes).toBe(0);
    } finally { await system.close(); }
  });

  it("preserves ordinary caller cancellation without inventing a WebSocket cause", async () => {
    const system = fixture();
    const caller = new AbortController();
    try {
      const handle = await system.router.startRelay({ model: "gpt-5.6-terra", input: "ordinary request" }, {
        signal: caller.signal,
      });
      const reason = new DOMException("private caller reason", "AbortError");
      caller.abort(reason);
      await expect(handle.final).rejects.toMatchObject({ code: "cancelled" });
      await flush();
      expect(caller.signal.reason).toBe(reason);
      const failures = system.events.filter(event => event.event === "request.failed");
      expect(failures).toHaveLength(1);
      expect(failures[0]).not.toHaveProperty("cancellation_origin");
      expect(failures[0]).not.toHaveProperty("websocket_close_code");
      expect(JSON.stringify(system.events)).not.toContain("private caller reason");
      expect(system.governor.snapshot().reservedBytes).toBe(0);
    } finally { await system.close(); }
  });
});
