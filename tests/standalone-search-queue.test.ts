import { describe, expect, it } from "bun:test";
import { Hono } from "hono";
import { createAuthenticatedTenantScope } from "../src/http/api-key";
import {
  registerStandaloneSearchRoute,
  type StandaloneSearchRequest,
} from "../src/http/standalone-search";
import { RouterCapacityTimeoutError, RouterShutdownError } from "../src/router/router";
import { createRouterStandaloneSearchHandler } from "../src/search/router-handler";
import { StandaloneSearchSessionIndex } from "../src/search/session-index";
import type { RunHandle } from "../src/relay/run-handle";

const request: StandaloneSearchRequest = {
  id: "session-queue-test",
  model: "gpt-5.6-sol",
  commands: { search_query: [{ q: "Offline test query" }] },
};

describe("standalone search queue HTTP ownership", () => {
  it.each([
    ["deadline", 408, "search_session_queue_timeout"],
    ["ingress-deadline", 408, "search_session_queue_timeout"],
    ["cancel", 499, "request_aborted"],
    ["clear", 503, "search_session_cleared"],
    ["shutdown", 503, "router_shutting_down"],
  ] as const)("%s returns %i before router entry and releases the ingress lease once", async (action, status, code) => {
    let now = 100;
    const timers = new Set<{ at: number; callback: () => void }>();
    const sessions = new StandaloneSearchSessionIndex({
      now: () => now,
      schedule(callback, delayMs) {
        const timer = { at: now + delayMs, callback };
        timers.add(timer);
        return () => { timers.delete(timer); };
      },
    });
    const scope = createAuthenticatedTenantScope();
    let releaseFinal!: (value: Record<string, unknown>) => void;
    const final = new Promise<Record<string, unknown>>(resolve => { releaseFinal = resolve; });
    let started!: () => void;
    const activeStarted = new Promise<void>(resolve => { started = resolve; });
    let calls = 0;
    const handler = createRouterStandaloneSearchHandler({
      async startRelay(body: Record<string, unknown>): Promise<RunHandle> {
        calls++;
        expect(body).toMatchObject({ stream: true, store: true });
        started();
        return {
          async *events() {},
          discardUnconsumedEvents() {},
          final,
          stats: { bytes: 1, frames: 1, firstByteMs: 0 },
        };
      },
    } as never, sessions);
    const first = handler(request, {
      authorizationScope: scope,
      clientId: "active-search",
      signal: new AbortController().signal,
      transferIncomingLease: () => undefined,
      encodedBodyBytes: 100,
      structuralBodyBytes: 100,
    });
    await activeStarted;

    let entered!: () => void;
    const queued = new Promise<void>(resolve => { entered = resolve; });
    const app = new Hono();
    registerStandaloneSearchRoute(app, {
      authorizationScope: scope,
      handler(body, context) {
        const result = handler(body, context);
        entered();
        return result;
      },
    });
    let releases = 0;
    let completions = 0;
    const client = new AbortController();
    const ingress = new AbortController();
    const pendingResponse = app.fetch(new Request("http://localhost/v1/alpha/search", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(request),
      signal: client.signal,
    }), {
      incomingBodyLease: {
        acceptedAtMs: 0,
        deadlineMs: 110,
        signal: ingress.signal,
        release() { releases++; },
        completeAdmission() { completions++; },
      },
    });
    await queued;
    expect(sessions.snapshot()).toMatchObject({ activeKeys: 1, pendingRequests: 1 });
    expect(releases).toBe(0);
    expect(completions).toBe(0);
    if (action === "deadline") {
      now = 110;
      for (const timer of [...timers]) {
        if (timer.at <= now) timer.callback();
      }
    } else if (action === "ingress-deadline") {
      ingress.abort(new RouterCapacityTimeoutError());
    } else if (action === "cancel") {
      client.abort();
    } else if (action === "clear") {
      handler.clear();
    } else {
      // System shutdown aborts ingress before it shuts the session index down.
      ingress.abort(new RouterShutdownError());
      handler.beginShutdown();
    }
    const response = await pendingResponse;
    expect(response.status).toBe(status);
    expect(await response.json()).toMatchObject({ error: { code } });
    expect(releases).toBe(1);
    expect(completions).toBe(0);
    expect(calls).toBe(1);
    expect(sessions.snapshot()).toMatchObject({ activeKeys: 1, pendingRequests: 0 });
    expect(timers.size).toBe(0);
    releaseFinal({
      id: "resp_original",
      status: "completed",
      output: [{ type: "message", content: [{ type: "output_text", text: "Offline result" }] }],
    });
    expect(await first).toMatchObject({ kind: "success" });
    expect(calls).toBe(1);
    expect(sessions.snapshot()).toMatchObject({ activeKeys: 0, pendingRequests: 0 });
  });

  it("rejects a request whose ingress deadline expired before handler entry without taking its lease", async () => {
    const sessions = new StandaloneSearchSessionIndex({ now: () => 20 });
    let calls = 0;
    let transfers = 0;
    const handler = createRouterStandaloneSearchHandler({
      async startRelay() { calls++; throw new Error("Expired search must not reach router"); },
    } as never, sessions);
    const result = Promise.resolve(handler(request, {
      authorizationScope: createAuthenticatedTenantScope(),
      clientId: "expired-search",
      signal: new AbortController().signal,
      incomingLease: { acceptedAtMs: 0, deadlineMs: 10, release() {} },
      transferIncomingLease() { transfers++; return undefined; },
      encodedBodyBytes: 100,
      structuralBodyBytes: 100,
    }));
    await expect(result).rejects.toMatchObject({ status: 408, options: { code: "search_session_queue_timeout" } });
    expect(calls).toBe(0);
    expect(transfers).toBe(0);
    expect(sessions.snapshot()).toMatchObject({ activeKeys: 0, pendingRequests: 0 });
  });
});
