import { describe, expect, it } from "bun:test";
import { SandboxGateway } from "../src/sandbox/gateway";
import { parseProvisionedSandbox } from "../src/prism/protocol";
import { projectId } from "../src/sandbox/types";
import type { AccountRequestClient } from "../src/prism/provisioning-client";
import type { Clock } from "../src/core/ports";

const PROJECT = projectId("e631d12c-d917-4434-b360-41b4715d6679");
const SANDBOX = parseProvisionedSandbox({
  url: "/s/sandboxes/proxy",
  token: "sandbox-A",
  sandbox_id: "sandbox-a",
});

function sandboxCredential(
  token: string,
  sessionId?: string,
  sandboxId = "sandbox-a",
  firstSeenAtMs?: number,
) {
  return parseProvisionedSandbox({
    url: "/s/sandboxes/proxy",
    token,
    sandbox_id: sandboxId,
    sandbox_session_id: sessionId,
    sandbox_session_first_seen_at_ms: firstSeenAtMs,
  });
}

class FakeAccount implements AccountRequestClient {
  calls: Array<{ path: string; method: string; headers: Headers; json: unknown }> = [];
  constructor(readonly handler: (path: string, init: RequestInit) => Promise<Response> | Response) {}
  async request(pathOrUrl: string, init: RequestInit = {}): Promise<Response> {
    const url = new URL(pathOrUrl, "https://prism.test");
    const call = {
      path: url.pathname,
      method: (init.method ?? "GET").toUpperCase(),
      headers: new Headers(init.headers),
      json: typeof init.body === "string" ? JSON.parse(init.body) : undefined,
    };
    this.calls.push(call);
    return this.handler(url.pathname, init);
  }
}

class FakeClock implements Clock {
  value = 1_787_400_000_000;
  readonly sleeps: number[] = [];
  now(): number { return this.value; }
  async sleep(ms: number): Promise<void> { this.sleeps.push(ms); this.value += ms; }
  async waitUntil(deadlineMs: number): Promise<void> { await this.sleep(Math.max(0, deadlineMs - this.value)); }
}

describe("SandboxGateway", () => {
  it("sends the runtime-specific sandbox token on direct requests", async () => {
    const account = new FakeAccount(() => Response.json({ ok: true }, {
      headers: { "x-session-id": "session-1" },
    }));
    const gateway = new SandboxGateway({ projectId: PROJECT, account, sandbox: SANDBOX });

    const result = await gateway.heartbeat();

    expect(account.calls[0].path).toBe("/s/sandboxes/proxy/heartbeat");
    expect(account.calls[0].headers.get("x-crixet-sandbox-token")).toBe("sandbox-A");
    expect(result).toMatchObject({ ok: true, status: 200, sessionId: "session-1" });
  });

  it("learns the effective session ID from a successful heartbeat", async () => {
    const account = new FakeAccount(() => Response.json({ ok: true }, {
      headers: { "x-session-id": "session-from-heartbeat" },
    }));
    const gateway = new SandboxGateway({ projectId: PROJECT, account, sandbox: SANDBOX });

    await gateway.heartbeat();
    const rotated = gateway.rotateSameSession(sandboxCredential(
      "sandbox-B",
      "session-from-heartbeat",
    ));
    await gateway.fetchText("after-rotation");

    expect(rotated).toBe(true);
    expect(account.calls.at(-1)?.headers.get("x-crixet-sandbox-token")).toBe("sandbox-B");
  });

  it("fails closed when a successful heartbeat silently changes session", async () => {
    const account = new FakeAccount(() => Response.json({ ok: true }, {
      headers: { "x-session-id": "session-2" },
    }));
    const gateway = new SandboxGateway({
      projectId: PROJECT,
      account,
      sandbox: sandboxCredential("sandbox-A", "session-1"),
    });

    await expect(gateway.heartbeat()).resolves.toMatchObject({
      ok: false,
      status: 409,
      sessionId: "session-2",
    });
    expect(gateway.rotateSameSession(
      sandboxCredential("sandbox-B", "session-2"),
    )).toBe(false);
  });

  it("returns heartbeat authentication status without waiting for a stalled body", async () => {
    const account = new FakeAccount(() => new Response(new ReadableStream({
      start() {},
      cancel: () => new Promise<void>(() => undefined),
    }), { status: 401 }));
    const gateway = new SandboxGateway({
      projectId: PROJECT,
      account,
      sandbox: SANDBOX,
      requestTimeoutMs: 1_000,
    });

    await expect(Promise.race([
      gateway.heartbeat(),
      new Promise<never>((_resolve, reject) => {
        setTimeout(() => reject(new Error("heartbeat waited for an unused body")), 100);
      }),
    ])).resolves.toMatchObject({ ok: false, status: 401 });
  });

  it("keeps an in-flight credential snapshot and uses a rotation on later requests", async () => {
    let enterFirst!: () => void;
    let releaseFirst!: () => void;
    const firstEntered = new Promise<void>(resolve => { enterFirst = resolve; });
    const firstGate = new Promise<void>(resolve => { releaseFirst = resolve; });
    let requests = 0;
    const account = new FakeAccount(async () => {
      requests++;
      if (requests === 1) {
        enterFirst();
        await firstGate;
      }
      return new Response("ok");
    });
    const gateway = new SandboxGateway({
      projectId: PROJECT,
      account,
      sandbox: sandboxCredential("sandbox-old", "session-1", "sandbox-a", 1_000),
    });

    const inFlight = gateway.fetchText("slow");
    await firstEntered;
    expect(gateway.rotateSameSession(
      sandboxCredential("sandbox-new", "session-1", "sandbox-a", 1_000),
    )).toBe(true);
    releaseFirst();
    await inFlight;
    await gateway.fetchText("later");

    expect(account.calls[0].headers.get("x-crixet-sandbox-token")).toBe("sandbox-old");
    expect(account.calls[1].headers.get("x-crixet-sandbox-token")).toBe("sandbox-new");
  });

  it("rejects rotation when session identity is missing, mismatched, or conflicting", () => {
    const unknown = new SandboxGateway({ projectId: PROJECT, account: new FakeAccount(() => new Response()), sandbox: SANDBOX });
    expect(unknown.rotateSameSession(sandboxCredential("candidate", "session-1"))).toBe(false);

    const known = new SandboxGateway({
      projectId: PROJECT,
      account: new FakeAccount(() => new Response()),
      sandbox: sandboxCredential("current", "session-1", "sandbox-a", 1_000),
    });
    expect(known.rotateSameSession(sandboxCredential("candidate"))).toBe(false);
    expect(known.rotateSameSession(sandboxCredential("candidate", "session-2"))).toBe(false);
    expect(known.rotateSameSession(sandboxCredential("candidate", "session-1", "sandbox-b", 1_000))).toBe(false);
    expect(known.rotateSameSession(sandboxCredential("candidate", "session-1", "sandbox-a", 2_000))).toBe(false);
  });

  it("coalesces concurrent workspace synchronization barriers", async () => {
    const account = new FakeAccount(path => path.endsWith("/wait-for-sync")
      ? Response.json({
        status: "synced",
        readinessCapabilities: ["current_y_sweet_provider"],
        tokens: { hasCurrentYSweetToken: true, hasSyncedYSweetProvider: true },
      })
      : new Response("not found", { status: 404 }));
    const gateway = new SandboxGateway({
      projectId: PROJECT,
      account,
      sandbox: SANDBOX,
      syncBatchMs: 0,
    });

    await Promise.all(Array.from({ length: 20 }, () => gateway.syncWorkspace()));

    expect(account.calls.filter(call => call.path.endsWith("/wait-for-sync"))).toHaveLength(1);
  });

  it("retries transient workspace-sync proxy failures within the barrier deadline", async () => {
    let attempts = 0;
    const clock = new FakeClock();
    const account = new FakeAccount(path => {
      if (!path.endsWith("/wait-for-sync")) return new Response("not found", { status: 404 });
      attempts++;
      if (attempts === 1) return new Response("temporary proxy failure", { status: 502 });
      return Response.json({
        status: "synced",
        readinessCapabilities: ["current_y_sweet_provider"],
        tokens: { hasCurrentYSweetToken: true, hasSyncedYSweetProvider: true },
      });
    });
    const gateway = new SandboxGateway({
      projectId: PROJECT,
      account,
      sandbox: SANDBOX,
      syncBatchMs: 0,
      clock,
    });

    await gateway.syncWorkspace();

    expect(attempts).toBe(2);
    expect(clock.sleeps.some(ms => ms > 0)).toBe(true);
  });

  it("runs another workspace barrier for a write arriving mid-flight", async () => {
    let releaseFirst!: () => void;
    let enteredFirst!: () => void;
    const firstEntered = new Promise<void>(resolve => { enteredFirst = resolve; });
    const firstGate = new Promise<void>(resolve => { releaseFirst = resolve; });
    let barriers = 0;
    const account = new FakeAccount(async path => {
      if (!path.endsWith("/wait-for-sync")) return new Response("not found", { status: 404 });
      barriers++;
      if (barriers === 1) {
        enteredFirst();
        await firstGate;
      }
      return Response.json({
        status: "synced",
        readinessCapabilities: ["current_y_sweet_provider"],
        tokens: { hasCurrentYSweetToken: true, hasSyncedYSweetProvider: true },
      });
    });
    const gateway = new SandboxGateway({
      projectId: PROJECT,
      account,
      sandbox: SANDBOX,
      syncBatchMs: 0,
    });

    const first = gateway.syncWorkspace();
    await firstEntered;
    const second = gateway.syncWorkspace();
    releaseFirst();
    await Promise.all([first, second]);

    expect(barriers).toBe(2);
  });

  it("posts render payloads through the scoped gateway", async () => {
    const account = new FakeAccount(path => new Response("{}", {
      status: path.endsWith("/render") ? 202 : 200,
    }));
    const gateway = new SandboxGateway({ projectId: PROJECT, account, sandbox: SANDBOX });
    await gateway.triggerRender({ mainDocument: "main.tex", clientStateVector: "state", clientDeleteSetUpdate: "delete" });
    expect(account.calls.at(-1)?.json).toEqual({ mainDocument: "main.tex", clientStateVector: "state", clientDeleteSetUpdate: "delete" });
  });

  it("accepts render headers promptly and fences a non-cooperative body cleanup", async () => {
    const account = new FakeAccount(() => new Response(new ReadableStream({
      start() {},
      cancel: () => new Promise<void>(() => undefined),
    }), { status: 202 }));
    const gateway = new SandboxGateway({
      projectId: PROJECT,
      account,
      sandbox: SANDBOX,
      requestTimeoutMs: 1_000,
    });
    const request = {
      mainDocument: "main.tex",
      clientStateVector: "state",
      clientDeleteSetUpdate: "delete",
    };

    await expect(Promise.race([
      gateway.triggerRender(request),
      new Promise<never>((_resolve, reject) => {
        setTimeout(() => reject(new Error("render waited for an unused body")), 100);
      }),
    ])).resolves.toBeUndefined();
    await expect(gateway.triggerRender(request)).rejects.toMatchObject({
      name: "SandboxGatewayError",
      status: 408,
      operation: "render",
    });
    expect(account.calls).toHaveLength(1);
  });

  it("bounds a sandbox request even when only the outer lifecycle remains live", async () => {
    const account = new FakeAccount(() => new Promise<Response>(() => undefined));
    const gateway = new SandboxGateway({
      projectId: PROJECT,
      account,
      sandbox: SANDBOX,
      requestTimeoutMs: 10,
    });

    await expect(gateway.fetchText("logs")).rejects.toMatchObject({
      name: "SandboxGatewayError",
      status: 408,
      operation: "logs",
    });
    await Promise.all(Array.from({ length: 8 }, () => (
      expect(gateway.fetchText("logs")).rejects.toMatchObject({ status: 408 })
    )));
    expect(account.calls).toHaveLength(1);
  });

  it("keeps one total sandbox deadline through a non-cooperative response body", async () => {
    let calls = 0;
    let bodyCancelled = false;
    const account = new FakeAccount(() => {
      calls++;
      if (calls === 1) {
        return new Response(new ReadableStream({
          start() {},
          cancel() { bodyCancelled = true; },
        }), { status: 200 });
      }
      return new Response("ok");
    });
    const first = new SandboxGateway({
      projectId: PROJECT,
      account,
      sandbox: SANDBOX,
      requestTimeoutMs: 10,
    });
    const second = new SandboxGateway({
      projectId: PROJECT,
      account,
      sandbox: SANDBOX,
      requestTimeoutMs: 10,
    });

    await expect(first.fetchText("logs")).rejects.toMatchObject({
      name: "SandboxGatewayError",
      status: 408,
      operation: "logs",
      message: "Sandbox logs request timed out",
    });
    expect(bodyCancelled).toBe(true);
    await new Promise<void>(resolve => setTimeout(resolve, 0));
    await expect(second.fetchText("logs")).resolves.toMatchObject({ text: "ok" });
    expect(account.calls).toHaveLength(2);
  });

  it("scopes a truly stuck body fence to its sandbox and operation", async () => {
    const account = new FakeAccount((_path, init) => {
      const token = new Headers(init.headers).get("x-crixet-sandbox-token");
      if (token === "sandbox-A") {
        return new Response(new ReadableStream({
          start() {},
          cancel: () => new Promise<void>(() => undefined),
        }), { status: 200 });
      }
      return new Response("ok", { headers: { "x-session-id": "session-b" } });
    });
    const sandboxA = new SandboxGateway({
      projectId: PROJECT,
      account,
      sandbox: SANDBOX,
      requestTimeoutMs: 10,
    });
    const sandboxB = new SandboxGateway({
      projectId: PROJECT,
      account,
      sandbox: sandboxCredential("sandbox-B", "session-b", "sandbox-b"),
      requestTimeoutMs: 100,
    });

    await expect(sandboxA.fetchText("logs")).rejects.toMatchObject({ status: 408 });
    await expect(sandboxB.heartbeat()).resolves.toMatchObject({ ok: true, status: 200 });
    await expect(sandboxA.fetchText("logs")).rejects.toMatchObject({ status: 408 });
    expect(account.calls).toHaveLength(2);
  });

  it("rejects an oversized sandbox response body", async () => {
    const account = new FakeAccount(() => new Response("123456789", { status: 200 }));
    const gateway = new SandboxGateway({
      projectId: PROJECT,
      account,
      sandbox: SANDBOX,
      requestTimeoutMs: 1_000,
      responseBodyLimitBytes: 8,
    });

    await expect(gateway.fetchText("logs")).rejects.toMatchObject({
      name: "SandboxGatewayError",
      status: 502,
      operation: "logs",
      message: "Sandbox logs response body exceeded the bounded limit",
    });
  });

  it("does not start a sandbox mutation when its caller is already aborted", async () => {
    const account = new FakeAccount(() => new Response("unexpected"));
    const gateway = new SandboxGateway({ projectId: PROJECT, account, sandbox: SANDBOX });
    const controller = new AbortController();
    const reason = new DOMException("owner stopped", "AbortError");
    controller.abort(reason);

    try {
      await gateway.postJson("token", { token: "new" }, controller.signal);
      throw new Error("expected request cancellation");
    } catch (error) {
      expect(error).toBe(reason);
    }
    expect(account.calls).toHaveLength(0);
  });

  it("stops waiting immediately when a caller aborts a non-cooperative sandbox request", async () => {
    let entered!: () => void;
    const requestEntered = new Promise<void>(resolve => { entered = resolve; });
    const account = new FakeAccount(() => {
      entered();
      return new Promise<Response>(() => undefined);
    });
    const gateway = new SandboxGateway({
      projectId: PROJECT,
      account,
      sandbox: SANDBOX,
      requestTimeoutMs: 1_000,
    });
    const controller = new AbortController();
    const reason = new DOMException("owner stopped", "AbortError");
    const pending = gateway.fetchText("logs", { signal: controller.signal });
    await requestEntered;

    controller.abort(reason);

    await expect(Promise.race([
      pending,
      new Promise<never>((_resolve, reject) => {
        setTimeout(() => reject(new Error("caller abort did not settle the request")), 100);
      }),
    ])).rejects.toBe(reason);
  });

  it("preserves caller cancellation while a sandbox response body is stalled", async () => {
    let bodyStarted!: () => void;
    const started = new Promise<void>(resolve => { bodyStarted = resolve; });
    const account = new FakeAccount(() => new Response(new ReadableStream({
      start() { bodyStarted(); },
    }), { status: 200 }));
    const gateway = new SandboxGateway({
      projectId: PROJECT,
      account,
      sandbox: SANDBOX,
      requestTimeoutMs: 1_000,
    });
    const controller = new AbortController();
    const reason = new DOMException("owner stopped during body", "AbortError");
    const pending = gateway.fetchText("logs", { signal: controller.signal });
    await started;

    controller.abort(reason);

    await expect(pending).rejects.toBe(reason);
  });

  it("discards a late sandbox response after the total request deadline", async () => {
    let resolveRequest!: (response: Response) => void;
    let bodyCancelled = false;
    const account = new FakeAccount(() => new Promise<Response>(resolve => { resolveRequest = resolve; }));
    const gateway = new SandboxGateway({
      projectId: PROJECT,
      account,
      sandbox: SANDBOX,
      requestTimeoutMs: 10,
    });
    const pending = gateway.fetchText("logs");

    await expect(pending).rejects.toMatchObject({ status: 408, operation: "logs" });
    resolveRequest(new Response(new ReadableStream({
      cancel() { bodyCancelled = true; },
    })));
    await new Promise<void>(resolve => setTimeout(resolve, 0));

    expect(bodyCancelled).toBe(true);
  });

  it("keeps the timed signal alive after a response is returned", async () => {
    let requestSignal!: AbortSignal;
    const account = new FakeAccount((_path, init) => {
      requestSignal = init.signal!;
      return new Response("ok");
    });
    const gateway = new SandboxGateway({
      projectId: PROJECT,
      account,
      sandbox: SANDBOX,
      requestTimeoutMs: 10,
    });

    await gateway.fetchText("logs");
    expect(requestSignal.aborted).toBe(false);
    await new Promise<void>(resolve => setTimeout(resolve, 20));
    expect(requestSignal.aborted).toBe(true);
  });
});
