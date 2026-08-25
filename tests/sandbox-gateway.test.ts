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
});
