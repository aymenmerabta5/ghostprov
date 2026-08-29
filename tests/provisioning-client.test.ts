import { describe, expect, it } from "bun:test";
import type { Clock, RandomSource } from "../src/core/ports";
import {
  PrismProvisioningClient,
  ProvisioningError,
  type AccountRequestClient,
} from "../src/prism/provisioning-client";
import { AccountAuthError } from "../src/prism/account-session";
import { parseProvisionedSandbox } from "../src/prism/protocol";
import { projectId, sandboxSessionId } from "../src/sandbox/types";

const PROJECT = "e631d12c-d917-4434-b360-41b4715d6679";
const SANDBOX = "750ba53d-dc42-40f0-933b-a64bdf3656ca";
const SESSION = "sandbox-session-1";
const Y_URL = `wss://prism.test/y/d/${PROJECT}/ws`;
const Y_BASE = `https://prism.test/y/d/${PROJECT}`;

interface Call {
  path: string;
  method: string;
  headers: Headers;
  json: any;
}

class FakeClock implements Clock {
  value = 1_000;
  sleeps: number[] = [];
  now(): number { return this.value; }
  async sleep(ms: number, signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) throw new DOMException("aborted", "AbortError");
    this.sleeps.push(ms);
    this.value += ms;
  }
  async waitUntil(deadlineMs: number, signal?: AbortSignal): Promise<void> {
    await this.sleep(Math.max(0, deadlineMs - this.value), signal);
  }
}

type ScriptedResponse = Response | ((call: Call) => Response);

class ScriptedAccountSession implements AccountRequestClient {
  calls: Call[] = [];
  constructor(private readonly responses: ScriptedResponse[]) {}
  async request(pathOrUrl: string, init: RequestInit = {}): Promise<Response> {
    const url = new URL(pathOrUrl, "https://prism.test/");
    const rawBody = typeof init.body === "string" ? init.body : "";
    const call: Call = {
      path: url.pathname,
      method: (init.method ?? "GET").toUpperCase(),
      headers: new Headers(init.headers),
      json: rawBody ? JSON.parse(rawBody) : undefined,
    };
    this.calls.push(call);
    const response = this.responses.shift();
    if (!response) throw new Error(`missing scripted response for ${call.path}`);
    return typeof response === "function" ? response(call) : response;
  }
}

const RANDOM: RandomSource = {
  next: () => 0.5,
  uuid: () => "request-series-1",
};

function json(status: number, body: unknown, headers?: HeadersInit): Response {
  return Response.json(body, { status, headers });
}

function successResponses(): ScriptedResponse[] {
  return [
    json(200, { url: "/s/sandboxes/proxy", token: "sandbox-token", sandbox_id: SANDBOX, sandbox_session_id: SESSION }),
    json(200, { access_token: "resource-token", expires_at: 2_000_000_000, resources_base_url: "https://prism.test/s/sandbox-resources/", scopes: ["project_files:read"] }),
    json(200, { success: true }),
    json(200, { url: Y_URL, baseUrl: Y_BASE, docId: PROJECT, token: "y-token", authorization: "full" }),
    json(200, { success: true }),
    json(200, { readinessCapabilities: ["current_y_sweet_provider"], status: "syncing", tokens: { hasCurrentYSweetToken: true, hasSyncedYSweetProvider: false } }),
    json(200, { readinessCapabilities: ["current_y_sweet_provider"], status: "synced", tokens: { hasCurrentYSweetToken: true, hasSyncedYSweetProvider: true } }),
  ];
}

function client(session: AccountRequestClient, clock = new FakeClock(), options: Record<string, number> = {}) {
  return new PrismProvisioningClient({
    account: session,
    clock,
    random: RANDOM,
    ...options,
  });
}

describe("PrismProvisioningClient", () => {
  it("performs the proven browserless bootstrap sequence with exact bodies", async () => {
    const clock = new FakeClock();
    const account = new ScriptedAccountSession(successResponses());

    const result = await client(account, clock).provision(projectId(PROJECT));

    expect(result.projectId).toBe(projectId(PROJECT));
    expect(result.sandbox.sessionId).toBe(sandboxSessionId(SESSION));
    expect(result.resource.token.reveal()).toBe("resource-token");
    expect(result.ySweet.token.reveal()).toBe("y-token");
    expect(account.calls.map(call => call.path)).toEqual([
      "/api/backend/1/new",
      `/api/projects/${PROJECT}/sandbox/resources-token`,
      "/s/sandboxes/proxy/resources-token",
      "/api/y",
      "/s/sandboxes/proxy/token",
      "/s/sandboxes/proxy/wait-for-sync",
      "/s/sandboxes/proxy/wait-for-sync",
    ]);
    expect(account.calls[1].json).toEqual({ sandbox_session_id: SESSION });
    expect(account.calls[2].json).toEqual({
      token: "resource-token",
      resourceBaseUrl: "https://prism.test/s/sandbox-resources/",
      projectId: PROJECT,
    });
    expect(account.calls[3].json).toMatchObject({
      docId: PROJECT,
      requestContext: {
        source: "initial-bootstrap",
        bootstrapAttempt: 1,
        previouslyConnected: false,
        sandboxUrl: "/s/sandboxes/proxy/",
        sandboxId: SANDBOX,
        sandboxSessionId: SESSION,
        maxAttempts: 5,
        requestSeriesId: "request-series-1",
      },
    });
    expect(account.calls[4].json).toMatchObject({ docId: PROJECT, token: "y-token" });
    expect(account.calls[2].headers.get("x-crixet-sandbox-token")).toBe("sandbox-token");
    expect(clock.sleeps).toEqual([250]);
  });

  it("exposes the sandbox immediately so heartbeat can start during credential sync", async () => {
    let acquired = false;
    const responses = successResponses();
    responses[1] = _call => {
      expect(acquired).toBe(true);
      return json(200, { access_token: "resource-token", expires_at: 2_000_000_000, resources_base_url: "https://prism.test/s/sandbox-resources/" });
    };
    const provisioning = client(new ScriptedAccountSession(responses));

    await provisioning.provision(projectId(PROJECT), undefined, {
      onSandboxAcquired: async sandbox => {
        expect(sandbox.token.reveal()).toBe("sandbox-token");
        acquired = true;
      },
    });

    expect(acquired).toBe(true);
  });

  it("acquires a sandbox without starting the remaining bootstrap", async () => {
    const account = new ScriptedAccountSession([successResponses()[0]]);

    const sandbox = await client(account).acquireSandbox();

    expect(sandbox.token.reveal()).toBe("sandbox-token");
    expect(sandbox.sessionId).toBe(sandboxSessionId(SESSION));
    expect(account.calls.map(call => call.path)).toEqual(["/api/backend/1/new"]);
  });

  it("bootstraps an acquired sandbox without making another acquisition request", async () => {
    const sandbox = parseProvisionedSandbox({
      url: "/s/sandboxes/proxy",
      token: "sandbox-token",
      sandbox_id: SANDBOX,
      sandbox_session_id: SESSION,
    });
    const account = new ScriptedAccountSession(successResponses().slice(1));
    let exposed = false;

    const result = await client(account).provisionAcquired(projectId(PROJECT), sandbox, undefined, {
      onSandboxAcquired: candidate => {
        exposed = candidate === sandbox;
      },
    });

    expect(result.sandbox).toBe(sandbox);
    expect(exposed).toBe(true);
    expect(account.calls.some(call => call.path === "/api/backend/1/new")).toBe(false);
    expect(account.calls[0].path).toBe(`/api/projects/${PROJECT}/sandbox/resources-token`);
  });

  it("quarantines a repeatedly failed sandbox session and periodically reprobes it", async () => {
    const sandbox = parseProvisionedSandbox({
      url: "/s/sandboxes/proxy",
      token: "sandbox-token",
      sandbox_id: SANDBOX,
      sandbox_session_id: SESSION,
    });
    const clock = new FakeClock();
    const account = new ScriptedAccountSession([
      ...successResponses().slice(1),
      ...successResponses().slice(1),
      ...successResponses().slice(1),
    ]);
    const provisioning = client(account, clock);

    // One transient generation failure may retry the same session.
    provisioning.reportSandboxFailure(projectId(PROJECT), sandbox);
    await expect(provisioning.provisionAcquired(projectId(PROJECT), sandbox))
      .resolves.toMatchObject({ sandbox });
    provisioning.reportSandboxReady(projectId(PROJECT), sandbox);

    // Two consecutive failures fence only this exact session before any
    // credential or workspace mutation is attempted.
    provisioning.reportSandboxFailure(projectId(PROJECT), sandbox);
    provisioning.reportSandboxFailure(projectId(PROJECT), sandbox);
    const callsBeforeFence = account.calls.length;
    await expect(provisioning.provisionAcquired(projectId(PROJECT), sandbox))
      .rejects.toMatchObject({
        name: "ProvisioningError",
        kind: "reprovision",
        status: 409,
      });
    expect(account.calls).toHaveLength(callsBeforeFence);

    const freshSandbox = parseProvisionedSandbox({
      url: "/s/sandboxes/proxy-fresh",
      token: "sandbox-token-fresh",
      sandbox_id: "750ba53d-dc42-40f0-933b-a64bdf3656cb",
      sandbox_session_id: "sandbox-session-2",
    });
    await expect(provisioning.provisionAcquired(projectId(PROJECT), freshSandbox))
      .resolves.toMatchObject({ sandbox: freshSandbox });

    // The bounded quarantine permits a later same-project probe when Prism
    // cannot supply a different physical session.
    clock.value += 5 * 60_000 + 1;
    await expect(provisioning.provisionAcquired(projectId(PROJECT), sandbox))
      .resolves.toMatchObject({ sandbox });
  });

  it("rejects a mismatched Y-Sweet document before syncing it into the sandbox", async () => {
    const sandbox = parseProvisionedSandbox({
      url: "/s/sandboxes/proxy",
      token: "sandbox-token",
      sandbox_id: SANDBOX,
      sandbox_session_id: SESSION,
    });
    const foreignProject = "69288e8b-c183-45f3-9131-d64b05b98928";
    const responses = successResponses().slice(1, 4);
    responses[2] = json(200, {
      url: `wss://prism.test/y/d/${foreignProject}/ws`,
      baseUrl: `https://prism.test/y/d/${foreignProject}`,
      docId: foreignProject,
      token: "foreign-y-token",
      authorization: "full",
    });
    const account = new ScriptedAccountSession(responses);

    await expect(
      client(account).provisionAcquired(projectId(PROJECT), sandbox),
    ).rejects.toMatchObject({
      name: "ProvisioningError",
      kind: "protocol",
      message: "Y-Sweet mint returned a credential for another project",
    });
    expect(account.calls.map(call => call.path)).toEqual([
      `/api/projects/${PROJECT}/sandbox/resources-token`,
      "/s/sandboxes/proxy/resources-token",
      "/api/y",
    ]);
  });

  it("accepts the proven legacy readiness shape", async () => {
    const responses = successResponses();
    responses.splice(-2, 2, json(200, {
      status: "synced",
      tokens: { hasCurrentYSweetToken: true, hasSyncedYSweetProvider: true },
    }));
    await expect(client(new ScriptedAccountSession(responses)).provision(projectId(PROJECT))).resolves.toBeDefined();
  });

  it("times out workspace synchronization on condition-based deadlines", async () => {
    const clock = new FakeClock();
    const prefix = successResponses().slice(0, 5);
    const syncing = Array.from({ length: 6 }, () => json(200, {
      readinessCapabilities: ["current_y_sweet_provider"],
      status: "syncing",
      tokens: { hasCurrentYSweetToken: true, hasSyncedYSweetProvider: false },
    }));
    const pending = client(
      new ScriptedAccountSession([...prefix, ...syncing]),
      clock,
      { workspaceSyncTimeoutMs: 500 },
    ).provision(projectId(PROJECT));

    await expect(pending).rejects.toMatchObject({ kind: "timeout" });
    expect(clock.value).toBeGreaterThanOrEqual(1_500);
  });

  it("classifies sandbox 409 as reprovisionable", async () => {
    const responses = successResponses().slice(0, 5);
    responses.push(json(409, { error: "expired" }));
    try {
      await client(new ScriptedAccountSession(responses)).provision(projectId(PROJECT));
      throw new Error("expected ProvisioningError");
    } catch (error) {
      expect(error).toBeInstanceOf(ProvisioningError);
      expect((error as ProvisioningError).kind).toBe("reprovision");
      expect((error as ProvisioningError).status).toBe(409);
    }
  });

  it("retries a rate-limited backend acquisition using Retry-After", async () => {
    const clock = new FakeClock();
    const responses = successResponses();
    responses.unshift(new Response("rate limited", { status: 429, headers: { "retry-after": "2" } }));
    await client(new ScriptedAccountSession(responses), clock).provision(projectId(PROJECT));
    expect(clock.sleeps[0]).toBe(2_000);
  });

  it("supports bounded acquisition attempts for externally coordinated retries", async () => {
    const once = new ScriptedAccountSession([
      new Response("temporary", { status: 503 }),
    ]);
    await expect(client(once).acquireSandbox(undefined, { maxAttempts: 1 })).rejects.toMatchObject({
      kind: "transport",
      status: 503,
    });
    expect(once.calls).toHaveLength(1);

    await expect(client(new ScriptedAccountSession([
      new Response("rate limited", { status: 429, headers: { "retry-after": "7" } }),
    ])).acquireSandbox(undefined, { maxAttempts: 1 })).rejects.toMatchObject({
      kind: "rate_limit",
      status: 429,
      retryAfterMs: 7_000,
    });

    await expect(client(new ScriptedAccountSession([])).acquireSandbox(undefined, {
      maxAttempts: 0,
    })).rejects.toBeInstanceOf(RangeError);
    await expect(client(new ScriptedAccountSession([])).acquireSandbox(undefined, {
      maxAttempts: 11,
    })).rejects.toBeInstanceOf(RangeError);
  });

  it("honors an already-aborted provisioning signal", async () => {
    const abort = new AbortController();
    const reason = new DOMException("caller stopped before provisioning", "AbortError");
    abort.abort(reason);
    await expect(client(new ScriptedAccountSession(successResponses())).provision(
      projectId(PROJECT),
      abort.signal,
    )).rejects.toBe(reason);
  });

  it("bounds each workspace poll by the remaining outer synchronization deadline", async () => {
    const prefix = new ScriptedAccountSession(successResponses().slice(0, 5));
    let calls = 0;
    const account: AccountRequestClient = {
      request: async (path, init) => {
        calls++;
        if (calls <= 5) return prefix.request(path, init);
        return new Promise<Response>(() => undefined);
      },
    };
    const startedAt = performance.now();

    await expect(client(account, new FakeClock(), {
      workspaceSyncTimeoutMs: 20,
      requestTimeoutMs: 1_000,
    }).provision(projectId(PROJECT))).rejects.toMatchObject({ kind: "timeout" });

    expect(performance.now() - startedAt).toBeLessThan(250);
    expect(calls).toBe(6);
  });

  it("bounds a Prism request even when the outer startup signal remains live", async () => {
    let calls = 0;
    const hanging: AccountRequestClient = {
      request: async () => {
        calls++;
        // AccountSession can be stuck in reload/jar persistence where the
        // request AbortSignal cannot settle this operation.
        return new Promise<Response>(() => undefined);
      },
    };
    const provisioning = client(hanging, new FakeClock(), { requestTimeoutMs: 10 });

    await expect(provisioning.acquireSandbox())
      .rejects.toMatchObject({
        name: "ProvisioningError",
        kind: "timeout",
      });
    await Promise.all(Array.from({ length: 8 }, () => (
      expect(provisioning.acquireSandbox()).rejects.toMatchObject({ kind: "timeout" })
    )));
    // A timeout is an ambiguous outcome and must not replay a mutating POST.
    expect(calls).toBe(1);
  });

  it("keeps one total Prism deadline through a non-cooperative response body", async () => {
    let calls = 0;
    let bodyCancelled = false;
    const account: AccountRequestClient = {
      request: async () => {
        calls++;
        if (calls === 1) {
          return new Response(new ReadableStream({
            start() {},
            cancel() { bodyCancelled = true; },
          }), { status: 200 });
        }
        return json(200, {
          url: "/s/sandboxes/proxy",
          token: "sandbox-token",
          sandbox_id: SANDBOX,
          sandbox_session_id: SESSION,
        });
      },
    };
    const provisioning = client(account, new FakeClock(), { requestTimeoutMs: 10 });

    await expect(provisioning.acquireSandbox()).rejects.toMatchObject({
      name: "ProvisioningError",
      kind: "timeout",
      message: "Prism request timed out",
    });
    expect(bodyCancelled).toBe(true);
    await new Promise<void>(resolve => setTimeout(resolve, 0));
    await expect(provisioning.acquireSandbox()).resolves.toMatchObject({ sandboxId: SANDBOX });
    expect(calls).toBe(2);
  });

  it("rejects a body that completes only after the total deadline", async () => {
    let calls = 0;
    const account: AccountRequestClient = {
      request: async () => {
        calls++;
        if (calls === 1) {
          return new Response(new ReadableStream<Uint8Array>({
            pull(controller) {
              const releaseAt = performance.now() + 20;
              while (performance.now() < releaseAt) { /* delayed body production */ }
              controller.enqueue(new TextEncoder().encode(JSON.stringify({ late: true })));
              controller.close();
            },
          }), { status: 200 });
        }
        return json(200, {
          url: "/s/sandboxes/proxy",
          token: "sandbox-token",
          sandbox_id: SANDBOX,
          sandbox_session_id: SESSION,
        });
      },
    };
    const provisioning = client(account, new FakeClock(), { requestTimeoutMs: 5 });

    await expect(provisioning.acquireSandbox()).rejects.toMatchObject({ kind: "timeout" });
    await expect(provisioning.acquireSandbox()).resolves.toMatchObject({ sandboxId: SANDBOX });
    expect(calls).toBe(2);
  });

  it("rejects an oversized Prism body before JSON parsing", async () => {
    const account: AccountRequestClient = {
      request: async () => new Response("123456789", { status: 200 }),
    };

    await expect(client(account, new FakeClock(), {
      requestTimeoutMs: 1_000,
      responseBodyLimitBytes: 8,
    }).acquireSandbox()).rejects.toMatchObject({
      name: "ProvisioningError",
      kind: "protocol",
      status: 200,
      message: "Prism response body exceeded the bounded limit",
    });
  });

  it("stops waiting immediately when a caller aborts a non-cooperative Prism request", async () => {
    let entered!: () => void;
    const requestEntered = new Promise<void>(resolve => { entered = resolve; });
    const account: AccountRequestClient = {
      request: async () => {
        entered();
        return new Promise<Response>(() => undefined);
      },
    };
    const controller = new AbortController();
    const reason = new DOMException("startup stopped", "AbortError");
    const pending = client(account, new FakeClock(), { requestTimeoutMs: 1_000 })
      .acquireSandbox(controller.signal);
    await requestEntered;

    controller.abort(reason);

    await expect(Promise.race([
      pending,
      new Promise<never>((_resolve, reject) => {
        setTimeout(() => reject(new Error("caller abort did not settle the request")), 100);
      }),
    ])).rejects.toBe(reason);
  });

  it("preserves caller cancellation while a Prism response body is stalled", async () => {
    let bodyStarted!: () => void;
    const started = new Promise<void>(resolve => { bodyStarted = resolve; });
    const account: AccountRequestClient = {
      request: async () => new Response(new ReadableStream({
        start() { bodyStarted(); },
      }), { status: 200 }),
    };
    const controller = new AbortController();
    const reason = new DOMException("startup stopped during body", "AbortError");
    const pending = client(account, new FakeClock(), { requestTimeoutMs: 1_000 })
      .acquireSandbox(controller.signal);
    await started;

    controller.abort(reason);

    await expect(pending).rejects.toBe(reason);
  });

  it("discards a late Prism response after the total request deadline", async () => {
    let resolveRequest!: (response: Response) => void;
    let bodyCancelled = false;
    const account: AccountRequestClient = {
      request: async () => new Promise<Response>(resolve => { resolveRequest = resolve; }),
    };
    const pending = client(account, new FakeClock(), { requestTimeoutMs: 10 }).acquireSandbox();

    await expect(pending).rejects.toMatchObject({ kind: "timeout" });
    resolveRequest(new Response(new ReadableStream({
      cancel() { bodyCancelled = true; },
    })));
    await new Promise<void>(resolve => setTimeout(resolve, 0));

    expect(bodyCancelled).toBe(true);
  });

  it("consumes a late Prism rejection after the total request deadline", async () => {
    let rejectRequest!: (error: unknown) => void;
    const account: AccountRequestClient = {
      request: async () => new Promise<Response>((_resolve, reject) => { rejectRequest = reject; }),
    };
    const pending = client(account, new FakeClock(), { requestTimeoutMs: 10 }).acquireSandbox();

    await expect(pending).rejects.toMatchObject({ kind: "timeout" });
    rejectRequest(new Error("late account failure"));
    await new Promise<void>(resolve => setTimeout(resolve, 0));
  });

  it("rejects a response completed after the deadline while the event loop was blocked", async () => {
    const account: AccountRequestClient = {
      request: async () => {
        const releaseAt = performance.now() + 20;
        while (performance.now() < releaseAt) { /* simulate synchronous jar work */ }
        return json(200, {
          url: "/s/sandboxes/proxy",
          token: "sandbox-token",
          sandbox_id: SANDBOX,
          sandbox_session_id: SESSION,
        });
      },
    };

    await expect(client(account, new FakeClock(), { requestTimeoutMs: 5 }).acquireSandbox())
      .rejects.toMatchObject({ kind: "timeout" });
  });

  it("classifies account authentication and invalid JSON without leaking responses", async () => {
    const authAccount: AccountRequestClient = {
      request: async () => { throw new AccountAuthError(401, "/api/backend/1/new"); },
    };
    await expect(client(authAccount).provision(projectId(PROJECT))).rejects.toMatchObject({
      kind: "auth",
      status: 401,
    });

    const invalid = new ScriptedAccountSession([
      new Response("not json", { status: 200, headers: { "content-type": "text/plain" } }),
    ]);
    await expect(client(invalid).provision(projectId(PROJECT))).rejects.toMatchObject({
      kind: "protocol",
    });
  });

  it("refreshes resource credentials for the same sandbox session", async () => {
    const responses = successResponses();
    responses.push(
      json(200, { access_token: "resource-token-2", max_age_seconds: 3600, resources_base_url: "https://prism.test/s/sandbox-resources/" }),
      json(200, { success: true }),
    );
    const account = new ScriptedAccountSession(responses);
    const provisioning = client(account);
    const current = await provisioning.provision(projectId(PROJECT));

    const refreshed = await provisioning.refreshResources(current);

    expect(refreshed.token.reveal()).toBe("resource-token-2");
    expect(account.calls.at(-2)?.json).toEqual({ sandbox_session_id: SESSION });
    expect(account.calls.at(-1)?.json).toMatchObject({ token: "resource-token-2", projectId: PROJECT });
  });
});
