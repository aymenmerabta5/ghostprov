import { describe, expect, it } from "bun:test";
import type { Clock, RandomSource } from "../src/core/ports";
import {
  PrismProvisioningClient,
  ProvisioningError,
  type AccountRequestClient,
} from "../src/prism/provisioning-client";
import { AccountAuthError } from "../src/prism/account-session";
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
    responses[1] = call => {
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

  it("honors an already-aborted provisioning signal", async () => {
    const abort = new AbortController();
    abort.abort();
    await expect(client(new ScriptedAccountSession(successResponses())).provision(
      projectId(PROJECT),
      abort.signal,
    )).rejects.toMatchObject({ name: "AbortError" });
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
