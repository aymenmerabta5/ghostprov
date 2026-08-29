import { afterEach, describe, expect, it } from "bun:test";
import { mkdir, mkdtemp, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import * as path from "node:path";
import {
  apiKeyAuthorized,
  createAuthenticatedTenantScope,
  resolveGhostProvApiKey,
} from "../src/http/api-key";
import { GhostProvHostSupervisor } from "../src/host/supervisor";
import { observabilityEvent } from "../src/observability/events";
import {
  ObservabilityLogger,
  type ObservabilitySink,
} from "../src/observability/logger";
import {
  assertSafeObservabilityEvent,
  safeObservabilityModel,
  sanitizeDiagnosticMessage,
} from "../src/observability/redaction";
import { RotatingJsonlSink } from "../src/observability/rotating-jsonl";
import { createGhostProvApp, type RouterLike } from "../src/server/app";
import { candidateTransportSnapshot } from "../scripts/verify_persisted_continuation";
import { V2_R8_RELEASE } from "../src/v2/sidecar/assets";

const API_KEY = "test-ghostprov-key-0123456789abcdef-0123456789";
const PROJECT_ID = "11111111-1111-4111-8111-111111111111";
const temporaryDirectories: string[] = [];
const supervisors: GhostProvHostSupervisor[] = [];

afterEach(async () => {
  await Promise.allSettled(supervisors.splice(0).map(supervisor => supervisor.close()));
  await Promise.all(temporaryDirectories.splice(0).map(directory => (
    rm(directory, { recursive: true, force: true })
  )));
});

function routerWithPrivateCanaries(): RouterLike {
  const authorizationScope = createAuthenticatedTenantScope();
  return {
    responseProvenanceAuthorizationScope: () => authorizationScope,
    async startRelay() { throw new Error("security boundary test must not relay"); },
    snapshot: () => ({
      capacity: 1,
      active: 0,
      available: 1,
      queued: 0,
      resources: {
        reservedBytes: 123,
        reservationCapacityBytes: 456,
        paused: false,
        queued: 0,
      },
      runtimes: [{
        projectId: PROJECT_ID,
        incarnation: 9,
        state: "ready",
        capacity: 1,
        active: 0,
        available: 1,
        quarantined: 0,
        heartbeatAgeMs: 1,
        credentialExpiresInMs: 60_000,
        ackEwmaMs: 1,
        completionEwmaMs: 2,
        recentFailureRate: 0,
        queueDepth: 0,
        memoryPressure: 0,
        failure: null,
        diagnostics: { host: { windowFrames: 2, apiKey: 123, prompt: 456 } },
        apiKey: "CANARY_RUNTIME_API_KEY",
        refresh_token: "CANARY_RUNTIME_REFRESH_TOKEN",
        prompt: "CANARY_RUNTIME_PROMPT",
        upstreamOrganization: {
          id: "org_PRIVATE_ACCOUNT_CANARY",
          model: "gpt-5.6-sol",
          source: "rate_limit",
          observedAgoMs: 1,
        },
      } as any],
    }),
  };
}

describe("public authentication and authority boundaries", () => {
  it("requires an explicit high-entropy inbound API key", () => {
    expect(() => resolveGhostProvApiKey(undefined)).toThrow("required");
    expect(() => resolveGhostProvApiKey("")).toThrow("required");
    expect(() => resolveGhostProvApiKey("GHOST_IS_KING")).toThrow();
    expect(() => resolveGhostProvApiKey("short-key")).toThrow("at least 32");
    expect(() => resolveGhostProvApiKey(`${"x".repeat(31)} y`)).toThrow("single-token");
    expect(resolveGhostProvApiKey(API_KEY)).toBe(API_KEY);
    expect(apiKeyAuthorized({ authorization: `Bearer ${API_KEY}` }, API_KEY, "openai"))
      .toBe(true);
  });

  it("keeps health minimal while authenticating and positively shaping sandbox details", async () => {
    const app = createGhostProvApp({
      router: routerWithPrivateCanaries(),
      apiKey: API_KEY,
      fleetSnapshot: () => ({
        desired: 2,
        ready: 1,
        unavailable: 1,
        unavailableProjects: [PROJECT_ID],
      } as any),
    });

    const health = await app.request("/health");
    const healthText = await health.text();
    expect(health.status).toBe(200);
    expect(healthText).not.toContain(PROJECT_ID);
    expect(healthText).not.toContain("releaseRevision");
    expect(healthText).not.toContain("resources");

    const missing = await app.request("/__sandboxes");
    expect(missing.status).toBe(401);
    const authorized = await app.request("/__sandboxes", {
      headers: { authorization: `Bearer ${API_KEY}` },
    });
    const text = await authorized.text();
    expect(authorized.status).toBe(200);
    expect(text).toContain(PROJECT_ID);
    for (const canary of [
      "CANARY_RUNTIME_API_KEY",
      "CANARY_RUNTIME_REFRESH_TOKEN",
      "CANARY_RUNTIME_PROMPT",
      "org_PRIVATE_ACCOUNT_CANARY",
    ]) expect(text).not.toContain(canary);
    expect(text).not.toMatch(/"(?:apiKey|prompt)"/);
  });

  it("rejects hostile Host, forwarded Host, and browser Origin while allowing explicit hosts", async () => {
    const router = routerWithPrivateCanaries();
    const app = createGhostProvApp({ router, apiKey: API_KEY });
    const headers = { authorization: `Bearer ${API_KEY}` };
    expect((await app.request("http://attacker.example/v1/models", { headers })).status)
      .toBe(421);
    expect((await app.request("http://attacker.test/v1/models", { headers })).status)
      .toBe(421);
    expect((await app.request("http://localhost/v1/models", {
      headers: { ...headers, origin: "https://attacker.example" },
    })).status).toBe(403);
    expect((await app.request("http://localhost/v1/models", {
      headers: { ...headers, origin: "https://attacker.test" },
    })).status).toBe(403);
    expect((await app.request("http://localhost/v1/models", {
      headers: { ...headers, "x-forwarded-host": "attacker.example" },
    })).status).toBe(421);
    expect((await app.request("http://127.0.0.1/v1/models", { headers })).status).toBe(200);

    const explicitlyAllowed = createGhostProvApp({
      router,
      apiKey: API_KEY,
      allowedHosts: ["ghostprov.test"],
    });
    expect((await explicitlyAllowed.request("http://ghostprov.test/v1/models", {
      headers: { ...headers, origin: "http://ghostprov.test" },
    })).status).toBe(200);
  });

  it("rejects unauthenticated and unknown requests before reserving the restart queue", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "ghostprov-auth-supervisor-"));
    temporaryDirectories.push(directory);
    const supervisor = new GhostProvHostSupervisor({
      externalPort: 0,
      apiKey: API_KEY,
      cwd: process.cwd(),
      workerEntry: path.resolve("tests/support/fake-supervised-worker.ts"),
      workerEnv: { GP_TEST_SUPERVISOR_RESTART_READY_DELAY_MS: "500" },
      readyTimeoutMs: 5_000,
      gracefulShutdownMs: 1_000,
      requestWaitMs: 2_000,
      restartBaseMs: 10,
      restartMaxMs: 20,
      stableResetMs: 100,
      sampleIntervalMs: 10_000,
      maxQueuedRequests: 4,
      maxQueuedBytes: 4_096,
      controlFile: path.join(directory, "control"),
      stopFile: path.join(directory, "stop"),
      statusFile: path.join(directory, "status"),
      logFile: path.join(directory, "log"),
    });
    supervisors.push(supervisor);
    await supervisor.start();
    const root = `http://127.0.0.1:${supervisor.snapshot().externalPort}`;
    expect((await fetch(`${root}/v1/messages`, {
      headers: { "x-api-key": API_KEY },
    })).status).toBe(200);
    expect((await fetch(`${root}/v1/models`, {
      headers: { "x-api-key": API_KEY },
    })).status).toBe(401);
    expect((await fetch(`${root}/v1/models`, {
      headers: {
        host: "attacker.example",
        authorization: `Bearer ${API_KEY}`,
      },
    })).status).toBe(421);
    expect((await fetch(`${root}/v1/models`, {
      headers: {
        host: "attacker.test",
        authorization: `Bearer ${API_KEY}`,
      },
    })).status).toBe(421);
    expect((await fetch(`${root}/v1/models`, {
      headers: {
        origin: "https://attacker.example",
        authorization: `Bearer ${API_KEY}`,
      },
    })).status).toBe(403);
    const restarting = supervisor.requestRestart("security-boundary-test");

    const unauthorized = await fetch(`${root}/v1/models`);
    const unknown = await fetch(`${root}/not-a-public-route`);
    expect(unauthorized.status).toBe(401);
    expect(unknown.status).toBe(404);
    expect(supervisor.snapshot()).toMatchObject({ queuedRequests: 0, queuedBytes: 0 });

    const authorized = fetch(`${root}/v1/models`, {
      headers: { authorization: `Bearer ${API_KEY}` },
    });
    for (let attempt = 0; attempt < 100 && supervisor.snapshot().queuedRequests < 1; attempt++) {
      await Bun.sleep(5);
    }
    expect(supervisor.snapshot().queuedRequests).toBe(1);
    await restarting;
    expect((await authorized).status).toBe(200);
  }, 10_000);

  it("authenticates the persisted-continuation sandbox snapshot", async () => {
    const originalFetch = globalThis.fetch;
    let authorization: string | null = null;
    globalThis.fetch = (async (input: string | URL | Request, init?: RequestInit) => {
      expect(String(input)).toBe("http://127.0.0.1:18888/__sandboxes");
      authorization = new Headers(init?.headers).get("authorization");
      const runtime = (index: number) => ({
        projectId: `00000000-0000-4000-8000-${String(index).padStart(12, "0")}`,
        state: "ready",
        releaseRevision: V2_R8_RELEASE.revision,
        protocolRevision: V2_R8_RELEASE.protocolRevision,
        transportProfile: V2_R8_RELEASE.transportProfile,
        buildHash: V2_R8_RELEASE.buildHash,
        schemaHash: V2_R8_RELEASE.schemaHash,
        incarnation: 1,
        quarantined: 0,
        ambiguousQuarantined: 0,
        diagnostics: { host: {
          dataPublishedBytesTotal: 0,
          flightCompletionsTotal: 0,
          retransmittedBytesTotal: 0,
        } },
      });
      return Response.json({
        ready: 2,
        unavailable: 0,
        capacity: 16,
        available: 16,
        runtimes: [runtime(1), runtime(2)],
      });
    }) as typeof fetch;
    try {
      await expect(candidateTransportSnapshot(API_KEY)).resolves.toMatchObject({
        ready: 2,
        capacity: 16,
      });
      expect(String(authorization)).toBe(`Bearer ${API_KEY}`);
    } finally {
      globalThis.fetch = originalFetch;
    }
  });
});

describe("observability confidentiality and bounds", () => {
  it("redacts structural credentials and every URL scheme before allowlist validation", () => {
    for (const [raw, canaries] of [
      ['provider {"api_key":"CANARY_JSON_SECRET_42"}', ["CANARY_JSON_SECRET_42"]],
      ["refresh_token=CANARY_REFRESH_SECRET_42", ["CANARY_REFRESH_SECRET_42"]],
      ["password=CANARY_PASSWORD_SECRET_42", ["CANARY_PASSWORD_SECRET_42"]],
      ["connect wss://private.example/socket?token=CANARY_WS_SECRET_42", ["private.example", "CANARY_WS_SECRET_42"]],
      ["open file:///private/path/config.json", ["/private/path"]],
      ["rate limit in organization org-PRIVATE_ACCOUNT", ["org-PRIVATE_ACCOUNT"]],
      ['provider {"organization":"org-PRIVATE_ACCOUNT"}', ["org-PRIVATE_ACCOUNT"]],
    ] as const) {
      const sanitized = sanitizeDiagnosticMessage(raw);
      for (const canary of canaries) expect(sanitized).not.toContain(canary);
      expect(() => assertSafeObservabilityEvent(observabilityEvent("request.failed", {
        error_message: sanitized,
      }))).not.toThrow();
    }
  });

  it("reduces request-controlled model values to a bounded safe category", () => {
    expect(safeObservabilityModel("gpt-5.6-sol")).toBe("gpt-5.6-sol");
    expect(safeObservabilityModel("PRIVATE PROMPT IN MODEL")).toBe("unknown");
    expect(safeObservabilityModel("x".repeat(129))).toBe("unknown");
    expect(safeObservabilityModel(null)).toBe("unknown");
  });

  it("rejects an event larger than its real per-event and queue byte limit", async () => {
    const lines: string[] = [];
    const sink: ObservabilitySink = {
      async writeBatch(batch) { lines.push(...batch); },
      async close() {},
    };
    const logger = new ObservabilityLogger({
      config: {
        level: "info",
        directory: "unused",
        rotateBytes: 64 * 1024 * 1024,
        retainFiles: 5,
        queueMaxEvents: 10,
        queueMaxBytes: 1024 * 1024,
        flushMs: 60_000,
        metricsIntervalMs: 5_000,
        slowRequestMs: 10_000,
        slowFirstEventMs: 5_000,
      },
      sinks: [sink],
    });
    logger.emit(observabilityEvent("request.accepted", {
      model: "M".repeat(2 * 1024 * 1024),
      service_tier: "fast",
    }), "progress");
    expect(logger.snapshot()).toMatchObject({
      queuedEvents: 0,
      queuedBytes: 0,
      droppedProgress: 1,
    });
    await logger.close();
    expect(lines.some(line => line.includes('"event":"request.accepted"'))).toBe(false);
    expect(lines.every(line => Buffer.byteLength(line) <= 2 * 1024)).toBe(true);
  });

  it("snapshots bounded event data so post-enqueue mutation cannot escape accounting", async () => {
    const lines: string[] = [];
    const sink: ObservabilitySink = {
      async writeBatch(batch) { lines.push(...batch); },
      async close() {},
    };
    const logger = new ObservabilityLogger({
      config: {
        level: "info",
        directory: "unused",
        rotateBytes: 64 * 1024 * 1024,
        retainFiles: 5,
        queueMaxEvents: 10,
        queueMaxBytes: 1024 * 1024,
        flushMs: 60_000,
        metricsIntervalMs: 5_000,
        slowRequestMs: 10_000,
        slowFirstEventMs: 5_000,
      },
      sinks: [sink],
    });
    const resources: { reserved_bytes: number; password?: string } = { reserved_bytes: 1 };
    logger.emit(observabilityEvent("fleet.sample", { resources }), "sample");
    resources.password = "MUTATED_SECRET_CANARY".repeat(100_000);
    await logger.close();
    expect(lines.some(line => line.includes('"reserved_bytes":1'))).toBe(true);
    expect(lines.join("")).not.toContain("MUTATED_SECRET_CANARY");
  });

  it("enforces prefix-wide retention while preserving a possibly live current file", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "ghostprov-global-logs-"));
    temporaryDirectories.push(directory);
    const prefix = "ghostprov-security-test";
    const live = `${prefix}.current.${process.pid}.jsonl`;
    await mkdir(directory, { recursive: true });
    await writeFile(path.join(directory, live), "live\n");

    for (const pid of [700_001, 700_002, 700_003]) {
      let now = Date.UTC(2026, 7, 29);
      const sink = new RotatingJsonlSink({
        directory,
        prefix,
        rotateBytes: 180,
        retainFiles: 1,
        pid,
        isProcessAlive: candidate => candidate === process.pid,
        now: () => now += 1_000,
      });
      for (let index = 0; index < 4; index++) {
        await sink.writeBatch([`${JSON.stringify({ pid, index, pad: "x".repeat(70) })}\n`]);
      }
      await sink.close();
    }

    const names = (await readdir(directory)).filter(name => name.startsWith(prefix));
    expect(names).toContain(live);
    expect(names.filter(name => name !== live)).toHaveLength(1);
  });
});
