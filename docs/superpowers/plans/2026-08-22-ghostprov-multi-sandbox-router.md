# GhostProv Browserless Multi-Sandbox Router Implementation Plan

> **Historical and superseded continuation policy (2026-08-28):** This document preserves the design record as written. Any upstream `store:false`, stateless replay, or local generated-history requirement below is superseded by the active `stream:true`/`store:true` persisted-continuation policy in `README.md` and `AGENTS.md`.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace GhostProv's Playwright-backed singleton Prism transport with a typed browserless router over independent Prism projects, providing adaptive machine-aware admission, intelligent least-work routing, exact 10-second per-sandbox heartbeats, and up to 120 physical lanes across four 30-slot daemons.

**Architecture:** One authenticated `PrismAccountSession` owns the shared account cookie jar. A `SandboxRegistry` owns one isolated `SandboxRuntime` per allowlisted project; each runtime owns provisioning, Yjs, gateway, heartbeat, daemon, ZIP, and slot state. `MultiSandboxRouter` acquires global resource reservations and dispatches each request to the healthy runtime with the lowest predicted completion time while preserving terminal-proof semantics.

**Tech Stack:** Bun 1.4.0, TypeScript 7.0.2, Hono 4.6, Yjs 13.6, y-websocket 2.0, ws 8.18, fflate 0.8, tough-cookie 6, Bun test with `--isolate`, Python daemon tests.

## Global Constraints

- Work only in `D:\MyWork\AI\ghostprov\.worktrees\multi-sandbox-router` on `br/multi-sandbox-router`.
- Never stop, restart, rebind, or mutate the existing process on `localhost:8787`.
- Candidate live servers must reject port 8787 and default to port 18787.
- Use no subagents.
- Every sandbox uses a separate explicitly allowlisted Prism project/document under the same account cookie jar.
- Never select or modify arbitrary account projects.
- Every syncing, deploying, ready, or draining sandbox receives an independent heartbeat on monotonic 10,000 ms deadlines.
- No request may move to another sandbox after authoritative publication.
- No published lane is released without matching ACK, request identity, and terminal proof.
- Physical capacity is the sum of healthy non-quarantined daemon lanes; four 30-slot runtimes may advertise 120 lanes.
- Admission also requires adaptive byte and CPU reservations derived from machine specs and live pressure.
- Remove Playwright only after direct parity and candidate-port proof pass.
- Preserve existing HTTP, SSE, Codex WebSocket, Images, error, and `store:false` contracts.
- Provisioning, token refresh, and daemon deployment must never execute on the steady-state request path.
- Router decision p95 must stay below 2 ms; under-capacity queue p95 below 250 ms; ≤2 MiB publish p50/p95 below 600/1500 ms; first-event overhead within 10% of the single-project candidate.
- Serialize each child request once and keep gzip, ZIP, and large base64 work off the main event loop.
- Use red-green TDD for every behavior change.
- Use Bun 1.4.0, `bun test --isolate`, and TypeScript 7.0.2.
- Bun remains the production runtime; ScriptC is an optional post-Playwright A/B gate and is never adopted without full compatibility plus ≥10% p95 latency or ≥20% RSS improvement and ≤5% throughput regression.

---

### Task 1: Strong IDs, Prism schemas, and multi-project configuration

**Files:**
- Create: `src/core/ports.ts`
- Create: `src/sandbox/types.ts`
- Create: `src/prism/protocol.ts`
- Create: `src/config/multi-sandbox.ts`
- Test: `tests/multi-sandbox-config.test.ts`
- Test: `tests/prism-protocol.test.ts`

**Interfaces:**
- Produces shared `Clock`, `RandomSource`, `FetchLike`, `MachineProbe`, and `MetricsSink` ports.
- Produces branded `ProjectId`, `SandboxId`, `SandboxSessionId`, `RelayRequestId`, and `SlotId` constructors.
- Produces `SandboxLifecycleState`, `SandboxSnapshot`, `YSweetEnvelope`, `ProvisionedSandbox`, `ResourceCredential`, and validated parser functions.
- Produces `MultiSandboxConfig` and `resolveMultiSandboxConfig(env, machine)`.

- [ ] **Step 1: Write failing branded-ID and schema tests**

```ts
import { describe, expect, it } from "bun:test";
import { projectId, slotId } from "../src/sandbox/types";
import { parseProvisionedSandbox, parseYSweetEnvelope } from "../src/prism/protocol";

describe("Prism protocol types", () => {
  it("validates opaque IDs at the boundary", () => {
    expect(projectId("e631d12c-d917-4434-b360-41b4715d6679")).toBe("e631d12c-d917-4434-b360-41b4715d6679");
    expect(() => projectId("not-a-uuid")).toThrow("ProjectId");
    expect(slotId(30)).toBe(30);
    expect(() => slotId(0)).toThrow("SlotId");
  });

  it("rejects incomplete provisioning and Y-Sweet envelopes", () => {
    expect(() => parseProvisionedSandbox({ url: "/s/sandboxes/proxy" })).toThrow("token");
    expect(() => parseYSweetEnvelope({ docId: "x" })).toThrow("token");
  });
});
```

- [ ] **Step 2: Write failing configuration tests**

```ts
it("deduplicates an explicit project allowlist and preserves legacy single-project config", () => {
  expect(resolveMultiSandboxConfig({
    PRISM_PROJECT_IDS: `${A},${B},${A}`,
    PRISM_COOKIE_FILE: "D:/secrets/prism-cookies.json",
  }, MACHINE).projectIds).toEqual([projectId(A), projectId(B)]);

  expect(resolveMultiSandboxConfig({
    PRISM_DOC_ID: A,
    PRISM_COOKIE: "session=value",
  }, MACHINE).projectIds).toEqual([projectId(A)]);
});

it("rejects candidate port 8787 and missing cookie sources", () => {
  expect(() => resolveMultiSandboxConfig({ PRISM_PROJECT_IDS: A, PORT: "8787", GP_CANDIDATE: "1" }, MACHINE)).toThrow("8787");
  expect(() => resolveMultiSandboxConfig({ PRISM_PROJECT_IDS: A }, MACHINE)).toThrow("PRISM_COOKIE");
});
```

- [ ] **Step 3: Run tests and verify RED**

Run: `bun test --isolate tests/prism-protocol.test.ts tests/multi-sandbox-config.test.ts`

Expected: FAIL because the new modules do not exist.

- [ ] **Step 4: Implement branded constructors and discriminated lifecycle types**

```ts
export type Brand<T, Name extends string> = T & { readonly __brand: Name };
export type ProjectId = Brand<string, "ProjectId">;
export type SandboxId = Brand<string, "SandboxId">;
export type SandboxSessionId = Brand<string, "SandboxSessionId">;
export type RelayRequestId = Brand<string, "RelayRequestId">;
export type SlotId = Brand<number, "SlotId">;

export type SandboxLifecycleState =
  | { type: "configured"; projectId: ProjectId }
  | { type: "provisioning"; projectId: ProjectId; generation: number }
  | { type: "syncing_credentials"; projectId: ProjectId; generation: number }
  | { type: "syncing_workspace"; projectId: ProjectId; generation: number }
  | { type: "deploying_daemon"; projectId: ProjectId; generation: number }
  | { type: "ready"; projectId: ProjectId; generation: number; capacity: number }
  | { type: "draining"; projectId: ProjectId; generation: number; reason: string }
  | { type: "recovering"; projectId: ProjectId; generation: number; attempt: number }
  | { type: "auth_required"; projectId: ProjectId; generation: number }
  | { type: "failed"; projectId: ProjectId; generation: number; error: Error }
  | { type: "stopped"; projectId: ProjectId; generation: number };
```

Define the shared production/test ports in `src/core/ports.ts`:

```ts
export type FetchLike = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;

export interface Clock {
  now(): number;
  sleep(ms: number, signal?: AbortSignal): Promise<void>;
  waitUntil(deadlineMs: number, signal?: AbortSignal): Promise<void>;
}

export interface RandomSource {
  next(): number;
  uuid(): string;
}

export interface MachineSample {
  totalBytes: number;
  freeBytes: number;
  rssBytes: number;
  heapBytes: number;
  externalBytes: number;
  logicalProcessors: number;
}

export interface MachineProbe {
  sample(): MachineSample;
}

export interface MetricsSink {
  count(name: string, value?: number, attributes?: Record<string, string | number | boolean>): void;
  observe(name: string, value: number, attributes?: Record<string, string | number | boolean>): void;
}
```

- [ ] **Step 5: Implement strict boundary parsers and configuration resolution**

`parseProvisionedSandbox` requires URL and sandbox token, normalizes a trailing slash, and parses optional sandbox/session IDs. `parseYSweetEnvelope` requires URL, doc ID, token, and authorization. `resolveMultiSandboxConfig` accepts only explicit project IDs, uses `PRISM_DOC_ID` as the one-project fallback, and defaults candidate mode to port 18787.

- [ ] **Step 6: Run focused tests and typecheck**

Run: `bun test --isolate tests/prism-protocol.test.ts tests/multi-sandbox-config.test.ts && bun run typecheck`

Expected: all focused tests pass and TypeScript emits no diagnostics.

- [ ] **Step 7: Commit**

```text
git add src/core/ports.ts src/sandbox/types.ts src/prism/protocol.ts src/config/multi-sandbox.ts tests/prism-protocol.test.ts tests/multi-sandbox-config.test.ts
git commit -m "feat: define multi-sandbox protocol types"
```

---

### Task 2: Persistent account cookie session without Playwright

**Files:**
- Modify: `package.json`
- Modify: `bun.lock`
- Create: `src/prism/account-session.ts`
- Test: `tests/account-session.test.ts`

**Interfaces:**
- Consumes `Clock`, `FetchLike`, cookie file path, initial Cookie header, and Prism origin.
- Produces `PrismAccountSession.request(pathOrUrl, init)` and `snapshot()`.
- Produces `AccountAuthError` and guarantees credential-redacted errors.

- [ ] **Step 1: Add the cookie dependency**

Run: `bun add tough-cookie@^6.0.0`

Expected: `package.json` and `bun.lock` contain `tough-cookie`; no Playwright change yet.

- [ ] **Step 2: Write failing cookie lifecycle tests**

```ts
it("applies matching cookies and persists Set-Cookie rotation atomically", async () => {
  const fetchCalls: Array<{ url: string; cookie: string | null }> = [];
  const session = await PrismAccountSession.create({
    origin: "https://prism.test",
    initialCookieHeader: "prism_session_token=old; prism-did=device",
    cookieFile: file,
    fetch: async (url, init) => {
      fetchCalls.push({ url: String(url), cookie: new Headers(init?.headers).get("cookie") });
      return new Response("{}", { status: 200, headers: { "set-cookie": "prism_session_token=new; Path=/; HttpOnly" } });
    },
  });

  await session.request("/api/y", { method: "POST" });
  expect(fetchCalls[0].cookie).toContain("prism_session_token=old");
  expect((await Bun.file(file).text())).toContain("prism_session_token");
  expect(session.snapshot()).not.toHaveProperty("cookies");
});
```

Add tests for hot reload, concurrent persistence serialization, domain/path matching, expired-cookie removal, 401 classification, and errors containing no cookie/token substrings.

- [ ] **Step 3: Run the focused test and verify RED**

Run: `bun test --isolate tests/account-session.test.ts`

Expected: FAIL because `PrismAccountSession` does not exist.

- [ ] **Step 4: Implement `PrismAccountSession` with `tough-cookie`**

```ts
export class PrismAccountSession {
  static async create(options: AccountSessionOptions): Promise<PrismAccountSession>;
  request(pathOrUrl: string, init?: RequestInit): Promise<Response>;
  snapshot(): AccountSessionSnapshot;
  reloadIfChanged(): Promise<void>;
}
```

Use `CookieJar.getCookieString()` before every request and `headers.getSetCookie()` after every response. Persist serialized jar JSON through a unique temporary file plus `rename`, guarded by a promise chain. Reload only when file mtime advances.

- [ ] **Step 5: Run focused tests, typecheck, and audit**

Run: `bun test --isolate tests/account-session.test.ts && bun run typecheck && bun audit --production`

Expected: tests and typecheck pass; audit reports no vulnerabilities.

- [ ] **Step 6: Commit**

```text
git add package.json bun.lock src/prism/account-session.ts tests/account-session.test.ts
git commit -m "feat: add browserless Prism account session"
```

---

### Task 3: Exact browserless sandbox provisioning protocol

**Files:**
- Create: `src/prism/provisioning-client.ts`
- Test: `tests/provisioning-client.test.ts`

**Interfaces:**
- Consumes `PrismAccountSession`, `Clock`, `RandomSource`, and validated protocol parsers.
- Produces `PrismProvisioningClient.provision(projectId, signal): Promise<ProvisionedRuntimeCredentials>`.
- Produces `refreshResources(...)`, `mintYSweet(...)`, and `waitForWorkspaceSync(...)` as focused methods.

- [ ] **Step 1: Write a failing exact-sequence test**

```ts
it("performs the proven Prism bootstrap sequence with exact request bodies", async () => {
  const transport = new ScriptedAccountSession([
    json(200, { url: "/s/sandboxes/proxy", token: "sandbox-token", sandbox_id: SANDBOX, sandbox_session_id: SESSION }),
    json(200, { access_token: "resource-token", expires_at: 2_000_000_000, resources_base_url: "https://prism.test/s/sandbox-resources/" }),
    json(200, { success: true }),
    json(200, { url: Y_URL, baseUrl: Y_BASE, docId: PROJECT, token: "y-token", authorization: "full" }),
    json(200, { success: true }),
    json(200, { readinessCapabilities: ["current_y_sweet_provider"], status: "syncing", tokens: { hasCurrentYSweetToken: true, hasSyncedYSweetProvider: false } }),
    json(200, { readinessCapabilities: ["current_y_sweet_provider"], status: "synced", tokens: { hasCurrentYSweetToken: true, hasSyncedYSweetProvider: true } }),
  ]);

  const result = await client(transport).provision(projectId(PROJECT));
  expect(result.projectId).toBe(PROJECT);
  expect(transport.calls.map(call => call.path)).toEqual([
    "/api/backend/1/new",
    `/api/projects/${PROJECT}/sandbox/resources-token`,
    "/s/sandboxes/proxy/resources-token",
    "/api/y",
    "/s/sandboxes/proxy/token",
    "/s/sandboxes/proxy/wait-for-sync",
    "/s/sandboxes/proxy/wait-for-sync",
  ]);
  expect(transport.calls[1].json).toEqual({ sandbox_session_id: SESSION });
  expect(transport.calls[2].json).toEqual({ token: "resource-token", resourceBaseUrl: "https://prism.test/s/sandbox-resources/", projectId: PROJECT });
});
```

- [ ] **Step 2: Add failing readiness/failure tests**

Cover 250 ms polling, 30-second timeout, 401 → `AccountAuthError`, 409/expired sandbox → reprovisionable failure, 429 retry-after, invalid JSON, stale generation cancellation, and resource refresh five minutes before expiry.

- [ ] **Step 3: Run focused tests and verify RED**

Run: `bun test --isolate tests/provisioning-client.test.ts`

Expected: FAIL because the provisioning client is absent.

- [ ] **Step 4: Implement the sequence with condition-based waits**

```ts
export class PrismProvisioningClient {
  async provision(projectId: ProjectId, signal?: AbortSignal): Promise<ProvisionedRuntimeCredentials>;
  async refreshResources(current: ProvisionedRuntimeCredentials, signal?: AbortSignal): Promise<ResourceCredential>;
  async mintYSweet(projectId: ProjectId, context: YSweetRequestContext, signal?: AbortSignal): Promise<YSweetEnvelope>;
  async waitForWorkspaceSync(sandbox: ProvisionedSandbox, signal?: AbortSignal): Promise<void>;
}
```

All sandbox requests include both account cookies through `PrismAccountSession` and the runtime's opaque sandbox-token header.

- [ ] **Step 5: Run focused tests and typecheck**

Run: `bun test --isolate tests/provisioning-client.test.ts && bun run typecheck`

Expected: all pass.

- [ ] **Step 6: Commit**

```text
git add src/prism/provisioning-client.ts tests/provisioning-client.test.ts
git commit -m "feat: provision Prism sandboxes without a browser"
```

---

### Task 4: Project-scoped Yjs document sessions

**Files:**
- Create: `src/sandbox/ydoc.ts`
- Test: `tests/sandbox-ydoc.test.ts`
- Unchanged in this task: `src/prism/ydoc.ts`; Task 10 replaces its active consumers before Task 11 removes the singleton implementation.

**Interfaces:**
- Consumes `ProjectId`, `YSweetEnvelope`, `YProviderFactory`, and `Clock`.
- Produces `SandboxYDoc` with `connect`, `readFile`, `writeFile`, `appendFile`, `deleteFile`, `ensureFolder`, `waitForFile`, `snapshot`, and `destroy`.

- [ ] **Step 1: Write failing isolation and reconnect tests**

```ts
it("keeps filename indexes and Yjs state isolated by project", async () => {
  const left = await createDoc(PROJECT_A);
  const right = await createDoc(PROJECT_B);
  await left.writeFile("job_s1.json", "left");
  await right.writeFile("job_s1.json", "right");
  expect(await left.readFile("job_s1.json")).toBe("left");
  expect(await right.readFile("job_s1.json")).toBe("right");
});

it("reconnects with a new Y-Sweet envelope after provider close", async () => {
  const doc = await createDoc(PROJECT_A, envelopes("token-1", "token-2"));
  provider.close();
  await doc.writeFile("x", "y");
  expect(factory.tokens).toEqual(["token-1", "token-2"]);
});
```

Also cover root/folder creation, atomic updates, appended chunks, tombstones, stale-generation suppression, sync timeout, and destroy cleanup.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `bun test --isolate tests/sandbox-ydoc.test.ts`

Expected: FAIL because `SandboxYDoc` does not exist.

- [ ] **Step 3: Implement a class-scoped port of the proven Yjs logic**

```ts
export interface SandboxYDoc {
  readonly projectId: ProjectId;
  connect(signal?: AbortSignal): Promise<void>;
  readFile(filename: string): Promise<string | null>;
  writeFile(filename: string, content: string, options?: { folderId?: string }): Promise<void>;
  appendFile(filename: string, content: string, chunkBytes?: number): Promise<void>;
  deleteFile(filename: string): Promise<boolean>;
  ensureFolder(name: string): Promise<string>;
  waitForFile(filename: string, predicate: (content: string | null) => boolean, timeoutMs: number): Promise<string>;
  snapshot(): YDocSnapshot;
  destroy(): Promise<void>;
}
```

Move connection, index, root-folder, content, append, and tombstone state onto the instance. Never read `config.docId`, bridge globals, or token files.

- [ ] **Step 4: Run focused and existing Yjs tests**

Run: `bun test --isolate tests/sandbox-ydoc.test.ts tests/ydoc.test.ts && bun run typecheck`

Expected: all pass.

- [ ] **Step 5: Commit**

```text
git add src/sandbox/ydoc.ts tests/sandbox-ydoc.test.ts
git commit -m "feat: isolate Yjs state per sandbox project"
```

---

### Task 5: Direct sandbox gateway, ZIP isolation, and exact heartbeat scheduler

**Files:**
- Create: `src/sandbox/gateway.ts`
- Create: `src/sandbox/heartbeat.ts`
- Test: `tests/sandbox-gateway.test.ts`
- Test: `tests/heartbeat.test.ts`

**Interfaces:**
- `SandboxGateway` consumes `PrismAccountSession`, `ProvisionedSandbox`, `Clock`, and one instance-local `ZipPollCoordinator`.
- `SandboxHeartbeat` consumes a gateway, monotonic `Clock`, project phase offset, and typed callbacks.
- Produces one in-flight ZIP and one in-flight heartbeat invariant per runtime.

- [ ] **Step 1: Write failing direct gateway tests**

```ts
it("sends account cookies and the runtime-specific sandbox token directly", async () => {
  const gateway = createGateway(PROJECT_A, "sandbox-A");
  await gateway.heartbeat();
  expect(account.calls[0]).toMatchObject({
    path: "/s/sandboxes/proxy/heartbeat",
    sandboxToken: "sandbox-A",
  });
});

it("coalesces named ZIP reads and never overlaps archive fetches", async () => {
  const [a, b] = await Promise.all([gateway.readFiles(["a"]), gateway.readFiles(["b"])]);
  expect(transport.maxZipActive).toBe(1);
  expect(transport.zipCalls).toBe(1);
});
```

- [ ] **Step 2: Write failing 10-second heartbeat tests with a fake monotonic clock**

```ts
it("heartbeats every sandbox on independent 10-second deadlines", async () => {
  const clock = new FakeClock();
  const heartbeats = [runtimeHeartbeat("a", clock), runtimeHeartbeat("b", clock), runtimeHeartbeat("c", clock), runtimeHeartbeat("d", clock)];
  heartbeats.forEach(heartbeat => heartbeat.start());
  await clock.advanceBy(60_000);
  for (const heartbeat of heartbeats) {
    expect(heartbeat.successfulDeadlines()).toEqual([10_000, 20_000, 30_000, 40_000, 50_000, 60_000]);
    expect(heartbeat.maxInFlight()).toBe(1);
  }
});
```

Add slow-call, delayed-tick, 401, expired-session, transient 5xx, lifecycle start/stop, and no-overlapping-catch-up cases.

- [ ] **Step 3: Run focused tests and verify RED**

Run: `bun test --isolate tests/sandbox-gateway.test.ts tests/heartbeat.test.ts`

Expected: FAIL because gateway/heartbeat modules do not exist.

- [ ] **Step 4: Implement direct gateway operations and streaming ZIP decode**

`SandboxGateway` exposes `heartbeat`, `waitForSync`, `fetchText`, `postJson`, `readFiles`, `triggerRender`, and `deleteFile`. All URLs are sandbox-runtime scoped and reuse persistent fetch connections. Feed `response.body.getReader()` chunks into `fflate.Unzip`; a missing body is a protocol error. Only one archive request/decode runs per runtime, while different runtimes decode in parallel. Active polling adapts between 60 and 250 ms without synchronized bursts.

- [ ] **Step 5: Implement monotonic deadline heartbeat**

```ts
nextDeadline = initialPhaseDeadline(projectId, startedAt);
while (!stopped) {
  await clock.waitUntil(nextDeadline, signal);
  if (!inFlight) await sendOneHeartbeat();
  nextDeadline += 10_000;
  while (nextDeadline <= clock.now()) nextDeadline += 10_000;
}
```

Initial phase is deterministic and less than 10 seconds; steady-state deadlines differ by exactly 10,000 ms.

- [ ] **Step 6: Run focused tests and typecheck**

Run: `bun test --isolate tests/sandbox-gateway.test.ts tests/heartbeat.test.ts tests/zip-coordinator.test.ts && bun run typecheck`

Expected: all pass.

- [ ] **Step 7: Commit**

```text
git add src/sandbox/gateway.ts src/sandbox/heartbeat.ts tests/sandbox-gateway.test.ts tests/heartbeat.test.ts
git commit -m "feat: add direct sandbox gateway and heartbeat"
```

---

### Task 6: Adaptive machine-aware resource governor

**Files:**
- Create: `src/router/workload.ts`
- Create: `src/router/resource-governor.ts`
- Create: `src/router/types.ts`
- Test: `tests/resource-governor.test.ts`
- Test: `tests/workload.test.ts`

**Interfaces:**
- Produces `MachineProfile`, `WorkloadDescriptor`, `ResourceReservation`, `ResourceGovernor`, and `classifyWorkload`.
- Consumes `MachineProbe`, `Clock`, request byte metadata, live RSS/free RAM, and abort signals.

- [ ] **Step 1: Write failing machine-sizing tests**

```ts
it.each([
  [8, 5, 1],
  [16, 12, 8],
  [32, 19.19, 13],
  [64, 52, 38.4],
])("derives safe process ceilings for %i GiB", (total, free, expectedGiB) => {
  const sizing = deriveResourceSizing({ totalBytes: gib(total), freeBytes: gib(free), rssBytes: mib(320), logicalProcessors: 12 });
  expect(sizing.processCeilingBytes / GiB).toBeCloseTo(expectedGiB, 0);
});
```

- [ ] **Step 2: Write failing weighted admission tests**

```ts
it("admits 120 text jobs but throttles large images by bytes rather than a fixed count", async () => {
  const governor = governorFor({ ceiling: gib(13), free: gib(19), cpuWorkers: 6 });
  const textLeases = await Promise.all(Array.from({ length: 120 }, () => governor.acquire(textWorkload(mib(2)))));
  expect(textLeases).toHaveLength(120);
  textLeases.forEach(lease => lease.release());

  const images = Array.from({ length: 120 }, () => governor.tryAcquire(imageEditWorkload(mib(40))));
  expect(images.filter(Boolean).length).toBeGreaterThan(16);
  expect(images.filter(Boolean).length).toBeLessThan(120);
});
```

Add pause/resume hysteresis, abort while queued, idempotent release, CPU permit, pre-body reservation, rolling estimate bounds, and no-starvation tests.

Add an event-loop responsiveness test proving six concurrent compression/ZIP transforms do not delay a 10 ms timer beyond the test's 50 ms bound on the measured host.

- [ ] **Step 3: Run focused tests and verify RED**

Run: `bun test --isolate tests/workload.test.ts tests/resource-governor.test.ts`

Expected: FAIL because workload/governor modules do not exist.

- [ ] **Step 4: Implement workload classification and conservative reservations**

```ts
export type WorkloadClass = "text-small" | "text-large" | "image-generation" | "image-edit";

export interface WorkloadDescriptor {
  class: WorkloadClass;
  requestBytes: number;
  reservedBytes: number;
  cpuPermits: 0 | 1;
  stream: boolean;
  model?: string;
}
```

Image-edit reservation includes raw body, base64 expansion, JSON/segment copies, and response reserve. Text reservation uses encoded bytes and bounded stream reserve. Rolling observations may raise/lower estimates only inside configured safety bounds.

CPU-heavy gzip, ZIP, and large base64 transformations run through a bounded worker executor sized by `cpuTransformWorkers`. Level-1 gzip is skipped when a bounded sample predicts that encoded size will not shrink.

- [ ] **Step 5: Implement `ResourceGovernor`**

Use byte and CPU semaphores, RSS/free-RAM sampling, pause/resume hysteresis, FIFO-with-aging waiters, and idempotent leases. Expose sanitized snapshots only.

- [ ] **Step 6: Run focused tests and typecheck**

Run: `bun test --isolate tests/workload.test.ts tests/resource-governor.test.ts && bun run typecheck`

Expected: all pass.

- [ ] **Step 7: Commit**

```text
git add src/router/workload.ts src/router/resource-governor.ts src/router/types.ts tests/workload.test.ts tests/resource-governor.test.ts
git commit -m "feat: add adaptive resource governor"
```

---

### Task 7: Sandbox lifecycle state machine and daemon controller

**Files:**
- Create: `src/sandbox/daemon-controller.ts`
- Create: `src/sandbox/runtime.ts`
- Test: `tests/daemon-controller.test.ts`
- Test: `tests/sandbox-runtime.test.ts`

**Interfaces:**
- `DaemonController.ensureReady(context, signal): Promise<DaemonDescriptor>`.
- `SandboxRuntime.start`, `drain`, `recover`, `stop`, `acquireLane`, `snapshot`, and lifecycle subscription.
- Consumes provisioning, Ydoc, gateway, heartbeat, and daemon factories.

- [ ] **Step 1: Write failing legal-transition and failure-isolation tests**

```ts
it("reaches ready through the only legal startup sequence", async () => {
  const runtime = harness(PROJECT_A);
  const states: string[] = [];
  runtime.subscribe(snapshot => states.push(snapshot.state.type));
  await runtime.start();
  expect(states).toEqual([
    "configured",
    "provisioning",
    "syncing_credentials",
    "syncing_workspace",
    "deploying_daemon",
    "ready",
  ]);
});

it("drains only the failed runtime after a stale heartbeat", async () => {
  const [left, right] = await readyPair();
  left.heartbeat.failExpired();
  expect(left.snapshot().state.type).toBe("draining");
  expect(right.snapshot().state.type).toBe("ready");
});
```

Add stale-generation, stop cleanup, resource-refresh, auth-required, reprovision, daemon mismatch, and 10-second heartbeat-active-state tests.

- [ ] **Step 2: Write failing daemon hash/deployment tests**

Assert unchanged assets are not rewritten, changed assets are written once, render is triggered once, stale daemon status is rejected, and advertised slot capacity becomes runtime capacity only after matching build/hash.

- [ ] **Step 3: Run focused tests and verify RED**

Run: `bun test --isolate tests/daemon-controller.test.ts tests/sandbox-runtime.test.ts`

Expected: FAIL because lifecycle/controller modules do not exist.

- [ ] **Step 4: Implement `DaemonController` from the proven deployment logic**

Inject daemon source, launcher source, Ydoc, gateway, expected build/protocols, and clock. Compare content hashes, write changed files, trigger bootstrap, and wait for matching `gp_status.json`.

- [ ] **Step 5: Implement `SandboxRuntime` as the sole lifecycle owner**

The runtime increments a generation on every reprovision, rejects stale async completions, starts heartbeat during credential synchronization, owns one project-scoped slot pool, and publishes immutable snapshots.

- [ ] **Step 6: Run focused tests and typecheck**

Run: `bun test --isolate tests/daemon-controller.test.ts tests/sandbox-runtime.test.ts && bun run typecheck`

Expected: all pass.

- [ ] **Step 7: Commit**

```text
git add src/sandbox/daemon-controller.ts src/sandbox/runtime.ts tests/daemon-controller.test.ts tests/sandbox-runtime.test.ts
git commit -m "feat: manage isolated sandbox runtimes"
```

---

### Task 8: Extract a project-scoped slot runner without weakening terminal proof

**Files:**
- Create: `src/relay/slot-runner.ts`
- Create: `src/sandbox/slot-pool.ts`
- Modify: `src/relay/slots.ts`
- Modify: `src/relay/pipeline.ts`
- Test: `tests/slot-runner.test.ts`
- Modify: `tests/terminal-rescue.test.ts`
- Modify: `tests/segments.test.ts`

**Interfaces:**
- Produces `SandboxSlotRunner.run(body, signal): Promise<RunHandle>`.
- Consumes one `SandboxRuntimeContext` containing project-scoped gateway, Ydoc, daemon, pool, folder/artifact state, and metrics.
- Preserves `RunHandle`, `TerminalBackedSseExtractor`, v6.3 segmentation, and terminal rescue contracts.

- [ ] **Step 1: Write failing cross-runtime isolation tests**

```ts
it("uses independent physical slot one in two projects without shared state", async () => {
  const left = runner(PROJECT_A, 30);
  const right = runner(PROJECT_B, 30);
  const [a, b] = await Promise.all([left.run(body("A")), right.run(body("B"))]);
  expect(left.publishedPaths()).toContain("job_s1.json");
  expect(right.publishedPaths()).toContain("job_s1.json");
  expect(left.gateway).not.toBe(right.gateway);
  await Promise.all([a.final, b.final]);
});
```

Add 30-lane cap, per-runtime artifact cleanup, initial SSE gap replay, failure terminal, stale envelope, pre-publication failure, post-publication quarantine, and cancellation tests.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `bun test --isolate tests/slot-runner.test.ts tests/terminal-rescue.test.ts tests/segments.test.ts`

Expected: new isolation tests fail against singleton slot state.

- [ ] **Step 3: Implement instance-scoped `SlotPool` and `SandboxSlotRunner`**

Move `pool`, live protocols, folder ID, lane max parts, artifacts, request counter, ZIP reads, Ydoc writes, and daemon readiness behind the injected runtime context. Preserve release forensics and terminal rescue byte-for-byte where behavior is already proven.

- [ ] **Step 4: Convert legacy `runSlotJob` into a compatibility adapter**

The old export delegates to one explicitly constructed legacy runtime during migration. It must not create hidden shared state used by the new router.

- [ ] **Step 5: Run relay-focused and existing tests**

Run: `bun test --isolate tests/slot-runner.test.ts tests/terminal-rescue.test.ts tests/segments.test.ts tests/sse.test.ts tests/concurrency.test.ts && bun run typecheck`

Expected: all pass.

- [ ] **Step 6: Commit**

```text
git add src/relay/slot-runner.ts src/sandbox/slot-pool.ts src/relay/slots.ts src/relay/pipeline.ts tests/slot-runner.test.ts tests/terminal-rescue.test.ts tests/segments.test.ts
git commit -m "refactor: scope relay lanes by sandbox runtime"
```

---

### Task 9: Sandbox registry and intelligent least-work router

**Files:**
- Create: `src/sandbox/registry.ts`
- Create: `src/router/policy.ts`
- Create: `src/router/router.ts`
- Test: `tests/router-policy.test.ts`
- Test: `tests/multi-sandbox-router.test.ts`

**Interfaces:**
- `SandboxRegistry.startAll`, `readyRuntimes`, `snapshots`, `stopAll`, and lifecycle events.
- Pure `chooseRuntime(workload, candidates, random): RouteDecision`.
- `MultiSandboxRouter.startRelay(body, options): Promise<RunHandle>`.

- [ ] **Step 1: Write failing policy tests**

```ts
it("chooses the lowest predicted completion time instead of round robin", () => {
  const decision = chooseRuntime(TEXT, [
    candidate("a", { active: 28, capacity: 30, ackMs: 900 }),
    candidate("b", { active: 4, capacity: 30, ackMs: 1400 }),
    candidate("c", { active: 12, capacity: 30, ackMs: 1000 }),
  ], seededRandom(1));
  expect(decision.runtimeId).toBe("b");
});

it("never chooses stale, draining, auth-required, or memory-ineligible runtimes", () => {
  expect(chooseRuntime(TEXT, ineligibleCandidates(), seededRandom(1))).toBeNull();
});
```

- [ ] **Step 2: Write failing aggregate-capacity/fairness tests**

```ts
it("admits exactly 120 simultaneous text jobs across four 30-slot runtimes", async () => {
  const router = await readyRouter({ projects: 4, slotsPerProject: 30, memory: gib(13) });
  const jobs = Array.from({ length: 121 }, (_, index) => router.startRelay(body(index)));
  await flush();
  expect(router.snapshot().active).toBe(120);
  expect(router.snapshot().queued).toBe(1);
  expect(router.snapshot().runtimes.map(runtime => runtime.active)).toEqual([30, 30, 30, 30]);
});
```

Add deterministic tie-breaking, workload-class EWMAs, client deficit round robin, aging, work-conserving behavior, dynamic capacity loss/recovery, byte reservation, queued abort, pre-publication reroute once, and post-publication no-reroute tests.

Add a pure-policy benchmark over four candidates and 100,000 decisions; require measured p95 below 2 ms per decision with no allocation growth proportional to request history.

- [ ] **Step 3: Run focused tests and verify RED**

Run: `bun test --isolate tests/router-policy.test.ts tests/multi-sandbox-router.test.ts`

Expected: FAIL because registry/router do not exist.

- [ ] **Step 4: Implement registry and pure predicted-finish policy**

Score in milliseconds using estimated queue waves, class completion EWMA, ACK EWMA, payload factor, failure/quarantine penalty, token/draining penalty, and memory pressure. Scan every healthy candidate; use seeded randomness only for equivalent scores.

- [ ] **Step 5: Implement fair global queue and leases**

Acquire a governor reservation before runtime selection, select only when a runtime lane is available, bind through terminal settlement, and update metrics on completion. Reroute only when `published === false`.

- [ ] **Step 6: Run focused tests and typecheck**

Run: `bun test --isolate tests/router-policy.test.ts tests/multi-sandbox-router.test.ts tests/scheduler.test.ts tests/concurrency.test.ts && bun run typecheck`

Expected: all pass.

- [ ] **Step 7: Commit**

```text
git add src/sandbox/registry.ts src/router/policy.ts src/router/router.ts tests/router-policy.test.ts tests/multi-sandbox-router.test.ts
git commit -m "feat: route work across sandbox projects"
```

---

### Task 10: Compose HTTP, WebSocket, Images, and diagnostics through the router

**Files:**
- Create: `src/server/app.ts`
- Create: `src/server/runtime.ts`
- Modify: `src/server.ts`
- Modify: `src/ws/server.ts`
- Modify: `src/images/routes.ts`
- Test: `tests/router-http.test.ts`
- Modify: `tests/websocket.test.ts`
- Modify: `tests/images-http.test.ts`
- Modify: `tests/mirror.test.ts`

**Interfaces:**
- `createGhostProvApp({ router, registry, config }): Hono`.
- `createGhostProvServer({ app, router, registry, port }): Server`.
- All relay consumers use `router.startRelay`.

- [ ] **Step 1: Write failing composition and protected-port tests**

```ts
it("refuses candidate mode on protected port 8787", () => {
  expect(() => createGhostProvServer({ ...deps, candidate: true, port: 8787 })).toThrow("protected");
});

it("routes HTTP, WebSocket, and image fan-out through the same router", async () => {
  await app.request("/v1/responses", responseRequest("http"));
  await websocketResponse("ws");
  await app.request("/v1/images/generations", imageRequest({ n: 2 }));
  expect(router.workloads.map(workload => workload.source)).toEqual(["http", "websocket", "image", "image"]);
});
```

- [ ] **Step 2: Write failing aggregate diagnostics tests**

Assert `/health` reports effective capacity/ready runtime count and `/__sandboxes` reports sanitized lifecycle, slots, heartbeat age, EWMA, failures, and reservations without credentials.

- [ ] **Step 3: Run focused tests and verify RED**

Run: `bun test --isolate tests/router-http.test.ts tests/websocket.test.ts tests/images-http.test.ts tests/mirror.test.ts`

Expected: new router composition tests fail.

- [ ] **Step 4: Extract composition roots and inject the router**

Keep exported `app` compatibility for existing tests while production startup builds account session → provisioning client → registry → governor → router → app. Candidate startup defaults to 18787 and refuses 8787.

- [ ] **Step 5: Integrate memory reservation before body buffering**

Use declared content length to reserve before collecting image edits and other large requests. Chunked collection grows reservations incrementally with backpressure. Release the raw-body portion after parsing only when successor workload reservations cover retained data.

- [ ] **Step 6: Run HTTP/WebSocket/image/integration tests**

Run: `bun test --isolate tests/router-http.test.ts tests/websocket.test.ts tests/images-http.test.ts tests/mirror.test.ts tests/http-body.test.ts tests/http-streaming.test.ts && bun run typecheck`

Expected: all pass.

- [ ] **Step 7: Commit**

```text
git add src/server/app.ts src/server/runtime.ts src/server.ts src/ws/server.ts src/images/routes.ts tests/router-http.test.ts tests/websocket.test.ts tests/images-http.test.ts tests/mirror.test.ts
git commit -m "feat: serve all transports through sandbox router"
```

---

### Task 11: Remove Playwright and singleton bridge transport

**Files:**
- Delete: `src/bridge/playwright.ts`
- Delete: `src/bridge/config.ts`
- Modify/Delete: `src/prism/gateway.ts`
- Modify/Delete: `src/prism/ydoc.ts`
- Modify: `src/proxy/files.ts`
- Modify: `src/proxy/render.ts`
- Modify: `src/proxy/yjs.ts`
- Modify: `package.json`
- Modify: `bun.lock`
- Delete/Replace: `tests/bridge.test.ts`
- Delete/Replace: `tests/bridge-config.test.ts`
- Create: `tests/no-playwright.test.ts`

**Interfaces:**
- Production modules depend only on account/session/runtime abstractions.
- No active import, package dependency, bridge global, token file, or page-evaluation path remains.

- [ ] **Step 1: Write a failing source/dependency guard**

```ts
it("contains no production Playwright dependency or bridge import", () => {
  const pkg = JSON.parse(read("package.json"));
  expect(pkg.dependencies).not.toHaveProperty("playwright");
  for (const file of productionTypeScriptFiles()) {
    expect(read(file)).not.toMatch(/bridge\/playwright|from ["']playwright["']|import\(["']playwright/);
  }
});
```

- [ ] **Step 2: Run the guard and verify RED**

Run: `bun test --isolate tests/no-playwright.test.ts`

Expected: FAIL on the current dependency/imports.

- [ ] **Step 3: Move remaining file/render/Yjs consumers to runtime-scoped clients**

Replace dynamic bridge imports, global tokens, page evaluation, and token-file lookups with injected account, gateway, Ydoc, and router interfaces. Preserve only live-proven file routes; unsupported legacy behavior returns explicit API errors.

- [ ] **Step 4: Remove Playwright files/dependency and regenerate lockfile**

Run: `bun remove playwright`

Delete obsolete bridge sources/tests after all consumers compile.

- [ ] **Step 5: Run the source guard, full unit suite, typecheck, and audit**

Run: `bun test --isolate tests/no-playwright.test.ts && bun run test:unit && bun run typecheck && bun audit --production`

Expected: all pass; audit reports no vulnerabilities.

- [ ] **Step 6: Commit**

```text
git add -A src/bridge src/prism src/proxy package.json bun.lock tests
git commit -m "refactor: remove Playwright Prism bridge"
```

---

### Task 12: Deterministic four-project fake Prism integration

**Files:**
- Create: `tests/support/fake-prism.ts`
- Create: `tests/multi-sandbox-integration.test.ts`
- Modify: `package.json`

**Interfaces:**
- Fake account endpoints, sandbox endpoints, Yjs provider factory, daemon terminal envelopes, fake clock, and fault injection.
- Runs the production account/provisioning/runtime/router/server stack with no external credentials.

- [ ] **Step 1: Build the fake Prism service contract in tests**

Implement scripted handlers for backend acquisition, resource tokens, `/api/y`, sandbox token push, `wait-for-sync`, heartbeats, ZIP, render, daemon status, and 30 slot files per project. Record every upstream execution ID to detect duplicates.

- [ ] **Step 2: Write the failing 120-request integration test**

```ts
it("runs 120 requests across four projects with no oversubscription or duplicates", async () => {
  const system = await startFakeSystem({ projects: 4, slotsPerProject: 30, totalRamGiB: 32 });
  const requests = Array.from({ length: 120 }, (_, index) => system.router.startRelay(body(index)));
  await system.untilAllPublished();
  expect(system.runtimeActive()).toEqual([30, 30, 30, 30]);
  await system.completeAll();
  await Promise.all(requests.map(async request => (await request).final));
  expect(system.executions.size).toBe(120);
  expect(system.duplicateExecutions).toEqual([]);
  expect(system.quarantinedLanes()).toBe(0);
});
```

- [ ] **Step 3: Add mixed/failure/heartbeat integration tests**

Cover 10-second heartbeats under load, one project expiry, token rotation, ZIP 500, initial SSE gaps, one quarantined lane, memory-limited images, queued client fairness, and recovery restoring capacity.

Record local timing spans in the fake integration and require under-capacity queue p95 below 250 ms. Use a controllable publish transport to assert the router adds less than 10 ms beyond scripted publication latency.

- [ ] **Step 4: Run integration tests and fix only production defects they expose**

Run: `bun test --isolate tests/multi-sandbox-integration.test.ts`

Expected: all scenarios pass with deterministic snapshots.

- [ ] **Step 5: Add the suite to stable scripts and commit**

```text
git add tests/support/fake-prism.ts tests/multi-sandbox-integration.test.ts package.json
git commit -m "test: verify four-project router integration"
```

---

### Task 13: Documentation, browserless proof tooling, and live load ramp

**Files:**
- Create: `scripts/probe_browserless_prism.ts`
- Create: `scripts/create_test_projects.ts`
- Create: `scripts/live_multi_sandbox.ts`
- Create: `scripts/benchmark_scriptc.ts`
- Modify: `scripts/live_verify.ts`
- Modify: `README.md`
- Modify: `tests/documentation.test.ts`
- Modify: `package.json`

**Interfaces:**
- Browserless probe prints statuses/lengths only, never credentials.
- Project creation is explicit opt-in and records only projects it created.
- Candidate server and load ramp refuse port 8787.

- [ ] **Step 1: Write failing documentation/tooling assertions**

```ts
expect(readme).toContain("PRISM_PROJECT_IDS");
expect(readme).toContain("PRISM_COOKIE_FILE");
expect(readme).toContain("10 seconds");
expect(readme).toContain("18787");
expect(pkg.dependencies).not.toHaveProperty("playwright");
expect(pkg.scripts).toHaveProperty("verify:multi-sandbox");
```

- [ ] **Step 2: Implement sanitized browserless proof**

`probe_browserless_prism.ts` mints Y-Sweet credentials, opens direct Yjs, pushes sandbox credentials, polls readiness, and heartbeats. Output includes HTTP status, token length, state, and timing only.

- [ ] **Step 3: Implement explicit test-project creation**

When `GP_LIVE_CREATE_TEST_PROJECTS=1`, call:

```http
POST /api/projects
Content-Type: application/json

{"project_uuid":"<random UUID>","title":"GhostProv Load Test <timestamp>","file_uuids":[]}
```

Persist created IDs in a dedicated local test manifest. Never delete or mutate projects absent from that manifest. Do not delete created projects automatically.

- [ ] **Step 4: Implement candidate-port live ramp**

`live_multi_sandbox.ts` requires:

```text
GP_LIVE_VERIFY=1
OPENAI_BASE_URL=http://127.0.0.1:18787/v1
PRISM_PROJECT_IDS=<four explicit test project IDs>
```

It checks port 8787 health before and after, verifies four ready daemons and heartbeat gaps, then ramps `16 → 32 → 64 → 96 → 120`. It records distribution, active peaks, router-decision/queue/serialization/publish/ACK/first-event/terminal latencies, RSS/free RAM, errors, retries, and quarantines. It runs an equivalent one-project candidate baseline and fails when first-event overhead regresses by more than 10% under equivalent upstream conditions.

- [ ] **Step 5: Update README and tests**

Document cookie provisioning, project allowlisting, candidate startup, adaptive sizing, monitoring, heartbeat contract, expected single-agent behavior, upstream account limits, and rollback.

- [ ] **Step 6: Run documentation tests and all local verification**

Before local verification, implement `benchmark_scriptc.ts` to run coverage only when `GP_BENCHMARK_SCRIPTC=1`. It records the current 24-error baseline, reruns coverage after Playwright removal, and proceeds to a compiled A/B test only when coverage has no reachable unsupported sites and a native toolchain is available. It never changes the default `dev` or production scripts.

Run:

```text
bun test --isolate
bun run test:daemon
bun run typecheck
bun audit --production
git diff --check
```

Expected: zero failures, no TypeScript diagnostics, no production vulnerabilities, and no whitespace errors.

- [ ] **Step 7: Commit**

```text
git add scripts README.md tests/documentation.test.ts package.json
git commit -m "docs: add browserless multi-sandbox operations"
```

---

### Task 14: Live proof, final verification, and feature-branch delivery

**Files:**
- No source changes unless a live failure first receives a failing automated regression test.

**Interfaces:**
- Uses the candidate server on port 18787 and explicitly created/allowlisted test projects.
- Treats port 8787 as read-only health evidence.

- [ ] **Step 1: Verify protected production health before candidate startup**

Run: `Invoke-WebRequest -UseBasicParsing http://127.0.0.1:8787/health`

Expected: HTTP 200 `{"ok":true}`.

- [ ] **Step 2: Start the browserless candidate server on port 18787**

Run in a long-lived session with `GP_CANDIDATE=1`, `PORT=18787`, `PRISM_COOKIE_FILE`, and four explicit test project IDs. Do not set or bind port 8787.

Expected: four runtimes progress to `ready`, each reports the expected daemon build/capacity, and each heartbeat age remains below the 10-second cadence plus request duration.

- [ ] **Step 3: Run the browserless control-plane proof**

Run: `bun scripts/probe_browserless_prism.ts`

Expected: direct account HTTP, resource token, Y-Sweet, direct Yjs, token push, readiness, heartbeat, and daemon status all pass without a Playwright process/import.

- [ ] **Step 4: Run the staged live ramp**

Run: `bun scripts/live_multi_sandbox.ts`

Expected: all stages through 120 pass when permitted by the upstream account. If an account-wide limit is lower, the router must discover it, maintain stable adaptive backpressure, and report the measured ceiling without oversubscription, duplication, early release, or lost terminal proof. Router p95 must remain below 2 ms, under-capacity queue p95 below 250 ms, defined publish p50/p95 below 600/1500 ms, and first-event overhead within 10% of the one-project candidate.

- [ ] **Step 5: Stop only the candidate process and recheck port 8787**

Terminate the known candidate session bound to 18787. Do not enumerate or terminate unrelated Bun/Chrome/Node processes.

Run: `Invoke-WebRequest -UseBasicParsing http://127.0.0.1:8787/health`

Expected: HTTP 200 `{"ok":true}`.

- [ ] **Step 6: Run fresh final verification**

Run:

```text
bun test --isolate
bun run test:daemon
bun run typecheck
bun audit --production
git diff --check
git status --short --branch
```

Expected: all checks pass and the feature worktree is clean.

- [ ] **Step 7: Push the feature branch without merging or restarting production**

Run: `git push -u origin br/multi-sandbox-router`

Expected: GitHub advertises the exact local HEAD. Keep the worktree and current port 8787 process intact for review.
