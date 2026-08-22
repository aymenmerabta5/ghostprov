# GhostProv V2 Awareness Sidecar Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver a production-ready GhostProv V2 that dynamically manages reusable Prism sandbox fleets and carries bounded, terminal-proven Responses traffic through a secure Y-Sweet Awareness sidecar with higher concurrency and lower latency than v1.

**Architecture:** Keep the public HTTP, SSE, WebSocket, image, continuation, router, and resource-governor layers stable. Add a durable Prism identity/fleet control plane, a shared host/Deno protocol package, encrypted phase-zero sidecar bootstrap, bounded reliable Awareness windows, and a V2 runtime that can be selected without contaminating the existing v1 implementation. Signed-in `auto` may use v1 only before V2 commitment; guest fleets are explicit, capped, reusable, V2-only, and fail closed.

**Tech Stack:** Bun 1.4, TypeScript 7.0.2, `bun:test` with `bun test --isolate`, Hono, Yjs 13.6.x, `y-websocket`, `ws`, Deno with sandbox-vendored `@y-sweet/client` 0.9.1, Web Crypto P-256 ECDH + HKDF-SHA-256 + AES-256-GCM.

## Global Constraints

- Work only on `br/v2-awareness-sidecar`; never modify or merge `master` during implementation.
- Execute inline in the current session; the user explicitly prohibited subagents.
- Never stop, replace, or bind over the production service on `127.0.0.1:8787`; candidate servers use `127.0.0.1:18787` or an OS-assigned test port.
- Keep Bun pinned to `1.4.0`, TypeScript pinned to `7.0.2`, and every automated test command based on `bun test --isolate`; do not add Vitest, Playwright, or Patchright.
- Do not patch, restart, or replace Crixet PID 1 or its supervised Deno process; V2 is a separately launched sidecar with no public port.
- Preserve the public Responses HTTP/SSE/WebSocket contracts, named lanes, `store:false` continuation graph, `previous_response_id`, reasoning modes, service tier, and GPT Image generation/edit compatibility.
- V2 makes zero project-export or sandbox `/zip` calls during bootstrap, request handling, recovery, shutdown, and guest operation.
- Use a hard 16 KiB encoded-frame limit, a hard 32-frame/512 KiB unacknowledged window, and a hard 64 MiB decoded-request limit.
- Cap one sidecar at 128 active upstream Responses calls; accepted excess work queues fairly and the `ResourceGovernor` may advertise less capacity.
- Heartbeat every sandbox independently every exactly 10 seconds with deterministic phase offsets.
- Store append-only claim journals and terminal recovery spools under `/tmp/ghostprov-v2`, never `/workspace`.
- Never retry or reroute after `FETCH_STARTING` may have reached upstream; preserve ambiguity and quarantine it until matching terminal proof or explicit recovery.
- Signed-in sessions are the default (`GP_PRISM_SESSION=authenticated`). Guest mode is explicit, defaults to one managed project (`GP_GUEST_MAX_PROJECTS=1`), reuses one durable identity/pool, and never creates identities or projects on every restart.
- Never automatically delete a Prism project. Explicit decommissioning must show exact managed project IDs and require a matching confirmation digest.
- Never write plaintext cookies, OpenAI keys, Prism session tokens, project resource tokens, or Y-Sweet tokens into tracked files or Yjs history.
- Keep source files focused by responsibility, use typed errors at every boundary, redact monitoring data, and avoid unrelated refactors.
- Each task follows red-green-refactor discipline and ends with its focused tests, relevant regression tests, TypeScript checking when interfaces change, and one reviewable commit.
- Live fleet tests remain capped at four explicitly authorized projects; deterministic tests cover 1, 4, 8, 16, and 32 runtimes.

---

## Planned File Structure

### Prism identity and fleet control plane

- `src/core/file-lease.ts` — cross-process exclusive lease with stale-owner detection.
- `src/prism/session-manager.ts` — signed-in/guest session resolution and durable identity policy.
- `src/prism/projects-client.ts` — typed project list/create/metadata/delete operations.
- `src/fleet/types.ts` — manifest, worker, reconciliation, and fleet snapshot types.
- `src/fleet/manifest.ts` — atomic owner-only manifest persistence and leader generations.
- `src/fleet/manager.ts` — idempotent dynamic inventory, creation, provisioning, drain, and reuse.
- `src/fleet/project-inspector.ts` — read-only managed-marker, writability, and emptiness proof through Yjs.
- `src/fleet/decommission.ts` — explicit confirmation-gated deletion of managed projects.

### Shared V2 protocol and host transport

- `src/v2/protocol/shared.ts` — runtime-neutral constants, envelopes, frames, bootstrap bindings, and canonical encoding shared with Deno.
- `src/v2/protocol/validate.ts` — strict untrusted-frame validation and typed protocol errors.
- `src/v2/protocol/transcript.ts` — canonical SSE transcript hashing and terminal identity proof.
- `src/v2/protocol/window.ts` — cumulative ACK, deduplication, gap detection, bounded retransmission window.
- `src/v2/protocol/scheduler.ts` — weighted-fair control/request/response frame scheduling.
- `src/v2/bootstrap/crypto.ts` — ephemeral ECDH credential sealing/opening.
- `src/v2/bootstrap/controller.ts` — non-secret asset deployment, render-log key handshake, ciphertext handoff, and readiness proof.
- `src/v2/awareness/channel.ts` — Awareness channel interface plus safe state lifecycle.
- `src/v2/awareness/y-sweet-channel.ts` — production Y-Sweet provider adapter.
- `src/v2/transport/session.ts` — one reliable host/sidecar session per runtime.
- `src/v2/transport/run-handle.ts` — committed request lifecycle, streaming queue, cancellation, terminal settlement, and stats.
- `src/v2/runtime.ts` — routable V2 sandbox runtime and adaptive metrics.
- `src/v2/transport-selector.ts` — signed-in pre-commit fallback policy and guest V2-only enforcement.

### Sidecar core and sandbox entrypoint

- `src/v2/sidecar/assembler.ts` — bounded request reassembly and integrity validation.
- `src/v2/sidecar/journal.ts` — storage-neutral claim state machine and group-commit coordinator.
- `src/v2/sidecar/admission.ts` — 128-active hard cap, host ceiling, queue bounds, and reset backoff.
- `src/v2/sidecar/upstream.ts` — Responses fetch boundary and incremental SSE parsing.
- `src/v2/sidecar/engine.ts` — request admission, journal, upstream, response scheduling, cancellation, and recovery orchestration.
- `src/v2/sidecar/assets.ts` — deterministic asset list/build hash and tracked-file mapping.
- `sandbox/v2/main.ts` — Deno/Y-Sweet process entrypoint.
- `sandbox/v2/awareness-peer.ts` — Deno Awareness adapter.
- `sandbox/v2/journal-storage.ts` — append/fsync/atomic terminal spool adapter under `/tmp/ghostprov-v2`.
- `sandbox/v2/bootstrap.ts` — phase-zero key generation, ciphertext watch, decryption, and secure cleanup.

### Integration and verification

- `src/sandbox/managed-v1-runtime.ts` — extracted existing v1 runtime, behavior unchanged.
- `src/sandbox/registry.ts` — mutable add/drain/remove/snapshot events.
- `src/server/system.ts` — composition only: identity, fleet, runtimes, router, server, shutdown.
- `scripts/live_v2_single.ts` — gated single-project bootstrap/stream/crash/rotation proof.
- `scripts/live_v2_fleet.ts` — gated 4-project 128/256/512 concurrency proof.
- `scripts/decommission_v2_fleet.ts` — explicit digest-confirmed managed-project cleanup command.
- `tests/support/fake-awareness.ts` — deterministic loss/duplication/reordering/reconnect transport.
- `tests/support/fake-v2-sidecar.ts` — in-memory sidecar engine and upstream fixture.

---

### Task 1: Resolve V2, identity, and dynamic fleet configuration

**Files:**
- Modify: `src/config/multi-sandbox.ts`
- Modify: `tests/multi-sandbox-config.test.ts`

**Interfaces:**
- Consumes: `MachineSample`, `projectId()`.
- Produces: `TransportMode`, `PrismSessionMode`, `MultiSandboxConfig`, and `resolveDesiredSandboxCount(config)` used by session, fleet, and system tasks.

- [ ] **Step 1: Write failing configuration tests**

Add exact cases covering authenticated defaults, explicit anonymous limits, dynamic derivation, no-project automatic creation, invalid guest/v1 combinations, candidate-port protection, and the fixed heartbeat:

```ts
it("defaults to authenticated V2-ready fleet policy", () => {
  const value = resolveMultiSandboxConfig({
    PRISM_COOKIE_FILE: "./cookies.json",
    PRISM_PROJECT_IDS: PROJECT_A,
    GP_CANDIDATE: "1",
  }, MACHINE);
  expect(value.transport).toBe("auto");
  expect(value.sessionMode).toBe("authenticated");
  expect(value.allowAnonymous).toBe(false);
  expect(value.guestMaxProjects).toBe(1);
  expect(value.v2SandboxCapacity).toBe(128);
  expect(value.v2ResponseMaxBytes).toBe(64 * 1024 * 1024);
  expect(value.v2TransportReservationBytes).toBe(16 * 1024 * 1024);
  expect(value.v2YSweetRefreshMs).toBe(45 * 60 * 1000);
  expect(value.v2SidecarMemoryBudgetBytes).toBe(512 * 1024 * 1024);
  expect(value.desiredSandboxes).toBe(4);
  expect(value.heartbeatMs).toBe(10_000);
});

it("allows a project-less managed V2 fleet and derives its size", () => {
  const value = resolveMultiSandboxConfig({
    PRISM_COOKIE_FILE: "./cookies.json",
    GP_TRANSPORT: "v2",
    GP_AUTO_CREATE_PROJECTS: "1",
    GP_TARGET_CONCURRENCY: "1024",
    GP_MAX_PROJECTS: "16",
    GP_CANDIDATE: "1",
  }, MACHINE);
  expect(value.projectIds).toEqual([]);
  expect(value.desiredSandboxes).toBe(8);
});

it("requires explicit guest mode and caps it independently", () => {
  const value = resolveMultiSandboxConfig({
    PRISM_COOKIE_FILE: "./guest.json",
    GP_PRISM_SESSION: "anonymous",
    GP_TRANSPORT: "v2",
    GP_TARGET_SANDBOXES: "4",
    GP_GUEST_MAX_PROJECTS: "2",
    GP_AUTO_CREATE_PROJECTS: "1",
    GP_CANDIDATE: "1",
  }, MACHINE);
  expect(value.sessionMode).toBe("anonymous");
  expect(value.desiredSandboxes).toBe(2);
  expect(() => resolveMultiSandboxConfig({
    PRISM_COOKIE_FILE: "./guest.json",
    GP_PRISM_SESSION: "anonymous",
    GP_TRANSPORT: "v1",
    PRISM_PROJECT_IDS: PROJECT_A,
    GP_CANDIDATE: "1",
  }, MACHINE)).toThrow("anonymous Prism sessions require GP_TRANSPORT=v2");
});
```

- [ ] **Step 2: Run the focused test and confirm red**

Run: `bun test --isolate tests/multi-sandbox-config.test.ts`

Expected: FAIL because `transport`, `sessionMode`, `desiredSandboxes`, and V2 capacity fields do not exist and the current resolver requires an explicit project ID.

- [ ] **Step 3: Implement strict configuration parsing**

Add these exact public types and fields while retaining the existing origin, cookie, port, candidate, machine, and v1 slot fields:

```ts
export type TransportMode = "v1" | "v2" | "auto";
export type PrismSessionMode = "authenticated" | "auto" | "anonymous";
export type ProjectReuseMode = "managed" | "eligible-owned" | "explicit";

export interface MultiSandboxConfig {
  projectIds: readonly ProjectId[];
  prismOrigin: string;
  cookieFile: string;
  initialCookieHeader?: string;
  port: number;
  candidate: boolean;
  heartbeatMs: 10_000;
  maxProjects: number;
  slotsPerProject: number;
  machine: MachineSample;
  transport: TransportMode;
  sessionMode: PrismSessionMode;
  allowAnonymous: boolean;
  guestMaxProjects: number;
  autoCreateProjects: boolean;
  projectReuse: ProjectReuseMode;
  targetConcurrency: number;
  targetSandboxes: number | null;
  v2SandboxCapacity: number;
  v2ResponseMaxBytes: number;
  v2TransportReservationBytes: number;
  v2YSweetRefreshMs: number;
  v2SidecarMemoryBudgetBytes: number;
  desiredSandboxes: number;
  provisionConcurrency: number;
  fleetManifestFile: string;
}

export function resolveDesiredSandboxCount(input: {
  targetConcurrency: number;
  targetSandboxes: number | null;
  v2SandboxCapacity: number;
  maxProjects: number;
  sessionMode: PrismSessionMode;
  guestMaxProjects: number;
}): number {
  const requested = input.targetSandboxes ?? Math.ceil(input.targetConcurrency / input.v2SandboxCapacity);
  const identityCeiling = input.sessionMode === "anonymous"
    ? Math.min(input.maxProjects, input.guestMaxProjects)
    : input.maxProjects;
  return Math.max(1, Math.min(requested, identityCeiling));
}
```

Parsing rules:

- `GP_TRANSPORT`: default `auto`; reject other strings.
- `GP_PRISM_SESSION`: default `authenticated`; reject other strings.
- `GP_ALLOW_ANONYMOUS`: true only for `1`.
- `GP_GUEST_MAX_PROJECTS`: default `1`, positive, no greater than `GP_MAX_PROJECTS`.
- `GP_TARGET_CONCURRENCY`: default `512`; `GP_TARGET_SANDBOXES` overrides when present.
- `GP_V2_SANDBOX_CAPACITY`: default and maximum `128`.
- `GP_V2_RESPONSE_MAX_BYTES`: default `64 MiB`, hard maximum `256 MiB`, and always reservation-backed.
- `GP_V2_TRANSPORT_RESERVATION_BYTES`: default `16 MiB`, minimum `1 MiB`, used only for the live capacity hint.
- `GP_V2_YSWEET_REFRESH_MS`: default `45 minutes`, minimum `5 minutes`, used when the opaque token has no typed expiry.
- `GP_V2_SIDECAR_MEMORY_BUDGET_BYTES`: default `512 MiB`, minimum `64 MiB`; sidecar admission enforces it independently from the 128-call ceiling.
- `GP_MAX_PROJECTS`: default `32` for V2/auto and at least the explicit-ID count.
- `GP_AUTO_CREATE_PROJECTS`: true only for `1`.
- `GP_PROJECT_REUSE`: default `managed`.
- `GP_PROVISION_CONCURRENCY`: default `4`, capped at desired sandbox count.
- `GP_FLEET_MANIFEST_FILE`: default `.ghostprov/fleet.json`, resolved absolutely.
- `PRISM_COOKIE_FILE` is mandatory in every mode so identities survive restarts. A raw `PRISM_COOKIE` may seed that file in signed-in/auto migration, but is rejected for anonymous mode and is never a persistence substitute.
- V1 still requires explicit project IDs and 30-or-fewer slots.
- Project-less V2/auto requires `GP_AUTO_CREATE_PROJECTS=1`.

- [ ] **Step 4: Run configuration tests and typecheck**

Run: `bun test --isolate tests/multi-sandbox-config.test.ts && bun run typecheck`

Expected: all configuration tests PASS and TypeScript exits 0.

- [ ] **Step 5: Commit**

```bash
git add src/config/multi-sandbox.ts tests/multi-sandbox-config.test.ts
git commit -m "feat: define v2 fleet configuration"
```

---

### Task 2: Make Prism identity resolution durable, explicit, and concurrency-safe

**Files:**
- Create: `src/core/file-lease.ts`
- Create: `src/prism/session-manager.ts`
- Modify: `src/prism/account-session.ts`
- Create: `tests/file-lease.test.ts`
- Create: `tests/session-manager.test.ts`
- Modify: `tests/account-session.test.ts`

**Interfaces:**
- Consumes: `PrismAccountSession`, `PrismSessionMode`, `Clock`, `RandomSource`.
- Produces: `FileLease`, `PrismSessionManager.resolve()`, and `ResolvedPrismIdentity` for fleet reconciliation.

- [ ] **Step 1: Write failing file-lease and cookie-permission tests**

```ts
it("serializes owners and rejects an active competing lease", async () => {
  const first = new FileLease({ file: leaseFile, staleAfterMs: 30_000 });
  const second = new FileLease({ file: leaseFile, staleAfterMs: 30_000 });
  await first.acquire();
  await expect(second.acquire({ timeoutMs: 5 })).rejects.toMatchObject({ name: "FileLeaseBusyError" });
  await first.release();
  await expect(second.acquire({ timeoutMs: 50 })).resolves.toBeUndefined();
  await second.release();
});

it("persists cookie jars with owner-only POSIX permissions", async () => {
  const session = await PrismAccountSession.create({
    origin: "https://prism.test",
    cookieFile,
    initialCookieHeader: "prism_session_token=value",
    fetch: async () => new Response("{}"),
  });
  await session.flush();
  if (process.platform !== "win32") expect((await stat(cookieFile)).mode & 0o777).toBe(0o600);
});
```

- [ ] **Step 2: Write failing identity-mode tests**

Use a scripted fetch that returns `/auth/session` states and `Set-Cookie` headers:

```ts
it("requires a signed-in user in authenticated mode", async () => {
  const manager = await PrismSessionManager.create(options({ mode: "authenticated" }, [
    json(200, { session: null, user: null }),
  ]));
  await expect(manager.resolve()).rejects.toMatchObject({ kind: "auth_required" });
});

it("creates one backend-owned guest only in explicit anonymous mode", async () => {
  const calls: string[] = [];
  const manager = await PrismSessionManager.create(options({ mode: "anonymous", calls }, [
    json(200, { session: null, user: null }),
    json(200, { session: { prism_session_token: "redacted", user: { id: "guest-1", is_anonymous: true } } }, {
      "set-cookie": "prism_session_token=guest; Path=/; HttpOnly",
    }),
    json(200, { session: { user: { id: "guest-1", is_anonymous: true } }, user: { id: "guest-1", is_anonymous: true } }),
  ]));
  const identity = await manager.resolve();
  expect(identity.kind).toBe("anonymous");
  expect(calls).toEqual(["/auth/session", "/api/auth/anonymous-session", "/auth/session"]);
  expect(identity.fingerprint).toMatch(/^[a-f0-9]{64}$/);
  expect(JSON.stringify(identity)).not.toContain("guest-1");
});

it("does not replace a lost guest identity silently", async () => {
  const manager = await PrismSessionManager.create(options({ mode: "anonymous", existingJar: true }, [
    json(200, { session: null, user: null }),
  ]));
  await expect(manager.resolve()).rejects.toMatchObject({ kind: "guest_identity_lost" });
});
```

- [ ] **Step 3: Run the focused tests and confirm red**

Run: `bun test --isolate tests/file-lease.test.ts tests/account-session.test.ts tests/session-manager.test.ts`

Expected: FAIL because the lease and session manager do not exist and cookie temp files do not set mode `0o600`.

- [ ] **Step 4: Implement the lease and identity manager**

Use these exact public contracts:

```ts
export class FileLeaseBusyError extends Error {
  readonly name = "FileLeaseBusyError";
}

export class FileLease {
  constructor(options: { file: string; staleAfterMs: number; clock?: Clock; random?: RandomSource });
  acquire(options?: { timeoutMs?: number; signal?: AbortSignal }): Promise<void>;
  release(): Promise<void>;
}

export type PrismIdentityKind = "authenticated" | "anonymous";

export interface ResolvedPrismIdentity {
  kind: PrismIdentityKind;
  fingerprint: string;
  account: PrismAccountSession;
  resolvedAtMs: number;
}

export class PrismSessionError extends Error {
  constructor(
    message: string,
    readonly kind: "auth_required" | "guest_identity_lost" | "mode_mismatch" | "protocol",
    readonly status?: number,
  );
}

export class PrismSessionManager {
  static create(options: {
    origin: string;
    mode: PrismSessionMode;
    allowAnonymous: boolean;
    cookieFile: string;
    identityFile?: string;
    initialCookieHeader?: string;
    fetch?: FetchLike;
    clock?: Clock;
    random?: RandomSource;
  }): Promise<PrismSessionManager>;
  resolve(signal?: AbortSignal): Promise<ResolvedPrismIdentity>;
  flush(): Promise<void>;
  close(): Promise<void>;
}
```

Lease algorithm:

1. Create the parent directory and acquire the lease file with exclusive `open("wx", 0o600)`.
2. Store `{ pid, nonce, acquiredAtMs }` without secrets and fsync before returning.
3. On collision, read metadata; reclaim only when its mtime exceeds `staleAfterMs` and the recorded local PID is not alive.
4. Poll with bounded jitter until timeout/abort; release only when the on-disk nonce still matches the owner.

Identity algorithm:

1. Acquire the account lease before resolution and retain it for the manager lifetime; `close()` flushes cookies and releases it so two GhostProv processes cannot reconcile the same identity concurrently.
2. `GET /auth/session`; parse only `session`, `user`, and `is_anonymous`.
3. Enforce `authenticated`, `anonymous`, and `auto` policy exactly as Task 1 defines.
4. Persist `{ version: 1, kind, fingerprint }` atomically with mode `0o600` in `identityFile ?? cookieFile + ".identity.json"`; it contains no raw user ID or credential.
5. Bootstrap a guest with `POST /api/auth/anonymous-session` only when explicitly allowed and no prior anonymous identity metadata exists. If metadata says anonymous but `/auth/session` is empty/rejected, raise `guest_identity_lost` and preserve the old fleet.
6. Treat HTTP 409 as an existing-session race and refresh `/auth/session` instead of creating again.
7. Store only SHA-256 of the Prism user ID as `fingerprint`; never expose the raw ID or session token.
8. Change `PrismAccountSession` temporary writes to mode `0o600`, chmod the destination to `0o600` on POSIX after rename, and preserve its serialized persistence chain.

- [ ] **Step 5: Run focused tests, account regressions, and typecheck**

Run: `bun test --isolate tests/file-lease.test.ts tests/account-session.test.ts tests/session-manager.test.ts && bun run typecheck`

Expected: all tests PASS; no secret values occur in snapshots or error strings.

- [ ] **Step 6: Commit**

```bash
git add src/core/file-lease.ts src/prism/session-manager.ts src/prism/account-session.ts tests/file-lease.test.ts tests/session-manager.test.ts tests/account-session.test.ts
git commit -m "feat: manage durable Prism identities"
```

---

### Task 3: Add typed Prism project inventory and crash-safe fleet manifests

**Files:**
- Create: `src/prism/projects-client.ts`
- Create: `src/fleet/types.ts`
- Create: `src/fleet/manifest.ts`
- Create: `tests/projects-client.test.ts`
- Create: `tests/fleet-manifest.test.ts`

**Interfaces:**
- Consumes: `AccountRequestClient`, `ProjectId`, `PrismIdentityKind`, `Clock`, `RandomSource`.
- Produces: `PrismProjectsClient`, `FleetManifestStore`, and immutable manifest types for `ProjectFleetManager`.

- [ ] **Step 1: Write failing project-client boundary tests**

```ts
it("lists and creates projects with the proven Prism contracts", async () => {
  const account = scripted([
    json(200, { projects: [{ uuid: PROJECT_A, title: "GhostProv Worker 1", owner: "owner", deleted: false }] }),
    json(200, { uuid: PROJECT_B, title: "GhostProv Worker 2", owner: "owner", deleted: false }),
  ]);
  const client = new PrismProjectsClient(account);
  expect(await client.list()).toEqual([expect.objectContaining({ projectId: projectId(PROJECT_A), title: "GhostProv Worker 1" })]);
  expect(await client.create({ projectId: projectId(PROJECT_B), title: "GhostProv Worker 2" })).toMatchObject({ projectId: projectId(PROJECT_B) });
  expect(account.calls).toEqual([
    { path: "/api/projects?section=your_projects", method: "GET", body: null },
    { path: "/api/projects", method: "POST", body: { project_uuid: PROJECT_B, title: "GhostProv Worker 2", file_uuids: [] } },
  ]);
});

it("does not expose a general delete operation", () => {
  const client = new PrismProjectsClient(scripted([]));
  expect("delete" in client).toBe(false);
});
```

- [ ] **Step 2: Write failing manifest durability tests**

```ts
it("persists creation intent before completion and increments leader generations", async () => {
  const store = new FleetManifestStore({ file: manifestFile, identityFingerprint: "a".repeat(64) });
  await store.initialize("authenticated");
  await store.reserveWorker({ index: 1, projectId: projectId(PROJECT_A), title: "GhostProv Worker 1", nowMs: 1000 });
  await store.markCreated(projectId(PROJECT_A), 1100);
  const generation = await store.nextLeaderGeneration(projectId(PROJECT_A));
  expect(generation).toBe(1);
  expect((await store.load()).workers[0]).toMatchObject({ state: "created", leaderGeneration: 1 });
  if (process.platform !== "win32") expect((await stat(manifestFile)).mode & 0o777).toBe(0o600);
});

it("rejects a manifest owned by another Prism identity", async () => {
  await writeManifest(manifestFile, manifest({ identityFingerprint: "b".repeat(64) }));
  const store = new FleetManifestStore({ file: manifestFile, identityFingerprint: "a".repeat(64) });
  await expect(store.load()).rejects.toMatchObject({ kind: "identity_mismatch" });
});
```

- [ ] **Step 3: Run focused tests and confirm red**

Run: `bun test --isolate tests/projects-client.test.ts tests/fleet-manifest.test.ts`

Expected: FAIL because the project client and manifest store do not exist.

- [ ] **Step 4: Implement typed project parsing and manifest persistence**

Use these public contracts:

```ts
export interface PrismProjectSummary {
  projectId: ProjectId;
  title: string;
  owner: string | null;
  deleted: boolean;
  createdAt: string | null;
}

export class PrismProjectsClient {
  constructor(account: AccountRequestClient);
  list(signal?: AbortSignal): Promise<readonly PrismProjectSummary[]>;
  get(projectId: ProjectId, signal?: AbortSignal): Promise<PrismProjectSummary>;
  create(input: { projectId: ProjectId; title: string }, signal?: AbortSignal): Promise<PrismProjectSummary>;
  deleteManaged(projectId: ProjectId, signal?: AbortSignal): Promise<void>;
}

export type FleetWorkerState = "creating" | "created" | "ready" | "draining" | "failed";

export interface FleetWorkerRecord {
  index: number;
  projectId: ProjectId;
  title: string;
  state: FleetWorkerState;
  createdAtMs: number;
  updatedAtMs: number;
  leaderGeneration: number;
  managed: boolean;
}

export interface FleetManifest {
  version: 1;
  identityKind: PrismIdentityKind;
  identityFingerprint: string;
  workers: readonly FleetWorkerRecord[];
}

export class FleetManifestStore {
  constructor(options: { file: string; identityFingerprint: string });
  initialize(kind: PrismIdentityKind): Promise<FleetManifest>;
  load(): Promise<FleetManifest>;
  reserveWorker(input: { index: number; projectId: ProjectId; title: string; nowMs: number; managed?: boolean }): Promise<FleetManifest>;
  markCreated(projectId: ProjectId, nowMs: number): Promise<FleetManifest>;
  markState(projectId: ProjectId, state: FleetWorkerState, nowMs: number): Promise<FleetManifest>;
  nextLeaderGeneration(projectId: ProjectId): Promise<number>;
  removeManaged(projectId: ProjectId): Promise<FleetManifest>;
}
```

Implementation rules:

- Validate every response object, UUID, title, deleted flag, and array shape before returning.
- `deleteManaged` remains a deliberately named narrow primitive used only by the later decommission service; the service owns multi-project confirmation and constant-time digest verification.
- Serialize manifest updates through one promise chain, write a same-directory temporary file with mode `0o600`, fsync, rename, and fsync the parent directory on POSIX.
- Sort workers by stable index and reject duplicates, negative indices, identity mismatches, unknown versions, malformed states, and decreasing generations.
- Never store cookies, raw user IDs, sandbox tokens, Y-Sweet tokens, or resource tokens.

- [ ] **Step 5: Run focused tests and typecheck**

Run: `bun test --isolate tests/projects-client.test.ts tests/fleet-manifest.test.ts && bun run typecheck`

Expected: all tests PASS and TypeScript exits 0.

- [ ] **Step 6: Commit**

```bash
git add src/prism/projects-client.ts src/fleet/types.ts src/fleet/manifest.ts tests/projects-client.test.ts tests/fleet-manifest.test.ts
git commit -m "feat: persist managed Prism fleets"
```

---

### Task 4: Reconcile dynamic fleets and make the runtime registry mutable

**Files:**
- Create: `src/fleet/manager.ts`
- Modify: `src/sandbox/registry.ts`
- Modify: `src/router/router.ts`
- Modify: `src/server/system.ts`
- Modify: `tests/support/fake-prism.ts`
- Create: `tests/fleet-manager.test.ts`
- Modify: `tests/multi-sandbox-router.test.ts`

**Interfaces:**
- Consumes: `PrismProjectsClient`, `FleetManifestStore`, `ManagedProjectInspector`, `MultiSandboxConfig`, `RoutableSandboxRuntime`.
- Produces: `ProjectFleetManager`, registry mutation events, and router wakeups when capacity changes.

- [ ] **Step 1: Write failing mutable-registry tests**

```ts
it("adds, drains, and removes runtimes without replacing the registry", async () => {
  const registry = new SandboxRegistry([]);
  const runtime = fakeRuntime(PROJECT_A);
  const events: string[] = [];
  const unsubscribe = registry.subscribe(event => events.push(`${event.type}:${event.projectId}`));
  registry.add(runtime);
  expect(registry.get(PROJECT_A)).toBe(runtime);
  await registry.drain(PROJECT_A, "scale-down");
  await registry.removeStopped(PROJECT_A);
  expect(registry.get(PROJECT_A)).toBeUndefined();
  expect(events.map(value => value.split(":")[0])).toEqual(["added", "changed", "removed"]);
  unsubscribe();
});

it("wakes queued routing when a ready runtime is added", async () => {
  const registry = new SandboxRegistry([]);
  const router = testRouter(registry);
  const pending = router.startRelay({ model: "gpt-5.6-sol", input: "queued", stream: true });
  registry.add(fakeRuntime(PROJECT_A));
  await expect(pending).resolves.toBeDefined();
  await router.close();
});
```

- [ ] **Step 2: Write failing fleet reconciliation tests**

```ts
it("reuses explicit then manifest then managed projects before creating the exact deficit", async () => {
  const harness = fleetHarness({
    desired: 4,
    explicit: [PROJECT_A],
    manifest: [worker(2, PROJECT_B)],
    remote: [project(PROJECT_A, "User explicit"), project(PROJECT_B, "GhostProv Worker 2"), project(PROJECT_C, "GhostProv Worker 3")],
    inspections: { [PROJECT_B]: managedInspection(2), [PROJECT_C]: managedInspection(3) },
  });
  const snapshot = await harness.manager.reconcile();
  expect(snapshot.projects).toHaveLength(4);
  expect(harness.projects.created).toHaveLength(1);
  expect(harness.registry.runtimes()).toHaveLength(4);
});

it("is restart-idempotent after a persisted creating intent", async () => {
  const harness = fleetHarness({ desired: 1, manifest: [creatingWorker(1, PROJECT_A)], remote: [project(PROJECT_A, "GhostProv Worker 1")] });
  await harness.manager.reconcile();
  expect(harness.projects.created).toEqual([]);
  expect((await harness.manifest.load()).workers[0].state).not.toBe("creating");
});

it("drains excess workers without deleting Prism projects", async () => {
  const harness = fleetHarness({ desired: 1, manifest: [worker(1, PROJECT_A), worker(2, PROJECT_B)], remote: [project(PROJECT_A), project(PROJECT_B)] });
  await harness.manager.reconcile();
  expect(harness.registry.get(PROJECT_B)).toBeUndefined();
  expect(harness.projects.deleted).toEqual([]);
});
```

- [ ] **Step 3: Run focused tests and confirm red**

Run: `bun test --isolate tests/fleet-manager.test.ts tests/multi-sandbox-router.test.ts`

Expected: FAIL because registry mutation/subscription and fleet reconciliation do not exist.

- [ ] **Step 4: Implement registry mutation and router subscription**

Extend the runtime and registry contracts exactly:

```ts
export interface RoutableSandboxRuntime {
  readonly projectId: ProjectId;
  start(): Promise<void>;
  drain(reason: string): Promise<void>;
  stop(): Promise<void>;
  snapshot(): RoutableSandboxSnapshot;
  run(body: Record<string, unknown>, signal?: AbortSignal): Promise<RunHandle>;
}

export type RegistryEvent =
  | { type: "added"; projectId: ProjectId }
  | { type: "changed"; projectId: ProjectId }
  | { type: "removed"; projectId: ProjectId };

export class SandboxRegistry {
  constructor(runtimes?: Iterable<RoutableSandboxRuntime>);
  add(runtime: RoutableSandboxRuntime): void;
  drain(projectId: ProjectId, reason: string): Promise<void>;
  removeStopped(projectId: ProjectId): Promise<void>;
  subscribe(listener: (event: RegistryEvent) => void): () => void;
  startAll(): Promise<void>;
  stopAll(): Promise<void>;
  get(projectId: ProjectId): RoutableSandboxRuntime | undefined;
  runtimes(): readonly RoutableSandboxRuntime[];
  snapshots(): RoutableSandboxSnapshot[];
  readyRuntimes(): RoutableSandboxRuntime[];
}
```

Mutation rules:

- Reject duplicate IDs and removal of non-stopped runtimes.
- Keep map mutation synchronous; call runtime start/drain/stop outside iteration snapshots.
- Forward runtime snapshot changes when the runtime offers `subscribe`; otherwise emit after lifecycle calls.
- `MultiSandboxRouter` subscribes once, schedules its queue drain on every event, and exposes `close()` that unsubscribes and rejects queued jobs while releasing all leases.
- Add a drain implementation to the current inner v1 runtime and `FakePrismRuntime`: stop new lane acquisition, wait for active handles to settle, then stop. Task 13 moves the characterized v1 implementation without changing this contract.

- [ ] **Step 5: Implement `ProjectFleetManager`**

Use these public ports and API:

```ts
export interface ManagedRuntimeFactory {
  create(input: { projectId: ProjectId; workerIndex: number; leaderGeneration: number }): RoutableSandboxRuntime;
}

export interface ManagedProjectInspection {
  writable: boolean;
  empty: boolean;
  marker: { protocol: "gp-awareness-v2"; workerIndex: number } | null;
}

export interface ManagedProjectInspector {
  inspect(projectId: ProjectId, signal?: AbortSignal): Promise<ManagedProjectInspection>;
}

export interface FleetSnapshot {
  desired: number;
  ready: number;
  provisioning: number;
  draining: number;
  failed: number;
  projects: readonly ProjectId[];
}

export class ProjectFleetManager {
  constructor(options: {
    config: MultiSandboxConfig;
    identity: ResolvedPrismIdentity;
    projects: PrismProjectsClient;
    inspector: ManagedProjectInspector;
    manifest: FleetManifestStore;
    registry: SandboxRegistry;
    runtimeFactory: ManagedRuntimeFactory;
    clock?: Clock;
    random?: RandomSource;
  });
  reconcile(signal?: AbortSignal): Promise<FleetSnapshot>;
  waitForReady(minimum: number, signal?: AbortSignal): Promise<FleetSnapshot>;
  shutdown(reason: string): Promise<void>;
  snapshot(): FleetSnapshot;
}
```

Reconciliation algorithm:

1. Load remote inventory and manifest under the identity lease.
2. Select deduplicated explicit IDs, then valid managed manifest IDs, then titled projects whose Yjs marker is valid, then writable/proven-empty owned projects only when configured. Persist explicit IDs with `managed:false`; removing one from `PRISM_PROJECT_IDS` removes its reuse authority. A title alone never authorizes modification.
3. Clamp the effective desired count by `guestMaxProjects` when the resolved identity is anonymous, including `GP_PRISM_SESSION=auto` that resolved to a guest.
4. For every deficit, persist a `creating` record with stable index/UUID before `POST /api/projects`; refetch on retry and never create a duplicate UUID.
5. Allocate a new persisted leader generation before each runtime start.
6. Start at most `provisionConcurrency` runtimes concurrently; add each runtime independently so healthy capacity becomes routable immediately.
7. Drain excess runtimes in descending worker-index order, wait for stop, and remove them locally without calling project deletion.
8. Bound retry backoff to 30 seconds with jitter, expose failures in snapshots, and leave healthy runtimes untouched.

- [ ] **Step 6: Run focused tests, router tests, and typecheck**

Run: `bun test --isolate tests/fleet-manager.test.ts tests/multi-sandbox-router.test.ts tests/router-policy.test.ts && bun run typecheck`

Expected: all tests PASS; queued work resumes when a runtime appears and no scale-down test records a cloud deletion.

- [ ] **Step 7: Commit**

```bash
git add src/fleet/manager.ts src/sandbox/registry.ts src/router/router.ts src/server/system.ts tests/support/fake-prism.ts tests/fleet-manager.test.ts tests/multi-sandbox-router.test.ts
git commit -m "feat: reconcile dynamic sandbox fleets"
```

---

### Task 5: Define one strict host/sidecar protocol and terminal transcript proof

**Files:**
- Create: `src/v2/protocol/shared.ts`
- Create: `src/v2/protocol/validate.ts`
- Create: `src/v2/protocol/transcript.ts`
- Create: `tests/v2-protocol.test.ts`
- Create: `tests/v2-transcript.test.ts`

**Interfaces:**
- Consumes: Web-standard `TextEncoder`, `TextDecoder`, `crypto.subtle`; existing `SseFrame` and `sanitizeFinal`.
- Produces: shared protocol constants/types, validators, canonical hashing, and terminal validation used by both peers.

- [ ] **Step 1: Write failing protocol-bound tests**

```ts
it("enforces exact frame and window limits", () => {
  expect(GP_V2_FRAME_MAX_BYTES).toBe(16 * 1024);
  expect(GP_V2_WINDOW_MAX_FRAMES).toBe(32);
  expect(GP_V2_WINDOW_MAX_BYTES).toBe(512 * 1024);
  expect(GP_V2_REQUEST_MAX_BYTES).toBe(64 * 1024 * 1024);
  expect(() => parseFrame({ ...requestDataFrame(), dataBase64Url: "eA".repeat(16 * 1024) })).toThrow("frame exceeds");
});

it("rejects wrong project, schema, build, leader, and frame kinds", () => {
  const envelope = validEnvelope();
  for (const mutation of [
    { projectId: PROJECT_B },
    { schemaHash: "wrong" },
    { buildHash: "wrong" },
    { leaderGeneration: 0 },
    { frames: [{ kind: "unknown" }] },
  ]) expect(() => parseAwarenessEnvelope({ ...envelope, ...mutation }, EXPECTED_BINDING)).toThrow(V2ProtocolError);
});
```

- [ ] **Step 2: Write failing transcript/terminal tests**

```ts
it("accepts only a terminal matching request, response, counts, and transcript digest", async () => {
  const proof = new TranscriptProof(REQUEST_ID, REQUEST_DIGEST);
  proof.append({ event: "response.output_text.delta", data: JSON.stringify({ type: "response.output_text.delta", delta: "hi" }) });
  proof.append({ event: "response.completed", data: JSON.stringify({ type: "response.completed", response: FINAL }) });
  const terminal = await proof.createTerminal(FINAL);
  await expect(validateTerminalProof(terminal, proof.snapshot())).resolves.toEqual(FINAL);
  await expect(validateTerminalProof({ ...terminal, transcriptSha256: "0".repeat(64) }, proof.snapshot())).rejects.toThrow("digest");
  await expect(validateTerminalProof({ ...terminal, responseId: "resp_wrong" }, proof.snapshot())).rejects.toThrow("response ID");
});
```

- [ ] **Step 3: Run focused tests and confirm red**

Run: `bun test --isolate tests/v2-protocol.test.ts tests/v2-transcript.test.ts`

Expected: FAIL because no V2 protocol package exists.

- [ ] **Step 4: Implement the shared protocol surface**

`shared.ts` must remain Deno/Bun portable and contain no Node imports:

```ts
export const GP_V2_PROTOCOL = "gp-awareness-v2" as const;
export const GP_V2_SCHEMA_VERSION = 1 as const;
export const GP_V2_FRAME_MAX_BYTES = 16 * 1024;
export const GP_V2_WINDOW_MAX_FRAMES = 32;
export const GP_V2_WINDOW_MAX_BYTES = 512 * 1024;
export const GP_V2_REQUEST_MAX_BYTES = 64 * 1024 * 1024;

export interface AckCursor { epoch: number; sequence: number }
export interface FrameIdentity { epoch: number; sequence: number }

export interface ProtocolBinding {
  projectId: string;
  sessionId: string;
  leaderGeneration: number;
  buildHash: string;
  schemaHash: string;
}

export class V2ProtocolError extends Error {
  constructor(message: string, readonly code: string);
}

export type HostFrame =
  | { kind: "request_open"; requestId: string; requestDigest: string; decodedBytes: number; encoding: "identity" | "gzip+base64"; partCount: number }
  | { kind: "request_data"; requestId: string; partIndex: number; offset: number; dataBase64Url: string }
  | { kind: "request_commit"; requestId: string }
  | { kind: "request_cancel"; requestId: string; reason: string }
  | { kind: "response_ack"; requestId: string; terminalDigest?: string }
  | { kind: "drain"; reason: string }
  | { kind: "ping"; sentAtMs: number };

export type SidecarFrame =
  | { kind: "request_ack"; requestId: string; requestDigest: string; committed: true }
  | { kind: "request_reject"; requestId: string; code: string; message: string }
  | { kind: "response_event"; requestId: string; eventIndex: number; event: string; partIndex: number; partCount: number; offset: number; dataBase64Url: string }
  | { kind: "response_terminal"; requestId: string; requestDigest: string; responseId: string; eventCount: number; byteCount: number; transcriptSha256: string; terminalStatus: "completed" | "failed" | "incomplete"; finalEventIndex: number; finalSha256: string }
  | { kind: "response_error"; requestId: string; code: string; message: string; ambiguous: boolean }
  | { kind: "drained"; active: number; queued: number }
  | { kind: "pong"; sentAtMs: number };

export type ReliableFrame<T> = T & FrameIdentity;

export interface AwarenessEnvelope<T> {
  protocol: typeof GP_V2_PROTOCOL;
  schemaVersion: typeof GP_V2_SCHEMA_VERSION;
  schemaHash: string;
  buildHash: string;
  projectId: string;
  sessionId: string;
  peerId: string;
  instanceId: string;
  leaderGeneration: number;
  epoch: number;
  ack: AckCursor;
  credits: number;
  active: number;
  queued: number;
  draining: boolean;
  frames: readonly ReliableFrame<T>[];
}
```

Use a frozen canonical schema descriptor to calculate the same SHA-256 `schemaHash` in host and sidecar. `validate.ts` must reject unknown keys that affect routing/commitment, non-safe integers, invalid hashes, malformed IDs, oversized encoded frames, non-contiguous data offsets, negative credits, and envelopes whose binding differs from the expected project/session/generation/build/schema.

Every request/response data part carries URL-safe unpadded base64 of bytes, never a JavaScript string slice. Offsets and digests are over decoded bytes, so Unicode and arbitrary tool/image payloads have identical boundaries in Bun and Deno.

`transcript.ts` reassembles all parts of one logical event before canonicalizing it exactly as `event: <event>\ndata: <data>\n\n` in UTF-8. It increments event/byte counters, hashes the complete byte stream, requires a final terminal event and `response.id`, verifies `finalEventIndex` and SHA-256 of that event's final Responses object, and returns `sanitizeFinal(final)` only after all fields match. This avoids duplicating an arbitrarily large final object in the terminal metadata frame.

- [ ] **Step 5: Run focused tests, SSE regressions, and typecheck**

Run: `bun test --isolate tests/v2-protocol.test.ts tests/v2-transcript.test.ts tests/sse.test.ts tests/terminal-rescue.test.ts && bun run typecheck`

Expected: all tests PASS and both runtimes can import `shared.ts` without Node globals.

- [ ] **Step 6: Commit**

```bash
git add src/v2/protocol/shared.ts src/v2/protocol/validate.ts src/v2/protocol/transcript.ts tests/v2-protocol.test.ts tests/v2-transcript.test.ts
git commit -m "feat: define bounded v2 protocol"
```

---

### Task 6: Implement reliable bounded windows and weighted-fair frame scheduling

**Files:**
- Create: `src/v2/protocol/window.ts`
- Create: `src/v2/protocol/scheduler.ts`
- Create: `tests/v2-window.test.ts`
- Create: `tests/v2-scheduler.test.ts`

**Interfaces:**
- Consumes: `ReliableFrame`, `AckCursor`, protocol byte limits.
- Produces: `OutboundFrameWindow`, `InboundFrameCursor`, and `WeightedFrameScheduler` used by host and sidecar engines.

- [ ] **Step 1: Write failing reliability tests**

```ts
it("retains unacknowledged frames, advances cumulatively, and rejects gaps", () => {
  const outbound = new OutboundFrameWindow<HostFrame>({ epoch: 7 });
  outbound.push(frame("ping", "a"));
  outbound.push(frame("ping", "b"));
  expect(outbound.snapshot().frames.map(value => value.sequence)).toEqual([0, 1]);
  outbound.acknowledge({ epoch: 7, sequence: 0 });
  expect(outbound.snapshot().frames.map(value => value.sequence)).toEqual([1]);
  expect(() => outbound.acknowledge({ epoch: 7, sequence: 9 })).toThrow("beyond published");

  const inbound = new InboundFrameCursor<HostFrame>({ epoch: 7 });
  expect(inbound.accept([reliable(1), reliable(0), reliable(1)])).toEqual([reliable(0), reliable(1)]);
  expect(inbound.ack()).toEqual({ epoch: 7, sequence: 1 });
});

it("never exceeds either hard window bound", () => {
  const window = new OutboundFrameWindow<HostFrame>({ epoch: 1 });
  for (let index = 0; index < 32; index++) window.push(sizedFrame(1024));
  expect(() => window.push(sizedFrame(1))).toThrow("32-frame window");
  const bytes = new OutboundFrameWindow<HostFrame>({ epoch: 1 });
  expect(() => bytes.push(sizedFrame(512 * 1024))).toThrow("512 KiB window");
});
```

- [ ] **Step 2: Write failing fairness tests**

```ts
it("prioritizes terminal/control traffic and round-robins active streams", () => {
  const scheduler = new WeightedFrameScheduler<SidecarFrame>();
  scheduler.enqueue("data", "large", eventFrame("large", 0));
  scheduler.enqueue("data", "large", eventFrame("large", 1));
  scheduler.enqueue("data", "small", eventFrame("small", 0));
  scheduler.enqueue("terminal", "small", terminalFrame("small"));
  expect(scheduler.take(4).map(frame => [frame.kind, frame.requestId])).toEqual([
    ["response_terminal", "small"],
    ["response_event", "large"],
    ["response_event", "small"],
    ["response_event", "large"],
  ]);
});
```

- [ ] **Step 3: Run focused tests and confirm red**

Run: `bun test --isolate tests/v2-window.test.ts tests/v2-scheduler.test.ts`

Expected: FAIL because window and scheduler classes do not exist.

- [ ] **Step 4: Implement windows and scheduler**

Use this exact API:

```ts
export class OutboundFrameWindow<T> {
  constructor(options: { epoch: number });
  push(frame: T): ReliableFrame<T>;
  acknowledge(cursor: AckCursor): void;
  snapshot(): { epoch: number; nextSequence: number; encodedBytes: number; frames: readonly ReliableFrame<T>[] };
  reset(epoch: number): void;
}

export class InboundFrameCursor<T> {
  constructor(options: { epoch: number });
  accept(frames: readonly ReliableFrame<T>[]): ReliableFrame<T>[];
  ack(): AckCursor;
  reset(epoch: number): void;
}

export type FramePriority = "terminal" | "control" | "commit" | "data";

export class WeightedFrameScheduler<T extends { requestId?: string }> {
  enqueue(priority: FramePriority, streamId: string, frame: T, weight?: number): void;
  take(maxFrames: number, maxEncodedBytes?: number): T[];
  removeStream(streamId: string): void;
  snapshot(): { queuedFrames: number; streams: number };
}
```

Algorithms:

- Measure UTF-8 bytes of the complete reliable frame before admission; enforce both limits before mutating state.
- Cumulative ACK removes only frames at or below the cursor in the same epoch; stale epochs are ignored and future ACKs throw.
- Inbound cursor buffers at most 32 out-of-order frames, emits each sequence once in ascending order, and never acknowledges across a gap.
- Scheduler uses strict priority between classes and deficit round-robin inside each class; terminal/control quantum is unlimited for one frame, commit quantum is 2, data quantum is the supplied positive weight.
- A stream with waiting data receives one frame before the same stream can receive a second when another ready stream exists.

- [ ] **Step 5: Add a deterministic 10,000-operation property loop**

Generate seeded enqueue/ACK/duplicate/reorder operations and assert:

```ts
expect(new Set(delivered.map(frame => `${frame.epoch}:${frame.sequence}`)).size).toBe(delivered.length);
expect(delivered.map(frame => frame.sequence)).toEqual([...delivered].map(frame => frame.sequence).sort((a, b) => a - b));
expect(outbound.snapshot().encodedBytes).toBeLessThanOrEqual(GP_V2_WINDOW_MAX_BYTES);
expect(outbound.snapshot().frames.length).toBeLessThanOrEqual(GP_V2_WINDOW_MAX_FRAMES);
```

- [ ] **Step 6: Run focused tests and typecheck**

Run: `bun test --isolate tests/v2-window.test.ts tests/v2-scheduler.test.ts && bun run typecheck`

Expected: all tests PASS with deterministic ordering across repeated isolated runs.

- [ ] **Step 7: Commit**

```bash
git add src/v2/protocol/window.ts src/v2/protocol/scheduler.ts tests/v2-window.test.ts tests/v2-scheduler.test.ts
git commit -m "feat: add reliable fair awareness windows"
```

---

### Task 7: Seal bootstrap credentials with an ephemeral authenticated key exchange

**Files:**
- Create: `src/v2/bootstrap/crypto.ts`
- Create: `tests/v2-bootstrap-crypto.test.ts`

**Interfaces:**
- Consumes: an explicit in-memory `BootstrapCredentialPayload`, Web Crypto, and shared canonical encoding.
- Produces: `createBootstrapKeyPair`, `sealBootstrapCredential`, and `openBootstrapCredential`, imported unchanged by the host and synced Deno sidecar without importing host secret-wrapper classes.

- [ ] **Step 1: Write failing round-trip, secrecy, tamper, and replay tests**

```ts
const BINDING: BootstrapBinding = {
  projectId: PROJECT_A,
  sandboxSessionId: "sandbox-session-1",
  leaderGeneration: 7,
  buildHash: "a".repeat(64),
  schemaHash: "b".repeat(64),
  nonce: "nonce-1",
};

it("round-trips a Y-Sweet envelope without serializing plaintext", async () => {
  const sidecar = await createBootstrapKeyPair(BINDING);
  const sealed = await sealBootstrapCredential({ binding: BINDING, sidecarPublicJwk: sidecar.publicJwk, credential: bootstrapCredential(YSWEET) });
  const serialized = JSON.stringify(sealed);
  expect(serialized).not.toContain(YSWEET.token.reveal());
  expect(serialized).not.toContain("prism_session_token");
  await expect(openBootstrapCredential({ sealed, sidecarPrivateKey: sidecar.privateKey, expectedBinding: BINDING })).resolves.toMatchObject({
    docId: PROJECT_A,
    token: YSWEET.token.reveal(),
    authorization: "full",
  });
});

it("rejects ciphertext, nonce, generation, build, and schema tampering", async () => {
  const sidecar = await createBootstrapKeyPair(BINDING);
  const sealed = await sealBootstrapCredential({ binding: BINDING, sidecarPublicJwk: sidecar.publicJwk, credential: bootstrapCredential(YSWEET) });
  for (const changed of [
    { ...sealed, ciphertext: flipBase64UrlByte(sealed.ciphertext) },
    { ...sealed, binding: { ...sealed.binding, nonce: "other" } },
    { ...sealed, binding: { ...sealed.binding, leaderGeneration: 8 } },
    { ...sealed, binding: { ...sealed.binding, buildHash: "c".repeat(64) } },
  ]) await expect(openBootstrapCredential({ sealed: changed, sidecarPrivateKey: sidecar.privateKey, expectedBinding: BINDING })).rejects.toBeInstanceOf(BootstrapCryptoError);
});
```

- [ ] **Step 2: Run the focused test and confirm red**

Run: `bun test --isolate tests/v2-bootstrap-crypto.test.ts`

Expected: FAIL because bootstrap cryptography does not exist.

- [ ] **Step 3: Implement the portable cryptographic surface**

```ts
export interface BootstrapBinding {
  projectId: string;
  sandboxSessionId: string;
  leaderGeneration: number;
  buildHash: string;
  schemaHash: string;
  nonce: string;
}

export interface BootstrapKeyPair {
  publicJwk: JsonWebKey;
  privateKey: CryptoKey;
}

export interface BootstrapCredentialPayload {
  url: string;
  baseUrl: string;
  docId: string;
  token: string;
  authorization: string;
}

export interface SealedBootstrapCredential {
  version: 1;
  binding: BootstrapBinding;
  hostPublicJwk: JsonWebKey;
  salt: string;
  iv: string;
  ciphertext: string;
}

export class BootstrapCryptoError extends Error {
  constructor(message: string, readonly code: "binding" | "key" | "ciphertext" | "payload");
}

export async function createBootstrapKeyPair(binding: BootstrapBinding): Promise<BootstrapKeyPair>;
export async function sealBootstrapCredential(input: {
  binding: BootstrapBinding;
  sidecarPublicJwk: JsonWebKey;
  credential: BootstrapCredentialPayload;
}): Promise<SealedBootstrapCredential>;
export async function openBootstrapCredential(input: {
  sealed: SealedBootstrapCredential;
  sidecarPrivateKey: CryptoKey;
  expectedBinding: BootstrapBinding;
}): Promise<BootstrapCredentialPayload>;
```

Exact construction:

1. Generate ephemeral extractable P-256 ECDH public/private pairs; keep sidecar private keys in process memory only.
2. Import the peer JWK with ECDH `deriveBits`, derive 256 bits, then import as HKDF material.
3. HKDF with random 32-byte salt, SHA-256, and UTF-8 info `ghostprov-v2-bootstrap/v1`.
4. AES-256-GCM with random 12-byte IV and canonical UTF-8 JSON of `BootstrapBinding` as `additionalData`.
5. Serialize only URL-safe unpadded base64, public JWKs, binding, salt, IV, and ciphertext.
6. The host maps `YSweetEnvelope` to the payload by calling `token.reveal()` only inside `sealBootstrapCredential`; no serializer ever sees the secret wrapper.
7. On open, compare every expected binding field before decryption, reject malformed lengths/keys, decrypt, validate every payload string/doc ID/URL, and zero temporary plaintext byte arrays in `finally`.

- [ ] **Step 4: Add a serialized-history canary scan**

Create 100 sealed credentials with distinct token canaries, encode them as the transient Yjs entry representation, and assert none of the raw token strings or cookie-key names occurs in JSON or encoded update bytes.

- [ ] **Step 5: Run focused tests and typecheck**

Run: `bun test --isolate tests/v2-bootstrap-crypto.test.ts && bun run typecheck`

Expected: all tests PASS, including tamper/replay rejection and plaintext scans.

- [ ] **Step 6: Commit**

```bash
git add src/v2/bootstrap/crypto.ts tests/v2-bootstrap-crypto.test.ts
git commit -m "feat: encrypt v2 bootstrap credentials"
```

---

### Task 8: Build a reconnecting Awareness channel with a deterministic fault harness

**Files:**
- Create: `src/v2/awareness/channel.ts`
- Create: `src/v2/awareness/y-sweet-channel.ts`
- Create: `tests/support/fake-awareness.ts`
- Create: `tests/v2-awareness-channel.test.ts`

**Interfaces:**
- Consumes: `YSweetEnvelope`, `AwarenessEnvelope`, expected protocol binding.
- Produces: `AwarenessChannel` for transport sessions and an in-memory network for fault/property tests.

- [ ] **Step 1: Write failing channel lifecycle tests**

```ts
it("publishes only one namespaced awareness field and clears it on close", async () => {
  const bus = new FakeAwarenessNetwork();
  const host = bus.connect("host");
  const sidecar = bus.connect("sidecar");
  const received: unknown[] = [];
  sidecar.subscribe(value => received.push(value));
  host.publish(validHostEnvelope());
  await bus.flush();
  expect(received).toEqual([validHostEnvelope()]);
  await host.close();
  await bus.flush();
  expect(sidecar.peerIds()).not.toContain("host");
  expect(bus.persistentWrites).toBe(0);
});

it("survives duplicated, reordered, and dropped Awareness updates", async () => {
  const bus = new FakeAwarenessNetwork({ drop: [2, 7], duplicate: [1, 5], reverseEvery: 3 });
  const host = bus.connect("host");
  const sidecar = bus.connect("sidecar");
  const seen = new Set<number>();
  sidecar.subscribe(value => seen.add((value as AwarenessEnvelope<HostFrame>).epoch));
  for (let epoch = 1; epoch <= 10; epoch++) host.publish({ ...validHostEnvelope(), epoch });
  await bus.flushAll();
  expect(Math.max(...seen)).toBe(10);
});
```

- [ ] **Step 2: Run the focused test and confirm red**

Run: `bun test --isolate tests/v2-awareness-channel.test.ts`

Expected: FAIL because channel abstractions and the fault harness do not exist.

- [ ] **Step 3: Implement the channel contract**

```ts
export interface AwarenessPeerUpdate<T> {
  peerClientId: number | string;
  envelope: AwarenessEnvelope<T>;
}

export interface AwarenessChannel<TLocal, TRemote> {
  readonly projectId: ProjectId;
  readonly localPeerId: string;
  connect(signal?: AbortSignal): Promise<void>;
  publish(envelope: AwarenessEnvelope<TLocal>): void;
  clear(): void;
  subscribe(listener: (update: AwarenessPeerUpdate<TRemote>) => void): () => void;
  peerIds(): readonly string[];
  close(): Promise<void>;
}

export class YSweetAwarenessChannel<TLocal, TRemote> implements AwarenessChannel<TLocal, TRemote> {
  constructor(options: {
    envelope: YSweetEnvelope;
    localPeerId: string;
    field: "ghostprovV2";
    parseRemote: (value: unknown) => AwarenessEnvelope<TRemote>;
    syncTimeoutMs?: number;
  });
}
```

Production adapter rules:

- Create a dedicated `Y.Doc({ guid: projectId })` and provider; never call `getMap`, `getArray`, or mutate persistent shared types.
- Set/clear only `provider.awareness.setLocalStateField("ghostprovV2", value)`.
- Validate every remote value before notifying listeners; ignore unrelated awareness fields and self states.
- Resolve `connect()` only after provider sync/connected state, reject timeout/abort, and rebind listeners across reconnect without duplicate delivery.
- `clear()` publishes null before close; `close()` is idempotent and destroys provider/doc.

The fake network must support deterministic delivery, drop, duplication, reordering, disconnect, reconnect, and snapshots without timers or persistent writes.

- [ ] **Step 4: Run focused tests plus existing YDoc tests**

Run: `bun test --isolate tests/v2-awareness-channel.test.ts tests/ydoc.test.ts tests/sandbox-ydoc.test.ts && bun run typecheck`

Expected: all tests PASS and existing project-file synchronization remains unchanged.

- [ ] **Step 5: Commit**

```bash
git add src/v2/awareness/channel.ts src/v2/awareness/y-sweet-channel.ts tests/support/fake-awareness.ts tests/v2-awareness-channel.test.ts
git commit -m "feat: add v2 awareness channel"
```

---

### Task 9: Implement request assembly, durable claim transitions, and adaptive admission

**Files:**
- Create: `src/v2/sidecar/assembler.ts`
- Create: `src/v2/sidecar/journal.ts`
- Create: `src/v2/sidecar/admission.ts`
- Create: `sandbox/v2/journal-storage.ts`
- Create: `tests/v2-assembler.test.ts`
- Create: `tests/v2-journal.test.ts`
- Create: `tests/v2-admission.test.ts`

**Interfaces:**
- Consumes: reliable host frames, protocol limits, `Clock`.
- Produces: verified request bytes, append-only claim state, terminal spools, and concurrency permits for the sidecar engine.

- [ ] **Step 1: Write failing assembler tests**

```ts
it("reassembles identity and gzip payloads only after exact integrity proof", async () => {
  const raw = new TextEncoder().encode(JSON.stringify({ model: "gpt-5.6-sol", input: "hello" }));
  const frames = requestFrames(raw, 97);
  const assembler = new RequestAssembler();
  for (const frame of [...frames].reverse()) assembler.accept(frame);
  await expect(assembler.commit(frames[0].requestId)).resolves.toEqual(raw);
});

it.each(["gap", "overlap", "duplicate-conflict", "wrong-digest", "oversized", "decompression-bomb"])("rejects %s before commitment", async fault => {
  const assembler = assemblerWithFault(fault);
  await expect(assembler.commit(REQUEST_ID)).rejects.toBeInstanceOf(RequestAssemblyError);
});
```

- [ ] **Step 2: Write failing journal state-machine tests**

```ts
it("group-commits CLAIMED before ACK and FETCH_STARTING before fetch", async () => {
  const storage = new FakeJournalStorage();
  const journal = new ClaimJournal({ storage, flushIntervalMs: 2, maxBatch: 64 });
  const claimed = journal.claim({ requestId: REQUEST_ID, requestDigest: DIGEST, leaderGeneration: 3 });
  expect(storage.syncedBatches).toBe(0);
  await storage.flushScheduled();
  await claimed;
  expect(storage.records.map(record => record.state)).toEqual(["claimed"]);
  await journal.markFetchStarting(REQUEST_ID);
  expect(storage.records.map(record => record.state)).toEqual(["claimed", "fetch_starting"]);
  expect(journal.canRetry(REQUEST_ID)).toBe(false);
});

it("replays proven terminal spools but preserves unresolved fetch ambiguity", async () => {
  const recovered = await recoverJournal(fixtureRecords(["claimed", "fetch_starting", "terminal"]));
  expect(recovered.terminals).toHaveLength(1);
  const ambiguous = await recoverJournal(fixtureRecords(["claimed", "fetch_starting"]));
  expect(ambiguous.ambiguous).toEqual([REQUEST_ID]);
  expect(ambiguous.retryable).toEqual([]);
});
```

- [ ] **Step 3: Write failing admission tests**

```ts
it("caps active work at 128 and applies the lower host ceiling", async () => {
  const admission = new AdmissionController({ physicalMax: 128, acceptedMax: 256, hostCeiling: 64, memoryBudgetBytes: 512 * 1024 * 1024 });
  const leases = await Promise.all(Array.from({ length: 64 }, () => admission.acquire(4 * 1024 * 1024)));
  expect(admission.snapshot()).toMatchObject({ active: 64, effectiveLimit: 64 });
  expect(admission.tryAcquire(4 * 1024 * 1024)).toBeNull();
  leases.forEach(lease => lease.release());
});

it("throttles large requests by bytes before the count limit", async () => {
  const admission = new AdmissionController({ physicalMax: 128, acceptedMax: 256, hostCeiling: 128, memoryBudgetBytes: 128 * 1024 * 1024 });
  const leases = Array.from({ length: 8 }, () => admission.tryAcquire(32 * 1024 * 1024)).filter(Boolean);
  expect(leases).toHaveLength(4);
  expect(admission.snapshot().reservedBytes).toBe(128 * 1024 * 1024);
  leases.forEach(lease => lease?.release());
});

it("halves capacity after reset bursts and recovers gradually", () => {
  const admission = new AdmissionController({ physicalMax: 128, acceptedMax: 256, hostCeiling: 128, memoryBudgetBytes: 512 * 1024 * 1024, clock });
  admission.recordProxyReset();
  admission.recordProxyReset();
  admission.recordProxyReset();
  expect(admission.snapshot().effectiveLimit).toBe(64);
  clock.advanceBy(30_000);
  admission.refresh();
  expect(admission.snapshot().effectiveLimit).toBe(65);
});
```

- [ ] **Step 4: Run focused tests and confirm red**

Run: `bun test --isolate tests/v2-assembler.test.ts tests/v2-journal.test.ts tests/v2-admission.test.ts`

Expected: FAIL because assembler, journal, and admission modules do not exist.

- [ ] **Step 5: Implement the sidecar-core contracts**

```ts
export class RequestAssembler {
  accept(frame: ReliableFrame<HostFrame>): void;
  commit(requestId: string): Promise<Uint8Array>;
  cancel(requestId: string): void;
  snapshot(): { requests: number; reservedBytes: number };
}

export class RequestAssemblyError extends Error {
  constructor(message: string, readonly code: "bounds" | "gap" | "overlap" | "conflict" | "digest" | "encoding");
}

export type ClaimState = "claimed" | "fetch_starting" | "terminal" | "acked";

export interface JournalRecord {
  version: 1;
  sequence: number;
  requestId: string;
  requestDigest: string;
  leaderGeneration: number;
  state: ClaimState;
  recordedAtMs: number;
  responseId?: string;
  terminalDigest?: string;
}

export interface JournalStorage {
  appendAndSync(records: readonly JournalRecord[]): Promise<void>;
  writeTerminal(requestId: string, bytes: Uint8Array): Promise<void>;
  readTerminal(requestId: string): Promise<Uint8Array | null>;
  deleteTerminal(requestId: string): Promise<void>;
  readRecords(): Promise<readonly JournalRecord[]>;
}

export class ClaimJournal {
  constructor(options: { storage: JournalStorage; clock?: Clock; flushIntervalMs: number; maxBatch: number });
  claim(input: { requestId: string; requestDigest: string; leaderGeneration: number }): Promise<void>;
  markFetchStarting(requestId: string): Promise<void>;
  markTerminal(requestId: string, responseId: string, terminalDigest: string, terminalBytes: Uint8Array): Promise<void>;
  acknowledgeTerminal(requestId: string): Promise<void>;
  canRetry(requestId: string): boolean;
  recover(): Promise<{ terminals: readonly string[]; ambiguous: readonly string[]; retryable: readonly string[] }>;
}

export class AdmissionController {
  constructor(options: { physicalMax: number; acceptedMax: number; hostCeiling: number; memoryBudgetBytes: number; clock?: Clock });
  acquire(reservedBytes: number, signal?: AbortSignal): Promise<{ release(): void }>;
  tryAcquire(reservedBytes: number): { release(): void } | null;
  setHostCeiling(value: number): void;
  recordProxyReset(): void;
  refresh(): void;
  snapshot(): { active: number; queued: number; effectiveLimit: number; physicalMax: number; reservedBytes: number; memoryBudgetBytes: number };
}
```

Assembler rules: reserve declared decoded bytes before accepting data; reject beyond 64 MiB; allow identical duplicates; reject conflicting duplicates, gaps, overlaps, bad offsets/counts/digests; decompress only after all compressed parts and enforce decoded length during decompression. Engine admission reserves `max(4 MiB, decodedBytes * 2)` for each active request, bounded by the configured sidecar memory budget, so large image/edit requests lower concurrency automatically.

Journal rules: recover the maximum valid sequence before assigning the next monotonic value; batch for at most 2 ms or 64 records; resolve transition promises only after `appendAndSync`; make transitions idempotent but reject backward/conflicting states; persist terminal spool before its terminal record; delete spool only after durable `acked`. Ignore only one checksum-invalid/truncated trailing record caused by a torn append; reject corruption before the tail.

Deno storage rules: root `/tmp/ghostprov-v2` mode `0o700`; journal and terminal files `0o600`; append JSONL with a per-record SHA-256 checksum; `Deno.FsFile.syncData()` every batch; terminal temp-write + sync + rename; validate request IDs before constructing paths.

- [ ] **Step 6: Run focused tests and typecheck**

Run: `bun test --isolate tests/v2-assembler.test.ts tests/v2-journal.test.ts tests/v2-admission.test.ts && bun run typecheck`

Expected: all tests PASS, including fault cases and the 128-active invariant.

- [ ] **Step 7: Commit**

```bash
git add src/v2/sidecar/assembler.ts src/v2/sidecar/journal.ts src/v2/sidecar/admission.ts sandbox/v2/journal-storage.ts tests/v2-assembler.test.ts tests/v2-journal.test.ts tests/v2-admission.test.ts
git commit -m "feat: add v2 sidecar safety core"
```

---

### Task 10: Stream upstream Responses through the journaled sidecar engine

**Files:**
- Create: `src/v2/sidecar/upstream.ts`
- Create: `src/v2/sidecar/engine.ts`
- Create: `tests/support/fake-v2-sidecar.ts`
- Create: `tests/v2-upstream.test.ts`
- Create: `tests/v2-sidecar-engine.test.ts`

**Interfaces:**
- Consumes: `RequestAssembler`, `ClaimJournal`, `AdmissionController`, scheduler, host frames, and an injected `fetch`.
- Produces: incremental segmented response frames and safe commitment/recovery behavior for the sandbox entrypoint.

- [ ] **Step 1: Write failing incremental upstream tests**

```ts
it("marks FETCH_STARTING before fetch and streams complete SSE events", async () => {
  const order: string[] = [];
  const client = new DirectResponsesClient({
    baseUrl: "https://openai.test/v1",
    apiKey: secret("api-key"),
    fetch: async (_url, init) => {
      order.push("fetch");
      expect(JSON.parse(String(init?.body))).toMatchObject({ model: "gpt-5.6-sol", stream: true });
      return sseResponse([
        ["response.created", { type: "response.created", response: { id: "resp_1" } }],
        ["response.output_text.delta", { type: "response.output_text.delta", delta: "hello" }],
        ["response.completed", { type: "response.completed", response: FINAL }],
      ], { splitEveryBytes: 7 });
    },
  });
  const events: SseFrame[] = [];
  const terminal = await client.execute({
    body: BODY_BYTES,
    beforeFetch: async () => { order.push("fetch_starting"); },
    onEvent: async event => { events.push(event); },
  });
  expect(order).toEqual(["fetch_starting", "fetch"]);
  expect(events.map(event => event.event)).toEqual(["response.created", "response.output_text.delta", "response.completed"]);
  expect(terminal).toMatchObject({ responseId: "resp_1", status: "completed" });
});

it("classifies a transport exception after FETCH_STARTING as ambiguous", async () => {
  const client = new DirectResponsesClient({ baseUrl: "https://openai.test/v1", apiKey: secret("key"), fetch: async () => { throw new TypeError("reset"); } });
  await expect(client.execute({ body: BODY_BYTES, beforeFetch: async () => {}, onEvent: async () => {} })).rejects.toMatchObject({
    name: "UpstreamExecutionError",
    ambiguous: true,
  });
});
```

- [ ] **Step 2: Write failing engine commitment and recovery tests**

```ts
it("ACKs only after durable claim and permit, then never retries after fetch start", async () => {
  const harness = sidecarHarness({ upstream: resetAfterStart() });
  await harness.sendRequest(BODY);
  expect(harness.timeline.slice(0, 4)).toEqual(["assembled", "claim_synced", "permit", "committed_ack"]);
  await harness.settle();
  expect(harness.upstream.calls).toBe(1);
  expect(harness.frames).toContainEqual(expect.objectContaining({ kind: "response_error", ambiguous: true }));
  expect(harness.journal.canRetry(harness.requestId)).toBe(false);
});

it("persists a terminal before publishing and deletes it only after host ACK", async () => {
  const harness = sidecarHarness({ upstream: completedResponse() });
  await harness.sendRequest(BODY);
  await harness.settle();
  expect(harness.timeline).toContain("terminal_spool_synced");
  expect(harness.timeline.indexOf("terminal_spool_synced")).toBeLessThan(harness.timeline.indexOf("terminal_published"));
  expect(await harness.storage.readTerminal(harness.requestId)).not.toBeNull();
  await harness.engine.accept(responseAck(harness.requestId, harness.terminalDigest));
  expect(await harness.storage.readTerminal(harness.requestId)).toBeNull();
});

it("replays a recovered terminal and quarantines an unresolved fetch", async () => {
  const recovered = sidecarHarness({ recovered: terminalFixture() });
  await recovered.engine.start();
  expect(recovered.frames).toContainEqual(expect.objectContaining({ kind: "response_terminal" }));
  const ambiguous = sidecarHarness({ recovered: fetchStartingFixture() });
  await ambiguous.engine.start();
  expect(ambiguous.frames).toContainEqual(expect.objectContaining({ kind: "response_error", ambiguous: true }));
  expect(ambiguous.upstream.calls).toBe(0);
});
```

- [ ] **Step 3: Run focused tests and confirm red**

Run: `bun test --isolate tests/v2-upstream.test.ts tests/v2-sidecar-engine.test.ts`

Expected: FAIL because direct upstream and sidecar engine modules do not exist.

- [ ] **Step 4: Implement `DirectResponsesClient`**

```ts
export interface DirectResponsesExecution {
  body: Uint8Array;
  signal?: AbortSignal;
  beforeFetch(): Promise<void>;
  onEvent(event: SseFrame): Promise<void>;
}

export interface UpstreamTerminal {
  responseId: string;
  status: "completed" | "failed" | "incomplete";
  eventCount: number;
  byteCount: number;
  transcriptSha256: string;
  final: Record<string, unknown>;
}

export class UpstreamExecutionError extends Error {
  constructor(message: string, readonly code: string, readonly ambiguous: boolean, readonly status?: number);
}

export class DirectResponsesClient {
  constructor(options: { baseUrl: string; apiKey: SecretValue<"OpenAIKey">; caPath?: string; fetch?: FetchLike });
  execute(input: DirectResponsesExecution): Promise<UpstreamTerminal>;
}
```

Execution rules:

- Parse the assembled JSON object, force `stream: true` for the sidecar's HTTP upstream call, and preserve every other supported Responses field unchanged.
- Call and await `beforeFetch()` immediately before `fetch`; every exception after that point is `ambiguous: true` unless an HTTP response or complete terminal event proves the outcome.
- POST `${baseUrl.replace(/\/+$/, "")}/responses` with bearer authorization, JSON content type, and SSE accept header; never log headers/body/key/CA contents.
- Parse arbitrary byte chunk boundaries with `TextDecoder(..., { stream: true })`, bound an individual logical SSE event by the configured response limit, and emit only complete events.
- Parse non-2xx OpenAI JSON into a sanitized `UpstreamExecutionError`; it is not retryable because upstream received the request.
- Require exactly one terminal Responses event and extract its final object/status/ID through `TranscriptProof`.

- [ ] **Step 5: Implement `SidecarEngine`**

```ts
export interface SidecarEngineSnapshot {
  ready: boolean;
  active: number;
  queued: number;
  credits: number;
  draining: boolean;
  ambiguous: number;
}

export class SidecarEngine {
  constructor(options: {
    binding: ProtocolBinding;
    assembler: RequestAssembler;
    journal: ClaimJournal;
    admission: AdmissionController;
    upstream: DirectResponsesClient;
    emit(frame: SidecarFrame, priority: FramePriority, streamId: string): void;
  });
  start(): Promise<void>;
  accept(frame: ReliableFrame<HostFrame>, signal?: AbortSignal): Promise<void>;
  drain(reason: string): Promise<void>;
  snapshot(): SidecarEngineSnapshot;
}
```

State order for `request_commit` is fixed: assemble and validate; durable `claim`; acquire the count-and-byte permit; publish `request_ack(committed=true)`; durable `fetch_starting`; call upstream once. A recovered `claimed` record never starts upstream by itself: it waits for the same host request ID/digest/generation to be republished, reacquires a permit, and replays the committed ACK. Segment every oversized SSE event into ordered `response_event` parts under 16 KiB. Persist the canonical terminal spool and terminal record before scheduling terminal metadata/parts. Release the permit in `finally`. A cancel before `fetch_starting` is safe; a cancel after it aborts upstream but remains non-reroutable. `drain()` rejects new commits, completes active work, then emits `drained`.

- [ ] **Step 6: Run focused tests, protocol tests, and typecheck**

Run: `bun test --isolate tests/v2-upstream.test.ts tests/v2-sidecar-engine.test.ts tests/v2-protocol.test.ts tests/v2-transcript.test.ts && bun run typecheck`

Expected: all tests PASS; every fixture records exactly one upstream call and terminal publication follows durable storage.

- [ ] **Step 7: Commit**

```bash
git add src/v2/sidecar/upstream.ts src/v2/sidecar/engine.ts tests/support/fake-v2-sidecar.ts tests/v2-upstream.test.ts tests/v2-sidecar-engine.test.ts
git commit -m "feat: stream Responses through v2 sidecar"
```

---

### Task 11: Package and securely bootstrap the Deno sidecar without ZIP

**Files:**
- Create: `src/v2/sidecar/assets.ts`
- Create: `src/v2/bootstrap/controller.ts`
- Create: `src/fleet/project-inspector.ts`
- Create: `sandbox/v2/bootstrap.ts`
- Create: `sandbox/v2/awareness-peer.ts`
- Create: `sandbox/v2/main.ts`
- Modify: `src/sandbox/ydoc.ts`
- Create: `scripts/probe_v2_bootstrap.ts`
- Create: `tests/v2-sidecar-assets.test.ts`
- Create: `tests/v2-bootstrap-controller.test.ts`
- Create: `tests/project-inspector.test.ts`
- Modify: `tests/sandbox-ydoc.test.ts`

**Interfaces:**
- Consumes: protocol/crypto/core sources, `SandboxYDoc`, `SandboxGateway`, Y-Sweet credential, leader binding.
- Produces: deterministic sidecar build assets and `V2BootstrapController.ensureReady()`.

- [ ] **Step 1: Write failing asset determinism and secret-scan tests**

```ts
it("produces a stable build hash from sorted non-secret assets", async () => {
  const first = await loadV2SidecarAssets();
  const second = await loadV2SidecarAssets();
  expect(first.buildHash).toMatch(/^[a-f0-9]{64}$/);
  expect(first).toEqual(second);
  expect(first.files.map(file => file.path)).toEqual([...first.files.map(file => file.path)].sort());
  const serialized = JSON.stringify(first);
  expect(serialized).not.toMatch(/sk-[A-Za-z0-9_-]{24,}|gAAAA[A-Za-z0-9_-]{24,}|eyJ[A-Za-z0-9_-]{24,}\.[A-Za-z0-9_-]{24,}/);
});
```

- [ ] **Step 2: Write failing bootstrap handshake tests**

```ts
it("deploys non-secret assets, accepts the bound hello, writes ciphertext, and requires matching readiness", async () => {
  const harness = bootstrapHarness();
  harness.logs.push(bootstrapLogLine(await harness.sidecar.hello()));
  harness.awareness.publish(harness.sidecar.readyEnvelope());
  const descriptor = await harness.controller.ensureReady();
  expect(descriptor).toMatchObject({ buildHash: harness.assets.buildHash, leaderGeneration: 4, capacity: 128 });
  expect(harness.gateway.calls.some(call => call.suffix.includes("zip"))).toBe(false);
  expect(harness.ydoc.writes.every(write => !write.content.includes(harness.ySweet.token.reveal()))).toBe(true);
  expect(harness.ydoc.deleted).toContain("bootstrap.enc.json");
});

it.each(["wrong-nonce", "wrong-build", "wrong-generation", "stale-instance", "timeout"])("fails closed on %s", async fault => {
  const harness = bootstrapHarness({ fault });
  await expect(harness.controller.ensureReady()).rejects.toBeInstanceOf(V2BootstrapError);
  expect(harness.gateway.calls.some(call => call.suffix.includes("zip"))).toBe(false);
});
```

- [ ] **Step 3: Run focused tests and confirm red**

Run: `bun test --isolate tests/v2-sidecar-assets.test.ts tests/v2-bootstrap-controller.test.ts tests/sandbox-ydoc.test.ts`

Expected: FAIL because sidecar assets/controller and scoped YDoc deployment do not exist.

- [ ] **Step 4: Extend `SandboxYDoc` for scoped non-secret assets**

Add exact methods:

```ts
export interface ScopedYDocFile {
  path: string;
  content: string;
}

export class SandboxYDoc {
  ensurePath(path: string): Promise<{ fileId: string; folderId: string }>;
  writePath(path: string, content: string): Promise<void>;
  tombstonePath(path: string): Promise<boolean>;
  readPath(path: string): Promise<string | null>;
}
```

Normalize POSIX relative paths; reject absolute paths, `..`, empty components, NULs, and collisions between file/folder nodes. Reuse existing folders and exact file IDs. Write content as `Y.Text`. Tombstoning clears text then sets `deleted=true`. Keep existing filename APIs as v1 compatibility wrappers.

- [ ] **Step 5: Implement deterministic asset loading**

```ts
export interface V2SidecarAsset { path: string; content: string; sha256: string }
export interface V2SidecarAssets { buildHash: string; schemaHash: string; files: readonly V2SidecarAsset[] }
export async function loadV2SidecarAssets(): Promise<V2SidecarAssets>;
```

Load the runtime-neutral V2 modules plus `sandbox/v2/*.ts`, map them under `.ghostprov-v2/`, hash `path + NUL + UTF-8 content` in sorted order, and fail when a source contains a high-entropy credential literal or imports a non-vendored package. Environment variable names and authorization-header code are allowed; credential values are not. `sandbox/v2/main.ts` imports `npm:yjs@13.6.27` and `npm:@y-sweet/client@0.9.1` with `--cached-only`, instantiates the Deno journal adapter and engine, and uses only `OPENAI_BASE_URL`, `OPENAI_API_KEY`, and `PROXY_CA_CERT_PATH` for upstream. `sandbox/v2/awareness-peer.ts` uses the same reliable window, cumulative cursor, fair scheduler, unchanged-frame retransmission, leader fence, and 16 KiB/32-frame/512 KiB limits as the host; it updates admission's host ceiling from validated host envelopes.

Create `YDocProjectInspector` implementing `ManagedProjectInspector`. It mints a project-scoped Y-Sweet envelope, treats only `authorization: "full"` as writable, connects read-only, parses `.ghostprov-v2/worker.json`, and reports `empty=true` only when every live node is the root/default `main.tex` or a recognized GhostProv marker/bootstrap node. It never writes during inspection and rejects malformed markers.

- [ ] **Step 6: Implement phase-zero bootstrap and controller**

```ts
export interface V2ReadyDescriptor {
  instanceId: string;
  buildHash: string;
  schemaHash: string;
  leaderGeneration: number;
  capacity: number;
}

export class V2BootstrapError extends Error {
  constructor(
    message: string,
    readonly kind: "deploy" | "render" | "hello" | "crypto" | "readiness" | "timeout",
    readonly attempt: number,
  );
}

export class V2BootstrapController {
  constructor(options: {
    projectId: ProjectId;
    workerIndex: number;
    sandboxSessionId: string;
    leaderGeneration: number;
    ySweet: YSweetEnvelope;
    ydoc: SandboxYDoc;
    gateway: SandboxGateway;
    awareness: AwarenessChannel<HostFrame, SidecarFrame>;
    assets: V2SidecarAssets;
    attempts?: number;
    timeoutMs?: number;
  });
  ensureReady(signal?: AbortSignal): Promise<V2ReadyDescriptor>;
  drainAndStop(reason: string, signal?: AbortSignal): Promise<void>;
}
```

Per attempt:

1. Deploy non-secret assets, static binding config, and `.ghostprov-v2/worker.json` containing only protocol, stable worker index, build hash, and creation timestamp.
2. Write a temporary `.latexmkrc` that starts `/code/crixet/deno run --cached-only --cert="$PROXY_CA_CERT_PATH"` with only `OPENAI_BASE_URL`, `OPENAI_API_KEY`, and `PROXY_CA_CERT_PATH` env access plus required net/read/write permissions, waits for `/tmp/ghostprov-v2/bootstrap-hello`, prints its single `GPV2_BOOTSTRAP <base64url-json>` line into render logs, and returns to latexmk.
3. Trigger render through `SandboxGateway.triggerRender`; poll `logs`/render status only, never `zip`.
4. Validate the hello binding, seal the Y-Sweet envelope, and write `.ghostprov-v2/bootstrap.enc.json`.
5. Require a matching Awareness readiness envelope, tombstone ciphertext, restore the inert launcher, and return the descriptor.
6. On failure, tombstone transient ciphertext, restore launcher, reject stale instances, and retry with a new nonce/key no more than three times.

- [ ] **Step 7: Implement the gated secure-bootstrap live probe**

`scripts/probe_v2_bootstrap.ts` must require `GP_LIVE_V2_BOOTSTRAP=1`, one explicit test project, candidate-only settings, and an existing cookie jar. It records port-8787 reachability before/after, deploys one sidecar, proves encrypted handoff/readiness/ping/drain, scans tracked Yjs values for token canaries, never deletes a project, and exits nonzero on any mismatch.

- [ ] **Step 8: Run deterministic tests and typecheck**

Run: `bun test --isolate tests/v2-sidecar-assets.test.ts tests/v2-bootstrap-controller.test.ts tests/project-inspector.test.ts tests/sandbox-ydoc.test.ts tests/v2-bootstrap-crypto.test.ts && bun run typecheck`

Expected: all tests PASS; mocks record no ZIP call and no plaintext credential write.

- [ ] **Step 9: Run the live bootstrap gate only when explicitly configured**

Run: `bun scripts/probe_v2_bootstrap.ts`

Expected without opt-in: exit nonzero with `Set GP_LIVE_V2_BOOTSTRAP=1`; expected with authorized live environment: one matching ready/drained instance, zero plaintext-token matches, and port 8787 reachable before and after.

- [ ] **Step 10: Commit**

```bash
git add src/v2/sidecar/assets.ts src/v2/bootstrap/controller.ts src/fleet/project-inspector.ts sandbox/v2 src/sandbox/ydoc.ts scripts/probe_v2_bootstrap.ts tests/v2-sidecar-assets.test.ts tests/v2-bootstrap-controller.test.ts tests/project-inspector.test.ts tests/sandbox-ydoc.test.ts
git commit -m "feat: securely bootstrap v2 sidecar"
```

---

### Task 12: Implement the host Awareness transport and committed `RunHandle`

**Files:**
- Create: `src/v2/transport/run-handle.ts`
- Create: `src/v2/transport/session.ts`
- Create: `tests/v2-run-handle.test.ts`
- Create: `tests/v2-transport-session.test.ts`

**Interfaces:**
- Consumes: Awareness channel, windows, scheduler, protocol/transcript validation.
- Produces: `V2TransportSession.run()` matching the existing `RunHandle` contract.

- [ ] **Step 1: Write failing commit-boundary and streaming tests**

```ts
it("does not return a RunHandle before committed ACK", async () => {
  const harness = transportHarness();
  const pending = harness.session.run(BODY);
  await harness.flushRequestFrames();
  expect(await promiseState(pending)).toBe("pending");
  harness.sidecar.publish(requestCommitted(harness.requestId, harness.requestDigest));
  const handle = await pending;
  expect(handle.stats.frames).toBe(0);
});

it("streams segmented events and settles only after terminal proof", async () => {
  const harness = transportHarness();
  const handle = await harness.startCommitted(BODY);
  harness.sidecar.publishSegments(eventsAndTerminalFixture());
  const events = await collect(handle.events());
  const final = await handle.final;
  expect(events.map(event => event.event)).toContain("response.completed");
  expect(final).toMatchObject({ id: "resp_1", status: "completed" });
  expect(harness.hostPublished).toContainEqual(expect.objectContaining({ kind: "response_ack" }));
});

it("throws before commitment but quarantines ambiguity after commitment", async () => {
  const before = transportHarness();
  const pending = before.session.run(BODY);
  before.sidecar.publish(requestRejected(before.requestId, "capacity"));
  await expect(pending).rejects.toMatchObject({ name: "V2PreCommitError" });

  const after = transportHarness();
  const handle = await after.startCommitted(BODY);
  after.sidecar.publish(responseError(after.requestId, true));
  await expect(handle.final).rejects.toMatchObject({ name: "V2CommittedError", ambiguous: true });
  expect(after.session.snapshot().quarantined).toBe(1);
});
```

- [ ] **Step 2: Write failing cancellation, reconnect, and backpressure tests**

```ts
it("cancels queued work safely and committed work without reroute", async () => {
  const queued = transportHarness();
  const abortQueued = new AbortController();
  const pending = queued.session.run(BODY, abortQueued.signal);
  abortQueued.abort();
  await expect(pending).rejects.toMatchObject({ name: "AbortError" });

  const committed = transportHarness();
  const abortCommitted = new AbortController();
  const handle = await committed.startCommitted(BODY, abortCommitted.signal);
  abortCommitted.abort();
  expect(committed.hostPublished).toContainEqual(expect.objectContaining({ kind: "request_cancel" }));
  await expect(handle.final).rejects.toMatchObject({ committed: true });
});

it("retransmits unchanged frames after reconnect without duplicate events", async () => {
  const harness = transportHarness({ dropEvery: 3, disconnectAt: 8 });
  const handle = await harness.startCommitted(LARGE_BODY);
  await harness.reconnectAndComplete();
  const events = await collect(handle.events());
  expect(new Set(events.map(event => `${event.event}:${event.data}`)).size).toBe(events.length);
  expect(await handle.final).toMatchObject({ status: "completed" });
});
```

- [ ] **Step 3: Run focused tests and confirm red**

Run: `bun test --isolate tests/v2-run-handle.test.ts tests/v2-transport-session.test.ts`

Expected: FAIL because host V2 transport classes do not exist.

- [ ] **Step 4: Implement the run-handle state machine**

```ts
export class V2PreCommitError extends Error {
  readonly committed = false;
  constructor(message: string, readonly code: string);
}

export class V2CommittedError extends Error {
  readonly committed = true;
  constructor(message: string, readonly code: string, readonly ambiguous: boolean);
}

export class V2RunHandle implements RunHandle {
  readonly final: Promise<Record<string, unknown>>;
  readonly stats: { bytes: number; firstByteMs: number | null; frames: number };
  events(): AsyncGenerator<SseFrame>;
  acceptEvent(event: SseFrame): Promise<void>;
  settle(final: Record<string, unknown>): void;
  fail(error: Error): void;
  cancel(error: Error): void;
}
```

Use a bounded async queue tied to the ResourceGovernor reservation. Acknowledge network frames after they are validated and copied into reserved memory; delay the cumulative network ACK only when that buffer cannot admit the next contiguous frame. Send the request-level `response_ack` only after terminal proof is consumed. Record first-byte latency once, count UTF-8 bytes/frames, close the generator exactly once, and reject/finalize idempotently.

- [ ] **Step 5: Implement `V2TransportSession`**

```ts
export interface V2TransportSnapshot {
  connected: boolean;
  committed: number;
  active: number;
  queued: number;
  quarantined: number;
  credits: number;
  retransmissions: number;
  awarenessRttMs: number | null;
}

export class V2TransportSession {
  constructor(options: {
    binding: ProtocolBinding;
    channel: AwarenessChannel<HostFrame, SidecarFrame>;
    clock?: Clock;
    gzip?: (bytes: Uint8Array) => Promise<Uint8Array>;
    maxResponseBytes: number;
  });
  connect(signal?: AbortSignal): Promise<void>;
  run(body: Record<string, unknown>, signal?: AbortSignal): Promise<RunHandle>;
  drain(reason: string, signal?: AbortSignal): Promise<void>;
  close(): Promise<void>;
  snapshot(): V2TransportSnapshot;
}
```

Session algorithm:

1. Canonically serialize and SHA-256 the request; gzip only when it reduces part count; segment below the 16 KiB encoded bound.
2. Enqueue open/data/commit frames through the fair scheduler and reliable window, honoring remote credits.
3. Return `V2RunHandle` only on matching committed ACK; rejection/disconnect before that rejects `run()` with `V2PreCommitError`.
4. Reassemble segmented event and terminal payloads by event/part index with byte reservations and strict bounds.
5. Feed logical events into `TranscriptProof`, validate terminal identity/digest/counts, settle final, publish terminal ACK, and release state.
6. After commitment, never throw from `run()`; all later failures reject `handle.final` with `V2CommittedError` and increment quarantine when ambiguous.
7. On reconnect, republish the same unacknowledged window/sequence identities and reject stale sidecar instance/generation states.
8. While connected, retransmit the unchanged window when cumulative ACK makes no progress for `max(250 ms, 2 * measured RTT)` plus bounded jitter; never allocate a new sequence for retransmission.
9. Drain rejects new runs, waits active terminal settlement, sends `drain`, requires `drained`, clears Awareness, and closes.

- [ ] **Step 6: Run focused tests, SSE tests, and typecheck**

Run: `bun test --isolate tests/v2-run-handle.test.ts tests/v2-transport-session.test.ts tests/v2-window.test.ts tests/v2-scheduler.test.ts tests/v2-transcript.test.ts tests/sse.test.ts && bun run typecheck`

Expected: all tests PASS; pre-commit and post-commit errors remain distinguishable.

- [ ] **Step 7: Commit**

```bash
git add src/v2/transport/run-handle.ts src/v2/transport/session.ts tests/v2-run-handle.test.ts tests/v2-transport-session.test.ts
git commit -m "feat: add committed v2 host transport"
```

---

### Task 13: Add the routable V2 runtime and enforce identity-aware fallback

**Files:**
- Create: `src/sandbox/managed-v1-runtime.ts`
- Create: `src/v2/runtime.ts`
- Create: `src/v2/transport-selector.ts`
- Modify: `src/router/resource-governor.ts`
- Modify: `src/router/types.ts`
- Modify: `src/sandbox/heartbeat.ts`
- Modify: `src/server/system.ts`
- Create: `tests/v2-runtime.test.ts`
- Create: `tests/transport-selector.test.ts`
- Modify: `tests/resource-governor.test.ts`
- Modify: `tests/heartbeat.test.ts`

**Interfaces:**
- Consumes: provisioning, heartbeat, YDoc, bootstrap controller, transport session, v1 runner, identity kind, governor.
- Produces: `V2SandboxRuntime`, unchanged extracted `ManagedV1Runtime`, and `TransportSelectingRuntime` for the fleet factory.

- [ ] **Step 1: Characterize and extract v1 without behavior changes**

Move the current inner `ManagedSandboxRuntime`, launcher constants, daemon asset helpers, and tracked-lane logic from `src/server/system.ts` into `src/sandbox/managed-v1-runtime.ts`. Add a characterization test:

```ts
it("preserves the v1 descriptor, capacity, and run path after extraction", async () => {
  const harness = v1Harness();
  const runtime = harness.runtime;
  await runtime.start();
  expect(runtime.snapshot()).toMatchObject({ state: "ready", capacity: 30, available: 30 });
  const handle = await runtime.run({ model: "gpt-5.6-sol", input: "v1", stream: true });
  harness.complete(handle);
  await expect(handle.final).resolves.toMatchObject({ status: "completed" });
});
```

Run: `bun test --isolate tests/sandbox-runtime.test.ts tests/multi-sandbox-integration.test.ts`

Expected before extraction: PASS. Run the same command after extraction and require identical PASS output before continuing.

- [ ] **Step 2: Write failing V2 lifecycle and metrics tests**

```ts
it("reaches ready only after provision, heartbeat, YDoc, bootstrap, and Awareness connect", async () => {
  const harness = v2RuntimeHarness();
  await harness.runtime.start();
  expect(harness.order).toEqual(["provision", "heartbeat", "ydoc", "bootstrap", "transport"]);
  expect(harness.runtime.snapshot()).toMatchObject({ state: "ready", capacity: 128, active: 0, quarantined: 0 });
});

it("advertises the lower sidecar, credit, and host-governor ceiling", async () => {
  const harness = v2RuntimeHarness({ sidecarCapacity: 128, credits: 90, governorCeiling: 64 });
  await harness.runtime.start();
  expect(harness.runtime.snapshot().capacity).toBe(64);
});

it("rotates credentials before expiry without dropping committed work", async () => {
  const harness = v2RuntimeHarness({ resourceExpiresAtMs: 600_000 });
  await harness.runtime.start();
  const handle = await harness.runtime.run(BODY);
  await harness.clock.advanceBy(300_001);
  expect(harness.rotations).toBe(1);
  harness.complete(handle);
  await expect(handle.final).resolves.toBeDefined();
});
```

- [ ] **Step 3: Write failing fallback-policy tests**

```ts
it("allows signed-in auto to fall back once before commitment", async () => {
  const runtime = selectorHarness({ identity: "authenticated", mode: "auto", v2Run: preCommitFailure(), v1Run: completedHandle() });
  const handle = await runtime.run(BODY);
  await expect(handle.final).resolves.toBeDefined();
  expect(runtime.calls).toEqual(["v2", "v1"]);
});

it("never falls back for guests or after V2 commitment", async () => {
  const guest = selectorHarness({ identity: "anonymous", mode: "auto", v2Run: preCommitFailure(), v1Run: completedHandle() });
  await expect(guest.run(BODY)).rejects.toBeInstanceOf(V2PreCommitError);
  expect(guest.calls).toEqual(["v2"]);

  const committed = selectorHarness({ identity: "authenticated", mode: "auto", v2Run: ambiguousCommittedHandle(), v1Run: completedHandle() });
  const handle = await committed.run(BODY);
  await expect(handle.final).rejects.toBeInstanceOf(V2CommittedError);
  expect(committed.calls).toEqual(["v2"]);
});
```

- [ ] **Step 4: Run new tests and confirm red**

Run: `bun test --isolate tests/v2-runtime.test.ts tests/transport-selector.test.ts tests/resource-governor.test.ts`

Expected: FAIL because V2 runtime/selector and capacity hints do not exist.

- [ ] **Step 5: Implement host capacity hints**

Add to `ResourceGovernor`:

```ts
capacityHint(perActiveRequestBytes: number, physicalMaximum: number): number {
  if (!Number.isFinite(perActiveRequestBytes) || perActiveRequestBytes <= 0) throw new RangeError("perActiveRequestBytes must be positive");
  this.refreshPressure();
  if (this.snapshot().paused) return 0;
  const remaining = Math.max(0, this.snapshot().reservationCapacityBytes - this.snapshot().reservedBytes);
  return Math.max(0, Math.min(physicalMaximum, Math.floor(remaining / perActiveRequestBytes)));
}
```

Use a configurable V2 transport reservation estimate, default 16 MiB per active stream, only as an advertised ceiling; the existing per-request lease remains authoritative.

Add deterministic heartbeat phasing:

```ts
export function heartbeatPhaseOffset(projectId: ProjectId, intervalMs = 10_000): number;
```

Hash the complete project ID with a stable 32-bit FNV-1a function and return `hash % intervalMs`. Each runtime uses that as its first delay, then preserves exact 10,000 ms start-to-start deadlines without overlap. Extend `tests/heartbeat.test.ts` to assert 32 projects occupy multiple phases and every individual sequence differs by exactly 10,000 ms.

- [ ] **Step 6: Implement V2 runtime and selector**

```ts
export class V2SandboxRuntime implements RoutableSandboxRuntime {
  readonly projectId: ProjectId;
  constructor(options: {
    projectId: ProjectId;
    workerIndex: number;
    leaderGeneration: number;
    provisioning: PrismProvisioningClient;
    account: PrismAccountSession;
    governor: ResourceGovernor;
    createYDoc(credentials: ProvisionedRuntimeCredentials): SandboxYDoc;
    createBootstrap(input: {
      workerIndex: number;
      projectId: ProjectId;
      leaderGeneration: number;
      credentials: ProvisionedRuntimeCredentials;
      ydoc: SandboxYDoc;
      gateway: SandboxGateway;
      awareness: AwarenessChannel<HostFrame, SidecarFrame>;
      assets: V2SidecarAssets;
    }): V2BootstrapController;
    createTransport(input: {
      binding: ProtocolBinding;
      channel: AwarenessChannel<HostFrame, SidecarFrame>;
      maxResponseBytes: number;
      gzip?: (bytes: Uint8Array) => Promise<Uint8Array>;
    }): V2TransportSession;
  });
  start(): Promise<void>;
  drain(reason: string): Promise<void>;
  stop(): Promise<void>;
  run(body: Record<string, unknown>, signal?: AbortSignal): Promise<RunHandle>;
  snapshot(): RoutableSandboxSnapshot;
}

export class TransportSelectingRuntime implements RoutableSandboxRuntime {
  constructor(options: {
    mode: TransportMode;
    identityKind: PrismIdentityKind;
    v2: RoutableSandboxRuntime;
    v1?: RoutableSandboxRuntime;
  });
}
```

V2 startup order is fixed and generation-fenced. Start heartbeat immediately after sandbox allocation. Connect the host YDoc and Awareness channel, complete secure bootstrap, then publish routable capacity. Schedule resource refresh at five minutes before its typed expiry and Y-Sweet refresh/replacement at the configured 45-minute horizon; deliver same-instance refresh only through encrypted Awareness control, and use drain/new-instance handoff when replacement is required. Drain rejects new `run()` calls and waits transport drain before stopping heartbeat/YDoc. Snapshot uses measured Awareness RTT, first-event/completion EWMAs, active/queued/credits, retransmissions, reset rate, and real credential horizon; it must not retain current placeholder constants.

Selector rules: `v1` starts only v1; `v2` starts only V2; `auto` prefers V2. Only authenticated `auto` may invoke v1 after a `V2PreCommitError`. A returned V2 handle proves commitment and can never trigger selector retry. Guest identities never construct a v1 runtime.

- [ ] **Step 7: Run runtime, selector, v1 characterization, and typecheck**

Run: `bun test --isolate tests/v2-runtime.test.ts tests/transport-selector.test.ts tests/resource-governor.test.ts tests/heartbeat.test.ts tests/sandbox-runtime.test.ts tests/multi-sandbox-integration.test.ts && bun run typecheck`

Expected: all tests PASS; extracted v1 behavior remains green and selector call logs prove no post-commit fallback.

- [ ] **Step 8: Commit**

```bash
git add src/sandbox/managed-v1-runtime.ts src/v2/runtime.ts src/v2/transport-selector.ts src/router/resource-governor.ts src/router/types.ts src/sandbox/heartbeat.ts src/server/system.ts tests/v2-runtime.test.ts tests/transport-selector.test.ts tests/resource-governor.test.ts tests/heartbeat.test.ts
git commit -m "feat: route through v2 sandbox runtimes"
```

---

### Task 14: Compose startup, graceful shutdown, and explicit decommissioning

**Files:**
- Create: `src/fleet/decommission.ts`
- Modify: `src/fleet/manager.ts`
- Modify: `src/router/router.ts`
- Modify: `src/server/system.ts`
- Create: `scripts/decommission_v2_fleet.ts`
- Create: `tests/system-v2.test.ts`
- Create: `tests/graceful-shutdown.test.ts`
- Create: `tests/decommission.test.ts`
- Modify: `tests/no-playwright.test.ts`

**Interfaces:**
- Consumes: session manager, manifest, project client, fleet manager, registry, runtime factory, router, server.
- Produces: one browserless V2-capable system lifecycle and an isolated destructive decommission API.

- [ ] **Step 1: Write failing system composition tests**

```ts
it("starts with one ready runtime while the rest reconcile independently", async () => {
  const system = await createBrowserlessSystem(v2TestEnv(), dependencies({ projects: 4, delayed: [2, 3, 4] }));
  const start = system.start();
  await dependencies().ready(1);
  await expect(start).resolves.toBeUndefined();
  expect(system.server.listening).toBe(true);
  expect(system.registry.readyRuntimes()).toHaveLength(1);
  dependencies().releaseAll();
  await system.fleet.waitForReady(4);
  expect(system.registry.readyRuntimes()).toHaveLength(4);
  await system.close();
});

it("never binds protected port 8787 in candidate tests", async () => {
  await expect(createBrowserlessSystem({ ...v2TestEnv(), GP_CANDIDATE: "1", PORT: "8787" })).rejects.toThrow("protected port 8787");
});
```

- [ ] **Step 2: Write failing graceful-shutdown tests**

```ts
it("stops admission, drains terminals, clears ephemeral state, and retains projects", async () => {
  const system = shutdownHarness();
  await system.start();
  const active = await system.request();
  const closing = system.close();
  expect(system.order[0]).toBe("server-stop-admission");
  system.complete(active);
  await closing;
  expect(system.order).toEqual([
    "server-stop-admission",
    "router-drain",
    "runtime-drain",
    "awareness-clear",
    "tmp-clean",
    "runtime-stop",
    "account-close",
    "cpu-pool-close",
  ]);
  expect(system.projects.deleted).toEqual([]);
  expect(system.projects.createdDuringClose).toEqual([]);
});
```

- [ ] **Step 3: Write failing decommission confirmation tests**

```ts
it("requires the exact displayed digest and deletes only managed IDs", async () => {
  const service = decommissionHarness([managed(PROJECT_A), managed(PROJECT_B), unmanaged(PROJECT_C)]);
  const preview = await service.preview([PROJECT_A, PROJECT_C]);
  expect(preview.projects).toEqual([PROJECT_A]);
  await expect(service.execute(preview, "wrong")).rejects.toThrow("confirmation digest");
  await service.execute(preview, preview.confirmationDigest);
  expect(service.deleted).toEqual([PROJECT_A]);
});
```

- [ ] **Step 4: Run focused tests and confirm red**

Run: `bun test --isolate tests/system-v2.test.ts tests/graceful-shutdown.test.ts tests/decommission.test.ts tests/no-playwright.test.ts`

Expected: FAIL because system dependency injection, drain order, and decommission service are absent.

- [ ] **Step 5: Implement system composition and drain APIs**

Add router lifecycle methods:

```ts
beginShutdown(): void;
awaitIdle(timeoutMs: number, signal?: AbortSignal): Promise<void>;
close(): Promise<void>;
```

`beginShutdown()` rejects new starts with a typed 503 error and rejects/release queued uncommitted jobs; existing handles remain active. `awaitIdle()` waits until reserved/active counts reach zero or rejects timeout without claiming terminals that did not arrive.

Refactor `createBrowserlessSystem` to accept optional dependency ports for deterministic tests, resolve identity once, construct manifest/projects/fleet/runtime factory, and return `fleet` in `BrowserlessSystem`. Startup performs initial reconciliation, waits for at least one ready runtime, binds the candidate/public server, and leaves remaining reconciliation running. Shutdown follows the tested order, calls `PrismSessionManager.close()` after runtime cleanup, and never calls project deletion.

- [ ] **Step 6: Implement confirmation-gated decommissioning**

```ts
export interface DecommissionPreview {
  identityFingerprint: string;
  projects: readonly ProjectId[];
  confirmationDigest: string;
}

export class FleetDecommissionService {
  constructor(options: { manifest: FleetManifestStore; projects: PrismProjectsClient });
  preview(requested: readonly ProjectId[]): Promise<DecommissionPreview>;
  execute(preview: DecommissionPreview, suppliedDigest: string, signal?: AbortSignal): Promise<void>;
}
```

Preview intersects requested IDs with `managed: true` manifest entries, sorts IDs, and computes SHA-256 over identity fingerprint plus exact IDs. Execute reloads the manifest, constant-time compares the digest, refuses changed/ambiguous targets, deletes sequentially with audit-safe ID-only output, and updates the manifest only after each successful deletion.

`scripts/decommission_v2_fleet.ts` defaults to preview-only. Deletion requires `GP_ALLOW_PROJECT_DELETE=1`, repeated `--project <uuid>` arguments, and `--confirm <digest>` from the same preview. It never accepts globs, titles, `all`, or a workspace root.

- [ ] **Step 7: Run focused integration tests and typecheck**

Run: `bun test --isolate tests/system-v2.test.ts tests/graceful-shutdown.test.ts tests/decommission.test.ts tests/no-playwright.test.ts tests/router-http.test.ts && bun run typecheck`

Expected: all tests PASS; candidate protection and zero automatic deletion are asserted.

- [ ] **Step 8: Commit**

```bash
git add src/fleet/decommission.ts src/fleet/manager.ts src/router/router.ts src/server/system.ts scripts/decommission_v2_fleet.ts tests/system-v2.test.ts tests/graceful-shutdown.test.ts tests/decommission.test.ts tests/no-playwright.test.ts
git commit -m "feat: complete v2 fleet lifecycle"
```

---

### Task 15: Prove concurrency, fairness, fault recovery, and dynamic scale deterministically

**Files:**
- Create: `tests/v2-concurrency.test.ts`
- Create: `tests/v2-faults.test.ts`
- Create: `tests/v2-dynamic-fleet.test.ts`
- Create: `tests/v2-large-payload.test.ts`
- Modify: `tests/support/fake-awareness.ts`
- Modify: `tests/support/fake-v2-sidecar.ts`

**Interfaces:**
- Consumes: complete in-memory host transport, sidecar engine, fleet manager, registry, router, and governor.
- Produces: deterministic acceptance evidence without cloud resources.

- [ ] **Step 1: Add the four-runtime 512-active proof**

```ts
it("settles 512 unique requests across four runtimes with 128 active each", async () => {
  const system = await fakeV2System({ runtimes: 4, physicalPerRuntime: 128, hostCeilingPerRuntime: 128 });
  const handles = await Promise.all(Array.from({ length: 512 }, (_, index) => system.router.startRelay(
    { model: "gpt-5.6-sol", input: `v2-${index}`, stream: true },
    { clientId: `client-${index % 32}`, source: "http" },
  )));
  expect(system.runtimes.map(runtime => runtime.sidecar.maxActive)).toEqual([128, 128, 128, 128]);
  const finals = await Promise.all(handles.map(handle => handle.final));
  expect(finals).toHaveLength(512);
  expect(new Set(finals.map(value => value.id)).size).toBe(512);
  expect(system.totalUpstreamCalls).toBe(512);
  expect(system.router.snapshot()).toMatchObject({ queued: 0 });
});
```

- [ ] **Step 2: Add accepted-queue and per-client fairness proofs**

Start 256 requests against one 128-active runtime. Hold the first wave, assert `maxActive=128`, release it, assert the second wave completes, and verify the first 32 admissions contain work from every one of 16 clients rather than one client's complete queue.

- [ ] **Step 3: Add fault-matrix proofs**

Use table-driven cases:

```ts
it.each([
  ["drop-and-reorder", fault({ dropEvery: 5, reverseEvery: 3 }), "completed"],
  ["sidecar-crash-before-commit", fault({ crash: "before_commit" }), "rerouted_once"],
  ["sidecar-crash-after-fetch-start", fault({ crash: "after_fetch_start" }), "ambiguous_no_retry"],
  ["stale-leader", fault({ leaderGeneration: -1 }), "rejected"],
  ["terminal-replay", fault({ crash: "after_terminal_spool" }), "recovered"],
  ["token-rotation", fault({ rotateAtEvent: 2 }), "completed"],
])("handles %s", async (_name, injected, expected) => {
  const result = await runFaultScenario(injected);
  expect(result.outcome).toBe(expected);
  expect(result.upstreamCalls).toBeLessThanOrEqual(1);
});
```

- [ ] **Step 4: Add dynamic-fleet matrix proofs**

For fleet sizes `[1, 4, 8, 16, 32]`, reconcile twice, assert exact runtime count/no duplicate creation, route at least `size * 4` requests, add and drain a runtime while work continues, and assert no fixed four-element assumptions or automatic project deletion.

- [ ] **Step 5: Add 4 MiB bidirectional and 64 MiB boundary proofs**

Generate deterministic bytes, send a 4 MiB request and response through segmented Awareness with drop/reorder, and compare SHA-256 exactly. Assert 64 MiB is admitted only when governor reservation succeeds; 64 MiB + 1 byte fails before commitment with a typed 413/protocol error and zero upstream calls.

- [ ] **Step 6: Run the acceptance suite and confirm behavior**

Run: `bun test --isolate tests/v2-concurrency.test.ts tests/v2-faults.test.ts tests/v2-dynamic-fleet.test.ts tests/v2-large-payload.test.ts`

Expected: all tests PASS; 512 unique finals, per-runtime `maxActive=128`, and every ambiguous scenario records at most one upstream call.

- [ ] **Step 7: Measure the 32-runtime routing hot path**

Warm `chooseRuntime` for 2,000 iterations, measure 20,000 point-in-time choices over 32 candidates with `performance.now()`, sort individual durations, and assert p95 below 2 ms. Print p50/p95/max on failure so environmental regressions are diagnosable; keep network, timers, logging, and provisioning outside the measured section.

- [ ] **Step 8: Run routing/resource regressions and typecheck**

Run: `bun test --isolate tests/router-policy.test.ts tests/resource-governor.test.ts tests/workload.test.ts tests/multi-sandbox-router.test.ts tests/multi-sandbox-integration.test.ts && bun run typecheck`

Expected: all tests PASS.

- [ ] **Step 9: Commit**

```bash
git add tests/v2-concurrency.test.ts tests/v2-faults.test.ts tests/v2-dynamic-fleet.test.ts tests/v2-large-payload.test.ts tests/support/fake-awareness.ts tests/support/fake-v2-sidecar.ts
git commit -m "test: prove v2 scale and recovery"
```

---

### Task 16: Prove every public HTTP, SSE, WebSocket, continuation, and image path on V2

**Files:**
- Modify: `tests/http-streaming.test.ts`
- Modify: `tests/websocket.test.ts`
- Modify: `tests/response-graph.test.ts`
- Modify: `tests/images-http.test.ts`
- Modify: `tests/images-compat.test.ts`
- Create: `tests/v2-public-api.test.ts`

**Interfaces:**
- Consumes: unchanged Hono/server APIs and a fake ready V2 runtime.
- Produces: compatibility proof that no public behavior regressed during transport replacement.

- [ ] **Step 1: Add an injected V2 public-server harness**

Bind `createGhostProvServer` to port `0` with a router backed by `FakeV2Sidecar`; return the assigned port and deterministic completion controls. Never reference or probe port 8787 in unit tests.

- [ ] **Step 2: Add HTTP/SSE compatibility assertions**

```ts
it("streams ordered Responses SSE and closes after the proven terminal", async () => {
  const server = await v2PublicHarness();
  const response = await fetch(`${server.url}/v1/responses`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ model: "gpt-5.6-sol", input: "hello", stream: true, reasoning: { effort: "max", mode: "pro" }, service_tier: "fast" }),
  });
  expect(response.headers.get("content-type")).toContain("text/event-stream");
  const events = parsePublicSse(await response.text());
  expect(events.at(-1)?.event).toBe("response.completed");
  expect(server.upstreamBodies[0]).toMatchObject({ reasoning: { effort: "max", mode: "pro" }, service_tier: "fast", stream: true });
});
```

- [ ] **Step 3: Add WebSocket and continuation assertions**

Test unnamed and named lanes, cancellation isolation, and concurrent frames. The public WebSocket request must reject an explicit `stream` field with the existing OpenAI-shaped error, while the sidecar's internal HTTP body must contain `stream: true`. Send two `store:false` turns with `previous_response_id`, assert the host response graph reconstructs the second input, and assert no unsupported `store:true` dependency reaches upstream.

- [ ] **Step 4: Add image generation/edit assertions**

Route `gpt-image-2` generation and multipart edit fixtures through V2. Assert input images/masks are bounded, transformed to the existing Responses-compatible form, response base64/URLs are preserved, large outputs segment below 16 KiB, and every final terminal digest validates.

- [ ] **Step 5: Run all public API tests**

Run: `bun test --isolate tests/v2-public-api.test.ts tests/http-streaming.test.ts tests/websocket.test.ts tests/response-graph.test.ts tests/images-http.test.ts tests/images-compat.test.ts tests/images-service.test.ts`

Expected: all tests PASS; WebSocket mode never forwards the client's explicit `stream` field, while V2's internal HTTP fetch streams correctly.

- [ ] **Step 6: Run no-Playwright and documentation regressions**

Run: `bun test --isolate tests/no-playwright.test.ts tests/documentation.test.ts && bun run typecheck`

Expected: all tests PASS and no Playwright/Patchright dependency or import exists.

- [ ] **Step 7: Commit**

```bash
git add tests/v2-public-api.test.ts tests/http-streaming.test.ts tests/websocket.test.ts tests/response-graph.test.ts tests/images-http.test.ts tests/images-compat.test.ts
git commit -m "test: prove public APIs on v2 transport"
```

---

### Task 17: Add guarded live canaries, operational docs, and final verification

**Files:**
- Create: `scripts/live_v2_single.ts`
- Create: `scripts/live_v2_fleet.ts`
- Modify: `package.json`
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-08-22-ghostprov-v2-awareness-sidecar-design.md` only if implementation evidence changes an explicitly measured value
- Create: `tests/v2-live-guards.test.ts`

**Interfaces:**
- Consumes: completed V2 system and explicit live environment.
- Produces: repeatable one/four-project canaries, safe commands, and final evidence.

- [ ] **Step 1: Write failing live-guard tests**

```ts
it("refuses live execution without every destructive/load opt-in", () => {
  expect(() => resolveLiveV2Options({})).toThrow("GP_LIVE_V2=1");
  expect(() => resolveLiveV2Options({ GP_LIVE_V2: "1", GP_LIVE_PROJECT_COUNT: "5" })).toThrow("1..4");
  expect(() => resolveLiveV2Options({ GP_LIVE_V2: "1", PORT: "8787" })).toThrow("protected port 8787");
});

it("never enables automatic project deletion", () => {
  const options = resolveLiveV2Options({ GP_LIVE_V2: "1", GP_LIVE_PROJECT_COUNT: "4", PORT: "18787" });
  expect(options.deleteProjects).toBe(false);
});
```

- [ ] **Step 2: Implement the single-project live canary**

Require `GP_LIVE_V2=1`, candidate port 18787, one explicit managed test project, and cookie file. Check `http://127.0.0.1:8787` is reachable before work and in `finally`. Prove secure bootstrap, one HTTP stream, one WebSocket continuation, one image fixture when configured, sidecar drain/relaunch, credential rotation, and a simulated sidecar process crash with queued and committed requests. Record sanitized latency/identity metrics only.

- [ ] **Step 3: Implement the four-project scale canary**

Require `GP_LIVE_V2=1`, `GP_LIVE_SCALE=1`, and `GP_LIVE_PROJECT_COUNT=4`. Run, in order:

1. 128 active direct short Responses on one sandbox.
2. 256 accepted on one sandbox with observed maximum active exactly 128.
3. 512 active across four sandboxes with 128 each.
4. 4 MiB bidirectional SHA-256 integrity.
5. Five sidecar lifecycle cycles plus one idle and one active token rotation.

Validate unique upstream response IDs, exact terminal/request digests, zero duplicate execution, zero unproven release, zero plaintext token scan hits, and port-8787 reachability before/after. Do not create more than four projects and do not delete any project.

Enforce the approved latency gates for the short proof prompt: one-sandbox 128-call first-event p95 below 4 seconds, four-sandbox 512-call first-event p95 below 7 seconds per sandbox, and small Awareness transport p95 below 1 second for 512 accepted/128 active. Report raw percentiles and fail rather than silently relaxing thresholds.

When `GP_LIVE_V2_SOAK=1` is separately set, run 100 single-project launch/replacement cycles, require at least 95 first-attempt launches and 100/100 success within three attempts, and preserve the project for reuse afterward.

- [ ] **Step 4: Add scripts and operational documentation**

Add package scripts:

```json
{
  "verify:v2-bootstrap": "bun scripts/probe_v2_bootstrap.ts",
  "verify:v2-single": "bun scripts/live_v2_single.ts",
  "verify:v2-fleet": "bun scripts/live_v2_fleet.ts"
}
```

Document authenticated-default setup, explicit guest opt-in/cap, stable project reuse, candidate startup, health/metrics, graceful shutdown, recovery states, explicit decommission preview/confirmation, v1 signed-in fallback, and the fact that cleanup does not conceal Prism-visible activity.

- [ ] **Step 5: Run deterministic live-guard and full automated suites**

Run:

```bash
bun test --isolate tests/v2-live-guards.test.ts
bun run typecheck
bun test --isolate
python -B -m unittest sandbox/test_gp_daemon.py
```

Expected: guard tests PASS; typecheck exits 0; the complete Bun suite reports zero failures; daemon unittest reports `OK`.

- [ ] **Step 6: Run candidate smoke without touching 8787**

Start the candidate on 18787 with a fake/deterministic runtime, send HTTP and WebSocket smoke requests, stop only the candidate, and verify `127.0.0.1:8787` remains reachable. Expected: candidate returns matching terminals and production reachability is unchanged.

- [ ] **Step 7: Run authorized live canaries**

Run only when the required live environment and explicit opt-ins are present:

```bash
bun run verify:v2-bootstrap
bun run verify:v2-single
bun run verify:v2-fleet
```

Expected: all configured gates PASS with the exact concurrency, uniqueness, terminal-proof, no-secret, lifecycle, and port-safety assertions above. If credentials/projects are unavailable, report live gates as not run; never represent deterministic tests as live proof.

- [ ] **Step 8: Inspect final repository state**

Run:

```bash
git status --short --branch
git diff --check
git log --oneline --decorate -20
```

Expected: branch is `br/v2-awareness-sidecar`, no whitespace errors, no unrelated changes, and `master` remains untouched.

- [ ] **Step 9: Commit**

```bash
git add scripts/live_v2_single.ts scripts/live_v2_fleet.ts package.json README.md tests/v2-live-guards.test.ts docs/superpowers/specs/2026-08-22-ghostprov-v2-awareness-sidecar-design.md
git commit -m "docs: operationalize ghostprov v2"
```

---

## Completion Gate

V2 is complete only when all of the following evidence exists in the current branch:

- Every focused red-green cycle in Tasks 1–17 is recorded by passing tests and reviewable commits.
- `bun run typecheck`, `bun test --isolate`, and `python -B -m unittest sandbox/test_gp_daemon.py` all exit successfully with zero failures.
- Deterministic tests prove 512 unique terminals across four runtimes, 128 maximum active per runtime, fair 256-accepted waves, dynamic 1/4/8/16/32 fleets, 4 MiB integrity, crash ambiguity, terminal recovery, and no duplicate upstream execution.
- Public HTTP/SSE/WebSocket/continuation/image tests all pass through a V2 runtime.
- Candidate smoke uses a non-production port and confirms `127.0.0.1:8787` remains reachable before and after.
- Authorized live evidence, when available, is reported separately from deterministic evidence and never exceeds four projects.
- No V2 path calls ZIP, writes plaintext credentials to Yjs, auto-deletes Prism projects, patches Crixet, or retries after `FETCH_STARTING`.
- The working tree contains no unrelated user-file changes and `master` has not moved.
