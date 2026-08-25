import { describe, expect, it } from "bun:test";
import type { Clock } from "../src/core/ports";
import type { RunHandle } from "../src/relay/run-handle";
import {
  parseProvisionedSandbox,
  parseResourceCredential,
  parseYSweetEnvelope,
  type ProvisionedSandbox,
} from "../src/prism/protocol";
import type {
  ProvisionedRuntimeCredentials,
  ProvisioningHooks,
} from "../src/prism/provisioning-client";
import { projectId } from "../src/sandbox/types";
import type { AwarenessChannel } from "../src/v2/awareness/channel";
import type { V2ReadyDescriptor } from "../src/v2/bootstrap/controller";
import type {
  AwarenessEnvelope,
  HostFrame,
  ProtocolBinding,
  SidecarFrame,
} from "../src/v2/protocol/shared";
import type { V2SidecarAssets } from "../src/v2/sidecar/assets";
import type {
  V2TransportSnapshot,
} from "../src/v2/transport/session";
import { V2CommittedError, V2RunHandle } from "../src/v2/transport/run-handle";
import {
  V2SandboxRuntime,
  type V2RuntimeBootstrap,
  type V2RuntimeGovernor,
  type V2RuntimeHeartbeat,
  type V2RuntimeHeartbeatCallbacks,
  type V2RuntimeTransport,
  type V2RuntimeYDoc,
} from "../src/v2/runtime";

const PROJECT = projectId("e631d12c-d917-4434-b360-41b4715d6679");
const ASSETS: V2SidecarAssets = {
  revision: "r1",
  buildHash: "a".repeat(64),
  schemaHash: "b".repeat(64),
  entrypointPath: `.ghostprov-v2/revisions/r1/${"b".repeat(64)}/${"a".repeat(64)}/sidecar.bundle.js`,
  manifest: {
    path: `.ghostprov-v2/revisions/r1/${"b".repeat(64)}/${"a".repeat(64)}/manifest.json`,
    logicalPath: "manifest.json",
    content: "{}\n",
    sha256: "c".repeat(64),
  },
  obsoletePaths: Object.freeze([]),
  files: [],
};

class ManualClock implements Clock {
  value = 0;
  waiters: Array<{
    deadline: number;
    resolve: () => void;
    reject: (error: unknown) => void;
    signal?: AbortSignal;
    onAbort?: () => void;
  }> = [];
  now(): number { return this.value; }
  sleep(ms: number, signal?: AbortSignal): Promise<void> {
    return this.waitUntil(this.value + ms, signal);
  }
  waitUntil(deadline: number, signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) return Promise.reject(new DOMException("aborted", "AbortError"));
    if (deadline <= this.value) return Promise.resolve();
    return new Promise((resolve, reject) => {
      const waiter: (typeof this.waiters)[number] = { deadline, resolve, reject, signal };
      waiter.onAbort = () => {
        const index = this.waiters.indexOf(waiter);
        if (index >= 0) this.waiters.splice(index, 1);
        reject(new DOMException("aborted", "AbortError"));
      };
      signal?.addEventListener("abort", waiter.onAbort, { once: true });
      this.waiters.push(waiter);
    });
  }
  async advanceBy(ms: number): Promise<void> {
    this.value += ms;
    for (const waiter of [...this.waiters]) {
      if (waiter.deadline > this.value) continue;
      this.waiters.splice(this.waiters.indexOf(waiter), 1);
      waiter.signal?.removeEventListener("abort", waiter.onAbort!);
      waiter.resolve();
    }
    for (let index = 0; index < 10; index++) await Promise.resolve();
  }
}

class FakeHeartbeat implements V2RuntimeHeartbeat {
  started = false;
  stops = 0;
  constructor(readonly order: string[], readonly clock: ManualClock) {}
  start(): void { this.started = true; this.order.push("heartbeat"); }
  async stop(): Promise<void> { this.stops++; this.started = false; }
  snapshot() { return { lastSuccessAtMs: this.clock.now() }; }
}

class FakeYDoc implements V2RuntimeYDoc {
  constructor(readonly order: string[]) {}
  async connect(): Promise<void> { this.order.push("ydoc"); }
  async destroy(): Promise<void> {}
}

class FakeAwareness implements AwarenessChannel<HostFrame, SidecarFrame> {
  readonly projectId = PROJECT;
  readonly localPeerId = "host";
  rotations = 0;
  async connect(): Promise<void> {}
  publish(_envelope: AwarenessEnvelope<HostFrame>): void {}
  clear(): void {}
  subscribe(): () => void { return () => {}; }
  peerIds(): readonly string[] { return []; }
  rotateCredential(): void { this.rotations++; }
  async close(): Promise<void> {}
}

class FakeBootstrap implements V2RuntimeBootstrap {
  drains = 0;
  constructor(
    readonly order: string[],
    readonly descriptor: V2ReadyDescriptor,
  ) {}
  async ensureReady(): Promise<V2ReadyDescriptor> {
    this.order.push("bootstrap");
    return this.descriptor;
  }
  async drainAndStop(): Promise<void> { this.drains++; }
  async sealCredentialRotation(): Promise<string> {
    return JSON.stringify({ encrypted: "rotation" });
  }
}

class FakeTransport implements V2RuntimeTransport {
  snapshotValue: V2TransportSnapshot;
  readonly pending: Array<{
    resolve: (value: Record<string, unknown>) => void;
    reject: (error: unknown) => void;
  }> = [];
  hostCeilings: number[] = [];
  rotations = 0;
  drains = 0;
  closes = 0;
  livenessProbes = 0;
  probeLiveness?: (signal?: AbortSignal) => Promise<{ rttMs: number; sentAtMs: number }>;

  constructor(
    readonly order: string[],
    snapshot: Partial<V2TransportSnapshot> = {},
  ) {
    this.snapshotValue = {
      connected: false,
      committed: 0,
      active: 0,
      queued: 0,
      quarantined: 0,
      credits: 128,
      retransmissions: 0,
      awarenessRttMs: 7,
      ...snapshot,
    };
  }
  async connect(): Promise<void> {
    this.order.push("transport");
    this.snapshotValue.connected = true;
  }
  async run(): Promise<RunHandle> {
    this.snapshotValue.active++;
    this.snapshotValue.committed++;
    const final = new Promise<Record<string, unknown>>((resolve, reject) => {
      this.pending.push({ resolve, reject });
    }).finally(() => {
      this.snapshotValue.active--;
      this.snapshotValue.committed--;
    });
    return {
      async *events() {},
      final,
      stats: { bytes: 32, firstByteMs: 5, frames: 1 },
    };
  }
  async drain(): Promise<void> { this.drains++; this.snapshotValue.connected = false; }
  async close(): Promise<void> { this.closes++; this.snapshotValue.connected = false; }
  snapshot(): V2TransportSnapshot { return { ...this.snapshotValue }; }
  setHostCeiling(value: number): void { this.hostCeilings.push(value); }
  async rotateCredential(): Promise<void> { this.rotations++; }
}

class FakeGovernor implements V2RuntimeGovernor {
  capacityHintCalls = 0;
  constructor(public ceiling: number) {}
  capacityHint(): number { this.capacityHintCalls++; return this.ceiling; }
  snapshot() {
    return {
      reservationCapacityBytes: 1024,
      reservedBytes: 0,
      paused: false,
    };
  }
}

function credentials(clock: ManualClock, expiresInMs = 600_000): ProvisionedRuntimeCredentials {
  const sandbox = parseProvisionedSandbox({
    url: "https://prism.test/s/sandboxes/proxy",
    token: "sandbox-token",
    sandbox_session_id: "sandbox-session-1",
  });
  return {
    projectId: PROJECT,
    sandbox,
    resource: parseResourceCredential({
      access_token: "resource-token",
      expires_at: (clock.now() + expiresInMs) / 1_000,
      scopes: ["project_files:read"],
      resources_base_url: "https://prism.test/s/sandbox-resources",
    }, clock.now()),
    ySweet: parseYSweetEnvelope({
      url: `wss://prism.test/y/d/${PROJECT}/ws`,
      baseUrl: `https://prism.test/y/d/${PROJECT}`,
      docId: PROJECT,
      token: "y-token",
      authorization: "full",
    }),
    provisionedAtMs: clock.now(),
  };
}

function runtimeHarness(options: {
  sidecarCapacity?: number;
  credits?: number;
  governorCeiling?: number;
  governor?: V2RuntimeGovernor;
  resourceExpiresInMs?: number;
  resourceRefreshError?: Error;
  livenessOutcomes?: Array<Error | null>;
  authSandbox?: ProvisionedSandbox;
  rotateSameSession?: boolean;
  initialSandbox?: ProvisionedSandbox;
} = {}) {
  const order: string[] = [];
  const clock = new ManualClock();
  let current = credentials(clock, options.resourceExpiresInMs ?? 600_000);
  let refreshes = 0;
  let ySweetMints = 0;
  let acquisitions = 0;
  let acquiredProvisions = 0;
  let provisions = 0;
  let refreshedSandboxToken: string | null = null;
  const credentialOutcomes: string[] = [];
  let heartbeatCallbacks: V2RuntimeHeartbeatCallbacks | null = null;
  const provisioner = {
    async provision(
      _projectId: typeof PROJECT,
      _signal?: AbortSignal,
      hooks: ProvisioningHooks = {},
    ) {
      provisions++;
      order.push("provision");
      await hooks.onSandboxAcquired?.(current.sandbox);
      return current;
    },
    async acquireSandbox() {
      acquisitions++;
      return options.authSandbox ?? current.sandbox;
    },
    async provisionAcquired(
      _projectId: typeof PROJECT,
      sandbox: ProvisionedSandbox,
      _signal?: AbortSignal,
      hooks: ProvisioningHooks = {},
    ) {
      acquiredProvisions++;
      order.push("provision-acquired");
      await hooks.onSandboxAcquired?.(sandbox);
      return { ...current, sandbox };
    },
    async refreshResources(value: ProvisionedRuntimeCredentials) {
      refreshes++;
      refreshedSandboxToken = value.sandbox.token.reveal();
      if (options.resourceRefreshError) throw options.resourceRefreshError;
      current = {
        ...value,
        resource: parseResourceCredential({
          access_token: `resource-token-${refreshes}`,
          expires_at: (clock.now() + 600_000) / 1_000,
          scopes: ["project_files:read"],
          resources_base_url: "https://prism.test/s/sandbox-resources",
        }, clock.now()),
      };
      return current.resource;
    },
    async mintYSweet() {
      ySweetMints++;
      return parseYSweetEnvelope({
        url: `wss://prism.test/y/d/${PROJECT}/ws`,
        baseUrl: `https://prism.test/y/d/${PROJECT}`,
        docId: PROJECT,
        token: `y-token-${ySweetMints}`,
        authorization: "full",
      });
    },
  };
  const heartbeat = new FakeHeartbeat(order, clock);
  const ydoc = new FakeYDoc(order);
  const awareness = new FakeAwareness();
  const descriptor: V2ReadyDescriptor = {
    instanceId: "sidecar-instance-1",
    protocolRevision: "r1",
    launchNonce: "runtime-launch-placeholder",
    buildHash: ASSETS.buildHash,
    schemaHash: ASSETS.schemaHash,
    leaderGeneration: 4,
    capacity: options.sidecarCapacity ?? 128,
  };
  const bootstrap = new FakeBootstrap(order, descriptor);
  const transport = new FakeTransport(order, { credits: options.credits ?? 128 });
  if (options.livenessOutcomes) {
    const outcomes = [...options.livenessOutcomes];
    transport.probeLiveness = async signal => {
      if (signal?.aborted) throw new DOMException("aborted", "AbortError");
      transport.livenessProbes++;
      const outcome = outcomes.shift() ?? null;
      if (outcome) throw outcome;
      return { rttMs: 1, sentAtMs: clock.now() };
    };
  }
  const governor = options.governor ?? new FakeGovernor(options.governorCeiling ?? 128);
  const runtime = new V2SandboxRuntime({
    projectId: PROJECT,
    workerIndex: 1,
    leaderGeneration: 4,
    provisioning: provisioner,
    governor,
    clock,
    assets: ASSETS,
    transportReservationBytes: 16 * 1024 * 1024,
    ySweetRefreshMs: 300_000,
    initialSandbox: options.initialSandbox,
    onCredentialRefresh: outcome => credentialOutcomes.push(outcome),
    createGateway: (_sandbox: ProvisionedSandbox) => ({
      heartbeat: async () => ({
        ok: true,
        status: 200,
        durationMs: 1,
        sessionId: "sandbox-session-1",
      }),
      rotateSameSession: () => options.rotateSameSession ?? false,
    }),
    createHeartbeat: (_gateway, callbacks) => {
      heartbeatCallbacks = callbacks;
      return heartbeat;
    },
    createYDoc: () => ydoc,
    createAwareness: (_binding: ProtocolBinding) => awareness,
    createBootstrap: input => {
      (descriptor as { launchNonce: string }).launchNonce = input.binding.launchNonce;
      return bootstrap;
    },
    createTransport: () => transport,
  });
  return {
    runtime,
    order,
    clock,
    heartbeat,
    transport,
    awareness,
    governor,
    bootstrap,
    refreshes: () => refreshes,
    ySweetMints: () => ySweetMints,
    acquisitions: () => acquisitions,
    provisions: () => provisions,
    acquiredProvisions: () => acquiredProvisions,
    refreshedSandboxToken: () => refreshedSandboxToken,
    credentialOutcomes,
    heartbeatSuccess: () => heartbeatCallbacks?.onSuccess({
      ok: true,
      status: 200,
      durationMs: 1,
      sessionId: "sandbox-session-1",
    }),
    heartbeatExpired: (status = 401) => heartbeatCallbacks?.onExpired(status),
    heartbeatAuthenticationFailure: (status = 401, signal = new AbortController().signal) =>
      heartbeatCallbacks?.onAuthenticationFailure(status, signal),
    heartbeatTransientFailure: (status: number | null = null) =>
      heartbeatCallbacks?.onTransientFailure(status),
    heartbeatUnreachable: (status: number | null = null) =>
      heartbeatCallbacks?.onUnreachable(status),
  };
}

describe("V2SandboxRuntime", () => {
  it("dynamically forwards prelude and late rate metadata from the transport handle", async () => {
    const harness = runtimeHarness();
    const realHandle = new V2RunHandle({
      maxBufferedBytes: 4_096,
      expectUpstreamPrelude: true,
    });
    harness.transport.run = async () => realHandle;
    await harness.runtime.start();

    const wrapped = await harness.runtime.run({ model: "gpt-5.6-sol", input: "metadata" });
    realHandle.acceptUpstreamPrelude({
      status: 200,
      contentType: "text/event-stream",
      upstreamRequestId: "req_runtime_prelude",
    });
    await expect(wrapped.upstreamPrelude).resolves.toMatchObject({
      upstreamRequestId: "req_runtime_prelude",
    });
    expect(wrapped.rateLimit).toBeUndefined();
    realHandle.settle(
      { id: "resp_runtime_metadata", status: "failed" },
      { retryAfterMs: 914, remainingTokens: 0 },
    );
    await expect(wrapped.final).resolves.toMatchObject({ id: "resp_runtime_metadata" });
    expect(wrapped.rateLimit).toEqual({ retryAfterMs: 914, remainingTokens: 0 });
    await harness.runtime.stop();
  });

  it("reaches ready only after provision, heartbeat, YDoc, bootstrap, and transport", async () => {
    const harness = runtimeHarness();

    await harness.runtime.start();

    expect(harness.order).toEqual(["provision", "heartbeat", "ydoc", "bootstrap", "transport"]);
    expect(harness.runtime.snapshot()).toMatchObject({
      state: "ready",
      capacity: 128,
      active: 0,
      quarantined: 0,
    });
    await harness.runtime.stop();
  });

  it("emits heartbeat successes without masking transient failures", async () => {
    const harness = runtimeHarness();
    let changes = 0;
    harness.runtime.subscribe(() => { changes++; });
    await harness.runtime.start();
    const afterStart = changes;

    harness.heartbeatTransientFailure(502);
    expect(changes).toBe(afterStart + 1);
    expect(harness.runtime.snapshot().recentFailureRate).toBeCloseTo(0.20);

    harness.heartbeatSuccess();
    expect(changes).toBe(afterStart + 2);
    expect(harness.runtime.snapshot().recentFailureRate).toBeCloseTo(0.20);
    await harness.runtime.stop();
  });

  it("becomes synchronously unroutable on heartbeat expiry without draining", async () => {
    const harness = runtimeHarness();
    await harness.runtime.start();

    harness.heartbeatExpired(401);

    expect(harness.runtime.snapshot()).toMatchObject({
      state: "failed",
      capacity: 0,
      available: 0,
      failure: {
        name: "V2RuntimeLifecycleFailure",
        kind: "heartbeat_expired",
        status: 401,
        operation: "heartbeat",
        message: "V2 sandbox heartbeat credentials expired (401)",
      },
    });
    expect(harness.transport.drains).toBe(0);
    expect(harness.bootstrap.drains).toBe(0);

    harness.heartbeatUnreachable(502);
    expect(harness.runtime.snapshot().failure?.kind).toBe("heartbeat_expired");
    await harness.runtime.stop();
  });

  it("rotates a proven same-session sandbox token without replacing the runtime", async () => {
    const candidate = parseProvisionedSandbox({
      url: "https://prism.test/s/sandboxes/proxy",
      token: "sandbox-token-2",
      sandbox_session_id: "sandbox-session-1",
    });
    const harness = runtimeHarness({
      authSandbox: candidate,
      rotateSameSession: true,
      resourceExpiresInMs: 300_001,
    });
    await harness.runtime.start();

    await expect(harness.heartbeatAuthenticationFailure()).resolves.toBe("retry");
    expect(harness.acquisitions()).toBe(1);
    expect(harness.runtime.takeReplacementSandbox()).toBeNull();
    expect(harness.runtime.snapshot()).toMatchObject({ state: "ready", capacity: 0 });
    harness.heartbeatSuccess();
    expect(harness.runtime.snapshot().capacity).toBe(128);
    expect(harness.credentialOutcomes).toEqual(["updated_in_place"]);
    await harness.clock.advanceBy(1);
    expect(harness.refreshedSandboxToken()).toBe("sandbox-token-2");
    await harness.runtime.stop();
  });

  it("hands a changed sandbox session to the next generation exactly once", async () => {
    const candidate = parseProvisionedSandbox({
      url: "https://prism.test/s/sandboxes/proxy",
      token: "sandbox-token-2",
      sandbox_session_id: "sandbox-session-2",
    });
    const harness = runtimeHarness({ authSandbox: candidate, rotateSameSession: false });
    await harness.runtime.start();

    await expect(harness.heartbeatAuthenticationFailure()).resolves.toBe("expired");
    expect(harness.runtime.takeReplacementSandbox()).toBe(candidate);
    expect(harness.credentialOutcomes).toEqual(["replacement_required"]);
    expect(harness.runtime.takeReplacementSandbox()).toBeNull();
    await harness.runtime.stop();
  });

  it("bootstraps a pre-acquired sandbox without another acquisition", async () => {
    const seed = parseProvisionedSandbox({
      url: "https://prism.test/s/sandboxes/proxy",
      token: "seed-token",
      sandbox_session_id: "sandbox-session-2",
    });
    const harness = runtimeHarness({ initialSandbox: seed });

    await harness.runtime.start();

    expect(harness.provisions()).toBe(0);
    expect(harness.acquiredProvisions()).toBe(1);
    await harness.runtime.stop();
  });

  it("becomes synchronously unroutable when heartbeat failures reach unreachable", async () => {
    const harness = runtimeHarness();
    await harness.runtime.start();

    harness.heartbeatUnreachable(503);

    expect(harness.runtime.snapshot()).toMatchObject({
      state: "failed",
      capacity: 0,
      available: 0,
      failure: {
        name: "V2RuntimeLifecycleFailure",
        kind: "heartbeat_unreachable",
        status: 503,
        operation: "heartbeat",
        message: "V2 sandbox heartbeat became unreachable (503)",
      },
    });
    expect(harness.transport.drains).toBe(0);
    expect(harness.bootstrap.drains).toBe(0);

    await harness.runtime.stop();
    harness.heartbeatExpired(410);
    expect(harness.runtime.snapshot().state).toBe("stopped");
  });

  it("fails closed with a sanitized lifecycle failure when resource refresh expires", async () => {
    const harness = runtimeHarness({
      resourceExpiresInMs: 1,
      resourceRefreshError: new DOMException(
        "authorization=Bearer super-secret-resource-token",
        "AbortError",
      ),
    });
    await harness.runtime.start();
    for (let index = 0; index < 10; index++) await Promise.resolve();

    expect(harness.refreshes()).toBe(1);
    expect(harness.runtime.snapshot().state).toBe("ready");

    await harness.clock.advanceBy(30_000);

    expect(harness.refreshes()).toBe(2);
    expect(harness.runtime.snapshot()).toMatchObject({
      state: "failed",
      capacity: 0,
      available: 0,
      failure: {
        name: "V2RuntimeLifecycleFailure",
        kind: "resource_credential_expired",
        status: null,
        operation: "refresh_resources",
        message: "V2 sandbox resource credential expired after refresh failed",
      },
    });
    expect(JSON.stringify(harness.runtime.snapshot().failure)).not.toContain("super-secret");
    expect(harness.transport.drains).toBe(0);
    await harness.runtime.stop();
  });

  it("gates readiness on a bound sidecar liveness proof", async () => {
    const harness = runtimeHarness({ livenessOutcomes: [null] });

    await harness.runtime.start();

    expect(harness.transport.livenessProbes).toBe(1);
    expect(harness.runtime.snapshot().state).toBe("ready");
    await harness.runtime.stop();
  });

  it("removes capacity on a missed sidecar probe, heals, then fails after three misses", async () => {
    const miss = () => new Error("sidecar pong timed out");
    const harness = runtimeHarness({
      livenessOutcomes: [null, miss(), null, miss(), miss(), miss()],
    });
    await harness.runtime.start();

    await harness.clock.advanceBy(10_000);
    expect(harness.runtime.snapshot()).toMatchObject({ state: "ready", capacity: 0, available: 0 });

    await harness.clock.advanceBy(10_000);
    expect(harness.runtime.snapshot().capacity).toBe(128);

    await harness.clock.advanceBy(10_000);
    await harness.clock.advanceBy(10_000);
    expect(harness.runtime.snapshot().state).toBe("ready");
    await harness.clock.advanceBy(10_000);
    expect(harness.runtime.snapshot()).toMatchObject({
      state: "failed",
      capacity: 0,
      failure: {
        kind: "sidecar_unreachable",
        operation: "sidecar_liveness",
      },
    });
    await harness.runtime.stop();
  });

  it("advertises the lower sidecar and credit ceiling without duplicating global memory", async () => {
    const harness = runtimeHarness({
      sidecarCapacity: 128,
      credits: 90,
      governorCeiling: 64,
    });
    await harness.runtime.start();

    expect(harness.runtime.snapshot()).toMatchObject({
      capacity: 90,
      available: 90,
      ackEwmaMs: 7,
    });
    expect(harness.transport.hostCeilings.at(-1)).toBe(128);
    expect((harness.governor as FakeGovernor).capacityHintCalls).toBe(0);
    await harness.runtime.stop();
  });

  it("does not repeat one global memory hint as each runtime's capacity", async () => {
    const sharedGovernor = new FakeGovernor(0);
    const first = runtimeHarness({
      sidecarCapacity: 30,
      credits: 30,
      governor: sharedGovernor,
    });
    const second = runtimeHarness({
      sidecarCapacity: 30,
      credits: 30,
      governor: sharedGovernor,
    });

    await Promise.all([first.runtime.start(), second.runtime.start()]);
    expect([first.runtime.snapshot().capacity, second.runtime.snapshot().capacity])
      .toEqual([30, 30]);
    expect(first.transport.hostCeilings.at(-1)).toBe(30);
    expect(second.transport.hostCeilings.at(-1)).toBe(30);
    expect(sharedGovernor.capacityHintCalls).toBe(0);
    await Promise.all([first.runtime.stop(), second.runtime.stop()]);
  });

  it("refreshes expiring resources without dropping committed work", async () => {
    const harness = runtimeHarness({ resourceExpiresInMs: 600_000 });
    await harness.runtime.start();
    const handle = await harness.runtime.run({ model: "gpt-5.6-sol", input: "active" });

    await harness.clock.advanceBy(300_001);

    expect(harness.refreshes()).toBe(1);
    expect(harness.ySweetMints()).toBe(1);
    expect(harness.transport.rotations).toBe(1);
    expect(harness.awareness.rotations).toBe(1);
    expect(harness.runtime.snapshot().active).toBe(1);
    harness.transport.pending[0]!.resolve({ id: "resp_1", status: "completed" });
    await expect(handle.final).resolves.toMatchObject({ id: "resp_1" });
    expect(harness.runtime.snapshot().completionEwmaMs).toBeGreaterThan(0);
    await harness.runtime.stop();
  });

  it("caps extreme completion duration and recovers its routing estimate while idle", async () => {
    const harness = runtimeHarness();
    await harness.runtime.start();
    expect(harness.runtime.snapshot().completionEwmaMs).toBe(10_000);

    const extreme = await harness.runtime.run({
      model: "gpt-5.6-sol",
      input: "extreme-duration",
    });
    await harness.clock.advanceBy(553_855);
    harness.transport.pending[0]!.resolve({ id: "resp_extreme", status: "completed" });
    await expect(extreme.final).resolves.toMatchObject({ id: "resp_extreme" });
    expect(harness.runtime.snapshot().completionEwmaMs).toBe(20_000);

    await harness.clock.advanceBy(60_000);
    expect(harness.runtime.snapshot().completionEwmaMs).toBe(15_000);

    const ordinary = await harness.runtime.run({
      model: "gpt-5.6-sol",
      input: "ordinary-duration",
    });
    await harness.clock.advanceBy(6_000);
    harness.transport.pending[1]!.resolve({ id: "resp_ordinary", status: "completed" });
    await expect(ordinary.final).resolves.toMatchObject({ id: "resp_ordinary" });
    expect(harness.runtime.snapshot().completionEwmaMs).toBeCloseTo(12_932.13, 1);
    await harness.runtime.stop();
  });

  it("keeps one hundred cancellations and definitive upstream errors health-neutral", async () => {
    const harness = runtimeHarness();
    await harness.runtime.start();

    for (let index = 0; index < 100; index++) {
      const handle = await harness.runtime.run({
        model: "gpt-5.6-sol",
        input: `cancel-${index}`,
      });
      harness.transport.pending[index]!.reject(new V2CommittedError(
        "V2 request was cancelled after commitment",
        "cancelled",
        true,
      ));
      await expect(handle.final).rejects.toMatchObject({ code: "cancelled" });
    }
    expect(harness.runtime.snapshot().recentFailureRate).toBe(0);

    const definitive = await harness.runtime.run({
      model: "gpt-5.6-sol",
      input: "definitive-upstream-error",
    });
    harness.transport.pending[100]!.reject(new V2CommittedError(
      "Missing required parameter: 'model'.",
      "missing_required_parameter",
      false,
    ));
    await expect(definitive.final).rejects.toMatchObject({
      code: "missing_required_parameter",
    });
    expect(harness.runtime.snapshot().recentFailureRate).toBe(0);
    await harness.runtime.stop();
  });

  it("ignores cancellation quarantine but fails at the capacity-aware ambiguity limit", async () => {
    const harness = runtimeHarness();
    await harness.runtime.start();

    const cancelled = await harness.runtime.run({
      model: "gpt-5.6-sol",
      input: "cancelled-at-full-window",
    });
    harness.transport.snapshotValue.quarantined = 4;
    harness.transport.snapshotValue.ambiguousQuarantined = 0;
    harness.transport.snapshotValue.ambiguityLimit = 1;
    harness.transport.pending[0]!.reject(new V2CommittedError(
      "V2 request was cancelled after commitment",
      "cancelled",
      true,
    ));
    await expect(cancelled.final).rejects.toMatchObject({ code: "cancelled" });
    expect(harness.runtime.snapshot().state).toBe("ready");

    const ambiguous = await harness.runtime.run({
      model: "gpt-5.6-sol",
      input: "permanent-ambiguity",
    });
    harness.transport.snapshotValue.quarantined = 1;
    harness.transport.snapshotValue.ambiguousQuarantined = 1;
    harness.transport.snapshotValue.ambiguityLimit = 1;
    harness.transport.pending[1]!.reject(new V2CommittedError(
      "Transport lost terminal proof",
      "transport",
      true,
    ));
    await expect(ambiguous.final).rejects.toMatchObject({ code: "transport" });
    expect(harness.runtime.snapshot()).toMatchObject({
      state: "failed",
      capacity: 0,
      failure: { kind: "ambiguity_exhausted" },
    });
    await harness.runtime.stop();
  });

  it("penalizes ambiguous transport failure temporarily and heals on success", async () => {
    const harness = runtimeHarness();
    await harness.runtime.start();

    const failed = await harness.runtime.run({
      model: "gpt-5.6-sol",
      input: "ambiguous",
    });
    harness.transport.pending[0]!.reject(new V2CommittedError(
      "Transport lost terminal proof",
      "transport",
      true,
    ));
    await expect(failed.final).rejects.toMatchObject({ code: "transport" });
    expect(harness.runtime.snapshot().recentFailureRate).toBeCloseTo(0.20);

    await harness.clock.advanceBy(60_000);
    expect(harness.runtime.snapshot().recentFailureRate).toBeCloseTo(0.10);

    const healthy = await harness.runtime.run({
      model: "gpt-5.6-sol",
      input: "healthy",
    });
    harness.transport.pending[1]!.resolve({ id: "resp_healthy", status: "completed" });
    await expect(healthy.final).resolves.toMatchObject({ id: "resp_healthy" });
    expect(harness.runtime.snapshot().recentFailureRate).toBeCloseTo(0.08);
    await harness.runtime.stop();
  });
});
