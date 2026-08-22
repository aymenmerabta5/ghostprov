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
import {
  V2SandboxRuntime,
  type V2RuntimeBootstrap,
  type V2RuntimeGovernor,
  type V2RuntimeHeartbeat,
  type V2RuntimeTransport,
  type V2RuntimeYDoc,
} from "../src/v2/runtime";

const PROJECT = projectId("e631d12c-d917-4434-b360-41b4715d6679");
const ASSETS: V2SidecarAssets = {
  buildHash: "a".repeat(64),
  schemaHash: "b".repeat(64),
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
  constructor(
    readonly order: string[],
    readonly descriptor: V2ReadyDescriptor,
  ) {}
  async ensureReady(): Promise<V2ReadyDescriptor> {
    this.order.push("bootstrap");
    return this.descriptor;
  }
  async drainAndStop(): Promise<void> {}
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
  async drain(): Promise<void> { this.snapshotValue.connected = false; }
  async close(): Promise<void> { this.snapshotValue.connected = false; }
  snapshot(): V2TransportSnapshot { return { ...this.snapshotValue }; }
  setHostCeiling(value: number): void { this.hostCeilings.push(value); }
  async rotateCredential(): Promise<void> { this.rotations++; }
}

class FakeGovernor implements V2RuntimeGovernor {
  constructor(public ceiling: number) {}
  capacityHint(): number { return this.ceiling; }
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
  resourceExpiresInMs?: number;
} = {}) {
  const order: string[] = [];
  const clock = new ManualClock();
  let current = credentials(clock, options.resourceExpiresInMs ?? 600_000);
  let refreshes = 0;
  let ySweetMints = 0;
  const provisioner = {
    async provision(
      _projectId: typeof PROJECT,
      _signal?: AbortSignal,
      hooks: ProvisioningHooks = {},
    ) {
      order.push("provision");
      await hooks.onSandboxAcquired?.(current.sandbox);
      return current;
    },
    async refreshResources(value: ProvisionedRuntimeCredentials) {
      refreshes++;
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
    buildHash: ASSETS.buildHash,
    schemaHash: ASSETS.schemaHash,
    leaderGeneration: 4,
    capacity: options.sidecarCapacity ?? 128,
  };
  const bootstrap = new FakeBootstrap(order, descriptor);
  const transport = new FakeTransport(order, { credits: options.credits ?? 128 });
  const governor = new FakeGovernor(options.governorCeiling ?? 128);
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
    createGateway: (_sandbox: ProvisionedSandbox) => ({ heartbeat: async () => ({
      ok: true,
      status: 200,
      durationMs: 1,
      sessionId: "sandbox-session-1",
    }) }),
    createHeartbeat: () => heartbeat,
    createYDoc: () => ydoc,
    createAwareness: (_binding: ProtocolBinding) => awareness,
    createBootstrap: () => bootstrap,
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
    refreshes: () => refreshes,
    ySweetMints: () => ySweetMints,
  };
}

describe("V2SandboxRuntime", () => {
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

  it("advertises the lower sidecar, credit, and host-governor ceiling", async () => {
    const harness = runtimeHarness({
      sidecarCapacity: 128,
      credits: 90,
      governorCeiling: 64,
    });
    await harness.runtime.start();

    expect(harness.runtime.snapshot()).toMatchObject({
      capacity: 64,
      available: 64,
      ackEwmaMs: 7,
    });
    expect(harness.transport.hostCeilings.at(-1)).toBe(64);
    await harness.runtime.stop();
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
});
