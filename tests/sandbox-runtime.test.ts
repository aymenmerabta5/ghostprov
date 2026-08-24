import { describe, expect, it } from "bun:test";
import { ManagedV1Runtime } from "../src/sandbox/managed-v1-runtime";
import {
  SandboxRuntime,
  type RuntimeHeartbeat,
  type RuntimeHeartbeatCallbacks,
  type RuntimeYDoc,
} from "../src/sandbox/runtime";
import { parseProvisionedSandbox, parseResourceCredential, parseYSweetEnvelope } from "../src/prism/protocol";
import { projectId } from "../src/sandbox/types";

const A = projectId("e631d12c-d917-4434-b360-41b4715d6679");
const B = projectId("69288e8b-c183-45f3-9131-d64b05b98928");

function credentials(project = A) {
  return {
    projectId: project,
    sandbox: parseProvisionedSandbox({ url: "/s/sandboxes/proxy", token: `sandbox-${project}` }),
    resource: parseResourceCredential({
      access_token: `resource-${project}`,
      max_age_seconds: 3600,
      resources_base_url: "https://prism.test/s/sandbox-resources/",
    }, 1_000),
    ySweet: parseYSweetEnvelope({
      url: `wss://prism.test/y/d/${project}/ws`,
      baseUrl: `https://prism.test/y/d/${project}`,
      docId: project,
      token: `y-${project}`,
      authorization: "full",
    }),
    provisionedAtMs: 1_000,
  };
}

class FakeYDoc implements RuntimeYDoc {
  connected = false;
  destroyed = false;
  async connect(): Promise<void> { this.connected = true; }
  async destroy(): Promise<void> { this.destroyed = true; }
}

class FakeHeartbeat implements RuntimeHeartbeat {
  started = false;
  stopped = false;
  start(): void { this.started = true; }
  async stop(): Promise<void> { this.stopped = true; }
  snapshot() { return { lastSuccessAtMs: this.started ? 1_000 : null }; }
}

function harness(project = A) {
  const events: string[] = [];
  const ydocs: FakeYDoc[] = [];
  const heartbeats: FakeHeartbeat[] = [];
  let callbacks: {
    onSuccess: () => void;
    onExpired: (status: number) => void;
    onTransientFailure: (status: number | null) => void;
    onUnreachable: (status: number | null) => void;
  } | null = null;
  const runtime = new SandboxRuntime({
    projectId: project,
    provision: async () => credentials(project),
    createYDoc: () => {
      const ydoc = new FakeYDoc();
      ydocs.push(ydoc);
      return ydoc;
    },
    createHeartbeat: (_credentials, values) => {
      callbacks = values;
      const heartbeat = new FakeHeartbeat();
      heartbeats.push(heartbeat);
      return heartbeat;
    },
    createDaemonController: () => ({
      ensureReady: async () => ({
        build: "v-test",
        pid: 123,
        capacity: 30,
        protocols: ["segmented-v6.3-embedded-commit"],
      }),
    }),
  });
  runtime.subscribe(snapshot => events.push(snapshot.state.type));
  return {
    runtime,
    events,
    ydocs,
    heartbeats,
    succeed: () => callbacks?.onSuccess(),
    expire: (status = 409) => callbacks?.onExpired(status),
    unreachable: (status: number | null = null) => callbacks?.onUnreachable(status),
  };
}

describe("SandboxRuntime", () => {
  it("reaches ready through the legal typed startup sequence", async () => {
    const value = harness();
    await value.runtime.start();
    expect(value.events).toEqual([
      "configured",
      "provisioning",
      "syncing_credentials",
      "syncing_workspace",
      "deploying_daemon",
      "ready",
    ]);
    expect(value.runtime.snapshot()).toMatchObject({ capacity: 30, available: 30, quarantined: 0 });
    expect(value.heartbeats[0].started).toBe(true);
    expect(value.ydocs[0].connected).toBe(true);
  });

  it("starts heartbeat as soon as a sandbox is acquired, before provisioning completes", async () => {
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const heartbeat = new FakeHeartbeat();
    const runtime = new SandboxRuntime({
      projectId: A,
      provision: async (_project, _signal, hooks) => {
        const acquired = hooks?.onSandboxAcquired;
        if (acquired) await acquired(credentials(A).sandbox);
        await gate;
        return credentials(A);
      },
      createYDoc: () => new FakeYDoc(),
      createHeartbeat: () => heartbeat,
      createDaemonController: () => ({ ensureReady: async () => ({ build: "v-test", pid: 1, capacity: 30, protocols: [] }) }),
    });

    const pending = runtime.start();
    for (let index = 0; index < 3; index++) await Promise.resolve();
    expect(heartbeat.started).toBe(true);
    release();
    await pending;
  });

  it("does not let startup overwrite a heartbeat failure from the same generation", async () => {
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    let callbacks: RuntimeHeartbeatCallbacks | null = null;
    const runtime = new SandboxRuntime({
      projectId: A,
      provision: async (_project, _signal, hooks) => {
        await hooks?.onSandboxAcquired?.(credentials(A).sandbox);
        await gate;
        return credentials(A);
      },
      createYDoc: () => new FakeYDoc(),
      createHeartbeat: (_sandbox, value) => {
        callbacks = value;
        return new FakeHeartbeat();
      },
      createDaemonController: () => ({
        ensureReady: async () => ({ build: "v-test", pid: 1, capacity: 30, protocols: [] }),
      }),
    });

    const starting = runtime.start();
    for (let index = 0; index < 3; index++) await Promise.resolve();
    callbacks!.onExpired(401);
    release();
    await starting;

    expect(runtime.snapshot().state).toMatchObject({
      type: "failed",
      error: { kind: "heartbeat_expired", status: 401 },
    });
  });

  it("fails only the runtime whose heartbeat reports expiry", async () => {
    const left = harness(A);
    const right = harness(B);
    await Promise.all([left.runtime.start(), right.runtime.start()]);
    left.expire();
    expect(left.runtime.snapshot().state).toMatchObject({
      type: "failed",
      error: { kind: "heartbeat_expired", status: 409 },
    });
    expect(right.runtime.snapshot().state.type).toBe("ready");
  });

  it("notifies subscribers when a heartbeat succeeds without changing lifecycle state", async () => {
    const value = harness();
    await value.runtime.start();
    const eventCount = value.events.length;

    value.succeed();

    expect(value.events).toHaveLength(eventCount + 1);
    expect(value.events.at(-1)).toBe("ready");
  });

  it("fails only the runtime whose heartbeat becomes unreachable", async () => {
    const left = harness(A);
    const right = harness(B);
    await Promise.all([left.runtime.start(), right.runtime.start()]);

    left.unreachable();

    expect(left.runtime.snapshot().state).toMatchObject({
      type: "failed",
      error: { kind: "heartbeat_unreachable", status: null },
    });
    expect(right.runtime.snapshot().state.type).toBe("ready");
  });

  it("stops owned heartbeat/Yjs state without touching another runtime", async () => {
    const left = harness(A);
    const right = harness(B);
    await Promise.all([left.runtime.start(), right.runtime.start()]);
    await left.runtime.stop();
    expect(left.heartbeats[0].stopped).toBe(true);
    expect(left.ydocs[0].destroyed).toBe(true);
    expect(left.runtime.snapshot().state.type).toBe("stopped");
    expect(right.runtime.snapshot().state.type).toBe("ready");
  });

  it("ignores stale completion from a superseded provisioning generation", async () => {
    let resolveFirst!: (value: ReturnType<typeof credentials>) => void;
    const first = new Promise<ReturnType<typeof credentials>>(resolve => { resolveFirst = resolve; });
    let calls = 0;
    const runtime = new SandboxRuntime({
      projectId: A,
      provision: async () => ++calls === 1 ? first : credentials(A),
      createYDoc: () => new FakeYDoc(),
      createHeartbeat: () => new FakeHeartbeat(),
      createDaemonController: () => ({ ensureReady: async () => ({ build: "v-test", pid: 1, capacity: 30, protocols: [] }) }),
    });
    const oldStart = runtime.start();
    const recovery = runtime.recover("test");
    resolveFirst(credentials(A));
    await Promise.allSettled([oldStart, recovery]);
    expect(runtime.snapshot().state.type).toBe("ready");
    expect(runtime.snapshot().generation).toBe(2);
  });

  it("ignores heartbeat callbacks from a superseded generation", async () => {
    const callbacks: Array<{
      onSuccess: () => void;
      onExpired: (status: number) => void;
      onTransientFailure: (status: number | null) => void;
      onUnreachable: (status: number | null) => void;
    }> = [];
    const runtime = new SandboxRuntime({
      projectId: A,
      provision: async () => credentials(A),
      createYDoc: () => new FakeYDoc(),
      createHeartbeat: (_sandbox, value) => {
        callbacks.push(value);
        return new FakeHeartbeat();
      },
      createDaemonController: () => ({ ensureReady: async () => ({ build: "v-test", pid: 1, capacity: 30, protocols: [] }) }),
    });
    const events: string[] = [];
    runtime.subscribe(snapshot => events.push(snapshot.state.type));
    await runtime.start();
    await runtime.recover("test");
    const eventCount = events.length;

    callbacks[0]!.onSuccess();
    callbacks[0]!.onExpired(409);
    callbacks[0]!.onUnreachable(null);

    expect(events).toHaveLength(eventCount);
    expect(runtime.snapshot()).toMatchObject({
      generation: 2,
      state: { type: "ready" },
    });
  });

  it("exposes a bounded sanitized failure summary for monitoring", async () => {
    const secret = "session-secret-value";
    const error = Object.assign(
      new Error(`Render returned 401; cookie=${secret}; token=${"a".repeat(96)}`),
      { name: "SandboxGatewayError", status: 401, operation: "render" },
    );
    const runtime = new SandboxRuntime({
      projectId: A,
      provision: async () => credentials(A),
      createYDoc: () => new FakeYDoc(),
      createHeartbeat: () => new FakeHeartbeat(),
      createDaemonController: () => ({ ensureReady: async () => { throw error; } }),
    });

    await expect(runtime.start()).rejects.toBe(error);

    const failure = runtime.snapshot().failure;
    expect(failure).toMatchObject({
      name: "SandboxGatewayError",
      kind: null,
      status: 401,
      operation: "render",
    });
    expect(failure?.message).toContain("Render returned 401");
    expect(failure?.message.length).toBeLessThanOrEqual(240);
    expect(JSON.stringify(failure)).not.toContain(secret);
    expect(JSON.stringify(failure)).not.toContain("a".repeat(64));
  });
});

describe("ManagedV1Runtime", () => {
  it("forwards lifecycle notifications and stops forwarding after unsubscribe", async () => {
    const failure = new Error("provisioning unavailable");
    const runtime = new ManagedV1Runtime({
      projectId: A,
      account: {} as never,
      provisioning: {
        provision: async () => { throw failure; },
      } as never,
      cpuPool: {} as never,
      daemonSource: "",
      expectedBuild: "v-test",
      requestedCapacity: 1,
    });
    const states: string[] = [];
    const unsubscribe = runtime.subscribe(() => states.push(runtime.snapshot().state));

    await expect(runtime.start()).rejects.toBe(failure);
    expect(states).toEqual(["configured", "provisioning", "failed"]);

    unsubscribe();
    await runtime.stop();
    expect(states).toEqual(["configured", "provisioning", "failed"]);
  });
});
