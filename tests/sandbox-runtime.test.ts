import { describe, expect, it } from "bun:test";
import { SandboxRuntime, type RuntimeHeartbeat, type RuntimeYDoc } from "../src/sandbox/runtime";
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
  let callbacks: { onExpired: (status: number) => void; onTransientFailure: (status: number | null) => void } | null = null;
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
  return { runtime, events, ydocs, heartbeats, expire: (status = 409) => callbacks?.onExpired(status) };
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

  it("drains only the runtime whose heartbeat reports expiry", async () => {
    const left = harness(A);
    const right = harness(B);
    await Promise.all([left.runtime.start(), right.runtime.start()]);
    left.expire();
    expect(left.runtime.snapshot().state.type).toBe("draining");
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
});
