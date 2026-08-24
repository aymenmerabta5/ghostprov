import { describe, expect, it } from "bun:test";
import type { RunHandle } from "../src/relay/run-handle";
import { projectId, type ProjectId } from "../src/sandbox/types";
import type {
  RoutableSandboxRuntime,
  RoutableSandboxSnapshot,
} from "../src/sandbox/registry";
import { TransportSelectingRuntime } from "../src/v2/transport-selector";
import {
  V2CommittedError,
  V2PreCommitError,
} from "../src/v2/transport/run-handle";

const PROJECT = projectId("e631d12c-d917-4434-b360-41b4715d6679");
const BODY = { model: "gpt-5.6-sol", input: "selector", stream: true };

function handle(final: Promise<Record<string, unknown>> = Promise.resolve({
  id: "resp_1",
  status: "completed",
})): RunHandle {
  return {
    async *events() {},
    final,
    stats: { bytes: 0, firstByteMs: null, frames: 0 },
  };
}

class FakeRuntime implements RoutableSandboxRuntime {
  starts = 0;
  stops = 0;
  drains = 0;
  runs = 0;
  state = "configured";
  readonly listeners = new Set<() => void>();

  constructor(
    readonly projectId: ProjectId,
    readonly behavior: () => Promise<RunHandle>,
    readonly calls: string[],
    readonly label: string,
  ) {}

  async start(): Promise<void> {
    this.starts++;
    this.state = "ready";
    this.calls.push(`${this.label}:start`);
    this.emit();
  }
  async drain(): Promise<void> {
    this.drains++;
    this.state = "stopped";
    this.calls.push(`${this.label}:drain`);
    this.emit();
  }
  async stop(): Promise<void> {
    this.stops++;
    this.state = "stopped";
    this.calls.push(`${this.label}:stop`);
    this.emit();
  }
  run(): Promise<RunHandle> {
    this.runs++;
    this.calls.push(this.label);
    return this.behavior();
  }
  snapshot(): RoutableSandboxSnapshot {
    return {
      projectId: this.projectId,
      state: this.state,
      capacity: this.state === "ready" ? 1 : 0,
      active: 0,
      available: this.state === "ready" ? 1 : 0,
      quarantined: 0,
      heartbeatAgeMs: 0,
      credentialExpiresInMs: 60_000,
      ackEwmaMs: 1,
      completionEwmaMs: 1,
      recentFailureRate: 0,
      queueDepth: 0,
      memoryPressure: 0,
      failure: null,
    };
  }
  subscribe(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }
  emit(): void { for (const listener of this.listeners) listener(); }
}

describe("TransportSelectingRuntime", () => {
  it("allows authenticated auto to fall back once before commitment", async () => {
    const calls: string[] = [];
    const v2 = new FakeRuntime(PROJECT, async () => {
      throw new V2PreCommitError("v2 unavailable", "unavailable");
    }, calls, "v2");
    const v1 = new FakeRuntime(PROJECT, async () => handle(), calls, "v1");
    const runtime = new TransportSelectingRuntime({
      mode: "auto",
      identityKind: "authenticated",
      v2,
      v1,
    });
    await runtime.start();

    const result = await runtime.run(BODY);

    await expect(result.final).resolves.toMatchObject({ status: "completed" });
    expect(calls).toEqual(["v2:start", "v2", "v1:start", "v1"]);
    expect(runtime.snapshot().state).toBe("ready");
  });

  it("falls back during authenticated startup before any request can commit", async () => {
    const calls: string[] = [];
    const v2 = new FakeRuntime(PROJECT, async () => handle(), calls, "v2");
    v2.start = async () => {
      calls.push("v2:start");
      v2.state = "failed";
      v2.emit();
      throw new Error("bootstrap unavailable");
    };
    const v1 = new FakeRuntime(PROJECT, async () => handle(), calls, "v1");
    const runtime = new TransportSelectingRuntime({
      mode: "auto",
      identityKind: "authenticated",
      v2,
      v1,
    });

    await runtime.start();
    const result = await runtime.run(BODY);

    await expect(result.final).resolves.toBeDefined();
    expect(calls).toEqual(["v2:start", "v1:start", "v1"]);
    expect(runtime.snapshot()).toEqual(v1.snapshot());
  });

  it("does not expose the intermediate V2 startup failure before V1 fallback is ready", async () => {
    const calls: string[] = [];
    const v2 = new FakeRuntime(PROJECT, async () => handle(), calls, "v2");
    v2.start = async () => {
      v2.state = "failed";
      v2.emit();
      throw new Error("V2 failed");
    };
    const v1 = new FakeRuntime(PROJECT, async () => handle(), calls, "v1");
    const runtime = new TransportSelectingRuntime({
      mode: "auto",
      identityKind: "authenticated",
      v2,
      v1,
    });
    const states: string[] = [];
    runtime.subscribe(() => states.push(runtime.snapshot().state));

    await runtime.start();

    expect(states).toEqual(["ready"]);
    expect(runtime.snapshot()).toEqual(v1.snapshot());
  });

  it("does not start V1 for neutral or concurrent V2 pre-commit failures", async () => {
    const calls: string[] = [];
    const v1 = new FakeRuntime(PROJECT, async () => handle(), calls, "v1");
    for (const code of ["capacity_changed", "request_too_large"] as const) {
      const v2 = new FakeRuntime(PROJECT, async () => {
        throw new V2PreCommitError("neutral", code);
      }, calls, `v2-${code}`);
      const runtime = new TransportSelectingRuntime({
        mode: "auto",
        identityKind: "authenticated",
        v2,
        v1,
      });
      await runtime.start();
      await expect(runtime.run(BODY)).rejects.toBeInstanceOf(V2PreCommitError);
    }
    expect(v1.starts).toBe(0);

    const busyV2 = new FakeRuntime(PROJECT, async () => {
      throw new V2PreCommitError("unavailable", "unavailable");
    }, calls, "busy-v2");
    const originalSnapshot = busyV2.snapshot.bind(busyV2);
    busyV2.snapshot = () => ({ ...originalSnapshot(), active: 1 });
    const busy = new TransportSelectingRuntime({
      mode: "auto",
      identityKind: "authenticated",
      v2: busyV2,
      v1,
    });
    await busy.start();
    await expect(busy.run(BODY)).rejects.toBeInstanceOf(V2PreCommitError);
    expect(v1.starts).toBe(0);
  });

  it("never falls back for anonymous identities", async () => {
    const calls: string[] = [];
    const v2 = new FakeRuntime(PROJECT, async () => {
      throw new V2PreCommitError("v2 unavailable", "unavailable");
    }, calls, "v2");
    const runtime = new TransportSelectingRuntime({
      mode: "auto",
      identityKind: "anonymous",
      v2,
    });
    await runtime.start();

    await expect(runtime.run(BODY)).rejects.toBeInstanceOf(V2PreCommitError);
    expect(calls).toEqual(["v2:start", "v2"]);
  });

  it("never falls back after V2 returned a committed handle", async () => {
    const calls: string[] = [];
    const error = new V2CommittedError("ambiguous", "transport", true);
    const v2 = new FakeRuntime(PROJECT, async () => handle(Promise.reject(error)), calls, "v2");
    const v1 = new FakeRuntime(PROJECT, async () => handle(), calls, "v1");
    const runtime = new TransportSelectingRuntime({
      mode: "auto",
      identityKind: "authenticated",
      v2,
      v1,
    });
    await runtime.start();

    const result = await runtime.run(BODY);

    await expect(result.final).rejects.toBe(error);
    expect(calls).toEqual(["v2:start", "v2"]);
    expect(v1.starts).toBe(0);
  });

  it("starts and stops only the explicitly selected transport", async () => {
    for (const mode of ["v1", "v2"] as const) {
      const calls: string[] = [];
      const v2 = new FakeRuntime(PROJECT, async () => handle(), calls, "v2");
      const v1 = new FakeRuntime(PROJECT, async () => handle(), calls, "v1");
      const runtime = new TransportSelectingRuntime({
        mode,
        identityKind: "authenticated",
        v2,
        v1,
      });
      await runtime.start();
      await runtime.stop();
      expect(calls).toEqual([`${mode}:start`, `${mode}:stop`]);
    }
  });

  it("stops an in-flight V2 startup and never leaves its late completion live", async () => {
    const calls: string[] = [];
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const v2 = new FakeRuntime(PROJECT, async () => handle(), calls, "v2");
    v2.start = async () => {
      v2.starts++;
      calls.push("v2:start");
      await gate;
      v2.state = "ready";
    };
    const runtime = new TransportSelectingRuntime({
      mode: "v2",
      identityKind: "authenticated",
      v2,
    });

    const starting = runtime.start();
    await Promise.resolve();
    await runtime.stop();
    release();

    await expect(starting).rejects.toThrow("stopped during V2 startup");
    expect(v2.stops).toBeGreaterThanOrEqual(1);
    expect(v2.state).toBe("stopped");
  });

  it("forces stop instead of draining an in-flight transport startup", async () => {
    const calls: string[] = [];
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const v2 = new FakeRuntime(PROJECT, async () => handle(), calls, "v2");
    v2.start = async () => {
      v2.starts++;
      await gate;
      v2.state = "ready";
    };
    const runtime = new TransportSelectingRuntime({
      mode: "v2",
      identityKind: "authenticated",
      v2,
    });

    const starting = runtime.start();
    await Promise.resolve();
    await runtime.drain("shutdown-during-start");
    release();

    await expect(starting).rejects.toThrow("stopped during V2 startup");
    expect(v2.drains).toBe(0);
    expect(v2.stops).toBeGreaterThanOrEqual(1);
  });

  it("prefers V2 again in a fresh generation after an older generation used V1", async () => {
    const calls: string[] = [];
    const oldV2 = new FakeRuntime(PROJECT, async () => handle(), calls, "old-v2");
    oldV2.start = async () => { throw new Error("old V2 bootstrap failed"); };
    const oldV1 = new FakeRuntime(PROJECT, async () => handle(), calls, "old-v1");
    const oldGeneration = new TransportSelectingRuntime({
      mode: "auto",
      identityKind: "authenticated",
      v2: oldV2,
      v1: oldV1,
    });
    await oldGeneration.start();
    expect(oldGeneration.snapshot()).toEqual(oldV1.snapshot());
    await oldGeneration.stop();

    const freshV2 = new FakeRuntime(PROJECT, async () => handle(), calls, "fresh-v2");
    const freshV1 = new FakeRuntime(PROJECT, async () => handle(), calls, "fresh-v1");
    const freshGeneration = new TransportSelectingRuntime({
      mode: "auto",
      identityKind: "authenticated",
      v2: freshV2,
      v1: freshV1,
    });
    await freshGeneration.start();

    expect(freshV2.starts).toBe(1);
    expect(freshV1.starts).toBe(0);
    expect(freshGeneration.snapshot()).toEqual(freshV2.snapshot());
    await freshGeneration.stop();
  });
});
