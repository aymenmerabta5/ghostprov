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
  }
  async drain(): Promise<void> {
    this.drains++;
    this.state = "stopped";
    this.calls.push(`${this.label}:drain`);
  }
  async stop(): Promise<void> {
    this.stops++;
    this.state = "stopped";
    this.calls.push(`${this.label}:stop`);
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
});
