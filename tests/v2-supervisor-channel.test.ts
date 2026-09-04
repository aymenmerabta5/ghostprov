import { describe, expect, it } from "bun:test";
import * as Y from "yjs";
import type { BootstrapBinding, SealedBootstrapCredential } from "../src/v2/bootstrap/crypto";
import { createCredentialRotationKey } from "../src/v2/bootstrap/crypto";
import { parseYSweetEnvelope } from "../src/prism/protocol";
import type {
  SupervisorAwarenessChangeListener,
  SupervisorAwarenessProviderLike,
} from "../src/v2/supervisor/provider";
import { V2SupervisorHost, SUPERVISOR_MAX_PENDING_OPERATIONS } from "../src/v2/supervisor/host";
import { DenoSupervisorPeer } from "../sandbox/v2/supervisor-peer";
import type { SupervisorCommand, SupervisorStatus } from "../src/v2/supervisor/protocol";

const BINDING: BootstrapBinding = Object.freeze({
  projectId: "11111111-1111-4111-8111-111111111111",
  sandboxSessionId: "sandbox-session-1",
  leaderGeneration: 4,
  buildHash: "a".repeat(64),
  schemaHash: "b".repeat(64),
  nonce: "supervisor-nonce-1",
  protocolRevision: "r2",
  schemaVersion: 2,
});

const CREDENTIAL = parseYSweetEnvelope({
  url: "wss://y.test/socket",
  baseUrl: "https://y.test",
  docId: BINDING.projectId,
  token: "y-token",
  authorization: "Bearer y-token",
});

class FakeBus {
  readonly states = new Map<number, Record<string, unknown>>();
  readonly providers = new Set<FakeProvider>();

  provider(doc: Y.Doc): FakeProvider {
    const provider = new FakeProvider(this, doc.clientID);
    this.providers.add(provider);
    return provider;
  }

  changed(source: FakeProvider, removed = false): void {
    for (const provider of this.providers) {
      queueMicrotask(() => provider.emitChange({
        added: removed ? [] : [source.clientId],
        updated: removed ? [] : [source.clientId],
        removed: removed ? [source.clientId] : [],
      }));
    }
  }
}

class FakeProvider implements SupervisorAwarenessProviderLike {
  readonly listeners = new Map<string, Set<(...args: any[]) => void>>();
  readonly local: Record<string, unknown> = {};
  readonly awareness;

  constructor(readonly bus: FakeBus, readonly clientId: number) {
    this.awareness = {
      getStates: () => this.bus.states,
      setLocalState: (value: Record<string, unknown> | null) => {
        if (value === null) {
          this.bus.states.delete(this.clientId);
          this.bus.changed(this, true);
          return;
        }
        for (const key of Object.keys(this.local)) delete this.local[key];
        Object.assign(this.local, value);
        this.bus.states.set(this.clientId, this.local);
        this.bus.changed(this);
      },
      setLocalStateField: (field: string, value: unknown) => {
        if (value === null) delete this.local[field];
        else this.local[field] = value;
        this.bus.states.set(this.clientId, this.local);
        this.bus.changed(this);
      },
      on: (_event: "change", listener: SupervisorAwarenessChangeListener) => this.on("change", listener),
      off: (_event: "change", listener: SupervisorAwarenessChangeListener) => this.off("change", listener),
    };
  }

  on(event: string, listener: (...args: any[]) => void): void {
    const listeners = this.listeners.get(event) ?? new Set();
    listeners.add(listener);
    this.listeners.set(event, listeners);
  }

  off(event: string, listener: (...args: any[]) => void): void {
    this.listeners.get(event)?.delete(listener);
  }

  connect(): void {
    queueMicrotask(() => this.emit("sync", true));
  }

  disconnect(): void {}
  destroy(): void { this.bus.providers.delete(this); }

  emitChange(change: Parameters<SupervisorAwarenessChangeListener>[0]): void {
    this.emit("change", change);
  }

  emit(event: string, ...args: any[]): void {
    for (const listener of this.listeners.get(event) ?? []) listener(...args);
  }
}

async function channelFixture(handle: (command: SupervisorCommand) => Promise<SupervisorStatus>) {
  const bus = new FakeBus();
  const rotationKey = createCredentialRotationKey();
  const peer = new DenoSupervisorPeer({
    credential: { ...CREDENTIAL, token: CREDENTIAL.token.reveal(), rotationKey }, binding: BINDING,
    instanceId: "supervisor-instance-1", providerFactory: doc => bus.provider(doc), handle,
  });
  const host = new V2SupervisorHost({
    binding: BINDING, instanceId: "supervisor-instance-1", rotationKey, credential: CREDENTIAL,
    connectTimeoutMs: 500, commandTimeoutMs: 500, providerFactory: doc => bus.provider(doc),
  });
  const connecting = host.connect();
  await peer.connect();
  await connecting;
  return { host, peer };
}

describe("V2 supervisor Awareness channel", () => {
  it("targets one supervisor and returns authenticated launch, credential, and stop proof", async () => {
    const bus = new FakeBus();
    const rotationKey = createCredentialRotationKey();
    const sidecar = { ...BINDING, nonce: "sidecar-launch-1" };
    const handled: string[] = [];
    const peer = new DenoSupervisorPeer({
      credential: {
        ...CREDENTIAL,
        token: CREDENTIAL.token.reveal(),
        rotationKey,
      },
      binding: BINDING,
      instanceId: "supervisor-instance-1",
      providerFactory: doc => bus.provider(doc),
      handle: async command => {
        handled.push(command.kind);
        if (command.kind === "launch") {
          return {
            kind: "launch_hello",
            sidecarBinding: command.sidecarBinding,
            encodedHello: "abc_DEF-123",
            process: { pid: 123, startTick: "456", processGroup: 123, session: 123 },
          };
        }
        if (command.kind === "credential") {
          return { kind: "credential_written", launchNonce: command.sidecarBinding.nonce };
        }
        if (command.kind === "stop") return { kind: "stopped", launchNonce: sidecar.nonce };
        return { kind: "shutdown" };
      },
    });
    const host = new V2SupervisorHost({
      binding: BINDING,
      instanceId: "supervisor-instance-1",
      rotationKey,
      credential: CREDENTIAL,
      connectTimeoutMs: 100,
      commandTimeoutMs: 100,
      providerFactory: doc => bus.provider(doc),
    });

    const connecting = host.connect();
    await peer.connect();
    await connecting;
    await expect(host.launch({
      sidecarBinding: sidecar,
      entrypointPath: ".ghostprov-v2/revisions/r9/a/b/sidecar.bundle.js",
      staticConfig: "{}",
    })).resolves.toEqual({
      encodedHello: "abc_DEF-123",
      process: { pid: 123, startTick: "456", processGroup: 123, session: 123 },
    });
    await expect(host.deliverCredential({
      sidecarBinding: sidecar,
      sealed: { version: 1 } as SealedBootstrapCredential,
    })).resolves.toBeUndefined();
    await expect(host.stop({ launchNonces: [sidecar.nonce], reason: "recovery" }))
      .resolves.toBe(sidecar.nonce);
    const rotated = parseYSweetEnvelope({
      url: "wss://y.test/socket",
      baseUrl: "https://y.test",
      docId: BINDING.projectId,
      token: "y-token-2",
      authorization: "full",
    });
    await expect(host.rotateCredential(rotated, "supervisor-rotation-1")).resolves.toBeUndefined();
    expect(handled).toEqual(["launch", "credential", "stop"]);
    await host.close();
    await peer.close();
  });

  it("does not accept a same-project supervisor with another command key", async () => {
    const bus = new FakeBus();
    const peer = new DenoSupervisorPeer({
      credential: {
        ...CREDENTIAL,
        token: CREDENTIAL.token.reveal(),
        rotationKey: createCredentialRotationKey(),
      },
      binding: BINDING,
      instanceId: "supervisor-instance-1",
      providerFactory: doc => bus.provider(doc),
      handle: async () => ({ kind: "ready" }),
    });
    const host = new V2SupervisorHost({
      binding: BINDING,
      instanceId: "supervisor-instance-1",
      rotationKey: createCredentialRotationKey(),
      credential: CREDENTIAL,
      connectTimeoutMs: 20,
      commandTimeoutMs: 20,
      providerFactory: doc => bus.provider(doc),
    });
    const connecting = host.connect();
    await peer.connect();
    await expect(connecting).rejects.toMatchObject({ name: "V2SupervisorError", code: "timeout" });
    await host.close();
    await peer.close();
  });

  it("caps queued lifecycle operations and preserves immutable command inputs", async () => {
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const nonces: string[] = [];
    const { host, peer } = await channelFixture(async command => {
      if (command.kind !== "stop") throw new Error("unexpected command");
      nonces.push(command.launchNonces[0]!);
      if (nonces.length === 1) { entered.resolve(); await release.promise; }
      return { kind: "stopped", launchNonce: command.launchNonces[0]! };
    });
    const pending = Array.from({ length: SUPERVISOR_MAX_PENDING_OPERATIONS }, (_, index) => {
      const launchNonces = [`launch-${index}`];
      const result = host.stop({ launchNonces, reason: "test" });
      launchNonces[0] = "mutated";
      return result;
    });
    await entered.promise;
    await expect(host.stop({ launchNonces: ["overflow"], reason: "test" })).rejects.toMatchObject({ code: "protocol" });
    release.resolve();
    expect(await Promise.all(pending)).toEqual(["launch-0", "launch-1", "launch-2", "launch-3"]);
    expect(nonces).toEqual(["launch-0", "launch-1", "launch-2", "launch-3"]);
    await host.close();
    await peer.close();
  });

  it("does not consume a command sequence for an aborted queued operation", async () => {
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const handled: string[] = [];
    const { host, peer } = await channelFixture(async command => {
      if (command.kind !== "stop") throw new Error("unexpected command");
      handled.push(command.reason);
      if (handled.length === 1) { entered.resolve(); await release.promise; }
      return { kind: "stopped", launchNonce: command.launchNonces[0]! };
    });
    const first = host.stop({ launchNonces: ["launch-1"], reason: "first" });
    await entered.promise;
    const controller = new AbortController();
    const aborted = host.stop({ launchNonces: ["launch-1"], reason: "aborted" }, controller.signal).then(() => null, error => error);
    controller.abort(new DOMException("test cancellation", "AbortError"));
    const last = host.stop({ launchNonces: ["launch-1"], reason: "last" });
    release.resolve();
    await first;
    expect(await aborted).toMatchObject({ name: "AbortError" });
    await expect(last).resolves.toBe("launch-1");
    expect(handled).toEqual(["first", "last"]);
    await host.close();
    await peer.close();
  });

  it("closes bounded queued operations without sending them after shutdown", async () => {
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    let handled = 0;
    const { host, peer } = await channelFixture(async () => {
      handled++;
      entered.resolve();
      await release.promise;
      return { kind: "stopped", launchNonce: "launch-1" };
    });
    const first = host.stop({ launchNonces: ["launch-1"], reason: "first" }).catch(error => error);
    await entered.promise;
    const queued = host.stop({ launchNonces: ["launch-1"], reason: "queued" }).catch(error => error);
    await host.close();
    expect(await first).toMatchObject({ code: "closed" });
    expect(await queued).toMatchObject({ code: "closed" });
    expect(handled).toBe(1);
    release.resolve();
    await peer.close();
  });
});
