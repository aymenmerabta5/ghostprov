import { describe, expect, it, spyOn } from "bun:test";
import * as Y from "yjs";
import { Awareness, applyAwarenessUpdate, encodeAwarenessUpdate } from "y-protocols/awareness";
import type { Clock } from "../src/core/ports";
import { createCredentialRotationKey, type BootstrapBinding } from "../src/v2/bootstrap/crypto";
import { parseYSweetEnvelope } from "../src/prism/protocol";
import { V2SupervisorHost } from "../src/v2/supervisor/host";
import { DenoSupervisorPeer } from "../sandbox/v2/supervisor-peer";
import { SidecarCredentialRotator } from "../src/v2/bootstrap/rotation";
import {
  GP_V2_SUPERVISOR_FIELD, GP_V2_SUPERVISOR_PROTOCOL, GP_V2_SUPERVISOR_VERSION, signSupervisorSandboxMessage,
  type SignedSupervisorHostMessage, type SignedSupervisorSandboxMessage,
} from "../src/v2/supervisor/protocol";
import type { SupervisorAwarenessChangeListener, SupervisorAwarenessProviderLike } from "../src/v2/supervisor/provider";
import { encodeSupervisorRetirementReceipt, SUPERVISOR_RETIREMENT_KEY, SUPERVISOR_RETIREMENT_MAP } from "../src/v2/supervisor/retirement-receipt";

const BINDING: BootstrapBinding = Object.freeze({
  projectId: "11111111-1111-4111-8111-111111111111", sandboxSessionId: "offline-reconnect",
  leaderGeneration: 1, buildHash: "a".repeat(64), schemaHash: "b".repeat(64),
  nonce: "offline-supervisor", protocolRevision: "r2", schemaVersion: 2,
});
const CREDENTIAL = parseYSweetEnvelope({ url: "wss://offline.invalid/ws", baseUrl: "https://offline.invalid",
  docId: BINDING.projectId, token: "offline-fixture-token", authorization: "full" });

class ManualClock implements Clock {
  value = 0;
  readonly waits = new Map<object, { deadline: number; finish: () => void }>();
  now() { return this.value; }
  sleep(ms: number, signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) return Promise.reject(signal.reason);
    return new Promise((resolve, reject) => {
      const key = {};
      const cleanup = () => { this.waits.delete(key); signal?.removeEventListener("abort", abort); };
      const abort = () => { cleanup(); reject(signal?.reason); };
      this.waits.set(key, { deadline: this.value + ms, finish: () => { cleanup(); resolve(); } });
      signal?.addEventListener("abort", abort, { once: true });
    });
  }
  waitUntil(deadline: number, signal?: AbortSignal) { return this.sleep(Math.max(0, deadline - this.value), signal); }
  advance(ms: number) {
    this.value += ms;
    for (const wait of [...this.waits.values()]) if (wait.deadline <= this.value) wait.finish();
  }
}

class Bus {
  readonly states = new Map<number, Record<string, unknown>>();
  readonly providers = new Set<Provider>();
  readonly commands: SignedSupervisorHostMessage[] = [];
  dropHost = false;
  dropPeer = false;
  make(doc: Y.Doc, side: "host" | "peer", token: () => string) {
    const provider = new Provider(this, doc.clientID, side, token); this.providers.add(provider); return provider;
  }
  changed(source: Provider) {
    if ((source.side === "host" && this.dropHost) || (source.side === "peer" && this.dropPeer)) return;
    for (const provider of this.providers) {
      if (provider.connected) queueMicrotask(() => provider.emit("change", { added: [], updated: [source.id], removed: [] }));
    }
  }
}

class Provider implements SupervisorAwarenessProviderLike {
  readonly listeners = new Map<string, Set<(...args: any[]) => void>>();
  readonly local: Record<string, unknown> = {};
  connected = false;
  autoSync = true;
  syncs = 0;
  requiredToken: string | null = null;
  readonly awareness;
  constructor(readonly bus: Bus, readonly id: number, readonly side: "host" | "peer", readonly token: () => string) {
    this.awareness = {
      getStates: () => bus.states,
      setLocalStateField: (key: string, value: unknown) => {
        if (value === null) delete this.local[key]; else this.local[key] = value;
        if (side === "host" && key === GP_V2_SUPERVISOR_FIELD && value) {
          if (bus.commands.length >= 64) throw new Error("Fixture command bound exceeded");
          bus.commands.push(value as SignedSupervisorHostMessage);
        }
        if (side === "host" && key === "ghostprovV2SupervisorPublication" && value && this.local[GP_V2_SUPERVISOR_FIELD]) {
          if (bus.commands.length >= 64) throw new Error("Fixture command bound exceeded");
          bus.commands.push(this.local[GP_V2_SUPERVISOR_FIELD] as SignedSupervisorHostMessage);
        }
        bus.states.set(id, this.local); bus.changed(this);
      },
      setLocalState: (value: Record<string, unknown> | null) => {
        if (value === null) bus.states.delete(id); else bus.states.set(id, value);
        bus.changed(this);
      },
      on: (_: "change", fn: SupervisorAwarenessChangeListener) => this.on("change", fn),
      off: (_: "change", fn: SupervisorAwarenessChangeListener) => this.off("change", fn),
    };
  }
  on(event: string, fn: (...args: any[]) => void) {
    const set = this.listeners.get(event) ?? new Set(); set.add(fn); this.listeners.set(event, set);
  }
  off(event: string, fn: (...args: any[]) => void) { this.listeners.get(event)?.delete(fn); }
  emit(event: string, ...args: any[]) { for (const fn of this.listeners.get(event) ?? []) fn(...args); }
  connect() {
    if (this.requiredToken !== null && this.token() !== this.requiredToken) {
      queueMicrotask(() => this.emit("connection-error")); return;
    }
    this.connected = true;
    if (!this.autoSync) return;
    queueMicrotask(() => {
      this.syncs++; this.emit("sync", true);
      this.emit("change", { added: [...this.bus.states.keys()].filter(id => id !== this.id), updated: [], removed: [] });
    });
  }
  disconnect() { this.connected = false; }
  loseConnection() { this.connected = false; this.emit("connection-close", { code: 1005 }); }
  destroy() { this.bus.providers.delete(this); }
  inject(state: SignedSupervisorSandboxMessage) {
    this.bus.states.set(999_999, { [GP_V2_SUPERVISOR_FIELD]: state });
    this.emit("change", { added: [999_999], updated: [], removed: [] });
  }
}

async function until(predicate: () => boolean): Promise<void> {
  for (let i = 0; i < 100; i++) {
    if (predicate()) return;
    // Native WebCrypto completion needs an I/O turn under concurrent tests;
    // 100 immediate callbacks can finish before one native signature returns.
    await new Promise<void>(resolve => setTimeout(resolve, 1));
  }
  throw new Error("Fixture did not reach its expected state");
}

async function fixture(initialReady = true) {
  const bus = new Bus(); const clock = new ManualClock(); const key = createCredentialRotationKey();
  let hostProvider!: Provider; let peerProvider!: Provider; let handled = 0;
  const peer = new DenoSupervisorPeer({ binding: BINDING, instanceId: "offline-instance", credential: {
    ...CREDENTIAL, token: CREDENTIAL.token.reveal(), rotationKey: key,
  }, providerFactory: (doc, credential) => (peerProvider = bus.make(doc, "peer", () => credential().token)),
  handle: async command => { handled++; return command.kind === "shutdown" ? { kind: "shutdown" } : { kind: "stopped", launchNonce: "known-child" }; } });
  const host = new V2SupervisorHost({ binding: BINDING, instanceId: "offline-instance", rotationKey: key, credential: CREDENTIAL,
    connectTimeoutMs: 100, commandTimeoutMs: 100, clock,
    providerFactory: (doc, credential) => (hostProvider = bus.make(doc, "host", () => credential().token.reveal())) });
  if (initialReady) { const initial = host.connect(); await peer.connect(); await initial; }
  return { bus, clock, key, host, peer, hostProvider, peerProvider, handled: () => handled,
    status: () => peerProvider.local[GP_V2_SUPERVISOR_FIELD] as SignedSupervisorSandboxMessage,
    async close() { await host.close(); await peer.close(); expect(clock.waits.size).toBe(0); } };
}

describe("R9 authenticated supervisor reconnect", () => {
  it.each([0, 3])("recovers exact durable shutdown sequence %d after every supervisor Awareness state disappeared", async sequence => {
    const f = await fixture(false);
    let replacement: V2SupervisorHost | null = null;
    const terminal = await signSupervisorSandboxMessage(f.key, {
      protocol: GP_V2_SUPERVISOR_PROTOCOL, version: GP_V2_SUPERVISOR_VERSION, direction: "sandbox", binding: BINDING,
      supervisorInstanceId: "offline-instance", sequence, message: { kind: "shutdown" },
    });
    try {
      replacement = new V2SupervisorHost({ binding: BINDING, instanceId: "offline-instance", rotationKey: f.key,
        credential: CREDENTIAL, recoverExisting: true, connectTimeoutMs: 100, commandTimeoutMs: 100, clock: f.clock,
        providerFactory: (doc, credential) => {
          doc.getMap(SUPERVISOR_RETIREMENT_MAP).set(SUPERVISOR_RETIREMENT_KEY, encodeSupervisorRetirementReceipt(terminal));
          return f.bus.make(doc, "host", () => credential().token.reveal());
        } });
      await replacement.shutdown("durable_terminal");
      await expect(replacement.connect()).rejects.toMatchObject({ code: "closed" });
      expect(f.bus.commands).toHaveLength(0);
      expect(f.handled()).toBe(0);
    } finally { await replacement?.close(); await f.close(); }
  });

  it("prefers durable terminal proof over a concurrently retained startup-ready status", async () => {
    const f = await fixture(false);
    let replacement: V2SupervisorHost | null = null;
    const base = { protocol: GP_V2_SUPERVISOR_PROTOCOL, version: GP_V2_SUPERVISOR_VERSION, direction: "sandbox" as const,
      binding: BINDING, supervisorInstanceId: "offline-instance", sequence: 0 };
    const [ready, terminal] = await Promise.all([
      signSupervisorSandboxMessage(f.key, { ...base, message: { kind: "ready" } }),
      signSupervisorSandboxMessage(f.key, { ...base, message: { kind: "shutdown" } }),
    ]);
    try {
      replacement = new V2SupervisorHost({ binding: BINDING, instanceId: "offline-instance", rotationKey: f.key,
        credential: CREDENTIAL, recoverExisting: true, connectTimeoutMs: 100, commandTimeoutMs: 100, clock: f.clock,
        providerFactory: (doc, credential) => {
          doc.getMap(SUPERVISOR_RETIREMENT_MAP).set(SUPERVISOR_RETIREMENT_KEY, encodeSupervisorRetirementReceipt(terminal));
          const provider = f.bus.make(doc, "host", () => credential().token.reveal());
          provider.inject(ready);
          return provider;
        } });
      let settled = false;
      const shutdown = replacement.shutdown("durable_before_ready").then(() => { settled = true; }, error => { settled = true; return error; });
      await until(() => settled || f.bus.commands.length > 0);
      f.clock.advance(100);
      expect(await shutdown).toBeUndefined();
      expect(f.bus.commands).toHaveLength(0);
    } finally { await replacement?.close(); await f.close(); }
  });

  it("uses an exact retained shutdown only as cleanup proof without publishing another command", async () => {
    const f = await fixture();
    let replacement: V2SupervisorHost | null = null;
    try {
      await f.host.shutdown("offline_done");
      const retired = f.status();
      await f.host.close(); await f.peer.close();
      replacement = new V2SupervisorHost({ binding: BINDING, instanceId: "offline-instance", rotationKey: f.key,
        credential: CREDENTIAL, recoverExisting: true, connectTimeoutMs: 100, commandTimeoutMs: 100, clock: f.clock,
        providerFactory: (doc, credential) => {
          const provider = f.bus.make(doc, "host", () => credential().token.reveal());
          provider.inject(retired);
          return provider;
        } });
      let settled = false;
      const shutdown = replacement.shutdown("recover_terminal").then(() => { settled = true; }, error => { settled = true; return error; });
      await until(() => settled);
      expect(await shutdown).toBeUndefined();
      await expect(replacement.connect()).rejects.toMatchObject({ code: "closed" });
      await expect(replacement.stop({ launchNonces: ["known-child"], reason: "retired" })).rejects.toMatchObject({ code: "closed" });
      await replacement.shutdown("already_done");
      expect(f.bus.commands.map(command => command.message.kind)).toEqual(["shutdown"]);
      expect(f.handled()).toBe(1);
    } finally { await replacement?.close(); await f.close(); }
  });

  it.each([
    ["key", "awareness"], ["binding", "awareness"], ["instance", "awareness"], ["ordinary_host", "awareness"],
    ["key", "document"], ["ordinary_host", "document"],
  ] as const)("rejects retained shutdown with invalid %s authority from %s", async (invalid, surface) => {
    const f = await fixture(false);
    let replacement: V2SupervisorHost | null = null;
    let provider!: Provider;
    const terminal = await signSupervisorSandboxMessage(invalid === "key" ? createCredentialRotationKey() : f.key, {
      protocol: GP_V2_SUPERVISOR_PROTOCOL, version: GP_V2_SUPERVISOR_VERSION, direction: "sandbox",
      binding: invalid === "binding" ? { ...BINDING, nonce: "different-supervisor" } : BINDING,
      supervisorInstanceId: invalid === "instance" ? "different-instance" : "offline-instance",
      sequence: 1, message: { kind: "shutdown" },
    });
    try {
      replacement = new V2SupervisorHost({ binding: BINDING, instanceId: "offline-instance", rotationKey: f.key,
        credential: CREDENTIAL, recoverExisting: invalid !== "ordinary_host", connectTimeoutMs: 100, commandTimeoutMs: 100, clock: f.clock,
        providerFactory: (doc, credential) => {
          provider = f.bus.make(doc, "host", () => credential().token.reveal());
          if (surface === "document") doc.getMap(SUPERVISOR_RETIREMENT_MAP).set(SUPERVISOR_RETIREMENT_KEY, encodeSupervisorRetirementReceipt(terminal));
          else provider.inject(terminal);
          return provider;
        } });
      const shutdown = replacement.shutdown("invalid_terminal").catch(error => error);
      await until(() => provider.syncs === 1);
      f.clock.advance(100);
      expect(await shutdown).toMatchObject({ code: "timeout", phase: "bootstrap" });
      expect(f.bus.commands).toHaveLength(0);
    } finally { await replacement?.close(); await f.close(); }
  });

  it("requires synchronized transport before consuming retained shutdown proof", async () => {
    const f = await fixture(false);
    let replacement: V2SupervisorHost | null = null;
    let provider!: Provider;
    const terminal = await signSupervisorSandboxMessage(f.key, {
      protocol: GP_V2_SUPERVISOR_PROTOCOL, version: GP_V2_SUPERVISOR_VERSION, direction: "sandbox", binding: BINDING,
      supervisorInstanceId: "offline-instance", sequence: 1, message: { kind: "shutdown" },
    });
    let verified = 0;
    const verify = crypto.subtle.verify.bind(crypto.subtle);
    const observer = spyOn(crypto.subtle, "verify").mockImplementation(async (...args) => {
      const result = await verify(...args); verified++; return result;
    });
    try {
      replacement = new V2SupervisorHost({ binding: BINDING, instanceId: "offline-instance", rotationKey: f.key,
        credential: CREDENTIAL, recoverExisting: true, connectTimeoutMs: 100, commandTimeoutMs: 100, clock: f.clock,
        providerFactory: (doc, credential) => {
          provider = f.bus.make(doc, "host", () => credential().token.reveal());
          provider.autoSync = false;
          provider.inject(terminal);
          return provider;
        } });
      let settled = false;
      const shutdown = replacement.shutdown("retained_terminal").then(() => { settled = true; });
      await until(() => verified > 0);
      expect(settled).toBe(false);
      expect(f.bus.commands).toHaveLength(0);
      provider.emit("sync", true);
      await shutdown;
      expect(settled).toBe(true);
      expect(f.bus.commands).toHaveLength(0);
    } finally { observer.mockRestore(); await replacement?.close(); await f.close(); }
  });

  it("recovers a journal-authorized new host from the exact peer's retained sequence using a fresh challenge", async () => {
    const f = await fixture();
    let replacement: V2SupervisorHost | null = null;
    try {
      await f.host.stop({ launchNonces: ["known-child"], reason: "offline" });
      expect(f.status().sequence).toBe(1);
      await f.host.close();
      replacement = new V2SupervisorHost({ binding: BINDING, instanceId: "offline-instance", rotationKey: f.key,
        credential: CREDENTIAL, recoverExisting: true, connectTimeoutMs: 100, commandTimeoutMs: 100, clock: f.clock,
        providerFactory: (doc, credential) => f.bus.make(doc, "host", () => credential().token.reveal()) });
      await replacement.connect();
      expect(f.bus.commands.map(command => [command.sequence, command.message.kind])).toEqual([[1, "stop"], [2, "rotate"]]);
      expect(f.handled()).toBe(1);
      await replacement.stop({ launchNonces: ["known-child"], reason: "offline" });
      expect(f.bus.commands.at(-1)?.sequence).toBe(3);
    } finally { await replacement?.close(); await f.close(); }
  });

  it("does not treat an authenticated retained reply as fresh recovery liveness", async () => {
    const f = await fixture();
    let replacement: V2SupervisorHost | null = null;
    try {
      await f.host.stop({ launchNonces: ["known-child"], reason: "offline" });
      await f.host.close(); f.bus.dropHost = true;
      replacement = new V2SupervisorHost({ binding: BINDING, instanceId: "offline-instance", rotationKey: f.key,
        credential: CREDENTIAL, recoverExisting: true, connectTimeoutMs: 100, commandTimeoutMs: 100, clock: f.clock,
        providerFactory: (doc, credential) => f.bus.make(doc, "host", () => credential().token.reveal()) });
      const recovery = replacement.connect().catch(error => error);
      await until(() => f.bus.commands.length === 2);
      f.clock.advance(100);
      expect(await recovery).toMatchObject({ code: "timeout", phase: "challenge" });
      expect(f.handled()).toBe(1);
    } finally { await replacement?.close(); await f.close(); }
  });

  it("does not let ordinary new hosts adopt an existing peer's command sequence", async () => {
    const f = await fixture();
    let replacement: V2SupervisorHost | null = null;
    try {
      await f.host.stop({ launchNonces: ["known-child"], reason: "offline" });
      await f.host.close();
      replacement = new V2SupervisorHost({ binding: BINDING, instanceId: "offline-instance", rotationKey: f.key,
        credential: CREDENTIAL, connectTimeoutMs: 100, commandTimeoutMs: 100, clock: f.clock,
        providerFactory: (doc, credential) => f.bus.make(doc, "host", () => credential().token.reveal()) });
      const recovery = replacement.connect().catch(error => error);
      await new Promise<void>(resolve => setImmediate(resolve)); f.clock.advance(100);
      expect(await recovery).toMatchObject({ code: "timeout", phase: "bootstrap" });
      expect(f.bus.commands).toHaveLength(1);
    } finally { await replacement?.close(); await f.close(); }
  });
  it("recovers a missed fresh challenge notification using the same signed command", async () => {
    const f = await fixture();
    try {
      f.hostProvider.loseConnection(); f.bus.dropHost = true;
      const reconnect = f.host.connect().catch(error => error);
      await until(() => f.bus.commands.length === 1);
      const first = JSON.stringify(f.bus.commands[0]);
      f.bus.dropHost = false; f.clock.advance(25);
      await until(() => f.bus.commands.length === 2);
      expect(await reconnect).toBeUndefined();
      expect(JSON.stringify(f.bus.commands[1])).toBe(first);
      expect(f.handled()).toBe(0); expect(f.clock.waits.size).toBe(0);
    } finally { await f.close(); }
  });

  it("recovers a missed fresh reply through the peer's exact-command cache without rotating twice", async () => {
    const f = await fixture();
    const accept = spyOn(SidecarCredentialRotator.prototype, "accept");
    try {
      f.hostProvider.loseConnection(); f.bus.dropPeer = true;
      const reconnect = f.host.connect().catch(error => error);
      await until(() => f.status().sequence === 1);
      f.bus.dropPeer = false; f.clock.advance(25);
      await until(() => f.bus.commands.length === 2);
      expect(await reconnect).toBeUndefined();
      expect(accept).toHaveBeenCalledTimes(1);
      expect(f.bus.commands[1]).toEqual(f.bus.commands[0]);
      expect(f.clock.waits.size).toBe(0);
    } finally { accept.mockRestore(); await f.close(); }
  });

  it("bounds challenge publications and cancels them at the original deadline", async () => {
    const f = await fixture();
    try {
      f.hostProvider.loseConnection(); f.bus.dropHost = true;
      const reconnect = f.host.connect().catch(error => error);
      await until(() => f.bus.commands.length === 1);
      for (let attempt = 2; attempt <= 4; attempt++) {
        f.clock.advance(25); await until(() => f.bus.commands.length === attempt);
      }
      f.clock.advance(25);
      expect(await reconnect).toMatchObject({ code: "timeout", phase: "challenge" });
      expect(f.bus.commands).toHaveLength(4); expect(f.handled()).toBe(0);
      f.clock.advance(1000); await new Promise<void>(resolve => setImmediate(resolve));
      expect(f.bus.commands).toHaveLength(4); expect(f.clock.waits.size).toBe(0);
    } finally { await f.close(); }
  });

  it("uses a bounded publication marker because Yjs change listeners ignore identical state renewals", () => {
    const localDoc = new Y.Doc(); const remoteDoc = new Y.Doc();
    const local = new Awareness(localDoc); const remote = new Awareness(remoteDoc);
    const deliver = () => applyAwarenessUpdate(remote, encodeAwarenessUpdate(local, [local.clientID]), null);
    try {
      const command = { sequence: 1, signed: "immutable-offline-fixture" };
      local.setLocalStateField(GP_V2_SUPERVISOR_FIELD, command); deliver();
      let changes = 0; remote.on("change", () => changes++);
      local.setLocalStateField(GP_V2_SUPERVISOR_FIELD, command); deliver();
      expect(changes).toBe(0);
      local.setLocalStateField("ghostprovV2SupervisorPublication", { sequence: 1, attempt: 1 }); deliver();
      expect(changes).toBe(1);
      expect(remote.getStates().get(local.clientID)?.[GP_V2_SUPERVISOR_FIELD]).toEqual(command);
    } finally { local.destroy(); remote.destroy(); localDoc.destroy(); remoteDoc.destroy(); }
  });
  it("requires a synchronized transport before accepting the initial signed ready", async () => {
    const f = await fixture(false);
    let verified = 0;
    const verify = crypto.subtle.verify.bind(crypto.subtle);
    const observer = spyOn(crypto.subtle, "verify").mockImplementation(async (...args) => {
      const result = await verify(...args); verified++; return result;
    });
    try {
      f.hostProvider.autoSync = false;
      let settled = false;
      const initial = f.host.connect().then(() => { settled = true; });
      await f.peer.connect();
      await until(() => verified > 0);
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(settled).toBe(false);
      expect(f.bus.commands).toHaveLength(0);
      f.hostProvider.emit("sync", true);
      await initial;
      expect(settled).toBe(true);
    } finally { observer.mockRestore(); await f.close(); }
  });

  it("keeps one initial handshake across a transient disconnect and rescans ready only after resync", async () => {
    const f = await fixture(false);
    try {
      let settled = false;
      const initial = f.host.connect().then(() => { settled = true; return null; }, error => { settled = true; return error; });
      await until(() => f.hostProvider.syncs === 1);
      f.hostProvider.loseConnection();
      await f.peer.connect();
      f.hostProvider.inject(f.status());
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(settled).toBe(false);
      expect(f.clock.waits.size).toBe(1);
      expect(f.bus.commands).toHaveLength(0);
      // Simulates the existing provider's reconnect; the host never resubmits a job.
      f.hostProvider.connect();
      expect(await initial).toBeNull();
      expect(f.bus.commands).toHaveLength(0);
    } finally { await f.close(); }
  });

  it("retains the original initial-handshake deadline across repeated transport errors", async () => {
    const f = await fixture(false);
    try {
      f.hostProvider.autoSync = false;
      const initial = f.host.connect().catch(error => error);
      f.clock.advance(40);
      f.hostProvider.emit("connection-error");
      f.clock.advance(40);
      f.hostProvider.loseConnection();
      expect(f.clock.waits.size).toBe(1);
      // Delayed timer callbacks must not allow a late ready to restore liveness.
      f.clock.value = 100;
      await f.peer.connect();
      f.hostProvider.emit("sync", true);
      expect(await initial).toMatchObject({ code: "timeout", phase: "bootstrap" });
      expect(f.clock.waits.size).toBe(0);
      expect(f.bus.commands).toHaveLength(0);
    } finally { await f.close(); }
  });

  it("reconnects after prior commands with a fresh rotation challenge and retains the peer", async () => {
    const f = await fixture();
    try {
      await f.host.rotateCredential(CREDENTIAL, "prior-rotation");
      for (let i = 0; i < 3; i++) {
        f.hostProvider.loseConnection();
        await Promise.all([f.host.connect(), f.host.connect()]);
        expect(f.status().message.kind).toBe("credential_rotated");
        expect(f.bus.commands.at(-1)?.message).toMatchObject({ kind: "rotate", rotationId: expect.stringMatching(/^reconnect-/) });
        await expect(f.host.stop({ launchNonces: ["known-child"], reason: "proof" })).resolves.toBe("known-child");
      }
      expect(f.handled()).toBe(3);
      expect(f.hostProvider.syncs).toBe(4);
      expect(f.peerProvider.connected).toBe(true);
      expect(f.clock.waits.size).toBe(0);
    } finally { await f.close(); }
  });

  it("requires fresh proof even if a signed startup-ready or old command reply is replayed", async () => {
    const f = await fixture();
    try {
      const ready = f.status();
      await f.host.rotateCredential(CREDENTIAL, "prior-rotation");
      const previous = f.status();
      f.hostProvider.loseConnection(); f.bus.dropPeer = true;
      const reconnect = f.host.connect().catch(error => error);
      await until(() => f.bus.commands.length === 2);
      f.hostProvider.inject(ready); f.hostProvider.inject(previous);
      f.clock.advance(100);
      expect(await reconnect).toMatchObject({ code: "timeout", phase: "challenge" });
      expect(f.handled()).toBe(0);
    } finally { await f.close(); }
  });

  it("rejects reconnect proof with a different key, instance, binding, or rotation identity", async () => {
    const f = await fixture();
    try {
      f.hostProvider.loseConnection(); f.bus.dropPeer = true;
      const reconnect = f.host.connect().catch(error => error);
      await until(() => f.bus.commands.length === 1);
      const command = f.bus.commands[0]!;
      const message = command.message;
      if (message.kind !== "rotate") throw new Error("Expected challenge");
      const base = { ...command, direction: "sandbox" as const, message: { kind: "credential_rotated" as const, rotationId: message.rotationId } };
      f.hostProvider.inject(await signSupervisorSandboxMessage(createCredentialRotationKey(), base));
      f.hostProvider.inject(await signSupervisorSandboxMessage(f.key, { ...base, supervisorInstanceId: "other-instance" }));
      f.hostProvider.inject(await signSupervisorSandboxMessage(f.key, { ...base, binding: { ...base.binding, nonce: "other-nonce" } }));
      f.hostProvider.inject(await signSupervisorSandboxMessage(f.key, { ...base, message: { kind: "credential_rotated", rotationId: "wrong-rotation" } }));
      expect(await reconnect).toMatchObject({ code: "protocol", phase: "challenge" });
      expect(f.handled()).toBe(0);
    } finally { await f.close(); }
  });

  it("never replays a command whose delivery was interrupted before the peer consumed it", async () => {
    const f = await fixture();
    try {
      f.bus.dropHost = true;
      const command = f.host.stop({ launchNonces: ["known-child"], reason: "lost-command" }).catch(error => error);
      await until(() => f.bus.commands.length === 1);
      f.hostProvider.loseConnection();
      expect(await command).toMatchObject({ code: "connect" });
      f.bus.dropHost = false;
      const reconnect = f.host.connect().catch(error => error);
      await until(() => f.hostProvider.syncs === 2);
      f.clock.advance(100);
      expect(await reconnect).toMatchObject({ code: "timeout", phase: "sequence" });
      expect(f.bus.commands).toHaveLength(1); expect(f.handled()).toBe(0);
    } finally { await f.close(); }
  });

  it("uses a retained signed reply only to reconcile consumption, then proves liveness without command replay", async () => {
    const f = await fixture();
    try {
      f.bus.dropPeer = true;
      const command = f.host.stop({ launchNonces: ["known-child"], reason: "lost-reply" }).catch(error => error);
      await until(() => f.status().sequence === 1);
      f.hostProvider.loseConnection();
      expect(await command).toMatchObject({ code: "connect" });
      f.bus.dropPeer = false;
      await f.host.connect();
      expect(f.bus.commands.map(value => value.message.kind)).toEqual(["stop", "rotate"]);
      expect(f.handled()).toBe(1);
      expect(f.status().sequence).toBe(2);
    } finally { await f.close(); }
  });

  it("uses newly minted authentication before attempting a disconnected supervisor rotation", async () => {
    const f = await fixture();
    try {
      await f.host.rotateCredential(CREDENTIAL, "prior-rotation");
      const next = parseYSweetEnvelope({ ...CREDENTIAL, token: "offline-new-auth" });
      f.hostProvider.requiredToken = next.token.reveal(); f.hostProvider.loseConnection();
      await f.host.rotateCredential(next, "next-rotation");
      expect(f.hostProvider.token()).toBe(next.token.reveal());
      expect(f.status().message).toEqual({ kind: "credential_rotated", rotationId: "next-rotation" });
    } finally { await f.close(); }
  });

  it("cancels reconnect and releases both waits without late publication or readiness", async () => {
    const f = await fixture();
    try {
      f.hostProvider.loseConnection(); f.bus.dropPeer = true;
      const abort = new AbortController(); const reconnect = f.host.connect(abort.signal).catch(error => error);
      await until(() => f.bus.commands.length === 1);
      abort.abort(new DOMException("cancelled", "AbortError"));
      expect(await reconnect).toMatchObject({ name: "AbortError" });
      expect(f.clock.waits.size).toBe(0);
      const publications = f.bus.commands.length;
      f.clock.advance(1_000); await new Promise<void>(resolve => setImmediate(resolve));
      expect(f.bus.commands).toHaveLength(publications);
      expect(f.hostProvider.local[GP_V2_SUPERVISOR_FIELD]).toBeUndefined();
    } finally { await f.close(); }
  });

  it("joins a reconnect during close and fences queued lifecycle work", async () => {
    const f = await fixture();
    f.hostProvider.loseConnection(); f.bus.dropPeer = true;
    const reconnect = f.host.connect().catch(error => error);
    const command = f.host.stop({ launchNonces: ["known-child"], reason: "queued" }).catch(error => error);
    await until(() => f.bus.commands.length === 1);
    await f.host.close();
    expect(await reconnect).toMatchObject({ code: "closed" });
    expect(await command).toMatchObject({ code: "closed" });
    expect(f.handled()).toBe(0); expect(f.bus.commands).toHaveLength(1);
    await f.peer.close(); expect(f.clock.waits.size).toBe(0);
  });

  it("rejects authenticated replies beyond the deadline even when timer callbacks are delayed", async () => {
    for (const reconnecting of [false, true]) {
      const f = await fixture();
      try {
        f.bus.dropPeer = true;
        if (reconnecting) f.hostProvider.loseConnection();
        const operation = (reconnecting ? f.host.connect()
          : f.host.stop({ launchNonces: ["known-child"], reason: "deadline" })).catch(error => error);
        if (reconnecting) f.clock.value = 50;
        await until(() => f.status().sequence === 1);
        // Advance the clock without executing its queued timer callbacks.
        f.clock.value = 101;
        f.hostProvider.inject(f.status());
        expect(await operation).toMatchObject({ code: "timeout", phase: reconnecting ? "challenge" : "command" });
      } finally { await f.close(); }
    }
  });
});
