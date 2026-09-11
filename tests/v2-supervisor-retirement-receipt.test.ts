import { describe, expect, it } from "bun:test";
import * as Y from "yjs";
import { DenoSupervisorPeer } from "../sandbox/v2/r10-supervisor-peer";
import { finalizeR10SupervisorRetirement, publishR10SupervisorRetirement,
  finishR10SupervisorProcess, waitForR10SupervisorChildCleanup } from "../sandbox/v2/r10-supervisor-receipt";
import { createCredentialRotationKey, type BootstrapBinding } from "../src/v2/bootstrap/crypto";
import { DenoSidecarOwnershipLease, type SidecarOwnershipBackend } from "../sandbox/v2/ownership-lease";
import {
  GP_V2_SUPERVISOR_FIELD, GP_V2_SUPERVISOR_PROTOCOL, GP_V2_SUPERVISOR_VERSION,
  signSupervisorHostMessage, signSupervisorSandboxMessage,
  type SignedSupervisorSandboxMessage, type SupervisorStatus,
} from "../src/v2/supervisor/protocol";
import type { SupervisorAwarenessChangeListener, SupervisorAwarenessProviderLike } from "../src/v2/supervisor/provider";
import {
  SUPERVISOR_RETIREMENT_KEY, SUPERVISOR_RETIREMENT_MAP, SUPERVISOR_RETIREMENT_MAX_BYTES,
  encodeSupervisorRetirementReceipt,
  parseSupervisorRetirementReceipt, readSupervisorRetirementReceipt, validateSupervisorRetirementReceipt,
} from "../src/v2/supervisor/retirement-receipt";
import { proveSupervisorRetirementReadback } from "../src/v2/supervisor/retirement-store";

const BINDING: BootstrapBinding = {
  projectId: "11111111-1111-4111-8111-111111111111", sandboxSessionId: "offline-session",
  leaderGeneration: 1, buildHash: "a".repeat(64), schemaHash: "b".repeat(64),
  nonce: "offline-supervisor", protocolRevision: "r2", schemaVersion: 2,
};
const INSTANCE = "offline-instance";
const CREDENTIAL = {
  url: "wss://offline.invalid/socket", baseUrl: "https://offline.invalid", docId: BINDING.projectId,
  token: "offline-token", authorization: "full",
};

async function until(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt++) {
    if (predicate()) return;
    await new Promise<void>(resolve => setTimeout(resolve, 1));
  }
  throw new Error("Offline retirement fixture did not reach the expected state");
}

class RemoteDocument {
  readonly doc = new Y.Doc();
  readonly providers = new Set<Provider>();
  readonly states = new Map<number, Record<string, unknown>>();
  allowWrites = true;
  allowReads = true;
  created = 0;

  provider(doc: Y.Doc): Provider {
    const provider = new Provider(this, doc);
    this.providers.add(provider);
    this.created++;
    return provider;
  }

  flush(source: Provider): void {
    if (source.connected && this.allowWrites) {
      Y.applyUpdate(this.doc, Y.encodeStateAsUpdate(source.doc), this);
      for (const provider of this.providers) {
        if (provider.connected && this.allowReads) Y.applyUpdate(provider.doc, Y.encodeStateAsUpdate(this.doc), this);
      }
    }
  }

  changed(id: number): void {
    for (const provider of this.providers) if (provider.connected) {
      queueMicrotask(() => provider.emit("change", { added: [], updated: [id], removed: [] }));
    }
  }

  close(): void { for (const provider of [...this.providers]) provider.destroy(); this.doc.destroy(); }
}

class Provider implements SupervisorAwarenessProviderLike {
  readonly listeners = new Map<string, Set<(...args: any[]) => void>>();
  readonly local: Record<string, unknown> = {};
  readonly awareness;
  connected = false;
  syncGate: Promise<void> | null = null;
  readonly onUpdate = (_update: Uint8Array, origin: unknown) => {
    if (origin !== this.remote) this.remote.flush(this);
  };

  constructor(readonly remote: RemoteDocument, readonly doc: Y.Doc) {
    this.doc.on("update", this.onUpdate);
    this.awareness = {
      getStates: () => remote.states,
      setLocalStateField: (key: string, value: unknown) => {
        this.local[key] = value; remote.states.set(doc.clientID, this.local); remote.changed(doc.clientID);
      },
      setLocalState: (value: Record<string, unknown> | null) => {
        if (value === null) remote.states.delete(doc.clientID); else remote.states.set(doc.clientID, value);
        remote.changed(doc.clientID);
      },
      on: (_event: "change", fn: SupervisorAwarenessChangeListener) => this.on("change", fn),
      off: (_event: "change", fn: SupervisorAwarenessChangeListener) => this.off("change", fn),
    };
  }
  on(event: string, fn: (...args: any[]) => void): void {
    const set = this.listeners.get(event) ?? new Set(); set.add(fn); this.listeners.set(event, set);
  }
  off(event: string, fn: (...args: any[]) => void): void { this.listeners.get(event)?.delete(fn); }
  emit(event: string, ...args: any[]): void { for (const fn of this.listeners.get(event) ?? []) fn(...args); }
  connect(): void {
    this.connected = true;
    this.remote.flush(this);
    if (this.remote.allowReads) Y.applyUpdate(this.doc, Y.encodeStateAsUpdate(this.remote.doc), this.remote);
    if (this.syncGate) void this.syncGate.then(() => this.emit("sync", true));
    else queueMicrotask(() => this.emit("sync", true));
  }
  disconnect(): void { this.connected = false; }
  destroy(): void {
    this.disconnect(); this.doc.off("update", this.onUpdate); this.listeners.clear(); this.remote.providers.delete(this);
  }
}

class Ownership {
  owned = true;
  async assertOwned(): Promise<void> { if (!this.owned) throw new Error("publication ownership lost"); }
  async withOwnership<T>(operation: () => Promise<T>): Promise<T> {
    await this.assertOwned();
    try { return await operation(); } finally { await this.assertOwned(); }
  }
  async release(): Promise<void> { this.owned = false; }
}

async function realPublicationLease(): Promise<DenoSidecarOwnershipLease> {
  let locked = false;
  let closed = false;
  let bytes = new Uint8Array();
  const backend: SidecarOwnershipBackend = {
    pid: 42, now: () => 0, uuid: () => "offline-publication-nonce",
    open: async () => ({
      tryLockExclusive: async () => { if (locked) return false; locked = true; return true; },
      replace: async value => { bytes = value.slice(); },
      read: async () => bytes.slice(),
      pathStillMatches: async () => !closed,
      unlock: async () => { locked = false; },
      close: () => { closed = true; },
    }),
    promoteProcessIdentity: async () => undefined,
  };
  return DenoSidecarOwnershipLease.acquire({ instanceId: INSTANCE, root: "/offline/publication", backend });
}

async function signed(key: string, status: SupervisorStatus = { kind: "shutdown" }, sequence = 1,
  binding: BootstrapBinding = BINDING, instanceId = INSTANCE) {
  return signSupervisorSandboxMessage(key, {
    protocol: GP_V2_SUPERVISOR_PROTOCOL, version: GP_V2_SUPERVISOR_VERSION, direction: "sandbox",
    binding, supervisorInstanceId: instanceId, sequence, message: status,
  });
}

async function fixture(options: { cleanup?: () => Promise<SupervisorStatus>; timeoutMs?: number; remote?: RemoteDocument } = {}) {
  const remote = options.remote ?? new RemoteDocument();
  const key = createCredentialRotationKey();
  let shutdowns = 0;
  let cleanups = 0;
  let cleaned = false;
  const ownership = new Ownership();
  const publicationLease = new Ownership();
  const order: string[] = [];
  let writer!: Provider;
  const peer = new DenoSupervisorPeer({
    binding: BINDING, instanceId: INSTANCE, credential: { ...CREDENTIAL, rotationKey: key },
    providerFactory: doc => { const provider = remote.provider(doc); writer ??= provider; return provider; },
    handle: async () => {
      cleanups++;
      const result = options.cleanup ? await options.cleanup() : { kind: "shutdown" as const };
      cleaned = result.kind === "shutdown";
      return result;
    },
    onShutdown: () => { shutdowns++; },
  });
  await peer.connect();
  const command = await signSupervisorHostMessage(key, {
    protocol: GP_V2_SUPERVISOR_PROTOCOL, version: GP_V2_SUPERVISOR_VERSION, direction: "host",
    binding: BINDING, supervisorInstanceId: INSTANCE, sequence: 1, message: { kind: "shutdown", reason: "offline" },
  });
  return {
    peer, remote, writer, key, ownership, publicationLease, order, cleanups: () => cleanups, shutdowns: () => shutdowns,
    send() { remote.states.set(1, { [GP_V2_SUPERVISOR_FIELD]: command }); remote.changed(1); },
    status: () => writer.local[GP_V2_SUPERVISOR_FIELD] as SignedSupervisorSandboxMessage,
    async finalize() {
      await finalizeR10SupervisorRetirement({
        cleanupChild: async () => {
          if (!cleaned) {
            cleanups++;
            const result = options.cleanup ? await options.cleanup() : { kind: "shutdown" as const };
            if (result.kind !== "shutdown") throw new Error("child cleanup unproven");
            cleaned = true;
          }
          order.push("child_cleaned");
        },
        closePeer: async () => { await peer.close(); order.push("peer_closed"); },
        ownership,
        acquirePublicationLease: async () => {
          expect(ownership.owned).toBe(true); order.push("publication_acquired"); return publicationLease;
        },
        retireIdentity: async () => { expect(ownership.owned).toBe(true); order.push("pid_retired"); },
        receipt: async () => {
          expect(ownership.owned).toBe(false); order.push("receipt_created");
          return { signed: await peer.retirementReceipt(), credential: peer.currentCredential() };
        },
        createProvider: doc => remote.provider(doc), timeoutMs: options.timeoutMs ?? 20,
      });
    },
    async close() { await peer.close(); expect(remote.providers.size).toBe(0); remote.close(); },
  };
}

describe("bounded signed supervisor retirement receipts", () => {
  it("accepts only exact canonical signed shutdown data and compares every authority field", async () => {
    const key = createCredentialRotationKey();
    const raw = encodeSupervisorRetirementReceipt(await signed(key));
    expect(await validateSupervisorRetirementReceipt(raw, BINDING, INSTANCE, key)).not.toBeNull();
    expect(await validateSupervisorRetirementReceipt(raw, BINDING, "other-instance", key)).toBeNull();
    expect(await validateSupervisorRetirementReceipt(raw, BINDING, INSTANCE, createCredentialRotationKey())).toBeNull();
    for (const binding of [
      { ...BINDING, projectId: "22222222-2222-4222-8222-222222222222" },
      { ...BINDING, sandboxSessionId: "other-session" }, { ...BINDING, leaderGeneration: 2 },
      { ...BINDING, nonce: "other-nonce" }, { ...BINDING, buildHash: "c".repeat(64) },
      { ...BINDING, schemaHash: "d".repeat(64) },
    ]) expect(await validateSupervisorRetirementReceipt(raw, binding, INSTANCE, key)).toBeNull();
    for (const invalid of [null, {}, "[]", raw + "\n", " " + raw, "x".repeat(SUPERVISOR_RETIREMENT_MAX_BYTES + 1),
      JSON.stringify(await signed(key, { kind: "ready" })),
      raw.replace('"shutdown"', '"ready"'), raw.replace('"direction":"sandbox"', '"direction":"host"'),
    ]) expect(parseSupervisorRetirementReceipt(invalid)).toBeNull();
    expect(parseSupervisorRetirementReceipt(encodeSupervisorRetirementReceipt(await signed(key, { kind: "shutdown" }, 0)))).not.toBeNull();
  });

  it("reads exactly one bounded slot and rejects additional foreign state", async () => {
    const doc = new Y.Doc();
    try {
      const first = encodeSupervisorRetirementReceipt(await signed(createCredentialRotationKey()));
      const map = doc.getMap(SUPERVISOR_RETIREMENT_MAP);
      map.set(SUPERVISOR_RETIREMENT_KEY, first);
      expect(readSupervisorRetirementReceipt(doc)).toBe(first);
      map.set("foreign", first);
      expect(readSupervisorRetirementReceipt(doc)).toBeNull();
      map.delete("foreign");
      expect(readSupervisorRetirementReceipt(doc)).toBe(first);
    } finally { doc.destroy(); }
  });


  it("publishes terminal proof only after cleanup and independent remote readback, retaining it after peer loss", async () => {
    const cleanup = Promise.withResolvers<SupervisorStatus>();
    const f = await fixture({ cleanup: () => cleanup.promise });
    try {
      f.send(); await until(() => f.cleanups() === 1);
      expect(readSupervisorRetirementReceipt(f.remote.doc)).toBeNull();
      expect(f.status().message.kind).toBe("ready");
      cleanup.resolve({ kind: "shutdown" });
      await until(() => f.status().message.kind === "shutdown");
      expect(readSupervisorRetirementReceipt(f.remote.doc)).toBeNull();
      await f.finalize();
      expect(f.order.slice(0, 5)).toEqual(["child_cleaned", "peer_closed", "publication_acquired", "pid_retired", "receipt_created"]);
      expect(f.remote.created).toBe(3);
      expect(f.remote.providers.size).toBe(0);
      const raw = readSupervisorRetirementReceipt(f.remote.doc);
      expect(await validateSupervisorRetirementReceipt(raw, BINDING, INSTANCE, f.key)).not.toBeNull();
      await f.peer.close();
      expect(f.remote.states.has(f.writer.doc.clientID)).toBe(false);
      const fresh = new Y.Doc(); const reader = f.remote.provider(fresh);
      reader.connect();
      expect(readSupervisorRetirementReceipt(fresh)).toBe(raw);
      reader.destroy(); fresh.destroy();
    } finally { cleanup.resolve({ kind: "shutdown" }); await f.close(); }
  });

  it.each(["write", "read"] as const)("does not mistake local sync for remote persistence when %s is lost", async lost => {
    const f = await fixture();
    try {
      if (lost === "write") f.remote.allowWrites = false;
      else f.remote.allowReads = false;
      await expect(f.finalize()).rejects.toThrow("deadline expired");
      expect(f.shutdowns()).toBe(0);
      expect(f.remote.providers.size).toBe(0);
      const retained = encodeSupervisorRetirementReceipt(await f.peer.retirementReceipt());
      expect(retained).not.toBeNull();
      if (lost === "write") expect(readSupervisorRetirementReceipt(f.remote.doc)).toBeNull();
      f.remote.allowWrites = true; f.remote.allowReads = true;
      await publishR10SupervisorRetirement({
        signed: await f.peer.retirementReceipt(), credential: f.peer.currentCredential(),
        publicationLease: new Ownership(), createProvider: doc => f.remote.provider(doc), timeoutMs: 20,
      });
      expect(readSupervisorRetirementReceipt(f.remote.doc)).toBe(retained);
      expect(f.cleanups()).toBe(1);
    } finally { await f.close(); }
  });

  it("does not overwrite a conflicting receipt or acknowledge shutdown after failed cleanup", async () => {
    const remote = new RemoteDocument();
    const foreign = encodeSupervisorRetirementReceipt(await signed(createCredentialRotationKey()));
    remote.doc.getMap(SUPERVISOR_RETIREMENT_MAP).set(SUPERVISOR_RETIREMENT_KEY, foreign);
    const f = await fixture({ remote });
    try {
      await expect(f.finalize()).rejects.toThrow("conflicts");
      expect(readSupervisorRetirementReceipt(remote.doc)).toBe(foreign);
      expect(remote.created).toBe(2);
      expect(f.shutdowns()).toBe(0);
    } finally { await f.close(); }
    const rejected = await fixture({ cleanup: async () => { throw new Error("retained child not cleaned"); } });
    try {
      await expect(rejected.finalize()).rejects.toThrow("retained child not cleaned");
      expect(readSupervisorRetirementReceipt(rejected.remote.doc)).toBeNull();
      expect(rejected.remote.created).toBe(1);
      expect(rejected.ownership.owned).toBe(true);
      expect(rejected.order).toEqual([]);
    } finally { await rejected.close(); }
  });

  it("allows a lease-ordered successor in the same leader generation to replace one registered predecessor", async () => {
    const remote = new RemoteDocument();
    const previous = encodeSupervisorRetirementReceipt(await signed(createCredentialRotationKey()));
    remote.doc.getMap(SUPERVISOR_RETIREMENT_MAP).set(SUPERVISOR_RETIREMENT_KEY, previous);
    const key = createCredentialRotationKey();
    const terminal = await signed(key, { kind: "shutdown" }, 1, { ...BINDING, nonce: "successor" }, "successor-instance");
    try {
      await publishR10SupervisorRetirement({ signed: terminal, credential: { ...CREDENTIAL, rotationKey: key },
        publicationLease: new Ownership(), createProvider: doc => remote.provider(doc), timeoutMs: 20 });
      expect(readSupervisorRetirementReceipt(remote.doc)).toBe(encodeSupervisorRetirementReceipt(terminal));
      expect(remote.doc.getMap(SUPERVISOR_RETIREMENT_MAP).size).toBe(1);
      expect(remote.providers.size).toBe(0);
    } finally { remote.close(); }
  });

  it("completes publication with the real serialized ownership lease without re-entering it", async () => {
    const remote = new RemoteDocument();
    const lease = await realPublicationLease();
    const key = createCredentialRotationKey();
    let settled = false;
    let failure: unknown = null;
    const publishing = publishR10SupervisorRetirement({
      signed: await signed(key), credential: { ...CREDENTIAL, rotationKey: key }, publicationLease: lease,
      createProvider: doc => remote.provider(doc), timeoutMs: 20,
    }).then(() => { settled = true; }, error => { settled = true; failure = error; });
    try {
      await until(() => settled);
      expect(failure).toBeNull();
      expect(readSupervisorRetirementReceipt(remote.doc)).not.toBeNull();
      expect(remote.providers.size).toBe(0);
    } finally {
      remote.close();
      // A failed regression retains only this isolated in-memory lease; do not
      // await its blocked queue and hide the original deadlock assertion.
      if (settled) { await publishing; await lease.release(); }
    }
  });

  it("fences a delayed older publisher after its publication ownership is lost", async () => {
    const remote = new RemoteDocument();
    const gate = Promise.withResolvers<void>();
    const oldLease = await realPublicationLease();
    const oldKey = createCredentialRotationKey();
    const oldTerminal = await signed(oldKey);
    let delayed!: Provider;
    const oldPublishing = publishR10SupervisorRetirement({
      signed: oldTerminal, credential: { ...CREDENTIAL, rotationKey: oldKey }, publicationLease: oldLease,
      createProvider: doc => { delayed = remote.provider(doc); delayed.syncGate = gate.promise; return delayed; },
      timeoutMs: 100,
    });
    const oldResult = oldPublishing.catch(error => error);
    try {
      await until(() => delayed?.connected === true);
      await oldLease.release();
      const key = createCredentialRotationKey();
      const successor = await signed(key, { kind: "shutdown" }, 1, { ...BINDING, nonce: "successor" }, "successor-instance");
      await publishR10SupervisorRetirement({
        signed: successor, credential: { ...CREDENTIAL, rotationKey: key }, publicationLease: new Ownership(),
        createProvider: doc => remote.provider(doc), timeoutMs: 20,
      });
      const expected = encodeSupervisorRetirementReceipt(successor);
      gate.resolve();
      expect(await oldResult).toMatchObject({ name: "SidecarOwnershipError", code: "lost" });
      expect(readSupervisorRetirementReceipt(remote.doc)).toBe(expected);
      expect(remote.providers.size).toBe(0);
    } finally { gate.resolve(); await oldResult; remote.close(); }
  });

  it.each(["newer", "unregistered", "foreign", "cross-paired"] as const)("rejects a %s predecessor without changing it", async kind => {
    const remote = new RemoteDocument();
    const previousBinding = {
      ...BINDING,
      ...(kind === "newer" ? { leaderGeneration: 2 } : {}),
      ...(kind === "unregistered" ? { buildHash: "e".repeat(64) } : {}),
      ...(kind === "foreign" ? { projectId: "22222222-2222-4222-8222-222222222222" } : {}),
      ...(kind === "cross-paired" ? { buildHash: "ce9f48171863bdaa0adcd1a6344ec175502dd1ef9d1b9fae9e4a512a1474c32a" } : {}),
    };
    const previous = encodeSupervisorRetirementReceipt(await signed(createCredentialRotationKey(), { kind: "shutdown" }, 1, previousBinding));
    remote.doc.getMap(SUPERVISOR_RETIREMENT_MAP).set(SUPERVISOR_RETIREMENT_KEY, previous);
    const key = createCredentialRotationKey();
    const terminal = await signed(key, { kind: "shutdown" }, 1, { ...BINDING, nonce: "successor" }, "successor-instance");
    try {
      await expect(publishR10SupervisorRetirement({ signed: terminal, credential: { ...CREDENTIAL, rotationKey: key },
        publicationLease: new Ownership(), createProvider: doc => remote.provider(doc), timeoutMs: 20 })).rejects.toThrow("conflicts");
      expect(readSupervisorRetirementReceipt(remote.doc)).toBe(previous);
      expect(remote.providers.size).toBe(0);
    } finally { remote.close(); }
  });

  it("publishes no terminal if exact PID retirement fails and releases only its own leases", async () => {
    const ownership = new Ownership(); const publicationLease = new Ownership();
    let created = 0; let closed = 0;
    const key = createCredentialRotationKey();
    await expect(finalizeR10SupervisorRetirement({
      cleanupChild: async () => undefined, closePeer: async () => { closed++; }, ownership,
      acquirePublicationLease: async () => publicationLease,
      retireIdentity: async () => { throw new Error("exact PID removal failed"); },
      receipt: async () => { created++; return { signed: await signed(key), credential: { ...CREDENTIAL, rotationKey: key } }; },
      createProvider: () => { throw new Error("must not connect"); }, timeoutMs: 20,
    })).rejects.toThrow("exact PID removal failed");
    expect(created).toBe(0); expect(closed).toBe(2);
    expect(ownership.owned).toBe(false); expect(publicationLease.owned).toBe(false);
  });

  it("takes the final publisher credential only after closing joins an accepted rotation", async () => {
    const remote = new RemoteDocument();
    const closing = Promise.withResolvers<void>();
    const entered = Promise.withResolvers<void>();
    const key = createCredentialRotationKey();
    let currentCredential = { ...CREDENTIAL, rotationKey: key };
    let finalCredential = currentCredential;
    let closed = false;
    const usedTokens: string[] = [];
    const pending = finalizeR10SupervisorRetirement({
      cleanupChild: async () => undefined,
      closePeer: async () => {
        if (closed) return;
        entered.resolve(); await closing.promise;
        currentCredential = { ...currentCredential, token: "rotated-before-close-finished" };
        closed = true;
      },
      ownership: new Ownership(), acquirePublicationLease: async () => new Ownership(),
      retireIdentity: async () => undefined,
      receipt: async () => {
        expect(closed).toBe(true);
        finalCredential = currentCredential;
        return { signed: await signed(key), credential: finalCredential };
      },
      createProvider: doc => { usedTokens.push(finalCredential.token); return remote.provider(doc); }, timeoutMs: 20,
    });
    try {
      await entered.promise;
      expect(usedTokens).toHaveLength(0);
      closing.resolve(); await pending;
      expect(usedTokens).toEqual(["rotated-before-close-finished", "rotated-before-close-finished"]);
      expect(remote.providers.size).toBe(0);
    } finally { closing.resolve(); await pending.catch(() => undefined); remote.close(); }
  });

  it("retains failed child cleanup until its one retry timer allows exact cleanup, then exits once", async () => {
    const sleep = Promise.withResolvers<void>();
    const delays: number[] = [];
    const events: string[] = [];
    let attempts = 0;
    const clock = {
      now: () => 0,
      sleep: async (ms: number) => { delays.push(ms); await sleep.promise; },
      waitUntil: async () => undefined,
    };
    const finishing = finishR10SupervisorProcess(
      () => waitForR10SupervisorChildCleanup(async () => {
        attempts++;
        if (attempts === 1) throw new Error("exact child still alive");
        events.push("cleanup_proven");
      }, clock),
      () => events.push("diagnostics_closed"),
      () => { events.push("self_exit"); },
    );
    for (let turn = 0; turn < 20; turn++) await Promise.resolve();
    expect(attempts).toBe(1); expect(delays).toEqual([30_000]); expect(events).toEqual([]);
    sleep.resolve(); await finishing;
    expect(attempts).toBe(2);
    expect(events).toEqual(["cleanup_proven", "diagnostics_closed", "self_exit"]);
  });

  it("never self-exits on incomplete or rejected retirement", async () => {
    const retirement = Promise.withResolvers<void>();
    const events: string[] = [];
    const finishing = finishR10SupervisorProcess(() => retirement.promise,
      () => events.push("diagnostics_closed"), () => { events.push("self_exit"); });
    const failure = finishing.catch(error => error);
    await Promise.resolve(); expect(events).toEqual([]);
    retirement.reject(new Error("retirement unproven"));
    expect(await failure).toMatchObject({ message: "retirement unproven" });
    expect(events).toEqual([]);
  });

  it("cancels independent readback without leaving a connected provider", async () => {
    const remote = new RemoteDocument();
    const controller = new AbortController();
    const raw = encodeSupervisorRetirementReceipt(await signed(createCredentialRotationKey()));
    const pending = proveSupervisorRetirementReadback({
      createProvider: doc => remote.provider(doc), expectedRaw: raw, timeoutMs: 1000, signal: controller.signal,
    });
    controller.abort();
    await expect(pending).rejects.toMatchObject({ name: "AbortError" });
    expect(remote.providers.size).toBe(0); remote.close();
  });
});
