import { describe, expect, it } from "bun:test";
import * as Y from "yjs";
import { WebsocketProvider } from "y-websocket";
import * as encoding from "lib0/encoding";
import * as syncProtocol from "y-protocols/sync";
import {
  Awareness,
  applyAwarenessUpdate,
  encodeAwarenessUpdate,
} from "y-protocols/awareness";
import { parseYSweetEnvelope } from "../src/prism/protocol";
import { projectId } from "../src/sandbox/types";
import type {
  AwarenessEnvelope,
  HostFrame,
  ProtocolBinding,
  SidecarFrame,
} from "../src/v2/protocol/shared";
import { parseAwarenessEnvelope } from "../src/v2/protocol/validate";
import {
  YSweetAwarenessChannel,
  type AwarenessProviderLike,
} from "../src/v2/awareness/y-sweet-channel";
import { FakeAwarenessNetwork } from "./support/fake-awareness";

const PROJECT = projectId("e631d12c-d917-4434-b360-41b4715d6679");
const BINDING: ProtocolBinding = {
  protocolRevision: "r1",
  launchNonce: "launch-nonce-r1",
  projectId: PROJECT,
  sessionId: "sandbox-session-1",
  leaderGeneration: 4,
  buildHash: "a".repeat(64),
  schemaHash: "b".repeat(64),
};

function hostEnvelope(epoch = 1): AwarenessEnvelope<HostFrame> {
  return {
    protocol: "gp-awareness-v2",
    schemaVersion: 1,
    schemaHash: BINDING.schemaHash,
    buildHash: BINDING.buildHash,
    projectId: PROJECT,
    sessionId: BINDING.sessionId,
    peerId: "host",
    instanceId: "host-instance",
    leaderGeneration: BINDING.leaderGeneration,
    epoch,
    revision: epoch,
    ack: { epoch, sequence: -1 },
    credits: 32,
    active: 0,
    queued: 0,
    draining: false,
    frames: [],
  };
}

function sidecarEnvelope(epoch = 1): AwarenessEnvelope<SidecarFrame> {
  return {
    ...hostEnvelope(epoch),
    peerId: "sidecar",
    instanceId: "sidecar-instance",
    credits: 128,
    frames: [],
  };
}

class FakeProvider implements AwarenessProviderLike {
  readonly awareness: Awareness;
  readonly listeners = new Map<string, Set<(...args: any[]) => void>>();
  destroyed = false;
  connects = 0;
  disconnects = 0;

  constructor(readonly doc: Y.Doc) {
    this.awareness = new Awareness(doc);
  }

  on(event: string, listener: (...args: any[]) => void): void {
    let values = this.listeners.get(event);
    if (!values) {
      values = new Set();
      this.listeners.set(event, values);
    }
    values.add(listener);
  }

  off(event: string, listener: (...args: any[]) => void): void {
    this.listeners.get(event)?.delete(listener);
  }

  connect(): void {
    this.connects++;
    queueMicrotask(() => this.emit("sync", true));
  }

  disconnect(): void { this.disconnects++; }

  destroy(): void {
    this.destroyed = true;
    this.awareness.destroy();
  }

  emit(event: string, ...args: any[]): void {
    for (const listener of this.listeners.get(event) ?? []) listener(...args);
  }
}

function manualSyncChannel() {
  let provider!: FakeProvider;
  let parseCalls = 0;
  const channel = new YSweetAwarenessChannel<HostFrame, SidecarFrame>({
    envelope: parseYSweetEnvelope({
      url: `wss://prism.test/y/d/${PROJECT}/ws`,
      baseUrl: `https://prism.test/y/d/${PROJECT}`,
      docId: PROJECT,
      token: "offline-retained-state",
      authorization: "full",
    }),
    localPeerId: "host:data",
    field: "ghostprovV2Data",
    parseRemote: value => {
      parseCalls++;
      return parseAwarenessEnvelope(value, BINDING, "sidecar");
    },
    providerFactory: doc => {
      doc.clientID = 1001;
      provider = new FakeProvider(doc);
      provider.connect = () => { provider.connects++; };
      return provider;
    },
    syncTimeoutMs: 1_000,
  });
  return {
    channel,
    provider,
    parseCalls: () => parseCalls,
    savedSync: () => [...(provider.listeners.get("sync") ?? [])][0]!,
    retain(value: unknown, clientId = 2001, clock = 1): void {
      const update = encoding.createEncoder();
      encoding.writeVarUint(update, 1);
      encoding.writeVarUint(update, clientId);
      encoding.writeVarUint(update, clock);
      encoding.writeVarString(update, JSON.stringify({ ghostprovV2Data: value }));
      applyAwarenessUpdate(provider.awareness, encoding.toUint8Array(update), "offline");
    },
  };
}

describe("Awareness channels", () => {
  it("replays retained pre-sync Awareness immediately without mutating clocks or duplicating identical refreshes", async () => {
    const test = manualSyncChannel();
    const received: AwarenessEnvelope<SidecarFrame>[] = [];
    test.channel.subscribe(update => received.push(update.envelope));
    try {
      const connecting = test.channel.connect();
      const savedSync = test.savedSync();
      const envelope = sidecarEnvelope();
      test.retain(envelope);
      const state = test.provider.awareness.getStates().get(2001);
      const metadata = test.provider.awareness.meta.get(2001);
      expect(received).toHaveLength(0);
      expect(test.parseCalls()).toBe(0);
      test.provider.emit("sync", false);
      expect(received).toHaveLength(0);
      test.provider.emit("sync", true);
      expect(received).toEqual([envelope]);
      expect(test.channel.peerIds()).toEqual(["sidecar"]);
      expect(test.provider.awareness.getStates().get(2001)).toBe(state);
      expect(test.provider.awareness.meta.get(2001)).toBe(metadata);
      await connecting;
      test.retain(envelope, 2001, 2);
      expect(test.provider.awareness.meta.get(2001)?.clock).toBe(2);
      savedSync(true);
      test.provider.emit("sync", true);
      expect(received).toEqual([envelope]);
      expect(test.parseCalls()).toBe(1);
    } finally { await test.channel.close(); }
  });

  it("replays the current retained envelope received during a reconnect handshake", async () => {
    const test = manualSyncChannel();
    const received: AwarenessEnvelope<SidecarFrame>[] = [];
    test.channel.subscribe(update => received.push(update.envelope));
    try {
      const initial = test.channel.connect();
      test.provider.emit("sync", true);
      await initial;
      test.retain(sidecarEnvelope(1));
      expect(received.map(value => value.revision)).toEqual([1]);
      const awareness = test.provider.awareness;
      const reconnecting = test.channel.reconnect("data");
      test.retain(sidecarEnvelope(2), 2001, 2);
      expect(received.map(value => value.revision)).toEqual([1]);
      test.provider.emit("sync", true);
      expect(received.map(value => value.revision)).toEqual([1, 2]);
      await reconnecting;
      expect(test.provider.awareness).toBe(awareness);
      expect(test.provider.connects).toBe(2);
      expect(test.provider.disconnects).toBe(1);
    } finally { await test.channel.close(); }
  });

  for (const replacement of ["aborted", "replaced"] as const) {
    it(`ignores saved pre-sync callbacks from the ${replacement} connection before replaying the current generation`, async () => {
      const test = manualSyncChannel();
      const received: AwarenessEnvelope<SidecarFrame>[] = [];
      const controller = new AbortController();
      test.channel.subscribe(update => received.push(update.envelope));
      try {
        const initial = test.channel.connect(controller.signal);
        const saved = test.savedSync();
        const rejection = initial.then(() => null, error => error);
        let current: Promise<void>;
        if (replacement === "aborted") {
          controller.abort();
          expect(await rejection).toMatchObject({ name: "AbortError" });
          current = test.channel.connect();
        } else {
          current = test.channel.reconnect("data");
          expect(await rejection).toMatchObject({ kind: "connect" });
        }
        test.retain(sidecarEnvelope());
        saved(true);
        expect(received).toHaveLength(0);
        expect(test.parseCalls()).toBe(0);
        expect(() => test.channel.publish(hostEnvelope())).toThrow();
        test.provider.emit("sync", true);
        expect(received).toEqual([sidecarEnvelope()]);
        await current;
        saved(true);
        expect(received).toHaveLength(1);
        expect(test.parseCalls()).toBe(1);
      } finally { await test.channel.close(); }
    });
  }

  it("ignores a saved synchronization callback after closing a pending connection", async () => {
    const test = manualSyncChannel();
    const received: AwarenessEnvelope<SidecarFrame>[] = [];
    test.channel.subscribe(update => received.push(update.envelope));
    const connecting = test.channel.connect();
    const rejected = connecting.then(() => null, error => error);
    const saved = test.savedSync();
    test.retain(sidecarEnvelope());
    await test.channel.close();
    expect(await rejected).toMatchObject({ kind: "closed" });
    saved(true);
    test.provider.emit("sync", true);
    expect(test.parseCalls()).toBe(0);
    expect(received).toHaveLength(0);
    expect(test.channel.peerIds()).toEqual([]);
  });

  it("limits retained-state synchronization to 128 remote entries and strictly rejects malformed envelopes", async () => {
    const test = manualSyncChannel();
    const received: AwarenessEnvelope<SidecarFrame>[] = [];
    test.channel.subscribe(update => received.push(update.envelope));
    try {
      const connecting = test.channel.connect();
      test.retain({ ...sidecarEnvelope(1), unexpected: true }, 2001);
      test.retain({ ...sidecarEnvelope(2), schemaHash: "c".repeat(64) }, 2002);
      for (let index = 2; index < 130; index++) {
        test.retain(sidecarEnvelope(index + 1), 2001 + index);
      }
      expect(test.parseCalls()).toBe(0);
      test.provider.emit("sync", true);
      await connecting;
      expect(test.parseCalls()).toBe(128);
      expect(received).toHaveLength(126);
      expect(received.map(value => value.revision)).toEqual(Array.from({ length: 126 }, (_, index) => index + 3));
      expect(test.provider.awareness.getStates().size).toBe(131);
      expect(test.provider.awareness.getStates().get(2130)?.ghostprovV2Data.revision).toBe(130);
    } finally { await test.channel.close(); }
  });

  it("stops retained-state replay and later subscribers when a listener closes the channel", async () => {
    const test = manualSyncChannel();
    const received: AwarenessEnvelope<SidecarFrame>[] = [];
    let laterCallbacks = 0;
    let closing: Promise<void> | undefined;
    test.channel.subscribe(update => {
      received.push(update.envelope);
      closing = test.channel.close();
    });
    test.channel.subscribe(() => { laterCallbacks++; });
    try {
      const connecting = test.channel.connect().then(() => null, error => error);
      test.retain(sidecarEnvelope(1), 2001);
      test.retain(sidecarEnvelope(2), 2002);
      test.provider.emit("sync", true);
      expect(received).toEqual([sidecarEnvelope(1)]);
      expect(test.parseCalls()).toBe(1);
      expect(laterCallbacks).toBe(0);
      await closing;
      const error = await connecting;
      if (error !== null) expect(error).toMatchObject({ kind: "closed" });
    } finally { await test.channel.close(); }
  });

  it("fences stale socket callbacks inside the installed provider after explicit reconnect", async () => {
    const sockets: OfflineSocket[] = [];
    class OfflineSocket {
      static OPEN = 1;
      readonly OPEN = 1;
      readyState = 1;
      binaryType = "arraybuffer";
      onopen: ((event: any) => void) | null = null;
      onmessage: ((event: any) => void) | null = null;
      onerror: ((event: any) => void) | null = null;
      onclose: ((event: any) => void) | null = null;
      sent = 0;
      constructor() { sockets.push(this); }
      send() { this.sent++; }
      close() { this.readyState = 3; }
    }
    const credential = parseYSweetEnvelope({
      url: `wss://prism.test/y/d/${PROJECT}/ws`, baseUrl: `https://prism.test/y/d/${PROJECT}`,
      docId: PROJECT, token: "offline-current", authorization: "full",
    });
    let provider!: WebsocketProvider;
    const failures: Error[] = [];
    const channel = new YSweetAwarenessChannel<HostFrame, SidecarFrame>({
      envelope: credential, localPeerId: "host:control", field: "ghostprovV2Control",
      parseRemote: value => parseAwarenessEnvelope(value, BINDING, "sidecar"),
      providerFactory: doc => provider = new WebsocketProvider("ws://offline.invalid", "doc", doc,
        { connect: false, disableBc: true, WebSocketPolyfill: OfflineSocket as any }),
      syncTimeoutMs: 100,
    });
    channel.onConnectionFailure(error => failures.push(error));
    try {
      const initial = channel.connect();
      sockets[0]!.onopen?.({ target: sockets[0] }); provider.emit("sync", [true]); await initial;
      const previous = sockets[0]!;
      const reconnect = channel.reconnect("control");
      let reconnectSettled = false;
      void reconnect.then(() => { reconnectSettled = true; });
      const staleSync = encoding.createEncoder();
      encoding.writeVarUint(staleSync, 0);
      syncProtocol.writeSyncStep2(staleSync, provider.doc);
      previous.onmessage?.({ target: previous, data: encoding.toUint8Array(staleSync).buffer });
      for (let turn = 0; turn < 20; turn++) await Promise.resolve();
      expect(reconnectSettled).toBe(false);
      sockets[1]!.onopen?.({ target: sockets[1] }); provider.emit("sync", [true]); await reconnect;
      const previousSent = previous.sent;
      previous.onerror?.({ target: previous });
      previous.onopen?.({ target: previous });
      previous.onmessage?.({ target: previous, data: Uint8Array.of(3).buffer });
      expect(failures).toHaveLength(0);
      expect(provider.ws).toBe(sockets[1] as any);
      expect(previous.sent).toBe(previousSent);
      expect(() => channel.publish(hostEnvelope())).not.toThrow();
    } finally { await channel.close(); }
  });

  it("single-flights explicit reconnect and fences delayed sync after cancellation", async () => {
    const credential = parseYSweetEnvelope({ url: `wss://prism.test/y/d/${PROJECT}/ws`,
      baseUrl: `https://prism.test/y/d/${PROJECT}`, docId: PROJECT, token: "offline-current", authorization: "full" });
    let provider!: FakeProvider;
    const channel = new YSweetAwarenessChannel<HostFrame, SidecarFrame>({
      envelope: credential, localPeerId: "host:control", field: "ghostprovV2Control",
      parseRemote: value => parseAwarenessEnvelope(value, BINDING, "sidecar"),
      providerFactory: doc => provider = new FakeProvider(doc), syncTimeoutMs: 500,
    });
    try {
      await channel.connect();
      const ordinaryConnect = provider.connect.bind(provider);
      provider.connect = () => { provider.connects++; };
      const controller = new AbortController();
      const pending = channel.reconnect("control", controller.signal);
      expect(channel.reconnect("control")).toBe(pending);
      expect(() => channel.publish(hostEnvelope())).toThrow("reconnecting");
      const oldSync = [...provider.listeners.get("sync")!][0]!;
      controller.abort();
      await expect(pending).rejects.toMatchObject({ name: "AbortError" });
      provider.connect = ordinaryConnect;
      const next = channel.reconnect("control");
      oldSync(true);
      expect(() => channel.publish(hostEnvelope())).toThrow("reconnecting");
      await next;
      expect(provider.connects).toBe(3);
      expect(() => channel.publish(hostEnvelope())).not.toThrow();
    } finally { await channel.close(); }
  });

  it("explicitly reconnects a silent connected provider without replacing its credential or state", async () => {
    const credential = parseYSweetEnvelope({
      url: `wss://prism.test/y/d/${PROJECT}/ws`, baseUrl: `https://prism.test/y/d/${PROJECT}`,
      docId: PROJECT, token: "offline-current-credential", authorization: "full",
    });
    let provider!: FakeProvider;
    let credentialSource!: () => typeof credential;
    let providers = 0;
    const channel = new YSweetAwarenessChannel<HostFrame, SidecarFrame>({
      envelope: credential, localPeerId: "host:control", field: "ghostprovV2Control",
      parseRemote: value => parseAwarenessEnvelope(value, BINDING, "sidecar"),
      providerFactory: (doc, current) => { providers++; credentialSource = current; return provider = new FakeProvider(doc); },
      syncTimeoutMs: 100,
    });
    try {
      await channel.connect();
      const state = hostEnvelope();
      channel.publish(state);
      // A silent half-open link emits no close/error event. Ordinary connect is
      // correctly idempotent, but recovery must explicitly replace that socket.
      await channel.connect();
      expect(provider.connects).toBe(1);
      await channel.reconnect?.("control");
      expect(provider.connects).toBe(2);
      expect(provider.disconnects).toBe(1);
      expect(providers).toBe(1);
      expect(credentialSource()).toBe(credential);
      expect(provider.awareness.getLocalState()?.ghostprovV2Control).toEqual(state);
    } finally { await channel.close(); }
  });

  it("publishes one ephemeral field and clears peer presence on close", async () => {
    const network = new FakeAwarenessNetwork<HostFrame, SidecarFrame>();
    const host = network.connectHost("host", PROJECT);
    const sidecar = network.connectSidecar("sidecar", PROJECT);
    const received: AwarenessEnvelope<HostFrame>[] = [];
    sidecar.subscribe(update => received.push(update.envelope));

    host.publish(hostEnvelope());
    await network.flushAll();

    expect(received).toEqual([hostEnvelope()]);
    expect(sidecar.peerIds()).toEqual(["host"]);
    expect(network.persistentWrites).toBe(0);
    await host.close();
    await network.flushAll();
    expect(sidecar.peerIds()).toEqual([]);
    await sidecar.close();
  });

  it("survives deterministic duplication, reordering, and dropped updates", async () => {
    const network = new FakeAwarenessNetwork<HostFrame, SidecarFrame>({
      drop: [2, 7],
      duplicate: [1, 5],
      reverseEvery: 3,
    });
    const host = network.connectHost("host", PROJECT);
    const sidecar = network.connectSidecar("sidecar", PROJECT);
    const seen = new Set<number>();
    sidecar.subscribe(update => seen.add(update.envelope.epoch));

    for (let epoch = 1; epoch <= 10; epoch++) host.publish(hostEnvelope(epoch));
    await network.flushAll();

    expect(Math.max(...seen)).toBe(10);
    expect(seen.size).toBeGreaterThanOrEqual(8);
    expect(network.persistentWrites).toBe(0);
    await host.close();
    await sidecar.close();
  });

  it("uses a dedicated empty Y.Doc and validates remote awareness state", async () => {
    let provider: FakeProvider | null = null;
    const ySweet = parseYSweetEnvelope({
      url: `wss://prism.test/y/d/${PROJECT}/ws`,
      baseUrl: `https://prism.test/y/d/${PROJECT}`,
      docId: PROJECT,
      token: "y-secret",
      authorization: "full",
    });
    const channel = new YSweetAwarenessChannel<HostFrame, SidecarFrame>({
      envelope: ySweet,
      localPeerId: "host",
      field: "ghostprovV2",
      parseRemote: value => parseAwarenessEnvelope(value, BINDING, "sidecar"),
      providerFactory: doc => {
        provider = new FakeProvider(doc);
        return provider;
      },
      syncTimeoutMs: 100,
    });
    const updates: AwarenessEnvelope<SidecarFrame>[] = [];
    channel.subscribe(update => updates.push(update.envelope));

    await channel.connect();
    channel.publish(hostEnvelope());
    expect(provider!.doc.share.size).toBe(0);
    expect(provider!.awareness.getLocalState()?.ghostprovV2).toEqual(hostEnvelope());

    const remoteDoc = new Y.Doc();
    const remoteAwareness = new Awareness(remoteDoc);
    remoteAwareness.setLocalStateField("ghostprovV2", sidecarEnvelope());
    applyAwarenessUpdate(
      provider!.awareness,
      encodeAwarenessUpdate(remoteAwareness, [remoteDoc.clientID]),
      "test",
    );
    expect(updates).toEqual([sidecarEnvelope()]);
    expect(channel.peerIds()).toEqual(["sidecar"]);

    remoteAwareness.setLocalStateField("ghostprovV2", { invalid: true });
    applyAwarenessUpdate(
      provider!.awareness,
      encodeAwarenessUpdate(remoteAwareness, [remoteDoc.clientID]),
      "test",
    );
    expect(updates).toHaveLength(1);

    await channel.close();
    expect(provider!.destroyed).toBe(true);
    expect(provider!.doc.share.size).toBe(0);
    remoteAwareness.destroy();
    remoteDoc.destroy();
  });

  it("rejects an in-progress connection immediately when closed", async () => {
    const ySweet = parseYSweetEnvelope({
      url: `wss://prism.test/y/d/${PROJECT}/ws`,
      baseUrl: `https://prism.test/y/d/${PROJECT}`,
      docId: PROJECT,
      token: "y-secret",
      authorization: "full",
    });
    const channel = new YSweetAwarenessChannel<HostFrame, SidecarFrame>({
      envelope: ySweet,
      localPeerId: "host",
      field: "ghostprovV2",
      parseRemote: value => parseAwarenessEnvelope(value, BINDING, "sidecar"),
      providerFactory: doc => new FakeProvider(doc),
      syncTimeoutMs: 1_000,
    });

    const pending = channel.connect();
    await channel.close();

    await expect(pending).rejects.toMatchObject({ kind: "closed" });
  });

  it("updates the credential source used by future reconnects", async () => {
    const first = parseYSweetEnvelope({
      url: `wss://prism.test/y/d/${PROJECT}/ws`,
      baseUrl: `https://prism.test/y/d/${PROJECT}`,
      docId: PROJECT,
      token: "y-secret-1",
      authorization: "full",
    });
    let current!: () => typeof first;
    const channel = new YSweetAwarenessChannel<HostFrame, SidecarFrame>({
      envelope: first,
      localPeerId: "host",
      field: "ghostprovV2",
      parseRemote: value => parseAwarenessEnvelope(value, BINDING, "sidecar"),
      providerFactory: (doc, credential) => {
        current = credential;
        return new FakeProvider(doc);
      },
      syncTimeoutMs: 100,
    });
    await channel.connect();
    const rotated = parseYSweetEnvelope({
      url: first.url,
      baseUrl: first.baseUrl,
      docId: PROJECT,
      token: "y-secret-2",
      authorization: "full",
    });

    channel.rotateCredential(rotated);

    expect(current().token.reveal()).toBe("y-secret-2");
    expect(() => channel.rotateCredential(parseYSweetEnvelope({
      url: first.url,
      baseUrl: first.baseUrl,
      docId: "69288e8b-c183-45f3-9131-d64b05b98928",
      token: "foreign",
      authorization: "full",
    }))).toThrow("another project");
    await channel.close();
  });

  it("fails publish immediately on post-sync auth revocation and reconnects with rotation", async () => {
    const first = parseYSweetEnvelope({
      url: `wss://prism.test/y/d/${PROJECT}/ws`,
      baseUrl: `https://prism.test/y/d/${PROJECT}`,
      docId: PROJECT,
      token: "y-secret-revoked",
      authorization: "full",
    });
    let provider!: FakeProvider;
    const channel = new YSweetAwarenessChannel<HostFrame, SidecarFrame>({
      envelope: first,
      localPeerId: "host",
      field: "ghostprovV2",
      parseRemote: value => parseAwarenessEnvelope(value, BINDING, "sidecar"),
      providerFactory: doc => provider = new FakeProvider(doc),
      syncTimeoutMs: 100,
    });
    const failures: Error[] = [];
    channel.onConnectionFailure(error => failures.push(error));
    await channel.connect();

    provider.emit("connection-error", { status: 401 });
    expect(failures).toHaveLength(1);
    expect(failures[0]).toMatchObject({ kind: "auth", connectionCode: 401 });
    expect(() => channel.publish(hostEnvelope())).toThrow("connection failed");

    const rotated = parseYSweetEnvelope({
      url: first.url,
      baseUrl: first.baseUrl,
      docId: PROJECT,
      token: "y-secret-reminted",
      authorization: "full",
    });
    await channel.rotateCredential(rotated);
    expect(provider.connects).toBe(2);
    expect(() => channel.publish(hostEnvelope())).not.toThrow();
    await channel.close();
  });

  it("single-flights a transient post-sync reconnect", async () => {
    const credential = parseYSweetEnvelope({
      url: `wss://prism.test/y/d/${PROJECT}/ws`,
      baseUrl: `https://prism.test/y/d/${PROJECT}`,
      docId: PROJECT,
      token: "y-secret-transient",
      authorization: "full",
    });
    let provider!: FakeProvider;
    const channel = new YSweetAwarenessChannel<HostFrame, SidecarFrame>({
      envelope: credential,
      localPeerId: "host",
      field: "ghostprovV2",
      parseRemote: value => parseAwarenessEnvelope(value, BINDING, "sidecar"),
      providerFactory: doc => provider = new FakeProvider(doc),
      syncTimeoutMs: 100,
    });
    const failures: Error[] = [];
    channel.onConnectionFailure(error => failures.push(error));
    await channel.connect();
    provider.emit("connection-close", { code: 1006 });
    expect(failures[0]).toMatchObject({ kind: "disconnected", connectionCode: 1006 });

    const left = channel.connect();
    const right = channel.connect();
    expect(right).toBe(left);
    await Promise.all([left, right]);
    expect(provider.connects).toBe(2);
    expect(() => channel.publish(hostEnvelope())).not.toThrow();
    await channel.close();
  });

  it("ignores a stale close callback from the provider generation before reconnect", async () => {
    const credential = parseYSweetEnvelope({
      url: `wss://prism.test/y/d/${PROJECT}/ws`,
      baseUrl: `https://prism.test/y/d/${PROJECT}`,
      docId: PROJECT,
      token: "y-secret-stale-close",
      authorization: "full",
    });
    let provider!: FakeProvider;
    const channel = new YSweetAwarenessChannel<HostFrame, SidecarFrame>({
      envelope: credential,
      localPeerId: "host",
      field: "ghostprovV2",
      parseRemote: value => parseAwarenessEnvelope(value, BINDING, "sidecar"),
      providerFactory: doc => provider = new FakeProvider(doc),
      syncTimeoutMs: 100,
    });
    const failures: Error[] = [];
    channel.onConnectionFailure(error => failures.push(error));
    await channel.connect();
    const staleClose = [...(provider.listeners.get("connection-close") ?? [])][0]!;
    provider.emit("connection-close", { code: 1006 });
    await channel.connect();
    expect(failures).toHaveLength(1);

    staleClose({ code: 1006 });
    expect(failures).toHaveLength(1);
    expect(() => channel.publish(hostEnvelope())).not.toThrow();
    await channel.close();
  });
});
