import { describe, expect, it } from "bun:test";
import * as Y from "yjs";
import { WebsocketProvider } from "y-websocket";
import { parseYSweetEnvelope } from "../src/prism/protocol";
import { awarenessSocketTarget } from "../src/v2/awareness/socket-target";
import { createYSweetAwarenessProvider } from "../src/v2/awareness/y-sweet-channel";
import { createV2SupervisorProvider } from "../src/v2/supervisor/host";
import { createR10AwarenessProvider } from "../sandbox/v2/r10-awareness-peer";
import { createR10SupervisorProvider } from "../sandbox/v2/r10-supervisor-peer";
import { SidecarCredentialRotator } from "../src/v2/bootstrap/rotation";
import { sealCredentialRotation, type BootstrapCredentialPayload } from "../src/v2/bootstrap/crypto";

const DOC_ID = "e631d12c-d917-4434-b360-41b4715d6679";
const FOREIGN_DOC_ID = "e631d12c-d917-4434-b360-41b4715d6678";
const INITIAL: BootstrapCredentialPayload = Object.freeze({
  url: "wss://old.invalid/first/path///", baseUrl: "https://old.invalid/first/path/",
  docId: DOC_ID, token: "offline-old-token", authorization: "full",
  rotationKey: Buffer.alloc(32, 7).toString("base64url"),
});
const NEXT: BootstrapCredentialPayload = Object.freeze({ ...INITIAL,
  url: "wss://new.invalid/escaped%20path/%2F///", baseUrl: "https://new.invalid/escaped%20path/%2F/",
  token: "offline-new/token+value=with space",
});

const factories = [
  { name: "host Awareness", create: (doc: Y.Doc, read: () => BootstrapCredentialPayload) =>
    createYSweetAwarenessProvider(doc, () => parseYSweetEnvelope(read())) },
  { name: "host supervisor", create: (doc: Y.Doc, read: () => BootstrapCredentialPayload) =>
    createV2SupervisorProvider(doc, () => parseYSweetEnvelope(read())) },
  { name: "R10 Awareness", create: (doc: Y.Doc, read: () => BootstrapCredentialPayload) =>
    createR10AwarenessProvider(doc, read, "data") },
  { name: "R10 supervisor", create: (doc: Y.Doc, read: () => BootstrapCredentialPayload) =>
    createR10SupervisorProvider(doc, read) },
] as const;

/** Preserve each production socket constructor while replacing only its native base. */
function fakeSockets(provider: WebsocketProvider) {
  const sockets: Array<{ url: string; protocols: unknown }> = [];
  class OfflineSocket {
    static OPEN = 1;
    readonly OPEN = 1;
    readyState = 0;
    binaryType = "arraybuffer";
    onopen: ((event: unknown) => void) | null = null;
    onmessage: ((event: unknown) => void) | null = null;
    onclose: ((event: unknown) => void) | null = null;
    onerror: ((event: unknown) => void) | null = null;
    constructor(url: string | URL, protocols?: unknown) {
      if (sockets.length >= 4) throw new Error("Offline socket bound exceeded");
      sockets.push({ url: String(url), protocols });
    }
    send(): void {}
    close(): void { this.readyState = 3; }
  }
  expect(provider.ws).toBeNull();
  expect(provider.shouldConnect).toBe(false);
  const socketConstructor = provider._WS;
  Object.setPrototypeOf(socketConstructor, OfflineSocket);
  Object.setPrototypeOf(socketConstructor.prototype, OfflineSocket.prototype);
  // Bun's built-in ws module cannot reliably be mocked. Assert this seam
  // before connecting so the production constructor cannot open a real socket.
  expect(Object.getPrototypeOf(socketConstructor)).toBe(OfflineSocket);
  expect(Object.getPrototypeOf(socketConstructor.prototype)).toBe(OfflineSocket.prototype);
  return sockets;
}

function pinnedTarget(credential: BootstrapCredentialPayload): string {
  const doc = new Y.Doc();
  const provider = new WebsocketProvider(credential.url, credential.docId, doc, {
    connect: false, disableBc: true,
  });
  try {
    const target = new URL(provider.url);
    target.searchParams.set("token", credential.token);
    return target.href;
  } finally { provider.destroy(); doc.destroy(); }
}

describe("Awareness socket targets", () => {
  it.each([
    "wss://offline.invalid",
    "wss://offline.invalid/",
    "wss://offline.invalid/path////",
    "wss://offline.invalid/path%20with%20spaces/%2F/%3F",
    "ws://offline.invalid:8080/a/b",
    "wss://[::1]:9443/escaped%23path/",
    "wss://offline.invalid/base?route=opaque",
  ])("matches pinned endpoint construction for %s without mutating credentials", url => {
    const credential = Object.freeze({ ...INITIAL, url });
    const before = JSON.stringify(credential);
    expect(awarenessSocketTarget(credential, DOC_ID).href).toBe(pinnedTarget(credential));
    expect(JSON.stringify(credential)).toBe(before);
  });

  it("rejects a changed document before constructing an endpoint", () => {
    expect(() => awarenessSocketTarget({ ...NEXT, docId: FOREIGN_DOC_ID }, DOC_ID))
      .toThrow("Awareness socket credential belongs to another document");
  });

  it("accepts an authenticated endpoint-changing rotation under the existing document binding", async () => {
    const binding = { projectId: DOC_ID, sessionId: "offline-session", leaderGeneration: 4,
      buildHash: "a".repeat(64), schemaHash: "b".repeat(64), launchNonce: "offline-launch", protocolRevision: "r2" as const };
    const rotator = new SidecarCredentialRotator({ binding, instanceId: "offline-instance", credential: INITIAL });
    const rotationId = "offline-endpoint-rotation";
    const sealed = await sealCredentialRotation({ rotationKey: INITIAL.rotationKey,
      binding: { projectId: DOC_ID, sandboxSessionId: binding.sessionId, leaderGeneration: binding.leaderGeneration,
        buildHash: binding.buildHash, schemaHash: binding.schemaHash, instanceId: "offline-instance", rotationId,
        protocolRevision: "r2", schemaVersion: 2, launchNonce: binding.launchNonce }, credential: NEXT });
    await rotator.accept(rotationId, JSON.stringify(sealed));
    expect(rotator.current()).toEqual(NEXT);
  });

  for (const factory of factories) {
    it.each(["endpoint and token", "token only"] as const)(`${factory.name} reconnects using one current %s snapshot`, change => {
      const doc = new Y.Doc();
      let current = INITIAL;
      let reads = 0;
      const provider = factory.create(doc, () => { reads++; return current; }) as WebsocketProvider;
      const sockets = fakeSockets(provider);
      const awareness = provider.awareness;
      const clientId = doc.clientID;
      const retained = Object.freeze({ sequence: 7, immutableFlight: "offline-retained-state" });
      provider.awareness.setLocalStateField("offlineRetained", retained);
      doc.getMap("offlineRetained").set("marker", "exact-document");
      const protocols = ["offline-protocol"];
      provider.protocols = protocols;
      try {
        expect(reads).toBe(1);
        provider.connect();
        expect(reads).toBe(2);
        expect(sockets).toHaveLength(1);
        expect(sockets[0]!.url).toBe(pinnedTarget(INITIAL));
        current = change === "endpoint and token" ? NEXT : Object.freeze({ ...INITIAL, token: NEXT.token });
        const before = JSON.stringify(current);
        expect(sockets).toHaveLength(1); // Rotation alone creates no replacement socket.
        provider.disconnect();
        provider.connect();
        expect(reads).toBe(3);
        expect(sockets).toHaveLength(2);
        expect(sockets[1]!.url).toBe(pinnedTarget(current));
        expect(new URL(sockets[1]!.url).searchParams.get("token")).toBe(NEXT.token);
        expect(sockets[1]!.protocols).toBe(protocols);
        expect(provider.doc).toBe(doc);
        expect(provider.awareness).toBe(awareness);
        expect(doc.clientID).toBe(clientId);
        expect(provider.awareness.getLocalState()?.offlineRetained).toBe(retained);
        expect(doc.getMap("offlineRetained").get("marker")).toBe("exact-document");
        expect(JSON.stringify(current)).toBe(before);
      } finally { provider.destroy(); doc.destroy(); }
    });

    it(`${factory.name} rejects in-place document mutation before opening another socket`, () => {
      const doc = new Y.Doc();
      const current = { ...INITIAL };
      const provider = factory.create(doc, () => current) as WebsocketProvider;
      const sockets = fakeSockets(provider);
      try {
        provider.connect();
        provider.disconnect();
        current.docId = FOREIGN_DOC_ID;
        current.url = NEXT.url;
        current.token = NEXT.token;
        expect(() => provider.connect()).toThrow("Awareness socket credential belongs to another document");
        expect(sockets).toHaveLength(1);
        expect(provider.doc).toBe(doc);
      } finally { provider.destroy(); doc.destroy(); }
    });
  }
});
