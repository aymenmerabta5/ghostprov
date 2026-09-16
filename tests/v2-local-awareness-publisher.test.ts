import { describe, expect, it } from "bun:test";
import * as Y from "yjs";
import { WebsocketProvider } from "y-websocket";
import * as decoding from "lib0/decoding";
import * as encoding from "lib0/encoding";
import * as syncProtocol from "y-protocols/sync";
import { parseYSweetEnvelope } from "../src/prism/protocol";
import { withLocalAwarenessPublisher } from "../src/v2/awareness/local-publisher";
import { createYSweetAwarenessProvider } from "../src/v2/awareness/y-sweet-channel";

const LOCAL_ID = 1001;
const REMOTE_ID = 2001;
const MAX_PACKETS = 64;
const roles = [
  { name: "host data", field: "ghostprovV2Data", value: { peerId: "host", revision: 1 } },
  { name: "host control", field: "ghostprovV2Control", value: { peerId: "host", revision: 1 } },
  { name: "sidecar data", field: "ghostprovV2Data", value: { peerId: "sidecar", revision: 1 } },
  { name: "sidecar control", field: "ghostprovV2Control", value: { peerId: "sidecar", revision: 1 } },
  { name: "host supervisor", field: "ghostprovV2Supervisor", value: { direction: "host", sequence: 1 } },
  { name: "sandbox supervisor", field: "ghostprovV2Supervisor", value: { direction: "sandbox", sequence: 1 } },
] as const;

interface WireState {
  readonly clientId: number;
  readonly clock: number;
  readonly state: Record<string, unknown> | null;
}

function awarenessPacket(entries: readonly WireState[]): Uint8Array {
  const inner = encoding.createEncoder();
  encoding.writeVarUint(inner, entries.length);
  for (const entry of entries) {
    encoding.writeVarUint(inner, entry.clientId);
    encoding.writeVarUint(inner, entry.clock);
    encoding.writeVarString(inner, JSON.stringify(entry.state));
  }
  const outer = encoding.createEncoder();
  encoding.writeVarUint(outer, 1);
  encoding.writeVarUint8Array(outer, encoding.toUint8Array(inner));
  return encoding.toUint8Array(outer);
}

function readStates(packet: Uint8Array): Array<WireState & { stateBytes: number }> {
  const outer = decoding.createDecoder(packet);
  expect(decoding.readVarUint(outer)).toBe(1);
  const inner = decoding.createDecoder(decoding.readVarUint8Array(outer));
  const count = decoding.readVarUint(inner);
  expect(count).toBeLessThanOrEqual(MAX_PACKETS);
  const entries: Array<WireState & { stateBytes: number }> = [];
  for (let index = 0; index < count; index++) {
    const clientId = decoding.readVarUint(inner);
    const clock = decoding.readVarUint(inner);
    const json = decoding.readVarString(inner);
    entries.push({ clientId, clock, state: JSON.parse(json), stateBytes: new TextEncoder().encode(json).byteLength });
  }
  return entries;
}

function fixture(localOnly = true, createProvider?: (doc: Y.Doc) => WebsocketProvider) {
  const doc = new Y.Doc();
  doc.clientID = LOCAL_ID;
  const sockets: OfflineSocket[] = [];
  const sent: Uint8Array[] = [];
  class OfflineSocket {
    static OPEN = 1;
    readonly OPEN = 1;
    readyState = 0;
    binaryType = "arraybuffer";
    onopen: ((event: unknown) => void) | null = null;
    onmessage: ((event: unknown) => void) | null = null;
    onclose: ((event: unknown) => void) | null = null;
    onerror: ((event: unknown) => void) | null = null;
    constructor() { sockets.push(this); }
    send(bytes: Uint8Array): void {
      if (sent.length >= MAX_PACKETS) throw new Error("Offline socket packet bound exceeded");
      sent.push(bytes);
    }
    close(): void { this.readyState = 3; }
  }
  const provider = createProvider?.(doc) ?? new WebsocketProvider("ws://offline.invalid", "local-publisher-test", doc, {
    connect: false,
    disableBc: true,
    WebSocketPolyfill: OfflineSocket as any,
  });
  provider._WS = OfflineSocket as any;
  const originalHandler = provider._awarenessUpdateHandler;
  const originalAwareness = provider.awareness;
  const originalMessageHandlers = provider.messageHandlers.slice();
  const returned = localOnly ? withLocalAwarenessPublisher(provider) : provider;
  const open = (): void => {
    const socket = sockets.at(-1)!;
    socket.readyState = 1;
    socket.onopen?.({ target: socket });
  };
  provider.connect();
  open();
  sent.length = 0;
  return {
    provider, returned, originalHandler, originalAwareness, originalMessageHandlers, doc, sockets, sent, open,
    receive(packet: Uint8Array): void {
      const socket = sockets.at(-1)!;
      socket.onmessage?.({ target: socket, data: packet.buffer.slice(packet.byteOffset, packet.byteOffset + packet.byteLength) });
    },
    clear(): void { sent.length = 0; },
    close(): void { provider.destroy(); doc.destroy(); },
  };
}

describe("local Awareness publication", () => {
  it("exports a disconnected native factory with remote delivery and local-only publication already installed", () => {
    const credential = parseYSweetEnvelope({
      url: "wss://offline.invalid/y/doc",
      baseUrl: "https://offline.invalid/y/doc",
      docId: "e631d12c-d917-4434-b360-41b4715d6679",
      token: "offline-native-factory",
      authorization: "full",
    });
    const test = fixture(false, doc => {
      const provider = createYSweetAwarenessProvider(doc, () => credential);
      expect(provider).toBeInstanceOf(WebsocketProvider);
      expect(provider.ws).toBeNull();
      expect(provider.wsconnected).toBe(false);
      expect(provider.shouldConnect).toBe(false);
      expect(provider.disableBc).toBe(true);
      return provider;
    });
    const remote = { ghostprovV2Data: { peerId: "sidecar", revision: 1 } };
    const local = { peerId: "host", revision: 1 };
    let remoteChanges = 0;
    test.provider.awareness.on("change", ({ added }: { added: number[] }) => {
      if (added.includes(REMOTE_ID)) remoteChanges++;
    });
    try {
      test.receive(awarenessPacket([{ clientId: REMOTE_ID, clock: 1, state: remote }]));
      expect(test.provider.awareness.getStates().get(REMOTE_ID)).toEqual(remote);
      expect(remoteChanges).toBe(1);
      expect(test.sent).toHaveLength(0);
      test.provider.awareness.setLocalStateField("ghostprovV2Control", local);
      expect(test.sent).toHaveLength(1);
      expect(readStates(test.sent[0]!)[0]).toMatchObject({
        clientId: LOCAL_ID,
        state: { ghostprovV2Control: local },
      });
    } finally { test.close(); }
  });

  it("returns the same provider and preserves its Awareness and message handlers", () => {
    const test = fixture();
    try {
      expect(test.returned).toBe(test.provider);
      expect(test.provider.doc).toBe(test.doc);
      expect(test.provider.awareness).toBe(test.originalAwareness);
      expect(test.provider.messageHandlers).toEqual(test.originalMessageHandlers);
      expect(test.provider._awarenessUpdateHandler).not.toBe(test.originalHandler);
    } finally { test.close(); }
  });

  it("delivers a 350 KiB remote data state without the original provider's full-size echo", () => {
    const stateBytes = 350 * 1024;
    const shell = { ghostprovV2Data: { peerId: "sidecar", revision: 1, data: "" } };
    const overhead = new TextEncoder().encode(JSON.stringify(shell)).byteLength;
    const state = { ghostprovV2Data: { ...shell.ghostprovV2Data, data: "x".repeat(stateBytes - overhead) } };
    const incoming = awarenessPacket([{ clientId: REMOTE_ID, clock: 1, state }]);
    for (const localOnly of [false, true]) {
      const test = fixture(localOnly);
      let changes = 0;
      test.provider.awareness.on("change", ({ added }: { added: number[] }) => {
        if (added.includes(REMOTE_ID)) changes++;
      });
      try {
        test.receive(incoming);
        expect(changes).toBe(1);
        expect(test.provider.awareness.getStates().get(REMOTE_ID)).toEqual(state);
        expect(test.provider.awareness.getLocalState()).toEqual({});
        expect(test.sent).toHaveLength(localOnly ? 0 : 1);
        if (!localOnly) {
          expect(test.sent[0]!.byteLength).toBe(incoming.byteLength);
          expect(readStates(test.sent[0]!)).toEqual([{ clientId: REMOTE_ID, clock: 1, state, stateBytes }]);
        }
      } finally { test.close(); }
    }
  });

  it("retains updates and removals for every remote transport role without cross-role echoes", () => {
    const test = fixture();
    let changes = 0;
    test.provider.awareness.on("change", () => { changes++; });
    try {
      for (const [index, role] of roles.entries()) {
        const clientId = REMOTE_ID + index;
        const state = { [role.field]: role.value };
        test.receive(awarenessPacket([{ clientId, clock: 1, state }]));
        expect(test.provider.awareness.getStates().get(clientId)).toEqual(state);
        test.receive(awarenessPacket([{ clientId, clock: 2, state: null }]));
        expect(test.provider.awareness.getStates().has(clientId)).toBe(false);
      }
      expect(changes).toBe(roles.length * 2);
      expect(test.sent).toHaveLength(0);
    } finally { test.close(); }
  });

  for (const role of roles) {
    it(`preserves ${role.name} publication, clock renewal, null removal, and own-state correction`, () => {
      const test = fixture();
      const state = { [role.field]: role.value };
      try {
        test.provider.awareness.setLocalStateField(role.field, role.value);
        expect(test.sent).toHaveLength(1);
        const first = readStates(test.sent[0]!)[0]!;
        expect(first.clientId).toBe(LOCAL_ID);
        expect(first.state).toEqual(state);

        test.clear();
        test.provider.awareness.setLocalState(test.provider.awareness.getLocalState());
        expect(test.sent).toHaveLength(1);
        expect(readStates(test.sent[0]!)[0]!.clock).toBe(first.clock + 1);

        test.clear();
        const clock = test.provider.awareness.meta.get(LOCAL_ID)!.clock;
        test.receive(awarenessPacket([{ clientId: LOCAL_ID, clock, state: null }]));
        expect(test.sent).toHaveLength(1);
        expect(readStates(test.sent[0]!)[0]).toMatchObject({ clientId: LOCAL_ID, clock: clock + 1, state });
        expect(test.provider.awareness.getLocalState()).toEqual(state);

        test.clear();
        test.provider.awareness.setLocalStateField(role.field, null);
        expect(test.sent).toHaveLength(1);
        expect(readStates(test.sent[0]!)[0]!.state).toEqual({ [role.field]: null });

        test.clear();
        test.provider.awareness.setLocalState(null);
        expect(test.sent).toHaveLength(1);
        expect(readStates(test.sent[0]!)[0]!.state).toBeNull();
      } finally { test.close(); }
    });
  }

  it("forwards only the local correction from a mixed local-tombstone and remote-update event", () => {
    const test = fixture();
    const local = { ghostprovV2Control: { peerId: "host", revision: 2 } };
    const remote = { ghostprovV2Supervisor: { direction: "sandbox", sequence: 3 } };
    try {
      test.provider.awareness.setLocalState(local);
      test.clear();
      const clock = test.provider.awareness.meta.get(LOCAL_ID)!.clock;
      test.receive(awarenessPacket([
        { clientId: LOCAL_ID, clock, state: null },
        { clientId: REMOTE_ID, clock: 1, state: remote },
      ]));
      expect(test.provider.awareness.getStates().get(REMOTE_ID)).toEqual(remote);
      expect(test.sent).toHaveLength(1);
      expect(readStates(test.sent[0]!)).toEqual([{
        clientId: LOCAL_ID,
        clock: clock + 1,
        state: local,
        stateBytes: new TextEncoder().encode(JSON.stringify(local)).byteLength,
      }]);
    } finally { test.close(); }
  });

  it("keeps the default Awareness query response with local and remote states", () => {
    const test = fixture();
    const local = { ghostprovV2Control: { peerId: "host" } };
    const remote = { ghostprovV2Data: { peerId: "sidecar" } };
    try {
      test.provider.awareness.setLocalState(local);
      test.receive(awarenessPacket([{ clientId: REMOTE_ID, clock: 1, state: remote }]));
      test.clear();
      test.receive(Uint8Array.of(3));
      expect(test.sent).toHaveLength(1);
      const states = readStates(test.sent[0]!);
      expect(states).toHaveLength(2);
      expect(states.find(entry => entry.clientId === LOCAL_ID)?.state).toEqual(local);
      expect(states.find(entry => entry.clientId === REMOTE_ID)?.state).toEqual(remote);
    } finally { test.close(); }
  });

  it("still synchronizes Y.Doc map changes while local Awareness state is null", () => {
    const test = fixture();
    const receiver = new Y.Doc();
    receiver.clientID = REMOTE_ID;
    try {
      test.provider.awareness.setLocalState(null);
      test.clear();
      test.doc.getMap<string>("files").set("offline.txt", "document changes remain synchronized");
      expect(test.sent).toHaveLength(1);
      const decoder = decoding.createDecoder(test.sent[0]!);
      expect(decoding.readVarUint(decoder)).toBe(0);
      const reply = encoding.createEncoder();
      expect(syncProtocol.readSyncMessage(decoder, reply, receiver, "offline")).toBe(syncProtocol.messageYjsUpdate);
      expect(receiver.getMap<string>("files").get("offline.txt")).toBe("document changes remain synchronized");
      expect(test.provider.awareness.getLocalState()).toBeNull();
    } finally { receiver.destroy(); test.close(); }
  });

  it("retains local state and the exact Awareness object across reconnect and reannounces it", () => {
    const test = fixture();
    const state = {
      ghostprovV2Supervisor: { direction: "host", sequence: 4 },
      ghostprovV2SupervisorPublication: { sequence: 4, attempt: 2 },
    };
    try {
      const awareness = test.provider.awareness;
      test.provider.awareness.setLocalState(state);
      const clock = awareness.meta.get(LOCAL_ID)!.clock;
      test.clear();
      test.provider.disconnect();
      expect(test.sent).toHaveLength(1);
      expect(readStates(test.sent[0]!)[0]).toMatchObject({ clientId: LOCAL_ID, clock, state: null });
      expect(awareness.getLocalState()).toEqual(state);
      test.clear();
      test.provider.connect();
      test.open();
      expect(test.sockets).toHaveLength(2);
      expect(test.provider.awareness).toBe(awareness);
      expect(test.provider.doc).toBe(test.doc);
      const packets = test.sent.filter(packet => packet[0] === 1);
      expect(packets).toHaveLength(1);
      expect(readStates(packets[0]!)[0]).toMatchObject({ clientId: LOCAL_ID, clock, state });
    } finally { test.close(); }
  });

  it("unregisters its replacement broadcaster when the provider is destroyed", () => {
    const test = fixture();
    try {
      const replacement = test.provider._awarenessUpdateHandler;
      const observers = (test.provider.awareness as unknown as {
        _observers: Map<string, Set<unknown>>;
      })._observers;
      expect(observers.get("update")?.has(test.originalHandler)).toBe(false);
      expect(observers.get("update")?.has(replacement)).toBe(true);
      test.provider.destroy();
      expect(observers.get("update")?.has(replacement) ?? false).toBe(false);
    } finally { test.close(); }
  });
});
