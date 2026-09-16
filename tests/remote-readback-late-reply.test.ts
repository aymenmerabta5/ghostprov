import { expect, it, spyOn } from "bun:test";
import { once } from "node:events";
import WebSocket, { WebSocketServer } from "ws";
import * as Y from "yjs";
import * as decoding from "lib0/decoding";
import * as encoding from "lib0/encoding";
import * as sync from "y-protocols/sync";
import { SandboxYDoc, remoteReadbackMemorySnapshot } from "../src/sandbox/ydoc";
import { parseYSweetEnvelope } from "../src/prism/protocol";
import { projectId } from "../src/sandbox/types";

it.each(["abort", "malformed-reply"] as const)("fences a late full reply before releasing readback permits after %s", async failure => {
  const serverDoc = new Y.Doc();
  const server = new WebSocketServer({ host: "127.0.0.1", port: 0, maxPayload: 8 * 1024 * 1024 });
  let queryEntered!: () => void;
  const queryPending = new Promise<void>(resolve => { queryEntered = resolve; });
  let emptyQueries = 0, serverErrors = 0;
  server.on("connection", socket => {
    socket.on("message", data => {
      try {
        const bytes = data instanceof ArrayBuffer ? new Uint8Array(data) : Array.isArray(data)
          ? Buffer.concat(data) : new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
        const decoder = decoding.createDecoder(bytes);
        if (decoding.readVarUint(decoder) !== 0) return;
        const kind = decoding.readVarUint(decoder), payload = decoding.readVarUint8Array(decoder);
        if (kind === 0) {
          if (Y.decodeStateVector(payload).size === 0 && ++emptyQueries === 2) {
            // Hold the requested full reply until cancellation/failure releases admission.
            queryEntered();
            return;
          }
          const reply = encoding.createEncoder();
          encoding.writeVarUint(reply, 0);
          sync.writeSyncStep2(reply, serverDoc, payload);
          socket.send(encoding.toUint8Array(reply));
        } else if (kind === 1 || kind === 2) Y.applyUpdate(serverDoc, payload, socket);
      } catch { serverErrors++; }
    });
  });
  await once(server, "listening");
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Missing ephemeral server address");
  const url = `ws://127.0.0.1:${address.port}`;
  const id = projectId("11111111-1111-4111-8111-111111111111");
  let nativeSocket: WebSocket | undefined, writerDoc: Y.Doc | undefined;
  const originalSend = WebSocket.prototype.send, originalGetMap = Y.Doc.prototype.getMap;
  const sendSpy = spyOn(WebSocket.prototype, "send").mockImplementation(function (this: WebSocket, ...args: any[]) {
    if (this.url?.startsWith(`${url}/`)) nativeSocket = this;
    return Reflect.apply(originalSend, this, args);
  });
  const getMapSpy = spyOn(Y.Doc.prototype, "getMap").mockImplementation(function (this: Y.Doc, ...args: any[]) {
    if (this.guid === id) writerDoc = this;
    return Reflect.apply(originalGetMap, this, args);
  });
  let activeGovernorLeases = 0, releaseCalls = 0, fencedBeforeRelease = false, destroyedBeforeRelease = false;
  const doc = new SandboxYDoc({ projectId: id, syncTimeoutMs: 1500,
    mintYSweet: async () => parseYSweetEnvelope({ url, baseUrl: `${url.replace("ws:", "http:")}/`, docId: id,
      token: "offline-fixture-token", authorization: "full" }),
    reserveRemoteReadbackBytes: () => {
      activeGovernorLeases++;
      return { release: () => {
        fencedBeforeRelease = nativeSocket !== undefined && nativeSocket.readyState !== WebSocket.OPEN;
        destroyedBeforeRelease = writerDoc?.isDestroyed === true;
        activeGovernorLeases--; releaseCalls++;
      } };
    },
  });
  const cancellation = new AbortController();
  let pending: Promise<unknown> | undefined;
  try {
    await doc.connect();
    await doc.writePath("exec", "staged bootstrap");
    pending = doc.verifyRemotePaths([{ path: "exec", content: "staged bootstrap" }], cancellation.signal)
      .then(() => "unexpected success", error => error);
    await queryPending;
    const socket = nativeSocket!, local = writerDoc!;
    expect(socket.readyState).toBe(WebSocket.OPEN);
    expect(activeGovernorLeases).toBe(1);
    expect(remoteReadbackMemorySnapshot().activePermits).toBe(1);
    const ordinaryMessageHandler = socket.onmessage;
    expect(typeof ordinaryMessageHandler).toBe("function");
    // A copied EventEmitter/onmessage callback can outlive listener removal.
    if (failure === "abort") cancellation.abort();
    else socket.emit("message", new Uint8Array([0, 1, 1, 0, 0]), true); // Step2 with trailing bytes.
    expect(await pending).toMatchObject({ name: "YDocSessionError" });
    expect(fencedBeforeRelease).toBe(true);
    expect(destroyedBeforeRelease).toBe(true);
    expect(activeGovernorLeases).toBe(0);
    expect(releaseCalls).toBe(1);
    expect(remoteReadbackMemorySnapshot().activePermits).toBe(0);

    serverDoc.getMap("late").set("marker", "solicited full reply after cancellation");
    const encoder = encoding.createEncoder();
    encoding.writeVarUint(encoder, 0);
    sync.writeSyncStep2(encoder, serverDoc, new Uint8Array([0]));
    const lateReply = encoding.toUint8Array(encoder);
    // Confirm the delayed payload is valid and would apply through the real protocol parser.
    const control = new Y.Doc();
    try {
      const decoder = decoding.createDecoder(lateReply);
      expect(decoding.readVarUint(decoder)).toBe(0);
      sync.readSyncMessage(decoder, encoding.createEncoder(), control, null);
      expect(control.getMap("late").get("marker")).toBe("solicited full reply after cancellation");
    } finally { control.destroy(); }
    const before = Y.encodeStateVector(local);
    ordinaryMessageHandler!.call(socket, { data: lateReply } as WebSocket.MessageEvent);
    expect(Y.encodeStateVector(local)).toEqual(before);
    expect(local.getMap("late").get("marker")).toBeUndefined();
    await expect(doc.verifyRemotePaths([{ path: "exec", content: "staged bootstrap" }])).rejects.toThrow();
    expect(emptyQueries).toBe(2);
    expect(activeGovernorLeases).toBe(0);
    expect(remoteReadbackMemorySnapshot().activePermits).toBe(0);
    expect(serverErrors).toBe(0);
  } finally {
    cancellation.abort();
    await pending;
    await doc.destroy();
    sendSpy.mockRestore(); getMapSpy.mockRestore();
    for (const socket of server.clients) socket.terminate();
    await new Promise<void>(resolve => { server.close(() => resolve()); });
    serverDoc.destroy();
  }
});
