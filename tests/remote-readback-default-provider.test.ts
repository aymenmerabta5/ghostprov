import { expect, it } from "bun:test";
import { once } from "node:events";
import { WebSocketServer } from "ws";
import * as Y from "yjs";
import * as decoding from "lib0/decoding";
import * as encoding from "lib0/encoding";
import { SandboxYDoc, remoteReadbackMemorySnapshot } from "../src/sandbox/ydoc";
import { parseYSweetEnvelope } from "../src/prism/protocol";
import { projectId } from "../src/sandbox/types";
import { loadV2SidecarAssets } from "../src/v2/sidecar/assets";

it("verifies five staged paths through the actual default provider and a local Yjs server", async () => {
  const serverDoc = new Y.Doc();
  const server = new WebSocketServer({ host: "127.0.0.1", port: 0, maxPayload: 8 * 1024 * 1024 });
  let emptyQueries = 0, lastQueryNodeCount = 0, serverErrors = 0, reserved = 0, released = 0;
  server.on("connection", socket => {
    socket.on("message", data => {
      try {
        const raw = data instanceof ArrayBuffer ? new Uint8Array(data) : Array.isArray(data) ? Buffer.concat(data) : new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
        const decoder = decoding.createDecoder(raw);
        if (decoding.readVarUint(decoder) !== 0) return; // Awareness does not prove document receipt.
        const kind = decoding.readVarUint(decoder), payload = decoding.readVarUint8Array(decoder);
        if (kind === 0) {
          if (Y.decodeStateVector(payload).size === 0) {
            emptyQueries++; lastQueryNodeCount = serverDoc.getMap("content").size;
            if (emptyQueries === 2) {
              serverDoc.getMap("settings").set("unrelated_server_metadata", "later");
              for (const node of serverDoc.getMap<Y.Map<unknown>>("content").values()) node.set("unrelated_metadata", true);
            }
          }
          const reply = encoding.createEncoder();
          encoding.writeVarUint(reply, 0); encoding.writeVarUint(reply, 1);
          encoding.writeVarUint8Array(reply, Y.encodeStateAsUpdate(serverDoc, payload));
          socket.send(encoding.toUint8Array(reply));
        } else if (kind === 1 || kind === 2) Y.applyUpdate(serverDoc, payload, socket);
      } catch { serverErrors++; }
    });
  });
  await once(server, "listening");
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Missing ephemeral server address");
  const id = projectId("11111111-1111-4111-8111-111111111111");
  const doc = new SandboxYDoc({ projectId: id, syncTimeoutMs: 1500,
    mintYSweet: async () => parseYSweetEnvelope({ url: `ws://127.0.0.1:${address.port}`, baseUrl: `http://127.0.0.1:${address.port}/`, docId: id, token: "offline-fixture-token", authorization: "full" }),
    reserveRemoteReadbackBytes: bytes => { reserved += bytes; return { release: () => { released += bytes; } }; },
  });
  try {
    const assets = await loadV2SidecarAssets({ revision: "r10" });
    const bundle = assets.files.find(value => value.path === assets.entrypointPath)!;
    const expected = [
      { path: bundle.path, content: bundle.content },
      { path: ".ghostprov-v2/supervisor/static.json", content: '{"offline":true}' },
      { path: "exec", content: "#!/bin/sh\nprintf offline\n" },
      { path: ".ghostprov-v2/direct-exec/install.sh", content: "#!/bin/sh\nexit 0\n" },
      { path: ".ghostprov-v2/supervisor/bootstrap.enc.json", content: '{"sealed":"offline-fixture"}' },
    ];
    await doc.connect();
    for (const file of expected) await doc.writePath(file.path, file.content);
    const receipt = await doc.verifyRemotePaths(expected, AbortSignal.timeout(3000));
    expect(() => receipt.assertCurrent()).not.toThrow();
    expect(emptyQueries).toBe(2); // Initial sync, then the explicit empty-vector publication query.
    expect(lastQueryNodeCount).toBeGreaterThan(5);
    expect(serverErrors).toBe(0);
    expect(reserved).toBe(128 * 1024 * 1024); expect(released).toBe(reserved);
    expect(remoteReadbackMemorySnapshot().activePermits).toBe(0);
    await doc.destroy();
    expect(() => receipt.assertCurrent()).toThrow();
  } finally {
    await doc.destroy();
    for (const socket of server.clients) socket.terminate();
    await new Promise<void>(resolve => { server.close(() => resolve()); });
    serverDoc.destroy();
  }
});
