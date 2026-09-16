import { describe, expect, it, spyOn } from "bun:test";
import * as encoding from "lib0/encoding";
import * as Y from "yjs";
import {
  MAX_REMOTE_PATH_UPDATE_BYTES, RemotePathReadbackError,
  prepareRemotePathExpectations, verifyRemotePathUpdate, type RemotePathReadbackReason,
} from "../src/sandbox/remote-path-readback";

const path = ".ghostprov-v2/revisions/r10/bundle.js", content = "registered bundle\n";
function fixture(value = content) {
  const doc = new Y.Doc(), entries = doc.getMap<Y.Map<unknown>>("content");
  const root = new Y.Map<unknown>();
  root.set("id", "root-id"); root.set("filename", "root"); root.set("type", "folder"); root.set("deleted", false);
  entries.set("root-id", root);
  let parent = "root-id", leaf!: Y.Map<unknown>;
  path.split("/").forEach((segment, index, parts) => {
    const map = new Y.Map<unknown>(), id = `node-${index}`;
    map.set("id", id); map.set("filename", segment); map.set("inFolder", parent); map.set("deleted", false);
    map.set("type", index === parts.length - 1 ? "text" : "folder");
    if (index === parts.length - 1) { map.set("content", new Y.Text(value)); leaf = map; }
    entries.set(id, map); parent = id;
  });
  return { doc, entries, leaf, root };
}
function raw(...values: number[]) { const e = encoding.createEncoder(); values.forEach(value => encoding.writeVarUint(e, value)); return encoding.toUint8Array(e); }
function rejected(operation: () => void, reason: RemotePathReadbackReason) {
  let failure: unknown;
  try { operation(); } catch (error) { failure = error; }
  expect(failure).toBeInstanceOf(RemotePathReadbackError);
  expect((failure as RemotePathReadbackError).reason).toBe(reason);
  expect((failure as Error).message).toBe(reason);
}
const prepared = () => prepareRemotePathExpectations([{ path, content }]);

describe("server publication readback", () => {
  it("local-only writer content is not server proof", () => {
    const writer = fixture(), server = new Y.Doc();
    try {
      rejected(() => verifyRemotePathUpdate(Y.encodeStateAsUpdate(server), prepared(), Y.encodeStateVector(writer.doc)), "stale_snapshot");
      expect((writer.leaf.get("content") as Y.Text).toString()).toBe(content);
    } finally { writer.doc.destroy(); server.destroy(); }
  });
  it("accepts only the real server update after it receives the writer state", () => {
    const writer = fixture(), server = new Y.Doc();
    try {
      Y.applyUpdate(server, Y.encodeStateAsUpdate(writer.doc));
      expect(() => verifyRemotePathUpdate(Y.encodeStateAsUpdate(server), prepared(), Y.encodeStateVector(writer.doc))).not.toThrow();
    } finally { writer.doc.destroy(); server.destroy(); }
  });
  it("accepts bounded ordinary metadata objects and arrays", () => {
    const { doc, leaf } = fixture();
    try {
      leaf.set("metadata", { nested: { enabled: true, labels: ["a", 2, null] }, scale: 1.5 });
      expect(() => verifyRemotePathUpdate(Y.encodeStateAsUpdate(doc), prepared(), Y.encodeStateVector(doc))).not.toThrow();
    } finally { doc.destroy(); }
  });
  it("accepts bounded unrelated shared Y.Array and JSON metadata", () => {
    const { doc, leaf } = fixture();
    try {
      const metadata = new Y.Array<unknown>();
      doc.getMap("settings").set("sharedMetadata", metadata);
      metadata.push(["note", { labels: ["retained", "ready"], flags: { enabled: true } }, 2]);
      leaf.set("metadata", { ordinary: [false, null, { language: "typescript" }] });
      expect(() => verifyRemotePathUpdate(Y.encodeStateAsUpdate(doc), prepared(), Y.encodeStateVector(doc))).not.toThrow();
    } finally { doc.destroy(); }
  });
  it("accepts a server snapshot retaining four old bundles alongside five required paths", () => {
    const bundleBytes = 615_869, currentBundle = "C".repeat(bundleBytes);
    const { doc, entries } = fixture(currentBundle);
    const addFile = (relative: string, body: string) => {
      let parent = "root-id";
      const segments = relative.split("/");
      for (let i = 0; i < segments.length; i++) {
        let map = [...entries.values()].find(value => value.get("inFolder") === parent && value.get("filename") === segments[i]);
        if (!map) {
          const id = `retained-fixture-${entries.size}`;
          map = new Y.Map<unknown>();
          map.set("id", id); map.set("filename", segments[i]); map.set("inFolder", parent); map.set("deleted", false);
          map.set("type", i === segments.length - 1 ? "text" : "folder");
          if (i === segments.length - 1) map.set("content", new Y.Text(body));
          entries.set(id, map);
        }
        parent = map.get("id") as string;
      }
    };
    try {
      const required = [
        { path, content: currentBundle },
        { path: ".ghostprov-v2/supervisor/static.json", content: '{"fixture":true}\n' },
        { path: "exec", content: "#!/bin/sh\nexit 0\n" },
        { path: ".ghostprov-v2/direct-exec/install.sh", content: "#!/bin/sh\n:\n" },
        { path: ".ghostprov-v2/supervisor/bootstrap.enc.json", content: '{"sealed":"offline-fixture"}\n' },
      ];
      for (const file of required.slice(1)) addFile(file.path, file.content);
      for (let i = 1; i <= 4; i++) {
        addFile(`.ghostprov-v2/revisions/r10/${"a".repeat(64)}/${String(i).repeat(64)}/sidecar.bundle.js`, String(i).repeat(bundleBytes));
      }
      const update = Y.encodeStateAsUpdate(doc);
      expect(update.length).toBeGreaterThan(2 * 1024 * 1024);
      expect(update.length).toBeLessThan(8 * 1024 * 1024);
      expect(() => verifyRemotePathUpdate(update, prepareRemotePathExpectations(required), Y.encodeStateVector(doc))).not.toThrow();
    } finally { doc.destroy(); }
  });
  it("requires target clock coverage while permitting later server metadata", () => {
    const { doc, leaf } = fixture();
    try {
      const target = Y.encodeStateVector(doc);
      leaf.set("metadata", { serverObserved: true });
      expect(() => verifyRemotePathUpdate(Y.encodeStateAsUpdate(doc), prepared(), target)).not.toThrow();
      const clock = Y.decodeStateVector(Y.encodeStateVector(doc)).get(doc.clientID)!;
      rejected(() => verifyRemotePathUpdate(Y.encodeStateAsUpdate(doc), prepared(), raw(1, doc.clientID, clock + 1)), "stale_snapshot");
    } finally { doc.destroy(); }
  });
  it("requires exact bytes without trimming or Unicode normalization", () => {
    const { doc } = fixture(`${content} `);
    try { rejected(() => verifyRemotePathUpdate(Y.encodeStateAsUpdate(doc), prepared(), Y.encodeStateVector(doc)), "content_mismatch"); }
    finally { doc.destroy(); }
  });
  it("accepts non-ASCII text with UTF-16 clocks and UTF-8 byte hashes", () => {
    const text = "a\u{1f680}\u00e9", { doc } = fixture(text);
    try { expect(() => verifyRemotePathUpdate(Y.encodeStateAsUpdate(doc), prepareRemotePathExpectations([{ path, content: text }]), Y.encodeStateVector(doc))).not.toThrow(); }
    finally { doc.destroy(); }
  });
  it("rejects tombstoned or missing selected nodes", () => {
    const { doc, leaf } = fixture();
    try { leaf.set("deleted", true); rejected(() => verifyRemotePathUpdate(Y.encodeStateAsUpdate(doc), prepared(), Y.encodeStateVector(doc)), "path_missing"); }
    finally { doc.destroy(); }
  });
  it("rejects duplicate live names and duplicate roots", () => {
    for (const rootDuplicate of [false, true]) {
      const { doc, entries, leaf, root } = fixture();
      try {
        const duplicate = new Y.Map<unknown>();
        for (const [key, value] of (rootDuplicate ? root : leaf).entries()) if (key !== "content") duplicate.set(key, value);
        duplicate.set("id", "duplicate"); entries.set("duplicate", duplicate);
        rejected(() => verifyRemotePathUpdate(Y.encodeStateAsUpdate(doc), prepared(), Y.encodeStateVector(doc)), "path_conflict");
      } finally { doc.destroy(); }
    }
  });
  it("rejects cycles in the live file tree", () => {
    const { doc, entries } = fixture();
    try { entries.get("node-0")!.set("inFolder", "node-1"); rejected(() => verifyRemotePathUpdate(Y.encodeStateAsUpdate(doc), prepared(), Y.encodeStateVector(doc)), "path_conflict"); }
    finally { doc.destroy(); }
  });
  it("rejects foreign shared types and binary metadata before applying", () => {
    for (const value of [new Y.XmlFragment(), new Uint8Array([1, 2])]) {
      const { doc, leaf } = fixture();
      try { leaf.set("metadata", value); rejected(() => verifyRemotePathUpdate(Y.encodeStateAsUpdate(doc), prepared(), Y.encodeStateVector(doc)), "unsupported_update"); }
      finally { doc.destroy(); }
    }
  });
  it("rejects deep metadata before recursive Yjs allocation", () => {
    const { doc, leaf } = fixture();
    try {
      let metadata: unknown = null; for (let i = 0; i < 34; i++) metadata = [metadata];
      leaf.set("metadata", metadata);
      rejected(() => verifyRemotePathUpdate(Y.encodeStateAsUpdate(doc), prepared(), Y.encodeStateVector(doc)), "structure_bound");
    } finally { doc.destroy(); }
  });
  it("rejects a selected metadata object instead of coercing it to text", () => {
    const { doc, leaf } = fixture();
    try {
      leaf.set("content", { toString: content });
      rejected(() => verifyRemotePathUpdate(Y.encodeStateAsUpdate(doc), prepared(), Y.encodeStateVector(doc)), "path_conflict");
    } finally { doc.destroy(); }
  });
  it("always destroys an applied scratch document on success and mismatch", () => {
    const { doc } = fixture(), update = Y.encodeStateAsUpdate(doc), vector = Y.encodeStateVector(doc);
    const destroy = spyOn(Y.Doc.prototype, "destroy");
    try {
      verifyRemotePathUpdate(update, prepared(), vector);
      rejected(() => verifyRemotePathUpdate(update, prepareRemotePathExpectations([{ path, content: "other" }]), vector), "content_mismatch");
      expect(destroy).toHaveBeenCalledTimes(2);
    } finally { destroy.mockRestore(); doc.destroy(); }
  });
});

describe("readback validation bounds", () => {
  it("freezes copied expectation metadata and enforces path/count/byte limits", () => {
    const source = [{ path, content }], result = prepareRemotePathExpectations(source);
    source[0]!.content = "changed";
    expect(result.entries[0]!.bytes).toBe(Buffer.byteLength(content));
    expect(Object.isFrozen(result)).toBe(true); expect(Object.isFrozen(result.entries[0]!.segments)).toBe(true);
    for (const invalid of ["", "/absolute", "a//b", "a/../b", "a\\b", "a\n", "\ud800"]) rejected(() => prepareRemotePathExpectations([{ path: invalid, content }]), "expectations_invalid");
    rejected(() => prepareRemotePathExpectations([{ path, content }, { path, content }]), "expectations_invalid");
    rejected(() => prepareRemotePathExpectations([{ path: "a", content }, { path: "a/b", content }]), "expectations_invalid");
    rejected(() => prepareRemotePathExpectations(Array.from({ length: 9 }, (_, i) => ({ path: `${i}`, content }))), "expectations_bound");
    rejected(() => prepareRemotePathExpectations([{ path, content: "a".repeat(1024 * 1024 + 1) }]), "expectations_bound");
    expect(prepareRemotePathExpectations([{ path: "a", content: "a".repeat(1024 * 1024) }, { path: "b", content: "b".repeat(1024 * 1024) }]).totalBytes).toBe(2 * 1024 * 1024);
    rejected(() => prepareRemotePathExpectations([{ path: "a", content: "a".repeat(1024 * 1024) }, { path: "b", content: "b".repeat(1024 * 1024) }, { path: "c", content: "c" }]), "expectations_bound");
  });
  it("rejects malformed vectors, duplicate clients and vector overflow", () => {
    const { doc } = fixture(), update = Y.encodeStateAsUpdate(doc);
    try {
      rejected(() => verifyRemotePathUpdate(update, prepared(), raw(1)), "vector_invalid");
      rejected(() => verifyRemotePathUpdate(update, prepared(), raw(2, 1, 0, 1, 0)), "vector_invalid");
      rejected(() => verifyRemotePathUpdate(update, prepared(), raw(0, 0)), "vector_invalid");
      rejected(() => verifyRemotePathUpdate(update, prepared(), raw(1025)), "vector_bound");
      rejected(() => verifyRemotePathUpdate(update, prepared(), new Uint8Array(16 * 1024 + 1)), "vector_bound");
    } finally { doc.destroy(); }
  });
  it("checks wire and declared struct/scalar/delete limits before allocation", () => {
    rejected(() => verifyRemotePathUpdate(new Uint8Array(MAX_REMOTE_PATH_UPDATE_BYTES + 1), prepared(), raw(0)), "update_bound");
    rejected(() => verifyRemotePathUpdate(raw(1025), prepared(), raw(0)), "structure_bound");
    rejected(() => verifyRemotePathUpdate(raw(1, 16385), prepared(), raw(0)), "structure_bound");
    // One map scalar ContentAny, parent root 'content', parentSub 'x', then hostile scalar count.
    const encoder = encoding.createEncoder();
    [1, 1, 1, 0, 40, 1].forEach(value => encoding.writeVarUint(encoder, value));
    encoding.writeVarString(encoder, "content"); encoding.writeVarString(encoder, "x"); encoding.writeVarUint(encoder, 16385);
    rejected(() => verifyRemotePathUpdate(encoding.toUint8Array(encoder), prepared(), raw(0)), "structure_bound");
    rejected(() => verifyRemotePathUpdate(raw(0, 1, 1, 16385), prepared(), raw(0)), "structure_bound");
  });
  it("rejects trailing, truncated, skipped and incomplete update histories", () => {
    const { doc } = fixture(), update = Y.encodeStateAsUpdate(doc);
    try {
      rejected(() => verifyRemotePathUpdate(new Uint8Array([...update, 0]), prepared(), raw(0)), "update_invalid");
      rejected(() => verifyRemotePathUpdate(update.subarray(0, -1), prepared(), raw(0)), "update_invalid");
      rejected(() => verifyRemotePathUpdate(raw(1, 1, 1, 1, 0, 1, 0), prepared(), raw(0)), "update_invalid");
      rejected(() => verifyRemotePathUpdate(raw(1, 1, 1, 0, 10, 1, 0), prepared(), raw(0)), "unsupported_update");
    } finally { doc.destroy(); }
  });
  it("rejects pending parent state after a bounded otherwise-valid decode", () => {
    const encoder = encoding.createEncoder();
    [1, 1, 1, 0, 40, 0, 2, 0].forEach(value => encoding.writeVarUint(encoder, value));
    encoding.writeVarString(encoder, "key");
    [1, 120, 0].forEach(value => encoding.writeVarUint(encoder, value));
    rejected(() => verifyRemotePathUpdate(encoding.toUint8Array(encoder), prepared(), raw(0)), "update_invalid");
  });
  it("bounds shared-type nodes before apply", () => {
    const doc = new Y.Doc();
    try {
      const map = doc.getMap("content"); for (let i = 0; i < 4097; i++) map.set(`${i}`, new Y.Map());
      rejected(() => verifyRemotePathUpdate(Y.encodeStateAsUpdate(doc), prepared(), raw(0)), "node_bound");
    } finally { doc.destroy(); }
  });
});
