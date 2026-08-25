import { describe, expect, it } from "bun:test";
import * as Y from "yjs";
import {
  SandboxYDoc,
  YDocSessionError,
  type YProviderEvent,
  type YProviderFactory,
  type YProviderLike,
} from "../src/sandbox/ydoc";
import { parseYSweetEnvelope, type YSweetEnvelope } from "../src/prism/protocol";
import { projectId, type ProjectId } from "../src/sandbox/types";

const A = projectId("e631d12c-d917-4434-b360-41b4715d6679");
const B = projectId("69288e8b-c183-45f3-9131-d64b05b98928");

class FakeProvider implements YProviderLike {
  private listeners = new Map<YProviderEvent, Array<(...args: any[]) => void>>();
  destroyed = false;
  on(event: YProviderEvent, listener: (...args: any[]) => void): void {
    const values = this.listeners.get(event) ?? [];
    values.push(listener);
    this.listeners.set(event, values);
  }
  emit(event: YProviderEvent, ...args: any[]): void {
    for (const listener of this.listeners.get(event) ?? []) listener(...args);
  }
  destroy(): void { this.destroyed = true; }
}

class FakeProviderFactory implements YProviderFactory {
  providers: FakeProvider[] = [];
  tokens: string[] = [];
  docs: Y.Doc[] = [];
  constructor(
    private readonly autoSync = true,
    private readonly seedRoot = true,
    private readonly closeFirstAfterSync = false,
  ) {}
  create(envelope: YSweetEnvelope, doc: Y.Doc): YProviderLike {
    this.tokens.push(envelope.token.reveal());
    this.docs.push(doc);
    if (this.seedRoot) {
      const content = doc.getMap("content");
      const root = new Y.Map();
      root.set("id", `root-${envelope.docId}`);
      root.set("filename", "root");
      root.set("type", "folder");
      root.set("deleted", false);
      content.set(`root-${envelope.docId}`, root);
    }
    const provider = new FakeProvider();
    this.providers.push(provider);
    if (this.autoSync) {
      const closeAfterSync = this.closeFirstAfterSync && this.providers.length === 1;
      queueMicrotask(() => {
        provider.emit("sync", true);
        if (closeAfterSync) provider.emit("connection-close");
      });
    }
    return provider;
  }
}

function envelopes(project: ProjectId, ...tokens: string[]) {
  let index = 0;
  return async () => parseYSweetEnvelope({
    url: `wss://prism.test/y/d/${project}/ws`,
    baseUrl: `https://prism.test/y/d/${project}`,
    docId: project,
    token: tokens[Math.min(index++, tokens.length - 1)],
    authorization: "full",
  });
}

function createDoc(project: ProjectId, factory = new FakeProviderFactory(), ...tokens: string[]) {
  return {
    doc: new SandboxYDoc({
      projectId: project,
      mintYSweet: envelopes(project, ...(tokens.length ? tokens : [`token-${project}`])),
      providerFactory: factory,
      syncTimeoutMs: 100,
      appendDelayMs: 0,
    }),
    factory,
  };
}

describe("SandboxYDoc", () => {
  it("keeps filename indexes and document state isolated by project", async () => {
    const left = createDoc(A);
    const right = createDoc(B);
    await Promise.all([left.doc.connect(), right.doc.connect()]);

    await left.doc.writeFile("job_s1.json", "left");
    await right.doc.writeFile("job_s1.json", "right");

    expect(await left.doc.readFile("job_s1.json")).toBe("left");
    expect(await right.doc.readFile("job_s1.json")).toBe("right");
    expect(left.doc.snapshot().projectId).toBe(A);
    expect(right.doc.snapshot().projectId).toBe(B);
  });

  it("reconnects with a new Y-Sweet token after provider close", async () => {
    const factory = new FakeProviderFactory();
    const { doc } = createDoc(A, factory, "token-1", "token-2");
    await doc.connect();
    factory.providers[0].emit("connection-close");

    expect(doc.snapshot().connected).toBe(false);

    await doc.writeFile("after-close.txt", "ready");

    expect(factory.tokens).toEqual(["token-1", "token-2"]);
    expect(await doc.readFile("after-close.txt")).toBe("ready");
    expect(doc.snapshot().connected).toBe(true);
  });

  it("does not report a local write as successful when its provider disconnects", async () => {
    const factory = new FakeProviderFactory();
    const { doc } = createDoc(A, factory, "token-1", "token-2");
    await doc.connect();
    factory.docs[0].once("update", () => {
      factory.providers[0].emit("connection-close");
    });

    await expect(doc.writeFile("offline.txt", "not-proven")).rejects.toMatchObject({
      name: "YDocSessionError",
      kind: "sync",
    } satisfies Partial<YDocSessionError>);
    expect(doc.snapshot().connected).toBe(false);

    await doc.writeFile("after-remint.txt", "proven");
    expect(factory.tokens).toEqual(["token-1", "token-2"]);
    expect(await doc.readFile("after-remint.txt")).toBe("proven");
  });

  it("does not install a provider that closes synchronously after sync", async () => {
    const factory = new FakeProviderFactory(true, true, true);
    const { doc } = createDoc(A, factory, "token-1", "token-2");

    await expect(doc.connect()).rejects.toMatchObject({
      name: "YDocSessionError",
      kind: "sync",
    } satisfies Partial<YDocSessionError>);
    expect(factory.providers[0].destroyed).toBe(true);
    expect(doc.snapshot().connected).toBe(false);

    await doc.connect();
    expect(factory.tokens).toEqual(["token-1", "token-2"]);
    expect(doc.snapshot().connected).toBe(true);
  });

  it("creates folders/files once and updates existing Y.Text content", async () => {
    const { doc } = createDoc(A);
    await doc.connect();
    const folder = await doc.ensureFolder("gp");
    expect(await doc.ensureFolder("gp")).toBe(folder);

    await doc.writeFile("request.json", "one", { folderId: folder });
    await doc.writeFile("request.json", "two", { folderId: folder });

    expect(await doc.readFile("request.json")).toBe("two");
    expect((await doc.listFiles()).filter(file => file.filename === "request.json")).toHaveLength(1);
  });

  it("creates, reads, updates, and tombstones normalized nested paths", async () => {
    const factory = new FakeProviderFactory(true, false);
    const { doc } = createDoc(A, factory);
    await doc.connect();

    const first = await doc.ensurePath(".ghostprov-v2/src/main.ts");
    const second = await doc.ensurePath(".ghostprov-v2/src/main.ts");
    expect(second).toEqual(first);
    await doc.writePath(".ghostprov-v2/src/main.ts", "one");
    await doc.writePath(".ghostprov-v2/src/main.ts", "two");
    expect(await doc.readPath(".ghostprov-v2/src/main.ts")).toBe("two");

    const entries = [...factory.docs[0].getMap<Y.Map<any>>("content").values()];
    expect(entries.filter(entry => entry.get("filename") === ".ghostprov-v2")).toHaveLength(1);
    expect(entries.filter(entry => entry.get("filename") === "src")).toHaveLength(1);
    expect(entries.filter(entry => entry.get("filename") === "main.ts")).toHaveLength(1);

    expect(await doc.tombstonePath(".ghostprov-v2/src/main.ts")).toBe(true);
    expect(await doc.readPath(".ghostprov-v2/src/main.ts")).toBeNull();

    await doc.writePath("same/same", "repeated component");
    expect(await doc.readPath("same/same")).toBe("repeated component");
    expect(await doc.tombstonePath("same/same")).toBe(true);
  });

  it("recursively tombstones an obsolete managed asset directory", async () => {
    const factory = new FakeProviderFactory(true, false);
    const { doc } = createDoc(A, factory);
    await doc.connect();
    await doc.writePath(".ghostprov-v2/src/main.ts", "main");
    await doc.writePath(".ghostprov-v2/src/nested/helper.ts", "helper");

    expect(await doc.tombstonePath(".ghostprov-v2/src")).toBe(true);

    expect(await doc.readPath(".ghostprov-v2/src/main.ts")).toBeNull();
    expect(await doc.readPath(".ghostprov-v2/src/nested/helper.ts")).toBeNull();
    const entries = [...factory.docs[0].getMap<Y.Map<any>>("content").values()];
    for (const name of ["src", "main.ts", "nested", "helper.ts"]) {
      expect(entries.find(entry => entry.get("filename") === name)?.get("deleted")).toBe(true);
    }
  });

  it("rejects path traversal, absolute paths, empty components, and node collisions", async () => {
    const { doc } = createDoc(A);
    await doc.connect();

    for (const invalid of ["/absolute", "../escape", "a//b", "a/./b", "a\\b", "a/\0b", ""]) {
      await expect(doc.ensurePath(invalid)).rejects.toMatchObject({ kind: "missing" });
    }

    await doc.writePath("collision", "file");
    await expect(doc.ensurePath("collision/child.ts")).rejects.toThrow("not a folder");
    await doc.ensurePath("folder/child.ts");
    await expect(doc.ensurePath("folder")).rejects.toThrow("folder");
  });

  it("initializes an empty project root once and attaches new entries to it", async () => {
    const factory = new FakeProviderFactory(true, false);
    const { doc } = createDoc(A, factory);
    await doc.connect();

    await doc.writeFile("managed-asset.txt", "asset");
    const folder = await doc.ensureFolder("gp");
    expect(await doc.ensureFolder("gp")).toBe(folder);

    const entries = [...factory.docs[0].getMap<Y.Map<any>>("content").values()];
    const roots = entries.filter(entry => entry.get("filename") === "root" && entry.get("type") === "folder");
    const asset = entries.find(entry => entry.get("filename") === "managed-asset.txt");
    const gp = entries.find(entry => entry.get("filename") === "gp");
    expect(roots).toHaveLength(1);
    expect(roots[0].get("id")).toMatch(/^[0-9a-f-]{36}$/i);
    expect(asset?.get("inFolder")).toBe(roots[0].get("id"));
    expect(gp?.get("inFolder")).toBe(roots[0].get("id"));
    expect(factory.docs[0].getMap("settings").get("init")).toBe(true);
    expect(factory.docs[0].getMap("settings").get("deleted")).toBe(false);
  });

  it("derives render synchronization vectors from the current project document", async () => {
    const factory = new FakeProviderFactory(true, false);
    const { doc } = createDoc(A, factory);
    await doc.connect();
    await doc.writeFile("main.tex", "current revision");

    const state = await doc.renderSyncState();
    const expectedVector = Y.encodeStateVector(factory.docs[0]);
    const expectedDeleteSet = Y.encodeStateAsUpdate(factory.docs[0], expectedVector);

    expect([...Buffer.from(state.clientStateVector, "base64")]).toEqual([...expectedVector]);
    expect([...Buffer.from(state.clientDeleteSetUpdate, "base64")]).toEqual([...expectedDeleteSet]);
  });

  it("batch-creates tracked daemon files once under the project root", async () => {
    const factory = new FakeProviderFactory(true, false);
    const { doc } = createDoc(A, factory);
    await doc.connect();

    const legacy = new Y.Map();
    factory.docs[0].transact(() => {
      legacy.set("id", "legacy-job");
      legacy.set("filename", "job_s1.json");
      legacy.set("deleted", false);
      legacy.set("type", "text");
      legacy.set("content", new Y.Text("legacy"));
      factory.docs[0].getMap("content").set("legacy-job", legacy);
    });

    await doc.ensureFiles(["gp_status.json", "job_s1.json", "resp_s1.json", "resp_s1.json"]);
    await doc.ensureFiles(["gp_status.json", "resp_s1.json"]);

    const entries = [...factory.docs[0].getMap<Y.Map<any>>("content").values()];
    const root = entries.find(entry => entry.get("filename") === "root");
    for (const name of ["gp_status.json", "job_s1.json", "resp_s1.json"]) {
      const matches = entries.filter(entry => entry.get("filename") === name && entry.get("deleted") !== true);
      expect(matches).toHaveLength(1);
      expect(matches[0].get("inFolder")).toBe(root?.get("id"));
      expect(matches[0].get("type")).toBe("text");
      expect(matches[0].get("content")).toBeInstanceOf(Y.Text);
    }
    expect((entries.find(entry => entry.get("filename") === "job_s1.json")?.get("content") as Y.Text).toString()).toBe("legacy");
  });

  it("appends large content in chunks and tombstones deleted entries", async () => {
    const { doc } = createDoc(A);
    await doc.connect();
    await doc.appendFile("large.txt", "abcdefghij", 3);
    expect(await doc.readFile("large.txt")).toBe("abcdefghij");
    expect(await doc.deleteFile("large.txt")).toBe(true);
    expect(await doc.readFile("large.txt")).toBeNull();
    expect((await doc.listFiles()).find(file => file.filename === "large.txt")?.deleted).toBe(true);
  });

  it("ignores a stale close event from a superseded provider", async () => {
    const factory = new FakeProviderFactory();
    const { doc } = createDoc(A, factory, "token-1", "token-2");
    await doc.connect();
    const first = factory.providers[0];
    first.emit("connection-close");
    await doc.connect();
    first.emit("connection-close");
    expect(doc.snapshot().connected).toBe(true);
    expect(factory.tokens).toEqual(["token-1", "token-2"]);
  });

  it("fails a provider that never synchronizes and destroys it", async () => {
    const factory = new FakeProviderFactory(false);
    const { doc } = createDoc(A, factory);
    await expect(doc.connect()).rejects.toBeInstanceOf(YDocSessionError);
    expect(factory.providers[0].destroyed).toBe(true);
    expect(doc.snapshot().connected).toBe(false);
  });

  it("destroys only its own provider and rejects later operations", async () => {
    const factory = new FakeProviderFactory();
    const { doc } = createDoc(A, factory);
    await doc.connect();
    await doc.destroy();
    expect(factory.providers[0].destroyed).toBe(true);
    await expect(doc.readFile("x")).rejects.toMatchObject({ kind: "stopped" });
  });
});
