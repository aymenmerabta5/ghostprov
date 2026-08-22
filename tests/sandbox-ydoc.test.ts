import { describe, expect, it } from "bun:test";
import * as Y from "yjs";
import {
  SandboxYDoc,
  YDocSessionError,
  type YProviderFactory,
  type YProviderLike,
} from "../src/sandbox/ydoc";
import { parseYSweetEnvelope, type YSweetEnvelope } from "../src/prism/protocol";
import { projectId, type ProjectId } from "../src/sandbox/types";

const A = projectId("e631d12c-d917-4434-b360-41b4715d6679");
const B = projectId("69288e8b-c183-45f3-9131-d64b05b98928");

class FakeProvider implements YProviderLike {
  private listeners = new Map<string, Array<(...args: any[]) => void>>();
  destroyed = false;
  on(event: string, listener: (...args: any[]) => void): void {
    const values = this.listeners.get(event) ?? [];
    values.push(listener);
    this.listeners.set(event, values);
  }
  emit(event: string, ...args: any[]): void {
    for (const listener of this.listeners.get(event) ?? []) listener(...args);
  }
  destroy(): void { this.destroyed = true; }
}

class FakeProviderFactory implements YProviderFactory {
  providers: FakeProvider[] = [];
  tokens: string[] = [];
  constructor(private readonly autoSync = true) {}
  create(envelope: YSweetEnvelope, doc: Y.Doc): YProviderLike {
    this.tokens.push(envelope.token.reveal());
    const content = doc.getMap("content");
    const root = new Y.Map();
    root.set("id", `root-${envelope.docId}`);
    root.set("filename", "root");
    root.set("type", "folder");
    root.set("deleted", false);
    content.set(`root-${envelope.docId}`, root);
    const provider = new FakeProvider();
    this.providers.push(provider);
    if (this.autoSync) queueMicrotask(() => provider.emit("sync", true));
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
    factory.providers[0].emit("connectionClose");

    await doc.writeFile("after-close.txt", "ready");

    expect(factory.tokens).toEqual(["token-1", "token-2"]);
    expect(await doc.readFile("after-close.txt")).toBe("ready");
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
    first.emit("connectionClose");
    await doc.connect();
    first.emit("connectionClose");
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
