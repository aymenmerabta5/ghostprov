import { describe, expect, it } from "bun:test";
import * as Y from "yjs";
import {
  ProjectInspectionError,
  YDocProjectInspector,
  type ProjectInspectorProviderFactory,
  type ProjectInspectorProviderLike,
} from "../src/fleet/project-inspector";
import { parseYSweetEnvelope, type YSweetEnvelope } from "../src/prism/protocol";
import { projectId, type ProjectId } from "../src/sandbox/types";

const PROJECT = projectId("11111111-1111-4111-8111-111111111111");

interface Entry {
  path: string;
  content?: string;
}

function projectUpdate(entries: readonly Entry[]): Uint8Array {
  const doc = new Y.Doc();
  const content = doc.getMap("content");
  const folders = new Map<string, string>([["", "root-id"]]);
  const root = new Y.Map();
  root.set("id", "root-id");
  root.set("filename", "root");
  root.set("deleted", false);
  root.set("type", "folder");
  content.set("root-id", root);

  const ensureFolder = (folderPath: string): string => {
    const existing = folders.get(folderPath);
    if (existing) return existing;
    const slash = folderPath.lastIndexOf("/");
    const parentPath = slash < 0 ? "" : folderPath.slice(0, slash);
    const filename = slash < 0 ? folderPath : folderPath.slice(slash + 1);
    const parentId = ensureFolder(parentPath);
    const id = `folder-${folders.size}`;
    const node = new Y.Map();
    node.set("id", id);
    node.set("filename", filename);
    node.set("deleted", false);
    node.set("inFolder", parentId);
    node.set("type", "folder");
    content.set(id, node);
    folders.set(folderPath, id);
    return id;
  };

  for (const [index, entry] of entries.entries()) {
    const slash = entry.path.lastIndexOf("/");
    const folderPath = slash < 0 ? "" : entry.path.slice(0, slash);
    const filename = slash < 0 ? entry.path : entry.path.slice(slash + 1);
    const node = new Y.Map();
    node.set("id", `file-${index}`);
    node.set("filename", filename);
    node.set("deleted", false);
    node.set("inFolder", ensureFolder(folderPath));
    node.set("type", "text");
    const text = new Y.Text();
    text.insert(0, entry.content ?? "");
    node.set("content", text);
    content.set(`file-${index}`, node);
  }
  const update = Y.encodeStateAsUpdate(doc);
  doc.destroy();
  return update;
}

class SeededProvider implements ProjectInspectorProviderLike {
  destroyed = false;

  on(event: string, listener: (...args: any[]) => void): void {
    if (event === "sync") queueMicrotask(() => listener(true));
  }

  destroy(): void {
    this.destroyed = true;
  }
}

class SeededProviderFactory implements ProjectInspectorProviderFactory {
  readonly providers: SeededProvider[] = [];
  writes = 0;

  constructor(readonly update: Uint8Array) {}

  create(_envelope: YSweetEnvelope, doc: Y.Doc): ProjectInspectorProviderLike {
    Y.applyUpdate(doc, this.update);
    doc.on("update", () => { this.writes++; });
    const provider = new SeededProvider();
    this.providers.push(provider);
    return provider;
  }
}

function envelope(
  project: ProjectId,
  authorization = "full",
): YSweetEnvelope {
  return parseYSweetEnvelope({
    url: "wss://example.test/y/ws",
    baseUrl: "https://example.test/y",
    docId: project,
    token: "sensitive-y-token",
    authorization,
  });
}

function createInspector(
  entries: readonly Entry[],
  authorization = "full",
): { inspector: YDocProjectInspector; factory: SeededProviderFactory; minted: ProjectId[] } {
  const factory = new SeededProviderFactory(projectUpdate(entries));
  const minted: ProjectId[] = [];
  const inspector = new YDocProjectInspector({
    mintYSweet: async project => {
      minted.push(project);
      return envelope(project, authorization);
    },
    providerFactory: factory,
    syncTimeoutMs: 100,
  });
  return { inspector, factory, minted };
}

describe("YDocProjectInspector", () => {
  it("parses a valid worker marker without mutating the project document", async () => {
    const marker = JSON.stringify({
      protocol: "gp-awareness-v2",
      workerIndex: 7,
      buildHash: "a".repeat(64),
      createdAtMs: 1_787_400_000_000,
    });
    const { inspector, factory, minted } = createInspector([
      { path: "main.tex", content: "default" },
      { path: ".ghostprov-v2/worker.json", content: marker },
      { path: ".ghostprov-v2/src/v2/protocol/shared.ts", content: "export {};" },
    ]);

    await expect(inspector.inspect(PROJECT)).resolves.toEqual({
      writable: true,
      empty: true,
      marker: { protocol: "gp-awareness-v2", workerIndex: 7 },
    });
    expect(minted).toEqual([PROJECT]);
    expect(factory.writes).toBe(0);
    expect(factory.providers[0]?.destroyed).toBe(true);
  });

  it("requires exact full authorization and reports unrelated live files as non-empty", async () => {
    const { inspector } = createInspector([
      { path: "main.tex" },
      { path: "notes.txt", content: "user content" },
    ], "read-only");

    await expect(inspector.inspect(PROJECT)).resolves.toEqual({
      writable: false,
      empty: false,
      marker: null,
    });
  });

  it.each([
    "not-json",
    JSON.stringify({ protocol: "wrong", workerIndex: 1, buildHash: "a".repeat(64), createdAtMs: 1 }),
    JSON.stringify({ protocol: "gp-awareness-v2", workerIndex: 0, buildHash: "a".repeat(64), createdAtMs: 1 }),
    JSON.stringify({ protocol: "gp-awareness-v2", workerIndex: 1, buildHash: "short", createdAtMs: 1 }),
    JSON.stringify({ protocol: "gp-awareness-v2", workerIndex: 1, buildHash: "a".repeat(64), createdAtMs: 1, secret: "no" }),
  ])("rejects malformed worker marker %s", async marker => {
    const { inspector } = createInspector([
      { path: ".ghostprov-v2/worker.json", content: marker },
    ]);

    await expect(inspector.inspect(PROJECT)).rejects.toMatchObject({
      name: "ProjectInspectionError",
      kind: "marker",
    } satisfies Partial<ProjectInspectionError>);
  });

  it("rejects a Y-Sweet envelope bound to another project", async () => {
    const factory = new SeededProviderFactory(projectUpdate([]));
    const inspector = new YDocProjectInspector({
      mintYSweet: async () => envelope(projectId("22222222-2222-4222-8222-222222222222")),
      providerFactory: factory,
      syncTimeoutMs: 100,
    });

    await expect(inspector.inspect(PROJECT)).rejects.toMatchObject({
      kind: "auth",
    } satisfies Partial<ProjectInspectionError>);
    expect(factory.providers).toHaveLength(0);
  });
});
