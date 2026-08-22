import { describe, expect, it } from "bun:test";
import type { AccountRequestClient } from "../src/prism/provisioning-client";
import {
  PrismProjectError,
  PrismProjectsClient,
} from "../src/prism/projects-client";
import { projectId } from "../src/sandbox/types";

const A = "e631d12c-d917-4434-b360-41b4715d6679";
const B = "69288e8b-c183-45f3-9131-d64b05b98928";

interface Call {
  path: string;
  method: string;
  body: unknown;
}

class ScriptedAccount implements AccountRequestClient {
  readonly calls: Call[] = [];

  constructor(private readonly responses: Response[]) {}

  async request(pathOrUrl: string, init: RequestInit = {}): Promise<Response> {
    const url = new URL(pathOrUrl, "https://prism.test/");
    this.calls.push({
      path: `${url.pathname}${url.search}`,
      method: (init.method ?? "GET").toUpperCase(),
      body: typeof init.body === "string" ? JSON.parse(init.body) : null,
    });
    const response = this.responses.shift();
    if (!response) throw new Error(`missing response for ${url.pathname}`);
    return response;
  }
}

function json(status: number, body: unknown): Response {
  return Response.json(body, { status });
}

function project(uuid: string, title: string) {
  return {
    uuid,
    title,
    owner: "owner-1",
    deleted: false,
    created_at: "2026-08-22T10:00:00.000Z",
    public_role: null,
    thumbnail_url: null,
    thumbnail_uuid: null,
  };
}

describe("PrismProjectsClient", () => {
  it("lists and creates projects with the proven Prism contracts", async () => {
    const account = new ScriptedAccount([
      json(200, { projects: [project(A, "GhostProv Worker 1")] }),
      json(200, project(B, "GhostProv Worker 2")),
    ]);
    const client = new PrismProjectsClient(account);

    expect(await client.list()).toEqual([{
      projectId: projectId(A),
      title: "GhostProv Worker 1",
      owner: "owner-1",
      deleted: false,
      createdAt: "2026-08-22T10:00:00.000Z",
    }]);
    expect(await client.create({
      projectId: projectId(B),
      title: "GhostProv Worker 2",
    })).toMatchObject({ projectId: projectId(B), title: "GhostProv Worker 2" });
    expect(account.calls).toEqual([
      { path: "/api/projects?section=your_projects", method: "GET", body: null },
      {
        path: "/api/projects",
        method: "POST",
        body: { project_uuid: B, title: "GhostProv Worker 2", file_uuids: [] },
      },
    ]);
  });

  it("loads metadata and deletes only through the named managed primitive", async () => {
    const account = new ScriptedAccount([
      json(200, { project: project(A, "GhostProv Worker 1") }),
      json(200, { success: true }),
    ]);
    const client = new PrismProjectsClient(account);

    expect(await client.get(projectId(A))).toMatchObject({ projectId: projectId(A) });
    await expect(client.deleteManaged(projectId(A))).resolves.toBeUndefined();
    expect("delete" in client).toBe(false);
    expect(account.calls.map(call => [call.method, call.path])).toEqual([
      ["GET", `/api/projects/${A}`],
      ["DELETE", `/api/projects/${A}`],
    ]);
  });

  it("rejects malformed project payloads at the boundary", async () => {
    const account = new ScriptedAccount([
      json(200, { projects: [{ ...project(A, "Worker"), deleted: "false" }] }),
    ]);
    const client = new PrismProjectsClient(account);

    await expect(client.list()).rejects.toMatchObject({
      name: "PrismProjectError",
      kind: "protocol",
    });
  });

  it("rejects a create response belonging to another project UUID", async () => {
    const account = new ScriptedAccount([
      json(200, project(A, "GhostProv Worker 2")),
    ]);
    const client = new PrismProjectsClient(account);

    await expect(client.create({
      projectId: projectId(B),
      title: "GhostProv Worker 2",
    })).rejects.toMatchObject({ kind: "protocol" });
  });

  it("classifies HTTP failures without exposing response secrets", async () => {
    const secret = "project-api-secret";
    const account = new ScriptedAccount([
      json(429, { error: secret }),
    ]);
    const client = new PrismProjectsClient(account);

    try {
      await client.list();
      throw new Error("expected project request failure");
    } catch (error) {
      expect(error).toBeInstanceOf(PrismProjectError);
      expect(error).toMatchObject({ kind: "http", status: 429 });
      expect(String(error)).not.toContain(secret);
    }
  });
});
