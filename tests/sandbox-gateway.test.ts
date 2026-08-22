import { describe, expect, it } from "bun:test";
import { zipSync } from "fflate";
import { SandboxGateway } from "../src/sandbox/gateway";
import { parseProvisionedSandbox } from "../src/prism/protocol";
import { projectId } from "../src/sandbox/types";
import type { AccountRequestClient } from "../src/prism/provisioning-client";

const PROJECT = projectId("e631d12c-d917-4434-b360-41b4715d6679");
const SANDBOX = parseProvisionedSandbox({
  url: "/s/sandboxes/proxy",
  token: "sandbox-A",
  sandbox_id: "sandbox-a",
});

class FakeAccount implements AccountRequestClient {
  calls: Array<{ path: string; method: string; headers: Headers; json: unknown }> = [];
  activeZip = 0;
  maxZipActive = 0;
  zipCalls = 0;
  constructor(readonly handler: (path: string, init: RequestInit) => Promise<Response> | Response) {}
  async request(pathOrUrl: string, init: RequestInit = {}): Promise<Response> {
    const url = new URL(pathOrUrl, "https://prism.test");
    const call = {
      path: url.pathname,
      method: (init.method ?? "GET").toUpperCase(),
      headers: new Headers(init.headers),
      json: typeof init.body === "string" ? JSON.parse(init.body) : undefined,
    };
    this.calls.push(call);
    if (url.pathname.endsWith("/zip")) {
      this.zipCalls++;
      this.activeZip++;
      this.maxZipActive = Math.max(this.maxZipActive, this.activeZip);
      try { return await this.handler(url.pathname, init); }
      finally { this.activeZip--; }
    }
    return this.handler(url.pathname, init);
  }
}

describe("SandboxGateway", () => {
  it("sends the runtime-specific sandbox token on direct requests", async () => {
    const account = new FakeAccount(() => Response.json({ ok: true }, {
      headers: { "x-session-id": "session-1" },
    }));
    const gateway = new SandboxGateway({ projectId: PROJECT, account, sandbox: SANDBOX, zipBatchMs: 0 });

    const result = await gateway.heartbeat();

    expect(account.calls[0].path).toBe("/s/sandboxes/proxy/heartbeat");
    expect(account.calls[0].headers.get("x-crixet-sandbox-token")).toBe("sandbox-A");
    expect(result).toMatchObject({ ok: true, status: 200, sessionId: "session-1" });
  });

  it("coalesces named ZIP reads and extracts only requested files", async () => {
    const archive = zipSync({
      "a.txt": new TextEncoder().encode("A"),
      "b.txt": new TextEncoder().encode("B"),
      "ignored.txt": new TextEncoder().encode("ignored"),
    });
    const account = new FakeAccount(async path => {
      if (path.endsWith("/zip")) return new Response(archive as unknown as BodyInit);
      return new Response("not found", { status: 404 });
    });
    const gateway = new SandboxGateway({ projectId: PROJECT, account, sandbox: SANDBOX, zipBatchMs: 0 });

    const [left, right] = await Promise.all([
      gateway.readFiles(["a.txt"]),
      gateway.readFiles(["b.txt"]),
    ]);

    expect(new TextDecoder().decode(left.get("a.txt"))).toBe("A");
    expect(new TextDecoder().decode(right.get("b.txt"))).toBe("B");
    expect(account.zipCalls).toBe(1);
    expect(account.maxZipActive).toBe(1);
  });

  it("serializes archive fetches created in separate batch windows", async () => {
    const archive = zipSync({ "a": new Uint8Array([1]) });
    let releaseFirst!: () => void;
    const firstGate = new Promise<void>(resolve => { releaseFirst = resolve; });
    const account = new FakeAccount(async path => {
      if (!path.endsWith("/zip")) return new Response("not found", { status: 404 });
      if (account.zipCalls === 1) await firstGate;
      return new Response(archive as unknown as BodyInit);
    });
    const gateway = new SandboxGateway({ projectId: PROJECT, account, sandbox: SANDBOX, zipBatchMs: 0 });

    const first = gateway.readFiles(["a"]);
    await new Promise(resolve => setTimeout(resolve, 5));
    const second = gateway.readFiles(["a"]);
    await new Promise(resolve => setTimeout(resolve, 5));
    expect(account.maxZipActive).toBe(1);
    releaseFirst();
    await Promise.all([first, second]);
    expect(account.zipCalls).toBe(2);
    expect(account.maxZipActive).toBe(1);
  });

  it("posts render and delete payloads through the same scoped gateway", async () => {
    const account = new FakeAccount(path => new Response("{}", {
      status: path.endsWith("/render") ? 202 : 200,
    }));
    const gateway = new SandboxGateway({ projectId: PROJECT, account, sandbox: SANDBOX });
    await gateway.triggerRender({ mainDocument: "main.tex", clientStateVector: "state", clientDeleteSetUpdate: "delete" });
    await gateway.deleteFile("old.txt");
    expect(account.calls.at(-2)?.json).toEqual({ mainDocument: "main.tex", clientStateVector: "state", clientDeleteSetUpdate: "delete" });
    expect(account.calls.at(-1)?.json).toEqual({ filename: "old.txt" });
  });
});
