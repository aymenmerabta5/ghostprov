import { describe, expect, it } from "bun:test";
import { createProjectConversation } from "../src/prism/conversation-client";
import type { AccountRequestClient } from "../src/prism/provisioning-client";
import { projectId } from "../src/sandbox/types";

const PROJECT = projectId("11111111-1111-4111-8111-111111111111");
const CONVERSATION = "cdx1_22222222-2222-4222-8222-222222222222";

class Account implements AccountRequestClient {
  readonly calls: Array<{ path: string; init: RequestInit }> = [];
  constructor(private readonly response: Response | Error) {}
  async request(path: string, init: RequestInit = {}): Promise<Response> {
    this.calls.push({ path, init });
    if (this.response instanceof Error) throw this.response;
    return this.response;
  }
}

function action(body: string, status = 200, contentType = "text/x-component"): Response {
  return new Response(body, { status, headers: { "content-type": contentType } });
}

describe("Prism project conversation client", () => {
  it("creates and strictly parses one project conversation", async () => {
    const account = new Account(action(`0:{"a":"$@1"}\n1:"${CONVERSATION}"\n`));
    await expect(createProjectConversation(account, PROJECT)).resolves.toBe(CONVERSATION);
    expect(account.calls).toHaveLength(1);
    expect(account.calls[0]!.path).toBe("/");
    expect(new Headers(account.calls[0]!.init.headers).get("next-action")).toMatch(/^[a-f0-9]{40,64}$/);
    expect(account.calls[0]!.init.body).toBe(JSON.stringify([PROJECT]));
  });

  it("rejects action failures, malformed output, duplicates, and oversized bodies", async () => {
    await expect(createProjectConversation(new Account(action("failure", 500)), PROJECT))
      .rejects.toMatchObject({ kind: "action", status: 500 });
    await expect(createProjectConversation(new Account(action("{}", 200, "application/json")), PROJECT))
      .rejects.toMatchObject({ kind: "protocol" });
    await expect(createProjectConversation(new Account(action(
      `0:"${CONVERSATION}"\n1:"cdx2_33333333-3333-4333-8333-333333333333"\n`,
    )), PROJECT)).rejects.toMatchObject({ kind: "protocol" });
    await expect(createProjectConversation(new Account(action("x".repeat(256 * 1024 + 1))), PROJECT))
      .rejects.toMatchObject({ kind: "protocol" });
  });

  it("maps transport failures without exposing their message", async () => {
    await expect(createProjectConversation(new Account(new Error("sensitive")), PROJECT))
      .rejects.toMatchObject({ kind: "transport", message: "Unable to create a Prism project conversation" });
  });
});
