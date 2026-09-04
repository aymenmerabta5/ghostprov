import { describe, expect, it } from "bun:test";
import type { AccountRequestClient } from "../src/prism/provisioning-client";
import {
  CodexExecError,
  collectCodexTranscript,
  runCodexCommand,
  scrubCodexText,
} from "../src/prism/codex-exec";
import { projectId } from "../src/sandbox/types";

const PROJECT = projectId("34d230c3-6185-4187-807e-0f48703bc974");

interface Call {
  path: string;
  method: string;
  body: unknown;
}

class ScriptedAccount implements AccountRequestClient {
  readonly calls: Call[] = [];

  constructor(private readonly script: Array<Response | Error>) {}

  async request(pathOrUrl: string, init: RequestInit = {}): Promise<Response> {
    const url = new URL(pathOrUrl, "https://prism.test/");
    this.calls.push({
      path: url.pathname,
      method: (init.method ?? "GET").toUpperCase(),
      body: typeof init.body === "string" ? JSON.parse(init.body) : null,
    });
    const next = this.script.shift();
    if (!next) throw new Error(`missing response for ${url.pathname}`);
    if (next instanceof Error) throw next;
    return next;
  }
}

function json(status: number, body: unknown): Response {
  return Response.json(body, { status });
}

function baseInput(overrides: Record<string, unknown> = {}) {
  return {
    account: null as unknown as AccountRequestClient,
    projectId: PROJECT,
    sandboxUrl: "https://prism.test/s/sandboxes/proxy/",
    sandboxToken: "sandbox-token",
    command: "echo HELLO-BLOCK",
    requestTimeoutMs: 1_000,
    pollIntervalMs: 1,
    ...overrides,
  };
}

function started(turnState: unknown = { version: 1 }) {
  return json(200, { status: "started", request_id: "req-1", turn_state: turnState });
}

function pending(turnState: unknown = { version: 2 }) {
  return json(200, { status: "pending", request_id: "req-1", turn_state: turnState, codex_live_progress: { eventPreviews: [] } });
}

function completedText(text: string) {
  return json(200, {
    status: "completed",
    request_id: "req-1",
    codex_live_progress: {
      eventPreviews: [
        {
          summary: "message",
          raw: JSON.stringify({
            type: "response_item",
            payload: {
              type: "message",
              content: [{ type: "output_text", text }],
            },
          }),
        },
      ],
    },
  });
}

describe("codex-exec client", () => {
  it("runs a command through start then status polling", async () => {
    const account = new ScriptedAccount([started(), pending(), completedText("EXEC-PROBE-OK")]);
    const result = await runCodexCommand({ ...baseInput(), account });
    expect(result.transcript).toBe("EXEC-PROBE-OK");
    expect(result.requestId).toBe("req-1");
    expect(result.polls).toBe(2);
    expect(account.calls.map(call => call.path)).toEqual([
      "/api/llm/response_with_tools_start",
      "/api/llm/response_with_tools_status",
      "/api/llm/response_with_tools_status",
    ]);
    const startBody = account.calls[0]!.body as Record<string, unknown>;
    expect(startBody.metadata).toMatchObject({ projectId: PROJECT });
    const statusBody = account.calls[1]!.body as Record<string, unknown>;
    expect(statusBody).toMatchObject({ request_id: "req-1", turn_state: { version: 1 } });
    // The session token must travel to the first-party API (as the observed
    // client does); it must never surface in errors or logs instead.
    const startMeta = (account.calls[0]!.body as Record<string, unknown>).metadata as Record<string, unknown>;
    expect(startMeta.sandbox_token).toBe("sandbox-token");
  });

  it("returns synchronously completed turns without polling", async () => {
    const account = new ScriptedAccount([completedText("SYNC-OK")]);
    const result = await runCodexCommand({ ...baseInput(), account });
    expect(result).toMatchObject({ transcript: "SYNC-OK", polls: 0 });
    expect(account.calls).toHaveLength(1);
  });

  it("restarts turns that end before running with sandbox_reconnecting", async () => {
    const account = new ScriptedAccount([
      json(200, { status: "completed", request_id: "req-1", response: { status: "error", payload: { reason: "sandbox_reconnecting" } } }),
      started(),
      completedText("RETRY-OK"),
    ]);
    const result = await runCodexCommand({ ...baseInput(), account });
    expect(result.transcript).toBe("RETRY-OK");
    expect(account.calls.filter(call => call.path.endsWith("start"))).toHaveLength(2);
  });

  it("retries start only on transport failure, then gives up bounded", async () => {
    const failure = new TypeError("fetch failed");
    const account = new ScriptedAccount([failure, failure, failure]);
    const error = await runCodexCommand({ ...baseInput(), account, maxStartAttempts: 3 }).catch(error => error);
    expect(error).toBeInstanceOf(CodexExecError);
    expect((error as CodexExecError).kind).toBe("transport");
    expect(account.calls).toHaveLength(3);
  });

  it("does not retry start after an HTTP failure", async () => {
    const account = new ScriptedAccount([json(401, { error: "nope" })]);
    const error = await runCodexCommand({ ...baseInput(), account }).catch(error => error);
    expect(error).toBeInstanceOf(CodexExecError);
    expect((error as CodexExecError).kind).toBe("auth");
    expect(account.calls).toHaveLength(1);
  });

  it("maps rate limits and times out exhausted polls", async () => {
    const limited = new ScriptedAccount([json(429, { error: "slow down" })]);
    const limitedError = await runCodexCommand({ ...baseInput(), account: limited }).catch(error => error);
    expect((limitedError as CodexExecError).kind).toBe("rate_limited");

    const hanging = new ScriptedAccount([started(), pending(), pending(), pending()]);
    const timeout = await runCodexCommand({ ...baseInput(), account: hanging, maxPolls: 3 }).catch(error => error);
    expect((timeout as CodexExecError).kind).toBe("timeout");
  });

  it("rejects empty transcripts and invalid options", async () => {
    const empty = new ScriptedAccount([started(), completedText("   ")]);
    const emptyError = await runCodexCommand({ ...baseInput(), account: empty }).catch(error => error);
    expect((emptyError as CodexExecError).kind).toBe("turn_failed");

    const valid = new ScriptedAccount([completedText("x")]);
    await expect(runCodexCommand({ ...baseInput(), account: valid, command: "" })).rejects.toMatchObject({ name: "CodexExecError" });
    await expect(runCodexCommand({ ...baseInput(), account: valid, maxPolls: 0 })).rejects.toMatchObject({ name: "CodexExecError" });
    await expect(runCodexCommand({ ...baseInput(), account: valid, sandboxToken: "" })).rejects.toMatchObject({ name: "CodexExecError" });
  });

  it("honors abort signals", async () => {
    const controller = new AbortController();
    controller.abort(new Error("stop now"));
    const account = new ScriptedAccount([completedText("x")]);
    await expect(runCodexCommand({ ...baseInput(), account, signal: controller.signal })).rejects.toThrow("stop now");
    expect(account.calls).toHaveLength(0);
  });

  it("scrubs credential-shaped text from error surfaces", async () => {
    expect(scrubCodexText("Bearer sk-abcdefghij1234567890 and eyJhbGciOiJIUzI1NiJ9.payload rest")).not.toMatch(/sk-abcdefghij|eyJhbGci/);
    expect(collectCodexTranscript({ codex_live_progress: { eventPreviews: [{ raw: "not-json" }] } })).toBe("");
    const nested = { a: [{ type: "output_text", text: "one" }, { type: "output_text", text: "two" }] };
    expect(collectCodexTranscript(nested)).toBe("one\ntwo");
    const leaked = new ScriptedAccount([
      started(),
      json(200, { status: "failed", request_id: "req-1", detail: "token sandbox-token-leak-check eyJhbGciOiJIUzI1NiJ9" }),
    ]);
    const failed = await runCodexCommand({ ...baseInput(), account: leaked }).catch(error => error);
    expect(String((failed as Error).message)).not.toMatch(/sandbox-token-leak-check|eyJhbGci/);
  });
});
