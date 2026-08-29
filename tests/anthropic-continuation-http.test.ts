import { describe, expect, it } from "bun:test";
import { Hono } from "hono";
import {
  registerAnthropicRoutes,
  type AnthropicRelayAdmission,
} from "../src/anthropic/routes";
import { AnthropicContinuationIndex } from "../src/anthropic/continuation";
import { createGhostProvApp } from "../src/server/app";
import type { IncomingResourceLease } from "../src/router/router";
import type { RunHandle } from "../src/relay/run-handle";
import type { SseFrame } from "../src/relay/sse";
import { createProviderErrorEnvelope } from "../src/v2/provider-error";
import { V2CommittedError } from "../src/v2/transport/run-handle";
import { classifyWorkload } from "../src/router/workload";
import {
  finalResponse,
  messageOutput,
  refusalPart,
  reasoningOutput,
  streamFrame,
  textPart,
  toolOutput,
} from "./support/anthropic-stream";

import { createAuthenticatedTenantScope, type AuthenticatedTenantScope } from "../src/http/api-key";

const HEADERS = {
  "content-type": "application/json",
  "anthropic-version": "2023-06-01",
} as const;
const MODELS = {
  opus: "gpt-5.6-sol",
  sonnet: "gpt-5.6-terra",
  haiku: "gpt-5.6-luna",
  fallback: "gpt-5.6-sol",
  passthrough: new Set(["gpt-5.6-sol", "gpt-5.6-terra", "gpt-5.6-luna"]),
};

type RelayFactory = (
  body: Record<string, unknown>,
  call: number,
  signal: AbortSignal,
  admission?: AnthropicRelayAdmission,
) => Promise<RunHandle> | RunHandle;

function completed(id: string, text: string) {
  return finalResponse(id, [messageOutput(`msg_${id}`, [textPart(text)])]);
}

function relayHandle(
  final: unknown | Promise<unknown>,
  events: () => AsyncGenerator<SseFrame> = async function* () {},
  upstreamPrelude?: Promise<unknown>,
): RunHandle {
  const stats: RunHandle["stats"] = { bytes: 0, firstByteMs: null, frames: 0 };
  const settledFinal = Promise.resolve(final).then(value => {
    stats.bytes = Buffer.byteLength(JSON.stringify(value), "utf8");
    return value as Record<string, unknown>;
  });
  return {
    events,
    final: settledFinal,
    ...(upstreamPrelude ? { upstreamPrelude: upstreamPrelude as Promise<any> } : {}),
    stats,
  };
}

const SCOPE = createAuthenticatedTenantScope();

function testApp(
  factory: RelayFactory,
  options: {
    scope?: AuthenticatedTenantScope | null;
    continuationIndex?: AnthropicContinuationIndex;
  } = { scope: SCOPE },
): {
  app: Hono;
  calls: Record<string, unknown>[];
  admissions: Array<AnthropicRelayAdmission | undefined>;
  incomingLeases: Array<IncomingResourceLease | undefined>;
  signals: AbortSignal[];
} {
  const app = new Hono();
  const calls: Record<string, unknown>[] = [];
  const admissions: Array<AnthropicRelayAdmission | undefined> = [];
  const incomingLeases: Array<IncomingResourceLease | undefined> = [];
  const signals: AbortSignal[] = [];
  registerAnthropicRoutes(app, {
    keepAliveMs: 60_000,
    models: MODELS,
    continuationScope: options.scope === null ? undefined : options.scope ?? SCOPE,
    continuationIndex: options.continuationIndex,
    startRelay: async (body, signal, _clientId, incomingLease, admission) => {
      calls.push(structuredClone(body));
      admissions.push(admission);
      incomingLeases.push(incomingLease);
      signals.push(signal);
      return factory(body, calls.length, signal, admission);
    },
  });
  return { app, calls, admissions, incomingLeases, signals };
}

function request(
  messages: unknown[],
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
    model: "claude-sonnet-4-6",
    max_tokens: 64,
    messages,
    ...overrides,
  };
}

async function post(
  app: Hono,
  body: Record<string, unknown>,
  path = "/v1/messages",
  env: Record<string, unknown> = {},
): Promise<Response> {
  return app.request(path, {
    method: "POST",
    headers: HEADERS,
    body: JSON.stringify(body),
  }, env);
}

async function waitFor(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("timed out waiting for continuation state");
    await Bun.sleep(5);
  }
}

function assistantContent(text: string): unknown[] {
  return [{ type: "text", text, citations: null }];
}

function responseEvents(value: string): Array<Record<string, unknown>> {
  return value.split("\n")
    .filter(line => line.startsWith("data: "))
    .map(line => JSON.parse(line.slice("data: ".length)) as Record<string, unknown>);
}

async function previousResponseNotFound(
  origin: "http" | "sse" = "http",
  status = 400,
  ambiguous = false,
) {
  const providerError = await createProviderErrorEnvelope({
    origin,
    status,
    value: {
      error: {
        message: "Previous response was not found",
        type: "invalid_request_error",
        param: "previous_response_id",
        code: "previous_response_not_found",
      },
    },
  });
  return new V2CommittedError(
    providerError.error.message,
    providerError.error.code ?? "provider_error",
    ambiguous,
    undefined,
    providerError,
  );
}

describe("Anthropic persisted continuation HTTP integration", () => {
  it("shares one index across stable and beta JSON routes", async () => {
    const { app, calls, admissions } = testApp((_body, call) =>
      relayHandle(completed(`resp_${call}`, call === 1 ? "first answer" : "second answer")));

    const first = await post(app, request([{ role: "user", content: "first" }]));
    expect(first.status).toBe(200);
    const firstMessage = await first.json() as any;
    const second = await post(app, request([
      { role: "user", content: "first" },
      { role: "assistant", content: firstMessage.content },
      { role: "user", content: "second" },
    ], {
      context_management: { edits: [{ type: "clear_tool_uses_20250919" }] },
    }), "/v1/messages?beta=true");

    expect(second.status).toBe(200);
    expect(calls).toHaveLength(2);
    expect(calls[0]).toMatchObject({ store: true, stream: true });
    expect(calls[1]).toMatchObject({
      store: true,
      stream: true,
      previous_response_id: "resp_1",
      model: "gpt-5.6-terra",
      max_output_tokens: 64,
      include: ["reasoning.encrypted_content"],
      input: [{
        type: "message",
        role: "user",
        content: [{ type: "input_text", text: "second" }],
      }],
    });
    expect(admissions[0]?.fairnessBytes).toBeUndefined();
    expect(admissions[1]?.fairnessBytes).toBeGreaterThan(
      Buffer.byteLength(JSON.stringify(calls[1]!.input), "utf8"),
    );
  });

  it("commits a successful full request after an ambiguous prefix", async () => {
    const { app, calls } = testApp((_body, call) => relayHandle(completed(
      `resp_ambiguous_${call}`,
      call <= 2 ? "same answer" : call === 3 ? "fresh answer" : "done",
    )));
    const root = { role: "user", content: "root" };

    await post(app, request([root]));
    await post(app, request([root]));
    await post(app, request([
      root,
      { role: "assistant", content: assistantContent("same answer") },
      { role: "user", content: "next" },
    ]));
    await post(app, request([
      root,
      { role: "assistant", content: assistantContent("same answer") },
      { role: "user", content: "next" },
      { role: "assistant", content: assistantContent("fresh answer") },
      { role: "user", content: "final" },
    ]));

    expect(calls).toHaveLength(4);
    expect(calls[2]).not.toHaveProperty("previous_response_id");
    expect(calls[2]!.input).toHaveLength(3);
    expect(calls[3]).toMatchObject({
      previous_response_id: "resp_ambiguous_3",
      input: [{ type: "message", role: "user" }],
    });
  });

  it("releases full ingress ownership after fallback transport handoff", async () => {
    const missing = await previousResponseNotFound();
    let resolveFallback!: (value: Record<string, unknown>) => void;
    const fallback = new Promise<Record<string, unknown>>(resolve => {
      resolveFallback = resolve;
    });
    const { app, calls, admissions, incomingLeases } = testApp((_body, call) => {
      if (call === 1) return relayHandle(completed("resp_parent", "parent"));
      if (call === 2) throw missing;
      return relayHandle(fallback);
    });
    await post(app, request([{ role: "user", content: "root" }]));

    const continuedBody = request([
      { role: "user", content: "root" },
      { role: "assistant", content: assistantContent("parent") },
      { role: "user", content: "next" },
    ]);
    let releases = 0;
    let admissionsCompleted = 0;
    const encodedBytes = Buffer.byteLength(JSON.stringify(continuedBody), "utf8");
    const ingressAbort = new AbortController();
    const ingressLease: IncomingResourceLease = {
      bodyBytes: encodedBytes,
      reservedBytes: 64 * 1024 * 1024,
      acceptedAtMs: 123,
      deadlineMs: 456,
      signal: ingressAbort.signal,
      completeAdmission: () => { admissionsCompleted++; },
      release: () => { releases++; },
    };
    const pending = post(app, continuedBody, "/v1/messages", {
      incomingBodyLease: ingressLease,
    });
    await waitFor(() => calls.length === 3);

    expect(admissionsCompleted).toBe(1);
    expect(releases).toBe(1);
    expect(incomingLeases.slice(1)).toEqual([
      expect.objectContaining({
        acceptedAtMs: 123,
        deadlineMs: 456,
        signal: ingressAbort.signal,
      }),
      expect.objectContaining({
        acceptedAtMs: 123,
        deadlineMs: 456,
        signal: ingressAbort.signal,
      }),
    ]);
    expect(admissions[1]?.fairnessBytes).toBeGreaterThanOrEqual(encodedBytes);
    expect(calls[1]).toHaveProperty("previous_response_id", "resp_parent");
    expect(calls[2]).not.toHaveProperty("previous_response_id");

    resolveFallback(completed("resp_fallback", "fallback"));
    expect((await pending).status).toBe(200);
    expect(releases).toBe(1);
  });

  it("continues a Claude Code tool turn with only the tool-result suffix", async () => {
    const parent = finalResponse("resp_tool_parent", [
      reasoningOutput("rs_tool", "checking", "encrypted-tool-reasoning"),
      toolOutput("fc_tool", "call_tool", "Read", "{\"path\":\"package.json\"}"),
    ]);
    const { app, calls } = testApp((_body, call) =>
      relayHandle(call === 1 ? parent : completed("resp_tool_child", "done")));
    const first = await post(app, request([{ role: "user", content: "read package" }]));
    const firstMessage = await first.json() as any;

    const second = await post(app, request([
      { role: "user", content: "read package" },
      { role: "assistant", content: firstMessage.content },
      {
        role: "user",
        content: [{ type: "tool_result", tool_use_id: "call_tool", content: "{}" }],
      },
    ]));
    expect(second.status).toBe(200);
    expect(calls[1]).toMatchObject({
      store: true,
      previous_response_id: "resp_tool_parent",
      input: [{
        type: "function_call_output",
        call_id: "call_tool",
        output: "{}",
      }],
    });
    expect(JSON.stringify(calls[1])).not.toContain("encrypted-tool-reasoning");
    expect(JSON.stringify(calls[1])).not.toContain("package.json");
  });

  it("commits JSON metadata before the lazy response body is consumed", async () => {

    const { app, calls } = testApp((_body, call) =>
      relayHandle(completed(`resp_${call}`, call === 1 ? "answer" : "next")));
    const first = await post(app, request([{ role: "user", content: "root" }]));
    expect(first.status).toBe(200);

    const second = await post(app, request([
      { role: "user", content: "root" },
      { role: "assistant", content: assistantContent("answer") },
      { role: "user", content: "next" },
    ]));
    expect(second.status).toBe(200);
    expect(calls[1]).toHaveProperty("previous_response_id", "resp_1");
    await first.body?.cancel();
  });

  it("indexes the provider-only phase emitted by live message outputs", async () => {
    const phased = {
      ...messageOutput("msg_resp_phase", [textPart("answer")]),
      phase: "final_answer" as const,
    };
    const { app, calls } = testApp((_body, call) => relayHandle(
      call === 1
        ? finalResponse("resp_phase", [phased])
        : completed("resp_phase_child", "next"),
    ));
    const first = await post(app, request([{ role: "user", content: "root" }]));
    const firstMessage = await first.json() as any;

    const second = await post(app, request([
      { role: "user", content: "root" },
      { role: "assistant", content: firstMessage.content },
      { role: "user", content: "next" },
    ]));

    expect(second.status).toBe(200);
    expect(calls[1]).toMatchObject({
      previous_response_id: "resp_phase",
      input: [{
        type: "message",
        role: "user",
        content: [{ type: "input_text", text: "next" }],
      }],
    });
  });

  it("keeps continuation indexes app-scoped", async () => {
    const first = testApp(() => relayHandle(completed("resp_app_a", "answer")));
    const second = testApp(() => relayHandle(completed("resp_app_b", "next")));
    await post(first.app, request([{ role: "user", content: "root" }]));
    await post(second.app, request([
      { role: "user", content: "root" },
      { role: "assistant", content: assistantContent("answer") },
      { role: "user", content: "next" },
    ]));

    expect(second.calls[0]).not.toHaveProperty("previous_response_id");
    expect(second.calls[0].input).toHaveLength(3);
  });

  it("derives one router scope and uses the injected app-scoped index", async () => {
    const scope = createAuthenticatedTenantScope();
    const continuationIndex = new AnthropicContinuationIndex();
    const calls: Array<{
      body: Record<string, unknown>;
      scope: AuthenticatedTenantScope | undefined;
    }> = [];
    const router = {
      responseProvenanceAuthorizationScope: () => scope,
      snapshot: () => ({
        capacity: 1,
        active: 0,
        available: 1,
        queued: 0,
        runtimes: [],
        resources: {},
      }),
      startRelay: async (
        body: Record<string, unknown>,
        options: { authorizationScope?: AuthenticatedTenantScope } = {},
      ) => {
        calls.push({ body: structuredClone(body), scope: options.authorizationScope });
        return relayHandle(completed(
          `resp_app_scope_${calls.length}`,
          calls.length === 1 ? "answer" : "next",
        ));
      },
    };
    const app = createGhostProvApp({
      router,
      anthropicModels: MODELS,
      anthropicContinuationIndex: continuationIndex,
    });

    await post(app, request([{ role: "user", content: "root" }]));
    const continued = await post(app, request([
      { role: "user", content: "root" },
      { role: "assistant", content: assistantContent("answer") },
      { role: "user", content: "next" },
    ]));

    expect(continued.status).toBe(200);
    expect(calls).toHaveLength(2);
    expect(calls.map(call => call.scope)).toEqual([scope, scope]);
    expect(calls[1]!.body).toMatchObject({
      stream: true,
      store: true,
      previous_response_id: "resp_app_scope_1",
      input: [{ type: "message", role: "user" }],
    });
    expect(continuationIndex.snapshot().records).toBe(2);
  });

  it("commits a reconciled stream before message_stop without changing terminal order", async () => {
    const parent = completed("resp_parent", "parent answer");
    const streamed = completed("resp_streamed", "streamed answer");
    const { app, calls } = testApp((_body, call) => {
      if (call === 1) return relayHandle(parent);
      if (call === 2) {
        return relayHandle(streamed, async function* () {
          yield streamFrame("response.completed", { response: streamed });
        }, Promise.resolve({ status: 200 }));
      }
      return relayHandle(completed("resp_after", "after"));
    });

    await post(app, request([{ role: "user", content: "root" }]));
    const stream = await post(app, request([
      { role: "user", content: "root" },
      { role: "assistant", content: assistantContent("parent answer") },
      { role: "user", content: "stream next" },
    ], { stream: true }), "/v1/messages?beta=true");
    expect(stream.status).toBe(200);
    const reader = stream.body!.getReader();
    const decoder = new TextDecoder();
    const types: unknown[] = [];
    while (types.at(-1) !== "message_delta") {
      const chunk = await reader.read();
      expect(chunk.done).toBe(false);
      const event = responseEvents(decoder.decode(chunk.value))[0];
      if (event) types.push(event.type);
    }
    expect(types).not.toContain("message_stop");

    // The next request starts while message_stop is still queued. A hit proves
    // the stream callback committed after reconciliation but before message_stop.
    const after = await post(app, request([
      { role: "user", content: "root" },
      { role: "assistant", content: assistantContent("parent answer") },
      { role: "user", content: "stream next" },
      { role: "assistant", content: assistantContent("streamed answer") },
      { role: "user", content: "after stream" },
    ]));
    expect(after.status).toBe(200);
    const stopChunk = await reader.read();
    expect(responseEvents(decoder.decode(stopChunk.value))[0]?.type).toBe("message_stop");
    expect((await reader.read()).done).toBe(true);
    expect(calls[1]).toMatchObject({
      previous_response_id: "resp_parent",
      input: [{ type: "message", role: "user" }],
    });
    expect(calls[2]).toMatchObject({
      previous_response_id: "resp_streamed",
      input: [{
        type: "message",
        role: "user",
        content: [{ type: "input_text", text: "after stream" }],
      }],
    });
  });

  it("withholds every public terminal chunk while the cooperative stream commit is pending", async () => {
    const continuationIndex = new AnthropicContinuationIndex();
    const originalCommit = continuationIndex.commitAsync.bind(continuationIndex);
    let releaseCommit!: () => void;
    let markCommitStarted!: () => void;
    const commitGate = new Promise<void>(resolve => { releaseCommit = resolve; });
    const commitStarted = new Promise<void>(resolve => { markCommitStarted = resolve; });
    continuationIndex.commitAsync = async (...args) => {
      if (args[2] === "resp_barrier_stream") {
        markCommitStarted();
        await commitGate;
      }
      return originalCommit(...args);
    };
    const parent = completed("resp_barrier_parent", "parent answer");
    const streamed = completed("resp_barrier_stream", "streamed answer");
    const { app } = testApp((_body, call) => {
      if (call === 1) return relayHandle(parent);
      return relayHandle(streamed, async function* () {
        yield streamFrame("response.completed", { response: streamed });
      }, Promise.resolve({ status: 200 }));
    }, { continuationIndex });

    await post(app, request([{ role: "user", content: "root" }]));
    const response = await post(app, request([
      { role: "user", content: "root" },
      { role: "assistant", content: assistantContent("parent answer") },
      { role: "user", content: "next" },
    ], { stream: true }));
    const reader = response.body!.getReader();
    expect(responseEvents(new TextDecoder().decode((await reader.read()).value))[0]?.type)
      .toBe("message_start");

    let readSettled = false;
    const blockedRead = reader.read().then(value => {
      readSettled = true;
      return value;
    });
    await commitStarted;
    await Bun.sleep(0);
    expect(readSettled).toBe(false);

    releaseCommit();
    const decoder = new TextDecoder();
    const types: unknown[] = [];
    let chunk = await blockedRead;
    while (!chunk.done) {
      types.push(...responseEvents(decoder.decode(chunk.value)).map(event => event.type));
      chunk = await reader.read();
    }
    expect(types.at(-1)).toBe("message_stop");
    expect(continuationIndex.snapshot().records).toBe(2);
  });

  it("does not reuse state without a trusted server-derived scope", async () => {
    const { app, calls } = testApp((_body, call) =>
      relayHandle(completed(`resp_${call}`, call === 1 ? "answer" : "next")), { scope: null });
    await post(app, request([{ role: "user", content: "root" }]));
    await post(app, request([
      { role: "user", content: "root" },
      { role: "assistant", content: assistantContent("answer") },
      { role: "user", content: "next" },
    ]));

    expect(calls[1]).not.toHaveProperty("previous_response_id");
    expect(calls[1].input).toHaveLength(3);
  });

  it("never indexes requests with stop sequences or max_tokens zero", async () => {
    const stopped = testApp((_body, call) =>
      relayHandle(completed(`resp_stop_${call}`, call === 1 ? "answer" : "next")));
    await post(stopped.app, request([{ role: "user", content: "root" }], {
      stop_sequences: ["UNSEEN_STOP"],
    }));
    await post(stopped.app, request([
      { role: "user", content: "root" },
      { role: "assistant", content: assistantContent("answer") },
      { role: "user", content: "next" },
    ], { stop_sequences: ["UNSEEN_STOP"] }));
    expect(stopped.calls[1]).not.toHaveProperty("previous_response_id");

    const prewarmFinal = finalResponse("resp_prewarm", [
      messageOutput("msg_prewarm", [textPart("hidden")], "incomplete"),
    ], {
      status: "incomplete",
      incomplete_details: { reason: "max_output_tokens" },
    });
    const prewarm = testApp((_body, call) =>
      relayHandle(call === 1 ? prewarmFinal : completed("resp_next", "next")));
    await post(prewarm.app, request([{ role: "user", content: "root" }], {
      max_tokens: 0,
    }));
    await post(prewarm.app, request([
      { role: "user", content: "root" },
      { role: "assistant", content: [] },
      { role: "user", content: "next" },
    ], { max_tokens: 0 }));
    expect(prewarm.calls[1]).not.toHaveProperty("previous_response_id");
  });

  it("skips commits for refusal, incomplete, and missing-reasoning-ciphertext terminals", async () => {
    const terminals = [
      finalResponse("resp_refusal", [
        messageOutput("msg_refusal", [refusalPart("declined")]),
      ]),
      finalResponse("resp_incomplete", [
        messageOutput("msg_incomplete", [textPart("partial")], "incomplete"),
      ], {
        status: "incomplete",
        incomplete_details: { reason: "max_output_tokens" },
      }),
      finalResponse("resp_missing_cipher", [
        { type: "reasoning", id: "rs_missing", status: "completed", summary: [] },
        messageOutput("msg_visible", [textPart("visible")]),
      ]),
    ];

    for (const terminal of terminals) {
      const visible = terminal === terminals[0]
        ? "declined"
        : terminal === terminals[1]
          ? "partial"
          : "visible";
      const { app, calls } = testApp((_body, call) =>
        relayHandle(call === 1 ? terminal : completed("resp_next", "next")));
      const first = await post(app, request([{ role: "user", content: "root" }]));
      expect(first.status).toBe(200);
      await first.text();
      await post(app, request([
        { role: "user", content: "root" },
        { role: "assistant", content: assistantContent(visible) },
        { role: "user", content: "next" },
      ]));
      expect(calls[1]).not.toHaveProperty("previous_response_id");
      expect(calls[1].input).toHaveLength(3);
    }
  });

  it("requires successful streaming terminal proof before committing", async () => {
    const hidden = completed("resp_hidden", "must not commit");
    const { app, calls } = testApp((_body, call) => {
      if (call === 1) {
        return relayHandle(hidden, async function* () {
          yield streamFrame("response.failed", {
            response: { id: "resp_hidden", status: "failed", error: { message: "failed" } },
          });
        }, Promise.resolve({ status: 200 }));
      }
      return relayHandle(completed("resp_next", "next"));
    });

    const failed = await post(app, request([{ role: "user", content: "root" }], {
      stream: true,
    }));
    expect(await failed.text()).toContain("event: error");
    await post(app, request([
      { role: "user", content: "root" },
      { role: "assistant", content: assistantContent("must not commit") },
      { role: "user", content: "next" },
    ]));
    expect(calls[1]).not.toHaveProperty("previous_response_id");
  });

  it("retains a streaming miss graph until the handler clears its prelude", async () => {
    let releasePrelude!: () => void;
    const prelude = new Promise<void>(resolve => { releasePrelude = resolve; });
    const terminal = completed("resp_streaming_miss", "answer");
    const { app, admissions } = testApp(() => relayHandle(
      terminal,
      async function* () {
        yield streamFrame("response.completed", { response: terminal });
      },
      prelude,
    ));

    const pending = post(app, request([{ role: "user", content: "root" }], {
      stream: true,
    }));
    await waitFor(() => admissions.length === 1);
    const lifetime = admissions[0]?.retainMemoryUntil;
    expect(lifetime).toBeDefined();
    let settled = false;
    void lifetime!.then(() => { settled = true; });
    await Bun.sleep(0);
    expect(settled).toBe(false);

    releasePrelude();
    const response = await pending;
    expect(response.status).toBe(200);
    await lifetime;
    expect(settled).toBe(true);
    await response.body?.cancel();
  });

  it("retries one definitive HTTP previous_response_not_found with full stored input", async () => {
    const missing = await previousResponseNotFound();
    const { app, calls, signals } = testApp((_body, call) => {
      if (call === 1) return relayHandle(completed("resp_parent", "parent"));
      if (call === 2) throw missing;
      return relayHandle(completed(`resp_${call}`, call === 3 ? "fallback" : "fork"));
    });

    await post(app, request([{ role: "user", content: "root" }]));
    const continued = await post(app, request([
      { role: "user", content: "root" },
      { role: "assistant", content: assistantContent("parent") },
      { role: "user", content: "next" },
    ]));
    expect(continued.status).toBe(200);
    expect(calls).toHaveLength(3);
    expect(calls[1]).toMatchObject({
      store: true,
      previous_response_id: "resp_parent",
      input: [{ type: "message", role: "user" }],
    });
    expect(calls[2]).toMatchObject({ store: true });
    expect(calls[2]).not.toHaveProperty("previous_response_id");
    expect(calls[2].input).toHaveLength(3);
    expect(signals[1].aborted).toBe(true);
    expect(signals[2].aborted).toBe(false);

    await post(app, request([
      { role: "user", content: "root" },
      { role: "assistant", content: assistantContent("parent") },
      { role: "user", content: "different fork" },
    ]));
    expect(calls[3]).not.toHaveProperty("previous_response_id");
  });

  it("sizes a tiny stored suffix separately from its multi-megabyte fallback", async () => {
    const MiB = 1_024 * 1_024;
    const largePrefix = `prefix-${"x".repeat(2 * MiB)}`;
    const missing = await previousResponseNotFound();
    const { app, calls, admissions } = testApp((_body, call) => {
      if (call === 1) return relayHandle(completed("resp_large_parent", "parent"));
      if (call === 2) throw missing;
      return relayHandle(completed("resp_large_fallback", "fallback"));
    });

    const first = await post(app, request([{ role: "user", content: largePrefix }]));
    expect(first.status).toBe(200);
    const fullRequest = request([
      { role: "user", content: largePrefix },
      { role: "assistant", content: assistantContent("parent") },
      { role: "user", content: "tiny suffix" },
    ]);
    const continued = await post(app, fullRequest);

    expect(continued.status).toBe(200);
    expect(calls).toHaveLength(3);
    expect(calls[1]).toMatchObject({
      previous_response_id: "resp_large_parent",
      input: [{ type: "message", role: "user" }],
    });
    expect(calls[2]).not.toHaveProperty("previous_response_id");
    const suffixBytes = Buffer.byteLength(JSON.stringify(calls[1]), "utf8");
    const fallbackBytes = Buffer.byteLength(JSON.stringify(calls[2]), "utf8");
    const inboundBytes = Buffer.byteLength(JSON.stringify(fullRequest), "utf8");
    expect(admissions[1]?.structuralBodyBytes).toBe(suffixBytes);
    expect(admissions[2]?.structuralBodyBytes).toBe(fallbackBytes);
    expect(suffixBytes).toBeLessThan(16 * 1_024);
    expect(fallbackBytes).toBeGreaterThan(2 * MiB);
    expect(classifyWorkload(calls[1]!, {
      structuralBytes: admissions[1]?.structuralBodyBytes,
    }).class).toBe("text-small");
    expect(classifyWorkload(calls[2]!, {
      structuralBytes: admissions[2]?.structuralBodyBytes,
    }).class).toBe("text-large");
    expect(admissions[1]?.fairnessBytes).toBeGreaterThanOrEqual(fallbackBytes);
    expect(admissions[1]?.fairnessBytes).toBeGreaterThanOrEqual(inboundBytes);
    expect(admissions[2]?.fairnessBytes).toBeGreaterThanOrEqual(fallbackBytes);
    expect(admissions[2]?.fairnessBytes).toBeGreaterThanOrEqual(inboundBytes);
  });

  it("does not replay an ambiguous committed rejection that retains its HTTP envelope", async () => {
    const unproven = await previousResponseNotFound("http", 400, true);
    const { app, calls } = testApp((_body, call) => {
      if (call === 1) return relayHandle(completed("resp_parent", "parent"));
      throw unproven;
    });

    await post(app, request([{ role: "user", content: "root" }]));
    const continued = await post(app, request([
      { role: "user", content: "root" },
      { role: "assistant", content: assistantContent("parent") },
      { role: "user", content: "next" },
    ]));

    expect(continued.status).toBe(400);
    expect(calls).toHaveLength(2);
    expect(calls[1]).toHaveProperty("previous_response_id", "resp_parent");
  });

  it("never makes a third attempt when the full fallback also fails", async () => {
    const missing = await previousResponseNotFound();
    const { app, calls } = testApp((_body, call) => {
      if (call === 1) return relayHandle(completed("resp_parent", "parent"));
      throw missing;
    });
    await post(app, request([{ role: "user", content: "root" }]));
    const response = await post(app, request([
      { role: "user", content: "root" },
      { role: "assistant", content: assistantContent("parent") },
      { role: "user", content: "next" },
    ]));

    expect(response.status).toBe(400);
    expect(calls).toHaveLength(3);
    expect(calls[1]).toHaveProperty("previous_response_id", "resp_parent");
    expect(calls[2]).not.toHaveProperty("previous_response_id");
  });

  it("does not retry transport, generic, SSE-origin, or nondefinitive failures", async () => {
    const cases: Array<[string, () => Promise<unknown> | unknown]> = [
      ["transport", () => new Error("previous_response_not_found")],
      ["generic", () => createProviderErrorEnvelope({
        origin: "http",
        status: 400,
        value: { error: { message: "bad", type: "invalid_request_error", param: null, code: "bad_request" } },
      }).then(providerError => Object.assign(new Error("bad"), { providerError }))],
      ["sse-origin", () => previousResponseNotFound("sse")],
      ["server-status", () => previousResponseNotFound("http", 500)],
    ];

    for (const [name, makeError] of cases) {
      const failure = await makeError();
      const { app, calls } = testApp((_body, call) => {
        if (call === 1) return relayHandle(completed("resp_parent", "parent"));
        throw failure;
      });
      await post(app, request([{ role: "user", content: "root" }]));
      const response = await post(app, request([
        { role: "user", content: "root" },
        { role: "assistant", content: assistantContent("parent") },
        { role: "user", content: "next" },
      ]));
      expect(response.status, name).toBeGreaterThanOrEqual(400);
      expect(calls, name).toHaveLength(2);
    }
  });

  it("does not retry a definitive error after any nonstream relay event", async () => {
    const missing = await previousResponseNotFound();
    const { app, calls } = testApp((_body, call) => {
      if (call === 1) return relayHandle(completed("resp_parent", "parent"));
      let rejectFinal!: (error: unknown) => void;
      const final = new Promise((_resolve, reject) => { rejectFinal = reject; });
      return relayHandle(final, async function* () {
        yield streamFrame("response.created", { response: { id: "pending" } });
        rejectFinal(missing);
      });
    });
    await post(app, request([{ role: "user", content: "root" }]));
    const response = await post(app, request([
      { role: "user", content: "root" },
      { role: "assistant", content: assistantContent("parent") },
      { role: "user", content: "next" },
    ]));

    expect(response.status).toBe(400);
    expect(calls).toHaveLength(2);
  });

  it("does not retry while nonstream event absence remains unproven", async () => {
    const missing = await previousResponseNotFound();
    const { app, calls } = testApp((_body, call) => {
      if (call === 1) return relayHandle(completed("resp_parent", "parent"));
      return relayHandle(Promise.reject(missing), async function* () {
        await Bun.sleep(100);
        yield streamFrame("response.created", { response: { id: "late" } });
      });
    });
    await post(app, request([{ role: "user", content: "root" }]));
    const response = await post(app, request([
      { role: "user", content: "root" },
      { role: "assistant", content: assistantContent("parent") },
      { role: "user", content: "next" },
    ]));

    expect(response.status).toBe(400);
    expect(calls).toHaveLength(2);
  });

  it("does not retry a definitive error after the outer SSE response commits", async () => {
    const missing = await previousResponseNotFound();
    const { app, calls } = testApp((_body, call) => {
      if (call === 1) return relayHandle(completed("resp_parent", "parent"));
      return relayHandle(new Promise(() => undefined), async function* () {
        throw missing;
      }, Promise.resolve({ status: 200 }));
    });
    await post(app, request([{ role: "user", content: "root" }]));
    const response = await post(app, request([
      { role: "user", content: "root" },
      { role: "assistant", content: assistantContent("parent") },
      { role: "user", content: "next" },
    ], { stream: true }));

    expect(response.status).toBe(200);
    expect(await response.text()).toContain("event: error");
    expect(calls).toHaveLength(2);
  });

  it("retries a definitive stream prelude failure before committing SSE", async () => {
    const missing = await previousResponseNotFound();
    const fallback = completed("resp_fallback", "fallback");
    const { app, calls, admissions } = testApp((_body, call) => {
      if (call === 1) return relayHandle(completed("resp_parent", "parent"));
      if (call === 2) {
        return relayHandle(Promise.reject(missing), async function* () {}, Promise.reject(missing));
      }
      return relayHandle(fallback, async function* () {
        yield streamFrame("response.completed", { response: fallback });
      }, Promise.resolve({ status: 200 }));
    });
    await post(app, request([{ role: "user", content: "root" }]));
    const response = await post(app, request([
      { role: "user", content: "root" },
      { role: "assistant", content: assistantContent("parent") },
      { role: "user", content: "next" },
    ], { stream: true }));

    expect(response.status).toBe(200);
    expect(await response.text()).toContain("event: message_stop");
    expect(calls).toHaveLength(3);
    expect(calls[1]).toHaveProperty("previous_response_id", "resp_parent");
    expect(calls[2]).not.toHaveProperty("previous_response_id");
    expect(calls[2].input).toHaveLength(3);
    expect(admissions[2]?.retainMemoryUntil).toBeDefined();
  });
});
