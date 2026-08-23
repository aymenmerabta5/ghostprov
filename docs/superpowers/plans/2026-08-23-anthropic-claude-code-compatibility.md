# GhostProv Anthropic and Claude Code Compatibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serve a current Anthropic Messages API and the existing OpenAI API from one GhostProv listener, with stateless reasoning/tool continuity and verified Claude Code 2.1.220 operation.

**Architecture:** Pure northbound adapters translate Anthropic Messages requests into canonical OpenAI Responses bodies and translate the resulting final objects/SSE back into current Anthropic shapes. Every generation remains exactly one job in the existing router; token counting stays local and side-effect free. A disposable forwarding router tests the production adapter on a non-protected port against the already-running listener at `127.0.0.1:18787`.

**Tech Stack:** Bun 1.4.0, TypeScript 7.0.2, Hono 4.6, Bun test with `--isolate`, Anthropic Messages 2023-06-01 wire format, OpenAI Responses API.

## Global Constraints

- Current official Anthropic documentation is authoritative over the reconstructed Claude Code 2.1.87 source.
- Installed Claude Code 2.1.220 is the final behavioral client.
- Never stop, restart, kill, or bind port `18787`; all branch runtime tests use another loopback port.
- Preserve every existing OpenAI Responses, WebSocket, Images, router, V1, and V2 behavior.
- One Anthropic generation creates exactly one existing-router job and uses the same client queue identity as HTTP Responses.
- Keep `store:false` and request `reasoning.encrypted_content` for stateless tool loops.
- Default mappings are Opus→`gpt-5.6-sol`, Sonnet→`gpt-5.6-terra`, and Haiku→`gpt-5.6-luna`.
- Support stable and `?beta=true` Messages/count-token paths; tolerate legacy/private Claude Code beta headers.
- Use pull-driven streaming and concurrently drain V2 events for non-streaming requests.
- Do not log prompts, response text, tool arguments/results, headers, keys, bearer tokens, or encrypted reasoning.
- Write and observe each focused test fail before adding its production behavior.

---

### Task 1: Define Anthropic wire types, errors, model routing, and reasoning envelopes

**Files:**
- Create: `src/anthropic/types.ts`
- Create: `src/anthropic/errors.ts`
- Create: `src/anthropic/model.ts`
- Create: `src/anthropic/reasoning.ts`
- Test: `tests/anthropic-foundations.test.ts`

**Interfaces:**
- Produces: `AnthropicRequestError`, `anthropicErrorResponse`, `anthropicErrorEvent`.
- Produces: `resolveAnthropicModel(requested, options): string`.
- Produces: `encodeReasoningItem(item): string` and `decodeReasoningItem(value): ResponsesReasoningItem | null`.
- Consumes later: `AnthropicMessageRequest`, `AnthropicMessage`, `AnthropicContentBlock`, `AnthropicUsage`.

- [ ] **Step 1: Write failing foundation tests**

```ts
import { describe, expect, it } from "bun:test";
import { resolveAnthropicModel } from "../src/anthropic/model";
import { decodeReasoningItem, encodeReasoningItem } from "../src/anthropic/reasoning";
import { AnthropicRequestError, anthropicErrorResponse } from "../src/anthropic/errors";

const models = {
  opus: "gpt-5.6-sol", sonnet: "gpt-5.6-terra",
  haiku: "gpt-5.6-luna", fallback: "gpt-5.6-sol",
  passthrough: new Set(["gpt-5.6-sol", "gpt-5.6-terra", "gpt-5.6-luna", "gpt-5.5"]),
};

describe("Anthropic foundations", () => {
  it("maps current and dated Claude model names by tier", () => {
    expect(resolveAnthropicModel("claude-opus-4-6", models)).toBe("gpt-5.6-sol");
    expect(resolveAnthropicModel("claude-sonnet-4-6-20260801", models)).toBe("gpt-5.6-terra");
    expect(resolveAnthropicModel("claude-haiku-4-5-20251001", models)).toBe("gpt-5.6-luna");
    expect(resolveAnthropicModel("gpt-5.5", models)).toBe("gpt-5.5");
  });

  it("round-trips only a bounded OpenAI reasoning item", () => {
    const item = { type: "reasoning", id: "rs_1", encrypted_content: "cipher", summary: [] } as const;
    const signature = encodeReasoningItem(item);
    expect(signature.startsWith("ghostprov-r1.")).toBe(true);
    expect(decodeReasoningItem(signature)).toEqual(item);
    expect(decodeReasoningItem("anthropic-native-signature")).toBeNull();
    expect(decodeReasoningItem("ghostprov-r1.not-base64")).toBeNull();
  });

  it("returns current Anthropic error envelopes", async () => {
    const response = anthropicErrorResponse(new AnthropicRequestError(400, "messages is required"), "req_test");
    expect(response.status).toBe(400);
    expect(response.headers.get("request-id")).toBe("req_test");
    expect(await response.json()).toEqual({
      type: "error", error: { type: "invalid_request_error", message: "messages is required" }, request_id: "req_test",
    });
  });
});
```

- [ ] **Step 2: Run the test and verify RED**

Run: `bun test --isolate tests/anthropic-foundations.test.ts`

Expected: FAIL because `src/anthropic/*` does not exist.

- [ ] **Step 3: Add permissive current wire types**

```ts
export type AnthropicContentBlock = { type: string; [key: string]: unknown };
export type AnthropicMessageParam = {
  role: "user" | "assistant";
  content: string | AnthropicContentBlock[];
};
export type AnthropicMessageRequest = {
  model: string;
  max_tokens: number;
  messages: AnthropicMessageParam[];
  stream?: boolean;
  system?: string | Array<{ type: "text"; text: string; [key: string]: unknown }>;
  tools?: Array<Record<string, unknown>>;
  tool_choice?: Record<string, unknown>;
  thinking?: Record<string, unknown>;
  output_config?: Record<string, unknown>;
  [key: string]: unknown;
};
export type AnthropicUsage = {
  input_tokens: number;
  output_tokens: number;
  cache_creation_input_tokens: number | null;
  cache_read_input_tokens: number | null;
};
```

- [ ] **Step 4: Implement typed errors and current status mapping**

```ts
import { normalizeApiError } from "../http/errors";

export class AnthropicRequestError extends Error {
  constructor(readonly status: number, message: string, readonly errorType = "invalid_request_error") {
    super(message); this.name = "AnthropicRequestError";
  }
}

export function anthropicErrorType(status: number): string {
  if (status === 400) return "invalid_request_error";
  if (status === 401) return "authentication_error";
  if (status === 402) return "billing_error";
  if (status === 403) return "permission_error";
  if (status === 404) return "not_found_error";
  if (status === 413) return "request_too_large";
  if (status === 429) return "rate_limit_error";
  if (status === 504) return "timeout_error";
  if (status === 529 || status === 503) return "overloaded_error";
  return "api_error";
}
```

`anthropicErrorResponse` must use an `AnthropicRequestError` directly, otherwise call `normalizeApiError`, map 503 to wire status 529, attach `request-id`, and return the documented envelope. `anthropicErrorEvent` returns the same envelope without an HTTP response.

- [ ] **Step 5: Implement model mapping and the versioned reasoning codec**

```ts
export function resolveAnthropicModel(requested: string, options: AnthropicModelOptions): string {
  if (options.passthrough.has(requested)) return requested;
  const value = requested.toLowerCase();
  if (value.includes("opus")) return options.opus;
  if (value.includes("sonnet")) return options.sonnet;
  if (value.includes("haiku")) return options.haiku;
  return options.fallback;
}

const PREFIX = "ghostprov-r1.";
export function encodeReasoningItem(item: ResponsesReasoningItem): string {
  return PREFIX + Buffer.from(JSON.stringify(item), "utf8").toString("base64url");
}
```

`decodeReasoningItem` must cap the encoded value at 8 MiB, catch decoding/JSON errors, require `type:"reasoning"`, require a string `encrypted_content`, accept only string `id` and array `summary`, and return a fresh object with only those four fields.

- [ ] **Step 6: Run focused tests and commit**

Run: `bun test --isolate tests/anthropic-foundations.test.ts`

Expected: PASS.

```text
git add src/anthropic/types.ts src/anthropic/errors.ts src/anthropic/model.ts src/anthropic/reasoning.ts tests/anthropic-foundations.test.ts
git commit -m "feat: add Anthropic compatibility foundations"
```

---

### Task 2: Translate current Messages requests into stateless Responses bodies

**Files:**
- Create: `src/anthropic/request.ts`
- Test: `tests/anthropic-request.test.ts`

**Interfaces:**
- Consumes: Task 1 model/error/reasoning interfaces.
- Produces: `translateAnthropicRequest(value, options): TranslatedAnthropicRequest`.
- Produces: `{ requestedModel, relayBody, stream, thinkingDisplay, stopSequences, estimatedInputTokens }`.

- [ ] **Step 1: Write failing text/tool/thinking mapping tests**

```ts
it("maps a Claude Code tool-result turn and restores encrypted reasoning", () => {
  const signature = encodeReasoningItem({ type: "reasoning", id: "rs_1", encrypted_content: "cipher", summary: [] });
  const translated = translateAnthropicRequest({
    model: "claude-sonnet-4-6", max_tokens: 32000, stream: true,
    system: [{ type: "text", text: "You are Claude Code", cache_control: { type: "ephemeral" } }],
    messages: [
      { role: "user", content: "read package.json" },
      { role: "assistant", content: [
        { type: "thinking", thinking: "", signature },
        { type: "tool_use", id: "call_1", name: "Read", input: { file_path: "package.json" } },
      ] },
      { role: "user", content: [{ type: "tool_result", tool_use_id: "call_1", content: "{}" }] },
    ],
    tools: [{ name: "Read", description: "Read a file", input_schema: { type: "object", properties: { file_path: { type: "string" } }, required: ["file_path"] } }],
    thinking: { type: "adaptive" }, output_config: { effort: "xhigh" },
  }, OPTIONS);
  expect(translated).toMatchObject({ requestedModel: "claude-sonnet-4-6", stream: true });
  expect(translated.relayBody).toMatchObject({
    model: "gpt-5.6-terra", store: false, max_output_tokens: 32000,
    include: ["reasoning.encrypted_content"], instructions: "You are Claude Code",
    reasoning: { effort: "xhigh", summary: "auto" },
    tools: [{ type: "function", name: "Read" }],
  });
  expect(JSON.stringify(translated.relayBody.input)).toContain("cipher");
  expect(translated.relayBody.input).toContainEqual(expect.objectContaining({ type: "function_call", call_id: "call_1" }));
  expect(translated.relayBody.input).toContainEqual(expect.objectContaining({ type: "function_call_output", call_id: "call_1" }));
});
```

- [ ] **Step 2: Write failing media, structured-output, choice, and validation tests**

Cover base64/URL/file-ID images; base64/URL/file-ID/plain-text documents; tool-result images; `tool_choice` auto/any/tool/none; `disable_parallel_tool_use`; `speed`; service tier; `output_config.format`; top-level/block cache controls; unknown additive properties; malformed required fields; unknown unsafe block/tool types; and the rule that tool results precede user text.

Use these exact assertions for the important wire mappings:

```ts
expect(body.text).toEqual({ format: { type: "json_schema", name: "anthropic_output", schema: OUTPUT_SCHEMA, strict: true } });
expect(body.tool_choice).toEqual({ type: "function", name: "Read" });
expect(body.parallel_tool_calls).toBe(false);
expect(body.service_tier).toBe("fast");
expect(body.input).toContainEqual(expect.objectContaining({ type: "message", role: "user", content: [
  { type: "input_image", image_url: "data:image/png;base64,AA==" },
  { type: "input_image", file_id: "file_image" },
] }));
```

- [ ] **Step 3: Run focused tests and verify RED**

Run: `bun test --isolate tests/anthropic-request.test.ts`

Expected: FAIL because `translateAnthropicRequest` does not exist.

- [ ] **Step 4: Implement strict required-field parsing with additive tolerance**

```ts
function requireRecord(value: unknown, name: string): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new AnthropicRequestError(400, `${name} must be an object`);
  return value as Record<string, unknown>;
}

function requireRequest(value: unknown): AnthropicMessageRequest {
  const body = requireRecord(value, "request");
  if (typeof body.model !== "string" || !body.model) throw new AnthropicRequestError(400, "model is required");
  if (!Number.isInteger(body.max_tokens) || Number(body.max_tokens) < 1) throw new AnthropicRequestError(400, "max_tokens must be a positive integer");
  if (!Array.isArray(body.messages)) throw new AnthropicRequestError(400, "messages is required");
  return body as AnthropicMessageRequest;
}
```

Ignore unknown top-level and additive object properties. Reject only a content/tool discriminator that has no safe Responses representation.

- [ ] **Step 5: Implement ordered content conversion**

Use `flushAssistantText()` and `flushUserContent()` helpers so reasoning items, assistant text, function calls, function outputs, and user media preserve order. Map assistant `tool_use.id` and user `tool_result.tool_use_id` to the same Responses `call_id`. Decode GhostProv thinking signatures and ignore native signatures without exposing their content.

Tool-result errors must prepend an `input_text` block containing `Tool execution failed:\n` and retain the returned content. User messages must throw `AnthropicRequestError` if a `tool_result` appears after ordinary text/media.

- [ ] **Step 6: Implement tools, thinking, output format, and request assembly**

```ts
const relayBody: Record<string, unknown> = {
  model: resolveAnthropicModel(body.model, options.models),
  store: false,
  stream: body.stream === true,
  input,
  include: ["reasoning.encrypted_content"],
  max_output_tokens: body.max_tokens,
  ...(instructions ? { instructions } : {}),
  ...(tools.length ? { tools } : {}),
  ...(toolChoice !== undefined ? { tool_choice: toolChoice } : {}),
  ...(parallelToolCalls !== undefined ? { parallel_tool_calls: parallelToolCalls } : {}),
  ...(reasoning ? { reasoning } : {}),
  ...(text ? { text } : {}),
  ...(serviceTier ? { service_tier: serviceTier } : {}),
  ...(typeof body.temperature === "number" ? { temperature: body.temperature } : {}),
  ...(typeof body.top_p === "number" ? { top_p: body.top_p } : {}),
};
```

Map manual thinking budgets deterministically: `<4096→low`, `<16000→medium`, `<32000→high`, otherwise `max`; explicit output effort wins. `disabled` maps to `none`; adaptive defaults to `high`. Preserve `display:"omitted"` in adapter context. Do not send `top_k`, private beta fields, or cache-control objects upstream.

- [ ] **Step 7: Run focused tests and commit**

Run: `bun test --isolate tests/anthropic-request.test.ts tests/anthropic-foundations.test.ts`

Expected: PASS.

```text
git add src/anthropic/request.ts tests/anthropic-request.test.ts
git commit -m "feat: translate Anthropic Messages requests"
```

---

### Task 3: Convert final Responses objects into current Anthropic Messages

**Files:**
- Create: `src/anthropic/response.ts`
- Test: `tests/anthropic-response.test.ts`

**Interfaces:**
- Produces: `responseToAnthropic(final, context): AnthropicMessage`.
- Produces: `usageFromResponses(usage, estimatedInputTokens): AnthropicUsage`.
- Produces: `stopReasonFromResponses(final, blocks, matchedStop): AnthropicStopReason`.

- [ ] **Step 1: Write failing final conversion tests**

```ts
it("converts ordered reasoning, text, parallel tools, usage, and nullables", () => {
  const message = responseToAnthropic({
    id: "resp_1", status: "completed",
    output: [
      { type: "reasoning", id: "rs_1", encrypted_content: "cipher", summary: [{ type: "summary_text", text: "considering" }] },
      { type: "message", role: "assistant", content: [{ type: "output_text", text: "checking", annotations: [] }] },
      { type: "function_call", call_id: "call_1", name: "Read", arguments: "{\"file_path\":\"a\"}" },
      { type: "function_call", call_id: "call_2", name: "Read", arguments: "{\"file_path\":\"b\"}" },
    ],
    usage: { input_tokens: 100, output_tokens: 20, input_tokens_details: { cached_tokens: 40 } },
  }, CONTEXT);
  expect(message).toMatchObject({ id: "msg_resp_1", type: "message", role: "assistant", model: CONTEXT.requestedModel,
    container: null, stop_reason: "tool_use", stop_sequence: null,
    usage: { input_tokens: 60, output_tokens: 20, cache_creation_input_tokens: null, cache_read_input_tokens: 40 },
  });
  expect(message.content.map(block => block.type)).toEqual(["thinking", "text", "tool_use", "tool_use"]);
  expect(message.content[0]).toMatchObject({ thinking: "considering" });
  expect((message.content[0] as any).signature).toStartWith("ghostprov-r1.");
  expect(message.content[1]).toEqual({ type: "text", text: "checking", citations: null });
});
```

Also cover omitted thinking display, malformed function JSON falling back to `{}`, incomplete/max-token status, refusal, empty output, multiple message content parts, and exact earliest stop-sequence truncation.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `bun test --isolate tests/anthropic-response.test.ts`

Expected: FAIL because `responseToAnthropic` does not exist.

- [ ] **Step 3: Implement ordered output conversion and usage**

Iterate `final.output` once. Convert reasoning to a thinking block with a GhostProv signature, message `output_text` to text blocks with `citations:null`, and `function_call` to tool-use blocks using `call_id` as `id`. Parse arguments only when valid JSON; use `{}` for an empty argument string and throw an `api_error` for non-empty invalid terminal JSON.

```ts
return {
  id: `msg_${String(final.id ?? requestId)}`,
  type: "message", role: "assistant", model: context.requestedModel,
  content, stop_reason, stop_sequence, container: null,
  usage: usageFromResponses(final.usage, context.estimatedInputTokens),
};
```

Cached input tokens are subtracted from Anthropic `input_tokens` and returned as `cache_read_input_tokens`; `cache_creation_input_tokens` is `null` because Responses does not distinguish a cache write in the final usage.

- [ ] **Step 4: Implement exact stop-sequence filtering**

Find the earliest requested stop sequence across concatenated text blocks, truncate at that boundary, remove later visible text, set `stop_reason:"stop_sequence"`, and set `stop_sequence` to the matched value. Tool use takes precedence only if it occurs before the matched text boundary; max-token/incomplete takes precedence when no stop sequence matched.

- [ ] **Step 5: Run focused tests and commit**

Run: `bun test --isolate tests/anthropic-response.test.ts tests/anthropic-request.test.ts`

Expected: PASS.

```text
git add src/anthropic/response.ts tests/anthropic-response.test.ts
git commit -m "feat: convert Responses results to Anthropic messages"
```

---

### Task 4: Translate Responses SSE into pull-safe Anthropic SSE

**Files:**
- Create: `src/anthropic/stream.ts`
- Test: `tests/anthropic-stream.test.ts`

**Interfaces:**
- Produces: `AnthropicStreamTranslator` with `start()`, `push(frame)`, `finish(final)`, and `fail(error)` returning `AnthropicSseEvent[]`.
- Produces: `anthropicSseFrame(event): string`.
- Consumes: Task 3 final conversion for authoritative fallback.

- [ ] **Step 1: Write failing canonical event-order tests**

Build synthetic Responses fixtures covering `response.created`, reasoning item added/summary delta/item done, text content added/delta/done, two function calls with interleaved argument deltas, `response.completed`, and unknown additive events.

Assert this exact Anthropic order:

```ts
expect(events.map(event => event.type)).toEqual([
  "message_start",
  "content_block_start", "content_block_delta", "content_block_delta", "content_block_stop",
  "content_block_start", "content_block_delta", "content_block_stop",
  "content_block_start", "content_block_delta", "content_block_stop",
  "content_block_start", "content_block_delta", "content_block_stop",
  "message_delta", "message_stop",
]);
expect(events.filter(e => e.type === "content_block_start").map((e: any) => e.index)).toEqual([0, 1, 2, 3]);
expect(events.find((e: any) => e.delta?.type === "signature_delta")).toMatchObject({ delta: { signature: expect.stringContaining("ghostprov-r1.") } });
```

- [ ] **Step 2: Write failing fallback, error, stop filtering, and bounded-state tests**

Cover missing `output_item.added`, terminal-only function calls, no duplicate text when final fallback arrives, `response.failed`, upstream `error`, stop sequences split across deltas, malformed frame JSON, repeated completion, and 10,000 tiny deltas without transcript accumulation.

- [ ] **Step 3: Run focused tests and verify RED**

Run: `bun test --isolate tests/anthropic-stream.test.ts`

Expected: FAIL because the translator does not exist.

- [ ] **Step 4: Implement a bounded item/block state machine**

Maintain only open blocks, emitted block fingerprints, streamed argument/text counters, usage, matched stop state, and the maximum stop-sequence suffix. Never store the full SSE transcript.

Map these official/observed events:

```text
response.output_item.added
response.content_part.added
response.reasoning_summary_part.added
response.reasoning_summary_text.delta
response.output_text.delta
response.output_text.done
response.function_call_arguments.delta
response.function_call_arguments.done
response.output_item.done
response.completed
response.incomplete
response.failed
error
```

Unknown events return `[]`. Open a block lazily if a delta arrives before its added event. Before stopping a reasoning block, emit one `signature_delta` carrying the completed reasoning item. Before stopping a tool block, emit full terminal arguments if no argument delta was seen.

- [ ] **Step 5: Implement streaming stop-sequence filtering**

Hold at most `maxStopSequenceLength - 1` un-emitted text characters. On each delta, append, emit only the safe prefix, and detect the earliest full sequence. Once matched, emit text before the sequence, suppress later visible text, continue draining upstream, and finish with `stop_reason:"stop_sequence"`.

- [ ] **Step 6: Run focused tests and commit**

Run: `bun test --isolate tests/anthropic-stream.test.ts tests/anthropic-response.test.ts`

Expected: PASS with canonical order and no duplicate terminal.

```text
git add src/anthropic/stream.ts tests/anthropic-stream.test.ts
git commit -m "feat: stream Responses as Anthropic events"
```

---

### Task 5: Add deterministic token counting and integrated Anthropic routes

**Files:**
- Create: `src/anthropic/token-count.ts`
- Create: `src/anthropic/routes.ts`
- Modify: `src/server/app.ts`
- Modify: `src/router/router.ts`
- Test: `tests/anthropic-token-count.test.ts`
- Test: `tests/anthropic-http.test.ts`

**Interfaces:**
- Produces: `estimateAnthropicInputTokens(value): number`.
- Produces: `registerAnthropicRoutes(app, dependencies): void`.
- Extends: `RelaySource` with `"anthropic"` for observability only.

- [ ] **Step 1: Write failing deterministic token-count tests**

```ts
it("counts text, tools, images, documents, and overhead deterministically", () => {
  const request = {
    model: "claude-sonnet-4-6", messages: [{ role: "user", content: [
      { type: "text", text: "hello" },
      { type: "image", source: { type: "base64", media_type: "image/png", data: "A".repeat(4096) } },
      { type: "document", source: { type: "text", media_type: "text/plain", data: "document text" } },
    ] }], tools: [{ name: "Read", input_schema: { type: "object", properties: {} } }],
  };
  const first = estimateAnthropicInputTokens(request);
  expect(first).toBeGreaterThan(100);
  expect(estimateAnthropicInputTokens(structuredClone(request))).toBe(first);
});
```

The estimator must cap traversal depth at 64, reject cyclic input, count UTF-8 text/JSON at `ceil(bytes/3.5)`, count base64 media by decoded bytes with a bounded media overhead, and return at least 1.

- [ ] **Step 2: Write failing Hono tests for stable/beta JSON, SSE, errors, drain, and cancellation**

Use a fake `RouterLike` and real `createGhostProvApp`. Assert:

- both `/v1/messages` and `/v1/messages?beta=true` work;
- both `/v1/messages/count_tokens` and the beta query work without relay dispatch;
- stream selection comes from `body.stream`, not `Accept`;
- `request-id` is returned;
- `x-api-key`, bearer, both, unknown headers, and legacy betas are tolerated;
- invalid JSON/required fields return current Anthropic errors;
- streaming emits named canonical SSE and no `[DONE]`;
- non-streaming drains `handle.events()` while awaiting `final`;
- reader cancellation aborts the relay signal;
- the router receives `source:"anthropic"` and the same `x-ghostprov-client-id ?? "http"` identity used by Responses.

- [ ] **Step 3: Run focused tests and verify RED**

Run: `bun test --isolate tests/anthropic-token-count.test.ts tests/anthropic-http.test.ts`

Expected: FAIL because routes and estimator do not exist.

- [ ] **Step 4: Implement bounded token estimation**

Use iterative traversal rather than recursive JSON stringification. Count keys and scalar values, add fixed message/block/tool overhead, decode only the base64 length formula (never allocate decoded media), and cap any one media contribution at 1,000,000 tokens.

- [ ] **Step 5: Implement stable and beta route registration**

Register each path once; Hono naturally ignores the query string when matching.

```ts
app.post("/v1/messages/count_tokens", async c => {
  const requestId = newRequestId();
  try {
    const body = await c.req.json();
    validateCountTokenRequest(body);
    return c.json({ input_tokens: estimateAnthropicInputTokens(body) }, 200, { "request-id": requestId });
  } catch (error) { return anthropicErrorResponse(error, requestId); }
});
```

For non-streaming Messages, start `Array.fromAsync(handle.events())` immediately, await `handle.final`, await the drain, and return `responseToAnthropic`. For streaming, create the translator, get one upstream iterator, and expose a `ReadableStream` whose `pull()` dequeues one translated frame or calls `iterator.next()` until output exists. `cancel()` must call `iterator.return()` and abort a route-local controller linked to `c.req.raw.signal`.

- [ ] **Step 6: Compose routes without changing existing OpenAI behavior**

```ts
registerAnthropicRoutes(app, {
  keepAliveMs,
  models: anthropicModelsFromEnv(),
  startRelay: (body, signal, clientId) => options.router.startRelay(body, {
    source: "anthropic", clientId, signal,
  }),
});
```

Place registration beside `registerImageRoutes`. Do not alter existing `/v1/responses`, `/v1/models`, Images, or WebSocket handlers.

- [ ] **Step 7: Run focused and shared HTTP tests and commit**

Run: `bun test --isolate tests/anthropic-token-count.test.ts tests/anthropic-http.test.ts tests/router-http.test.ts tests/http-streaming.test.ts tests/http-body.test.ts tests/images-http.test.ts`

Expected: PASS.

```text
git add src/anthropic/token-count.ts src/anthropic/routes.ts src/server/app.ts src/router/router.ts tests/anthropic-token-count.test.ts tests/anthropic-http.test.ts
git commit -m "feat: serve Anthropic Messages routes"
```

---

### Task 6: Document configuration and add the non-protected live forwarding verifier

**Files:**
- Modify: `.env.example`
- Modify: `README.md`
- Create: `scripts/verify_claude_code.ts`
- Modify: `package.json`
- Modify: `tests/documentation.test.ts`
- Test: `tests/claude-code-verifier.test.ts`

**Interfaces:**
- Produces: `bun run verify:claude-code`.
- Consumes: `GP_CLAUDE_VERIFY_PORT` (default `18788`) and `GP_CLAUDE_VERIFY_UPSTREAM` (fixed default `http://127.0.0.1:18787`).

- [ ] **Step 1: Write failing docs and verifier-safety tests**

```ts
it("documents same-port Claude Code setup and tier mapping", () => {
  const readme = readFileSync("README.md", "utf8");
  expect(readme).toContain("POST /v1/messages");
  expect(readme).toContain("POST /v1/messages/count_tokens");
  expect(readme).toContain("ANTHROPIC_BASE_URL=http://127.0.0.1:18787");
  expect(readme).toContain("Opus → `gpt-5.6-sol`");
});

it("refuses to bind or manage the protected port", () => {
  expect(() => validateVerifierOptions({ port: 18787, upstream: "http://127.0.0.1:18787" })).toThrow("protected");
  expect(validateVerifierOptions({ port: 18788, upstream: "http://127.0.0.1:18787" })).toEqual({ port: 18788, upstream: "http://127.0.0.1:18787" });
});
```

- [ ] **Step 2: Run tests and verify RED**

Run: `bun test --isolate tests/documentation.test.ts tests/claude-code-verifier.test.ts`

Expected: FAIL because the docs/script are absent.

- [ ] **Step 3: Implement a forwarding `RouterLike` verifier**

The script must create the production `createGhostProvApp` on `127.0.0.1:<non-protected-port>`. Its `startRelay` POSTs the translated Responses body to `<upstream>/v1/responses`, parses upstream SSE into a bounded async queue and authoritative final, and returns a normal `RunHandle`. It must never enumerate, signal, stop, or restart processes. Ctrl+C/normal completion closes only its own listener.

Export `validateVerifierOptions` and `createForwardingRouter` behind `if (import.meta.main)` so tests can import them. Reject non-loopback upstreams, upstream paths other than the root, and ports `18787` or `8787`.

- [ ] **Step 4: Document current setup and model overrides**

Add:

```powershell
$env:ANTHROPIC_BASE_URL = "http://127.0.0.1:18787"
$env:ANTHROPIC_API_KEY = "ghostprov-local"
claude
```

Document that the base URL must not end in `/v1`; list stable/beta routes, model mappings and four override variables, accepted API-key/bearer headers, Messages limitations/non-goals, and the separate-port verifier.

- [ ] **Step 5: Add script and run tests**

Add `"verify:claude-code": "bun scripts/verify_claude_code.ts"` to `package.json`.

Run: `bun test --isolate tests/documentation.test.ts tests/claude-code-verifier.test.ts`

Expected: PASS.

- [ ] **Step 6: Commit**

```text
git add .env.example README.md scripts/verify_claude_code.ts package.json tests/documentation.test.ts tests/claude-code-verifier.test.ts
git commit -m "docs: add Claude Code setup and verifier"
```

---

### Task 7: Verify real Claude Code 2.1.220 text, sequential tools, and parallel tools

**Files:**
- Create during verification only: `.tmp/claude-code-verification/*`
- Modify only if a failure is reproduced: the smallest relevant `src/anthropic/*` file plus a failing regression test.

**Interfaces:**
- Consumes: installed `claude.exe` 2.1.220.
- Produces: sanitized command output and filesystem proof; no prompt/response content is committed.

- [ ] **Step 1: Confirm protected and test listeners before starting**

Run:

```powershell
Get-NetTCPConnection -LocalPort 18787 -State Listen | Select-Object LocalAddress,LocalPort,OwningProcess
Get-NetTCPConnection -LocalPort 18788 -State Listen -ErrorAction SilentlyContinue
```

Expected: one existing listener on 18787 and none on 18788. Record the 18787 owning PID and never signal it.

- [ ] **Step 2: Start only the forwarding verifier on 18788**

Run in a background PTY/session:

```powershell
$env:GP_CLAUDE_VERIFY_PORT = "18788"
$env:GP_CLAUDE_VERIFY_UPSTREAM = "http://127.0.0.1:18787"
bun run verify:claude-code
```

Expected: verifier reports `http://127.0.0.1:18788`; the recorded 18787 PID remains unchanged.

- [ ] **Step 3: Verify direct stable/beta, count-token, and malformed requests**

Run `Invoke-WebRequest` calls against port 18788 for non-streaming `/v1/messages`, streaming `/v1/messages?beta=true`, both count-token forms, and a missing-`messages` body. Assert current Anthropic objects/events, request IDs, status 400 for malformed input, and no OpenAI event names in the Anthropic stream.

- [ ] **Step 4: Run text-only installed Claude Code**

```powershell
$env:ANTHROPIC_BASE_URL = "http://127.0.0.1:18788"
$env:ANTHROPIC_API_KEY = "ghostprov-local-verification"
claude --bare --print --model claude-sonnet-4-6 --max-turns 1 "Reply with exactly GHOSTPROV_CLAUDE_OK"
```

Expected: exit 0 and output contains `GHOSTPROV_CLAUDE_OK`.

- [ ] **Step 5: Run a sequential real tool loop with filesystem proof**

Create an empty `.tmp/claude-code-verification` directory, run Claude Code there with `--dangerously-skip-permissions`, and ask it to use its file-writing tool to create `tool-proof.txt` containing `TOOL_LOOP_OK`, read it back, then answer with that marker. Expected: exit 0, final output contains the marker, and the file bytes equal `TOOL_LOOP_OK`.

- [ ] **Step 6: Run a parallel tool loop**

Ask Claude Code to read two pre-created small files independently before answering with both markers. Capture verifier counters proving two `tool_use` blocks occurred in one assistant message or, if the model legitimately chooses sequential calls twice, rerun once with explicit parallel wording. Expected: exit 0, correct combined marker, no Anthropic stream parse/API validation error.

- [ ] **Step 7: Reproduce and fix every observed failure with TDD**

For each runtime failure, add one focused failing test with the exact sanitized wire shape, run it to verify RED, make the minimal adapter change, verify GREEN, and rerun the failed live command. Do not weaken or disable Claude Code features merely to hide a protocol defect.

- [ ] **Step 8: Stop only the verifier and re-check the protected PID**

Send Ctrl+C only to the verifier's PTY/session. Re-run `Get-NetTCPConnection` and assert port 18788 is free while port 18787 still has the original owning PID.

---

### Task 8: Full regression, compatibility audit, and final review

**Files:**
- Modify only for proven defects discovered by verification.

- [ ] **Step 1: Run focused Anthropic suite**

Run:

```text
bun test --isolate tests/anthropic-foundations.test.ts tests/anthropic-request.test.ts tests/anthropic-response.test.ts tests/anthropic-stream.test.ts tests/anthropic-token-count.test.ts tests/anthropic-http.test.ts tests/claude-code-verifier.test.ts tests/documentation.test.ts
```

Expected: all tests pass with zero failures.

- [ ] **Step 2: Run all repository verification**

Run:

```text
bun test --isolate
bun run test:daemon
bun run typecheck
bun audit --production
git diff --check
```

Expected: every command exits 0.

- [ ] **Step 3: Audit every completion criterion against evidence**

Verify route registration, same-router dispatch, all current request/response shapes, reasoning signature round-trip, sequential and parallel tool results, current errors, token counting, cancellation, docs, installed Claude Code success, and unchanged protected PID. Treat missing evidence as incomplete work.

- [ ] **Step 4: Request an ultra-reasoning code review and fix findings**

Dispatch an independent reviewer with the spec, plan, branch diff, test output, and live-verification summary. For every valid finding, add a failing regression test before changing production code, then rerun focused and full verification.

- [ ] **Step 5: Commit final verified adjustments**

```text
git add -A
git commit -m "feat: support Claude Code through Anthropic Messages"
```

Skip this commit only if the working tree is already clean because every adjustment was committed task-by-task.

