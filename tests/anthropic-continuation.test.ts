import { describe, expect, it } from "bun:test";
import {
  ANTHROPIC_CONTINUATION_ENTRY_METADATA_BYTES,
  ANTHROPIC_CONTINUATION_MAX_ENTRIES,
  ANTHROPIC_CONTINUATION_METADATA_BYTES,
  ANTHROPIC_CONTINUATION_TTL_MS,
  AnthropicContinuationIndex,
  projectAnthropicReplay,
} from "../src/anthropic/continuation";
import type {
  ResponsesInputItem,
  ResponsesRequestBody,
} from "../src/anthropic/request-types";
import { responseToAnthropic } from "../src/anthropic/response";
import { CONTEXT, context } from "./support/anthropic-response";
import {
  finalResponse,
  messageOutput,
  reasoningOutput,
  refusalPart,
  textPart,
  toolOutput,
} from "./support/anthropic-stream";

import { createAuthenticatedTenantScope } from "../src/http/api-key";

const KEY = new Uint8Array(32).fill(0x5a);
const SCOPE = createAuthenticatedTenantScope();

function user(text: string): ResponsesInputItem {
  return {
    type: "message",
    role: "user",
    content: [{ type: "input_text", text }],
  };
}

function assistant(text: string): ResponsesInputItem {
  return { type: "message", role: "assistant", content: text };
}

function requestBody(
  input: ResponsesInputItem[],
  overrides: Record<string, unknown> = {},
): ResponsesRequestBody {
  return {
    model: "gpt-5.6-terra",
    store: true,
    stream: false,
    input,
    include: ["reasoning.encrypted_content"],
    max_output_tokens: 64,
    service_tier: "default",
    ...overrides,
  } as ResponsesRequestBody;
}

describe("Anthropic persisted continuation index", () => {
  it("uses an independent random process-local HMAC key by default", () => {
    const first = new AnthropicContinuationIndex();
    const second = new AnthropicContinuationIndex();
    const input = [user("same")];
    expect(first.digestInput(input).digest).not.toBe(second.digestInput(input).digest);
    expect(first.fingerprintSettings(requestBody(input)))
      .not.toBe(second.fingerprintSettings(requestBody(input)));
  });

  it("uses canonical, ordered, item-boundary rolling HMAC digests", () => {
    const index = new AnthropicContinuationIndex({ key: KEY });
    const left = {
      type: "message",
      role: "user",
      content: [{ type: "input_text", text: "hello", extra: { b: 2, a: 1 } }],
    } as unknown as ResponsesInputItem;
    const right = {
      content: [{ extra: { a: 1, b: 2 }, text: "hello", type: "input_text" }],
      role: "user",
      type: "message",
    } as unknown as ResponsesInputItem;

    expect(index.digestInput([left])).toEqual(index.digestInput([right]));
    expect(index.digestInput([left, user("tail")]).digest)
      .not.toBe(index.digestInput([user("tail"), right]).digest);
    expect(index.digestInput([assistant("a"), assistant("b")]).digest)
      .not.toBe(index.digestInput([assistant("ab")]).digest);
    expect(index.digestInput([left])).toEqual(index.digestInput([left]));
  });


  it("streams canonical string escaping without changing JSON byte identity", () => {
    const index = new AnthropicContinuationIndex({ key: KEY });
    for (const text of [
      "plain text",
      `quote=" slash=\\ controls=${String.fromCharCode(8, 9, 10, 12, 13, 0, 31)}`,
      `paired=😀 lone-high=${String.fromCharCode(0xd800)} ` +
        `lone-low=${String.fromCharCode(0xdc00)} ` +
        `separators=${String.fromCharCode(0x2028, 0x2029)}`,
      "x".repeat(1024 * 1024),
    ]) {
      const item = user(text);
      expect(index.digestInput([item]).canonicalBytes, JSON.stringify(text))
        .toBe(Buffer.byteLength(JSON.stringify(item), "utf8"));
    }
  });

  it("fingerprints every top-level relay setting except continuation transport fields", () => {
    const index = new AnthropicContinuationIndex({ key: KEY });
    const body = requestBody([user("one")], {
      instructions: "system",
      tools: [{ type: "function", name: "Read", parameters: { type: "object" } }],
      tool_choice: "auto",
      parallel_tool_calls: false,
      reasoning: { effort: "high", summary: "auto" },
      text: {
        format: {
          type: "json_schema",
          name: "anthropic_output",
          schema: { type: "object" },
          strict: true,
        },
      },
      temperature: 0.2,
      top_p: 0.9,
      safety_identifier: "safe",
      metadata: { anthropic_user_id_sha256: "abc" },
      future_request_setting: { enabled: true },
    });
    const fingerprint = index.fingerprintSettings(body);
    const excludedOnly = {
      ...body,
      input: [user("different")],
      stream: true,
      store: false,
      previous_response_id: "resp_parent",
    } as unknown as ResponsesRequestBody;
    expect(index.fingerprintSettings(excludedOnly)).toBe(fingerprint);

    for (const [field, replacement] of [
      ["model", "other-model"],
      ["include", []],
      ["max_output_tokens", 65],
      ["instructions", "other-system"],
      ["tools", []],
      ["tool_choice", "none"],
      ["parallel_tool_calls", true],
      ["reasoning", { effort: "low" }],
      ["text", { format: { type: "text" } }],
      ["service_tier", "auto"],
      ["temperature", 0.3],
      ["top_p", 0.8],
      ["safety_identifier", "other"],
      ["metadata", { anthropic_user_id_sha256: "def" }],
      ["future_request_setting", { enabled: false }],
    ] as const) {
      expect(index.fingerprintSettings({ ...body, [field]: replacement }), field)
        .not.toBe(fingerprint);
    }

    const reordered = Object.fromEntries(Object.entries(body).reverse());
    expect(index.fingerprintSettings(reordered)).toBe(fingerprint);
  });

  it("falls back to full history when the index mutates during cooperative hashing", async () => {
    const index = new AnthropicContinuationIndex({ key: KEY });
    const input = Array.from({ length: 600 }, (_, value) => user(`item-${value}`));
    const short = index.prepare(requestBody(input.slice(0, 1)), SCOPE);
    index.commit(short, SCOPE, "resp_short", [input[1]!]);
    const longPrefix = index.prepare(requestBody(input.slice(0, 399)), SCOPE);

    const pending = index.prepareAsync(requestBody(input), SCOPE);
    index.commit(longPrefix, SCOPE, "resp_long_left", [input[399]!]);
    index.commit(longPrefix, SCOPE, "resp_long_right", [input[399]!]);
    const prepared = await pending;

    expect(index.snapshot().ambiguousBuckets).toBe(1);
    expect(prepared.decision).toBe("miss");
    expect(prepared.match).toBeNull();
    expect(prepared.relayBody.input).toEqual(input);
    expect(prepared.relayBody).not.toHaveProperty("previous_response_id");
  });

  it("keeps a valid hit when unrelated unique metadata changes during hashing", async () => {
    const index = new AnthropicContinuationIndex({ key: KEY });
    const input = Array.from({ length: 600 }, (_, value) => user(`stable-${value}`));
    const short = index.prepare(requestBody(input.slice(0, 1)), SCOPE);
    index.commit(short, SCOPE, "resp_stable_short", [input[1]!]);
    const unrelated = index.prepare(requestBody([user("unrelated")]), SCOPE);

    const pending = index.prepareAsync(requestBody(input), SCOPE);
    index.commit(unrelated, SCOPE, "resp_unrelated", [assistant("answer")]);
    const prepared = await pending;

    expect(prepared.decision).toBe("hit");
    expect(prepared.match?.providerResponseId).toBe("resp_stable_short");
    expect(prepared.match?.prefixItemCount).toBe(2);
  });

  it("aborts cooperative hashing at a yield boundary", async () => {
    const index = new AnthropicContinuationIndex({ key: KEY });
    const controller = new AbortController();
    const input = Array.from({ length: 600 }, (_, value) => user(`abort-${value}`));
    const pending = index.prepareAsync(requestBody(input), SCOPE, controller.signal);
    controller.abort(new DOMException("stop hashing", "AbortError"));

    await expect(pending).rejects.toMatchObject({ name: "AbortError" });
  });

  it("sends only the exact nonempty suffix with the stored parent and current settings", () => {
    const index = new AnthropicContinuationIndex({ key: KEY });
    const initial = requestBody([user("one")]);
    const prepared = index.prepare(initial, SCOPE);
    expect(prepared.decision).toBe("miss");
    index.commit(prepared, SCOPE, "resp_parent", [assistant("answer")]);

    const current = requestBody([
      user("one"),
      assistant("answer"),
      user("two"),
    ], {
      stream: true,
      store: false,
      previous_response_id: "caller-supplied-must-not-win",
    });
    const continuation = index.prepare(current, SCOPE);
    expect(continuation.decision).toBe("hit");
    expect(continuation.match).toMatchObject({
      providerResponseId: "resp_parent",
      prefixItemCount: 2,
    });
    expect(continuation.relayBody).toEqual({
      ...current,
      store: true,
      input: [user("two")],
      previous_response_id: "resp_parent",
    });

    const changedSettings = index.prepare(requestBody([
      user("one"),
      assistant("answer"),
      user("two"),
    ], { max_output_tokens: 65 }), SCOPE);
    expect(changedSettings.decision).toBe("miss");
    expect(changedSettings.relayBody).not.toHaveProperty("previous_response_id");

    const empty = index.prepare(requestBody([user("one"), assistant("answer")]), SCOPE);
    expect(empty.decision).toBe("empty_suffix");
    expect(empty.relayBody.input).toHaveLength(2);
    expect(empty.relayBody).not.toHaveProperty("previous_response_id");
  });

  it("chooses the longest unique boundary and refuses an ambiguous longest fork", () => {
    const index = new AnthropicContinuationIndex({ key: KEY });
    const root = requestBody([user("root")]);
    const rootPrepared = index.prepare(root, SCOPE);
    index.commit(rootPrepared, SCOPE, "resp_root", [assistant("root-answer")]);

    const childInput = [user("root"), assistant("root-answer"), user("child")];
    const childPrepared = index.prepare(requestBody(childInput), SCOPE);
    expect(childPrepared.relayBody.input).toEqual([user("child")]);
    index.commit(childPrepared, SCOPE, "resp_child", [assistant("child-answer")]);

    const grandchild = requestBody([
      ...childInput,
      assistant("child-answer"),
      user("grandchild"),
    ]);
    expect(index.prepare(grandchild, SCOPE)).toMatchObject({
      decision: "hit",
      match: { providerResponseId: "resp_child", prefixItemCount: 4 },
      relayBody: { input: [user("grandchild")] },
    });

    // A concurrent identical completion creates two valid parents for the same
    // longest history. Falling back to the shorter root would pick a false fork.
    index.commit(childPrepared, SCOPE, "resp_child_concurrent", [assistant("child-answer")]);
    const ambiguous = index.prepare(grandchild, SCOPE);
    expect(ambiguous.decision).toBe("ambiguous");
    expect(ambiguous.match).toBeNull();
    expect(ambiguous.relayBody.input).toEqual(grandchild.input);
    expect(ambiguous.relayBody).not.toHaveProperty("previous_response_id");

    // The ambiguous request is sent in full. Its successful completion creates a
    // new, longer exact history without selecting either ambiguous parent.
    index.commit(
      ambiguous,
      SCOPE,
      "resp_after_ambiguous_full",
      [assistant("grandchild-answer")],
    );
    expect(index.prepare(requestBody([
      ...grandchild.input,
      assistant("grandchild-answer"),
      user("after ambiguity"),
    ]), SCOPE).match).toMatchObject({
      providerResponseId: "resp_after_ambiguous_full",
      prefixItemCount: grandchild.input.length + 1,
    });
  });

  it("keeps distinct forks exact and isolates trusted authorization scopes", () => {
    const index = new AnthropicContinuationIndex({ key: KEY });
    const scopeA = createAuthenticatedTenantScope();
    const scopeB = createAuthenticatedTenantScope();
    const scopeC = createAuthenticatedTenantScope();
    const preparedA = index.prepare(requestBody([user("same")]), scopeA);
    const preparedB = index.prepare(requestBody([user("same")]), scopeB);
    expect(index.commit(preparedA, scopeB, "wrong-scope", [assistant("left")]))
      .toBeNull();
    index.commit(preparedA, scopeA, "resp_left", [assistant("left")]);
    index.commit(preparedB, scopeB, "resp_right", [assistant("left")]);

    expect(index.prepare(
      requestBody([user("same"), assistant("left"), user("next")]),
      scopeA,
    ).match).toMatchObject({ providerResponseId: "resp_left" });
    expect(index.prepare(
      requestBody([user("same"), assistant("left"), user("next")]),
      scopeB,
    ).match?.providerResponseId).toBe("resp_right");
    expect(index.prepare(
      requestBody([user("same"), assistant("left"), user("next")]),
      scopeC,
    ).decision).toBe("miss");

    const forkBase = index.prepare(requestBody([user("fork")]), SCOPE);
    index.commit(forkBase, SCOPE, "resp_fork_left", [assistant("left")]);
    index.commit(forkBase, SCOPE, "resp_fork_right", [assistant("right")]);
    expect(index.prepare(
      requestBody([user("fork"), assistant("left"), user("next")]),
      SCOPE,
    ).match?.providerResponseId).toBe("resp_fork_left");
    expect(index.prepare(
      requestBody([user("fork"), assistant("right"), user("next")]),
      SCOPE,
    ).match?.providerResponseId).toBe("resp_fork_right");
  });

  it("rejects late commits after shutdown begins and clears idempotently", () => {
    const index = new AnthropicContinuationIndex({ key: KEY });
    const prepared = index.prepare(requestBody([user("shutdown")]), SCOPE);

    index.beginShutdown();
    expect(index.commit(prepared, SCOPE, "resp_late", [assistant("late")])).toBeNull();
    expect(index.snapshot().entries).toBe(0);
    index.clear();
    index.clear();
    expect(index.snapshot().entries).toBe(0);
  });

  it("uses a 60-minute absolute TTL even when an entry is touched", () => {
    let now = 1_000;
    const index = new AnthropicContinuationIndex({ key: KEY, now: () => now });
    const prepared = index.prepare(requestBody([user("ttl")]), SCOPE);
    index.commit(prepared, SCOPE, "resp_ttl", [assistant("answer")]);
    const query = requestBody([user("ttl"), assistant("answer"), user("next")]);

    now += ANTHROPIC_CONTINUATION_TTL_MS - 1;
    expect(index.prepare(query, SCOPE).decision).toBe("hit");
    now += 1;
    expect(index.prepare(query, SCOPE).decision).toBe("miss");
    expect(index.snapshot().entries).toBe(0);
  });

  it("keeps ambiguity sticky through LRU pressure until its absolute TTL", () => {
    let now = 50_000;
    const index = new AnthropicContinuationIndex({ key: KEY, now: () => now });
    const base = index.prepare(requestBody([user("ambiguous-root")]), SCOPE);
    index.commit(base, SCOPE, "resp_ambiguous_left", [assistant("same-answer")]);
    index.commit(base, SCOPE, "resp_ambiguous_right", [assistant("same-answer")]);
    const query = requestBody([
      user("ambiguous-root"),
      assistant("same-answer"),
      user("next"),
    ]);
    expect(index.prepare(query, SCOPE).decision).toBe("ambiguous");

    for (let value = 0; value < ANTHROPIC_CONTINUATION_MAX_ENTRIES; value += 1) {
      const prepared = index.prepare(requestBody([user(`pressure-${value}`)]), SCOPE);
      index.commit(prepared, SCOPE, `resp_pressure_${value}`, [assistant(`answer-${value}`)]);
    }
    expect(index.snapshot()).toMatchObject({
      entries: ANTHROPIC_CONTINUATION_MAX_ENTRIES,
      ambiguousBuckets: 1,
    });
    expect(index.prepare(query, SCOPE).decision).toBe("ambiguous");

    now += ANTHROPIC_CONTINUATION_TTL_MS - 1;
    expect(index.prepare(query, SCOPE).decision).toBe("ambiguous");
    now += 1;
    expect(index.prepare(query, SCOPE).decision).toBe("miss");
    expect(index.snapshot().ambiguousBuckets).toBe(0);
  });

  it("enforces 4096-entry LRU and the fixed approximately 2 MiB metadata budget", () => {
    let now = 10_000;
    const index = new AnthropicContinuationIndex({ key: KEY, now: () => now++ });
    const base = requestBody([user("root")]);
    const prepared = index.prepare(base, SCOPE);
    for (let value = 0; value < ANTHROPIC_CONTINUATION_MAX_ENTRIES; value += 1) {
      index.commit(prepared, SCOPE, `resp_${value}`, [assistant(`answer-${value}`)]);
    }
    expect(index.snapshot()).toMatchObject({
      entries: ANTHROPIC_CONTINUATION_MAX_ENTRIES,
      logicalMetadataBytes: ANTHROPIC_CONTINUATION_METADATA_BYTES,
      metadataBudgetBytes: ANTHROPIC_CONTINUATION_METADATA_BYTES,
    });
    expect(ANTHROPIC_CONTINUATION_ENTRY_METADATA_BYTES).toBe(512);

    const first = requestBody([user("root"), assistant("answer-0"), user("next")]);
    const second = requestBody([user("root"), assistant("answer-1"), user("next")]);
    expect(index.prepare(first, SCOPE).decision).toBe("hit"); // refresh generation 1
    index.commit(prepared, SCOPE, "resp_overflow", [assistant("overflow")]);
    expect(index.snapshot().entries).toBe(ANTHROPIC_CONTINUATION_MAX_ENTRIES);
    expect(index.prepare(first, SCOPE).match?.providerResponseId).toBe("resp_0");
    expect(index.prepare(second, SCOPE).decision).toBe("miss");
  });

  it("stores only content-free records and exposes no private state", () => {
    const index = new AnthropicContinuationIndex({ key: KEY });
    const retentionScope = createAuthenticatedTenantScope();
    const secrets = [
      "PROMPT_SECRET_789",
      "OUTPUT_SECRET_789",
      "TOOL_SECRET_789",
      "CIPHER_SECRET_789",
      "data:image/png;base64,MEDIA_SECRET_789",
    ];
    const input = requestBody([{
      type: "message",
      role: "user",
      content: [
        { type: "input_text", text: secrets[0] },
        { type: "input_image", image_url: secrets[4] },
      ],
    }]);
    const retainedWrites: unknown[] = [];
    const originalSet = Map.prototype.set;
    Map.prototype.set = function (this: Map<unknown, unknown>, key: unknown, value: unknown) {
      retainedWrites.push([key, value]);
      return originalSet.call(this, key, value);
    } as typeof Map.prototype.set;
    try {
      const prepared = index.prepare(input, retentionScope);
      index.commit(prepared, retentionScope, "resp_private", [
        assistant(secrets[1]),
        {
          type: "reasoning",
          id: "rs_private",
          encrypted_content: secrets[3],
          summary: [],
        },
        {
          type: "function_call",
          call_id: "call_private",
          name: secrets[2],
          arguments: "{}",
        },
      ]);
    } finally {
      Map.prototype.set = originalSet;
    }

    expect(retainedWrites.length).toBeGreaterThan(0);
    const retained = JSON.stringify(retainedWrites);
    for (const secret of secrets) expect(retained).not.toContain(secret);
    expect(Reflect.ownKeys(index)).toEqual([]);
  });

  it("compare-deletes only the exact matched generation", () => {
    const index = new AnthropicContinuationIndex({ key: KEY });
    const root = index.prepare(requestBody([user("cas")]), SCOPE);
    index.commit(root, SCOPE, "resp_old", [assistant("answer")]);
    const query = requestBody([user("cas"), assistant("answer"), user("next")]);
    const oldMatch = index.prepare(query, SCOPE).match!;
    expect(index.compareDelete(oldMatch)).toBe(true);
    index.commit(root, SCOPE, "resp_new", [assistant("answer")]);
    expect(index.compareDelete(oldMatch)).toBe(false);
    expect(index.prepare(query, SCOPE).match?.providerResponseId).toBe("resp_new");
  });
});

describe("Anthropic lossless replay projection", () => {
  it("round-trips ordered text, encrypted reasoning, and completed tool calls", () => {
    const final = finalResponse("resp_projection", [
      messageOutput("msg_1", [textPart("first"), textPart(" second")]),
      {
        ...reasoningOutput("rs_1", "first second", "ciphertext"),
        summary: [
          { type: "summary_text", text: "first " },
          { type: "summary_text", text: "second" },
        ],
      },
      messageOutput("msg_2", [textPart("third")]),
      toolOutput("fc_1", "call_1", "Read", "{ \"path\": \"a\" }"),
      messageOutput("msg_3", [textPart("tail")]),
    ]);
    const projected = responseToAnthropic(final, CONTEXT);

    expect(projectAnthropicReplay(final, projected)).toEqual([
      { type: "message", role: "assistant", content: "first second" },
      {
        type: "reasoning",
        id: "rs_1",
        encrypted_content: "ciphertext",
        summary: [
          { type: "summary_text", text: "first " },
          { type: "summary_text", text: "second" },
        ],
      },
      { type: "message", role: "assistant", content: "third" },
      {
        type: "function_call",
        call_id: "call_1",
        name: "Read",
        arguments: "{\"path\":\"a\"}",
      },
      { type: "message", role: "assistant", content: "tail" },
    ]);
  });

  it("projects provider-only message phases through the client-visible history", () => {
    for (const phase of ["commentary", "final_answer"] as const) {
      const final = finalResponse(`resp_${phase}`, [{
        ...messageOutput(`msg_${phase}`, [textPart("answer")]),
        phase,
      }]);
      expect(projectAnthropicReplay(final, responseToAnthropic(final, CONTEXT))).toEqual([
        { type: "message", role: "assistant", content: "answer" },
      ]);
    }
  });

  it("rejects replay when additive provider metadata or tool numbers cannot round-trip", () => {
    const additiveItem = finalResponse("resp_additive_item", [{
      ...messageOutput("msg_additive_item", [textPart("answer")]),
      future_item_state: { retained: true },
    }]);
    expect(projectAnthropicReplay(
      additiveItem,
      responseToAnthropic(additiveItem, CONTEXT),
    )).toBeNull();

    const additivePart = finalResponse("resp_additive_part", [messageOutput(
      "msg_additive_part",
      [{ ...textPart("answer"), future_part_state: true }],
    )]);
    expect(projectAnthropicReplay(
      additivePart,
      responseToAnthropic(additivePart, CONTEXT),
    )).toBeNull();

    const annotated = finalResponse("resp_annotation", [messageOutput("msg_annotation", [{
      ...textPart("cited"),
      annotations: [{
        type: "url_citation",
        title: "source",
        url: "https://example.invalid/source",
        start_index: 0,
        end_index: 5,
      }],
    }])]);
    expect(projectAnthropicReplay(
      annotated,
      responseToAnthropic(annotated, CONTEXT),
    )).toBeNull();

    const logprobs = finalResponse("resp_logprobs", [messageOutput("msg_logprobs", [{
      ...textPart("token"),
      logprobs: [{
        token: "token",
        bytes: [116, 111, 107, 101, 110],
        logprob: -0.25,
        top_logprobs: [],
      }],
    }])]);
    expect(projectAnthropicReplay(
      logprobs,
      responseToAnthropic(logprobs, CONTEXT),
    )).toBeNull();

    const rawReasoning = finalResponse("resp_reasoning_content", [{
      ...reasoningOutput("rs_content", "summary", "cipher"),
      content: [{ type: "reasoning_text", text: "private chain" }],
    }]);
    expect(projectAnthropicReplay(
      rawReasoning,
      responseToAnthropic(rawReasoning, CONTEXT),
    )).toBeNull();

    const unsafeInteger = finalResponse("resp_unsafe_integer", [
      toolOutput("fc_unsafe", "call_unsafe", "Calculate", '{"value":9007199254740993}'),
    ]);
    expect(projectAnthropicReplay(
      unsafeInteger,
      responseToAnthropic(unsafeInteger, CONTEXT),
    )).toBeNull();
  });

  it("skips refusal, filtering, incompleteness, missing ciphertext, and empty output", () => {
    const refusal = finalResponse("resp_refusal", [
      messageOutput("msg_refusal", [refusalPart("no")]),
    ]);
    expect(projectAnthropicReplay(refusal, responseToAnthropic(refusal, CONTEXT))).toBeNull();

    const filtered = finalResponse("resp_filter", [
      messageOutput("msg_filter", [textPart("partial")], "incomplete"),
    ], {
      status: "incomplete",
      incomplete_details: { reason: "content_filter" },
    });
    expect(projectAnthropicReplay(filtered, responseToAnthropic(filtered, CONTEXT))).toBeNull();

    const incomplete = finalResponse("resp_incomplete", [
      messageOutput("msg_incomplete", [textPart("partial")], "incomplete"),
    ], {
      status: "incomplete",
      incomplete_details: { reason: "max_output_tokens" },
    });
    expect(projectAnthropicReplay(incomplete, responseToAnthropic(incomplete, CONTEXT))).toBeNull();

    const noCipher = finalResponse("resp_no_cipher", [
      { type: "reasoning", id: "rs_no_cipher", status: "completed", summary: [] },
      messageOutput("msg_after", [textPart("visible")]),
    ]);
    expect(projectAnthropicReplay(noCipher, responseToAnthropic(noCipher, CONTEXT))).toBeNull();

    for (const output of [[], [messageOutput("msg_empty", [])], [
      messageOutput("msg_empty_text", [textPart("")]),
    ]]) {
      const empty = finalResponse("resp_empty", output);
      expect(projectAnthropicReplay(empty, responseToAnthropic(empty, CONTEXT))).toBeNull();
    }
  });

  it("skips stop-truncated, prewarm, unknown, and non-roundtrippable output", () => {
    const stopped = finalResponse("resp_stopped", [
      messageOutput("msg_stopped", [textPart("before STOP after")]),
    ]);
    const stoppedProjection = responseToAnthropic(stopped, context({ stopSequences: ["STOP"] }));
    expect(projectAnthropicReplay(stopped, stoppedProjection)).toBeNull();

    const prewarm = finalResponse("resp_prewarm", [
      messageOutput("msg_prewarm", [textPart("generated")], "incomplete"),
    ], {
      status: "incomplete",
      incomplete_details: { reason: "max_output_tokens" },
    });
    const prewarmProjection = responseToAnthropic(prewarm, context({ maxTokensZero: true }));
    expect(projectAnthropicReplay(prewarm, prewarmProjection)).toBeNull();

    const ordinary = finalResponse("resp_mutated", [
      messageOutput("msg_mutated", [textPart("authoritative")]),
    ]);
    const mutated = structuredClone(responseToAnthropic(ordinary, CONTEXT));
    (mutated.content[0] as { text: string }).text = "different";
    expect(projectAnthropicReplay(ordinary, mutated)).toBeNull();

    const programCall = finalResponse("resp_program", [{
      ...toolOutput("fc_program", "call_program", "Read", "{}"),
      caller: { type: "program", caller_id: "code" },
    }]);
    expect(projectAnthropicReplay(
      programCall,
      responseToAnthropic(programCall, CONTEXT),
    )).toBeNull();

    const unknown = finalResponse("resp_unknown", [{ type: "future_output", secret: "x" }]);
    expect(projectAnthropicReplay(unknown, responseToAnthropic(ordinary, CONTEXT))).toBeNull();
  });
});
