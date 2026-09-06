import { describe, expect, it } from "bun:test";
import { createAuthenticatedTenantScope } from "../src/http/api-key";
import {
  ResponsesHttpContinuationIndex,
  responsesContinuationPreparationCostBytes,
} from "../src/http/responses-continuation";

const KEY = new Uint8Array(32).fill(0x31);
const SCOPE = createAuthenticatedTenantScope();

function user(text: string) {
  return {
    type: "message",
    role: "user",
    content: [{ type: "input_text", text }],
  };
}

function assistant(text: string) {
  return {
    type: "message",
    id: `msg_${text}`,
    status: "completed",
    role: "assistant",
    content: [{ type: "output_text", text, annotations: [], logprobs: [] }],
  };
}

function body(input: unknown[], overrides: Record<string, unknown> = {}) {
  return {
    model: "gpt-5.6-sol",
    input,
    store: true,
    stream: true,
    ...overrides,
  };
}

function final(id: string, output: unknown[], status = "completed") {
  return { id, object: "response", status, output };
}

describe("native Responses full-history continuation", () => {
  it("charges immutable snapshot bytes even when ingress covers the source", () => {
    expect(responsesContinuationPreparationCostBytes(64 * 1024 * 1024, 128 * 1024 * 1024))
      .toBe(64 * 1024 * 1024);
    expect(responsesContinuationPreparationCostBytes(8_192, 2_048)).toBe(14_336);
    for (const invalid of [-1, 1.5, Number.MAX_SAFE_INTEGER + 1]) {
      expect(() => responsesContinuationPreparationCostBytes(invalid, 0)).toThrow(RangeError);
      expect(() => responsesContinuationPreparationCostBytes(1, invalid)).toThrow(RangeError);
      expect(() => responsesContinuationPreparationCostBytes(1, 0, invalid)).toThrow(RangeError);
    }
  });

  it("sends only an exact suffix after a provider output boundary", async () => {
    const index = new ResponsesHttpContinuationIndex({ key: KEY });
    const controller = new AbortController();
    const firstInput = [user("one")];
    const first = await index.prepare(body(firstInput), SCOPE, controller.signal, 1_024);
    expect(first?.token.decision).toBe("miss");
    const answer = assistant("answer");
    expect(await index.commit(
      first?.token ?? null,
      SCOPE,
      final("resp_parent", [answer]),
    )).toBeTrue();

    const current = body([
      ...firstInput,
      answer,
      user("two"),
    ], { temperature: 0.2 });
    const changed = await index.prepare(current, SCOPE, controller.signal, 2_048);
    expect(changed?.token.decision).toBe("miss");
    expect(changed?.relayBody).not.toHaveProperty("previous_response_id");

    const matchingSettings = body([
      ...firstInput,
      answer,
      user("two"),
    ]);
    const continued = await index.prepare(
      matchingSettings,
      SCOPE,
      controller.signal,
      2_048,
    );
    expect(continued?.token).toMatchObject({
      decision: "hit",
      match: { providerResponseId: "resp_parent", prefixItemCount: 2 },
    });
    expect(continued?.relayBody).toEqual({
      ...matchingSettings,
      input: [user("two")],
      previous_response_id: "resp_parent",
    });
  });

  for (const sizeMiB of [18, 55]) it(`reuses an exact ${sizeMiB} MiB history without retaining its content in the index`, async () => {
    const index = new ResponsesHttpContinuationIndex({ key: KEY });
    const controller = new AbortController();
    const root = user("large-history-".padEnd(sizeMiB * 1024 * 1024, "x"));
    const cost = (2 * sizeMiB + 1) * 1024 * 1024;
    const first = await index.prepare(body([root]), SCOPE, controller.signal, cost);
    const answer = assistant("large-answer");
    expect(await index.commit(first?.token ?? null, SCOPE, final("resp_large", [answer])))
      .toBeTrue();
    const suffix = user("next");
    const next = await index.prepare(
      body([root, answer, suffix]), SCOPE, controller.signal, cost,
    );
    expect(next?.token.decision).toBe("hit");
    expect(next?.relayBody).toEqual(body([suffix], { previous_response_id: "resp_large" }));
    expect(next?.fullRelayBody.input).toEqual([root, answer, suffix]);
    expect(index.snapshot()).toMatchObject({ records: 2, logicalMetadataBytes: 3_072 });
    index.beginShutdown();
    index.clear();
  });

  it("matches Codex replay while forwarding new tracing labels and every model setting", async () => {
    const index = new ResponsesHttpContinuationIndex({ key: KEY });
    const signal = new AbortController().signal;
    const root = user("root");
    const firstMetadata = { session_id: "same", turn_id: "first", "x-codex-turn-metadata": "trace-first" };
    const first = await index.prepare(body([root], { client_metadata: firstMetadata }), SCOPE, signal, 1_024);
    const answer = assistant("answer");
    expect(await index.commit(first!.token, SCOPE, final("resp_codex", [answer]))).toBeTrue();
    const replay = { type: "message", id: answer.id, role: "assistant", content: [{ type: "output_text", text: "answer" }] };
    const metadata = { session_id: "same", turn_id: "second", "x-codex-turn-metadata": "trace-second" };
    const nextBody = body([root, replay, user("next")], { client_metadata: metadata });
    const next = await index.prepare(nextBody, SCOPE, signal, 2_048);
    expect(next?.token.decision).toBe("hit");
    expect(next?.relayBody).toEqual(body([user("next")], { previous_response_id: "resp_codex", client_metadata: metadata }));
    expect(next?.fullRelayBody).toEqual(nextBody);
    expect(next?.relayBody.client_metadata).not.toBe(metadata);
    expect(answer).toHaveProperty("status", "completed");
    expect(answer.content[0]).toHaveProperty("annotations", []);
    const changed = await index.prepare({ ...nextBody, temperature: 0.5 }, SCOPE, signal, 2_048);
    expect(changed?.token.decision).toBe("miss");
    index.beginShutdown();
    index.clear();
  });

  it("requires exact unknown metadata and never strips meaningful replay fields", async () => {
    const index = new ResponsesHttpContinuationIndex({ key: KEY });
    const signal = new AbortController().signal;
    const root = user("root");
    const settings = { client_metadata: { turn_id: "one", future_setting: "strict" } };
    const first = await index.prepare(body([root], settings), SCOPE, signal, 1_024);
    const answer = {
      ...assistant("answer"), future_output: true,
      content: [{ type: "output_text", text: "answer", annotations: [{ type: "citation" }], logprobs: [] }],
    };
    expect(await index.commit(first!.token, SCOPE, final("resp_exact", [answer]))).toBeTrue();
    const changed = await index.prepare(body([root, answer, user("next")], {
      client_metadata: { ...settings.client_metadata, turn_id: "two" },
    }), SCOPE, signal, 2_048);
    expect(changed?.token.decision).toBe("miss");
    const lossy = { type: "message", id: answer.id, role: "assistant", content: [{ type: "output_text", text: "answer" }] };
    const omitted = await index.prepare(body([root, lossy, user("next")], settings), SCOPE, signal, 2_048);
    expect(omitted?.token.decision).toBe("miss");
    index.beginShutdown();
    index.clear();
  });

  it("keeps the tracing byte bound exact and treats unsupported values as settings", async () => {
    for (const [value, expected] of [
      ["x".repeat(32 * 1024 - "turn_id".length), "hit"],
      ["x".repeat(32 * 1024 - "turn_id".length + 1), "miss"],
      [null, "miss"],
      [{ nested: "value" }, "miss"],
    ] as const) {
      const index = new ResponsesHttpContinuationIndex({ key: KEY });
      const signal = new AbortController().signal;
      const root = user("root");
      const prepared = await index.prepare(body([root], { client_metadata: { turn_id: value } }), SCOPE, signal, 100_000);
      const answer = assistant("answer");
      expect(await index.commit(prepared!.token, SCOPE, final("resp_trace_bound", [answer]))).toBeTrue();
      const next = await index.prepare(body([root, answer, user("next")], {
        client_metadata: { turn_id: "new" },
      }), SCOPE, signal, 100_000);
      expect(next?.token.decision).toBe(expected);
      index.beginShutdown();
      index.clear();
    }
  });

  it("matches a complete reasoning and message output sequence without reordering", async () => {
    const index = new ResponsesHttpContinuationIndex({ key: KEY });
    const controller = new AbortController();
    const root = user("root");
    const prepared = await index.prepare(body([root]), SCOPE, controller.signal, 1_024);
    const reasoning = {
      type: "reasoning",
      id: "rs_complete",
      encrypted_content: "opaque",
      summary: [],
    };
    const message = assistant("complete");
    expect(await index.commit(
      prepared?.token ?? null,
      SCOPE,
      final("resp_reasoned", [reasoning, message]),
    )).toBeTrue();

    const next = body([root, reasoning, message, user("next")]);
    const continued = await index.prepare(next, SCOPE, controller.signal, 2_048);
    expect(continued?.token).toMatchObject({
      decision: "hit",
      match: { providerResponseId: "resp_reasoned", prefixItemCount: 3 },
    });
    expect(continued?.relayBody).toEqual({
      ...next,
      input: [user("next")],
      previous_response_id: "resp_reasoned",
    });
  });

  it("never replaces an explicit caller continuation field", async () => {
    const index = new ResponsesHttpContinuationIndex({ key: KEY });
    const controller = new AbortController();
    for (const previous_response_id of ["resp_explicit", null, undefined]) {
      const request = body([user("delta")], { previous_response_id });
      expect(await index.prepare(request, SCOPE, controller.signal, 1)).toBeNull();
      expect(request).toHaveProperty("previous_response_id", previous_response_id);
    }
  });

  it("skips failed, incomplete, empty, and malformed provider output", async () => {
    const index = new ResponsesHttpContinuationIndex({ key: KEY });
    const controller = new AbortController();
    const cases = [
      final("resp_failed", [assistant("failed")], "failed"),
      final("resp_incomplete", [assistant("partial")], "incomplete"),
      final("resp_empty", []),
      final("resp_malformed", [{ id: "missing-type" }]),
      final(
        "resp_too_many",
        Array.from({ length: 4_097 }, () => assistant("repeated")),
      ),
    ];
    for (const [position, terminal] of cases.entries()) {
      const root = user(`root-${position}`);
      const prepared = await index.prepare(body([root]), SCOPE, controller.signal, 1);
      expect(await index.commit(prepared?.token ?? null, SCOPE, terminal)).toBeFalse();
      const output = (terminal.output as unknown[])[0];
      if (!output || typeof output !== "object" || !("type" in output)) continue;
      const next = await index.prepare(
        body([root, output, user("next")]),
        SCOPE,
        controller.signal,
        1,
      );
      expect(next?.token.decision ?? "miss").toBe("miss");
    }
    expect(index.snapshot().records).toBe(0);
  });

  it("rejects a cumulative prefix even when input and replay fit separately", async () => {
    const index = new ResponsesHttpContinuationIndex({
      key: KEY,
      maxCanonicalBytes: 1_024,
    });
    const controller = new AbortController();
    const root = user("bounded-root");
    const prepared = await index.prepare(body([root]), SCOPE, controller.signal, 1);
    expect((prepared?.token.settingsCanonicalBytes ?? 0) +
      (prepared?.token.inputDigest.canonicalBytes ?? 0)).toBeLessThan(1_024);

    expect(await index.commit(
      prepared?.token ?? null,
      SCOPE,
      final("resp_oversized", [assistant("x".repeat(600))]),
    )).toBeFalse();
    expect(index.snapshot().records).toBe(0);
  });

  it("closes queued and running cooperative preparation on shutdown", async () => {
    const index = new ResponsesHttpContinuationIndex({ key: KEY });
    const controller = new AbortController();
    const largeInput = Array.from(
      { length: 1_024 },
      (_, position) => user(`shutdown-${position}-${"x".repeat(128)}`),
    );
    const running = index.prepare(body(largeInput), SCOPE, controller.signal, 1);
    const queued = index.prepare(body([user("queued")]), SCOPE, controller.signal, 1);

    await new Promise<void>(resolve => setTimeout(resolve, 0));
    index.beginShutdown();

    expect(await running).toBeNull();
    expect(await queued).toBeNull();
    expect(await index.prepare(body([user("late")]), SCOPE, controller.signal, 1))
      .toBeNull();
    expect(index.snapshot().records).toBe(0);
  });

  it("does not publish a cooperative commit that shutdown interrupts", async () => {
    const index = new ResponsesHttpContinuationIndex({ key: KEY });
    const controller = new AbortController();
    const prepared = await index.prepare(
      body([user("commit-shutdown")]),
      SCOPE,
      controller.signal,
      1,
    );
    const pending = index.commit(
      prepared?.token ?? null,
      SCOPE,
      final("resp_interrupted", [assistant("x".repeat(256 * 1_024))]),
    );

    await new Promise<void>(resolve => setTimeout(resolve, 0));
    index.beginShutdown();

    expect(await pending).toBeFalse();
    expect(index.snapshot().records).toBe(0);
  });

  it("does not publish a cooperative commit that its request cancels", async () => {
    const index = new ResponsesHttpContinuationIndex({ key: KEY });
    const controller = new AbortController();
    const prepared = await index.prepare(
      body([user("commit-cancel")]),
      SCOPE,
      controller.signal,
      1,
    );
    const pending = index.commit(
      prepared?.token ?? null,
      SCOPE,
      final("resp_cancelled", [assistant("x".repeat(256 * 1_024))]),
      controller.signal,
    );

    await new Promise<void>(resolve => setTimeout(resolve, 0));
    controller.abort(new DOMException("cancelled", "AbortError"));

    expect(await pending).toBeFalse();
    expect(index.snapshot().records).toBe(0);
  });
});
