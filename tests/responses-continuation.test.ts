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
