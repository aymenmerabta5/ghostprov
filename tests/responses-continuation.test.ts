import { describe, expect, it } from "bun:test";
import { createAuthenticatedTenantScope } from "../src/http/api-key";
import { ResponsesHttpContinuationIndex } from "../src/http/responses-continuation";

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
  it("sends only an exact suffix after a provider output boundary", async () => {
    const index = new ResponsesHttpContinuationIndex({ key: KEY });
    const controller = new AbortController();
    const firstInput = [user("one")];
    const first = await index.prepare(body(firstInput), SCOPE, controller.signal, 1_024);
    expect(first?.token.decision).toBe("miss");
    const answer = assistant("answer");
    index.commit(first?.token ?? null, SCOPE, final("resp_parent", [answer]));

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
      index.commit(prepared?.token ?? null, SCOPE, terminal);
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
    expect(index.snapshot().entries).toBe(0);
  });
});
