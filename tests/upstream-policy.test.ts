import { describe, expect, it } from "bun:test";
import { normalizeUpstreamResponsesRequest } from "../src/relay/upstream-policy";

describe("normalizeUpstreamResponsesRequest", () => {
  it("returns the original request when both upstream invariants already hold", () => {
    const body = Object.freeze({
      model: "gpt-5.6-sol",
      input: "hello",
      stream: true,
      store: true,
      previous_response_id: "resp_parent_exact",
    } as const);

    const normalized = normalizeUpstreamResponsesRequest(body);

    expect(normalized).toBe(body);
    expect(normalizeUpstreamResponsesRequest(normalized)).toBe(normalized);
  });

  it.each([
    { stream: false, store: false },
    { stream: true, store: false },
    { stream: false, store: true },
    { stream: undefined, store: undefined },
  ] as const)(
    "makes one shallow copy for explicit non-true policy values %#",
    policy => {
      const extension = Object.freeze({ future: ["verbatim"] });
      const input = Object.freeze([{ role: "user", content: "continue" }]);
      const body = {
        model: "gpt-5.6-sol",
        input,
        previous_response_id: "resp_parent_exact",
        future_extension: extension,
        ...policy,
      };

      const normalized = normalizeUpstreamResponsesRequest(body);

      expect(normalized).not.toBe(body);
      expect(normalized).toEqual({ ...body, stream: true, store: true });
      expect(normalized.stream).toBe(true);
      expect(normalized.store).toBe(true);
      expect(normalized.previous_response_id).toBe("resp_parent_exact");
      expect(normalized.input).toBe(input);
      expect(normalized.future_extension).toBe(extension);
      expect(body.stream).toBe(policy.stream);
      expect(body.store).toBe(policy.store);
      expect(normalizeUpstreamResponsesRequest(normalized)).toBe(normalized);
    },
  );

  it("adds explicit true fields when stream and store are omitted", () => {
    const body = {
      model: "gpt-5.6-sol",
      input: [],
      previous_response_id: "resp_parent_exact",
      vendor_option: null,
    } as const;

    const normalized = normalizeUpstreamResponsesRequest(body);

    expect(normalized).not.toBe(body);
    expect(Object.hasOwn(body, "stream")).toBe(false);
    expect(Object.hasOwn(body, "store")).toBe(false);
    expect(normalized).toEqual({ ...body, stream: true, store: true });
    expect(Object.hasOwn(normalized, "stream")).toBe(true);
    expect(Object.hasOwn(normalized, "store")).toBe(true);
    expect(normalized.previous_response_id).toBe(body.previous_response_id);
    expect(normalized.vendor_option).toBeNull();
  });

  it("materializes inherited policy values as explicit own fields", () => {
    const body = Object.assign(Object.create({ stream: true, store: true }), {
      model: "gpt-5.6-sol",
      input: [],
      previous_response_id: "resp_parent_exact",
    }) as Record<string, unknown>;

    const normalized = normalizeUpstreamResponsesRequest(body);

    expect(normalized).not.toBe(body);
    expect(Object.hasOwn(normalized, "stream")).toBe(true);
    expect(Object.hasOwn(normalized, "store")).toBe(true);
    expect(normalized).toEqual({
      model: "gpt-5.6-sol",
      input: [],
      previous_response_id: "resp_parent_exact",
      stream: true,
      store: true,
    });
    expect(Object.hasOwn(body, "stream")).toBe(false);
    expect(Object.hasOwn(body, "store")).toBe(false);
  });
});
