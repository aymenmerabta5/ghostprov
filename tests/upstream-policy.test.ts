import { describe, expect, it } from "bun:test";
import { normalizeHostUpstreamResponsesRequest as normalizeUpstreamResponsesRequest } from "../src/relay/host-upstream-policy";
import { applyUpstreamServiceTierPolicy } from "../src/relay/service-tier-policy";

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

  it("materializes non-enumerable policy fields so they are present on the wire", () => {
    const body = {
      model: "gpt-5.6-sol",
      input: [],
      previous_response_id: "resp_parent_exact",
    } as Record<string, unknown>;
    Object.defineProperties(body, {
      stream: { value: true, enumerable: false },
      store: { value: true, enumerable: false },
    });

    const normalized = normalizeUpstreamResponsesRequest(body);

    expect(normalized).not.toBe(body);
    expect(Object.prototype.propertyIsEnumerable.call(normalized, "stream")).toBe(true);
    expect(Object.prototype.propertyIsEnumerable.call(normalized, "store")).toBe(true);
    expect(JSON.parse(JSON.stringify(normalized))).toMatchObject({
      stream: true,
      store: true,
      previous_response_id: "resp_parent_exact",
    });
  });

  it("copies enumerable accessors so later reads cannot change wire policy", () => {
    let reads = 0;
    const body = { model: "gpt-5.6-sol", input: [] } as Record<string, unknown>;
    Object.defineProperties(body, {
      stream: {
        enumerable: true,
        get: () => ++reads === 1,
      },
      store: {
        enumerable: true,
        get: () => true,
      },
    });

    const normalized = normalizeUpstreamResponsesRequest(body);

    expect(normalized).not.toBe(body);
    expect(JSON.parse(JSON.stringify(normalized))).toMatchObject({ stream: true, store: true });
  });
});

describe("applyUpstreamServiceTierPolicy", () => {
  it("forces every omitted or explicit text tier to fast idempotently", () => {
    for (const service_tier of [undefined, null, "auto", "default", "flex", "priority", "ultrafast"]) {
      const body = {
        model: "gpt-5.6-sol",
        input: "text",
        ...(service_tier === undefined ? {} : { service_tier }),
      };
      const normalized = applyUpstreamServiceTierPolicy(body);
      expect(normalized).toEqual({ ...body, service_tier: "fast" });
      expect(applyUpstreamServiceTierPolicy(normalized)).toBe(normalized);
    }
  });

  it("strips image tiers without mutating caller-owned bodies", () => {
    for (const body of [
      { model: "gpt-image-2", input: "image", service_tier: "priority" },
      {
        model: "gpt-5.6-sol",
        input: "image",
        service_tier: "fast",
        tools: [{ type: "image_generation" }],
      },
    ]) {
      const normalized = applyUpstreamServiceTierPolicy(body);
      expect(normalized).not.toBe(body);
      expect(normalized).not.toHaveProperty("service_tier");
      expect(body).toHaveProperty("service_tier");
    }
    const tierless = { model: "gpt-image-2", input: "image" };
    expect(applyUpstreamServiceTierPolicy(tierless)).toBe(tierless);
    const inherited = Object.assign(Object.create({ service_tier: "priority" }), {
      model: "gpt-image-2",
      input: "image",
    });
    const normalizedInherited = applyUpstreamServiceTierPolicy(inherited);
    expect(normalizedInherited).not.toHaveProperty("service_tier");
    expect("service_tier" in normalizedInherited).toBe(false);
  });
});
