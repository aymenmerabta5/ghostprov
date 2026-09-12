import { describe, expect, it } from "bun:test";
import { applyUpstreamModelPolicy, canFallbackFromModel } from "../src/relay/model-policy";
import { prepareHostUpstreamResponsesRequest } from "../src/relay/host-request-policy";
import { normalizeHostUpstreamResponsesRequest } from "../src/relay/host-upstream-policy";

describe("upstream model policy", () => {
  it("bounds fallback model metadata and excludes a repeated Terra attempt", () => {
    expect(canFallbackFromModel("m".repeat(128))).toBe(true);
    for (const model of ["m".repeat(129), "", null, undefined, "gpt-5.6-terra"]) {
      expect(canFallbackFromModel(model)).toBe(false);
    }
  });
  it("aliases Luna without mutating caller history or provider lineage", () => {
    const input = Object.freeze([{ role: "user", content: "continue" }]);
    const tools = Object.freeze([{ type: "web_search" }]);
    const body = Object.freeze({
      model: "gpt-5.6-luna", input, tools,
      previous_response_id: "resp_exact_parent",
      stream: false, store: false, vendor_option: null,
    });
    const result = prepareHostUpstreamResponsesRequest(body);
    expect(result).toEqual({ ...body, model: "gpt-5.6-terra", stream: true, store: true });
    expect(result.input).toBe(input);
    expect(result.tools).toBe(tools);
    expect(body.model).toBe("gpt-5.6-luna");
    expect(body.stream).toBe(false);
    expect(prepareHostUpstreamResponsesRequest(result)).toBe(result);
    // Persistence normalization remains separate and changes only its two fields.
    expect(normalizeHostUpstreamResponsesRequest(body).model).toBe("gpt-5.6-luna");
  });

  it.each(["gpt-5.6-terra", "gpt-6-astra", "gpt-5.2", "gpt-5.6-luna-extra", "GPT-5.6-LUNA", " gpt-5.6-luna", null])(
    "preserves every non-alias model exactly: %p", model => {
      const body = Object.freeze({ model, input: [], stream: true, store: true });
      expect(applyUpstreamModelPolicy(body)).toBe(body);
      expect(prepareHostUpstreamResponsesRequest(body)).toBe(body);
    },
  );

  it("does not invent missing model or continuation fields", () => {
    const body = Object.freeze({ input: [], stream: true, store: true });
    expect(prepareHostUpstreamResponsesRequest(body)).toBe(body);
    expect(prepareHostUpstreamResponsesRequest(body)).not.toHaveProperty("model");
    expect(prepareHostUpstreamResponsesRequest(body)).not.toHaveProperty("previous_response_id");
  });
});
