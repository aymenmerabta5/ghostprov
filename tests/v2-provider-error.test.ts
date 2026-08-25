import { describe, expect, it } from "bun:test";
import {
  PROVIDER_ERROR_ENVELOPE_MAX_BYTES,
  PROVIDER_ERROR_PAYLOAD_MAX_BYTES,
  createProviderErrorEnvelope,
  parseProviderErrorEnvelope,
  providerErrorEnvelopeFrom,
} from "../src/v2/provider-error";

const encoder = new TextEncoder();

describe("bounded provider error envelopes", () => {
  it("bounds payload bytes and never includes credential headers", async () => {
    const envelope = await createProviderErrorEnvelope({
      origin: "http",
      status: 400,
      model: "gpt-5.6-sol",
      headers: new Headers({
        authorization: "Bearer header-secret",
        cookie: "session=header-secret",
        "retry-after": "0.125",
        "x-request-id": "req_safe_1",
      }),
      value: {
        error: {
          message: "bad request",
          type: "invalid_request_error",
          param: "input",
          code: "invalid_value",
        },
        access_token: "payload-secret",
        extra: "x".repeat(PROVIDER_ERROR_PAYLOAD_MAX_BYTES * 2),
      },
    });

    expect(envelope.headers).toEqual({ "retry-after": "0.125" });
    expect(envelope.upstreamRequestId).toBe("req_safe_1");
    expect(envelope.payload.truncated).toBe(true);
    expect(encoder.encode(envelope.payload.text).byteLength)
      .toBeLessThanOrEqual(PROVIDER_ERROR_PAYLOAD_MAX_BYTES);
    expect(encoder.encode(JSON.stringify(envelope)).byteLength)
      .toBeLessThanOrEqual(PROVIDER_ERROR_ENVELOPE_MAX_BYTES);
    expect(envelope.payload.text).not.toContain("payload-secret");
    expect(JSON.stringify(envelope)).not.toContain("header-secret");
  });

  it("strictly rejects unknown headers and oversized cross-process values", async () => {
    const envelope = await createProviderErrorEnvelope({
      origin: "sse",
      value: {
        type: "error",
        message: "upstream failed",
        code: "upstream_error",
      },
    });
    expect(parseProviderErrorEnvelope(envelope)).toBe(envelope);
    expect(providerErrorEnvelopeFrom({
      details: { providerError: envelope },
    })).toBe(envelope);
    expect(parseProviderErrorEnvelope({
      ...envelope,
      headers: { ...envelope.headers, authorization: "secret" },
    })).toBeNull();
    expect(parseProviderErrorEnvelope({
      ...envelope,
      payload: { ...envelope.payload, text: "x".repeat(PROVIDER_ERROR_PAYLOAD_MAX_BYTES + 1) },
    })).toBeNull();
  });
});
