import { describe, expect, it } from "bun:test";
import {
  createProviderErrorEnvelope as createFrozenProviderErrorEnvelope,
  parseProviderErrorEnvelope,
  PROVIDER_ERROR_MESSAGE_MAX_BYTES,
  PROVIDER_ERROR_PAYLOAD_MAX_BYTES,
  sanitizeProviderText,
  type CreateProviderErrorEnvelopeInput,
  type ProviderErrorEnvelope,
} from "../src/v2/provider-error";
import { normalizeR10ProviderError, normalizeR10ProviderErrorEnvelope } from "../src/v2/sidecar/r10-provider-error";
import { UpstreamExecutionError } from "../src/v2/sidecar/upstream";
import { SidecarEngine as R10SidecarEngine } from "../src/v2/sidecar/r10-engine";
import { SidecarHarness, TEST_R2_BINDING } from "./support/fake-v2-sidecar";

const encoder = new TextEncoder();
async function createProviderErrorEnvelope(input: CreateProviderErrorEnvelopeInput) {
  return normalizeR10ProviderErrorEnvelope(await createFrozenProviderErrorEnvelope(input));
}

describe("R10 provider-error normalization", () => {
  it("keeps every redaction-marker cutoff valid without exceeding field or payload byte caps", async () => {
    for (const label of ["authorization: ", "Bearer "]) {
      for (let retainedMarkerBytes = 1; retainedMarkerBytes < "[REDACTED]".length; retainedMarkerBytes++) {
        for (const target of ["message", "payload"] as const) {
          const limit = target === "message" ? PROVIDER_ERROR_MESSAGE_MAX_BYTES : PROVIDER_ERROR_PAYLOAD_MAX_BYTES;
          const text = "Rate limit reached. ".padEnd(limit - label.length - retainedMarkerBytes, " ") +
            label + "fixture-credential";
          const envelope = await createProviderErrorEnvelope({
            origin: "http",
            status: 429,
            value: { error: { message: target === "message" ? text : "Rate limit reached", code: "rate_limit_exceeded" } },
            ...(target === "payload" ? { payload: text } : {}),
          });
          expect(parseProviderErrorEnvelope(envelope)).toBe(envelope);
          const bounded = target === "message" ? envelope.error.message : envelope.payload.text;
          expect(encoder.encode(bounded).byteLength).toBeLessThanOrEqual(limit);
          expect(sanitizeProviderText(bounded)).toBe(bounded);
          expect(JSON.stringify(envelope)).not.toContain("fixture-credential");
        }
      }
    }
  });

  it("repairs escaped diagnostics after redaction and payload truncation", async () => {
    for (const label of ["authorization: ", "Bearer "]) {
      for (const padding of [0, ...Array.from({ length: 160 }, (_, index) => 8_030 + index)]) {
        const quotedMessage = JSON.stringify({ message: "x".repeat(padding) + " " + label + "fixture-credential" });
        const envelope = await createProviderErrorEnvelope({
          origin: "sse",
          status: 429,
          value: { error: { message: quotedMessage, code: "rate_limit_exceeded" } },
        });
        expect(parseProviderErrorEnvelope(envelope)).toBe(envelope);
        expect(sanitizeProviderText(envelope.payload.text)).toBe(envelope.payload.text);
        expect(JSON.stringify(envelope)).not.toContain("fixture-credential");
        if (!envelope.payload.truncated) {
          expect(JSON.parse(envelope.payload.text).error.message).toContain("[REDACTED]");
        }
      }
    }
    for (const credential of ["real\\secret", "[REDACTED]real-secret", "[REDACTED]\\real-secret"]) {
      expect(sanitizeProviderText(`authorization: ${credential}`)).toBe("authorization: [REDACTED]");
      expect(sanitizeProviderText(`Bearer ${credential}`)).toBe("Bearer [REDACTED]");
    }
  });

  it("preserves valid envelopes and refuses structural or status-conflicting repairs", async () => {
    const valid = await createFrozenProviderErrorEnvelope({ origin: "http", status: 429, value: { error: { message: "Rate limit reached", code: "rate_limit_exceeded" } } });
    expect(await normalizeR10ProviderErrorEnvelope(valid)).toBe(valid);
    const invalidText = await createFrozenProviderErrorEnvelope({ origin: "http", status: 429, value: { error: { message: "Rate limit reached. ".padEnd(2032, " ") + "authorization: fake", code: "rate_limit_exceeded" } } });
    expect(parseProviderErrorEnvelope(invalidText)).toBeNull();
    expect(await normalizeR10ProviderErrorEnvelope(invalidText, 500)).toBe(invalidText);
    const throwingSerializer = Object.assign(Object.create({
      toJSON() { throw new Error("synthetic malformed envelope serializer"); },
    }), valid) as ProviderErrorEnvelope;
    expect(parseProviderErrorEnvelope(throwingSerializer)).toBeNull();
    expect(await normalizeR10ProviderErrorEnvelope(throwingSerializer)).toBe(throwingSerializer);
    for (const malformed of [
      { ...invalidText, version: 2 },
      { ...invalidText, origin: "unknown" },
      { ...invalidText, status: 200 },
      { ...invalidText, extra: true },
      { ...invalidText, error: { ...invalidText.error, code: 429 } },
      { ...invalidText, headers: { authorization: "fixture-value" } },
      { ...invalidText, payload: { ...invalidText.payload, truncated: "yes" } },
      { ...invalidText, organizationFingerprint: "wrong" },
    ]) {
      expect(await normalizeR10ProviderErrorEnvelope(malformed as ProviderErrorEnvelope)).toBe(malformed as ProviderErrorEnvelope);
    }
    const ambiguous = new UpstreamExecutionError("unproven", "rate_limit_exceeded", true, 429, undefined, invalidText);
    expect(await normalizeR10ProviderError(ambiguous)).toBe(ambiguous);
    const withoutEnvelope = new UpstreamExecutionError("Rate limit reached", "rate_limit_exceeded", false, 429);
    expect((await normalizeR10ProviderError(withoutEnvelope)).providerError).toBeUndefined();
  });

  it("keeps malformed serializer evidence on the journal failure path without escaping the execution task", async () => {
    const valid = await createFrozenProviderErrorEnvelope({ origin: "http", status: 429, value: { error: { message: "Rate limit reached", code: "rate_limit_exceeded" } } });
    const malformed = Object.assign(Object.create({
      toJSON() { throw new Error("synthetic malformed envelope serializer"); },
    }), valid) as ProviderErrorEnvelope;
    const fatals: Error[] = [];
    const harness = new SidecarHarness({
      engineClass: R10SidecarEngine,
      binding: TEST_R2_BINDING,
      onFatalError: error => fatals.push(error),
      behavior: async input => {
        await input.beforeFetch();
        throw new UpstreamExecutionError("Rate limit reached", "rate_limit_exceeded", false, 429, undefined, malformed);
      },
    });
    await harness.sendRequest({ model: "gpt-5.6-sol", input: "malformed evidence", stream: true, store: true });
    await harness.settle();
    expect(harness.upstream.calls).toBe(1);
    expect(harness.journal.status(harness.requestId)).toBe("fetch_starting");
    expect(harness.journal.canRetry(harness.requestId)).toBe(false);
    expect(fatals).toHaveLength(1);
    expect(fatals[0]).toMatchObject({ name: "ClaimJournalError", code: "transition" });
    expect(harness.frames.filter(frame => frame.kind === "response_error")).toEqual([
      expect.objectContaining({ requestId: harness.requestId, code: "rate_limit_exceeded", ambiguous: true }),
    ]);
    expect(harness.engine.snapshot()).toMatchObject({ active: 0, ambiguous: 1 });
  });
});
