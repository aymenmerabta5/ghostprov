import { describe, expect, it } from "bun:test";
import { SidecarEngine } from "../src/v2/sidecar/engine";
import { SidecarEngine as R10SidecarEngine } from "../src/v2/sidecar/r10-engine";
import { normalizeR10ProviderError } from "../src/v2/sidecar/r10-provider-error";
import { UpstreamExecutionError } from "../src/v2/sidecar/upstream";
import { SidecarHarness, TEST_R2_BINDING } from "./support/fake-v2-sidecar";

const BODY = { model: "gpt-5.6-sol", input: "journal compatibility", stream: true, store: true };
// A prior journal writer accepted this exact 512-character summary. Its full
// bounded normalizer reproduced the partial marker on every validation pass.
const LEGACY_MESSAGE = "x ".repeat(248) + "authorization: [";

describe.each([
  ["frozen journal after R10 normalization", SidecarEngine],
  ["R10", R10SidecarEngine],
] as const)("%s provider-error journal compatibility", (_revision, engineClass) => {
  async function rejectedRequest(message: string): Promise<SidecarHarness> {
    const fatals: Error[] = [];
    const harness = new SidecarHarness({
      engineClass,
      binding: TEST_R2_BINDING,
      onFatalError: error => fatals.push(error),
      behavior: async input => {
        await input.beforeFetch();
        throw await normalizeR10ProviderError(new UpstreamExecutionError(message, "rate_limit_exceeded", false, 429));
      },
    });
    await harness.sendRequest(BODY);
    await harness.settle();
    expect(harness.journal.status(harness.requestId)).toBe("provider_error");
    expect(harness.upstream.calls).toBe(1);
    expect(fatals).toEqual([]);
    return harness;
  }

  it("recovers an existing partial-marker summary without provider execution", async () => {
    expect(LEGACY_MESSAGE).toHaveLength(512);
    const source = await rejectedRequest("Rate limit reached");
    const records = structuredClone(source.storage.records);
    const outcome = records.find(record => record.state === "provider_error")?.outcome;
    if (outcome?.kind !== "provider_error") throw new Error("Missing provider-error fixture");
    outcome.error.message = LEGACY_MESSAGE;

    const restarted = new SidecarHarness({ engineClass, binding: TEST_R2_BINDING, records });
    await restarted.engine.start();
    await restarted.engine.replayRecovered();

    expect(restarted.upstream.calls).toBe(0);
    expect(restarted.journal.status(source.requestId)).toBe("provider_error");
    expect(restarted.journal.canRetry(source.requestId)).toBe(false);
    expect(restarted.frames.filter(frame => frame.kind === "response_error")).toEqual([
      expect.objectContaining({
        requestId: source.requestId,
        code: "rate_limit_exceeded",
        message: LEGACY_MESSAGE,
        ambiguous: false,
      }),
    ]);
    expect(restarted.engine.snapshot()).toMatchObject({ active: 0, ambiguous: 0, replaying: 0 });
    await restarted.engine.acknowledgeDeliveredOutcomes([source.requestId]);
    expect(restarted.journal.status(source.requestId)).toBe("acked");
    expect(restarted.upstream.calls).toBe(0);
  });

  it("durably round-trips an ordinary summary cut within whitespace", async () => {
    const message = "Rate limit reached. ".padEnd(512, " ") + "More detail";
    expect(message.slice(0, 512).endsWith(" ")).toBe(true);
    const source = await rejectedRequest(message);
    const stored = source.storage.records.find(record => record.state === "provider_error")?.outcome;
    expect(stored).toMatchObject({ kind: "provider_error", error: { message: "Rate limit reached." } });
    expect(source.frames.filter(frame => frame.kind === "response_error")).toEqual([
      expect.objectContaining({ code: "rate_limit_exceeded", ambiguous: false }),
    ]);

    const restarted = new SidecarHarness({
      engineClass,
      binding: TEST_R2_BINDING,
      records: structuredClone(source.storage.records),
    });
    await restarted.engine.start();
    await restarted.engine.replayRecovered();
    expect(restarted.upstream.calls).toBe(0);
    expect(restarted.frames.filter(frame => frame.kind === "response_error")).toEqual([
      expect.objectContaining({
        requestId: source.requestId,
        code: "rate_limit_exceeded",
        message: "Rate limit reached.",
        ambiguous: false,
      }),
    ]);
    expect(restarted.journal.canRetry(source.requestId)).toBe(false);
    await restarted.engine.acknowledgeDeliveredOutcomes([source.requestId]);
    expect(restarted.journal.status(source.requestId)).toBe("acked");
  });
});
