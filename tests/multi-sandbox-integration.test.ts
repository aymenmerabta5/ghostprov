import { describe, expect, it } from "bun:test";
import { createFakeV2System } from "./support/fake-v2-system";
import { percentile, validateProofResponse } from "../scripts/live_multi_sandbox";

const system = () => createFakeV2System({
  runtimes: 4,
  physicalPerRuntime: 30,
  holdUpstream: true,
});

describe("four-project router integration", () => {
  it("validates live terminal identity, exact output markers, and percentiles", () => {
    const raw = JSON.stringify({
      id: "resp_unique",
      status: "completed",
      output: [{ type: "message", content: [{ type: "output_text", text: "RAMP_16_0" }] }],
    });
    expect(validateProofResponse(raw, "RAMP_16_0")).toEqual({ id: "resp_unique", status: "completed" });
    expect(() => validateProofResponse(raw, "wrong")).toThrow("expected wrong");
    expect(percentile([1, 2, 3, 4, 100], 0.95)).toBe(100);
  });

  it("publishes and terminally settles 120 requests without duplicates or oversubscription", async () => {
    const value = await system();
    try {
      const started = Array.from({ length: 120 }, (_, index) => value.router.startRelay(
        { model: "gpt-5.6-sol", input: `integration-${index}`, stream: true },
        { clientId: `client-${index % 8}`, source: "http" },
      ));
      const handles = await Promise.all(started);
      await value.waitForActive(120);

      expect(value.runtimes.map(runtime => runtime.snapshot().active)).toEqual([30, 30, 30, 30]);
      expect(value.runtimes.every(runtime => runtime.upstream.maxActive <= 30)).toBe(true);
      expect(value.router.snapshot()).toMatchObject({ active: 120, capacity: 120, queued: 0 });

      value.releaseUpstream();
      const finals = await Promise.all(handles.map(handle => handle.final));

      expect(finals).toHaveLength(120);
      expect(new Set(finals.map(final => final.id)).size).toBe(120);
      expect(value.totalUpstreamCalls()).toBe(120);
      expect(value.runtimes.reduce((sum, runtime) => sum + runtime.snapshot().quarantined, 0)).toBe(0);
    } finally {
      await value.close();
    }
  }, 15_000);

  it("isolates a draining project while keeping the other 90 lanes routable", async () => {
    const value = await system();
    try {
      await value.runtimes[0]!.drain("integration-test");
      const handles = await Promise.all(Array.from({ length: 90 }, (_, index) => value.router.startRelay(
        { model: "gpt-5.6-sol", input: `healthy-${index}`, stream: true },
        { clientId: "healthy", source: "http" },
      )));
      await value.waitForActive(90);
      expect(value.runtimes[0]!.snapshot().active).toBe(0);
      expect(value.runtimes.slice(1).map(runtime => runtime.snapshot().active)).toEqual([30, 30, 30]);
      value.releaseUpstream();
      await Promise.all(handles.map(handle => handle.final));
    } finally {
      await value.close();
    }
  }, 15_000);
});
