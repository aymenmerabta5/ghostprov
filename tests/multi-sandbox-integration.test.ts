import { describe, expect, it } from "bun:test";
import { SandboxRegistry } from "../src/sandbox/registry";
import { ResourceGovernor } from "../src/router/resource-governor";
import { MultiSandboxRouter } from "../src/router/router";
import { FakePrismRuntime } from "./support/fake-prism";
import { percentile, validateProofResponse } from "../scripts/live_multi_sandbox";

function system() {
  const runtimes = [1, 2, 3, 4].map(index => new FakePrismRuntime(index));
  const registry = new SandboxRegistry(runtimes);
  const governor = new ResourceGovernor({
    probe: {
      sample: () => ({
        totalBytes: 32 * 1024 ** 3,
        freeBytes: 20 * 1024 ** 3,
        rssBytes: 320 * 1024 ** 2,
        heapBytes: 100 * 1024 ** 2,
        externalBytes: 20 * 1024 ** 2,
        logicalProcessors: 12,
      }),
    },
  });
  const router = new MultiSandboxRouter({
    registry,
    governor,
    random: { next: () => 0, uuid: () => "integration" },
  });
  return { runtimes, router };
}

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
    const value = system();
    const started = Array.from({ length: 120 }, (_, index) => value.router.startRelay(
      { model: "gpt-5.6-sol", input: `integration-${index}`, stream: true },
      { clientId: `client-${index % 8}`, source: "http" },
    ));
    const handles = await Promise.all(started);

    expect(value.runtimes.map(runtime => runtime.snapshot().active)).toEqual([30, 30, 30, 30]);
    expect(value.runtimes.every(runtime => runtime.transport.maxActive <= 30)).toBe(true);
    expect(value.router.snapshot()).toMatchObject({ active: 120, capacity: 120, queued: 0 });

    value.runtimes.forEach(runtime => runtime.completeAll());
    const finals = await Promise.all(handles.map(handle => handle.final));

    expect(finals).toHaveLength(120);
    expect(new Set(finals.map(final => final.id)).size).toBe(120);
    expect(value.runtimes.flatMap(runtime => runtime.transport.duplicateIds)).toEqual([]);
    expect(value.runtimes.reduce((sum, runtime) => sum + runtime.snapshot().quarantined, 0)).toBe(0);
  }, 15_000);

  it("isolates a draining project while keeping the other 90 lanes routable", async () => {
    const value = system();
    value.runtimes[0].state = "draining";
    const handles = await Promise.all(Array.from({ length: 90 }, (_, index) => value.router.startRelay(
      { model: "gpt-5.6-sol", input: `healthy-${index}`, stream: true },
      { clientId: "healthy", source: "http" },
    )));
    expect(value.runtimes[0].snapshot().active).toBe(0);
    expect(value.runtimes.slice(1).map(runtime => runtime.snapshot().active)).toEqual([30, 30, 30]);
    value.runtimes.forEach(runtime => runtime.completeAll());
    await Promise.all(handles.map(handle => handle.final));
  }, 15_000);
});
