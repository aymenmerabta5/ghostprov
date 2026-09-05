import { describe, expect, it } from "bun:test";
import { deleteOwnedToolTraceProject, resolveToolLatencyOptions, traceUsageCounts } from "../scripts/live_tool_latency";
import { PrismProjectError, type PrismProjectSummary } from "../src/prism/projects-client";
import { projectId } from "../src/sandbox/types";

const enabled = { GP_LIVE_TOOL_LATENCY_TRACE: "1", GP_LIVE_ALLOW_TEST_PROJECT_MUTATION: "1",
  PRISM_COOKIE_FILE: "offline-unused-cookies.json" };
const ownedProject: PrismProjectSummary = { projectId: projectId("11111111-1111-4111-8111-111111111111"),
  title: "Dedicated offline trace", owner: null, deleted: false, createdAt: null };

describe("live tool latency trace guards", () => {
  it("cleans the exact attempted project and requires authoritative deletion confirmation", async () => {
    for (const status of ["deleted", 404, 410] as const) {
      let deletes = 0;
      await deleteOwnedToolTraceProject({
        async get(id) {
          expect(id).toBe(ownedProject.projectId);
          if (deletes === 0) return ownedProject;
          if (status === "deleted") return { ...ownedProject, deleted: true };
          throw new PrismProjectError("Missing", "http", status);
        },
        async deleteManaged(id) { expect(id).toBe(ownedProject.projectId); deletes++; },
      }, ownedProject.projectId, ownedProject.title);
      expect(deletes).toBe(1);
    }
    let deletes = 0;
    await expect(deleteOwnedToolTraceProject({ async get() { return ownedProject; },
      async deleteManaged() { deletes++; } }, ownedProject.projectId, ownedProject.title)).rejects.toThrow("not confirmed");
    expect(deletes).toBe(1);
  });

  it("never deletes a foreign project or treats an unknown lookup failure as successful cleanup", async () => {
    for (const current of [{ ...ownedProject, title: "Foreign project" },
      { ...ownedProject, projectId: projectId("22222222-2222-4222-8222-222222222222") }]) {
      let deletes = 0;
      await expect(deleteOwnedToolTraceProject({ async get() { return current; }, async deleteManaged() { deletes++; } },
        ownedProject.projectId, ownedProject.title)).rejects.toThrow("identity mismatch");
      expect(deletes).toBe(0);
    }
    for (const status of [404, 410, 403, 503]) {
      let deletes = 0;
      const operation = deleteOwnedToolTraceProject({ async get() { throw new PrismProjectError("Lookup failed", "http", status); },
        async deleteManaged() { deletes++; } }, ownedProject.projectId, ownedProject.title);
      if (status === 404 || status === 410) await operation;
      else await expect(operation).rejects.toMatchObject({ status });
      expect(deletes).toBe(0);
    }
  });

  it("retains only allowlisted numeric usage counters", () => {
    expect(traceUsageCounts({ input_tokens: 10, output_tokens: 4, secret: "do-not-retain",
      input_tokens_details: { cached_tokens: 8, cache_write_tokens: 2, secret: "do-not-retain" },
      output_tokens_details: { reasoning_tokens: 1, thinking_tokens: 1, raw: "do-not-retain" } }))
      .toEqual({ input_tokens: 10, output_tokens: 4, cached_tokens: 8, cache_write_tokens: 2, reasoning_tokens: 1, thinking_tokens: 1 });
    expect(traceUsageCounts({ input_tokens: "secret", output_tokens: -1, total_tokens: Infinity })).toEqual({});
  });

  it("requires both live guards before any credential or project operation", () => {
    expect(() => resolveToolLatencyOptions({})).toThrow();
    expect(() => resolveToolLatencyOptions({ ...enabled, GP_LIVE_TOOL_LATENCY_TRACE: "0" })).toThrow();
    expect(() => resolveToolLatencyOptions({ ...enabled, GP_LIVE_ALLOW_TEST_PROJECT_MUTATION: "0" })).toThrow();
    expect(() => resolveToolLatencyOptions({ ...enabled, PRISM_COOKIE_FILE: "" })).toThrow();
  });

  it("protects production and accepts only explicit bounded feature selections", () => {
    for (const port of ["18787", "0", "18889"]) expect(() => resolveToolLatencyOptions({ ...enabled, PORT: port })).toThrow();
    expect(resolveToolLatencyOptions(enabled)).toMatchObject({ batching: true, receiptCoalescing: true });
    expect(resolveToolLatencyOptions({ ...enabled, GP_TRACE_BATCHING: "0", GP_V2_TERMINAL_RECEIPT_COALESCING: "0" }))
      .toMatchObject({ batching: false, receiptCoalescing: false });
    expect(() => resolveToolLatencyOptions({ ...enabled, GP_TRACE_BATCHING: "false" })).toThrow();
    expect(() => resolveToolLatencyOptions({ ...enabled, GP_V2_TERMINAL_RECEIPT_COALESCING: "2" })).toThrow();
  });
});
