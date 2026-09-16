import { describe, expect, it } from "bun:test";
import { accountCapacitySupervisorShutdown, CapacityBackendEvidence, CapacityDiagnosticFailures, CapacityRequestProof, CapacitySupervisorProof, capacityAbsenceProvesDeletion, capacityEnvelopeMetadata, capacityProjectMayDelete, main, readCapacityExecutionDiagnostic, resolveCapacityProofOptions, resolveStartupArtifactDiagnosticOptions, runStartupArtifactDiagnostic, startupArtifactCapturePlan, waitCapacityStartup, type CapacityPrivateBackendRecord } from "../scripts/live_capacity_reliability";
import { SANDBOX_LOG_FILES } from "../src/v2/diagnostics/sandbox-log-schema";
import { inspectCapacityFixtures } from "../scripts/lib/capacity-workload";
import type { R2RequestStartFrame, SidecarFrame } from "../src/v2/protocol/shared";
import { GP_V2_R2_SCHEMA_DESCRIPTOR } from "../src/v2/protocol/shared";

const guarded = { GP_LIVE_CAPACITY_RELIABILITY: "1", GP_LIVE_ALLOW_TEST_PROJECT_MUTATION: "1", PORT: "18888", PRISM_COOKIE_FILE: "unused-test-cookie-file.json" };

describe("bounded content-free frame diagnostics", () => {
  it("recognizes every registered R2 frame kind without accepting obsolete or arbitrary names", () => {
    const kinds = [...new Set([...GP_V2_R2_SCHEMA_DESCRIPTOR.hostFrameKinds, ...GP_V2_R2_SCHEMA_DESCRIPTOR.sidecarFrameKinds])];
    const result = capacityEnvelopeMetadata({ frames: [...kinds.map((kind, sequence) => ({ kind, sequence })),
      { kind: "hello" }, { kind: "private-category" }] });
    expect(result.kinds).toEqual(Object.fromEntries([...kinds.map(kind => [kind, 1]), ["other", 2]]));
  });
  it("records exact error categories without provider text, identifiers, headers, or mutation", () => {
    const secret = "PRIVATE_DIAGNOSTIC_CANARY";
    const frame = Object.freeze({ kind: "response_error", sequence: 7, code: "prepared_expired", ambiguous: false,
      message: secret, requestId: secret, providerError: Object.freeze({ version: 1, origin: "http", status: 400,
        error: Object.freeze({ message: secret, code: null, type: "invalid_request_error", param: "input" }),
        payload: Object.freeze({ format: "json", text: secret, truncated: true }),
        headers: Object.freeze({ authorization: secret }), upstreamRequestId: secret, model: secret,
      }) });
    const envelope = Object.freeze({ epoch: 1, revision: 2, ack: Object.freeze({ epoch: 1, sequence: -1 }),
      frames: Object.freeze([frame, Object.freeze({ kind: "request_reject", sequence: 8, code: "prepared_failed", message: secret })]) });
    const before = JSON.stringify(envelope), result = capacityEnvelopeMetadata(envelope);
    expect(result.errors).toEqual([
      { sequence: 7, kind: "response_error", code: "prepared_expired", ambiguous: false,
        provider: { origin: "http", status: 400, code: null, type: "invalid_request_error", param: "input", payloadFormat: "json", payloadTruncated: true } },
      { sequence: 8, kind: "request_reject", code: "prepared_failed", ambiguous: null, provider: null },
    ]);
    expect(result).toMatchObject({ epoch: 1, revision: 2, ackEpoch: 1, ackSequence: -1, frameCount: 2,
      inspectedFrames: 2, inspectedBatchItems: 0, minSequence: 7, maxSequence: 8 });
    expect(JSON.stringify(result)).not.toContain(secret);
    expect(JSON.stringify(envelope)).toBe(before);
  });
  it("uses separate exact provider allowlists and collapses hostile or oversized categories to other", () => {
    const huge = "PRIVATE_UNKNOWN_CATEGORY".repeat(100_000);
    const result = capacityEnvelopeMetadata({ frames: [{ kind: "response_error", sequence: 1.5, code: huge, ambiguous: "true",
      providerError: { version: 1, origin: "https://private.invalid/secret", status: "400",
        error: { code: "prepared_expired", type: "invalid_request_error\n", param: huge },
        payload: { format: huge, text: huge, truncated: 1 } } },
      { kind: "response_error", code: "upstream", ambiguous: true, providerError: { version: 1, origin: "sse", status: 429,
        error: { code: "rate_limit_exceeded", type: "rate_limit_error", param: null }, payload: { format: "text", truncated: false } } },
      { kind: huge }, { kind: "response_error\n" }] });
    expect(result.errors[0]).toEqual({ sequence: null, kind: "response_error", code: "other", ambiguous: null,
      provider: { origin: "other", status: null, code: "other", type: "other", param: "other", payloadFormat: "other", payloadTruncated: null } });
    expect(result.errors[1]).toMatchObject({ code: "upstream", ambiguous: true,
      provider: { origin: "sse", status: 429, code: "rate_limit_exceeded", type: "rate_limit_error", param: null,
        payloadFormat: "text", payloadTruncated: false } });
    expect(result.kinds.other).toBe(2);
    expect(JSON.stringify(result)).not.toContain("PRIVATE_UNKNOWN_CATEGORY");
    expect(JSON.stringify(result)).not.toContain("private.invalid");
    expect(Buffer.byteLength(JSON.stringify(result))).toBeLessThan(2_000);
  });
  it("never invokes accessors, inherited fields, iterators, or string conversion", () => {
    let reads = 0;
    const accessor = { get() { reads++; throw new Error("unexpected diagnostic accessor"); } };
    const unread = (keys: readonly string[]) => Object.defineProperties({}, Object.fromEntries(keys.map(key => [key, accessor])));
    const frame = Object.assign(unread(["sequence", "code", "ambiguous", "message", "requestId", "dataBase64Url"]), {
      kind: "response_error", providerError: Object.assign(unread(["origin", "status", "headers", "upstreamRequestId", "model"]), {
        version: 1, error: unread(["code", "type", "param", "message"]), payload: unread(["format", "truncated", "text"]),
      }),
    });
    const frames = [frame, Object.create({ kind: "response_error", code: "prepared_expired" })];
    Object.defineProperty(frames, "2", accessor);
    Object.defineProperty(frames, "slice", accessor);
    Object.defineProperty(frames, Symbol.iterator, accessor);
    const envelope = Object.assign(unread(["epoch", "revision", "toJSON"]), { frames, ack: unread(["epoch", "sequence"]) });
    const result = capacityEnvelopeMetadata(envelope);
    expect(result.errors).toEqual([{ sequence: null, kind: "response_error", code: null, ambiguous: null,
      provider: { origin: null, status: null, code: null, type: null, param: null, payloadFormat: null, payloadTruncated: null } }]);
    expect(result.kinds).toEqual({ response_error: 1, other: 1 });
    expect(reads).toBe(0);
    const category = unread(["toString", "valueOf"]);
    expect(capacityEnvelopeMetadata({ frames: [{ kind: category }] }).kinds).toEqual({ other: 1 });
    expect(reads).toBe(0);
  });
  it("rejects proxies at every inspected layer without invoking traps, including revoked proxies", () => {
    let traps = 0;
    const trap = () => { traps++; throw new Error("unexpected diagnostic proxy trap"); };
    const proxy = new Proxy({}, { get: trap, getOwnPropertyDescriptor: trap, ownKeys: trap, getPrototypeOf: trap });
    const arrayProxy = new Proxy([], { get: trap, getOwnPropertyDescriptor: trap, ownKeys: trap });
    const revoked = Proxy.revocable({}, {}); revoked.revoke();
    for (const input of [proxy, revoked.proxy, { frames: arrayProxy }]) {
      expect(capacityEnvelopeMetadata(input)).toMatchObject({ frameCount: 0, inspectedFrames: 0, errors: [] });
    }
    const result = capacityEnvelopeMetadata({ ack: proxy, frames: [proxy, revoked.proxy,
      { kind: "response_error", providerError: proxy },
      { kind: "response_error", providerError: { version: 1, error: proxy, payload: revoked.proxy } },
      { kind: "response_batch", items: [proxy, revoked.proxy] },
      { kind: "response_batch", items: arrayProxy }] });
    expect(result.errors).toHaveLength(2);
    expect(result.errors[0]!.provider).toBeNull();
    expect(result.errors[1]!.provider).toMatchObject({ code: null, type: null, param: null, payloadFormat: null });
    expect(result.payloadChars).toBe(0);
    expect(traps).toBe(0);
  });
  it("caps frame evidence and every kind map at 64 entries even for huge sparse arrays", () => {
    const frames: unknown[] = [];
    frames.length = 1_000_000;
    for (let i = 0; i < 64; i++) frames[i] = { kind: "response_error", sequence: i, code: "prepared_expired", ambiguous: false,
      providerError: { version: 1, origin: "http", status: 400, error: { code: "missing_required_parameter",
        type: "invalid_request_error", param: "previous_response_id" }, payload: { format: "json", truncated: true } } };
    let reads = 0;
    Object.defineProperty(frames, "64", { get() { reads++; throw new Error("outside inspected frame bound"); } });
    const result = capacityEnvelopeMetadata({ frames });
    expect(result).toMatchObject({ frameCount: 1_000_000, inspectedFrames: 64, minSequence: 0, maxSequence: 63 });
    expect(result.errors).toHaveLength(64);
    expect(Object.keys(result.kinds).length).toBeLessThanOrEqual(64);
    expect(Buffer.byteLength(JSON.stringify(result))).toBeLessThan(64 * 1024);
    expect(reads).toBe(0);
  });
  it("shares one 64-item batch budget without starving the remaining inspected error frames", () => {
    const items: unknown[] = Array.from({ length: 64 }, () => ({ dataBase64Url: "AAAA" }));
    items.length = 1_000_000;
    let reads = 0;
    Object.defineProperty(items, "64", { get() { reads++; throw new Error("outside inspected item bound"); } });
    const result = capacityEnvelopeMetadata({ frames: [{ kind: "response_batch", items }, { kind: "response_batch", items },
      ...Array.from({ length: 62 }, (_, sequence) => ({ kind: "request_reject", sequence, code: "prepared_failed" }))] });
    expect(result).toMatchObject({ inspectedFrames: 64, inspectedBatchItems: 64, payloadChars: 256,
      kinds: { response_batch: 2, request_reject: 62 } });
    expect(result.errors).toHaveLength(62);
    expect(reads).toBe(0);
  });
  it("retains valid numeric boundaries and rejects malformed provider shapes and status values", () => {
    for (const status of [400, 599]) {
      const result = capacityEnvelopeMetadata({ frames: [{ kind: "response_error", sequence: Number.MAX_SAFE_INTEGER,
        providerError: { version: 1, origin: "http", status } }] });
      expect(result.errors[0]).toMatchObject({ sequence: Number.MAX_SAFE_INTEGER, provider: { status } });
    }
    for (const status of [399, 600, 400.1, "400", NaN, Infinity]) {
      expect(capacityEnvelopeMetadata({ frames: [{ kind: "response_error", providerError: { version: 1, status } }] })
        .errors[0]!.provider!.status).toBeNull();
    }
    for (const providerError of [[], { version: 2 }, "provider", null]) {
      expect(capacityEnvelopeMetadata({ frames: [{ kind: "response_error", providerError }] }).errors[0]!.provider).toBeNull();
    }
    expect(capacityEnvelopeMetadata(Object.assign([], { frames: [{ kind: "response_error" }] })).frameCount).toBe(0);
  });
});

describe("failed-project diagnostic ordering", () => {
  it("reads the last failed runtime's exact retained gateway before closed healthy controllers exhaust the budget", () => {
    const ids = Array.from({ length: 8 }, (_, index) => `project-${index + 1}`), failures = new CapacityDiagnosticFailures(ids);
    const records = ids.map((projectId, index) => ({ projectId, closed: index < 7, controllerIndex: index + 1, gateway: { identity: index + 1 } }));
    failures.observe({ event: "request.failed", project_id: ids[7] });
    const ordered = failures.prioritize(records);
    const firstReadWithinBudget = ordered.slice(0, 1);
    expect(firstReadWithinBudget[0]).toBe(records[7]);
    expect(firstReadWithinBudget[0]!.gateway).toBe(records[7]!.gateway);
    expect(records[0]!.controllerIndex).toBe(1);
  });
  it("retains only configured failures and ignores startup, busy-lane observations, and unrelated projects", () => {
    const failures = new CapacityDiagnosticFailures(["one", "two"]);
    for (const event of [
      { event: "sandbox.recovery.started", project_id: "one", cause: "startup" },
      { event: "sandbox.liveness", project_id: "one", liveness_outcome: "failed" },
      { event: "request.failed", project_id: "foreign" },
      { event: "request.completed", project_id: "two" },
    ]) failures.observe(event);
    expect(failures.size).toBe(0);
    failures.observe({ event: "sandbox.recovery.started", project_id: "one", cause: "runtime" });
    failures.observe({ event: "sandbox.recovery.retry_scheduled", project_id: "two" });
    failures.observe({ event: "request.failed", project_id: "one" });
    failures.observe({ event: "sandbox.recovery.completed", project_id: "one" });
    expect(failures.size).toBe(2);
    expect(failures.has("one")).toBe(true);
    expect(failures.has("foreign")).toBe(false);
  });
  it("bounds the index at the configured project count and rejects oversized or duplicate ownership", () => {
    const ids = Array.from({ length: 8 }, (_, index) => `project-${index}`), failures = new CapacityDiagnosticFailures(ids);
    for (let i = 0; i < 100; i++) failures.observe({ event: "request.failed", project_id: `project-${i}` });
    expect(failures.size).toBe(8);
    expect(() => new CapacityDiagnosticFailures([...ids, "extra"])).toThrow();
    expect(() => new CapacityDiagnosticFailures(["same", "same"])).toThrow();
  });
});

describe("capacity proof authorization and workload guards", () => {
  it("requires both live guards, explicit candidate port, and an explicit credential source", () => {
    for (const key of Object.keys(guarded)) expect(() => resolveCapacityProofOptions({ ...guarded, [key]: undefined })).toThrow();
    for (const port of ["18787", "0", "018888", "18888\n", "18889", "65536"]) expect(() => resolveCapacityProofOptions({ ...guarded, PORT: port })).toThrow();
    expect(resolveCapacityProofOptions(guarded)).toMatchObject({ projects: 1, cycles: 1, rootBytes: 1048576, stabilityMs: 120000 });
  });
  it("accepts only one or eight owned projects and at most two cycles", () => {
    expect(resolveCapacityProofOptions({ ...guarded, GP_LIVE_CAPACITY_PROJECTS: "8", GP_LIVE_CAPACITY_CYCLES: "2" })).toMatchObject({ projects: 8, cycles: 2 });
    for (const value of ["0", "2", "7", "9", "8\n", "01"]) expect(() => resolveCapacityProofOptions({ ...guarded, GP_LIVE_CAPACITY_PROJECTS: value })).toThrow();
    for (const value of ["0", "3", "2\n"]) expect(() => resolveCapacityProofOptions({ ...guarded, GP_LIVE_CAPACITY_CYCLES: value })).toThrow();
  });
  it("keeps Astra xhigh, exact root sizes, and a real 120-second minimum stability interval", () => {
    expect(resolveCapacityProofOptions({ ...guarded, GP_LIVE_CAPACITY_ROOT_BYTES: "262144", GP_LIVE_CAPACITY_STABILITY_SECONDS: "600" })).toMatchObject({ rootBytes: 262144, stabilityMs: 600000 });
    for (const value of ["0", "119", "601", "120\n", "0120", "1e3"]) expect(() => resolveCapacityProofOptions({ ...guarded, GP_LIVE_CAPACITY_STABILITY_SECONDS: value })).toThrow();
    for (const value of ["262145", "1048577", "1048576\n"]) expect(() => resolveCapacityProofOptions({ ...guarded, GP_LIVE_CAPACITY_ROOT_BYTES: value })).toThrow();
    for (const value of ["gpt-5.6-terra", "gpt-6-astra\n", ""]) expect(() => resolveCapacityProofOptions({ ...guarded, GP_LIVE_MODEL: value })).toThrow();
    expect(() => resolveCapacityProofOptions({ ...guarded, GP_LIVE_REASONING_EFFORT: "high" })).toThrow();
    for (const value of ["http://prism.openai.com", "https://user:secret@prism.openai.com", "https://prism.openai.com/path"]) expect(() => resolveCapacityProofOptions({ ...guarded, PRISM_ORIGIN: value })).toThrow();
  });
  it("does not treat an ambiguous create followed by absence as cleanup proof", () => {
    for (const status of [404, 410]) {
      expect(capacityAbsenceProvesDeletion(status, false)).toBe(false);
      expect(capacityAbsenceProvesDeletion(status, true)).toBe(true);
    }
    for (const status of [200, 401, 403, 500, undefined, "404"]) expect(capacityAbsenceProvesDeletion(status, true)).toBe(false);
  });
  it("requires explicit single-project authorization for fault hooks before any I/O", async () => {
    expect(resolveCapacityProofOptions({ ...guarded, GP_LIVE_CAPACITY_FAULTS: "1" }).faults).toBe(true);
    expect(() => resolveCapacityProofOptions({ ...guarded, GP_LIVE_CAPACITY_FAULTS: "1", GP_LIVE_CAPACITY_PROJECTS: "8" })).toThrow();
    expect(() => resolveCapacityProofOptions({ ...guarded, GP_LIVE_CAPACITY_FAULTS: "2" })).toThrow();
    await expect(main({ ...guarded, GP_LIVE_CAPACITY_FAULTS: "1" })).rejects.toThrow("fault_hooks_require_explicit_guard");
    await expect(main(guarded, { afterWorkload: async () => ({}) })).rejects.toThrow("fault_hooks_require_explicit_guard");
  });
  it("keeps optional startup captures on exact workers one and eight in the first cycle", () => {
    expect(resolveCapacityProofOptions(guarded).startupArtifacts).toBe(false);
    expect(() => resolveCapacityProofOptions({ ...guarded, GP_LIVE_CAPACITY_STARTUP_ARTIFACTS: "1" })).toThrow();
    for (const flag of ["2", "1\n", "true"]) expect(() => resolveCapacityProofOptions({ ...guarded, GP_LIVE_CAPACITY_PROJECTS: "8", GP_LIVE_CAPACITY_STARTUP_ARTIFACTS: flag })).toThrow();
    const options = resolveCapacityProofOptions({ ...guarded, GP_LIVE_CAPACITY_PROJECTS: "8", GP_LIVE_CAPACITY_CYCLES: "2", GP_LIVE_CAPACITY_STARTUP_ARTIFACTS: "1" });
    expect(options).toMatchObject({ projects: 8, cycles: 2, startupArtifacts: true, rootBytes: 1048576 });
    expect(startupArtifactCapturePlan("capacity_reliability", options.startupArtifacts)).toEqual({ cycle: 1, workers: [1, 8], expectedSessions: 2, expectedRows: 24 });
    expect(startupArtifactCapturePlan("capacity_reliability", false)).toEqual({ cycle: 1, workers: [], expectedSessions: 0, expectedRows: 0 });
    expect(startupArtifactCapturePlan("startup_artifact_diagnostic", false)).toEqual({ cycle: 1, workers: [1, 2], expectedSessions: 2, expectedRows: 24 });
  });
});

describe("capacity complete-fleet startup deadline", () => {
  it("retains the six-minute budget when the first worker becomes ready early", async () => {
    let now = 0, starts = 0;
    await waitCapacityStartup({ start: async () => { starts++; now = 20_000; }, allUsable: () => now >= 150_000,
      overallDeadlineMs: 45 * 60_000, signal: new AbortController().signal, nowMs: () => now, sleep: async ms => { now += ms; } });
    expect(starts).toBe(1); expect(now).toBe(150_000);
  });
  it("does not grant another budget after a slow first worker", async () => {
    let now = 0, starts = 0;
    await expect(waitCapacityStartup({ start: async () => { starts++; now = 350_000; }, allUsable: () => now >= 370_000,
      overallDeadlineMs: 45 * 60_000, signal: new AbortController().signal, nowMs: () => now, sleep: async ms => { now += ms; } })).rejects.toThrow("all_runtimes_not_usable");
    expect(starts).toBe(1); expect(now).toBe(360_000);
  });
  it("preserves the five-minute cleanup reserve inside the overall deadline", async () => {
    let now = 0;
    await expect(waitCapacityStartup({ start: async () => { now = 20_000; }, allUsable: () => false,
      overallDeadlineMs: 350_000, signal: new AbortController().signal, nowMs: () => now, sleep: async ms => { now += ms; } })).rejects.toThrow("all_runtimes_not_usable");
    expect(now).toBe(50_000);
    let started = false;
    await expect(waitCapacityStartup({ start: async () => { started = true; }, allUsable: () => false,
      overallDeadlineMs: 300_000, signal: new AbortController().signal, nowMs: () => 0 })).rejects.toThrow("startup_budget_exhausted");
    expect(started).toBe(false);
  });
});

describe("separate startup artifact diagnostic authorization", () => {
  const diagnostic = { GP_LIVE_STARTUP_ARTIFACT_DIAGNOSTIC: "1", GP_LIVE_ALLOW_TEST_PROJECT_MUTATION: "1", PORT: "18888",
    PRISM_COOKIE_FILE: "unused-test-cookie-file.json", GP_STARTUP_ARTIFACT_SOURCE_LABEL: "before-atomic-write" };
  it("requires its own guard and selects exactly two projects without enabling a provider workload", async () => {
    expect(resolveStartupArtifactDiagnosticOptions(diagnostic)).toMatchObject({ scope: "startup_artifact_diagnostic", projects: 2, cycles: 1, faults: false, sourceLabel: "before-atomic-write" });
    for (const key of Object.keys(diagnostic)) expect(() => resolveStartupArtifactDiagnosticOptions({ ...diagnostic, [key]: undefined })).toThrow();
    await expect(runStartupArtifactDiagnostic({ ...diagnostic, GP_LIVE_STARTUP_ARTIFACT_DIAGNOSTIC: "0" })).rejects.toThrow("explicit_startup_artifact_and_project_guards_required");
  });
  it("leaves normal reliability project/turn constraints intact", () => {
    expect(() => resolveCapacityProofOptions({ ...guarded, GP_LIVE_CAPACITY_PROJECTS: "2" })).toThrow("project_count_must_be_1_or_8");
    for (const projects of ["1", "8", "2\n"]) expect(() => resolveStartupArtifactDiagnosticOptions({ ...diagnostic, GP_LIVE_CAPACITY_PROJECTS: projects })).toThrow();
    expect(() => resolveStartupArtifactDiagnosticOptions({ ...diagnostic, GP_LIVE_CAPACITY_CYCLES: "2" })).toThrow();
    expect(() => resolveStartupArtifactDiagnosticOptions({ ...diagnostic, GP_LIVE_CAPACITY_FAULTS: "1" })).toThrow();
    expect(() => resolveStartupArtifactDiagnosticOptions({ ...diagnostic, GP_LIVE_CAPACITY_STARTUP_ARTIFACTS: "1" })).toThrow();
    expect(() => resolveStartupArtifactDiagnosticOptions({ ...diagnostic, GP_STARTUP_ARTIFACT_SOURCE_LABEL: "before\n" })).toThrow();
  });
});

describe("capacity proof signed supervisor lifecycle", () => {
  const projects = { project: "11111111-1111-4111-8111-111111111111",
    clean: "22222222-2222-4222-8222-222222222222", held: "33333333-3333-4333-8333-333333333333",
    other: "44444444-4444-4444-8444-444444444444" } as const;
  const binding = (label: keyof typeof projects, nonce: string) => ({ projectId: projects[label], sandboxSessionId: `offline-${nonce}`,
    leaderGeneration: 1, buildHash: "a".repeat(64), schemaHash: "b".repeat(64), nonce,
    protocolRevision: "r2" as const, schemaVersion: 2 as const });
  it("accounts only a fulfilled shutdown from the exact registered host", async () => {
    const exact = binding("project", "first");
    for (const outcome of ["success", "shutdown_failure", "identity_conflict", "persistence_failure"] as const) {
      const proof = new CapacitySupervisorProof();
      proof.register(exact); proof.submit(exact.projectId, exact.nonce);
      let persisted = 0;
      let shutdownCalls = 0;
      const host = accountCapacitySupervisorShutdown({
        binding: outcome === "identity_conflict" ? { ...exact, nonce: "other" } : exact,
        async shutdown(_reason: string, _signal?: AbortSignal) {
          shutdownCalls++;
          if (outcome === "shutdown_failure") throw new Error("offline shutdown failed");
        },
      }, proof, () => { if (outcome === "persistence_failure") throw new Error("offline persistence failed"); persisted++; });
      if (outcome === "success") await host.shutdown("offline");
      else await expect(host.shutdown("offline")).rejects.toThrow();
      expect(proof.snapshot()).toEqual({ submitted: 1, signedShutdowns: outcome === "success" ? 1 : 0,
        unprovedShutdowns: outcome === "success" ? 0 : 1 });
      expect(persisted).toBe(outcome === "success" ? 1 : 0);
      expect(shutdownCalls).toBe(outcome === "identity_conflict" ? 0 : 1);
    }
  });

  it("binds full supervisor identity, coalesces shutdown, and retries only failed persistence", async () => {
    const exact = binding("project", "first"), proof = new CapacitySupervisorProof();
    proof.register(exact); proof.submit(exact.projectId, exact.nonce);
    for (const changed of [
      { projectId: projects.other }, { sandboxSessionId: "other" }, { leaderGeneration: 2 },
      { buildHash: "c".repeat(64) }, { schemaHash: "d".repeat(64) }, { nonce: "other" },
      { protocolRevision: "r1" as const }, { schemaVersion: 1 as const },
    ]) {
      let calls = 0;
      const host = accountCapacitySupervisorShutdown({ binding: { ...exact, ...changed } as any,
        async shutdown(_reason: string, _signal?: AbortSignal) { calls++; } }, proof, () => {});
      await expect(host.shutdown("offline")).rejects.toThrow("identity");
      expect(calls).toBe(0);
    }
    const gate = Promise.withResolvers<void>();
    let shutdowns = 0, persists = 0;
    const host = accountCapacitySupervisorShutdown({ binding: exact,
      async shutdown(_reason: string, _signal?: AbortSignal) { shutdowns++; await gate.promise; } }, proof, () => {
      if (++persists === 1) throw new Error("offline first persistence failed");
    });
    const first = host.shutdown("offline").catch(error => error), joined = host.shutdown("offline").catch(error => error);
    gate.resolve();
    await expect(first).resolves.toThrow("persistence");
    await expect(joined).resolves.toThrow("persistence");
    expect(shutdowns).toBe(1);
    await host.shutdown("offline");
    expect({ shutdowns, persists, snapshot: proof.snapshot() }).toEqual({ shutdowns: 1, persists: 2,
      snapshot: { submitted: 1, signedShutdowns: 1, unprovedShutdowns: 0 } });
    await host.shutdown("offline");
    expect(shutdowns).toBe(1);

    let reproofs = 0;
    const replacement = accountCapacitySupervisorShutdown({ binding: exact,
      async shutdown(_reason: string, _signal?: AbortSignal) { reproofs++; } }, proof,
    () => { throw new Error("signed reproof must not persist twice"); });
    await replacement.shutdown("journal-save-recovery");
    expect(reproofs).toBe(1);
  });

  it("forbids a repeated launch and requires the exact previous nonce cleanup before restart", () => {
    const proof = new CapacitySupervisorProof(), first = binding("project", "first"), second = binding("project", "second");
    proof.register(first); proof.submit(first.projectId, first.nonce); proof.register(second);
    expect(() => proof.submit(first.projectId, first.nonce)).toThrow("bootstrap_submission_replay_forbidden");
    expect(() => proof.submit(second.projectId, second.nonce)).toThrow("bootstrap_submission_replay_forbidden");
    expect(() => proof.signedShutdown("wrong", "first")).toThrow();
    expect(() => proof.signedShutdown(second.projectId, second.nonce)).toThrow();
    proof.signedShutdown(first.projectId, first.nonce); proof.submit(second.projectId, second.nonce);
    expect(proof.snapshot()).toEqual({ submitted: 2, signedShutdowns: 1, unprovedShutdowns: 1 });
    proof.signedShutdown(second.projectId, second.nonce); expect(proof.snapshot().unprovedShutdowns).toBe(0);
  });
  it("bounds identities and rejects duplicate nonces", () => {
    const proof = new CapacitySupervisorProof();
    for (let i = 0; i < 128; i++) proof.register(binding("project", String(i)));
    expect(() => proof.register(binding("other", "0"))).toThrow();
    expect(() => proof.register(binding("other", "129"))).toThrow();
  });
  it("allows one proven project while a sibling retains unresolved shutdown debt", () => {
    const proof = new CapacitySupervisorProof(), cleanBinding = binding("clean", "clean-nonce"), heldBinding = binding("held", "held-nonce");
    proof.register(cleanBinding); proof.submit(cleanBinding.projectId, cleanBinding.nonce); proof.signedShutdown(cleanBinding.projectId, cleanBinding.nonce);
    proof.register(heldBinding); proof.submit(heldBinding.projectId, heldBinding.nonce);
    const clean = { cleanupStarted: true, controllersClosed: true, unprovedShutdowns: proof.projectSnapshot(cleanBinding.projectId).unprovedShutdowns, journalEmpty: true, evidencePersisted: true };
    expect(proof.snapshot().unprovedShutdowns).toBe(1);
    expect(capacityProjectMayDelete(clean)).toBe(true);
    expect(capacityProjectMayDelete({ ...clean, unprovedShutdowns: proof.projectSnapshot(heldBinding.projectId).unprovedShutdowns })).toBe(false);
    for (const key of ["cleanupStarted", "controllersClosed", "journalEmpty", "evidencePersisted"]) expect(capacityProjectMayDelete({ ...clean, [key]: false })).toBe(false);
    expect(proof.recordsForProject(cleanBinding.projectId)).toEqual([{ nonce: "clean-nonce", submitted: true, signedShutdown: true }]);
    expect(capacityProjectMayDelete({ ...clean, unprovedShutdowns: 0, controllersClosed: false })).toBe(false);
  });
});

describe("capacity private backend evidence", () => {
  const token = "private-fixture-sandbox-token", execution = "12345678-1234-4234-8234-123456789abc";
  const headers = (value = token) => new Headers({ "x-crixet-sandbox-token": value });
  it("binds interleaved project/session/token evidence without a timing inference", async () => {
    const saved: CapacityPrivateBackendRecord[] = [], evidence = new CapacityBackendEvidence(new Set(["one", "two"]), async value => { saved.push(value); });
    await evidence.acquired({ url: "https://sandbox.example/one/", token, sandbox_session_id: "sandbox-one", ignored_secret: "must-not-persist" });
    await evidence.acquired({ url: "https://sandbox.example/two/", token: "second-token", sandbox_session_id: "sandbox-two" });
    evidence.resourceRequest("two", { sandbox_session_id: "sandbox-two" });
    evidence.resourceRequest("one", { sandbox_session_id: "sandbox-one" });
    await evidence.delivered("https://sandbox.example/two/resources-token", headers("second-token"), { projectId: "two" });
    await evidence.delivered("https://sandbox.example/one/resources-token", headers(), { projectId: "one" });
    evidence.registerCommand("one", "nonce-one", execution);
    await evidence.command("https://sandbox.example/one/codex/codex_exec_async?prism_cache_bust=test", headers(), { session_id: execution });
    expect(saved.at(-1)).toMatchObject({ projectId: "one", sandboxSessionId: "sandbox-one", executions: [{ sessionId: execution, nonce: "nonce-one" }] });
    expect(JSON.stringify(saved)).not.toContain("must-not-persist");
    expect(JSON.stringify(evidence.snapshot())).not.toContain(token);
    expect(evidence.snapshot()).toEqual({ acquired: 2, associated: 2, commandSessions: 1, resourceBindings: 2 });
    await expect(evidence.command("https://sandbox.example/one/codex/codex_exec_async", headers(), { session_id: execution })).rejects.toThrow("command_evidence_replay");
  });
  it("refuses mismatched token, session, project, backend, and unregistered commands", async () => {
    const evidence = new CapacityBackendEvidence(new Set(["one", "two"]), async () => {});
    await evidence.acquired({ url: "https://sandbox.example/one/", token, sandbox_session_id: "sandbox-one" });
    evidence.resourceRequest("one", { sandbox_session_id: "wrong-session" });
    await expect(evidence.delivered("https://sandbox.example/one/resources-token", headers(), { projectId: "one" })).rejects.toThrow();
    evidence.resourceRequest("one", { sandbox_session_id: "sandbox-one" });
    await expect(evidence.delivered("https://sandbox.example/one/resources-token", headers("wrong-token"), { projectId: "one" })).rejects.toThrow();
    await expect(evidence.delivered("https://sandbox.example/other/resources-token", headers(), { projectId: "one" })).rejects.toThrow();
    await expect(evidence.delivered("https://sandbox.example/one/resources-token", headers(), { projectId: "unowned" })).rejects.toThrow();
    await evidence.delivered("https://sandbox.example/one/resources-token", headers(), { projectId: "one" });
    evidence.resourceRequest("two", { sandbox_session_id: "sandbox-one" });
    await expect(evidence.delivered("https://sandbox.example/one/resources-token", headers(), { projectId: "two" })).rejects.toThrow();
    await expect(evidence.command("https://sandbox.example/one/codex/codex_exec_async", headers(), { session_id: execution })).rejects.toThrow();
  });
  it("does not release a command until its private evidence is durably persisted", async () => {
    let writes = 0, release!: () => void;
    const pending = new Promise<void>(resolve => { release = resolve; });
    const evidence = new CapacityBackendEvidence(new Set(["one"]), async () => { if (++writes === 3) await pending; });
    await evidence.acquired({ url: "https://sandbox.example/one/", token, sandbox_session_id: "sandbox-one" });
    evidence.resourceRequest("one", { sandbox_session_id: "sandbox-one" });
    await evidence.delivered("https://sandbox.example/one/resources-token", headers(), { projectId: "one" });
    evidence.registerCommand("one", "nonce", execution);
    let finished = false;
    const dispatch = evidence.command("https://sandbox.example/one/codex/codex_exec_async", headers(), { session_id: execution }).then(() => { finished = true; });
    await Promise.resolve(); await Promise.resolve(); expect(finished).toBe(false);
    release(); await dispatch; expect(finished).toBe(true);
  });
  it("bounds private records and rejects credential-bearing URL aliases", async () => {
    const evidence = new CapacityBackendEvidence(new Set(["one"]), async value => { expect(Buffer.byteLength(JSON.stringify(value))).toBeLessThanOrEqual(16384); });
    for (const url of ["http://sandbox.example/", "https://user:password@sandbox.example/", "https://sandbox.example/?token=secret"]) await expect(evidence.acquired({ url, token })).rejects.toThrow();
    await expect(evidence.acquired({ url: "https://sandbox.example/", token: "a".repeat(8193) })).rejects.toThrow();
    for (let i = 0; i < 128; i++) await evidence.acquired({ url: `https://sandbox.example/${i}/`, token, sandbox_session_id: String(i) });
    await expect(evidence.acquired({ url: "https://sandbox.example/overflow/", token })).rejects.toThrow();
  });
  it("reads retained gateway logs after controller closure and validates exact file identity", async () => {
    const file = SANDBOX_LOG_FILES[0]!, expected = { sessionId: execution, binding: { projectId: "one" } };
    let calls = 0;
    const raw = { session_id: execution, path: `/codex_workspace/${execution}/.ghostprov-v2/diagnostics/${file}`, bytes: 0, body_b64: "" };
    const gateway = { readDiagnosticFile: async (sessionId: string, selected: string) => { calls++; expect(sessionId).toBe(execution); expect(selected).toBe(file); return { status: 200, value: raw }; } };
    expect(await readCapacityExecutionDiagnostic(gateway, "one", expected, file, AbortSignal.timeout(1000))).toBe("");
    await expect(readCapacityExecutionDiagnostic(gateway, "other", expected, file, AbortSignal.timeout(1000))).rejects.toThrow();
    expect(calls).toBe(1);
    await expect(readCapacityExecutionDiagnostic({ readDiagnosticFile: async () => ({ status: 200, value: { ...raw, session_id: "other" } }) }, "one", expected, file, AbortSignal.timeout(1000))).rejects.toThrow();
  });
});

const digest = "a".repeat(64);
const start: R2RequestStartFrame = { kind: "request_start", requestId: "request-1", requestDigest: digest,
  protocolRevision: "r2", schemaVersion: 2, projectId: "project", sessionId: "session", leaderGeneration: 1,
  buildHash: "build", schemaHash: "schema", launchNonce: "launch", hostInstanceId: "host", targetInstanceId: "sidecar",
  decodedBytes: 50, encodedBytes: 50, encoding: "identity", partCount: 1 };
const terminal: Extract<SidecarFrame, { kind: "response_terminal" }> = { kind: "response_terminal", requestId: "request-1", requestDigest: digest,
  responseId: "resp-1", terminalStatus: "completed", eventCount: 1, byteCount: 100, transcriptSha256: "transcript", finalEventIndex: 0, finalSha256: "final" };

describe("capacity wire execution identity", () => {
  it("accepts reliable retransmission but only one execution identity and exact public response", () => {
    const proof = new CapacityRequestProof(); proof.begin(digest, 0, 0);
    proof.start("project", start); proof.start("project", { ...start });
    expect(() => proof.complete(digest, terminal.responseId)).toThrow("public_terminal_lacks_exact_wire_proof");
    proof.terminal("project", terminal); proof.terminal("project", { ...terminal });
    expect(() => proof.complete(digest, "other")).toThrow();
    proof.complete(digest, terminal.responseId);
    expect(proof.snapshot()).toMatchObject({ invoked: 1, executionIdentities: 1, providerTerminals: 1, accepted: 1 });
    expect(proof.snapshot().requests[0]?.startPublications).toBe(2);
    expect(JSON.stringify(proof.snapshot())).not.toContain(terminal.responseId);
    expect(JSON.stringify(proof.snapshot())).not.toContain(start.requestId);
  });
  it("rejects changed request, project, digest, launch fence, and duplicated provider identities", () => {
    const proof = new CapacityRequestProof(); proof.begin(digest, 0, 0); proof.start("project", start);
    for (const changed of [{ ...start, requestId: "second" }, { ...start, launchNonce: "other" }, { ...start, requestDigest: "b".repeat(64) }]) expect(() => proof.start("project", changed)).toThrow();
    expect(() => proof.start("other", start)).toThrow();
    expect(() => proof.terminal("other", terminal)).toThrow();
    expect(() => proof.terminal("project", { ...terminal, requestDigest: "other" })).toThrow();
    expect(() => proof.terminal("project", { ...terminal, terminalStatus: "incomplete" })).toThrow();
    proof.terminal("project", terminal);
    const second = "b".repeat(64); proof.begin(second, 1, 0); proof.start("project", { ...start, requestDigest: second, requestId: "second" });
    expect(() => proof.terminal("project", { ...terminal, requestDigest: second, requestId: "second" })).toThrow("duplicate_provider_terminal");
  });
  it("bounds expected operations and refuses invalid digests or lane coordinates", () => {
    const proof = new CapacityRequestProof();
    expect(() => proof.begin(digest + "\n", 0, 0)).toThrow();
    expect(() => proof.begin(digest, 10, 0)).toThrow();
    for (let index = 0; index < 100; index++) proof.begin(index.toString(16).padStart(64, "0"), Math.floor(index / 10), index % 10);
    expect(() => proof.begin("f".repeat(64), 0, 0)).toThrow();
  });
  it("retains provider identity uniqueness across system restart cycles", () => {
    const shared = new Set<string>(), first = new CapacityRequestProof(shared), second = new CapacityRequestProof(shared);
    first.begin(digest, 0, 0); first.start("project", start); first.terminal("project", terminal);
    second.begin(digest, 0, 0); second.start("project", { ...start, requestId: "second-cycle" });
    expect(() => second.terminal("project", { ...terminal, requestId: "second-cycle" })).toThrow("duplicate_provider_terminal");
  });
});

describe("capacity synthetic high-context workload", () => {
  it("constructs ten exact roots, fifty 8-KiB tool results, and bounded lineage-only followups", () => {
    for (const rootBytes of [262144, 1048576] as const) {
      const fixtures = inspectCapacityFixtures(rootBytes);
      expect(fixtures).toHaveLength(100);
      const roots = fixtures.filter(value => value.turn === 0), tools = fixtures.filter(value => value.kind === "tool_result");
      expect(roots).toHaveLength(10); expect(tools).toHaveLength(50);
      expect(roots.every(value => value.requestBytes === rootBytes && value.fixtureGzipBytes >= 16384)).toBe(true);
      expect(tools.every(value => value.toolResultBytes === 8192 && value.requestBytes <= 12288)).toBe(true);
      expect(fixtures.every(value => value.parentMatched)).toBe(true);
      expect(new Set(fixtures.map(value => value.canonicalRequestSha256)).size).toBe(100);
      expect(JSON.stringify(fixtures)).not.toContain("Synthetic report");
    }
  });
});
