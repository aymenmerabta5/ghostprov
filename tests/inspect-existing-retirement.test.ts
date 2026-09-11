import { describe, expect, it } from "bun:test";
import { readFile } from "node:fs/promises";
import { existingRetirementOptions, latestFleetSampleFromCompleteTail, requireOwnedInspectionLauncher,
  validateExistingFleetSample, describeExistingInspectionError } from "../scripts/inspect_existing_retirement";

const first = "11111111-1111-4111-8111-111111111111";
const second = "22222222-2222-4222-8222-222222222222";
const now = 1_800_000_000_000;
const runtime = (projectId = first) => ({ projectId, state: "failed", capacity: 0, available: 0, active: 0,
  queueDepth: 0, quarantined: 0, ambiguousQuarantined: 0 });
const sample = () => ({ version: 1, capturedAtMs: now, sampleTs: new Date(now - 1000).toISOString(), desired: 2,
  runtimes: [runtime(first), runtime(second)] });
const event = (overrides: Record<string, unknown> = {}) => ({ event: "fleet.sample", ts: new Date(now - 1000).toISOString(), desired: 2,
  runtimes: [first, second].map(project_id => ({ project_id, state: "failed", capacity: 0, available: 0, active: 0,
    queue_depth: 0, quarantined: 0, ambiguous_quarantined: 0 })), ...overrides });

describe("existing retirement inspection authorization", () => {
  const guarded = { GP_LIVE_EXISTING_RETIREMENT_INSPECTION: "1", PORT: "18888", PRISM_COOKIE_FILE: "test-only-cookie.json",
    GP_LIVE_EXISTING_RETIREMENT_MANIFEST_FILE: "test-only-fleet.json", GP_LIVE_EXISTING_RETIREMENT_FLEET_FILE: "test-only-fleet.jsonl",
    GP_LIVE_EXISTING_RETIREMENT_PROJECT_IDS: first };
  it("requires explicit guard, exact candidate port and every input path", () => {
    expect(existingRetirementOptions(guarded).projects).toEqual([first]);
    for (const key of Object.keys(guarded)) expect(() => existingRetirementOptions({ ...guarded, [key]: undefined })).toThrow();
    for (const port of ["18787", "0", "18888\n", "18889"]) expect(() => existingRetirementOptions({ ...guarded, PORT: port })).toThrow();
  });
  it("bounds selected IDs and rejects duplicates or invalid scope", () => {
    expect(existingRetirementOptions({ ...guarded, GP_LIVE_EXISTING_RETIREMENT_PROJECT_IDS: `${first},${second}` }).projects).toHaveLength(2);
    for (const ids of ["", "arbitrary", `${first},${first}`, `${first},${second},${first}`, `${first},`]) {
      expect(() => existingRetirementOptions({ ...guarded, GP_LIVE_EXISTING_RETIREMENT_PROJECT_IDS: ids })).toThrow();
    }
    expect(() => existingRetirementOptions({ ...guarded, GP_LIVE_EXISTING_RETIREMENT_FLEET_FILE: guarded.PRISM_COOKIE_FILE })).toThrow();
  });
});

describe("fresh content-free fleet inspection proof", () => {
  it("accepts fresh zero-work targets alongside unrelated ready workers", () => {
    const value = sample();
    value.runtimes[1] = { ...runtime(second), state: "ready", capacity: 8, available: 7, active: 1 };
    expect(validateExistingFleetSample(value, [first], now).desired).toBe(2);
    expect(() => validateExistingFleetSample(value, [second], now)).toThrow("target_has_work_or_is_routable");
  });
  it("rejects stale or future capture and actual sample timestamps", () => {
    for (const changed of [{ capturedAtMs: now - 15001 }, { sampleTs: new Date(now - 15001).toISOString() },
      { capturedAtMs: now + 1001 }, { sampleTs: new Date(now + 1001).toISOString() }, { sampleTs: "invalid" }]) {
      expect(() => validateExistingFleetSample({ ...sample(), ...changed }, [first], now)).toThrow();
    }
  });
  it("fails closed for every work and ambiguity field without defaulting missing values", () => {
    for (const field of ["available", "active", "queueDepth", "quarantined", "ambiguousQuarantined"]) {
      for (const value of [1, undefined]) {
        const changed = { ...runtime(), [field]: value };
        expect(() => validateExistingFleetSample({ ...sample(), runtimes: [changed] }, [first], now)).toThrow();
      }
    }
    for (const state of ["ready", "draining"]) expect(() => validateExistingFleetSample({ ...sample(), runtimes: [{ ...runtime(), state }] }, [first], now)).toThrow();
  });
  it("rejects duplicates, missing selected projects and oversized samples", () => {
    expect(() => validateExistingFleetSample({ ...sample(), runtimes: [runtime(), runtime()] }, [first], now)).toThrow();
    expect(() => validateExistingFleetSample({ ...sample(), runtimes: [] }, [first], now)).toThrow();
    expect(() => validateExistingFleetSample({ ...sample(), runtimes: Array.from({ length: 129 }, () => runtime()) }, [first], now)).toThrow();
  });
  it("uses the latest complete JSONL sample and ignores only an incomplete final line", () => {
    const text = `${JSON.stringify(event())}\n${JSON.stringify(event({ ts: new Date(now).toISOString() }))}\n{"event":"fleet.sample"`;
    expect(latestFleetSampleFromCompleteTail(text, [first], now).sampleTs).toBe(new Date(now).toISOString());
  });
  it("never falls back past a newer unsafe, stale or malformed fleet sample", () => {
    const safe = JSON.stringify(event()) + "\n";
    const active = event(); active.runtimes[0]!.active = 1;
    for (const newer of [JSON.stringify(active), JSON.stringify(event({ ts: new Date(now - 16000).toISOString() })), '{"event":"fleet.sample","runtimes":']) {
      expect(() => latestFleetSampleFromCompleteTail(safe + newer + "\n", [first], now)).toThrow();
    }
    expect(() => latestFleetSampleFromCompleteTail("x".repeat(1024 * 1024 + 1), [first], now)).toThrow();
  });
});

describe("existing project launcher ownership", () => {
  it("permits only absence or byte-exact registered old launcher", () => {
    const exact = "#!/bin/sh\nexec sh exact-registered-installer\n";
    expect(() => requireOwnedInspectionLauncher(null, exact)).not.toThrow();
    expect(() => requireOwnedInspectionLauncher(exact, exact)).not.toThrow();
    for (const observed of ["", exact + "\n", "prefix" + exact, exact.replace("registered", "foreign")]) {
      expect(() => requireOwnedInspectionLauncher(observed, exact)).toThrow("foreign_exec_launcher");
    }
  });
  it("contains no supervisor lifecycle, project mutation or journal save calls", async () => {
    const source = await readFile(new URL("../scripts/inspect_existing_retirement.ts", import.meta.url), "utf8");
    expect(source).not.toMatch(/\b(?:createBrowserlessSystem|V2SandboxSupervisor|deleteManaged|pidfd_send_signal)\b|entry\.(?:save|clear)\(|\.shutdown\(/);
    expect(source).not.toContain("127.0.0.1:18787");
    expect(source).toContain("diagnostic_execution_unproven_restore_held");
  });
  it("reports known no-send exception categories without exception messages or arbitrary fields", () => {
    expect(describeExistingInspectionError({ name: "YDocSessionError", kind: "sync", message: "private secret", url: "private secret" }))
      .toEqual({ name: "YDocSessionError", kind: "sync", code: null, status: null });
    expect(describeExistingInspectionError({ name: "FileLeaseBusyError", file: "private secret" }).name).toBe("FileLeaseBusyError");
    const unknown = describeExistingInspectionError({ name: "private secret", kind: "private secret", code: "private secret", status: "private secret" });
    expect(unknown).toEqual({ name: null, kind: null, code: null, status: null });
    expect(JSON.stringify(unknown)).not.toContain("private secret");
  });
});
