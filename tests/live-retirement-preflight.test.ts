import { describe, expect, it } from "bun:test";
import { readFile } from "node:fs/promises";
import { retirementPreflightOptions } from "../scripts/live_retirement_preflight";
import { decodeRetirementPreflightFile, parseRetirementPreflightResult, preflightResultPath, summarizeRetirementPreflightReadback } from "../scripts/lib/retirement-preflight-proof";

const sessionId = "11111111-1111-4111-8111-111111111111";
const probeNonce = "22222222-2222-4222-8222-222222222222";
const expected = { sessionId, probeNonce, authoritySha256: "a".repeat(64), scriptSha256: "b".repeat(64) };
const result = () => ({ version: 1, ...expected, outcome: "retired", code: "retired",
  checks: { workspace: true, static: true, artifacts: true, retirement: true, owners: true, processes: true, unchanged: true },
  processesInspected: 100, retirementSha256: "c".repeat(64), processScansCompleted: 2,
  processConflictCount: 0, processConflictsTruncated: false, processConflicts: [], fileFailure: null } as const);
function envelope(value: unknown = result()) {
  const bytes = Buffer.from(JSON.stringify(value)), relative = preflightResultPath(probeNonce);
  return { path: `/codex_workspace/${sessionId}/${relative}`, relative_path: `${sessionId}/${relative}`, bytes: bytes.length,
    mime_type: "application/json", body_b64: bytes.toString("base64"), session_id: sessionId };
}

describe("retirement preflight authorization", () => {
  const guarded = { GP_LIVE_RETIREMENT_PREFLIGHT: "1", GP_LIVE_ALLOW_TEST_PROJECT_MUTATION: "1", PORT: "18888", PRISM_COOKIE_FILE: "test-only.json" };
  it("requires both live guards, candidate port and explicit cookie source", () => {
    expect(() => retirementPreflightOptions(guarded)).not.toThrow();
    for (const key of Object.keys(guarded)) expect(() => retirementPreflightOptions({ ...guarded, [key]: undefined })).toThrow();
    expect(() => retirementPreflightOptions({ ...guarded, PORT: "18787" })).toThrow();
  });
  it("defaults to the current release and requires an exact release selector", () => {
    expect(retirementPreflightOptions(guarded).releaseKind).toBe("current");
    expect(retirementPreflightOptions({ ...guarded, GP_LIVE_RETIREMENT_PREFLIGHT_RELEASE: "initial" }).releaseKind).toBe("initial");
    for (const release of ["r10", "latest", "current\n", ""]) {
      expect(() => retirementPreflightOptions({ ...guarded, GP_LIVE_RETIREMENT_PREFLIGHT_RELEASE: release })).toThrow();
    }
  });
  it("does not accept path injection or nonce suffix aliases", () => {
    for (const nonce of ["../test", `${probeNonce}\n`, `${probeNonce}/suffix`, ""]) expect(() => preflightResultPath(nonce)).toThrow();
  });
});

describe("retirement preflight exact result readback", () => {
  it("accepts only complete positive proof bound to the script, authority and fresh session", () => {
    expect(decodeRetirementPreflightFile(envelope(), expected)).toEqual(result());
    for (const key of ["sessionId", "probeNonce", "authoritySha256", "scriptSha256"]) {
      expect(() => parseRetirementPreflightResult({ ...result(), [key]: "different" }, expected)).toThrow();
    }
  });
  it("does not treat a retirement record or incomplete checks as positive proof", () => {
    for (const key of Object.keys(result().checks)) {
      expect(() => parseRetirementPreflightResult({ ...result(), checks: { ...result().checks, [key]: false } }, expected)).toThrow();
    }
    expect(() => parseRetirementPreflightResult({ ...result(), retirementSha256: null }, expected)).toThrow();
    expect(() => parseRetirementPreflightResult({ ...result(), processesInspected: 16385 }, expected)).toThrow();
    expect(() => parseRetirementPreflightResult({ ...result(), extra: true }, expected)).toThrow();
  });
  it("retains a valid blocked result as blocked", () => {
    const blocked = { ...result(), outcome: "blocked", code: "owner_locked", checks: { ...result().checks, owners: false, processes: false, unchanged: false } };
    expect(parseRetirementPreflightResult(blocked, expected).outcome).toBe("blocked");
  });
  it("rejects workspace/session mismatch, malformed encoding and unexpected MIME or fields", () => {
    const body = envelope();
    for (const mutated of [
      { ...body, path: body.path + "\n" }, { ...body, relative_path: "../result.json" }, { ...body, session_id: probeNonce },
      { ...body, bytes: body.bytes + 1 }, { ...body, body_b64: body.body_b64 + "\n" },
      { ...body, mime_type: "text/plain" }, { ...body, extra: true },
    ]) expect(() => decodeRetirementPreflightFile(mutated, expected)).toThrow();
  });
  it("checker contains no process execution or signal primitives", async () => {
    const source = await readFile(new URL("../scripts/lib/retirement-preflight.py", import.meta.url), "utf8");
    expect(source).not.toMatch(/\b(?:subprocess|Popen|os\.system|os\.kill|os\.killpg|os\.exec\w*|os\.spawn\w*)\b/);
    expect(source).toContain("os.O_NOFOLLOW");
    expect(source).toContain("fcntl.LOCK_EX | fcntl.LOCK_NB");
  });
  it("requires the measured session-prefixed relative path and rejects bare aliases", () => {
    const body = envelope();
    const shape = summarizeRetirementPreflightReadback(200, body, expected);
    expect(shape).toMatchObject({ expectedPathMatch: true, expectedSessionMatch: true,
      expectedRelativePathMatch: false, expectedSessionRelativePathMatch: true, base64Valid: true, declaredBodyBytesMatch: true });
    expect(decodeRetirementPreflightFile(body, expected)).toEqual(result());
    expect(() => decodeRetirementPreflightFile({ ...body, relative_path: preflightResultPath(probeNonce) }, expected)).toThrow("preflight_read_identity_invalid");
  });
  it("never emits raw identities, body, unknown keys or unallowlisted MIME values", () => {
    const body = { ...envelope(), private_secret_key: "secret-value", mime_type: "secret-value" };
    const shape = summarizeRetirementPreflightReadback(200, body, expected);
    expect(shape.unknownKeyCount).toBe(1);
    expect(shape.mimeType).toBeNull();
    const encoded = JSON.stringify(shape);
    for (const forbidden of [sessionId, probeNonce, body.path, body.body_b64, "secret-value", "private_secret_key"]) {
      expect(encoded).not.toContain(forbidden);
    }
    expect(summarizeRetirementPreflightReadback(404, { detail: "secret-value" }, expected)).toMatchObject({
      responseKeys: ["detail"], expectedPathMatch: false, base64Valid: false, bodyBytes: null,
    });
  });
  it("classifies an exact zombie without treating it as a passing retirement proof", () => {
    const conflict = { reason: "related_process", scan: 1, state: "Z", oldPidMatch: true, oldStartTickMatch: true,
      childPidMatch: false, parentMatch: false, groupMatch: true, sessionMatch: true, diagnosticSelf: false,
      ghostprovArgument: false, oldEntrypointArgument: false, oldWorkspaceArgument: false };
    const blocked = { ...result(), outcome: "blocked", code: "live_process", checks: { ...result().checks, processes: false },
      processConflictCount: 1, processConflicts: [conflict] };
    expect(parseRetirementPreflightResult(blocked, expected).processConflicts[0]?.state).toBe("Z");
    expect(() => parseRetirementPreflightResult({ ...blocked, outcome: "retired", code: "retired", checks: result().checks }, expected)).toThrow();
    for (const invalid of [{ ...conflict, state: "secret-state" }, { ...conflict, state: "Z\n" },
      { ...conflict, reason: "raw-path" }, { ...conflict, pid: 1234 }, { ...conflict, oldPidMatch: "true" }]) {
      expect(() => parseRetirementPreflightResult({ ...blocked, processConflicts: [invalid] }, expected)).toThrow();
    }
  });
  it("bounds conflict capture and records truncation without losing failure", () => {
    const conflict = { reason: "argument_match", scan: 2, state: "S", oldPidMatch: false, oldStartTickMatch: false,
      childPidMatch: false, parentMatch: false, groupMatch: false, sessionMatch: false, diagnosticSelf: true,
      ghostprovArgument: true, oldEntrypointArgument: false, oldWorkspaceArgument: true };
    const blocked = { ...result(), outcome: "blocked", code: "live_process", checks: { ...result().checks, processes: false },
      processConflictCount: 9, processConflictsTruncated: true, processConflicts: Array.from({ length: 8 }, () => conflict) };
    expect(Buffer.byteLength(JSON.stringify(blocked))).toBeLessThanOrEqual(4096);
    expect(parseRetirementPreflightResult(blocked, expected).processConflictsTruncated).toBe(true);
    expect(() => parseRetirementPreflightResult({ ...blocked, processConflictsTruncated: false }, expected)).toThrow();
    expect(() => parseRetirementPreflightResult({ ...blocked, processConflicts: [...blocked.processConflicts, conflict] }, expected)).toThrow();
  });
  it("retains precise allowlisted file failure categories without raw filesystem details", () => {
    const blocked = { ...result(), outcome: "blocked", code: "authority_file", checks: { ...result().checks, static: false },
      fileFailure: { role: "static", reason: "absent", component: "workspace" } as const };
    expect(parseRetirementPreflightResult(blocked, expected).fileFailure).toEqual(blocked.fileFailure);
    for (const reason of ["symlink", "nonregular", "uid_mismatch", "mode_writable", "size_bound", "size_mismatch", "hash_mismatch", "permission"]) {
      expect(parseRetirementPreflightResult({ ...blocked, fileFailure: { role: "artifact", reason, component: "opened" } }, expected).outcome).toBe("blocked");
    }
    for (const fileFailure of [{ ...blocked.fileFailure, path: "/private/path" }, { ...blocked.fileFailure, uid: 1000 },
      { ...blocked.fileFailure, role: "/private/path" }, { ...blocked.fileFailure, reason: "raw-error" }, { ...blocked.fileFailure, component: "raw-directory" }]) {
      expect(() => parseRetirementPreflightResult({ ...blocked, fileFailure }, expected)).toThrow();
    }
    expect(() => parseRetirementPreflightResult({ ...result(), fileFailure: blocked.fileFailure }, expected)).toThrow();
  });
});
