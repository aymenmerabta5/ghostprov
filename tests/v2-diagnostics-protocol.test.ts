import { describe, expect, it } from "bun:test";
import { safeDiagnostics } from "./v2-diagnostics-types.test";
import type { AwarenessEnvelope, HostFrame, ProtocolBinding } from "../src/v2/protocol/shared";
import { parseAwarenessEnvelope, parseAwarenessEnvelopeWithDiagnostics, parseFrame } from "../src/v2/protocol/validate";

const BINDING: ProtocolBinding = { projectId: "e631d12c-d917-4434-b360-41b4715d6679", sessionId: "s-1", leaderGeneration: 1, buildHash: "a".repeat(64), schemaHash: "b".repeat(64) };
function envelope(): AwarenessEnvelope<HostFrame> { return { protocol: "gp-awareness-v2", schemaVersion: 1, schemaHash: BINDING.schemaHash, buildHash: BINDING.buildHash, projectId: BINDING.projectId, sessionId: BINDING.sessionId, peerId: "host", instanceId: "host-1", leaderGeneration: 1, epoch: 1, revision: 1, ack: { epoch: 1, sequence: -1 }, credits: 128, active: 0, queued: 0, draining: false, frames: [] }; }

describe("optional V2 protocol diagnostics", () => {
  it("preserves valid diagnostics and strips malformed diagnostics without rejecting core", () => {
    const valid = { ...envelope(), diagnostics: safeDiagnostics() };
    expect(parseAwarenessEnvelope(valid, BINDING, "host").diagnostics).toEqual(safeDiagnostics());
    const malformed = parseAwarenessEnvelopeWithDiagnostics({ ...envelope(), diagnostics: { version: 1, prompt: "no" } }, BINDING, "host");
    expect(malformed.envelope.diagnostics).toBeUndefined();
    expect(malformed.malformed).toBe(true);
  });

  it("accepts bounded ACK diagnostics but still rejects invalid core fields", () => {
    const frame = parseFrame({ kind: "request_ack", requestId: "req-1", requestDigest: "a".repeat(64), committed: true, diagnostics: { firstFrameToCommitMs: 4, commitToAssemblyMs: 1, claimSyncMs: 2, admissionWaitMs: 0, commitProcessingMs: 3 }, epoch: 1, sequence: 0 }, "sidecar");
    expect((frame as any).diagnostics).toMatchObject({ claimSyncMs: 2 });
    expect(() => parseFrame({ ...(frame as any), committed: false }, "sidecar")).toThrow("commitment");
  });
});
