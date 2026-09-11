import { describe, expect, it } from "bun:test";
import { OutageRequestProof, OutageSupervisorProof, resolveTransportOutageProofOptions, validateOutageTerminal } from "../scripts/live_transport_outage_recovery";
import type { R2RequestStartFrame, SidecarFrame } from "../src/v2/protocol/shared";

const guarded = {
  GP_LIVE_TRANSPORT_OUTAGE_RECOVERY: "1",
  GP_LIVE_ALLOW_TEST_PROJECT_MUTATION: "1",
  PRISM_COOKIE_FILE: "test-only-cookie-path.json",
};

describe("live transport outage proof guards", () => {
  it("rejects missing authorization before any credential or project access", () => {
    expect(() => resolveTransportOutageProofOptions({})).toThrow();
    expect(() => resolveTransportOutageProofOptions({ ...guarded, GP_LIVE_TRANSPORT_OUTAGE_RECOVERY: "0" })).toThrow();
    expect(() => resolveTransportOutageProofOptions({ ...guarded, GP_LIVE_ALLOW_TEST_PROJECT_MUTATION: "0" })).toThrow();
    expect(() => resolveTransportOutageProofOptions({ ...guarded, PRISM_COOKIE_FILE: "" })).toThrow();
  });
  it("accepts only candidate 18888 and refuses production or arbitrary ports", () => {
    expect(() => resolveTransportOutageProofOptions(guarded)).not.toThrow();
    expect(() => resolveTransportOutageProofOptions({ ...guarded, PORT: "18888" })).not.toThrow();
    for (const port of ["18787", "0", "18788", "NaN", "65536"]) {
      expect(() => resolveTransportOutageProofOptions({ ...guarded, PORT: port })).toThrow();
    }
  });
  it("rejects invalid models including trailing newline aliases", () => {
    for (const model of ["", "gpt-5.6-sol\n", "gpt-5.6-sol ", " leading", "x".repeat(129)]) {
      expect(() => resolveTransportOutageProofOptions({ ...guarded, GP_LIVE_MODEL: model })).toThrow();
    }
  });
});

describe("live outage signed cleanup accounting", () => {
  it("does not let an earlier signed shutdown hide a later ambiguous submission", () => {
    const proof = new OutageSupervisorProof();
    proof.register("project", "first");
    proof.submit("project", "first");
    proof.signedShutdown("project", "first");
    proof.register("project", "second");
    proof.submit("project", "second");
    expect(proof.snapshot()).toEqual({ submitted: 2, signedShutdowns: 1, unprovedShutdowns: 1 });
    proof.signedShutdown("project", "second");
    expect(proof.snapshot().unprovedShutdowns).toBe(0);
  });
  it("forbids replay and a successor until the exact prior nonce has signed cleanup", () => {
    const proof = new OutageSupervisorProof();
    proof.register("project", "first");
    proof.submit("project", "first");
    proof.register("project", "second");
    expect(() => proof.submit("project", "first")).toThrow("bootstrap_submission_replay_forbidden");
    expect(() => proof.submit("project", "second")).toThrow("bootstrap_submission_replay_forbidden");
    expect(() => proof.signedShutdown("other", "first")).toThrow();
    expect(() => proof.signedShutdown("project", "second")).toThrow();
    expect(proof.snapshot().unprovedShutdowns).toBe(1);
  });
  it("bounds receipts and rejects duplicate identities", () => {
    const proof = new OutageSupervisorProof();
    for (let index = 0; index < 128; index++) proof.register("project", String(index));
    expect(() => proof.register("project", "129")).toThrow();
    expect(() => proof.register("other", "0")).toThrow();
  });
});

const marker = "[OUTAGE_PROOF_30000_1]";
const start: R2RequestStartFrame = {
  kind: "request_start", requestId: "request-1", requestDigest: "digest", protocolRevision: "r2", schemaVersion: 2,
  projectId: "project", sessionId: "session", leaderGeneration: 1, buildHash: "build", schemaHash: "schema",
  launchNonce: "launch", hostInstanceId: "host", targetInstanceId: "sidecar", decodedBytes: 50, encodedBytes: 50,
  encoding: "identity", partCount: 1,
};
const terminal: Extract<SidecarFrame, { kind: "response_terminal" }> = {
  kind: "response_terminal", requestId: start.requestId, requestDigest: start.requestDigest, responseId: "resp_1",
  terminalStatus: "completed", eventCount: 1, byteCount: 100, transcriptSha256: "transcript", finalEventIndex: 0, finalSha256: "final",
};
const response = (text = marker, id = "resp_1") => ({ id, status: "completed", output: [{ type: "message", content: [{ type: "output_text", text }] }] });

describe("live outage execution and terminal proof", () => {
  it("accepts retransmissions of one exact execution and correlates the final provider ID", () => {
    const proof = new OutageRequestProof();
    proof.begin(marker, "project");
    proof.start("project", start);
    proof.start("project", { ...start });
    proof.terminal("project", terminal);
    proof.terminal("project", { ...terminal });
    proof.complete(marker, response(), "responses");
    expect(proof.snapshot()).toMatchObject({ invoked: 1, executionIdentities: 1, providerTerminals: 1, accepted: 1, pending: 0 });
    expect(proof.snapshot().requests[0]?.startPublications).toBe(2);
  });
  it("rejects a new execution ID or changed execution fence for the same operation", () => {
    const proof = new OutageRequestProof();
    proof.begin(marker, "project");
    proof.start("project", start);
    expect(() => proof.start("project", { ...start, requestId: "request-2" })).toThrow("request_execution_replayed_or_untracked");
    expect(() => proof.start("project", { ...start, launchNonce: "other" })).toThrow("request_execution_identity_changed");
  });
  it("requires the terminal to match the request, project, digest and provider identity", () => {
    const proof = new OutageRequestProof();
    proof.begin(marker, "project");
    proof.start("project", start);
    expect(() => proof.complete(marker, response(), "responses")).toThrow("public_terminal_lacks_exact_wire_proof");
    expect(() => proof.terminal("other", terminal)).toThrow();
    expect(() => proof.terminal("project", { ...terminal, requestDigest: "other" })).toThrow();
    expect(() => proof.terminal("project", { ...terminal, terminalStatus: "incomplete" })).toThrow();
    proof.terminal("project", terminal);
    expect(() => proof.complete(marker, response(marker, "resp_other"), "responses")).toThrow();
  });
  it("rejects another worker's terminal marker and a marker prefix alias", () => {
    expect(() => validateOutageTerminal(response("[OUTAGE_PROOF_30000_2]"), marker, "responses")).toThrow();
    expect(() => validateOutageTerminal(response(`${marker}_extra`), marker, "responses")).toThrow();
    expect(() => validateOutageTerminal({ ...response(), status: "incomplete" }, marker, "responses")).toThrow();
    expect(validateOutageTerminal(response(`Some text. ${marker}\n`), marker, "responses")).toBe("resp_1");
  });
  it("requires Anthropic text and end_turn while keeping its public ID separate", () => {
    const proof = new OutageRequestProof();
    proof.begin(marker, null);
    proof.start("project", start);
    proof.terminal("project", terminal);
    const message = { id: "msg_public", type: "message", stop_reason: "end_turn", content: [{ type: "text", text: marker }] };
    expect(() => validateOutageTerminal({ ...message, stop_reason: "max_tokens" }, marker, "anthropic")).toThrow();
    proof.complete(marker, message, "anthropic");
    expect(proof.snapshot().accepted).toBe(1);
  });
});
