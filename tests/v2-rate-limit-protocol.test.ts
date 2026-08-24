import { describe, expect, it } from "bun:test";
import { parseFrame, V2ProtocolError } from "../src/v2/protocol/validate";

const terminal = {
  kind: "response_terminal",
  requestId: "req-rate-limit",
  requestDigest: "a".repeat(64),
  responseId: "resp-rate-limit",
  eventCount: 2,
  byteCount: 128,
  transcriptSha256: "b".repeat(64),
  terminalStatus: "failed",
  finalEventIndex: 1,
  finalSha256: "c".repeat(64),
  epoch: 1,
  sequence: 3,
} as const;

describe("V2 rate-limit protocol metadata", () => {
  it("carries token-header metadata on a streamed terminal", () => {
    expect(parseFrame({
      ...terminal,
      rateLimit: {
        retryAfterMs: 109,
        resetTokensMs: 360_000,
        limitTokens: 40_000_000,
        remainingTokens: 0,
      },
    }, "sidecar")).toMatchObject({
      rateLimit: {
        retryAfterMs: 109,
        resetTokensMs: 360_000,
        limitTokens: 40_000_000,
        remainingTokens: 0,
      },
    });
  });

  it("rejects malformed or unbounded rate-limit metadata", () => {
    expect(() => parseFrame({
      ...terminal,
      rateLimit: { remainingTokens: -1 },
    }, "sidecar")).toThrow(V2ProtocolError);
    expect(() => parseFrame({
      ...terminal,
      rateLimit: { resetTokensMs: 3_600_001 },
    }, "sidecar")).toThrow(V2ProtocolError);
  });
});
