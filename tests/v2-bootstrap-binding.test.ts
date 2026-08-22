import { describe, expect, it } from "bun:test";
import { protocolBindingFromBootstrap } from "../src/v2/bootstrap/binding";

describe("V2 bootstrap binding conversion", () => {
  it("maps the sandbox-session field to the Awareness session field exactly", () => {
    const binding = protocolBindingFromBootstrap({
      projectId: "e631d12c-d917-4434-b360-41b4715d6679",
      sandboxSessionId: "sandbox-session-1",
      leaderGeneration: 4,
      buildHash: "a".repeat(64),
      schemaHash: "b".repeat(64),
      nonce: "bootstrap-nonce",
    });

    expect(binding).toEqual({
      projectId: "e631d12c-d917-4434-b360-41b4715d6679",
      sessionId: "sandbox-session-1",
      leaderGeneration: 4,
      buildHash: "a".repeat(64),
      schemaHash: "b".repeat(64),
    });
    expect(binding).not.toHaveProperty("sandboxSessionId");
    expect(binding).not.toHaveProperty("nonce");
  });
});
