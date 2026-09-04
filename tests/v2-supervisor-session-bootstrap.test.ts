import { describe, expect, it } from "bun:test";
import { createCredentialRotationKey } from "../src/v2/bootstrap/crypto";
import {
  openSupervisorSessionBootstrap,
  sealSupervisorSessionBootstrap,
} from "../src/v2/supervisor/session-bootstrap";

const binding = Object.freeze({
  projectId: "11111111-1111-4111-8111-111111111111",
  sandboxSessionId: "sandbox-session-1",
  leaderGeneration: 3,
  buildHash: "a".repeat(64),
  schemaHash: "b".repeat(64),
  nonce: "supervisor-nonce-1",
  protocolRevision: "r2" as const,
  schemaVersion: 2 as const,
});

const payload = Object.freeze({
  instanceId: "supervisor-instance-1",
  credential: Object.freeze({
    url: "wss://y-sweet.test/ws",
    baseUrl: "https://y-sweet.test/",
    docId: binding.projectId,
    token: "ysweet-secret-token",
    authorization: "Bearer",
    rotationKey: createCredentialRotationKey(),
  }),
});

describe("V2 supervisor Codex-session bootstrap", () => {
  it("round-trips the credential only for the exact turn session and binding", async () => {
    const sealed = await sealSupervisorSessionBootstrap({
      sessionId: "codex-session-1111111111111111",
      binding,
      payload,
    });
    expect(JSON.stringify(sealed)).not.toContain(payload.credential.token);
    await expect(openSupervisorSessionBootstrap({
      sessionId: "codex-session-1111111111111111",
      expectedBinding: binding,
      sealed,
    })).resolves.toEqual(payload);
    await expect(openSupervisorSessionBootstrap({
      sessionId: "codex-session-2222222222222222",
      expectedBinding: binding,
      sealed,
    })).rejects.toMatchObject({ code: "ciphertext" });
    await expect(openSupervisorSessionBootstrap({
      sessionId: "codex-session-1111111111111111",
      expectedBinding: { ...binding, leaderGeneration: 4 },
      sealed,
    })).rejects.toMatchObject({ code: "binding" });
  });

  it("rejects malformed session identities and modified ciphertext", async () => {
    await expect(sealSupervisorSessionBootstrap({ sessionId: "short", binding, payload }))
      .rejects.toMatchObject({ code: "key" });
    const sealed = await sealSupervisorSessionBootstrap({
      sessionId: "codex-session-1111111111111111",
      binding,
      payload,
    });
    await expect(openSupervisorSessionBootstrap({
      sessionId: "codex-session-1111111111111111",
      expectedBinding: binding,
      sealed: { ...sealed, ciphertext: `${sealed.ciphertext.slice(0, -1)}A` },
    })).rejects.toMatchObject({ code: "ciphertext" });
  });
});
