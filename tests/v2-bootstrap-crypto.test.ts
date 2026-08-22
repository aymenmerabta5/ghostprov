import { describe, expect, it } from "bun:test";
import * as Y from "yjs";
import {
  BootstrapCryptoError,
  createBootstrapKeyPair,
  openBootstrapCredential,
  sealBootstrapCredential,
  type BootstrapBinding,
  type BootstrapCredentialPayload,
  type SealedBootstrapCredential,
} from "../src/v2/bootstrap/crypto";

const PROJECT = "e631d12c-d917-4434-b360-41b4715d6679";
const BINDING: BootstrapBinding = {
  projectId: PROJECT,
  sandboxSessionId: "sandbox-session-1",
  leaderGeneration: 7,
  buildHash: "a".repeat(64),
  schemaHash: "b".repeat(64),
  nonce: "bootstrap-nonce-1",
};

function credential(token = "ysweet-token-canary"): BootstrapCredentialPayload {
  return {
    url: `wss://prism.test/y/d/${PROJECT}/ws`,
    baseUrl: `https://prism.test/y/d/${PROJECT}`,
    docId: PROJECT,
    token,
    authorization: "full",
  };
}

function flip(value: string): string {
  const first = value[0] === "A" ? "B" : "A";
  return first + value.slice(1);
}

async function sealedFixture(token?: string): Promise<{
  sidecar: Awaited<ReturnType<typeof createBootstrapKeyPair>>;
  sealed: SealedBootstrapCredential;
}> {
  const sidecar = await createBootstrapKeyPair(BINDING);
  const sealed = await sealBootstrapCredential({
    binding: BINDING,
    sidecarPublicJwk: sidecar.publicJwk,
    credential: credential(token),
  });
  return { sidecar, sealed };
}

describe("V2 bootstrap cryptography", () => {
  it("round-trips a Y-Sweet credential without serializing plaintext", async () => {
    const { sidecar, sealed } = await sealedFixture();
    const serialized = JSON.stringify(sealed);

    expect(serialized).not.toContain("ysweet-token-canary");
    expect(serialized).not.toContain("prism_session_token");
    await expect(openBootstrapCredential({
      sealed,
      sidecarPrivateKey: sidecar.privateKey,
      expectedBinding: BINDING,
    })).resolves.toEqual(credential());
  });

  it("rejects ciphertext and authenticated-binding tampering", async () => {
    const { sidecar, sealed } = await sealedFixture();
    const mutations: SealedBootstrapCredential[] = [
      { ...sealed, ciphertext: flip(sealed.ciphertext) },
      { ...sealed, iv: flip(sealed.iv) },
      { ...sealed, binding: { ...sealed.binding, nonce: "other-nonce" } },
      { ...sealed, binding: { ...sealed.binding, leaderGeneration: 8 } },
      { ...sealed, binding: { ...sealed.binding, buildHash: "c".repeat(64) } },
    ];

    for (const mutation of mutations) {
      await expect(openBootstrapCredential({
        sealed: mutation,
        sidecarPrivateKey: sidecar.privateKey,
        expectedBinding: BINDING,
      })).rejects.toBeInstanceOf(BootstrapCryptoError);
    }
  });

  it("rejects a different sidecar private key and redacts credential values", async () => {
    const secret = "credential-secret-never-log";
    const { sealed } = await sealedFixture(secret);
    const other = await createBootstrapKeyPair(BINDING);

    try {
      await openBootstrapCredential({
        sealed,
        sidecarPrivateKey: other.privateKey,
        expectedBinding: BINDING,
      });
      throw new Error("expected decryption failure");
    } catch (error) {
      expect(error).toBeInstanceOf(BootstrapCryptoError);
      expect(String(error)).not.toContain(secret);
    }
  });

  it("rejects a credential for another project even when encrypted correctly", async () => {
    const sidecar = await createBootstrapKeyPair(BINDING);
    await expect(sealBootstrapCredential({
      binding: BINDING,
      sidecarPublicJwk: sidecar.publicJwk,
      credential: { ...credential(), docId: "69288e8b-c183-45f3-9131-d64b05b98928" },
    })).rejects.toMatchObject({ code: "payload" });
  });

  it("rejects credential material embedded in Y-Sweet URLs", async () => {
    const sidecar = await createBootstrapKeyPair(BINDING);
    await expect(sealBootstrapCredential({
      binding: BINDING,
      sidecarPublicJwk: sidecar.publicJwk,
      credential: {
        ...credential(),
        url: `wss://prism.test/y/d/${PROJECT}/ws?token=plaintext-secret`,
      },
    })).rejects.toMatchObject({ code: "payload" });
  });

  it("leaves no plaintext token canary in serialized Yjs history", async () => {
    const doc = new Y.Doc();
    const history = doc.getMap<string>("bootstrap");
    const sidecar = await createBootstrapKeyPair(BINDING);
    const canaries: string[] = [];

    for (let index = 0; index < 100; index++) {
      const token = `ysweet-plaintext-canary-${index}-${"x".repeat(32)}`;
      canaries.push(token);
      const sealed = await sealBootstrapCredential({
        binding: { ...BINDING, nonce: `bootstrap-nonce-${index}` },
        sidecarPublicJwk: sidecar.publicJwk,
        credential: credential(token),
      });
      history.set(String(index), JSON.stringify(sealed));
    }

    const serializedState = new TextDecoder().decode(Y.encodeStateAsUpdate(doc));
    const serializedJson = JSON.stringify([...history.values()]);
    for (const canary of canaries) {
      expect(serializedState).not.toContain(canary);
      expect(serializedJson).not.toContain(canary);
    }
    expect(serializedState).not.toContain("prism_session_token");
    doc.destroy();
  });

  it("validates bootstrap binding fields before generating keys", async () => {
    await expect(createBootstrapKeyPair({
      ...BINDING,
      leaderGeneration: 0,
    })).rejects.toMatchObject({ code: "binding" });
    await expect(createBootstrapKeyPair({
      ...BINDING,
      buildHash: "bad",
    })).rejects.toMatchObject({ code: "binding" });
  });
});
