import { describe, expect, it } from "bun:test";
import {
  BootstrapCryptoError,
  createCredentialRotationKey,
  openCredentialRotation,
  sealCredentialRotation,
  type CredentialRotationBinding,
  type RotatableCredentialPayload,
} from "../src/v2/bootstrap/crypto";
import { base64UrlDecode, base64UrlEncode } from "../src/v2/protocol/shared";

const BINDING: CredentialRotationBinding = {
  projectId: "e631d12c-d917-4434-b360-41b4715d6679",
  sandboxSessionId: "sandbox-session-1",
  leaderGeneration: 4,
  buildHash: "a".repeat(64),
  schemaHash: "b".repeat(64),
  instanceId: "sidecar-instance-1",
  rotationId: "rotation-1",
};
const CREDENTIAL: RotatableCredentialPayload = {
  url: `wss://prism.test/y/d/${BINDING.projectId}/ws`,
  baseUrl: `https://prism.test/y/d/${BINDING.projectId}`,
  docId: BINDING.projectId,
  token: `rotated-token-${"x".repeat(48)}`,
  authorization: "full",
};

describe("V2 credential rotation cryptography", () => {
  it("round-trips a rotated Y-Sweet token without serializing key or plaintext", async () => {
    const key = createCredentialRotationKey();
    const sealed = await sealCredentialRotation({
      rotationKey: key,
      binding: BINDING,
      credential: CREDENTIAL,
    });
    const serialized = JSON.stringify(sealed);

    expect(serialized).not.toContain(key);
    expect(serialized).not.toContain(CREDENTIAL.token);
    await expect(openCredentialRotation({
      rotationKey: key,
      sealed,
      expectedBinding: BINDING,
    })).resolves.toEqual(CREDENTIAL);
  });

  it("rejects another key, instance, rotation ID, and ciphertext tampering", async () => {
    const key = createCredentialRotationKey();
    const sealed = await sealCredentialRotation({
      rotationKey: key,
      binding: BINDING,
      credential: CREDENTIAL,
    });
    const tamperedCiphertext = base64UrlDecode(sealed.ciphertext);
    tamperedCiphertext[Math.floor(tamperedCiphertext.length / 2)] ^= 1;
    const mutations = [
      { key: createCredentialRotationKey(), sealed, binding: BINDING },
      { key, sealed, binding: { ...BINDING, instanceId: "sidecar-instance-2" } },
      { key, sealed, binding: { ...BINDING, rotationId: "rotation-2" } },
      {
        key,
        sealed: {
          ...sealed,
          ciphertext: base64UrlEncode(tamperedCiphertext),
        },
        binding: BINDING,
      },
    ];

    for (const mutation of mutations) {
      await expect(openCredentialRotation({
        rotationKey: mutation.key,
        sealed: mutation.sealed,
        expectedBinding: mutation.binding,
      })).rejects.toBeInstanceOf(BootstrapCryptoError);
    }
  });
});
