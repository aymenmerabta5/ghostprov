import { describe, expect, it } from "bun:test";
import {
  createCredentialRotationKey,
  sealCredentialRotation,
  type BootstrapCredentialPayload,
  type CredentialRotationBinding,
  type RotatableCredentialPayload,
} from "../src/v2/bootstrap/crypto";
import {
  SidecarCredentialRotator,
  SidecarCredentialRotationError,
} from "../src/v2/bootstrap/rotation";
import type { ProtocolBinding } from "../src/v2/protocol/shared";

const BINDING: ProtocolBinding = {
  projectId: "e631d12c-d917-4434-b360-41b4715d6679",
  sessionId: "sandbox-session-1",
  leaderGeneration: 4,
  buildHash: "a".repeat(64),
  schemaHash: "b".repeat(64),
};
const INSTANCE = "sidecar-instance-1";

function initial(): BootstrapCredentialPayload {
  return {
    url: `wss://prism.test/y/d/${BINDING.projectId}/ws`,
    baseUrl: `https://prism.test/y/d/${BINDING.projectId}`,
    docId: BINDING.projectId,
    token: "initial-token",
    authorization: "full",
    rotationKey: createCredentialRotationKey(),
  };
}

async function sealed(
  credential: BootstrapCredentialPayload,
  rotationId: string,
  token: string,
): Promise<string> {
  const binding: CredentialRotationBinding = {
    projectId: BINDING.projectId,
    sandboxSessionId: BINDING.sessionId,
    leaderGeneration: BINDING.leaderGeneration,
    buildHash: BINDING.buildHash,
    schemaHash: BINDING.schemaHash,
    instanceId: INSTANCE,
    rotationId,
  };
  const next: RotatableCredentialPayload = {
    url: credential.url,
    baseUrl: credential.baseUrl,
    docId: credential.docId,
    token,
    authorization: credential.authorization,
  };
  return JSON.stringify(await sealCredentialRotation({
    rotationKey: credential.rotationKey,
    binding,
    credential: next,
  }));
}

describe("SidecarCredentialRotator", () => {
  it("replaces only the in-memory token and accepts an identical replay", async () => {
    const credential = initial();
    const rotator = new SidecarCredentialRotator({
      credential,
      binding: BINDING,
      instanceId: INSTANCE,
    });
    const sealedJson = await sealed(credential, "rotation-1", "rotated-token");

    await expect(rotator.accept("rotation-1", sealedJson)).resolves.toMatchObject({
      token: "rotated-token",
      rotationKey: credential.rotationKey,
    });
    await expect(rotator.accept("rotation-1", sealedJson)).resolves.toEqual(rotator.current());
    expect(JSON.stringify(rotator.current())).not.toContain(sealedJson);
  });

  it("rejects a conflicting replay and a rotation bound to another instance", async () => {
    const credential = initial();
    const rotator = new SidecarCredentialRotator({
      credential,
      binding: BINDING,
      instanceId: INSTANCE,
    });
    const first = await sealed(credential, "rotation-1", "first-token");
    const conflict = await sealed(credential, "rotation-1", "second-token");
    await rotator.accept("rotation-1", first);

    await expect(rotator.accept("rotation-1", conflict)).rejects.toBeInstanceOf(
      SidecarCredentialRotationError,
    );

    const other = new SidecarCredentialRotator({
      credential,
      binding: BINDING,
      instanceId: "sidecar-instance-2",
    });
    await expect(other.accept("rotation-1", first)).rejects.toBeInstanceOf(
      SidecarCredentialRotationError,
    );
  });
});
