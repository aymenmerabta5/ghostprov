import {
  openCredentialRotation,
  type BootstrapCredentialPayload,
  type CredentialRotationBinding,
  type SealedCredentialRotation,
} from "./crypto";
import type { ProtocolBinding } from "../protocol/shared";

export class SidecarCredentialRotationError extends Error {
  readonly name = "SidecarCredentialRotationError";

  constructor(message: string, readonly code: "binding" | "conflict" | "crypto") {
    super(message);
  }
}

export class SidecarCredentialRotator {
  readonly #binding: ProtocolBinding;
  readonly #instanceId: string;
  readonly #seen = new Map<string, string>();
  #credential: BootstrapCredentialPayload;

  constructor(options: {
    credential: BootstrapCredentialPayload;
    binding: ProtocolBinding;
    instanceId: string;
  }) {
    if (!options.instanceId) {
      throw new SidecarCredentialRotationError("Sidecar instance ID is missing", "binding");
    }
    if (options.credential.docId !== options.binding.projectId) {
      throw new SidecarCredentialRotationError("Credential belongs to another project", "binding");
    }
    this.#binding = options.binding;
    this.#instanceId = options.instanceId;
    this.#credential = { ...options.credential };
  }

  current(): BootstrapCredentialPayload {
    return { ...this.#credential };
  }

  async accept(
    rotationId: string,
    sealedJson: string,
  ): Promise<BootstrapCredentialPayload> {
    if (!/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(rotationId)) {
      throw new SidecarCredentialRotationError("Credential rotation ID is invalid", "binding");
    }
    const previous = this.#seen.get(rotationId);
    if (previous !== undefined) {
      if (previous !== sealedJson) {
        throw new SidecarCredentialRotationError(
          "Credential rotation ID has a conflicting replay",
          "conflict",
        );
      }
      return this.current();
    }
    let sealed: SealedCredentialRotation;
    try {
      sealed = JSON.parse(sealedJson) as SealedCredentialRotation;
    } catch {
      throw new SidecarCredentialRotationError("Credential rotation is malformed", "crypto");
    }
    const expectedBinding: CredentialRotationBinding = {
      projectId: this.#binding.projectId,
      sandboxSessionId: this.#binding.sessionId,
      leaderGeneration: this.#binding.leaderGeneration,
      buildHash: this.#binding.buildHash,
      schemaHash: this.#binding.schemaHash,
      instanceId: this.#instanceId,
      rotationId,
    };
    try {
      const next = await openCredentialRotation({
        rotationKey: this.#credential.rotationKey,
        sealed,
        expectedBinding,
      });
      this.#credential = {
        ...next,
        rotationKey: this.#credential.rotationKey,
      };
    } catch {
      throw new SidecarCredentialRotationError(
        "Credential rotation authentication failed",
        "crypto",
      );
    }
    this.#seen.set(rotationId, sealedJson);
    if (this.#seen.size > 128) {
      this.#seen.delete(this.#seen.keys().next().value!);
    }
    return this.current();
  }
}
