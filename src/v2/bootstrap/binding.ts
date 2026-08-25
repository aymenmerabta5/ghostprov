import type { ProtocolBinding } from "../protocol/shared";
import type { BootstrapBinding } from "./crypto";

export function protocolBindingFromBootstrap(
  binding: BootstrapBinding,
  revision: "r1" | "r2",
): ProtocolBinding {
  const base = {
    projectId: binding.projectId,
    sessionId: binding.sandboxSessionId,
    leaderGeneration: binding.leaderGeneration,
    buildHash: binding.buildHash,
    schemaHash: binding.schemaHash,
    launchNonce: binding.nonce,
  };
  if (revision === "r2") {
    if (binding.protocolRevision !== "r2" || binding.schemaVersion !== 2) {
      throw new TypeError("R2 protocol binding requires an R2 bootstrap binding");
    }
    return { ...base, protocolRevision: "r2" };
  }
  if (binding.protocolRevision !== undefined || binding.schemaVersion !== undefined) {
    throw new TypeError("R1 protocol binding requires an exact R1 bootstrap binding");
  }
  return { ...base, protocolRevision: "r1" };
}
