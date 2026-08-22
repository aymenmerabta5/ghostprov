import type { ProtocolBinding } from "../protocol/shared";
import type { BootstrapBinding } from "./crypto";

export function protocolBindingFromBootstrap(
  binding: BootstrapBinding,
): ProtocolBinding {
  return {
    projectId: binding.projectId,
    sessionId: binding.sandboxSessionId,
    leaderGeneration: binding.leaderGeneration,
    buildHash: binding.buildHash,
    schemaHash: binding.schemaHash,
  };
}
