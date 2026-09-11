import type * as Y from "yjs";
import type { BootstrapBinding } from "../bootstrap/crypto";
import { canonicalJson } from "../protocol/shared";
import {
  parseSupervisorSandboxMessage,
  sameSupervisorBinding,
  verifySupervisorSandboxMessage,
  type SignedSupervisorSandboxMessage,
} from "./protocol";

export const SUPERVISOR_RETIREMENT_MAP = "ghostprovV2SupervisorRetirement";
export const SUPERVISOR_RETIREMENT_KEY = "receipt";
export const SUPERVISOR_RETIREMENT_MAX_BYTES = 8 * 1024;

/** Content-free, immutable terminal proof. Parsing alone conveys no authority. */
export function parseSupervisorRetirementReceipt(raw: unknown): SignedSupervisorSandboxMessage | null {
  if (typeof raw !== "string" || raw.length > SUPERVISOR_RETIREMENT_MAX_BYTES ||
    new TextEncoder().encode(raw).byteLength > SUPERVISOR_RETIREMENT_MAX_BYTES) return null;
  try {
    const parsed = parseSupervisorSandboxMessage(JSON.parse(raw));
    if (parsed.message.kind !== "shutdown" || canonicalJson(parsed) !== raw) return null;
    return parsed;
  } catch {
    return null;
  }
}

export function encodeSupervisorRetirementReceipt(signed: SignedSupervisorSandboxMessage): string {
  const raw = canonicalJson(parseSupervisorSandboxMessage(signed));
  if (!parseSupervisorRetirementReceipt(raw)) throw new Error("Invalid supervisor retirement receipt");
  return raw;
}

export function readSupervisorRetirementReceipt(doc: Y.Doc): string | null {
  const receipts = doc.getMap<unknown>(SUPERVISOR_RETIREMENT_MAP);
  if (receipts.size !== 1) return null;
  const raw = receipts.get(SUPERVISOR_RETIREMENT_KEY);
  return parseSupervisorRetirementReceipt(raw) ? raw as string : null;
}

export async function validateSupervisorRetirementReceipt(
  raw: unknown,
  binding: BootstrapBinding,
  instanceId: string,
  rotationKey: string,
): Promise<SignedSupervisorSandboxMessage | null> {
  const parsed = parseSupervisorRetirementReceipt(raw);
  if (!parsed || !sameSupervisorBinding(parsed.binding, binding) ||
    parsed.supervisorInstanceId !== instanceId || !await verifySupervisorSandboxMessage(rotationKey, parsed)) return null;
  return parsed;
}
