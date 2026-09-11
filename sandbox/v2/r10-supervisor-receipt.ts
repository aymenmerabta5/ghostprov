import * as Y from "yjs";
import { SYSTEM_CLOCK, type Clock } from "../../src/core/ports.ts";
import type { BootstrapCredentialPayload } from "../../src/v2/bootstrap/crypto.ts";
import { ARCHIVED_R10_RELEASE_IDENTITIES } from "../../src/v2/sidecar/retirement-identities.ts";
import type { SignedSupervisorSandboxMessage } from "../../src/v2/supervisor/protocol.ts";
import type { SupervisorAwarenessProviderLike } from "../../src/v2/supervisor/provider.ts";
import {
  SUPERVISOR_RETIREMENT_KEY, SUPERVISOR_RETIREMENT_MAP,
  encodeSupervisorRetirementReceipt, parseSupervisorRetirementReceipt,
  validateSupervisorRetirementReceipt,
} from "../../src/v2/supervisor/retirement-receipt.ts";
import { proveSupervisorRetirementReadback } from "../../src/v2/supervisor/retirement-store.ts";
import type { SidecarOwnershipProof } from "./ownership-lease.ts";

export interface R10SupervisorRetirementPublication {
  readonly signed: SignedSupervisorSandboxMessage;
  readonly credential: BootstrapCredentialPayload;
  /** Acquired before releasing normal supervisor ownership, held through readback. */
  readonly publicationLease: Pick<SidecarOwnershipProof, "withOwnership" | "assertOwned">;
  readonly createProvider: (doc: Y.Doc) => SupervisorAwarenessProviderLike;
  readonly timeoutMs?: number;
  readonly clock?: Clock;
}

type ReleasableOwnership = Pick<SidecarOwnershipProof, "withOwnership" | "assertOwned"> & { release(): Promise<void> };

export interface R10SupervisorFinalRetirement {
  readonly cleanupChild: () => Promise<void>;
  readonly closePeer: () => Promise<void>;
  readonly ownership: ReleasableOwnership;
  readonly acquirePublicationLease: () => Promise<ReleasableOwnership>;
  readonly retireIdentity: () => Promise<void>;
  readonly receipt: () => Promise<Pick<R10SupervisorRetirementPublication, "signed" | "credential">>;
  readonly createProvider: (doc: Y.Doc) => SupervisorAwarenessProviderLike;
  readonly timeoutMs?: number;
  readonly clock?: Clock;
}

/** Exact local retirement always precedes any remotely consumable final receipt. */
export async function finalizeR10SupervisorRetirement(options: R10SupervisorFinalRetirement): Promise<void> {
  let publicationLease: ReleasableOwnership | null = null;
  let childCleanupProven = false;
  try {
    await options.cleanupChild();
    childCleanupProven = true;
    await options.closePeer();
    await options.ownership.assertOwned();
    publicationLease = await options.acquirePublicationLease();
    await options.ownership.withOwnership(options.retireIdentity);
    await options.ownership.release();
    const receipt = await options.receipt();
    await publicationLease.assertOwned();
    await publishR10SupervisorRetirement({ ...receipt, publicationLease,
      createProvider: options.createProvider, timeoutMs: options.timeoutMs, clock: options.clock });
  } finally {
    // An unproven retained child still needs this live owner and control peer.
    // Caller may retry exact-child cleanup; failure never authorizes detachment.
    if (childCleanupProven) {
      try { await options.closePeer(); }
      finally {
        try { await options.ownership.release(); }
        finally { await publicationLease?.release(); }
      }
    }
  }
}

/** One owner and one timer; a failed cleanup never becomes process-exit proof. */
export async function waitForR10SupervisorChildCleanup(
  cleanup: () => Promise<void>,
  clock: Clock = SYSTEM_CLOCK,
): Promise<void> {
  for (;;) {
    try { await cleanup(); return; }
    catch { await clock.sleep(30_000); }
  }
}

/** The standalone process exits only after its complete lifecycle succeeds. */
export async function finishR10SupervisorProcess(
  operation: () => Promise<void>,
  closeDiagnostics: () => void,
  exitSuccess: () => never | void,
): Promise<void> {
  await operation();
  closeDiagnostics();
  exitSuccess();
}

/** The predecessor is discarded as metadata; it never authorizes any lifecycle operation. */
function mayReplacePredecessor(previousRaw: unknown, current: SignedSupervisorSandboxMessage): boolean {
  const previous = parseSupervisorRetirementReceipt(previousRaw);
  if (!previous || previous.binding.projectId !== current.binding.projectId ||
    previous.binding.leaderGeneration > current.binding.leaderGeneration) return false;
  if (previous.binding.nonce === current.binding.nonce || previous.supervisorInstanceId === current.supervisorInstanceId) return false;
  const registered = previous.binding.buildHash === current.binding.buildHash &&
    previous.binding.schemaHash === current.binding.schemaHash ||
    ARCHIVED_R10_RELEASE_IDENTITIES.some(identity => identity.buildHash === previous.binding.buildHash &&
      identity.schemaHash === previous.binding.schemaHash);
  // Equal leader generations are ordered by the two ownership-lock handoffs,
  // not by random nonces, receipt arrival, or wall-clock timestamps.
  return registered;
}

export async function publishR10SupervisorRetirement(options: R10SupervisorRetirementPublication): Promise<void> {
  const timeoutMs = options.timeoutMs ?? 15_000;
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 15_000) throw new RangeError("Invalid retirement publication timeout");
  const clock = options.clock ?? SYSTEM_CLOCK;
  const raw = encodeSupervisorRetirementReceipt(options.signed);
  if (options.credential.docId !== options.signed.binding.projectId ||
    !await validateSupervisorRetirementReceipt(raw, options.signed.binding, options.signed.supervisorInstanceId, options.credential.rotationKey)) {
    throw new Error("Supervisor retirement publication authority does not match");
  }
  await options.publicationLease.assertOwned();
    const deadline = clock.now() + timeoutMs;
    const doc = new Y.Doc({ guid: `ghostprov:retirement-publisher:${crypto.randomUUID()}` });
    let provider: SupervisorAwarenessProviderLike | null = null;
    try {
      provider = options.createProvider(doc);
      const writer = provider;
      await new Promise<void>((resolve, reject) => {
        let settled = false;
        const timer = new AbortController();
        const finish = (error?: unknown) => {
          if (settled) return;
          settled = true; timer.abort(); writer.off("sync", onSync);
          if (error !== undefined) reject(error); else resolve();
        };
        const onSync = (synced: boolean) => { if (synced) finish(); };
        writer.on("sync", onSync);
        void clock.sleep(timeoutMs, timer.signal).then(
          () => finish(new Error("Supervisor retirement publication deadline expired")),
          error => { if (!timer.signal.aborted) finish(error); },
        );
        try { writer.connect(); } catch (error) { finish(error); }
      });
      // The native publication lease remains held by the finalizer throughout
      // network I/O. Its operation queue is non-reentrant: only the immediate
      // validation/write enters withOwnership, with no nested ownership calls.
      await options.publicationLease.withOwnership(async () => {
        const receipts = doc.getMap<unknown>(SUPERVISOR_RETIREMENT_MAP);
        const previous = receipts.get(SUPERVISOR_RETIREMENT_KEY);
        if (receipts.size > 1 || receipts.size === 1 && !receipts.has(SUPERVISOR_RETIREMENT_KEY) ||
          receipts.has(SUPERVISOR_RETIREMENT_KEY) && previous !== raw && !mayReplacePredecessor(previous, options.signed)) {
          throw new Error("Supervisor retirement receipt conflicts with its predecessor");
        }
        if (clock.now() >= deadline) throw new Error("Supervisor retirement publication deadline expired");
        if (previous !== raw) receipts.set(SUPERVISOR_RETIREMENT_KEY, raw);
      });
      const remaining = Math.floor(deadline - clock.now());
      if (remaining < 1) throw new Error("Supervisor retirement publication deadline expired");
      await proveSupervisorRetirementReadback({
        createProvider: options.createProvider, expectedRaw: raw, timeoutMs: remaining, clock,
      });
      await options.publicationLease.assertOwned();
    } finally {
      try { provider?.disconnect(); }
      finally { try { provider?.destroy(); } finally { doc.destroy(); } }
    }
}
