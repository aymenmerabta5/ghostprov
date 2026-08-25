import type { ProtocolRevision } from "../../src/v2/protocol/shared.ts";

/**
 * The ownership lock stays global so two sidecar revisions can never execute
 * concurrently in one sandbox. Durable journals are revision-local because
 * their record schemas are immutable parts of their respective releases.
 */
export const SIDECAR_OWNERSHIP_ROOT = "/tmp/ghostprov-v2";
/** The immutable R1 artifact reads this legacy location directly. */
export const FROZEN_R1_JOURNAL_ROOT = SIDECAR_OWNERSHIP_ROOT;

export function sidecarJournalRoot(revision: ProtocolRevision): string {
  if (revision !== "r1" && revision !== "r2") {
    throw new TypeError(`Unsupported sidecar journal revision: ${String(revision)}`);
  }
  return revision === "r1"
    ? FROZEN_R1_JOURNAL_ROOT
    : `${SIDECAR_OWNERSHIP_ROOT}/journals/r2`;
}
