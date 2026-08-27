const KiB = 1024;
const MiB = KiB ** 2;
const LARGE_REQUEST_STEP_BYTES = 512 * KiB;
/** One bulk transfer plus one ordinary request may be pre-commit concurrently. */
export const V2_HOST_TRANSFER_BUDGET = 5;
/** A request at this cost owns the bulk lane; two such transfers never overlap. */
export const V2_BULK_TRANSFER_COST = 4;

/**
 * Limits concurrent pre-commit body transfer, not upstream model concurrency.
 * The first MiB costs one slot so the established ordinary workload keeps its
 * concurrency. Each additional 512 KiB consumes another slot, capped at the
 * existing four-uncommitted-request safety boundary.
 */
export function v2RequestTransferCost(bytes: number): number {
  if (!Number.isSafeInteger(bytes) || bytes < 0) {
    throw new RangeError("V2 request transfer bytes must be a non-negative safe integer");
  }
  if (bytes <= MiB) return 1;
  return Math.min(
    V2_BULK_TRANSFER_COST,
    1 + Math.ceil((bytes - MiB) / LARGE_REQUEST_STEP_BYTES),
  );
}
