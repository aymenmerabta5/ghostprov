const KiB = 1024;
const MiB = KiB ** 2;
const STANDARD_LARGE_REQUEST_STEP_BYTES = 512 * KiB;
export type V2AdmissionProfile = "standard" | "bulk-v1";
export const V2_STANDARD_HOST_TRANSFER_BUDGET = 5;
/** Four medium transfers, or one true bulk plus two ordinary requests. */
export const V2_BULK_HOST_TRANSFER_BUDGET = 6;
/** Three observed-size transfers retain at most 9 MiB in aggregate. */
export const V2_OBSERVED_TRANSFER_COST = 2;
/** A request at this cost owns the bulk lane; two such transfers never overlap. */
export const V2_BULK_TRANSFER_COST = 4;

export function v2HostTransferBudget(profile: V2AdmissionProfile): number {
  if (profile !== "standard" && profile !== "bulk-v1") {
    throw new RangeError("V2 admission profile is invalid");
  }
  return profile === "bulk-v1"
    ? V2_BULK_HOST_TRANSFER_BUDGET
    : V2_STANDARD_HOST_TRANSFER_BUDGET;
}

/**
 * Limits concurrent pre-commit body transfer, not upstream model concurrency.
 * Bulk-v1 charges up to 2 MiB as one unit, permitting the existing four-
 * uncommitted-request ceiling without exceeding the retained bytes of one
 * legal bulk request. Requests through 3 MiB use two units, admitting three
 * observed-size bodies while retaining at most 9 MiB total. Anything larger
 * owns four units, so two true bulk transfers cannot overlap inside the six-
 * unit peer budget. Standard rollback retains its original graduated 512 KiB
 * accounting above the first MiB.
 */
export function v2RequestTransferCost(
  bytes: number,
  profile: V2AdmissionProfile = "standard",
): number {
  if (!Number.isSafeInteger(bytes) || bytes < 0) {
    throw new RangeError("V2 request transfer bytes must be a non-negative safe integer");
  }
  v2HostTransferBudget(profile);
  if (bytes <= MiB) return 1;
  if (profile === "bulk-v1") {
    if (bytes <= 2 * MiB) return 1;
    return bytes <= 3 * MiB ? V2_OBSERVED_TRANSFER_COST : V2_BULK_TRANSFER_COST;
  }
  return Math.min(
    V2_BULK_TRANSFER_COST,
    1 + Math.ceil((bytes - MiB) / STANDARD_LARGE_REQUEST_STEP_BYTES),
  );
}
