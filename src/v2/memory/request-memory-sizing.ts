/** Body plus one possible fetch/runtime transport copy. */
export const V2_REQUEST_BODY_WORKING_SET_FACTOR = 2;
/** Incremental hash state, request metadata, and transport bookkeeping. */
export const V2_REQUEST_BODY_FIXED_OVERHEAD_BYTES = 64 * 1024;

export function v2RequestBodyWorkingSetBytes(decodedBytes: number): number {
  if (!Number.isSafeInteger(decodedBytes) || decodedBytes < 1) {
    throw new RangeError("decodedBytes must be a positive safe integer");
  }
  const total = decodedBytes * V2_REQUEST_BODY_WORKING_SET_FACTOR +
    V2_REQUEST_BODY_FIXED_OVERHEAD_BYTES;
  if (!Number.isSafeInteger(total)) {
    throw new RangeError("Request body working set exceeds safe integer capacity");
  }
  return total;
}
